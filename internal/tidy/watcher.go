package tidy

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/DevNewbie1826/omosense/internal/core"
)

// Test hooks, replaced in tests so the loop runs on a fake clock and
// finishes without waiting out real intervals (plan: "clock injected").
var (
	nowFn   = time.Now
	sleepFn = sleepCtx
)

// reemitMs is memory-tidy.ts's REEMIT_MS: a repo whose watermark entry is
// still stale is re-announced 6h after the previous announcement.
const reemitMs = 6 * 3600_000

// maxTidyPerLine caps how many repos one TIDY line carries (IS-1): a tick
// with more ready repos prints consecutive lines of this size, the last
// line the remainder, in the same order.
const maxTidyPerLine = 10

// sleepCtx sleeps for d, returning ctx.Err() when ctx was cancelled first.
func sleepCtx(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

// tidyChange is one entry of the TIDY payload; field order fixes the JSON
// shape {repo, from, to} (committedAt never leaves the process).
type tidyChange struct {
	Repo string `json:"repo"`
	From any    `json:"from"`
	To   string `json:"to"`
}

type tidyEvent struct {
	Changed []tidyChange `json:"changed"`
}

// change is one repo whose watermark sha differs from HEAD. committedAt is
// %ct*1000 (NaN when git printed no parseable committer time, so the quiet
// rule never fires for it, matching Number(undefined)).
type change struct {
	repo        string
	from        any
	to          string
	committedAt float64
}

// emitRec remembers what and when a repo was last announced, the TS
// emitted map entry.
type emitRec struct {
	to string
	at float64
}

// tidyer is the memory-tidy watcher: resolved paths, which repos heads()
// may report, thresholds, and the injected clock/sleep.
type tidyer struct {
	state       string
	memory      string
	exclude     map[string]bool
	learnOthers bool
	agents      string
	backups     string
	sink        core.Sink
	checkMs     float64
	quietMs     float64
	now         func() time.Time
	sleep       func(context.Context, time.Duration) error
	// firstRun is IS-2: <state>/memory-tidy.json did not exist when
	// runLoop started. The first tick that emits consumes it with one
	// guidance LOG (the first tick's backup pass may create the file).
	firstRun bool
}

// newTidyer resolves AGENTS via the shared core rule ($OMO_MEMORY_AGENTS
// or ~/.omo/memory/agents, IS-14) and BACKUPS (~/.omo/memory-backups)
// once, with the thresholds from the tidy config over the TS defaults
// (the CLI flags apply on top in Run).
func newTidyer(c *core.Ctx, sink core.Sink) *tidyer {
	home, _ := os.UserHomeDir()
	// Without a home the agents path degrades exactly as before; tests
	// and live folders always have one (or OMO_MEMORY_AGENTS set).
	agents, _ := core.MemoryAgentsDir()
	exclude := make(map[string]bool, len(c.Profile.Tidy.Exclude))
	for _, name := range c.Profile.Tidy.Exclude {
		exclude[name] = true
	}
	checkMs := defaultCheckMin * 60_000.0
	if m := c.Profile.Tidy.CheckMin; m != nil {
		checkMs = *m * 60_000
	}
	quietMs := defaultQuietMin * 60_000.0
	if m := c.Profile.Tidy.QuietMin; m != nil {
		quietMs = *m * 60_000
	}
	return &tidyer{
		state:       c.State,
		memory:      c.Profile.Memory,
		exclude:     exclude,
		learnOthers: c.Profile.Tidy.LearnOthers,
		agents:      agents,
		backups:     filepath.Join(home, ".omo", "memory-backups"),
		sink:        sink,
		checkMs:     checkMs,
		quietMs:     quietMs,
		now:         nowFn,
		sleep:       sleepFn,
	}
}

// runLoop is the locked watcher loop: the startup LOG, then tick /
// check-interval forever. Tick errors are logged and the loop continues,
// matching the TS try/catch; ctx cancellation (a daemon stop) returns nil.
func (t *tidyer) runLoop(ctx context.Context) error {
	t.sink.Log(fmt.Sprintf("memory-tidy watcher starting (check %sm, quiet %sm)",
		minStr(t.checkMs), minStr(t.quietMs)))
	// IS-2: first run = no watermark file when the loop starts. Captured
	// here, before the first tick, because that tick's backup pass writes
	// the file.
	if _, err := os.Stat(t.watermarkPath()); err != nil {
		t.firstRun = true
	}
	// IS-4/IS-5: the announcement record is persisted, so a restart does
	// not reset the 6h re-emit window; an unreadable file fails open.
	emitted := t.readAnnounced()
	for {
		if ctx.Err() != nil {
			return nil
		}
		if err := t.tick(ctx, t.nowMs(), emitted); err != nil && ctx.Err() == nil {
			t.sink.Log("memory-tidy " + err.Error())
		}
		if err := t.sleep(ctx, time.Duration(t.checkMs)*time.Millisecond); err != nil {
			return nil
		}
	}
}

// tick is one TS try block: purge repos that left the changed set, mark
// the ready ones (quiet elapsed since the HEAD commit, and not muted by
// the 6h re-emit guard), emit the ready repos as TIDY lines of at most
// maxTidyPerLine entries, persist the announcement record, then run the
// daily backup. An error aborts the rest of the pass, like the throw.
func (t *tidyer) tick(ctx context.Context, now float64, emitted map[string]emitRec) error {
	changes, err := t.changed(ctx)
	if err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	stillChanged := make(map[string]bool, len(changes))
	for _, x := range changes {
		stillChanged[x.repo] = true
	}
	purged := false
	for repo := range emitted {
		if !stillChanged[repo] {
			delete(emitted, repo)
			purged = true
		}
	}
	var ready []change
	for _, x := range changes {
		if e, ok := emitted[x.repo]; ok && e.to == x.to && now-e.at < reemitMs {
			continue
		}
		if now-x.committedAt >= t.quietMs {
			ready = append(ready, x)
		}
	}
	if len(ready) > 0 {
		groups := tidyGroups(ready)
		if t.firstRun {
			t.sink.Log(fmt.Sprintf("memory-tidy no watermark yet: %d repos reported in %d TIDY lines; run \"omosense tidy --write-watermark\" to mark the current HEADs as tidied",
				len(ready), len(groups)))
			t.firstRun = false
		}
		t.emitTidy(groups)
		for _, x := range ready {
			emitted[x.repo] = emitRec{to: x.to, at: now}
		}
	}
	if purged || len(ready) > 0 {
		// IS-5: a failed save must not abort the tick (the backup below
		// still runs) and must not drop the in-memory record, which keeps
		// the 6h mute rule working for the rest of this process.
		if err := t.writeAnnounced(emitted); err != nil {
			t.sink.Log(fmt.Sprintf("memory-tidy announced state not saved: %v", err))
		}
	}
	_, err = t.backup(ctx, true)
	return err
}

// tidyGroups cuts changes into consecutive groups of at most
// maxTidyPerLine entries, preserving order (IS-1).
func tidyGroups(changes []change) [][]change {
	var groups [][]change
	for start := 0; start < len(changes); start += maxTidyPerLine {
		groups = append(groups, changes[start:min(start+maxTidyPerLine, len(changes))])
	}
	return groups
}

// emitTidy prints one TIDY line per group, each carrying the same
// TIDY {"changed":[...]} shape as before (IS-1).
func (t *tidyer) emitTidy(groups [][]change) {
	for _, g := range groups {
		payload := make([]tidyChange, len(g))
		for i, x := range g {
			payload[i] = tidyChange{Repo: x.repo, From: x.from, To: x.to}
		}
		t.sink.Emit("TIDY", tidyEvent{Changed: payload})
	}
}

func (t *tidyer) nowMs() float64 { return float64(t.now().UnixMilli()) }

// minStr renders ms/60000 the way JS String() would: integral values
// without decimals or exponent notation.
func minStr(ms float64) string {
	return strconv.FormatFloat(ms/60_000, 'f', -1, 64)
}
