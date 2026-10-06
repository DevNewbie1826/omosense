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

// tidyer is one profile's memory-tidy watcher: resolved paths, which repos
// heads() may report, thresholds, and the injected clock/sleep.
type tidyer struct {
	state       string
	profile     string
	memory      string
	exclude     map[string]bool
	learnOthers bool
	agents      string
	backups     string
	sink        core.Sink
	checkMs     float64
	quietMs     float64
	maxMs       float64
	now         func() time.Time
	sleep       func(context.Context, time.Duration) error
}

// newTidyer resolves AGENTS ($OMO_MEMORY_AGENTS or ~/.omo/memory/agents)
// and BACKUPS (~/.omo/memory-backups) once, with the TS thresholds.
func newTidyer(c *core.Ctx, sink core.Sink) *tidyer {
	home, _ := os.UserHomeDir()
	agents := os.Getenv("OMO_MEMORY_AGENTS")
	if agents == "" {
		agents = filepath.Join(home, ".omo", "memory", "agents")
	}
	exclude := make(map[string]bool, len(c.Profile.Tidy.Exclude))
	for _, name := range c.Profile.Tidy.Exclude {
		exclude[name] = true
	}
	return &tidyer{
		state:       c.State,
		profile:     c.Profile.Name,
		memory:      c.Profile.Memory,
		exclude:     exclude,
		learnOthers: c.Profile.Tidy.LearnOthers,
		agents:      agents,
		backups:     filepath.Join(home, ".omo", "memory-backups"),
		sink:        sink,
		checkMs:     defaultCheckMin * 60_000,
		quietMs:     defaultQuietMin * 60_000,
		maxMs:       defaultMaxMin * 60_000,
		now:         nowFn,
		sleep:       sleepFn,
	}
}

// runLoop is the locked watcher loop: the startup LOG, then tick /
// check-interval forever. Tick errors are logged and the loop continues,
// matching the TS try/catch; ctx cancellation (a daemon stop) returns nil.
func (t *tidyer) runLoop(ctx context.Context) error {
	t.sink.Log(fmt.Sprintf("memory-tidy watcher starting (profile %s, check %sm, quiet %sm, max %sm)",
		t.profile, minStr(t.checkMs), minStr(t.quietMs), minStr(t.maxMs)))
	pendingSince := map[string]float64{}
	emitted := map[string]emitRec{}
	for {
		if ctx.Err() != nil {
			return nil
		}
		if err := t.tick(ctx, t.nowMs(), pendingSince, emitted); err != nil && ctx.Err() == nil {
			t.sink.Log("memory-tidy " + err.Error())
		}
		if err := t.sleep(ctx, time.Duration(t.checkMs)*time.Millisecond); err != nil {
			return nil
		}
	}
}

// tick is one TS try block: purge repos that left the changed set, mark
// the ready ones (quiet elapsed since the commit, or max elapsed since the
// repo was first seen, and not muted by the 6h re-emit guard), emit one
// TIDY line for all ready repos, then run the daily backup. An error
// aborts the rest of the pass, like the throw.
func (t *tidyer) tick(ctx context.Context, now float64, pendingSince map[string]float64, emitted map[string]emitRec) error {
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
	for repo := range pendingSince {
		if !stillChanged[repo] {
			delete(pendingSince, repo)
			delete(emitted, repo)
		}
	}
	var ready []change
	for _, x := range changes {
		if _, ok := pendingSince[x.repo]; !ok {
			pendingSince[x.repo] = now
		}
		since := pendingSince[x.repo]
		if e, ok := emitted[x.repo]; ok && e.to == x.to && now-e.at < reemitMs {
			continue
		}
		if now-x.committedAt >= t.quietMs || now-since >= t.maxMs {
			ready = append(ready, x)
		}
	}
	if len(ready) > 0 {
		t.emitTidy(ready)
		for _, x := range ready {
			emitted[x.repo] = emitRec{to: x.to, at: now}
		}
	}
	_, err = t.backup(ctx, true)
	return err
}

func (t *tidyer) emitTidy(changes []change) {
	payload := make([]tidyChange, len(changes))
	for i, x := range changes {
		payload[i] = tidyChange{Repo: x.repo, From: x.from, To: x.to}
	}
	t.sink.Emit("TIDY", tidyEvent{Changed: payload})
}

func (t *tidyer) nowMs() float64 { return float64(t.now().UnixMilli()) }

// minStr renders ms/60000 the way JS String() would: integral values
// without decimals or exponent notation.
func minStr(ms float64) string {
	return strconv.FormatFloat(ms/60_000, 'f', -1, 64)
}
