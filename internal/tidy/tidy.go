// Package tidy hosts the memory-tidy watcher: it prints a TIDY line when a
// source memory repo's HEAD moved past the watermark, keeps daily
// git-bundle backups, and mirrors the memory-tidy watermark file.
package tidy

import (
	"context"
	"fmt"
	"os"
	"slices"
	"strings"

	"github.com/DevNewbie1826/omosense/internal/core"
)

// Help is the usage text printed by omosense tidy --help.
const Help = `Usage: omosense tidy [--once|--now] [flags]

Runs the memory tidy watcher, printing TIDY and LOG lines to stdout.

Flags:
  --check-min/--quiet-min/--max-min [m]   thresholds in minutes (JS number)
  --write-watermark [repo=sha ...]        update the watermark and exit
  --backup-now                            run the daily backup and exit
  --once, --now                           read-only change check
`

// Run is the compat subcommand host, in memory-tidy.ts order: the
// threshold flags parse first (a bad value logs and exits 2), then a
// disabled profile prints one LOG line and exits 0. Otherwise
// --write-watermark, --backup-now and the read-only --now/--once run
// without the lock, and everything else enters the locked watcher loop.
func Run(c *core.Ctx, args []string) int {
	t := newTidyer(c, c.Out)
	checkMs, quietMs, maxMs, ok := parseMinutes(c.Out, args)
	if !ok {
		return 2
	}
	t.checkMs, t.quietMs, t.maxMs = checkMs, quietMs, maxMs
	if !c.Profile.Tidy.Enabled {
		t.sink.Log("tidy disabled")
		return 0
	}

	switch {
	case c.Flags["--write-watermark"]:
		if err := t.writeWatermarkCmd(context.Background(), args); err != nil {
			fmt.Fprintln(os.Stderr, "omosense:", err)
			return 1
		}
		return 0
	case c.Flags["--backup-now"]:
		allGood, err := t.backup(context.Background(), false)
		if err != nil {
			fmt.Fprintln(os.Stderr, "omosense:", err)
			return 1
		}
		if !allGood {
			return 1
		}
		return 0
	case c.Flags["--now"] || c.Flags["--once"]:
		if err := t.nowOnce(context.Background()); err != nil {
			fmt.Fprintln(os.Stderr, "omosense:", err)
			return 1
		}
		return 0
	}
	release := c.Acquire("memory-tidy", "")
	defer release()
	if err := t.runLoop(context.Background()); err != nil {
		fmt.Fprintln(os.Stderr, "omosense:", err)
		return 1
	}
	return 0
}

// writeWatermarkCmd is --write-watermark: the repo=sha pairs after the
// flag (skipping --flags and values without "=") override the current
// heads as the repos map, lastRun is stamped, and the LOG line reports
// the unique key count. The JS split("=", 2) limit drops any tail:
// "a=b=c" sets repo "a" to "b".
func (t *tidyer) writeWatermarkCmd(ctx context.Context, args []string) error {
	w, err := t.readWatermark()
	if err != nil {
		return err
	}
	set := core.NewOMap()
	if i := slices.Index(args, "--write-watermark"); i >= 0 {
		for _, a := range args[i+1:] {
			if strings.HasPrefix(a, "--") || !strings.Contains(a, "=") {
				continue
			}
			parts := strings.SplitN(a, "=", 3)
			set.Set(parts[0], parts[1])
		}
	}
	count := set.Len()
	if count > 0 {
		if repos := reposOf(w); repos != nil {
			for _, k := range set.Keys() {
				v, _ := set.Get(k)
				repos.Set(k, v)
			}
		}
	} else {
		heads, err := t.heads(ctx)
		if err != nil {
			return err
		}
		count = len(heads)
		if repos := reposOf(w); repos != nil {
			for _, h := range heads {
				repos.Set(h.name, h.sha)
			}
		}
	}
	w.Set("lastRun", core.ISO(t.now()))
	if err := writeWatermarkDoc(t.watermarkPath(), w); err != nil {
		return err
	}
	t.sink.Log(fmt.Sprintf("memory-tidy watermark set %d repos", count))
	return nil
}

// nowOnce is the read-only --now/--once check: print one TIDY line when
// any repo changed; never lock and never write (IS-9).
func (t *tidyer) nowOnce(ctx context.Context) error {
	changes, err := t.changed(ctx)
	if err != nil {
		return err
	}
	if len(changes) > 0 {
		t.emitTidy(changes)
	}
	return nil
}

// Sources returns the tidy source when tidy.enabled is set, and no source
// otherwise. It takes the memory-tidy lock.
func Sources(c *core.Ctx) []core.Source {
	if !c.Profile.Tidy.Enabled {
		return nil
	}
	return []core.Source{src{
		c:        c,
		name:     "tidy",
		prefixes: []string{"TIDY"},
		lock:     "memory-tidy",
	}}
}

// src is the host's tidy source. The host owns the lock (IS-15), so Run only
// watches, with the TS default thresholds.
type src struct {
	c        *core.Ctx
	name     string
	prefixes []string
	lock     string
}

func (s src) Name() string               { return s.name }
func (s src) Prefixes() []string         { return s.prefixes }
func (s src) AlwaysOn() bool             { return false }
func (s src) LockName() (string, string) { return s.lock, "" }

func (s src) Run(ctx context.Context, sink core.Sink) error {
	return newTidyer(s.c, sink).runLoop(ctx)
}
