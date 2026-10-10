// Package herdr hosts the herdr pane watcher, printing HERDR and LOG lines
// with watch-herdr.ts tick rules, plus a read-only --once SNAP snapshot.
package herdr

import (
	"context"
	"fmt"
	"os"

	"github.com/DevNewbie1826/omosense/internal/core"
)

// Help is the usage text printed by omosense herdr --help.
const Help = `Usage: omosense herdr [--once]

Watches herdr panes every 5 seconds, printing HERDR and LOG lines to
stdout.

A blocked line is printed only for registered job panes. Set
herdr.blockedAll to true (default false) to print one for every pane
except omosense's own pane. A pane that re-enters blocked within
herdr.blockedCooldownSec seconds (default 60, 0 = off) of its last
printed blocked line prints nothing until the window has passed. A job
pane's working to idle/done goes into one done-batch line, printed once
5 quiet minutes pass with no new done.

Flags:
  --once    read-only single snapshot: prints SNAP lines, no lock
`

// Run is the compat subcommand host: it runs the herdr source in-process
// under the watch-herdr lock with the TS stdout grammar and exit semantics,
// or a single read-only --once snapshot without the lock.
func Run(c *core.Ctx, args []string) int {
	_ = args
	if c.Flags["--once"] {
		newWatcher(c, c.Out).once(context.Background())
		return 0
	}
	release := c.Acquire("watch-herdr", "")
	defer release()
	if err := newWatcher(c, c.Out).run(context.Background()); err != nil {
		fmt.Fprintln(os.Stderr, "omosense:", err)
		return 1
	}
	return 0
}

// Sources returns the herdr source. It is a default source (the host
// registers it unless herdr.enabled is explicitly false) and takes the
// watch-herdr lock.
func Sources(c *core.Ctx) []core.Source {
	return []core.Source{src{
		c:        c,
		name:     "herdr",
		prefixes: []string{"HERDR"},
		lock:     "watch-herdr",
	}}
}

// src is the host's herdr source. The host owns the lock (IS-15), so Run
// only watches.
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
	return newWatcher(s.c, sink).run(ctx)
}
