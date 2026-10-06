// Package google hosts the calendar and mail watcher backed by the zele
// CLI, printing CAL, SOON, MAIL and LOG lines with watch-google.ts
// semantics: Bun-compatible seen keys, a year-corrected SOON window, the
// NOISE mail filter, 7-day pruning and a read-only --once snapshot mode.
package google

import (
	"context"
	"fmt"
	"os"

	"github.com/DevNewbie1826/omosense/internal/core"
)

// Help is the usage text printed by omosense google --help.
const Help = `Usage: omosense google [--once]

Watches calendars and mail via zele, printing CAL, SOON, MAIL and LOG
lines to stdout.

Flags:
  --once    read-only single pass: no lock, no state writes, no sends
`

// Run is the compat subcommand host: it runs the google source in-process
// under the watch-google lock with the TS stdout grammar and exit
// semantics, or a single read-only --once pass without the lock.
func Run(c *core.Ctx, args []string) int {
	_ = args
	if c.Flags["--once"] {
		w, err := newWatcher(c, c.Out, false)
		if err != nil {
			fmt.Fprintln(os.Stderr, "omosense:", err)
			return 1
		}
		w.once(context.Background())
		return 0
	}
	release := c.Acquire("watch-google", "")
	defer release()
	w, err := newWatcher(c, c.Out, true)
	if err != nil {
		fmt.Fprintln(os.Stderr, "omosense:", err)
		return 1
	}
	if err := w.run(context.Background()); err != nil {
		fmt.Fprintln(os.Stderr, "omosense:", err)
		return 1
	}
	return 0
}

// Sources returns the google source of the folder's config.
func Sources(c *core.Ctx) []core.Source {
	return []core.Source{
		src{
			c:        c,
			name:     "google",
			prefixes: []string{"CAL", "SOON", "MAIL"},
			lock:     "watch-google",
		},
	}
}

// src is the host's google source.
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

// Run hosts the watcher on the host's sink. The host owns the
// watch-google lock (IS-15), so no lock is taken here.
func (s src) Run(ctx context.Context, sink core.Sink) error {
	w, err := newWatcher(s.c, sink, true)
	if err != nil {
		return err
	}
	return w.run(ctx)
}
