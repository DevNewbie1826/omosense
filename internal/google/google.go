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
const Help = `Usage: omosense google [--profile P] [--once]

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
	name, legacy := lockNames(c.Profile.Name)
	release := c.Acquire(name, legacy)
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

func lockNames(profile string) (name, legacy string) {
	return "watch-google-" + profile, "watch-google"
}

// Sources returns the google source of ctx's profile.
func Sources(c *core.Ctx) []core.Source {
	name, legacy := lockNames(c.Profile.Name)
	return []core.Source{
		src{
			c:        c,
			name:     "google",
			prefixes: []string{"CAL", "SOON", "MAIL"},
			lock:     name,
			legacy:   legacy,
		},
	}
}

// src is the daemon-hosted google source: one pausable watcher per profile.
type src struct {
	c        *core.Ctx
	name     string
	prefixes []string
	lock     string
	legacy   string
}

func (s src) Name() string               { return s.name }
func (s src) Prefixes() []string         { return s.prefixes }
func (s src) AlwaysOn() bool             { return false }
func (s src) LockName() (string, string) { return s.lock, s.legacy }

// Run hosts the watcher on the daemon's sink. The daemon host owns the
// watch-google lock (IS-15), so no lock is taken here.
func (s src) Run(ctx context.Context, sink core.Sink) error {
	w, err := newWatcher(s.c, sink, true)
	if err != nil {
		return err
	}
	return w.run(ctx)
}
