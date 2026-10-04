// Package remind hosts the reminder scheduler: it reads the reminder file
// every 20 seconds, sends due reminders through the say subcommand and
// prints REMIND and LOG lines. sent, skipped, failed and cancelled are
// terminal. A failed send records failed and error and is never retried
// (plan IS-8), fixing remind.ts, which retried failed sends on every tick.
package remind

import (
	"context"
	"fmt"
	"os"

	"github.com/DevNewbie1826/omosense/internal/core"
)

// Help is the usage text printed by omosense remind --help.
const Help = `Usage: omosense remind [--profile P]

Runs the reminder scheduler, printing REMIND and LOG lines to stdout.
`

// Run is the compat subcommand host: it runs the remind source in-process
// under the remind lock with the TS stdout grammar and exit semantics.
func Run(c *core.Ctx, args []string) int {
	_ = args
	name, legacy := lockNames(c.Profile.Name)
	release := c.Acquire(name, legacy)
	defer release()
	if err := newScheduler(c, c.Out).run(context.Background()); err != nil {
		fmt.Fprintln(os.Stderr, "omosense:", err)
		return 1
	}
	return 0
}

func lockNames(profile string) (name, legacy string) {
	return "remind-" + profile, "remind"
}

// Sources returns the remind source of ctx's profile. It is always-on in
// the daemon (IS-13): reminders must fire even with no client attached.
func Sources(c *core.Ctx) []core.Source {
	name, legacy := lockNames(c.Profile.Name)
	return []core.Source{
		src{
			c:        c,
			name:     "remind",
			prefixes: []string{"REMIND"},
			lock:     name,
			legacy:   legacy,
		},
	}
}

// src is the daemon-hosted remind source; the daemon host owns the lock
// (IS-15), so Run only schedules.
type src struct {
	c        *core.Ctx
	name     string
	prefixes []string
	lock     string
	legacy   string
}

func (s src) Name() string               { return s.name }
func (s src) Prefixes() []string         { return s.prefixes }
func (s src) AlwaysOn() bool             { return true }
func (s src) LockName() (string, string) { return s.lock, s.legacy }

func (s src) Run(ctx context.Context, sink core.Sink) error {
	return newScheduler(s.c, sink).run(ctx)
}
