// Package remind hosts the reminder scheduler: it reads the reminder file
// every 20 seconds, sends due reminders through the say subcommand and
// prints REMIND and LOG lines. sent, skipped, failed and cancelled are
// terminal. A failed send records failed and error and is never retried
// (plan IS-8), fixing remind.ts, which retried failed sends on every tick.
//
// Each tick selects the due entries under a short reminders.lock and sends
// them outside it, recording every result under a second short lock, so
// `remind add` never waits for an in-flight send.
package remind

import (
	"context"
	"fmt"
	"os"

	"github.com/DevNewbie1826/omosense/internal/core"
)

// Help is the usage text printed by omosense remind --help.
const Help = `Usage: omosense remind
       omosense remind add (--at TIME | --in DURATION) --platform telegram|discord --target JSON --text TEXT [--id ID]

Runs the reminder scheduler, printing REMIND and LOG lines to stdout.

add appends one pending reminder to <state>/reminders.json and prints
"REMIND added <entry>". TIME is an ISO time (2026-10-10T09:00:00+09:00);
DURATION is a Go duration from now (30m, 2h). A time more than 6h in the
past is refused. Example:

  omosense remind add --in 30m --platform telegram --target '{"chat_id":123}' --text "stand-up"
`

// Run is the compat subcommand host: it runs the remind source in-process
// under the remind lock with the TS stdout grammar and exit semantics.
func Run(c *core.Ctx, args []string) int {
	if len(args) > 0 && args[0] == "add" {
		return add(c, args[1:], os.Stdout, os.Stderr)
	}
	release := c.Acquire("remind", "")
	defer release()
	if err := newScheduler(c, c.Out).run(context.Background()); err != nil {
		fmt.Fprintln(os.Stderr, "omosense:", err)
		return 1
	}
	return 0
}

// Sources returns the remind source. It is always-on in the host: reminders
// must fire even with no client attached.
func Sources(c *core.Ctx) []core.Source {
	return []core.Source{
		src{
			c:        c,
			name:     "remind",
			prefixes: []string{"REMIND"},
			lock:     "remind",
		},
	}
}

// src is the host's remind source; the host owns the lock (IS-15), so Run
// only schedules.
type src struct {
	c        *core.Ctx
	name     string
	prefixes []string
	lock     string
}

func (s src) Name() string               { return s.name }
func (s src) Prefixes() []string         { return s.prefixes }
func (s src) AlwaysOn() bool             { return true }
func (s src) LockName() (string, string) { return s.lock, "" }

func (s src) Run(ctx context.Context, sink core.Sink) error {
	return newScheduler(s.c, sink).run(ctx)
}
