// Package remind hosts the reminder scheduler: it reads the reminder file
// every 20 seconds and sends due reminders through say, printing REMIND and
// LOG lines.
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
func Run(ctx *core.Ctx, args []string) int {
	fmt.Fprintln(os.Stderr, "omosense remind: not implemented yet")
	return 1
}

// Sources returns the remind source of ctx's profile. It is always-on in
// the daemon because it must send even with no client attached.
func Sources(ctx *core.Ctx) []core.Source {
	return []core.Source{
		src{
			name:     "remind",
			prefixes: []string{"REMIND"},
			alwaysOn: true,
			lock:     "remind-" + ctx.Profile.Name,
			legacy:   "remind",
		},
	}
}

// src is the metadata-only Source used until the real source lands.
type src struct {
	name     string
	prefixes []string
	alwaysOn bool
	lock     string
	legacy   string
}

func (s src) Name() string               { return s.name }
func (s src) Prefixes() []string         { return s.prefixes }
func (s src) AlwaysOn() bool             { return s.alwaysOn }
func (s src) LockName() (string, string) { return s.lock, s.legacy }

func (s src) Run(ctx context.Context, sink core.Sink) error {
	return fmt.Errorf("%s: not implemented", s.name)
}
