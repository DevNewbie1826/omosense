// Package google hosts the calendar and mail watcher backed by the zele
// CLI, printing CAL, SOON, MAIL and LOG lines.
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
// under the watch-google lock with the TS stdout grammar and exit semantics.
func Run(ctx *core.Ctx, args []string) int {
	fmt.Fprintln(os.Stderr, "omosense google: not implemented yet")
	return 1
}

// Sources returns the google source of ctx's profile.
func Sources(ctx *core.Ctx) []core.Source {
	return []core.Source{
		src{
			name:     "google",
			prefixes: []string{"CAL", "SOON", "MAIL"},
			lock:     "watch-google-" + ctx.Profile.Name,
			legacy:   "watch-google",
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
