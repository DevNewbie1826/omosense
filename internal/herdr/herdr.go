// Package herdr hosts the herdr pane watcher, printing HERDR and LOG lines.
package herdr

import (
	"context"
	"fmt"
	"os"

	"github.com/DevNewbie1826/omosense/internal/core"
)

// Help is the usage text printed by omosense herdr --help.
const Help = `Usage: omosense herdr [--profile P] [--once]

Watches herdr panes every 5 seconds, printing HERDR and LOG lines to
stdout.

Flags:
  --once    read-only single snapshot: prints SNAP lines, no lock
`

// Run is the compat subcommand host: it runs the herdr source in-process
// under the watch-herdr lock with the TS stdout grammar and exit semantics.
func Run(ctx *core.Ctx, args []string) int {
	fmt.Fprintln(os.Stderr, "omosense herdr: not implemented yet")
	return 1
}

// Sources returns the herdr source of ctx's profile (no legacy lock name).
func Sources(ctx *core.Ctx) []core.Source {
	return []core.Source{
		src{
			name:     "herdr",
			prefixes: []string{"HERDR"},
			lock:     "watch-herdr-" + ctx.Profile.Name,
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
