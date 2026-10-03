// Package tidy hosts the memory-tidy watcher, printing TIDY and LOG lines.
package tidy

import (
	"context"
	"fmt"
	"os"

	"github.com/DevNewbie1826/omosense/internal/core"
)

// Help is the usage text printed by omosense tidy --help.
const Help = `Usage: omosense tidy [--profile P] [--once|--now] [flags]

Runs the memory tidy watcher, printing TIDY and LOG lines to stdout.

Flags:
  --check-min/--quiet-min/--max-min [m]   thresholds in minutes
  --write-watermark [repo=sha ...]        update the watermark and exit
  --backup-now                            run a backup and exit
  --once, --now                           read-only check
`

// Run is the compat subcommand host: it runs the tidy source in-process
// under the memory-tidy lock with the TS stdout grammar and exit semantics.
func Run(ctx *core.Ctx, args []string) int {
	fmt.Fprintln(os.Stderr, "omosense tidy: not implemented yet")
	return 1
}

// Sources returns the tidy source of ctx's profile (no legacy lock name).
func Sources(ctx *core.Ctx) []core.Source {
	return []core.Source{
		src{
			name:     "tidy",
			prefixes: []string{"TIDY"},
			lock:     "memory-tidy-" + ctx.Profile.Name,
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
