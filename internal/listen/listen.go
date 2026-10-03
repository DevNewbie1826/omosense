// Package listen hosts the inbound sources: the Telegram bots polled with
// long-poll getUpdates and the Discord gateway connection. Both print EVENT
// and LOG lines.
package listen

import (
	"context"
	"fmt"
	"os"

	"github.com/DevNewbie1826/omosense/internal/core"
)

// Help is the usage text printed by omosense listen --help.
const Help = `Usage: omosense listen [--profile P] [--dry-run]

Runs the profile's Telegram bots and the Discord gateway listener,
printing EVENT and LOG lines to stdout.

Flags:
  --dry-run    print the run plan and exit 0 without taking the lock
`

// Run is the compat subcommand host: it runs the listen sources in-process
// under the listen lock with the TS stdout grammar and exit semantics.
func Run(ctx *core.Ctx, args []string) int {
	fmt.Fprintln(os.Stderr, "omosense listen: not implemented yet")
	return 1
}

// Sources returns the telegram and discord sources of ctx's profile. They
// share the listen-<profile> lock (legacy "listen"); the daemon refcounts
// that lock across the two.
func Sources(ctx *core.Ctx) []core.Source {
	p := ctx.Profile.Name
	return []core.Source{
		src{
			name:     "telegram",
			prefixes: []string{"EVENT"},
			lock:     "listen-" + p,
			legacy:   "listen",
		},
		src{
			name:     "discord",
			prefixes: []string{"EVENT"},
			alwaysOn: true,
			lock:     "listen-" + p,
			legacy:   "listen",
		},
	}
}

// src is the metadata-only Source used until the real sources land.
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
