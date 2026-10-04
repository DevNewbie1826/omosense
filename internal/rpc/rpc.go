// Package rpc watches webchat sessions through read-only rpc.sock snapshots.
package rpc

import (
	"context"

	"github.com/DevNewbie1826/omosense/internal/core"
)

// Help is the compat subcommand's usage.
const Help = `Usage: omosense rpc [--profile P] [--once] [--all]

Watches registered webchat sessions every 5 seconds, printing RPC and LOG lines.

Flags:
  --once    read-only single snapshot: prints SNAP lines, no lock
  --all     watch all sessions, not only threads.json registrations
`

// Run hosts the compat watcher; snapshots never acquire a lock.
func Run(c *core.Ctx, args []string) int {
	_ = args
	w := newWatcher(c, c.Out)
	if c.Flags["--once"] {
		if err := w.once(context.Background()); err != nil {
			c.Out.Log("rpc " + err.Error())
			return 1
		}
		return 0
	}
	release := c.Acquire("watch-rpc-"+c.Profile.Name, "")
	defer release()
	if err := w.run(context.Background()); err != nil {
		c.Out.Log("rpc " + err.Error())
		return 1
	}
	return 0
}

// Sources supplies the pause-while-idle source. The daemon owns its lock.
func Sources(c *core.Ctx) []core.Source {
	return []core.Source{src{c: c}}
}

type src struct{ c *core.Ctx }

func (s src) Name() string               { return "rpc" }
func (s src) Prefixes() []string         { return []string{"RPC"} }
func (s src) AlwaysOn() bool             { return false }
func (s src) LockName() (string, string) { return "watch-rpc-" + s.c.Profile.Name, "" }
func (s src) Run(ctx context.Context, sink core.Sink) error {
	return newWatcher(s.c, sink).run(ctx)
}
