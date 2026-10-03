// Package listen hosts the inbound sources: the Telegram bots polled with
// long-poll getUpdates and the Discord gateway connection. Both print EVENT
// and LOG lines.
package listen

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

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
	if ctx.Flags["--dry-run"] || core.ParseArgs(args).Flags["--dry-run"] {
		bots := ctx.Profile.Telegram
		if bots == nil {
			bots = []string{}
		}
		ctx.Out.Emit("PLAN", map[string]any{"profile": ctx.Profile.Name, "discord": ctx.Profile.Discord, "telegram": bots, "lock": "listen-" + ctx.Profile.Name, "state": ctx.State})
		return 0
	}
	release := ctx.Acquire("listen-"+ctx.Profile.Name, "listen")
	defer release()
	ctx.Out.Log("omomeow listener starting (profile " + ctx.Profile.Name + ")")
	runCtx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM, syscall.SIGHUP)
	defer cancel()
	var wg sync.WaitGroup
	errs := make(chan error, 2)
	for _, source := range Sources(ctx) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := source.Run(runCtx, ctx.Out); err != nil {
				errs <- err
				cancel()
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		fmt.Fprintln(os.Stderr, "omosense listen:", err)
		return 1
	}
	return 0
}

// Sources returns the telegram and discord sources of ctx's profile. They
// share the listen-<profile> lock (legacy "listen"); the daemon refcounts
// that lock across the two.
func Sources(ctx *core.Ctx) []core.Source {
	p := ctx.Profile.Name
	result := []core.Source{
		src{
			cfg:      ctx,
			sleep:    sleep,
			name:     "telegram",
			prefixes: []string{"EVENT"},
			lock:     "listen-" + p,
			legacy:   "listen",
		},
	}
	if ctx.Profile.Discord {
		result = append(result, src{
			cfg:      ctx,
			sleep:    sleep,
			name:     "discord",
			prefixes: []string{"EVENT"},
			alwaysOn: true,
			lock:     "listen-" + p,
			legacy:   "listen",
		})
	}
	return result
}

// src hosts one platform under either the daemon or compat host.
type src struct {
	cfg      *core.Ctx
	sleep    func(context.Context, time.Duration) error
	clock    clock
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
	if s.name == "telegram" {
		return s.telegram(ctx, sink)
	}
	return s.discord(ctx, sink)
}

func sleep(ctx context.Context, duration time.Duration) error {
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
