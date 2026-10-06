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
const Help = `Usage: omosense listen [--dry-run]

Runs the Telegram poller and the Discord gateway session this folder's
config.json names (telegram.bot / discord.bot), printing EVENT and LOG
lines to stdout. A platform without a bot contributes no source.

Flags:
  --dry-run    print the run plan and exit 0 without taking the lock
`

// Run is the compat subcommand host: it runs the listen sources in-process
// under the listen lock with the TS stdout grammar and exit semantics.
func Run(ctx *core.Ctx, args []string) int {
	if ctx.Flags["--dry-run"] || core.ParseArgs(args).Flags["--dry-run"] {
		ctx.Out.Emit("PLAN", map[string]any{"dir": ctx.Dir, "telegram": platformBots(ctx.Profile.Telegram.Bot), "discord": platformBots(ctx.Profile.Discord.Bot), "lock": "listen", "state": ctx.State})
		return 0
	}
	release := ctx.Acquire("listen", "")
	defer release()
	ctx.Out.Log("omosense listener starting")
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

// Sources returns the telegram and discord sources of the folder's config.
// A platform without a bot registers no source: a folder may listen on one
// platform only, or on none at all (calendar/herdr-only). Both sources
// share the listen lock, which the host refcounts across them.
func Sources(ctx *core.Ctx) []core.Source {
	var result []core.Source
	if ctx.Profile.Telegram.Bot != "" {
		result = append(result, src{
			cfg:      ctx,
			sleep:    sleep,
			name:     "telegram",
			prefixes: []string{"EVENT"},
			lock:     "listen",
		})
	}
	if ctx.Profile.Discord.Bot != "" {
		result = append(result, src{
			cfg:      ctx,
			sleep:    sleep,
			name:     "discord",
			prefixes: []string{"EVENT"},
			alwaysOn: true,
			lock:     "listen",
		})
	}
	return result
}

// platformBots is the single bot as the bot list the pollers iterate, so an
// unset bot is an empty list rather than a one-element list holding "".
func platformBots(bot string) []string {
	if bot == "" {
		return []string{}
	}
	return []string{bot}
}

// src hosts one platform under either the session host or the compat host.
type src struct {
	cfg      *core.Ctx
	sleep    func(context.Context, time.Duration) error
	clock    clock
	name     string
	prefixes []string
	alwaysOn bool
	lock     string
}

func (s src) Name() string               { return s.name }
func (s src) Prefixes() []string         { return s.prefixes }
func (s src) AlwaysOn() bool             { return s.alwaysOn }
func (s src) LockName() (string, string) { return s.lock, "" }

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
