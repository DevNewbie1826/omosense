// Package host runs bare `omosense` as ONE foreground process hosting every
// source the folder's flat config.json enables. It carries the supervisor
// semantics of the retired daemon (5s doubling to 5m backoff, ALREADY_RUNNING
// retry every 30s, panic recovery, join-before-release, a refcounted shared
// listen lock) without pause, grace, halt, attach or journal: every
// registered source is always wanted.
package host

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/DevNewbie1826/omosense/internal/core"
	"github.com/DevNewbie1826/omosense/internal/google"
	"github.com/DevNewbie1826/omosense/internal/herdr"
	"github.com/DevNewbie1826/omosense/internal/listen"
	"github.com/DevNewbie1826/omosense/internal/remind"
	"github.com/DevNewbie1826/omosense/internal/rpc"
	"github.com/DevNewbie1826/omosense/internal/tidy"
)

// Clock is the injectable time source. The retry and backoff timers read
// through it, so a test drives a crash/restart without waiting out real
// intervals.
type Clock interface {
	Now() time.Time
	After(time.Duration) <-chan time.Time
}

type realClock struct{}

func (realClock) Now() time.Time                         { return time.Now() }
func (realClock) After(d time.Duration) <-chan time.Time { return time.After(d) }

// Options configures a Host. Zero values select production behavior.
type Options struct {
	// Registry supplies the sources to host; nil selects Sources.
	Registry func(*core.Ctx) []core.Source
	// Clock is the retry/backoff time source; nil selects the wall clock.
	Clock Clock
	// Ready, when non-nil, is closed once every worker loop has started.
	Ready chan struct{}
}

// Sources is the IS-1 registry: telegram when telegram.bot is set, discord
// when discord.bot is set (both share the listen lock), google always,
// remind always, herdr unless herdr.enabled is explicitly false, rpc when
// rpc.enabled, tidy when tidy.enabled. say is a one-shot sender, never a
// hosted source.
func Sources(c *core.Ctx) []core.Source {
	out := make([]core.Source, 0, 6)
	out = append(out, listen.Sources(c)...)
	out = append(out, google.Sources(c)...)
	out = append(out, remind.Sources(c)...)
	if c.Profile.Herdr.On() {
		out = append(out, herdr.Sources(c)...)
	}
	if c.Profile.RPC.Enabled {
		out = append(out, rpc.Sources(c)...)
	}
	out = append(out, tidy.Sources(c)...)
	return out
}

// Run is the bare `omosense` entrypoint: it hosts the folder's sources
// until SIGINT/SIGTERM/SIGHUP, then joins every source, releases every lock
// and returns 0.
func Run(c *core.Ctx) int {
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM, syscall.SIGHUP)
	defer cancel()
	if err := New(c, Options{}).Run(ctx); err != nil {
		fmt.Fprintln(os.Stderr, "omosense:", err)
		return 1
	}
	return 0
}

// Host supervises one worker per source.
type Host struct {
	ctx     *core.Ctx
	options Options
	clock   Clock
	locks   *lockPool
	workers []*worker
}

// New builds a host for c. It registers every source the registry returns;
// an empty registry is a host that starts and immediately idles.
func New(c *core.Ctx, opts Options) *Host {
	registry := opts.Registry
	if registry == nil {
		registry = Sources
	}
	clock := opts.Clock
	if clock == nil {
		clock = realClock{}
	}
	h := &Host{ctx: c, options: opts, clock: clock, locks: newLockPool()}
	for _, source := range registry(c) {
		h.workers = append(h.workers, &worker{ctx: c, source: source, locks: h.locks, clock: clock})
	}
	return h
}

// Run prints the startup LOG naming the folder and the started sources, then
// runs every worker until ctx is cancelled. Every source is joined before
// its lock is released, so a stopped host leaves no lock file behind.
func (h *Host) Run(ctx context.Context) error {
	names := make([]string, 0, len(h.workers))
	for _, w := range h.workers {
		names = append(names, w.source.Name())
	}
	h.ctx.Out.Log(fmt.Sprintf("omosense host starting dir=%s sources=%s", h.ctx.Dir, strings.Join(names, ",")))
	jobCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	var wg sync.WaitGroup
	for _, w := range h.workers {
		wg.Add(1)
		go func(w *worker) { defer wg.Done(); w.run(jobCtx) }(w)
	}
	if h.options.Ready != nil {
		close(h.options.Ready)
	}
	<-ctx.Done()
	cancel()
	wg.Wait()
	return nil
}

// worker owns one source's job, its lock and its retry timer. A source is
// always wanted; a crash logs one line and re-arms the backoff timer, and a
// lock held by another live process logs ALREADY_RUNNING once per holder and
// retries every 30s.
type worker struct {
	ctx    *core.Ctx
	source core.Source
	locks  *lockPool
	clock  Clock
}

func (w *worker) run(ctx context.Context) {
	var done <-chan error
	var cancel context.CancelFunc
	var release func()
	var retry <-chan time.Time
	lastBlock := ""
	backoff := 5 * time.Second
	stopJob := func() {
		if done != nil {
			cancel()
			<-done
			release()
			done, cancel, release = nil, nil, nil
		}
	}
	defer stopJob()
	for {
		if ctx.Err() != nil {
			return
		}
		if done == nil && retry == nil {
			name, legacy := w.source.LockName()
			rel, blocked, err := w.locks.acquire(w.ctx, name, legacy)
			switch {
			case err != nil:
				w.crashed(err)
				retry = w.clock.After(backoff)
				backoff = min(backoff*2, 5*time.Minute)
			case blocked != "":
				if blocked != lastBlock {
					w.ctx.Out.Log("ALREADY_RUNNING " + blocked)
					lastBlock = blocked
				}
				retry = w.clock.After(30 * time.Second)
			default:
				lastBlock = ""
				release = rel
				job, jobCancel := context.WithCancel(ctx)
				cancel = jobCancel
				ch := make(chan error, 1)
				done = ch
				go func() {
					defer jobCancel()
					ch <- runSource(job, w.source, w.ctx.Out)
				}()
			}
		}
		select {
		case <-ctx.Done():
			return
		case <-retry:
			retry = nil
		case err := <-done:
			cancel()
			release()
			done, cancel, release = nil, nil, nil
			if ctx.Err() != nil {
				return
			}
			if err == nil {
				err = errors.New("source returned unexpectedly")
			}
			w.crashed(err)
			retry = w.clock.After(backoff)
			backoff = min(backoff*2, 5*time.Minute)
		}
	}
}

func (w *worker) crashed(err error) {
	w.ctx.Out.Log("omosense source " + w.source.Name() + " crashed: " + err.Error())
}

// runSource runs one source and turns a panic into an error, so a crashing
// source never takes the host down.
func runSource(ctx context.Context, s core.Source, sink core.Sink) (err error) {
	defer func() {
		if v := recover(); v != nil {
			err = fmt.Errorf("panic: %v", v)
		}
	}()
	return s.Run(ctx, sink)
}
