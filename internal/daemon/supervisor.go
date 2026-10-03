package daemon

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/DevNewbie1826/omosense/internal/core"
)

type clock interface {
	Now() time.Time
	After(time.Duration) <-chan time.Time
}

type realClock struct{}

func (realClock) Now() time.Time                         { return time.Now() }
func (realClock) After(d time.Duration) <-chan time.Time { return time.After(d) }

type worker struct {
	ctx    *core.Ctx
	source core.Source
	locks  *lockPool
	clock  clock
	emit   func(string)
	grace  time.Duration
	wanted atomic.Bool
	wake   chan struct{}
	mu     sync.Mutex
	status string
}

func newWorker(ctx *core.Ctx, s core.Source, locks *lockPool, c clock, emit func(string)) *worker {
	return &worker{ctx: ctx, source: s, locks: locks, clock: c, emit: emit,
		grace: time.Minute, wake: make(chan struct{}, 1), status: "paused"}
}

func (w *worker) setWanted(wanted bool) {
	w.wanted.Store(wanted)
	select {
	case w.wake <- struct{}{}:
	default:
	}
}

func (w *worker) state() string {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.status
}

func (w *worker) setState(s string) {
	w.mu.Lock()
	w.status = s
	w.mu.Unlock()
}

// run owns the job, timers and release callback. A source is joined before its
// lock is released or it is restarted; other workers never wait on this job.
func (w *worker) run(ctx context.Context) {
	var done <-chan error
	var cancel context.CancelFunc
	var release func()
	var grace, retry <-chan time.Time
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
		wanted := w.source.AlwaysOn() || w.wanted.Load()
		if !wanted && done == nil {
			retry = nil
			w.setState("paused")
			lastBlock = ""
		}
		if wanted {
			grace = nil
			if done == nil && retry == nil && ctx.Err() == nil {
				name, legacy := w.source.LockName()
				rel, blocked, err := w.locks.acquire(w.ctx, name, legacy)
				switch {
				case err != nil:
					w.setState("error")
					w.emit("LOG omosense source " + w.source.Name() + " crashed: " + err.Error())
					retry = w.clock.After(backoff)
					backoff = min(backoff*2, 5*time.Minute)
				case blocked != "":
					w.setState("locked-by-other")
					if blocked != lastBlock {
						w.emit("LOG ALREADY_RUNNING " + blocked)
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
					w.setState("running")
					go func() {
						defer jobCancel()
						ch <- runSource(job, w.source, core.NewOut(lineWriter{w.emit}))
					}()
				}
			}
		} else if done != nil && grace == nil {
			grace = w.clock.After(w.grace)
		}
		select {
		case <-ctx.Done():
			return
		case <-w.wake:
		case <-grace:
			grace = nil
			if !w.source.AlwaysOn() && !w.wanted.Load() {
				stopJob()
				w.setState("paused")
			}
		case <-retry:
			retry = nil
		case err := <-done:
			cancel()
			release()
			done, cancel, release = nil, nil, nil
			grace = nil
			if ctx.Err() != nil {
				return
			}
			if err == nil {
				err = fmt.Errorf("source returned unexpectedly")
			}
			w.setState("error")
			w.emit("LOG omosense source " + w.source.Name() + " crashed: " + err.Error())
			retry = w.clock.After(backoff)
			backoff = min(backoff*2, 5*time.Minute)
		}
	}
}

func runSource(ctx context.Context, s core.Source, sink core.Sink) (err error) {
	defer func() {
		if v := recover(); v != nil {
			err = fmt.Errorf("panic: %v", v)
		}
	}()
	return s.Run(ctx, sink)
}

type lineWriter struct{ emit func(string) }

func (w lineWriter) Write(b []byte) (int, error) {
	w.emit(strings.TrimSuffix(string(b), "\n"))
	return len(b), nil
}
