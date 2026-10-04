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
	ctx         *core.Ctx
	source      core.Source
	locks       *lockPool
	clock       clock
	emit        func(string)
	grace       time.Duration
	wanted      atomic.Bool
	wake        chan struct{}
	mu          sync.Mutex
	status      string
	halted      bool
	haltAck     chan struct{}
	haltPending bool
}

func newWorker(ctx *core.Ctx, s core.Source, locks *lockPool, c clock, emit func(string)) *worker {
	return &worker{ctx: ctx, source: s, locks: locks, clock: c, emit: emit,
		grace: time.Minute, wake: make(chan struct{}, 1), status: "paused"}
}

func (w *worker) setWanted(wanted bool) {
	w.wanted.Store(wanted)
	w.signal()
}

func (w *worker) signal() {
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

// halt requests an immediate stop without joining Run or emitting through the
// server. The loop closes the acknowledgement only after joining and releasing
// the lock. Repeated calls share the same acknowledgement until resume.
func (w *worker) halt() <-chan struct{} {
	w.mu.Lock()
	if !w.halted && !w.haltPending {
		w.haltAck = make(chan struct{})
		w.haltPending = true
	}
	w.halted = true
	ack := w.haltAck
	w.mu.Unlock()
	w.signal()
	return ack
}

func (w *worker) resume() {
	w.mu.Lock()
	w.halted = false
	w.mu.Unlock()
	w.signal()
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
	acknowledge := func() {
		w.mu.Lock()
		defer w.mu.Unlock()
		w.status = "stopped"
		if w.haltPending {
			close(w.haltAck)
			w.haltPending = false
		}
	}
	defer func() {
		stopJob()
		acknowledge()
	}()
	for {
		// Serialize startup with halt, but never hold mu while joining a job
		// or emitting: emit can need the server mutex held by halt's caller.
		w.mu.Lock()
		if w.halted || w.haltPending {
			w.mu.Unlock()
			stopJob()
			grace, retry = nil, nil
			lastBlock, backoff = "", 5*time.Second
			acknowledge()
			w.mu.Lock()
			halted := w.halted
			w.mu.Unlock()
			if halted {
				select {
				case <-ctx.Done():
					return
				case <-w.wake:
				}
			}
			continue
		}
		log := ""
		wanted := w.source.AlwaysOn() || w.wanted.Load()
		if !wanted && done == nil {
			retry = nil
			w.status = "paused"
			lastBlock = ""
		}
		if wanted {
			grace = nil
			if done == nil && retry == nil && ctx.Err() == nil {
				name, legacy := w.source.LockName()
				rel, blocked, err := w.locks.acquire(w.ctx, name, legacy)
				switch {
				case err != nil:
					w.status = "error"
					log = "LOG omosense source " + w.source.Name() + " crashed: " + err.Error()
					retry = w.clock.After(backoff)
					backoff = min(backoff*2, 5*time.Minute)
				case blocked != "":
					w.status = "locked-by-other"
					if blocked != lastBlock {
						log = "LOG ALREADY_RUNNING " + blocked
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
					w.status = "running"
					go func() {
						defer jobCancel()
						ch <- runSource(job, w.source, core.NewOut(lineWriter{w.emit}))
					}()
				}
			}
		} else if done != nil && grace == nil {
			grace = w.clock.After(w.grace)
		}
		w.mu.Unlock()
		if log != "" {
			w.emit(log)
		}
		select {
		case <-ctx.Done():
			return
		case <-w.wake:
		case <-grace:
			grace = nil
			w.mu.Lock()
			pause := !w.halted && !w.haltPending && !w.source.AlwaysOn() && !w.wanted.Load()
			w.mu.Unlock()
			if pause {
				stopJob()
				w.mu.Lock()
				if !w.halted && !w.haltPending {
					w.status = "paused"
				}
				w.mu.Unlock()
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
			w.mu.Lock()
			if w.halted || w.haltPending {
				w.mu.Unlock()
				continue
			}
			if err == nil {
				err = fmt.Errorf("source returned unexpectedly")
			}
			w.status = "error"
			retry = w.clock.After(backoff)
			backoff = min(backoff*2, 5*time.Minute)
			w.mu.Unlock()
			w.emit("LOG omosense source " + w.source.Name() + " crashed: " + err.Error())
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
