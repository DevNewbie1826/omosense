package daemon

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/DevNewbie1826/omosense/internal/core"
)

type serverOptions struct {
	version  string
	registry func(*core.Ctx) []core.Source
	clock    clock
	ready    chan struct{}
}

type profileHost struct {
	workers map[string]*worker
	journal *journal
}

type server struct {
	base     *core.Ctx
	paths    paths
	options  serverOptions
	profiles map[string]*profileHost
	mu       sync.Mutex
	clients  map[*client]hello
	conns    map[net.Conn]struct{}
	stopping bool
	failure  error
	stop     chan string
	jobs     sync.WaitGroup
	handlers sync.WaitGroup
}

func newServer(base *core.Ctx, p paths, opts serverOptions) (*server, error) {
	s := &server{base: base, paths: p, options: opts,
		profiles: make(map[string]*profileHost), clients: make(map[*client]hello),
		conns: make(map[net.Conn]struct{}), stop: make(chan string, 1)}
	names := []string{"main"}
	if base.Cfg.Profiles != nil {
		names = base.Cfg.Profiles.Keys()
	}
	pool := &lockPool{held: make(map[string]*sharedLock)}
	for _, name := range names {
		prof, err := base.Cfg.Resolve(name)
		if err != nil {
			return nil, err
		}
		c := *base
		c.Profile, c.Flags, c.Args = prof, map[string]bool{}, nil
		host := &profileHost{workers: make(map[string]*worker)}
		for _, source := range opts.registry(&c) {
			src := source
			host.workers[src.Name()] = newWorker(&c, src, pool, opts.clock, func(line string) {
				s.emit(name, src.Name(), src.AlwaysOn(), line)
			})
		}
		s.profiles[name] = host
	}
	return s, nil
}

func (s *server) serve(ctx context.Context) (err error) {
	owner, err := acquireLifetime(s.paths)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, owner.close()) }()
	for name, p := range s.profiles {
		p.journal, err = openJournal(filepath.Join(s.base.State, "omosense-journal-"+name+".jsonl"), s.options.clock.Now())
		if err != nil {
			break
		}
	}
	defer func() {
		for _, p := range s.profiles {
			if p.journal != nil {
				err = errors.Join(err, p.journal.close())
			}
		}
	}()
	if err != nil {
		return err
	}
	if err := testNotify("gated"); err != nil {
		return err
	}
	if err := waitReadyGate(ctx, s.options.clock); err != nil {
		return err
	}
	ln, err := owner.bind()
	if err != nil {
		return err
	}
	jobCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	for _, p := range s.profiles {
		for _, w := range p.workers {
			s.jobs.Add(1)
			go func() { defer s.jobs.Done(); w.run(jobCtx) }()
		}
	}
	s.jobs.Add(1)
	go func() { defer s.jobs.Done(); s.compactLoop(jobCtx) }()
	acceptDone := make(chan error, 1)
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				acceptDone <- err
				return
			}
			s.mu.Lock()
			s.conns[c] = struct{}{}
			s.mu.Unlock()
			s.handlers.Add(1)
			go func() { defer s.handlers.Done(); s.handle(c) }()
		}
	}()
	if s.options.ready != nil {
		close(s.options.ready)
	}
	reason := "stop"
	select {
	case <-ctx.Done():
	case reason = <-s.stop:
	case err = <-acceptDone:
		acceptDone = nil
	}
	s.mu.Lock()
	s.stopping = true
	s.mu.Unlock()
	ln.Close()
	if acceptDone != nil {
		<-acceptDone
	}
	cancel()
	s.jobs.Wait()
	s.mu.Lock()
	for c := range s.clients {
		c.enqueue(frame{Ctl: "shutdown", Reason: reason})
	}
	for conn := range s.conns {
		attached := false
		for c := range s.clients {
			if c.conn == conn {
				attached = true
				break
			}
		}
		if !attached {
			conn.Close()
		}
	}
	s.mu.Unlock()
	s.handlers.Wait()
	return errors.Join(err, s.failure)
}

func (s *server) requestStop(reason string) {
	select {
	case s.stop <- reason:
	default:
	}
}

func (s *server) failLocked(err error) {
	s.failure = errors.Join(s.failure, err)
	s.requestStop("stop")
}

func (s *server) emit(profile, source string, always bool, line string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	p := s.profiles[profile]
	if p == nil {
		return
	}
	delivered := false
	for c, h := range s.clients {
		if matches(h, profile, source, line) && !c.closed() {
			delivered = true
		}
	}
	if always {
		if err := p.journal.append(s.options.clock.Now(), source, line, delivered); err != nil {
			s.failLocked(err)
			return
		}
	}
	for c, h := range s.clients {
		if matches(h, profile, source, line) {
			c.enqueue(frame{Line: line})
		}
	}
}

func (s *server) compactLoop(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-s.options.clock.After(time.Hour):
			s.mu.Lock()
			for name, p := range s.profiles {
				if err := p.journal.compact(s.options.clock.Now()); err != nil {
					s.failLocked(fmt.Errorf("compact %s: %w", name, err))
				}
			}
			s.mu.Unlock()
		}
	}
}

// OMOSENSE_TEST_READY_GATE is test-only: subprocess QA blocks before binding
// until this file exists. Never point this hook at real state.
func waitReadyGate(ctx context.Context, c clock) error {
	gate := os.Getenv("OMOSENSE_TEST_READY_GATE")
	for gate != "" {
		if _, err := os.Stat(gate); err == nil {
			return nil
		} else if !os.IsNotExist(err) {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-c.After(25 * time.Millisecond):
		}
	}
	return nil
}
