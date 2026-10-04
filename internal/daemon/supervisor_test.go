package daemon

import (
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/DevNewbie1826/omosense/internal/core"
)

type testSource struct {
	name, lock, legacy string
	always             bool
	run                func(context.Context, core.Sink) error
}

func (s *testSource) Name() string                               { return s.name }
func (s *testSource) Prefixes() []string                         { return []string{"EVENT"} }
func (s *testSource) AlwaysOn() bool                             { return s.always }
func (s *testSource) LockName() (string, string)                 { return s.lock, s.legacy }
func (s *testSource) Run(c context.Context, out core.Sink) error { return s.run(c, out) }

func workerFixture(t *testing.T, s core.Source) (*worker, *lockPool) {
	t.Helper()
	pool := &lockPool{held: make(map[string]*sharedLock)}
	w := newWorker(&core.Ctx{State: t.TempDir()}, s, pool, realClock{}, func(string) {})
	return w, pool
}

func TestSupervisorPauseGraceAndAlwaysOn(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		var starts, stops atomic.Int32
		s := &testSource{name: "herdr", lock: "herdr-main", run: func(c context.Context, _ core.Sink) error {
			starts.Add(1)
			<-c.Done()
			stops.Add(1)
			return c.Err()
		}}
		w, _ := workerFixture(t, s)
		done := make(chan struct{})
		go func() { w.run(ctx); close(done) }()
		synctest.Wait()
		if starts.Load() != 0 || w.state() != "paused" {
			t.Fatal("idle source started")
		}
		w.setWanted(true)
		synctest.Wait()
		if starts.Load() != 1 || w.state() != "running" {
			t.Fatal("interest did not start source")
		}
		w.setWanted(false)
		synctest.Wait()
		timer := time.NewTimer(59 * time.Second)
		<-timer.C // Virtual time, not a scheduling sleep.
		synctest.Wait()
		if stops.Load() != 0 {
			t.Fatal("stopped before grace")
		}
		w.setWanted(true)
		synctest.Wait()
		timer.Reset(2 * time.Second)
		<-timer.C
		synctest.Wait()
		if starts.Load() != 1 || stops.Load() != 0 {
			t.Fatal("reattach did not cancel grace")
		}
		w.setWanted(false)
		synctest.Wait()
		timer.Reset(time.Minute)
		<-timer.C
		synctest.Wait()
		if stops.Load() != 1 || w.state() != "paused" {
			t.Fatalf("grace expired without stop: %s", w.state())
		}
		cancel()
		<-done
		if _, err := os.Stat(filepath.Join(w.ctx.State, "herdr-main.lock.json")); !os.IsNotExist(err) {
			t.Fatal("pause kept lock")
		}
	})
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		s := &testSource{name: "remind", lock: "remind-main", always: true, run: func(c context.Context, _ core.Sink) error {
			<-c.Done()
			return c.Err()
		}}
		w, _ := workerFixture(t, s)
		done := make(chan struct{})
		go func() { w.run(ctx); close(done) }()
		synctest.Wait()
		if w.state() != "running" {
			t.Fatal("always-on did not start without clients")
		}
		cancel()
		<-done
	})
}

func TestSupervisorSharedListenLockReleasedAfterLastSource(t *testing.T) {
	ctx := &core.Ctx{State: t.TempDir()}
	pool := &lockPool{held: make(map[string]*sharedLock)}
	releaseA, blocked, err := pool.acquire(ctx, "listen-main", "listen")
	if err != nil || blocked != "" {
		t.Fatalf("first acquire: %q %v", blocked, err)
	}
	releaseB, blocked, err := pool.acquire(ctx, "listen-main", "listen")
	if err != nil || blocked != "" {
		t.Fatalf("second acquire: %q %v", blocked, err)
	}
	releaseA()
	path := filepath.Join(ctx.State, "listen-main.lock.json")
	if _, err := os.Stat(path); err != nil {
		t.Fatal("telegram stopping removed discord lock")
	}
	releaseB()
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("last source kept lock: %v", err)
	}
}

func TestSupervisorBlockedLegacyLockRetriesWhenFreed(t *testing.T) {
	// A same-user child blocks on stdin; no timer or real monitor is involved.
	holder := exec.Command(os.Args[0], "-test.run=^TestSupervisorLockHolder$")
	holder.Env = append(os.Environ(), "OMOSENSE_LOCK_HELPER=1")
	stdin, err := holder.StdinPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := holder.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { stdin.Close(); holder.Wait() })
	state := t.TempDir()
	path := filepath.Join(state, "listen.lock.json")
	metadata := fmt.Sprintf(`{"pid":%d}`, holder.Process.Pid)
	if err := os.WriteFile(path, []byte(metadata), 0o600); err != nil {
		t.Fatal(err)
	}
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		var logs []string // The synctest Wait barrier makes inspection race-free.
		var starts atomic.Int32
		src := &testSource{name: "telegram", lock: "listen-main", legacy: "listen", run: func(c context.Context, _ core.Sink) error {
			starts.Add(1)
			<-c.Done()
			return c.Err()
		}}
		w, _ := workerFixture(t, src)
		w.ctx.State = state
		w.emit = func(line string) { logs = append(logs, line) }
		w.setWanted(true)
		done := make(chan struct{})
		go func() { w.run(ctx); close(done) }()
		synctest.Wait()
		if w.state() != "locked-by-other" || starts.Load() != 0 || len(logs) != 1 || logs[0] != "LOG ALREADY_RUNNING listen "+metadata {
			t.Fatalf("locked: %s starts=%d logs=%v", w.state(), starts.Load(), logs)
		}
		timer := time.NewTimer(30 * time.Second)
		<-timer.C
		synctest.Wait()
		if len(logs) != 1 {
			t.Fatal("repeated ALREADY_RUNNING while same holder still blocks")
		}
		if err := os.Remove(path); err != nil {
			t.Fatal(err)
		}
		timer.Reset(30 * time.Second)
		<-timer.C
		synctest.Wait()
		if starts.Load() != 1 || w.state() != "running" {
			t.Fatal("did not acquire freed lock on retry")
		}
		cancel()
		<-done
	})
}

func TestSupervisorLockHolder(t *testing.T) {
	if os.Getenv("OMOSENSE_LOCK_HELPER") == "1" {
		io.Copy(io.Discard, os.Stdin)
		os.Exit(0)
	}
}

func TestSupervisorPanicAndErrorRestartOnlyFailedSource(t *testing.T) {
	for _, panicRun := range []bool{true, false} {
		t.Run(fmt.Sprint(panicRun), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				var crashing, healthy atomic.Int32
				src := &testSource{name: "discord", lock: "listen-main", always: true, run: func(c context.Context, _ core.Sink) error {
					if crashing.Add(1) == 1 {
						if panicRun {
							panic("fake crash")
						}
						return fmt.Errorf("fake error")
					}
					<-c.Done()
					return c.Err()
				}}
				ok := &testSource{name: "remind", lock: "remind-main", always: true, run: func(c context.Context, _ core.Sink) error {
					healthy.Add(1)
					<-c.Done()
					return c.Err()
				}}
				w, _ := workerFixture(t, src)
				h, _ := workerFixture(t, ok)
				a, b := make(chan struct{}), make(chan struct{})
				go func() { w.run(ctx); close(a) }()
				go func() { h.run(ctx); close(b) }()
				synctest.Wait()
				if w.state() != "error" || h.state() != "running" {
					t.Fatalf("states: %s %s", w.state(), h.state())
				}
				timer := time.NewTimer(5 * time.Second)
				<-timer.C
				synctest.Wait()
				if crashing.Load() != 2 || healthy.Load() != 1 {
					t.Fatalf("restarts failed=%d healthy=%d", crashing.Load(), healthy.Load())
				}
				cancel()
				<-a
				<-b
			})
		})
	}
}

func TestWorkerHaltAlwaysOnJoinsBeforeAck(t *testing.T) {
	for _, runError := range []bool{false, true} {
		t.Run(fmt.Sprint(runError), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				ctx, cancel := context.WithCancel(context.Background())
				cancelled, returned, finish := make(chan struct{}), make(chan struct{}), make(chan struct{})
				var starts atomic.Int32
				src := &testSource{name: "remind", lock: "remind-family", always: true, run: func(c context.Context, _ core.Sink) error {
					starts.Add(1)
					<-c.Done()
					close(cancelled)
					<-finish
					close(returned)
					if runError {
						return fmt.Errorf("halted source error")
					}
					return c.Err()
				}}
				w, _ := workerFixture(t, src)
				var logs []string
				w.emit = func(line string) { logs = append(logs, line) }
				done := make(chan struct{})
				go func() { w.run(ctx); close(done) }()
				defer func() { cancel(); <-done }()
				defer close(finish)
				synctest.Wait()
				if starts.Load() != 1 {
					t.Fatal("always-on source did not start")
				}

				ack := w.halt()
				if again := w.halt(); again != ack {
					t.Fatal("repeated halt did not share the pending acknowledgement")
				}
				synctest.Wait()
				select {
				case <-cancelled:
				default:
					t.Fatal("halt did not cancel Run")
				}
				select {
				case <-ack:
					t.Fatal("halt acknowledged before Run returned")
				default:
				}
				path := filepath.Join(w.ctx.State, src.lock+".lock.json")
				if _, err := os.Stat(path); err != nil {
					t.Fatalf("lock released before Run returned: %v", err)
				}
				// Release the join gate without closing it twice during cleanup.
				finish <- struct{}{}
				synctest.Wait()
				select {
				case <-returned:
				default:
					t.Fatal("Run did not return after the gate")
				}
				select {
				case <-ack:
				default:
					t.Fatal("joined worker did not acknowledge halt")
				}
				if _, err := os.Stat(path); !os.IsNotExist(err) {
					t.Fatalf("acknowledgement retained lock: %v", err)
				}
				if w.state() != "stopped" || len(logs) != 0 {
					t.Fatalf("halted exit: state=%s logs=%v", w.state(), logs)
				}
				w.setWanted(true)
				timer := time.NewTimer(10 * time.Minute)
				<-timer.C // Advance virtual time beyond grace and maximum backoff.
				synctest.Wait()
				if starts.Load() != 1 || w.state() != "stopped" || len(logs) != 0 {
					t.Fatalf("halted worker restarted: starts=%d state=%s logs=%v", starts.Load(), w.state(), logs)
				}
			})
		})
	}
}

func TestWorkerHaltWantedDoesNotStart(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		var starts atomic.Int32
		src := &testSource{name: "google", lock: "google-family", run: func(c context.Context, _ core.Sink) error {
			starts.Add(1)
			<-c.Done()
			return c.Err()
		}}
		w, _ := workerFixture(t, src)
		ack := w.halt() // Also covers markers loaded before the worker loop starts.
		done := make(chan struct{})
		go func() { w.run(ctx); close(done) }()
		defer func() { cancel(); <-done }()
		synctest.Wait()
		select {
		case <-ack:
		default:
			t.Fatal("idle worker did not acknowledge halt")
		}
		w.setWanted(true)
		synctest.Wait()
		if starts.Load() != 0 || w.state() != "stopped" {
			t.Fatalf("wanted started halted worker: starts=%d state=%s", starts.Load(), w.state())
		}
		if _, err := os.Stat(filepath.Join(w.ctx.State, src.lock+".lock.json")); !os.IsNotExist(err) {
			t.Fatalf("halted worker acquired lock: %v", err)
		}
	})
}

func TestWorkerResumeRestartsAlwaysOn(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		var starts atomic.Int32
		src := &testSource{name: "remind", lock: "remind-family", always: true, run: func(c context.Context, _ core.Sink) error {
			starts.Add(1)
			<-c.Done()
			return c.Err()
		}}
		w, _ := workerFixture(t, src)
		done := make(chan struct{})
		go func() { w.run(ctx); close(done) }()
		defer func() { cancel(); <-done }()
		synctest.Wait()
		ack := w.halt()
		synctest.Wait()
		select {
		case <-ack:
		default:
			t.Fatal("halt did not complete")
		}
		w.resume()
		synctest.Wait()
		if starts.Load() != 2 || w.state() != "running" {
			t.Fatalf("resume did not restart always-on: starts=%d state=%s", starts.Load(), w.state())
		}
		ack = w.halt()
		synctest.Wait()
		select {
		case <-ack:
		default:
			t.Fatal("second halt cycle did not complete")
		}
		if w.state() != "stopped" {
			t.Fatal("second halt cycle did not stop")
		}
	})
}

func TestWorkerHaltIdleAndBackoff(t *testing.T) {
	for _, initial := range []string{"paused", "error", "locked-by-other"} {
		t.Run(initial, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				ctx, cancel := context.WithCancel(context.Background())
				var starts atomic.Int32
				src := &testSource{name: "remind", lock: "remind-family", always: initial != "paused", run: func(context.Context, core.Sink) error {
					starts.Add(1)
					return fmt.Errorf("source error")
				}}
				w, _ := workerFixture(t, src)
				legacy := filepath.Join(w.ctx.State, "foreign.lock.json")
				if initial == "locked-by-other" {
					src.legacy = "foreign"
					if err := os.WriteFile(legacy, []byte(fmt.Sprintf(`{"pid":%d}`, os.Getppid())), 0o600); err != nil {
						t.Fatal(err)
					}
				}
				done := make(chan struct{})
				go func() { w.run(ctx); close(done) }()
				defer func() { cancel(); <-done }()
				synctest.Wait()
				if w.state() != initial {
					t.Fatalf("fixture state=%s want=%s", w.state(), initial)
				}
				before := starts.Load()
				ack := w.halt()
				synctest.Wait()
				select {
				case <-ack:
				default:
					t.Fatal("idle halt did not acknowledge promptly")
				}
				if initial == "locked-by-other" {
					if err := os.Remove(legacy); err != nil {
						t.Fatal(err)
					}
				}
				w.setWanted(true)
				timer := time.NewTimer(10 * time.Minute)
				<-timer.C
				synctest.Wait()
				if starts.Load() != before || w.state() != "stopped" {
					t.Fatalf("halted idle worker started: starts=%d state=%s", starts.Load(), w.state())
				}
				if _, err := os.Stat(filepath.Join(w.ctx.State, src.lock+".lock.json")); !os.IsNotExist(err) {
					t.Fatalf("halted idle worker acquired lock: %v", err)
				}
			})
		})
	}
}

func TestWorkerHaltOverridesGrace(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		src := &testSource{name: "google", lock: "google-family", run: func(c context.Context, _ core.Sink) error {
			<-c.Done()
			return c.Err()
		}}
		w, _ := workerFixture(t, src)
		w.setWanted(true)
		done := make(chan struct{})
		go func() { w.run(ctx); close(done) }()
		defer func() { cancel(); <-done }()
		synctest.Wait()
		w.setWanted(false)
		synctest.Wait()
		ack := w.halt()
		synctest.Wait() // No virtual-time advance: halt must bypass the grace.
		select {
		case <-ack:
		default:
			t.Fatal("halt waited for grace")
		}
		timer := time.NewTimer(2 * time.Minute)
		<-timer.C
		synctest.Wait()
		if w.state() != "stopped" {
			t.Fatalf("grace overwrote stopped state: %s", w.state())
		}
	})
}
