package host

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/DevNewbie1826/omosense/internal/core"
)

// chanSink records grammar lines and mirrors them onto a channel, so a test
// awaits an exact line with a bounded timeout instead of sleeping.
type chanSink struct {
	mu    sync.Mutex
	lines []string
	ch    chan string
}

func newChanSink() *chanSink { return &chanSink{ch: make(chan string, 64)} }

func (s *chanSink) add(line string) {
	s.mu.Lock()
	s.lines = append(s.lines, line)
	s.mu.Unlock()
	select {
	case s.ch <- line:
	default:
	}
}

func (s *chanSink) Emit(prefix string, payload any) { s.add(prefix + " <emit>") }
func (s *chanSink) Raw(prefix, text string)         { s.add(prefix + " " + text) }
func (s *chanSink) Log(msg string)                  { s.add("LOG " + msg) }

func (s *chanSink) await(t *testing.T, substr string) string {
	t.Helper()
	deadline := time.After(10 * time.Second)
	for {
		select {
		case line := <-s.ch:
			if strings.Contains(line, substr) {
				return line
			}
		case <-deadline:
			t.Fatalf("timed out waiting for a line containing %q; saw %v", substr, s.snapshot())
		}
	}
}

func (s *chanSink) snapshot() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.lines...)
}

// fakeClock fires the pending timers when the test advances it, and
// announces every After duration on a channel: the test waits for the
// announcement instead of racing the worker's next statement.
type fakeClock struct {
	mu      sync.Mutex
	now     time.Time
	pending []chan time.Time
	after   chan time.Duration
}

func newFakeClock() *fakeClock {
	return &fakeClock{now: time.Unix(0, 0), after: make(chan time.Duration, 64)}
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

// advanceTime moves the clock forward by d without firing any timer, so a
// job that spans it measures a real runtime (IS-9's five-minute reset).
func (c *fakeClock) advanceTime(d time.Duration) {
	c.mu.Lock()
	c.now = c.now.Add(d)
	c.mu.Unlock()
}

func (c *fakeClock) After(d time.Duration) <-chan time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	ch := make(chan time.Time, 1)
	c.pending = append(c.pending, ch)
	c.after <- d
	return ch
}

func (c *fakeClock) advance() {
	c.mu.Lock()
	pending := c.pending
	c.pending = nil
	c.mu.Unlock()
	for _, ch := range pending {
		ch <- time.Unix(0, 0)
	}
}

func (c *fakeClock) awaitAfter(t *testing.T) time.Duration {
	t.Helper()
	select {
	case d := <-c.after:
		return d
	case <-time.After(10 * time.Second):
		t.Fatal("no timer was armed")
		return 0
	}
}

// awaitAfterNot drains timer announcements until one differs from skip: the
// state-file watcher arms a steady 10m drum that breaker tests must filter
// out to observe the worker's own backoff.
func (c *fakeClock) awaitAfterNot(t *testing.T, skip time.Duration) time.Duration {
	t.Helper()
	deadline := time.After(10 * time.Second)
	for {
		select {
		case d := <-c.after:
			if d != skip {
				return d
			}
		case <-deadline:
			t.Fatal("no timer other than the state-file drum was armed")
			return 0
		}
	}
}

// fakeSource is a scripted core.Source: each Run signals started, then runs
// body (nil returns immediately, i.e. a crash).
type fakeSource struct {
	name     string
	prefixes []string
	lock     string
	legacy   string
	started  chan struct{}
	body     func(ctx context.Context) error
}

func (s *fakeSource) Name() string               { return s.name }
func (s *fakeSource) Prefixes() []string         { return s.prefixes }
func (s *fakeSource) AlwaysOn() bool             { return true }
func (s *fakeSource) LockName() (string, string) { return s.lock, s.legacy }

func (s *fakeSource) Run(ctx context.Context, _ core.Sink) error {
	select {
	case s.started <- struct{}{}:
	default:
	}
	if s.body == nil {
		return errors.New("boom")
	}
	return s.body(ctx)
}

// hostCtx is a run context whose state dir is a temp dir and whose Out feeds the test's channel sink.
func hostCtx(t *testing.T) (*core.Ctx, *chanSink) {
	t.Helper()
	state := t.TempDir()
	sink := newChanSink()
	return &core.Ctx{Dir: filepath.Join(state, "proj"), State: state, Out: core.NewOut(&bufWriter{sink})}, sink
}

func startHost(t *testing.T, ctx *core.Ctx, opts Options) (cancel context.CancelFunc, done <-chan error) {
	t.Helper()
	runCtx, cancel := context.WithCancel(context.Background())
	opts.Ready = make(chan struct{})
	h := New(ctx, opts)
	errc := make(chan error, 1)
	go func() { errc <- h.Run(runCtx) }()
	select {
	case <-opts.Ready:
	case <-time.After(10 * time.Second):
		cancel()
		t.Fatal("host did not start")
	}
	return cancel, errc
}

func waitHost(t *testing.T, cancel context.CancelFunc, done <-chan error) {
	t.Helper()
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run returned %v, want nil", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Run did not return after cancel")
	}
}

// TestSourcesMatrix pins the IS-1 registry: which sources a folder's config
// starts. herdr is a default source (absent = on, explicit false = off),
// google and remind always run, and rpc/tidy follow their enabled flags.
func TestSourcesMatrix(t *testing.T) {
	no := false
	yes := true
	cases := []struct {
		name string
		prof core.Profile
		want []string
	}{
		{"bare", core.Profile{}, []string{"google", "remind", "herdr"}},
		{"herdr explicit false", core.Profile{Herdr: core.HerdrCfg{Enabled: &no}}, []string{"google", "remind"}},
		{"herdr explicit true", core.Profile{Herdr: core.HerdrCfg{Enabled: &yes}}, []string{"google", "remind", "herdr"}},
		{"telegram bot", core.Profile{Telegram: core.PlatformCfg{Bot: "tb"}}, []string{"telegram", "google", "remind", "herdr"}},
		{"discord bot", core.Profile{Discord: core.PlatformCfg{Bot: "db"}}, []string{"discord", "google", "remind", "herdr"}},
		{"both bots", core.Profile{Telegram: core.PlatformCfg{Bot: "tb"}, Discord: core.PlatformCfg{Bot: "db"}},
			[]string{"telegram", "discord", "google", "remind", "herdr"}},
		{"rpc enabled", core.Profile{RPC: core.RPCCfg{Enabled: true}}, []string{"google", "remind", "herdr", "rpc"}},
		{"tidy enabled", core.Profile{Tidy: core.TidyCfg{Enabled: true}}, []string{"google", "remind", "herdr", "tidy"}},
		{"everything", core.Profile{
			Telegram: core.PlatformCfg{Bot: "tb"},
			Discord:  core.PlatformCfg{Bot: "db"},
			RPC:      core.RPCCfg{Enabled: true},
			Tidy:     core.TidyCfg{Enabled: true},
			Herdr:    core.HerdrCfg{Enabled: &no},
		}, []string{"telegram", "discord", "google", "remind", "rpc", "tidy"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			srcs := Sources(&core.Ctx{Profile: tc.prof})
			names := make([]string, 0, len(srcs))
			for _, s := range srcs {
				names = append(names, s.Name())
			}
			if strings.Join(names, ",") != strings.Join(tc.want, ",") {
				t.Errorf("sources = %v, want %v", names, tc.want)
			}
		})
	}
}

// TestRunStartupLogNamesDirAndSources pins the one startup line: it names
// the folder and every started source.
func TestRunStartupLogNamesDirAndSources(t *testing.T) {
	ctx, sink := hostCtx(t)
	src := &fakeSource{name: "alpha", prefixes: []string{"A"}, lock: "alpha", started: make(chan struct{}, 4),
		body: func(ctx context.Context) error { <-ctx.Done(); return nil }}
	cancel, done := startHost(t, ctx, Options{Registry: func(*core.Ctx) []core.Source { return []core.Source{src} }})
	defer waitHost(t, cancel, done)

	line := sink.await(t, "omosense host starting")
	if !strings.Contains(line, "dir="+ctx.Dir) || !strings.Contains(line, "sources=alpha") {
		t.Fatalf("startup line = %q, want the dir and the started sources", line)
	}
}

// TestCrashRestartsWithBackoff pins the supervisor semantics: a source that
// returns logs one crashed line and is restarted, with the 5s-doubling
// backoff the retired daemon used.
func TestCrashRestartsWithBackoff(t *testing.T) {
	ctx, sink := hostCtx(t)
	clock := newFakeClock()
	src := &fakeSource{name: "boom", lock: "boom", started: make(chan struct{}, 8)}
	cancel, done := startHost(t, ctx, Options{
		Registry: func(*core.Ctx) []core.Source { return []core.Source{src} },
		Clock:    clock,
	})
	defer waitHost(t, cancel, done)

	for i, want := range []time.Duration{5 * time.Second, 10 * time.Second, 20 * time.Second} {
		select {
		case <-src.started:
		case <-time.After(10 * time.Second):
			t.Fatalf("start #%d never happened", i+1)
		}
		sink.await(t, "LOG omosense source boom crashed: boom")
		if got := clock.awaitAfterNot(t, 10*time.Minute); got != want {
			t.Fatalf("backoff wait #%d = %v, want %v", i+1, got, want)
		}
		clock.advance()
	}
	if _, err := os.Stat(filepath.Join(ctx.State, "boom.lock.json")); !os.IsNotExist(err) {
		t.Fatalf("crashed source left a lock behind: %v", err)
	}
}

// TestLockHeldByLiveProcessRetries pins the second-instance rule: a live
// holder logs ALREADY_RUNNING with its lock JSON, retries every 30s, logs
// once per holder, and starts as soon as the holder is gone.
func TestLockHeldByLiveProcessRetries(t *testing.T) {
	ctx, sink := hostCtx(t)
	clock := newFakeClock()
	if err := os.MkdirAll(ctx.State, 0o755); err != nil {
		t.Fatal(err)
	}
	sleeper := exec.Command("sleep", "60")
	if err := sleeper.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = sleeper.Process.Kill()
		_, _ = sleeper.Process.Wait()
	})
	held := `{"pid":` + strconv.Itoa(sleeper.Process.Pid) + `,"session":null,"pane":null,"cwd":"/tmp","started":"2026-10-03T00:00:00.000Z"}`
	if err := os.WriteFile(filepath.Join(ctx.State, "boom.lock.json"), []byte(held), 0o644); err != nil {
		t.Fatal(err)
	}
	src := &fakeSource{name: "boom", lock: "boom", started: make(chan struct{}, 8),
		body: func(ctx context.Context) error { <-ctx.Done(); return nil }}
	cancel, done := startHost(t, ctx, Options{
		Registry: func(*core.Ctx) []core.Source { return []core.Source{src} },
		Clock:    clock,
	})
	defer waitHost(t, cancel, done)

	line := sink.await(t, "LOG ALREADY_RUNNING boom ")
	if !strings.Contains(line, `"pid":`+strconv.Itoa(sleeper.Process.Pid)) {
		t.Fatalf("ALREADY_RUNNING line = %q, want the holder's lock JSON", line)
	}
	if got := clock.awaitAfterNot(t, 10*time.Minute); got != 30*time.Second {
		t.Fatalf("retry wait = %v, want 30s", got)
	}
	// A second blocked attempt reports the same holder without logging again.
	clock.advance()
	deadline := time.After(2 * time.Second)
	select {
	case line := <-sink.ch:
		t.Fatalf("second blocked attempt logged again: %q", line)
	case <-deadline:
	}
	// The holder dies: the next retry takes the lock and starts the source.
	if err := os.Remove(filepath.Join(ctx.State, "boom.lock.json")); err != nil {
		t.Fatal(err)
	}
	clock.advance()
	select {
	case <-src.started:
	case <-time.After(10 * time.Second):
		t.Fatal("source never started after the holder went away")
	}
}

// TestSharedLockRefcounted pins the telegram/discord listen lock: two
// sources share one lock file, and the file disappears only when the last
// of them stops.
func TestSharedLockRefcounted(t *testing.T) {
	ctx, _ := hostCtx(t)
	release := make(chan struct{})
	body := func(ctx context.Context) error {
		select {
		case <-release:
		case <-ctx.Done():
		}
		return nil
	}
	first := &fakeSource{name: "telegram", lock: "listen", started: make(chan struct{}, 4), body: body}
	second := &fakeSource{name: "discord", lock: "listen", started: make(chan struct{}, 4), body: body}
	cancel, done := startHost(t, ctx, Options{
		Registry: func(*core.Ctx) []core.Source { return []core.Source{first, second} },
	})
	lock := filepath.Join(ctx.State, "listen.lock.json")
	select {
	case <-first.started:
	case <-time.After(10 * time.Second):
		t.Fatal("telegram source never started")
	}
	select {
	case <-second.started:
	case <-time.After(10 * time.Second):
		t.Fatal("discord source never started")
	}
	if _, err := os.Stat(lock); err != nil {
		t.Fatalf("shared listen lock missing while both sources run: %v", err)
	}
	close(release)
	waitHost(t, cancel, done)
	if _, err := os.Stat(lock); !os.IsNotExist(err) {
		t.Fatalf("listen lock not released after both sources stopped: %v", err)
	}
}

// TestPanicRecoveredAndRestarted pins the panic guard: a panicking source
// becomes a crashed LOG line instead of taking the host down.
func TestPanicRecoveredAndRestarted(t *testing.T) {
	ctx, sink := hostCtx(t)
	clock := newFakeClock()
	src := &fakeSource{name: "boom", lock: "boom", started: make(chan struct{}, 8),
		body: func(context.Context) error { panic("kaboom") }}
	cancel, done := startHost(t, ctx, Options{
		Registry: func(*core.Ctx) []core.Source { return []core.Source{src} },
		Clock:    clock,
	})
	defer waitHost(t, cancel, done)

	sink.await(t, "LOG omosense source boom crashed: panic: kaboom")
	select {
	case <-src.started:
	case <-time.After(10 * time.Second):
		t.Fatal("source was not restarted after a panic")
	}
}

// TestBreakerIgnoresBlockedRetries pins IS-9's exclusion: a source that
// only ever hit ALREADY_RUNNING (or a lock error) is never counted by the
// breaker, however many 30s retries pass.
func TestBreakerIgnoresBlockedRetries(t *testing.T) {
	ctx, sink := hostCtx(t)
	clock := newFakeClock()
	if err := os.MkdirAll(ctx.State, 0o755); err != nil {
		t.Fatal(err)
	}
	sleeper := exec.Command("sleep", "300")
	if err := sleeper.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = sleeper.Process.Kill()
		_, _ = sleeper.Process.Wait()
	})
	held := `{"pid":` + strconv.Itoa(sleeper.Process.Pid) + `,"session":null,"pane":null,"cwd":"/tmp","started":"2026-10-03T00:00:00.000Z"}`
	if err := os.WriteFile(filepath.Join(ctx.State, "boom.lock.json"), []byte(held), 0o644); err != nil {
		t.Fatal(err)
	}
	src := &fakeSource{name: "boom", lock: "boom", started: make(chan struct{}, 8),
		body: func(ctx context.Context) error { <-ctx.Done(); return nil }}
	cancel, done := startHost(t, ctx, Options{
		Registry: func(*core.Ctx) []core.Source { return []core.Source{src} },
		Clock:    clock,
	})
	defer waitHost(t, cancel, done)

	sink.await(t, "LOG ALREADY_RUNNING boom ")
	// Far more blocked retries than the six the breaker needs. The wait
	// fails the moment a stop or crash line appears, and each 30s arm
	// drives the next blocked attempt.
	for i := 0; i < 10; i++ {
	wait:
		for {
			select {
			case line := <-sink.ch:
				if strings.Contains(line, "source-stopped") || strings.Contains(line, "crashed") {
					t.Fatalf("blocked retries tripped the breaker: %q", line)
				}
			case d := <-clock.after:
				if d == 10*time.Minute {
					continue // the state-file watcher's drum
				}
				if d != 30*time.Second {
					t.Fatalf("retry wait #%d = %v, want 30s", i+1, d)
				}
				break wait
			case <-time.After(10 * time.Second):
				t.Fatal("no 30s retry timer was armed within 10s")
			}
		}
		clock.advance()
	}
	t.Logf("REACHED: eleven blocked 30s retries, more than the breaker's six")
	for _, line := range sink.snapshot() {
		if strings.Contains(line, "source-stopped") {
			t.Fatalf("blocked retries tripped the breaker: %q", line)
		}
	}
	// The holder goes away: the retry loop is still alive and starts the job.
	// Each announcement proves the worker re-armed (or the state drum
	// re-armed); firing everything pending drives the next acquire.
	if err := os.Remove(filepath.Join(ctx.State, "boom.lock.json")); err != nil {
		t.Fatal(err)
	}
	deadline := time.After(10 * time.Second)
	for {
		select {
		case <-src.started:
			return
		case <-clock.after:
			clock.advance()
		case <-deadline:
			t.Fatal("source never started after the holder went away")
		}
	}
}

// TestBreakerStopsCrashLoopingSource pins IS-9: the sixth consecutive crash
// stops only that source — one exact source-stopped line, its lock released,
// no seventh start — while a healthy second source keeps running and the
// host still joins everything and returns nil on shutdown.
func TestBreakerStopsCrashLoopingSource(t *testing.T) {
	ctx, sink := hostCtx(t)
	clock := newFakeClock()
	boom := &fakeSource{name: "boom", lock: "boom", started: make(chan struct{}, 16)}
	calm := &fakeSource{name: "calm", lock: "calm", started: make(chan struct{}, 4),
		body: func(ctx context.Context) error { <-ctx.Done(); return nil }}
	cancel, done := startHost(t, ctx, Options{
		Registry: func(*core.Ctx) []core.Source { return []core.Source{boom, calm} },
		Clock:    clock,
	})

	select {
	case <-calm.started:
	case <-time.After(10 * time.Second):
		cancel()
		t.Fatal("healthy source never started")
	}
	for i := 1; i <= 6; i++ {
		select {
		case <-boom.started:
		case <-time.After(10 * time.Second):
			cancel()
			t.Fatalf("start #%d never happened", i)
		}
		sink.await(t, "LOG omosense source boom crashed: boom")
		if i < 6 {
			clock.awaitAfterNot(t, 10*time.Minute)
			clock.advance()
		}
	}
	t.Logf("REACHED: six consecutive crashes of source boom")
	want := `LOG source-stopped {"source":"boom","crashes":6,"last_error":"boom"}`
	if line := sink.await(t, "LOG source-stopped "); line != want {
		t.Fatalf("source-stopped line = %q, want %q", line, want)
	}
	if _, err := os.Stat(filepath.Join(ctx.State, "boom.lock.json")); !os.IsNotExist(err) {
		t.Fatalf("stopped worker left its lock behind: %v", err)
	}
	// No seventh start and no further lines from the stopped worker.
	deadline := time.After(2 * time.Second)
	for {
		var stop bool
		select {
		case line := <-sink.ch:
			if strings.Contains(line, "source-stopped") || strings.Contains(line, "boom crashed") {
				t.Fatalf("stopped worker kept logging: %q", line)
			}
		case <-boom.started:
			t.Fatal("stopped worker started a seventh job")
		case <-deadline:
			stop = true
		}
		if stop {
			break
		}
	}
	if _, err := os.Stat(filepath.Join(ctx.State, "calm.lock.json")); err != nil {
		t.Fatalf("healthy source's lock missing: %v", err)
	}
	waitHost(t, cancel, done)
	for _, lock := range []string{"boom.lock.json", "calm.lock.json"} {
		if _, err := os.Stat(filepath.Join(ctx.State, lock)); !os.IsNotExist(err) {
			t.Fatalf("%s left behind after shutdown: %v", lock, err)
		}
	}
}

// TestBreakerResetsAfterLongRun pins IS-9's reset: a job that ran five
// minutes or more before crashing is crash #1 again, so five quick crashes
// plus one long crash do not stop the source.
func TestBreakerResetsAfterLongRun(t *testing.T) {
	ctx, sink := hostCtx(t)
	clock := newFakeClock()
	endRun := make(chan struct{})
	runs := 0
	src := &fakeSource{name: "slow", lock: "slow", started: make(chan struct{}, 16),
		body: func(context.Context) error {
			runs++
			if runs == 6 {
				<-endRun // the long-running job
				return errors.New("late crash")
			}
			return errors.New("quick crash")
		}}
	cancel, done := startHost(t, ctx, Options{
		Registry: func(*core.Ctx) []core.Source { return []core.Source{src} },
		Clock:    clock,
	})
	defer waitHost(t, cancel, done)

	for i := 1; i <= 5; i++ {
		select {
		case <-src.started:
		case <-time.After(10 * time.Second):
			t.Fatalf("start #%d never happened", i)
		}
		sink.await(t, "LOG omosense source slow crashed: quick crash")
		clock.awaitAfterNot(t, 10*time.Minute)
		clock.advance()
	}
	// Job #6 starts, then the clock moves past the five-minute reset mark
	// while it runs.
	select {
	case <-src.started:
	case <-time.After(10 * time.Second):
		t.Fatal("start #6 never happened")
	}
	clock.advanceTime(5 * time.Minute)
	close(endRun)
	sink.await(t, "LOG omosense source slow crashed: late crash")
	t.Logf("REACHED: five quick crashes, then a crash after a five-minute run")
	// The worker must arm a backoff (still alive) rather than print a stop.
	deadline := time.After(10 * time.Second)
armWait:
	for {
		select {
		case line := <-sink.ch:
			if strings.Contains(line, "source-stopped") {
				t.Fatalf("long run did not reset the breaker: %q", line)
			}
		case d := <-clock.after:
			if d == 10*time.Minute {
				continue // the state-file watcher's drum
			}
			break armWait // the worker's own backoff: it did not stop
		case <-deadline:
			t.Fatal("worker neither armed a backoff nor stopped")
		}
	}
	clock.advance()
	select {
	case <-src.started:
	case <-time.After(10 * time.Second):
		t.Fatal("worker stopped although the long run reset the crash count")
	}
	for _, line := range sink.snapshot() {
		if strings.Contains(line, "source-stopped") {
			t.Fatalf("long run did not reset the breaker: %q", line)
		}
	}
}

// TestStateFileLargeWarning pins IS-10: a state file over guard.stateFileBytes
// logs one exact state-file-large line at start, does not repeat while it
// stays over, re-arms once it is back at or under the limit, and a file
// exactly at the limit never warns.
func TestStateFileLargeWarning(t *testing.T) {
	ctx, sink := hostCtx(t)
	if err := os.MkdirAll(filepath.Join(ctx.State, "inbox"), 0o755); err != nil {
		t.Fatal(err)
	}
	const limit = 1024
	ctx.Profile.Guard.StateFileBytes = limit
	big := filepath.Join(ctx.State, "inbox", "big.json")
	if err := os.WriteFile(big, bytes.Repeat([]byte("x"), limit+1), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(ctx.State, "exact.bin"), bytes.Repeat([]byte("x"), limit), 0o644); err != nil {
		t.Fatal(err)
	}
	clock := newFakeClock()
	cancel, done := startHost(t, ctx, Options{
		Registry: func(*core.Ctx) []core.Source { return nil },
		Clock:    clock,
	})
	defer waitHost(t, cancel, done)

	count := func() int {
		n := 0
		for _, line := range sink.snapshot() {
			if strings.Contains(line, "LOG state-file-large ") {
				n++
			}
		}
		return n
	}
	want := fmt.Sprintf(`LOG state-file-large {"path":%q,"bytes":%d,"limit":%d}`, big, limit+1, limit)
	if line := sink.await(t, "LOG state-file-large "); line != want {
		t.Fatalf("state-file-large line = %q, want %q", line, want)
	}
	t.Logf("REACHED: over-limit file warned once at start")
	// Still over the limit: the next check must not repeat the line.
	if got := clock.awaitAfter(t); got != 10*time.Minute {
		t.Fatalf("check interval = %v, want 10m", got)
	}
	clock.advance()
	clock.awaitAfter(t) // the next timer armed: the second check has run
	if n := count(); n != 1 {
		t.Fatalf("state-file-large repeated while still over the limit: %d lines", n)
	}
	// Back under the limit: re-armed silently.
	if err := os.WriteFile(big, []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	clock.advance()
	clock.awaitAfter(t)
	// Over the limit again: exactly one more line.
	if err := os.WriteFile(big, bytes.Repeat([]byte("x"), limit+5), 0o644); err != nil {
		t.Fatal(err)
	}
	clock.advance()
	want = fmt.Sprintf(`LOG state-file-large {"path":%q,"bytes":%d,"limit":%d}`, big, limit+5, limit)
	if line := sink.await(t, "LOG state-file-large "); line != want {
		t.Fatalf("re-armed state-file-large line = %q, want %q", line, want)
	}
	for _, line := range sink.snapshot() {
		if strings.Contains(line, "exact.bin") {
			t.Fatalf("file exactly at the limit warned: %q", line)
		}
	}
}

// bufWriter adapts a chanSink to the io.Writer core.Out wraps, so every
// emitted line reaches the test's channel.
type bufWriter struct{ sink *chanSink }

func (w *bufWriter) Write(b []byte) (int, error) {
	w.sink.add(strings.TrimSuffix(string(b), "\n"))
	return len(b), nil
}
