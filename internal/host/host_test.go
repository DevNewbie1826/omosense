package host

import (
	"context"
	"errors"
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
	pending []chan time.Time
	after   chan time.Duration
}

func newFakeClock() *fakeClock { return &fakeClock{after: make(chan time.Duration, 64)} }

func (c *fakeClock) Now() time.Time { return time.Unix(0, 0) }

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
		if got := clock.awaitAfter(t); got != want {
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
	if got := clock.awaitAfter(t); got != 30*time.Second {
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

// bufWriter adapts a chanSink to the io.Writer core.Out wraps, so every
// emitted line reaches the test's channel.
type bufWriter struct{ sink *chanSink }

func (w *bufWriter) Write(b []byte) (int, error) {
	w.sink.add(strings.TrimSuffix(string(b), "\n"))
	return len(b), nil
}
