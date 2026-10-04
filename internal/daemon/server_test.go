package daemon

import (
	"context"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/DevNewbie1826/omosense/internal/core"
)

func await[T any](t *testing.T, ch <-chan T) T {
	t.Helper()
	select {
	case v := <-ch:
		return v
	case <-time.After(10 * time.Second):
		t.Fatal("bounded signal await timed out")
		var zero T
		return zero
	}
}

func serverFixture(t *testing.T, registry func(*core.Ctx) []core.Source) *server {
	t.Helper()
	p := socketPaths(t)
	state := t.TempDir()
	base := &core.Ctx{Dir: p.dir, State: state, Cfg: &core.Cfg{Profiles: core.NewOMap()}}
	base.Cfg.Profiles.Set("main", core.NewOMap())
	base.Cfg.Profiles.Set("family", core.NewOMap())
	ready := make(chan struct{})
	s, err := newServer(base, p, serverOptions{version: "test-v1", registry: registry, clock: realClock{}, ready: ready})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- s.serve(ctx) }()
	select {
	case <-ready:
	case err := <-done:
		t.Fatalf("server did not become ready: %v", err)
	case <-time.After(10 * time.Second):
		t.Fatal("server readiness timed out")
	}
	t.Cleanup(func() {
		cancel()
		if err := await(t, done); err != nil {
			t.Errorf("server teardown: %v", err)
		}
		for _, path := range []string{p.socket, p.pid} {
			if _, err := os.Stat(path); !os.IsNotExist(err) {
				t.Errorf("server leaked %s: %v", path, err)
			}
		}
	})
	return s
}

func wireClient(t *testing.T, s *server, h hello) (net.Conn, *framer, reply) {
	t.Helper()
	conn, err := net.Dial("unix", s.paths.socket)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	conn.SetDeadline(time.Now().Add(10 * time.Second))
	if h.Hello == 0 {
		h.Hello = 1
	}
	if h.Version == "" {
		h.Version = "test-v1"
	}
	if err := writeFrame(conn, h); err != nil {
		t.Fatal(err)
	}
	f := newFramer(conn)
	var r reply
	if err := f.read(&r); err != nil {
		t.Fatal(err)
	}
	if r.OK {
		if err := writeFrame(conn, command{Cmd: "subscribe", Version: r.Version}); err != nil {
			t.Fatal(err)
		}
		if err := f.read(&r); err != nil {
			t.Fatal(err)
		}
	}
	return conn, f, r
}

func readWire(t *testing.T, f *framer) frame {
	t.Helper()
	var v frame
	if err := f.read(&v); err != nil {
		t.Fatal(err)
	}
	return v
}

func requestStatus(t *testing.T, s *server) status {
	t.Helper()
	conn, err := net.Dial("unix", s.paths.socket)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	conn.SetDeadline(time.Now().Add(10 * time.Second))
	if err := writeFrame(conn, command{Cmd: "status"}); err != nil {
		t.Fatal(err)
	}
	var st status
	if err := newFramer(conn).read(&st); err != nil {
		t.Fatal(err)
	}
	return st
}

func TestProtocolUnknownProfileHelloRejectsWithoutStartingSources(t *testing.T) {
	s := serverFixture(t, func(*core.Ctx) []core.Source { return nil })
	_, _, r := wireClient(t, s, hello{Profile: "nope", Sources: []string{"herdr"}})
	if r.OK || r.Error != "unknown profile nope" {
		t.Fatalf("reply: %+v", r)
	}
	if st := requestStatus(t, s); len(st.Clients) != 0 || st.PID != os.Getpid() || st.Version != "test-v1" {
		t.Fatalf("status: %+v", st)
	}
}

func TestRoutingRealSocketInterestUsesSourcesNotOnly(t *testing.T) {
	started := make(chan core.Sink, 2)
	s := serverFixture(t, func(c *core.Ctx) []core.Source {
		var out []core.Source
		for _, name := range []string{"telegram", "google", "herdr", "tidy"} {
			name := name
			out = append(out, &testSource{name: name, lock: name + "-" + c.Profile.Name, run: func(ctx context.Context, sink core.Sink) error {
				if name == "herdr" {
					started <- sink
				}
				<-ctx.Done()
				return ctx.Err()
			}})
		}
		return out
	})
	_, f, r := wireClient(t, s, hello{Profile: "main", Sources: []string{"herdr"}, Only: []string{"LOG"}})
	if !r.OK {
		t.Fatalf("hello: %+v", r)
	}
	sink := await(t, started)
	s.emit("main", "google", false, "LOG foreign")
	s.emit("family", "herdr", false, "LOG other profile")
	sink.Raw("HERDR", `{"hidden":true}`)
	sink.Log("own")
	s.emit("main", "", false, "LOG omosense barrier")
	if a, b := readWire(t, f), readWire(t, f); a.Line != "LOG own" || b.Line != "LOG omosense barrier" {
		t.Fatalf("filter/routing: %+v %+v", a, b)
	}
	st := requestStatus(t, s)
	for _, src := range st.Sources {
		want := "paused"
		if src.Profile == "main" && src.Name == "herdr" {
			want = "running"
		}
		if src.State != want {
			t.Fatalf("source interest wrong: %+v", src)
		}
	}
	if len(st.Clients) != 1 {
		t.Fatalf("clients: %+v", st.Clients)
	}
}

func TestJournalSocketReplayBeforeLiveAndSecondClientGetsNoReplay(t *testing.T) {
	started := make(chan core.Sink, 2)
	emitted := make(chan struct{}, 2)
	s := serverFixture(t, func(c *core.Ctx) []core.Source {
		return []core.Source{&testSource{name: "remind", lock: "remind-" + c.Profile.Name, always: true,
			run: func(ctx context.Context, sink core.Sink) error {
				sink.Raw("REMIND", "sent "+c.Profile.Name)
				emitted <- struct{}{}
				started <- sink
				<-ctx.Done()
				return ctx.Err()
			}}}
	})
	await(t, emitted)
	await(t, emitted)
	await(t, started)
	await(t, started)
	_, f, r := wireClient(t, s, hello{Profile: "main", Sources: []string{"remind"}})
	if !r.OK {
		t.Fatal(r.Error)
	}
	s.emit("main", "remind", true, "REMIND live")
	if a, b := readWire(t, f), readWire(t, f); a.Line != "REMIND sent main" || b.Line != "REMIND live" {
		t.Fatalf("replay/live order: %+v %+v", a, b)
	}
	_, second, r := wireClient(t, s, hello{Profile: "main", Sources: []string{"remind"}})
	if !r.OK {
		t.Fatal(r.Error)
	}
	s.emit("main", "", false, "LOG omosense barrier")
	if got := readWire(t, second); got.Line != "LOG omosense barrier" {
		t.Fatalf("second attach replayed: %+v", got)
	}
}

func TestProtocolStopAndUpgradeControlFrames(t *testing.T) {
	for _, reason := range []string{"stop", "upgrade"} {
		t.Run(reason, func(t *testing.T) {
			s := serverFixture(t, func(*core.Ctx) []core.Source { return nil })
			_, a, _ := wireClient(t, s, hello{Profile: "main"})
			_, b, _ := wireClient(t, s, hello{Profile: "family"})
			c, err := net.Dial("unix", s.paths.socket)
			if err != nil {
				t.Fatal(err)
			}
			defer c.Close()
			c.SetDeadline(time.Now().Add(10 * time.Second))
			if err := writeFrame(c, command{Cmd: "stop", Reason: reason}); err != nil {
				t.Fatal(err)
			}
			for _, f := range []*framer{a, b} {
				got := readWire(t, f)
				if got.Ctl != "shutdown" || got.Reason != reason || got.Line != "" {
					t.Fatalf("control: %+v", got)
				}
				var extra frame
				if err := f.read(&extra); err == nil {
					t.Fatal("control was not followed by EOF")
				}
			}
		})
	}
}

func TestProtocolMalformedHelloIsRejected(t *testing.T) {
	s := serverFixture(t, func(*core.Ctx) []core.Source { return nil })
	for _, h := range []hello{{Hello: 2, Profile: "main"}, {Hello: 1, Profile: "main", Sources: []string{"not-a-source"}}} {
		_, _, r := wireClient(t, s, h)
		if r.OK || !strings.Contains(r.Error, "invalid") {
			t.Fatalf("malformed hello accepted: %+v", r)
		}
	}
}

func TestProtocolAllProfilesRegistered(t *testing.T) {
	registered := make(chan string, 2)
	s := serverFixture(t, func(c *core.Ctx) []core.Source {
		registered <- c.Profile.Name
		return nil
	})
	seen := map[string]bool{await(t, registered): true, await(t, registered): true}
	if !seen["main"] || !seen["family"] {
		t.Fatal(seen)
	}
	if _, err := os.Stat(filepath.Join(s.base.State, "omosense-journal-family.jsonl")); err != nil {
		t.Fatal(fmt.Errorf("family journal: %w", err))
	}
}

func TestRoutingCrashNoticeReachesOtherSourcesOfSameProfile(t *testing.T) {
	crash := make(chan struct{})
	s := serverFixture(t, func(c *core.Ctx) []core.Source {
		return []core.Source{&testSource{name: "discord", lock: "listen-" + c.Profile.Name,
			run: func(ctx context.Context, _ core.Sink) error {
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-crash:
					panic("fake crash")
				}
			}}}
	})
	_, observer, _ := wireClient(t, s, hello{Profile: "main", Sources: []string{"herdr"}, Only: []string{"HERDR"}})
	_, origin, _ := wireClient(t, s, hello{Profile: "main", Sources: []string{"discord"}})
	close(crash)
	if got := readWire(t, origin); got.Line != "LOG omosense source discord crashed: panic: fake crash" {
		t.Fatalf("crash not surfaced: %+v", got)
	}
	s.emit("main", "", false, "LOG omosense barrier")
	if got := readWire(t, observer); got.Line != "LOG omosense source discord crashed: panic: fake crash" {
		t.Fatalf("profile-wide crash notice lost: %+v", got)
	}
}

type controlledClock struct {
	mu         sync.Mutex
	now        time.Time
	registered chan time.Duration
	tick       chan time.Time
}

func (c *controlledClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *controlledClock) After(d time.Duration) <-chan time.Time {
	c.registered <- d
	return c.tick
}

func TestJournalHourlyCompactionUsesInjectedClock(t *testing.T) {
	now := time.Date(2026, 10, 3, 0, 0, 0, 0, time.UTC)
	c := &controlledClock{now: now, registered: make(chan time.Duration, 1), tick: make(chan time.Time)}
	j, err := openJournal(filepath.Join(t.TempDir(), "journal.jsonl"), now)
	if err != nil {
		t.Fatal(err)
	}
	defer j.close()
	if err := j.append(now, "discord", "EVENT expires", false); err != nil {
		t.Fatal(err)
	}
	s := &server{options: serverOptions{clock: c}, profiles: map[string]*profileHost{"main": {journal: j}}}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { s.compactLoop(ctx); close(done) }()
	defer func() { cancel(); await(t, done) }()
	if got := await(t, c.registered); got != time.Hour {
		t.Fatalf("compaction interval = %s, want 1h", got)
	}
	c.mu.Lock()
	c.now = now.Add(25 * time.Hour)
	c.mu.Unlock()
	c.tick <- c.Now()
	await(t, c.registered) // The next registration follows completed compaction.
	s.mu.Lock()
	pending := j.pending(hello{Profile: "main", Sources: []string{"discord"}})
	s.mu.Unlock()
	if len(pending) != 0 {
		t.Fatalf("hourly compaction retained expired entry: %+v", pending)
	}
}
