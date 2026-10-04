package daemon

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/DevNewbie1826/omosense/internal/core"
)

type profileStopWire struct {
	OK        bool     `json:"ok"`
	Error     string   `json:"error"`
	Profile   string   `json:"profile"`
	Stopped   []string `json:"stopped"`
	Cancelled int      `json:"cancelled_reminders"`
	Daemon    string   `json:"daemon"`
}

func sendProfileStop(t *testing.T, s *server, profile string) (net.Conn, *framer) {
	t.Helper()
	conn, err := net.Dial("unix", s.paths.socket)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	conn.SetDeadline(time.Now().Add(30 * time.Second))
	if err := writeFrame(conn, map[string]string{"cmd": "stop-profile", "profile": profile}); err != nil {
		t.Fatal(err)
	}
	return conn, newFramer(conn)
}

func stopProfileWire(t *testing.T, s *server, profile string) profileStopWire {
	t.Helper()
	conn, f := sendProfileStop(t, s, profile)
	defer conn.Close()
	var r profileStopWire
	if err := f.read(&r); err != nil {
		t.Fatal(err)
	}
	t.Logf("stop %s: %+v", profile, r)
	return r
}

func assertProfileMarker(t *testing.T, state, profile string) {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(state, "omosense-profile-"+profile+".stopped"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := time.Parse(time.RFC3339Nano, strings.TrimSpace(string(b))); err != nil || !strings.HasSuffix(string(b), "\n") {
		t.Fatalf("invalid marker %q: %v", b, err)
	}
}

func reminderClientsAttached(st status) bool {
	for _, c := range st.Clients {
		for _, source := range c.Sources {
			if source == "remind" {
				return true
			}
		}
	}
	return false
}

// awaitReminderClientsDetached waits until status no longer lists a remind
// subscriber. Conn.Close returns before the server read loop drops that client,
// and an emit in between marks the line delivered.
func awaitReminderClientsDetached(t *testing.T, s *server) {
	t.Helper()
	detached := make(chan struct{})
	stop := make(chan struct{})
	go func() {
		defer close(detached)
		for {
			if !reminderClientsAttached(requestStatus(t, s)) {
				return
			}
			select {
			case <-stop:
				return
			default:
			}
		}
	}()
	defer close(stop)
	await(t, detached)
}

func TestProfileStopKeepsMainRunningDiscardsJournalAndShutsClients(t *testing.T) {
	// Given: both profiles have AlwaysOn and attach-driven sources.
	s := serverFixture(t, fakeRegistry)
	_, main, r := wireClient(t, s, hello{Profile: "main", Sources: []string{"google"}})
	if !r.OK {
		t.Fatal(r.Error)
	}
	_, family, r := wireClient(t, s, hello{Profile: "family", Sources: []string{"google"}})
	if !r.OK {
		t.Fatal(r.Error)
	}
	for _, f := range []*framer{main, family} {
		readWire(t, f) // fake LOG
		if got := readWire(t, f); !strings.HasPrefix(got.Line, "CAL ") {
			t.Fatal(got)
		}
	}
	for _, profile := range []string{"main", "family"} {
		c, f, r := wireClient(t, s, hello{Profile: profile, Sources: []string{"remind"}, Only: []string{"REMIND"}})
		if !r.OK || !strings.HasPrefix(readWire(t, f).Line, "REMIND ") {
			t.Fatal("AlwaysOn source did not start")
		}
		c.Close()
	}
	awaitReminderClientsDetached(t, s)
	s.emit("family", "remind", true, `REMIND failed {"id":"due"}`)
	reminders := filepath.Join(s.base.State, "reminders-family.json")
	if err := os.WriteFile(reminders, []byte(`[{"id":"future","at":"2030-01-01"},{"id":"sent","sent":"yes"}]`), 0o600); err != nil {
		t.Fatal(err)
	}
	before := requestStatus(t, s)

	// When: profile control is sent through the real Unix socket.
	got := stopProfileWire(t, s, "family")

	// Then: all FAMILY workers stop, MAIN remains live, stale replay is discarded.
	if !got.OK || got.Daemon != "running" || got.Profile != "family" || got.Cancelled != 1 ||
		!reflect.DeepEqual(got.Stopped, []string{"google", "herdr", "remind", "telegram", "tidy"}) {
		t.Fatalf("profile stop reply: %+v", got)
	}
	assertProfileMarker(t, s.base.State, "family")
	after := requestStatus(t, s)
	if after.PID != before.PID || len(after.Clients) == 0 {
		t.Fatalf("MAIN daemon/clients changed: %+v", after)
	}
	for _, c := range after.Clients {
		if c.Profile != "main" {
			t.Fatalf("family client remains: %+v", c)
		}
	}
	for _, src := range after.Sources {
		if src.Profile == "family" && src.State != "stopped" {
			t.Fatalf("family still active: %+v", src)
		}
		if src.Profile == "main" {
			for _, old := range before.Sources {
				if old.Profile == src.Profile && old.Name == src.Name && old.State != src.State {
					t.Fatalf("MAIN changed: %+v -> %+v", old, src)
				}
			}
		}
	}
	if got := readWire(t, family); got.Ctl != "shutdown" || got.Reason != "stop" {
		t.Fatalf("family shutdown: %+v", got)
	}
	if err := family.read(&frame{}); err == nil {
		t.Fatal("family connection was not closed")
	}
	s.emit("main", "google", false, "CAL main-still-live")
	if got := readWire(t, main); got.Line != "CAL main-still-live" {
		t.Fatal(got)
	}
	b, err := os.ReadFile(reminders)
	if err != nil || strings.Count(string(b), `"cancelled"`) != 1 || !strings.Contains(string(b), `"sent": "yes"`) {
		t.Fatalf("reminder teardown: %s %v", b, err)
	}
	journalPath := filepath.Join(s.base.State, "omosense-journal-family.jsonl")
	journalBody, err := os.ReadFile(journalPath)
	if err != nil {
		t.Fatal(err)
	}
	var due uint64
	marked := map[uint64]bool{}
	for _, line := range strings.Split(strings.TrimSpace(string(journalBody)), "\n") {
		var e journalEntry
		if err := json.Unmarshal([]byte(line), &e); err != nil {
			t.Fatal(err)
		}
		if strings.Contains(e.Line, `"id":"due"`) {
			due = e.Seq
			if e.Delivered {
				t.Fatal("fixture due line was already delivered")
			}
		}
		marked[e.Mark] = true
	}
	if due == 0 || !marked[due] {
		t.Fatalf("discard not persisted for due seq %d: %s", due, journalBody)
	}
	_, replay, r := wireClient(t, s, hello{Profile: "family", Sources: []string{"remind"}, Only: []string{"REMIND"}})
	if !r.OK {
		t.Fatal(r.Error)
	}
	s.emit("family", "", false, "LOG omosense replay-barrier")
	for {
		f := readWire(t, replay)
		if strings.Contains(f.Line, `"id":"due"`) {
			t.Fatal("stale family reminder replayed")
		}
		if f.Line == "LOG omosense replay-barrier" {
			break
		}
	}
	_, err = os.Stat(filepath.Join(s.base.State, "omosense-profile-family.stopped"))
	if !os.IsNotExist(err) {
		t.Fatalf("hello did not remove marker: %v", err)
	}
}

func TestProfileStopRejectsHelloAndSecondStopWhileJoining(t *testing.T) {
	// Given: FAMILY Run acknowledges cancellation but gates its return.
	started, cancelled, release := make(chan struct{}), make(chan struct{}), make(chan struct{})
	otherStarted, otherCancelled := make(chan struct{}), make(chan struct{})
	var once atomic.Bool
	var otherOnce atomic.Bool
	s := serverFixture(t, func(c *core.Ctx) []core.Source {
		if c.Profile.Name != "family" {
			return nil
		}
		return []core.Source{&testSource{name: "remind", lock: "remind-family", always: true,
			run: func(ctx context.Context, _ core.Sink) error {
				if once.CompareAndSwap(false, true) {
					close(started)
					<-ctx.Done()
					close(cancelled)
					<-release
				} else {
					<-ctx.Done()
				}
				return ctx.Err()
			}},
			&testSource{name: "google", lock: "google-family", always: true,
				run: func(ctx context.Context, _ core.Sink) error {
					if otherOnce.CompareAndSwap(false, true) {
						close(otherStarted)
						<-ctx.Done()
						close(otherCancelled)
						<-release
					} else {
						<-ctx.Done()
					}
					return ctx.Err()
				}},
		}
	})
	t.Cleanup(func() {
		select {
		case <-release:
		default:
			close(release)
		}
	})
	await(t, started)
	await(t, otherStarted)
	_, f := sendProfileStop(t, s, "family")
	await(t, cancelled)
	await(t, otherCancelled) // Every halt must be issued before awaiting any join.

	// When: hello and another stop arrive while the first stop is joining.
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	conn, _, _, err := handshake(ctx, s.paths, hello{Hello: 1, Version: "test-v1", Profile: "family"})
	if conn != nil {
		conn.Close()
	}
	if err == nil || err.Error() != "profile family is stopping" {
		t.Fatalf("mid-stop hello: %v", err)
	}
	if got := stopProfileWire(t, s, "family"); got.OK || got.Error != "profile family is already stopping" {
		t.Fatalf("second stop: %+v", got)
	}
	close(release)
	var r profileStopWire
	if err := f.read(&r); err != nil || !r.OK {
		t.Fatalf("joined reply: %+v %v", r, err)
	}
	_, _, helloReply := wireClient(t, s, hello{Profile: "family"})
	if !helloReply.OK {
		t.Fatal(helloReply.Error)
	}
}

func TestProfileStopConcurrentNormalHellosBothSucceed(t *testing.T) {
	s := serverFixture(t, func(*core.Ctx) []core.Source { return nil })
	start := make(chan struct{})
	results := make(chan error, 2)
	for range 2 {
		go func() {
			<-start
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			c, _, _, err := handshake(ctx, s.paths, hello{Hello: 1, Version: "test-v1", Profile: "family"})
			if c != nil {
				c.Close()
			}
			results <- err
		}()
	}
	close(start)
	for range 2 {
		if err := await(t, results); err != nil {
			t.Fatal(err)
		}
	}
}

func foreignReminderLock(t *testing.T, state, name string) string {
	t.Helper()
	holder := exec.Command(os.Args[0], "-test.run=^TestSupervisorLockHolder$")
	sandbox := socketPaths(t)
	holder.Env = append(os.Environ(), "OMOSENSE_LOCK_HELPER=1",
		"HOME="+sandbox.dir, "OMOMEOW_DIR="+sandbox.dir,
		"OMOMEOW_STATE="+state, "OMOSENSE_SOCK="+sandbox.socket)
	stdin, err := holder.StdinPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := holder.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		stdin.Close()
		if err := holder.Wait(); err != nil {
			t.Error(err)
		}
		t.Logf("cleanup: foreign holder %d reaped", holder.Process.Pid)
	})
	metadata := fmt.Sprintf(`{"pid":%d,"session":"foreign-profile-stop","pane":"holder"}`, holder.Process.Pid)
	if err := os.WriteFile(filepath.Join(state, name+".lock.json"), []byte(metadata), 0o600); err != nil {
		t.Fatal(err)
	}
	return metadata
}

func TestProfileStopForeignReminderLockStillPersistsTeardown(t *testing.T) {
	for _, name := range []string{"remind-family", "remind"} {
		t.Run(name, func(t *testing.T) {
			s := serverFixture(t, func(*core.Ctx) []core.Source { return nil })
			body := `[ {"id":"pending","note":"untouched"} ]`
			file := filepath.Join(s.base.State, "reminders-family.json")
			if err := os.WriteFile(file, []byte(body), 0o600); err != nil {
				t.Fatal(err)
			}
			metadata := foreignReminderLock(t, s.base.State, name)
			_, client, _ := wireClient(t, s, hello{Profile: "family"})
			s.emit("family", "remind", true, "REMIND stale")
			got := stopProfileWire(t, s, "family")
			if got.OK || !strings.Contains(got.Error, name+" "+metadata) {
				t.Fatalf("missing lock holder metadata: %+v", got)
			}
			b, err := os.ReadFile(file)
			if err != nil || string(b) != body {
				t.Fatalf("blocked cancellation touched file: %q %v", b, err)
			}
			assertProfileMarker(t, s.base.State, "family")
			if got := readWire(t, client); got.Ctl != "shutdown" || got.Reason != "stop" {
				t.Fatal(got)
			}
			s.mu.Lock()
			pending := s.profiles["family"].journal.pending(hello{Profile: "family", Sources: []string{"remind"}})
			s.mu.Unlock()
			if len(pending) != 0 {
				t.Fatal("failed cancellation prevented journal discard")
			}
		})
	}
}

func TestProfileStopUnknownProfileDoesNotStopDaemon(t *testing.T) {
	s := serverFixture(t, func(*core.Ctx) []core.Source { return nil })
	if got := stopProfileWire(t, s, "nope"); got.OK || got.Error != "unknown profile nope" {
		t.Fatalf("unknown stop: %+v", got)
	}
	requestStatus(t, s)
}

func TestProfileStopMarkersLoadAfterLifetimeBeforeWorkers(t *testing.T) {
	// Given: construct the server first, then write a marker before serve.
	// This distinguishes the lifetime-boundary load from a newServer load.
	p := socketPaths(t)
	state := t.TempDir()
	cfg := &core.Cfg{Profiles: core.NewOMap()}
	cfg.Profiles.Set("main", core.NewOMap())
	cfg.Profiles.Set("family", core.NewOMap())
	started := make(chan string, 4)
	var familyRuns atomic.Int32
	s, err := newServer(&core.Ctx{Dir: p.dir, State: state, Cfg: cfg}, p, serverOptions{
		version: "test-v1", clock: realClock{}, ready: make(chan struct{}),
		registry: func(c *core.Ctx) []core.Source {
			return []core.Source{&testSource{name: "remind", lock: "remind-" + c.Profile.Name, always: true,
				run: func(ctx context.Context, _ core.Sink) error {
					if c.Profile.Name == "family" {
						familyRuns.Add(1)
					}
					started <- c.Profile.Name
					<-ctx.Done()
					return ctx.Err()
				}}}
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(state, "omosense-profile-family.stopped"), []byte(core.ISO(time.Now())+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- s.serve(ctx) }()
	t.Cleanup(func() {
		cancel()
		if err := await(t, done); err != nil {
			t.Error(err)
		}
	})
	await(t, s.options.ready)
	if got := await(t, started); got != "main" {
		t.Fatalf("marked profile ran at startup: %s", got)
	}
	for _, src := range requestStatus(t, s).Sources {
		if src.Profile == "family" && src.State != "stopped" {
			t.Fatalf("marked worker not stopped: %+v", src)
		}
	}
	if familyRuns.Load() != 0 {
		t.Fatal("FAMILY ran before hello")
	}
	_, _, r := wireClient(t, s, hello{Profile: "family"})
	if !r.OK {
		t.Fatal(r.Error)
	}
	if got := await(t, started); got != "family" || familyRuns.Load() != 1 {
		t.Fatalf("hello did not resume family: %s count=%d", got, familyRuns.Load())
	}
	if _, err := os.Stat(filepath.Join(state, "omosense-profile-family.stopped")); !os.IsNotExist(err) {
		t.Fatalf("marker remains: %v", err)
	}
}

func TestProfileStopDeadlinesAndTimeoutPersistStopped(t *testing.T) {
	for _, timeout := range []bool{false, true} {
		t.Run(fmt.Sprintf("timeout=%t", timeout), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				// Given: an in-memory connection and worker whose cancellation
				// either takes six virtual seconds or is gated past the join bound.
				state := t.TempDir()
				cfg := &core.Cfg{Profiles: core.NewOMap()}
				cfg.Profiles.Set("family", core.NewOMap())
				started, release := make(chan struct{}), make(chan struct{})
				s, err := newServer(&core.Ctx{State: state, Cfg: cfg}, paths{}, serverOptions{
					version: "test-v1", clock: realClock{},
					registry: func(*core.Ctx) []core.Source {
						return []core.Source{&testSource{name: "remind", lock: "remind-family", always: true,
							run: func(ctx context.Context, _ core.Sink) error {
								close(started)
								<-ctx.Done()
								if timeout {
									<-release
								} else {
									timer := time.NewTimer(6 * time.Second)
									defer timer.Stop()
									<-timer.C // Time is the deadline behavior under test.
								}
								return ctx.Err()
							}}}
					},
				})
				if err != nil {
					t.Fatal(err)
				}
				j, err := openJournal(filepath.Join(state, "journal"), time.Now())
				if err != nil {
					t.Fatal(err)
				}
				defer j.close()
				s.profiles["family"].journal = j
				body := `[{"id":"pending"}]`
				file := filepath.Join(state, "reminders-family.json")
				if err := os.WriteFile(file, []byte(body), 0o600); err != nil {
					t.Fatal(err)
				}
				ctx, cancel := context.WithCancel(context.Background())
				done := make(chan struct{})
				go func() { s.profiles["family"].workers["remind"].run(ctx); close(done) }()
				defer func() { close(release); cancel(); <-done }()
				<-started
				serverConn, peer := net.Pipe()
				defer serverConn.Close()
				defer peer.Close()
				var cmd command
				if err := json.Unmarshal([]byte(`{"cmd":"stop-profile","profile":"family"}`), &cmd); err != nil {
					t.Fatal(err)
				}
				controlled := make(chan struct{})

				// When: the real control writer handles the stop on a live connection.
				begin := time.Now()
				go func() { s.control(serverConn, cmd); close(controlled) }()
				var r profileStopWire
				err = newFramer(peer).read(&r)

				// Then: the write deadline starts after teardown; timeout still
				// persists stop and does not cancel while Run owns the lock.
				if err != nil {
					t.Fatalf("control write deadline expired: %v", err)
				}
				<-controlled
				if timeout {
					if r.OK || !strings.Contains(r.Error, "timeout") || time.Since(begin) != 20*time.Second {
						t.Fatalf("join bound: %+v elapsed=%s", r, time.Since(begin))
					}
					b, err := os.ReadFile(file)
					if err != nil || string(b) != body {
						t.Fatalf("unjoined worker cancellation: %q %v", b, err)
					}
				} else if !r.OK || r.Cancelled != 1 || time.Since(begin) != 6*time.Second {
					t.Fatalf("late successful teardown: %+v elapsed=%s", r, time.Since(begin))
				}
				assertProfileMarker(t, state, "family")
			})
		})
	}
}
