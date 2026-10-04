package rpc

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/DevNewbie1826/omosense/internal/core"
)

type scriptTick struct {
	sessions []map[string]any
	states   map[string]any
	err      string
	drop     bool
}

type fakeRPC struct {
	listener net.Listener
	path     string
	mu       sync.Mutex
	conn     net.Conn
	calls    []string
	done     chan struct{}
}

// Each accepted connection is one tick. Responses include an unsolicited
// record and an unrelated response, proving correlation at the wire boundary.
func serveRPC(t *testing.T, path string, ticks []scriptTick) *fakeRPC {
	t.Helper()
	ln, err := net.Listen("unix", path)
	if err != nil {
		t.Fatal(err)
	}
	f := &fakeRPC{listener: ln, path: path, done: make(chan struct{})}
	go func() {
		defer close(f.done)
		n := 0
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			f.mu.Lock()
			f.conn = conn
			f.mu.Unlock()
			scan := bufio.NewScanner(conn)
			for scan.Scan() {
				var req struct {
					ID      string `json:"id"`
					Type    string `json:"type"`
					Session string `json:"sessionId"`
				}
				if err := json.Unmarshal(scan.Bytes(), &req); err != nil {
					return
				}
				f.mu.Lock()
				f.calls = append(f.calls, req.Type+" "+req.Session)
				f.mu.Unlock()
				i := n
				if i >= len(ticks) {
					i = len(ticks) - 1
				}
				tick := ticks[i]
				var data any
				var failure string
				switch req.Type {
				case "list_sessions":
					n++
					data = map[string]any{"sessions": tick.sessions}
					failure = tick.err
				case "get_state":
					// get_state belongs to the most recent list request.
					i = n - 1
					if i >= len(ticks) {
						i = len(ticks) - 1
					}
					if ticks[i].drop {
						conn.Close()
						continue
					}
					data = ticks[i].states[req.Session]
					if s, ok := data.(string); ok {
						failure = s
						data = nil
					}
				default:
					failure = "forbidden_command"
				}
				enc := json.NewEncoder(conn)
				if enc.Encode(map[string]any{"type": "session_event", "id": req.ID}) != nil {
					break
				}
				if enc.Encode(map[string]any{"type": "response", "id": "unrelated", "success": false, "error": "ignore"}) != nil {
					break
				}
				if enc.Encode(map[string]any{"type": "response", "id": req.ID, "command": req.Type, "success": failure == "", "error": failure, "data": data}) != nil {
					break
				}
			}
			conn.Close()
			f.mu.Lock()
			f.conn = nil
			f.mu.Unlock()
		}
	}()
	t.Cleanup(func() {
		ln.Close()
		f.mu.Lock()
		if f.conn != nil {
			f.conn.Close()
		}
		f.mu.Unlock()
		select {
		case <-f.done:
		case <-time.After(5 * time.Second):
			t.Error("fake RPC did not stop")
		}
	})
	return f
}

func shortSocket(t *testing.T) string {
	t.Helper()
	dir, err := os.MkdirTemp("/tmp", "rpcw")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := os.RemoveAll(dir); err != nil {
			t.Error(err)
		}
	})
	return filepath.Join(dir, "s")
}

func rpcCtx(t *testing.T, out *bytes.Buffer, threads string) *core.Ctx {
	t.Helper()
	state := t.TempDir()
	if threads != "" {
		if err := os.WriteFile(filepath.Join(state, "threads.json"), []byte(threads), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	return &core.Ctx{State: state, Profile: core.Profile{Name: "main"}, Flags: map[string]bool{}, Out: core.NewOut(out)}
}

func session(handle, id string) map[string]any {
	return map[string]any{"sessionId": handle, "durableSessionId": id}
}

func state(status string, count int) map[string]any {
	s := map[string]any{"messageCount": count}
	switch status {
	case "working":
		s["isStreaming"] = true
	case "compacting":
		s["isCompacting"] = true
	case "blocked":
		s["pendingQuestions"] = []any{map[string]any{"questions": []any{map[string]any{"question": "Which?"}}}}
	}
	return s
}

func runTicks(t *testing.T, c *core.Ctx, n int) {
	t.Helper()
	old := sleepFn
	defer func() { sleepFn = old }()
	calls := 0
	sleepFn = func(ctx context.Context, d time.Duration) error {
		if d != 5*time.Second {
			t.Errorf("interval = %v", d)
		}
		calls++
		if calls >= n {
			return context.Canceled
		}
		return nil
	}
	if err := newWatcher(c, c.Out).run(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func events(t *testing.T, b *bytes.Buffer) []map[string]any {
	t.Helper()
	var result []map[string]any
	for _, line := range strings.Split(strings.TrimSpace(b.String()), "\n") {
		if !strings.HasPrefix(line, "RPC ") {
			continue
		}
		var m map[string]any
		if err := json.Unmarshal([]byte(strings.TrimPrefix(line, "RPC ")), &m); err != nil {
			t.Fatal(err)
		}
		result = append(result, m)
	}
	return result
}

func TestTransitions(t *testing.T) {
	cases := []struct {
		name   string
		states []string
		counts []int
		want   []string
		from   []any
	}{
		{"first blocked", []string{"blocked"}, []int{0}, []string{"blocked"}, []any{nil}},
		{"first idle", []string{"idle"}, []int{4}, nil, nil},
		{"first working", []string{"working"}, []int{4}, nil, nil},
		{"working idle", []string{"working", "idle"}, []int{0, 0}, []string{"done"}, []any{"working"}},
		{"working blocked idle", []string{"working", "blocked", "idle"}, []int{0, 0, 0}, []string{"blocked", "done"}, []any{"working", "blocked"}},
		{"idle growth", []string{"idle", "idle"}, []int{1, 2}, []string{"done"}, []any{"idle"}},
		{"blocked growth", []string{"blocked", "idle"}, []int{1, 2}, []string{"blocked", "done"}, []any{nil, "blocked"}},
		{"blocked no run", []string{"blocked", "idle"}, []int{1, 1}, []string{"blocked"}, []any{nil}},
		{"quiet transitions", []string{"idle", "working", "blocked", "working"}, []int{0, 0, 0, 0}, []string{"blocked"}, []any{"working"}},
		{"done clears run", []string{"working", "idle", "idle"}, []int{0, 0, 0}, []string{"done"}, []any{"working"}},
		{"compacting", []string{"compacting", "idle"}, []int{0, 0}, []string{"done"}, []any{"working"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// Given a scripted sequence on a real NDJSON unix socket.
			var ticks []scriptTick
			for i, st := range tc.states {
				ticks = append(ticks, scriptTick{sessions: []map[string]any{session("rpc-1", "durable")}, states: map[string]any{"rpc-1": state(st, tc.counts[i])}})
			}
			path := shortSocket(t)
			serveRPC(t, path, ticks)
			t.Setenv("OMOSENSE_RPC_SOCK", path)
			var b bytes.Buffer
			c := rpcCtx(t, &b, `{"job":{"session_id":"durable"}}`)
			// When the loop observes each scripted tick.
			runTicks(t, c, len(ticks))
			// Then only the specified semantic events are emitted.
			got := events(t, &b)
			if len(got) != len(tc.want) {
				t.Fatalf("events = %v, want %v; stdout %s", got, tc.want, b.String())
			}
			for i, e := range got {
				to := "idle"
				q := []any{}
				if tc.want[i] == "blocked" {
					to = "blocked"
					q = []any{"Which?"}
				}
				want := map[string]any{"event": tc.want[i], "session": "rpc-1", "id": "durable", "name": nil, "cwd": nil, "thread": "job", "from": tc.from[i], "to": to, "questions": q}
				if !reflect.DeepEqual(e, want) {
					t.Fatalf("payload = %#v, want %#v", e, want)
				}
			}
			if strings.Count(b.String(), "LOG rpc ready (1 sessions, 1 watched)") != 1 {
				t.Fatalf("ready not exactly once: %s", b.String())
			}
		})
	}
}

func TestOpenedClosedAndDurableIdentity(t *testing.T) {
	path := shortSocket(t)
	serveRPC(t, path, []scriptTick{
		{sessions: []map[string]any{session("rpc-1", "a")}, states: map[string]any{"rpc-1": state("working", 0)}},
		{sessions: []map[string]any{session("rpc-7", "a"), session("rpc-2", "b")}, states: map[string]any{"rpc-7": state("idle", 0), "rpc-2": state("idle", 0)}},
		{sessions: []map[string]any{session("rpc-2", "b")}, states: map[string]any{"rpc-2": state("idle", 0)}},
	})
	t.Setenv("OMOSENSE_RPC_SOCK", path)
	var b bytes.Buffer
	c := rpcCtx(t, &b, "")
	c.Flags["--all"] = true
	runTicks(t, c, 3)
	got := events(t, &b)
	if len(got) != 3 || got[0]["event"] != "done" || got[0]["session"] != "rpc-7" || got[1]["event"] != "opened" || got[1]["to"] != "idle" || got[1]["from"] != nil || got[2]["event"] != "closed" || got[2]["id"] != "a" || got[2]["from"] != "idle" {
		t.Fatalf("events = %v", got)
	}
	if strings.Contains(b.String(), "unknown_session") {
		t.Fatal(b.String())
	}
}

func TestUnobservedSessionEvents(t *testing.T) {
	for _, tc := range []struct {
		name     string
		baseline bool
		recovers bool
	}{
		{"new recovers", false, true},
		{"new disappears", false, false},
		{"baseline recovers", true, true},
		{"baseline disappears", true, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// Given a listed session whose state cannot yet be observed.
			failed := scriptTick{sessions: []map[string]any{session("rpc-1", "d")}, states: map[string]any{"rpc-1": "unknown_session"}}
			ticks := []scriptTick{failed}
			if !tc.baseline {
				ticks = append([]scriptTick{{}}, ticks...)
			}
			last := scriptTick{}
			if tc.recovers {
				last = scriptTick{sessions: failed.sessions, states: map[string]any{"rpc-1": state("working", 0)}}
			}
			ticks = append(ticks, last)
			path := shortSocket(t)
			serveRPC(t, path, ticks)
			t.Setenv("OMOSENSE_RPC_SOCK", path)
			var b bytes.Buffer
			c := rpcCtx(t, &b, "")
			c.Flags["--all"] = true
			w := newWatcher(c, c.Out)
			// When the failed observation is followed by recovery or disappearance.
			for i := range ticks {
				w.tick(context.Background())
				got := events(t, &b)
				want := 0
				if i == len(ticks)-1 && tc.recovers && !tc.baseline {
					want = 1
				}
				// Then only a successful observation of a new session opens it.
				if len(got) != want {
					t.Fatalf("tick %d events = %v, want %d", i, got, want)
				}
				if want == 1 && (got[0]["event"] != "opened" || got[0]["to"] != "working" || got[0]["from"] != nil) {
					t.Fatalf("opened = %v", got)
				}
			}
		})
	}
}

func TestFailedObservationPreservesRecord(t *testing.T) {
	// Given a successful working observation followed by a changed handle and error.
	path := shortSocket(t)
	serveRPC(t, path, []scriptTick{
		{sessions: []map[string]any{session("rpc-1", "d")}, states: map[string]any{"rpc-1": state("working", 3)}},
		{sessions: []map[string]any{session("rpc-7", "d")}, states: map[string]any{"rpc-7": "unknown_session"}},
		{},
	})
	t.Setenv("OMOSENSE_RPC_SOCK", path)
	var b bytes.Buffer
	c := rpcCtx(t, &b, "")
	c.Flags["--all"] = true
	w := newWatcher(c, c.Out)
	w.tick(context.Background())
	prev := w.seen["d"]
	// When get_state fails and the session then disappears.
	w.tick(context.Background())
	if !reflect.DeepEqual(w.seen["d"], prev) || len(events(t, &b)) != 0 {
		t.Fatalf("failed observation changed record: %#v, was %#v; %s", w.seen["d"], prev, b.String())
	}
	w.tick(context.Background())
	// Then closed uses the last successfully observed metadata and state.
	got := events(t, &b)
	if len(got) != 1 || got[0]["event"] != "closed" || got[0]["session"] != "rpc-1" || got[0]["from"] != "working" {
		t.Fatalf("closed = %v", got)
	}
}

func TestTransportFailureSkipsTick(t *testing.T) {
	// Given an observed session and two subsequent disconnects listing a different one.
	path := shortSocket(t)
	serveRPC(t, path, []scriptTick{
		{sessions: []map[string]any{session("rpc-1", "a")}, states: map[string]any{"rpc-1": state("working", 0)}},
		{sessions: []map[string]any{session("rpc-2", "b")}, drop: true},
		{sessions: []map[string]any{session("rpc-2", "b")}, drop: true},
		{sessions: []map[string]any{session("rpc-1", "a")}, states: map[string]any{"rpc-1": state("idle", 0)}},
	})
	t.Setenv("OMOSENSE_RPC_SOCK", path)
	var b bytes.Buffer
	c := rpcCtx(t, &b, "")
	c.Flags["--all"] = true
	// When the watcher disconnects and recovers.
	runTicks(t, c, 4)
	// Then the failed ticks neither close nor open anything, and the run survives.
	got := events(t, &b)
	if len(got) != 1 || got[0]["event"] != "done" || got[0]["id"] != "a" || got[0]["from"] != "working" {
		t.Fatalf("transport changed baseline: %s", b.String())
	}
	if strings.Count(b.String(), "EOF") != 1 {
		t.Fatalf("transport error not deduped: %s", b.String())
	}
}

func TestMalformedStateSkipsOnlySession(t *testing.T) {
	// Given two observed sessions, one with malformed state on the next tick.
	path := shortSocket(t)
	rows := []map[string]any{session("rpc-1", "a"), session("rpc-2", "b")}
	serveRPC(t, path, []scriptTick{
		{sessions: rows, states: map[string]any{"rpc-1": state("working", 0), "rpc-2": state("working", 0)}},
		{sessions: rows, states: map[string]any{"rpc-1": []any{}, "rpc-2": state("idle", 0)}},
	})
	t.Setenv("OMOSENSE_RPC_SOCK", path)
	var b bytes.Buffer
	c := rpcCtx(t, &b, "")
	c.Flags["--all"] = true
	// When the watcher receives malformed state alongside a successful observation.
	runTicks(t, c, 2)
	// Then the healthy session still completes; the malformed one is skipped.
	got := events(t, &b)
	if len(got) != 1 || got[0]["event"] != "done" || got[0]["id"] != "b" || !strings.Contains(b.String(), "LOG rpc a json:") {
		t.Fatalf("malformed state affected sibling: %s", b.String())
	}
}

func TestThreadFiltersAndWatchAll(t *testing.T) {
	for _, tc := range []struct {
		name, threads, config string
		all                   bool
		keys                  []any
	}{
		{"object", `{"a":{"session_id":"d1"},"b":{"session":"rpc-2"}}`, `{}`, false, []any{"a", "b"}},
		{"array", `[{"session":"d1"},{"session_id":"rpc-2"}]`, `{}`, false, []any{"0", "1"}},
		{"missing", "", `{}`, false, nil},
		{"malformed", `broken`, `{}`, false, nil},
		{"non true", "", `{"rpc":{"all":"true"}}`, false, nil},
		{"flag", `{"a":{"session":"d1"}}`, `{}`, true, []any{"a", nil, nil}},
		{"config", "", `{"rpc":{"all":true}}`, false, []any{nil, nil, nil}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			path := shortSocket(t)
			f := serveRPC(t, path, []scriptTick{{sessions: []map[string]any{session("rpc-1", "d1"), session("rpc-2", "d2"), session("rpc-3", "")}, states: map[string]any{"rpc-1": state("blocked", 0), "rpc-2": state("blocked", 0), "rpc-3": state("blocked", 0)}}})
			t.Setenv("OMOSENSE_RPC_SOCK", path)
			var b bytes.Buffer
			c := rpcCtx(t, &b, tc.threads)
			c.Flags["--all"] = tc.all
			v, err := core.ParseJSON([]byte(tc.config))
			if err != nil {
				t.Fatal(err)
			}
			c.Cfg = &core.Cfg{Raw: v.(*core.OMap)}
			runTicks(t, c, 1)
			got := events(t, &b)
			if len(got) != len(tc.keys) {
				t.Fatalf("events = %v, want %d", got, len(tc.keys))
			}
			for i, e := range got {
				if e["thread"] != tc.keys[i] {
					t.Fatalf("thread = %v, want %v", e["thread"], tc.keys[i])
				}
				if i == 2 && e["id"] != "rpc-3" {
					t.Fatalf("missing durable fallback: %v", e)
				}
			}
			f.mu.Lock()
			calls := append([]string(nil), f.calls...)
			f.mu.Unlock()
			if len(calls) != 1+len(tc.keys) {
				t.Fatalf("calls = %v", calls)
			}
			for _, cmd := range calls {
				if cmd != "list_sessions " && !strings.HasPrefix(cmd, "get_state rpc-") {
					t.Fatalf("non-readonly command %s", cmd)
				}
			}
		})
	}
}

func TestErrorsAndRecovery(t *testing.T) {
	path := shortSocket(t)
	t.Setenv("OMOSENSE_RPC_SOCK", path)
	var b bytes.Buffer
	c := rpcCtx(t, &b, "")
	c.Flags["--all"] = true
	w := newWatcher(c, c.Out)
	w.tick(context.Background())
	w.tick(context.Background())
	if strings.Count(b.String(), "LOG rpc ") != 1 {
		t.Fatalf("missing socket dedupe: %s", b.String())
	}
	serveRPC(t, path, []scriptTick{
		{sessions: []map[string]any{session("rpc-1", "d")}, states: map[string]any{"rpc-1": state("working", 0)}},
		{err: "list_failed"},
		{err: "list_failed"},
		{sessions: []map[string]any{session("rpc-1", "d")}, states: map[string]any{"rpc-1": "state_failed"}},
		{sessions: []map[string]any{session("rpc-1", "d")}, states: map[string]any{"rpc-1": "state_failed"}},
		{sessions: []map[string]any{session("rpc-1", "d")}, states: map[string]any{"rpc-1": state("idle", 0)}},
	})
	for i := 0; i < 6; i++ {
		w.tick(context.Background())
	}
	got := events(t, &b)
	if len(got) != 1 || got[0]["event"] != "done" {
		t.Fatalf("failed ticks changed baseline: %s", b.String())
	}
	if strings.Count(b.String(), "LOG rpc list_failed") != 1 || strings.Count(b.String(), "state_failed") != 1 || strings.Count(b.String(), "LOG rpc ready (1 sessions, 1 watched)") != 2 {
		t.Fatalf("error/ready counts: %s", b.String())
	}
}

func TestThreadsRereadLateObservation(t *testing.T) {
	for _, tc := range []struct {
		name, status string
		emptyFirst   bool
	}{
		{"baseline blocked", "blocked", false},
		{"listed after baseline idle", "idle", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// Given an unwatched session listed before thread registration.
			listed := scriptTick{sessions: []map[string]any{session("rpc-1", "d")}, states: map[string]any{"rpc-1": state(tc.status, 0)}}
			ticks := []scriptTick{listed, listed}
			if tc.emptyFirst {
				ticks = append([]scriptTick{{}}, ticks...)
			}
			path := shortSocket(t)
			serveRPC(t, path, ticks)
			t.Setenv("OMOSENSE_RPC_SOCK", path)
			var b bytes.Buffer
			c := rpcCtx(t, &b, "")
			old := sleepFn
			defer func() { sleepFn = old }()
			n := 0
			sleepFn = func(context.Context, time.Duration) error {
				n++
				if n == len(ticks) {
					return context.Canceled
				}
				if n == len(ticks)-1 {
					if err := os.WriteFile(filepath.Join(c.State, "threads.json"), []byte(`{"later":{"session":"d"}}`), 0o600); err != nil {
						t.Fatal(err)
					}
				}
				return nil
			}
			// When the source rereads threads.json on the next successful list.
			if err := Sources(c)[0].Run(context.Background(), c.Out); err != nil {
				t.Fatal(err)
			}
			// Then first observation can block but never falsely opens it.
			got := events(t, &b)
			if tc.status == "blocked" {
				if len(got) != 1 || got[0]["event"] != "blocked" || got[0]["from"] != nil || got[0]["thread"] != "later" {
					t.Fatalf("late observation = %v", got)
				}
			} else if len(got) != 0 {
				t.Fatalf("late registration of previously listed session emitted events: %v; stdout %s", got, b.String())
			}
			t.Logf("Source.Run late registration: %s", b.String())
			t.Log("cleanup: test Cleanup closes and joins fake RPC, removes temporary socket/state, and restores environment and sleepFn")
		})
	}
}

func TestQuestionsAndMetadata(t *testing.T) {
	path := shortSocket(t)
	s := state("blocked", 0)
	s["isStreaming"] = true
	s["pendingQuestions"] = []any{map[string]any{"questions": []any{map[string]any{"question": strings.Repeat("한", 201)}, map[string]any{"header": "Fallback"}}}}
	row := session("rpc-1", "d")
	row["name"], row["cwd"] = "named", "/work"
	serveRPC(t, path, []scriptTick{{sessions: []map[string]any{row}, states: map[string]any{"rpc-1": s}}})
	t.Setenv("OMOSENSE_RPC_SOCK", path)
	var b bytes.Buffer
	c := rpcCtx(t, &b, "")
	c.Flags["--all"] = true
	runTicks(t, c, 1)
	got := events(t, &b)
	if len(got) != 1 || got[0]["name"] != "named" || got[0]["cwd"] != "/work" || !reflect.DeepEqual(got[0]["questions"], []any{strings.Repeat("한", 200), "Fallback"}) {
		t.Fatalf("payload = %v", got)
	}
}

func TestOpenedBlockedHasQuestionsOnlyOnBlockedEvent(t *testing.T) {
	path := shortSocket(t)
	serveRPC(t, path, []scriptTick{
		{},
		{sessions: []map[string]any{session("rpc-1", "d")}, states: map[string]any{"rpc-1": state("blocked", 0)}},
	})
	t.Setenv("OMOSENSE_RPC_SOCK", path)
	var b bytes.Buffer
	c := rpcCtx(t, &b, "")
	c.Flags["--all"] = true
	runTicks(t, c, 2)
	got := events(t, &b)
	if len(got) != 2 || got[0]["event"] != "blocked" || got[1]["event"] != "opened" || got[1]["to"] != "blocked" || !reflect.DeepEqual(got[1]["questions"], []any{}) {
		t.Fatalf("new blocked session events = %v", got)
	}
}

func TestSurfaceSnapshotAndSource(t *testing.T) {
	path := shortSocket(t)
	serveRPC(t, path, []scriptTick{
		{sessions: []map[string]any{session("rpc-1", "d")}, states: map[string]any{"rpc-1": state("blocked", 0)}},
		{sessions: []map[string]any{session("rpc-1", "d")}, states: map[string]any{"rpc-1": state("blocked", 0)}},
		{sessions: []map[string]any{session("rpc-1", "d")}, states: map[string]any{"rpc-1": state("working", 0)}},
		{sessions: []map[string]any{session("rpc-1", "d")}, states: map[string]any{"rpc-1": state("idle", 0)}},
		{},
	})
	t.Setenv("OMOSENSE_RPC_SOCK", path)
	var b bytes.Buffer
	c := rpcCtx(t, &b, "")
	c.State = filepath.Join(t.TempDir(), "absent")
	c.Flags["--once"], c.Flags["--all"] = true, true
	if code := Run(c, []string{"--once", "--all"}); code != 0 || b.String() != "SNAP d blocked - -\n" {
		t.Fatalf("snapshot code/stdout: %d %q", code, b.String())
	}
	if _, err := os.Stat(c.State); !os.IsNotExist(err) {
		t.Fatalf("snapshot created state: %v", err)
	}
	t.Logf("compat Run(--once,--all): %s", b.String())
	b.Reset()
	sources := Sources(c)
	if len(sources) != 1 {
		t.Fatalf("sources = %v", sources)
	}
	src := sources[0]
	lock, legacy := src.LockName()
	if src.Name() != "rpc" || !reflect.DeepEqual(src.Prefixes(), []string{"RPC"}) || src.AlwaysOn() || lock != "watch-rpc-main" || legacy != "" {
		t.Fatalf("source contract: %v", src)
	}
	old := sleepFn
	defer func() { sleepFn = old }()
	n := 0
	sleepFn = func(context.Context, time.Duration) error {
		n++
		if n == 4 {
			return context.Canceled
		}
		return nil
	}
	if err := src.Run(context.Background(), c.Out); err != nil {
		t.Fatal(err)
	}
	got := events(t, &b)
	if len(got) != 3 || got[0]["event"] != "blocked" || got[1]["event"] != "done" || got[2]["event"] != "closed" {
		t.Fatalf("source output %s", b.String())
	}
	if !strings.HasPrefix(b.String(), fmt.Sprintf("LOG rpc watcher starting (profile main, every 5s, watch all, sock %s)\n", path)) {
		t.Fatal(b.String())
	}
	t.Logf("Source.Run: %s", b.String())
	t.Log("cleanup: test Cleanup closes listener/connections, joins server and removes short socket directory")
}

func TestOnceMissingSocket(t *testing.T) {
	t.Setenv("OMOSENSE_RPC_SOCK", shortSocket(t))
	var b bytes.Buffer
	c := rpcCtx(t, &b, "")
	c.Flags["--once"] = true
	if code := Run(c, nil); code != 1 || !strings.HasPrefix(b.String(), "LOG rpc ") {
		t.Fatalf("code/stdout = %d %q", code, b.String())
	}
}

func TestOnceGetStateErrors(t *testing.T) {
	for _, tc := range []struct {
		name string
		tick scriptTick
		code int
		log  string
	}{
		{"disconnect", scriptTick{drop: true}, 1, "EOF"},
		{"command failure", scriptTick{states: map[string]any{"rpc-1": "state_failed"}}, 1, "state_failed"},
		{"malformed state", scriptTick{states: map[string]any{"rpc-1": []any{}}}, 1, "cannot unmarshal"},
		{"unknown session", scriptTick{states: map[string]any{"rpc-1": "unknown_session"}}, 0, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// Given a successful list followed by a get_state failure.
			tc.tick.sessions = []map[string]any{session("rpc-1", "d")}
			path := shortSocket(t)
			serveRPC(t, path, []scriptTick{tc.tick})
			t.Setenv("OMOSENSE_RPC_SOCK", path)
			var b bytes.Buffer
			c := rpcCtx(t, &b, "")
			c.Flags["--once"], c.Flags["--all"] = true, true
			// When the compat snapshot runs against the real socket.
			code := Run(c, []string{"--once", "--all"})
			// Then only unknown_session is a successful skip.
			if code != tc.code || strings.Contains(b.String(), "SNAP ") {
				t.Fatalf("code/stdout = %d %q, want %d", code, b.String(), tc.code)
			}
			if tc.log != "" && (!strings.HasPrefix(b.String(), "LOG rpc ") || !strings.Contains(b.String(), tc.log)) {
				t.Fatalf("missing error log: %q", b.String())
			}
			if tc.code == 0 && b.Len() != 0 {
				t.Fatalf("unknown_session logged: %q", b.String())
			}
			t.Logf("Run(--once,--all): exit=%d stdout=%q", code, b.String())
		})
	}
}
