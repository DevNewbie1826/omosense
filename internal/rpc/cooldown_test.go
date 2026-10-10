package rpc

import (
	"bytes"
	"context"
	"testing"
	"time"
)

// The rpc blocked re-emission cooldown (rpc.blockedCooldownSec, default 0 =
// off), the same rule as herdr's: after an emitted blocked, a re-entry into
// blocked inside the window prints nothing; the window is measured from the
// last EMITTED line; keys are session ids; a session that leaves the list
// loses its cooldown. Ticks run against a fake clock: nothing waits real time.

// cooldownStep is one tick: the clock advance and each listed session's status.
type cooldownStep struct {
	after    time.Duration
	statuses map[string]string // durable id -> status; absent = not listed
}

// runCooldown drives steps through a watcher with the given cooldown (nil =
// unset) and returns, per step, the durable ids that printed a blocked line.
func runCooldown(t *testing.T, sec *int, steps []cooldownStep) [][]string {
	t.Helper()
	var b bytes.Buffer
	c := rpcCtx(t, &b, "")
	c.Profile.RPC.All = true
	c.Profile.RPC.BlockedCooldownSec = sec
	clock := time.Date(2026, 10, 5, 0, 0, 0, 0, time.UTC)
	old := nowFn
	nowFn = func() time.Time { return clock }
	t.Cleanup(func() { nowFn = old })

	var ticks []scriptTick
	for _, s := range steps {
		tk := scriptTick{states: map[string]any{}}
		for _, id := range []string{"a", "b"} {
			if st, ok := s.statuses[id]; ok {
				tk.sessions = append(tk.sessions, session("rpc-"+id, id))
				tk.states["rpc-"+id] = state(st, 0)
			}
		}
		ticks = append(ticks, tk)
	}
	path := shortSocket(t)
	serveRPC(t, path, ticks)
	t.Setenv("OMOSENSE_RPC_SOCK", path)
	w := newWatcher(c, c.Out)

	var out [][]string
	for _, s := range steps {
		clock = clock.Add(s.after)
		b.Reset()
		w.tick(context.Background())
		var ids []string
		for _, ev := range events(t, &b) {
			if ev["event"] == "blocked" {
				ids = append(ids, ev["id"].(string))
			}
		}
		out = append(out, ids)
	}
	return out
}

func wantBlocked(t *testing.T, got [][]string, want ...[]string) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("got %d steps, want %d", len(got), len(want))
	}
	for i := range want {
		if len(got[i]) != len(want[i]) {
			t.Fatalf("step %d: blocked lines %v, want %v (all steps %v)", i, got[i], want[i], got)
		}
		for j := range want[i] {
			if got[i][j] != want[i][j] {
				t.Fatalf("step %d: blocked lines %v, want %v (all steps %v)", i, got[i], want[i], got)
			}
		}
	}
}

func secs(n int) *int { return &n }

func st(kv ...string) map[string]string {
	m := map[string]string{}
	for i := 0; i < len(kv); i += 2 {
		m[kv[i]] = kv[i+1]
	}
	return m
}

// TestRPCBlockedCooldownDefaultOff: unset and 0 both print every re-entry.
func TestRPCBlockedCooldownDefaultOff(t *testing.T) {
	steps := []cooldownStep{
		{0, st("a", "blocked")},
		{5 * time.Second, st("a", "working")},
		{5 * time.Second, st("a", "blocked")},
		{5 * time.Second, st("a", "working")},
		{5 * time.Second, st("a", "blocked")},
	}
	for _, sec := range []*int{nil, secs(0)} {
		wantBlocked(t, runCooldown(t, sec, steps), []string{"a"}, nil, []string{"a"}, nil, []string{"a"})
	}
}

// TestRPCBlockedCooldownSuppressAndBoundary: a re-entry 15s after the emit is
// suppressed and does not extend the window; 59s still suppresses, 60s emits.
func TestRPCBlockedCooldownSuppressAndBoundary(t *testing.T) {
	got := runCooldown(t, secs(60), []cooldownStep{
		{0, st("a", "blocked")},
		{10 * time.Second, st("a", "working")},
		{5 * time.Second, st("a", "blocked")}, // t+15 suppressed
		{40 * time.Second, st("a", "working")},
		{4 * time.Second, st("a", "blocked")},        // t+59 suppressed
		{500 * time.Millisecond, st("a", "working")}, // t+59.5
		{500 * time.Millisecond, st("a", "blocked")}, // t+60 emits
		{5 * time.Second, st("a", "working")},        // t+65
		{5 * time.Second, st("a", "blocked")},        // t+70: 10s after the new emit
	})
	wantBlocked(t, got, []string{"a"}, nil, nil, nil, nil, nil, []string{"a"}, nil, nil)
}

// TestRPCBlockedCooldownVanishedAndIndependent: another session keeps its own
// window, and a session that left the list returns with a fresh one.
func TestRPCBlockedCooldownVanishedAndIndependent(t *testing.T) {
	got := runCooldown(t, secs(60), []cooldownStep{
		{0, st("a", "blocked", "b", "working")},
		{5 * time.Second, st("a", "working", "b", "blocked")}, // b: first blocked
		{5 * time.Second, st("a", "blocked", "b", "working")}, // a suppressed
		{5 * time.Second, st("b", "working")},                 // a leaves the list
		{5 * time.Second, st("a", "blocked", "b", "blocked")}, // a fresh, b suppressed
	})
	wantBlocked(t, got, []string{"a"}, []string{"b"}, nil, nil, []string{"a"})
}
