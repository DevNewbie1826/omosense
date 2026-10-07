package rpc

import (
	"bytes"
	"context"
	"testing"
	"time"
)

// TestSilentSessionSteadyDecreaseIsReported guards IS-6's other half: a
// decrease is not growth, so a working session whose messageCount falls on
// every poll has no output growth and is reported silent once its window
// passes, exactly like a frozen session. A decrease case that runs before the
// silence check would keep such a session from ever being reported.
func TestSilentSessionSteadyDecreaseIsReported(t *testing.T) {
	var b bytes.Buffer
	c := rpcCtx(t, &b, `{"shrink":{"session_id":"s"},"frozen":{"session_id":"c"}}`)
	c.Profile.SilentMinutes = 30
	clock := time.Date(2026, 10, 5, 0, 0, 0, 0, time.UTC)
	old := nowFn
	nowFn = func() time.Time { return clock }
	t.Cleanup(func() { nowFn = old })

	counts := []int{100, 99, 98, 97, 96, 95, 94, 93, 92, 91}
	var ticks []scriptTick
	for _, n := range counts {
		ticks = append(ticks, scriptTick{
			sessions: []map[string]any{session("rpc-s", "s"), session("rpc-c", "c")},
			states: map[string]any{
				"rpc-s": state("working", n),
				"rpc-c": state("working", 3),
			},
		})
	}
	path := shortSocket(t)
	serveRPC(t, path, ticks)
	t.Setenv("OMOSENSE_RPC_SOCK", path)
	w := newWatcher(c, c.Out)

	for i, n := range counts {
		clock = clock.Add(5 * time.Minute)
		w.tick(context.Background())
		lines := logLines(&b, "silent-session")
		want := 0
		if i >= 6 {
			want = 1
		}
		if got := countLines(lines, `"id":"s"`); got != want {
			t.Fatalf("tick %d (count %d): shrinking session silent lines = %d, want %d: %v", i, n, got, want, lines)
		}
		if got := countLines(lines, `"id":"c"`); got != want {
			t.Fatalf("tick %d: frozen control silent lines = %d, want %d: %v", i, got, want, lines)
		}
	}
}
