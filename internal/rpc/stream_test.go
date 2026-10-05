package rpc

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type streamHarness struct {
	w        *watcher
	out      *bytes.Buffer
	recorded []rpcEvent
}

func newStreamHarness(t *testing.T, ticks ...scriptTick) *streamHarness {
	t.Helper()
	path := shortSocket(t)
	serveRPC(t, path, ticks)
	t.Setenv("OMOSENSE_RPC_SOCK", path)
	b := &bytes.Buffer{}
	c := rpcCtx(t, b, "")
	c.Flags["--all"] = true
	h := &streamHarness{w: newWatcher(c, c.Out), out: b}
	h.w.record = func(id string, ev rpcEvent) {
		if id == "" || id != ev.ID {
			t.Errorf("invalid durable record %q: %+v", id, ev)
		}
		h.recorded = append(h.recorded, ev)
	}
	t.Cleanup(func() {
		t.Logf("RPC stdout:\n%srecord hook: %+v", b.String(), h.recorded)
		t.Log("cleanup: joined fake server; removed socket/state directories; restored stream dial and environment")
	})
	return h
}

func tickFor(handle, durable, status string) scriptTick {
	return scriptTick{sessions: []map[string]any{session(handle, durable)}, states: map[string]any{handle: state(status, 0)}}
}

func (h *streamHarness) frame(kind, handle string) {
	h.w.applyStream(context.Background(), streamItem{frame: streamFrame{Type: kind, Session: handle}, lookup: "l1"})
}

func (h *streamHarness) list(id string, rows ...map[string]any) {
	b, err := json.Marshal(map[string]any{"sessions": rows})
	if err != nil {
		panic(err)
	}
	h.w.applyStream(context.Background(), streamItem{frame: streamFrame{Type: "response", ID: id, Success: true, Data: b}})
}

func (h *streamHarness) connected() {
	h.w.streamUp, h.w.streamEpoch = true, 1
}

func (h *streamHarness) done(t *testing.T, ids ...string) {
	t.Helper()
	var got []string
	for _, ev := range events(t, h.out) {
		if ev["event"] == "done" {
			got = append(got, ev["id"].(string))
		}
	}
	if !reflect.DeepEqual(got, ids) {
		t.Fatalf("done ids = %v, want %v; stdout %s", got, ids, h.out.String())
	}
}

func TestStreamShortTurn(t *testing.T) {
	// Given an idle baseline and no polls during the complete turn.
	h := newStreamHarness(t, tickFor("H", "D7", "idle"))
	h.w.tick(context.Background())
	h.connected()
	// When start and settle arrive between ticks.
	h.frame("agent_start", "H")
	h.list("l1", session("H", "D7"))
	h.frame("agent_settled", "H")
	// Then both the RPC line and record hook report the durable job once.
	h.done(t, "D7")
	if len(h.recorded) != 1 || h.recorded[0].ID != "D7" {
		t.Fatalf("record hook = %+v", h.recorded)
	}
}

func TestStreamPollExactlyOnce(t *testing.T) {
	h := newStreamHarness(t, tickFor("H", "D7", "idle"))
	h.w.tick(context.Background())
	h.connected()
	h.frame("agent_start", "H")
	h.list("l1", session("H", "D7"))
	// Idle polls before and after settle cannot conclude a current stream turn.
	h.w.tick(context.Background())
	h.done(t)
	h.frame("agent_settled", "H")
	h.w.tick(context.Background())
	h.done(t, "D7")
	if len(h.recorded) != 1 {
		t.Fatalf("record hook = %+v", h.recorded)
	}
}

func eventNames(t *testing.T, h *streamHarness) []string {
	t.Helper()
	var got []string
	for _, ev := range events(t, h.out) {
		got = append(got, fmt.Sprint(ev["event"], " ", ev["id"]))
	}
	return got
}

func TestStreamCloseTombstone(t *testing.T) {
	t.Run("listed", func(t *testing.T) {
		h := newStreamHarness(t, tickFor("H", "D7", "idle"), tickFor("H", "D7", "idle"), scriptTick{})
		h.w.tick(context.Background())
		h.connected()
		h.frame("session_closed", "H")
		h.frame("session_parked", "H")
		// Host lag repeats the ended handle; disappearance must not close it again.
		h.w.tick(context.Background())
		h.w.tick(context.Background())
		if got := eventNames(t, h); !reflect.DeepEqual(got, []string{"closed D7"}) {
			t.Fatalf("close events = %v", got)
		}
	})
	t.Run("never polled", func(t *testing.T) {
		h := newStreamHarness(t, scriptTick{}, tickFor("H", "D7", "idle"), scriptTick{})
		h.w.tick(context.Background())
		h.connected()
		h.frame("agent_start", "H")
		h.list("l1", session("H", "D7"))
		h.frame("agent_settled", "H")
		h.frame("session_closed", "H")
		// A lagging list of a session only the stream knew must not reopen it.
		h.w.tick(context.Background())
		h.w.tick(context.Background())
		if got := eventNames(t, h); !reflect.DeepEqual(got, []string{"done D7", "closed D7"}) {
			t.Fatalf("events = %v", got)
		}
	})
}

func TestStreamCloseRemovedTurn(t *testing.T) {
	h := newStreamHarness(t, tickFor("H", "D7", "idle"))
	h.w.tick(context.Background())
	h.connected()
	h.frame("agent_start", "H")
	h.list("l1", session("H", "D7"))
	h.list("s0") // the bound turn remains known despite a newer absent list
	h.frame("session_closed", "H")
	ev := events(t, h.out)
	if len(ev) != 1 || ev[0]["event"] != "closed" || ev[0]["id"] != "D7" {
		t.Fatalf("known removed turn closed events = %v", ev)
	}
}

func TestStreamOutageReconnect(t *testing.T) {
	h := newStreamHarness(t, tickFor("H", "D7", "working"), tickFor("H", "D7", "idle"))
	h.w.tick(context.Background())
	// Completion happened offline, then the connection precedes the next poll.
	h.w.applyStream(context.Background(), streamItem{up: true})
	h.done(t, "D7")
	h.frame("agent_settled", "H")
	h.done(t, "D7")
}

func TestStreamRemovedTurn(t *testing.T) {
	h := newStreamHarness(t, scriptTick{})
	h.w.tick(context.Background())
	h.connected()
	h.frame("agent_start", "H")
	h.list("l1", session("H", "D7"))
	h.list("s0")
	h.frame("agent_settled", "H")
	h.done(t, "D7")
}

func TestStreamReopenedHandle(t *testing.T) {
	h := newStreamHarness(t, tickFor("rpc-7", "D7", "idle"), tickFor("rpc-9", "D7", "working"), tickFor("rpc-9", "D7", "idle"))
	h.w.tick(context.Background())
	h.frame("session_closed", "rpc-7")
	// The old tombstone cannot silence the same durable id at a new handle.
	h.w.tick(context.Background())
	h.w.tick(context.Background())
	h.done(t, "D7")
}

func TestStreamReplacementFromUnwatched(t *testing.T) {
	h := newStreamHarness(t, tickFor("H", "A", "idle"), tickFor("H", "B", "idle"))
	h.w.all = false
	if err := os.WriteFile(filepath.Join(h.w.stateDir, "threads.json"), []byte(`{"job":{"session_id":"B"}}`), 0o600); err != nil {
		t.Fatal(err)
	}
	h.w.tick(context.Background())
	h.connected()
	h.frame("agent_start", "H")
	h.list("l1", session("H", "B"))
	h.frame("agent_settled", "H")
	h.w.tick(context.Background())
	// The unwatched pre-start identity leaves no phantom record to close.
	if got := eventNames(t, h); !reflect.DeepEqual(got, []string{"done B", "opened B"}) {
		t.Fatalf("events = %v", got)
	}
}

func TestStreamLookupLostOnDown(t *testing.T) {
	h := newStreamHarness(t, scriptTick{}, tickFor("H", "D9", "idle"))
	h.w.tick(context.Background())
	h.connected()
	h.frame("agent_start", "H")
	// Settle is held for the lookup, then the connection ends unanswered.
	h.frame("agent_settled", "H")
	h.w.applyStream(context.Background(), streamItem{err: io.EOF})
	h.done(t, "D9")
}

func TestStreamReconnectRebindsTurn(t *testing.T) {
	h := newStreamHarness(t, tickFor("H", "A", "idle"), tickFor("H", "B", "working"))
	h.w.tick(context.Background())
	h.connected()
	h.frame("agent_start", "H")
	h.list("l1", session("H", "A"))
	h.w.applyStream(context.Background(), streamItem{err: io.EOF})
	// A's settle was lost; B now runs behind H when the stream reconnects.
	h.w.applyStream(context.Background(), streamItem{up: true})
	h.frame("agent_settled", "H")
	h.done(t, "B")
}

func TestStreamDownDurableAssignment(t *testing.T) {
	h := newStreamHarness(t, tickFor("H", "", "idle"), tickFor("H", "D7", "idle"))
	h.w.tick(context.Background())
	h.w.tick(context.Background())
	// Stream down: a newly assigned durable id keeps today's poll events.
	if got := eventNames(t, h); !reflect.DeepEqual(got, []string{"opened D7", "closed H"}) {
		t.Fatalf("events = %v", got)
	}
}

func TestStreamReplacement(t *testing.T) {
	h := newStreamHarness(t, tickFor("H", "A", "idle"))
	h.w.tick(context.Background())
	h.connected()
	h.frame("agent_start", "H")
	h.list("l1", session("H", "B"))
	h.frame("agent_settled", "H")
	h.done(t, "B")
	if len(h.recorded) != 1 || h.recorded[0].ID != "B" {
		t.Fatalf("records = %+v", h.recorded)
	}
}

func TestStreamDeferredDurable(t *testing.T) {
	h := newStreamHarness(t, tickFor("H", "", "idle"))
	h.w.tick(context.Background())
	h.connected()
	h.frame("agent_start", "H")
	h.list("l1", session("H", ""))
	h.frame("agent_settled", "H")
	if len(h.recorded) != 0 {
		t.Fatalf("routing handle was recorded: %+v", h.recorded)
	}
	h.list("s0", session("H", "D7"))
	h.list("s0", session("H", "D7"))
	h.frame("agent_settled", "H")
	h.done(t, "H")
	if len(h.recorded) != 1 || h.recorded[0].ID != "D7" {
		t.Fatalf("late durable record = %+v", h.recorded)
	}
}

func TestStreamStaleWorking(t *testing.T) {
	var q *streamFIFO
	stale := tickFor("H", "D7", "working")
	stale.beforeState = func() {
		q.append(streamItem{frame: streamFrame{Type: "agent_settled", Session: "H"}})
		q.append(streamItem{err: io.EOF})
	}
	h := newStreamHarness(t, tickFor("H", "D7", "idle"), stale, tickFor("H", "D7", "idle"))
	h.w.tick(context.Background())
	h.connected()
	h.frame("agent_start", "H")
	h.list("l1", session("H", "D7"))
	q = newStreamFIFO()
	h.w.queue = q
	// The snapshot starts before settle, and is applied after settle+disconnect.
	h.w.tick(context.Background())
	h.w.tick(context.Background())
	h.done(t, "D7")
}

func TestStreamQueuedSettle(t *testing.T) {
	h := newStreamHarness(t, tickFor("H", "D7", "working"), tickFor("H", "D7", "idle"))
	h.w.tick(context.Background())
	h.w.streamEpoch, h.w.streamUp = 1, true
	// The poll concludes old-epoch activity before a queued settle is applied.
	h.w.tick(context.Background())
	h.frame("agent_settled", "H")
	h.frame("agent_settled", "H")
	h.done(t, "D7")
}

func TestStreamMidturnEarlySettle(t *testing.T) {
	h := newStreamHarness(t, tickFor("H", "D7", "idle"), scriptTick{err: "reconciliation unavailable"})
	h.w.tick(context.Background())
	h.w.applyStream(context.Background(), streamItem{up: true})
	// Settled precedes the first observe response; the dial epoch already exists.
	h.frame("agent_settled", "H")
	h.list("s0", session("H", "D7"))
	h.done(t, "D7")
}

func TestStreamCompaction(t *testing.T) {
	h := newStreamHarness(t, tickFor("H", "D7", "idle"), tickFor("H", "D7", "compacting"), tickFor("H", "D7", "idle"))
	h.w.tick(context.Background())
	h.connected()
	h.w.tick(context.Background())
	h.w.tick(context.Background())
	h.w.tick(context.Background())
	h.done(t, "D7")
}

func TestStreamStartIdentity(t *testing.T) {
	for _, replacement := range []bool{false, true} {
		t.Run(fmt.Sprint(replacement), func(t *testing.T) {
			h := newStreamHarness(t, tickFor("H", "A", "idle"))
			h.w.tick(context.Background())
			h.connected()
			h.frame("agent_start", "H")
			h.list("l1", session("H", "A"))
			if replacement {
				h.list("s0", session("H", "B"))
			} else {
				h.list("s0")
			}
			h.frame("agent_settled", "H")
			// A later replacement response cannot rewrite the concluded turn.
			h.list("l1", session("H", "B"))
			h.done(t, "A")
		})
	}
}

func TestStreamSettleBeforeLookup(t *testing.T) {
	h := newStreamHarness(t, tickFor("H", "A", "idle"))
	h.w.tick(context.Background())
	h.connected()
	h.frame("agent_start", "H")
	h.frame("agent_settled", "H")
	// The response is later in the stream than settle: it may show replacement B.
	h.list("l1", session("H", "B"))
	h.done(t, "A")
}

func TestStreamDeferredExpiryAndClose(t *testing.T) {
	for _, end := range []string{"expiry", "session_closed", "session_parked"} {
		t.Run(end, func(t *testing.T) {
			h := newStreamHarness(t, tickFor("H", "", "idle"))
			clock := time.Date(2026, 10, 5, 0, 0, 0, 0, time.UTC)
			old := nowFn
			nowFn = func() time.Time { return clock }
			defer func() { nowFn = old }()
			h.w.tick(context.Background())
			h.connected()
			h.frame("agent_start", "H")
			h.list("l1", session("H", ""))
			h.frame("agent_settled", "H")
			if end == "expiry" {
				clock = clock.Add(10 * time.Minute)
				h.w.expireDeferred()
			} else {
				h.frame(end, "H")
			}
			h.list("s0", session("H", "D7"))
			if len(h.recorded) != 0 || !strings.Contains(h.out.String(), "rpc done H has no durable id; not pending") {
				t.Fatalf("unresolved completion was recorded or not logged: %s", h.out.String())
			}
		})
	}
}

func TestStreamStaleListDeferral(t *testing.T) {
	h := newStreamHarness(t, tickFor("H", "", "idle"))
	h.w.tick(context.Background())
	h.connected()
	h.frame("agent_start", "H")
	h.list("l1", session("H", ""))
	h.frame("agent_settled", "H")
	h.list("s0") // stale absence must not discard the completion
	h.list("s0", session("H", "D7"))
	if len(h.recorded) != 1 || h.recorded[0].ID != "D7" {
		t.Fatalf("stale list erased completion: %+v", h.recorded)
	}
}

func TestStreamReconciliationIdentity(t *testing.T) {
	for _, outage := range []bool{false, true} {
		t.Run(fmt.Sprint(outage), func(t *testing.T) {
			h := newStreamHarness(t, tickFor("H", "A", "working"))
			if outage {
				// A poll armed the turn while the stream was down.
				h.w.tick(context.Background())
			}
			h.w.applyStream(context.Background(), streamItem{up: true})
			h.list("s0", session("H", "B"))
			h.frame("agent_settled", "H")
			h.done(t, "A")
		})
	}
}

func TestStreamReconciliationOnce(t *testing.T) {
	h := newStreamHarness(t, tickFor("H", "D7", "working"), tickFor("H", "D7", "idle"))
	h.w.applyStream(context.Background(), streamItem{up: true})
	h.w.tick(context.Background())
	h.done(t)
	h.frame("agent_settled", "H")
	h.w.tick(context.Background())
	h.done(t, "D7")
}

func TestStreamNestedReconciliation(t *testing.T) {
	q := newStreamFIFO()
	outer := tickFor("H", "D7", "working")
	// Server barrier: connect and disconnect are queued while the outer
	// poll's working snapshot is in flight, so its drain reconciles first.
	outer.beforeState = func() {
		q.append(streamItem{up: true})
		q.append(streamItem{err: io.EOF})
	}
	h := newStreamHarness(t, tickFor("H", "D7", "working"), outer, tickFor("H", "D7", "idle"))
	store := newPendingStore(h.w.stateDir, h.w.profile)
	record := h.w.record
	h.w.record = func(id string, ev rpcEvent) {
		record(id, ev)
		if _, err := store.Record(id, ev); err != nil {
			t.Error(err)
		}
	}
	h.w.queue = q
	// Given D7 armed by a poll while the stream is down.
	h.w.tick(context.Background())
	// When reconciliation concludes it inside the outer poll's drain, and
	// the next poll sees idle.
	h.w.tick(context.Background())
	h.w.tick(context.Background())
	// Then the older working snapshot cannot re-arm the completed turn.
	entries, err := store.List()
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 || entries[0].Seq != 1 || entries[0].Count != 1 {
		t.Errorf("pending entries = %+v, want D7 seq 1 count 1", entries)
	}
	h.done(t, "D7")
}

func TestStreamFlappingPollFallback(t *testing.T) {
	q := newStreamFIFO()
	connect := make(chan struct{})
	ctx, cancel := context.WithCancel(context.Background())
	// Server barrier: each flapping get_state admits one stream connection
	// and answers only once that connection's EOF reached the owner's FIFO.
	flap := func() {
		select {
		case connect <- struct{}{}:
		case <-ctx.Done():
			return
		}
		for {
			select {
			case <-q.wake:
				q.mu.Lock()
				n := len(q.items)
				down := n > 0 && q.items[n-1].err != nil
				q.mu.Unlock()
				if down {
					return
				}
			case <-time.After(5 * time.Second):
				t.Error("stream EOF was not queued")
				return
			}
		}
	}
	working, quiet := tickFor("H", "D7", "working"), tickFor("H", "D7", "idle")
	working.beforeState, quiet.beforeState = flap, flap
	h := newStreamHarness(t, working, tickFor("H", "D7", "idle"), quiet, tickFor("H", "D7", "idle"))
	store := newPendingStore(h.w.stateDir, h.w.profile)
	record := h.w.record
	h.w.record = func(id string, ev rpcEvent) {
		record(id, ev)
		if _, err := store.Record(id, ev); err != nil {
			t.Error(err)
		}
	}
	h.w.queue = q
	h.w.sleep = func(ctx context.Context, _ time.Duration) error { return ctx.Err() }
	var hosts sync.WaitGroup
	oldDial := streamDialFn
	streamDialFn = func(ctx context.Context, _ string) (*client, error) {
		select {
		case <-connect:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
		a, b := net.Pipe()
		hosts.Add(1)
		go func() {
			defer hosts.Done()
			// The host reads the observe request and hangs up at once.
			if _, err := bufio.NewReader(b).ReadBytes('\n'); err != nil && ctx.Err() == nil {
				t.Errorf("observe request: %v", err)
			}
			b.Close()
		}()
		return &client{conn: a, scan: bufio.NewScanner(a), stop: context.AfterFunc(ctx, func() { a.Close() })}, nil
	}
	readerDone := make(chan struct{})
	go func() { defer close(readerDone); h.w.streamLoop(ctx, q) }()
	t.Cleanup(func() {
		cancel()
		awaitStream(t, readerDone)
		hosts.Wait()
		streamDialFn = oldDial
		t.Log("cleanup: stream reader and pipe hosts joined, stream dial restored")
	})
	// Given polls that see working then idle while every stream connection
	// ends right after it connects.
	h.w.tick(context.Background())
	h.w.tick(context.Background())
	h.w.tick(context.Background())
	// Then the poll fallback reports the turn exactly once.
	if h.w.streamEpoch != 2 || h.w.streamUp {
		t.Fatalf("stream epoch %d up %v, want two ended connections", h.w.streamEpoch, h.w.streamUp)
	}
	entries, err := store.List()
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 || entries[0].ID != "D7" || entries[0].Seq != 1 || entries[0].Count != 1 {
		t.Errorf("pending entries = %+v, want D7 seq 1 count 1", entries)
	}
	h.done(t, "D7")
}

func TestStreamHeldReconciliationRebindsTurn(t *testing.T) {
	q := newStreamFIFO()
	// Server barriers: the reconnect is queued while an ordinary poll's
	// working snapshot is in flight, and B's settle while the
	// reconciliation's working snapshot is in flight.
	outer := tickFor("H", "B", "working")
	outer.beforeState = func() {
		q.append(streamItem{up: true})
	}
	reconcile := tickFor("H", "B", "working")
	reconcile.beforeState = func() {
		q.append(streamItem{frame: streamFrame{Type: "agent_settled", Session: "H"}})
	}
	h := newStreamHarness(t, tickFor("H", "A", "idle"), outer, reconcile, tickFor("H", "B", "idle"))
	store := newPendingStore(h.w.stateDir, h.w.profile)
	record := h.w.record
	h.w.record = func(id string, ev rpcEvent) {
		record(id, ev)
		if _, err := store.Record(id, ev); err != nil {
			t.Error(err)
		}
	}
	h.w.queue = q
	// Given A's turn bound on a connection that ended before A settled.
	h.w.tick(context.Background())
	h.connected()
	h.frame("agent_start", "H")
	h.list("l1", session("H", "A"))
	h.w.applyStream(context.Background(), streamItem{err: io.EOF})
	// When H runs B at the reconnect and B settles during the
	// reconciliation's snapshot I/O.
	h.w.tick(context.Background())
	h.w.tick(context.Background())
	// Then the settle is attributed to B, bound by the reconciliation.
	entries, err := store.List()
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 || entries[0].ID != "B" || entries[0].Seq != 1 || entries[0].Count != 1 {
		t.Errorf("pending entries = %+v, want B seq 1 count 1", entries)
	}
	h.done(t, "B")
}

func TestStreamReconciliationDropsEndedTurn(t *testing.T) {
	q := newStreamFIFO()
	// Server barrier: the reconnect and B's settle are queued while an
	// ordinary poll's working snapshot is in flight.
	outer := tickFor("H", "B", "working")
	outer.beforeState = func() {
		q.append(streamItem{up: true})
		q.append(streamItem{frame: streamFrame{Type: "agent_settled", Session: "H"}})
	}
	h := newStreamHarness(t, tickFor("H", "A", "idle"), outer, tickFor("H", "B", "idle"))
	store := newPendingStore(h.w.stateDir, h.w.profile)
	record := h.w.record
	h.w.record = func(id string, ev rpcEvent) {
		record(id, ev)
		if _, err := store.Record(id, ev); err != nil {
			t.Error(err)
		}
	}
	h.w.queue = q
	// Given A's turn bound on a connection that ended before A settled.
	h.w.tick(context.Background())
	h.connected()
	h.frame("agent_start", "H")
	h.list("l1", session("H", "A"))
	h.w.applyStream(context.Background(), streamItem{err: io.EOF})
	// When H runs B and B settles right after the reconnect, so the
	// reconciliation sees B idle.
	h.w.tick(context.Background())
	h.w.tick(context.Background())
	// Then B is reported once, and A's ended binding takes no settle.
	entries, err := store.List()
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 || entries[0].ID != "B" || entries[0].Seq != 1 || entries[0].Count != 1 {
		t.Errorf("pending entries = %+v, want B seq 1 count 1", entries)
	}
	h.done(t, "B")
}

func TestStreamReconnectRemovedKnownTurn(t *testing.T) {
	h := newStreamHarness(t, tickFor("H", "A", "idle"), scriptTick{})
	store := newPendingStore(h.w.stateDir, h.w.profile)
	record := h.w.record
	h.w.record = func(id string, ev rpcEvent) {
		record(id, ev)
		if _, err := store.Record(id, ev); err != nil {
			t.Error(err)
		}
	}
	// Given A's start and durable identity were observed before disconnect.
	h.w.tick(context.Background())
	h.connected()
	h.frame("agent_start", "H")
	h.list("l1", session("H", "A"))
	h.w.applyStream(context.Background(), streamItem{err: io.EOF})
	// When A settles on the new connection, then disappears before its
	// reconciliation list is served. The owner receives both queued items
	// before running that list, exactly as a busy owner may observe them.
	h.w.queue = newStreamFIFO()
	h.w.queue.append(streamItem{up: true})
	h.w.queue.append(streamItem{frame: streamFrame{Type: "agent_settled", Session: "H"}})
	h.w.drainStream(context.Background())
	// Then the known turn's completion remains attributable and durable.
	entries, err := store.List()
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 || entries[0].ID != "A" || entries[0].Seq != 1 || entries[0].Count != 1 {
		t.Errorf("pending entries = %+v, want A seq 1 count 1", entries)
	}
	h.done(t, "A")
}

func awaitStream[T any](t *testing.T, ch <-chan T) T {
	t.Helper()
	select {
	case value := <-ch:
		return value
	case <-time.After(5 * time.Second):
		t.Fatal("timed out awaiting explicit stream barrier")
		var zero T
		return zero
	}
}

func TestStreamObserveFirst(t *testing.T) {
	a, b := net.Pipe()
	q := newStreamFIFO()
	c := &client{conn: a, scan: bufio.NewScanner(a)}
	defer a.Close()
	defer b.Close()
	done := make(chan error, 1)
	go func() { done <- readStream(c, q) }()
	var req map[string]any
	if err := json.NewDecoder(b).Decode(&req); err != nil {
		t.Fatal(err)
	}
	b.Close()
	awaitStream(t, done)
	want := map[string]any{"id": "s0", "type": "list_sessions", "observe": true}
	if !reflect.DeepEqual(req, want) {
		t.Fatalf("first frame = %v, want %v", req, want)
	}
	t.Logf("wire request: %v; cleanup: both pipe endpoints closed and reader joined", req)
}

func TestStreamFakeDoesNotConsumeTick(t *testing.T) {
	h := newStreamHarness(t, tickFor("H", "D7", "blocked"), scriptTick{})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	c, err := dial(ctx, h.w.socket)
	if err != nil {
		t.Fatal(err)
	}
	err = readStream(c, newStreamFIFO())
	c.close()
	if err != io.EOF {
		t.Fatalf("unavailable fake stream = %v", err)
	}
	h.w.tick(ctx)
	ev := events(t, h.out)
	if len(ev) != 1 || ev[0]["event"] != "blocked" || ev[0]["id"] != "D7" {
		t.Fatalf("observe connection consumed tick script: %v", ev)
	}
}

// A live unix-socket fixture for owner/reader interleavings and real-surface QA.
// The script controls stream frames, while poll state replies can be held.
type liveStreamServer struct {
	ln       net.Listener
	mu       sync.Mutex
	conns    []net.Conn
	wg       sync.WaitGroup
	requests chan streamFrame
	stream   chan net.Conn
	states   chan net.Conn
}

func serveLiveStream(t *testing.T, holdState bool) *liveStreamServer {
	t.Helper()
	path := shortSocket(t)
	ln, err := net.Listen("unix", path)
	if err != nil {
		t.Fatal(err)
	}
	s := &liveStreamServer{ln: ln, requests: make(chan streamFrame, 32), stream: make(chan net.Conn, 1), states: make(chan net.Conn, 8)}
	t.Setenv("OMOSENSE_RPC_SOCK", path)
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			s.mu.Lock()
			s.conns = append(s.conns, conn)
			s.mu.Unlock()
			s.wg.Add(1)
			go func() {
				defer s.wg.Done()
				defer conn.Close()
				scan := bufio.NewScanner(conn)
				first, observing := true, false
				for scan.Scan() {
					var req struct {
						streamFrame
						Observe bool `json:"observe"`
					}
					if json.Unmarshal(scan.Bytes(), &req) != nil {
						return
					}
					if first {
						observing = req.Type == "list_sessions" && req.Observe
						if observing {
							s.stream <- conn
						}
						first = false
					}
					if observing {
						s.requests <- req.streamFrame
						continue
					}
					if req.Type == "get_state" && holdState {
						s.states <- conn
						continue
					}
					data := any(map[string]any{"sessions": []map[string]any{session("H", "A")}})
					if req.Type == "get_state" {
						data = state("idle", 0)
					}
					if writeStreamResponse(conn, req.ID, data) != nil {
						return
					}
				}
				if err := scan.Err(); err != nil && !errors.Is(err, net.ErrClosed) {
					t.Errorf("live stream scan: %v", err)
				}
			}()
		}
	}()
	t.Cleanup(func() {
		ln.Close()
		s.mu.Lock()
		for _, conn := range s.conns {
			conn.Close()
		}
		s.mu.Unlock()
		done := make(chan struct{})
		go func() { s.wg.Wait(); close(done) }()
		awaitStream(t, done)
		t.Log("cleanup: listener and all connections closed, accept/handler goroutines joined, socket directory removed")
	})
	return s
}

func writeStreamResponse(c net.Conn, id string, data any) error {
	return json.NewEncoder(c).Encode(map[string]any{"type": "response", "id": id, "success": true, "data": data})
}

func TestStreamRunCancellation(t *testing.T) {
	s := serveLiveStream(t, false)
	var b bytes.Buffer
	c := rpcCtx(t, &b, "")
	c.Flags["--all"] = true
	w := newWatcher(c, c.Out)
	entered := make(chan struct{}, 1)
	w.sleep = func(ctx context.Context, _ time.Duration) error {
		select {
		case entered <- struct{}{}:
		default:
		}
		<-ctx.Done()
		return ctx.Err()
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	var runErr error
	go func() { runErr = w.run(ctx); close(done) }()
	t.Cleanup(func() { cancel(); awaitStream(t, done) })
	awaitStream(t, entered)
	req := awaitStream(t, s.requests)
	if req.ID != "s0" {
		t.Fatalf("run first request = %+v", req)
	}
	cancel()
	awaitStream(t, done)
	if runErr != nil {
		t.Fatal(runErr)
	}
	t.Logf("run canceled and joined reader/timer; stdout:\n%s", b.String())
}

func TestStreamLookupWhileTickHeld(t *testing.T) {
	s := serveLiveStream(t, true)
	var b bytes.Buffer
	c := rpcCtx(t, &b, "")
	c.Flags["--all"] = true
	w := newWatcher(c, c.Out)
	var recorded []rpcEvent
	w.record = func(_ string, ev rpcEvent) { recorded = append(recorded, ev) }
	w.queue = newStreamFIFO()
	ctx, cancel := context.WithCancel(context.Background())
	readerDone := make(chan struct{})
	go func() {
		defer close(readerDone)
		client, err := dial(ctx, w.socket)
		if err != nil {
			w.queue.append(streamItem{err: err})
			return
		}
		w.queue.append(streamItem{up: true})
		err = readStream(client, w.queue)
		client.close()
		w.queue.append(streamItem{err: err})
	}()
	t.Cleanup(func() { cancel(); awaitStream(t, readerDone) })
	conn := awaitStream(t, s.stream)
	first := awaitStream(t, s.requests)
	if first.ID != "s0" {
		t.Fatalf("first observe request = %+v", first)
	}
	// Hold the owner's connect-time get_state until lookup+settle were read.
	scriptDone := make(chan error, 1)
	go func() {
		var held net.Conn
		select {
		case held = <-s.states:
		case <-ctx.Done():
			scriptDone <- fmt.Errorf("start lookup never arrived while get_state was held: %w", ctx.Err())
			return
		}
		if err := writeStreamResponse(conn, "s0", map[string]any{"sessions": []map[string]any{session("H", "A")}}); err != nil {
			scriptDone <- err
			return
		}
		for _, kind := range []string{"agent_settled", "agent_start"} {
			handle := "ignored"
			if kind == "agent_start" {
				handle = "H"
			}
			if err := json.NewEncoder(conn).Encode(map[string]any{"type": kind, "sessionId": handle}); err != nil {
				scriptDone <- err
				return
			}
		}
		select {
		case req := <-s.requests:
			if req.Type != "list_sessions" || !strings.HasPrefix(req.ID, "l") {
				scriptDone <- fmt.Errorf("start lookup = %+v", req)
				return
			}
			if err := writeStreamResponse(conn, req.ID, map[string]any{"sessions": []map[string]any{session("H", "B")}}); err != nil {
				scriptDone <- err
				return
			}
		case <-ctx.Done():
			scriptDone <- fmt.Errorf("server never received the l<n> start lookup while get_state was held: %w", ctx.Err())
			return
		}
		if err := json.NewEncoder(conn).Encode(map[string]any{"type": "agent_settled", "sessionId": "H"}); err != nil {
			scriptDone <- err
			return
		}
		// EOF is a reader barrier: streamDown is queued only after settle.
		conn.Close()
		select {
		case <-readerDone:
		case <-ctx.Done():
		}
		// The held request is the first tick-local get_state, id 2.
		scriptDone <- writeStreamResponse(held, "2", state("idle", 0))
	}()
	// A bounded context makes a delayed writer fail, rather than deadlock.
	timeout := time.AfterFunc(5*time.Second, cancel)
	defer timeout.Stop()
	w.drainStream(ctx)
	if err := awaitStream(t, scriptDone); err != nil {
		t.Fatal(err)
	}
	// The EOF barrier joins the reader before releasing the held snapshot.
	cancel()
	awaitStream(t, readerDone)
	w.drainStream(context.Background())
	h := &streamHarness{w: w, out: &b}
	h.done(t, "B")
	if len(recorded) != 1 || recorded[0].ID != "B" {
		t.Fatalf("held-tick pending identity = %+v", recorded)
	}
	t.Logf("held-tick RPC output:\n%s", b.String())
}

func TestStreamOverflow(t *testing.T) {
	h := newStreamHarness(t, tickFor("H", "D7", "idle"))
	h.w.tick(context.Background())
	q := newStreamFIFO()
	h.w.queue = q
	a, b := net.Pipe()
	defer a.Close()
	defer b.Close()
	ctx, cancel := context.WithCancel(context.Background())
	var completed atomic.Int32
	h.w.record = func(_ string, _ rpcEvent) { completed.Add(1) }
	reconnecting := make(chan int32, 1)
	oldDial := streamDialFn
	dials := 0
	streamDialFn = func(ctx context.Context, _ string) (*client, error) {
		dials++
		if dials > 1 {
			reconnecting <- completed.Load()
			<-ctx.Done()
			return nil, ctx.Err()
		}
		return &client{conn: a, scan: bufio.NewScanner(a), stop: context.AfterFunc(ctx, func() { a.Close() })}, nil
	}
	h.w.sleep = func(context.Context, time.Duration) error { return nil }
	readerDone := make(chan struct{})
	go func() { defer close(readerDone); h.w.streamLoop(ctx, q) }()
	t.Cleanup(func() {
		cancel()
		awaitStream(t, readerDone)
		streamDialFn = oldDial
	})
	var req map[string]any
	dec := json.NewDecoder(b)
	if err := dec.Decode(&req); err != nil {
		t.Fatal(err)
	}
	enc := json.NewEncoder(b)
	if err := enc.Encode(map[string]any{"type": "agent_start", "sessionId": "H"}); err != nil {
		t.Fatal(err)
	}
	if err := dec.Decode(&req); err != nil {
		t.Fatal(err)
	}
	if err := writeStreamResponse(b, req["id"].(string), map[string]any{"sessions": []map[string]any{session("H", "D7")}}); err != nil {
		t.Fatal(err)
	}
	// Up+start+lookup-response+filler fill the bound. Settle crosses it.
	for i := 0; i < streamBacklog-3; i++ {
		if err := writeStreamResponse(b, "s0", map[string]any{"sessions": []map[string]any{session("H", "D7")}}); err != nil {
			t.Fatal(err)
		}
	}
	if err := enc.Encode(map[string]any{"type": "agent_settled", "sessionId": "H"}); err != nil {
		t.Fatal(err)
	}
	b.Close()
	// The owner stays wedged until the reader queues streamDown: a pipe write
	// returns once the reader buffered its bytes, not once it queued their
	// items, and an earlier drain would keep the FIFO under the bound. Every
	// append after a consumed wake re-arms it, so this cannot miss streamDown.
wedged:
	for {
		select {
		case <-q.wake:
			q.mu.Lock()
			n := len(q.items)
			down := n > 0 && q.items[n-1].err != nil
			q.mu.Unlock()
			if down {
				break wedged
			}
		case count := <-reconnecting:
			t.Fatalf("reconnect overtook queued completion: count=%d", count)
		case <-time.After(5 * time.Second):
			t.Fatal("reader did not queue streamDown")
		}
	}
	// The second dial observes completion only after streamDown's
	// acknowledgment releases the reader.
	h.w.drainStream(ctx)
	if count := awaitStream(t, reconnecting); count != 1 {
		t.Fatalf("reconnect overtook queued completion: count=%d", count)
	}
	h.done(t, "D7")
	text := h.out.String()
	if h.w.streamSeq != streamBacklog {
		t.Fatalf("applied stream frames = %d, want %d", h.w.streamSeq, streamBacklog)
	}
	if !strings.Contains(text, "rpc stream stream backlog overflow") || strings.Index(text, `"event":"done"`) > strings.Index(text, "rpc stream stream backlog overflow") || h.w.streamUp {
		t.Fatalf("overflow down overtook queued done:\n%s", text)
	}
	t.Log("cleanup: overflow pipe endpoints closed, reader joined, down acknowledged after queued done")
}
