package rpc

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/DevNewbie1826/omosense/internal/core"
)

// TestVerifyTimeout guards S5-1 at the delivery boundary: the per-attempt
// timeout a done-verification hook actually runs under is verify.timeoutSec
// (7 -> 7 s; 0 and -3 fall back to the 60 s default). A real completion is
// driven through run() and the duration handed to the hook runner is captured,
// so a call site that hard-codes a timeout - or a changed fallback - fails
// here, not just the helper.
func TestVerifyTimeout(t *testing.T) {
	for _, tc := range []struct {
		name string
		sec  int
		want time.Duration
	}{
		{"configured 7", 7, 7 * time.Second},
		{"zero falls back", 0, 60 * time.Second},
		{"negative falls back", -3, 60 * time.Second},
	} {
		t.Run(tc.name, func(t *testing.T) {
			v := newVerifyFixture(t, "0")
			handed := make(chan time.Duration, 4)
			old := runVerifyFn
			runVerifyFn = func(ctx context.Context, command []string, timeout time.Duration, env []string, dir string) core.VerifyResult {
				select {
				case handed <- timeout:
				default:
				}
				return core.RunVerify(ctx, command, timeout, env, dir)
			}
			t.Cleanup(func() { runVerifyFn = old })

			committed := hookSignals(t)
			c, _ := verifyCtx(t, v, tc.sec)
			path := shortSocket(t)
			serveRPC(t, path, verifyTicks("working", "idle"))
			t.Setenv("OMOSENSE_RPC_SOCK", path)
			w := newWatcher(c, c.Out)
			done, cancel := startRun(t, c, w)
			tickBound(t, w, "completion tick")
			awaitHook(t, committed, 1)
			cancel()
			awaitSource(t, done, 5*time.Second, "timeout hook")

			select {
			case got := <-handed:
				if got != tc.want {
					t.Fatalf("hook timeout for timeoutSec=%d = %s, want %s", tc.sec, got, tc.want)
				}
			default:
				t.Fatal("the completion never reached the hook runner")
			}
		})
	}
}

// TestEntryBlockBudgetCountsUnverifiedLine guards S5-3: the mandatory
// unverified line shares the entry's slice of the 32 KiB batch budget
// (deliver.go entryBlock subtracts its length), so entries carrying verdicts
// keep blocks inside their slice and a batch of them stays within batchLimit
// with every entry still listed.
func TestEntryBlockBudgetCountsUnverifiedLine(t *testing.T) {
	detail := strings.Repeat("d", 2000)
	const n = 7
	entries := make([]pendingEntry, 0, n)
	for i := 1; i <= n; i++ {
		e := pendingEntry{
			ID: fmt.Sprintf("D%d", i), Session: fmt.Sprintf("rpc-%d", i),
			Name: ptr("job"), Cwd: ptr("/jobs"), Thread: ptr(fmt.Sprintf("t%d", i)),
			Seq: uint64(i), Count: 1, DoneAt: "2026-10-05T00:00:00.000Z",
			Verify: "unverified", VerifyDetail: detail,
		}
		if i == 1 {
			// One entry fills its whole display share, so the truncation
			// budget - not the field values - sets the block's length.
			e.Name, e.Cwd, e.Thread = ptr(strings.Repeat("n", 4000)), ptr(strings.Repeat("/c", 2000)), ptr(strings.Repeat("t", 4000))
		}
		entries = append(entries, e)
	}
	for i, e := range entries {
		if block := entryBlock(core.RPCCfg{}, e, "/proj", "/proj/state", n); len(block) > batchLimit/n {
			t.Fatalf("entry %d: block with an unverified line = %d bytes, over its %d-byte budget slice:\n%.300s", i+1, len(block), batchLimit/n, block)
		}
	}
	text := mustBatchText(t, core.RPCCfg{}, entries, "/proj", "/proj/state")
	if len(text) > batchLimit {
		t.Fatalf("batch with unverified lines = %d bytes, over the %d budget", len(text), batchLimit)
	}
	for _, e := range entries {
		if !strings.Contains(text, ackCommand(e, "/proj", "/proj/state")) {
			t.Fatalf("the budget pushed entry %s out of its own batch:\n%s", e.ID, text)
		}
	}
	if got := strings.Count(text, detail); got != n {
		t.Fatalf("unverified lines rendered %d times, want %d", got, n)
	}
	t.Logf("batch: %d entries, %d bytes", n, len(text))
}

// TestSilentNotReportedWhenNotWorking guards S5-4: only a working session can
// be reported silent. Idle and blocked registered sessions stay unchanged well
// past the silence window without a single silent-session line, while the
// working control is reported exactly once (watcher.go's silent report
// requires status == "working").
func TestSilentNotReportedWhenNotWorking(t *testing.T) {
	var b bytes.Buffer
	c := rpcCtx(t, &b, `{"idlejob":{"session_id":"i"},"blockedjob":{"session_id":"b"},"workjob":{"session_id":"w"}}`)
	c.Profile.SilentMinutes = 30
	clock := time.Date(2026, 10, 5, 0, 0, 0, 0, time.UTC)
	old := nowFn
	nowFn = func() time.Time { return clock }
	t.Cleanup(func() { nowFn = old })

	steps := []time.Duration{0, 29 * time.Minute, 2 * time.Minute, 5 * time.Minute, 5 * time.Minute}
	ticks := make([]scriptTick, len(steps))
	for i := range ticks {
		ticks[i] = scriptTick{
			sessions: []map[string]any{session("rpc-i", "i"), session("rpc-b", "b"), session("rpc-w", "w")},
			states: map[string]any{
				"rpc-i": state("idle", 3),
				"rpc-b": state("blocked", 3),
				"rpc-w": state("working", 3),
			},
		}
	}
	path := shortSocket(t)
	serveRPC(t, path, ticks)
	t.Setenv("OMOSENSE_RPC_SOCK", path)
	w := newWatcher(c, c.Out)
	for i, d := range steps {
		clock = clock.Add(d)
		w.tick(context.Background())
		lines := logLines(&b, "silent-session")
		if got := countLines(lines, `"id":"i"`); got != 0 {
			t.Fatalf("tick %d (%s): an idle session was reported silent %d time(s): %v", i, clock, got, lines)
		}
		if got := countLines(lines, `"id":"b"`); got != 0 {
			t.Fatalf("tick %d (%s): a blocked session was reported silent %d time(s): %v", i, clock, got, lines)
		}
		want := 0
		if i >= 2 {
			want = 1
		}
		if got := countLines(lines, `"id":"w"`); got != want {
			t.Fatalf("tick %d (%s): the working control reported %d silent line(s), want %d: %v", i, clock, got, want, lines)
		}
	}
	t.Log("cleanup: test Cleanup closes and joins fake RPC, removes temporary socket/state, restores environment and nowFn")
}

// TestRecordWithoutHookClearsPreviousVerdict guards S5-5: re-recording an id
// without a hook clears the verdict of the done it replaces, so a newer done
// never inherits the previous verdict - on the returned entry and on disk,
// where the omitted keys keep the hook-less JSON byte-identical (pending.go
// Record's hook-less branch).
func TestRecordWithoutHookClearsPreviousVerdict(t *testing.T) {
	s := newPendingStore(t.TempDir())
	ev := rpcEvent{Session: "rpc-1", Name: ptr("job"), Cwd: ptr("/jobs"), Thread: ptr("7")}
	pending, err := s.Record("D7", ev, true)
	if err != nil {
		t.Fatal(err)
	}
	if pending.Verify != "pending" || pending.VerifyDetail != "" {
		t.Fatalf("a verifying record must start pending: %+v", pending)
	}
	if ok, err := s.SetVerify("D7", pending.Seq, "unverified", "exit 1: boom"); err != nil || !ok {
		t.Fatalf("SetVerify = %v, %v", ok, err)
	}
	cleared, err := s.Record("D7", ev, false)
	if err != nil {
		t.Fatal(err)
	}
	if cleared.Verify != "" || cleared.VerifyDetail != "" {
		t.Fatalf("a hook-less re-record inherited the previous verdict: %+v", cleared)
	}
	es := pendingList(t, s)
	if len(es) != 1 || es[0].Verify != "" || es[0].VerifyDetail != "" {
		t.Fatalf("the persisted entry kept the stale verdict: %+v", es)
	}
	raw, err := os.ReadFile(s.path)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(raw), "verify") {
		t.Fatalf("the stale verdict stayed in the pending JSON: %s", raw)
	}
}

// TestBatchFittingKeepsAllEntriesDespiteHugeMoreLabel guards S5-6: a batch
// whose blocks all fit the 32 KiB budget keeps every entry no matter how large
// the configured `more` label is - a fitting batch must never depend on the
// overflow label at all (deliver.go batchText's fitting early-return).
func TestBatchFittingKeepsAllEntriesDespiteHugeMoreLabel(t *testing.T) {
	entries := []pendingEntry{
		{ID: "D1", Session: "rpc-1", Name: ptr("job one"), Cwd: ptr("/jobs/one"), Thread: ptr("7"), Seq: 1, Count: 1, DoneAt: "2026-10-05T00:00:00.000Z"},
		{ID: "D2", Session: "rpc-2", Name: ptr("job two"), Cwd: ptr("/jobs/two"), Thread: ptr("8"), Seq: 2, Count: 1, DoneAt: "2026-10-05T00:01:00.000Z"},
		{ID: "D3", Session: "rpc-3", Name: ptr("job three"), Cwd: ptr("/jobs/three"), Thread: ptr("9"), Seq: 3, Count: 1, DoneAt: "2026-10-05T00:02:00.000Z"},
	}
	r := core.RPCCfg{Labels: map[string]string{"more": strings.Repeat("M", 2*batchLimit)}}
	text := mustBatchText(t, r, entries, "/proj", "/proj/state")
	if len(text) > batchLimit {
		t.Fatalf("a fitting batch = %d bytes, over the %d budget", len(text), batchLimit)
	}
	for _, e := range entries {
		if !strings.Contains(text, ackCommand(e, "/proj", "/proj/state")) {
			t.Fatalf("a fully fitting batch dropped entry %s for the more label:\n%s", e.ID, text)
		}
	}
	if strings.Contains(text, "omosense rpc pending") {
		t.Fatalf("a fully fitting batch carries an overflow line:\n%s", text)
	}
}
