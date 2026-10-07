package rpc

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/DevNewbie1826/omosense/internal/core"
)

func omap(t *testing.T, doc string) *core.OMap {
	t.Helper()
	v, err := core.ParseJSON([]byte(doc))
	if err != nil {
		t.Fatal(err)
	}
	m, ok := v.(*core.OMap)
	if !ok {
		t.Fatalf("not an object: %s", doc)
	}
	return m
}

func logLines(b *bytes.Buffer, token string) []string {
	var out []string
	for _, line := range strings.Split(strings.TrimSpace(b.String()), "\n") {
		if strings.HasPrefix(line, "LOG "+token+" ") || line == "LOG "+token {
			out = append(out, strings.TrimPrefix(line, "LOG "+token+" "))
		}
	}
	return out
}

// mustBatchText renders a batch, failing the test when its mandatory content
// cannot fit the budget.
func mustBatchText(t *testing.T, r core.RPCCfg, entries []pendingEntry, dir, state string) string {
	t.Helper()
	text, err := batchText(r, entries, dir, state)
	if err != nil {
		t.Fatalf("batchText: %v", err)
	}
	return text
}

// TestClosedThreadSkipped guards Risk row "a closed thread's pane/session still
// produces job events" for the rpc reader (IS-4): a thread whose status is
// "done"/"closed", or that carries a non-empty closed string, is not watched,
// so its completion is never recorded.
func TestClosedThreadSkipped(t *testing.T) {
	for _, tc := range []struct {
		name, extra string
		watched     bool
	}{
		{"status done", `,"status":"done"`, false},
		{"status closed", `,"status":"closed"`, false},
		{"closed timestamp", `,"closed":"2026-10-05T00:00:00.000Z"`, false},
		{"closed empty string", `,"closed":""`, true},
		{"closed null", `,"closed":null`, true},
		{"closed non-string", `,"closed":7`, true},
		{"status working", `,"status":"working"`, true},
		{"no lifecycle keys", ``, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			threads := fmt.Sprintf(`{"job":{"session_id":"d"%s}}`, tc.extra)
			path := shortSocket(t)
			serveRPC(t, path, []scriptTick{
				{sessions: []map[string]any{session("rpc-1", "d")}, states: map[string]any{"rpc-1": state("working", 0)}},
				{sessions: []map[string]any{session("rpc-1", "d")}, states: map[string]any{"rpc-1": state("idle", 0)}},
			})
			t.Setenv("OMOSENSE_RPC_SOCK", path)
			var b bytes.Buffer
			c := rpcCtx(t, &b, threads)
			runTicks(t, c, 2)
			es := pendingList(t, newPendingStore(c.State))
			if tc.watched {
				if len(es) != 1 || es[0].ID != "d" {
					t.Fatalf("active thread not watched: %+v; stdout %s", es, b.String())
				}
			} else if len(es) != 0 {
				t.Fatalf("closed thread produced a completion: %+v; stdout %s", es, b.String())
			}
			t.Logf("threads.json %s -> recorded %+v", threads, es)
			t.Log("cleanup: test Cleanup closes and joins fake RPC, removes temporary socket/state, restores environment and sleepFn")
		})
	}
}

// TestOwnedThreads guards IS-12's exported matcher: it reports exactly the
// thread keys a live rpc session owns, using the rule the watcher applies, and
// surfaces a socket failure instead of silently owning nothing.
func TestOwnedThreads(t *testing.T) {
	t.Run("matches", func(t *testing.T) {
		path := shortSocket(t)
		row := session("H1", "D1")
		row["cwd"] = "/work/job"
		f := serveRPC(t, path, []scriptTick{{sessions: []map[string]any{row}}})
		t.Setenv("OMOSENSE_RPC_SOCK", path)
		entries := map[string]*core.OMap{
			"durable":  omap(t, `{"session_id":"D1"}`),
			"field":    omap(t, `{"durable_session_id":"D1"}`),
			"handle":   omap(t, `{"session":"H1","cwd":"/work/job"}`),
			"cleaned":  omap(t, `{"session":"H1","cwd":"/work/./job/"}`),
			"nocwd":    omap(t, `{"session":"H1"}`),
			"othercwd": omap(t, `{"session":"H1","cwd":"/work/elsewhere"}`),
			"unlisted": omap(t, `{"session_id":"NOPE"}`),
		}
		owned, err := OwnedThreads(context.Background(), entries)
		if err != nil {
			t.Fatal(err)
		}
		want := map[string]bool{"durable": true, "field": true, "handle": true, "cleaned": true}
		if fmt.Sprint(owned) != fmt.Sprint(want) {
			t.Fatalf("owned = %v, want %v", owned, want)
		}
		f.mu.Lock()
		calls := append([]string(nil), f.calls...)
		f.mu.Unlock()
		if len(calls) != 1 || calls[0] != "list_sessions " {
			t.Fatalf("calls = %v, want exactly one list_sessions", calls)
		}
	})
	t.Run("socket down", func(t *testing.T) {
		t.Setenv("OMOSENSE_RPC_SOCK", shortSocket(t))
		owned, err := OwnedThreads(context.Background(), map[string]*core.OMap{"job": omap(t, `{"session_id":"D1"}`)})
		if err == nil || owned != nil {
			t.Fatalf("socket failure = %v, %v; want an error", owned, err)
		}
	})
	t.Run("no entries", func(t *testing.T) {
		t.Setenv("OMOSENSE_RPC_SOCK", shortSocket(t))
		owned, err := OwnedThreads(context.Background(), map[string]*core.OMap{})
		if err != nil || len(owned) != 0 {
			t.Fatalf("empty entries = %v, %v", owned, err)
		}
	})
}

// The golden constants are the pre-change outputs for the fixed entry set
// below, captured in evidence run1/n-rpc-reference-batch.txt.
const goldenBatch = `작업: job one
thread: 7
cwd: /jobs/one
완료 id: D1
seq: 1
done_at: 2026-10-05T00:00:00.000Z
count: 1
확인 명령: OMOSENSE_DIR='/proj' OMOSENSE_STATE='/proj/state' omosense rpc ack D1 1
작업: job two
thread: 8
cwd: /jobs/two
완료 id: D2
seq: 2
done_at: 2026-10-05T00:01:00.000Z
count: 3
확인 명령: OMOSENSE_DIR='/proj' OMOSENSE_STATE='/proj/state' omosense rpc ack D2 2`

const goldenEntryBlock = `작업: job one
thread: 7
cwd: /jobs/one
완료 id: D1
seq: 1
done_at: 2026-10-05T00:00:00.000Z
count: 1
확인 명령: OMOSENSE_DIR='/proj' OMOSENSE_STATE='/proj/state' omosense rpc ack D1 1`

const goldenOverflow = `+2 more: OMOSENSE_DIR='/proj' OMOSENSE_STATE='/proj/state' omosense rpc pending`

const goldenPendingJSON = `{"version":1,"seq":1,"notified_seq":0,"entries":{"D1":{"id":"D1","session":"rpc-1","name":"job one","cwd":"/jobs/one","thread":"7","seq":1,"count":1,"first_at":"2026-10-05T00:00:00.000Z","done_at":"2026-10-05T00:00:00.000Z"}}}`

const goldenPendingLine = `PENDING {"id":"D1","session":"rpc-1","name":"job one","cwd":"/jobs/one","thread":"7","seq":1,"count":1,"first_at":"2026-10-05T00:00:00.000Z","done_at":"2026-10-05T00:00:00.000Z"}`

// TestNoHookGolden guards Risk row "without a hook the batch text or pending
// JSON differs from 0.1.0": with default labels and no verify.command the
// batch text, the overflow line, the entry block and the pending JSON are
// byte-identical to the pre-change outputs.
func TestNoHookGolden(t *testing.T) {
	entries := []pendingEntry{
		{ID: "D1", Session: "rpc-1", Name: ptr("job one"), Cwd: ptr("/jobs/one"), Thread: ptr("7"), Seq: 1, Count: 1, DoneAt: "2026-10-05T00:00:00.000Z"},
		{ID: "D2", Session: "rpc-2", Name: ptr("job two"), Cwd: ptr("/jobs/two"), Thread: ptr("8"), Seq: 2, Count: 3, DoneAt: "2026-10-05T00:01:00.000Z"},
	}
	r := core.RPCCfg{}
	if got := mustBatchText(t, r, entries, "/proj", "/proj/state"); got != goldenBatch {
		t.Fatalf("batch text changed from 0.1.0:\n--- got ---\n%s\n--- want ---\n%s", got, goldenBatch)
	}
	if got := entryBlock(r, entries[0], "/proj", "/proj/state", 2); got != goldenEntryBlock {
		t.Fatalf("entry block changed from 0.1.0:\n--- got ---\n%s\n--- want ---\n%s", got, goldenEntryBlock)
	}
	if got := overflowLine(r, 2, "/proj", "/proj/state"); got != goldenOverflow {
		t.Fatalf("overflow line changed from 0.1.0: %q", got)
	}

	dir := t.TempDir()
	store := newPendingStore(dir)
	old := nowFn
	nowFn = func() time.Time { return time.Date(2026, 10, 5, 0, 0, 0, 0, time.UTC) }
	t.Cleanup(func() { nowFn = old })
	e, err := store.Record("D1", rpcEvent{Event: "done", Session: "rpc-1", ID: "D1", Name: ptr("job one"), Cwd: ptr("/jobs/one"), Thread: ptr("7"), To: "idle"}, false)
	if err != nil {
		t.Fatal(err)
	}
	b, err := os.ReadFile(filepath.Join(dir, "rpc-pending.json"))
	if err != nil {
		t.Fatal(err)
	}
	if string(b) != goldenPendingJSON {
		t.Fatalf("pending JSON changed from 0.1.0:\n--- got ---\n%s\n--- want ---\n%s", b, goldenPendingJSON)
	}
	var line bytes.Buffer
	core.NewOut(&line).Emit("PENDING", e)
	if got := strings.TrimSuffix(line.String(), "\n"); got != goldenPendingLine {
		t.Fatalf("PENDING line changed from 0.1.0:\n--- got ---\n%s\n--- want ---\n%s", got, goldenPendingLine)
	}
}

// TestBatchLabelsCustom guards Risk row "custom labels ignored ... or unknown
// label key accepted" for the rpc reader (IS-13): every configured label
// replaces its default, the `<label>: ` shape holds, and the ACK command is
// neither a label nor truncated even when the labels are long.
func TestBatchLabelsCustom(t *testing.T) {
	labels := map[string]string{
		"task": "TASK", "thread": "THREAD", "cwd": "CWD", "id": "ID", "seq": "SEQ",
		"doneAt": "DONE", "count": "N", "ack": "ACK", "unverified": "UNV",
		"verifyPending": "VPEND", "more": "MORE",
	}
	r := core.RPCCfg{Labels: labels}
	e := pendingEntry{ID: "D1", Session: "rpc-1", Name: ptr("n"), Cwd: ptr("/c"), Thread: ptr("7"), Seq: 1, Count: 1, DoneAt: "2026-10-05T00:00:00.000Z", Verify: "unverified", VerifyDetail: "exit 1: boom"}
	text := mustBatchText(t, r, []pendingEntry{e}, "/proj", "/proj/state")
	for _, want := range []string{
		"TASK: n", "THREAD: 7", "CWD: /c", "ID: D1", "SEQ: 1",
		"DONE: 2026-10-05T00:00:00.000Z", "N: 1", "UNV: exit 1: boom",
		"ACK: OMOSENSE_DIR='/proj' OMOSENSE_STATE='/proj/state' omosense rpc ack D1 1",
	} {
		if !strings.Contains(text, want) {
			t.Fatalf("batch lacks %q:\n%s", want, text)
		}
	}
	for _, leak := range []string{"작업", "확인 명령", "미검증", "완료 id"} {
		if strings.Contains(text, leak) {
			t.Fatalf("default label %q leaked into a labelled batch:\n%s", leak, text)
		}
	}
	if got := overflowLine(r, 2, "/proj", "/proj/state"); !strings.HasPrefix(got, "+2 MORE: ") {
		t.Fatalf("overflow label = %q", got)
	}
	if got := entryBlock(core.RPCCfg{Labels: map[string]string{"unverified": "UNV", "verifyPending": "VPEND"}}, pendingEntry{ID: "D1", Seq: 1, Count: 1, DoneAt: "x", Verify: "pending"}, "/proj", "/proj/state", 1); !strings.Contains(got, "UNV: VPEND") {
		t.Fatalf("pending line = %q", got)
	}

	long := core.RPCCfg{Labels: map[string]string{"task": strings.Repeat("t", 500), "ack": strings.Repeat("a", 500), "id": strings.Repeat("i", 500), "more": "MORE"}}
	two := []pendingEntry{
		{ID: "D1", Session: "rpc-1", Name: ptr("n"), Cwd: ptr("/c"), Thread: ptr("7"), Seq: 1, Count: 1, DoneAt: "2026-10-05T00:00:00.000Z"},
		{ID: "D2", Session: "rpc-2", Name: ptr("n"), Cwd: ptr("/c"), Thread: ptr("8"), Seq: 2, Count: 2, DoneAt: "2026-10-05T00:01:00.000Z"},
	}
	text = mustBatchText(t, long, two, "/proj", "/proj/state")
	if len(text) > batchLimit {
		t.Fatalf("batch over the 32 KiB budget with long labels: %d bytes", len(text))
	}
	for _, e := range two {
		if !strings.Contains(text, ackCommand(e, "/proj", "/proj/state")) {
			t.Fatalf("ACK command for %s was truncated:\n%s", e.ID, text)
		}
	}
}

// TestBatchOverflowLabelBounded guards IS-13's budget for the overflow line: a
// long configured `more` label is truncated to the bytes the mandatory
// `omosense rpc pending` command leaves, so the argument the batcher actually
// sends stays inside 32 KiB, is valid UTF-8, and still carries the whole
// pending command.
func TestBatchOverflowLabelBounded(t *testing.T) {
	for _, tc := range []struct {
		name string
		more string
	}{
		{"ascii", strings.Repeat("M", batchLimit)},
		{"multi-byte", strings.Repeat("가", batchLimit)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			x := newBatchFixture(t)
			// A `task` label longer than the whole budget forces the overflow
			// path: no entry block fits, so the batch is the overflow line.
			x.b.c.Profile.RPC.Labels = map[string]string{"task": strings.Repeat("T", batchLimit), "more": tc.more}
			x.subscribe(t, "MAINQA")
			pendingRecord(t, x.b.store, "D1", "rpc-1")
			pendingRecord(t, x.b.store, "D2", "rpc-2")
			x.advance(batchQuiet)
			x.pass(t)
			sends := x.f.sub(t, "send")
			if len(sends) != 1 {
				t.Fatalf("sends = %q", sends)
			}
			text := sends[0][3]
			if len(text) > batchLimit {
				t.Fatalf("sent argument = %d bytes, over the %d budget", len(text), batchLimit)
			}
			if !utf8.ValidString(text) {
				t.Fatalf("sent argument split a rune: %q", text)
			}
			want := fmt.Sprintf("OMOSENSE_DIR='%s' OMOSENSE_STATE='%s' omosense rpc pending", x.b.c.Dir, x.b.c.State)
			if !strings.Contains(text, want) {
				t.Fatalf("the pending command is missing or truncated:\n%s", text)
			}
			if !strings.HasPrefix(text, "+2 ") {
				t.Fatalf("the batch is not the overflow line:\n%s", text)
			}
			if strings.Contains(text, tc.more) {
				t.Fatalf("the whole %d-byte more label was kept", len(tc.more))
			}
			if len(text) < batchLimit-2 {
				t.Fatalf("the label was not filled to the budget: %d bytes", len(text))
			}
			if un, err := x.b.store.Unnotified(); err != nil || len(un) != 0 {
				t.Fatalf("un-notified after the batch: %+v %v", un, err)
			}
			t.Logf("sent argument: %d bytes, prefix %q", len(text), text[:24])
		})
	}
}

// TestBatchOverflowMandatoryTooLarge guards IS-13's failure rule: when the
// mandatory pending command cannot fit the budget, no batch is sent, the
// entries stay un-notified, and one LOG line names the reason.
func TestBatchOverflowMandatoryTooLarge(t *testing.T) {
	x := newBatchFixture(t)
	x.subscribe(t, "MAINQA")
	pendingRecord(t, x.b.store, "D1", "rpc-1")
	pendingRecord(t, x.b.store, "D2", "rpc-2")
	// A folder path long enough that the overflow line's mandatory suffix alone
	// exceeds the budget.
	x.b.c.Dir = "/" + strings.Repeat("d", batchLimit)
	x.advance(batchQuiet)
	x.pass(t)
	if sends := x.f.sub(t, "send"); len(sends) != 0 {
		t.Fatalf("a batch whose mandatory suffix cannot fit was sent: %q", sends)
	}
	un, err := x.b.store.Unnotified()
	if err != nil || len(un) != 2 {
		t.Fatalf("entries after a refused batch: %+v %v", un, err)
	}
	lines := logLines(x.logs, "rpc batch too large:")
	if len(lines) != 1 {
		t.Fatalf("LOG lines = %v", logLines(x.logs, "rpc"))
	}
	if !strings.Contains(lines[0], fmt.Sprintf("limit %d", batchLimit)) {
		t.Fatalf("reason = %q", lines[0])
	}
	t.Logf("refused batch: %s", lines[0])
}

// TestBatchUnverifiedLine guards Risk row "batch omits the unverified line or
// prints it for verified entries" (IS-13): an unverified done carries one
// `<unverified>: <detail>` line, a still-running hook `<unverified>:
// <verifyPending>`, and a verified or hook-less done adds nothing. The line
// sits between the count and the ACK command.
func TestBatchUnverifiedLine(t *testing.T) {
	r := core.RPCCfg{}
	base := pendingEntry{ID: "D1", Session: "rpc-1", Name: ptr("n"), Cwd: ptr("/c"), Thread: ptr("7"), Seq: 1, Count: 1, DoneAt: "2026-10-05T00:00:00.000Z"}
	for _, tc := range []struct{ verify, detail, want string }{
		{"", "", ""},
		{"verified", "", ""},
		{"pending", "", "미검증: 검증이 끝나지 않음"},
		{"unverified", "exit 1: boom", "미검증: exit 1: boom"},
	} {
		t.Run(tc.verify+"_"+tc.detail, func(t *testing.T) {
			e := base
			e.Verify, e.VerifyDetail = tc.verify, tc.detail
			block := entryBlock(r, e, "/proj", "/proj/state", 1)
			if tc.want == "" {
				if strings.Contains(block, "미검증") {
					t.Fatalf("verified/hook-less entry printed an unverified line:\n%s", block)
				}
				return
			}
			if !strings.Contains(block, tc.want) {
				t.Fatalf("block lacks %q:\n%s", tc.want, block)
			}
			count := strings.Index(block, "count: 1")
			unv := strings.Index(block, "미검증: ")
			ack := strings.Index(block, "확인 명령: ")
			if count < 0 || unv < 0 || ack < 0 || !(count < unv && unv < ack) {
				t.Fatalf("unverified line is not between count and ack:\n%s", block)
			}
		})
	}
}

// verifyFixture is a fake done-verification hook: it records its argv and
// environment, optionally blocks on a FIFO barrier, and exits with the code
// read from <dir>/control at run time.
type verifyFixture struct {
	dir string
}

func newVerifyFixture(t *testing.T, control string) *verifyFixture {
	t.Helper()
	v := &verifyFixture{dir: t.TempDir()}
	script := `#!/bin/sh
dir=${0%/*}
{
  printf '%s\n' "$OMOSENSE_DONE_SOURCE"
  printf '%s\n' "$OMOSENSE_DONE_ID"
  printf '%s\n' "$OMOSENSE_DONE_SESSION"
  printf '%s\n' "$OMOSENSE_DONE_THREAD"
  printf '%s\n' "$OMOSENSE_DONE_CWD"
} >> "$dir/env"
if [ -e "$dir/gate" ] && [ -p "$dir/ready" ]; then
  printf 'ready\n' > "$dir/ready"
  IFS= read -r release < "$dir/release"
fi
if [ -f "$dir/stderr" ]; then cat "$dir/stderr" >&2; fi
IFS= read -r code < "$dir/control"
exit "$code"
`
	deliveryWrite(t, filepath.Join(v.dir, "hook.sh"), script)
	if err := os.Chmod(filepath.Join(v.dir, "hook.sh"), 0o700); err != nil {
		t.Fatal(err)
	}
	v.setControl(t, control)
	return v
}

func (v *verifyFixture) path() string { return filepath.Join(v.dir, "hook.sh") }

func (v *verifyFixture) setControl(t *testing.T, control string) {
	t.Helper()
	deliveryWrite(t, filepath.Join(v.dir, "control"), control+"\n")
}

func (v *verifyFixture) envs(t *testing.T) [][]string {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(v.dir, "env"))
	if err != nil {
		return nil
	}
	fields := strings.Split(strings.TrimSuffix(string(b), "\n"), "\n")
	var out [][]string
	for i := 0; i+5 <= len(fields); i += 5 {
		out = append(out, fields[i:i+5])
	}
	return out
}

// gate installs the FIFO barrier and returns a wait for the first entry and a
// release for it.
func (v *verifyFixture) gate(t *testing.T) (wait, release func()) {
	t.Helper()
	deliveryWrite(t, filepath.Join(v.dir, "gate"), "")
	open := func(name string) *os.File {
		path := filepath.Join(v.dir, name)
		if err := syscall.Mkfifo(path, 0o600); err != nil {
			t.Fatal(err)
		}
		fd, err := os.OpenFile(path, os.O_RDWR, 0o600)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { fd.Close() })
		return fd
	}
	ready, gate := open("ready"), open("release")
	signaled := make(chan error, 1)
	go func() {
		_, err := bufio.NewReader(ready).ReadString('\n')
		signaled <- err
	}()
	wait = func() {
		select {
		case err := <-signaled:
			if err != nil {
				t.Fatal(err)
			}
		case <-time.After(10 * time.Second):
			t.Fatal("hook did not enter the FIFO barrier")
		}
	}
	release = func() {
		if _, err := gate.WriteString("release\n"); err != nil {
			t.Fatal(err)
		}
	}
	return wait, release
}

// hookSignals routes the committed-hook-result seam into a channel, so a test
// observes the store write instead of racing the source's shutdown.
func hookSignals(t *testing.T) <-chan struct{} {
	t.Helper()
	ch := make(chan struct{}, 16)
	old := hookDoneFn
	hookDoneFn = func() { ch <- struct{}{} }
	t.Cleanup(func() { hookDoneFn = old })
	return ch
}

func awaitHook(t *testing.T, ch <-chan struct{}, n int) {
	t.Helper()
	for i := 0; i < n; i++ {
		select {
		case <-ch:
		case <-time.After(10 * time.Second):
			t.Fatalf("hook result %d was not committed", i+1)
		}
	}
}

// verifyCtx builds a source context whose profile carries the fake hook.
func verifyCtx(t *testing.T, v *verifyFixture, timeoutSec int) (*core.Ctx, *bytes.Buffer) {
	t.Helper()
	b := &bytes.Buffer{}
	c := rpcCtx(t, b, "")
	c.Flags["--all"] = true
	c.Profile.Verify = core.VerifyCfg{Command: []string{v.path()}, TimeoutSec: timeoutSec}
	return c, b
}

// startRun drives the real run entry with a sleeping tick loop, so the test
// advances polls itself through tickBound and observes each one as soon as it
// returns. The pacer replaces both the package hook and the watcher's captured
// copy, which newWatcher took before this call.
func startRun(t *testing.T, c *core.Ctx, w *watcher) (chan error, context.CancelFunc) {
	t.Helper()
	entered := make(chan struct{}, 1)
	old := sleepFn
	pacer := func(ctx context.Context, _ time.Duration) error {
		select {
		case entered <- struct{}{}:
		default:
		}
		<-ctx.Done()
		return ctx.Err()
	}
	sleepFn = pacer
	w.sleep = pacer
	t.Cleanup(func() { sleepFn = old })
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- run(ctx, c, c.Out, w) }()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("the source did not start ticking")
	}
	return done, cancel
}

// tickBound applies one poll and fails if it does not return, which is what a
// hook running inside the watcher would cause.
func tickBound(t *testing.T, w *watcher, what string) {
	t.Helper()
	done := make(chan struct{})
	go func() {
		defer close(done)
		w.tick(context.Background())
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatalf("%s: the poll did not return while the hook was blocked", what)
	}
}

func awaitSource(t *testing.T, done chan error, bound time.Duration, what string) {
	t.Helper()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(bound):
		t.Fatalf("%s: the source did not return within %s", what, bound)
	}
}

func verifyTicks(statuses ...string) []scriptTick {
	var ticks []scriptTick
	for i, status := range statuses {
		ticks = append(ticks, scriptTick{
			sessions: []map[string]any{session("rpc-1", "A")},
			states:   map[string]any{"rpc-1": state(status, i)},
		})
	}
	return ticks
}

// TestVerifyHookResult guards the IS-7 hook contract: exit 0 verifies, any
// other outcome retries exactly once and then records `unverified` with the
// reason, and the hook receives the done's identity in its environment.
func TestVerifyHookResult(t *testing.T) {
	for _, tc := range []struct {
		name, control, stderr, want, detail string
		runs                                int
	}{
		{"pass", "0", "", "verified", "", 1},
		{"fail", "1", "boom\n", "unverified", "exit 1: boom", 2},
		{"fail without stderr", "1", "", "unverified", "exit 1", 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			v := newVerifyFixture(t, tc.control)
			if tc.stderr != "" {
				deliveryWrite(t, filepath.Join(v.dir, "stderr"), tc.stderr)
			}
			committed := hookSignals(t)
			c, _ := verifyCtx(t, v, 60)
			path := shortSocket(t)
			serveRPC(t, path, verifyTicks("working", "idle"))
			t.Setenv("OMOSENSE_RPC_SOCK", path)
			w := newWatcher(c, c.Out)
			done, cancel := startRun(t, c, w)
			tickBound(t, w, "completion tick")
			awaitHook(t, committed, 1)
			cancel()
			awaitSource(t, done, 5*time.Second, "hook result")

			es := pendingList(t, newPendingStore(c.State))
			if len(es) != 1 || es[0].Verify != tc.want || es[0].VerifyDetail != tc.detail {
				t.Fatalf("entry = %+v, want verify=%q detail=%q", es, tc.want, tc.detail)
			}
			envs := v.envs(t)
			if len(envs) != tc.runs {
				t.Fatalf("hook ran %d times, want %d", len(envs), tc.runs)
			}
			want := []string{"rpc", "A", "rpc-1", "", ""}
			if fmt.Sprint(envs[0]) != fmt.Sprint(want) {
				t.Fatalf("hook env = %v, want %v", envs[0], want)
			}
		})
	}
}

// TestVerifyHookCwd guards the IS-7 working directory rule: the hook runs in
// the done's cwd when it is an existing directory.
func TestVerifyHookCwd(t *testing.T) {
	v := newVerifyFixture(t, "0")
	committed := hookSignals(t)
	c, _ := verifyCtx(t, v, 60)
	dir := t.TempDir()
	path := shortSocket(t)
	row := session("rpc-1", "A")
	row["cwd"] = dir
	serveRPC(t, path, []scriptTick{
		{sessions: []map[string]any{row}, states: map[string]any{"rpc-1": state("working", 0)}},
		{sessions: []map[string]any{row}, states: map[string]any{"rpc-1": state("idle", 0)}},
	})
	t.Setenv("OMOSENSE_RPC_SOCK", path)
	w := newWatcher(c, c.Out)
	done, cancel := startRun(t, c, w)
	tickBound(t, w, "completion tick")
	awaitHook(t, committed, 1)
	cancel()
	awaitSource(t, done, 5*time.Second, "cwd hook")
	envs := v.envs(t)
	if len(envs) != 1 || envs[0][4] != dir {
		t.Fatalf("OMOSENSE_DONE_CWD = %v, want %q", envs, dir)
	}
}

// TestVerifyHookDoesNotBlockWatcher guards Risk row "a failing hook drops or
// delays the done's recording, or blocks the watcher" (IS-7): the done is
// recorded before the hook runs, and the watcher keeps observing while the
// hook is still blocked.
func TestVerifyHookDoesNotBlockWatcher(t *testing.T) {
	v := newVerifyFixture(t, "1")
	c, _ := verifyCtx(t, v, 60)
	path := shortSocket(t)
	serveRPC(t, path, verifyTicks("working", "idle", "working", "idle"))
	t.Setenv("OMOSENSE_RPC_SOCK", path)
	wait, release := v.gate(t)
	w := newWatcher(c, c.Out)
	done, cancel := startRun(t, c, w)

	ticked := make(chan struct{})
	go func() {
		defer close(ticked)
		w.tick(context.Background())
	}()
	wait()
	t.Log("the hook is blocked at the FIFO barrier; the poll that started it must still finish")
	es := pendingList(t, newPendingStore(c.State))
	if len(es) != 1 || es[0].Seq != 1 || es[0].Verify != "pending" {
		t.Fatalf("the hook delayed the done's recording: %+v", es)
	}
	select {
	case <-ticked:
	case <-time.After(5 * time.Second):
		t.Fatal("the poll did not return while the hook was blocked")
	}
	if err := os.Remove(filepath.Join(v.dir, "gate")); err != nil {
		t.Fatal(err)
	}

	tickBound(t, w, "second tick")
	tickBound(t, w, "third tick")
	es = pendingList(t, newPendingStore(c.State))
	if len(es) != 1 || es[0].Seq != 2 {
		t.Fatalf("the watcher stalled behind the hook: %+v", es)
	}
	t.Logf("a second done (seq %d) was recorded while the first hook was still blocked", es[0].Seq)

	release()
	cancel()
	awaitSource(t, done, 5*time.Second, "blocked hook")
}

// TestVerifyHookLateResultDiscarded guards Risk row "a late hook result
// overwrites a newer done" (IS-7): a hook result whose sequence no longer
// matches the stored entry is discarded, and the newer done keeps its own
// verdict.
func TestVerifyHookLateResultDiscarded(t *testing.T) {
	v := newVerifyFixture(t, "1")
	committed := hookSignals(t)
	c, _ := verifyCtx(t, v, 60)
	path := shortSocket(t)
	serveRPC(t, path, verifyTicks("working", "idle", "working", "idle"))
	t.Setenv("OMOSENSE_RPC_SOCK", path)
	wait, release := v.gate(t)
	w := newWatcher(c, c.Out)
	done, cancel := startRun(t, c, w)

	tickBound(t, w, "first tick")
	wait()
	if err := os.Remove(filepath.Join(v.dir, "gate")); err != nil {
		t.Fatal(err)
	}

	tickBound(t, w, "second tick")
	tickBound(t, w, "third tick")
	es := pendingList(t, newPendingStore(c.State))
	if len(es) != 1 || es[0].Seq != 2 || es[0].Verify != "pending" {
		t.Fatalf("newer done not recorded as pending: %+v", es)
	}
	awaitHook(t, committed, 1)
	es = pendingList(t, newPendingStore(c.State))
	if es[0].Verify != "unverified" {
		t.Fatalf("newer done verdict = %+v, want unverified", es)
	}
	t.Logf("the newer hook settled seq %d as unverified before the stale one was released", es[0].Seq)

	v.setControl(t, "0")
	release()
	awaitHook(t, committed, 1)
	cancel()
	awaitSource(t, done, 5*time.Second, "late hook result")
	es = pendingList(t, newPendingStore(c.State))
	if len(es) != 1 || es[0].Seq != 2 || es[0].Verify != "unverified" || es[0].VerifyDetail != "exit 1" {
		t.Fatalf("the stale hook result overwrote the newer done: %+v", es)
	}
}

// TestVerifyHookNotRerunAfterRestart guards the IS-7 lifecycle rule that a
// restart never re-runs a hook: an entry left pending by a previous process is
// not re-verified, it stays pending until the owner reads it.
func TestVerifyHookNotRerunAfterRestart(t *testing.T) {
	v := newVerifyFixture(t, "0")
	committed := hookSignals(t)
	c, _ := verifyCtx(t, v, 60)
	store := newPendingStore(c.State)
	if err := os.MkdirAll(c.State, 0o755); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Record("A", rpcEvent{Session: "rpc-1", ID: "A"}, true); err != nil {
		t.Fatal(err)
	}
	path := shortSocket(t)
	serveRPC(t, path, []scriptTick{
		{sessions: []map[string]any{session("rpc-1", "A")}, states: map[string]any{"rpc-1": state("idle", 0)}},
		{sessions: []map[string]any{session("rpc-1", "A")}, states: map[string]any{"rpc-1": state("idle", 1)}},
	})
	t.Setenv("OMOSENSE_RPC_SOCK", path)
	w := newWatcher(c, c.Out)
	done, cancel := startRun(t, c, w)
	tickBound(t, w, "first tick")
	tickBound(t, w, "second tick")
	// A hook started at startup would have committed long before this bound;
	// the wait observes that it never does, it does not pace the source.
	select {
	case <-committed:
		t.Fatal("a restart re-ran the hook")
	case <-time.After(time.Second):
	}
	cancel()
	awaitSource(t, done, 5*time.Second, "restart")

	if envs := v.envs(t); len(envs) != 0 {
		t.Fatalf("a restart re-ran the hook: %v", envs)
	}
	es := pendingList(t, newPendingStore(c.State))
	if len(es) != 1 || es[0].Verify != "pending" || es[0].VerifyDetail != "" {
		t.Fatalf("pending entry after a restart = %+v", es)
	}
}

// TestVerifyHookResultAfterBatch guards Risk row "a late hook result (after its
// batch) is re-sent, or is not visible in `rpc pending`" (IS-7): a result that
// lands once the batch already covered the entry is written and stays readable,
// and it never arms another send.
func TestVerifyHookResultAfterBatch(t *testing.T) {
	v := newVerifyFixture(t, "0")
	committed := hookSignals(t)
	c, _ := verifyCtx(t, v, 60)
	path := shortSocket(t)
	serveRPC(t, path, verifyTicks("working", "idle"))
	t.Setenv("OMOSENSE_RPC_SOCK", path)
	wait, release := v.gate(t)
	w := newWatcher(c, c.Out)
	done, cancel := startRun(t, c, w)

	tickBound(t, w, "first tick")
	wait()
	store := newPendingStore(c.State)
	if _, err := store.MarkNotified(1); err != nil {
		t.Fatal(err)
	}
	if un, _ := store.Unnotified(); len(un) != 0 {
		t.Fatalf("batch did not cover the entry: %+v", un)
	}

	release()
	awaitHook(t, committed, 1)
	cancel()
	awaitSource(t, done, 5*time.Second, "late result after the batch")

	es := pendingList(t, newPendingStore(c.State))
	if len(es) != 1 || es[0].Seq != 1 || es[0].Verify != "verified" {
		t.Fatalf("late result is not visible in rpc pending: %+v", es)
	}
	if un, _ := store.Unnotified(); len(un) != 0 {
		t.Fatalf("the late result armed another send: %+v", un)
	}
}

// TestVerifyHookShutdown guards Risk row "shutdown during a running hook writes
// a verify result or delays the source's return" (IS-7): cancelling while the
// hook is blocked returns the source promptly and leaves the entry pending.
func TestVerifyHookShutdown(t *testing.T) {
	v := newVerifyFixture(t, "0")
	c, _ := verifyCtx(t, v, 60)
	path := shortSocket(t)
	serveRPC(t, path, verifyTicks("working", "idle"))
	t.Setenv("OMOSENSE_RPC_SOCK", path)
	wait, _ := v.gate(t)
	w := newWatcher(c, c.Out)
	done, cancel := startRun(t, c, w)

	tickBound(t, w, "first tick")
	wait()
	es := pendingList(t, newPendingStore(c.State))
	if len(es) != 1 || es[0].Verify != "pending" {
		t.Fatalf("hook did not start pending: %+v", es)
	}
	t.Log("hook is blocked; cancelling the source")

	cancel()
	awaitSource(t, done, 3*time.Second, "shutdown during a blocked hook")
	es = pendingList(t, newPendingStore(c.State))
	if len(es) != 1 || es[0].Verify != "pending" || es[0].VerifyDetail != "" {
		t.Fatalf("a cancelled hook wrote a verify result: %+v", es)
	}
}

// TestSilentSessionReportedOnce guards Risk rows "a silent working session is
// never reported" and "silent-session repeats every tick instead of once"
// (IS-6): a registered session working with no status change and no
// messageCount growth is reported once per silence window, and a growth
// re-arms it.
func TestSilentSessionReportedOnce(t *testing.T) {
	var b bytes.Buffer
	c := rpcCtx(t, &b, `{"job":{"session_id":"d"}}`)
	c.Profile.SilentMinutes = 30
	clock := time.Date(2026, 10, 5, 0, 0, 0, 0, time.UTC)
	old := nowFn
	nowFn = func() time.Time { return clock }
	t.Cleanup(func() { nowFn = old })

	counts := []int{1, 1, 1, 1, 2, 2, 2}
	var ticks []scriptTick
	for _, n := range counts {
		ticks = append(ticks, scriptTick{
			sessions: []map[string]any{session("rpc-1", "d")},
			states:   map[string]any{"rpc-1": state("working", n)},
		})
	}
	path := shortSocket(t)
	serveRPC(t, path, ticks)
	t.Setenv("OMOSENSE_RPC_SOCK", path)
	w := newWatcher(c, c.Out)

	steps := []time.Duration{0, 29 * time.Minute, 2 * time.Minute, 5 * time.Minute, 5 * time.Minute, 31 * time.Minute, time.Minute}
	for i, d := range steps {
		clock = clock.Add(d)
		w.tick(context.Background())
		want := 0
		if i >= 2 {
			want = 1
		}
		if i >= 5 {
			want = 2
		}
		if got := len(logLines(&b, "silent-session")); got != want {
			t.Fatalf("tick %d (t+%s, count %d): silent lines = %d, want %d\n%s", i, clock, counts[i], got, want, b.String())
		}
	}
	lines := logLines(&b, "silent-session")
	var payload struct {
		Source       string `json:"source"`
		Session      string `json:"session"`
		ID           string `json:"id"`
		Thread       string `json:"thread"`
		Since        string `json:"since"`
		Minutes      int    `json:"minutes"`
		MessageCount int    `json:"messageCount"`
	}
	if err := json.Unmarshal([]byte(lines[0]), &payload); err != nil {
		t.Fatal(err)
	}
	if payload.Source != "rpc" || payload.Session != "rpc-1" || payload.ID != "d" || payload.Thread != "job" ||
		payload.Since != "2026-10-05T00:00:00.000Z" || payload.Minutes != 31 || payload.MessageCount != 1 {
		t.Fatalf("silent payload = %+v", payload)
	}
	if err := json.Unmarshal([]byte(lines[1]), &payload); err != nil {
		t.Fatal(err)
	}
	if payload.Since != "2026-10-05T00:41:00.000Z" || payload.Minutes != 31 || payload.MessageCount != 2 {
		t.Fatalf("second silent payload = %+v", payload)
	}
	t.Logf("silent-session lines: %v", lines)
	t.Log("cleanup: test Cleanup closes and joins fake RPC, removes temporary socket/state, restores environment, sleepFn and nowFn")
}

// countLines counts the rendered lines that carry substr.
func countLines(lines []string, substr string) int {
	n := 0
	for _, l := range lines {
		if strings.Contains(l, substr) {
			n++
		}
	}
	return n
}

// TestSilentSessionCountDecreaseDoesNotRearm guards IS-6's growth rule: a
// messageCount is compared with the immediately preceding sample, so a session
// whose count drops and then grows on every poll is never silent, while a
// frozen session is still reported once. The reviewed defect kept the pre-drop
// high-water mark, so real growth below it was read as silence.
func TestSilentSessionCountDecreaseDoesNotRearm(t *testing.T) {
	var b bytes.Buffer
	c := rpcCtx(t, &b, `{"grow":{"session_id":"g"},"frozen":{"session_id":"c"}}`)
	c.Profile.SilentMinutes = 30
	clock := time.Date(2026, 10, 5, 0, 0, 0, 0, time.UTC)
	old := nowFn
	nowFn = func() time.Time { return clock }
	t.Cleanup(func() { nowFn = old })

	counts := []int{100, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17}
	var ticks []scriptTick
	for _, n := range counts {
		ticks = append(ticks, scriptTick{
			sessions: []map[string]any{session("rpc-g", "g"), session("rpc-c", "c")},
			states: map[string]any{
				"rpc-g": state("working", n),
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
		if got := countLines(lines, `"id":"g"`); got != 0 {
			t.Fatalf("tick %d (count %d): the growing session was reported silent %d time(s): %v", i, n, got, lines)
		}
		want := 0
		if i >= 6 {
			want = 1
		}
		if got := countLines(lines, `"id":"c"`); got != want {
			t.Fatalf("tick %d: frozen control silent lines = %d, want %d: %v", i, got, want, lines)
		}
	}
	t.Logf("silent-session lines: %v", logLines(&b, "silent-session"))
	t.Log("cleanup: test Cleanup closes and joins fake RPC, removes temporary socket/state, restores environment and nowFn")
}
