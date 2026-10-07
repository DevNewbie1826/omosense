package rpc

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/DevNewbie1826/omosense/internal/core"
)

type fakeDelivery struct {
	dir string
}

func deliveryWrite(t *testing.T, path, value string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(value), 0o600); err != nil {
		t.Fatal(err)
	}
}

// fakeDeliveryCLI is the omo shim: it records every argv, answers `thread list`
// from the list file (gated by listcontrol) and `thread send` with the control
// exit code, optionally through a FIFO barrier.
func fakeDeliveryCLI(t *testing.T) *fakeDelivery {
	t.Helper()
	f := &fakeDelivery{dir: t.TempDir()}
	script := `#!/bin/sh
dir=${0%/*}
printf '%s\000' "$@" >> "$dir/argv"
printf '\000' >> "$dir/argv"
if [ "$2" = list ]; then
  IFS= read -r lcode < "$dir/listcontrol"
  if [ "$lcode" != 0 ]; then printf 'list refused\n' >&2; exit "$lcode"; fi
  if [ -p "$dir/listready" ]; then
    printf 'ready\n' > "$dir/listready"
    IFS= read -r lrelease < "$dir/listrelease"
  fi
  while IFS= read -r line; do printf '%s\n' "$line"; done < "$dir/list"
  exit 0
fi
if [ -p "$dir/ready" ]; then
  printf 'ready\n' > "$dir/ready"
  IFS= read -r release < "$dir/release"
fi
IFS= read -r code < "$dir/control"
if [ "$code" != 0 ]; then printf 'send refused\n' >&2; fi
exit "$code"
`
	path := filepath.Join(f.dir, "omo")
	deliveryWrite(t, path, script)
	if err := os.Chmod(path, 0o700); err != nil {
		t.Fatal(err)
	}
	deliveryWrite(t, filepath.Join(f.dir, "control"), "0\n")
	deliveryWrite(t, filepath.Join(f.dir, "listcontrol"), "0\n")
	deliveryWrite(t, filepath.Join(f.dir, "list"), "[]\n")
	t.Setenv("OMOSENSE_OMO", path)
	return f
}

func (f *fakeDelivery) calls(t *testing.T) [][]string {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(f.dir, "argv"))
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		t.Fatal(err)
	}
	var calls [][]string
	for _, call := range bytes.Split(b, []byte{0, 0}) {
		if len(call) != 0 {
			calls = append(calls, strings.Split(string(call), "\x00"))
		}
	}
	t.Logf("real shell argv: %q", calls)
	return calls
}

func (f *fakeDelivery) sub(t *testing.T, verb string) [][]string {
	t.Helper()
	var out [][]string
	for _, call := range f.calls(t) {
		if len(call) > 1 && call[1] == verb {
			out = append(out, call)
		}
	}
	return out
}

const deliveryFolder = "proj"

// batchFixture is one project folder: <dir>/.omosense + its state dir, the omo
// shim, and an injected clock. The clock is mutex-guarded because a test may
// advance it while the batcher's Run goroutine reads it.
type batchFixture struct {
	b    *batcher
	f    *fakeDelivery
	logs *bytes.Buffer
	mu   sync.Mutex
	now  time.Time
}

func newBatchFixture(t *testing.T) *batchFixture {
	t.Helper()
	f := fakeDeliveryCLI(t)
	dir := filepath.Join(t.TempDir(), deliveryFolder)
	state := filepath.Join(dir, "state")
	if err := os.MkdirAll(state, 0o755); err != nil {
		t.Fatal(err)
	}
	var logs bytes.Buffer
	out := core.NewOut(&logs)
	x := &batchFixture{f: f, logs: &logs, now: time.Date(2026, 10, 5, 0, 0, 0, 0, time.UTC)}
	old := nowFn
	nowFn = x.clock
	t.Cleanup(func() { nowFn = old })
	c := &core.Ctx{Dir: dir, State: state, Profile: core.Profile{}, Out: out}
	x.b = newBatcher(c, newPendingStore(state), out)
	return x
}

func (x *batchFixture) clock() time.Time {
	x.mu.Lock()
	defer x.mu.Unlock()
	return x.now
}

func (x *batchFixture) advance(d time.Duration) {
	x.mu.Lock()
	x.now = x.now.Add(d)
	x.mu.Unlock()
}

// setClock forces the injected clock, so a test can publish two records that
// carry the same timestamp.
func (x *batchFixture) setClock(at time.Time) {
	x.mu.Lock()
	x.now = at
	x.mu.Unlock()
}

func (x *batchFixture) pass(t *testing.T) {
	t.Helper()
	x.b.pass(context.Background())
}

// subscribe writes the folder's subscription and makes the shim report the
// session alive.
func (x *batchFixture) subscribe(t *testing.T, session string) {
	t.Helper()
	if err := writeSubscription(x.b.c.State, newSubscription(session, x.clock())); err != nil {
		t.Fatal(err)
	}
	x.alive(t, session, true)
}

func (x *batchFixture) alive(t *testing.T, session string, ok bool) {
	t.Helper()
	rows := "[]"
	if ok {
		rows = fmt.Sprintf(`[{"thread_id":"%s","sessionId":"%s","alive":true}]`, session, session)
	}
	deliveryWrite(t, filepath.Join(x.f.dir, "list"), rows+"\n")
}

// replaceSubscription models a real `omosense rpc unsubscribe` followed by
// `omosense rpc subscribe <session>` completing while a liveness check is in
// flight (IS-7).
func (x *batchFixture) replaceSubscription(t *testing.T, session string) {
	t.Helper()
	args := x.b.c.Args
	x.b.c.Args = []string{"unsubscribe"}
	if code := unsubscribe(x.b.c); code != 0 {
		t.Fatalf("unsubscribe exit %d", code)
	}
	x.b.c.Args = []string{"subscribe", session}
	if code := subscribe(x.b.c); code != 0 {
		t.Fatalf("subscribe exit %d", code)
	}
	x.b.c.Args = args
}

// removeBarrier unlinks gated FIFOs so a later pass does not wait on them.
func (x *batchFixture) removeBarrier(t *testing.T, names ...string) {
	t.Helper()
	for _, n := range names {
		if err := os.Remove(filepath.Join(x.f.dir, n)); err != nil {
			t.Fatal(err)
		}
	}
}

// TestBatchDebounce guards Risk row "several dones within 5 minutes produce
// more than one notification, or none": the quiet window restarts on every
// done and exactly one batch leaves, never re-sent.
func TestBatchDebounce(t *testing.T) {
	x := newBatchFixture(t)
	x.subscribe(t, "MAINQA")
	pendingRecord(t, x.b.store, "X", "rpc-x")
	x.pass(t)
	if sends := x.f.sub(t, "send"); len(sends) != 0 {
		t.Fatalf("sent inside the quiet window: %q", sends)
	}
	x.advance(4 * time.Minute)
	x.pass(t)
	if sends := x.f.sub(t, "send"); len(sends) != 0 {
		t.Fatalf("sent one minute before the window: %q", sends)
	}
	pendingRecord(t, x.b.store, "Y", "rpc-y")
	x.advance(4 * time.Minute)
	x.pass(t)
	if sends := x.f.sub(t, "send"); len(sends) != 0 {
		t.Fatalf("window did not restart on the newer done: %q", sends)
	}
	x.advance(time.Minute)
	x.pass(t)
	sends := x.f.sub(t, "send")
	if len(sends) != 1 {
		t.Fatalf("batch count = %q", sends)
	}
	send := sends[0]
	if len(send) != 8 || send[0] != "thread" || send[1] != "send" || send[2] != "MAINQA" ||
		send[4] != "--all-scope" || send[5] != "--idempotency-key" ||
		send[6] != "omosense-rpc-batch-2" || send[7] != "--json" {
		t.Fatalf("send argv = %q", send)
	}
	for _, want := range []string{"X", "Y", "seq: 1", "seq: 2", "omosense rpc ack X 1", "omosense rpc ack Y 2"} {
		if !strings.Contains(send[3], want) {
			t.Fatalf("batch text lacks %q:\n%s", want, send[3])
		}
	}
	if un, err := x.b.store.Unnotified(); err != nil || len(un) != 0 {
		t.Fatalf("un-notified after the batch: %+v %v", un, err)
	}
	x.advance(24 * time.Hour)
	x.pass(t)
	if sends := x.f.sub(t, "send"); len(sends) != 1 {
		t.Fatalf("re-sent without a new done: %q", sends)
	}
	t.Log(x.logs.String())
}

// TestBatchNewestSeq guards Risk row "a newer done of the same id inside the
// window is lost (older seq shown/acked)".
func TestBatchNewestSeq(t *testing.T) {
	x := newBatchFixture(t)
	x.subscribe(t, "MAINQA")
	pendingRecord(t, x.b.store, "D7", "rpc-7")
	x.advance(30 * time.Second)
	second := pendingRecord(t, x.b.store, "D7", "rpc-9")
	if second.Seq != 2 || second.Count != 2 {
		t.Fatalf("second record = %+v", second)
	}
	x.advance(batchQuiet)
	x.pass(t)
	sends := x.f.sub(t, "send")
	if len(sends) != 1 {
		t.Fatalf("sends = %q", sends)
	}
	text := sends[0][3]
	if !strings.Contains(text, "seq: 2") || !strings.Contains(text, "count: 2") ||
		strings.Contains(text, "seq: 1") || !strings.Contains(text, "omosense rpc ack D7 2") {
		t.Fatalf("batch text = %q", text)
	}
	if sends[0][6] != "omosense-rpc-batch-2" {
		t.Fatalf("idempotency key = %q", sends[0][6])
	}
	if result, err := x.b.store.Ack("D7", 1, true); err != nil || result != "newer" {
		t.Fatalf("stale ack = %s %v", result, err)
	}
	if result, err := x.b.store.Ack("D7", 2, true); err != nil || result != "acked" {
		t.Fatalf("ack = %s %v", result, err)
	}
	if es := pendingList(t, x.b.store); len(es) != 0 {
		t.Fatalf("pending = %+v", es)
	}
}

// TestBatchRecordDuringSend guards Risk row "a done recorded while a batch is
// being sent is lost": the in-flight record keeps seq > notified_seq and arms
// the next window.
func TestBatchRecordDuringSend(t *testing.T) {
	x := newBatchFixture(t)
	x.subscribe(t, "MAINQA")
	pendingRecord(t, x.b.store, "D7", "rpc-7")
	x.advance(batchQuiet)
	wait, release := deliveryBarrier(t, x.f)
	done := deliveryAsyncPass(t, x.b)
	wait()
	t.Logf("first send entered the FIFO barrier; the shim holds it until release")
	second := pendingRecord(t, x.b.store, "D7", "rpc-9")
	t.Logf("in-flight record D7 seq=%d recorded while the send is inside the barrier", second.Seq)
	release()
	done()
	un, err := x.b.store.Unnotified()
	if err != nil || len(un) != 1 || un[0].Seq != second.Seq {
		t.Fatalf("in-flight record = %+v %v", un, err)
	}
	// No next send needs a barrier; remove it once its process has exited.
	for _, name := range []string{"ready", "release"} {
		if err := os.Remove(filepath.Join(x.f.dir, name)); err != nil {
			t.Fatal(err)
		}
	}
	x.advance(batchQuiet)
	x.pass(t)
	sends := x.f.sub(t, "send")
	if len(sends) != 2 || sends[1][6] != "omosense-rpc-batch-2" || !strings.Contains(sends[1][3], "seq: 2") {
		t.Fatalf("next batch = %q", sends)
	}
}

// TestBatchRestartFires guards Risk row "restart with un-notified entries never
// notifies" and the B3 rule fire = max(start, lastDoneAt+5m).
func TestBatchRestartFires(t *testing.T) {
	x := newBatchFixture(t)
	pendingRecord(t, x.b.store, "D7", "rpc-7")
	x.subscribe(t, "MAINQA")
	x.advance(4*time.Minute + 59*time.Second)
	restarted := newBatcher(x.b.c, newPendingStore(x.b.c.State), x.b.sink)
	restarted.pass(context.Background())
	if sends := x.f.sub(t, "send"); len(sends) != 0 {
		t.Fatalf("restart fired before the window: %q", sends)
	}
	x.advance(time.Second)
	restarted.pass(context.Background())
	if sends := x.f.sub(t, "send"); len(sends) != 1 {
		t.Fatalf("restart did not fire at lastDoneAt+5m: %q", sends)
	}
	pendingRecord(t, x.b.store, "D8", "rpc-8")
	x.advance(24 * time.Hour)
	late := newBatcher(x.b.c, newPendingStore(x.b.c.State), x.b.sink)
	late.pass(context.Background())
	if sends := x.f.sub(t, "send"); len(sends) != 2 {
		t.Fatalf("a long-late restart did not fire on its first pass: %q", sends)
	}
}

// TestBatchNoSubscriber guards Risk row "no subscriber -> something is sent /
// pending deleted".
func TestBatchNoSubscriber(t *testing.T) {
	x := newBatchFixture(t)
	pendingRecord(t, x.b.store, "D7", "rpc-7")
	x.advance(batchQuiet)
	x.pass(t)
	if sends := x.f.sub(t, "send"); len(sends) != 0 {
		t.Fatalf("sent without a subscriber: %q", sends)
	}
	if es := pendingList(t, x.b.store); len(es) != 1 || es[0].ID != "D7" {
		t.Fatalf("pending = %+v", es)
	}
	if un, _ := x.b.store.Unnotified(); len(un) != 0 {
		t.Fatalf("drop left the batch un-notified: %+v", un)
	}
	if strings.Count(x.logs.String(), "rpc batch dropped: no subscriber") != 1 {
		t.Fatalf("log = %s", x.logs.String())
	}
	x.advance(24 * time.Hour)
	x.pass(t)
	if sends := x.f.sub(t, "send"); len(sends) != 0 {
		t.Fatalf("dropped batch was pushed later: %q", sends)
	}
	if strings.Count(x.logs.String(), "rpc batch dropped: no subscriber") != 1 {
		t.Fatalf("log not deduplicated: %s", x.logs.String())
	}
}

// TestBatchDeadSubscriber guards Risk row "dead subscriber not unsubscribed, or
// sent anyway".
func TestBatchDeadSubscriber(t *testing.T) {
	for _, tc := range []struct{ name, rows string }{
		{"absent", `[]`},
		{"not alive", `[{"thread_id":"DEAD","sessionId":"DEAD","alive":false}]`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			x := newBatchFixture(t)
			x.subscribe(t, "DEAD")
			deliveryWrite(t, filepath.Join(x.f.dir, "list"), tc.rows+"\n")
			pendingRecord(t, x.b.store, "D7", "rpc-7")
			x.advance(batchQuiet)
			x.pass(t)
			if sends := x.f.sub(t, "send"); len(sends) != 0 {
				t.Fatalf("sent to a dead subscriber: %q", sends)
			}
			if sub, err := readSubscription(x.b.c.State); err != nil || sub != nil {
				t.Fatalf("subscription kept: %+v %v", sub, err)
			}
			if !strings.Contains(x.logs.String(), "rpc batch dropped: subscriber DEAD not alive; unsubscribed") {
				t.Fatalf("log = %s", x.logs.String())
			}
			if es := pendingList(t, x.b.store); len(es) != 1 {
				t.Fatalf("pending = %+v", es)
			}
		})
	}
}

// TestBatchStaleLivenessKeepsReplacement guards IS-7: a subscription replaced
// while the liveness check for the previous one is in flight must survive, and
// its batch must not be consumed by the stale result. Removal is
// compare-and-remove under rpc-subscription.lock.
func TestBatchStaleLivenessKeepsReplacement(t *testing.T) {
	x := newBatchFixture(t)
	x.subscribe(t, "DEAD")
	deliveryWrite(t, filepath.Join(x.f.dir, "list"), `[{"thread_id":"LIVE","sessionId":"LIVE","alive":true}]`+"\n")
	pendingRecord(t, x.b.store, "D7", "rpc-7")
	x.advance(batchQuiet)
	wait, release := listBarrier(t, x.f)
	done := deliveryAsyncPass(t, x.b)
	wait()
	x.replaceSubscription(t, "LIVE")
	release()
	done()
	sub, err := readSubscription(x.b.c.State)
	if err != nil || sub == nil || sub.Session != "LIVE" {
		t.Fatalf("stale liveness check deleted the replacement: %+v %v", sub, err)
	}
	if un, _ := x.b.store.Unnotified(); len(un) != 1 {
		t.Fatalf("stale liveness result consumed the batch: %+v", un)
	}
	if sends := x.f.sub(t, "send"); len(sends) != 0 {
		t.Fatalf("batch sent from the stale liveness result: %q", sends)
	}
	x.removeBarrier(t, "listready", "listrelease")
	x.advance(deliverRetry)
	x.pass(t)
	sends := x.f.sub(t, "send")
	if len(sends) != 1 || sends[0][2] != "LIVE" {
		t.Fatalf("replacement did not receive the batch: %q", sends)
	}
	if un, _ := x.b.store.Unnotified(); len(un) != 0 {
		t.Fatalf("replacement batch not consumed: %+v", un)
	}
}

// TestBatchStaleLivenessAliveDoesNotSend guards the symmetric half of IS-7: a
// subscription replaced while the liveness check for the previous one is in
// flight must not receive the batch. The stale "alive" answer belongs to the
// examined subscription, so the batch waits for the replacement instead.
func TestBatchStaleLivenessAliveDoesNotSend(t *testing.T) {
	x := newBatchFixture(t)
	x.subscribe(t, "A")
	pendingRecord(t, x.b.store, "D7", "rpc-7")
	x.advance(batchQuiet)
	wait, release := listBarrier(t, x.f)
	done := deliveryAsyncPass(t, x.b)
	wait()
	x.replaceSubscription(t, "B")
	release()
	done()
	if sends := x.f.sub(t, "send"); len(sends) != 0 {
		t.Fatalf("batch sent to the replaced subscription: %q", sends)
	}
	if un, _ := x.b.store.Unnotified(); len(un) != 1 {
		t.Fatalf("stale alive result consumed the batch: %+v", un)
	}
	if sub, err := readSubscription(x.b.c.State); err != nil || sub == nil || sub.Session != "B" {
		t.Fatalf("subscription = %+v %v", sub, err)
	}
	x.removeBarrier(t, "listready", "listrelease")
	x.alive(t, "B", true)
	x.advance(deliverRetry)
	x.pass(t)
	sends := x.f.sub(t, "send")
	if len(sends) != 1 || sends[0][2] != "B" {
		t.Fatalf("replacement did not receive the batch: %q", sends)
	}
	if un, _ := x.b.store.Unnotified(); len(un) != 0 {
		t.Fatalf("replacement batch not consumed: %+v", un)
	}
}

// TestBatchStaleLivenessKeepsRenewal guards the IS-7 renewal case: a
// subscription for the SAME session republished while the liveness check for
// the previous record is in flight is a different instance, so the stale dead
// result must not remove it or consume its batch. The batch is delivered after
// a fresh live check. The fixture clock is frozen, so the renewal shares the
// millisecond subscribed_at of the record it replaces - only the instance
// token distinguishes them.
func TestBatchStaleLivenessKeepsRenewal(t *testing.T) {
	x := newBatchFixture(t)
	start := x.clock()
	x.subscribe(t, "SUB")
	deliveryWrite(t, filepath.Join(x.f.dir, "list"), "[]\n") // stale list: SUB absent
	pendingRecord(t, x.b.store, "D7", "rpc-7")
	x.advance(batchQuiet)
	passNow := x.clock()
	wait, release := listBarrier(t, x.f)
	done := deliveryAsyncPass(t, x.b)
	wait()
	examined, err := readSubscription(x.b.c.State)
	if err != nil || examined == nil {
		t.Fatalf("examined subscription = %+v %v", examined, err)
	}
	// Republish the same session with the clock reading of the record being
	// replaced, so the renewal carries the very same subscribed_at: only the
	// instance token can tell the two records apart.
	x.setClock(start)
	x.replaceSubscription(t, "SUB")
	renewed, err := readSubscription(x.b.c.State)
	if err != nil || renewed == nil {
		t.Fatalf("renewed subscription = %+v %v", renewed, err)
	}
	if renewed.SubscribedAt != examined.SubscribedAt {
		t.Fatalf("scenario drift: renewal moved the clock, examined %+v renewed %+v", examined, renewed)
	}
	if renewed.Instance == examined.Instance {
		t.Fatalf("renewal reused the instance token: %+v", renewed)
	}
	release()
	done()
	sub, err := readSubscription(x.b.c.State)
	if err != nil || sub == nil || sub.Session != "SUB" || sub.Instance != renewed.Instance {
		t.Fatalf("stale dead result removed the renewal: %+v %v", sub, err)
	}
	if un, _ := x.b.store.Unnotified(); len(un) != 1 {
		t.Fatalf("stale dead result consumed the batch: %+v", un)
	}
	if got := notifiedSeq(t, x.b.store); got != 0 {
		t.Fatalf("stale dead result advanced notified_seq to %d", got)
	}
	if sends := x.f.sub(t, "send"); len(sends) != 0 {
		t.Fatalf("batch sent from the stale dead result: %q", sends)
	}
	x.removeBarrier(t, "listready", "listrelease")
	x.alive(t, "SUB", true)
	x.setClock(passNow)
	x.advance(deliverRetry)
	x.pass(t)
	sends := x.f.sub(t, "send")
	if len(sends) != 1 || sends[0][2] != "SUB" {
		t.Fatalf("renewal did not receive the batch: %q", sends)
	}
	if un, _ := x.b.store.Unnotified(); len(un) != 0 {
		t.Fatalf("renewal batch not consumed: %+v", un)
	}
}

// notifiedSeq reads the raw watermark, so an assertion cannot be satisfied by
// the un-notified view alone.
func notifiedSeq(t *testing.T, s *pendingStore) uint64 {
	t.Helper()
	b, err := os.ReadFile(s.path)
	if err != nil {
		t.Fatal(err)
	}
	var f pendingFile
	if err := json.Unmarshal(b, &f); err != nil {
		t.Fatal(err)
	}
	return f.NotifiedSeq
}

// TestBatchRetryRespectsQuietWindow guards IS-6: a done recorded during a
// retry waits its own five quiet minutes, while a plain retry with no new done
// still fires after the one-minute backoff.
func TestBatchRetryRespectsQuietWindow(t *testing.T) {
	x := newBatchFixture(t)
	x.subscribe(t, "MAINQA")
	pendingRecord(t, x.b.store, "X", "rpc-x")
	x.advance(batchQuiet)
	deliveryWrite(t, filepath.Join(x.f.dir, "control"), "1\n")
	x.pass(t)
	if sends := x.f.sub(t, "send"); len(sends) != 1 {
		t.Fatalf("first attempt = %q", sends)
	}
	second := pendingRecord(t, x.b.store, "Y", "rpc-y")
	x.advance(deliverRetry)
	deliveryWrite(t, filepath.Join(x.f.dir, "control"), "0\n")
	x.pass(t)
	if sends := x.f.sub(t, "send"); len(sends) != 1 {
		t.Fatalf("new done sent inside its own quiet window: %q", sends)
	}
	x.advance(batchQuiet - deliverRetry)
	x.pass(t)
	sends := x.f.sub(t, "send")
	if len(sends) != 2 {
		t.Fatalf("quiet window did not release the retry: %q", sends)
	}
	text := sends[1][3]
	if !strings.Contains(text, "X") || !strings.Contains(text, "Y") ||
		!strings.Contains(text, fmt.Sprintf("seq: %d", second.Seq)) {
		t.Fatalf("batch text = %q", text)
	}
	if un, _ := x.b.store.Unnotified(); len(un) != 0 {
		t.Fatalf("batch not consumed: %+v", un)
	}
	// A plain retry with no new done still fires after one minute.
	pendingRecord(t, x.b.store, "Z", "rpc-z")
	x.advance(batchQuiet)
	deliveryWrite(t, filepath.Join(x.f.dir, "control"), "1\n")
	x.pass(t)
	if sends := x.f.sub(t, "send"); len(sends) != 3 {
		t.Fatalf("plain retry attempt = %q", sends)
	}
	x.advance(deliverRetry)
	deliveryWrite(t, filepath.Join(x.f.dir, "control"), "0\n")
	x.pass(t)
	if sends := x.f.sub(t, "send"); len(sends) != 4 {
		t.Fatalf("plain retry did not fire after one minute: %q", sends)
	}
}

// TestBatchAckCommandQuotedPath guards IS-9/B1: the printed ack command and the
// overflow line must survive a project path containing a single quote. Both are
// executed through /bin/sh, the surface the subscriber's session uses, and the
// shim reports the exact argv and environment the shell produced.
func TestBatchAckCommandQuotedPath(t *testing.T) {
	root := t.TempDir()
	dir := filepath.Join(root, "quote'proj name")
	state := filepath.Join(dir, "state")
	if err := os.MkdirAll(state, 0o755); err != nil {
		t.Fatal(err)
	}
	shimDir := filepath.Join(root, "bin")
	if err := os.MkdirAll(shimDir, 0o755); err != nil {
		t.Fatal(err)
	}
	logPath := filepath.Join(root, "argv.log")
	shim := `#!/bin/sh
{
  printf 'argv:'
  for a in "$@"; do printf ' [%s]' "$a"; done
  printf '\nDIR=[%s]\nSTATE=[%s]\n--\n' "$OMOSENSE_DIR" "$OMOSENSE_STATE"
} >> "$OMOSENSE_ACK_LOG"
case "$2" in
  ack) printf 'ACK {"id":"D7","seq":3,"result":"acked"}\n' ;;
  pending) printf 'PENDING {"id":"D7","seq":3}\n' ;;
  *) printf 'unexpected verb %s\n' "$2" >&2; exit 3 ;;
esac
`
	if err := os.WriteFile(filepath.Join(shimDir, "omosense"), []byte(shim), 0o700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", shimDir+":"+os.Getenv("PATH"))
	t.Setenv("OMOSENSE_ACK_LOG", logPath)
	e := pendingEntry{ID: "D7", Session: "rpc-7", Seq: 3, Count: 1, DoneAt: "2026-10-05T00:00:00.000Z"}
	const marker = "+2 more: "
	overflow := overflowLine(core.RPCCfg{}, 2, dir, state)
	if !strings.HasPrefix(overflow, marker) {
		t.Fatalf("overflow line = %q", overflow)
	}
	for _, run := range []struct{ name, command, want string }{
		{"ack", ackCommand(e, dir, state),
			fmt.Sprintf("argv: [rpc] [ack] [D7] [3]\nDIR=[%s]\nSTATE=[%s]\n--\n", dir, state)},
		{"pending", strings.TrimPrefix(overflow, marker),
			fmt.Sprintf("argv: [rpc] [pending]\nDIR=[%s]\nSTATE=[%s]\n--\n", dir, state)},
	} {
		before, err := os.ReadFile(logPath)
		if err != nil && !os.IsNotExist(err) {
			t.Fatal(err)
		}
		out, err := exec.Command("/bin/sh", "-c", run.command).CombinedOutput()
		if err != nil {
			t.Fatalf("%s command failed: %v\ncommand: %s\noutput: %s", run.name, err, run.command, out)
		}
		if run.name == "ack" && !strings.Contains(string(out), `"result":"acked"`) {
			t.Fatalf("ack output = %q", out)
		}
		after, err := os.ReadFile(logPath)
		if err != nil {
			t.Fatal(err)
		}
		if got := string(after[len(before):]); got != run.want {
			t.Fatalf("%s shell environment = %q\nwant %q\ncommand: %s", run.name, got, run.want, run.command)
		}
	}
}

// TestBatchSendFailure guards Risk row "transient send failure drops the
// batch".
func TestBatchSendFailure(t *testing.T) {
	x := newBatchFixture(t)
	x.subscribe(t, "MAINQA")
	pendingRecord(t, x.b.store, "D7", "rpc-7")
	x.advance(batchQuiet)
	deliveryWrite(t, filepath.Join(x.f.dir, "control"), "1\n")
	x.pass(t)
	if sends := x.f.sub(t, "send"); len(sends) != 1 {
		t.Fatalf("first attempt = %q", sends)
	}
	if un, _ := x.b.store.Unnotified(); len(un) != 1 {
		t.Fatalf("failed send consumed the batch: %+v", un)
	}
	x.pass(t)
	if sends := x.f.sub(t, "send"); len(sends) != 1 {
		t.Fatalf("retried before the delay: %q", sends)
	}
	x.advance(deliverRetry)
	deliveryWrite(t, filepath.Join(x.f.dir, "control"), "0\n")
	x.pass(t)
	sends := x.f.sub(t, "send")
	if len(sends) != 2 || sends[1][6] != "omosense-rpc-batch-1" {
		t.Fatalf("retry = %q", sends)
	}
	if un, _ := x.b.store.Unnotified(); len(un) != 0 {
		t.Fatalf("retry did not consume the batch: %+v", un)
	}
	if strings.Contains(x.logs.String(), "rpc batch dropped") {
		t.Fatalf("failed send dropped the batch: %s", x.logs.String())
	}
}

// TestBatchListFailure guards Risk row "an omo outage/timeout unsubscribes the
// live session and drops the batch" (B4).
func TestBatchListFailure(t *testing.T) {
	for _, tc := range []struct{ name, control, rows string }{
		{"exit 1", "1\n", "[]\n"},
		{"bad json", "0\n", "not json\n"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			x := newBatchFixture(t)
			x.subscribe(t, "MAINQA")
			deliveryWrite(t, filepath.Join(x.f.dir, "listcontrol"), tc.control)
			deliveryWrite(t, filepath.Join(x.f.dir, "list"), tc.rows)
			pendingRecord(t, x.b.store, "D7", "rpc-7")
			x.advance(batchQuiet)
			x.pass(t)
			if sends := x.f.sub(t, "send"); len(sends) != 0 {
				t.Fatalf("sent despite a failed list: %q", sends)
			}
			if sub, err := readSubscription(x.b.c.State); err != nil || sub == nil || sub.Session != "MAINQA" {
				t.Fatalf("live subscription dropped on a list failure: %+v %v", sub, err)
			}
			if un, _ := x.b.store.Unnotified(); len(un) != 1 {
				t.Fatalf("list failure consumed the batch: %+v", un)
			}
			x.advance(deliverRetry)
			deliveryWrite(t, filepath.Join(x.f.dir, "listcontrol"), "0\n")
			x.alive(t, "MAINQA", true)
			x.pass(t)
			if sends := x.f.sub(t, "send"); len(sends) != 1 {
				t.Fatalf("retry after the outage = %q", sends)
			}
			if un, _ := x.b.store.Unnotified(); len(un) != 0 {
				t.Fatalf("retry did not consume the batch: %+v", un)
			}
		})
	}
}

// TestBatchSelfAck guards the adopted note that the subscriber is matched by
// the entry's durable id and by its session handle.
func TestBatchSelfAck(t *testing.T) {
	t.Run("mixed", func(t *testing.T) {
		x := newBatchFixture(t)
		x.subscribe(t, "MAINQA")
		pendingRecord(t, x.b.store, "MAINQA", "rpc-self")
		pendingRecord(t, x.b.store, "D7", "MAINQA")
		normal := pendingRecord(t, x.b.store, "D8", "rpc-8")
		x.advance(batchQuiet)
		x.pass(t)
		sends := x.f.sub(t, "send")
		if len(sends) != 1 {
			t.Fatalf("sends = %q", sends)
		}
		if strings.Contains(sends[0][3], "MAINQA") || strings.Contains(sends[0][3], "D7") {
			t.Fatalf("self completion was sent: %q", sends[0][3])
		}
		if !strings.Contains(sends[0][3], "D8") {
			t.Fatalf("normal completion missing: %q", sends[0][3])
		}
		if sends[0][6] != "omosense-rpc-batch-3" {
			t.Fatalf("idempotency key = %q", sends[0][6])
		}
		es := pendingList(t, x.b.store)
		if len(es) != 1 || es[0].ID != normal.ID {
			t.Fatalf("self completions not acked: %+v", es)
		}
	})
	t.Run("only self", func(t *testing.T) {
		x := newBatchFixture(t)
		x.subscribe(t, "MAINQA")
		pendingRecord(t, x.b.store, "MAINQA", "rpc-self")
		x.advance(batchQuiet)
		x.pass(t)
		if sends := x.f.sub(t, "send"); len(sends) != 0 {
			t.Fatalf("empty batch sent: %q", sends)
		}
		if es := pendingList(t, x.b.store); len(es) != 0 {
			t.Fatalf("self completion not acked: %+v", es)
		}
		if un, _ := x.b.store.Unnotified(); len(un) != 0 {
			t.Fatalf("self completion stayed un-notified: %+v", un)
		}
	})
}

// TestBatchTextBound guards IS-9: under 32 KiB, whole ACK commands for every
// listed entry, and one overflow line counting the rest.
func TestBatchTextBound(t *testing.T) {
	var entries []pendingEntry
	for i := 1; i <= 400; i++ {
		entries = append(entries, pendingEntry{
			ID: fmt.Sprintf("D%d", i), Session: fmt.Sprintf("rpc-%d", i), Seq: uint64(i), Count: uint64(i),
			DoneAt: "2026-10-05T00:00:00.000Z", Name: ptr(strings.Repeat("가", 200)),
			Thread: ptr(strings.Repeat("t", 200)), Cwd: ptr(strings.Repeat("/c", 200)),
		})
	}
	text := mustBatchText(t, core.RPCCfg{}, entries, "/proj", "/proj/state")
	if len(text) > 32768 {
		t.Fatalf("batch = %d bytes", len(text))
	}
	if !utf8.ValidString(text) {
		t.Fatal("batch split a rune")
	}
	overflow := regexp.MustCompile(`\+(\d+) more: OMOSENSE_DIR='/proj' OMOSENSE_STATE='/proj/state' omosense rpc pending`).FindStringSubmatch(text)
	if overflow == nil {
		t.Fatalf("no overflow line:\n%s", text)
	}
	rest, err := strconv.Atoi(overflow[1])
	if err != nil {
		t.Fatal(err)
	}
	listed := 0
	for i := 1; i <= 400; i++ {
		ack := fmt.Sprintf("OMOSENSE_DIR='/proj' OMOSENSE_STATE='/proj/state' omosense rpc ack D%d %d", i, i)
		if strings.Contains(text, ack) {
			listed++
		}
	}
	if listed == 0 || listed+rest != 400 {
		t.Fatalf("listed %d + overflow %d != 400", listed, rest)
	}
	if labels := strings.Count(text, "확인 명령: "); labels != listed {
		t.Fatalf("ack labels = %d, listed = %d", labels, listed)
	}
	big := []pendingEntry{{ID: "BIG", Seq: 7, Count: 1, DoneAt: "2026-10-05T00:00:00.000Z", Name: ptr(strings.Repeat("가", 32768))}}
	one := mustBatchText(t, core.RPCCfg{}, big, "/proj", "/proj/state")
	if len(one) > 32768 || !utf8.ValidString(one) || !strings.Contains(one, "omosense rpc ack BIG 7") {
		t.Fatalf("single-entry batch = %d bytes, valid=%v", len(one), utf8.ValidString(one))
	}
	t.Logf("batch bound: %d entries listed, %d overflow, %d bytes", listed, rest, len(text))
}

// The script announces entry into a gated command on <prefix>ready and waits on
// <prefix>release. Both FIFOs have an O_RDWR test descriptor so neither opening
// relies on scheduling.
func deliveryBarrier(t *testing.T, f *fakeDelivery) (wait func(), release func()) {
	t.Helper()
	return fifoBarrier(t, f, "")
}

// listBarrier gates `omo thread list` so a subscription can be replaced while a
// liveness check is in flight (IS-7).
func listBarrier(t *testing.T, f *fakeDelivery) (wait func(), release func()) {
	t.Helper()
	return fifoBarrier(t, f, "list")
}

func fifoBarrier(t *testing.T, f *fakeDelivery, prefix string) (wait func(), release func()) {
	t.Helper()
	open := func(name string) *os.File {
		path := filepath.Join(f.dir, prefix+name)
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
			t.Fatal("fake send did not enter FIFO barrier")
		}
	}
	release = func() {
		if _, err := gate.WriteString("release\n"); err != nil {
			t.Fatal(err)
		}
	}
	return wait, release
}

func deliveryAsyncPass(t *testing.T, b *batcher) func() {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	done := make(chan struct{})
	go func() { defer close(done); b.pass(ctx) }()
	t.Cleanup(func() {
		cancel()
		<-done
	})
	return func() {
		select {
		case <-done:
		case <-ctx.Done():
			t.Fatal("batch pass did not finish")
		}
	}
}

type batchLogSignal struct {
	core.Sink
	sent chan struct{}
}

func (s batchLogSignal) Log(msg string) {
	s.Sink.Log(msg)
	if strings.HasPrefix(msg, "rpc batch sent ") {
		s.sent <- struct{}{}
	}
}

// TestBatchRunNotify drives the real Run loop: a wake with the window elapsed
// sends, and cancellation joins the goroutine.
func TestBatchRunNotify(t *testing.T) {
	x := newBatchFixture(t)
	x.subscribe(t, "MAINQA")
	signals := make(chan struct{}, 2)
	x.b.sink = batchLogSignal{Sink: x.b.sink, sent: signals}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	done := make(chan struct{})
	go func() { defer close(done); x.b.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		<-done
	})
	pendingRecord(t, x.b.store, "D7", "rpc-7")
	x.b.Notify()
	x.b.Notify()
	x.advance(batchQuiet)
	x.b.Notify()
	select {
	case <-signals:
	case <-ctx.Done():
		t.Fatalf("Run did not send; logs %s", x.logs.String())
	}
	cancel()
	<-done
	if sends := x.f.sub(t, "send"); len(sends) != 1 {
		t.Fatalf("Run sends = %q", sends)
	}
	t.Log(x.logs.String())
}
