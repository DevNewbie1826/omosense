package rpc

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
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

func fakeDeliveryCLI(t *testing.T) *fakeDelivery {
	t.Helper()
	f := &fakeDelivery{dir: t.TempDir()}
	script := `#!/bin/sh
dir=${0%/*}
printf '%s\000' "$@" >> "$dir/argv"
printf '\000' >> "$dir/argv"
if [ "$2" = list ]; then
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

func deliveryFixture(t *testing.T, profile string) (*deliverer, *fakeDelivery, *bytes.Buffer) {
	t.Helper()
	f := fakeDeliveryCLI(t)
	dir := t.TempDir()
	deliveryWrite(t, filepath.Join(dir, "sessions.json"), `{"`+profile+`":{"session_id":"TARGET","cwd":"/agent"}}`)
	c := &core.Ctx{State: dir, Profile: core.Profile{Name: profile}}
	var logs bytes.Buffer
	d := newDeliverer(c, newPendingStore(dir, profile), core.NewOut(&logs))
	return d, f, &logs
}

func deliveryClock(t *testing.T) *time.Time {
	t.Helper()
	now := time.Date(2026, 10, 5, 0, 0, 0, 0, time.UTC)
	old := nowFn
	nowFn = func() time.Time { return now }
	t.Cleanup(func() { nowFn = old })
	return &now
}

func TestDeliverRetry(t *testing.T) {
	d, f, logs := deliveryFixture(t, "main")
	now := deliveryClock(t)
	pendingRecord(t, d.store, "D7", "rpc-7")
	deliveryWrite(t, filepath.Join(f.dir, "control"), "1\n")
	d.pass(context.Background())
	es := pendingList(t, d.store)
	if len(es) != 1 || es[0].Attempts != 1 || es[0].LastError != "send refused" ||
		es[0].NextAt != core.ISO(now.Add(deliverRetry)) {
		t.Fatalf("failure dropped or failed to schedule completion: %+v", es)
	}
	d.pass(context.Background())
	if calls := f.calls(t); len(calls) != 1 {
		t.Fatalf("retry before due: %q", calls)
	}
	*now = now.Add(deliverRetry)
	deliveryWrite(t, filepath.Join(f.dir, "control"), "0\n")
	d.pass(context.Background())
	calls := f.calls(t)
	if len(calls) != 2 || calls[0][6] != "omosense-rpc-main-D7-1-1" ||
		calls[1][6] != "omosense-rpc-main-D7-1-2" {
		t.Fatalf("retry did not advance idempotency key: %q", calls)
	}
	es = pendingList(t, d.store)
	if es[0].Attempts != 2 || es[0].LastError != "" || es[0].DeliveredAt != core.ISO(*now) ||
		es[0].NextAt != core.ISO(now.Add(deliverRemind)) {
		t.Fatalf("success state: %+v", es)
	}
	d.pass(context.Background())
	if len(f.calls(t)) != 2 {
		t.Fatal("reminder before due")
	}
	*now = now.Add(deliverRemind)
	d.pass(context.Background())
	if calls = f.calls(t); len(calls) != 3 || calls[2][6] != "omosense-rpc-main-D7-1-3" {
		t.Fatalf("missing reminder: %q", calls)
	}
	t.Log(logs.String())
}

func TestDeliverSelf(t *testing.T) {
	d, f, logs := deliveryFixture(t, "main")
	pendingRecord(t, d.store, "TARGET", "rpc-main")
	d.pass(context.Background())
	if calls := f.calls(t); len(calls) != 0 {
		t.Fatalf("self completion sent: %q", calls)
	}
	if es := pendingList(t, d.store); len(es) != 0 {
		t.Fatalf("self completion not dropped: %+v", es)
	}
	t.Log(logs.String())
}

func deliveryCandidates(t *testing.T, rows string) {
	t.Helper()
	d, f, logs := deliveryFixture(t, "main")
	deliveryWrite(t, filepath.Join(d.c.State, "sessions.json"), `{"main":{"cwd":"/agent"}}`)
	deliveryWrite(t, filepath.Join(f.dir, "list"), rows+"\n")
	pendingRecord(t, d.store, "D7", "rpc-7")
	d.pass(context.Background())
	d.pass(context.Background())
	for _, call := range f.calls(t) {
		if len(call) < 2 || call[1] != "list" {
			t.Fatalf("guessed target: %q", call)
		}
	}
	if es := pendingList(t, d.store); len(es) != 1 || es[0].Attempts != 0 {
		t.Fatalf("unresolved entry changed: %+v", es)
	}
	if strings.Count(logs.String(), "LOG rpc deliver target:") != 1 {
		t.Fatalf("distinct error not deduplicated: %s", logs.String())
	}
	t.Log(logs.String())
}

func TestDeliverAmbiguous(t *testing.T) {
	deliveryCandidates(t, `[{"thread_id":"A","alive":true,"kind":"interactive","surface":"tui","cwd":"/agent"},
{"thread_id":"B","alive":true,"kind":"interactive","surface":"tui","cwd":"/agent/."}]`)
}

func TestDeliverZero(t *testing.T) {
	deliveryCandidates(t, `[{"thread_id":"dead","alive":false,"kind":"interactive","surface":"tui","cwd":"/agent"},
{"thread_id":"worker","alive":true,"kind":"worker","surface":"tui","cwd":"/agent"},
{"thread_id":"web","alive":true,"kind":"interactive","surface":"webchat","cwd":"/agent"},
{"thread_id":"other","alive":true,"kind":"interactive","surface":"tui","cwd":"/other"}]`)
}

func TestDeliverProfile(t *testing.T) {
	d, f, _ := deliveryFixture(t, "family")
	// IS-5: the profile's rpc.session pins the delivery target even when
	// the webchat sessions.json would resolve another one.
	d.c.Profile.RPC.Session = "FAMTARGET"
	main := newPendingStore(d.c.State, "main")
	pendingRecord(t, main, "D7", "rpc-main")
	before, err := os.ReadFile(main.path)
	if err != nil {
		t.Fatal(err)
	}
	e := pendingRecord(t, d.store, "D7", "rpc-family")
	d.pass(context.Background())
	calls := f.calls(t)
	if len(calls) != 1 || calls[0][2] != "FAMTARGET" ||
		!strings.Contains(calls[0][3], "omosense rpc ack D7 1 --profile family") ||
		calls[0][6] != "omosense-rpc-family-D7-1-1" {
		t.Fatalf("profile delivery: %q", calls)
	}
	if result, err := d.store.Ack("D7", e.Seq, true); err != nil || result != "acked" {
		t.Fatalf("family ack: %s %v", result, err)
	}
	after, err := os.ReadFile(main.path)
	if err != nil || !bytes.Equal(before, after) {
		t.Fatalf("family touched main file: %s, %v", after, err)
	}
	if len(pendingList(t, d.store)) != 0 || len(pendingList(t, main)) != 1 {
		t.Fatal("family ack cleared wrong profile")
	}
}

// The script announces entry into send on ready and waits on release. Both
// FIFOs have an O_RDWR test descriptor so neither opening relies on scheduling.
func deliveryBarrier(t *testing.T, f *fakeDelivery) (wait func(), release func()) {
	t.Helper()
	open := func(name string) *os.File {
		path := filepath.Join(f.dir, name)
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

func deliveryAsyncPass(t *testing.T, d *deliverer) func() {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	done := make(chan struct{})
	go func() { defer close(done); d.pass(ctx) }()
	t.Cleanup(func() {
		cancel()
		<-done
	})
	return func() {
		select {
		case <-done:
		case <-ctx.Done():
			t.Fatal("delivery pass did not finish")
		}
	}
}

func TestDeliverAckInFlight(t *testing.T) {
	d, f, _ := deliveryFixture(t, "main")
	e := pendingRecord(t, d.store, "D7", "rpc-7")
	wait, release := deliveryBarrier(t, f)
	done := deliveryAsyncPass(t, d)
	wait()
	if result, err := d.store.Ack(e.ID, e.Seq, true); err != nil || result != "acked" {
		t.Fatalf("in-flight ack: %s %v", result, err)
	}
	release()
	done()
	if es := pendingList(t, d.store); len(es) != 0 {
		t.Fatalf("send resurrected acked entry: %+v", es)
	}
	if len(f.calls(t)) != 1 {
		t.Fatal("missing in-flight send")
	}
}

func TestDeliverRecordInFlight(t *testing.T) {
	d, f, _ := deliveryFixture(t, "main")
	pendingRecord(t, d.store, "D7", "rpc-7")
	wait, release := deliveryBarrier(t, f)
	done := deliveryAsyncPass(t, d)
	wait()
	second := pendingRecord(t, d.store, "D7", "rpc-9")
	release()
	done()
	if es := pendingList(t, d.store); len(es) != 1 || es[0].Seq != second.Seq || es[0].Attempts != 0 {
		t.Fatalf("old send changed newer completion: %+v", es)
	}
	// No next send needs a barrier; remove it after its process has exited.
	for _, name := range []string{"ready", "release"} {
		if err := os.Remove(filepath.Join(f.dir, name)); err != nil {
			t.Fatal(err)
		}
	}
	d.pass(context.Background())
	calls := f.calls(t)
	if len(calls) != 2 || calls[1][6] != "omosense-rpc-main-D7-2-1" {
		t.Fatalf("newer completion not sent: %q", calls)
	}
}

func TestDeliverTargetPrecedence(t *testing.T) {
	for _, tc := range []struct {
		name, sessions, rows, want string
		session                    string // profile rpc.session ("" = unset)
	}{
		{"rpc.session", `{}`, `[]`, "OVERRIDE", "OVERRIDE"},
		{"sessions.json", `{"main":{"session_id":"DIRECT"}}`, `[]`, "DIRECT", ""},
		{"cwd", `{"main":{"cwd":"/agent/."}}`, `[{"thread_id":"MATCH","alive":true,"kind":"interactive","surface":"tui","cwd":"/agent"}]`, "MATCH", ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			d, f, _ := deliveryFixture(t, "main")
			d.c.Profile.RPC.Session = tc.session
			deliveryWrite(t, filepath.Join(d.c.State, "sessions.json"), tc.sessions)
			deliveryWrite(t, filepath.Join(f.dir, "list"), tc.rows+"\n")
			pendingRecord(t, d.store, "D7", "rpc-7")
			d.pass(context.Background())
			calls := f.calls(t)
			last := calls[len(calls)-1]
			if last[1] != "send" || last[2] != tc.want {
				t.Fatalf("wrong resolved target: %q", calls)
			}
		})
	}
}

func TestDeliverTextBound(t *testing.T) {
	e := pendingEntry{ID: "D7", Seq: 9, Count: 5, DoneAt: "2026-10-05T00:00:00.000Z",
		Name: ptr(strings.Repeat("가", 32768)), Thread: ptr("7"), Cwd: ptr("/jobs")}
	text := deliveryText(e, "family")
	if len(text) > 32768 || !utf8.ValidString(text) ||
		!strings.Contains(text, "omosense rpc ack D7 9 --profile family") {
		t.Fatalf("invalid delivery text: bytes=%d utf8=%v", len(text), utf8.ValidString(text))
	}
}

type deliveryLogSignal struct {
	core.Sink
	delivered chan struct{}
}

func (s deliveryLogSignal) Log(msg string) {
	s.Sink.Log(msg)
	if strings.HasPrefix(msg, "rpc delivered ") {
		s.delivered <- struct{}{}
	}
}

func TestDeliverRunNotifyAndCancel(t *testing.T) {
	d, f, logs := deliveryFixture(t, "main")
	signals := make(chan struct{}, 2)
	d.sink = deliveryLogSignal{Sink: d.sink, delivered: signals}
	pendingRecord(t, d.store, "D7", "rpc-7")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	done := make(chan struct{})
	go func() { defer close(done); d.Run(ctx) }()
	t.Cleanup(func() { cancel(); <-done })
	wait := func() {
		select {
		case <-signals:
		case <-ctx.Done():
			t.Fatal("Run did not deliver")
		}
	}
	wait()
	pendingRecord(t, d.store, "D8", "rpc-8")
	d.Notify()
	d.Notify()
	wait()
	cancel()
	<-done
	if calls := f.calls(t); len(calls) != 2 {
		t.Fatalf("Run sends: %q", calls)
	}
	t.Log(logs.String())
}

func TestDeliverAttemptInvariant(t *testing.T) {
	d, _, _ := deliveryFixture(t, "main")
	e := pendingRecord(t, d.store, "D7", "rpc-7")
	if applied, err := d.store.update(e, func(e *pendingEntry) { e.Attempts++ }); err != nil || !applied {
		t.Fatal(applied, err)
	}
	if applied, err := d.store.update(e, func(e *pendingEntry) { e.LastError = "stale" }); err != nil || applied {
		t.Fatalf("stale attempt applied: %v %v", applied, err)
	}
	es := pendingList(t, d.store)
	b, err := json.Marshal(es)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("attempt invariant: %s", b)
}
