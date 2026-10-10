package remind

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// These tests pin CancelPending's lock: it takes reminders.lock itself (the
// withReminders path add uses), so it serializes with add and with a tick's
// read/record, and it writes through a temp file + rename.

const onePending = `[{"id":"p","at":"2030-01-01T00:00:00.000Z","platform":"telegram","target":{"chat_id":1},"text":"p"}]`

// TestCancelPendingWaitsForLock: while another holder keeps reminders.lock,
// CancelPending neither returns nor touches the file; once the lock is
// released it proceeds and cancels the pending entry.
func TestCancelPendingWaitsForLock(t *testing.T) {
	ctx := loadCtx(t)
	if err := os.WriteFile(remindersFile(ctx), []byte(onePending), 0o644); err != nil {
		t.Fatal(err)
	}
	unlock, err := lockReminders(ctx.State)
	if err != nil {
		t.Fatal(err)
	}
	released := false
	t.Cleanup(func() {
		if !released {
			unlock()
		}
	})

	type result struct {
		n   int
		err error
	}
	done := make(chan result, 1)
	go func() {
		n, err := CancelPending(ctx, fixedTime)
		done <- result{n, err}
	}()
	// The timer only bounds how long the absence of a result is observed; it
	// does not pace CancelPending. Without the lock it returns at once.
	select {
	case r := <-done:
		t.Fatalf("CancelPending returned (n=%d err=%v) while reminders.lock was held", r.n, r.err)
	case <-time.After(300 * time.Millisecond):
	}
	if got := string(mustRead(t, remindersFile(ctx))); got != onePending {
		t.Fatalf("file changed while the lock was held:\n%s", got)
	}

	released = true
	unlock()
	select {
	case r := <-done:
		if r.err != nil || r.n != 1 {
			t.Fatalf("after release: n=%d err=%v, want 1, nil", r.n, r.err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("CancelPending did not proceed after the lock was released")
	}
	if es := entries(t, ctx); len(es) != 1 || !doneEntry(es[0], "cancelled") {
		t.Fatalf("entry not cancelled: %s", mustRead(t, remindersFile(ctx)))
	}
}

// TestCancelPendingWithAddKeepsBoth: an add started while CancelPending is
// between its read and its write waits for the lock, so the cancel marker and
// the new entry both survive. The hook also probes that the lock is held
// during the read-modify-write.
func TestCancelPendingWithAddKeepsBoth(t *testing.T) {
	ctx := loadCtx(t)
	withHooks(t, fixedTime, "", nil)
	if err := os.WriteFile(remindersFile(ctx), []byte(onePending), 0o644); err != nil {
		t.Fatal(err)
	}
	addDone := make(chan int, 1)
	held := false
	prev := cancelPendingLockedHook
	cancelPendingLockedHook = func() {
		held = lockHeld(t, ctx.State)
		go func() {
			var out, errb bytes.Buffer
			addDone <- add(ctx, []string{"--in", "1h", "--platform", "telegram", "--target", `{"chat_id":2}`, "--text", "new", "--id", "added"}, &out, &errb)
		}()
	}
	t.Cleanup(func() { cancelPendingLockedHook = prev })

	n, err := CancelPending(ctx, fixedTime)
	if err != nil || n != 1 {
		t.Fatalf("CancelPending n=%d err=%v, want 1, nil", n, err)
	}
	if !held {
		t.Error("reminders.lock was not held while CancelPending read and modified the file")
	}
	select {
	case code := <-addDone:
		if code != 0 {
			t.Fatalf("add exit %d, want 0", code)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("add did not finish after CancelPending released the lock")
	}
	es := entries(t, ctx)
	if len(es) != 2 || fieldStr(es[0], "id") != "p" || !doneEntry(es[0], "cancelled") ||
		fieldStr(es[1], "id") != "added" || !pendingEntry(es[1]) {
		t.Fatalf("want p(cancelled) then added(pending), got %s", mustRead(t, remindersFile(ctx)))
	}
}

// TestCancelPendingWritesAtomically: the rewrite replaces reminders.json by
// rename, so a reader holding the old file keeps the old bytes and the path
// names a new inode.
func TestCancelPendingWritesAtomically(t *testing.T) {
	ctx := loadCtx(t)
	file := remindersFile(ctx)
	if err := os.WriteFile(file, []byte(onePending), 0o644); err != nil {
		t.Fatal(err)
	}
	before, err := os.Stat(file)
	if err != nil {
		t.Fatal(err)
	}
	old, err := os.Open(file)
	if err != nil {
		t.Fatal(err)
	}
	defer old.Close()
	if n, err := CancelPending(ctx, fixedTime); err != nil || n != 1 {
		t.Fatalf("n=%d err=%v, want 1, nil", n, err)
	}
	after, err := os.Stat(file)
	if err != nil {
		t.Fatal(err)
	}
	if os.SameFile(before, after) {
		t.Fatal("reminders.json was rewritten in place, want temp file + rename")
	}
	var buf bytes.Buffer
	if _, err := buf.ReadFrom(old); err != nil {
		t.Fatal(err)
	}
	if buf.String() != onePending {
		t.Fatalf("the old file's bytes changed: %q", buf.String())
	}
	if after.Mode().Perm() != 0o644 {
		t.Errorf("mode = %v, want 0644", after.Mode().Perm())
	}
	if m, _ := filepath.Glob(filepath.Join(ctx.State, ".reminders-*")); len(m) != 0 {
		t.Errorf("temp files left behind: %v", m)
	}
}

// TestCancelDuringSendNotRecorded: CancelPending runs while the entry's send is
// in flight (the fake say is parked on a FIFO). The entry was pending when the
// cancel read the file, so it is cancelled; when the send finishes its record
// finds the entry changed, logs one not-recorded line and writes no sent mark.
func TestCancelDuringSendNotRecorded(t *testing.T) {
	ctx := loadCtx(t)
	dir := t.TempDir()
	inSend := fifo(t, dir, "in-send")
	g := newGate(t, dir, "release")
	capture := filepath.Join(dir, "args")
	withHooks(t, fixedTime, gatedSay(t, capture, inSend, "", g.path, `"text":"due"`), nil)
	body := `[{"id":"due","at":"2026-10-03T10:00:00.000Z","platform":"telegram","target":{"chat_id":1},"text":"due"}]`
	if err := os.WriteFile(remindersFile(ctx), []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(g.release)

	sink := newChanSink()
	tickDone := make(chan error, 1)
	go func() { tickDone <- newScheduler(ctx, sink).tick(context.Background()) }()
	awaitFifo(t, inSend) // the say child is mid-send, outside the lock

	n, err := CancelPending(ctx, fixedTime)
	if err != nil || n != 1 {
		g.release()
		t.Fatalf("CancelPending during the send: n=%d err=%v, want 1, nil", n, err)
	}
	g.release()
	select {
	case err := <-tickDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("tick did not finish")
	}

	es := entries(t, ctx)
	if len(es) != 1 || !doneEntry(es[0], "cancelled") || doneEntry(es[0], "sent") {
		t.Fatalf("want the entry cancelled and not sent, got %s", mustRead(t, remindersFile(ctx)))
	}
	const wantLog = "LOG remind due changed or removed during send; not recorded"
	logs := 0
	for _, ln := range sink.snapshot() {
		if ln == wantLog {
			logs++
		}
		if strings.HasPrefix(ln, "REMIND") {
			t.Errorf("unrecorded send emitted a REMIND line: %q", ln)
		}
	}
	if logs != 1 {
		t.Errorf("not-recorded LOG lines = %d, want 1; saw %q", logs, sink.snapshot())
	}
	if got := readCapture(t, capture); strings.Count(got, "\n") != 1 {
		t.Errorf("say invocations = %q, want 1", got)
	}
}
