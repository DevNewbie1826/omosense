package remind

import (
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/DevNewbie1826/omosense/internal/core"
)

// These tests pin the lock shape of tick(): the read and every result write
// take reminders.lock briefly, never across a send. Every wait is a FIFO
// handshake or a bounded select - no sleeps.

// fifo creates a named pipe used as a deterministic handshake: the fake say
// blocks on one and announces itself through another.
func fifo(t *testing.T, dir, name string) string {
	t.Helper()
	p := filepath.Join(dir, name)
	if err := syscall.Mkfifo(p, 0o600); err != nil {
		t.Fatal(err)
	}
	return p
}

// awaitFifo returns once something opens p for writing - the fake say reached
// that statement - and fails after a bounded wait.
func awaitFifo(t *testing.T, p string) {
	t.Helper()
	done := make(chan struct{})
	go func() {
		defer close(done)
		f, err := os.Open(p)
		if err != nil {
			return
		}
		_, _ = io.Copy(io.Discard, f)
		_ = f.Close()
	}()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatalf("fake say never reached %s", filepath.Base(p))
	}
}

// gate is a release FIFO written at most once, from the test body or from a
// cleanup. The write is non-blocking so a failing test cannot hang on a FIFO
// whose reader is already gone.
type gate struct {
	path string
	once sync.Once
}

func (g *gate) release() {
	g.once.Do(func() {
		f, err := os.OpenFile(g.path, os.O_WRONLY|syscall.O_NONBLOCK, 0o600)
		if err != nil {
			return // no reader left; nothing to unblock
		}
		_, _ = f.Write([]byte("\n"))
		_ = f.Close()
	})
}

// gatedSay installs a fake say that appends its argv to capture and, when the
// argv contains blockText, announces itself on inFifo and blocks until release
// is written.
func gatedSay(t *testing.T, capture, inFifo, release, blockText string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "gated-say")
	script := `#!/bin/sh
printf '%s\n' "$*" >> "$FAKE_SAY_CAPTURE"
case "$*" in
*"$BLOCK_TEXT"*) printf x > "$IN_FIFO"; read _ < "$RELEASE" ;;
esac
exit "${FAKE_SAY_EXIT:-0}"
`
	if err := os.WriteFile(p, []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("FAKE_SAY_CAPTURE", capture)
	t.Setenv("FAKE_SAY_EXIT", "0")
	t.Setenv("BLOCK_TEXT", blockText)
	t.Setenv("IN_FIFO", inFifo)
	t.Setenv("RELEASE", release)
	return p
}

const twoDue = `[{"id":"a","at":"2026-10-03T10:00:00.000Z","platform":"telegram","target":{"chat_id":1},"text":"a"},{"id":"b","at":"2026-10-03T10:00:00.000Z","platform":"telegram","target":{"chat_id":2},"text":"b"}]`

// TestTickSendsEachDueOnce (IS-3): both due entries are sent exactly once, in
// file order, and the next tick sends nothing more.
func TestTickSendsEachDueOnce(t *testing.T) {
	ctx := loadCtx(t)
	if err := os.WriteFile(remindersFile(ctx), []byte(twoDue), 0o644); err != nil {
		t.Fatal(err)
	}
	capture := filepath.Join(t.TempDir(), "args")
	sleep := newFakeSleeper()
	withHooks(t, fixedTime, writeFakeSay(t, capture), sleep.sleep)

	sink := newChanSink()
	cancel, done := runSource(t, ctx, sink)
	sink.waitLine(t, "LOG reminder scheduler starting")
	sleep.waitCalls(t, 1)

	want := "say telegram send {\"chat_id\":1,\"text\":\"a\"}\nsay telegram send {\"chat_id\":2,\"text\":\"b\"}\n"
	if got := readCapture(t, capture); got != want {
		t.Fatalf("tick 1 sends = %q, want %q", got, want)
	}
	var lines []string
	for _, ln := range sink.snapshot() {
		if strings.HasPrefix(ln, "REMIND sent ") {
			lines = append(lines, ln)
		}
	}
	if len(lines) != 2 || !strings.Contains(lines[0], `"id":"a"`) || !strings.Contains(lines[1], `"id":"b"`) {
		t.Fatalf("REMIND sent lines = %q, want a then b", lines)
	}
	es := entries(t, ctx)
	if len(es) != 2 || !doneEntry(es[0], "sent") || !doneEntry(es[1], "sent") {
		t.Fatalf("entries not both sent: %s", mustRead(t, remindersFile(ctx)))
	}

	sleep.nextTick(t)
	if got := readCapture(t, capture); got != want {
		t.Fatalf("tick 2 sends = %q, want no new send (both entries are terminal)", got)
	}
	es = entries(t, ctx)
	if len(es) != 2 || !doneEntry(es[0], "sent") || !doneEntry(es[1], "sent") {
		t.Fatalf("tick 2 rewrote the entries: %s", mustRead(t, remindersFile(ctx)))
	}

	cancel()
	if err := waitDone(t, done); err != nil {
		t.Errorf("Run returned %v, want nil after cancel", err)
	}
}

// TestTickRecordsPerEntry (IS-4): the first result is durable before the second
// send runs, so cancelling while the second send is blocked leaves entry 1 sent
// and entry 2 pending (at-least-once: it is sent again next run).
func TestTickRecordsPerEntry(t *testing.T) {
	ctx := loadCtx(t)
	dir := t.TempDir()
	inFifo := fifo(t, dir, "in-send")
	g := &gate{path: fifo(t, dir, "release")}
	capture := filepath.Join(dir, "args")
	withHooks(t, fixedTime, gatedSay(t, capture, inFifo, g.path, `"text":"b"`), sleepCtx)
	if err := os.WriteFile(remindersFile(ctx), []byte(twoDue), 0o644); err != nil {
		t.Fatal(err)
	}
	sink := newChanSink()
	cancel, done := runSource(t, ctx, sink)
	t.Cleanup(cancel)

	awaitFifo(t, inFifo) // entry b's send is in flight
	// Entry a's result must already be durable while b's send is still in
	// flight: results are written per entry, not batched at the end of the tick.
	mid := entries(t, ctx)
	if len(mid) != 2 || !doneEntry(mid[0], "sent") || !pendingEntry(mid[1]) {
		t.Fatalf("while b's send was in flight the file was %s; want a recorded and b pending", mustRead(t, remindersFile(ctx)))
	}
	cancel()
	if err := waitDone(t, done); err != nil {
		t.Fatal(err)
	}

	es := entries(t, ctx)
	if len(es) != 2 {
		t.Fatalf("entries = %d, want 2", len(es))
	}
	if !doneEntry(es[0], "sent") {
		t.Errorf("entry a not recorded before the second send: %s", mustRead(t, remindersFile(ctx)))
	}
	if !pendingEntry(es[1]) {
		t.Errorf("entry b recorded although its send was cancelled: %s", mustRead(t, remindersFile(ctx)))
	}
	for _, ln := range sink.snapshot() {
		if strings.HasPrefix(ln, "REMIND") && strings.Contains(ln, `"id":"b"`) {
			t.Errorf("cancelled send emitted a terminal line: %q", ln)
		}
	}
	if got := readCapture(t, capture); strings.Count(got, "\n") != 2 {
		t.Errorf("say invocations = %q, want 2 (the cancelled second one included)", got)
	}
}

// TestTickChangedEntryNotRecorded (IS-6): an entry edited or removed while its
// send was in flight is not recorded, is reported once by id, and the other
// entries are still recorded.
func TestTickChangedEntryNotRecorded(t *testing.T) {
	t.Run("changed", func(t *testing.T) {
		runChangedEntry(t, twoDue, `[{"id":"a","at":"2026-10-03T10:00:00.000Z","platform":"telegram","target":{"chat_id":1},"text":"edited"},{"id":"b","at":"2026-10-03T10:00:00.000Z","platform":"telegram","target":{"chat_id":2},"text":"b"}]`, "edited")
	})
	t.Run("removed", func(t *testing.T) {
		runChangedEntry(t, twoDue, `[{"id":"b","at":"2026-10-03T10:00:00.000Z","platform":"telegram","target":{"chat_id":2},"text":"b"}]`, "")
	})
}

func runChangedEntry(t *testing.T, body, rewritten, wantText string) {
	t.Helper()
	ctx := loadCtx(t)
	dir := t.TempDir()
	inFifo := fifo(t, dir, "in-send")
	g := &gate{path: fifo(t, dir, "release")}
	capture := filepath.Join(dir, "args")
	sleep := newFakeSleeper()
	withHooks(t, fixedTime, gatedSay(t, capture, inFifo, g.path, `"text":"a"`), sleep.sleep)
	if err := os.WriteFile(remindersFile(ctx), []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
	sink := newChanSink()
	cancel, done := runSource(t, ctx, sink)
	t.Cleanup(g.release)
	t.Cleanup(cancel)

	awaitFifo(t, inFifo) // entry a's send is in flight
	if err := os.WriteFile(remindersFile(ctx), []byte(rewritten), 0o644); err != nil {
		t.Fatal(err)
	}
	g.release()
	sleep.waitCalls(t, 1) // the tick that started the send finished

	byID := map[string]*core.OMap{}
	for _, e := range entries(t, ctx) {
		byID[fieldStr(e, "id")] = e
	}
	if a, ok := byID["a"]; ok {
		if !pendingEntry(a) {
			t.Errorf("entry a was recorded although it changed during the send: %s", mustRead(t, remindersFile(ctx)))
		}
		if wantText == "" {
			t.Errorf("removed entry a came back: %s", mustRead(t, remindersFile(ctx)))
		} else if got := fieldStr(a, "text"); got != wantText {
			t.Errorf("entry a text = %q, want the edited %q", got, wantText)
		}
	} else if wantText != "" {
		t.Errorf("entry a disappeared: %s", mustRead(t, remindersFile(ctx)))
	}
	b, ok := byID["b"]
	if !ok || !doneEntry(b, "sent") {
		t.Errorf("entry b not recorded after a's send: %s", mustRead(t, remindersFile(ctx)))
	}
	wantLog := "LOG remind a changed or removed during send; not recorded"
	n := 0
	for _, ln := range sink.snapshot() {
		if ln == wantLog {
			n++
		}
		if strings.HasPrefix(ln, "REMIND") && strings.Contains(ln, `"id":"a"`) {
			t.Errorf("unrecorded entry emitted a REMIND line: %q", ln)
		}
	}
	if n != 1 {
		t.Errorf("not-recorded LOG lines = %d, want exactly 1; saw %q", n, sink.snapshot())
	}
	if got := readCapture(t, capture); strings.Count(got, "\n") != 2 {
		t.Errorf("say invocations = %q, want 2", got)
	}
	cancel()
	if err := waitDone(t, done); err != nil {
		t.Errorf("Run returned %v, want nil after cancel", err)
	}
}

// TestTickWritesAtomically (IS-7): a recording tick replaces reminders.json
// with a temp file + rename - it is never rewritten in place - and leaves no
// temp file behind.
func TestTickWritesAtomically(t *testing.T) {
	ctx := loadCtx(t)
	file := remindersFile(ctx)
	one := `[{"id":"a","at":"2026-10-03T10:00:00.000Z","platform":"telegram","target":{"chat_id":1},"text":"a"}]`
	if err := os.WriteFile(file, []byte(one), 0o644); err != nil {
		t.Fatal(err)
	}
	before, err := os.Stat(file)
	if err != nil {
		t.Fatal(err)
	}
	held, err := os.Open(file)
	if err != nil {
		t.Fatal(err)
	}
	defer held.Close()

	capture := filepath.Join(t.TempDir(), "args")
	sleep := newFakeSleeper()
	withHooks(t, fixedTime, writeFakeSay(t, capture), sleep.sleep)
	sink := newChanSink()
	cancel, done := runSource(t, ctx, sink)
	sink.waitLine(t, "REMIND sent ")
	sleep.waitCalls(t, 1)

	after, err := os.Stat(file)
	if err != nil {
		t.Fatal(err)
	}
	if os.SameFile(before, after) {
		t.Errorf("reminders.json was rewritten in place; want a temp file + rename")
	}
	if _, err := held.Seek(0, io.SeekStart); err != nil {
		t.Fatal(err)
	}
	got, err := io.ReadAll(held)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != one {
		t.Errorf("the open descriptor sees %q; the tick overwrote the file instead of replacing it", got)
	}
	des, err := os.ReadDir(ctx.State)
	if err != nil {
		t.Fatal(err)
	}
	for _, de := range des {
		if strings.HasPrefix(de.Name(), ".reminders-") {
			t.Errorf("temp file left behind: %s", de.Name())
		}
	}

	cancel()
	if err := waitDone(t, done); err != nil {
		t.Errorf("Run returned %v, want nil after cancel", err)
	}
}

// TestTickRecordNoMatchWritesNothing (COMPANION: withReminders' no-write path;
// IS-6): a record that matches no pending entry writes NOTHING. The entry's
// content cannot show that - rewriting the file with the re-read array leaves
// the same bytes - so this guard pins the write itself: a reminders.json
// deleted while the send was in flight is not recreated, and a file whose
// entry changed mid-send is not rewritten (same inode, same bytes).
func TestTickRecordNoMatchWritesNothing(t *testing.T) {
	const oneDue = `[{"id":"a","at":"2026-10-03T10:00:00.000Z","platform":"telegram","target":{"chat_id":1},"text":"a"}]`
	const wantLog = "LOG remind a changed or removed during send; not recorded"

	assertNotRecorded := func(t *testing.T, sink *chanSink) {
		t.Helper()
		n := 0
		for _, ln := range sink.snapshot() {
			if ln == wantLog {
				n++
			}
			if strings.HasPrefix(ln, "REMIND") {
				t.Errorf("unrecorded entry emitted a REMIND line: %q", ln)
			}
		}
		if n != 1 {
			t.Errorf("not-recorded LOG lines = %d, want exactly 1; saw %q", n, sink.snapshot())
		}
	}

	type gatedTick struct {
		ctx   *core.Ctx
		sink  *chanSink
		gate  *gate
		sleep *fakeSleeper
		stop  func() error
	}
	// start runs the scheduler over one due entry whose send blocks in the fake
	// say, and returns once that send is in flight.
	start := func(t *testing.T) gatedTick {
		t.Helper()
		ctx := loadCtx(t)
		dir := t.TempDir()
		inFifo := fifo(t, dir, "in-send")
		g := &gate{path: fifo(t, dir, "release")}
		capture := filepath.Join(dir, "args")
		sleep := newFakeSleeper()
		withHooks(t, fixedTime, gatedSay(t, capture, inFifo, g.path, `"text":"a"`), sleep.sleep)
		if err := os.MkdirAll(ctx.State, 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(remindersFile(ctx), []byte(oneDue), 0o644); err != nil {
			t.Fatal(err)
		}
		sink := newChanSink()
		cancel, done := runSource(t, ctx, sink)
		t.Cleanup(g.release)
		t.Cleanup(cancel)
		awaitFifo(t, inFifo) // entry a's send is in flight
		return gatedTick{ctx: ctx, sink: sink, gate: g, sleep: sleep, stop: func() error {
			cancel()
			return waitDone(t, done)
		}}
	}

	t.Run("deleted", func(t *testing.T) {
		gt := start(t)
		if err := os.Remove(remindersFile(gt.ctx)); err != nil {
			t.Fatal(err)
		}
		gt.gate.release()
		gt.sleep.waitCalls(t, 1) // the tick that started the send finished

		if _, err := os.Stat(remindersFile(gt.ctx)); !os.IsNotExist(err) {
			t.Errorf("reminders.json after a no-match record: err=%v; the no-write path must not create it", err)
		}
		assertNotRecorded(t, gt.sink)
		if err := gt.stop(); err != nil {
			t.Errorf("Run returned %v, want nil after cancel", err)
		}
	})

	t.Run("changed", func(t *testing.T) {
		gt := start(t)
		const rewritten = `[{"id":"a","at":"2026-10-03T10:00:00.000Z","platform":"telegram","target":{"chat_id":1},"text":"edited"}]`
		if err := os.WriteFile(remindersFile(gt.ctx), []byte(rewritten), 0o644); err != nil {
			t.Fatal(err)
		}
		before, err := os.Stat(remindersFile(gt.ctx))
		if err != nil {
			t.Fatal(err)
		}
		gt.gate.release()
		gt.sleep.waitCalls(t, 1) // the tick that started the send finished

		after, err := os.Stat(remindersFile(gt.ctx))
		if err != nil {
			t.Fatal(err)
		}
		if !os.SameFile(before, after) {
			t.Errorf("a no-match record rewrote reminders.json; want the file left exactly as it was")
		}
		if got := string(mustRead(t, remindersFile(gt.ctx))); got != rewritten {
			t.Errorf("reminders.json = %q, want the changed entry untouched (%q)", got, rewritten)
		}
		assertNotRecorded(t, gt.sink)
		if err := gt.stop(); err != nil {
			t.Errorf("Run returned %v, want nil after cancel", err)
		}
	})
}
