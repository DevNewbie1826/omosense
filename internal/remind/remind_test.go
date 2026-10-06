package remind

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/DevNewbie1826/omosense/internal/core"
)

var fixedTime = time.Date(2026, 10, 3, 10, 5, 0, 0, time.UTC)

// testEnv points HOME, OMOSENSE_DIR and OMOSENSE_STATE at temp dirs with a
// minimal config.json.
func testEnv(t *testing.T) (dir, state string) {
	t.Helper()
	home := t.TempDir()
	dir = filepath.Join(home, ".omomeow")
	state = filepath.Join(dir, "state")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "config.json"), []byte(`{"profiles":{"main":{"telegram":{"bots":["b1"]},"discord":{"bots":["d1"]}}}}`), 0o644); err != nil {
		t.Fatal(err)
	}
	t.Setenv("HOME", home)
	t.Setenv("OMOSENSE_DIR", dir)
	t.Setenv("OMOSENSE_STATE", state)
	return dir, state
}

func loadCtx(t *testing.T) *core.Ctx {
	t.Helper()
	_, _ = testEnv(t)
	ctx, err := core.Load(core.ParseArgs(nil), true)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	return ctx
}

func remindersFile(ctx *core.Ctx) string {
	return filepath.Join(ctx.State, "reminders-"+ctx.Profile.Name+".json")
}

func mustRead(t *testing.T, p string) []byte {
	t.Helper()
	b, err := os.ReadFile(p)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func withHooks(t *testing.T, fixed time.Time, sayBin string, sleep func(context.Context, time.Duration) bool) {
	t.Helper()
	oldNow, oldSleep, oldBin := hookNow, hookSleep, hookSayBin
	hookNow = func() time.Time { return fixed }
	hookSleep = sleep
	hookSayBin = sayBin
	t.Cleanup(func() { hookNow, hookSleep, hookSayBin = oldNow, oldSleep, oldBin })
}

// writeFakeSay installs a script that records "$*" and exits per env, so a
// test can flip FAKE_SAY_EXIT/STDOUT/STDERR between ticks.
func writeFakeSay(t *testing.T, capture string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "fake-say")
	script := `#!/bin/sh
printf '%s\n' "$*" >> "$FAKE_SAY_CAPTURE"
[ -n "$FAKE_SAY_STDOUT" ] && printf '%s' "$FAKE_SAY_STDOUT"
[ -n "$FAKE_SAY_STDERR" ] && printf '%s' "$FAKE_SAY_STDERR" >&2
exit "$FAKE_SAY_EXIT"
`
	if err := os.WriteFile(p, []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("FAKE_SAY_CAPTURE", capture)
	t.Setenv("FAKE_SAY_EXIT", "0")
	t.Setenv("FAKE_SAY_STDOUT", "")
	t.Setenv("FAKE_SAY_STDERR", "")
	return p
}

// fakeSleeper gates every loop iteration: sleep records the interval,
// announces itself, then blocks until the test allows the next tick.
type fakeSleeper struct {
	mu     sync.Mutex
	durs   []time.Duration
	called chan struct{}
	gate   chan struct{}
}

func newFakeSleeper() *fakeSleeper {
	return &fakeSleeper{called: make(chan struct{}, 64), gate: make(chan struct{})}
}

func (f *fakeSleeper) sleep(ctx context.Context, d time.Duration) bool {
	f.mu.Lock()
	f.durs = append(f.durs, d)
	f.mu.Unlock()
	select {
	case f.called <- struct{}{}:
	default:
	}
	select {
	case <-f.gate:
		return true
	case <-ctx.Done():
		return false
	}
}

// waitCalls blocks until n sleep calls have been announced.
func (f *fakeSleeper) waitCalls(t *testing.T, n int) {
	t.Helper()
	for i := 0; i < n; i++ {
		select {
		case <-f.called:
		case <-time.After(10 * time.Second):
			t.Fatalf("timed out waiting for tick #%d", i+1)
		}
	}
}

// nextTick allows one blocked sleep and waits for the following tick.
func (f *fakeSleeper) nextTick(t *testing.T) {
	t.Helper()
	f.allow()
	f.waitCalls(t, 1)
}

func (f *fakeSleeper) allow() { f.gate <- struct{}{} }

func (f *fakeSleeper) allDurations() []time.Duration {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]time.Duration(nil), f.durs...)
}

// chanSink records every grammar line and mirrors them to a channel so a
// test can await an exact line with a bounded timeout.
type chanSink struct {
	ch    chan string
	mu    sync.Mutex
	lines []string
}

func newChanSink() *chanSink { return &chanSink{ch: make(chan string, 64)} }

func (s *chanSink) record(line string) {
	s.mu.Lock()
	s.lines = append(s.lines, line)
	s.mu.Unlock()
	select {
	case s.ch <- line:
	default:
	}
}

func (s *chanSink) Emit(prefix string, payload any) { s.record(prefix + " <emit>") }
func (s *chanSink) Raw(prefix, text string)         { s.record(prefix + " " + text) }
func (s *chanSink) Log(msg string)                  { s.record("LOG " + msg) }

func (s *chanSink) waitLine(t *testing.T, substr string) string {
	t.Helper()
	deadline := time.After(10 * time.Second)
	for {
		select {
		case ln := <-s.ch:
			if strings.Contains(ln, substr) {
				return ln
			}
		case <-deadline:
			t.Fatalf("timed out waiting for a line containing %q; saw %q", substr, s.snapshot())
		}
	}
}

func (s *chanSink) snapshot() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.lines...)
}

func runSource(t *testing.T, ctx *core.Ctx, sink *chanSink) (cancel context.CancelFunc, done <-chan error) {
	t.Helper()
	srcs := Sources(ctx)
	if len(srcs) != 1 {
		t.Fatalf("Sources = %d, want 1", len(srcs))
	}
	runCtx, cancel := context.WithCancel(context.Background())
	errc := make(chan error, 1)
	go func() { errc <- srcs[0].Run(runCtx, sink) }()
	return cancel, errc
}

func waitDone(t *testing.T, done <-chan error) error {
	t.Helper()
	select {
	case err := <-done:
		return err
	case <-time.After(10 * time.Second):
		t.Fatal("source Run did not return after cancel")
		return nil
	}
}

const initialFile = `[{"zz":1,"id":"r1","at":"2026-10-03T10:00:00.000Z","platform":"telegram","target":{"b":2,"a":1},"text":"hi 안","extra":true},{"id":"r2","at":"2030-01-01T00:00:00.000Z","platform":"discord","target":{"channel_id":5},"text":"later"}]`

// goldenAfterSend is bun's JSON.stringify(list, null, 2) for the file after
// r1 is sent at the injected clock (generated with `bun -e`; byte target:
// two-space indent, unknown fields and key order kept, sent appended, no
// trailing newline).
const goldenAfterSend = `[
  {
    "zz": 1,
    "id": "r1",
    "at": "2026-10-03T10:00:00.000Z",
    "platform": "telegram",
    "target": {
      "b": 2,
      "a": 1
    },
    "text": "hi 안",
    "extra": true,
    "sent": "2026-10-03T10:05:00.000Z"
  },
  {
    "id": "r2",
    "at": "2030-01-01T00:00:00.000Z",
    "platform": "discord",
    "target": {
      "channel_id": 5
    },
    "text": "later"
  }
]`

func TestRunLoopSendsDueReminderOnce(t *testing.T) {
	ctx := loadCtx(t)
	file := remindersFile(ctx)
	if err := os.WriteFile(file, []byte(initialFile), 0o644); err != nil {
		t.Fatal(err)
	}
	capture := filepath.Join(t.TempDir(), "captured-args")
	sleep := newFakeSleeper()
	withHooks(t, fixedTime, writeFakeSay(t, capture), sleep.sleep)

	sink := newChanSink()
	cancel, done := runSource(t, ctx, sink)

	if got := sink.waitLine(t, "LOG reminder scheduler starting"); got != "LOG reminder scheduler starting (profile main)" {
		t.Errorf("startup line = %q, want the TS grammar with the profile name", got)
	}
	sent := sink.waitLine(t, "REMIND sent ")
	if !strings.Contains(sent, `"sent":"2026-10-03T10:05:00.000Z"`) {
		t.Errorf("REMIND sent line lacks the injected-clock ISO: %q", sent)
	}
	if !strings.Contains(sent, `"zz":1`) {
		t.Errorf("REMIND sent line must carry the whole entry: %q", sent)
	}

	capArgs := mustRead(t, capture)
	if want := "say telegram send {\"b\":2,\"a\":1,\"text\":\"hi 안\"}\n"; string(capArgs) != want {
		t.Errorf("say args = %q, want %q (target order kept, text merged)", capArgs, want)
	}

	sleep.waitCalls(t, 1)
	if after := mustRead(t, file); string(after) != goldenAfterSend {
		t.Errorf("file after send:\n got %q\nwant %q", after, goldenAfterSend)
	}

	sleep.nextTick(t)
	if n := strings.Count(string(mustRead(t, capture)), "\n"); n != 1 {
		t.Errorf("say execed %d times, want exactly 1 (sent entries are skipped)", n)
	}
	for _, ln := range sink.snapshot() {
		if strings.HasPrefix(ln, "REMIND") && ln != sent {
			t.Errorf("unexpected extra REMIND line: %q", ln)
		}
	}
	for _, d := range sleep.allDurations() {
		if d != 20*time.Second {
			t.Errorf("sleep interval = %v, want 20s", d)
		}
	}

	cancel()
	if err := waitDone(t, done); err != nil {
		t.Errorf("Run returned %v, want nil after cancel", err)
	}
}

func TestFailedReminderIsTerminal(t *testing.T) {
	ctx := loadCtx(t)
	file := remindersFile(ctx)
	due := `[{"id":"r1","at":"2026-10-03T10:04:00.000Z","platform":"telegram","target":{"chat_id":42},"text":"hi"}]`
	if err := os.WriteFile(file, []byte(due), 0o644); err != nil {
		t.Fatal(err)
	}
	capture := filepath.Join(t.TempDir(), "captured-args")
	sayBin := writeFakeSay(t, capture)
	t.Setenv("FAKE_SAY_EXIT", "1")
	t.Setenv("FAKE_SAY_STDOUT", "partial out ")
	t.Setenv("FAKE_SAY_STDERR", "boom")
	sleep := newFakeSleeper()
	withHooks(t, fixedTime, sayBin, sleep.sleep)

	sink := newChanSink()
	cancel, done := runSource(t, ctx, sink)

	if got := sink.waitLine(t, "LOG remind send failed"); got != "LOG remind send failed r1: boom" {
		t.Errorf("TS log line = %q, want %q", got, "LOG remind send failed r1: boom")
	}
	failLine := sink.waitLine(t, "REMIND failed ")
	if !strings.Contains(failLine, `"failed":"2026-10-03T10:05:00.000Z"`) {
		t.Errorf("REMIND failed line lacks the failed ISO: %q", failLine)
	}
	if !strings.Contains(failLine, `"error":"partial out boom"`) {
		t.Errorf("REMIND failed line lacks the trimmed stdout+stderr error: %q", failLine)
	}
	if after := mustRead(t, file); !strings.Contains(string(after), `"error": "partial out boom"`) {
		t.Errorf("state file lacks failed/error: %s", after)
	}

	t.Setenv("FAKE_SAY_EXIT", "0")
	sleep.nextTick(t)
	sleep.nextTick(t)
	if n := strings.Count(string(mustRead(t, capture)), "\n"); n != 1 {
		t.Errorf("say execed %d times after a failed send, want exactly 1 (failed is terminal)", n)
	}
	for _, ln := range sink.snapshot() {
		if strings.Contains(ln, "REMIND sent") {
			t.Errorf("failed entry was retried and sent: %q", ln)
		}
	}

	cancel()
	if err := waitDone(t, done); err != nil {
		t.Errorf("Run returned %v, want nil after cancel", err)
	}
}

// review-1 P1 #2 regression: a Telegram API failure body echoing the
// credential-bearing /bot<token>/ URL reaches the reminder only sanitized.
// The scheduler execs the REAL omosense binary as the say child (as the
// reproduction did), so this fails if say ever prints the token again.
func TestFailedReminderErrorHasNoToken(t *testing.T) {
	ctx := loadCtx(t)
	creds := filepath.Join(os.Getenv("HOME"), ".config", "agent-messenger")
	if err := os.MkdirAll(creds, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(creds, "telegrambot-credentials.json"), []byte(`{"bots":{"b1":{"token":"TGTOK1"}}}`), 0o644); err != nil {
		t.Fatal(err)
	}
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		msg := "backend rejected " + r.URL.EscapedPath()
		w.WriteHeader(http.StatusForbidden)
		_, _ = w.Write([]byte(`{"ok":false,"description":"` + msg + `","message":"` + msg + `"}`))
	}))
	t.Cleanup(api.Close)
	t.Setenv("OMOSENSE_TELEGRAM_API", api.URL)

	file := remindersFile(ctx)
	due := `[{"id":"r1","at":"2026-10-03T10:04:00.000Z","platform":"telegram","target":{"chat_id":42},"text":"hi"}]`
	if err := os.WriteFile(file, []byte(due), 0o644); err != nil {
		t.Fatal(err)
	}
	sleep := newFakeSleeper()
	withHooks(t, fixedTime, omosenseBin(t), sleep.sleep)

	sink := newChanSink()
	cancel, done := runSource(t, ctx, sink)

	failLine := sink.waitLine(t, "REMIND failed ")
	if strings.Contains(failLine, "TGTOK1") {
		t.Errorf("REMIND failed line contains the bot token: %q", failLine)
	}
	var failedEntry map[string]any
	if err := json.Unmarshal([]byte(strings.TrimPrefix(failLine, "REMIND failed ")), &failedEntry); err != nil {
		t.Fatalf("REMIND failed line is not JSON: %q (%v)", failLine, err)
	}
	// say's sanitized failure stdout, verbatim, is the only diagnostic the
	// reminder may store (review-1 P1 #2).
	wantErr := `{"ok":false,"description":"backend rejected /bot[redacted]/sendMessage","message":"backend rejected /bot[redacted]/sendMessage"}`
	if got := failedEntry["error"]; got != wantErr {
		t.Errorf("REMIND failed error = %v, want say's sanitized stdout", got)
	}
	for _, ln := range sink.snapshot() {
		if strings.Contains(ln, "TGTOK1") {
			t.Errorf("grammar line contains the bot token: %q", ln)
		}
	}
	after := string(mustRead(t, file))
	if strings.Contains(after, "TGTOK1") {
		t.Errorf("state file contains the bot token: %s", after)
	}
	var entries []map[string]any
	if err := json.Unmarshal([]byte(after), &entries); err != nil {
		t.Fatalf("state file is not JSON: %s (%v)", after, err)
	}
	if len(entries) != 1 {
		t.Fatalf("state file has %d entries, want 1", len(entries))
	}
	if got := entries[0]["error"]; got != wantErr {
		t.Errorf("state error = %v, want say's sanitized stdout", got)
	}
	if got := entries[0]["failed"]; got != "2026-10-03T10:05:00.000Z" {
		t.Errorf("state failed = %v, want the injected-clock ISO", got)
	}
	if _, ok := entries[0]["sent"]; ok {
		t.Errorf("a failed send must not be marked sent: %s", after)
	}

	cancel()
	if err := waitDone(t, done); err != nil {
		t.Errorf("Run returned %v, want nil after cancel", err)
	}
}

func TestLateReminderSkippedLate(t *testing.T) {
	ctx := loadCtx(t)
	file := remindersFile(ctx)
	late := `[{"id":"r1","at":"2026-10-03T03:00:00.000Z","platform":"telegram","target":{"chat_id":42},"text":"hi"}]`
	if err := os.WriteFile(file, []byte(late), 0o644); err != nil {
		t.Fatal(err)
	}
	capture := filepath.Join(t.TempDir(), "captured-args")
	sleep := newFakeSleeper()
	withHooks(t, fixedTime, writeFakeSay(t, capture), sleep.sleep)

	sink := newChanSink()
	cancel, done := runSource(t, ctx, sink)

	line := sink.waitLine(t, "REMIND skipped-late ")
	if !strings.Contains(line, `"skipped":"2026-10-03T10:05:00.000Z"`) {
		t.Errorf("REMIND skipped-late line = %q, want the skipped ISO", line)
	}
	if _, err := os.Stat(capture); !os.IsNotExist(err) {
		t.Errorf("a late reminder must never be sent (capture exists: %v)", err)
	}
	if after := mustRead(t, file); !strings.Contains(string(after), `"skipped"`) {
		t.Errorf("state file lacks skipped: %s", after)
	}

	sleep.nextTick(t)
	skipped := 0
	for _, ln := range sink.snapshot() {
		if strings.Contains(ln, "skipped-late") {
			skipped++
		}
	}
	if skipped != 1 {
		t.Errorf("skipped-late emitted %d times, want 1", skipped)
	}

	cancel()
	if err := waitDone(t, done); err != nil {
		t.Errorf("Run returned %v, want nil after cancel", err)
	}
}

func TestFutureReminderUntouchedNoWrite(t *testing.T) {
	ctx := loadCtx(t)
	file := remindersFile(ctx)
	future := `[{"id":"f1","at":"2026-10-03T11:00:00.000Z","platform":"telegram","target":{"chat_id":1},"text":"x"}]`
	if err := os.WriteFile(file, []byte(future), 0o644); err != nil {
		t.Fatal(err)
	}
	capture := filepath.Join(t.TempDir(), "captured-args")
	sleep := newFakeSleeper()
	withHooks(t, fixedTime, writeFakeSay(t, capture), sleep.sleep)

	sink := newChanSink()
	cancel, done := runSource(t, ctx, sink)
	sink.waitLine(t, "LOG reminder scheduler starting")

	sleep.waitCalls(t, 1)
	if after := mustRead(t, file); string(after) != future {
		t.Errorf("unchanged file was rewritten:\n got %s\nwant %s", after, future)
	}
	if _, err := os.Stat(capture); !os.IsNotExist(err) {
		t.Errorf("future reminder must not be sent: %v", err)
	}

	sleep.nextTick(t)
	if after := mustRead(t, file); string(after) != future {
		t.Errorf("unchanged file was rewritten on the second tick:\n got %s\nwant %s", after, future)
	}
	for _, ln := range sink.snapshot() {
		if strings.HasPrefix(ln, "REMIND") {
			t.Errorf("unexpected REMIND line: %q", ln)
		}
	}

	cancel()
	if err := waitDone(t, done); err != nil {
		t.Errorf("Run returned %v, want nil after cancel", err)
	}
}

func TestMissingReminderFileIsNoop(t *testing.T) {
	ctx := loadCtx(t)
	sleep := newFakeSleeper()
	withHooks(t, fixedTime, writeFakeSay(t, filepath.Join(t.TempDir(), "captured-args")), sleep.sleep)

	sink := newChanSink()
	cancel, done := runSource(t, ctx, sink)
	sink.waitLine(t, "LOG reminder scheduler starting")
	sleep.waitCalls(t, 1)

	for _, ln := range sink.snapshot() {
		if strings.HasPrefix(ln, "REMIND") || strings.HasPrefix(ln, "LOG remind ") {
			t.Errorf("unexpected line for a missing file: %q", ln)
		}
	}

	cancel()
	if err := waitDone(t, done); err != nil {
		t.Errorf("Run returned %v, want nil after cancel", err)
	}
}

func TestTickErrorsAreLogged(t *testing.T) {
	ctx := loadCtx(t)
	if err := os.WriteFile(remindersFile(ctx), []byte("{oops"), 0o644); err != nil {
		t.Fatal(err)
	}
	sleep := newFakeSleeper()
	withHooks(t, fixedTime, writeFakeSay(t, filepath.Join(t.TempDir(), "captured-args")), sleep.sleep)

	sink := newChanSink()
	cancel, done := runSource(t, ctx, sink)

	first := sink.waitLine(t, "LOG remind ")
	if len(first) <= len("LOG remind ") {
		t.Errorf("error line carries no error text: %q", first)
	}
	sleep.nextTick(t)
	sink.waitLine(t, "LOG remind ")

	cancel()
	if err := waitDone(t, done); err != nil {
		t.Errorf("Run returned %v, want nil after cancel (tick errors are logged, not fatal)", err)
	}
}
