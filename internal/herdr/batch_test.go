package herdr

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/DevNewbie1826/omosense/internal/core"
)

// iso0 is the fixture clock's start instant, the `at`/`first_at` of a
// transition recorded on the first tick.
const iso0 = "2026-10-05T00:00:00.000Z"

// quietFixture is the IS-3/IS-4 harness: a fake herdr, a project state dir
// with registered panes, a fake clock, and the watcher under test.
type quietFixture struct {
	dir, state string
	buf        *bytes.Buffer
	w          *watcher
	advance    func(time.Duration)
}

func newQuietFixture(t *testing.T, threads, cfg string) *quietFixture {
	t.Helper()
	dir := t.TempDir()
	state := t.TempDir()
	installFakeHerdr(t, dir)
	writeFile(t, filepath.Join(dir, "machines.out"), "[]\n")
	writeFile(t, filepath.Join(dir, "local.out"), `{"result":{"agents":[]}}`)
	// The IS-5 pane check runs once a minute, so every pane a test registers
	// is also alive on the fake herdr; a missing pane list would log instead.
	alivePanes(t, dir, "p1", "p2")
	writeFile(t, filepath.Join(state, "threads.json"), threads)
	advance := fakeClock(t, time.Date(2026, 10, 5, 0, 0, 0, 0, time.UTC))
	var buf bytes.Buffer
	w := newWatcher(herdrCtx(t, state, cfg, &buf), core.NewOut(&buf))
	return &quietFixture{dir: dir, state: state, buf: &buf, w: w, advance: advance}
}

// tick runs one tick over the given agent rows.
func (f *quietFixture) tick(t *testing.T, rows ...string) {
	t.Helper()
	writeFile(t, filepath.Join(f.dir, "local.out"), agents(rows...))
	if err := f.w.tick(context.Background(), false); err != nil {
		t.Fatal(err)
	}
}

// TestBatchNotBeforeQuietWindow guards the Risk row "batch fires before 5 quiet
// minutes": a recorded done prints nothing until the window has fully passed.
func TestBatchNotBeforeQuietWindow(t *testing.T) {
	f := newQuietFixture(t, `{"job":{"pane":"p1"}}`, "{}")
	f.tick(t, ag("p1", "working", "j"))
	f.tick(t, ag("p1", "idle", "j"))
	wantPending(t, f.state, "local/p1")
	if got := linesOf(f.buf); len(got) != 0 {
		t.Fatalf("the done printed immediately: %q", got)
	}

	f.advance(5*time.Minute - time.Second)
	f.tick(t, ag("p1", "idle", "j"))
	if got := linesOf(f.buf); len(got) != 0 {
		t.Fatalf("the batch fired before the quiet window: %q", got)
	}

	f.advance(time.Second)
	f.tick(t, ag("p1", "idle", "j"))
	wantLines(t, linesOf(f.buf),
		`HERDR {"event":"done-batch","entries":[{"machine":"local","pane":"p1","tab":null,"agent":null,"cwd":null,"from":"working","to":"idle","at":"`+iso0+`","first_at":"`+iso0+`","count":1}]}`,
	)
}

// TestBatchLaterDonePushesWindow guards the Risk row "a later done does not push
// the window": the window is measured from the NEWEST recorded transition, so a
// done arriving after the first one keeps the whole burst waiting.
func TestBatchLaterDonePushesWindow(t *testing.T) {
	f := newQuietFixture(t, `{"job":{"pane":"p1"},"two":{"pane":"p2"}}`, "{}")
	f.tick(t, ag("p1", "working", "j"), ag("p2", "working", "j"))
	f.tick(t, ag("p1", "idle", "j"), ag("p2", "working", "j"))
	f.advance(4 * time.Minute)
	f.tick(t, ag("p1", "idle", "j"), ag("p2", "idle", "j"))
	wantPending(t, f.state, "local/p1", "local/p2")

	// p1's own window has passed, but p2's newer done moved the deadline.
	f.advance(time.Minute)
	f.tick(t, ag("p1", "idle", "j"), ag("p2", "idle", "j"))
	if got := linesOf(f.buf); len(got) != 0 {
		t.Fatalf("the later done did not push the window: %q", got)
	}

	f.advance(4 * time.Minute)
	f.tick(t, ag("p1", "idle", "j"), ag("p2", "idle", "j"))
	wantLines(t, linesOf(f.buf),
		`HERDR {"event":"done-batch","entries":[`+
			`{"machine":"local","pane":"p1","tab":null,"agent":null,"cwd":null,"from":"working","to":"idle","at":"`+iso0+`","first_at":"`+iso0+`","count":1},`+
			`{"machine":"local","pane":"p2","tab":null,"agent":null,"cwd":null,"from":"working","to":"idle","at":"2026-10-05T00:04:00.000Z","first_at":"2026-10-05T00:04:00.000Z","count":1}]}`,
	)
}

// TestBatchCollapsesSamePane guards the Risk row "same pane twice -> two
// entries / wrong count / first_at overwritten": a repeated transition of one
// pane collapses into a single entry carrying the last transition and the
// transition count, with the first observation time preserved.
func TestBatchCollapsesSamePane(t *testing.T) {
	f := newQuietFixture(t, `{"job":{"pane":"p1"}}`, "{}")
	f.tick(t, ag("p1", "working", "j"))
	f.tick(t, ag("p1", "idle", "j"))
	f.advance(time.Minute)
	f.tick(t, ag("p1", "working", "j"))
	f.tick(t, ag("p1", "done", "j"))

	wantPending(t, f.state, "local/p1")
	e := pendingEntries(t, f.state)["local/p1"]
	if e.Count != 2 || e.FirstAt != iso0 || e.At != "2026-10-05T00:01:00.000Z" || e.To != "done" || deref(e.From) != "working" {
		t.Fatalf("collapsed entry = %+v, want count 2, first_at %s, at the last transition", e, iso0)
	}

	f.advance(5 * time.Minute)
	f.tick(t, ag("p1", "done", "j"))
	wantLines(t, linesOf(f.buf),
		`HERDR {"event":"done-batch","entries":[{"machine":"local","pane":"p1","tab":null,"agent":null,"cwd":null,"from":"working","to":"done","at":"2026-10-05T00:01:00.000Z","first_at":"`+iso0+`","count":2}]}`,
	)
}

// TestBatchResumesAfterRestart guards the Risk row "restart loses pending done
// or fires immediately": a second watcher over the same state loads the record
// and fires only once the loaded record's own quiet window has passed.
func TestBatchResumesAfterRestart(t *testing.T) {
	f := newQuietFixture(t, `{"job":{"pane":"p1"}}`, "{}")
	f.tick(t, ag("p1", "working", "j"))
	f.tick(t, ag("p1", "idle", "j"))
	wantPending(t, f.state, "local/p1")
	alivePanes(t, f.dir, "p1")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var buf bytes.Buffer
	counts := []int{}
	prev := sleepFn
	sleepFn = func(ctx context.Context, d time.Duration) error {
		counts = append(counts, len(linesOf(&buf)))
		switch len(counts) {
		case 1:
			f.advance(4 * time.Minute)
			return nil
		case 2:
			f.advance(time.Minute)
			return nil
		}
		cancel()
		return ctx.Err()
	}
	t.Cleanup(func() { sleepFn = prev })

	w2 := newWatcher(herdrCtx(t, f.state, "{}", &buf), core.NewOut(&buf))
	if err := w2.run(ctx); err != nil {
		t.Fatal(err)
	}
	if len(counts) != 3 {
		t.Fatalf("sleeps = %d, want 3", len(counts))
	}
	if counts[0] != 1 || counts[1] != 1 {
		t.Fatalf("lines per tick after the restart = %v, want the batch held until the window", counts)
	}
	wantLines(t, linesOf(&buf),
		"LOG herdr watcher starting (every 5s, skip none)",
		`HERDR {"event":"done-batch","entries":[{"machine":"local","pane":"p1","tab":null,"agent":null,"cwd":null,"from":"working","to":"idle","at":"`+iso0+`","first_at":"`+iso0+`","count":1}]}`,
	)
}

// TestPendingMalformedFilePreserved guards the Risk row "malformed pending file
// silently overwritten": the error is logged once and the file is moved aside
// byte-for-byte instead of being replaced.
func TestPendingMalformedFilePreserved(t *testing.T) {
	dir := t.TempDir()
	state := t.TempDir()
	installFakeHerdr(t, dir)
	writeFile(t, filepath.Join(dir, "machines.out"), "[]\n")
	writeFile(t, filepath.Join(dir, "local.out"), `{"result":{"agents":[]}}`)
	writeFile(t, filepath.Join(state, "threads.json"), `{}`)
	bad := filepath.Join(state, "herdr-pending.json")
	writeFile(t, bad, "not json\n")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	prev := sleepFn
	sleepFn = func(ctx context.Context, d time.Duration) error {
		cancel()
		return ctx.Err()
	}
	t.Cleanup(func() { sleepFn = prev })

	var buf bytes.Buffer
	w := newWatcher(herdrCtx(t, state, "{}", &buf), core.NewOut(&buf))
	if err := w.run(ctx); err != nil {
		t.Fatal(err)
	}
	lines := linesOf(&buf)
	if len(lines) != 2 || !strings.HasPrefix(lines[1], "LOG herdr pending: herdr-pending.json: ") {
		t.Fatalf("lines = %q, want the startup LOG and one pending error", lines)
	}
	if _, err := os.Stat(bad); !os.IsNotExist(err) {
		t.Fatalf("the malformed file was not moved aside: %v", err)
	}
	matches, err := filepath.Glob(bad + ".bad-*")
	if err != nil || len(matches) != 1 {
		t.Fatalf("quarantine files = %q (%v), want exactly one", matches, err)
	}
	b, err := os.ReadFile(matches[0])
	if err != nil || string(b) != "not json\n" {
		t.Fatalf("quarantined content = %q (%v), want the original bytes", b, err)
	}
}

// TestOnceLeavesPendingFileAlone guards the Risk row "--once writes/renames
// state": the read-only snapshot path never loads, never quarantines and never
// logs a pending error.
func TestOnceLeavesPendingFileAlone(t *testing.T) {
	root := t.TempDir()
	dir := filepath.Join(root, "fake")
	state := filepath.Join(root, "state")
	installFakeHerdr(t, dir)
	writeFile(t, filepath.Join(dir, "machines.out"), "[]\n")
	writeFile(t, filepath.Join(dir, "local.out"), `{"result":{"agents":[{"pane_id":"p1","agent_status":"blocked"}]}}`)
	bad := filepath.Join(state, "herdr-pending.json")
	if err := os.MkdirAll(state, 0o755); err != nil {
		t.Fatal(err)
	}
	writeFile(t, bad, "not json\n")

	var buf bytes.Buffer
	c := testCtx(state, "main", &buf)
	c.Flags = map[string]bool{"--once": true}
	if code := Run(c, []string{"--once"}); code != 0 {
		t.Fatalf("Run --once = %d, want 0", code)
	}
	if b, err := os.ReadFile(bad); err != nil || string(b) != "not json\n" {
		t.Fatalf("--once touched the pending file: %q (%v)", b, err)
	}
	matches, _ := filepath.Glob(bad + ".bad-*")
	if len(matches) != 0 {
		t.Fatalf("--once quarantined the pending file: %q", matches)
	}
	for _, l := range linesOf(&buf) {
		if strings.HasPrefix(l, "LOG herdr pending:") {
			t.Fatalf("--once logged a pending error: %q", l)
		}
	}
}

// TestPendingFileHasEntryWhileHookInFlight guards the Risk row "crash while the
// hook runs loses the done": the record is written before the hook can finish,
// so the entry - carrying verify pending - is already durable.
func TestPendingFileHasEntryWhileHookInFlight(t *testing.T) {
	dir := t.TempDir()
	state := t.TempDir()
	installFakeHerdr(t, dir)
	writeFile(t, filepath.Join(dir, "machines.out"), "[]\n")
	writeFile(t, filepath.Join(dir, "local.out"), `{"result":{"agents":[]}}`)
	writeFile(t, filepath.Join(state, "threads.json"), `{"job":{"pane":"p1"}}`)
	h := newVerifyHook(t, "0")
	wait, release := h.gate(t)
	fakeClock(t, time.Date(2026, 10, 5, 0, 0, 0, 0, time.UTC))

	var buf bytes.Buffer
	w := newWatcher(herdrCtx(t, state, hookCfg(h, true), &buf), core.NewOut(&buf))
	writeFile(t, filepath.Join(dir, "local.out"), agents(ag("p1", "working", "j")))
	if err := w.tick(context.Background(), true); err != nil {
		t.Fatal(err)
	}
	writeFile(t, filepath.Join(dir, "local.out"), agents(ag("p1", "idle", "j")))
	if err := w.tick(context.Background(), false); err != nil {
		t.Fatal(err)
	}
	wait()
	if got := pendingEntries(t, state)["local/p1"].Verify; got != "pending" {
		t.Fatalf("entry verify while the hook runs = %q, want pending", got)
	}
	release()
	w.hooks.Wait()
	if got := pendingEntries(t, state)["local/p1"].Verify; got != "verified" {
		t.Fatalf("entry verify after the hook = %q, want verified", got)
	}
	t.Log("cleanup: test Cleanup removes the fake herdr dir and the hook dir, closes both FIFOs and restores the environment")
}

// TestBatchHeldWhileHookInFlight guards the Risk row "batch fires while a hook
// is in flight (verdict missing)": the flush waits for the in-flight hook, so
// the line always carries the finished verdict.
func TestBatchHeldWhileHookInFlight(t *testing.T) {
	dir := t.TempDir()
	state := t.TempDir()
	installFakeHerdr(t, dir)
	writeFile(t, filepath.Join(dir, "machines.out"), "[]\n")
	writeFile(t, filepath.Join(dir, "local.out"), `{"result":{"agents":[]}}`)
	writeFile(t, filepath.Join(state, "threads.json"), `{"job":{"pane":"p1"}}`)
	h := newVerifyHook(t, "0")
	wait, release := h.gate(t)
	advance := fakeClock(t, time.Date(2026, 10, 5, 0, 0, 0, 0, time.UTC))
	alivePanes(t, dir, "p1")

	var buf bytes.Buffer
	w := newWatcher(herdrCtx(t, state, hookCfg(h, true), &buf), core.NewOut(&buf))
	writeFile(t, filepath.Join(dir, "local.out"), agents(ag("p1", "working", "j")))
	if err := w.tick(context.Background(), true); err != nil {
		t.Fatal(err)
	}
	writeFile(t, filepath.Join(dir, "local.out"), agents(ag("p1", "idle", "j")))
	if err := w.tick(context.Background(), false); err != nil {
		t.Fatal(err)
	}
	wait()

	advance(5 * time.Minute)
	if err := w.tick(context.Background(), false); err != nil {
		t.Fatal(err)
	}
	if got := linesOf(&buf); len(got) != 0 {
		t.Fatalf("the batch fired while the hook was in flight: %q", got)
	}

	release()
	w.hooks.Wait()
	if err := w.tick(context.Background(), false); err != nil {
		t.Fatal(err)
	}
	wantLines(t, linesOf(&buf),
		`HERDR {"event":"done-batch","entries":[{"machine":"local","pane":"p1","tab":null,"agent":null,"cwd":null,"from":"working","to":"idle","at":"`+iso0+`","first_at":"`+iso0+`","count":1,"verify":"verified"}]}`,
	)
	t.Log("cleanup: test Cleanup removes the fake herdr dir and the hook dir, closes both FIFOs and restores the environment")
}
