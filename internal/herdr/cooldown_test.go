package herdr

import (
	"bytes"
	"context"
	"fmt"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/DevNewbie1826/omosense/internal/core"
)

// The os7 blocked re-emission cooldown (herdr.blockedCooldownSec, default
// 60s, 0 = off): after an emitted blocked line, a re-entry into blocked
// inside the window prints nothing; the window is measured from the last
// EMITTED line, never extended by a suppressed one; keys are machine/pane
// and a pane that leaves the snapshot loses its cooldown. Every test
// drives ticks directly against the fake clock: nothing waits real time.

// blockedLine is the HERDR blocked line a fixture pane emits. from is ""
// for a first observation (JSON null).
func blockedLine(machine, pane, from string) string {
	f := "null"
	if from != "" {
		f = strconv.Quote(from)
	}
	return fmt.Sprintf(`HERDR {"machine":%q,"pane":%q,"tab":null,"agent":null,"title":null,"cwd":null,"from":%s,"to":"blocked"}`, machine, pane, f)
}

// cooldownFixture installs the fake herdr with no machines and the given
// threads.json; the returned watcher writes to buf and dir holds the fake
// herdr's mode files.
func cooldownFixture(t *testing.T, threads string) (*watcher, *bytes.Buffer, string) {
	t.Helper()
	unsetEnv(t, "HERDR_PANE_ID")
	dir := t.TempDir()
	state := t.TempDir()
	installFakeHerdr(t, dir)
	writeFile(t, filepath.Join(dir, "machines.out"), "[]\n")
	if threads != "" {
		writeFile(t, filepath.Join(state, "threads.json"), threads)
	}
	var buf bytes.Buffer
	return newWatcher(testCtx(state, "main", &buf), core.NewOut(&buf)), &buf, dir
}

// tickW advances the fake clock by step, swaps local.out and runs one tick.
func tickW(t *testing.T, w *watcher, advance func(time.Duration), step time.Duration, first bool, dir, localAgents string) {
	t.Helper()
	advance(step)
	writeFile(t, filepath.Join(dir, "local.out"), localAgents)
	if err := w.tick(context.Background(), first); err != nil {
		t.Fatal(err)
	}
}

// TestBlockedReentryInsideWindowSuppressed guards IS-1: a pane that leaves
// blocked and re-enters it inside the 60s window prints no second line.
func TestBlockedReentryInsideWindowSuppressed(t *testing.T) {
	w, buf, dir := cooldownFixture(t, `{"job":{"pane":"p1"}}`)
	advance := fakeClock(t, time.Date(2026, 10, 5, 0, 0, 0, 0, time.UTC))

	// t+0 first observation blocked prints; t+10 working; t+15 blocked
	// again, 15s after the emit, stays silent.
	tickW(t, w, advance, 0, true, dir, agents(ag("p1", "blocked", "")))
	wantLines(t, linesOf(buf), blockedLine("local", "p1", ""))
	buf.Reset()
	tickW(t, w, advance, 10*time.Second, false, dir, agents(ag("p1", "working", "")))
	if got := linesOf(buf); len(got) != 0 {
		t.Fatalf("working printed %q, want none", got)
	}
	buf.Reset()
	tickW(t, w, advance, 5*time.Second, false, dir, agents(ag("p1", "blocked", "")))
	if got := linesOf(buf); len(got) != 0 {
		t.Fatalf("re-entry inside the window printed %q, want none", got)
	}
}

// TestSuppressedBlockedDoesNotExtendWindow guards IS-2: the window is
// measured from the last EMITTED blocked, so a suppressed re-entry at t+45
// must not silence the re-entry at t+70.
func TestSuppressedBlockedDoesNotExtendWindow(t *testing.T) {
	w, buf, dir := cooldownFixture(t, `{"job":{"pane":"p1"}}`)
	advance := fakeClock(t, time.Date(2026, 10, 5, 0, 0, 0, 0, time.UTC))

	tickW(t, w, advance, 0, true, dir, agents(ag("p1", "blocked", "")))
	if got := linesOf(buf); len(got) != 1 {
		t.Fatalf("first blocked printed %d lines, want 1", len(got))
	}
	buf.Reset()
	// t+45: 45s after the emit the re-entry is suppressed and must NOT move
	// the window. t+70: 70s after the emit, only 25s after the suppressed
	// observation, prints again.
	tickW(t, w, advance, 40*time.Second, false, dir, agents(ag("p1", "working", "")))
	tickW(t, w, advance, 5*time.Second, false, dir, agents(ag("p1", "blocked", "")))
	if got := linesOf(buf); len(got) != 0 {
		t.Fatalf("suppressed re-entry printed %q, want none", got)
	}
	buf.Reset()
	tickW(t, w, advance, 5*time.Second, false, dir, agents(ag("p1", "working", "")))
	tickW(t, w, advance, 20*time.Second, false, dir, agents(ag("p1", "blocked", "")))
	wantLines(t, linesOf(buf), blockedLine("local", "p1", "working"))
}

// TestBlockedCooldownBoundary guards IS-3: elapsed < window suppresses,
// elapsed >= window emits - checked at exactly t+59 and t+60.
func TestBlockedCooldownBoundary(t *testing.T) {
	w, buf, dir := cooldownFixture(t, `{"job":{"pane":"p1"}}`)
	advance := fakeClock(t, time.Date(2026, 10, 5, 0, 0, 0, 0, time.UTC))

	tickW(t, w, advance, 0, true, dir, agents(ag("p1", "blocked", "")))
	if got := linesOf(buf); len(got) != 1 {
		t.Fatalf("first blocked printed %d lines, want 1", len(got))
	}
	buf.Reset()
	// t+59: 59s after the emit is still inside the window.
	tickW(t, w, advance, 58*time.Second, false, dir, agents(ag("p1", "working", "")))
	tickW(t, w, advance, 1*time.Second, false, dir, agents(ag("p1", "blocked", "")))
	if got := linesOf(buf); len(got) != 0 {
		t.Fatalf("re-entry at t+59 printed %q, want none", got)
	}
	buf.Reset()
	// t+60: exactly the window has passed, so it emits. The working tick in
	// between keeps this a re-entry, not an unchanged status.
	tickW(t, w, advance, 500*time.Millisecond, false, dir, agents(ag("p1", "working", "")))
	tickW(t, w, advance, 500*time.Millisecond, false, dir, agents(ag("p1", "blocked", "")))
	wantLines(t, linesOf(buf), blockedLine("local", "p1", "working"))
}

// TestBlockedCooldownPerKeyIndependence guards IS-4: the cooldown key is
// machine/pane, so another pane, and the same pane id on another machine,
// are never silenced by p1's window.
func TestBlockedCooldownPerKeyIndependence(t *testing.T) {
	w, buf, dir := cooldownFixture(t, `{"j1":{"pane":"p1"},"j2":{"pane":"p2"},"jr":{"pane":"p1","machine":"box"}}`)
	advance := fakeClock(t, time.Date(2026, 10, 5, 0, 0, 0, 0, time.UTC))
	writeFile(t, filepath.Join(dir, "machines.out"), "[{\"label\":\"box\"}]\n")
	writeFile(t, filepath.Join(dir, "box.out"), agents(ag("p1", "working", "")))

	// t+0: p1 and p2 print their first blocked; box/p1 is still working.
	tickW(t, w, advance, 0, true, dir, agents(ag("p1", "blocked", ""), ag("p2", "blocked", "")))
	wantLines(t, linesOf(buf),
		blockedLine("local", "p1", ""),
		blockedLine("local", "p2", ""))
	buf.Reset()

	// t+10: box/p1 - the same pane id on another machine - hits its FIRST
	// blocked while local/p1 and p2 are inside their windows. A key that
	// ignored the machine would silence it.
	tickW(t, w, advance, 5*time.Second, false, dir, agents(ag("p1", "working", ""), ag("p2", "working", "")))
	buf.Reset()
	writeFile(t, filepath.Join(dir, "box.out"), agents(ag("p1", "blocked", "")))
	tickW(t, w, advance, 5*time.Second, false, dir, agents(ag("p1", "working", ""), ag("p2", "working", "")))
	wantLines(t, linesOf(buf), blockedLine("box", "p1", "working"))

	// t+20: local/p1 re-enters blocked 20s after its own emit and stays
	// silent; p2 and box/p1 keep their own clocks.
	buf.Reset()
	writeFile(t, filepath.Join(dir, "box.out"), agents(ag("p1", "working", "")))
	tickW(t, w, advance, 10*time.Second, false, dir, agents(ag("p1", "blocked", ""), ag("p2", "working", "")))
	if got := linesOf(buf); len(got) != 0 {
		t.Fatalf("local/p1 was not silenced inside its window: %q", got)
	}
}

// TestVanishedPaneLosesCooldown guards IS-5: a pane absent from the
// snapshot is pruned, so its blocked return is a first blocked and prints
// even inside the previous window.
func TestVanishedPaneLosesCooldown(t *testing.T) {
	w, buf, dir := cooldownFixture(t, `{"job":{"pane":"p1"}}`)
	advance := fakeClock(t, time.Date(2026, 10, 5, 0, 0, 0, 0, time.UTC))

	tickW(t, w, advance, 0, true, dir, agents(ag("p1", "blocked", "")))
	if got := linesOf(buf); len(got) != 1 {
		t.Fatalf("first blocked printed %d lines, want 1", len(got))
	}
	buf.Reset()
	// The pane leaves the snapshot entirely (closed), then returns blocked
	// 10s after the emit: without the prune this would be suppressed.
	tickW(t, w, advance, 5*time.Second, false, dir, agents())
	buf.Reset()
	tickW(t, w, advance, 5*time.Second, false, dir, agents(ag("p1", "blocked", "")))
	wantLines(t, linesOf(buf), blockedLine("local", "p1", ""))
}

// TestPersistentBlockedStaysQuietPastWindow guards IS-6: a pane that stays
// blocked never re-notifies, before or after the window - the cooldown
// only gates re-entry, not an unchanged status.
func TestPersistentBlockedStaysQuietPastWindow(t *testing.T) {
	w, buf, dir := cooldownFixture(t, `{"job":{"pane":"p1"}}`)
	advance := fakeClock(t, time.Date(2026, 10, 5, 0, 0, 0, 0, time.UTC))

	tickW(t, w, advance, 0, true, dir, agents(ag("p1", "blocked", "")))
	if got := linesOf(buf); len(got) != 1 {
		t.Fatalf("first blocked printed %d lines, want 1", len(got))
	}
	buf.Reset()
	for at := 5 * time.Second; at <= 130*time.Second; at += 5 * time.Second {
		tickW(t, w, advance, 5*time.Second, false, dir, agents(ag("p1", "blocked", "")))
	}
	if got := linesOf(buf); len(got) != 0 {
		t.Fatalf("persistent blocked re-notified: %q", got)
	}
}

// TestBlockedCooldownZeroPrintsEveryReentry guards IS-7's 0 semantics in
// the watcher: 0 turns the cooldown off, so every re-entry prints (the
// pre-os7 behavior).
func TestBlockedCooldownZeroPrintsEveryReentry(t *testing.T) {
	dir := t.TempDir()
	state := t.TempDir()
	installFakeHerdr(t, dir)
	writeFile(t, filepath.Join(dir, "machines.out"), "[]\n")
	writeFile(t, filepath.Join(state, "threads.json"), `{"job":{"pane":"p1"}}`)
	var buf bytes.Buffer
	c := testCtx(state, "main", &buf)
	zero := 0
	c.Profile.Herdr.BlockedCooldownSec = &zero
	w := newWatcher(c, core.NewOut(&buf))
	advance := fakeClock(t, time.Date(2026, 10, 5, 0, 0, 0, 0, time.UTC))

	tickW(t, w, advance, 0, true, dir, agents(ag("p1", "blocked", "")))
	buf.Reset()
	tickW(t, w, advance, 5*time.Second, false, dir, agents(ag("p1", "working", "")))
	buf.Reset()
	tickW(t, w, advance, 5*time.Second, false, dir, agents(ag("p1", "blocked", "")))
	wantLines(t, linesOf(&buf), blockedLine("local", "p1", "working"))
	buf.Reset()
	tickW(t, w, advance, 5*time.Second, false, dir, agents(ag("p1", "working", "")))
	buf.Reset()
	tickW(t, w, advance, 5*time.Second, false, dir, agents(ag("p1", "blocked", "")))
	wantLines(t, linesOf(&buf), blockedLine("local", "p1", "working"))
}

// TestBlockedAllPaneFollowsCooldown guards IS-8: a blockedAll pane (not a
// registered job pane) is subject to the same cooldown window.
func TestBlockedAllPaneFollowsCooldown(t *testing.T) {
	dir := t.TempDir()
	state := t.TempDir()
	installFakeHerdr(t, dir)
	writeFile(t, filepath.Join(dir, "machines.out"), "[]\n")
	var buf bytes.Buffer
	c := testCtx(state, "main", &buf)
	c.Profile.Herdr.BlockedAll = true
	w := newWatcher(c, core.NewOut(&buf))
	advance := fakeClock(t, time.Date(2026, 10, 5, 0, 0, 0, 0, time.UTC))

	// t+0: the first blocked prints under blockedAll. t+10: the re-entry is
	// suppressed. t+65: past the window, it prints again.
	tickW(t, w, advance, 0, true, dir, agents(ag("u1", "blocked", "")))
	wantLines(t, linesOf(&buf), blockedLine("local", "u1", ""))
	buf.Reset()
	tickW(t, w, advance, 5*time.Second, false, dir, agents(ag("u1", "working", "")))
	buf.Reset()
	tickW(t, w, advance, 5*time.Second, false, dir, agents(ag("u1", "blocked", "")))
	if got := linesOf(&buf); len(got) != 0 {
		t.Fatalf("blockedAll re-entry inside the window printed %q, want none", got)
	}
	buf.Reset()
	tickW(t, w, advance, 50*time.Second, false, dir, agents(ag("u1", "working", "")))
	buf.Reset()
	tickW(t, w, advance, 5*time.Second, false, dir, agents(ag("u1", "blocked", "")))
	wantLines(t, linesOf(&buf), blockedLine("local", "u1", "working"))
}
