package herdr

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/DevNewbie1826/omosense/internal/core"
)

// TestBlockedEmittedOnFirstTick guards IS-1: by default a blocked HERDR line is
// printed only for a registered (active threads.json, not rpc-owned) job pane,
// and herdr.blockedAll=true restores the pre-quiet rule for every pane except
// the own pane. Seen is still updated for every pane either way, so a later
// status change of a silenced pane is not mistaken for a first observation.
func TestBlockedEmittedOnFirstTick(t *testing.T) {
	unsetEnv(t, "HERDR_PANE_ID")
	const first = `{"result":{"agents":[
		{"pane_id":"p1","tab_id":"tab-1","display_agent":"Claude","agent":"claude","agent_status":"blocked","cwd":"/work","terminal_title_stripped":"fix <bug> 한글"},
		{"pane_id":"p2","agent_status":"blocked"},
		{"pane_id":"p3","agent_status":"idle"},
		{"pane_id":"p4","display_agent":"","agent":"codex","agent_status":"blocked","terminal_title_stripped":""}
	]}}`
	// The later transition carries the previous status; an unchanged pane stays
	// quiet.
	const later = `{"result":{"agents":[
		{"pane_id":"p1","tab_id":"tab-1","display_agent":"Claude","agent":"claude","agent_status":"blocked","cwd":"/work","terminal_title_stripped":"fix <bug> 한글"},
		{"pane_id":"p2","agent_status":"blocked"},
		{"pane_id":"p3","agent_status":"blocked"},
		{"pane_id":"p4","display_agent":"","agent":"codex","agent_status":"blocked","terminal_title_stripped":""}
	]}}`
	const p1Line = `HERDR {"machine":"local","pane":"p1","tab":"tab-1","agent":"Claude","title":"fix <bug> 한글","cwd":"/work","from":null,"to":"blocked"}`
	const p2Line = `HERDR {"machine":"local","pane":"p2","tab":null,"agent":null,"title":null,"cwd":null,"from":null,"to":"blocked"}`
	const p4Line = `HERDR {"machine":"local","pane":"p4","tab":null,"agent":"","title":"","cwd":null,"from":null,"to":"blocked"}`
	const p3Line = `HERDR {"machine":"local","pane":"p3","tab":null,"agent":null,"title":null,"cwd":null,"from":"idle","to":"blocked"}`

	t.Run("registered job pane only", func(t *testing.T) {
		dir := t.TempDir()
		state := t.TempDir()
		installFakeHerdr(t, dir)
		writeFile(t, filepath.Join(dir, "machines.out"), "[]\n")
		writeFile(t, filepath.Join(state, "threads.json"), `{"job":{"pane":"p1"}}`)
		writeFile(t, filepath.Join(dir, "local.out"), first)

		var buf bytes.Buffer
		w := newWatcher(testCtx(state, "main", &buf), core.NewOut(&buf))
		if err := w.tick(context.Background(), true); err != nil {
			t.Fatal(err)
		}
		wantLines(t, linesOf(&buf), p1Line)

		buf.Reset()
		writeFile(t, filepath.Join(dir, "local.out"), later)
		if err := w.tick(context.Background(), false); err != nil {
			t.Fatal(err)
		}
		if got := linesOf(&buf); len(got) != 0 {
			t.Fatalf("unregistered panes woke the agent: %q", got)
		}
	})

	t.Run("blockedAll restores every pane", func(t *testing.T) {
		dir := t.TempDir()
		state := t.TempDir()
		installFakeHerdr(t, dir)
		writeFile(t, filepath.Join(dir, "machines.out"), "[]\n")
		writeFile(t, filepath.Join(dir, "local.out"), first)

		var buf bytes.Buffer
		c := testCtx(state, "main", &buf)
		c.Profile.Herdr.BlockedAll = true
		w := newWatcher(c, core.NewOut(&buf))
		if err := w.tick(context.Background(), true); err != nil {
			t.Fatal(err)
		}
		wantLines(t, linesOf(&buf), p1Line, p2Line, p4Line)

		buf.Reset()
		writeFile(t, filepath.Join(dir, "local.out"), later)
		if err := w.tick(context.Background(), false); err != nil {
			t.Fatal(err)
		}
		wantLines(t, linesOf(&buf), p3Line)
	})
}

// TestJobTransitionRules guards IS-1 and IS-3: an unregistered pane's blocked
// line is silent by default, and a registered job pane's working->idle/done is
// recorded for the quiet batch instead of being printed. Five quiet minutes
// after the newest record the batch prints as exactly ONE done-batch line and
// the record is cleared.
func TestJobTransitionRules(t *testing.T) {
	dir := t.TempDir()
	state := t.TempDir()
	installFakeHerdr(t, dir)
	t.Setenv("HERDR_PANE_ID", "own")
	writeFile(t, filepath.Join(state, "threads.json"), `{
		"job":{"pane":"job"},
		"blk":{"pane":"blk"},
		"own":{"pane":"own"},
		"remote":{"pane":"rjob","machine":"box"},
		"blank":{"pane":""},
		"late":{"pane":"late","machine":"box"}
	}`)
	writeFile(t, filepath.Join(dir, "machines.out"), `[{"label":"box"}]`)

	start := time.Date(2026, 10, 5, 0, 0, 0, 0, time.UTC)
	advance := fakeClock(t, start)

	var buf bytes.Buffer
	w := newWatcher(testCtx(state, "main", &buf), core.NewOut(&buf))

	// first=true twice: a working→idle observed while the tick is still the
	// first one must not be recorded. The guard is otherwise dead, because a
	// fresh watcher has no previous status on its first call.
	writeFile(t, filepath.Join(dir, "local.out"), agents(ag("job", "working", "j")))
	writeFile(t, filepath.Join(dir, "box.out"), `{"result":{"agents":[]}}`)
	if err := w.tick(context.Background(), true); err != nil {
		t.Fatal(err)
	}
	writeFile(t, filepath.Join(dir, "local.out"), agents(ag("job", "idle", "j")))
	if err := w.tick(context.Background(), true); err != nil {
		t.Fatal(err)
	}
	if got := linesOf(&buf); len(got) != 0 {
		t.Fatalf("first tick emitted %q, want nothing", got)
	}
	if got := pendingKeys(t, state); len(got) != 0 {
		t.Fatalf("first tick recorded %q, want nothing", got)
	}

	// Realistic stream on a fresh watcher.
	buf.Reset()
	w = newWatcher(testCtx(state, "main", &buf), core.NewOut(&buf))
	writeFile(t, filepath.Join(dir, "local.out"), agents(
		ag("job", "working", "j"),
		ag("blk", "working", ""),
		ag("fam", "working", ""),
		ag("own", "blocked", "skip-me"),
		ag("nope", "working", ""),
		ag("stay", "working", "same"),
		ag("gone", "blocked", "g"),
		ag("titlech", "idle", "a"),
	))
	writeFile(t, filepath.Join(dir, "box.out"), agents(
		ag("rjob", "working", ""),
		ag("own", "blocked", "remote-own"),
	))
	if err := w.tick(context.Background(), true); err != nil {
		t.Fatal(err)
	}
	// IS-1: gone and the remote own pane are unregistered, so their blocked
	// observations are silent.
	if got := linesOf(&buf); len(got) != 0 {
		t.Fatalf("unregistered blocked panes emitted %q, want nothing", got)
	}

	buf.Reset()
	writeFile(t, filepath.Join(dir, "local.out"), agents(
		ag("job", "idle", "j"),
		ag("blk", "blocked", ""),
		ag("fam", "idle", ""),
		ag("own", "idle", ""),
		ag("nope", "done", ""),
		ag("stay", "working", "changed"),
		ag("titlech", "idle", "b"),
	))
	writeFile(t, filepath.Join(dir, "box.out"), agents(
		ag("rjob", "done", ""),
		ag("own", "blocked", "remote-own"),
		ag("late", "idle", ""),
	))
	if err := w.tick(context.Background(), false); err != nil {
		t.Fatal(err)
	}
	// IS-1: the registered pane blk still wakes the agent on blocked. IS-3:
	// the job panes' done is recorded, not printed.
	wantLines(t, linesOf(&buf),
		`HERDR {"machine":"local","pane":"blk","tab":null,"agent":null,"title":null,"cwd":null,"from":"working","to":"blocked"}`,
	)
	wantPending(t, state, "box/rjob", "local/job")

	buf.Reset()
	writeFile(t, filepath.Join(dir, "local.out"), agents(
		ag("job", "idle", "j"),
		ag("blk", "blocked", ""),
		ag("fam", "blocked", ""),
		ag("own", "blocked", "skip-me"),
		ag("gone", "blocked", "g2"),
	))
	writeFile(t, filepath.Join(dir, "box.out"), agents(
		ag("rjob", "done", ""),
		ag("own", "idle", ""),
		ag("late", "working", ""),
	))
	if err := w.tick(context.Background(), false); err != nil {
		t.Fatal(err)
	}
	// Inside the quiet window the batch is held, an unchanged registered pane
	// stays quiet, and an unregistered blocked pane still stays silent.
	if got := linesOf(&buf); len(got) != 0 {
		t.Fatalf("lines before the quiet window = %q, want none", got)
	}

	buf.Reset()
	advance(5 * time.Minute)
	if err := w.tick(context.Background(), false); err != nil {
		t.Fatal(err)
	}
	const at = `"at":"2026-10-05T00:00:00.000Z","first_at":"2026-10-05T00:00:00.000Z","count":1`
	wantLines(t, linesOf(&buf),
		`HERDR {"event":"done-batch","entries":[`+
			`{"machine":"box","pane":"rjob","tab":null,"agent":null,"cwd":null,"from":"working","to":"done",`+at+`},`+
			`{"machine":"local","pane":"job","tab":null,"agent":null,"cwd":null,"from":"working","to":"idle",`+at+`}]}`,
	)
	if got := pendingKeys(t, state); len(got) != 0 {
		t.Fatalf("the flushed record was not cleared: %q", got)
	}
}

func TestHerdrErrorDedupe(t *testing.T) {
	unsetEnv(t, "HERDR_PANE_ID")
	dir := t.TempDir()
	installFakeHerdr(t, dir)
	writeFile(t, filepath.Join(dir, "machines.out"), "[]\n")

	var buf bytes.Buffer
	w := newWatcher(testCtx(t.TempDir(), "main", &buf), core.NewOut(&buf))

	fail := func(stderr string) {
		t.Helper()
		writeFile(t, filepath.Join(dir, "local.err"), stderr)
		writeFile(t, filepath.Join(dir, "local.code"), "1\n")
	}
	fail("  boom \n")
	if err := w.tick(context.Background(), true); err != nil {
		t.Fatal(err)
	}
	if err := w.tick(context.Background(), false); err != nil {
		t.Fatal(err)
	}
	wantLines(t, linesOf(&buf), "LOG herdr local Error: boom")

	buf.Reset()
	fail("other\n")
	if err := w.tick(context.Background(), false); err != nil {
		t.Fatal(err)
	}
	wantLines(t, linesOf(&buf), "LOG herdr local Error: other")

	// A successful snapshot clears the remembered message, so the same
	// failure is reported again the next time it happens.
	buf.Reset()
	removeFile(t, filepath.Join(dir, "local.err"))
	removeFile(t, filepath.Join(dir, "local.code"))
	writeFile(t, filepath.Join(dir, "local.out"), `{"result":{"agents":[]}}`)
	if err := w.tick(context.Background(), false); err != nil {
		t.Fatal(err)
	}
	if got := linesOf(&buf); len(got) != 0 {
		t.Fatalf("success logged %q, want silence", got)
	}
	buf.Reset()
	fail("  boom \n")
	if err := w.tick(context.Background(), false); err != nil {
		t.Fatal(err)
	}
	wantLines(t, linesOf(&buf), "LOG herdr local Error: boom")
}

func TestHerdrErrorShapes(t *testing.T) {
	unsetEnv(t, "HERDR_PANE_ID")
	dir := t.TempDir()
	installFakeHerdr(t, dir)

	t.Run("invalid json", func(t *testing.T) {
		writeFile(t, filepath.Join(dir, "machines.out"), "[]\n")
		writeFile(t, filepath.Join(dir, "local.out"), "not-json\n")
		var buf bytes.Buffer
		w := newWatcher(testCtx(t.TempDir(), "main", &buf), core.NewOut(&buf))
		if err := w.tick(context.Background(), true); err != nil {
			t.Fatal(err)
		}
		if err := w.tick(context.Background(), false); err != nil {
			t.Fatal(err)
		}
		got := linesOf(&buf)
		if len(got) != 1 || !strings.HasPrefix(got[0], "LOG herdr local SyntaxError: ") {
			t.Fatalf("lines = %q, want one SyntaxError LOG", got)
		}
	})

	t.Run("blank stderr", func(t *testing.T) {
		writeFile(t, filepath.Join(dir, "machines.out"), "[]\n")
		writeFile(t, filepath.Join(dir, "local.out"), "")
		writeFile(t, filepath.Join(dir, "local.err"), " \n\t")
		writeFile(t, filepath.Join(dir, "local.code"), "7\n")
		var buf bytes.Buffer
		w := newWatcher(testCtx(t.TempDir(), "main", &buf), core.NewOut(&buf))
		if err := w.tick(context.Background(), true); err != nil {
			t.Fatal(err)
		}
		wantLines(t, linesOf(&buf), "LOG herdr local Error: exit 7")
	})

	t.Run("rune truncation", func(t *testing.T) {
		writeFile(t, filepath.Join(dir, "machines.out"), "[]\n")
		removeFile(t, filepath.Join(dir, "local.out"))
		writeFile(t, filepath.Join(dir, "local.err"), "\n"+strings.Repeat("😀", 201)+"\n")
		writeFile(t, filepath.Join(dir, "local.code"), "1\n")
		var buf bytes.Buffer
		w := newWatcher(testCtx(t.TempDir(), "main", &buf), core.NewOut(&buf))
		if err := w.tick(context.Background(), true); err != nil {
			t.Fatal(err)
		}
		wantLines(t, linesOf(&buf), "LOG herdr local Error: "+strings.Repeat("😀", 200))
	})

	t.Run("machine list failure is silent", func(t *testing.T) {
		writeFile(t, filepath.Join(dir, "machines.out"), "nope\n")
		writeFile(t, filepath.Join(dir, "machines.code"), "1\n")
		writeFile(t, filepath.Join(dir, "machines.err"), "machine down\n")
		removeFile(t, filepath.Join(dir, "local.code"))
		removeFile(t, filepath.Join(dir, "local.err"))
		writeFile(t, filepath.Join(dir, "local.out"), `{"result":{"agents":[{"pane_id":"p","agent_status":"blocked"}]}}`)
		removeFile(t, filepath.Join(dir, "calls"))
		state := t.TempDir()
		// The pane is registered so its blocked line still proves the local
		// snapshot ran despite the failed machine list (IS-1).
		writeFile(t, filepath.Join(state, "threads.json"), `{"job":{"pane":"p"}}`)
		var buf bytes.Buffer
		w := newWatcher(testCtx(state, "main", &buf), core.NewOut(&buf))
		if err := w.tick(context.Background(), true); err != nil {
			t.Fatal(err)
		}
		wantLines(t, linesOf(&buf),
			`HERDR {"machine":"local","pane":"p","tab":null,"agent":null,"title":null,"cwd":null,"from":null,"to":"blocked"}`,
		)
		wantCalls(t, dir,
			"machine list --json",
			"agent list",
		)
	})
}

func TestOnceSNAPReadOnly(t *testing.T) {
	root := t.TempDir()
	dir := filepath.Join(root, "fake")
	state := filepath.Join(root, "state")
	installFakeHerdr(t, dir)
	t.Setenv("HERDR_PANE_ID", "p1")
	writeFile(t, filepath.Join(dir, "machines.out"), `{"result":{"machines":[
		{"label":"badbox","enabled":true},
		{"label":"off","enabled":false},
		{"id":"byid"},
		{"label":"","id":"dropped"},
		{"label":null,"id":"fromid"},
		{"label":"box"},
		{"label":"strfalse","enabled":"false"}
	]}}`)
	writeFile(t, filepath.Join(dir, "local.out"), `{"result":{"agents":[
		{"pane_id":"p1","agent_status":"blocked","display_agent":"Claude","terminal_title_stripped":"fix <bug>"},
		{"pane_id":"p2","agent":"codex","terminal_title_stripped":"second task"},
		{"pane_id":"p-empty","display_agent":"","agent":"hidden","agent_status":"blocked","terminal_title_stripped":"t"}
	]}}`)
	writeFile(t, filepath.Join(dir, "badbox.code"), "1\n")
	writeFile(t, filepath.Join(dir, "badbox.err"), "nope\n")
	// Top-level agents (no result.agents) are ignored, matching the TS reader.
	writeFile(t, filepath.Join(dir, "byid.out"), `{"agents":[{"pane_id":"nope","agent_status":"blocked","agent":"x","terminal_title_stripped":"no"}]}`)
	writeFile(t, filepath.Join(dir, "fromid.out"), `{"result":{"agents":[]}}`)
	writeFile(t, filepath.Join(dir, "box.out"), `{"result":{"agents":[{"pane_id":"r1","agent_status":"working","agent":"pi","terminal_title_stripped":"remote"}]}}`)
	writeFile(t, filepath.Join(dir, "strfalse.out"), `{"result":{"agents":[]}}`)

	var buf bytes.Buffer
	c := testCtx(state, "main", &buf)
	c.Flags = map[string]bool{"--once": true}
	if code := Run(c, []string{"--once"}); code != 0 {
		t.Fatalf("Run --once = %d, want 0", code)
	}
	wantLines(t, linesOf(&buf),
		"LOG herdr badbox Error: nope",
		"SNAP local/p1 blocked Claude fix <bug>",
		"SNAP local/p2 unknown codex second task",
		"SNAP local/p-empty blocked  t",
		"SNAP box/r1 working pi remote",
	)
	if _, err := os.Stat(state); !os.IsNotExist(err) {
		t.Fatalf("state dir created by --once: %v", err)
	}
	wantCalls(t, dir,
		"machine list --json",
		"agent list",
		"--machine badbox agent list",
		"--machine byid agent list",
		"--machine fromid agent list",
		"--machine box agent list",
		"--machine strfalse agent list",
	)
}

func TestMachineListShapes(t *testing.T) {
	unsetEnv(t, "HERDR_PANE_ID")
	cases := []struct {
		name  string
		body  string
		extra []string
	}{
		{
			name:  "top-level array",
			body:  `[{"label":"a"},{"id":"b","enabled":false},{"label":"c","enabled":"false"}]`,
			extra: []string{"--machine a agent list", "--machine c agent list"},
		},
		{
			name:  "null result.machines falls through",
			body:  `{"result":{"machines":null},"machines":[{"label":"z"}]}`,
			extra: []string{"--machine z agent list"},
		},
		{
			name:  "result.machines wins",
			body:  `{"result":{"machines":[{"label":"r"}]},"machines":[{"label":"nope"}]}`,
			extra: []string{"--machine r agent list"},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			installFakeHerdr(t, dir)
			writeFile(t, filepath.Join(dir, "machines.out"), tc.body)
			writeFile(t, filepath.Join(dir, "local.out"), `{"result":{"agents":[]}}`)
			for _, call := range tc.extra {
				name := strings.TrimPrefix(call, "--machine ")
				name = strings.TrimSuffix(name, " agent list")
				writeFile(t, filepath.Join(dir, name+".out"), `{"result":{"agents":[]}}`)
			}
			var buf bytes.Buffer
			w := newWatcher(testCtx(t.TempDir(), "main", &buf), core.NewOut(&buf))
			if err := w.tick(context.Background(), true); err != nil {
				t.Fatal(err)
			}
			if got := linesOf(&buf); len(got) != 0 {
				t.Fatalf("lines = %q, want none", got)
			}
			want := append([]string{"machine list --json", "agent list"}, tc.extra...)
			wantCalls(t, dir, want...)
		})
	}
}

func TestRunStartupLockStop(t *testing.T) {
	dir := t.TempDir()
	state := t.TempDir()
	installFakeHerdr(t, dir)
	t.Setenv("HERDR_PANE_ID", "pane-9")
	writeFile(t, filepath.Join(state, "threads.json"), `{"job":{"pane":"p1"}}`)
	writeFile(t, filepath.Join(dir, "machines.out"), "[]\n")
	writeFile(t, filepath.Join(dir, "local.out"), agents(ag("p1", "working", "w")))

	sleeper := exec.Command("sleep", "30")
	if err := sleeper.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = sleeper.Process.Kill()
		_, _ = sleeper.Process.Wait()
	})
	var buf bytes.Buffer
	c := testCtx(state, "main", &buf)
	sleeps := 0
	prev := sleepFn
	sleepFn = func(ctx context.Context, d time.Duration) error {
		sleeps++
		if d != 5*time.Second {
			t.Errorf("interval = %v, want 5s", d)
		}
		if _, err := os.Stat(filepath.Join(state, "watch-herdr.lock.json")); err != nil {
			t.Errorf("watch-herdr.lock.json: %v", err)
		}
		body, err := os.ReadFile(filepath.Join(state, "watch-herdr.lock.json"))
		if err != nil || !bytes.Contains(body, []byte(fmt.Sprintf(`"pid":%d`, os.Getpid()))) {
			t.Errorf("our own lock not held: %s (%v)", body, err)
		}
		if sleeps == 1 {
			writeFile(t, filepath.Join(dir, "local.out"), agents(ag("p1", "idle", "w")))
			return nil
		}
		return context.Canceled
	}
	t.Cleanup(func() { sleepFn = prev })

	if code := Run(c, nil); code != 0 {
		t.Fatalf("Run = %d, want 0", code)
	}
	if sleeps != 2 {
		t.Fatalf("sleeps = %d, want 2", sleeps)
	}
	// IS-3: the compat Run path records the transition for the quiet batch
	// instead of printing it, and the batch is not due yet.
	wantLines(t, linesOf(&buf),
		"LOG herdr watcher starting (every 5s, skip pane-9)",
	)
	wantPending(t, state, "local/p1")
	if _, err := os.Stat(filepath.Join(state, "watch-herdr.lock.json")); !os.IsNotExist(err) {
		t.Fatalf("lock still held after Run: %v", err)
	}
}

func TestSourceRunStopsOnCancel(t *testing.T) {
	unsetEnv(t, "HERDR_PANE_ID")
	dir := t.TempDir()
	state := filepath.Join(t.TempDir(), "state")
	installFakeHerdr(t, dir)
	writeFile(t, filepath.Join(dir, "machines.out"), "[]\n")
	writeFile(t, filepath.Join(dir, "local.out"), `{"result":{"agents":[{"pane_id":"p","agent_status":"blocked"}]}}`)

	var buf bytes.Buffer
	c := testCtx(state, "main", &buf)
	ctx, cancel := context.WithCancel(context.Background())
	prev := sleepFn
	sleepFn = func(ctx context.Context, d time.Duration) error {
		cancel()
		return ctx.Err()
	}
	t.Cleanup(func() { sleepFn = prev })

	start := time.Now()
	err := Sources(c)[0].Run(ctx, c.Out)
	if err != nil {
		t.Fatal(err)
	}
	if time.Since(start) > 2*time.Second {
		t.Fatalf("source run blocked for %s; cancel must not wait out the 5s interval", time.Since(start))
	}
	wantLines(t, linesOf(&buf),
		"LOG herdr watcher starting (every 5s, skip none)",
	)
	// IS-1: the unregistered blocked pane is silent, so nothing is recorded
	// and the read-only state dir stays absent (IS-4).
	if got := pendingKeys(t, state); len(got) != 0 {
		t.Fatalf("cancel recorded %q, want nothing", got)
	}
	if _, err := os.Stat(state); !os.IsNotExist(err) {
		t.Fatalf("daemon source created state/lock: %v", err)
	}
}

func TestSleepReturnsOnCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	start := time.Now()
	if err := sleepCtx(ctx, 5*time.Second); err == nil {
		t.Fatal("sleepCtx returned nil on a cancelled context")
	}
	if time.Since(start) > time.Second {
		t.Fatalf("cancelled sleep took %s", time.Since(start))
	}
}

func TestSourcesPausable(t *testing.T) {
	srcs := Sources(&core.Ctx{Profile: core.Profile{}})
	if len(srcs) != 1 {
		t.Fatalf("Sources = %d, want 1", len(srcs))
	}
	s := srcs[0]
	if s.Name() != "herdr" {
		t.Errorf("name = %q", s.Name())
	}
	if s.AlwaysOn() {
		t.Errorf("herdr must be pausable (pause-while-idle)")
	}
	name, legacy := s.LockName()
	if name != "watch-herdr" || legacy != "" {
		t.Errorf("lock = %q/%q, want watch-herdr with no legacy", name, legacy)
	}
	if got, want := s.Prefixes(), []string{"HERDR"}; len(got) != 1 || got[0] != want[0] {
		t.Errorf("prefixes = %v, want %v", got, want)
	}
}

// TestNullPaneNotFamily pins IS-1 and IS-3: a registered job pane whose pane id
// is JSON null is watched like any other job pane. The removed family special
// case read the state's family pane and compared it with every pane, and
// paneID.equal treats two null-kind ids as equal, so with no family entry a
// null-id pane was mistaken for the family pane and its working→idle/done
// transition was dropped. It must be recorded for the quiet batch.
func TestNullPaneNotFamily(t *testing.T) {
	for _, status := range []string{"idle", "done"} {
		t.Run(status, func(t *testing.T) {
			dir := t.TempDir()
			state := t.TempDir()
			installFakeHerdr(t, dir)
			// A non-empty own pane id keeps the own-pane skip out of the way:
			// only the family comparison could suppress this transition.
			t.Setenv("HERDR_PANE_ID", "own")
			writeFile(t, filepath.Join(state, "threads.json"), `{"job":{"pane":"null"}}`)
			writeFile(t, filepath.Join(dir, "machines.out"), "[]\n")
			writeFile(t, filepath.Join(dir, "local.out"), `{"result":{"agents":[{"pane_id":null,"agent_status":"working"}]}}`)

			start := time.Date(2026, 10, 5, 0, 0, 0, 0, time.UTC)
			advance := fakeClock(t, start)
			var buf bytes.Buffer
			w := newWatcher(testCtx(state, "main", &buf), core.NewOut(&buf))
			if err := w.tick(context.Background(), true); err != nil {
				t.Fatal(err)
			}
			if got := linesOf(&buf); len(got) != 0 {
				t.Fatalf("first tick emitted %q, want nothing", got)
			}

			writeFile(t, filepath.Join(dir, "local.out"), fmt.Sprintf(`{"result":{"agents":[{"pane_id":null,"agent_status":%q}]}}`, status))
			if err := w.tick(context.Background(), false); err != nil {
				t.Fatal(err)
			}
			if got := linesOf(&buf); len(got) != 0 {
				t.Fatalf("a done printed immediately: %q", got)
			}
			wantPending(t, state, "local/null")
			if got := pendingEntries(t, state)["local/null"].To; got != status {
				t.Fatalf("recorded to = %q, want %q", got, status)
			}

			buf.Reset()
			advance(5 * time.Minute)
			if err := w.tick(context.Background(), false); err != nil {
				t.Fatal(err)
			}
			wantLines(t, linesOf(&buf),
				fmt.Sprintf(`HERDR {"event":"done-batch","entries":[{"machine":"local","pane":null,"tab":null,"agent":null,"cwd":null,"from":"working","to":%q,"at":"2026-10-05T00:00:00.000Z","first_at":"2026-10-05T00:00:00.000Z","count":1}]}`, status),
			)
		})
	}
}

func testCtx(state, _ string, buf *bytes.Buffer) *core.Ctx {
	return &core.Ctx{
		State:   state,
		Profile: core.Profile{},
		Flags:   map[string]bool{},
		Out:     core.NewOut(buf),
	}
}

func linesOf(buf *bytes.Buffer) []string {
	s := buf.String()
	if s == "" {
		return nil
	}
	return strings.Split(strings.TrimSuffix(s, "\n"), "\n")
}

func wantLines(t *testing.T, got []string, want ...string) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("lines = %d, want %d\ngot:\n%s\nwant:\n%s", len(got), len(want), strings.Join(got, "\n"), strings.Join(want, "\n"))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("line %d =\n%s\nwant\n%s", i, got[i], want[i])
		}
	}
}

func wantCalls(t *testing.T, dir string, want ...string) {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(dir, "calls"))
	if err != nil {
		t.Fatal(err)
	}
	got := strings.Split(strings.TrimSuffix(string(b), "\n"), "\n")
	if len(got) != len(want) {
		t.Fatalf("herdr calls =\n%s\nwant\n%s", strings.Join(got, "\n"), strings.Join(want, "\n"))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("call %d = %q, want %q", i, got[i], want[i])
		}
	}
}

func ag(pane, status, title string) string {
	if title == "" && status != "" {
		// Distinguish "no title key" (JSON null) from an empty title. The
		// job-rule fixtures omit the title unless one is given; an empty
		// string here means "omit", and blocked-shape coverage lives in
		// TestBlockedEmittedOnFirstTick.
		return fmt.Sprintf(`{"pane_id":%q,"agent_status":%q}`, pane, status)
	}
	return fmt.Sprintf(`{"pane_id":%q,"agent_status":%q,"terminal_title_stripped":%q}`, pane, status, title)
}

func agents(list ...string) string {
	return `{"result":{"agents":[` + strings.Join(list, ",") + `]}}`
}

func installFakeHerdr(t *testing.T, dir string) {
	t.Helper()
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	bin := filepath.Join(dir, "bin")
	if err := os.MkdirAll(bin, 0o755); err != nil {
		t.Fatal(err)
	}
	script := `#!/bin/sh
dir=$HERDR_FAKE_DIR
if [ -z "$dir" ]; then
  echo "HERDR_FAKE_DIR unset" >&2
  exit 2
fi
printf '%s\n' "$*" >> "$dir/calls"
mode=local
if [ "$1" = "machine" ]; then
  mode=machines
elif [ "$1" = "--machine" ]; then
  mode=$2
  shift 2
fi
if [ "$1" = "pane" ] && [ "$2" = "list" ]; then
  mode="$mode.pane"
fi
if [ "$1" = "pane" ] && [ "$2" = "process-info" ]; then
  mode="$mode.pinfo.$4"
fi
if [ -f "$dir/$mode.err" ]; then
  cat "$dir/$mode.err" >&2
fi
if [ -f "$dir/$mode.out" ]; then
  cat "$dir/$mode.out"
fi
if [ -f "$dir/$mode.code" ]; then
  exit "$(cat "$dir/$mode.code")"
fi
exit 0
`
	if err := os.WriteFile(filepath.Join(bin, "herdr"), []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("HERDR_FAKE_DIR", dir)
	t.Setenv("PATH", bin+string(os.PathListSeparator)+os.Getenv("PATH"))
}

func writeFile(t *testing.T, path, body string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
}

func removeFile(t *testing.T, path string) {
	t.Helper()
	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		t.Fatal(err)
	}
}

func unsetEnv(t *testing.T, key string) {
	t.Helper()
	val, ok := os.LookupEnv(key)
	if err := os.Unsetenv(key); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if ok {
			_ = os.Setenv(key, val)
		} else {
			_ = os.Unsetenv(key)
		}
	})
}
