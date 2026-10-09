package tidy

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/DevNewbie1826/omosense/internal/core"
)

// testStartLog is the loop's first stdout line for the 5m/90m thresholds
// the tests below configure.
const testStartLog = "LOG memory-tidy watcher starting (check 5m, quiet 90m)"

// testEpoch is 2025-06-15T15:30:00Z = 2025-06-16 00:30 in Seoul, so a run
// that spans a few hours stays inside one backup date.
const testEpoch = int64(1750001400)

// tickDriver installs the loop's sleep hook for exactly n ticks under the
// shared fake clock. Each sleep is one tick that finished: onTick, when
// set, runs before the clock advances, then the clock advances by interval
// and the n-th sleep ends the loop. When the loop returns without having
// slept n times, and the test has not already failed, the cleanup fails
// the test. A per-tick hook that fails the test aborts before advance, so
// the cleanup stays quiet and that hook's assertion is the failure.
func tickDriver(t *testing.T, advance func(time.Duration), interval time.Duration, n int, onTick ...func(completed int)) {
	t.Helper()
	var hook func(int)
	if len(onTick) > 1 {
		t.Fatal("tickDriver accepts at most one per-tick hook")
	}
	if len(onTick) == 1 {
		hook = onTick[0]
	}
	prev := sleepFn
	passes := 0
	sleepFn = func(ctx context.Context, d time.Duration) error {
		if d != interval {
			t.Errorf("interval = %v, want %v", d, interval)
		}
		passes++
		if hook != nil {
			hook(passes)
		}
		advance(d)
		if passes >= n {
			return context.Canceled
		}
		return nil
	}
	t.Cleanup(func() {
		sleepFn = prev
		if !t.Failed() && passes != n {
			t.Errorf("ticks ran = %d, want %d", passes, n)
		}
	})
}

// tidyRun runs one locked watcher loop for n fake-clock ticks and returns
// its stdout lines. The watermark must already be seeded when the lane
// needs an existing file. onTick, when set, sees each finished tick's
// lines before the clock advances.
func tidyRun(t *testing.T, state string, advance func(time.Duration), n int, onTick ...func(completed int, lines []string)) []string {
	t.Helper()
	var hook func(int, []string)
	if len(onTick) > 1 {
		t.Fatal("tidyRun accepts at most one per-tick hook")
	}
	if len(onTick) == 1 {
		hook = onTick[0]
	}
	var buf bytes.Buffer
	c := testCtx(state, "main", &buf)
	c.Profile.Tidy.CheckMin = floatPtr(5)
	c.Profile.Tidy.QuietMin = floatPtr(90)
	tickDriver(t, advance, 5*time.Minute, n, func(completed int) {
		if hook != nil {
			hook(completed, linesOf(&buf))
		}
	})
	if code := Run(c, nil); code != 0 {
		t.Fatalf("Run = %d, want 0", code)
	}
	return linesOf(&buf)
}

// countPrefix counts the lines that start with prefix.
func countPrefix(lines []string, prefix string) int {
	n := 0
	for _, line := range lines {
		if strings.HasPrefix(line, prefix) {
			n++
		}
	}
	return n
}

// containsLine reports whether lines holds want as one whole line.
func containsLine(lines []string, want string) bool {
	for _, line := range lines {
		if line == want {
			return true
		}
	}
	return false
}

// seedWatermark writes an existing memory-tidy.json with today's backup
// date so the loop's backup pass stays silent.
func seedWatermark(t *testing.T, state string, date string) string {
	t.Helper()
	path := filepath.Join(state, "memory-tidy.json")
	writeFile(t, path, fmt.Sprintf(`{"repos":{},"lastRun":null,"lastBackupDate":%q}`, date))
	return path
}

// announcedDoc is the exact expected tidy-announced.json body for one repo.
func announcedDoc(name, sha string, at int64) string {
	return fmt.Sprintf("{\n  \"repos\": {\n    %q: {\n      \"to\": %q,\n      \"at\": %d\n    }\n  }\n}\n", name, sha, at)
}

// tidyLine is today's single-repo TIDY line.
func tidyLine(name, sha string) string {
	return fmt.Sprintf(`TIDY {"changed":[{"repo":%q,"from":null,"to":%q}]}`, name, sha)
}

func TestFirstRunLog(t *testing.T) {
	// Guard (IS-2): exactly one guidance LOG on the first tick that emits
	// when no memory-tidy.json existed at loop start, never when it did,
	// and never twice across two emitting ticks.
	t.Run("absent watermark logs once with counts", func(t *testing.T) {
		_, agents, state := sandbox(t)
		sha := commitAt(t, makeRepo(t, agents, "alpha"), testEpoch, "a1")
		start := time.UnixMilli((testEpoch + 90*60) * 1000) // commit exactly quiet at tick 1
		advance := fakeClock(t, start)
		lines := tidyRun(t, state, advance, 1)
		date := seoulDateOf(t, start)
		wantLines(t, lines,
			testStartLog,
			`LOG memory-tidy no watermark yet: 1 repos reported in 1 TIDY lines; run "omosense tidy --write-watermark" to mark the current HEADs as tidied`,
			tidyLine("alpha", sha),
			fmt.Sprintf("LOG memory-tidy backup %s repos=1 bytes=%d failed=none",
				date, statOf(t, filepath.Join(backupsOf(t), date, "alpha.bundle")).Size()),
		)
		if n := countPrefix(lines, "LOG memory-tidy announced state unreadable: "); n != 0 {
			t.Fatalf("announced unreadable LOGs = %d, want 0 (a missing file is an empty record)", n)
		}
		if got := readFile(t, filepath.Join(state, "tidy-announced.json")); got != announcedDoc("alpha", sha, (testEpoch+90*60)*1000) {
			t.Fatalf("tidy-announced.json:\ngot:\n%s\nwant:\n%s", got, announcedDoc("alpha", sha, (testEpoch+90*60)*1000))
		}
	})

	t.Run("existing watermark logs nothing", func(t *testing.T) {
		_, agents, state := sandbox(t)
		sha := commitAt(t, makeRepo(t, agents, "alpha"), testEpoch, "a1")
		start := time.UnixMilli((testEpoch + 90*60) * 1000)
		advance := fakeClock(t, start)
		seedWatermark(t, state, seoulDateOf(t, start))
		wantLines(t, tidyRun(t, state, advance, 1),
			testStartLog,
			tidyLine("alpha", sha),
		)
	})

	t.Run("printed once across two emitting ticks", func(t *testing.T) {
		_, agents, state := sandbox(t)
		shaA := commitAt(t, makeRepo(t, agents, "alpha"), testEpoch, "a1")
		shaB := commitAt(t, makeRepo(t, agents, "beta"), testEpoch+60*60, "b1")
		start := time.UnixMilli((testEpoch + 90*60) * 1000) // alpha quiet; beta is 30m old
		advance := fakeClock(t, start)
		// 13 ticks of 5m: tick 1 emits alpha, tick 13 (start+60m) emits the
		// now-90m-old beta. A flag that is not cleared logs the guidance twice.
		// The tick-1 backup and the alpha line are asserted before the clock
		// advances, so a later tick cannot supply them.
		date := seoulDateOf(t, start)
		guide := "LOG memory-tidy no watermark yet: "
		lines := tidyRun(t, state, advance, 13, func(completed int, got []string) {
			if completed == 1 {
				if _, err := os.Stat(filepath.Join(backupsOf(t), date, "alpha.bundle")); err != nil {
					t.Fatalf("tick 1 returned without its backup: %v", err)
				}
				if body := readFile(t, filepath.Join(state, "memory-tidy.json")); !strings.Contains(body, fmt.Sprintf("%q", date)) {
					t.Fatalf("tick 1 returned without lastBackupDate stamped: %s", body)
				}
			}
			if n := countPrefix(got, "LOG memory-tidy backup "); n != 1 {
				t.Fatalf("tick %d backup LOGs = %d, want the tick-1 backup only:\n%s", completed, n, strings.Join(got, "\n"))
			}
			if n := countPrefix(got, guide); n != 1 {
				t.Fatalf("tick %d guidance LOGs = %d, want 1", completed, n)
			}
			if completed < 13 {
				if countPrefix(got, "TIDY ") != 1 || !containsLine(got, tidyLine("alpha", shaA)) {
					t.Fatalf("tick %d TIDY lines, want only alpha:\n%s", completed, strings.Join(got, "\n"))
				}
				if containsLine(got, tidyLine("beta", shaB)) {
					t.Fatalf("tick %d emitted beta before it was quiet", completed)
				}
				return
			}
			if !containsLine(got, tidyLine("beta", shaB)) || countPrefix(got, "TIDY ") != 2 {
				t.Fatalf("tick 13 did not emit beta after alpha:\n%s", strings.Join(got, "\n"))
			}
		})
		wantLines(t, lines,
			testStartLog,
			`LOG memory-tidy no watermark yet: 1 repos reported in 1 TIDY lines; run "omosense tidy --write-watermark" to mark the current HEADs as tidied`,
			tidyLine("alpha", shaA),
			fmt.Sprintf("LOG memory-tidy backup %s repos=2 bytes=%d failed=none", date,
				statOf(t, filepath.Join(backupsOf(t), date, "alpha.bundle")).Size()+
					statOf(t, filepath.Join(backupsOf(t), date, "beta.bundle")).Size()),
			tidyLine("beta", shaB),
		)
	})
}

func TestAnnouncedSurvivesRestart(t *testing.T) {
	// Guard (IS-4): the record is persisted, so a restart inside the 6h
	// window re-announces nothing and a restart after +6h re-announces
	// once. Run 1 goes through the host source path to prove the live host
	// persists too.
	_, agents, state := sandbox(t)
	sha := commitAt(t, makeRepo(t, agents, "alpha"), testEpoch, "a1")
	start := time.UnixMilli((testEpoch + 30*60) * 1000)
	advance := fakeClock(t, start)
	seedWatermark(t, state, seoulDateOf(t, start))
	announced := filepath.Join(state, "tidy-announced.json")
	line := tidyLine("alpha", sha)
	firstAt := (testEpoch + 90*60) * 1000

	var buf1 bytes.Buffer
	c1 := testCtx(state, "main", &buf1)
	c1.Profile.Tidy.CheckMin = floatPtr(5)
	c1.Profile.Tidy.QuietMin = floatPtr(90)
	tickDriver(t, advance, 5*time.Minute, 13, func(completed int) { // tick 13 runs at commit+90m
		n := countPrefix(linesOf(&buf1), "TIDY ")
		if completed < 13 && n != 0 {
			t.Fatalf("tick %d announced before the quiet threshold (%d TIDY lines)", completed, n)
		}
		if completed == 13 && n != 1 {
			t.Fatalf("tick 13 TIDY lines = %d, want 1", n)
		}
	})
	if err := Sources(c1)[0].Run(context.Background(), c1.Out); err != nil {
		t.Fatal(err)
	}
	wantLines(t, linesOf(&buf1), testStartLog, line)
	if got := readFile(t, announced); got != announcedDoc("alpha", sha, firstAt) {
		t.Fatalf("tidy-announced.json after run 1:\ngot:\n%s\nwant:\n%s", got, announcedDoc("alpha", sha, firstAt))
	}

	suppressed := 0
	wantLines(t, tidyRun(t, state, advance, 5, func(completed int, got []string) {
		suppressed = completed
		if len(got) != 1 || got[0] != testStartLog {
			t.Fatalf("tick %d of the restarted loop re-announced inside 6h:\n%s", completed, strings.Join(got, "\n"))
		}
	}), testStartLog)
	if suppressed != 5 {
		t.Fatalf("next ticks did not run: completed ticks = %d, want 5 after restart", suppressed)
	}
	if got := readFile(t, announced); got != announcedDoc("alpha", sha, firstAt) {
		t.Fatalf("run 2 rewrote tidy-announced.json:\ngot:\n%s\nwant:\n%s", got, announcedDoc("alpha", sha, firstAt))
	}

	advance(6 * time.Hour)
	reemitted := 0
	wantLines(t, tidyRun(t, state, advance, 1, func(completed int, got []string) {
		reemitted = completed
		if !containsLine(got, line) {
			t.Fatalf("tick %d did not re-announce after 6h:\n%s", completed, strings.Join(got, "\n"))
		}
	}), testStartLog, line)
	if reemitted != 1 {
		t.Fatalf("6h re-announce tick did not run: completed ticks = %d, want 1", reemitted)
	}
	// Run 3's first tick runs 30m after run 2 ended (5 + 5 ticks of 5m) plus
	// the 6h jump.
	reemitAt := firstAt + 30*60*1000 + 6*3600*1000
	if got := readFile(t, announced); got != announcedDoc("alpha", sha, reemitAt) {
		t.Fatalf("tidy-announced.json after the 6h re-emit:\ngot:\n%s\nwant:\n%s",
			got, announcedDoc("alpha", sha, reemitAt))
	}
}

func TestAnnouncedHeadMovedReannounces(t *testing.T) {
	// Guard (IS-4): a repo whose HEAD moved after an announcement is
	// announced again with the new `to`, inside the 6h window included.
	_, agents, state := sandbox(t)
	repo := makeRepo(t, agents, "alpha")
	sha1 := commitAt(t, repo, testEpoch, "a1")
	start := time.UnixMilli((testEpoch + 30*60) * 1000)
	advance := fakeClock(t, start)
	seedWatermark(t, state, seoulDateOf(t, start))

	wantLines(t, tidyRun(t, state, advance, 13, func(completed int, got []string) {
		n := countPrefix(got, "TIDY ")
		if completed < 13 && n != 0 {
			t.Fatalf("tick %d announced before the quiet threshold", completed)
		}
		if completed == 13 && !containsLine(got, tidyLine("alpha", sha1)) {
			t.Fatalf("tick 13 did not announce %s:\n%s", sha1, strings.Join(got, "\n"))
		}
	}), testStartLog, tidyLine("alpha", sha1))
	sha2 := commitAt(t, repo, testEpoch, "a2")
	moved := 0
	wantLines(t, tidyRun(t, state, advance, 1, func(completed int, got []string) {
		moved = completed
		if !containsLine(got, tidyLine("alpha", sha2)) {
			t.Fatalf("tick %d did not re-announce the moved HEAD:\n%s", completed, strings.Join(got, "\n"))
		}
	}), testStartLog, tidyLine("alpha", sha2))
	if moved != 1 {
		t.Fatalf("moved-HEAD tick did not run: completed ticks = %d, want 1", moved)
	}
	got := readFile(t, filepath.Join(state, "tidy-announced.json"))
	if !strings.Contains(got, sha2) || strings.Contains(got, sha1) {
		t.Fatalf("tidy-announced.json = %s, want the record replaced with %s", got, sha2)
	}
}

func TestAnnouncedPurgePersisted(t *testing.T) {
	// Guard (IS-4): a repo that left the changed set loses its record, and
	// the removal is persisted.
	_, agents, state := sandbox(t)
	sha := commitAt(t, makeRepo(t, agents, "alpha"), testEpoch, "a1")
	start := time.UnixMilli((testEpoch + 30*60) * 1000)
	advance := fakeClock(t, start)
	date := seoulDateOf(t, start)
	seedWatermark(t, state, date)

	wantLines(t, tidyRun(t, state, advance, 13, func(completed int, got []string) {
		n := countPrefix(got, "TIDY ")
		if completed < 13 && n != 0 {
			t.Fatalf("tick %d announced before the quiet threshold", completed)
		}
		if completed == 13 && !containsLine(got, tidyLine("alpha", sha)) {
			t.Fatalf("tick 13 did not announce %s:\n%s", sha, strings.Join(got, "\n"))
		}
	}), testStartLog, tidyLine("alpha", sha))

	// The watermark catches up (as `tidy --write-watermark` would), so
	// alpha is no longer in the changed set.
	writeFile(t, filepath.Join(state, "memory-tidy.json"),
		fmt.Sprintf(`{"repos":{"alpha":%q},"lastRun":null,"lastBackupDate":%q}`, sha, date))
	purged := 0
	wantLines(t, tidyRun(t, state, advance, 1, func(completed int, got []string) {
		purged = completed
		if countPrefix(got, "TIDY ") != 0 {
			t.Fatalf("purge tick %d still announced:\n%s", completed, strings.Join(got, "\n"))
		}
	}), testStartLog)
	if purged != 1 {
		t.Fatalf("purge tick did not run: completed ticks = %d, want 1", purged)
	}
	if got, want := readFile(t, filepath.Join(state, "tidy-announced.json")), "{\n  \"repos\": {}\n}\n"; got != want {
		t.Fatalf("tidy-announced.json after the purge:\ngot:\n%s\nwant:\n%s", got, want)
	}
}

func TestAnnouncedUnreadableFailsOpen(t *testing.T) {
	// Guard (IS-5): a malformed file logs once and the loop still emits.
	_, agents, state := sandbox(t)
	sha := commitAt(t, makeRepo(t, agents, "alpha"), testEpoch, "a1")
	start := time.UnixMilli((testEpoch + 90*60) * 1000)
	advance := fakeClock(t, start)
	seedWatermark(t, state, seoulDateOf(t, start))
	announced := filepath.Join(state, "tidy-announced.json")
	writeFile(t, announced, "{oops")

	lines := tidyRun(t, state, advance, 1)
	if len(lines) != 3 {
		t.Fatalf("lines = %d, want the start LOG, one unreadable LOG and one TIDY:\n%s", len(lines), strings.Join(lines, "\n"))
	}
	if lines[0] != testStartLog {
		t.Errorf("line 0 = %q, want %q", lines[0], testStartLog)
	}
	if n := countPrefix(lines, "LOG memory-tidy announced state unreadable: "); n != 1 {
		t.Fatalf("unreadable LOGs = %d, want 1:\n%s", n, strings.Join(lines, "\n"))
	}
	if !strings.HasSuffix(lines[1], "; starting empty") {
		t.Errorf("line 1 = %q, want the unreadable LOG ending in %q", lines[1], "; starting empty")
	}
	if lines[2] != tidyLine("alpha", sha) {
		t.Errorf("line 2 = %q, want %q", lines[2], tidyLine("alpha", sha))
	}
	if got := readFile(t, announced); got != announcedDoc("alpha", sha, (testEpoch+90*60)*1000) {
		t.Fatalf("tidy-announced.json was not rewritten valid:\ngot:\n%s", got)
	}
}

func TestAnnouncedSaveFailureKeepsTick(t *testing.T) {
	// Guard (IS-5): a failed save logs once and the tick continues: the
	// TIDY line stands, the daily backup still runs, and the loop reaches
	// its next tick (2 ticks only complete if the second one runs).
	_, agents, state := sandbox(t)
	sha := commitAt(t, makeRepo(t, agents, "alpha"), testEpoch, "a1")
	start := time.UnixMilli((testEpoch + 90*60) * 1000)
	advance := fakeClock(t, start)
	date := seoulDateOf(t, start)
	wm := filepath.Join(state, "memory-tidy.json")
	writeFile(t, wm, `{"repos":{},"lastRun":null,"lastBackupDate":null}`)
	announced := filepath.Join(state, "tidy-announced.json")
	mkdirAll(t, announced+".tmp") // the write target is a directory, so it fails

	bundle := filepath.Join(backupsOf(t), date, "alpha.bundle")
	reached := 0
	lines := tidyRun(t, state, advance, 2, func(completed int, got []string) {
		reached = completed
		if completed != 1 {
			// Tick 2's backup check is quiet: the date was stamped by tick 1,
			// so this tick must not print a second backup LOG.
			if countPrefix(got, "LOG memory-tidy backup ") != 1 {
				t.Fatalf("tick 2 backup check was not quiet:\n%s", strings.Join(got, "\n"))
			}
			if _, err := os.Stat(bundle); err != nil {
				t.Fatalf("tick 2 did not keep the failed-save tick's backup: %v", err)
			}
			return
		}
		// Before the clock advances, this tick's own backup must already
		// exist. The next tick would otherwise stamp it and mask a skip.
		if _, err := os.Stat(bundle); err != nil {
			t.Fatalf("the failed-save tick returned without its backup: %v", err)
		}
		if body := readFile(t, wm); !strings.Contains(body, fmt.Sprintf("%q", date)) {
			t.Fatalf("the failed-save tick returned without lastBackupDate stamped: %s", body)
		}
		if len(got) != 4 {
			t.Fatalf("tick 1 lines = %d, want the start LOG, one TIDY, one not-saved LOG and the backup LOG:\n%s", len(got), strings.Join(got, "\n"))
		}
		if got[0] != testStartLog {
			t.Errorf("tick 1 line 0 = %q, want %q", got[0], testStartLog)
		}
		if got[1] != tidyLine("alpha", sha) {
			t.Errorf("tick 1 line 1 = %q, want %q", got[1], tidyLine("alpha", sha))
		}
		if n := countPrefix(got, "LOG memory-tidy announced state not saved: "); n != 1 {
			t.Fatalf("tick 1 not-saved LOGs = %d, want 1:\n%s", n, strings.Join(got, "\n"))
		}
		if !strings.Contains(got[2], announced+".tmp") {
			t.Errorf("tick 1 line 2 = %q, want it to name %s", got[2], announced+".tmp")
		}
		wantBackup := fmt.Sprintf("LOG memory-tidy backup %s repos=1 bytes=%d failed=none",
			date, statOf(t, bundle).Size())
		if got[3] != wantBackup {
			t.Errorf("tick 1 line 3 = %q, want the backup LOG %q", got[3], wantBackup)
		}
	})
	if reached != 2 {
		t.Fatalf("next tick did not run: completed ticks = %d, want 2 after failed save", reached)
	}
	if len(lines) != 4 {
		t.Fatalf("lines = %d, want the start LOG, one TIDY, one not-saved LOG and the backup LOG:\n%s", len(lines), strings.Join(lines, "\n"))
	}
	if lines[0] != testStartLog {
		t.Errorf("line 0 = %q, want %q", lines[0], testStartLog)
	}
	if lines[1] != tidyLine("alpha", sha) {
		t.Errorf("line 1 = %q, want %q", lines[1], tidyLine("alpha", sha))
	}
	if n := countPrefix(lines, "LOG memory-tidy announced state not saved: "); n != 1 {
		t.Fatalf("not-saved LOGs = %d, want 1:\n%s", n, strings.Join(lines, "\n"))
	}
	if !strings.Contains(lines[2], announced+".tmp") {
		t.Errorf("line 2 = %q, want it to name %s", lines[2], announced+".tmp")
	}
	wantBackup := fmt.Sprintf("LOG memory-tidy backup %s repos=1 bytes=%d failed=none",
		date, statOf(t, filepath.Join(backupsOf(t), date, "alpha.bundle")).Size())
	if lines[3] != wantBackup {
		t.Errorf("line 3 = %q, want the backup LOG %q (the tick must still back up)", lines[3], wantBackup)
	}
	if _, err := os.Stat(announced); !os.IsNotExist(err) {
		t.Fatal("a failed save left a tidy-announced.json behind")
	}
	if !strings.Contains(readFile(t, wm), fmt.Sprintf("%q", date)) {
		t.Fatalf("backup did not run after the failed save: %s", readFile(t, wm))
	}
}

func TestNowOnceLeavesAnnouncedAbsent(t *testing.T) {
	// Guard (IS-5): the read-only one-shots never read or write
	// tidy-announced.json.
	_, agents, state := sandbox(t)
	commitAt(t, makeRepo(t, agents, "alpha"), testEpoch, "a1")
	announced := filepath.Join(state, "tidy-announced.json")
	for _, tc := range []struct {
		name string
		flag string
		args []string
	}{
		{"now", "--now", []string{"--now"}},
		{"once", "--once", []string{"--once"}},
		{"write-watermark", "--write-watermark", []string{"--write-watermark"}},
		{"backup-now", "--backup-now", []string{"--backup-now"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var buf bytes.Buffer
			c := testCtx(state, "main", &buf)
			c.Flags[tc.flag] = true
			if code := Run(c, tc.args); code != 0 {
				t.Fatalf("Run %s = %d, want 0", tc.name, code)
			}
			if _, err := os.Stat(announced); !os.IsNotExist(err) {
				t.Fatalf("%s touched tidy-announced.json", tc.name)
			}
		})
	}
}

func TestAnnouncedRenameFailureRemovesTmp(t *testing.T) {
	// Guard (D7): tidy-announced.json is a non-empty directory, so the
	// temp file is written and Rename fails. The temp must not remain.
	// Reading that directory fails open (one unreadable LOG). The tick
	// still emits TIDY, logs the save failure once, and backs up.
	_, agents, state := sandbox(t)
	sha := commitAt(t, makeRepo(t, agents, "alpha"), testEpoch, "a1")
	start := time.UnixMilli((testEpoch + 90*60) * 1000)
	advance := fakeClock(t, start)
	date := seoulDateOf(t, start)
	wm := filepath.Join(state, "memory-tidy.json")
	writeFile(t, wm, `{"repos":{},"lastRun":null,"lastBackupDate":null}`)
	announced := filepath.Join(state, "tidy-announced.json")
	mkdirAll(t, announced)
	writeFile(t, filepath.Join(announced, "keep"), "x")

	bundle := filepath.Join(backupsOf(t), date, "alpha.bundle")
	tmp := announced + ".tmp"
	reached := 0
	lines := tidyRun(t, state, advance, 2, func(completed int, got []string) {
		reached = completed
		if completed != 1 {
			return
		}
		if _, err := os.Stat(bundle); err != nil {
			t.Fatalf("the failed-rename tick returned without its backup: %v", err)
		}
		if body := readFile(t, wm); !strings.Contains(body, fmt.Sprintf("%q", date)) {
			t.Fatalf("the failed-rename tick returned without lastBackupDate stamped: %s", body)
		}
		if _, err := os.Stat(tmp); !os.IsNotExist(err) {
			t.Fatalf("tick 1 left %s behind: %v", tmp, err)
		}
		if !containsLine(got, tidyLine("alpha", sha)) {
			t.Fatalf("tick 1 missing TIDY:\n%s", strings.Join(got, "\n"))
		}
		if n := countPrefix(got, "LOG memory-tidy announced state not saved: "); n != 1 {
			t.Fatalf("tick 1 not-saved LOGs = %d, want 1:\n%s", n, strings.Join(got, "\n"))
		}
	})
	if reached != 2 {
		t.Fatalf("next tick did not run: completed ticks = %d, want 2 after failed rename", reached)
	}
	_, tmpErr := os.Stat(tmp)
	if len(lines) != 5 {
		t.Fatalf("lines = %d, want the start LOG, one unreadable LOG, one TIDY, one not-saved LOG and the backup LOG; tmp stat err=%v\n%s", len(lines), tmpErr, strings.Join(lines, "\n"))
	}
	if lines[0] != testStartLog {
		t.Errorf("line 0 = %q, want %q", lines[0], testStartLog)
	}
	if !strings.HasPrefix(lines[1], "LOG memory-tidy announced state unreadable: ") || !strings.Contains(lines[1], "is a directory") {
		t.Errorf("line 1 = %q, want the unreadable LOG for the directory", lines[1])
	}
	if lines[2] != tidyLine("alpha", sha) {
		t.Errorf("line 2 = %q, want %q", lines[2], tidyLine("alpha", sha))
	}
	if n := countPrefix(lines, "LOG memory-tidy announced state not saved: "); n != 1 {
		t.Fatalf("not-saved LOGs = %d, want 1:\n%s", n, strings.Join(lines, "\n"))
	}
	wantBackup := fmt.Sprintf("LOG memory-tidy backup %s repos=1 bytes=%d failed=none",
		date, statOf(t, filepath.Join(backupsOf(t), date, "alpha.bundle")).Size())
	if lines[4] != wantBackup {
		t.Errorf("line 4 = %q, want the backup LOG %q (the tick must still back up)", lines[4], wantBackup)
	}
	if !os.IsNotExist(tmpErr) {
		t.Fatalf("tidy-announced.json.tmp still exists after the failed rename: %v", tmpErr)
	}
	if _, err := os.Stat(filepath.Join(announced, "keep")); err != nil {
		t.Fatalf("destination directory was replaced: %v", err)
	}
}

func TestWriteWatermarkRenameFailureRemovesTmp(t *testing.T) {
	// Guard (D7 companion): writeWatermarkDoc removes <path>.tmp when
	// Rename fails because the destination is a non-empty directory.
	dir := t.TempDir()
	path := filepath.Join(dir, "memory-tidy.json")
	mkdirAll(t, path)
	writeFile(t, filepath.Join(path, "keep"), "x")
	w := core.NewOMap()
	w.Set("repos", core.NewOMap())
	err := writeWatermarkDoc(path, w)
	if err == nil {
		t.Fatal("writeWatermarkDoc returned nil, want the rename error")
	}
	if _, statErr := os.Stat(path + ".tmp"); !os.IsNotExist(statErr) {
		t.Fatalf("%s.tmp still exists after the failed rename: %v", path, statErr)
	}
	if _, statErr := os.Stat(filepath.Join(path, "keep")); statErr != nil {
		t.Fatalf("destination directory was replaced: %v", statErr)
	}
}
