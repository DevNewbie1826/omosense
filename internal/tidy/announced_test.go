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
)

// testStartLog is the loop's first stdout line for the 5m/90m thresholds
// the tests below configure.
const testStartLog = "LOG memory-tidy watcher starting (check 5m, quiet 90m)"

// testEpoch is 2025-06-15T15:30:00Z = 2025-06-16 00:30 in Seoul, so a run
// that spans a few hours stays inside one backup date.
const testEpoch = int64(1750001400)

// tickDriver installs the loop's sleep hook for exactly n ticks under the
// shared fake clock: each tick's sleep advances the clock by interval and
// the n-th ends the loop, so a loop that returns has run n ticks.
func tickDriver(t *testing.T, advance func(time.Duration), interval time.Duration, n int) {
	t.Helper()
	prev := sleepFn
	passes := 0
	sleepFn = func(ctx context.Context, d time.Duration) error {
		if d != interval {
			t.Errorf("interval = %v, want %v", d, interval)
		}
		advance(d)
		passes++
		if passes >= n {
			return context.Canceled
		}
		return nil
	}
	t.Cleanup(func() { sleepFn = prev })
}

// tidyRun runs one locked watcher loop for n fake-clock ticks and returns
// its stdout lines. The watermark must already be seeded when the lane
// needs an existing file.
func tidyRun(t *testing.T, state string, advance func(time.Duration), n int) []string {
	t.Helper()
	var buf bytes.Buffer
	c := testCtx(state, "main", &buf)
	c.Profile.Tidy.CheckMin = floatPtr(5)
	c.Profile.Tidy.QuietMin = floatPtr(90)
	tickDriver(t, advance, 5*time.Minute, n)
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
		lines := tidyRun(t, state, advance, 13)
		date := seoulDateOf(t, start)
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
	tickDriver(t, advance, 5*time.Minute, 13) // tick 13 runs at commit+90m
	if err := Sources(c1)[0].Run(context.Background(), c1.Out); err != nil {
		t.Fatal(err)
	}
	wantLines(t, linesOf(&buf1), testStartLog, line)
	if got := readFile(t, announced); got != announcedDoc("alpha", sha, firstAt) {
		t.Fatalf("tidy-announced.json after run 1:\ngot:\n%s\nwant:\n%s", got, announcedDoc("alpha", sha, firstAt))
	}

	wantLines(t, tidyRun(t, state, advance, 5), testStartLog)
	if got := readFile(t, announced); got != announcedDoc("alpha", sha, firstAt) {
		t.Fatalf("run 2 rewrote tidy-announced.json:\ngot:\n%s\nwant:\n%s", got, announcedDoc("alpha", sha, firstAt))
	}

	advance(6 * time.Hour)
	wantLines(t, tidyRun(t, state, advance, 1), testStartLog, line)
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

	wantLines(t, tidyRun(t, state, advance, 13), testStartLog, tidyLine("alpha", sha1))
	sha2 := commitAt(t, repo, testEpoch, "a2")
	wantLines(t, tidyRun(t, state, advance, 1), testStartLog, tidyLine("alpha", sha2))
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

	wantLines(t, tidyRun(t, state, advance, 13), testStartLog, tidyLine("alpha", sha))

	// The watermark catches up (as `tidy --write-watermark` would), so
	// alpha is no longer in the changed set.
	writeFile(t, filepath.Join(state, "memory-tidy.json"),
		fmt.Sprintf(`{"repos":{"alpha":%q},"lastRun":null,"lastBackupDate":%q}`, sha, date))
	wantLines(t, tidyRun(t, state, advance, 1), testStartLog)
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

	lines := tidyRun(t, state, advance, 2)
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
