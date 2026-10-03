package tidy

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"
)

func TestBackupNowCreatesVerifiesPrunes(t *testing.T) {
	home, agents, state := sandbox(t)
	repoA := makeRepo(t, agents, "a")
	commitAt(t, repoA, 1750000000, "a1")
	repoX := makeRepo(t, agents, "owo-mode-57b805e5")
	commitAt(t, repoX, 1750000100, "x1")                // EXCLUDE set: still backed up
	makeRepo(t, agents, "empty")                        // no commits: qualifies via rev-parse, bundle create fails
	mkdirAll(t, filepath.Join(agents, "plain", "repo")) // not a git repo: skipped
	mkdirAll(t, filepath.Join(agents, "agentonly"))     // no repo/ subdir: skipped

	// 2026-10-03T15:30:00Z is 2026-10-04 00:30 in Seoul.
	start := time.Date(2026, 10, 3, 15, 30, 0, 0, time.UTC)
	fakeClock(t, start)
	date := "2026-10-04"

	backups := filepath.Join(home, ".omo", "memory-backups")
	for i := 1; i <= 16; i++ {
		day := filepath.Join(backups, fmt.Sprintf("2001-01-%02d", i))
		mkdirAll(t, day)
		writeFile(t, filepath.Join(day, "old.bundle"), "junk")
	}

	var buf bytes.Buffer
	c := testCtx(state, "main", &buf)
	c.Flags["--backup-now"] = true
	if code := Run(c, []string{"--backup-now"}); code != 1 {
		t.Fatalf("Run --backup-now = %d, want 1 (the empty repo fails to bundle)", code)
	}

	day := filepath.Join(backups, date)
	if perm := statOf(t, day).Mode().Perm(); perm != 0o700 {
		t.Fatalf("backup dir mode = %o, want 700", perm)
	}
	bundleA := filepath.Join(day, "a.bundle")
	bundleX := filepath.Join(day, "owo-mode-57b805e5.bundle")
	sizeA := statOf(t, bundleA).Size()
	sizeX := statOf(t, bundleX).Size()
	if _, err := os.Stat(filepath.Join(day, "empty.bundle")); !os.IsNotExist(err) {
		t.Fatal("failed bundle was not removed")
	}
	// The bundles are real: verify the one produced from repoA.
	gitIn{dir: repoA}.run(t, "bundle", "verify", bundleA)

	// repos=3 counts a, owo-mode (EXCLUDE) and empty; plain and agentonly
	// never qualify. bytes sums only the verified bundles.
	wantLines(t, linesOf(&buf),
		fmt.Sprintf("LOG memory-tidy backup %s repos=3 bytes=%d failed=empty", date, sizeA+sizeX))

	// Prune keeps the newest 14 date dirs: today plus 2001-01-04..2001-01-16.
	entries, err := os.ReadDir(backups)
	if err != nil {
		t.Fatal(err)
	}
	var days []string
	for _, e := range entries {
		days = append(days, e.Name())
	}
	sort.Strings(days)
	got := strings.Join(days, ",")
	want := ""
	for i := 4; i <= 16; i++ {
		want += fmt.Sprintf("2001-01-%02d,", i)
	}
	want += "2026-10-04"
	if got != want {
		t.Fatalf("remaining date dirs = %s\nwant %s", got, want)
	}

	wantWM := fmt.Sprintf("{\n  \"repos\": {},\n  \"lastRun\": null,\n  \"lastBackupDate\": %q\n}\n", date)
	if gotWM := readFile(t, filepath.Join(state, "memory-tidy.json")); gotWM != wantWM {
		t.Fatalf("watermark bytes:\ngot:\n%s\nwant:\n%s", gotWM, wantWM)
	}

	// Same Seoul day: quietIfDone=false prints the already-ran line, exit 0,
	// and writes nothing new.
	var buf2 bytes.Buffer
	c2 := testCtx(state, "main", &buf2)
	c2.Flags["--backup-now"] = true
	if code := Run(c2, []string{"--backup-now"}); code != 0 {
		t.Fatalf("second Run --backup-now = %d, want 0", code)
	}
	wantLines(t, linesOf(&buf2), fmt.Sprintf("LOG memory-tidy backup %s already ran today", date))
	if statOf(t, bundleA).Size() != sizeA {
		t.Fatal("second --backup-now rewrote a bundle")
	}
}
