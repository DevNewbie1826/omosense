package tidy

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

func TestChangedDetectionNow(t *testing.T) {
	_, agents, state := sandbox(t)
	repoA := makeRepo(t, agents, "alpha")
	shaA := commitAt(t, repoA, 1750000000, "a1")
	repoB := makeRepo(t, agents, "beta")
	shaB := commitAt(t, repoB, 1750000100, "b1")
	excluded := makeRepo(t, agents, "owo-mode-57b805e5")
	commitAt(t, excluded, 1750000200, "e1")             // EXCLUDE set: never observed by heads()
	mkdirAll(t, filepath.Join(agents, "norepo"))        // no repo/ subdir
	mkdirAll(t, filepath.Join(agents, "nogit", "repo")) // repo/ without .git
	// alpha has a stale sha (from = that sha); beta is null in the watermark
	// (from = null, still changed).
	writeFile(t, filepath.Join(state, "memory-tidy.json"), `{"repos":{"alpha":"OLD","beta":null}}`)

	var buf bytes.Buffer
	c := testCtx(state, "main", &buf)
	c.Flags["--now"] = true
	if code := Run(c, []string{"--now"}); code != 0 {
		t.Fatalf("Run --now = %d, want 0", code)
	}
	wantLines(t, linesOf(&buf),
		fmt.Sprintf(`TIDY {"changed":[{"repo":"alpha","from":"OLD","to":%q},{"repo":"beta","from":null,"to":%q}]}`, shaA, shaB))
}

func TestNowWithMissingAgentsExitsOne(t *testing.T) {
	_, _, state := sandbox(t)
	t.Setenv("OMO_MEMORY_AGENTS", filepath.Join(t.TempDir(), "nope"))
	var buf bytes.Buffer
	c := testCtx(state, "main", &buf)
	c.Flags["--now"] = true
	if code := Run(c, []string{"--now"}); code != 1 {
		t.Fatalf("Run --now = %d, want 1", code)
	}
	if buf.Len() != 0 {
		t.Fatalf("stdout = %q, want no lines", buf.String())
	}
}

func TestOnceReadOnly(t *testing.T) {
	_, agents, state := sandbox(t)
	repoA := makeRepo(t, agents, "alpha")
	shaA := commitAt(t, repoA, 1750000000, "a1")
	repoB := makeRepo(t, agents, "beta")
	shaB := commitAt(t, repoB, 1750000100, "b1")
	wm := filepath.Join(state, "memory-tidy.json")
	body := "{\n  \"repos\": {},\n  \"unknown\": [1, 2],\n  \"lastRun\": \"keep\",\n  \"lastBackupDate\": null\n}\n"
	writeFile(t, wm, body)

	var buf bytes.Buffer
	c := testCtx(state, "main", &buf)
	c.Flags["--once"] = true
	if code := Run(c, []string{"--once"}); code != 0 {
		t.Fatalf("Run --once = %d, want 0", code)
	}
	wantLines(t, linesOf(&buf),
		fmt.Sprintf(`TIDY {"changed":[{"repo":"alpha","from":null,"to":%q},{"repo":"beta","from":null,"to":%q}]}`, shaA, shaB))
	if got := readFile(t, wm); got != body {
		t.Fatalf("--once rewrote the watermark:\ngot:\n%s\nwant:\n%s", got, body)
	}
	if _, err := os.Stat(filepath.Join(state, "memory-tidy-main.lock.json")); !os.IsNotExist(err) {
		t.Fatal("--once took the lock")
	}
	if _, err := os.Stat(wm + ".tmp"); !os.IsNotExist(err) {
		t.Fatal("--once left a tmp file")
	}

	// A state dir that does not exist yet must stay absent (IS-9).
	absent := filepath.Join(t.TempDir(), "no-state")
	var buf2 bytes.Buffer
	c2 := testCtx(absent, "main", &buf2)
	c2.Flags["--once"] = true
	if code := Run(c2, []string{"--once"}); code != 0 {
		t.Fatalf("Run --once (absent state) = %d, want 0", code)
	}
	wantLines(t, linesOf(&buf2),
		fmt.Sprintf(`TIDY {"changed":[{"repo":"alpha","from":null,"to":%q},{"repo":"beta","from":null,"to":%q}]}`, shaA, shaB))
	if _, err := os.Stat(absent); !os.IsNotExist(err) {
		t.Fatal("--once created the state dir")
	}
}

func TestWriteWatermarkPreservesFieldsAndOrder(t *testing.T) {
	_, agents, state := sandbox(t)
	fakeClock(t, time.UnixMilli(1750000000000))
	iso := core.ISO(time.UnixMilli(1750000000000))
	repoR1 := makeRepo(t, agents, "r1")
	sha1 := commitAt(t, repoR1, 1750000000, "one")
	repoR2 := makeRepo(t, agents, "r2")
	sha2 := commitAt(t, repoR2, 1750000100, "two")
	excluded := makeRepo(t, agents, "owo-family-bccd4b63")
	commitAt(t, excluded, 1750000200, "x") // EXCLUDE set: heads() skips it
	wm := filepath.Join(state, "memory-tidy.json")
	writeFile(t, wm, `{"note":"x","repos":{"b":"B0"},"extra":1}`)

	var buf bytes.Buffer
	c := testCtx(state, "main", &buf)
	c.Flags["--write-watermark"] = true
	// "r3=a=b" locks the JS split("=", 2) limit: the value is "a", not "a=b".
	if code := Run(c, []string{"--write-watermark", "r1=AAA", "r3=a=b"}); code != 0 {
		t.Fatalf("Run --write-watermark = %d, want 0", code)
	}
	wantLines(t, linesOf(&buf), "LOG memory-tidy watermark set 2 repos")
	want1 := fmt.Sprintf(`{
  "note": "x",
  "repos": {
    "b": "B0",
    "r1": "AAA",
    "r3": "a"
  },
  "extra": 1,
  "lastRun": %q,
  "lastBackupDate": null
}
`, iso)
	if got := readFile(t, wm); got != want1 {
		t.Fatalf("watermark bytes:\ngot:\n%s\nwant:\n%s", got, want1)
	}

	// Without pairs the watermark defaults to current heads: r1 keeps its
	// position with the new sha, r2 is appended, the EXCLUDE repo is absent.
	buf.Reset()
	if code := Run(c, []string{"--write-watermark"}); code != 0 {
		t.Fatalf("Run --write-watermark (no pairs) = %d, want 0", code)
	}
	wantLines(t, linesOf(&buf), "LOG memory-tidy watermark set 2 repos")
	want2 := fmt.Sprintf(`{
  "note": "x",
  "repos": {
    "b": "B0",
    "r1": %q,
    "r3": "a",
    "r2": %q
  },
  "extra": 1,
  "lastRun": %q,
  "lastBackupDate": null
}
`, sha1, sha2, iso)
	if got := readFile(t, wm); got != want2 {
		t.Fatalf("watermark bytes:\ngot:\n%s\nwant:\n%s", got, want2)
	}
}

func TestBadMinutesFlagExitsTwo(t *testing.T) {
	for _, tc := range []struct {
		name string
		args []string
		flag string
	}{
		{"value form", []string{"--check-min=abc"}, "--check-min"},
		{"space form negative", []string{"--quiet-min", "-5"}, "--quiet-min"},
		{"max garbage", []string{"--max-min=5x"}, "--max-min"},
		{"trailing flag has no value", []string{"--check-min"}, "--check-min"},
		{"first bad flag wins", []string{"--check-min=x", "--quiet-min=y"}, "--check-min"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, _, state := sandbox(t)
			var buf bytes.Buffer
			c := testCtx(state, "main", &buf)
			if code := Run(c, tc.args); code != 2 {
				t.Fatalf("Run = %d, want 2", code)
			}
			wantLines(t, linesOf(&buf), "LOG memory-tidy bad "+tc.flag)
		})
	}
}

func TestParseMinutesForms(t *testing.T) {
	for _, tc := range []struct {
		name  string
		args  []string
		check float64
		quiet float64
		max   float64
		ok    bool
	}{
		{"defaults", nil, 600000, 3600000, 14400000, true},
		{"space form", []string{"--check-min", "5"}, 300000, 3600000, 14400000, true},
		{"equals form", []string{"--check-min=5"}, 300000, 3600000, 14400000, true},
		{"empty means zero", []string{"--check-min="}, 0, 3600000, 14400000, true},
		{"js trims whitespace", []string{"--check-min", " 7 "}, 420000, 3600000, 14400000, true},
		{"js hex literal", []string{"--check-min", "0x10"}, 960000, 3600000, 14400000, true},
		{"js exponent", []string{"--check-min", "1e2"}, 6000000, 3600000, 14400000, true},
		{"negative zero is finite", []string{"--check-min", "-0"}, 0, 3600000, 14400000, true},
		{"infinity rejected", []string{"--check-min", "Infinity"}, 0, 0, 0, false},
		{"nan rejected", []string{"--check-min", "NaN"}, 0, 0, 0, false},
		{"underscore rejected", []string{"--check-min", "1_000"}, 0, 0, 0, false},
		{"negative rejected", []string{"--quiet-min", "-1"}, 0, 0, 0, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			check, quiet, max, ok := parseMinutes(core.NewOut(&bytes.Buffer{}), tc.args)
			if ok != tc.ok {
				t.Fatalf("ok = %v, want %v", ok, tc.ok)
			}
			if !ok {
				return
			}
			if check != tc.check || quiet != tc.quiet || max != tc.max {
				t.Fatalf("minutes = %v/%v/%v, want %v/%v/%v", check, quiet, max, tc.check, tc.quiet, tc.max)
			}
		})
	}
}

func TestLoopQuietThenReemit(t *testing.T) {
	_, agents, state := sandbox(t)
	epoch := int64(1750000000)
	repo := makeRepo(t, agents, "alpha")
	sha := commitAt(t, repo, epoch, "a1")
	start := time.UnixMilli((epoch + 30*60) * 1000) // 30m after the commit
	advance := fakeClock(t, start)
	// Pre-seed today's lastBackupDate so the loop's backup(quiet=true) pass
	// returns silently and the only lines are the watcher's own.
	seed := fmt.Sprintf(`{"repos":{},"lastRun":null,"lastBackupDate":%q}`, seoulDateOf(t, start))
	writeFile(t, filepath.Join(state, "memory-tidy.json"), seed)

	var buf bytes.Buffer
	c := testCtx(state, "main", &buf)
	lockHeld := false
	prev := sleepFn
	sleepFn = func(ctx context.Context, d time.Duration) error {
		if d != 5*time.Minute {
			t.Errorf("interval = %v, want 5m", d)
		}
		if _, err := os.Stat(filepath.Join(state, "memory-tidy-main.lock.json")); err == nil {
			lockHeld = true
		} else {
			t.Errorf("lock missing during loop: %v", err)
		}
		advance(d)
		if strings.Count(buf.String(), "TIDY ") >= 2 {
			return context.Canceled
		}
		return nil
	}
	t.Cleanup(func() { sleepFn = prev })

	if code := Run(c, []string{"--check-min", "5", "--quiet-min", "90"}); code != 0 {
		t.Fatalf("Run = %d, want 0", code)
	}
	if !lockHeld {
		t.Fatal("loop never observed its lock")
	}
	if _, err := os.Stat(filepath.Join(state, "memory-tidy-main.lock.json")); !os.IsNotExist(err) {
		t.Fatal("lock not released after Run")
	}
	if got := readFile(t, filepath.Join(state, "memory-tidy.json")); got != seed {
		t.Fatalf("loop rewrote the watermark:\ngot:\n%s\nwant:\n%s", got, seed)
	}
	// The commit is 30m old; quiet is 90m, so the first TIDY comes at
	// commit+90m (pass 13 of 5m), and the re-emit fires 6h after that —
	// the max rule (240m since first seen) must NOT fire earlier, because
	// the re-emit guard short-circuits first.
	line := fmt.Sprintf(`TIDY {"changed":[{"repo":"alpha","from":null,"to":%q}]}`, sha)
	wantLines(t, linesOf(&buf),
		"LOG memory-tidy watcher starting (profile main, check 5m, quiet 90m, max 240m)",
		line,
		line,
	)
}

func TestLoopMaxEmitsAndBackups(t *testing.T) {
	home, agents, state := sandbox(t)
	epoch := int64(1750000000)
	repo := makeRepo(t, agents, "alpha")
	sha := commitAt(t, repo, epoch, "a1")
	start := time.UnixMilli((epoch + 60) * 1000) // 1m after the commit
	advance := fakeClock(t, start)
	date := seoulDateOf(t, start)

	var buf bytes.Buffer
	c := testCtx(state, "main", &buf)
	prev := sleepFn
	sleepFn = func(ctx context.Context, d time.Duration) error {
		if d != 240*time.Minute {
			t.Errorf("interval = %v, want 240m", d)
		}
		advance(d)
		if strings.Count(buf.String(), "TIDY ") >= 1 {
			return context.Canceled
		}
		return nil
	}
	t.Cleanup(func() { sleepFn = prev })

	if code := Run(c, []string{"--check-min=240", "--quiet-min=600"}); code != 0 {
		t.Fatalf("Run = %d, want 0", code)
	}
	// Pass 1: not quiet (1m old) and now-since = 0, so no TIDY, but the
	// daily backup runs and writes the watermark. Pass 2 (+240m): the max
	// rule (now-since >= 240m) emits even though the commit is 241m old
	// and quiet is 600m.
	bundle := filepath.Join(home, ".omo", "memory-backups", date, "alpha.bundle")
	size := statOf(t, bundle).Size()
	wantLines(t, linesOf(&buf),
		"LOG memory-tidy watcher starting (profile main, check 240m, quiet 600m, max 240m)",
		fmt.Sprintf("LOG memory-tidy backup %s repos=1 bytes=%d failed=none", date, size),
		fmt.Sprintf(`TIDY {"changed":[{"repo":"alpha","from":null,"to":%q}]}`, sha),
	)
	want := fmt.Sprintf("{\n  \"repos\": {},\n  \"lastRun\": null,\n  \"lastBackupDate\": %q\n}\n", date)
	if got := readFile(t, filepath.Join(state, "memory-tidy.json")); got != want {
		t.Fatalf("watermark bytes:\ngot:\n%s\nwant:\n%s", got, want)
	}
}

func TestSourceRunStopsOnCancel(t *testing.T) {
	_, agents, state := sandbox(t)
	epoch := int64(1750000000)
	repo := makeRepo(t, agents, "alpha")
	commitAt(t, repo, epoch, "a1")
	fakeClock(t, time.UnixMilli(epoch*1000)) // commit is 0m old

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
	if err := Sources(c)[0].Run(ctx, c.Out); err != nil {
		t.Fatal(err)
	}
	if time.Since(start) > 2*time.Second {
		t.Fatal("cancel must not wait out the check interval")
	}
	// The daemon owns the lock, so the source must not create one; the
	// first tick still runs its daily backup (0m old commit is not quiet,
	// so no TIDY).
	if _, err := os.Stat(filepath.Join(state, "memory-tidy-main.lock.json")); !os.IsNotExist(err) {
		t.Fatal("daemon-hosted source took the lock")
	}
	date := seoulDateOf(t, time.UnixMilli(epoch*1000))
	wantLines(t, linesOf(&buf),
		"LOG memory-tidy watcher starting (profile main, check 10m, quiet 60m, max 240m)",
		fmt.Sprintf("LOG memory-tidy backup %s repos=1 bytes=%d failed=none", date, statOf(t, filepath.Join(backupsOf(t), date, "alpha.bundle")).Size()),
	)
}

func TestSourcesPausable(t *testing.T) {
	for _, prof := range []string{"main", "family"} {
		srcs := Sources(&core.Ctx{Profile: core.Profile{Name: prof}})
		if len(srcs) != 1 {
			t.Fatalf("profile %s: Sources = %d, want 1", prof, len(srcs))
		}
		s := srcs[0]
		if s.Name() != "tidy" {
			t.Errorf("name = %q", s.Name())
		}
		if s.AlwaysOn() {
			t.Errorf("tidy must be pausable (pause-while-idle)")
		}
		name, legacy := s.LockName()
		if name != "memory-tidy-"+prof || legacy != "" {
			t.Errorf("lock = %q/%q, want memory-tidy-%s with no legacy", name, legacy, prof)
		}
		if got, want := s.Prefixes(), []string{"TIDY"}; len(got) != 1 || got[0] != want[0] {
			t.Errorf("prefixes = %v, want %v", got, want)
		}
	}
}

func testCtx(state, profile string, buf *bytes.Buffer) *core.Ctx {
	return &core.Ctx{
		State:   state,
		Profile: core.Profile{Name: profile},
		Flags:   map[string]bool{},
		Out:     core.NewOut(buf),
	}
}

// sandbox points HOME and OMO_MEMORY_AGENTS at temp dirs so backups and
// repo scans never touch the real ~/.omo. The returned state dir exists.
func sandbox(t *testing.T) (home, agents, state string) {
	t.Helper()
	root := t.TempDir()
	home = filepath.Join(root, "home")
	agents = filepath.Join(root, "agents")
	state = filepath.Join(root, "state")
	for _, d := range []string{home, agents, state} {
		mkdirAll(t, d)
	}
	t.Setenv("HOME", home)
	t.Setenv("OMO_MEMORY_AGENTS", agents)
	return home, agents, state
}

// fakeClock pins nowFn and makes sleepFn advance the fake clock; it returns
// the advance func so tests can layer their own sleepFn on top.
func fakeClock(t *testing.T, start time.Time) func(time.Duration) {
	t.Helper()
	cur := start
	prevNow, prevSleep := nowFn, sleepFn
	advance := func(d time.Duration) { cur = cur.Add(d) }
	nowFn = func() time.Time { return cur }
	sleepFn = func(context.Context, time.Duration) error { return nil }
	t.Cleanup(func() { nowFn, sleepFn = prevNow, prevSleep })
	return advance
}

func backupsOf(t *testing.T) string {
	t.Helper()
	home, err := os.UserHomeDir()
	if err != nil {
		t.Fatal(err)
	}
	return filepath.Join(home, ".omo", "memory-backups")
}

func seoulDateOf(t *testing.T, ts time.Time) string {
	t.Helper()
	loc, err := time.LoadLocation("Asia/Seoul")
	if err != nil {
		t.Fatal(err)
	}
	return ts.In(loc).Format("2006-01-02")
}

type gitIn struct {
	dir string
	env []string
}

func (g gitIn) run(t *testing.T, args ...string) string {
	t.Helper()
	cmd := exec.Command("git", args...)
	cmd.Dir = g.dir
	cmd.Env = append(os.Environ(),
		"GIT_AUTHOR_NAME=test", "GIT_AUTHOR_EMAIL=test@example.com",
		"GIT_COMMITTER_NAME=test", "GIT_COMMITTER_EMAIL=test@example.com",
	)
	cmd.Env = append(cmd.Env, g.env...)
	var out, errb bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &errb
	if err := cmd.Run(); err != nil {
		t.Fatalf("git %v: %v\n%s", args, err, errb.String())
	}
	return strings.TrimSpace(out.String())
}

func makeRepo(t *testing.T, agents, name string) string {
	t.Helper()
	repo := filepath.Join(agents, name, "repo")
	gitIn{}.run(t, "init", "-q", "-b", "main", repo)
	return repo
}

// commitAt creates an empty commit whose committer date is pinned to epoch,
// so committedAt (%ct) is deterministic under the fake clock. It returns HEAD.
func commitAt(t *testing.T, repo string, epoch int64, msg string) string {
	t.Helper()
	date := fmt.Sprintf("%d +0000", epoch)
	gitIn{dir: repo, env: []string{
		"GIT_AUTHOR_DATE=" + date, "GIT_COMMITTER_DATE=" + date,
	}}.run(t, "commit", "--allow-empty", "-q", "-m", msg)
	return gitIn{dir: repo}.run(t, "rev-parse", "HEAD")
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

func writeFile(t *testing.T, path, body string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
}

func readFile(t *testing.T, path string) string {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

func mkdirAll(t *testing.T, path string) {
	t.Helper()
	if err := os.MkdirAll(path, 0o755); err != nil {
		t.Fatal(err)
	}
}

func statOf(t *testing.T, path string) os.FileInfo {
	t.Helper()
	fi, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	return fi
}
