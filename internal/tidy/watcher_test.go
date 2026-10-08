package tidy

import (
	"bytes"
	"context"
	"fmt"
	"io"
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
	excluded := makeRepo(t, agents, "skipped-repo")
	commitAt(t, excluded, 1750000200, "e1")             // tidy.exclude: never observed by heads()
	mkdirAll(t, filepath.Join(agents, "norepo"))        // no repo/ subdir
	mkdirAll(t, filepath.Join(agents, "nogit", "repo")) // repo/ without .git
	// alpha has a stale sha (from = that sha); beta is null in the watermark
	// (from = null, still changed).
	writeFile(t, filepath.Join(state, "memory-tidy.json"), `{"repos":{"alpha":"OLD","beta":null}}`)

	var buf bytes.Buffer
	c := testCtx(state, "main", &buf)
	c.Profile.Tidy.Exclude = []string{"skipped-repo"}
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
	if _, err := os.Stat(filepath.Join(state, "memory-tidy.lock.json")); !os.IsNotExist(err) {
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
	excluded := makeRepo(t, agents, "skipped-repo")
	commitAt(t, excluded, 1750000200, "x") // tidy.exclude: heads() skips it
	wm := filepath.Join(state, "memory-tidy.json")
	writeFile(t, wm, `{"note":"x","repos":{"b":"B0"},"extra":1}`)

	var buf bytes.Buffer
	c := testCtx(state, "main", &buf)
	c.Profile.Tidy.Exclude = []string{"skipped-repo"}
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
	// position with the new sha, r2 is appended, the excluded repo is absent.
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
		name     string
		args     []string
		check    float64
		quiet    float64
		checkSet bool
		quietSet bool
		ok       bool
	}{
		{"none given", nil, 0, 0, false, false, true},
		{"space form", []string{"--check-min", "5"}, 300000, 0, true, false, true},
		{"equals form", []string{"--check-min=5"}, 300000, 0, true, false, true},
		{"empty means zero", []string{"--check-min="}, 0, 0, true, false, true},
		{"js trims whitespace", []string{"--check-min", " 7 "}, 420000, 0, true, false, true},
		{"js hex literal", []string{"--check-min", "0x10"}, 960000, 0, true, false, true},
		{"js exponent", []string{"--check-min", "1e2"}, 6000000, 0, true, false, true},
		{"negative zero is finite", []string{"--check-min", "-0"}, 0, 0, true, false, true},
		{"quiet only", []string{"--quiet-min", "90"}, 0, 5400000, false, true, true},
		{"both flags", []string{"--check-min", "5", "--quiet-min=90"}, 300000, 5400000, true, true, true},
		{"infinity rejected", []string{"--check-min", "Infinity"}, 0, 0, false, false, false},
		{"nan rejected", []string{"--check-min", "NaN"}, 0, 0, false, false, false},
		{"underscore rejected", []string{"--check-min", "1_000"}, 0, 0, false, false, false},
		{"negative rejected", []string{"--quiet-min", "-1"}, 0, 0, false, false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			check, quiet, checkSet, quietSet, ok := parseMinutes(core.NewOut(&bytes.Buffer{}), tc.args)
			if ok != tc.ok {
				t.Fatalf("ok = %v, want %v", ok, tc.ok)
			}
			if !ok {
				return
			}
			if check != tc.check || quiet != tc.quiet || checkSet != tc.checkSet || quietSet != tc.quietSet {
				t.Fatalf("minutes = %v/%v (set %v/%v), want %v/%v (set %v/%v)",
					check, quiet, checkSet, quietSet, tc.check, tc.quiet, tc.checkSet, tc.quietSet)
			}
		})
	}
}

func TestMovingHeadNoAlert(t *testing.T) {
	_, agents, state := sandbox(t)
	epoch := int64(1750000000)
	repo := makeRepo(t, agents, "alpha")
	start := time.UnixMilli(epoch * 1000)
	advance := fakeClock(t, start)
	// Pre-seed today's lastBackupDate so the loop's backup pass stays
	// silent and any TIDY line would stand out.
	seed := fmt.Sprintf(`{"repos":{},"lastRun":null,"lastBackupDate":%q}`, seoulDateOf(t, start))
	writeFile(t, filepath.Join(state, "memory-tidy.json"), seed)

	var buf bytes.Buffer
	c := testCtx(state, "main", &buf)
	prev := sleepFn
	passes := 0
	sleepFn = func(ctx context.Context, d time.Duration) error {
		if d != 5*time.Minute {
			t.Errorf("interval = %v, want 5m", d)
		}
		advance(d)
		// HEAD moves every check: a fresh empty commit pinned to the new
		// fake now, so the HEAD commit is never quiet.
		commitAt(t, repo, nowFn().Unix(), "m")
		passes++
		if passes >= 130 { // 130 checks x 5m = 650 minutes of moving HEAD
			return context.Canceled
		}
		return nil
	}
	t.Cleanup(func() { sleepFn = prev })

	if code := Run(c, []string{"--check-min", "5", "--quiet-min", "60"}); code != 0 {
		t.Fatalf("Run = %d, want 0", code)
	}
	if n := strings.Count(buf.String(), "TIDY "); n != 0 {
		t.Fatalf("moving HEAD emitted %d TIDY lines, want 0:\n%s", n, buf.String())
	}
}

func TestQuietRepoAlertsOnceNotWithin6h(t *testing.T) {
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
	passes := 0
	sleepFn = func(ctx context.Context, d time.Duration) error {
		if d != 5*time.Minute {
			t.Errorf("interval = %v, want 5m", d)
		}
		if _, err := os.Stat(filepath.Join(state, "memory-tidy.lock.json")); err == nil {
			lockHeld = true
		} else {
			t.Errorf("lock missing during loop: %v", err)
		}
		advance(d)
		passes++
		// 100 passes = 500m: the first TIDY lands at commit+90m (pass 13)
		// and the 6h re-emit at pass 85, so a rule that never emits cannot
		// hang the loop waiting for a second line that never comes.
		if passes >= 100 {
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
	if _, err := os.Stat(filepath.Join(state, "memory-tidy.lock.json")); !os.IsNotExist(err) {
		t.Fatal("lock not released after Run")
	}
	if got := readFile(t, filepath.Join(state, "memory-tidy.json")); got != seed {
		t.Fatalf("loop rewrote the watermark:\ngot:\n%s\nwant:\n%s", got, seed)
	}
	// Exactly one TIDY once the commit is 90m quiet (pass 13), no re-emit
	// for the same `to` within 6h, and exactly one more after 6h (pass 85).
	line := fmt.Sprintf(`TIDY {"changed":[{"repo":"alpha","from":null,"to":%q}]}`, sha)
	wantLines(t, linesOf(&buf),
		"LOG memory-tidy watcher starting (check 5m, quiet 90m)",
		line,
		line,
	)
}

// emitClock wraps the loop's stdout buffer and stamps the fake-clock time
// (ms since epoch) of every line Out writes, so a test can assert WHEN the
// watcher emitted, not just how often: Out serializes each Emit/Log into
// exactly one Write, and the clock only moves in the injected sleep between
// ticks, so each stamp is the emitting tick's own now.
type emitClock struct {
	buf   *bytes.Buffer
	lines []string
	at    []float64
}

func (e *emitClock) Write(p []byte) (int, error) {
	e.lines = append(e.lines, strings.TrimSuffix(string(p), "\n"))
	e.at = append(e.at, float64(nowFn().UnixMilli()))
	return e.buf.Write(p)
}

// tidyTimes returns the stamped times of the TIDY lines, in order.
func (e *emitClock) tidyTimes() []float64 {
	var out []float64
	for i, line := range e.lines {
		if strings.HasPrefix(line, "TIDY ") {
			out = append(out, e.at[i])
		}
	}
	return out
}

func TestQuietThresholdEmitTimes(t *testing.T) {
	// Guard (v1 B3): the quiet threshold gates the FIRST TIDY at the
	// first check at or after commit+quietMin (the boundary is
	// inclusive: IS-4's "quiet >= quietMin"), and the re-emit lands at
	// exactly first emit + 6h — asserted on the stamped emission times,
	// so a threshold swapped for another configured value (checkMin, any
	// shorter span) or a halved re-emit window fails here even when the
	// final line counts happen to match.
	_, agents, state := sandbox(t)
	epoch := int64(1750000000)
	repo := makeRepo(t, agents, "alpha")
	commitAt(t, repo, epoch, "a1")
	start := time.UnixMilli((epoch + 30*60) * 1000) // 30m after the commit
	advance := fakeClock(t, start)
	// Pre-seed today's lastBackupDate so the loop's backup pass stays
	// silent and only the watcher's own lines are stamped.
	seed := fmt.Sprintf(`{"repos":{},"lastRun":null,"lastBackupDate":%q}`, seoulDateOf(t, start))
	writeFile(t, filepath.Join(state, "memory-tidy.json"), seed)

	var buf bytes.Buffer
	rec := &emitClock{buf: &buf}
	c := testCtx(state, "main", rec)
	prev := sleepFn
	passes := 0
	sleepFn = func(ctx context.Context, d time.Duration) error {
		if d != 5*time.Minute {
			t.Errorf("interval = %v, want 5m", d)
		}
		advance(d)
		passes++
		// 100 passes = 500m: tick 13 runs at commit+90m (the first
		// ready check) and tick 85 at commit+450m (the 6h re-emit), so
		// both emissions are observed whatever their timing.
		if passes >= 100 {
			return context.Canceled
		}
		return nil
	}
	t.Cleanup(func() { sleepFn = prev })

	if code := Run(c, []string{"--check-min", "5", "--quiet-min", "90"}); code != 0 {
		t.Fatalf("Run = %d, want 0", code)
	}
	got := rec.tidyTimes()
	wantFirst := float64((epoch + 90*60) * 1000) // commit + quietMin, tick 13
	wantReemit := wantFirst + reemitMs           // first emit + 6h, tick 85
	if len(got) != 2 {
		t.Fatalf("TIDY emission times = %v, want exactly [commit+90m, commit+90m+6h] (quiet 90m, re-emit 6h)", got)
	}
	if got[0] != wantFirst {
		t.Errorf("first TIDY at %v ms, want %v (first check at or after commit+quietMin; a shorter threshold emits earlier)", got[0], wantFirst)
	}
	if got[1] != wantReemit {
		t.Errorf("re-emit TIDY at %v ms, want %v (exactly 6h after the first emit)", got[1], wantReemit)
	}
}

func TestNoTidyBeforeQuietThreshold(t *testing.T) {
	// Guard (v1 B3): zero TIDY before commit+quietMin: the loop stops
	// with its last check at commit+85m (5m short of the 90m boundary),
	// with checkMin 5 so a threshold swapped for checkMin (5m) or any
	// value <= 85m (e.g. a 30m stand-in) emits inside the window.
	_, agents, state := sandbox(t)
	epoch := int64(1750000000)
	repo := makeRepo(t, agents, "alpha")
	commitAt(t, repo, epoch, "a1")
	start := time.UnixMilli(epoch * 1000) // at the commit itself
	advance := fakeClock(t, start)
	seed := fmt.Sprintf(`{"repos":{},"lastRun":null,"lastBackupDate":%q}`, seoulDateOf(t, start))
	writeFile(t, filepath.Join(state, "memory-tidy.json"), seed)

	var buf bytes.Buffer
	rec := &emitClock{buf: &buf}
	c := testCtx(state, "main", rec)
	prev := sleepFn
	passes := 0
	sleepFn = func(ctx context.Context, d time.Duration) error {
		if d != 5*time.Minute {
			t.Errorf("interval = %v, want 5m", d)
		}
		advance(d)
		passes++
		// 18 passes: ticks run at commit+0..85m and the loop exits
		// before any tick at commit+90m.
		if passes >= 18 {
			return context.Canceled
		}
		return nil
	}
	t.Cleanup(func() { sleepFn = prev })

	if code := Run(c, []string{"--check-min", "5", "--quiet-min", "90"}); code != 0 {
		t.Fatalf("Run = %d, want 0", code)
	}
	if got := rec.tidyTimes(); len(got) != 0 {
		t.Fatalf("TIDY emitted at %v ms after the commit, want none before commit+90m (last check at +85m)", got)
	}
	if n := strings.Count(buf.String(), "TIDY "); n != 0 {
		t.Fatalf("emitted %d TIDY lines before the quiet threshold, want 0:\n%s", n, buf.String())
	}
}

// cadenceRun runs the locked watcher loop for one tick over a repo whose
// HEAD is fresh, under a fake clock: it returns the loop's stdout lines
// and asserts the interval the loop sleeps between ticks (the effective
// check cadence).
func cadenceRun(t *testing.T, tidyCfg core.TidyCfg, args []string, wantInterval time.Duration) []string {
	t.Helper()
	_, agents, state := sandbox(t)
	start := time.UnixMilli(1750000000 * 1000)
	commitAt(t, makeRepo(t, agents, "alpha"), 1750000000, "a1")
	seed := fmt.Sprintf(`{"repos":{},"lastRun":null,"lastBackupDate":%q}`, seoulDateOf(t, start))
	writeFile(t, filepath.Join(state, "memory-tidy.json"), seed)

	var buf bytes.Buffer
	c := testCtx(state, "main", &buf)
	c.Profile.Tidy.CheckMin = tidyCfg.CheckMin
	c.Profile.Tidy.QuietMin = tidyCfg.QuietMin
	advance := fakeClock(t, start)
	prev := sleepFn
	sleepFn = func(ctx context.Context, d time.Duration) error {
		if d != wantInterval {
			t.Errorf("interval = %v, want %v", d, wantInterval)
		}
		advance(d)
		return context.Canceled
	}
	t.Cleanup(func() { sleepFn = prev })
	if code := Run(c, args); code != 0 {
		t.Fatalf("Run = %d, want 0", code)
	}
	return linesOf(&buf)
}

func TestConfigCadenceReachesWatcher(t *testing.T) {
	// Guard: tidy.checkMin/tidy.quietMin set both the check interval the
	// loop sleeps and the start LOG's effective thresholds.
	got := cadenceRun(t, core.TidyCfg{CheckMin: floatPtr(7), QuietMin: floatPtr(93)}, nil, 7*time.Minute)
	wantLines(t, got, "LOG memory-tidy watcher starting (check 7m, quiet 93m)")
}

func TestFlagBeatsConfigCadence(t *testing.T) {
	// Guard: --check-min/--quiet-min override the config values.
	got := cadenceRun(t, core.TidyCfg{CheckMin: floatPtr(7), QuietMin: floatPtr(93)},
		[]string{"--check-min", "3", "--quiet-min", "11"}, 3*time.Minute)
	wantLines(t, got, "LOG memory-tidy watcher starting (check 3m, quiet 11m)")
}

func TestUnknownFlagIgnored(t *testing.T) {
	// A removed threshold flag is now just an unknown flag: like every
	// unknown flag it neither errors nor changes the thresholds, so the
	// loop runs on the 10/60 defaults (the report names the removed
	// flag).
	got := cadenceRun(t, core.TidyCfg{}, []string{"--gone-flag=240"}, 10*time.Minute)
	wantLines(t, got, "LOG memory-tidy watcher starting (check 10m, quiet 60m)")
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
	if _, err := os.Stat(filepath.Join(state, "memory-tidy.lock.json")); !os.IsNotExist(err) {
		t.Fatal("daemon-hosted source took the lock")
	}
	date := seoulDateOf(t, time.UnixMilli(epoch*1000))
	wantLines(t, linesOf(&buf),
		"LOG memory-tidy watcher starting (check 10m, quiet 60m)",
		fmt.Sprintf("LOG memory-tidy backup %s repos=1 bytes=%d failed=none", date, statOf(t, filepath.Join(backupsOf(t), date, "alpha.bundle")).Size()),
	)
}

func TestSourcesPausable(t *testing.T) {
	srcs := Sources(&core.Ctx{Profile: core.Profile{Tidy: core.TidyCfg{Enabled: true}}})
	if len(srcs) != 1 {
		t.Fatalf("Sources = %d, want 1", len(srcs))
	}
	s := srcs[0]
	if s.Name() != "tidy" {
		t.Errorf("name = %q", s.Name())
	}
	if s.AlwaysOn() {
		t.Errorf("tidy must be pausable (pause-while-idle)")
	}
	name, legacy := s.LockName()
	if name != "memory-tidy" || legacy != "" {
		t.Errorf("lock = %q/%q, want memory-tidy with no legacy", name, legacy)
	}
	if got, want := s.Prefixes(), []string{"TIDY"}; len(got) != 1 || got[0] != want[0] {
		t.Errorf("prefixes = %v, want %v", got, want)
	}
}

func TestTidySkipRules(t *testing.T) {
	_, agents, state := sandbox(t)
	commitAt(t, makeRepo(t, agents, "self"), 1750000000, "self")
	shaX := commitAt(t, makeRepo(t, agents, "x"), 1750000100, "x")
	shaY := commitAt(t, makeRepo(t, agents, "y"), 1750000200, "y")
	onlyY := fmt.Sprintf(`TIDY {"changed":[{"repo":"y","from":null,"to":%q}]}`, shaY)

	t.Run("own memory", func(t *testing.T) {
		var buf bytes.Buffer
		c := testCtx(state, "main", &buf)
		c.Profile.Memory = "self"
		c.Flags["--now"] = true
		if code := Run(c, []string{"--now"}); code != 0 {
			t.Fatalf("Run --now = %d, want 0", code)
		}
		wantLines(t, linesOf(&buf),
			fmt.Sprintf(`TIDY {"changed":[{"repo":"x","from":null,"to":%q},{"repo":"y","from":null,"to":%q}]}`, shaX, shaY))
	})
	t.Run("exclude beats learnOthers", func(t *testing.T) {
		var buf bytes.Buffer
		c := testCtx(state, "main", &buf)
		c.Profile.Memory = "self"
		c.Profile.Tidy.Exclude = []string{"x"}
		c.Flags["--now"] = true
		if code := Run(c, []string{"--now"}); code != 0 {
			t.Fatalf("Run --now = %d, want 0", code)
		}
		wantLines(t, linesOf(&buf), onlyY)
	})
	t.Run("learnOthers false", func(t *testing.T) {
		var buf bytes.Buffer
		c := testCtx(state, "main", &buf)
		c.Profile.Memory = "self"
		c.Profile.Tidy.Exclude = []string{"x"}
		c.Profile.Tidy.LearnOthers = false
		c.Flags["--now"] = true
		if code := Run(c, []string{"--now"}); code != 0 {
			t.Fatalf("Run --now = %d, want 0", code)
		}
		if buf.Len() != 0 {
			t.Fatalf("stdout = %q, want no lines", buf.String())
		}
	})
}

func TestDisabledTidyOneShot(t *testing.T) {
	for _, tc := range []struct {
		name string
		flag string
		args []string
	}{
		{"now", "--now", []string{"--now"}},
		{"write-watermark", "--write-watermark", []string{"--write-watermark", "y=abc"}},
		{"backup-now", "--backup-now", []string{"--backup-now"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, agents, state := sandbox(t)
			commitAt(t, makeRepo(t, agents, "y"), 1750000000, "y")
			var buf bytes.Buffer
			c := testCtx(state, "family", &buf)
			c.Profile.Tidy.Enabled = false
			c.Profile.Tidy.LearnOthers = true
			c.Flags[tc.flag] = true
			if code := Run(c, tc.args); code != 0 {
				t.Fatalf("Run = %d, want 0", code)
			}
			wantLines(t, linesOf(&buf), "LOG tidy disabled")
			if _, err := os.Stat(filepath.Join(state, "memory-tidy.json")); !os.IsNotExist(err) {
				t.Fatal("disabled tidy wrote a watermark")
			}
			if _, err := os.Stat(backupsOf(t)); !os.IsNotExist(err) {
				t.Fatal("disabled tidy wrote a backup")
			}
		})
	}
}

func TestSourcesDisabled(t *testing.T) {
	srcs := Sources(&core.Ctx{Profile: core.Profile{}})
	if len(srcs) != 0 {
		t.Fatalf("Sources = %d, want none when tidy is disabled", len(srcs))
	}
}

func floatPtr(f float64) *float64 { return &f }

func testCtx(state, _ string, w io.Writer) *core.Ctx {
	return &core.Ctx{
		State: state,
		Profile: core.Profile{
			Tidy: core.TidyCfg{Enabled: true, LearnOthers: true},
		},
		Flags: map[string]bool{},
		Out:   core.NewOut(w),
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
