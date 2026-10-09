package tidy

import (
	"bytes"
	"encoding/json"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// makeRepos creates n agent repos named r01..rNN, each with one commit at
// epoch, and returns the names in heads() scan order with their HEADs.
func makeRepos(t *testing.T, agents string, n int, epoch int64) ([]string, map[string]string) {
	t.Helper()
	names := make([]string, 0, n)
	shas := make(map[string]string, n)
	for i := 1; i <= n; i++ {
		name := fmt.Sprintf("r%02d", i)
		names = append(names, name)
		shas[name] = commitAt(t, makeRepo(t, agents, name), epoch, "c-"+name)
	}
	return names, shas
}

// tidyPayload is one decoded TIDY line.
type tidyPayload struct {
	Changed []struct {
		Repo string `json:"repo"`
		From any    `json:"from"`
		To   string `json:"to"`
	} `json:"changed"`
}

// decodeTidyLines decodes the TIDY lines of a run, in order.
func decodeTidyLines(t *testing.T, lines []string) []tidyPayload {
	t.Helper()
	var out []tidyPayload
	for _, line := range lines {
		if !strings.HasPrefix(line, "TIDY ") {
			continue
		}
		var p tidyPayload
		if err := json.Unmarshal([]byte(strings.TrimPrefix(line, "TIDY ")), &p); err != nil {
			t.Fatalf("TIDY line %q: %v", line, err)
		}
		out = append(out, p)
	}
	return out
}

// wantOneLine rebuilds today's single-line TIDY shape from the repo names
// and shas, so the <=10 case is pinned byte-for-byte.
func wantOneLine(names []string, shas map[string]string) string {
	var sb strings.Builder
	sb.WriteString(`TIDY {"changed":[`)
	for i, name := range names {
		if i > 0 {
			sb.WriteString(",")
		}
		fmt.Fprintf(&sb, `{"repo":%q,"from":null,"to":%q}`, name, shas[name])
	}
	sb.WriteString("]}")
	return sb.String()
}

func TestTidySplitsTenPerLine(t *testing.T) {
	// Guard (IS-1 + COMPANION): every TIDY line carries at most the owner's
	// literal 10 repos, and --now/--once splits the same way.
	if maxTidyPerLine != 10 {
		t.Fatalf("maxTidyPerLine = %d, want the owner's 10", maxTidyPerLine)
	}
	epoch := int64(1750001400)
	for _, tc := range []struct {
		repos int
		sizes []int
	}{
		{10, []int{10}},
		{11, []int{10, 1}},
		{23, []int{10, 10, 3}},
	} {
		t.Run(fmt.Sprintf("%d repos", tc.repos), func(t *testing.T) {
			_, agents, state := sandbox(t)
			names, shas := makeRepos(t, agents, tc.repos, epoch)

			var buf bytes.Buffer
			c := testCtx(state, "main", &buf)
			c.Flags["--now"] = true
			if code := Run(c, []string{"--now"}); code != 0 {
				t.Fatalf("Run --now = %d, want 0", code)
			}
			lines := linesOf(&buf)
			if len(lines) != len(tc.sizes) {
				t.Fatalf("TIDY lines = %d, want %d:\n%s", len(lines), len(tc.sizes), strings.Join(lines, "\n"))
			}
			if tc.repos == 10 {
				if got := strings.Join(lines, "\n"); got != wantOneLine(names, shas) {
					t.Fatalf("10-repo output:\ngot:\n%s\nwant:\n%s", got, wantOneLine(names, shas))
				}
			}
			var got []string
			for i, p := range decodeTidyLines(t, lines) {
				if len(p.Changed) != tc.sizes[i] {
					t.Errorf("line %d carries %d repos, want %d", i, len(p.Changed), tc.sizes[i])
				}
				for _, ch := range p.Changed {
					if ch.To != shas[ch.Repo] {
						t.Errorf("line %d repo %q to = %q, want %q", i, ch.Repo, ch.To, shas[ch.Repo])
					}
					got = append(got, ch.Repo)
				}
			}
			if strings.Join(got, ",") != strings.Join(names, ",") {
				t.Fatalf("reported repos = %v, want %v (no loss, no duplicate, order kept)", got, names)
			}
		})
	}
}

func TestTidyTickSplitsTenPerLine(t *testing.T) {
	// Guard (IS-1): the locked watcher loop splits a 23-repo tick into
	// consecutive TIDY lines of 10/10/3, in order, no repo lost or repeated.
	_, agents, state := sandbox(t)
	epoch := int64(1750001400)
	names, shas := makeRepos(t, agents, 23, epoch)
	start := time.UnixMilli((epoch + 90*60) * 1000)
	advance := fakeClock(t, start)
	writeFile(t, filepath.Join(state, "memory-tidy.json"),
		fmt.Sprintf(`{"repos":{},"lastRun":null,"lastBackupDate":%q}`, seoulDateOf(t, start)))

	var buf bytes.Buffer
	c := testCtx(state, "main", &buf)
	c.Profile.Tidy.CheckMin = floatPtr(5)
	c.Profile.Tidy.QuietMin = floatPtr(90)
	tickDriver(t, advance, 5*time.Minute, 1)
	if code := Run(c, nil); code != 0 {
		t.Fatalf("Run = %d, want 0", code)
	}
	lines := linesOf(&buf)
	if len(lines) != 4 {
		t.Fatalf("lines = %d, want the start LOG plus 3 TIDY lines:\n%s", len(lines), strings.Join(lines, "\n"))
	}
	if lines[0] != "LOG memory-tidy watcher starting (check 5m, quiet 90m)" {
		t.Fatalf("line 0 = %q", lines[0])
	}
	if got := strings.Join(lines[1:], "\n"); got != wantSplitLines(names, shas, []int{10, 10, 3}) {
		t.Fatalf("tick output:\ngot:\n%s\nwant:\n%s", got, wantSplitLines(names, shas, []int{10, 10, 3}))
	}
}

// wantSplitLines rebuilds the exact consecutive TIDY lines for sizes.
func wantSplitLines(names []string, shas map[string]string, sizes []int) string {
	var lines []string
	at := 0
	for _, size := range sizes {
		lines = append(lines, wantOneLine(names[at:at+size], shas))
		at += size
	}
	return strings.Join(lines, "\n")
}
