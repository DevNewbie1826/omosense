package thread_test

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/DevNewbie1826/omosense/internal/core"
	"github.com/DevNewbie1826/omosense/internal/thread"
)

// newCtx returns a Ctx over a temp state dir whose THREAD lines go to buf.
func newCtx(t *testing.T) (c *core.Ctx, buf *bytes.Buffer, state string) {
	t.Helper()
	state = t.TempDir()
	buf = &bytes.Buffer{}
	return &core.Ctx{State: state, Out: core.NewOut(buf)}, buf, state
}

func threadsPath(state string) string {
	return filepath.Join(state, "threads.json")
}

func writeThreads(t *testing.T, state, content string) {
	t.Helper()
	if err := os.WriteFile(threadsPath(state), []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
}

func readThreads(t *testing.T, state string) string {
	t.Helper()
	b, err := os.ReadFile(threadsPath(state))
	if err != nil {
		t.Fatalf("read threads.json: %v", err)
	}
	return string(b)
}

// captureStderr collects everything Run writes to stderr (usage and error
// lines) while fn runs.
func captureStderr(t *testing.T, fn func()) string {
	t.Helper()
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	old := os.Stderr
	os.Stderr = w
	defer func() { os.Stderr = old }()
	done := make(chan string, 1)
	go func() {
		b, _ := io.ReadAll(r)
		r.Close()
		done <- string(b)
	}()
	fn()
	w.Close()
	return <-done
}

// parseThreadsFile parses the state's threads.json for structural
// assertions (values, key order).
func parseThreadsFile(t *testing.T, state string) *core.OMap {
	t.Helper()
	v, err := core.ParseJSON([]byte(readThreads(t, state)))
	if err != nil {
		t.Fatalf("parse threads.json: %v", err)
	}
	root, ok := v.(*core.OMap)
	if !ok {
		t.Fatalf("threads.json is not an object")
	}
	return root
}

// parseLine parses the single THREAD line Run printed.
func parseLine(t *testing.T, buf *bytes.Buffer) *core.OMap {
	t.Helper()
	v, err := core.ParseJSON([]byte(strings.TrimPrefix(buf.String(), "THREAD ")))
	if err != nil {
		t.Fatalf("parse THREAD line %q: %v", buf.String(), err)
	}
	m, ok := v.(*core.OMap)
	if !ok {
		t.Fatalf("THREAD line is not an object: %q", buf.String())
	}
	return m
}

func entryMap(t *testing.T, root *core.OMap, id string) *core.OMap {
	t.Helper()
	v, ok := root.Get(id)
	if !ok {
		t.Fatalf("threads.json has no entry %q (keys %v)", id, root.Keys())
	}
	e, isMap := v.(*core.OMap)
	if !isMap {
		t.Fatalf("entry %q is not an object", id)
	}
	return e
}

// isoReMatch matches core.ISO: UTC with millisecond precision,
// 2006-01-02T15:04:05.000Z.
func isoReMatch(s string) bool {
	for i, c := range s {
		switch i {
		case 4, 7:
			if c != '-' {
				return false
			}
		case 10:
			if c != 'T' {
				return false
			}
		case 13, 16:
			if c != ':' {
				return false
			}
		case 19:
			if c != '.' {
				return false
			}
		case 23:
			if c != 'Z' {
				return false
			}
		default:
			if c < '0' || c > '9' {
				return false
			}
		}
	}
	return len(s) == 24
}

// fixture is a live-Main-shaped registry: entry job1 carries every field
// the readers know plus unknown ones (tab, brief, note) and Korean text;
// entry other must survive untouched.
const fixture = `{
  "job1": {
    "platform": "telegram",
    "name": "다히 일감 스레드",
    "tab": 2,
    "pane": "w3:p5X",
    "session_id": "old-sess",
    "cwd": "/Volumes/storage/workspace/omosense",
    "brief": "improve-8 위시",
    "status": "working",
    "started": "2026-10-07T01:02:03.000Z",
    "closed": "2026-10-07T04:05:06.000Z",
    "note": "수동 편집 금지"
  },
  "other": {
    "pane": "w9:p1"
  }
}
`

// TestRegisterPreservesEntryFieldsAndOrder guards the first Risk row: a
// register that drops the existing entry, an unknown field, or the key
// order (of the file or of the entry) fails this byte-golden compare.
// Only session_id/pane/cwd (the flags given), status (-> active) and
// closed (removed) may change; started keeps its value.
func TestRegisterPreservesEntryFieldsAndOrder(t *testing.T) {
	c, buf, state := newCtx(t)
	writeThreads(t, state, fixture)

	var code int
	stderr := captureStderr(t, func() {
		code = thread.Run(c, []string{"register", "job1", "--session", "new-sess", "--pane", "w3:p9", "--cwd", "/tmp/other"})
	})
	if code != 0 {
		t.Fatalf("exit = %d, stderr = %q", code, stderr)
	}

	wantFile := `{
  "job1": {
    "platform": "telegram",
    "name": "다히 일감 스레드",
    "tab": 2,
    "pane": "w3:p9",
    "session_id": "new-sess",
    "cwd": "/tmp/other",
    "brief": "improve-8 위시",
    "status": "active",
    "started": "2026-10-07T01:02:03.000Z",
    "note": "수동 편집 금지"
  },
  "other": {
    "pane": "w9:p1"
  }
}
`
	if got := readThreads(t, state); got != wantFile {
		t.Errorf("threads.json byte diff:\n got: %q\nwant: %q", got, wantFile)
	}
	wantLine := `THREAD {"id":"job1","platform":"telegram","name":"다히 일감 스레드","tab":2,"pane":"w3:p9","session_id":"new-sess","cwd":"/tmp/other","brief":"improve-8 위시","status":"active","started":"2026-10-07T01:02:03.000Z","note":"수동 편집 금지"}` + "\n"
	if got := buf.String(); got != wantLine {
		t.Errorf("stdout = %q, want %q", got, wantLine)
	}
}

// TestRegisterNewEntryAppendsSetsStartedOnce pins IS-1's new-entry half:
// the id is appended after the existing keys, started is an ISO timestamp
// set only when absent, and --flag=value parses like --flag value.
func TestRegisterNewEntryAppendsSetsStartedOnce(t *testing.T) {
	c, _, state := newCtx(t)
	writeThreads(t, state, `{"old": {"pane": "p0"}}`)

	if code := thread.Run(c, []string{"register", "fresh", "--session=s1"}); code != 0 {
		t.Fatalf("first register exit = %d", code)
	}
	root := parseThreadsFile(t, state)
	if got, want := strings.Join(root.Keys(), ","), "old,fresh"; got != want {
		t.Errorf("file key order = %s, want %s", got, want)
	}
	entry := entryMap(t, root, "fresh")
	if got, want := strings.Join(entry.Keys(), ","), "session_id,status,started"; got != want {
		t.Errorf("entry key order = %s, want %s", got, want)
	}
	started, _ := entry.Get("started")
	s, isStr := started.(string)
	if !isStr || !isoReMatch(s) {
		t.Errorf("started = %v, want a core.ISO timestamp", started)
	}

	// A second register (space form) adds pane but must not touch started.
	if code := thread.Run(c, []string{"register", "fresh", "--pane", "w1:p1"}); code != 0 {
		t.Fatalf("second register exit = %d", code)
	}
	root = parseThreadsFile(t, state)
	entry = entryMap(t, root, "fresh")
	if got, want := strings.Join(entry.Keys(), ","), "session_id,status,started,pane"; got != want {
		t.Errorf("entry key order after re-register = %s, want %s (a new key is appended)", got, want)
	}
	again, _ := entry.Get("started")
	if again != started {
		t.Errorf("started changed on re-register: %v -> %v", started, again)
	}
	wantFile := fmt.Sprintf(`{
  "old": {
    "pane": "p0"
  },
  "fresh": {
    "session_id": "s1",
    "status": "active",
    "started": "%s",
    "pane": "w1:p1"
  }
}
`, s)
	if got := readThreads(t, state); got != wantFile {
		t.Errorf("threads.json byte diff:\n got: %q\nwant: %q", got, wantFile)
	}
}

// TestRegisterAndCloseRefuseMalformedAndArrayFiles guards the second Risk
// row: a malformed or array-form threads.json is never overwritten (exit
// 1, file byte-identical) and names the file with the pinned texts.
func TestRegisterAndCloseRefuseMalformedAndArrayFiles(t *testing.T) {
	cases := []struct {
		name      string
		verb      string
		content   string
		wantExact string // full stderr when the plan pins it, else prefix-checked
	}{
		{name: "malformed register", verb: "register", content: `{"job":`},
		{name: "malformed close", verb: "close", content: `{"job":`},
		{name: "array register", verb: "register", content: `[{"pane": "p1"}]`,
			wantExact: "omosense: thread register: threads.json is an array; thread register needs the object form\n"},
		{name: "array close", verb: "close", content: `[]`,
			wantExact: "omosense: thread close: threads.json is an array; thread close needs the object form\n"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			buf := &bytes.Buffer{}
			c := &core.Ctx{State: t.TempDir(), Out: core.NewOut(buf)}
			writeThreads(t, c.State, tc.content)
			args := []string{tc.verb, "job"}
			if tc.verb == "register" {
				args = append(args, "--session", "s")
			}
			var code int
			stderr := captureStderr(t, func() { code = thread.Run(c, args) })
			if code != 1 {
				t.Fatalf("exit = %d, want 1 (stderr %q)", code, stderr)
			}
			if tc.wantExact != "" {
				if stderr != tc.wantExact {
					t.Errorf("stderr = %q, want %q", stderr, tc.wantExact)
				}
			} else if !strings.HasPrefix(stderr, "omosense: thread "+tc.verb+": threads.json: ") || strings.HasSuffix(stderr, "threads.json: \n") {
				t.Errorf("stderr = %q, want the parse-error shape with a non-empty detail", stderr)
			}
			if got := readThreads(t, c.State); got != tc.content {
				t.Errorf("threads.json was modified:\n got: %q\nwant: %q", got, tc.content)
			}
			if buf.Len() != 0 {
				t.Errorf("stdout = %q, want empty", buf.String())
			}
		})
	}
}

// TestCloseUnknownThreadLeavesFileUntouched pins IS-2's error path: exit 1
// with the exact unknown-thread text and nothing written.
func TestCloseUnknownThreadLeavesFileUntouched(t *testing.T) {
	c, buf, state := newCtx(t)
	writeThreads(t, state, fixture)
	var code int
	stderr := captureStderr(t, func() {
		code = thread.Run(c, []string{"close", "nope"})
	})
	if code != 1 {
		t.Fatalf("exit = %d, want 1", code)
	}
	if want := "omosense: thread close: unknown thread \"nope\"\n"; stderr != want {
		t.Errorf("stderr = %q, want %q", stderr, want)
	}
	if got := readThreads(t, state); got != fixture {
		t.Errorf("threads.json changed on unknown close")
	}
	if buf.Len() != 0 {
		t.Errorf("stdout = %q, want empty", buf.String())
	}
}

// closeFixture is fixture without a closed key: the realistic close
// target, a working thread that was never closed yet.
const closeFixture = `{
  "job1": {
    "platform": "telegram",
    "name": "다히 일감 스레드",
    "tab": 2,
    "pane": "w3:p5X",
    "session_id": "old-sess",
    "cwd": "/Volumes/storage/workspace/omosense",
    "brief": "improve-8 위시",
    "status": "working",
    "started": "2026-10-07T01:02:03.000Z",
    "note": "수동 편집 금지"
  },
  "other": {
    "pane": "w9:p1"
  }
}
`

// TestCloseSetsDoneAndClosed pins IS-2: status done, closed = ISO now
// (appended when absent), every other key and the order preserved, THREAD
// line printed.
func TestCloseSetsDoneAndClosed(t *testing.T) {
	c, buf, state := newCtx(t)
	writeThreads(t, state, closeFixture)
	var code int
	stderr := captureStderr(t, func() {
		code = thread.Run(c, []string{"close", "job1"})
	})
	if code != 0 {
		t.Fatalf("exit = %d, stderr = %q", code, stderr)
	}
	line := parseLine(t, buf)
	if v, _ := line.Get("id"); v != "job1" {
		t.Errorf("line id = %v, want job1", v)
	}
	if v, _ := line.Get("status"); v != "done" {
		t.Errorf("line status = %v, want done", v)
	}
	closed, _ := line.Get("closed")
	cs, isStr := closed.(string)
	if !isStr || !isoReMatch(cs) {
		t.Errorf("line closed = %v, want a core.ISO timestamp", closed)
	}
	if v, _ := line.Get("started"); v != "2026-10-07T01:02:03.000Z" {
		t.Errorf("line started = %v, want the fixture value", v)
	}

	root := parseThreadsFile(t, state)
	entry := entryMap(t, root, "job1")
	if got, want := strings.Join(entry.Keys(), ","), "platform,name,tab,pane,session_id,cwd,brief,status,started,note,closed"; got != want {
		t.Errorf("entry key order = %s, want %s (closed appended)", got, want)
	}
	if v, _ := entry.Get("status"); v != "done" {
		t.Errorf("file status = %v, want done", v)
	}
	if v, _ := entry.Get("name"); v != "다히 일감 스레드" {
		t.Errorf("file name = %v, want the fixture value", v)
	}
	if v, _ := entry.Get("closed"); v != cs {
		t.Errorf("file closed = %v, want the THREAD line value %v", v, cs)
	}
	other := entryMap(t, root, "other")
	if v, _ := other.Get("pane"); v != "w9:p1" {
		t.Errorf("other entry changed: pane = %v", v)
	}
}

// TestUsageErrorsWriteNothing guards the fourth Risk row: every bad argv
// (missing value, unknown flag, repeated flag, missing id, no
// --session/--pane, flag on close, unknown verb, no verb) prints the Help
// to stderr, exits 2, and leaves threads.json byte-identical with no
// THREAD line and no lock file.
func TestUsageErrorsWriteNothing(t *testing.T) {
	cases := [][]string{
		{},
		{"wat", "t1"},
		{"register"},
		{"register", "--session", "s"},
		{"register", "t1", "--session"},
		{"register", "t1", "--session", "--pane", "p"},
		{"register", "t1", "--session=a", "--session=b"},
		{"register", "t1", "--bogus", "x"},
		{"register", "t1", "--name", "x"},
		{"register", "t1", "t2", "--session", "s"},
		{"close"},
		{"close", "t1", "--pane", "p"},
		{"close", "t1", "--bogus"},
	}
	for _, args := range cases {
		buf := &bytes.Buffer{}
		c := &core.Ctx{State: t.TempDir(), Out: core.NewOut(buf)}
		writeThreads(t, c.State, fixture)
		var code int
		stderr := captureStderr(t, func() { code = thread.Run(c, args) })
		if code != 2 {
			t.Errorf("%v: exit = %d, want 2", args, code)
		}
		if !strings.HasPrefix(stderr, "Usage: omosense thread") {
			t.Errorf("%v: stderr = %q, want the thread Help", args, stderr)
		}
		if got := readThreads(t, c.State); got != fixture {
			t.Errorf("%v: threads.json changed", args)
		}
		if buf.Len() != 0 {
			t.Errorf("%v: stdout = %q, want empty", args, buf.String())
		}
		if _, err := os.Stat(filepath.Join(c.State, "threads.lock")); !os.IsNotExist(err) {
			t.Errorf("%v: a lock file exists after a usage error", args)
		}
	}
}

// TestConcurrentRegistersKeepEveryEntry guards the third Risk row: 10
// rounds of 20 goroutines released together through the real Run path
// must leave all 200 ids registered. The flock on threads.lock is the
// only serialization; removing it must lose entries here.
func TestConcurrentRegistersKeepEveryEntry(t *testing.T) {
	c, _, state := newCtx(t)
	const rounds, per = 10, 20
	for r := 0; r < rounds; r++ {
		start := make(chan struct{})
		var wg sync.WaitGroup
		errs := make(chan int, per)
		for i := 0; i < per; i++ {
			id := "r" + strconv.Itoa(r) + "-" + strconv.Itoa(i)
			wg.Add(1)
			go func() {
				defer wg.Done()
				<-start
				if code := thread.Run(c, []string{"register", id, "--session", "s-" + id}); code != 0 {
					errs <- code
				}
			}()
		}
		close(start)
		wg.Wait()
		close(errs)
		for code := range errs {
			t.Fatalf("a concurrent register exited %d", code)
		}
	}
	root := parseThreadsFile(t, state)
	if got := root.Len(); got != rounds*per {
		t.Fatalf("threads.json holds %d entries, want %d (lost updates)", got, rounds*per)
	}
	for r := 0; r < rounds; r++ {
		for i := 0; i < per; i++ {
			want := "r" + strconv.Itoa(r) + "-" + strconv.Itoa(i)
			if !root.Has(want) {
				t.Fatalf("entry %q missing after concurrent registers", want)
			}
		}
	}
}
