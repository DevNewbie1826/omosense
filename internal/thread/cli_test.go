package thread_test

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/DevNewbie1826/omosense/internal/core"
)

var (
	binDir    string
	binPath   string
	buildOnce sync.Once
	buildErr  error
)

// pinnedGoEnv keeps the child `go build` on the real Go caches: the tests
// repoint HOME at temp dirs, and a module cache inside t.TempDir leaves
// read-only files that break the cleanup (same lesson as core's cli_test).
var pinnedGoEnv = func() []string {
	out, err := exec.Command("go", "env", "GOMODCACHE", "GOCACHE").Output()
	if err != nil {
		return nil
	}
	vals := strings.Split(strings.TrimSpace(string(out)), "\n")
	if len(vals) != 2 {
		return nil
	}
	return []string{"GOMODCACHE=" + vals[0], "GOCACHE=" + vals[1]}
}()

func TestMain(m *testing.M) {
	code := m.Run()
	if binDir != "" {
		os.RemoveAll(binDir)
	}
	os.Exit(code)
}

func buildBinary(t *testing.T) {
	t.Helper()
	buildOnce.Do(func() {
		var err error
		binDir, err = os.MkdirTemp("", "omosense-threadcli-")
		if err != nil {
			buildErr = err
			return
		}
		binPath = filepath.Join(binDir, "omosense")
		build := exec.Command("go", "build", "-o", binPath, "../../cmd/omosense")
		build.Env = append(os.Environ(), pinnedGoEnv...)
		if out, err := build.CombinedOutput(); err != nil {
			buildErr = fmt.Errorf("go build cmd/omosense: %v\n%s", err, out)
		}
	})
	if buildErr != nil {
		t.Fatalf("build: %v", buildErr)
	}
}

// cliSandbox builds the real binary and points HOME/OMOSENSE_DIR/
// OMOSENSE_STATE at a temp folder with a minimal flat config.
func cliSandbox(t *testing.T) (dir, state string) {
	t.Helper()
	buildBinary(t)
	home := t.TempDir()
	dir = filepath.Join(home, ".omosense")
	state = filepath.Join(dir, "state")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "config.json"), []byte(`{"telegram":{"bot":"b1"}}`), 0o644); err != nil {
		t.Fatal(err)
	}
	t.Setenv("HOME", home)
	t.Setenv("OMOSENSE_DIR", dir)
	t.Setenv("OMOSENSE_STATE", state)
	return dir, state
}

func runBin(t *testing.T, args ...string) (stdout, stderr string, code int) {
	t.Helper()
	cmd := exec.Command(binPath, args...)
	var ob, eb bytes.Buffer
	cmd.Stdout, cmd.Stderr = &ob, &eb
	err := cmd.Run()
	code = 0
	var ee *exec.ExitError
	if errors.As(err, &ee) {
		code = ee.ExitCode()
	} else if err != nil {
		t.Fatalf("run %v: %v", args, err)
	}
	return ob.String(), eb.String(), code
}

// parseOneLine asserts stdout is exactly one THREAD line and parses it.
func parseOneLine(t *testing.T, stdout string) *core.OMap {
	t.Helper()
	lines := strings.Split(strings.TrimSuffix(stdout, "\n"), "\n")
	if len(lines) != 1 || !strings.HasPrefix(lines[0], "THREAD {") {
		t.Fatalf("stdout = %q, want exactly one THREAD line", stdout)
	}
	v, err := core.ParseJSON([]byte(strings.TrimPrefix(lines[0], "THREAD ")))
	if err != nil {
		t.Fatalf("parse THREAD line: %v", err)
	}
	m, ok := v.(*core.OMap)
	if !ok {
		t.Fatalf("THREAD payload is not an object: %q", lines[0])
	}
	return m
}

// TestCLIThreadRegisterAndCloseOnBuiltBinary drives the real binary: the
// wired dispatch, register, close, the close-unknown error, a usage
// error, and both help surfaces (IS-1..IS-3, IS-16).
func TestCLIThreadRegisterAndCloseOnBuiltBinary(t *testing.T) {
	_, state := cliSandbox(t)
	threads := filepath.Join(state, "threads.json")

	stdout, stderr, code := runBin(t, "thread", "register", "t1", "--session", "s1", "--pane", "w3:p5X")
	if code != 0 || stderr != "" {
		t.Fatalf("register: exit=%d stderr=%q stdout=%q", code, stderr, stdout)
	}
	line := parseOneLine(t, stdout)
	if got, want := strings.Join(line.Keys(), ","), "id,session_id,pane,status,started"; got != want {
		t.Errorf("register line keys = %s, want %s", got, want)
	}
	if v, _ := line.Get("status"); v != "active" {
		t.Errorf("register status = %v, want active", v)
	}
	started, _ := line.Get("started")
	if s, ok := started.(string); !ok || !isoReMatch(s) {
		t.Errorf("register started = %v, want core.ISO", started)
	}
	fi, err := os.Stat(threads)
	if err != nil {
		t.Fatalf("threads.json missing after register: %v", err)
	}
	if fi.Mode().Perm() != 0o600 {
		t.Errorf("threads.json mode = %v, want 0600", fi.Mode().Perm())
	}
	if _, err := os.Stat(filepath.Join(state, "threads.lock")); err != nil {
		t.Errorf("threads.lock missing after register: %v", err)
	}

	stdout, stderr, code = runBin(t, "thread", "close", "t1")
	if code != 0 || stderr != "" {
		t.Fatalf("close: exit=%d stderr=%q stdout=%q", code, stderr, stdout)
	}
	line = parseOneLine(t, stdout)
	if v, _ := line.Get("status"); v != "done" {
		t.Errorf("close status = %v, want done", v)
	}
	closed, _ := line.Get("closed")
	if s, ok := closed.(string); !ok || !isoReMatch(s) {
		t.Errorf("close closed = %v, want core.ISO", closed)
	}
	if v, _ := line.Get("started"); v != started {
		t.Errorf("close changed started: %v -> %v", started, v)
	}
	root := parseThreadsFile(t, state)
	entry := entryMap(t, root, "t1")
	if v, _ := entry.Get("session_id"); v != "s1" {
		t.Errorf("file session_id = %v, want s1", v)
	}
	if v, _ := entry.Get("pane"); v != "w3:p5X" {
		t.Errorf("file pane = %v, want w3:p5X", v)
	}
	afterClose := readThreads(t, state)

	_, stderr, code = runBin(t, "thread", "close", "nope")
	if code != 1 || stderr != "omosense: thread close: unknown thread \"nope\"\n" {
		t.Errorf("close unknown: exit=%d stderr=%q", code, stderr)
	}
	if got := readThreads(t, state); got != afterClose {
		t.Errorf("close unknown modified threads.json")
	}

	_, stderr, code = runBin(t, "thread", "register", "x", "--bogus", "y")
	if code != 2 || !strings.HasPrefix(stderr, "Usage: omosense thread") {
		t.Errorf("bad flag: exit=%d stderr=%q", code, stderr)
	}
	if got := readThreads(t, state); got != afterClose {
		t.Errorf("bad flag modified threads.json")
	}

	stdout, stderr, code = runBin(t, "thread", "--help")
	if code != 0 || stderr != "" || !strings.HasPrefix(stdout, "Usage: omosense thread") {
		t.Errorf("thread --help: exit=%d stdout=%.80q stderr=%q", code, stdout, stderr)
	}

	stdout, _, _ = runBin(t, "--help")
	if !strings.Contains(stdout, "  thread    register or close a job thread in threads.json") {
		t.Errorf("top-level usage does not list the thread line: %q", stdout)
	}
}
