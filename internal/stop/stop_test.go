package stop

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"
)

var (
	buildDir  string
	mainBin   string
	helperBin string
	buildOnce sync.Once
	buildErr  error
)

// pinnedGoEnv keeps the child `go build` on the user's real Go caches: the
// tests repoint HOME at temp dirs, and a module cache inside t.TempDir
// leaves read-only files that break the cleanup.
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
	if buildDir != "" {
		os.RemoveAll(buildDir)
	}
	os.Exit(code)
}

func buildBinaries(t *testing.T) {
	t.Helper()
	buildOnce.Do(func() {
		dir, err := os.MkdirTemp("", "omosense-stopbin-")
		if err != nil {
			buildErr = err
			return
		}
		buildDir = dir
		mainBin = filepath.Join(dir, "omosense")
		helperBin = filepath.Join(dir, "helper")
		for _, b := range []struct{ out, pkg string }{
			{mainBin, "../../cmd/omosense"},
			{helperBin, "./testdata/helper"},
		} {
			cmd := exec.Command("go", "build", "-o", b.out, b.pkg)
			cmd.Env = append(os.Environ(), pinnedGoEnv...)
			if out, err := cmd.CombinedOutput(); err != nil {
				buildErr = fmt.Errorf("go build %s: %v\n%s", b.pkg, err, out)
				return
			}
		}
	})
	if buildErr != nil {
		t.Fatalf("build: %v", buildErr)
	}
}

// child is a started test process whose exit is reaped as soon as it
// happens: stop polls kill(pid, 0), which keeps succeeding for an unreaped
// zombie, so the pid must be reaped promptly.
type child struct {
	cmd  *exec.Cmd
	done chan struct{}
	out  *bufio.Reader
}

func startChild(t *testing.T, name string, args ...string) *child {
	t.Helper()
	cmd := exec.Command(name, args...)
	cmd.Stdout, cmd.Stderr = io.Discard, io.Discard
	if err := cmd.Start(); err != nil {
		t.Fatalf("start %s: %v", name, err)
	}
	return adopt(t, cmd, nil)
}

// startChildPiped starts name with its stdout piped so the test can await a
// startup line before driving it.
func startChildPiped(t *testing.T, name string, args ...string) *child {
	t.Helper()
	cmd := exec.Command(name, args...)
	cmd.Stderr = io.Discard
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := cmd.Start(); err != nil {
		t.Fatalf("start %s: %v", name, err)
	}
	return adopt(t, cmd, bufio.NewReader(stdout))
}

func adopt(t *testing.T, cmd *exec.Cmd, out *bufio.Reader) *child {
	t.Helper()
	c := &child{cmd: cmd, done: make(chan struct{}), out: out}
	go func() { _ = cmd.Wait(); close(c.done) }()
	t.Cleanup(func() {
		_ = cmd.Process.Kill()
		<-c.done
	})
	return c
}

func (c *child) pid() int { return c.cmd.Process.Pid }

// awaitLine blocks until the process prints a line containing want. The
// reader keeps draining afterwards so the child never blocks on a full pipe.
func (c *child) awaitLine(t *testing.T, want string) {
	t.Helper()
	if c.out == nil {
		t.Fatal("awaitLine needs a piped child")
	}
	linec := make(chan string, 1)
	go func() {
		found := false
		for {
			ln, err := c.out.ReadString('\n')
			if err != nil {
				if !found {
					linec <- ""
				}
				return
			}
			if !found && strings.Contains(ln, want) {
				found = true
				linec <- ln
			}
		}
	}()
	select {
	case ln := <-linec:
		if ln == "" {
			t.Fatalf("process ended before printing %q", want)
		}
	case <-time.After(10 * time.Second):
		t.Fatalf("timed out waiting for %q", want)
	}
}

// startHelper runs the testdata helper and waits for its readiness line, so
// the test never signals it before its signal handling is installed.
func startHelper(t *testing.T, bin string, args ...string) *child {
	t.Helper()
	c := startChildPiped(t, bin, args...)
	c.awaitLine(t, "ready")
	return c
}
func copyHelper(t *testing.T, name string) string {
	t.Helper()
	buildBinaries(t)
	src, err := os.ReadFile(helperBin)
	if err != nil {
		t.Fatal(err)
	}
	dst := filepath.Join(t.TempDir(), name)
	if err := os.WriteFile(dst, src, 0o755); err != nil {
		t.Fatal(err)
	}
	return dst
}

// stopEnv points HOME, OMOSENSE_DIR and OMOSENSE_STATE at a temp folder and
// returns the run dir and state dir.
func stopEnv(t *testing.T) (dir, state string) {
	t.Helper()
	home := t.TempDir()
	dir = filepath.Join(home, ".omosense")
	state = filepath.Join(dir, "state")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("HOME", home)
	t.Setenv("OMOSENSE_DIR", dir)
	t.Setenv("OMOSENSE_STATE", state)
	return dir, state
}

func writeConfig(t *testing.T, dir, cfg string) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(dir, "config.json"), []byte(cfg), 0o644); err != nil {
		t.Fatal(err)
	}
}

func writeLock(t *testing.T, state, name, content string) string {
	t.Helper()
	if err := os.MkdirAll(state, 0o755); err != nil {
		t.Fatal(err)
	}
	p := filepath.Join(state, name+".lock.json")
	if err := os.WriteFile(p, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
	return p
}

func liveLockJSON(pid int) string {
	return fmt.Sprintf(`{"pid":%d,"session":null,"pane":null,"cwd":"/tmp","started":"2026-10-08T00:00:00.000Z"}`, pid)
}

func runStop(t *testing.T) (stdout, stderr string, code int) {
	t.Helper()
	var ob, eb bytes.Buffer
	code = Run(&ob, &eb)
	return ob.String(), eb.String(), code
}

// assertOneLine fails unless s is exactly one newline-terminated line: the
// stop contract prints one result line on every path.
func assertOneLine(t *testing.T, s string) {
	t.Helper()
	if !strings.HasSuffix(s, "\n") || strings.Count(s, "\n") != 1 {
		t.Errorf("stdout = %q, want exactly one line", s)
	}
}

// deadPid returns a pid no process holds.
func deadPid(t *testing.T) int {
	t.Helper()
	for i := 0; i < 100; i++ {
		cmd := exec.Command("true")
		if err := cmd.Start(); err != nil {
			t.Fatal(err)
		}
		pid := cmd.Process.Pid
		_ = cmd.Wait()
		if syscall.Kill(pid, 0) != nil {
			return pid
		}
	}
	t.Fatal("no dead pid found")
	return 0
}

// TestStopStopsRunningHost starts a real host in a temp folder and stops it:
// the process and every lock are gone and one line names the pid.
func TestStopStopsRunningHost(t *testing.T) {
	dir, state := stopEnv(t)
	writeConfig(t, dir, `{"herdr":{"enabled":false},"tidy":{"enabled":false}}`)
	buildBinaries(t)
	host := startChildPiped(t, mainBin)
	// The worker takes the source's lock before its job logs the starting
	// line, so awaiting that line is the exact readiness event for the lock.
	host.awaitLine(t, "LOG reminder scheduler starting")

	stdout, stderr, code := runStop(t)
	if code != 0 {
		t.Fatalf("exit = %d, want 0 (stdout=%q stderr=%q)", code, stdout, stderr)
	}
	want := fmt.Sprintf("stopped omosense pid %d\n", host.pid())
	if stdout != want {
		t.Errorf("stdout = %q, want %q", stdout, want)
	}
	<-host.done
	if left := locksIn(t, state); len(left) != 0 {
		t.Errorf("locks left after stop: %v", left)
	}
}

// TestStopNotRunningCreatesNothing pins that a folder with no .omosense at
// all reports "not running", exits 0 and is left byte-identical.
func TestStopNotRunningCreatesNothing(t *testing.T) {
	home := t.TempDir()
	dir := filepath.Join(home, ".omosense")
	t.Setenv("HOME", home)
	t.Setenv("OMOSENSE_DIR", dir)
	t.Setenv("OMOSENSE_STATE", filepath.Join(dir, "state"))

	stdout, stderr, code := runStop(t)
	if code != 0 || stdout != "not running\n" {
		t.Fatalf("exit = %d stdout = %q stderr = %q, want 0/not running", code, stdout, stderr)
	}
	entries, err := os.ReadDir(home)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Errorf("stop created %v, want nothing", entries)
	}
}

// TestStopStaleLockBytesUnchanged pins that a dead pid is reported as a
// stale lock and its file is neither deleted nor modified.
func TestStopStaleLockBytesUnchanged(t *testing.T) {
	_, state := stopEnv(t)
	pid := deadPid(t)
	p := writeLock(t, state, "remind", liveLockJSON(pid))
	before, err := os.ReadFile(p)
	if err != nil {
		t.Fatal(err)
	}

	stdout, stderr, code := runStop(t)
	if code != 0 {
		t.Fatalf("exit = %d, want 0 (stdout=%q stderr=%q)", code, stdout, stderr)
	}
	want := fmt.Sprintf("stale lock %s (pid %d is dead)\n", p, pid)
	if stdout != want {
		t.Errorf("stdout = %q, want %q", stdout, want)
	}
	after, err := os.ReadFile(p)
	if err != nil {
		t.Fatalf("stale lock was removed: %v", err)
	}
	if !bytes.Equal(before, after) {
		t.Errorf("stale lock bytes changed: %q -> %q", before, after)
	}
}

// TestStopForeignPidNotSignaled pins that a recycled pid owned by another
// program is reported, not signaled, and left running.
func TestStopForeignPidNotSignaled(t *testing.T) {
	_, state := stopEnv(t)
	sleep := startChild(t, "sleep", "300")
	writeLock(t, state, "remind", liveLockJSON(sleep.pid()))

	stdout, stderr, code := runStop(t)
	if code != 0 {
		t.Fatalf("exit = %d, want 0 (stdout=%q stderr=%q)", code, stdout, stderr)
	}
	if !strings.Contains(stdout, "is not omosense") {
		t.Errorf("stdout = %q, want a not-omosense report", stdout)
	}
	if syscall.Kill(sleep.pid(), 0) != nil {
		t.Errorf("foreign pid %d was signaled", sleep.pid())
	}
}

// TestStopMixedOutcomesOneLine pins that a stale report, a foreign report
// and a stopped host all merge into exactly one stdout line.
func TestStopMixedOutcomesOneLine(t *testing.T) {
	_, state := stopEnv(t)
	stale1 := writeLock(t, state, "a-stale", liveLockJSON(deadPid(t)))
	stale2 := writeLock(t, state, "b-stale", liveLockJSON(deadPid(t)))
	sleep := startChild(t, "sleep", "300")
	foreign := writeLock(t, state, "c-foreign", liveLockJSON(sleep.pid()))
	bin := copyHelper(t, "omosense-mixed")
	hostLock := filepath.Join(state, "watch-herdr.lock.json")
	host := startHelper(t, bin, "-lock", hostLock)
	writeLock(t, state, "watch-herdr", liveLockJSON(host.pid()))

	stdout, stderr, code := runStop(t)
	if code != 0 {
		t.Fatalf("exit = %d, want 0 (stdout=%q stderr=%q)", code, stdout, stderr)
	}
	assertOneLine(t, stdout)
	if !strings.Contains(stdout, fmt.Sprintf("stopped omosense pid %d", host.pid())) {
		t.Errorf("stdout = %q, want the stopped pid %d", stdout, host.pid())
	}
	for _, p := range []string{stale1, stale2} {
		if !strings.Contains(stdout, "stale lock "+p) {
			t.Errorf("stdout = %q, want the stale report for %s", stdout, p)
		}
	}
	if !strings.Contains(stdout, "lock "+foreign+": pid") || !strings.Contains(stdout, "is not omosense") {
		t.Errorf("stdout = %q, want the foreign report for %s", stdout, foreign)
	}
	<-host.done
	if syscall.Kill(sleep.pid(), 0) != nil {
		t.Errorf("foreign pid %d was signaled", sleep.pid())
	}
}

// TestStopStaleAndForeignOneLine pins the same one-line contract for the path
// with no host to signal: several reports still collapse to one line.
func TestStopStaleAndForeignOneLine(t *testing.T) {
	_, state := stopEnv(t)
	stale1 := writeLock(t, state, "a-stale", liveLockJSON(deadPid(t)))
	stale2 := writeLock(t, state, "b-stale", liveLockJSON(deadPid(t)))
	sleep := startChild(t, "sleep", "300")
	foreign := writeLock(t, state, "c-foreign", liveLockJSON(sleep.pid()))

	stdout, stderr, code := runStop(t)
	if code != 0 {
		t.Fatalf("exit = %d, want 0 (stdout=%q stderr=%q)", code, stdout, stderr)
	}
	assertOneLine(t, stdout)
	for _, p := range []string{stale1, stale2} {
		if !strings.Contains(stdout, "stale lock "+p) {
			t.Errorf("stdout = %q, want the stale report for %s", stdout, p)
		}
	}
	if !strings.Contains(stdout, "lock "+foreign+": pid") {
		t.Errorf("stdout = %q, want the foreign report for %s", stdout, foreign)
	}
}

// TestStopLaterArgOnlyOmosenseNotMatched pins that only argv[0] is matched:
// a process whose later argument contains "omosense" is not signaled.
func TestStopLaterArgOnlyOmosenseNotMatched(t *testing.T) {
	_, state := stopEnv(t)
	bin := copyHelper(t, "plainhelper")
	lockPath := filepath.Join(state, "watch-herdr.lock.json")
	h := startHelper(t, bin, "-lock", lockPath)
	writeLock(t, state, "watch-herdr", liveLockJSON(h.pid()))

	stdout, stderr, code := runStop(t)
	if code != 0 {
		t.Fatalf("exit = %d, want 0 (stdout=%q stderr=%q)", code, stdout, stderr)
	}
	if !strings.Contains(stdout, "is not omosense") {
		t.Errorf("stdout = %q, want a not-omosense report", stdout)
	}
	if syscall.Kill(h.pid(), 0) != nil {
		t.Errorf("pid %d was signaled though only its argument named omosense", h.pid())
	}
}

// TestStopRenamedBinaryMatched pins the production name shape: a binary
// named omosense-<os>-<arch> is matched and stopped.
func TestStopRenamedBinaryMatched(t *testing.T) {
	_, state := stopEnv(t)
	bin := copyHelper(t, "omosense-darwin-arm64")
	lockPath := filepath.Join(state, "watch-herdr.lock.json")
	h := startHelper(t, bin, "-lock", lockPath)
	writeLock(t, state, "watch-herdr", liveLockJSON(h.pid()))

	stdout, stderr, code := runStop(t)
	if code != 0 {
		t.Fatalf("exit = %d, want 0 (stdout=%q stderr=%q)", code, stdout, stderr)
	}
	want := fmt.Sprintf("stopped omosense pid %d\n", h.pid())
	if stdout != want {
		t.Errorf("stdout = %q, want %q", stdout, want)
	}
	<-h.done
	if _, err := os.Stat(lockPath); !os.IsNotExist(err) {
		t.Errorf("lock not released: %v", err)
	}
}

// TestStopMalformedLocks pins that every malformed pid shape is rejected
// with exit 1 naming the file and no signal to a live canary.
func TestStopMalformedLocks(t *testing.T) {
	_, state := stopEnv(t)
	bin := copyHelper(t, "omosense-canary")
	canary := startHelper(t, bin, "-lock", filepath.Join(state, "watch-herdr.lock.json"))
	writeLock(t, state, "watch-herdr", liveLockJSON(canary.pid()))

	cases := []struct{ name, content string }{
		{"negative", `{"pid":-1}`},
		{"zero", `{"pid":0}`},
		{"one", `{"pid":1}`},
		{"nonint", `{"pid":"abc"}`},
		{"empty", ``},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			p := writeLock(t, state, "remind", tc.content)
			defer os.Remove(p)
			stdout, stderr, code := runStop(t)
			if code != 1 {
				t.Fatalf("exit = %d, want 1 (stdout=%q stderr=%q)", code, stdout, stderr)
			}
			if !strings.Contains(stderr, filepath.Base(p)) {
				t.Errorf("stderr = %q, want the offending file name", stderr)
			}
			if syscall.Kill(canary.pid(), 0) != nil {
				t.Fatalf("canary %d was signaled", canary.pid())
			}
		})
	}
}

// TestStopArgvUnreadable pins that an unreadable argv aborts with exit 1
// naming the pid and signals nothing.
func TestStopArgvUnreadable(t *testing.T) {
	_, state := stopEnv(t)
	sleep := startChild(t, "sleep", "300")
	writeLock(t, state, "remind", liveLockJSON(sleep.pid()))

	orig := procArgv0
	procArgv0 = func(int) (string, error) { return "", errors.New("ps: boom") }
	defer func() { procArgv0 = orig }()

	stdout, stderr, code := runStop(t)
	if code != 1 {
		t.Fatalf("exit = %d, want 1 (stdout=%q stderr=%q)", code, stdout, stderr)
	}
	if !strings.Contains(stderr, strconv.Itoa(sleep.pid())) {
		t.Errorf("stderr = %q, want the pid", stderr)
	}
	if syscall.Kill(sleep.pid(), 0) != nil {
		t.Errorf("pid %d was signaled", sleep.pid())
	}
}

// TestStopTwoHosts pins that two distinct omosense pids are both stopped and
// reported on one line.
func TestStopTwoHosts(t *testing.T) {
	dir, _ := stopEnv(t)
	writeConfig(t, dir, `{"herdr":{"enabled":false},"tidy":{"enabled":false}}`)
	buildBinaries(t)
	remind := startChildPiped(t, mainBin, "remind")
	remind.awaitLine(t, "LOG reminder scheduler starting")
	herdr := startChildPiped(t, mainBin, "herdr")
	herdr.awaitLine(t, "LOG herdr watcher starting")

	stdout, stderr, code := runStop(t)
	if code != 0 {
		t.Fatalf("exit = %d, want 0 (stdout=%q stderr=%q)", code, stdout, stderr)
	}
	pids := []int{remind.pid(), herdr.pid()}
	sort.Ints(pids)
	want := fmt.Sprintf("stopped omosense pids %d, %d\n", pids[0], pids[1])
	if stdout != want {
		t.Errorf("stdout = %q, want %q", stdout, want)
	}
	<-remind.done
	<-herdr.done
}

// TestStopTimeoutNamesRetainedLock pins that a host which exits but leaves
// its lock behind is still named, with the lock it left, on exit 1.
func TestStopTimeoutNamesRetainedLock(t *testing.T) {
	_, state := stopEnv(t)
	bin := copyHelper(t, "omosense-retain")
	h := startHelper(t, bin) // no -lock: the lock survives the helper's exit
	lockPath := writeLock(t, state, "remind", liveLockJSON(h.pid()))

	origTimeout, origPoll := waitTimeout, pollEvery
	waitTimeout, pollEvery = 300*time.Millisecond, 20*time.Millisecond
	defer func() { waitTimeout, pollEvery = origTimeout, origPoll }()

	stdout, stderr, code := runStop(t)
	if code != 1 {
		t.Fatalf("exit = %d, want 1 (stdout=%q stderr=%q)", code, stdout, stderr)
	}
	<-h.done // the process exited, so only the lock keeps stop waiting
	if syscall.Kill(h.pid(), 0) == nil {
		t.Fatalf("helper %d should have exited", h.pid())
	}
	if _, err := os.Stat(lockPath); err != nil {
		t.Fatalf("lock should remain: %v", err)
	}
	if !strings.Contains(stdout, "could not stop omosense pid "+strconv.Itoa(h.pid())) {
		t.Errorf("stdout = %q, want the timeout line to name pid %d", stdout, h.pid())
	}
	if !strings.Contains(stdout, lockPath) {
		t.Errorf("stdout = %q, want the retained lock %s", stdout, lockPath)
	}
	assertOneLine(t, stdout)
}

// TestStopUnstoppableHost pins that a host which ignores SIGTERM is reported
// with exit 1 after the (injected) timeout.
func TestStopUnstoppableHost(t *testing.T) {
	_, state := stopEnv(t)
	bin := copyHelper(t, "omosense-test")
	lockPath := filepath.Join(state, "watch-herdr.lock.json")
	h := startHelper(t, bin, "-ignore-sigterm", "-lock", lockPath)
	writeLock(t, state, "watch-herdr", liveLockJSON(h.pid()))

	origTimeout, origPoll := waitTimeout, pollEvery
	waitTimeout, pollEvery = 300*time.Millisecond, 20*time.Millisecond
	defer func() { waitTimeout, pollEvery = origTimeout, origPoll }()

	stdout, stderr, code := runStop(t)
	if code != 1 {
		t.Fatalf("exit = %d, want 1 (stdout=%q stderr=%q)", code, stdout, stderr)
	}
	if !strings.Contains(stdout, strconv.Itoa(h.pid())) {
		t.Errorf("stdout = %q, want the surviving pid", stdout)
	}
	if syscall.Kill(h.pid(), 0) != nil {
		t.Fatalf("helper %d should still be alive", h.pid())
	}
}

// TestStopCLINotRunning drives the built binary's `stop` dispatch in an
// empty folder: not running, exit 0, nothing created.
func TestStopCLINotRunning(t *testing.T) {
	buildBinaries(t)
	home := t.TempDir()
	dir := filepath.Join(home, ".omosense")
	stdout, stderr, code := runCLI(t, []string{"stop"}, "HOME="+home, "OMOSENSE_DIR="+dir, "OMOSENSE_STATE="+filepath.Join(dir, "state"))
	if code != 0 || stdout != "not running\n" {
		t.Fatalf("exit = %d stdout = %q stderr = %q, want 0/not running", code, stdout, stderr)
	}
	if entries, _ := os.ReadDir(home); len(entries) != 0 {
		t.Errorf("stop created %v, want nothing", entries)
	}
}

// TestStopCLIHelp pins `omosense stop --help`.
func TestStopCLIHelp(t *testing.T) {
	buildBinaries(t)
	stdout, _, code := runCLI(t, []string{"stop", "--help"})
	if code != 0 || !strings.Contains(stdout, "Usage: omosense stop") {
		t.Fatalf("exit = %d stdout = %q, want the stop help", code, stdout)
	}
}

// TestStopCLIUsageError pins that an unexpected argument exits 2.
func TestStopCLIUsageError(t *testing.T) {
	buildBinaries(t)
	_, _, code := runCLI(t, []string{"stop", "extra"})
	if code != 2 {
		t.Fatalf("exit = %d, want 2", code)
	}
}

func runCLI(t *testing.T, args []string, env ...string) (stdout, stderr string, code int) {
	t.Helper()
	cmd := exec.Command(mainBin, args...)
	cmd.Env = append(os.Environ(), env...)
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

// locksIn lists the *.lock.json entries of dir by reading the literal
// directory: the test helper never globs, so a folder whose NAME contains
// glob metacharacters cannot silently redirect the check to a sibling.
func locksIn(t *testing.T, dir string) []string {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil
		}
		t.Fatal(err)
	}
	var out []string
	for _, e := range entries {
		if strings.HasSuffix(e.Name(), ".lock.json") {
			out = append(out, e.Name())
		}
	}
	return out
}

// stopEnvAt points HOME, OMOSENSE_DIR and OMOSENSE_STATE at a folder the
// caller names, so a test can put glob metacharacters in it.
func stopEnvAt(t *testing.T, home, folder string) (dir, state string) {
	t.Helper()
	dir = filepath.Join(home, folder, ".omosense")
	state = filepath.Join(dir, "state")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("HOME", home)
	t.Setenv("OMOSENSE_DIR", dir)
	t.Setenv("OMOSENSE_STATE", state)
	return dir, state
}

// stateDirIn creates a folder beside the tested one, without touching the
// environment, so a decoy host can own its own state dir and lock.
func stateDirIn(t *testing.T, home, folder string) string {
	t.Helper()
	state := filepath.Join(home, folder, ".omosense", "state")
	if err := os.MkdirAll(state, 0o755); err != nil {
		t.Fatal(err)
	}
	return state
}

// TestStopBracketFolderStopsOnlyItself pins R1: the state directory is
// enumerated literally, so stopping from "project[1]" signals only that
// folder's host. A glob treats "[1]" as a character class and would signal
// the sibling "project1" instead, leaving the intended host running.
func TestStopBracketFolderStopsOnlyItself(t *testing.T) {
	home := t.TempDir()
	_, state := stopEnvAt(t, home, "project[1]")
	siblingState := stateDirIn(t, home, "project1")
	bin := copyHelper(t, "omosense-test")
	target := startHelper(t, bin, "-lock", filepath.Join(state, "watch-herdr.lock.json"))
	sibling := startHelper(t, bin, "-lock", filepath.Join(siblingState, "watch-herdr.lock.json"))
	writeLock(t, state, "watch-herdr", liveLockJSON(target.pid()))
	siblingLock := writeLock(t, siblingState, "watch-herdr", liveLockJSON(sibling.pid()))
	before, err := os.ReadFile(siblingLock)
	if err != nil {
		t.Fatal(err)
	}

	stdout, stderr, code := runStop(t)
	if code != 0 {
		t.Fatalf("exit = %d, want 0 (stdout=%q stderr=%q)", code, stdout, stderr)
	}
	want := fmt.Sprintf("stopped omosense pid %d\n", target.pid())
	if stdout != want {
		t.Fatalf("stdout = %q, want %q; the sibling host (pid %d, state %s) must not be touched",
			stdout, want, sibling.pid(), siblingState)
	}
	<-target.done
	if syscall.Kill(sibling.pid(), 0) != nil {
		t.Errorf("sibling pid %d was signaled", sibling.pid())
	}
	after, err := os.ReadFile(siblingLock)
	if err != nil {
		t.Fatalf("sibling lock was removed: %v", err)
	}
	if !bytes.Equal(before, after) {
		t.Errorf("sibling lock bytes changed: %q -> %q", before, after)
	}
}

// TestStopBracketFolderAloneStops pins that a metacharacter folder with no
// matching sibling is still stopped: a glob finds nothing there and reports
// "not running" while the host keeps running.
func TestStopBracketFolderAloneStops(t *testing.T) {
	home := t.TempDir()
	_, state := stopEnvAt(t, home, "project[1]")
	bin := copyHelper(t, "omosense-test")
	host := startHelper(t, bin, "-lock", filepath.Join(state, "watch-herdr.lock.json"))
	writeLock(t, state, "watch-herdr", liveLockJSON(host.pid()))

	stdout, stderr, code := runStop(t)
	if code != 0 {
		t.Fatalf("exit = %d, want 0 (stdout=%q stderr=%q)", code, stdout, stderr)
	}
	want := fmt.Sprintf("stopped omosense pid %d\n", host.pid())
	if stdout != want {
		t.Fatalf("stdout = %q, want %q", stdout, want)
	}
	<-host.done
}

// TestStopStarQuestFolderStopsOnlyItself pins the same literal-directory rule
// for a state dir named with "*" and "?" beside a sibling ("projectax") the
// pattern would match: only the intended host may be signaled.
func TestStopStarQuestFolderStopsOnlyItself(t *testing.T) {
	home := t.TempDir()
	_, state := stopEnvAt(t, home, "pro*ject?x")
	decoyState := stateDirIn(t, home, "projectax")
	bin := copyHelper(t, "omosense-test")
	target := startHelper(t, bin, "-lock", filepath.Join(state, "watch-herdr.lock.json"))
	decoy := startHelper(t, bin, "-lock", filepath.Join(decoyState, "watch-herdr.lock.json"))
	writeLock(t, state, "watch-herdr", liveLockJSON(target.pid()))
	decoyLock := writeLock(t, decoyState, "watch-herdr", liveLockJSON(decoy.pid()))
	before, err := os.ReadFile(decoyLock)
	if err != nil {
		t.Fatal(err)
	}

	stdout, stderr, code := runStop(t)
	if code != 0 {
		t.Fatalf("exit = %d, want 0 (stdout=%q stderr=%q)", code, stdout, stderr)
	}
	want := fmt.Sprintf("stopped omosense pid %d\n", target.pid())
	if stdout != want {
		t.Fatalf("stdout = %q, want %q; the decoy host (pid %d, state %s) must not be touched",
			stdout, want, decoy.pid(), decoyState)
	}
	<-target.done
	if syscall.Kill(decoy.pid(), 0) != nil {
		t.Errorf("decoy pid %d was signaled", decoy.pid())
	}
	after, err := os.ReadFile(decoyLock)
	if err != nil {
		t.Fatalf("decoy lock was removed: %v", err)
	}
	if !bytes.Equal(before, after) {
		t.Errorf("decoy lock bytes changed: %q -> %q", before, after)
	}
}

// TestStopStateDirUnreadableFails pins that a state path that cannot be read
// (here its parent is a regular file, so the open fails with ENOTDIR) is
// reported on stderr with exit 1 instead of a silent "not running".
func TestStopStateDirUnreadableFails(t *testing.T) {
	home := t.TempDir()
	file := filepath.Join(home, "not-a-dir")
	if err := os.WriteFile(file, []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	t.Setenv("HOME", home)
	t.Setenv("OMOSENSE_DIR", filepath.Join(home, ".omosense"))
	t.Setenv("OMOSENSE_STATE", filepath.Join(file, "state"))

	stdout, stderr, code := runStop(t)
	if code != 1 {
		t.Fatalf("exit = %d, want 1 (stdout=%q stderr=%q)", code, stdout, stderr)
	}
	if !strings.Contains(stderr, "omosense: stop:") {
		t.Errorf("stderr = %q, want a stop error", stderr)
	}
	if stdout != "" {
		t.Errorf("stdout = %q, want no result line", stdout)
	}
}
