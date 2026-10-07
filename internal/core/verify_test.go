package core

import (
	"bufio"
	"context"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"
)

// writeHook writes a /bin/sh hook body and returns the command argv that
// runs it plus the attempts file path the hook appends one line to per
// run (delivered through env, which also proves the env plumbing).
func writeHook(t *testing.T, body string) (command []string, attempts string) {
	t.Helper()
	dir := t.TempDir()
	p := filepath.Join(dir, "hook.sh")
	if err := os.WriteFile(p, []byte(body), 0o755); err != nil {
		t.Fatal(err)
	}
	return []string{"/bin/sh", p}, filepath.Join(dir, "attempts")
}

func hookEnv(attempts string) []string { return []string{"ATTEMPTS=" + attempts} }

func attemptLines(t *testing.T, attempts string) int {
	t.Helper()
	b, err := os.ReadFile(attempts)
	if err != nil {
		return 0
	}
	return strings.Count(string(b), "\n")
}

// awaitAttempts blocks (bounded) until the hook has appended its n-th
// line, so a test triggers its action on the observable attempt instead
// of on a timer.
func awaitAttempts(t *testing.T, attempts string, n int) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if attemptLines(t, attempts) >= n {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("hook did not reach %d attempts within 5s", n)
}

func TestRunVerifyPassFirstAttempt(t *testing.T) {
	dir := t.TempDir()
	command, attempts := writeHook(t, `echo "$(pwd)" >> "$ATTEMPTS"; exit 0`)
	res := RunVerify(context.Background(), command, 10*time.Second, hookEnv(attempts), dir)
	if res.Status != "verified" || res.Detail != "" {
		t.Fatalf("res = %+v, want verified with no detail", res)
	}
	if n := attemptLines(t, attempts); n != 1 {
		t.Errorf("hook ran %d attempts, want 1", n)
	}
	b, err := os.ReadFile(attempts)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(b), filepath.Base(dir)) {
		t.Errorf("hook cwd = %q, want the passed dir (a missing dir would fall back to the inherited cwd)", string(b))
	}
}

func TestRunVerifyRetriesOnceThenPasses(t *testing.T) {
	command, attempts := writeHook(t, `
n=$(awk 'END{print NR}' "$ATTEMPTS")
echo x >> "$ATTEMPTS"
if [ "$n" -ge 1 ]; then exit 0; fi
exit 1
`)
	res := RunVerify(context.Background(), command, 10*time.Second, hookEnv(attempts), "")
	if res.Status != "verified" || res.Detail != "" {
		t.Fatalf("res = %+v, want verified after the retry", res)
	}
	if n := attemptLines(t, attempts); n != 2 {
		t.Errorf("hook ran %d attempts, want 2", n)
	}
}

func TestRunVerifyFailsAfterExactlyTwoAttempts(t *testing.T) {
	command, attempts := writeHook(t, `echo x >> "$ATTEMPTS"; echo boom >&2; exit 3`)
	res := RunVerify(context.Background(), command, 10*time.Second, hookEnv(attempts), "")
	if res.Status != "unverified" || res.Detail != "exit 3: boom" {
		t.Fatalf("res = %+v, want unverified with detail %q", res, "exit 3: boom")
	}
	if n := attemptLines(t, attempts); n != 2 {
		t.Errorf("hook ran %d attempts, want exactly 2", n)
	}
}

func TestRunVerifyEmptyStderrDropsDetailColon(t *testing.T) {
	command, attempts := writeHook(t, `echo x >> "$ATTEMPTS"; exit 7`)
	res := RunVerify(context.Background(), command, 10*time.Second, hookEnv(attempts), "")
	if res.Status != "unverified" || res.Detail != "exit 7" {
		t.Fatalf("res = %+v, want unverified with detail %q", res, "exit 7")
	}
}

func TestRunVerifyStartError(t *testing.T) {
	missing := filepath.Join(t.TempDir(), "missing-hook")
	res := RunVerify(context.Background(), []string{missing}, 10*time.Second, nil, "")
	if res.Status != "unverified" || !strings.HasPrefix(res.Detail, "start error: ") {
		t.Fatalf("res = %+v, want unverified with a start error detail", res)
	}
}

// TestRunVerifyCancelledWhileHookBlocks pins the shutdown rule: a
// cancelled ctx returns cancelled (no retry, empty detail) promptly while
// the hook is still running, leaving exactly one attempt.
func TestRunVerifyCancelledWhileHookBlocks(t *testing.T) {
	command, attempts := writeHook(t, `echo x >> "$ATTEMPTS"; sleep 30`)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan VerifyResult, 1)
	go func() { done <- RunVerify(ctx, command, time.Minute, hookEnv(attempts), "") }()
	awaitAttempts(t, attempts, 1)
	cancel()
	var res VerifyResult
	select {
	case res = <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("RunVerify did not return within 3s of the cancel")
	}
	if res.Status != "cancelled" || res.Detail != "" {
		t.Fatalf("res = %+v, want cancelled with no detail", res)
	}
	if n := attemptLines(t, attempts); n != 1 {
		t.Errorf("hook ran %d attempts after a cancel, want 1 (no retry)", n)
	}
}

// childProcessGone confirms the exit already signalled by FIFO EOF.
// A dead orphan awaiting reaping is not a surviving child, even though
// kill(pid, 0) still succeeds. The checks do not poll for reaping.
func childProcessGone(pid int) bool {
	if pid <= 0 {
		return true
	}
	p, err := os.FindProcess(pid)
	if err != nil {
		return true
	}
	if p.Signal(syscall.Signal(0)) != nil {
		return true
	}
	state, err := exec.Command("ps", "-p", strconv.Itoa(pid), "-o", "stat=").Output()
	return (err == nil && strings.HasPrefix(strings.TrimSpace(string(state)), "Z")) ||
		p.Signal(syscall.Signal(0)) != nil
}

// mkfifo creates the named pipe the hook's child signals through.
func mkfifo(t *testing.T, path string) {
	t.Helper()
	if err := syscall.Mkfifo(path, 0o600); err != nil {
		t.Fatal(err)
	}
}

// subscribeFifo subscribes to a FIFO BEFORE the source starts. The
// goroutine blocks in open() until the child opens the FIFO for writing,
// reports the pid line the child writes (readiness), then blocks until the
// last writer closes and reports that (io.EOF) - which happens exactly when
// the child dies. Both waits are OS events on the pipe; neither polls.
func subscribeFifo(t *testing.T, path string) (<-chan string, <-chan error) {
	t.Helper()
	pids := make(chan string, 1)
	gones := make(chan error, 1)
	go func() {
		f, err := os.OpenFile(path, os.O_RDONLY, 0)
		if err != nil {
			pids <- ""
			gones <- err
			return
		}
		defer f.Close()
		br := bufio.NewReader(f)
		line, err := br.ReadString('\n')
		if err != nil {
			pids <- ""
			gones <- err
			return
		}
		pids <- strings.TrimSpace(line)
		_, err = br.ReadByte()
		gones <- err
	}()
	return pids, gones
}

// awaitFifoPid waits (bounded) for the child's readiness line.
func awaitFifoPid(t *testing.T, pids <-chan string, bound time.Duration) int {
	t.Helper()
	select {
	case line := <-pids:
		n, err := strconv.Atoi(line)
		if err != nil || n <= 0 {
			t.Fatalf("child pid line %q: %v", line, err)
		}
		return n
	case <-time.After(bound):
		t.Fatalf("the child never announced readiness through the FIFO within %s", bound)
		return 0
	}
}

// awaitFifoGone waits (bounded) for the FIFO's last writer to close. The
// child holds the FIFO open for writing, so a surviving child keeps this
// read blocked and the bound is what fails.
func awaitFifoGone(t *testing.T, gones <-chan error, bound time.Duration) {
	t.Helper()
	select {
	case err := <-gones:
		if !errors.Is(err, io.EOF) {
			t.Fatalf("FIFO read ended with %v, want EOF once the child died", err)
		}
	case <-time.After(bound):
		t.Fatalf("the child still holds the FIFO %s after the cancel: it survived the kill", bound)
	}
}

// TestRunVerifyCancelKillsBackgroundChild pins the hook's process-group
// contract (SourceCommand/RunSource): the hook starts an ordinary child
// WITHOUT exec, so that child stays in the hook's process group and
// inherits its stderr pipe. The child opens a FIFO for writing (the test
// subscribes to it before the hook starts), announces its pid, then blocks
// holding both the FIFO and the inherited pipe. Cancelling the ctx must
// return cancelled within the bound AND close the FIFO's last writer by
// killing that child - a bare exec.CommandContext kills only the hook, so
// the child survives and the FIFO read never ends.
func TestRunVerifyCancelKillsBackgroundChild(t *testing.T) {
	dir := t.TempDir()
	childPath := filepath.Join(dir, "child.sh")
	fifoPath := filepath.Join(dir, "child.fifo")
	child := "#!/bin/sh\nexec 9>\"$CHILD_FIFO\"\nprintf '%s\\n' \"$$\" >&9\nwhile :; do :; done\n"
	if err := os.WriteFile(childPath, []byte(child), 0o755); err != nil {
		t.Fatal(err)
	}
	command, _ := writeHook(t, "\"$CHILD\" &\nwait\n")
	mkfifo(t, fifoPath)
	pids, gones := subscribeFifo(t, fifoPath)
	env := []string{"CHILD=" + childPath, "CHILD_FIFO=" + fifoPath}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan VerifyResult, 1)
	go func() { done <- RunVerify(ctx, command, time.Minute, env, "") }()
	pid := awaitFifoPid(t, pids, 5*time.Second)
	t.Cleanup(func() {
		if !childProcessGone(pid) {
			if p, err := os.FindProcess(pid); err == nil {
				_ = p.Signal(syscall.SIGKILL)
			}
		}
	})
	t.Logf("hook child pid %d is holding the pipes; cancelling the context", pid)
	cancel()
	var res VerifyResult
	select {
	case res = <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("RunVerify did not return within 3s of the cancel while a child held its pipes")
	}
	if res.Status != "cancelled" || res.Detail != "" {
		t.Fatalf("res = %+v, want cancelled with no detail", res)
	}
	awaitFifoGone(t, gones, 3*time.Second)
	if !childProcessGone(pid) {
		t.Fatalf("hook child pid %d still alive after the cancel: RunVerify did not kill the hook's process group", pid)
	}
}

// TestRunVerifyTimeout pins the per-attempt timeout: a hook that outlives
// its timeout is unverified with a timeout detail and retries once.
func TestRunVerifyTimeout(t *testing.T) {
	command, attempts := writeHook(t, `echo x >> "$ATTEMPTS"; echo waiting >&2; sleep 30`)
	res := RunVerify(context.Background(), command, 150*time.Millisecond, hookEnv(attempts), "")
	if res.Status != "unverified" || !strings.HasPrefix(res.Detail, "timeout") {
		t.Fatalf("res = %+v, want unverified with a timeout detail", res)
	}
	if !strings.HasPrefix(res.Detail, "timeout: waiting") {
		t.Errorf("detail = %q, want the trimmed stderr after the timeout kind", res.Detail)
	}
	if n := attemptLines(t, attempts); n != 2 {
		t.Errorf("hook ran %d attempts, want 2 (a timeout is a failure and retries once)", n)
	}
}

// TestRunVerifyMissingDirInheritsCwd pins the dir rule: a dir that is not
// an existing directory is ignored and the inherited cwd is used.
func TestRunVerifyMissingDirInheritsCwd(t *testing.T) {
	command, attempts := writeHook(t, `echo "$(pwd)" >> "$ATTEMPTS"; exit 0`)
	wd, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	res := RunVerify(context.Background(), command, 10*time.Second, hookEnv(attempts), filepath.Join(t.TempDir(), "not-created"))
	if res.Status != "verified" {
		t.Fatalf("res = %+v, want verified", res)
	}
	b, err := os.ReadFile(attempts)
	if err != nil {
		t.Fatal(err)
	}
	if got := strings.TrimSpace(string(b)); got != wd {
		t.Errorf("hook cwd = %q, want the inherited %q", got, wd)
	}
}
