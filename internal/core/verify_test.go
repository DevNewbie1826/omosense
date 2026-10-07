package core

import (
	"context"
	"os"
	"path/filepath"
	"strings"
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
