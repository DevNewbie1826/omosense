package core

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"regexp"
	"strconv"
	"testing"
)

func liveSleeper(t *testing.T) int {
	t.Helper()
	cmd := exec.Command("sleep", "300")
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = cmd.Process.Kill()
		_, _ = cmd.Process.Wait()
	})
	return cmd.Process.Pid
}

func deadPid(t *testing.T) int {
	t.Helper()
	cmd := exec.Command("sleep", "300")
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	pid := cmd.Process.Pid
	if err := cmd.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	// Reap so syscall.Kill(pid, 0) reports the pid as dead.
	_ = cmd.Wait()
	return pid
}

func writeLock(t *testing.T, state, name, content string) {
	t.Helper()
	if err := os.MkdirAll(state, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(state, name+".lock.json"), []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
}

func testCtx(state string) *Ctx {
	return &Ctx{State: state, Out: NewOut(os.Stdout)}
}

func heldJSON(pid int) string {
	return `{"pid":` + strconv.Itoa(pid) + `,"session":null,"pane":null,"cwd":"/tmp","started":"2026-10-03T00:00:00.000Z"}`
}

func pidOf(t *testing.T, m *OMap) string {
	t.Helper()
	v, ok := m.Get("pid")
	if !ok {
		t.Fatal("lock has no pid key")
	}
	n, ok := v.(json.Number)
	if !ok {
		t.Fatalf("pid is %T, want json.Number", v)
	}
	return n.String()
}

func TestTryAcquireBlocksOnLivePrimaryPid(t *testing.T) {
	state := t.TempDir()
	pid := liveSleeper(t)
	writeLock(t, state, "listen", heldJSON(pid))

	release, blockedBy, held, err := testCtx(state).TryAcquire("listen", "")
	if err != nil {
		t.Fatalf("tryacquire: %v", err)
	}
	if release != nil {
		t.Errorf("release must be nil when blocked")
	}
	if blockedBy != "listen" {
		t.Errorf("blockedBy = %q, want listen", blockedBy)
	}
	if held == nil {
		t.Fatal("held lock metadata missing")
	}
	if got := pidOf(t, held); got != strconv.Itoa(pid) {
		t.Errorf("held pid = %s, want %d", got, pid)
	}
}

func TestTryAcquireBlocksOnLiveLegacyPid(t *testing.T) {
	// A live legacy lock blocks and reports the legacy name (lock.ts:14).
	// No shipped caller passes a legacy name any more, so the pair here is
	// artificial.
	state := t.TempDir()
	pid := liveSleeper(t)
	writeLock(t, state, "listen-legacy", heldJSON(pid))

	release, blockedBy, held, err := testCtx(state).TryAcquire("listen", "listen-legacy")
	if err != nil {
		t.Fatalf("tryacquire: %v", err)
	}
	if release != nil || blockedBy != "listen-legacy" || held == nil {
		t.Errorf("got release?%v blockedBy=%q held?%v, want none/listen-legacy/non-nil", release != nil, blockedBy, held != nil)
	}
}

func TestTryAcquireDeadPidOverwritten(t *testing.T) {
	state := t.TempDir()
	writeLock(t, state, "listen", heldJSON(deadPid(t)))

	release, blockedBy, _, err := testCtx(state).TryAcquire("listen", "")
	if err != nil {
		t.Fatalf("tryacquire: %v", err)
	}
	if blockedBy != "" || release == nil {
		t.Fatalf("dead holder must be overwritten: blockedBy=%q release?%v", blockedBy, release != nil)
	}

	b, err := os.ReadFile(filepath.Join(state, "listen.lock.json"))
	if err != nil {
		t.Fatal(err)
	}
	m, err := ParseJSON(b)
	if err != nil {
		t.Fatal(err)
	}
	if got := pidOf(t, m.(*OMap)); got != strconv.Itoa(os.Getpid()) {
		t.Errorf("holder = %s, want our pid %d", got, os.Getpid())
	}

	release()
	if _, err := os.Stat(filepath.Join(state, "listen.lock.json")); !os.IsNotExist(err) {
		t.Errorf("release must remove our lock: %v", err)
	}
}

func TestTryAcquireWritesOwnerLock(t *testing.T) {
	state := t.TempDir()
	unsetLockEnv(t)

	release, _, _, err := testCtx(state).TryAcquire("remind", "")
	if err != nil {
		t.Fatalf("tryacquire: %v", err)
	}
	defer release()

	b, err := os.ReadFile(filepath.Join(state, "remind.lock.json"))
	if err != nil {
		t.Fatalf("lock file: %v", err)
	}
	if regexp.MustCompile(`\s`).MatchString(string(b)) {
		t.Errorf("lock file must be compact JSON, got %q", b)
	}
	m, err := ParseJSON(b)
	if err != nil {
		t.Fatalf("parse lock: %v", err)
	}
	om := m.(*OMap)
	if got := om.Keys(); !reflect.DeepEqual(got, []string{"pid", "session", "pane", "cwd", "started"}) {
		t.Errorf("key order = %v", got)
	}
	if got := pidOf(t, om); got != strconv.Itoa(os.Getpid()) {
		t.Errorf("pid = %s, want %d", got, os.Getpid())
	}
	if s, ok := om.Get("session"); !ok || s != nil {
		t.Errorf("session = %v ok=%v, want null (env unset)", s, ok)
	}
	started, _ := om.Get("started")
	s, _ := started.(string)
	if !regexp.MustCompile(`^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$`).MatchString(s) {
		t.Errorf("started = %q, want JS toISOString format", s)
	}
}

func TestTryAcquireSessionEnv(t *testing.T) {
	state := t.TempDir()
	t.Setenv("PI_SESSION_ID", "s1")
	t.Setenv("HERDR_PANE_ID", "p1")

	release, _, _, err := testCtx(state).TryAcquire("memory-tidy", "")
	if err != nil {
		t.Fatalf("tryacquire: %v", err)
	}
	defer release()

	b, err := os.ReadFile(filepath.Join(state, "memory-tidy.lock.json"))
	if err != nil {
		t.Fatal(err)
	}
	m, _ := ParseJSON(b)
	om := m.(*OMap)
	if s, _ := om.Get("session"); s != "s1" {
		t.Errorf("session = %v, want s1", s)
	}
	if p, _ := om.Get("pane"); p != "p1" {
		t.Errorf("pane = %v, want p1", p)
	}
}

func TestReleaseOnlyWhenPidMatches(t *testing.T) {
	state := t.TempDir()
	pid := liveSleeper(t)

	release, _, _, err := testCtx(state).TryAcquire("watch-herdr", "")
	if err != nil {
		t.Fatalf("tryacquire: %v", err)
	}
	writeLock(t, state, "watch-herdr", heldJSON(pid))
	release()

	if _, err := os.Stat(filepath.Join(state, "watch-herdr.lock.json")); err != nil {
		t.Fatalf("release removed a lock we no longer own: %v", err)
	}
}

func TestTryAcquireUnparsableLock(t *testing.T) {
	state := t.TempDir()
	writeLock(t, state, "listen", "{oops")
	if _, _, _, err := testCtx(state).TryAcquire("listen", ""); err == nil {
		t.Fatal("expected an error for an unparsable lock file")
	}
}

// unsetLockEnv clears the lock-owner env vars the harness process may
// carry, so the owner-lock assertions see the unset state deterministically.
func unsetLockEnv(t *testing.T) {
	t.Helper()
	for _, key := range []string{"PI_SESSION_ID", "HERDR_PANE_ID", "PI_SESSION_CWD"} {
		if v, ok := os.LookupEnv(key); ok {
			os.Unsetenv(key)
			t.Cleanup(func() { os.Setenv(key, v) })
		}
	}
}
