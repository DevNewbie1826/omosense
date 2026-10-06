package remind

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"
)

var binOnce sync.Once
var binPath string
var binErr error

// pinnedGoEnv keeps the child `go build` on the user's real Go caches:
// the tests repoint HOME at temp dirs, and a module cache inside
// t.TempDir leaves read-only files that break the cleanup. Resolved at
// package init, before any test runs t.Setenv.
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

func omosenseBin(t *testing.T) string {
	t.Helper()
	binOnce.Do(func() {
		dir, err := os.MkdirTemp("", "omosense-remindcli-")
		if err != nil {
			binErr = err
			return
		}
		binPath = filepath.Join(dir, "omosense")
		build := exec.Command("go", "build", "-o", binPath, "../../cmd/omosense")
		build.Env = append(os.Environ(), pinnedGoEnv...)
		if out, err := build.CombinedOutput(); err != nil {
			binErr = fmt.Errorf("go build: %v\n%s", err, out)
		}
	})
	if binErr != nil {
		t.Fatal(binErr)
	}
	return binPath
}

func TestCompatRunLocksAndReleasesOnSIGTERM(t *testing.T) {
	_, state := testEnv(t)
	bin := omosenseBin(t)

	cmd := exec.Command(bin, "remind")
	var eb bytes.Buffer
	cmd.Stderr = &eb
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	linec := make(chan string, 1)
	go func() {
		ln, err := bufio.NewReader(stdout).ReadString('\n')
		if err != nil {
			linec <- ""
			return
		}
		linec <- ln
	}()
	var first string
	select {
	case first = <-linec:
	case <-time.After(30 * time.Second):
		t.Fatal("timed out waiting for the startup line")
	}
	if want := "LOG reminder scheduler starting\n"; first != want {
		t.Fatalf("first line = %q, want %q", first, want)
	}

	lockPath := filepath.Join(state, "remind.lock.json")
	if _, err := os.Stat(lockPath); err != nil {
		t.Fatalf("remind lock not held while running: %v", err)
	}

	if err := cmd.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	waitErr := cmd.Wait()
	code := 0
	var ee *exec.ExitError
	if errors.As(waitErr, &ee) {
		code = ee.ExitCode()
	} else if waitErr != nil {
		t.Fatal(waitErr)
	}
	if code != 0 {
		t.Errorf("exit after SIGTERM = %d, want 0 (stderr=%q)", code, eb.String())
	}
	if _, err := os.Stat(lockPath); !os.IsNotExist(err) {
		t.Errorf("lock not released on SIGTERM: %v", err)
	}
}

func TestCompatBlockedByLiveLockExits3(t *testing.T) {
	_, state := testEnv(t)
	sleeper := exec.Command("sleep", "300")
	if err := sleeper.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = sleeper.Process.Kill()
		_ = sleeper.Wait()
	})
	held := fmt.Sprintf(`{"pid":%d,"session":null,"pane":null,"cwd":"/tmp","started":"2026-10-03T00:00:00.000Z"}`, sleeper.Process.Pid)
	if err := os.MkdirAll(state, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(state, "remind.lock.json"), []byte(held), 0o644); err != nil {
		t.Fatal(err)
	}

	cmd := exec.Command(omosenseBin(t), "remind")
	var ob bytes.Buffer
	cmd.Stdout = &ob
	err := cmd.Run()
	code := 0
	var ee *exec.ExitError
	if errors.As(err, &ee) {
		code = ee.ExitCode()
	} else if err != nil {
		t.Fatal(err)
	}
	if code != 3 {
		t.Errorf("exit = %d, want 3 when the remind lock is live", code)
	}
	if !strings.HasPrefix(ob.String(), "LOG ALREADY_RUNNING remind {") {
		t.Errorf("stdout = %q, want LOG ALREADY_RUNNING remind <json>", ob.String())
	}
}
