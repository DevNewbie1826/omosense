package core_test

import (
	"bufio"
	"bytes"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"

	"github.com/DevNewbie1826/omosense/internal/core"
)

func liveCliSleeper(t *testing.T) int {
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

func writeCliLock(t *testing.T, state, name, content string) {
	t.Helper()
	if err := os.MkdirAll(state, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(state, name+".lock.json"), []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
}

func cliHeldJSON(pid int) string {
	return `{"pid":` + strconv.Itoa(pid) + `,"session":null,"pane":null,"cwd":"/tmp","started":"2026-10-03T00:00:00.000Z"}`
}

func runProbe(t *testing.T, args []string, stdin *strings.Reader, env ...string) (stdout, stderr string, code int) {
	t.Helper()
	buildBinaries(t)
	cmd := exec.Command(probePath, args...)
	if stdin != nil {
		cmd.Stdin = stdin
	}
	cmd.Env = append(os.Environ(), env...)
	if stdin != nil {
		cmd.Stdin = stdin
	}
	var ob, eb bytes.Buffer
	cmd.Stdout, cmd.Stderr = &ob, &eb
	err := cmd.Run()
	code = 0
	var ee *exec.ExitError
	if errors.As(err, &ee) {
		code = ee.ExitCode()
	} else if err != nil {
		t.Fatalf("run probe: %v", err)
	}
	return ob.String(), eb.String(), code
}

func TestAcquireExits3OnLiveHolder(t *testing.T) {
	_, dir, state := cliEnv(t)
	writeCliConfig(t, dir, cliProfilesCfg)
	pid := liveCliSleeper(t)
	writeCliLock(t, state, "listen-main", cliHeldJSON(pid))

	stdOut, _, code := runProbe(t, []string{"--profile", "main"}, nil, "PROBE_LOCK=listen-main")
	if code != 3 {
		t.Fatalf("exit = %d, want 3", code)
	}
	if !strings.HasPrefix(stdOut, "LOG ALREADY_RUNNING listen-main {") || !strings.HasSuffix(stdOut, "}\n") {
		t.Fatalf("stdout = %q, want LOG ALREADY_RUNNING listen-main <json>", stdOut)
	}
	v, err := core.ParseJSON([]byte(strings.TrimPrefix(strings.TrimSuffix(stdOut, "\n"), "LOG ALREADY_RUNNING listen-main ")))
	if err != nil {
		t.Fatalf("held json: %v", err)
	}
	if got, _ := v.(*core.OMap).Get("pid"); numStr(got) != strconv.Itoa(pid) {
		t.Errorf("held pid = %v, want %d", got, pid)
	}
}

func TestAcquireExits3OnLiveLegacyHolder(t *testing.T) {
	_, dir, state := cliEnv(t)
	writeCliConfig(t, dir, cliProfilesCfg)
	pid := liveCliSleeper(t)
	writeCliLock(t, state, "listen", cliHeldJSON(pid))

	stdOut, _, code := runProbe(t, []string{"--profile", "main"}, nil, "PROBE_LOCK=listen-main", "PROBE_LEGACY=listen")
	if code != 3 {
		t.Fatalf("exit = %d, want 3", code)
	}
	if !strings.HasPrefix(stdOut, "LOG ALREADY_RUNNING listen {") {
		t.Fatalf("stdout = %q, want the legacy name in the LOG line", stdOut)
	}
}

func TestAcquireUnparsableLockExits1(t *testing.T) {
	_, dir, state := cliEnv(t)
	writeCliConfig(t, dir, cliProfilesCfg)
	writeCliLock(t, state, "listen-main", "{oops")

	stdOut, stderr, code := runProbe(t, []string{"--profile", "main"}, nil, "PROBE_LOCK=listen-main")
	if code != 1 {
		t.Fatalf("exit = %d, want 1", code)
	}
	if stderr == "" || stdOut != "" {
		t.Errorf("stderr = %q stdout = %q, want error on stderr only", stderr, stdOut)
	}
}

func TestAcquireReleasesOnStdinEOF(t *testing.T) {
	_, dir, state := cliEnv(t)
	writeCliConfig(t, dir, cliProfilesCfg)

	stdOut, _, code := runProbe(t, []string{"--profile", "main"}, strings.NewReader(""), "PROBE_LOCK=listen-main", "PROBE_LEGACY=listen")
	if code != 0 || stdOut != "acquired\n" {
		t.Fatalf("code=%d stdout=%q, want 0/acquired", code, stdOut)
	}
	if _, err := os.Stat(filepath.Join(state, "listen-main.lock.json")); !os.IsNotExist(err) {
		t.Errorf("lock file not released on exit: %v", err)
	}
}

func TestAcquireReleasesOnSignal(t *testing.T) {
	_, dir, state := cliEnv(t)
	writeCliConfig(t, dir, cliProfilesCfg)
	buildBinaries(t)

	cmd := exec.Command(probePath, "--profile", "main")
	cmd.Env = append(os.Environ(), "PROBE_LOCK=listen-main", "PROBE_LEGACY=listen")
	stdin, err := cmd.StdinPipe()
	if err != nil {
		t.Fatal(err)
	}
	defer stdin.Close()
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	var eb bytes.Buffer
	cmd.Stderr = &eb
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}

	line, err := bufio.NewReader(stdout).ReadString('\n')
	if err != nil || line != "acquired\n" {
		t.Fatalf("probe line = %q err=%v, want acquired", line, err)
	}
	if _, err := os.Stat(filepath.Join(state, "listen-main.lock.json")); err != nil {
		t.Fatalf("lock file missing while held: %v", err)
	}

	if err := cmd.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	err = cmd.Wait()
	code := 0
	var ee *exec.ExitError
	if errors.As(err, &ee) {
		code = ee.ExitCode()
	} else if err != nil {
		t.Fatalf("wait: %v", err)
	}
	if code != 0 {
		t.Errorf("exit after SIGTERM = %d, want 0 (stderr=%q)", code, eb.String())
	}
	if _, err := os.Stat(filepath.Join(state, "listen-main.lock.json")); !os.IsNotExist(err) {
		t.Errorf("lock file not released on signal: %v", err)
	}
}

func numStr(v any) string {
	type stringer interface{ String() string }
	if s, ok := v.(stringer); ok {
		return s.String()
	}
	return "?"
}
