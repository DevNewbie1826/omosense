package core_test

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

var (
	buildDir  string
	binPath   string
	probePath string
	buildOnce sync.Once
	buildErr  error
)

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
		var err error
		buildDir, err = os.MkdirTemp("", "omosense-testbin-")
		if err != nil {
			buildErr = err
			return
		}
		binPath = filepath.Join(buildDir, "omosense")
		if out, err := exec.Command("go", "build", "-o", binPath, "../../cmd/omosense").CombinedOutput(); err != nil {
			buildErr = fmt.Errorf("go build cmd/omosense: %v\n%s", err, out)
			return
		}
		probePath = filepath.Join(buildDir, "lockprobe")
		if out, err := exec.Command("go", "build", "-o", probePath, "./testdata/lockprobe").CombinedOutput(); err != nil {
			buildErr = fmt.Errorf("go build lockprobe: %v\n%s", err, out)
		}
	})
	if buildErr != nil {
		t.Fatalf("build: %v", buildErr)
	}
}

func cliEnv(t *testing.T) (home, dir, state string) {
	t.Helper()
	home = t.TempDir()
	dir = filepath.Join(home, ".omosense")
	state = filepath.Join(dir, "state")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("HOME", home)
	t.Setenv("OMOSENSE_DIR", dir)
	t.Setenv("OMOSENSE_STATE", state)
	return home, dir, state
}

func writeCliConfig(t *testing.T, dir, cfg string) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(dir, "config.json"), []byte(cfg), 0o644); err != nil {
		t.Fatal(err)
	}
}

const cliProfilesCfg = `{"profiles":{"main":{"telegram":{"bots":["b1","b2"]},"discord":{"bots":["d1"]},"tidy":{"enabled":true}}}}`

func runBin(t *testing.T, args ...string) (stdout, stderr string, code int) {
	t.Helper()
	buildBinaries(t)
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

func TestCLINoArgsUsage(t *testing.T) {
	stdout, stderr, code := runBin(t)
	if code != 2 {
		t.Errorf("exit = %d, want 2", code)
	}
	if stdout != "" {
		t.Errorf("stdout = %q, want empty", stdout)
	}
	if !strings.Contains(stderr, "Usage: omosense <subcommand>") {
		t.Errorf("stderr = %q, want usage", stderr)
	}
}

func TestCLIUnknownSubcommand(t *testing.T) {
	_, stderr, code := runBin(t, "bogus")
	if code != 2 {
		t.Errorf("exit = %d, want 2", code)
	}
	if !strings.Contains(stderr, "Usage: omosense <subcommand>") {
		t.Errorf("stderr = %q, want usage", stderr)
	}
}

func TestCLITopLevelHelp(t *testing.T) {
	stdout, stderr, code := runBin(t, "--help")
	if code != 0 {
		t.Errorf("exit = %d, want 0", code)
	}
	if !strings.Contains(stdout, "Usage: omosense <subcommand>") {
		t.Errorf("stdout = %q, want usage", stdout)
	}
	if stderr != "" {
		t.Errorf("stderr = %q, want empty", stderr)
	}
}

func TestCLIUnknownProfileExits2(t *testing.T) {
	_, dir, _ := cliEnv(t)
	writeCliConfig(t, dir, cliProfilesCfg)

	stdout, stderr, code := runBin(t, "listen", "--profile", "nope")
	if code != 2 {
		t.Errorf("exit = %d, want 2", code)
	}
	if stderr != "unknown profile nope\n" {
		t.Errorf("stderr = %q", stderr)
	}
	if stdout != "" {
		t.Errorf("stdout = %q, want empty", stdout)
	}
}

func TestCLISayProfileBothSyntaxes(t *testing.T) {
	_, dir, _ := cliEnv(t)
	writeCliConfig(t, dir, cliProfilesCfg)

	for _, args := range [][]string{
		{"say", "telegram", "send", "{}", "--profile", "nope"},
		{"say", "--profile=nope", "telegram"},
	} {
		_, stderr, code := runBin(t, args...)
		if code != 2 {
			t.Errorf("%v: exit = %d, want 2", args, code)
		}
		if stderr != "unknown profile nope\n" {
			t.Errorf("%v: stderr = %q", args, stderr)
		}
	}
}

func TestCLIReadOnlyPathsLeaveStateAbsent(t *testing.T) {
	_, dir, state := cliEnv(t)
	writeCliConfig(t, dir, cliProfilesCfg)

	for _, args := range [][]string{
		{"google", "--once", "--profile", "main"},
		{"herdr", "--once", "--profile", "main"},
		{"listen", "--dry-run", "--profile", "main"},
		{"say", "--profile", "main"},
	} {
		runBin(t, args...)
		if _, err := os.Stat(state); !os.IsNotExist(err) {
			t.Errorf("%v created the state dir (err=%v)", args, err)
		}
	}
}

func TestCLIWritableRunCreatesState(t *testing.T) {
	_, dir, state := cliEnv(t)
	writeCliConfig(t, dir, cliProfilesCfg)

	// remind is a long-running writable path with no external calls when the
	// reminders file is absent; it must create the state dir, then stop on SIGTERM.
	// The startup LOG line is printed only after the state dir exists and the
	// lock is held, so it is the exact readiness signal.
	buildBinaries(t)
	cmd := exec.Command(binPath, "remind", "--profile", "main")
	out, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	ready := make(chan string, 1)
	go func() {
		sc := bufio.NewScanner(out)
		for sc.Scan() {
			if strings.HasPrefix(sc.Text(), "LOG reminder scheduler starting") {
				ready <- sc.Text()
			}
		}
		close(ready)
	}()
	select {
	case line, ok := <-ready:
		if !ok {
			t.Fatalf("remind exited before its startup line")
		}
		_ = line
	case <-time.After(10 * time.Second):
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
		t.Fatalf("remind printed no startup line within 10s")
	}
	if fi, err := os.Stat(state); err != nil || !fi.IsDir() {
		t.Errorf("writable run did not create the state dir (err=%v)", err)
	}
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	_ = cmd.Process.Signal(syscall.SIGTERM)
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		_ = cmd.Process.Kill()
		t.Fatalf("remind did not exit on SIGTERM")
	}
}

func TestCLILegacyConfigFailsLoudly(t *testing.T) {
	_, dir, _ := cliEnv(t)
	writeCliConfig(t, dir, `{"telegram":{"bot":"b1"},"discord":{"bot":"d1"}}`)

	_, stderr, code := runBin(t, "listen", "--profile", "main", "--dry-run")
	if code != 1 {
		t.Errorf("exit = %d, want 1", code)
	}
	if !strings.Contains(stderr, `legacy top-level key "telegram" is no longer supported`) || !strings.Contains(stderr, "profiles") {
		t.Errorf("stderr = %q, want the legacy config message", stderr)
	}

	// A legacy document that also carries a profiles block fails the same
	// way instead of loading with the old sections quietly ignored.
	writeCliConfig(t, dir, `{"telegram":{"bot":"b1"},"profiles":{"main":{"telegram":{"bots":["b1"]}}}}`)
	_, stderr, code = runBin(t, "listen", "--profile", "main", "--dry-run")
	if code != 1 || !strings.Contains(stderr, `legacy top-level key "telegram"`) {
		t.Errorf("exit = %d stderr = %q, want legacy failure", code, stderr)
	}
}

func TestCLIMissingProfilesFailsLoudly(t *testing.T) {
	_, dir, _ := cliEnv(t)
	writeCliConfig(t, dir, `{"mail": true}`)
	_, stderr, code := runBin(t, "listen", "--profile", "main", "--dry-run")
	if code != 1 || !strings.Contains(stderr, `config.json: "profiles" is required`) {
		t.Errorf("exit = %d stderr = %q, want profiles required", code, stderr)
	}
}

func TestCLIStaleOmomeowEnvFails(t *testing.T) {
	home := t.TempDir()
	dir := filepath.Join(home, ".omomeow")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	writeCliConfig(t, dir, cliProfilesCfg)
	buildBinaries(t)
	var env []string
	for _, e := range os.Environ() {
		if strings.HasPrefix(e, "OMOSENSE_DIR=") || strings.HasPrefix(e, "OMOSENSE_STATE=") ||
			strings.HasPrefix(e, "OMOMEOW_DIR=") || strings.HasPrefix(e, "OMOMEOW_STATE=") {
			continue
		}
		env = append(env, e)
	}
	cmd := exec.Command(binPath, "listen", "--dry-run")
	cmd.Env = append(env, "HOME="+home, "OMOMEOW_DIR="+dir)
	var ob, eb bytes.Buffer
	cmd.Stdout, cmd.Stderr = &ob, &eb
	err := cmd.Run()
	code := 0
	var ee *exec.ExitError
	if errors.As(err, &ee) {
		code = ee.ExitCode()
	} else if err != nil {
		t.Fatal(err)
	}
	if code != 1 || !strings.Contains(eb.String(), "OMOMEOW_DIR is no longer read; set OMOSENSE_DIR") {
		t.Errorf("exit = %d stderr = %q, want stale OMOMEOW_DIR failure", code, eb.String())
	}
}
