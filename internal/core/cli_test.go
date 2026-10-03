package core_test

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
	dir = filepath.Join(home, ".omomeow")
	state = filepath.Join(dir, "state")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("HOME", home)
	t.Setenv("OMOMEOW_DIR", dir)
	t.Setenv("OMOMEOW_STATE", state)
	return home, dir, state
}

func writeCliConfig(t *testing.T, dir, cfg string) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(dir, "config.json"), []byte(cfg), 0o644); err != nil {
		t.Fatal(err)
	}
}

const cliFallbackCfg = `{"telegram":{"bot":"b1","dm_bot":"b2"},"discord":{"bot":"d1"}}`

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
	writeCliConfig(t, dir, cliFallbackCfg)

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
	writeCliConfig(t, dir, cliFallbackCfg)

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
	writeCliConfig(t, dir, cliFallbackCfg)

	for _, args := range [][]string{
		{"google", "--once", "--profile", "main"},
		{"herdr", "--now", "--profile", "main"},
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
	writeCliConfig(t, dir, cliFallbackCfg)

	runBin(t, "google", "--profile", "main")
	if fi, err := os.Stat(state); err != nil || !fi.IsDir() {
		t.Errorf("writable run did not create the state dir (err=%v)", err)
	}
}
