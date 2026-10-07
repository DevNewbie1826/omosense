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

// pinnedGoEnv keeps the child `go build` on the user's real Go caches: the
// tests repoint HOME at temp dirs, and a module cache inside t.TempDir
// leaves read-only files that break the cleanup. Resolved at package init,
// before any test runs t.Setenv.
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
		var err error
		buildDir, err = os.MkdirTemp("", "omosense-testbin-")
		if err != nil {
			buildErr = err
			return
		}
		binPath = filepath.Join(buildDir, "omosense")
		build := exec.Command("go", "build", "-o", binPath, "../../cmd/omosense")
		build.Env = append(os.Environ(), pinnedGoEnv...)
		if out, err := build.CombinedOutput(); err != nil {
			buildErr = fmt.Errorf("go build cmd/omosense: %v\n%s", err, out)
			return
		}
		probePath = filepath.Join(buildDir, "lockprobe")
		probe := exec.Command("go", "build", "-o", probePath, "./testdata/lockprobe")
		probe.Env = append(os.Environ(), pinnedGoEnv...)
		if out, err := probe.CombinedOutput(); err != nil {
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

const cliFlatCfg = `{"telegram":{"bot":"b1"},"discord":{"bot":"d1"},"tidy":{"enabled":true}}`

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

// TestCLIBareHostMissingConfig pins bare `omosense` as the session host: it
// loads the folder's config.json instead of printing usage, so a folder
// without one fails loudly naming the exact path.
func TestCLIBareHostMissingConfig(t *testing.T) {
	// Build before cliEnv points HOME at a temp dir: go build under a fresh
	// HOME would fill that dir with a read-only module cache.
	buildBinaries(t)
	_, dir, _ := cliEnv(t)
	stdout, stderr, code := runBin(t)
	if code != 1 {
		t.Errorf("exit = %d, want 1", code)
	}
	if stdout != "" {
		t.Errorf("stdout = %q, want empty", stdout)
	}
	if !strings.Contains(stderr, filepath.Join(dir, "config.json")) {
		t.Errorf("stderr = %q, want the config path", stderr)
	}
}

func TestCLIUnknownSubcommand(t *testing.T) {
	_, stderr, code := runBin(t, "bogus")
	if code != 2 {
		t.Errorf("exit = %d, want 2", code)
	}
	if !strings.Contains(stderr, "Usage: omosense") {
		t.Errorf("stderr = %q, want usage", stderr)
	}
}

// TestCLIDaemonAndAttachRemoved pins IS-2: the retired daemon and its attach
// client are unknown subcommands, so a stale caller fails loudly.
func TestCLIDaemonAndAttachRemoved(t *testing.T) {
	for _, args := range [][]string{{"daemon"}, {"daemon", "status"}, {"attach", "listen"}} {
		stdout, stderr, code := runBin(t, args...)
		if code != 2 {
			t.Errorf("%v: exit = %d, want 2", args, code)
		}
		if stdout != "" {
			t.Errorf("%v: stdout = %q, want empty", args, stdout)
		}
		if !strings.Contains(stderr, "Usage: omosense") {
			t.Errorf("%v: stderr = %q, want usage", args, stderr)
		}
	}
}

func TestCLITopLevelHelp(t *testing.T) {
	stdout, stderr, code := runBin(t, "--help")
	if code != 0 {
		t.Errorf("exit = %d, want 0", code)
	}
	if !strings.Contains(stdout, "Usage: omosense") || !strings.Contains(stdout, "Bare omosense runs the session host") {
		t.Errorf("stdout = %q, want the session-host usage", stdout)
	}
	if strings.Contains(stdout, "daemon") || strings.Contains(stdout, "attach") {
		t.Errorf("stdout = %q, must not advertise the retired daemon", stdout)
	}
	if stderr != "" {
		t.Errorf("stderr = %q, want empty", stderr)
	}
}

// TestCLIProfileFlagRemoved pins the stale-caller guard: --profile fails
// loudly (exit 2) in every position instead of silently reading another
// folder's config, and --help does not rescue it.
func TestCLIProfileFlagRemoved(t *testing.T) {
	_, dir, _ := cliEnv(t)
	writeCliConfig(t, dir, cliFlatCfg)

	for _, args := range [][]string{
		{"listen", "--profile", "nope"},
		{"listen", "--profile=nope"},
		{"say", "telegram", "send", "{}", "--profile", "nope"},
		{"say", "--profile=nope", "telegram"},
		{"remind", "--profile", "main"},
		{"listen", "--help", "--profile", "main"},
	} {
		stdout, stderr, code := runBin(t, args...)
		if code != 2 {
			t.Errorf("%v: exit = %d, want 2", args, code)
		}
		if stdout != "" {
			t.Errorf("%v: stdout = %q, want empty", args, stdout)
		}
		if !strings.Contains(stderr, "--profile was removed") {
			t.Errorf("%v: stderr = %q, want the removal message", args, stderr)
		}
	}
}

func TestCLIReadOnlyPathsLeaveStateAbsent(t *testing.T) {
	_, dir, state := cliEnv(t)
	writeCliConfig(t, dir, cliFlatCfg)

	for _, args := range [][]string{
		{"google", "--once"},
		{"herdr", "--once"},
		{"listen", "--dry-run"},
		{"say", "telegram", "send", "{}"},
	} {
		runBin(t, args...)
		if _, err := os.Stat(state); !os.IsNotExist(err) {
			t.Errorf("%v created the state dir (err=%v)", args, err)
		}
	}
}

func TestCLIWritableRunCreatesState(t *testing.T) {
	_, dir, state := cliEnv(t)
	writeCliConfig(t, dir, cliFlatCfg)

	// remind is a long-running writable path with no external calls when the
	// reminders file is absent; it must create the state dir, then stop on SIGTERM.
	// The startup LOG line is printed only after the state dir exists and the
	// lock is held, so it is the exact readiness signal.
	buildBinaries(t)
	cmd := exec.Command(binPath, "remind")
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

// TestCLILegacyConfigFailsLoudly pins the stale-shape guard on the real
// binary: a profiles document, a legacy owner/wife key and a bots array must
// each fail loudly instead of running with no bots.
func TestCLILegacyConfigFailsLoudly(t *testing.T) {
	_, dir, _ := cliEnv(t)
	for _, tc := range []struct{ cfg, want string }{
		{`{"profiles":{"main":{"telegram":{"bots":["b1"]}}}}`, `"profiles" is no longer supported`},
		{`{"owner":"12"}`, `legacy top-level key "owner" is no longer supported`},
		{`{"wife":"13"}`, `legacy top-level key "wife" is no longer supported`},
		{`{"bots":["b1"]}`, `"bots" is no longer supported`},
		{`{"telegram":{"bots":["b1"]}}`, `telegram.bots is no longer supported`},
	} {
		writeCliConfig(t, dir, tc.cfg)
		_, stderr, code := runBin(t, "listen", "--dry-run")
		if code != 1 {
			t.Errorf("cfg %s: exit = %d, want 1", tc.cfg, code)
		}
		if !strings.Contains(stderr, tc.want) {
			t.Errorf("cfg %s: stderr = %q, want %q", tc.cfg, stderr, tc.want)
		}
	}
}

// TestCLIShapeErrorNamesTheKey pins the loud shape errors: a present key of
// the wrong type names its path in config.json.
func TestCLIShapeErrorNamesTheKey(t *testing.T) {
	_, dir, _ := cliEnv(t)
	writeCliConfig(t, dir, `{"telegram":{"bot":7}}`)
	_, stderr, code := runBin(t, "listen", "--dry-run")
	if code != 1 || !strings.Contains(stderr, "config.json: telegram.bot must be a string") {
		t.Errorf("exit = %d stderr = %q, want the telegram.bot shape error", code, stderr)
	}
}

func TestCLIStaleOmomeowEnvFails(t *testing.T) {
	home := t.TempDir()
	dir := filepath.Join(home, ".omomeow")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	writeCliConfig(t, dir, cliFlatCfg)
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

// TestCLIBareHostMemoryCheck pins IS-14 on the real binary: bare
// omosense with a configured memory id whose repo is missing exits 1
// naming the key and the checked path before any lock or source starts;
// with the repo present the host starts (its startup LOG) and stops
// cleanly on SIGTERM. herdr is disabled so the test host never touches a
// real herdr socket.
func TestCLIBareHostMemoryCheck(t *testing.T) {
	buildBinaries(t)
	home, dir, state := cliEnv(t)
	agents := filepath.Join(home, "agents")
	t.Setenv("OMO_MEMORY_AGENTS", agents)

	writeCliConfig(t, dir, `{"memory":"nope","herdr":{"enabled":false}}`)
	stdout, stderr, code := runBin(t)
	want := "omosense: config.json: memory \"nope\": repo not found at " + filepath.Join(agents, "nope", "repo") + "\n"
	if code != 1 || stderr != want || stdout != "" {
		t.Fatalf("exit = %d stderr = %q stdout = %q, want exit 1 with exactly %q", code, stderr, stdout, want)
	}
	locks, err := filepath.Glob(filepath.Join(state, "*.lock"))
	if err != nil || len(locks) != 0 {
		t.Errorf("a lock was taken before the memory check: %v (err=%v)", locks, err)
	}

	if err := os.MkdirAll(filepath.Join(agents, "ok", "repo"), 0o755); err != nil {
		t.Fatal(err)
	}
	writeCliConfig(t, dir, `{"memory":"ok","herdr":{"enabled":false}}`)
	cmd := exec.Command(binPath)
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
			if strings.HasPrefix(sc.Text(), "LOG omosense host starting") {
				ready <- sc.Text()
			}
		}
		close(ready)
	}()
	select {
	case line, ok := <-ready:
		if !ok {
			_ = cmd.Process.Kill()
			_ = cmd.Wait()
			t.Fatal("the host exited before its startup line")
		}
		_ = line
	case <-time.After(10 * time.Second):
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
		t.Fatal("the host did not start within 10s (memory check refused a present repo)")
	}
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	_ = cmd.Process.Signal(syscall.SIGTERM)
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		_ = cmd.Process.Kill()
		t.Fatal("the host did not exit on SIGTERM")
	}
}
