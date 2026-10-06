package listen

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/DevNewbie1826/omosense/internal/core"
)

func TestCompatProcess(t *testing.T) {
	if os.Getenv("OMOSENSE_LISTEN_TEST_CHILD") != "1" {
		return
	}
	// The parent sets sandbox directories and fake API endpoints before exec.
	var args []string
	if os.Getenv("OMOSENSE_LISTEN_TEST_DRY") == "1" {
		args = append(args, "--dry-run")
	}
	c, err := core.Load(core.ParseArgs(args), !strings.Contains(strings.Join(args, " "), "--dry-run"))
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	os.Exit(Run(c, args))
}

func compatCommand(t *testing.T, c *core.Ctx, dry bool) *exec.Cmd {
	t.Helper()
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	t.Cleanup(cancel)
	cmd := exec.CommandContext(ctx, exe, "-test.run=^TestCompatProcess$")
	cmd.Env = append(os.Environ(), "OMOSENSE_LISTEN_TEST_CHILD=1", "OMOSENSE_LISTEN_TEST_DRY=0")
	if dry {
		cmd.Env[len(cmd.Env)-1] = "OMOSENSE_LISTEN_TEST_DRY=1"
	}
	return cmd
}

func compatConfig(t *testing.T, c *core.Ctx, telegram bool) {
	t.Helper()
	if err := os.MkdirAll(c.Dir, 0700); err != nil {
		t.Fatal(err)
	}
	bot := `""`
	if telegram {
		bot = `"test"`
	}
	cfg := fmt.Sprintf(`{"telegram":{"bot":%s,"roles":{"12":"owner"}},"discord":{"bot":"","roles":{}}}`, bot)
	if err := os.WriteFile(filepath.Join(c.Dir, "config.json"), []byte(cfg), 0600); err != nil {
		t.Fatal(err)
	}
}

func TestCompatDryRunAndSourcePolicies(t *testing.T) {
	c := testCtx(t)
	c.Profile.Discord.Bot = "d1"
	sources := Sources(c)
	if len(sources) != 2 || sources[0].Name() != "telegram" || sources[0].AlwaysOn() || sources[1].Name() != "discord" || !sources[1].AlwaysOn() {
		t.Fatal("source run policy")
	}
	for _, source := range sources {
		lock, legacy := source.LockName()
		if lock != "listen" || legacy != "" || len(source.Prefixes()) != 1 || source.Prefixes()[0] != "EVENT" {
			t.Fatal("shared lock or prefix", lock, legacy, source.Prefixes())
		}
	}
	c.Profile.Discord.Bot = ""
	if len(Sources(c)) != 1 {
		t.Fatal("discord enabled for a disabled profile")
	}
	var direct bytes.Buffer
	c.Out = core.NewOut(&direct)
	if exit := Run(c, []string{"--dry-run"}); exit != 0 {
		t.Fatal(exit)
	}
	compatConfig(t, c, true)
	// Dry-run must neither inspect credentials nor create the missing state.
	if err := os.RemoveAll(filepath.Join(os.Getenv("HOME"), ".config")); err != nil {
		t.Fatal(err)
	}
	if err := os.RemoveAll(c.State); err != nil {
		t.Fatal(err)
	}
	out, err := compatCommand(t, c, true).CombinedOutput()
	if err != nil {
		t.Fatalf("dry-run: %v %s", err, out)
	}
	if !bytes.Equal(direct.Bytes(), out) || !bytes.HasPrefix(out, []byte("PLAN ")) {
		t.Fatalf("dry-run output %s want %s", out, direct.Bytes())
	}
	var plan map[string]any
	if err := json.Unmarshal(bytes.TrimPrefix(bytes.TrimSpace(out), []byte("PLAN ")), &plan); err != nil || plan["lock"] != "listen" || plan["dir"] != c.Dir || plan["state"] != c.State {
		t.Fatal(plan, err)
	}
	if !reflect.DeepEqual(plan["telegram"], []any{"test"}) || !reflect.DeepEqual(plan["discord"], []any{}) {
		t.Fatal("plan bots", plan)
	}
	if _, err := os.Stat(c.State); !os.IsNotExist(err) {
		t.Fatal("dry-run created state", err)
	}
	t.Logf("PASS: sandbox compat process --profile test --dry-run -> %s; cleanup: subprocess exited, no state files created", bytes.TrimSpace(out))
}

func TestCompatStartupAndLiveLock(t *testing.T) {
	c := testCtx(t)
	compatConfig(t, c, false)
	out, err := compatCommand(t, c, false).CombinedOutput()
	if err != nil || string(out) != "LOG omosense listener starting\n" {
		t.Fatalf("startup: %v %s", err, out)
	}
	if _, err := os.Stat(filepath.Join(c.State, "listen.lock.json")); !os.IsNotExist(err) {
		t.Fatal("compat leaked lock", err)
	}
	// A live holder of the (now unsuffixed) listen lock blocks the second
	// listener instead of letting it start.
	held := filepath.Join(c.State, "listen.lock.json")
	if err := os.WriteFile(held, []byte(fmt.Sprintf(`{"pid":%d,"session":"fixture"}`, os.Getpid())), 0600); err != nil {
		t.Fatal(err)
	}
	out, err = compatCommand(t, c, false).CombinedOutput()
	exit, ok := err.(*exec.ExitError)
	if !ok || exit.ExitCode() != 3 || !strings.HasPrefix(string(out), "LOG ALREADY_RUNNING listen ") || strings.Contains(string(out), "starting") {
		t.Fatalf("live-holder exclusion: %v %s", err, out)
	}
}

func TestCompatDryRunEmptyBots(t *testing.T) {
	c := testCtx(t)
	compatConfig(t, c, false)
	out, err := compatCommand(t, c, true).CombinedOutput()
	if err != nil {
		t.Fatal(err, string(out))
	}
	var plan map[string]any
	if err := json.Unmarshal(bytes.TrimPrefix(bytes.TrimSpace(out), []byte("PLAN ")), &plan); err != nil {
		t.Fatal(err)
	}
	bots, ok := plan["telegram"].([]any)
	if !ok || len(bots) != 0 {
		t.Fatalf("empty profile must emit an empty telegram array: %s", out)
	}
	discord, ok := plan["discord"].([]any)
	if !ok || len(discord) != 0 {
		t.Fatalf("empty profile must emit an empty discord bot list: %s", out)
	}
}

// TestCompatDryRunPlanBots pins the PLAN bot lists to the flat config: each
// platform reports its one configured bot as a one-element list.
func TestCompatDryRunPlanBots(t *testing.T) {
	c := testCtx(t)
	if err := os.MkdirAll(c.Dir, 0700); err != nil {
		t.Fatal(err)
	}
	cfg := `{"telegram":{"bot":"t1","roles":{}},"discord":{"bot":"d1","roles":{}}}`
	if err := os.WriteFile(filepath.Join(c.Dir, "config.json"), []byte(cfg), 0600); err != nil {
		t.Fatal(err)
	}
	out, err := compatCommand(t, c, true).CombinedOutput()
	if err != nil {
		t.Fatal(err, string(out))
	}
	var plan map[string]any
	if err := json.Unmarshal(bytes.TrimPrefix(bytes.TrimSpace(out), []byte("PLAN ")), &plan); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(plan["telegram"], []any{"t1"}) || !reflect.DeepEqual(plan["discord"], []any{"d1"}) {
		t.Fatalf("PLAN bot lists: %s", out)
	}
	t.Logf("PASS: PLAN reports the configured bot lists -> %s; cleanup: subprocess exited, no state created", bytes.TrimSpace(out))
}

func TestCompatTelegramStdoutAndSignalCleanup(t *testing.T) {
	c := testCtx(t)
	compatConfig(t, c, true)
	polling := make(chan struct{}, 1)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body map[string]any
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Error(err)
			return
		}
		if body["offset"] == float64(0) {
			fmt.Fprint(w, `{"ok":true,"result":[{"update_id":7,"message":{"message_id":9,"chat":{"id":10,"type":"private"},"from":{"id":12,"username":"fixture"},"text":"compat wire"}}]}`)
		} else {
			polling <- struct{}{}
			select {
			case <-r.Context().Done():
			case <-ctx.Done():
			}
		}
	}))
	defer server.Close()
	t.Setenv("OMOSENSE_TELEGRAM_API", server.URL)
	cmd := compatCommand(t, c, false)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	joined := false
	defer func() {
		cancel()
		if !joined {
			cmd.Process.Kill()
			cmd.Wait()
		}
	}()
	lines := make(chan string, 8)
	readerDone := make(chan struct{})
	go func() {
		defer close(readerDone)
		scanner := bufio.NewScanner(stdout)
		for scanner.Scan() {
			lines <- scanner.Text()
		}
	}()
	if line := await(t, lines); line != "LOG omosense listener starting" {
		t.Fatal(line)
	}
	line := await(t, lines)
	var event map[string]any
	if !strings.HasPrefix(line, "EVENT ") || json.Unmarshal([]byte(strings.TrimPrefix(line, "EVENT ")), &event) != nil || event["text"] != "compat wire" || event["role"] != "owner" {
		t.Fatal(line)
	}
	await(t, polling)
	if err := cmd.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	err = cmd.Wait()
	joined = true
	await(t, readerDone)
	if err != nil || stderr.Len() != 0 {
		t.Fatalf("compat signal exit: %v %s", err, stderr.Bytes())
	}
	if _, err := os.Stat(filepath.Join(c.State, "listen.lock.json")); !os.IsNotExist(err) {
		t.Fatal("signal leaked lock", err)
	}
	b, err := os.ReadFile(filepath.Join(c.State, "tg-offset-test"))
	if err != nil || string(b) != "8" {
		t.Fatal("compat offset", string(b), err)
	}
	t.Logf("PASS: compat subprocess POST fake getUpdates -> stdout %s -> SIGTERM exit0, offset8; cleanup: process waited, lock removed, HTTP server closed", line)
}
