package daemon

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

func profileCLIState(t *testing.T, f *processFixture) string {
	t.Helper()
	for _, e := range f.env {
		if state, ok := strings.CutPrefix(e, "OMOSENSE_STATE="); ok {
			if err := os.MkdirAll(state, 0o700); err != nil {
				t.Fatal(err)
			}
			return state
		}
	}
	t.Fatal("missing isolated state env")
	return ""
}

func assertProfileCLIJSON(t *testing.T, out string, daemon string, cancelled int) {
	t.Helper()
	var got profileStopWire
	dec := json.NewDecoder(strings.NewReader(out))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&got); err != nil {
		t.Fatal(err)
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal([]byte(out), &fields); err != nil || len(fields) != 4 {
		t.Fatalf("expected exactly four result fields: %q %v", out, err)
	}
	if strings.Count(out, "\n") != 1 || got.Profile != "family" || got.Daemon != daemon ||
		got.Cancelled != cancelled || !reflect.DeepEqual(got.Stopped, []string{"google", "herdr", "remind", "telegram", "tidy"}) {
		t.Fatalf("CLI result: %q -> %+v", out, got)
	}
}

func profileCLIAsync(t *testing.T, f *processFixture, args ...string) (*exec.Cmd, <-chan struct {
	out  string
	code int
}) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	cmd := exec.CommandContext(ctx, f.bin, args...)
	cmd.Env = f.env
	var out strings.Builder
	cmd.Stdout, cmd.Stderr = &out, &out
	t.Logf("RUN %s %s", f.bin, strings.Join(args, " "))
	if err := cmd.Start(); err != nil {
		cancel()
		t.Fatal(err)
	}
	result := make(chan struct {
		out  string
		code int
	}, 1)
	done := make(chan struct{})
	go func() {
		err := cmd.Wait()
		code := 0
		if err != nil {
			var exited *exec.ExitError
			if errors.As(err, &exited) {
				code = exited.ExitCode()
			} else {
				code = -1
			}
		}
		result <- struct {
			out  string
			code int
		}{out.String(), code}
		close(done)
	}()
	t.Cleanup(func() {
		cancel()
		await(t, done)
		t.Logf("cleanup: profile control %d reaped", cmd.Process.Pid)
	})
	return cmd, result
}

func TestProfileStopCLI(t *testing.T) {
	bin := filepath.Join(t.TempDir(), "omosense")
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	cmd := exec.CommandContext(ctx, "go", "build", "-race", "-o", bin, "./cmd/omosense")
	cmd.Dir = filepath.Join("..", "..")
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("build: %v\n%s", err, out)
	}
	for _, flag := range [][]string{{"--profile", "family"}, {"--profile=family"}} {
		t.Run("offline_"+strings.Join(flag, "_"), func(t *testing.T) {
			f := subprocessFixture(t, bin)
			state := profileCLIState(t, f)
			file := filepath.Join(state, "reminders-family.json")
			if err := os.WriteFile(file, []byte(`[{"id":"future","at":"2030-01-01"},{"id":"sent","sent":"yes"}]`), 0o600); err != nil {
				t.Fatal(err)
			}
			// A sentinel pid file proves offline flock does not write or remove it.
			if err := os.WriteFile(f.p.pid, []byte("sentinel\n"), 0o600); err != nil {
				t.Fatal(err)
			}
			out, code := f.run(append([]string{"daemon", "stop"}, flag...)...)
			if code != 0 {
				t.Fatalf("offline stop: exit=%d %s", code, out)
			}
			assertProfileCLIJSON(t, out, "offline", 1)
			assertProfileMarker(t, state, "family")
			b, err := os.ReadFile(file)
			if err != nil || strings.Count(string(b), `"cancelled"`) != 1 || !strings.Contains(string(b), `"sent": "yes"`) {
				t.Fatalf("reminders: %s %v", b, err)
			}
			pid, err := os.ReadFile(f.p.pid)
			if err != nil || string(pid) != "sentinel\n" {
				t.Fatalf("offline pid file changed: %q %v", pid, err)
			}
		})
	}
	t.Run("offline_discards_stale_journal_even_when_reminder_lock_blocks", func(t *testing.T) {
		f := subprocessFixture(t, bin)
		state := profileCLIState(t, f)
		path := filepath.Join(state, "omosense-journal-family.jsonl")
		j, err := openJournal(path, time.Now())
		if err != nil {
			t.Fatal(err)
		}
		if err := j.append(time.Now(), "remind", `REMIND failed {"id":"stale-offline"}`, false); err != nil {
			t.Fatal(err)
		}
		if err := j.close(); err != nil {
			t.Fatal(err)
		}
		foreignReminderLock(t, state, "remind-family")
		_, code := f.run("daemon", "stop", "--profile", "family")
		if code != 1 {
			t.Fatalf("blocked offline stop exit=%d", code)
		}
		b, err := os.ReadFile(path)
		if err != nil || !strings.Contains(string(b), `"delivered_seq":1`) {
			t.Fatalf("offline stop left stale replay: %s %v", b, err)
		}
		assertProfileMarker(t, state, "family")
	})
	t.Run("offline_unknown_and_malformed_exit_two", func(t *testing.T) {
		f := subprocessFixture(t, bin)
		for _, args := range [][]string{
			{"daemon", "stop", "--profile", "nope"},
			{"daemon", "stop", "--profile"},
			{"daemon", "stop", "--profile="},
			{"daemon", "stop", "--profile", "--unexpected"},
			{"daemon", "stop", "--profile=family", "extra"},
			{"daemon", "stop", "--profile=family", "--profile=main"},
			{"daemon", "status", "--profile=family"},
		} {
			out, code := f.run(args...)
			if code != 2 || out == "" {
				t.Fatalf("bad/unknown args exit=%d output=%q args=%v", code, out, args)
			}
		}
	})
	for _, lock := range []string{"remind-family", "remind"} {
		t.Run("offline_blocked_"+lock, func(t *testing.T) {
			f := subprocessFixture(t, bin)
			state := profileCLIState(t, f)
			file := filepath.Join(state, "reminders-family.json")
			body := `[ {"id":"pending","text":"keep bytes"} ]`
			if err := os.WriteFile(file, []byte(body), 0o600); err != nil {
				t.Fatal(err)
			}
			metadata := foreignReminderLock(t, state, lock)
			out, code := f.run("daemon", "stop", "--profile", "family")
			if code != 1 || !strings.Contains(out, lock+" "+metadata) {
				t.Fatalf("blocked stop: exit=%d %q", code, out)
			}
			b, err := os.ReadFile(file)
			if err != nil || string(b) != body {
				t.Fatalf("blocked stop rewrote file: %q %v", b, err)
			}
			assertProfileMarker(t, state, "family")
		})
	}
	t.Run("online_stop_shutdown_persistence_and_reenable", func(t *testing.T) {
		f := subprocessFixture(t, bin)
		state := profileCLIState(t, f)
		file := filepath.Join(state, "reminders-family.json")
		if err := os.WriteFile(file, []byte(`[{"id":"future","at":"2030-01-01"}]`), 0o600); err != nil {
			t.Fatal(err)
		}
		main := f.attach("v1", "main")
		pid := f.streamPID(main, 0)
		f.waitEvent("source:family:remind", pid)
		family := f.attach("v1", "family", "google")
		f.waitEvent("source:family:google", pid)
		out, code := f.run("daemon", "stop", "--profile=family")
		if code != 0 {
			t.Fatalf("online stop: %d %s", code, out)
		}
		assertProfileCLIJSON(t, out, "running", 1)
		if err := await(t, family.done); err != nil {
			t.Fatalf("family attach exit: %v", err)
		} else {
			family.done <- err
		}
		assertProfileMarker(t, state, "family")
		out, code = f.run("daemon", "status")
		var st status
		if err := json.Unmarshal([]byte(out), &st); err != nil || code != 0 || st.PID != pid {
			t.Fatalf("MAIN daemon changed: %s exit=%d %v", out, code, err)
		}
		for _, src := range st.Sources {
			if src.Profile == "family" && src.State != "stopped" {
				t.Fatal(src)
			}
		}
		if out, code := f.run("daemon", "stop", "--profile", "nope"); code != 2 || !strings.Contains(out, "unknown profile nope") {
			t.Fatalf("online unknown: %d %s", code, out)
		}
		f.stop(main)
		nextMain := f.attach("v1", "main")
		nextPID := f.streamPID(nextMain, 0)
		f.waitEvent("source:main:remind", nextPID)
		out, code = f.run("daemon", "status")
		if err := json.Unmarshal([]byte(out), &st); err != nil || code != 0 {
			t.Fatalf("restart status: %s %v", out, err)
		}
		for _, src := range st.Sources {
			if src.Profile == "family" && src.State != "stopped" {
				t.Fatal("restart revived family", src)
			}
		}
		nextFamily := f.attach("v1", "family", "remind")
		f.waitEvent("source:family:remind", nextPID)
		if _, err := os.Stat(filepath.Join(state, "omosense-profile-family.stopped")); !os.IsNotExist(err) {
			t.Fatalf("attach did not remove marker: %v", err)
		}
		f.stop(nextMain, nextFamily)
	})
	t.Run("lifetime_busy_before_bind_uses_online_not_offline", func(t *testing.T) {
		f := subprocessFixture(t, bin)
		state := profileCLIState(t, f)
		gate := filepath.Join(f.p.dir, "ready")
		f.env = append(f.env, "OMOSENSE_TEST_READY_GATE="+gate)
		main := f.attach("v1", "main")
		pid := f.waitEvent("gated", 0)
		control, result := profileCLIAsync(t, f, "daemon", "stop", "--profile", "family")
		// Auto-spawn still owns spawn.lock: this exact event proves the
		// control cannot prematurely classify the unbound daemon offline.
		f.waitEvent("spawn-wait", control.Process.Pid)
		if _, err := os.Stat(filepath.Join(state, "omosense-profile-family.stopped")); !os.IsNotExist(err) {
			t.Fatalf("premature offline marker: %v", err)
		}
		if err := os.WriteFile(gate, nil, 0o600); err != nil {
			t.Fatal(err)
		}
		if got := f.streamPID(main, 0); got != pid {
			t.Fatalf("server pid changed: %d != %d", got, pid)
		}
		got := await(t, result)
		t.Logf("EXIT %d: %s", got.code, got.out)
		if got.code != 0 {
			t.Fatalf("gated online: %d %s", got.code, got.out)
		}
		assertProfileCLIJSON(t, got.out, "running", 0)
		f.stop(main)
	})
	t.Run("foreground_lifetime_busy_before_bind_redials", func(t *testing.T) {
		f := subprocessFixture(t, bin)
		profileCLIState(t, f)
		gate := filepath.Join(f.p.dir, "ready")
		f.env = append(f.env, "OMOSENSE_TEST_READY_GATE="+gate)
		_, daemonResult := profileCLIAsync(t, f, "daemon")
		pid := f.waitEvent("gated", 0)
		control, result := profileCLIAsync(t, f, "daemon", "stop", "--profile", "family")
		// Unlike auto-spawn, the foreground daemon holds no spawn lock.
		f.waitEvent("profile-stop-online-wait", control.Process.Pid)
		if err := os.WriteFile(gate, nil, 0o600); err != nil {
			t.Fatal(err)
		}
		got := await(t, result)
		if got.code != 0 {
			t.Fatalf("foreground online retry: %d %s", got.code, got.out)
		}
		assertProfileCLIJSON(t, got.out, "running", 0)
		if _, code := f.run("daemon", "stop"); code != 0 {
			t.Fatal("whole stop failed")
		}
		f.waitEvent("exit", pid)
		if got := await(t, daemonResult); got.code != 0 {
			t.Fatalf("foreground daemon exit: %+v", got)
		}
	})
}
