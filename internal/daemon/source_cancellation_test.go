package daemon

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"
)

// The helper is exec'd, not shell-forked, so its PID is the source's child.
// It never releases its gate: only cancellation can end the command.
func TestBlockedSourceCommand(t *testing.T) {
	if os.Getenv("OS_CANCEL_HELPER") != "1" {
		return
	}
	conn, err := net.Dial("unix", os.Getenv("OS_CANCEL_GATE"))
	if err != nil {
		os.Exit(2)
	}
	defer conn.Close()
	if err := json.NewEncoder(conn).Encode(processEvent{Event: "blocked", PID: os.Getpid()}); err != nil {
		os.Exit(2)
	}
	var b [1]byte
	_, _ = conn.Read(b[:])
	os.Exit(0)
}

func TestSourceCommandCancellation(t *testing.T) {
	bin := filepath.Join(t.TempDir(), "omosense")
	build := exec.Command("go", "build", "-o", bin, "./cmd/omosense")
	build.Dir = filepath.Join("..", "..")
	if out, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build: %v\n%s", err, out)
	}
	helper, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct{ source, command, lock string }{
		{"google", "zele", "watch-google-main"},
		{"herdr", "herdr", "watch-herdr-main"},
		{"tidy", "git", "memory-tidy-main"},
	} {
		t.Run(tc.source, func(t *testing.T) {
			p := socketPaths(t)
			root := t.TempDir()
			state := filepath.Join(root, "state")
			bindir := filepath.Join(root, "bin")
			agents := filepath.Join(root, "agents")
			for _, dir := range []string{state, bindir, filepath.Join(agents, "fixture", "repo", ".git")} {
				if err := os.MkdirAll(dir, 0o700); err != nil {
					t.Fatal(err)
				}
			}
			config := `{"profiles":{"main":{"discord":false,"telegram":[],"calendars":null,"mail":false}}}`
			if err := os.WriteFile(filepath.Join(p.dir, "config.json"), []byte(config), 0o600); err != nil {
				t.Fatal(err)
			}
			script := fmt.Sprintf("#!/bin/sh\nexec %q -test.run '^TestBlockedSourceCommand$'\n", helper)
			if err := os.WriteFile(filepath.Join(bindir, tc.command), []byte(script), 0o700); err != nil {
				t.Fatal(err)
			}
			gate := filepath.Join(p.dir, "command.sock")
			ln, err := net.Listen("unix", gate)
			if err != nil {
				t.Fatal(err)
			}
			defer ln.Close()
			type blockedCommand struct {
				conn net.Conn
				pid  int
				err  error
			}
			blocked := make(chan blockedCommand, 1)
			go func() {
				conn, err := ln.Accept()
				if err != nil {
					blocked <- blockedCommand{err: err}
					return
				}
				var e processEvent
				err = json.NewDecoder(conn).Decode(&e)
				blocked <- blockedCommand{conn: conn, pid: e.PID, err: err}
			}()
			env := append(os.Environ(), "HOME="+root, "OMOMEOW_DIR="+p.dir,
				"OMOMEOW_STATE="+state, "OMOSENSE_SOCK="+p.socket, "OMO_MEMORY_AGENTS="+agents,
				"PATH="+bindir+":"+os.Getenv("PATH"), "OS_CANCEL_HELPER=1", "OS_CANCEL_GATE="+gate,
				"OMOSENSE_TEST_REGISTRY=", "OMOSENSE_TEST_EVENTS=", "OMOSENSE_TEST_VERSION=",
				"OMOSENSE_TEST_READY_GATE=", "OMOSENSE_TEST_SPAWN_LOG=")
			a := exec.Command(bin, "attach", tc.source, "--profile", "main")
			a.Env = env
			stdout, err := a.StdoutPipe()
			if err != nil {
				t.Fatal(err)
			}
			a.Stderr = os.Stderr
			t.Logf("RUN %s attach %s --profile main (real source; socket-gated fake %s)", bin, tc.source, tc.command)
			if err := a.Start(); err != nil {
				t.Fatal(err)
			}
			done := make(chan error, 1)
			go func() {
				scan := bufio.NewScanner(stdout)
				for scan.Scan() {
					t.Log("STDOUT " + scan.Text())
				}
				done <- a.Wait()
			}()
			t.Cleanup(func() {
				_ = a.Process.Kill()
				select {
				case <-done:
				case <-time.After(15 * time.Second):
					t.Error("attach cleanup did not join")
				}
			})
			b := await(t, blocked)
			if b.err != nil {
				t.Fatal(b.err)
			}
			defer b.conn.Close()
			t.Logf("BLOCKED %s pid=%d; gate will not be released", tc.command, b.pid)
			var daemonPID int
			t.Cleanup(func() {
				_ = syscall.Kill(b.pid, syscall.SIGKILL)
				if daemonPID != 0 {
					_ = syscall.Kill(daemonPID, syscall.SIGKILL)
				}
			})
			run := func(args ...string) (string, error) {
				ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
				defer cancel()
				cmd := exec.CommandContext(ctx, bin, args...)
				cmd.Env = env
				out, err := cmd.CombinedOutput()
				t.Logf("RUN %s %s; exit=%v output=%s", bin, strings.Join(args, " "), err, out)
				return string(out), err
			}
			out, err := run("daemon", "status")
			var st status
			if err != nil || json.Unmarshal([]byte(out), &st) != nil {
				t.Fatalf("status: %v %s", err, out)
			}
			daemonPID = st.PID
			lock := filepath.Join(state, tc.lock+".lock.json")
			if _, err := os.Stat(lock); err != nil {
				t.Fatalf("running source lock: %v", err)
			}
			if _, err := run("daemon", "stop"); err != nil {
				t.Fatalf("stop did not cancel in-flight %s: %v", tc.command, err)
			}
			if err := await(t, done); err != nil {
				t.Fatalf("attach must exit zero without respawn: %v", err)
			}
			done <- nil // Paired cleanup can join without polling.
			if err := syscall.Kill(b.pid, 0); !errors.Is(err, syscall.ESRCH) {
				t.Fatalf("child %s still alive/not reaped: %v", strconv.Itoa(b.pid), err)
			}
			for _, path := range []string{lock, filepath.Join(state, "remind-main.lock.json"), p.socket, p.pid} {
				if _, err := os.Stat(path); !os.IsNotExist(err) {
					t.Fatalf("stop left %s: %v", path, err)
				}
			}
			daemonPID = 0 // Successfully stopped; never signal a reused PID.
			t.Log("PASS: child reaped without gate release; stop/attach exit 0; locks/socket/pid absent; no respawn")
			t.Log("cleanup: attach joined; stopped daemon; fake child reaped; gate listener closed; fixture directories removed by testing")
		})
	}
}
