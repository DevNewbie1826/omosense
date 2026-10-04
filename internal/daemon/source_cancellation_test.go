package daemon

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
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

// The parent and its optional pipe-holding child never release their gates.
// Readiness is announced over sockets, not inferred from elapsed time.
func TestBlockedSourceCommand(t *testing.T) {
	if os.Getenv("OS_CANCEL_HELPER") != "1" {
		return
	}
	conn, err := net.Dial("unix", os.Getenv("OS_CANCEL_GATE"))
	if err != nil {
		os.Exit(2)
	}
	defer conn.Close()
	event := "blocked"
	if os.Getenv("OS_CANCEL_CHILD") == "1" {
		event = "child"
	} else if mode := os.Getenv("OS_CANCEL_MODE"); mode != "direct" {
		child := exec.Command(os.Args[0], "-test.run=^TestBlockedSourceCommand$")
		child.Env = append(os.Environ(), "OS_CANCEL_CHILD=1")
		child.Stdout, child.Stderr = os.Stdout, os.Stderr
		if mode == "escaped" {
			child.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
		}
		if err := child.Start(); err != nil {
			os.Exit(2)
		}
	}
	if err := json.NewEncoder(conn).Encode(processEvent{Event: event, PID: os.Getpid()}); err != nil {
		os.Exit(2)
	}
	if event == "blocked" && os.Getenv("OS_CANCEL_MODE") == "parent-exits" {
		os.Exit(0) // The child keeps the inherited pipes and its gate.
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
	for _, tc := range []struct{ source, command, lock, mode string }{
		{"google", "zele", "watch-google-main", "direct"},
		{"herdr", "herdr", "watch-herdr-main", "direct"},
		{"tidy", "git", "memory-tidy-main", "direct"},
		{"google", "zele", "watch-google-main", "descendant"},
		{"herdr", "herdr", "watch-herdr-main", "descendant"},
		{"tidy", "git", "memory-tidy-main", "descendant"},
		{"google", "zele", "watch-google-main", "escaped"},
		{"herdr", "herdr", "watch-herdr-main", "escaped"},
		{"tidy", "git", "memory-tidy-main", "escaped"},
		{"google", "zele", "watch-google-main", "parent-exits"},
		{"herdr", "herdr", "watch-herdr-main", "parent-exits"},
		{"tidy", "git", "memory-tidy-main", "parent-exits"},
	} {
		t.Run(tc.source+"/"+tc.mode, func(t *testing.T) {
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
				kind string
				err  error
			}
			count := 1
			if tc.mode != "direct" {
				count = 2
			}
			blocked := make(chan blockedCommand, count)
			go func() {
				for range count {
					conn, err := ln.Accept()
					if err != nil {
						blocked <- blockedCommand{err: err}
						return
					}
					var e processEvent
					err = json.NewDecoder(conn).Decode(&e)
					blocked <- blockedCommand{conn: conn, pid: e.PID, kind: e.Event, err: err}
				}
			}()
			env := append(os.Environ(), "HOME="+root, "OMOMEOW_DIR="+p.dir,
				"OMOMEOW_STATE="+state, "OMOSENSE_SOCK="+p.socket, "OMO_MEMORY_AGENTS="+agents,
				"PATH="+bindir+":"+os.Getenv("PATH"), "OS_CANCEL_HELPER=1", "OS_CANCEL_GATE="+gate,
				"OS_CANCEL_MODE="+tc.mode, "OS_CANCEL_CHILD=",
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
			var processes []blockedCommand
			var daemonPID int
			t.Cleanup(func() {
				for _, b := range processes {
					_ = syscall.Kill(b.pid, syscall.SIGKILL)
					_ = b.conn.Close()
				}
				if daemonPID != 0 {
					_ = syscall.Kill(daemonPID, syscall.SIGKILL)
				}
			})
			for range count {
				b := await(t, blocked)
				if b.err != nil {
					t.Fatal(b.err)
				}
				processes = append(processes, b)
				t.Logf("BLOCKED %s %s pid=%d; gate will not be released", tc.command, b.kind, b.pid)
			}
			run := func(args ...string) (string, error) {
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
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
			if tc.mode == "parent-exits" {
				// The parent exits on its own; the source must reap the rest of
				// its process group before any stop is requested.
				for _, b := range processes {
					if err := b.conn.SetReadDeadline(time.Now().Add(10 * time.Second)); err != nil {
						t.Fatal(err)
					}
					var data [1]byte
					if n, err := b.conn.Read(data[:]); n != 0 || !errors.Is(err, io.EOF) {
						t.Fatalf("%s %d outlived its exited parent: n=%d err=%v", b.kind, b.pid, n, err)
					}
				}
				t.Log("PARENT-EXITS: parent and descendant both gone before stop")
			}
			lock := filepath.Join(state, tc.lock+".lock.json")
			if _, err := os.Stat(lock); err != nil {
				t.Fatalf("running source lock: %v", err)
			}
			start := time.Now()
			if _, err := run("daemon", "stop"); err != nil {
				t.Fatalf("stop did not cancel in-flight %s: %v", tc.command, err)
			}
			if elapsed := time.Since(start); elapsed >= 5*time.Second {
				t.Fatalf("stop exceeded short bound: %s", elapsed)
			} else {
				t.Logf("STOP elapsed=%s (bound 5s)", elapsed)
			}
			if err := await(t, done); err != nil {
				t.Fatalf("attach must exit zero without respawn: %v", err)
			}
			done <- nil // Paired cleanup can join without polling.
			for _, b := range processes {
				if tc.mode == "escaped" && b.kind == "child" {
					if err := syscall.Kill(b.pid, 0); err != nil {
						t.Fatalf("escaped pipe holder exited before teardown: %v", err)
					}
					if err := syscall.Kill(b.pid, syscall.SIGKILL); err != nil {
						t.Fatal(err)
					}
				}
				// EOF is the exact death signal; the gate is never opened.
				if err := b.conn.SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil {
					t.Fatal(err)
				}
				var data [1]byte
				if n, err := b.conn.Read(data[:]); n != 0 || !errors.Is(err, io.EOF) {
					t.Fatalf("process %d retained its gate: n=%d err=%v", b.pid, n, err)
				}
				if err := syscall.Kill(b.pid, 0); !errors.Is(err, syscall.ESRCH) {
					// An escaped orphan is reaped asynchronously by init.
					// EOF proves exit; a still-visible PID must be a zombie,
					// not a live process. Do not poll or sleep for init.
					if (tc.mode != "escaped" && tc.mode != "parent-exits") || b.kind != "child" {
						t.Fatalf("process %s still alive/not reaped: %v", strconv.Itoa(b.pid), err)
					}
					state, psErr := exec.Command("ps", "-p", strconv.Itoa(b.pid), "-o", "stat=").Output()
					var exit *exec.ExitError
					absent := errors.As(psErr, &exit) && exit.ExitCode() == 1 && len(state) == 0
					if (psErr != nil && !absent) || (len(state) != 0 && !strings.HasPrefix(strings.TrimSpace(string(state)), "Z")) {
						t.Fatalf("escaped process %d remains live: state=%q err=%v", b.pid, state, psErr)
					}
				}
				_ = b.conn.Close()
			}
			for _, path := range []string{lock, filepath.Join(state, "remind-main.lock.json"), p.socket, p.pid} {
				if _, err := os.Stat(path); !os.IsNotExist(err) {
					t.Fatalf("stop left %s: %v", path, err)
				}
			}
			daemonPID = 0   // Successfully stopped; never signal a reused PID.
			processes = nil // All PIDs verified dead; never signal reused PIDs.
			t.Log("PASS: stop/attach exit 0; parent/descendant dead; locks/socket/pid absent; no respawn")
			t.Log("cleanup: attach joined; stopped daemon; fake processes dead; gate listener closed; fixture directories removed by testing")
		})
	}
}
