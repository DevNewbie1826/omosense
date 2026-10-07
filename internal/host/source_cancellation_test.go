package host

import (
	"bufio"
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

type processEvent struct {
	Event string `json:"event"`
	PID   int    `json:"pid"`
}

type blockedCommand struct {
	conn net.Conn
	pid  int
	kind string
	err  error
}

func awaitEvent(t *testing.T, ch <-chan blockedCommand) blockedCommand {
	t.Helper()
	select {
	case b := <-ch:
		return b
	case <-time.After(30 * time.Second):
		t.Fatal("bounded event wait expired")
		return blockedCommand{}
	}
}

// TestSourceCommandCancellation re-homes the retired daemon's
// source_cancellation proof onto the bare session host (plan review B5): a
// source's real child process is cancelled with the host. Direct children,
// same-group descendants and parent-exits descendants die with the host; a
// descendant that escaped the process group (setsid-style Setpgid) is outside
// the host's reach, exactly as at base, so the escaped shape asserts it is
// still alive and kills it from the test. The host is started as bare
// `omosense` with its cwd in the project folder, so config and state resolve
// from the folder alone.
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
		{"google", "zele", "watch-google", "direct"},
		{"herdr", "herdr", "watch-herdr", "direct"},
		{"tidy", "git", "memory-tidy", "direct"},
		{"google", "zele", "watch-google", "descendant"},
		{"herdr", "herdr", "watch-herdr", "descendant"},
		{"tidy", "git", "memory-tidy", "descendant"},
		{"google", "zele", "watch-google", "escaped"},
		{"herdr", "herdr", "watch-herdr", "escaped"},
		{"tidy", "git", "memory-tidy", "escaped"},
		{"google", "zele", "watch-google", "parent-exits"},
		{"herdr", "herdr", "watch-herdr", "parent-exits"},
		{"tidy", "git", "memory-tidy", "parent-exits"},
	} {
		t.Run(tc.source+"/"+tc.mode, func(t *testing.T) {
			root := t.TempDir()
			project := filepath.Join(root, "proj")
			dir := filepath.Join(project, ".omosense")
			state := filepath.Join(dir, "state")
			bindir := filepath.Join(root, "bin")
			agents := filepath.Join(root, "agents")
			for _, d := range []string{dir, state, bindir, filepath.Join(agents, "fixture", "repo", ".git")} {
				if err := os.MkdirAll(d, 0o700); err != nil {
					t.Fatal(err)
				}
			}
			config := `{"calendars":null,"mail":false,"tidy":{"enabled":true}}`
			if err := os.WriteFile(filepath.Join(dir, "config.json"), []byte(config), 0o600); err != nil {
				t.Fatal(err)
			}
			script := fmt.Sprintf("#!/bin/sh\nexec %q -test.run '^TestBlockedSourceCommand$'\n", helper)
			if err := os.WriteFile(filepath.Join(bindir, tc.command), []byte(script), 0o700); err != nil {
				t.Fatal(err)
			}
			// A unix socket path is capped near 104 bytes, so the gate lives
			// in its own short /tmp dir rather than under the long TempDir.
			sockDir, err := os.MkdirTemp("/tmp", "os-gate-")
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if err := os.RemoveAll(sockDir); err != nil {
					t.Error(err)
				}
			})
			gate := filepath.Join(sockDir, "command.sock")
			ln, err := net.Listen("unix", gate)
			if err != nil {
				t.Fatal(err)
			}
			defer ln.Close()
			count := 1
			if tc.mode != "direct" {
				count = 2
			}
			blocked := make(chan blockedCommand, 8)
			go func() {
				for {
					conn, err := ln.Accept()
					if err != nil {
						return
					}
					var e processEvent
					err = json.NewDecoder(conn).Decode(&e)
					blocked <- blockedCommand{conn: conn, pid: e.PID, kind: e.Event, err: err}
				}
			}()
			env := append(sandboxEnv(t, root),
				"PATH="+bindir+":"+os.Getenv("PATH"), "OMO_MEMORY_AGENTS="+agents,
				"OS_CANCEL_HELPER=1", "OS_CANCEL_GATE="+gate,
				"OS_CANCEL_MODE="+tc.mode, "OS_CANCEL_CHILD=")
			a := exec.Command(bin)
			a.Dir = project
			a.Env = env
			stdout, err := a.StdoutPipe()
			if err != nil {
				t.Fatal(err)
			}
			a.Stderr = os.Stderr
			t.Logf("RUN %s (bare host, cwd %s; socket-gated fake %s)", bin, project, tc.command)
			if err := a.Start(); err != nil {
				t.Fatal(err)
			}
			lines := make(chan string, 16)
			done := make(chan error, 1)
			go func() {
				scan := bufio.NewScanner(stdout)
				for scan.Scan() {
					line := scan.Text()
					t.Log("STDOUT " + line)
					lines <- line
				}
				done <- a.Wait()
			}()
			t.Cleanup(func() {
				_ = a.Process.Kill()
				select {
				case <-done:
				case <-time.After(15 * time.Second):
					t.Error("host cleanup did not join")
				}
			})
			var processes []blockedCommand
			t.Cleanup(func() {
				for _, b := range processes {
					_ = syscall.Kill(b.pid, syscall.SIGKILL)
					_ = b.conn.Close()
				}
			})
			for range count {
				b := awaitEvent(t, blocked)
				if b.err != nil {
					t.Fatal(b.err)
				}
				processes = append(processes, b)
				t.Logf("BLOCKED %s %s pid=%d; gate will not be released", tc.command, b.kind, b.pid)
			}
			startup := awaitLine(t, lines)
			if !strings.Contains(startup, "sources=") || !strings.Contains(startup, tc.source) {
				t.Fatalf("startup LOG %q does not name the %s source", startup, tc.source)
			}
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
			if err := a.Process.Signal(syscall.SIGTERM); err != nil {
				t.Fatal(err)
			}
			if err := awaitExit(t, done); err != nil {
				t.Fatalf("host must exit zero on SIGTERM: %v", err)
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
			locks, err := filepath.Glob(filepath.Join(state, "*.lock.json"))
			if err != nil {
				t.Fatal(err)
			}
			if len(locks) != 0 {
				t.Fatalf("stop left lock files behind: %v", locks)
			}
			socks, err := filepath.Glob(filepath.Join(project, "**", "*.sock"))
			if err != nil {
				t.Fatal(err)
			}
			if len(socks) != 0 {
				t.Fatalf("bare host created a socket: %v", socks)
			}
			t.Log("PASS: bare host SIGTERM exit 0; parent/descendant dead; no lock files, no socket")
			t.Log("cleanup: host joined; fake processes dead; gate listener closed; fixture directories removed by testing")
		})
	}
}

// sandboxEnv is the process environment with every OMOSENSE_/OMOMEOW_
// variable removed, so the host resolves config and state from its cwd
// alone (and a stale OMOMEOW_* in the harness cannot fail it).
func sandboxEnv(t *testing.T, home string) []string {
	t.Helper()
	var env []string
	for _, e := range os.Environ() {
		if strings.HasPrefix(e, "OMOSENSE_") || strings.HasPrefix(e, "OMOMEOW_") {
			continue
		}
		env = append(env, e)
	}
	return append(env, "HOME="+home)
}

func awaitLine(t *testing.T, lines <-chan string) string {
	t.Helper()
	select {
	case line := <-lines:
		return line
	case <-time.After(30 * time.Second):
		t.Fatal("host printed no startup line")
		return ""
	}
}

func awaitExit(t *testing.T, done <-chan error) error {
	t.Helper()
	select {
	case err := <-done:
		return err
	case <-time.After(30 * time.Second):
		t.Fatal("host did not exit on SIGTERM")
		return nil
	}
}
