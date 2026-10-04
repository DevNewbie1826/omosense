package remind

import (
	"encoding/json"
	"errors"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"
)

func TestBlockedSayCommand(t *testing.T) {
	if os.Getenv("OS_SAY_HELPER") != "1" {
		return
	}
	conn, err := net.Dial("unix", os.Getenv("OS_SAY_GATE"))
	if err != nil {
		os.Exit(2)
	}
	defer conn.Close()
	if os.Getenv("OS_SAY_DESCENDANT") == "1" && os.Getenv("OS_SAY_CHILD") != "1" {
		child := exec.Command(os.Args[0], "-test.run=^TestBlockedSayCommand$")
		child.Env = append(os.Environ(), "OS_SAY_CHILD=1")
		child.Stdout, child.Stderr = os.Stdout, os.Stderr
		if err := child.Start(); err != nil {
			os.Exit(2)
		}
	}
	_ = json.NewEncoder(conn).Encode(os.Getpid())
	var b [1]byte
	_, _ = conn.Read(b[:])
	os.Exit(0)
}

func TestCancelledSend(t *testing.T) {
	t.Run("direct", func(t *testing.T) { testCancelledSend(t, false) })
	t.Run("descendant", func(t *testing.T) { testCancelledSend(t, true) })
}

func testCancelledSend(t *testing.T, descendant bool) {
	c := loadCtx(t)
	gatedir, err := os.MkdirTemp("/tmp", "os-say-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(gatedir)
	gate := filepath.Join(gatedir, "gate.sock")
	ln, err := net.Listen("unix", gate)
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	t.Setenv("OS_SAY_HELPER", "1")
	t.Setenv("OS_SAY_GATE", gate)
	t.Setenv("OS_SAY_CHILD", "")
	t.Setenv("OS_SAY_DESCENDANT", "0")
	count := 1
	if descendant {
		count = 2
		t.Setenv("OS_SAY_DESCENDANT", "1")
	}
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	script := filepath.Join(t.TempDir(), "say")
	if err := os.WriteFile(script, []byte("#!/bin/sh\nexec \""+exe+"\" -test.run '^TestBlockedSayCommand$'\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	withHooks(t, fixedTime, script, sleepCtx)
	data := `[{"id":"first","at":"invalid","platform":"telegram","text":"first"},{"id":"second","at":"invalid","platform":"telegram","text":"second"}]`
	if err := os.WriteFile(remindersFile(c), []byte(data), 0o600); err != nil {
		t.Fatal(err)
	}
	type child struct {
		conn net.Conn
		pid  int
		err  error
	}
	started := make(chan child, count)
	go func() {
		for range count {
			conn, err := ln.Accept()
			if err != nil {
				started <- child{err: err}
				return
			}
			var pid int
			err = json.NewDecoder(conn).Decode(&pid)
			started <- child{conn, pid, err}
		}
	}()
	sink := newChanSink()
	cancel, done := runSource(t, c, sink)
	defer cancel()
	var processes []child
	// Failure cleanup kills the blocked command so no scheduler is left.
	joined := false
	defer func() {
		for _, b := range processes {
			_ = syscall.Kill(b.pid, syscall.SIGKILL)
			_ = b.conn.Close()
		}
		cancel()
		if !joined {
			// Unblock any unexpected second invocation only during failure cleanup.
			ln.Close()
			_ = waitDone(t, done)
		}
		t.Log("cleanup: source joined, fake say killed/reaped, listener closed, temporary gate removed")
	}()
	for range count {
		select {
		case b := <-started:
			if b.err != nil {
				t.Fatal(b.err)
			}
			processes = append(processes, b)
			t.Logf("BLOCKED fake say process pid=%d", b.pid)
		case <-time.After(10 * time.Second):
			t.Fatal("fake say did not announce startup")
		}
	}
	t.Log("RUN real remind source with blocked fake say; cancel source context")
	cancel()
	select {
	case err := <-done:
		joined = true
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("source cancellation did not reap in-flight say")
	}
	for _, b := range processes {
		if err := b.conn.SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil {
			t.Fatal(err)
		}
		var data [1]byte
		if _, err := b.conn.Read(data[:]); !errors.Is(err, io.EOF) {
			t.Fatalf("fake say %d retained gate: %v", b.pid, err)
		}
		if err := syscall.Kill(b.pid, 0); !errors.Is(err, syscall.ESRCH) {
			t.Fatalf("cancelled say %d remains alive: %v", b.pid, err)
		}
		_ = b.conn.Close()
	}
	processes = nil
	if got := string(mustRead(t, remindersFile(c))); got != data {
		t.Fatalf("cancelled send must not mark entries sent/failed or execute later sends: %s", got)
	}
	for _, line := range sink.snapshot() {
		if strings.HasPrefix(line, "REMIND ") {
			t.Fatalf("cancelled send emitted terminal state: %s", line)
		}
	}
	// A queued connection proves that another send started despite cancellation.
	if err := ln.(*net.UnixListener).SetDeadline(time.Now()); err != nil {
		t.Fatal(err)
	}
	if next, err := ln.Accept(); err == nil {
		next.Close()
		t.Fatal("second reminder started after cancellation")
	}
	t.Log("PASS: cancelled send reaped; state unchanged; no terminal event or second send")
}
