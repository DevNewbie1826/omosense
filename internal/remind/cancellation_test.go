package remind

import (
	"encoding/json"
	"net"
	"os"
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
	_ = json.NewEncoder(conn).Encode(os.Getpid())
	var b [1]byte
	_, _ = conn.Read(b[:])
	os.Exit(0)
}

func TestCancelledSend(t *testing.T) {
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
	started := make(chan child, 1)
	go func() {
		conn, err := ln.Accept()
		if err != nil {
			started <- child{err: err}
			return
		}
		var pid int
		err = json.NewDecoder(conn).Decode(&pid)
		started <- child{conn, pid, err}
	}()
	sink := newChanSink()
	cancel, done := runSource(t, c, sink)
	defer cancel()
	var b child
	select {
	case b = <-started:
	case <-time.After(10 * time.Second):
		t.Fatal("fake say did not announce startup")
	}
	if b.err != nil {
		t.Fatal(b.err)
	}
	defer b.conn.Close()
	// Failure cleanup kills the blocked command so no scheduler is left.
	joined := false
	defer func() {
		_ = syscall.Kill(b.pid, syscall.SIGKILL)
		if !joined {
			// Unblock any unexpected second invocation only during failure cleanup.
			ln.Close()
			_ = waitDone(t, done)
		}
		t.Log("cleanup: source joined, fake say killed/reaped, listener closed, temporary gate removed")
	}()
	t.Logf("RUN real remind source with blocked fake say pid=%d; cancel source context", b.pid)
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
	if err := syscall.Kill(b.pid, 0); err == nil {
		t.Fatal("cancelled say remains alive")
	}
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
