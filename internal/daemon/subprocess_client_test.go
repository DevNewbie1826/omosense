package daemon

import (
	"bufio"
	"bytes"
	"encoding/json"
	"net"
	"os"
	"os/exec"
	"strings"
	"time"
)

type attachProcess struct {
	cmd   *exec.Cmd
	lines chan string
	done  chan error
}

func (f *processFixture) attach(version, profile string) *attachProcess {
	f.t.Helper()
	a := &attachProcess{lines: make(chan string, 1024), done: make(chan error, 1)}
	a.cmd = exec.Command(f.bin, "attach", "herdr", "--profile", profile)
	a.cmd.Env = append(f.env, "OMOSENSE_TEST_VERSION="+version)
	stdout, err := a.cmd.StdoutPipe()
	if err != nil {
		f.t.Fatal(err)
	}
	var stderr bytes.Buffer
	a.cmd.Stderr = &stderr
	f.t.Logf("RUN %s attach herdr --profile %s (version %s)", f.bin, profile, version)
	if err := a.cmd.Start(); err != nil {
		f.t.Fatal(err)
	}
	readDone := make(chan struct{})
	go func() {
		defer close(readDone)
		scan := bufio.NewScanner(stdout)
		for scan.Scan() {
			a.lines <- scan.Text()
		}
		close(a.lines)
	}()
	go func() { <-readDone; a.done <- a.cmd.Wait() }()
	f.t.Cleanup(func() {
		select {
		case err := <-a.done:
			f.t.Logf("cleanup: attach %d reaped (%v), stderr=%s", a.cmd.Process.Pid, err, stderr.String())
		default:
			a.cmd.Process.Kill()
			err := await(f.t, a.done)
			f.t.Logf("cleanup: attach %d killed/reaped (%v), stderr=%s", a.cmd.Process.Pid, err, stderr.String())
		}
		if strings.Contains(stderr.String(), "WARNING: DATA RACE") {
			f.t.Errorf("subprocess attach race:\n%s", stderr.String())
		}
	})
	return a
}

func (f *processFixture) streamPID(a *attachProcess, previous int) int {
	f.t.Helper()
	for {
		var line string
		select {
		case value, ok := <-a.lines:
			if !ok {
				f.t.Fatal("attach closed without expected source output")
			}
			line = value
		case <-time.After(15 * time.Second):
			f.t.Fatal("bounded source-output await timed out")
		}
		f.t.Log("STDOUT " + line)
		if strings.HasPrefix(line, "HERDR ") {
			var payload struct {
				PID int `json:"pid"`
			}
			if err := json.Unmarshal([]byte(strings.TrimPrefix(line, "HERDR ")), &payload); err != nil {
				f.t.Fatal(err)
			}
			if payload.PID != previous {
				return payload.PID
			}
		}
	}
}

func (f *processFixture) stop(clients ...*attachProcess) {
	f.t.Helper()
	out, code := f.run("daemon", "status")
	var st status
	if err := json.Unmarshal([]byte(out), &st); err != nil || code != 0 {
		f.t.Fatalf("status before stop: %v, exit=%d", err, code)
	}
	pid := st.PID
	count := f.spawnCount()
	if _, code := f.run("daemon", "stop"); code != 0 {
		f.t.Fatalf("stop exit=%d", code)
	}
	for _, a := range clients {
		err := await(f.t, a.done)
		if err != nil {
			f.t.Fatalf("intentional stop: %v", err)
		}
		a.done <- err // Leave the terminal result for paired cleanup.
	}
	f.waitEvent("exit", pid)
	if got := f.spawnCount(); got != count {
		f.t.Fatalf("stop caused respawn: %d -> %d", count, got)
	}
	if conn, err := net.Dial("unix", f.p.socket); err == nil {
		conn.Close()
		f.t.Fatal("stop left a live daemon")
	}
	for _, path := range []string{f.p.socket, f.p.pid} {
		if _, err := os.Stat(path); !os.IsNotExist(err) {
			f.t.Fatalf("stop left %s: %v", path, err)
		}
	}
	f.t.Log("cleanup: stop exits clients 0; socket/pid absent; no respawn")
}
