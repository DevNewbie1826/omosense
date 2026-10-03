package daemon

import (
	"context"
	"encoding/json"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"
)

func TestSubprocessLifecycle(t *testing.T) {
	bin := filepath.Join(t.TempDir(), "omosense")
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	cmd := exec.CommandContext(ctx, "go", "build", "-race", "-o", bin, "./cmd/omosense")
	cmd.Dir = filepath.Join("..", "..")
	t.Logf("RUN go build -race -o %s ./cmd/omosense", bin)
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("build: %v\n%s", err, out)
	}
	t.Run("readiness_flock_five_clients_singleton_status_stop", func(t *testing.T) {
		f := subprocessFixture(t, bin)
		gate := filepath.Join(t.TempDir(), "ready")
		f.env = append(f.env, "OMOSENSE_TEST_READY_GATE="+gate)
		first := f.attach("v1", "main")
		pid := f.waitEvent("gated", 0)
		clients := []*attachProcess{first}
		for i := 0; i < 4; i++ {
			a := f.attach("v1", "main")
			clients = append(clients, a)
			f.waitEvent("spawn-wait", a.cmd.Process.Pid)
		}
		if count := f.spawnCount(); count != 1 {
			t.Fatalf("gate-closed spawns=%d, want 1", count)
		}
		if err := os.WriteFile(gate, nil, 0o600); err != nil {
			t.Fatal(err)
		}
		for _, a := range clients {
			if got := f.streamPID(a, 0); got != pid {
				t.Fatalf("multiple daemons: %d != %d", got, pid)
			}
		}
		if count := f.spawnCount(); count != 1 {
			t.Fatalf("ready spawns=%d, want 1", count)
		}
		f.waitEvent("source:main:discord", pid)
		f.waitEvent("source:main:remind", pid)
		out, code := f.run("daemon", "status")
		var st status
		if err := json.Unmarshal([]byte(out), &st); err != nil || code != 0 || st.PID != pid || len(st.Clients) != 5 {
			t.Fatalf("status: %+v, %v, exit=%d", st, err, code)
		}
		for _, src := range st.Sources {
			want := "paused"
			if src.Name == "remind" || src.Name == "discord" || src.Profile == "main" && src.Name == "herdr" {
				want = "running"
			}
			if src.State != want {
				t.Fatalf("source interest: %+v want %s", src, want)
			}
		}
		if _, code := f.run("daemon"); code != 3 {
			t.Fatalf("second singleton exit=%d", code)
		}
		f.stop(clients...)
	})
	t.Run("unexpected_kill_respawns_and_streams", func(t *testing.T) {
		f := subprocessFixture(t, bin)
		a := f.attach("v1", "main")
		old := f.streamPID(a, 0)
		if err := syscall.Kill(old, syscall.SIGKILL); err != nil {
			t.Fatal(err)
		}
		f.waitEvent("exit", old)
		next := f.streamPID(a, old)
		if next == old {
			t.Fatal("attach did not respawn")
		}
		if count := f.spawnCount(); count != 2 {
			t.Fatalf("kill spawns=%d, want 2", count)
		}
		f.stop(a)
	})
	t.Run("upgrade_reconnects_old_peers_without_downgrade", func(t *testing.T) {
		f := subprocessFixture(t, bin)
		a := f.attach("v1", "main")
		old := f.streamPID(a, 0)
		b := f.attach("v2", "main")
		next := f.streamPID(b, old)
		if got := f.streamPID(a, old); got != next {
			t.Fatalf("peer reconnected to %d, want %d", got, next)
		}
		f.waitEvent("exit", old)
		if count := f.spawnCount(); count != 2 {
			t.Fatalf("upgrade spawns=%d, want 2", count)
		}
		f.stop(a, b)
	})
	t.Run("unknown_profile_exits_two", func(t *testing.T) {
		f := subprocessFixture(t, bin)
		if out, code := f.run("attach", "herdr", "--profile", "nope"); code != 2 || !strings.Contains(out, "unknown profile nope") {
			t.Fatalf("rejection: %d %q", code, out)
		}
		f.stop()
	})
	t.Run("always_on_journal_replays_old_pid_after_restart_once", func(t *testing.T) {
		f := subprocessFixture(t, bin)
		a := f.attach("v1", "main")
		old := f.streamPID(a, 0)
		f.waitEvent("source:main:remind", old) // Its initial line is durably pending.
		if err := syscall.Kill(old, syscall.SIGKILL); err != nil {
			t.Fatal(err)
		}
		f.waitEvent("exit", old)
		next := f.streamPID(a, old)
		f.waitEvent("source:main:remind", next)
		_, reader, r := wireClient(t, &server{paths: f.p},
			hello{Version: "v1", Profile: "main", Sources: []string{"remind"}, Only: []string{"REMIND"}})
		if !r.OK {
			t.Fatal(r.Error)
		}
		for _, want := range []int{old, next} {
			line := readWire(t, reader).Line
			t.Log("REPLAY " + line)
			var payload struct {
				PID int `json:"pid"`
			}
			if err := json.Unmarshal([]byte(strings.TrimPrefix(line, "REMIND ")), &payload); err != nil || payload.PID != want {
				t.Fatalf("journal restart/order: %q want pid %d (%v)", line, want, err)
			}
		}
		_, second, _ := wireClient(t, &server{paths: f.p},
			hello{Version: "v1", Profile: "main", Sources: []string{"remind"}, Only: []string{"REMIND"}})
		line := readWire(t, second).Line
		var payload struct {
			PID int `json:"pid"`
		}
		if err := json.Unmarshal([]byte(strings.TrimPrefix(line, "REMIND ")), &payload); err != nil || payload.PID != next {
			t.Fatalf("second attach repeated old pending entry: %q (%v)", line, err)
		}
		f.stop(a)
	})
}

func TestSubprocessEventHookPreservesProcessIdentity(t *testing.T) {
	p := socketPaths(t)
	path := filepath.Join(p.dir, "events.sock")
	ln, err := net.Listen("unix", path)
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	t.Setenv("OMOSENSE_TEST_REGISTRY", "fake")
	t.Setenv("OMOSENSE_TEST_EVENTS", path)
	got := make(chan processEvent, 1)
	go func() {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		var e processEvent
		if newFramer(conn).read(&e) == nil {
			got <- e
		}
	}()
	if err := testNotify("proof"); err != nil {
		t.Fatal(err)
	}
	defer func() {
		testEventConn.Lock()
		defer testEventConn.Unlock()
		testEventConn.conn.Close()
		testEventConn.conn = nil
	}()
	if e := await(t, got); e.Event != "proof" || e.PID != os.Getpid() {
		t.Fatal(e)
	}
}
