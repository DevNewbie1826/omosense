package daemon

import (
	"context"
	"net"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func profileStopCompatDaemon(t *testing.T, features []string, stopReply map[string]any) *atomic.Int32 {
	t.Helper()
	dir, err := os.MkdirTemp("/tmp", "osc-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := os.RemoveAll(dir); err != nil {
			t.Error(err)
		}
		t.Logf("cleanup: removed fake daemon directory %s", dir)
	})
	socket := filepath.Join(dir, "d.sock")
	t.Setenv("OMOSENSE_SOCK", socket)
	t.Setenv("OMOMEOW_DIR", dir)
	listener, err := net.Listen("unix", socket)
	if err != nil {
		t.Fatal(err)
	}
	stops := &atomic.Int32{}
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			conn.SetDeadline(time.Now().Add(10 * time.Second))
			var cmd command
			if err := newFramer(conn).read(&cmd); err != nil {
				t.Errorf("fake daemon command: %v", err)
				conn.Close()
				continue
			}
			t.Logf("fake daemon received: %+v", cmd)
			switch cmd.Cmd {
			case "status":
				// origin/main status has no features; omit the field entirely.
				st := map[string]any{"pid": os.Getpid(), "version": "old"}
				if features != nil {
					st["features"] = features
				}
				err = writeFrame(conn, st)
			case "stop":
				// origin/main ignores profile and stops the entire daemon.
				stops.Add(1)
				err = writeFrame(conn, stopReply)
			default:
				t.Errorf("unexpected command: %+v", cmd)
			}
			if err != nil {
				t.Errorf("fake daemon reply: %v", err)
			}
			conn.Close()
		}
	}()
	t.Cleanup(func() {
		listener.Close()
		await(t, done)
		t.Log("cleanup: fake listener closed and server goroutine joined")
	})
	return stops
}

func runProfileStopCompatCLI(t *testing.T) (int, string, string) {
	t.Helper()
	out, err := os.CreateTemp(t.TempDir(), "stdout")
	if err != nil {
		t.Fatal(err)
	}
	defer out.Close()
	stderr, err := os.CreateTemp(t.TempDir(), "stderr")
	if err != nil {
		t.Fatal(err)
	}
	defer stderr.Close()
	originalOut, originalErr := os.Stdout, os.Stderr
	os.Stdout, os.Stderr = out, stderr
	defer func() { os.Stdout, os.Stderr = originalOut, originalErr }()
	code := RunDaemon([]string{"stop", "--profile", "family"})
	read := func(f *os.File) string {
		b, err := os.ReadFile(f.Name())
		if err != nil {
			t.Fatal(err)
		}
		return string(b)
	}
	return code, read(out), read(stderr)
}

func TestProfileStopCompatRefusesOldDaemon(t *testing.T) {
	// Given: an old resident daemon speaking the origin/main control protocol.
	stops := profileStopCompatDaemon(t, nil, map[string]any{"ok": true, "version": "old"})

	// When: the actual CLI entry point requests FAMILY stop.
	code, out, stderr := runProfileStopCompatCLI(t)

	// Then: no destructive command reached the old daemon.
	t.Logf("CLI exit=%d stdout=%q stderr=%q stop commands=%d", code, out, stderr, stops.Load())
	want := "omosense daemon (version old) does not support stop --profile; upgrade the running daemon first (attach any source with the new binary, which negotiates the upgrade), then retry\n"
	if code != 1 || stderr != want || out != "" || stops.Load() != 0 {
		t.Fatalf("old daemon not safely refused: exit=%d stdout=%q stderr=%q stops=%d", code, out, stderr, stops.Load())
	}
}

func TestProfileStopCompatRequiresMatchingEcho(t *testing.T) {
	for _, profile := range []string{"", "main"} {
		t.Run("echo_"+profile, func(t *testing.T) {
			// Given: support is advertised but the reply omits or mismatches the profile.
			r := map[string]any{"ok": true, "version": "old"}
			if profile != "" {
				r["profile"] = profile
			}
			stops := profileStopCompatDaemon(t, []string{"profile-stop"}, r)

			// When: the actual CLI entry point requests FAMILY stop.
			code, out, stderr := runProfileStopCompatCLI(t)

			// Then: a generic success cannot be accepted as profile-stop success.
			t.Logf("CLI exit=%d stdout=%q stderr=%q stop commands=%d", code, out, stderr, stops.Load())
			if code != 1 || out != "" || !strings.Contains(stderr, "profile") || stops.Load() != 1 {
				t.Fatalf("invalid echo accepted: exit=%d stdout=%q stderr=%q stops=%d", code, out, stderr, stops.Load())
			}
		})
	}
}

func TestProfileStopCompatCurrentDaemon(t *testing.T) {
	// Given: a current daemon with active MAIN workers.
	s := serverFixture(t, fakeRegistry)
	_, main, r := wireClient(t, s, hello{Profile: "main", Sources: []string{"google"}})
	if !r.OK {
		t.Fatal(r.Error)
	}
	readWire(t, main)
	readWire(t, main)
	before := requestStatus(t, s)
	if !slices.Contains(before.Features, "profile-stop") {
		t.Fatal("current daemon status does not advertise profile-stop")
	}

	// When: the real profile-stop client negotiates and stops FAMILY.
	result, err := daemonProfileStop(context.Background(), s.paths, "family")

	// Then: the matching reply succeeds and MAIN and the daemon remain live.
	if err != nil || result.Profile != "family" || result.Daemon != "running" {
		t.Fatalf("current daemon profile stop: %+v %v", result, err)
	}
	after := requestStatus(t, s)
	if after.PID != before.PID || len(after.Clients) != 1 || after.Clients[0].Profile != "main" {
		t.Fatalf("MAIN or daemon changed: %+v", after)
	}
	t.Logf("current daemon status features=%v result=%+v MAIN preserved", before.Features, result)
}
