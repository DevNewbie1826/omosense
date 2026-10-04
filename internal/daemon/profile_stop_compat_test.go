package daemon

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

type compatDaemon struct {
	wholeStops   atomic.Int32
	profileStops atomic.Int32
	connections  atomic.Int32
	first        chan command
	replace      chan struct{}
}

func profileStopCompatDaemon(t *testing.T, replacement bool, stopReply map[string]any) *compatDaemon {
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
	peer := &compatDaemon{}
	if replacement {
		peer.first = make(chan command, 1)
		peer.replace = make(chan struct{})
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			conn.SetDeadline(time.Now().Add(10 * time.Second))
			first := peer.connections.Add(1) == 1
			var cmd command
			if err := newFramer(conn).read(&cmd); err != nil {
				t.Errorf("fake daemon command: %v", err)
				conn.Close()
				continue
			}
			t.Logf("fake daemon received: %+v", cmd)
			if replacement && first {
				peer.first <- cmd
				// The test releases replacement before the next accept.
				// A first status probe still receives the supported reply.
				select {
				case <-peer.replace:
				case <-time.After(10 * time.Second):
					t.Error("replacement gate timed out")
				}
			}
			switch cmd.Cmd {
			case "status":
				st := map[string]any{"pid": os.Getpid(), "version": "old"}
				if replacement && first {
					st["features"] = []string{"profile-stop"}
				}
				err = writeFrame(conn, st)
			case "stop":
				// origin/main ignores profile and stops the entire daemon.
				peer.wholeStops.Add(1)
				err = writeFrame(conn, reply{OK: true, Version: "old"})
			case "stop-profile":
				if stopReply != nil {
					peer.profileStops.Add(1)
					err = writeFrame(conn, stopReply)
				} else {
					err = writeFrame(conn, reply{Error: "unknown command stop-profile"})
				}
			default:
				err = writeFrame(conn, reply{Error: fmt.Sprintf("unknown command %s", cmd.Cmd)})
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
	return peer
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
	peer := profileStopCompatDaemon(t, false, nil)

	// When: the actual CLI entry point requests FAMILY stop.
	code, out, stderr := runProfileStopCompatCLI(t)

	// Then: no destructive command reached the old daemon.
	assertCompatRefusal(t, peer, code, out, stderr)
	if peer.connections.Load() != 1 {
		t.Fatalf("profile stop used %d connections", peer.connections.Load())
	}
}

func assertCompatRefusal(t *testing.T, peer *compatDaemon, code int, out, stderr string) {
	t.Helper()
	t.Logf("CLI exit=%d stdout=%q stderr=%q WHOLE STOP=%d", code, out, stderr, peer.wholeStops.Load())
	want := "omosense daemon (version unknown) does not support stop --profile; upgrade the running daemon first (attach any source with the new binary, which negotiates the upgrade), then retry\n"
	if code != 1 || stderr != want || out != "" || peer.wholeStops.Load() != 0 {
		t.Fatalf("legacy daemon not safely refused: exit=%d stdout=%q stderr=%q WHOLE STOP=%d", code, out, stderr, peer.wholeStops.Load())
	}
}

func TestProfileStopCompatRefusesReplacement(t *testing.T) {
	peer := profileStopCompatDaemon(t, true, nil)
	type cliResult struct {
		code        int
		out, stderr string
	}
	done := make(chan cliResult, 1)
	go func() {
		code, out, stderr := runProfileStopCompatCLI(t)
		done <- cliResult{code, out, stderr}
	}()
	first := await(t, peer.first)
	t.Logf("replacement gate: first command=%s; later connections use legacy control", first.Cmd)
	close(peer.replace)
	got := await(t, done)
	assertCompatRefusal(t, peer, got.code, got.out, got.stderr)
	if first.Cmd != "stop-profile" || peer.connections.Load() != 1 {
		t.Fatalf("profile stop probed or redialed: first=%s connections=%d", first.Cmd, peer.connections.Load())
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
			peer := profileStopCompatDaemon(t, false, r)

			// When: the actual CLI entry point requests FAMILY stop.
			code, out, stderr := runProfileStopCompatCLI(t)

			// Then: a generic success cannot be accepted as profile-stop success.
			t.Logf("CLI exit=%d stdout=%q stderr=%q profile stops=%d", code, out, stderr, peer.profileStops.Load())
			if code != 1 || out != "" || !strings.Contains(stderr, "reply profile") || peer.profileStops.Load() != 1 || peer.wholeStops.Load() != 0 {
				t.Fatalf("invalid echo accepted: exit=%d stdout=%q stderr=%q profile stops=%d whole stops=%d", code, out, stderr, peer.profileStops.Load(), peer.wholeStops.Load())
			}
		})
	}
}

func TestProfileStopCompatReplyVersion(t *testing.T) {
	peer := profileStopCompatDaemon(t, false, map[string]any{
		"error": "unknown command stop-profile", "version": "old-v1",
	})
	code, out, stderr := runProfileStopCompatCLI(t)
	want := "omosense daemon (version old-v1) does not support stop --profile; upgrade the running daemon first (attach any source with the new binary, which negotiates the upgrade), then retry\n"
	t.Logf("CLI exit=%d stdout=%q stderr=%q WHOLE STOP=%d", code, out, stderr, peer.wholeStops.Load())
	if code != 1 || out != "" || stderr != want || peer.wholeStops.Load() != 0 {
		t.Fatalf("reply version refusal: exit=%d stdout=%q stderr=%q", code, out, stderr)
	}
}

func TestProfileStopCompatDoesNotRequireFeatures(t *testing.T) {
	peer := profileStopCompatDaemon(t, false, map[string]any{
		"ok": true, "profile": "family", "stopped": []string{"google"}, "daemon": "running",
	})
	code, out, stderr := runProfileStopCompatCLI(t)
	var result profileStopResult
	if err := json.Unmarshal([]byte(out), &result); err != nil {
		t.Fatal(err)
	}
	t.Logf("CLI exit=%d stdout=%q stderr=%q connections=%d", code, out, stderr, peer.connections.Load())
	if code != 0 || stderr != "" || result.Profile != "family" || result.Daemon != "running" ||
		!slices.Contains(result.Stopped, "google") || peer.profileStops.Load() != 1 ||
		peer.wholeStops.Load() != 0 || peer.connections.Load() != 1 {
		t.Fatalf("dedicated opcode success: exit=%d stdout=%q stderr=%q", code, out, stderr)
	}
}

func TestProfileStopCompatCurrentDaemon(t *testing.T) {
	// Given: a current daemon with active MAIN workers.
	s := serverFixture(t, fakeRegistry)
	mainConn, main, r := wireClient(t, s, hello{Profile: "main", Sources: []string{"google"}})
	if !r.OK {
		t.Fatal(r.Error)
	}
	readWire(t, main)
	readWire(t, main)
	_, family, r := wireClient(t, s, hello{Profile: "family", Sources: []string{"google"}})
	if !r.OK {
		t.Fatal(r.Error)
	}
	readWire(t, family)
	readWire(t, family)
	before := requestStatus(t, s)
	if !slices.Contains(before.Features, "profile-stop") {
		t.Fatal("current daemon status does not advertise profile-stop")
	}

	// A stop carrying a profile must reject without stopping either profile.
	conn, err := net.Dial("unix", s.paths.socket)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	conn.SetDeadline(time.Now().Add(10 * time.Second))
	if err := writeFrame(conn, command{Cmd: "stop", Profile: "family"}); err != nil {
		t.Fatal(err)
	}
	var rejected reply
	if err := newFramer(conn).read(&rejected); err != nil {
		t.Fatal(err)
	}
	t.Logf("stop with profile reply: %+v", rejected)
	if rejected.OK || rejected.Error == "" {
		t.Fatalf("profiled stop accepted: %+v", rejected)
	}
	if err := writeFrame(mainConn, command{Cmd: "stop", Profile: "family"}); err != nil {
		t.Fatal(err)
	}
	if err := main.read(&rejected); err != nil {
		t.Fatal(err)
	}
	t.Logf("attached stop with profile reply: %+v", rejected)
	if rejected.OK || rejected.Error == "" {
		t.Fatalf("attached profiled stop accepted: %+v", rejected)
	}
	unchanged := requestStatus(t, s)
	if unchanged.PID != before.PID || len(unchanged.Clients) != 2 {
		t.Fatalf("profiled stop changed clients: %+v", unchanged)
	}
	for _, source := range unchanged.Sources {
		if source.State == "stopped" {
			t.Fatalf("profiled stop stopped source: %+v", source)
		}
	}
	if _, err := os.Stat(filepath.Join(s.base.State, "omosense-profile-family.stopped")); !os.IsNotExist(err) {
		t.Fatalf("rejected stop wrote marker: %v", err)
	}

	// When: the real profile-stop client sends the dedicated opcode.
	result, err := daemonProfileStop(context.Background(), s.paths, "family")

	// Then: the matching reply succeeds and MAIN and the daemon remain live.
	if err != nil || result.Profile != "family" || result.Daemon != "running" || !slices.Contains(result.Stopped, "google") {
		t.Fatalf("current daemon profile stop: %+v %v", result, err)
	}
	after := requestStatus(t, s)
	if after.PID != before.PID || len(after.Clients) != 1 || after.Clients[0].Profile != "main" {
		t.Fatalf("MAIN or daemon changed: %+v", after)
	}
	t.Logf("current daemon status features=%v result=%+v MAIN preserved", before.Features, result)
}
