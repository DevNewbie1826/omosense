package rpc

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/DevNewbie1826/omosense/internal/core"
)

// Exercise the actual compat entrypoint in a separate process, including Load,
// positional parsing and its process-global watch lock/signal handlers.
func TestRPCProcess(t *testing.T) {
	if os.Getenv("OMOSENSE_RPC_TEST_PROCESS") != "1" {
		return
	}
	var args []string
	for i, arg := range os.Args {
		if arg == "--" {
			args = os.Args[i+1:]
			break
		}
	}
	pa := core.ParseArgs(args)
	c, err := core.Load(pa, !pa.Flags["--once"])
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	if os.Getenv("OMOSENSE_RPC_TEST_SOURCE") == "1" {
		ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGTERM)
		defer cancel()
		if err := Sources(c)[0].Run(ctx, c.Out); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		os.Exit(0)
	}
	os.Exit(Run(c, args))
}

// runBound bounds every wait on the child process; it never paces the test.
const runBound = 30 * time.Second

func awaitRun[T any](t *testing.T, ch <-chan T) T {
	t.Helper()
	select {
	case value := <-ch:
		return value
	case <-time.After(runBound):
		t.Fatal("timed out awaiting the rpc process")
		var zero T
		return zero
	}
}

func rpcProcess(t *testing.T, args ...string) *exec.Cmd {
	t.Helper()
	bin, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(bin, append([]string{"-test.run=^TestRPCProcess$", "--"}, args...)...)
	cmd.Env = append(os.Environ(), "OMOSENSE_RPC_TEST_PROCESS=1")
	return cmd
}

func integrationFixture(t *testing.T) (string, *fakeDelivery) {
	t.Helper()
	f := fakeDeliveryCLI(t)
	dir := t.TempDir()
	stateDir := filepath.Join(dir, "state")
	if err := os.Mkdir(stateDir, 0o700); err != nil {
		t.Fatal(err)
	}
	deliveryWrite(t, filepath.Join(dir, "config.json"), `{"profiles":{"main":{},"family":{}}}`)
	deliveryWrite(t, filepath.Join(stateDir, "sessions.json"), `{"main":{"session_id":"MAINQA"},"family":{"session_id":"FAMILYQA"}}`)
	t.Setenv("HOME", dir)
	t.Setenv("OMOSENSE_DIR", dir)
	t.Setenv("OMOSENSE_STATE", stateDir)
	t.Setenv("OMOSENSE_RPC_SOCK", filepath.Join(dir, "absent.sock"))
	t.Setenv("OMOSENSE_SOCK", filepath.Join(dir, "absent-daemon.sock"))
	return stateDir, f
}

func runCommand(t *testing.T, args ...string) (int, string, string) {
	t.Helper()
	cmd := rpcProcess(t, args...)
	var out, stderr bytes.Buffer
	cmd.Stdout, cmd.Stderr = &out, &stderr
	err := cmd.Run()
	code := 0
	if err != nil {
		if e, ok := err.(*exec.ExitError); ok {
			code = e.ExitCode()
		} else {
			t.Fatal(err)
		}
	}
	t.Logf("Run %q: exit %d; stdout %s; stderr %s", args, code, out.String(), stderr.String())
	return code, out.String(), stderr.String()
}

func requireCommand(t *testing.T, want string, args ...string) {
	t.Helper()
	code, out, stderr := runCommand(t, args...)
	if code != 0 || out != want || stderr != "" {
		t.Fatalf("command %q: exit=%d stdout=%q stderr=%q; want %q", args, code, out, stderr, want)
	}
}

// The observer is subscribed before starting the process. A delivered log is
// emitted after the real shell exits and its result is committed to the store.
func runCompletion(t *testing.T, profile string, source bool) (string, *fakeDelivery, []string) {
	t.Helper()
	stateDir, f := integrationFixture(t)
	s := serveLiveStream(t, false)
	cmd := rpcProcess(t, "--all", "--profile", profile)
	if source {
		cmd.Env = append(cmd.Env, "OMOSENSE_RPC_TEST_SOURCE=1")
	}
	out, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	lines := make(chan string, 1024)
	scanned := make(chan struct{})
	var transcript []string
	go func() {
		defer close(scanned)
		scan := bufio.NewScanner(out)
		for scan.Scan() {
			line := scan.Text()
			transcript = append(transcript, line)
			lines <- line
		}
	}()
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = cmd.Process.Kill()
		// Wait is done below on success; on a failing guard it reaps the child.
		_ = cmd.Wait()
		select {
		case <-scanned:
		case <-time.After(runBound):
			t.Error("stdout observer did not stop")
		}
		t.Logf("Run transcript:\n%s\nstderr: %s", strings.Join(transcript, "\n"), stderr.String())
		t.Log("cleanup: child reaped, stdout observer joined, fixture environment restored; server cleanup joins handlers and removes socket/state dirs")
	})
	conn := awaitRun(t, s.stream)
	req := awaitRun(t, s.requests)
	if req.ID != "s0" {
		t.Fatalf("first stream request = %+v", req)
	}
	write := func(v any) {
		t.Helper()
		if err := json.NewEncoder(conn).Encode(v); err != nil {
			t.Fatal(err)
		}
	}
	rows := map[string]any{"sessions": []map[string]any{session("H", "A")}}
	if err := writeStreamResponse(conn, req.ID, rows); err != nil {
		t.Fatal(err)
	}
	write(map[string]any{"type": "agent_start", "sessionId": "H"})
	req = awaitRun(t, s.requests)
	if err := writeStreamResponse(conn, req.ID, rows); err != nil {
		t.Fatal(err)
	}
	write(map[string]any{"type": "agent_settled", "sessionId": "H"})
	timer := time.NewTimer(runBound)
	defer timer.Stop()
delivered:
	for {
		select {
		case line := <-lines:
			if line == "LOG rpc delivered A seq 1 attempt 1" {
				break delivered
			}
		case <-scanned:
			t.Fatal("watcher exited before delivery")
		case <-timer.C:
			t.Fatal("completion was not delivered")
		}
	}
	if err := cmd.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	// Wait closes the pipe, so read stdout to EOF first: no line may be lost.
	awaitRun(t, scanned)
	if err := cmd.Wait(); err != nil {
		t.Fatal(err)
	}
	doneCount := 0
	for _, line := range transcript {
		if strings.HasPrefix(line, "RPC ") {
			var ev rpcEvent
			if err := json.Unmarshal([]byte(strings.TrimPrefix(line, "RPC ")), &ev); err != nil {
				t.Fatal(err)
			}
			if ev.Event == "done" && ev.ID == "A" {
				doneCount++
			}
		}
	}
	entries := pendingList(t, newPendingStore(stateDir, profile))
	calls := f.calls(t)
	if doneCount != 1 || len(entries) != 1 || entries[0].Attempts != 1 || len(calls) != 1 {
		t.Fatalf("done=%d entries=%+v calls=%q", doneCount, entries, calls)
	}
	call := calls[0]
	target := "MAINQA"
	if profile == "family" {
		target = "FAMILYQA"
	}
	if len(call) != 8 || call[0] != "thread" || call[1] != "send" || call[2] != target ||
		call[4] != "--all-scope" || call[5] != "--idempotency-key" ||
		call[6] != "omosense-rpc-"+profile+"-A-1-1" || call[7] != "--json" {
		t.Fatalf("send argv = %q", call)
	}
	if _, err := os.Stat(filepath.Join(stateDir, "watch-rpc-"+profile+".lock.json")); !os.IsNotExist(err) {
		t.Fatalf("watch lock was not released: %v", err)
	}
	return stateDir, f, call
}

func TestRunCompletionDelivery(t *testing.T) {
	// Given a real Run process and wire-level idle snapshots, When a short turn
	// settles, Then it is pushed once and an offline ACK closes the pending loop.
	for _, source := range []bool{false, true} {
		t.Run(fmt.Sprintf("daemon_%t", source), func(t *testing.T) {
			stateDir, _, _ := runCompletion(t, "main", source)
			entries := pendingList(t, newPendingStore(stateDir, "main"))
			code, out, _ := runCommand(t, "pending")
			if code != 0 || strings.Count(out, "PENDING ") != 1 {
				t.Fatalf("pending exit=%d stdout=%q", code, out)
			}
			requireCommand(t, fmt.Sprintf("ACK {\"id\":\"A\",\"seq\":%d,\"result\":\"acked\"}\n", entries[0].Seq), "ack", "A", "1")
			requireCommand(t, "", "pending")
		})
	}
}

func TestRunFamilyAck(t *testing.T) {
	// Given a family delivery, When its machine-consumed command is run, Then
	// family is cleared without creating or changing main's pending file.
	stateDir, _, call := runCompletion(t, "family", false)
	mainPath := filepath.Join(stateDir, "rpc-pending-main.json")
	if _, err := os.Stat(mainPath); !os.IsNotExist(err) {
		t.Fatalf("family watch touched main pending: %v", err)
	}
	mainStore := newPendingStore(stateDir, "main")
	pendingRecord(t, mainStore, "A", "main-handle")
	before, err := os.ReadFile(mainPath)
	if err != nil {
		t.Fatal(err)
	}
	start := strings.Index(call[3], "omosense rpc ack ")
	if start < 0 {
		t.Fatal("delivery lacks machine-consumed ACK command")
	}
	command := strings.SplitN(call[3][start:], "\n", 2)[0]
	args := strings.Fields(command)[2:]
	requireCommand(t, "ACK {\"id\":\"A\",\"seq\":1,\"result\":\"acked\"}\n", args...)
	requireCommand(t, "", "pending", "--profile", "family")
	after, err := os.ReadFile(mainPath)
	if err != nil || !bytes.Equal(before, after) {
		t.Fatalf("family ACK modified main: %s -> %s, %v", before, after, err)
	}
}

func TestRunMalformedAck(t *testing.T) {
	stateDir, _ := integrationFixture(t)
	store := newPendingStore(stateDir, "main")
	pendingRecord(t, store, "A", "H")
	before, err := os.ReadFile(store.path)
	if err != nil {
		t.Fatal(err)
	}
	for _, args := range [][]string{
		{"ack"}, {"ack", ""}, {"ack", "A", "0"}, {"ack", "A", "-1"},
		{"ack", "A", "1.5"}, {"ack", "A", "x"}, {"ack", "A", "0x1"}, {"ack", "A", "+1"},
		{"ack", "A", "18446744073709551616"}, {"ack", "A", "1", "extra"}, {"pending", "extra"},
	} {
		t.Run(strings.Join(args, "_"), func(t *testing.T) {
			code, out, stderr := runCommand(t, args...)
			after, err := os.ReadFile(store.path)
			if code != 2 || out != "" || !strings.HasPrefix(stderr, "Usage:") || err != nil || !bytes.Equal(before, after) {
				t.Fatalf("invalid args accepted or changed file: exit=%d out=%q stderr=%q file=%s err=%v", code, out, stderr, after, err)
			}
			if _, err := os.Stat(filepath.Join(stateDir, "watch-rpc-main.lock.json")); !os.IsNotExist(err) {
				t.Fatalf("invalid args took watch lock: %v", err)
			}
		})
	}
}

func TestRunOfflineCommands(t *testing.T) {
	stateDir, _ := integrationFixture(t)
	store := newPendingStore(stateDir, "main")
	pendingRecord(t, store, "Z", "H1")
	pendingRecord(t, store, "A", "H2")
	// A corrupt watch lock and absent sockets must not impede offline commands.
	deliveryWrite(t, filepath.Join(stateDir, "watch-rpc-main.lock.json"), "invalid")
	code, out, _ := runCommand(t, "pending")
	var ids []string
	for _, line := range strings.Split(strings.TrimSpace(out), "\n") {
		var e pendingEntry
		if err := json.Unmarshal([]byte(strings.TrimPrefix(line, "PENDING ")), &e); err != nil {
			t.Fatal(err)
		}
		ids = append(ids, e.ID)
	}
	if code != 0 || strings.Join(ids, ",") != "Z,A" {
		t.Fatalf("pending not seq ordered: exit=%d ids=%v", code, ids)
	}
	requireCommand(t, "ACK {\"id\":\"A\",\"seq\":1,\"result\":\"newer\"}\n", "ack", "A", "1")
	requireCommand(t, "ACK {\"id\":\"A\",\"seq\":2,\"result\":\"acked\"}\n", "ack", "A", "2")
	requireCommand(t, "ACK {\"id\":\"A\",\"seq\":null,\"result\":\"not_pending\"}\n", "ack", "A")
	requireCommand(t, "ACK {\"id\":\"Z\",\"seq\":null,\"result\":\"acked\"}\n", "ack", "Z")
	requireCommand(t, "", "pending")
	deliveryWrite(t, store.path, "corrupt")
	for _, args := range [][]string{{"pending"}, {"ack", "Z"}} {
		code, out, stderr := runCommand(t, args...)
		if code != 1 || out != "" || stderr == "" {
			t.Fatalf("I/O error exit=%d out=%q stderr=%q", code, out, stderr)
		}
	}
}

func TestRunOnceDoesNotDeliver(t *testing.T) {
	stateDir, f := integrationFixture(t)
	store := newPendingStore(stateDir, "main")
	pendingRecord(t, store, "A", "H")
	before, err := os.ReadFile(store.path)
	if err != nil {
		t.Fatal(err)
	}
	serveLiveStream(t, false)
	code, out, stderr := runCommand(t, "--once", "--all")
	after, err := os.ReadFile(store.path)
	if code != 0 || !strings.HasPrefix(out, "SNAP ") || stderr != "" || err != nil ||
		!bytes.Equal(before, after) || len(f.calls(t)) != 0 {
		t.Fatalf("once side effects: exit=%d stdout=%q stderr=%q file=%s err=%v", code, out, stderr, after, err)
	}
	if _, err := os.Stat(filepath.Join(stateDir, "watch-rpc-main.lock.json")); !os.IsNotExist(err) {
		t.Fatalf("once took lock: %v", err)
	}
}
