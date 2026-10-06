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
	"regexp"
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
	// Test seam: the child process configures the batcher's quiet window and
	// its poll so the real subprocess batch path runs without a five-minute
	// wait and without depending on the 30s poll granularity.
	if q := os.Getenv("OMOSENSE_RPC_TEST_QUIET"); q != "" {
		d, err := time.ParseDuration(q)
		if err != nil {
			fmt.Fprintln(os.Stderr, "bad OMOSENSE_RPC_TEST_QUIET:", err)
			os.Exit(1)
		}
		batchQuiet = d
		if poll := d / 2; poll > time.Millisecond {
			deliverPoll = poll
		}
	}
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

// rpcFolder is the run folder's base name.
const rpcFolder = "proj"

func integrationFixture(t *testing.T) (string, *fakeDelivery) {
	t.Helper()
	f := fakeDeliveryCLI(t)
	root := t.TempDir()
	dir := filepath.Join(root, rpcFolder)
	stateDir := filepath.Join(dir, "state")
	if err := os.MkdirAll(stateDir, 0o700); err != nil {
		t.Fatal(err)
	}
	deliveryWrite(t, filepath.Join(dir, "config.json"), `{"telegram":{"bot":""}}`)
	t.Setenv("HOME", root)
	t.Setenv("OMOSENSE_DIR", dir)
	t.Setenv("OMOSENSE_STATE", stateDir)
	t.Setenv("OMOSENSE_RPC_SOCK", filepath.Join(root, "absent.sock"))
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

// The observer and the subscription are in place before starting the process.
// The batch is emitted after the real shell exits and its result is committed
// to the store, and the done is never printed (IS-8).
func runCompletion(t *testing.T, source bool) (string, *fakeDelivery, []string) {
	t.Helper()
	stateDir, f := integrationFixture(t)
	deliveryWrite(t, filepath.Join(f.dir, "list"), fmt.Sprintf(`[{"thread_id":"%s","sessionId":"%s","alive":true}]`+"\n", rpcSubscriber, rpcSubscriber))
	if err := writeSubscription(stateDir, subscription{Session: rpcSubscriber, SubscribedAt: core.ISO(time.Now())}); err != nil {
		t.Fatal(err)
	}
	s := serveLiveStream(t, false)
	cmd := rpcProcess(t, "--all")
	cmd.Env = append(cmd.Env, "OMOSENSE_RPC_TEST_QUIET=50ms")
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
batched:
	for {
		select {
		case line := <-lines:
			if strings.HasPrefix(line, "LOG rpc batch sent ") {
				break batched
			}
		case <-scanned:
			t.Fatal("watcher exited before the batch")
		case <-timer.C:
			t.Fatal("completion was not batched")
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
			if ev.Event == "done" {
				doneCount++
			}
		}
		if strings.HasPrefix(line, "LOG rpc pending ") {
			t.Fatalf("per-done LOG survived IS-8: %q", line)
		}
	}
	entries := pendingList(t, newPendingStore(stateDir))
	calls := f.calls(t)
	if doneCount != 0 || len(entries) != 1 || len(calls) != 2 {
		t.Fatalf("done=%d entries=%+v calls=%q", doneCount, entries, calls)
	}
	call := calls[1]
	if calls[0][1] != "list" || len(call) != 8 || call[0] != "thread" || call[1] != "send" ||
		call[2] != rpcSubscriber || call[4] != "--all-scope" || call[5] != "--idempotency-key" ||
		call[6] != "omosense-rpc-batch-1" || call[7] != "--json" {
		t.Fatalf("send argv = %q", call)
	}
	if _, err := os.Stat(filepath.Join(stateDir, "watch-rpc.lock.json")); !os.IsNotExist(err) {
		t.Fatalf("watch lock was not released: %v", err)
	}
	return stateDir, f, call
}

// rpcSubscriber is the session the subprocess batch is pushed to.
const rpcSubscriber = "MAINQA"

func TestRunCompletionDelivery(t *testing.T) {
	// Given a real Run process and wire-level idle snapshots, When a short turn
	// settles, Then it is recorded and batched once, and an offline ACK closes
	// the pending loop.
	for _, source := range []bool{false, true} {
		t.Run(fmt.Sprintf("source_%t", source), func(t *testing.T) {
			stateDir, _, _ := runCompletion(t, source)
			entries := pendingList(t, newPendingStore(stateDir))
			code, out, _ := runCommand(t, "pending")
			if code != 0 || strings.Count(out, "PENDING ") != 1 {
				t.Fatalf("pending exit=%d stdout=%q", code, out)
			}
			requireCommand(t, fmt.Sprintf("ACK {\"id\":\"A\",\"seq\":%d,\"result\":\"acked\"}\n", entries[0].Seq), "ack", "A", "1")
			requireCommand(t, "", "pending")
		})
	}
}

// TestRunAckCommandFromBatch runs the ACK command the batch text itself
// carries: it must be valid verbatim (absolute env-pinned paths, no --profile)
// and clear the entry (B1).
func TestRunAckCommandFromBatch(t *testing.T) {
	stateDir, _, call := runCompletion(t, false)
	dir, state := os.Getenv("OMOSENSE_DIR"), os.Getenv("OMOSENSE_STATE")
	m := regexp.MustCompile(`OMOSENSE_DIR='([^']*)' OMOSENSE_STATE='([^']*)' omosense rpc ack (\S+) (\d+)`).FindStringSubmatch(call[3])
	if m == nil {
		t.Fatalf("batch lacks the machine-consumed ACK command: %q", call[3])
	}
	if m[1] != dir || m[2] != state {
		t.Fatalf("ack command paths = %q %q, want %q %q", m[1], m[2], dir, state)
	}
	if strings.Contains(m[0], "--profile") {
		t.Fatalf("ack command still names a profile: %q", m[0])
	}
	requireCommand(t, fmt.Sprintf("ACK {\"id\":\"A\",\"seq\":%s,\"result\":\"acked\"}\n", m[4]), "ack", m[3], m[4])
	requireCommand(t, "", "pending")
	if entries := pendingList(t, newPendingStore(stateDir)); len(entries) != 0 {
		t.Fatalf("ack left entries pending: %+v", entries)
	}
}

// TestRunSubscribeRoundTrip covers the IS-7 CLI: subscribe writes the file and
// prints it, subscription prints it, unsubscribe removes it.
func TestRunSubscribeRoundTrip(t *testing.T) {
	stateDir, _ := integrationFixture(t)
	code, out, stderr := runCommand(t, "subscribe", rpcSubscriber)
	var sub subscription
	if code != 0 || stderr != "" || !strings.HasPrefix(out, "SUB ") ||
		json.Unmarshal([]byte(strings.TrimPrefix(out, "SUB ")), &sub) != nil ||
		sub.Session != rpcSubscriber || sub.SubscribedAt == "" {
		t.Fatalf("subscribe exit=%d stdout=%q stderr=%q", code, out, stderr)
	}
	b, err := os.ReadFile(subscriptionPath(stateDir))
	if err != nil || json.Unmarshal(b, &sub) != nil || sub.Session != rpcSubscriber {
		t.Fatalf("subscription file = %s %v", b, err)
	}
	requireCommand(t, out, "subscription")
	requireCommand(t, "UNSUB {\"session\":\""+rpcSubscriber+"\"}\n", "unsubscribe")
	if _, err := os.Stat(subscriptionPath(stateDir)); !os.IsNotExist(err) {
		t.Fatalf("unsubscribe kept the file: %v", err)
	}
	requireCommand(t, "", "subscription")
	requireCommand(t, "UNSUB {\"session\":null}\n", "unsubscribe")
	names, err := os.ReadDir(stateDir)
	if err != nil {
		t.Fatal(err)
	}
	for _, n := range names {
		if strings.HasPrefix(n.Name(), ".rpc-subscription-") {
			t.Fatalf("temp file left behind: %s", n.Name())
		}
	}
}

func TestRunMalformedAck(t *testing.T) {
	stateDir, _ := integrationFixture(t)
	store := newPendingStore(stateDir)
	pendingRecord(t, store, "A", "H")
	before, err := os.ReadFile(store.path)
	if err != nil {
		t.Fatal(err)
	}
	for _, args := range [][]string{
		{"ack"}, {"ack", ""}, {"ack", "A", "0"}, {"ack", "A", "-1"},
		{"ack", "A", "1.5"}, {"ack", "A", "x"}, {"ack", "A", "0x1"}, {"ack", "A", "+1"},
		{"ack", "A", "18446744073709551616"}, {"ack", "A", "1", "extra"}, {"pending", "extra"},
		{"subscribe"}, {"subscribe", ""}, {"subscribe", "A", "extra"},
		{"unsubscribe", "extra"}, {"subscription", "extra"},
	} {
		t.Run(strings.Join(args, "_"), func(t *testing.T) {
			code, out, stderr := runCommand(t, args...)
			after, err := os.ReadFile(store.path)
			if code != 2 || out != "" || !strings.HasPrefix(stderr, "Usage:") || err != nil || !bytes.Equal(before, after) {
				t.Fatalf("invalid args accepted or changed file: exit=%d out=%q stderr=%q file=%s err=%v", code, out, stderr, after, err)
			}
			if _, err := os.Stat(filepath.Join(stateDir, "watch-rpc.lock.json")); !os.IsNotExist(err) {
				t.Fatalf("invalid args took watch lock: %v", err)
			}
			if _, err := os.Stat(subscriptionPath(stateDir)); !os.IsNotExist(err) {
				t.Fatalf("invalid args wrote a subscription: %v", err)
			}
		})
	}
}

func TestRunOfflineCommands(t *testing.T) {
	stateDir, _ := integrationFixture(t)
	store := newPendingStore(stateDir)
	pendingRecord(t, store, "Z", "H1")
	pendingRecord(t, store, "A", "H2")
	// A corrupt watch lock and absent sockets must not impede offline commands.
	deliveryWrite(t, filepath.Join(stateDir, "watch-rpc.lock.json"), "invalid")
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
	store := newPendingStore(stateDir)
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
	if _, err := os.Stat(filepath.Join(stateDir, "watch-rpc.lock.json")); !os.IsNotExist(err) {
		t.Fatalf("once took lock: %v", err)
	}
}
