package herdr

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/DevNewbie1826/omosense/internal/core"
)

// The measured live senpi argv (F-H1): argv0 is bun, the agent lives in the
// bundle path, and the whole command line is what the default agentPattern
// must match.
var measuredSenpiArgv = []string{
	"/opt/homebrew/Cellar/bun/1.4.2/bin/bun",
	"/Users/x/.bun/install/global/node_modules/@code-yeongyu/senpi/dist/bundle/cli.js",
	"--extension",
	"/Users/x/.bun/install/global/node_modules/omo-ai/plugin",
}

const measuredSenpiCmdline = "/opt/homebrew/Cellar/bun/1.4.2/bin/bun /Users/x/.bun/install/global/node_modules/@code-yeongyu/senpi/dist/bundle/cli.js --extension /Users/x/.bun/install/global/node_modules/omo-ai/plugin"

const defaultPattern = `senpi|omo|claude|codex|opencode|(^|/)pi( |$)`

// herdrCtx builds the source context the way the host does: core.Load parses
// config.json, so the profile carries the resolved herdr pattern, rpc section
// and verify hook exactly as production sees them.
func herdrCtx(t *testing.T, state, cfg string, out *bytes.Buffer) *core.Ctx {
	t.Helper()
	unsetEnv(t, "HERDR_PANE_ID")
	dir := t.TempDir()
	writeFile(t, filepath.Join(dir, "config.json"), cfg)
	t.Setenv("OMOSENSE_DIR", dir)
	t.Setenv("OMOSENSE_STATE", state)
	c, err := core.Load(core.ParsedArgs{Flags: map[string]bool{}}, false)
	if err != nil {
		t.Fatal(err)
	}
	c.Out = core.NewOut(out)
	return c
}

// fakeClock replaces the IS-3/IS-5/IS-6 clock. It returns the advance function
// the test uses to move time forward; nothing waits on real time. The clock is
// mutex-guarded because a test may advance it while a watcher goroutine reads
// it.
func fakeClock(t *testing.T, start time.Time) func(time.Duration) {
	t.Helper()
	var mu sync.Mutex
	clock := start
	old := nowFn
	nowFn = func() time.Time {
		mu.Lock()
		defer mu.Unlock()
		return clock
	}
	t.Cleanup(func() { nowFn = old })
	return func(d time.Duration) {
		mu.Lock()
		defer mu.Unlock()
		clock = clock.Add(d)
	}
}

// pendingEntries reads the persisted IS-4 store. A missing file is an empty
// store, matching what the watcher itself sees.
func pendingEntries(t *testing.T, state string) map[string]doneEntry {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(state, "herdr-pending.json"))
	if os.IsNotExist(err) {
		return map[string]doneEntry{}
	}
	if err != nil {
		t.Fatal(err)
	}
	var f pendingFile
	if err := json.Unmarshal(b, &f); err != nil {
		t.Fatalf("herdr-pending.json: %v (%s)", err, b)
	}
	return f.Entries
}

// pendingKeys is the recorded pane set, sorted, so a test can state exactly
// which panes produced a done.
func pendingKeys(t *testing.T, state string) []string {
	t.Helper()
	entries := pendingEntries(t, state)
	out := make([]string, 0, len(entries))
	for k := range entries {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

func wantPending(t *testing.T, state string, keys ...string) {
	t.Helper()
	got := pendingKeys(t, state)
	if strings.Join(got, ",") != strings.Join(keys, ",") {
		t.Fatalf("recorded panes = %q, want %q", got, keys)
	}
}

func paneListJSON(ids ...string) string {
	rows := []map[string]any{}
	for _, id := range ids {
		rows = append(rows, map[string]any{"pane_id": id})
	}
	return marshalFixture(map[string]any{"result": map[string]any{"panes": rows}})
}

func processInfoJSON(procs ...map[string]any) string {
	list := []map[string]any{}
	list = append(list, procs...)
	return marshalFixture(map[string]any{"result": map[string]any{"process_info": map[string]any{"foreground_processes": list}}})
}

func procFixture(argv []string, cmdline string) map[string]any {
	return map[string]any{"argv": argv, "argv0": argv[0], "cmdline": cmdline, "name": "bun", "pid": 71108}
}

func marshalFixture(v any) string {
	b, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return string(b)
}

// alivePanes makes the IS-5 check see every named pane as a live agent pane:
// each is listed by `pane list` and its foreground is the measured senpi argv.
func alivePanes(t *testing.T, dir string, panes ...string) {
	t.Helper()
	writeFile(t, filepath.Join(dir, "local.pane.out"), paneListJSON(panes...))
	for _, p := range panes {
		writeFile(t, filepath.Join(dir, "local.pinfo."+p+".out"), processInfoJSON(procFixture(measuredSenpiArgv, measuredSenpiCmdline)))
	}
}

func withPrefix(lines []string, prefix string) []string {
	var out []string
	for _, l := range lines {
		if strings.HasPrefix(l, prefix) {
			out = append(out, l)
		}
	}
	return out
}

func callsOf(t *testing.T, dir string) []string {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(dir, "calls"))
	if err != nil {
		t.Fatal(err)
	}
	return strings.Split(strings.TrimSuffix(string(b), "\n"), "\n")
}

// TestClosedThreadPaneSkipped guards the Risk row "a closed thread's pane still
// produces job events" for the herdr reader (IS-4): an entry whose status is
// done/closed, or that carries a non-empty closed string, is not watched, so
// its pane's working->idle emits nothing.
func TestClosedThreadPaneSkipped(t *testing.T) {
	dir := t.TempDir()
	state := t.TempDir()
	installFakeHerdr(t, dir)
	writeFile(t, filepath.Join(dir, "machines.out"), "[]\n")
	writeFile(t, filepath.Join(state, "threads.json"), `{
		"live":{"pane":"p1"},
		"done":{"pane":"p2","status":"done"},
		"closedstr":{"pane":"p3","status":"closed"},
		"closedat":{"pane":"p4","closed":"2026-10-05T00:00:00.000Z"},
		"closednull":{"pane":"p5","closed":null}
	}`)
	var buf bytes.Buffer
	w := newWatcher(herdrCtx(t, state, "{}", &buf), core.NewOut(&buf))

	working := agents(ag("p1", "working", "j"), ag("p2", "working", "j"), ag("p3", "working", "j"), ag("p4", "working", "j"), ag("p5", "working", "j"))
	idle := agents(ag("p1", "idle", "j"), ag("p2", "idle", "j"), ag("p3", "idle", "j"), ag("p4", "idle", "j"), ag("p5", "idle", "j"))
	writeFile(t, filepath.Join(dir, "local.out"), working)
	if err := w.tick(context.Background(), true); err != nil {
		t.Fatal(err)
	}
	buf.Reset()
	writeFile(t, filepath.Join(dir, "local.out"), idle)
	if err := w.tick(context.Background(), false); err != nil {
		t.Fatal(err)
	}
	// IS-3: the done is recorded for the quiet batch; only the watched
	// (non-closed) panes p1 and p5 produce a record.
	if got := linesOf(&buf); len(got) != 0 {
		t.Fatalf("a done printed immediately: %q", got)
	}
	wantPending(t, state, "local/p1", "local/p5")
	t.Log("closed entries done/closed/closed-at produced no job record; closed:null stays watched (IS-4)")
	t.Log("cleanup: test Cleanup removes the fake herdr dir and restores HERDR_PANE_ID, OMOSENSE_DIR, OMOSENSE_STATE")
}

// TestDeadPaneAgentExitedToShell guards the Risk row "an agent that exited to
// the shell is never reported dead" (IS-5): with the agent gone the foreground
// list is empty or holds only the shell, and exactly one `LOG dead-pane` names
// the pane with reason "no agent process".
func TestDeadPaneAgentExitedToShell(t *testing.T) {
	cases := []struct {
		name  string
		procs string
		want  string
	}{
		{"empty foreground", processInfoJSON(), `[]`},
		{"shell at prompt", processInfoJSON(map[string]any{"argv": []string{"-zsh"}, "argv0": "-zsh", "cmdline": "-zsh", "name": "zsh"}), `["-zsh"]`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			state := t.TempDir()
			installFakeHerdr(t, dir)
			writeFile(t, filepath.Join(dir, "machines.out"), "[]\n")
			writeFile(t, filepath.Join(state, "threads.json"), `{"job":{"pane":"p1"}}`)
			writeFile(t, filepath.Join(dir, "local.out"), agents(ag("p1", "working", "j")))
			writeFile(t, filepath.Join(dir, "local.pane.out"), paneListJSON("p1"))
			writeFile(t, filepath.Join(dir, "local.pinfo.p1.out"), tc.procs)

			advance := fakeClock(t, time.Date(2026, 10, 5, 0, 0, 0, 0, time.UTC))
			var buf bytes.Buffer
			w := newWatcher(herdrCtx(t, state, "{}", &buf), core.NewOut(&buf))
			advance(deadInterval)
			if err := w.tick(context.Background(), false); err != nil {
				t.Fatal(err)
			}
			wantLines(t, linesOf(&buf),
				`LOG dead-pane {"machine":"local","pane":"p1","thread":"job","reason":"no agent process","foreground":`+tc.want+`,"pattern":"`+defaultPattern+`"}`,
			)
			t.Log("cleanup: test Cleanup removes the fake herdr dir and restores the clock and environment")
		})
	}
}

// TestDeadPaneLiveSenpiNotDead guards the Risk row "a live senpi agent (argv0
// bun) is reported dead" (IS-5): the measured argv matches the default
// agentPattern, so no dead-pane line and no LOG herdr appear.
func TestDeadPaneLiveSenpiNotDead(t *testing.T) {
	dir := t.TempDir()
	state := t.TempDir()
	installFakeHerdr(t, dir)
	writeFile(t, filepath.Join(dir, "machines.out"), "[]\n")
	writeFile(t, filepath.Join(state, "threads.json"), `{"job":{"pane":"p1"}}`)
	writeFile(t, filepath.Join(dir, "local.out"), agents(ag("p1", "working", "j")))
	alivePanes(t, dir, "p1")

	advance := fakeClock(t, time.Date(2026, 10, 5, 0, 0, 0, 0, time.UTC))
	var buf bytes.Buffer
	w := newWatcher(herdrCtx(t, state, "{}", &buf), core.NewOut(&buf))
	advance(deadInterval)
	if err := w.tick(context.Background(), false); err != nil {
		t.Fatal(err)
	}
	if got := linesOf(&buf); len(got) != 0 {
		t.Fatalf("a live senpi pane was reported: %q", got)
	}
	if got := callsOf(t, dir); !containsCall(got, "pane process-info --pane p1") {
		t.Fatalf("process-info was not called: %q", got)
	}
	t.Log("cleanup: test Cleanup removes the fake herdr dir and restores the clock and environment")
}

// TestDeadPaneHerdrFailureNotDead guards the Risk row "a herdr process-info or
// pane list failure is reported as dead-pane, or a pane absent from pane list
// produces a LOG herdr error line" (IS-5): a failed call goes to the LOG herdr
// path and is never dead, and an absent pane is "pane gone" with no
// process-info call and no LOG herdr.
func TestDeadPaneHerdrFailureNotDead(t *testing.T) {
	setup := func(t *testing.T) (string, string, *watcher, *bytes.Buffer, func(time.Duration)) {
		t.Helper()
		dir := t.TempDir()
		state := t.TempDir()
		installFakeHerdr(t, dir)
		writeFile(t, filepath.Join(dir, "machines.out"), "[]\n")
		writeFile(t, filepath.Join(state, "threads.json"), `{"job":{"pane":"p1"}}`)
		writeFile(t, filepath.Join(dir, "local.out"), agents(ag("p1", "working", "j")))
		advance := fakeClock(t, time.Date(2026, 10, 5, 0, 0, 0, 0, time.UTC))
		var buf bytes.Buffer
		w := newWatcher(herdrCtx(t, state, "{}", &buf), core.NewOut(&buf))
		return dir, state, w, &buf, advance
	}

	t.Run("pane list fails", func(t *testing.T) {
		dir, _, w, buf, advance := setup(t)
		writeFile(t, filepath.Join(dir, "local.pane.err"), "pane down\n")
		writeFile(t, filepath.Join(dir, "local.pane.code"), "1\n")
		advance(deadInterval)
		if err := w.tick(context.Background(), false); err != nil {
			t.Fatal(err)
		}
		wantLines(t, linesOf(buf), "LOG herdr local Error: pane down")
	})

	t.Run("process-info fails", func(t *testing.T) {
		dir, _, w, buf, advance := setup(t)
		writeFile(t, filepath.Join(dir, "local.pane.out"), paneListJSON("p1"))
		writeFile(t, filepath.Join(dir, "local.pinfo.p1.err"), "no proc\n")
		writeFile(t, filepath.Join(dir, "local.pinfo.p1.code"), "1\n")
		advance(deadInterval)
		if err := w.tick(context.Background(), false); err != nil {
			t.Fatal(err)
		}
		wantLines(t, linesOf(buf), "LOG herdr local Error: no proc")
	})

	t.Run("pane absent is pane gone with no process-info", func(t *testing.T) {
		dir, _, w, buf, advance := setup(t)
		writeFile(t, filepath.Join(dir, "local.pane.out"), paneListJSON("other"))
		advance(deadInterval)
		if err := w.tick(context.Background(), false); err != nil {
			t.Fatal(err)
		}
		wantLines(t, linesOf(buf),
			`LOG dead-pane {"machine":"local","pane":"p1","thread":"job","reason":"pane gone","foreground":[],"pattern":"`+defaultPattern+`"}`,
		)
		for _, call := range callsOf(t, dir) {
			if strings.Contains(call, "process-info") {
				t.Fatalf("an absent pane got a process-info call: %q", call)
			}
		}
	})
}

// TestDeadPaneReportedOnceAndRearmed guards the Risk row "dead-pane repeats
// every tick instead of once" (IS-5): a dead pane is reported once, stays quiet
// while it is still dead, and re-arms only after it was alive again.
func TestDeadPaneReportedOnceAndRearmed(t *testing.T) {
	dir := t.TempDir()
	state := t.TempDir()
	installFakeHerdr(t, dir)
	writeFile(t, filepath.Join(dir, "machines.out"), "[]\n")
	writeFile(t, filepath.Join(state, "threads.json"), `{"job":{"pane":"p1"}}`)
	writeFile(t, filepath.Join(dir, "local.out"), agents(ag("p1", "working", "j")))
	writeFile(t, filepath.Join(dir, "local.pane.out"), paneListJSON("p1"))
	writeFile(t, filepath.Join(dir, "local.pinfo.p1.out"), processInfoJSON())

	advance := fakeClock(t, time.Date(2026, 10, 5, 0, 0, 0, 0, time.UTC))
	var buf bytes.Buffer
	w := newWatcher(herdrCtx(t, state, "{}", &buf), core.NewOut(&buf))
	dead := `LOG dead-pane {"machine":"local","pane":"p1","thread":"job","reason":"no agent process","foreground":[],"pattern":"` + defaultPattern + `"}`

	advance(deadInterval)
	if err := w.tick(context.Background(), false); err != nil {
		t.Fatal(err)
	}
	advance(deadInterval)
	if err := w.tick(context.Background(), false); err != nil {
		t.Fatal(err)
	}
	wantLines(t, linesOf(&buf), dead)

	buf.Reset()
	writeFile(t, filepath.Join(dir, "local.pinfo.p1.out"), processInfoJSON(procFixture(measuredSenpiArgv, measuredSenpiCmdline)))
	advance(deadInterval)
	if err := w.tick(context.Background(), false); err != nil {
		t.Fatal(err)
	}
	if got := linesOf(&buf); len(got) != 0 {
		t.Fatalf("an alive pane was reported: %q", got)
	}

	buf.Reset()
	writeFile(t, filepath.Join(dir, "local.pinfo.p1.out"), processInfoJSON())
	advance(deadInterval)
	if err := w.tick(context.Background(), false); err != nil {
		t.Fatal(err)
	}
	wantLines(t, linesOf(&buf), dead)
	t.Log("cleanup: test Cleanup removes the fake herdr dir and restores the clock and environment")
}

// TestSilentSessionReportedOnceAndRearmed guards the Risk rows "a silent
// working session is never reported" and "silent-session repeats every tick
// instead of once" for herdr (IS-6): a registered pane that stays working with
// no status change and no revision growth is reported once, an idle pane never
// is, and a revision change re-arms the window.
func TestSilentSessionReportedOnceAndRearmed(t *testing.T) {
	dir := t.TempDir()
	state := t.TempDir()
	installFakeHerdr(t, dir)
	writeFile(t, filepath.Join(dir, "machines.out"), "[]\n")
	writeFile(t, filepath.Join(state, "threads.json"), `{"job":{"pane":"p1"},"quiet":{"pane":"p2"}}`)
	alivePanes(t, dir, "p1", "p2")
	agentsJSON := func(rev string) string {
		return `{"result":{"agents":[
			{"pane_id":"p1","agent_status":"working","revision":` + rev + `},
			{"pane_id":"p2","agent_status":"idle","revision":1}
		]}}`
	}

	start := time.Date(2026, 10, 5, 0, 0, 0, 0, time.UTC)
	advance := fakeClock(t, start)
	var buf bytes.Buffer
	w := newWatcher(herdrCtx(t, state, `{"silentMinutes":1}`, &buf), core.NewOut(&buf))

	writeFile(t, filepath.Join(dir, "local.out"), agentsJSON("5"))
	if err := w.tick(context.Background(), true); err != nil {
		t.Fatal(err)
	}
	advance(30 * time.Second)
	if err := w.tick(context.Background(), false); err != nil {
		t.Fatal(err)
	}
	if got := linesOf(&buf); len(got) != 0 {
		t.Fatalf("reported inside the silence window: %q", got)
	}

	advance(31 * time.Second)
	if err := w.tick(context.Background(), false); err != nil {
		t.Fatal(err)
	}
	silent5 := `LOG silent-session {"source":"herdr","machine":"local","pane":"p1","thread":"job","since":"2026-10-05T00:00:00.000Z","minutes":1,"signal":5}`
	wantLines(t, linesOf(&buf), silent5)

	advance(60 * time.Second)
	if err := w.tick(context.Background(), false); err != nil {
		t.Fatal(err)
	}
	wantLines(t, linesOf(&buf), silent5)

	buf.Reset()
	writeFile(t, filepath.Join(dir, "local.out"), agentsJSON("6"))
	if err := w.tick(context.Background(), false); err != nil {
		t.Fatal(err)
	}
	if got := linesOf(&buf); len(got) != 0 {
		t.Fatalf("revision growth did not re-arm the window: %q", got)
	}

	advance(61 * time.Second)
	if err := w.tick(context.Background(), false); err != nil {
		t.Fatal(err)
	}
	wantLines(t, linesOf(&buf),
		`LOG silent-session {"source":"herdr","machine":"local","pane":"p1","thread":"job","since":"2026-10-05T00:02:01.000Z","minutes":1,"signal":6}`,
	)

	// A revision that moves backwards is not growth (IS-6): the reported
	// window stays reported instead of re-arming.
	writeFile(t, filepath.Join(dir, "local.out"), agentsJSON("2"))
	if err := w.tick(context.Background(), false); err != nil {
		t.Fatal(err)
	}
	advance(61 * time.Second)
	if err := w.tick(context.Background(), false); err != nil {
		t.Fatal(err)
	}
	if got := linesOf(&buf); len(got) != 1 {
		t.Fatalf("a revision decrease re-armed the window: %q", got)
	}
	t.Log("the idle pane p2 was never silent; cleanup: test Cleanup removes the fake herdr dir and restores the clock and environment")
}

// verifyHook is a fake done-verification command: it records its environment
// and exits with the code in <dir>/control, optionally waiting on a release
// FIFO after signaling <dir>/ready.
type verifyHook struct {
	dir string
}

func newVerifyHook(t *testing.T, code string) *verifyHook {
	t.Helper()
	h := &verifyHook{dir: t.TempDir()}
	script := `#!/bin/sh
dir=${0%/*}
printf '%s\n' "$OMOSENSE_DONE_SOURCE" "$OMOSENSE_DONE_PANE" "$OMOSENSE_DONE_MACHINE" "$OMOSENSE_DONE_THREAD" "$OMOSENSE_DONE_CWD" >> "$dir/env"
if [ -e "$dir/gate" ]; then
  printf 'ready\n' > "$dir/ready"
  IFS= read -r release < "$dir/release"
fi
if [ -f "$dir/stderr" ]; then cat "$dir/stderr" >&2; fi
IFS= read -r code < "$dir/control"
exit "$code"
`
	writeFile(t, filepath.Join(h.dir, "hook.sh"), script)
	if err := os.Chmod(filepath.Join(h.dir, "hook.sh"), 0o700); err != nil {
		t.Fatal(err)
	}
	h.setCode(t, code)
	return h
}

func (h *verifyHook) path() string { return filepath.Join(h.dir, "hook.sh") }

func (h *verifyHook) setCode(t *testing.T, code string) {
	t.Helper()
	writeFile(t, filepath.Join(h.dir, "control"), code+"\n")
}

func (h *verifyHook) env(t *testing.T) []string {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(h.dir, "env"))
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		t.Fatal(err)
	}
	return strings.Split(strings.TrimSuffix(string(b), "\n"), "\n")
}

// gate installs the FIFO barrier: wait returns once the hook entered it,
// release lets it finish. It is a barrier, not a sleep.
func (h *verifyHook) gate(t *testing.T) (wait, release func()) {
	t.Helper()
	writeFile(t, filepath.Join(h.dir, "gate"), "")
	open := func(name string) *os.File {
		path := filepath.Join(h.dir, name)
		if err := syscall.Mkfifo(path, 0o600); err != nil {
			t.Fatal(err)
		}
		fd, err := os.OpenFile(path, os.O_RDWR, 0o600)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { fd.Close() })
		return fd
	}
	ready, rel := open("ready"), open("release")
	signaled := make(chan error, 1)
	go func() {
		_, err := bufio.NewReader(ready).ReadString('\n')
		signaled <- err
	}()
	wait = func() {
		select {
		case err := <-signaled:
			if err != nil {
				t.Fatal(err)
			}
		case <-time.After(10 * time.Second):
			t.Fatal("hook did not enter the barrier")
		}
	}
	release = func() { _, _ = rel.WriteString("release\n") }
	return wait, release
}

func hookCfg(h *verifyHook, herdrVerify bool) string {
	return fmt.Sprintf(`{"verify":{"command":[%q],"timeoutSec":10},"herdr":{"verify":%t}}`, h.path(), herdrVerify)
}

func verifyPane(t *testing.T, dir, state string, w *watcher) {
	t.Helper()
	writeFile(t, filepath.Join(dir, "machines.out"), "[]\n")
	writeFile(t, filepath.Join(state, "threads.json"), `{"job":{"pane":"p1"}}`)
	writeFile(t, filepath.Join(dir, "local.out"), agents(`{"pane_id":"p1","agent_status":"working","cwd":"/work/job","terminal_title_stripped":"j"}`))
	if err := w.tick(context.Background(), true); err != nil {
		t.Fatal(err)
	}
	writeFile(t, filepath.Join(dir, "local.out"), agents(`{"pane_id":"p1","agent_status":"idle","cwd":"/work/job","terminal_title_stripped":"j"}`))
	if err := w.tick(context.Background(), false); err != nil {
		t.Fatal(err)
	}
}

// TestHerdrVerifyAbsentNoFieldsNoHook guards the Risk row "herdr runs the hook
// or adds fields when herdr.verify is absent" (IS-5): without the opt-in the
// batch entry is byte-identical to 0.1.0 - no verify field at all - and the
// hook never runs.
func TestHerdrVerifyAbsentNoFieldsNoHook(t *testing.T) {
	const base = `{"machine":"local","pane":"p1","tab":null,"agent":null,"cwd":"/work/job","from":"working","to":"idle","at":"2026-10-05T00:00:00.000Z","first_at":"2026-10-05T00:00:00.000Z","count":1}`
	start := time.Date(2026, 10, 5, 0, 0, 0, 0, time.UTC)

	t.Run("herdr.verify absent", func(t *testing.T) {
		dir := t.TempDir()
		state := t.TempDir()
		installFakeHerdr(t, dir)
		h := newVerifyHook(t, "0")
		alivePanes(t, dir, "p1")
		advance := fakeClock(t, start)
		var buf bytes.Buffer
		w := newWatcher(herdrCtx(t, state, fmt.Sprintf(`{"verify":{"command":[%q],"timeoutSec":10}}`, h.path()), &buf), core.NewOut(&buf))
		verifyPane(t, dir, state, w)
		w.hooks.Wait()
		if env := h.env(t); env != nil {
			t.Fatalf("the hook ran without herdr.verify: %q", env)
		}
		if got := pendingEntries(t, state)["local/p1"].Verify; got != "" {
			t.Fatalf("entry verify = %q, want no verify field", got)
		}
		advance(5 * time.Minute)
		if err := w.tick(context.Background(), false); err != nil {
			t.Fatal(err)
		}
		wantLines(t, linesOf(&buf), `HERDR {"event":"done-batch","entries":[`+base+`]}`)
	})

	t.Run("verify.command absent", func(t *testing.T) {
		dir := t.TempDir()
		state := t.TempDir()
		installFakeHerdr(t, dir)
		alivePanes(t, dir, "p1")
		advance := fakeClock(t, start)
		var buf bytes.Buffer
		w := newWatcher(herdrCtx(t, state, `{"herdr":{"verify":true}}`, &buf), core.NewOut(&buf))
		verifyPane(t, dir, state, w)
		w.hooks.Wait()
		advance(5 * time.Minute)
		if err := w.tick(context.Background(), false); err != nil {
			t.Fatal(err)
		}
		wantLines(t, linesOf(&buf), `HERDR {"event":"done-batch","entries":[`+base+`]}`)
	})
}

// TestHerdrVerifyRunsHook guards the IS-5 happy path: with verify.command set
// and herdr.verify true the hook sees the transition's identity in its
// environment and its verdict lands in the batch entry instead of a separate
// line.
func TestHerdrVerifyRunsHook(t *testing.T) {
	dir := t.TempDir()
	state := t.TempDir()
	installFakeHerdr(t, dir)
	h := newVerifyHook(t, "0")
	alivePanes(t, dir, "p1")
	advance := fakeClock(t, time.Date(2026, 10, 5, 0, 0, 0, 0, time.UTC))
	var buf bytes.Buffer
	w := newWatcher(herdrCtx(t, state, hookCfg(h, true), &buf), core.NewOut(&buf))
	verifyPane(t, dir, state, w)
	w.hooks.Wait()
	want := []string{"herdr", "p1", "local", "job", "/work/job"}
	if got := h.env(t); len(got) != len(want) || strings.Join(got, "|") != strings.Join(want, "|") {
		t.Fatalf("hook env = %q, want %q", got, want)
	}
	if got := pendingEntries(t, state)["local/p1"].Verify; got != "verified" {
		t.Fatalf("entry verify = %q, want verified", got)
	}
	advance(5 * time.Minute)
	if err := w.tick(context.Background(), false); err != nil {
		t.Fatal(err)
	}
	wantLines(t, linesOf(&buf),
		`HERDR {"event":"done-batch","entries":[{"machine":"local","pane":"p1","tab":null,"agent":null,"cwd":"/work/job","from":"working","to":"idle","at":"2026-10-05T00:00:00.000Z","first_at":"2026-10-05T00:00:00.000Z","count":1,"verify":"verified"}]}`,
	)

	buf.Reset()
	h.setCode(t, "3")
	writeFile(t, filepath.Join(h.dir, "stderr"), "boom\n")
	verifyPane(t, dir, state, w)
	w.hooks.Wait()
	if got := pendingEntries(t, state)["local/p1"].VerifyDetail; got != "exit 3: boom" {
		t.Fatalf("entry verify_detail = %q, want the hook failure", got)
	}
	advance(5 * time.Minute)
	if err := w.tick(context.Background(), false); err != nil {
		t.Fatal(err)
	}
	wantLines(t, linesOf(&buf),
		`HERDR {"event":"done-batch","entries":[{"machine":"local","pane":"p1","tab":null,"agent":null,"cwd":"/work/job","from":"working","to":"idle","at":"2026-10-05T00:05:00.000Z","first_at":"2026-10-05T00:05:00.000Z","count":1,"verify":"unverified","verify_detail":"exit 3: boom"}]}`,
	)
	t.Log("cleanup: test Cleanup removes the fake herdr dir, the hook dir and restores the environment")
}

// TestVerifyTimeout guards S5-2 at the delivery boundary: the timeout a herdr
// done hook actually runs under is verify.timeoutSec (7 -> 7 s; 0 and -3 fall
// back to 60 s). A real pane transition drives emitJob and the duration handed
// to the hook runner is captured, so a call site that hard-codes a timeout - or
// a changed fallback - fails here, not just the stored field. The profile is
// built by hand because config load rejects a negative timeoutSec and rewrites
// 0 to the 60 s default (core.go posIntKey/416), so the fallback branch is only
// reachable from a hand-built profile; TestHerdrVerifyRunsHook covers the
// config.json -> hook path.
func TestVerifyTimeout(t *testing.T) {
	unsetEnv(t, "HERDR_PANE_ID")
	cases := []struct {
		sec  int
		want time.Duration
	}{
		{sec: 7, want: 7 * time.Second},
		{sec: 0, want: 60 * time.Second},
		{sec: -3, want: 60 * time.Second},
	}
	for _, tc := range cases {
		t.Run(fmt.Sprintf("timeoutSec=%d", tc.sec), func(t *testing.T) {
			dir := t.TempDir()
			state := t.TempDir()
			installFakeHerdr(t, dir)
			h := newVerifyHook(t, "0")
			var buf bytes.Buffer
			c := &core.Ctx{
				State: state,
				Out:   core.NewOut(&buf),
				Profile: core.Profile{
					Verify: core.VerifyCfg{Command: []string{h.path()}, TimeoutSec: tc.sec},
					Herdr:  core.HerdrCfg{Verify: true},
				},
			}
			w := newWatcher(c, core.NewOut(&buf))
			if !w.verify {
				t.Fatal("herdr verify hook is off")
			}
			handed := make(chan time.Duration, 4)
			old := runVerifyFn
			runVerifyFn = func(ctx context.Context, command []string, timeout time.Duration, env []string, dir string) core.VerifyResult {
				select {
				case handed <- timeout:
				default:
				}
				return core.RunVerify(ctx, command, timeout, env, dir)
			}
			t.Cleanup(func() { runVerifyFn = old })

			verifyPane(t, dir, state, w)
			w.hooks.Wait()
			select {
			case got := <-handed:
				if got != tc.want {
					t.Fatalf("hook timeout for timeoutSec=%d = %s, want %s", tc.sec, got, tc.want)
				}
			default:
				t.Fatal("the transition never reached the hook runner")
			}
		})
	}
}

// TestCancelledHoldPrintsUnverified guards the Risk row "shutdown during a
// running hook writes a verify result or delays the source's return; a
// cancelled herdr hold loses the transition" (IS-5): the transition is recorded
// before the hook starts, so the cancel cannot lose it - the cancelled verdict
// lands on the persisted entry and the shutdown never prints the batch.
func TestCancelledHoldPrintsUnverified(t *testing.T) {
	dir := t.TempDir()
	state := t.TempDir()
	installFakeHerdr(t, dir)
	h := newVerifyHook(t, "0")
	wait, release := h.gate(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var buf bytes.Buffer
	w := newWatcher(herdrCtx(t, state, hookCfg(h, true), &buf), core.NewOut(&buf))
	writeFile(t, filepath.Join(dir, "machines.out"), "[]\n")
	writeFile(t, filepath.Join(state, "threads.json"), `{"job":{"pane":"p1"}}`)
	writeFile(t, filepath.Join(dir, "local.out"), agents(ag("p1", "working", "j")))
	if err := w.tick(ctx, true); err != nil {
		t.Fatal(err)
	}
	writeFile(t, filepath.Join(dir, "local.out"), agents(ag("p1", "idle", "j")))
	if err := w.tick(ctx, false); err != nil {
		t.Fatal(err)
	}
	if got := linesOf(&buf); len(got) != 0 {
		t.Fatalf("the transition printed before the quiet window: %q", got)
	}
	wait()
	cancel()
	w.hooks.Wait()
	release()
	entry := pendingEntries(t, state)["local/p1"]
	if entry.Verify != "unverified" || entry.VerifyDetail != "cancelled" {
		t.Fatalf("cancelled entry = %+v, want unverified/cancelled", entry)
	}
	if got := linesOf(&buf); len(got) != 0 {
		t.Fatalf("the shutdown printed the batch: %q", got)
	}
	t.Log("the recorded done survived the cancel with verify unverified/cancelled (IS-5)")
	t.Log("cleanup: test Cleanup removes the fake herdr dir and the hook dir, closes both FIFOs and restores the environment")
}

// fakeRPCSocket serves list_sessions on a unix socket the way the real rpc
// server does, so the herdr fallback is exercised at the wire boundary.
type fakeRPCSocket struct {
	path  string
	calls int32
}

func serveSessions(t *testing.T, sessions []map[string]any) *fakeRPCSocket {
	t.Helper()
	dir, err := os.MkdirTemp("/tmp", "herdrrpc")
	if err != nil {
		t.Fatal(err)
	}
	f := &fakeRPCSocket{path: filepath.Join(dir, "s")}
	ln, err := net.Listen("unix", f.path)
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go func(conn net.Conn) {
				defer conn.Close()
				sc := bufio.NewScanner(conn)
				for sc.Scan() {
					var req struct {
						ID   string `json:"id"`
						Type string `json:"type"`
					}
					if json.Unmarshal(sc.Bytes(), &req) != nil {
						return
					}
					atomic.AddInt32(&f.calls, 1)
					if err := json.NewEncoder(conn).Encode(map[string]any{
						"id": req.ID, "type": "response", "success": true,
						"data": map[string]any{"sessions": sessions},
					}); err != nil {
						return
					}
				}
			}(conn)
		}
	}()
	t.Cleanup(func() {
		ln.Close()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Error("fake rpc socket did not stop")
		}
		if err := os.RemoveAll(dir); err != nil {
			t.Error(err)
		}
	})
	return f
}

func (f *fakeRPCSocket) count() int32 { return atomic.LoadInt32(&f.calls) }

// ownedJob drives ticks over a registered pane with a session field and reports
// the lines the herdr source printed. longPause keeps the pane working well past
// the silence window, so a silent-session line would appear if the pane were not
// suppressed.
func ownedJob(t *testing.T, dir, state string, buf *bytes.Buffer, cfg string, longPause bool) {
	t.Helper()
	writeFile(t, filepath.Join(dir, "machines.out"), "[]\n")
	writeFile(t, filepath.Join(dir, "local.out"), agents(ag("p1", "working", "j"), ag("p2", "working", "j")))
	advance := fakeClock(t, time.Date(2026, 10, 5, 0, 0, 0, 0, time.UTC))
	advance(deadInterval)
	w := newWatcher(herdrCtx(t, state, cfg, buf), core.NewOut(buf))
	if err := w.tick(context.Background(), true); err != nil {
		t.Fatal(err)
	}
	if longPause {
		advance(2 * time.Hour)
		if err := w.tick(context.Background(), false); err != nil {
			t.Fatal(err)
		}
	}
	advance(deadInterval)
	writeFile(t, filepath.Join(dir, "local.out"), agents(ag("p1", "idle", "j"), ag("p2", "idle", "j")))
	if err := w.tick(context.Background(), false); err != nil {
		t.Fatal(err)
	}
}

// TestRpcOwnedPaneSuppressed guards the Risk row "herdr duplicate report when
// rpc owns the session" (IS-12): a pane whose session is listed on the rpc
// socket gets no job transition, no silent-session and no dead-pane check -
// the socket is asked once per tick, not once per pane.
func TestRpcOwnedPaneSuppressed(t *testing.T) {
	dir := t.TempDir()
	state := t.TempDir()
	installFakeHerdr(t, dir)
	sock := serveSessions(t, []map[string]any{{"sessionId": "H1", "durableSessionId": "D1"}})
	t.Setenv("OMOSENSE_RPC_SOCK", sock.path)
	writeFile(t, filepath.Join(state, "threads.json"), `{"one":{"pane":"p1","session_id":"D1"},"two":{"pane":"p2","durable_session_id":"D1"}}`)
	alivePanes(t, dir, "p1", "p2")

	var buf bytes.Buffer
	ownedJob(t, dir, state, &buf, `{"rpc":{"enabled":true}}`, true)
	if got := linesOf(&buf); len(got) != 0 {
		t.Fatalf("an rpc-owned pane was reported (job transition, silent-session or dead-pane): %q", got)
	}
	if got := pendingKeys(t, state); len(got) != 0 {
		t.Fatalf("an rpc-owned pane was recorded: %q", got)
	}
	for _, call := range callsOf(t, dir) {
		if strings.Contains(call, "pane") {
			t.Fatalf("an rpc-owned pane got a herdr pane call: %q", call)
		}
	}
	if got := sock.count(); got != 3 {
		t.Fatalf("list_sessions calls = %d, want one per tick (3)", got)
	}
	t.Log("cleanup: test Cleanup closes the fake rpc socket and removes its temp dir")
}

// TestRpcFallbackSocketDown guards the Risk row "no fallback when the socket
// is down" (IS-12): with no listener the pane is reported as usual and the
// failure adds no LOG line.
func TestRpcFallbackSocketDown(t *testing.T) {
	dir := t.TempDir()
	state := t.TempDir()
	installFakeHerdr(t, dir)
	sockDir, err := os.MkdirTemp("/tmp", "herdrnosock")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(sockDir) })
	t.Setenv("OMOSENSE_RPC_SOCK", filepath.Join(sockDir, "s"))
	writeFile(t, filepath.Join(state, "threads.json"), `{"job":{"pane":"p1","session_id":"D1"}}`)
	alivePanes(t, dir, "p1")

	var buf bytes.Buffer
	ownedJob(t, dir, state, &buf, `{"rpc":{"enabled":true}}`, false)
	// IS-3/IS-12: the fallback reports the pane as usual, which now means the
	// done is recorded for the quiet batch.
	if got := linesOf(&buf); len(got) != 0 {
		t.Fatalf("a fallback done printed immediately: %q", got)
	}
	wantPending(t, state, "local/p1")
	t.Log("cleanup: test Cleanup removes the fake herdr dir and the unused socket dir")
}

// TestRpcFallbackSessionNotListed guards the Risk row "herdr does not fall back
// when the socket is up but the session is not listed" (IS-12): a successful
// list without a match leaves the pane to herdr.
func TestRpcFallbackSessionNotListed(t *testing.T) {
	dir := t.TempDir()
	state := t.TempDir()
	installFakeHerdr(t, dir)
	sock := serveSessions(t, []map[string]any{{"sessionId": "OTHER", "durableSessionId": "X"}})
	t.Setenv("OMOSENSE_RPC_SOCK", sock.path)
	writeFile(t, filepath.Join(state, "threads.json"), `{"job":{"pane":"p1","session_id":"D1"}}`)
	alivePanes(t, dir, "p1")

	var buf bytes.Buffer
	ownedJob(t, dir, state, &buf, `{"rpc":{"enabled":true}}`, false)
	if got := linesOf(&buf); len(got) != 0 {
		t.Fatalf("a fallback done printed immediately: %q", got)
	}
	wantPending(t, state, "local/p1")
	if got := sock.count(); got != 2 {
		t.Fatalf("list_sessions calls = %d, want one per tick (2)", got)
	}
	t.Log("cleanup: test Cleanup closes the fake rpc socket and removes its temp dir")
}

func containsCall(calls []string, want string) bool {
	for _, c := range calls {
		if c == want {
			return true
		}
	}
	return false
}
