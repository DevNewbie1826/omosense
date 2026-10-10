package herdr

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/DevNewbie1826/omosense/internal/core"
)

// TestRemoteMachineBackoff guards the failing-machine retry: a remote machine
// whose agent list keeps failing is asked again after 10s, 20s, 40s ... capped
// at 5m, logs only its first failure even when the message changes, logs one
// recovery line on success and is then asked every tick again. local is asked
// every tick throughout.
func TestRemoteMachineBackoff(t *testing.T) {
	unsetEnv(t, "HERDR_PANE_ID")
	dir := t.TempDir()
	installFakeHerdr(t, dir)
	writeFile(t, filepath.Join(dir, "machines.out"), `[{"label":"box"}]`)
	writeFile(t, filepath.Join(dir, "local.out"), `{"result":{"agents":[]}}`)
	writeFile(t, filepath.Join(dir, "box.code"), "1\n")
	advance := fakeClock(t, time.Date(2026, 10, 5, 0, 0, 0, 0, time.UTC))
	var buf bytes.Buffer
	w := newWatcher(testCtx(t.TempDir(), "", &buf), core.NewOut(&buf))

	// asked runs one tick and reports whether box was queried.
	n := 0
	asked := func(errText string) bool {
		t.Helper()
		writeFile(t, filepath.Join(dir, "box.err"), errText)
		if err := os.Remove(filepath.Join(dir, "calls")); err != nil && !os.IsNotExist(err) {
			t.Fatal(err)
		}
		if err := w.tick(context.Background(), false); err != nil {
			t.Fatal(err)
		}
		b, err := os.ReadFile(filepath.Join(dir, "calls"))
		if err != nil {
			t.Fatal(err)
		}
		calls := string(b)
		if !strings.Contains(calls, "\nagent list\n") {
			t.Fatalf("tick %d did not ask local:\n%s", n, calls)
		}
		n++
		return strings.Contains(calls, "--machine box agent list")
	}

	if !asked("down 0\n") {
		t.Fatal("first tick did not ask box")
	}
	// Each wait is the delay before box is asked again; every tick in between
	// (5s apart) must skip it.
	for i, wait := range []time.Duration{10, 20, 40, 80, 160, 300, 300} {
		wait *= time.Second
		for el := 5 * time.Second; el < wait; el += 5 * time.Second {
			advance(5 * time.Second)
			if asked("down skipped\n") {
				t.Fatalf("retry %d: box asked %s after the failure, want %s", i+1, el, wait)
			}
		}
		advance(5 * time.Second)
		if !asked("down " + string(rune('1'+i)) + "\n") {
			t.Fatalf("retry %d: box not asked after %s", i+1, wait)
		}
	}
	wantLines(t, linesOf(&buf), "LOG herdr box Error: down 0")

	buf.Reset()
	removeFile(t, filepath.Join(dir, "box.code"))
	writeFile(t, filepath.Join(dir, "box.out"), `{"result":{"agents":[]}}`)
	advance(300 * time.Second)
	if !asked("") {
		t.Fatal("box not asked after the capped delay")
	}
	wantLines(t, linesOf(&buf), "LOG herdr box recovered")
	advance(5 * time.Second)
	if !asked("") {
		t.Fatal("box not asked on the next tick after recovering")
	}
}
