package remind

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/DevNewbie1826/omosense/internal/core"
)

func runAdd(t *testing.T, ctx *core.Ctx, args ...string) (int, string, string) {
	t.Helper()
	var out, errb bytes.Buffer
	code := add(ctx, args, &out, &errb)
	return code, out.String(), errb.String()
}

func entries(t *testing.T, ctx *core.Ctx) []*core.OMap {
	t.Helper()
	v, err := core.ParseJSON(mustRead(t, remindersFile(ctx)))
	if err != nil {
		t.Fatal(err)
	}
	var out []*core.OMap
	for _, e := range v.([]any) {
		out = append(out, e.(*core.OMap))
	}
	return out
}

// TestAddWritesSchemaEntry: --in is resolved against the injected clock and
// the entry has exactly the scheduler's fields, appended after existing ones.
func TestAddWritesSchemaEntry(t *testing.T) {
	ctx := loadCtx(t)
	withHooks(t, fixedTime, "", nil)
	if err := os.MkdirAll(ctx.State, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(remindersFile(ctx), []byte(`[{"id":"old","at":"2030-01-01T00:00:00.000Z","platform":"discord","target":{"channel_id":5},"text":"later","x":1}]`), 0o644); err != nil {
		t.Fatal(err)
	}
	code, out, errOut := runAdd(t, ctx, "--in", "30m", "--platform", "telegram", "--target", `{"chat_id":42}`, "--text", "stand-up 안", "--id", "r1")
	if code != 0 {
		t.Fatalf("exit %d, stderr %q", code, errOut)
	}
	want := `{"id":"r1","at":"2026-10-03T10:35:00.000Z","platform":"telegram","target":{"chat_id":42},"text":"stand-up 안"}`
	if out != "REMIND added "+want+"\n" {
		t.Fatalf("stdout = %q", out)
	}
	es := entries(t, ctx)
	if len(es) != 2 {
		t.Fatalf("entries = %d, want 2", len(es))
	}
	if b, _ := es[0].Marshal(); !strings.Contains(string(b), `"x":1`) {
		t.Errorf("existing entry changed: %s", b)
	}
	if b, _ := es[1].Marshal(); string(b) != want {
		t.Errorf("added entry = %s, want %s", b, want)
	}
}

// TestAddBadInput: every invalid invocation exits 2 with the usage and
// leaves no file behind.
func TestAddBadInput(t *testing.T) {
	ctx := loadCtx(t)
	withHooks(t, fixedTime, "", nil)
	ok := []string{"--platform", "telegram", "--target", `{"chat_id":1}`, "--text", "x"}
	cases := map[string][]string{
		"no time":        ok,
		"both times":     append([]string{"--at", "2026-10-03T11:00:00Z", "--in", "1m"}, ok...),
		"bad at":         append([]string{"--at", "tomorrow"}, ok...),
		"bad in":         append([]string{"--in", "soon"}, ok...),
		"negative in":    append([]string{"--in", "-1m"}, ok...),
		"too old":        append([]string{"--at", "2026-10-03T04:04:00Z"}, ok...),
		"bad platform":   {"--in", "1m", "--platform", "slack", "--target", `{"chat_id":1}`, "--text", "x"},
		"target array":   {"--in", "1m", "--platform", "telegram", "--target", `[1]`, "--text", "x"},
		"no text":        {"--in", "1m", "--platform", "telegram", "--target", `{"chat_id":1}`},
		"unknown flag":   append([]string{"--in", "1m", "--every", "1h"}, ok...),
		"stray argument": append([]string{"--in", "1m", "extra"}, ok...),
	}
	for name, args := range cases {
		code, out, errOut := runAdd(t, ctx, args...)
		if code != 2 || out != "" || !strings.Contains(errOut, "Usage: omosense remind add") {
			t.Errorf("%s: exit %d stdout %q stderr %q, want 2 + usage", name, code, out, errOut)
		}
	}
	if _, err := os.Stat(remindersFile(ctx)); !os.IsNotExist(err) {
		t.Errorf("bad input wrote reminders.json: %v", err)
	}
}

// TestAddPastWithinLateWindowSendsNextTick: a time up to 6h in the past is
// accepted and the scheduler's next tick sends it (its late-delivery rule).
func TestAddPastWithinLateWindowSendsNextTick(t *testing.T) {
	ctx := loadCtx(t)
	capture := filepath.Join(t.TempDir(), "args")
	withHooks(t, fixedTime, writeFakeSay(t, capture), nil)
	if code, _, errOut := runAdd(t, ctx, "--at", "2026-10-03T05:00:00Z", "--platform", "telegram", "--target", `{"chat_id":7}`, "--text", "late"); code != 0 {
		t.Fatalf("exit %d: %s", code, errOut)
	}
	sink := newChanSink()
	if err := newScheduler(ctx, sink).tick(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := string(mustRead(t, capture)); got != "say telegram send {\"chat_id\":7,\"text\":\"late\"}\n" {
		t.Errorf("say args = %q", got)
	}
	if !doneEntry(entries(t, ctx)[0], "sent") {
		t.Error("entry not marked sent")
	}
}

// TestConcurrentAddsAllKept: parallel adds serialize on reminders.lock.
func TestConcurrentAddsAllKept(t *testing.T) {
	ctx := loadCtx(t)
	withHooks(t, fixedTime, "", nil)
	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			var out, errb bytes.Buffer
			if code := add(ctx, []string{"--in", "1h", "--platform", "telegram", "--target", `{"chat_id":1}`, "--text", "x", "--id", fmt.Sprint("c", i)}, &out, &errb); code != 0 {
				t.Errorf("add %d exit %d: %s", i, code, errb.String())
			}
		}(i)
	}
	wg.Wait()
	if n := len(entries(t, ctx)); n != 16 {
		t.Fatalf("entries = %d, want 16", n)
	}
}

// TestAddDuringTickNotLost: a tick holds reminders.lock while its say send
// runs, so an add made mid-send lands after the tick's rewrite instead of
// being overwritten by it. The fake say blocks on a FIFO the test releases.
// Whether the lock is held is checked with LOCK_NB, which only fixes the
// order of the two writers - the assertion is the final file either way.
func TestAddDuringTickNotLost(t *testing.T) {
	ctx := loadCtx(t)
	tmp := t.TempDir()
	inSend, release := filepath.Join(tmp, "in-send"), filepath.Join(tmp, "release")
	for _, p := range []string{inSend, release} {
		if err := syscall.Mkfifo(p, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	say := filepath.Join(tmp, "say")
	if err := os.WriteFile(say, []byte("#!/bin/sh\nprintf x > \"$IN_SEND\"\nread _ < \"$RELEASE\"\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("IN_SEND", inSend)
	t.Setenv("RELEASE", release)
	withHooks(t, fixedTime, say, nil)
	if err := os.MkdirAll(ctx.State, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(remindersFile(ctx), []byte(`[{"id":"due","at":"2026-10-03T10:00:00.000Z","platform":"telegram","target":{"chat_id":1},"text":"due"}]`), 0o644); err != nil {
		t.Fatal(err)
	}

	tickDone := make(chan error, 1)
	go func() { tickDone <- newScheduler(ctx, newChanSink()).tick(context.Background()) }()
	if _, err := os.ReadFile(inSend); err != nil { // returns once say is mid-send
		t.Fatal(err)
	}

	addDone := make(chan int, 1)
	doAdd := func() {
		var out, errb bytes.Buffer
		addDone <- add(ctx, []string{"--in", "1h", "--platform", "telegram", "--target", `{"chat_id":2}`, "--text", "new", "--id", "added"}, &out, &errb)
	}
	if lockHeld(t, ctx.State) {
		go doAdd()
	} else {
		doAdd()
	}
	if err := os.WriteFile(release, []byte("\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-tickDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("tick did not finish")
	}
	select {
	case code := <-addDone:
		if code != 0 {
			t.Fatalf("add exit %d", code)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("add did not finish")
	}

	es := entries(t, ctx)
	ids := []string{}
	for _, e := range es {
		ids = append(ids, fieldStr(e, "id"))
	}
	if strings.Join(ids, ",") != "due,added" || !doneEntry(es[0], "sent") {
		t.Fatalf("entries = %v (due sent=%v), want due(sent),added", ids, len(es) > 0 && doneEntry(es[0], "sent"))
	}
}

func lockHeld(t *testing.T, state string) bool {
	t.Helper()
	f, err := os.OpenFile(filepath.Join(state, "reminders.lock"), os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		return true
	}
	syscall.Flock(int(f.Fd()), syscall.LOCK_UN)
	return false
}
