package tidy

import (
	"bytes"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/DevNewbie1826/omosense/internal/core"
)

// TestWatermarkConcurrentBackupAndWriteWatermark is the IS-1/IS-2 guard:
// the daily backup stamp and --write-watermark serialize on
// <state>/memory-tidy.json.lock, so neither run loses the other's fields,
// AND the second writer reads the watermark only after the first writer
// wrote it. The backup writer is parked inside the lock by the hook (after
// its own in-lock read) and a LOCK_EX|LOCK_NB probe proves the lock is
// held; only then is the --write-watermark writer started. The readiness
// signal is emitted by the watermarkFlockFn wrapper immediately before it
// calls the real syscall.Flock, so it marks the acquisition itself: a
// correct updateWatermark has read nothing when it fires, while a re-read
// hoisted above the acquisition, or a stale pre-lock document, has already
// read by then - so the guard fails even if the second writer is
// descheduled between the signal and the Flock. Releasing the backup writer
// then forces the order write(1), read(2), write(2), with read(1) already
// recorded. Fails when the Flock call is removed (the probe succeeds).
func TestWatermarkConcurrentBackupAndWriteWatermark(t *testing.T) {
	_, agents, state := sandbox(t)
	shaA := commitAt(t, makeRepo(t, agents, "alpha"), testEpoch, "a1")
	start := time.UnixMilli((testEpoch + 90*60) * 1000)
	fakeClock(t, start)
	date := seoulDateOf(t, start)
	wm := filepath.Join(state, "memory-tidy.json")
	writeFile(t, wm, `{"repos":{},"lastRun":null,"lastBackupDate":null}`)

	// watermarkEventHook records the writers' externally visible steps so
	// the ordering can be asserted: "read" after every watermark read,
	// "prelock" immediately before a writer requests the lock, "write"
	// immediately after it writes the document. Everything recorded before
	// the backup writer enters the lock belongs to it, so the prelock that
	// arrives afterwards is the --write-watermark writer's.
	var mu sync.Mutex
	var events []string
	reads, writes := 0, 0
	afterEntered := false
	prelock2 := make(chan struct{})
	var prelock2Once sync.Once
	prevEvent := watermarkEventHook
	watermarkEventHook = func(ev string) {
		mu.Lock()
		events = append(events, ev)
		switch ev {
		case "read":
			reads++
		case "write":
			writes++
		case "prelock":
			if afterEntered {
				prelock2Once.Do(func() { close(prelock2) })
			}
		}
		mu.Unlock()
	}
	t.Cleanup(func() { watermarkEventHook = prevEvent })

	// Emit the "prelock" readiness signal from the acquisition itself: the
	// wrapper signals and then calls the real Flock, so any production read
	// placed above the acquisition necessarily runs before this signal - and
	// therefore before the test can release the first writer.
	prevFlock := watermarkFlockFn
	watermarkFlockFn = func(fd, how int) error {
		if watermarkEventHook != nil {
			watermarkEventHook("prelock")
		}
		return syscall.Flock(fd, how)
	}
	t.Cleanup(func() { watermarkFlockFn = prevFlock })

	entered := make(chan struct{})
	release := make(chan struct{})
	calls := 0
	prev := watermarkLockedHook
	watermarkLockedHook = func() {
		mu.Lock()
		calls++
		n := calls
		mu.Unlock()
		if n == 1 {
			close(entered)
			<-release
		}
	}
	t.Cleanup(func() { watermarkLockedHook = prev })

	var wg sync.WaitGroup
	codes := make([]int, 2)
	wg.Add(2)
	go func() {
		defer wg.Done()
		var buf bytes.Buffer
		c := testCtx(state, "main", &buf)
		c.Flags["--backup-now"] = true
		codes[0] = Run(c, []string{"--backup-now"})
	}()
	select {
	case <-entered:
	case <-time.After(30 * time.Second):
		t.Fatal("the backup writer never entered the watermark lock")
	}

	// The backup writer holds the lock now, so every event the hook records
	// from here on belongs to the --write-watermark writer.
	mu.Lock()
	readsBefore := reads
	afterEntered = true
	mu.Unlock()
	if readsBefore == 0 {
		t.Fatal("the event hook recorded no watermark read: the lock/read instrumentation is inert")
	}

	probe, err := os.OpenFile(filepath.Join(state, "memory-tidy.json.lock"), os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		t.Fatalf("open the lock probe: %v", err)
	}
	defer probe.Close()
	if err := syscall.Flock(int(probe.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err == nil {
		_ = syscall.Flock(int(probe.Fd()), syscall.LOCK_UN)
		t.Fatal("the lock probe succeeded while the backup writer holds memory-tidy.json.lock")
	} else if !errors.Is(err, syscall.EWOULDBLOCK) {
		t.Fatalf("lock probe error = %v, want EWOULDBLOCK", err)
	}

	go func() {
		defer wg.Done()
		var buf bytes.Buffer
		c := testCtx(state, "main", &buf)
		c.Flags["--write-watermark"] = true
		codes[1] = Run(c, []string{"--write-watermark"})
	}()

	// Wait (no sleep) until the second writer is parked at the lock, then
	// assert it has not read the watermark: a re-read hoisted out of the
	// lock, or a stale pre-lock document, has already read by now.
	select {
	case <-prelock2:
	case <-time.After(30 * time.Second):
		t.Fatal("the --write-watermark writer never reached the watermark lock")
	}
	mu.Lock()
	readsAtLock := reads
	mu.Unlock()
	if readsAtLock != readsBefore {
		t.Fatalf("the --write-watermark writer read the watermark %d time(s) before it requested the lock (reads before = %d, at the lock = %d): the re-read escapes the lock",
			readsAtLock-readsBefore, readsBefore, readsAtLock)
	}

	// Release the backup writer: it writes first, then the second writer
	// reads (now seeing the backup stamp) and writes.
	mu.Lock()
	events = nil
	mu.Unlock()
	close(release)
	wg.Wait()

	mu.Lock()
	gotEvents := append([]string(nil), events...)
	gotCalls, gotWrites := calls, writes
	mu.Unlock()
	if gotCalls != 2 {
		t.Fatalf("updateWatermark calls = %d, want 2 (one per writer)", gotCalls)
	}
	if gotWrites != 2 {
		t.Fatalf("watermark document writes = %d, want 2 (one per writer)", gotWrites)
	}
	if got := strings.Join(gotEvents, ","); got != "write,read,write" {
		t.Fatalf("events after the backup writer was released = [%s], want [write,read,write]: the second writer must read only after the first wrote", got)
	}
	if codes[0] != 0 || codes[1] != 0 {
		t.Fatalf("exit codes = %v, want both 0", codes)
	}
	body := readFile(t, wm)
	var doc struct {
		Repos          map[string]string `json:"repos"`
		LastRun        *string           `json:"lastRun"`
		LastBackupDate *string           `json:"lastBackupDate"`
	}
	if err := json.Unmarshal([]byte(body), &doc); err != nil {
		t.Fatalf("watermark is not JSON: %v\n%s", err, body)
	}
	if doc.LastBackupDate == nil || *doc.LastBackupDate != date {
		t.Fatalf("lastBackupDate = %v, want %q (the backup stamp was lost):\n%s", doc.LastBackupDate, date, body)
	}
	if doc.LastRun == nil {
		t.Fatalf("lastRun is null (the --write-watermark stamp was lost):\n%s", body)
	}
	if doc.Repos["alpha"] != shaA {
		t.Fatalf("repos[alpha] = %q, want %q (the --write-watermark entry was lost):\n%s", doc.Repos["alpha"], shaA, body)
	}
}

// failWriteFileFn replaces writeFileFn with a fake that writes the real
// temp file and then fails, so the caller's cleanup is observable (D4).
func failWriteFileFn(t *testing.T) {
	t.Helper()
	prev := writeFileFn
	writeFileFn = func(name string, data []byte, perm os.FileMode) error {
		if err := os.WriteFile(name, data, perm); err != nil {
			return err
		}
		return errors.New("no space left on device")
	}
	t.Cleanup(func() { writeFileFn = prev })
}

// TestWriteWatermarkWriteFileFailureRemovesTmp is the IS-4 guard for
// writeWatermarkDoc: a failed WriteFile leaves no <path>.tmp.
func TestWriteWatermarkWriteFileFailureRemovesTmp(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "memory-tidy.json")
	failWriteFileFn(t)

	w := core.NewOMap()
	w.Set("repos", core.NewOMap())
	err := writeWatermarkDoc(path, w)
	if err == nil || !strings.Contains(err.Error(), "no space left on device") {
		t.Fatalf("writeWatermarkDoc error = %v, want the write failure", err)
	}
	if _, statErr := os.Stat(path + ".tmp"); !os.IsNotExist(statErr) {
		t.Fatalf("%s.tmp still exists after the failed write: %v", path, statErr)
	}
	if _, statErr := os.Stat(path); !os.IsNotExist(statErr) {
		t.Fatalf("the destination exists after the failed write: %v", statErr)
	}
}

// TestWriteAnnouncedWriteFileFailureRemovesTmp is the IS-4 guard for
// writeAnnounced: a failed WriteFile leaves no <path>.tmp.
func TestWriteAnnouncedWriteFileFailureRemovesTmp(t *testing.T) {
	_, _, state := sandbox(t)
	failWriteFileFn(t)
	tt := &tidyer{state: state}

	err := tt.writeAnnounced(map[string]emitRec{"alpha": {to: "AAA", at: 1}})
	if err == nil || !strings.Contains(err.Error(), "no space left on device") {
		t.Fatalf("writeAnnounced error = %v, want the write failure", err)
	}
	path := tt.announcedPath()
	if _, statErr := os.Stat(path + ".tmp"); !os.IsNotExist(statErr) {
		t.Fatalf("%s.tmp still exists after the failed write: %v", path, statErr)
	}
	if _, statErr := os.Stat(path); !os.IsNotExist(statErr) {
		t.Fatalf("the destination exists after the failed write: %v", statErr)
	}
}

// TestWriteWatermarkLockOpenFailure is the IS-3 guard for the lock-open
// error path: when <state>/memory-tidy.json.lock cannot be opened,
// --write-watermark exits 1 and leaves the watermark byte-identical. The
// second half is the positive control for the same fixture.
func TestWriteWatermarkLockOpenFailure(t *testing.T) {
	_, _, state := sandbox(t)
	fakeClock(t, time.UnixMilli(1750000000000))
	wm := filepath.Join(state, "memory-tidy.json")
	body := "{\n  \"repos\": {\n    \"alpha\": \"OLD\"\n  },\n  \"lastRun\": null,\n  \"lastBackupDate\": null\n}\n"
	writeFile(t, wm, body)
	lockPath := filepath.Join(state, "memory-tidy.json.lock")
	mkdirAll(t, lockPath)

	var buf bytes.Buffer
	c := testCtx(state, "main", &buf)
	c.Flags["--write-watermark"] = true
	if code := Run(c, []string{"--write-watermark", "beta=BB"}); code != 1 {
		t.Fatalf("Run --write-watermark (lock is a directory) = %d, want 1", code)
	}
	if got := readFile(t, wm); got != body {
		t.Fatalf("the watermark changed despite the lock failure:\ngot:\n%s\nwant:\n%s", got, body)
	}
	if buf.Len() != 0 {
		t.Fatalf("stdout = %q, want no lines", buf.String())
	}

	if err := os.Remove(lockPath); err != nil {
		t.Fatal(err)
	}
	var buf2 bytes.Buffer
	c2 := testCtx(state, "main", &buf2)
	c2.Flags["--write-watermark"] = true
	if code := Run(c2, []string{"--write-watermark", "beta=BB"}); code != 0 {
		t.Fatalf("Run --write-watermark (lock free) = %d, want 0", code)
	}
	if got := readFile(t, wm); !strings.Contains(got, `"beta": "BB"`) {
		t.Fatalf("the control run did not write:\n%s", got)
	}
	wantLines(t, linesOf(&buf2), "LOG memory-tidy watermark set 1 repos")
}
