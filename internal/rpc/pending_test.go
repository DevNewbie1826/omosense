package rpc

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"syscall"
	"testing"
	"time"
)

func pendingRecord(t *testing.T, s *pendingStore, id, handle string) pendingEntry {
	t.Helper()
	e, err := s.Record(id, rpcEvent{Session: handle, Name: ptr("job"), Cwd: ptr("/jobs"), Thread: ptr("7")})
	if err != nil {
		t.Fatal(err)
	}
	return e
}

func pendingList(t *testing.T, s *pendingStore) []pendingEntry {
	t.Helper()
	es, err := s.List()
	if err != nil {
		t.Fatal(err)
	}
	b, err := os.ReadFile(s.path)
	if err == nil {
		t.Logf("persisted JSON: %s", b)
	}
	return es
}

func TestPendingRestartHandleChange(t *testing.T) {
	dir := t.TempDir()
	s := newPendingStore(dir)
	first := pendingRecord(t, s, "D7", "rpc-7")
	restarted := newPendingStore(dir)
	if es := pendingList(t, restarted); len(es) != 1 || es[0].ID != "D7" {
		t.Fatalf("restart lost durable completion: %+v", es)
	}
	second := pendingRecord(t, restarted, "D7", "rpc-9")
	es := pendingList(t, newPendingStore(dir))
	if len(es) != 1 || es[0].ID != "D7" || second.Seq != 2 || second.Count != 2 ||
		es[0].Session != "rpc-9" || second.FirstAt != first.FirstAt {
		t.Fatalf("handle change split or lost completion: %+v", es)
	}
}

func TestPendingStaleAck(t *testing.T) {
	s := newPendingStore(t.TempDir())
	first := pendingRecord(t, s, "D7", "rpc-7")
	second := pendingRecord(t, s, "D7", "rpc-7")
	result, err := s.Ack("D7", first.Seq, true)
	if err != nil || result != "newer" {
		t.Fatalf("stale ack: %s, %v", result, err)
	}
	if es := pendingList(t, s); len(es) != 1 || es[0].Seq != second.Seq {
		t.Fatalf("stale ack cleared newer completion: %+v", es)
	}
	for _, want := range []string{"acked", "not_pending"} {
		result, err = s.Ack("D7", 0, false)
		if err != nil || result != want {
			t.Fatalf("ack: %s, %v; want %s", result, err, want)
		}
	}
}

func TestPendingConcurrentRecordAck(t *testing.T) {
	dir := t.TempDir()
	s := newPendingStore(dir)
	pendingRecord(t, s, "D7", "rpc-7")
	// Probe from inside Record's timestamp hook. LOCK_NB must fail on an
	// independent descriptor; this proves the real critical section without
	// relying on timing luck to provoke lost updates.
	oldNow := nowFn
	t.Cleanup(func() { nowFn = oldNow })
	errs := make(chan error, 64)
	nowFn = func() time.Time {
		f, err := os.OpenFile(s.lockPath, os.O_RDWR, 0o600)
		if err != nil {
			errs <- err
			return oldNow()
		}
		defer f.Close()
		err = syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
		if err == nil {
			syscall.Flock(int(f.Fd()), syscall.LOCK_UN)
			errs <- fmt.Errorf("Record does not hold exclusive flock")
		} else if err != syscall.EWOULDBLOCK {
			errs <- err
		}
		return oldNow()
	}
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := 0; i < 24; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			other := newPendingStore(dir)
			if _, err := other.Record("D7", rpcEvent{Session: "rpc-9"}); err != nil {
				errs <- err
			}
			if result, err := other.Ack("D7", 0, true); err != nil || result != "newer" {
				errs <- fmt.Errorf("stale ack %s: %v", result, err)
			}
		}()
	}
	close(start)
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
	es := pendingList(t, s)
	if len(es) != 1 || es[0].Count != 25 || es[0].Seq != 25 {
		t.Fatalf("lost concurrent update: %+v", es)
	}
}

func TestPendingEmptyID(t *testing.T) {
	s := newPendingStore(t.TempDir())
	if _, err := s.Record("", rpcEvent{Session: "rpc-7"}); err == nil {
		t.Fatal("accepted empty durable id")
	}
	if es := pendingList(t, s); len(es) != 0 {
		t.Fatalf("created handle-keyed entry: %+v", es)
	}
}

func TestPendingCorruptFile(t *testing.T) {
	for _, data := range []string{`{`, `null`, `{"version":2,"entries":{}}`, `{"version":1,"entries":null}`} {
		t.Run(data, func(t *testing.T) {
			s := newPendingStore(t.TempDir())
			if err := os.WriteFile(s.path, []byte(data), 0o600); err != nil {
				t.Fatal(err)
			}
			if _, err := s.Record("D7", rpcEvent{}); err == nil {
				t.Fatal("corrupt file accepted")
			}
			if _, err := s.List(); err == nil {
				t.Fatal("corrupt list accepted")
			}
			if _, err := s.Ack("D7", 0, false); err == nil {
				t.Fatal("corrupt ack accepted")
			}
			b, err := os.ReadFile(s.path)
			if err != nil || string(b) != data {
				t.Fatalf("corrupt file overwritten: %s %v", b, err)
			}
		})
	}
}

// TestPendingNotifiedSeq pins the IS-6 file contract: un-notified means
// seq > notified_seq, and notified_seq never regresses.
func TestPendingNotifiedSeq(t *testing.T) {
	s := newPendingStore(t.TempDir())
	pendingRecord(t, s, "Z", "rpc-1")
	second := pendingRecord(t, s, "A", "rpc-2")
	un, err := s.Unnotified()
	if err != nil || len(un) != 2 || un[0].ID != "Z" || un[1].ID != "A" {
		t.Fatalf("un-notified = %+v %v", un, err)
	}
	if changed, err := s.MarkNotified(1); err != nil || !changed {
		t.Fatalf("first mark = %v %v", changed, err)
	}
	un, err = s.Unnotified()
	if err != nil || len(un) != 1 || un[0].ID != "A" || un[0].Seq != second.Seq {
		t.Fatalf("after mark = %+v %v", un, err)
	}
	if changed, err := s.MarkNotified(1); err != nil || changed {
		t.Fatalf("notified_seq regressed: %v %v", changed, err)
	}
	if changed, err := s.MarkNotified(second.Seq); err != nil || !changed {
		t.Fatalf("second mark = %v %v", changed, err)
	}
	if un, err = s.Unnotified(); err != nil || len(un) != 0 {
		t.Fatalf("un-notified after the batch = %+v %v", un, err)
	}
	b, err := os.ReadFile(s.path)
	if err != nil {
		t.Fatal(err)
	}
	var f pendingFile
	if err := json.Unmarshal(b, &f); err != nil || f.NotifiedSeq != second.Seq || len(f.Entries) != 2 {
		t.Fatalf("file = %s, %v", b, err)
	}
}

func TestPendingOrderingAndFormat(t *testing.T) {
	s := newPendingStore(t.TempDir())
	if es := pendingList(t, s); len(es) != 0 {
		t.Fatal(es)
	}
	pendingRecord(t, s, "Z", "rpc-1")
	pendingRecord(t, s, "A", "rpc-2")
	es := pendingList(t, s)
	if len(es) != 2 || es[0].ID != "Z" || es[1].ID != "A" {
		t.Fatal(es)
	}
	b, err := os.ReadFile(filepath.Join(filepath.Dir(s.path), "rpc-pending.json"))
	if err != nil {
		t.Fatal(err)
	}
	var f pendingFile
	if err := json.Unmarshal(b, &f); err != nil || f.Version != 1 || f.Seq != 2 || f.NotifiedSeq != 0 {
		t.Fatalf("file format: %s, %v", b, err)
	}
	if _, err := time.Parse("2006-01-02T15:04:05.000Z", es[0].DoneAt); err != nil {
		t.Fatal(err)
	}
}
