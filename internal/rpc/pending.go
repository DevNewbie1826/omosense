package rpc

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"syscall"
	"time"

	"github.com/DevNewbie1826/omosense/internal/core"
)

var nowFn = time.Now

type pendingEntry struct {
	ID      string  `json:"id"`
	Session string  `json:"session"`
	Name    *string `json:"name"`
	Cwd     *string `json:"cwd"`
	Thread  *string `json:"thread"`
	Seq     uint64  `json:"seq"`
	Count   uint64  `json:"count"`
	FirstAt string  `json:"first_at"`
	DoneAt  string  `json:"done_at"`
	// Verify is the done-verification state: "pending" while the hook runs,
	// then "verified" or "unverified". Both fields are omitted when empty, so
	// a folder without a hook keeps the 0.1.0 pending JSON byte-for-byte.
	Verify       string `json:"verify,omitempty"`
	VerifyDetail string `json:"verify_detail,omitempty"`
}

// pendingFile is the on-disk rpc-pending.json. Seq is the last sequence
// handed out; NotifiedSeq is the newest sequence a batch has already covered,
// so an entry is un-notified exactly when its Seq is greater.
type pendingFile struct {
	Version     int                     `json:"version"`
	Seq         uint64                  `json:"seq"`
	NotifiedSeq uint64                  `json:"notified_seq"`
	Entries     map[string]pendingEntry `json:"entries"`
}

type pendingStore struct {
	path, lockPath string
}

func newPendingStore(stateDir string) *pendingStore {
	base := filepath.Join(stateDir, "rpc-pending")
	return &pendingStore{path: base + ".json", lockPath: base + ".lock"}
}

// Each operation opens its own lock descriptor: flock must also serialize
// goroutines and separate store instances, not just separate processes.
func (s *pendingStore) transact(fn func(*pendingFile) bool) error {
	if err := os.MkdirAll(filepath.Dir(s.path), 0o755); err != nil {
		return err
	}
	lock, err := os.OpenFile(s.lockPath, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return err
	}
	defer lock.Close()
	if err := syscall.Flock(int(lock.Fd()), syscall.LOCK_EX); err != nil {
		return err
	}
	defer syscall.Flock(int(lock.Fd()), syscall.LOCK_UN)
	f := pendingFile{Version: 1, Entries: map[string]pendingEntry{}}
	b, err := os.ReadFile(s.path)
	if err == nil {
		f = pendingFile{}
		if err := json.Unmarshal(b, &f); err != nil {
			return fmt.Errorf("pending file: %w", err)
		}
		if f.Version != 1 || f.Entries == nil {
			return errors.New("pending file: invalid version or entries")
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	if !fn(&f) {
		return nil
	}
	b, err = json.Marshal(f)
	if err != nil {
		return err
	}
	tmp, err := os.CreateTemp(filepath.Dir(s.path), ".rpc-pending-*")
	if err != nil {
		return err
	}
	defer os.Remove(tmp.Name())
	if _, err = tmp.Write(b); err == nil {
		err = tmp.Sync()
	}
	closeErr := tmp.Close()
	if err != nil {
		return err
	}
	if closeErr != nil {
		return closeErr
	}
	return os.Rename(tmp.Name(), s.path)
}

// Record upserts the completion of id. verifying marks the entry pending for
// the done-verification hook; a record made without a hook clears any earlier
// verification state, so a newer done of the same id never inherits the
// verdict of the one it replaced (IS-7).
func (s *pendingStore) Record(id string, ev rpcEvent, verifying bool) (pendingEntry, error) {
	if id == "" {
		return pendingEntry{}, errors.New("pending: empty durable id")
	}
	var out pendingEntry
	err := s.transact(func(f *pendingFile) bool {
		e := f.Entries[id]
		now := core.ISO(nowFn())
		f.Seq++
		e.ID, e.Session, e.Name, e.Cwd, e.Thread = id, ev.Session, ev.Name, ev.Cwd, ev.Thread
		e.Seq, e.Count = f.Seq, e.Count+1
		if e.FirstAt == "" {
			e.FirstAt = now
		}
		e.DoneAt = now
		if verifying {
			e.Verify, e.VerifyDetail = "pending", ""
		} else {
			e.Verify, e.VerifyDetail = "", ""
		}
		f.Entries[id] = e
		out = e
		return true
	})
	return out, err
}

// SetVerify writes a hook result onto the entry carrying seq. The sequence
// names the very done the hook was started for: a result that arrives after
// the id was recorded again, or acked, is discarded instead of overwriting a
// newer done. The write never touches notified_seq, so a result landing after
// its batch stays readable through `omosense rpc pending` without arming
// another send (IS-7).
func (s *pendingStore) SetVerify(id string, seq uint64, status, detail string) (bool, error) {
	written := false
	err := s.transact(func(f *pendingFile) bool {
		e, ok := f.Entries[id]
		if !ok || e.Seq != seq {
			return false
		}
		if e.Verify == status && e.VerifyDetail == detail {
			return false
		}
		e.Verify, e.VerifyDetail = status, detail
		f.Entries[id] = e
		written = true
		return true
	})
	return written, err
}

func (s *pendingStore) Ack(id string, seq uint64, haveSeq bool) (string, error) {
	result := "not_pending"
	err := s.transact(func(f *pendingFile) bool {
		e, ok := f.Entries[id]
		if !ok {
			return false
		}
		if haveSeq && seq != e.Seq {
			result = "newer"
			return false
		}
		delete(f.Entries, id)
		result = "acked"
		return true
	})
	return result, err
}

func (s *pendingStore) List() ([]pendingEntry, error) {
	out := []pendingEntry{}
	err := s.transact(func(f *pendingFile) bool {
		for _, e := range f.Entries {
			out = append(out, e)
		}
		return false
	})
	sort.Slice(out, func(i, j int) bool { return out[i].Seq < out[j].Seq })
	return out, err
}

// Unnotified lists the entries no batch has covered yet, oldest first, so the
// last element carries the newest sequence of the batch window.
func (s *pendingStore) Unnotified() ([]pendingEntry, error) {
	out := []pendingEntry{}
	err := s.transact(func(f *pendingFile) bool {
		for _, e := range f.Entries {
			if e.Seq > f.NotifiedSeq {
				out = append(out, e)
			}
		}
		return false
	})
	sort.Slice(out, func(i, j int) bool { return out[i].Seq < out[j].Seq })
	return out, err
}

// MarkNotified advances notified_seq to seq. It never regresses, so a batch
// whose send overlapped a newer Record leaves the newer sequence un-notified
// and arms the next window. It reports whether the file changed.
func (s *pendingStore) MarkNotified(seq uint64) (bool, error) {
	changed := false
	err := s.transact(func(f *pendingFile) bool {
		if seq <= f.NotifiedSeq {
			return false
		}
		f.NotifiedSeq = seq
		changed = true
		return true
	})
	return changed, err
}
