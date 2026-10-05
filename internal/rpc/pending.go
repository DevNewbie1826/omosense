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
	ID          string  `json:"id"`
	Session     string  `json:"session"`
	Name        *string `json:"name"`
	Cwd         *string `json:"cwd"`
	Thread      *string `json:"thread"`
	Seq         uint64  `json:"seq"`
	Count       uint64  `json:"count"`
	FirstAt     string  `json:"first_at"`
	DoneAt      string  `json:"done_at"`
	Attempts    uint64  `json:"attempts"`
	DeliveredAt string  `json:"delivered_at"`
	NextAt      string  `json:"next_at"`
	LastError   string  `json:"last_error"`
}

type pendingFile struct {
	Version int                     `json:"version"`
	Seq     uint64                  `json:"seq"`
	Entries map[string]pendingEntry `json:"entries"`
}

type pendingStore struct {
	path, lockPath, profile string
}

func newPendingStore(stateDir, profile string) *pendingStore {
	base := filepath.Join(stateDir, "rpc-pending-"+profile)
	return &pendingStore{path: base + ".json", lockPath: base + ".lock", profile: profile}
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

func (s *pendingStore) Record(id string, ev rpcEvent) (pendingEntry, error) {
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
		e.DoneAt, e.NextAt = now, now
		e.Attempts, e.LastError = 0, ""
		f.Entries[id] = e
		out = e
		return true
	})
	return out, err
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

// update applies a send result only to the exact completion and attempt sent.
// An ACK or a newer Record during exec must never be undone by its result.
func (s *pendingStore) update(sent pendingEntry, fn func(*pendingEntry)) (bool, error) {
	applied := false
	err := s.transact(func(f *pendingFile) bool {
		e, ok := f.Entries[sent.ID]
		if !ok || e.Seq != sent.Seq || e.Attempts != sent.Attempts {
			return false
		}
		fn(&e)
		f.Entries[e.ID] = e
		applied = true
		return true
	})
	return applied, err
}
