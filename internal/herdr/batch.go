package herdr

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"github.com/DevNewbie1826/omosense/internal/core"
)

// batchQuiet is the IS-3 quiet window: a job pane's done burst prints one
// done-batch line five quiet minutes after its newest transition, the same
// rule the rpc batcher applies through core.QuietDue. It is a var so a test
// can shorten it.
var batchQuiet = 5 * time.Minute

// doneEntry is one recorded job-pane transition: an IS-3 batch entry. The
// JSON fields are the owner's line fields. key and seq are in-process
// bookkeeping - never serialized - so a hook verdict is applied only to the
// very transition it was started for, and a collapsed-over verdict is
// discarded.
type doneEntry struct {
	Machine string  `json:"machine"`
	Pane    *string `json:"pane"`
	Tab     *string `json:"tab"`
	Agent   *string `json:"agent"`
	Cwd     *string `json:"cwd"`
	From    *string `json:"from"`
	To      string  `json:"to"`
	At      string  `json:"at"`
	FirstAt string  `json:"first_at"`
	Count   int     `json:"count"`
	// Verify is the opt-in herdr.done verdict, exactly like the held HERDR
	// line's fields: pending while the hook runs, then verified/unverified.
	Verify       string `json:"verify,omitempty"`
	VerifyDetail string `json:"verify_detail,omitempty"`

	key string
	seq uint64
}

// doneBatch is the IS-3 line payload: one HERDR line for a whole quiet burst.
type doneBatch struct {
	Event   string      `json:"event"`
	Entries []doneEntry `json:"entries"`
}

// pendingFile is the on-disk <state>/herdr-pending.json. It carries the same
// entries the line prints, so a restart resumes the identical quiet rule.
type pendingFile struct {
	Version int                  `json:"version"`
	Entries map[string]doneEntry `json:"entries"`
}

// doneStore is the IS-3/4/5 quiet batch: recorded transitions, persisted
// before the tick continues, printed once as one line, then cleared. Its
// mutex serializes the tick goroutine and the hook goroutines, and is held
// across the flush so the print and the clear cannot interleave.
type doneStore struct {
	path    string
	mu      sync.Mutex
	seq     uint64
	entries map[string]doneEntry
}

func newDoneStore(stateDir string) *doneStore {
	return &doneStore{
		path:    filepath.Join(stateDir, "herdr-pending.json"),
		entries: map[string]doneEntry{},
	}
}

// record upserts one job pane's transition. A repeated transition of the same
// pane collapses into the existing entry: the last from/to/tab/agent/cwd/at
// win, count is the number of transitions, and first_at keeps the first one.
// Every record hands out a fresh sequence, so a verdict whose sequence no
// longer matches the entry is discarded rather than overwriting a newer one.
func (s *doneStore) record(key string, ev herdrEvent, at time.Time, verifying bool) (doneEntry, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.seq++
	e := s.entries[key]
	e.Machine, e.Pane, e.Tab, e.Agent = ev.Machine, ev.Pane, ev.Tab, ev.Agent
	e.Cwd, e.From, e.To = ev.Cwd, ev.From, ev.To
	e.At = core.ISO(at)
	if e.FirstAt == "" {
		e.FirstAt = e.At
	}
	e.Count++
	e.key, e.seq = key, s.seq
	if verifying {
		e.Verify, e.VerifyDetail = "pending", ""
	} else {
		e.Verify, e.VerifyDetail = "", ""
	}
	s.entries[key] = e
	if err := s.save(); err != nil {
		return doneEntry{}, err
	}
	return e, nil
}

// setVerify writes a hook verdict onto the entry carrying seq. The sequence
// names the very transition the hook was started for: a verdict for a
// collapsed-over transition is discarded instead of overwriting a newer one.
func (s *doneStore) setVerify(key string, seq uint64, status, detail string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	e, ok := s.entries[key]
	if !ok || e.seq != seq {
		return nil
	}
	if e.Verify == status && e.VerifyDetail == detail {
		return nil
	}
	e.Verify, e.VerifyDetail = status, detail
	s.entries[key] = e
	return s.save()
}

// flush prints the whole store as one done-batch line and clears it, under the
// mutex so the print, the clear and the save cannot interleave with a record.
// It reports whether a line was printed. An empty store prints nothing, and an
// entry whose `at` cannot be parsed contributes the zero time, so a damaged
// record fires at once instead of never (the rpc lastDoneAt precedent).
func (s *doneStore) flush(sink core.Sink, now time.Time, quiet time.Duration) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.entries) == 0 {
		return false, nil
	}
	if now.Before(core.QuietDue(s.newestAt(), time.Time{}, quiet)) {
		return false, nil
	}
	sink.Emit("HERDR", doneBatch{Event: "done-batch", Entries: s.sorted()})
	s.entries = map[string]doneEntry{}
	return true, s.save()
}

func (s *doneStore) newestAt() time.Time {
	var out time.Time
	for _, e := range s.entries {
		t, err := time.Parse(time.RFC3339Nano, e.At)
		if err != nil {
			continue
		}
		if t.After(out) {
			out = t
		}
	}
	return out
}

// sorted is the IS-3 entry order: by first_at, then by key so the order is
// total. first_at is core.ISO, so the string order is the chronological one.
func (s *doneStore) sorted() []doneEntry {
	out := make([]doneEntry, 0, len(s.entries))
	for _, e := range s.entries {
		out = append(out, e)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].FirstAt != out[j].FirstAt {
			return out[i].FirstAt < out[j].FirstAt
		}
		return out[i].key < out[j].key
	})
	return out
}

// load reads the persisted store. A missing file is an empty store and
// creates nothing - the state dir stays absent on a read-only path. A
// malformed file is returned as an error so the caller can preserve it
// instead of silently overwriting it.
func (s *doneStore) load() error {
	b, err := os.ReadFile(s.path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	var f pendingFile
	if err := json.Unmarshal(b, &f); err != nil {
		return fmt.Errorf("herdr-pending.json: %w", err)
	}
	if f.Version != 1 || f.Entries == nil {
		return errors.New("herdr-pending.json: invalid version or entries")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.entries = f.Entries
	for k, e := range s.entries {
		e.key = k
		s.entries[k] = e
	}
	s.seq = uint64(len(s.entries))
	return nil
}

// quarantine moves a malformed store aside so the next save starts empty
// without destroying whatever the file held.
func (s *doneStore) quarantine() error {
	dst := fmt.Sprintf("%s.bad-%d", s.path, nowFn().Unix())
	return os.Rename(s.path, dst)
}

// save writes the store with a temp file and a rename, so a reader never sees
// a half-written file. The state dir is created only when there is something
// to write.
func (s *doneStore) save() error {
	b, err := json.Marshal(pendingFile{Version: 1, Entries: s.entries})
	if err != nil {
		return err
	}
	dir := filepath.Dir(s.path)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(dir, ".herdr-pending-*")
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
