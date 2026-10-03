package daemon

import (
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"
)

func TestJournalOrderedFilteredReplayExactlyOnce(t *testing.T) {
	now := time.Date(2026, 10, 3, 0, 0, 0, 0, time.UTC)
	j, err := openJournal(filepath.Join(t.TempDir(), "journal.jsonl"), now)
	if err != nil {
		t.Fatal(err)
	}
	defer j.close()
	for _, e := range []struct {
		source, line string
		delivered    bool
	}{
		{"discord", "EVENT {}", false},
		{"remind", "LOG pending", false},
		{"remind", "REMIND sent {}", true},
		{"discord", "LOG pending", false},
	} {
		if err := j.append(now, e.source, e.line, e.delivered); err != nil {
			t.Fatal(err)
		}
	}
	h := hello{Profile: "main", Sources: []string{"discord"}, Only: []string{"LOG"}}
	pending := j.pending(h)
	if len(pending) != 1 || pending[0].Seq != 4 {
		t.Fatalf("filtered pending = %+v", pending)
	}
	if err := j.deliver(pending[0].Seq); err != nil {
		t.Fatal(err)
	}
	if len(j.pending(h)) != 0 {
		t.Fatal("replayed an already delivered line")
	}
	h.Sources, h.Only = []string{"discord", "remind"}, nil
	got := j.pending(h)
	if len(got) != 2 || got[0].Seq != 1 || got[1].Seq != 2 {
		t.Fatalf("remaining order: %+v", got)
	}
}

func TestJournalPersistsPendingAndAppendOnlyMarkersAcrossRestart(t *testing.T) {
	now := time.Date(2026, 10, 3, 0, 0, 0, 0, time.UTC)
	path := filepath.Join(t.TempDir(), "journal.jsonl")
	j, err := openJournal(path, now)
	if err != nil {
		t.Fatal(err)
	}
	if err := j.append(now, "remind", "REMIND sent {}", false); err != nil {
		t.Fatal(err)
	}
	if err := j.append(now, "discord", "EVENT {}", false); err != nil {
		t.Fatal(err)
	}
	if err := j.deliver(1); err != nil {
		t.Fatal(err)
	}
	if err := j.close(); err != nil {
		t.Fatal(err)
	}
	h := hello{Profile: "main", Sources: []string{"remind", "discord"}}
	j, err = openJournal(path, now)
	if err != nil {
		t.Fatal(err)
	}
	defer j.close()
	p := j.pending(h)
	if len(p) != 1 || p[0].Seq != 2 || p[0].Source != "discord" {
		t.Fatalf("restart pending: %+v", p)
	}
	if err := j.append(now, "remind", "REMIND failed {}", false); err != nil {
		t.Fatal(err)
	}
	p = j.pending(h)
	if len(p) != 2 || p[1].Seq != 3 {
		t.Fatalf("sequence reset across restart: %+v", p)
	}
}

func TestJournalRetentionAtStartupAndCompaction(t *testing.T) {
	now := time.Date(2026, 10, 3, 0, 0, 0, 0, time.UTC)
	path := filepath.Join(t.TempDir(), "journal.jsonl")
	j, err := openJournal(path, now)
	if err != nil {
		t.Fatal(err)
	}
	if err := j.append(now.Add(-25*time.Hour), "discord", "EVENT old", false); err != nil {
		t.Fatal(err)
	}
	if err := j.append(now, "remind", "REMIND new", false); err != nil {
		t.Fatal(err)
	}
	j.close()
	j, err = openJournal(path, now)
	if err != nil {
		t.Fatal(err)
	}
	defer j.close()
	h := hello{Profile: "main", Sources: []string{"discord", "remind"}}
	p := j.pending(h)
	if len(p) != 1 || p[0].Line != "REMIND new" {
		t.Fatalf("startup retained old entries: %+v", p)
	}
	if err := j.compact(now.Add(25 * time.Hour)); err != nil {
		t.Fatal(err)
	}
	if len(j.pending(h)) != 0 {
		t.Fatal("compaction retained expired entries")
	}
	j.close()
	j, err = openJournal(path, now.Add(25*time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	defer j.close()
	if err := j.append(now.Add(25*time.Hour), "remind", "REMIND next", false); err != nil {
		t.Fatal(err)
	}
	if got := j.pending(h); len(got) != 1 || got[0].Seq != 3 {
		t.Fatalf("compaction lost sequence highwater: %+v", got)
	}
	if files, err := filepath.Glob(path + ".tmp*"); err != nil || !reflect.DeepEqual(files, []string(nil)) {
		t.Fatalf("compaction leaked temp file: %v %v", files, err)
	}
}

func TestJournalRejectsCorruptRecord(t *testing.T) {
	path := filepath.Join(t.TempDir(), "journal.jsonl")
	if err := os.WriteFile(path, []byte("not json\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if j, err := openJournal(path, time.Now()); err == nil {
		j.close()
		t.Fatal("corrupt journal silently discarded")
	}
}
