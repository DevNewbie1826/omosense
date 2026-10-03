package daemon

import (
	"errors"
	"fmt"
	"io"
	"os"
	"time"
)

type journalEntry struct {
	Seq       uint64    `json:"seq,omitempty"`
	At        time.Time `json:"at,omitzero"`
	Source    string    `json:"source,omitempty"`
	Line      string    `json:"line,omitempty"`
	Delivered bool      `json:"delivered,omitempty"`
	Mark      uint64    `json:"delivered_seq,omitempty"`
}

// journal is owned by server.mu: append, replay markers and compaction never
// race each other. Delivered markers are append-only between compactions.
type journal struct {
	path    string
	file    *os.File
	seq     uint64
	entries []journalEntry
}

func openJournal(path string, now time.Time) (*journal, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o600)
	if err != nil {
		return nil, fmt.Errorf("open journal: %w", err)
	}
	j := &journal{path: path, file: f}
	reader := newFramer(f)
	for {
		var e journalEntry
		if err := reader.read(&e); errors.Is(err, io.EOF) {
			break
		} else if err != nil {
			f.Close()
			return nil, fmt.Errorf("read journal: %w", err)
		}
		j.seq = max(j.seq, e.Seq)
		if e.Mark != 0 {
			for i := range j.entries {
				if j.entries[i].Seq == e.Mark {
					j.entries[i].Delivered = true
					break
				}
			}
		} else if e.Source != "" {
			j.entries = append(j.entries, e)
		}
	}
	if err := j.compact(now); err != nil {
		j.close()
		return nil, err
	}
	return j, nil
}

func (j *journal) append(now time.Time, source, line string, delivered bool) error {
	e := journalEntry{Seq: j.seq + 1, At: now, Source: source, Line: line, Delivered: delivered}
	if err := j.record(e); err != nil {
		return err
	}
	j.seq = e.Seq
	j.entries = append(j.entries, e)
	return nil
}

func (j *journal) record(e journalEntry) error {
	if err := writeFrame(j.file, e); err != nil {
		return fmt.Errorf("append journal: %w", err)
	}
	if err := j.file.Sync(); err != nil {
		return fmt.Errorf("sync journal: %w", err)
	}
	return nil
}

func (j *journal) pending(h hello) []journalEntry {
	var out []journalEntry
	for _, e := range j.entries {
		if !e.Delivered && matches(h, h.Profile, e.Source, e.Line) {
			out = append(out, e)
		}
	}
	return out
}

func (j *journal) deliver(seq uint64) error {
	if err := j.record(journalEntry{Mark: seq}); err != nil {
		return err
	}
	for i := range j.entries {
		if j.entries[i].Seq == seq {
			j.entries[i].Delivered = true
			break
		}
	}
	return nil
}

func (j *journal) compact(now time.Time) error {
	cutoff := now.Add(-24 * time.Hour)
	entries := make([]journalEntry, 0, len(j.entries))
	for _, e := range j.entries {
		if !e.At.Before(cutoff) {
			entries = append(entries, e)
		}
	}
	tmp := j.path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o600)
	if err != nil {
		return fmt.Errorf("journal temp: %w", err)
	}
	defer os.Remove(tmp)
	// Persist highwater even when retention drops every event.
	err = writeFrame(f, journalEntry{Seq: j.seq})
	for _, e := range entries {
		if err == nil {
			err = writeFrame(f, e)
		}
	}
	if err == nil {
		err = f.Sync()
	}
	err = errors.Join(err, f.Close())
	if err != nil {
		return fmt.Errorf("compact journal: %w", err)
	}
	if err := os.Rename(tmp, j.path); err != nil {
		return fmt.Errorf("replace journal: %w", err)
	}
	next, err := os.OpenFile(j.path, os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		return fmt.Errorf("reopen journal: %w", err)
	}
	err = j.file.Close()
	j.file, j.entries = next, entries
	return err
}

func (j *journal) close() error {
	if j.file == nil {
		return nil
	}
	err := j.file.Close()
	j.file = nil
	return err
}
