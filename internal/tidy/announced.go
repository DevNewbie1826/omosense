package tidy

// announced.go persists the watcher's announcement record (repo ->
// {to, at}) in <State>/tidy-announced.json, so the 6h re-emit window
// survives a host restart (IS-4). The file is state, not config: --now/
// --once, --write-watermark and --backup-now never touch it.

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"

	"github.com/DevNewbie1826/omosense/internal/core"
)

// announcedPath is <State>/tidy-announced.json.
func (t *tidyer) announcedPath() string {
	return filepath.Join(t.state, "tidy-announced.json")
}

// readAnnounced loads the persisted announcement record. A missing file is
// an empty record; an unreadable or malformed file logs once and yields an
// empty record so the loop keeps running (IS-5, fail open: it may
// re-announce, never silently keep a stale mute).
func (t *tidyer) readAnnounced() map[string]emitRec {
	b, err := os.ReadFile(t.announcedPath())
	if err != nil {
		if !os.IsNotExist(err) {
			t.sink.Log(fmt.Sprintf("memory-tidy announced state unreadable: %v; starting empty", err))
		}
		return map[string]emitRec{}
	}
	rec, err := parseAnnounced(b)
	if err != nil {
		t.sink.Log(fmt.Sprintf("memory-tidy announced state unreadable: %v; starting empty", err))
		return map[string]emitRec{}
	}
	return rec
}

// parseAnnounced decodes {"repos":{"<name>":{"to":"<sha>","at":<ms>}}}.
// Any other shape is an error, so the caller fails open with one LOG
// instead of guessing at a partial record.
func parseAnnounced(b []byte) (map[string]emitRec, error) {
	v, err := core.ParseJSON(b)
	if err != nil {
		return nil, err
	}
	doc, ok := v.(*core.OMap)
	if !ok {
		return nil, errors.New("not a JSON object")
	}
	reposV, ok := doc.Get("repos")
	if !ok {
		return nil, errors.New(`missing "repos"`)
	}
	repos, ok := reposV.(*core.OMap)
	if !ok {
		return nil, errors.New(`"repos" is not an object`)
	}
	out := make(map[string]emitRec, repos.Len())
	for _, name := range repos.Keys() {
		ev, _ := repos.Get(name)
		entry, ok := ev.(*core.OMap)
		if !ok {
			return nil, fmt.Errorf("entry %q is not an object", name)
		}
		toV, ok := entry.Get("to")
		to, isStr := toV.(string)
		if !ok || !isStr {
			return nil, fmt.Errorf("entry %q has no string \"to\"", name)
		}
		atV, ok := entry.Get("at")
		num, isNum := atV.(json.Number)
		if !ok || !isNum {
			return nil, fmt.Errorf("entry %q has no numeric \"at\"", name)
		}
		at, finite := jsNumber(num.String())
		if !finite {
			return nil, fmt.Errorf("entry %q has a non-finite \"at\"", name)
		}
		out[name] = emitRec{to: to, at: at}
	}
	return out, nil
}

// writeAnnounced saves the record as {"repos":{...}} with two-space indent
// and a trailing newline, through <path>.tmp + rename (IS-4). The keys are
// written in sorted order so the file is deterministic.
func (t *tidyer) writeAnnounced(rec map[string]emitRec) error {
	names := make([]string, 0, len(rec))
	for name := range rec {
		names = append(names, name)
	}
	sort.Strings(names)
	repos := core.NewOMap()
	for _, name := range names {
		entry := core.NewOMap()
		entry.Set("to", rec[name].to)
		entry.Set("at", rec[name].at)
		repos.Set(name, entry)
	}
	doc := core.NewOMap()
	doc.Set("repos", repos)
	b, err := doc.MarshalIndent2()
	if err != nil {
		return err
	}
	path := t.announcedPath()
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, append(b, '\n'), 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}
