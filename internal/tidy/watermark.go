package tidy

import (
	"context"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"syscall"

	"github.com/DevNewbie1826/omosense/internal/core"
)

func (t *tidyer) watermarkPath() string {
	return filepath.Join(t.state, "memory-tidy.json")
}

// writeFileFn is the atomic-write primitive behind writeWatermarkDoc and
// writeAnnounced; tests replace it to force a write failure.
var writeFileFn = os.WriteFile

// watermarkLockedHook, when set, runs inside updateWatermark's lock right
// after the in-lock read and before the mutation is applied; nil in
// production.
var watermarkLockedHook func()

// watermarkEventHook, when set, observes the steps a writer takes on the
// watermark: "read" after each watermark read (readWatermark), "prelock"
// immediately BEFORE the exclusive lock is acquired, and "write"
// immediately after it writes the document. Tests use it to prove the
// re-read happens inside the lock; nil in production, where it has no
// effect. The "prelock" event is emitted by the test wrapper around
// watermarkFlockFn, never by production code, so the readiness signal is
// inseparable from the acquisition it marks.
var watermarkEventHook func(ev string)

// watermarkFlockFn acquires the watermark lock. It is syscall.Flock in
// production; tests replace it with a wrapper that emits the writer's
// "prelock" readiness signal immediately before calling the real
// syscall.Flock, so a read moved above the acquisition necessarily happens
// before the test observes readiness.
var watermarkFlockFn = syscall.Flock

// updateWatermark runs fn against the FRESH <State>/memory-tidy.json under
// the exclusive <State>/memory-tidy.json.lock, then writes the result
// through a temp file + rename. A nil fn return writes; an error leaves the
// file untouched. The lock is opened per call so flock also serializes
// goroutines, not only separate processes (like thread.transact).
func (t *tidyer) updateWatermark(fn func(w *core.OMap) error) error {
	if err := os.MkdirAll(t.state, 0o755); err != nil {
		return errors.New("Error: " + err.Error())
	}
	lock, err := os.OpenFile(filepath.Join(t.state, "memory-tidy.json.lock"), os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return errors.New("Error: " + err.Error())
	}
	defer lock.Close()
	if err := watermarkFlockFn(int(lock.Fd()), syscall.LOCK_EX); err != nil {
		return errors.New("Error: " + err.Error())
	}
	defer syscall.Flock(int(lock.Fd()), syscall.LOCK_UN)
	w, err := t.readWatermark()
	if err != nil {
		return err
	}
	if watermarkLockedHook != nil {
		watermarkLockedHook()
	}
	if err := fn(w); err != nil {
		return err
	}
	err = writeWatermarkDoc(t.watermarkPath(), w)
	if watermarkEventHook != nil {
		watermarkEventHook("write")
	}
	return err
}

// readWatermark parses <State>/memory-tidy.json, filling the TS defaults
// (repos, lastRun, lastBackupDate, appended in that order) exactly like
// {...raw, repos: ..., lastRun: ..., lastBackupDate: ...} does. A missing
// file yields the defaults; a parse failure is a SyntaxError, like
// JSON.parse in the TS.
func (t *tidyer) readWatermark() (*core.OMap, error) {
	if watermarkEventHook != nil {
		defer watermarkEventHook("read")
	}
	b, err := os.ReadFile(t.watermarkPath())
	if err != nil {
		if os.IsNotExist(err) {
			return withDefaults(core.NewOMap()), nil
		}
		return nil, errors.New("Error: " + err.Error())
	}
	v, err := core.ParseJSON(b)
	if err != nil {
		return nil, errors.New("SyntaxError: " + err.Error())
	}
	m, ok := v.(*core.OMap)
	if !ok {
		// {...primitive} spreads to an empty object; only an array (with
		// index keys) diverges, which the TS never writes.
		m = core.NewOMap()
	}
	return withDefaults(m), nil
}

func withDefaults(m *core.OMap) *core.OMap {
	if v, ok := m.Get("repos"); !ok || v == nil {
		m.Set("repos", core.NewOMap())
	}
	if v, ok := m.Get("lastRun"); !ok || v == nil {
		m.Set("lastRun", nil)
	}
	if v, ok := m.Get("lastBackupDate"); !ok || v == nil {
		m.Set("lastBackupDate", nil)
	}
	return m
}

// reposOf returns w.repos when it is an object, else nil: JS property
// lookups on a non-object are undefined and Object.assign onto one is a
// no-op, so callers skip assignment when it is nil.
func reposOf(w *core.OMap) *core.OMap {
	v, _ := w.Get("repos")
	m, _ := v.(*core.OMap)
	return m
}

// writeWatermarkDoc writes JSON.stringify(w, null, 2) + "\n" through a
// .tmp file + rename (IS-4).
func writeWatermarkDoc(path string, w *core.OMap) error {
	b, err := w.MarshalIndent2()
	if err != nil {
		return errors.New("Error: " + err.Error())
	}
	tmp := path + ".tmp"
	if err := writeFileFn(tmp, append(b, '\n'), 0o644); err != nil {
		_ = os.Remove(tmp)
		return errors.New("Error: " + err.Error())
	}
	if err := os.Rename(tmp, path); err != nil {
		_ = os.Remove(tmp)
		return errors.New("Error: " + err.Error())
	}
	return nil
}

// sortedAgents lists AGENTS entries in readdir(...).sort() order (both
// byte-wise and UTF-16 code-unit order agree on the names that appear).
func sortedAgents(agents string) ([]string, error) {
	entries, err := os.ReadDir(agents)
	if err != nil {
		return nil, errors.New("Error: " + err.Error())
	}
	names := make([]string, 0, len(entries))
	for _, e := range entries {
		names = append(names, e.Name())
	}
	sort.Strings(names)
	return names, nil
}

type headRec struct {
	name string
	sha  string
	at   float64
}

// heads lists HEAD of every agent repo that has a repo/.git entry, after
// skips in order: (1) the profile's own memory repo, (2) tidy.exclude,
// (3) every remaining repo when learnOthers is false. Excluded repos are
// still omitted when learnOthers is true. at is %ct*1000; a git log
// failure is logged per repo and skipped.
func (t *tidyer) heads(ctx context.Context) ([]headRec, error) {
	names, err := sortedAgents(t.agents)
	if err != nil {
		return nil, err
	}
	var out []headRec
	for _, name := range names {
		if name == t.memory {
			continue
		}
		if t.exclude[name] {
			continue
		}
		if !t.learnOthers {
			continue
		}
		repo := filepath.Join(t.agents, name, "repo")
		if _, err := os.Stat(filepath.Join(repo, ".git")); err != nil {
			continue
		}
		stdout, stderr, code, err := runGit(ctx, "-C", repo, "log", "-1", "--format=%H%x20%ct")
		if err != nil {
			return nil, err
		}
		if code != 0 {
			t.sink.Log(fmt.Sprintf("memory-tidy git log failed %s: %s", name, core.Trunc(strings.TrimSpace(stderr), 200)))
			continue
		}
		parts := strings.Split(strings.TrimSpace(stdout), " ")
		rec := headRec{name: name, at: math.NaN()}
		if len(parts) > 0 {
			rec.sha = parts[0]
		}
		if len(parts) > 1 {
			if n, finite := jsNumber(parts[1]); finite {
				rec.at = n * 1000
			}
		}
		out = append(out, rec)
	}
	return out, nil
}

// changed lists the repos whose watermark sha differs from HEAD. The
// comparison is a JS strict !== against the sha string, so a json.Number
// or boolean watermark value always counts as changed; from is the raw
// watermark value, null when absent.
func (t *tidyer) changed(ctx context.Context) ([]change, error) {
	w, err := t.readWatermark()
	if err != nil {
		return nil, err
	}
	repos := reposOf(w)
	heads, err := t.heads(ctx)
	if err != nil {
		return nil, err
	}
	var out []change
	for _, h := range heads {
		cur, _ := repos.Get(h.name)
		if s, isStr := cur.(string); isStr && s == h.sha {
			continue
		}
		from := any(nil)
		if cur != nil {
			from = cur
		}
		out = append(out, change{repo: h.name, from: from, to: h.sha, committedAt: h.at})
	}
	return out, nil
}
