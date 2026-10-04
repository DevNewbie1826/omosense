package google

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"github.com/DevNewbie1826/omosense/internal/core"
)

// soonWindow is the fixed SOON lead time (user decision): fire when
// 0 < realStart-now <= 30min. The bun version could never fire because its
// yearless starts parsed as year 2001.
const soonWindow = 30 * time.Minute

// zeleResult is one zele CLI invocation: stdout, stderr and exit code.
type zeleResult struct {
	stdout string
	stderr string
	code   int
}

// zeleFunc runs a zele command line; tests inject fakes here.
type zeleFunc func(ctx context.Context, args []string) zeleResult

var calArgs = []string{"cal", "events", "--all", "--days", "2", "--limit", "50"}
var mailArgs = []string{"mail", "list", "--filter", "is:unread category:primary newer_than:1d", "--limit", "30"}

// noiseRe matches sign-up welcomes, passkey notices and similar FYI mail
// that never needs the agent (watch-google.ts); security alerts still pass.
var noiseRe = regexp.MustCompile(`(?i)welcome|you're in|thanks for (coming|joining|signing up)|회원가입을 축하|가입을 환영|passkey|패스키|계정 데이터 일부를|shared some of your google account data`)

var nowFunc = time.Now

func sleepCtx(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

func zeleExec(ctx context.Context, args []string) zeleResult {
	cmd := core.SourceCommand(ctx, "zele", args...)
	var out, errb bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &errb
	if err := cmd.Run(); err != nil {
		var ee *exec.ExitError
		if errors.As(err, &ee) {
			return zeleResult{stdout: out.String(), stderr: errb.String(), code: ee.ExitCode()}
		}
		// bun spawns zele through a shell: a missing binary surfaces as sh's
		// "command not found" on stderr with exit 127.
		return zeleResult{stdout: out.String(), stderr: "zele: command not found", code: 127}
	}
	return zeleResult{stdout: out.String(), stderr: errb.String(), code: 0}
}

var execZele zeleFunc = zeleExec

// watcher is one profile's google source state machine.
type watcher struct {
	prof     core.Profile
	state    string
	sink     core.Sink
	seen     *core.OMap
	firstRun bool
	writable bool
	zele     zeleFunc
	now      func() time.Time
	sleep    func(context.Context, time.Duration) error
}

// newWatcher loads the profile's seen map (firstRun = it was empty at
// load, watch-google.ts:12) and binds the sink plus the injectable zele /
// clock / sleep dependencies. writable false is the read-only --once shape:
// save becomes a no-op and nothing is ever created or written.
func newWatcher(c *core.Ctx, sink core.Sink, writable bool) (*watcher, error) {
	path := filepath.Join(c.State, "google-seen-"+c.Profile.Name+".json")
	seen := core.NewOMap()
	if b, err := os.ReadFile(path); err == nil {
		v, err := core.ParseJSON(b)
		if err != nil {
			return nil, fmt.Errorf("parse %s: %w", path, err)
		}
		m, ok := v.(*core.OMap)
		if !ok {
			return nil, fmt.Errorf("%s: expected a JSON object", path)
		}
		seen = m
	} else if !os.IsNotExist(err) {
		return nil, fmt.Errorf("read %s: %w", path, err)
	}
	return &watcher{
		prof:     c.Profile,
		state:    c.State,
		sink:     sink,
		seen:     seen,
		firstRun: seen.Len() == 0,
		writable: writable,
		zele:     execZele,
		now:      nowFunc,
		sleep:    sleepCtx,
	}, nil
}

// items runs one zele command and returns its .items list. A non-zero exit
// logs "zele error <args>: <stderr truncated>" and yields an empty list,
// exactly like the TS helper; a YAML parse error propagates to the caller
// (the loop's try block).
func (w *watcher) items(ctx context.Context, args []string) ([]any, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	r := w.zele(ctx, args)
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if r.code != 0 {
		w.sink.Log(fmt.Sprintf("zele error %s: %s", strings.Join(args, " "), core.Trunc(r.stderr, 200)))
		return nil, nil
	}
	v, err := parseYAML([]byte(r.stdout))
	if err != nil {
		return nil, err
	}
	if m, ok := v.(*core.OMap); ok {
		if iv, ok := m.Get("items"); ok {
			if arr, ok := iv.([]any); ok {
				return arr, nil
			}
		}
	}
	return nil, nil
}

// calendar emits CAL for each unseen event (key cal:<id>@<Date.parse-ms>)
// filtered by the profile's calendar allow-list, and SOON for events whose
// year-corrected realStart falls inside the 30 minute window.
func (w *watcher) calendar(ctx context.Context) error {
	items, err := w.items(ctx, calArgs)
	if err != nil {
		return err
	}
	now := w.now()
	nowMs := now.UnixMilli()
	for _, it := range items {
		e, ok := it.(*core.OMap)
		if !ok {
			continue
		}
		if w.prof.Calendars != nil && !containsCalendar(*w.prof.Calendars, omapStr(e, "calendar")) {
			continue
		}
		start := startOfString(e)
		ms, msOK := startMs(start)
		key := fmt.Sprintf("%s@%s", omapStr(e, "id"), msKeyText(ms, msOK))
		if !w.seen.Has("cal:" + key) {
			w.seen.Set("cal:"+key, nowMs)
			w.sink.Emit("CAL", e)
		}
		if rs, ok := realStart(start, now); ok {
			if d := rs.Sub(now); d > 0 && d <= soonWindow && !w.seen.Has("soon:"+key) {
				w.seen.Set("soon:"+key, nowMs)
				w.sink.Emit("SOON", e)
			}
		}
	}
	return nil
}

// mail marks each unseen mail seen (NOISE included), then emits
// MAIL {account,id,from?,subject?,snippet} unless the NOISE filter matched
// or the pass is silent. Absent from/subject keys stay absent; the snippet
// is always present, truncated to 200 runes.
func (w *watcher) mail(ctx context.Context, silent bool) error {
	items, err := w.items(ctx, mailArgs)
	if err != nil {
		return err
	}
	for _, it := range items {
		m, ok := it.(*core.OMap)
		if !ok {
			continue
		}
		k := fmt.Sprintf("mail:%s:%s", omapStr(m, "account"), omapStr(m, "id"))
		if w.seen.Has(k) {
			continue
		}
		w.seen.Set(k, w.now().UnixMilli())
		if noiseRe.MatchString(omapStr(m, "from") + " " + omapStr(m, "subject")) {
			continue
		}
		if silent {
			continue
		}
		p := core.NewOMap()
		if v, ok := m.Get("account"); ok {
			p.Set("account", v)
		}
		if v, ok := m.Get("id"); ok {
			p.Set("id", v)
		}
		if v, ok := m.Get("from"); ok {
			p.Set("from", v)
		}
		if v, ok := m.Get("subject"); ok {
			p.Set("subject", v)
		}
		p.Set("snippet", core.Trunc(omapStr(m, "snippet"), 200))
		w.sink.Emit("MAIL", p)
	}
	return nil
}

// prune drops seen values strictly older than 7 days (watch-google.ts).
func (w *watcher) prune() {
	cut := w.now().Add(-7 * 24 * time.Hour).UnixMilli()
	for _, k := range w.seen.Keys() {
		v, _ := w.seen.Get(k)
		if f, ok := numberFloat(v); ok && f < float64(cut) {
			w.seen.Delete(k)
		}
	}
}

// save writes the seen map as a compact JSON object, key order preserved
// (IS-4), unless the watcher is read-only.
func (w *watcher) save() error {
	if !w.writable {
		return nil
	}
	b, err := w.seen.Marshal()
	if err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(w.state, "google-seen-"+w.prof.Name+".json"), b, 0o644)
}

// once is the read-only --once pass (IS-9): calendar plus non-silent mail
// only when the profile reads mail; no lock, no starting LOG, no writes.
func (w *watcher) once(ctx context.Context) {
	if err := w.calendar(ctx); err != nil {
		w.sink.Log("google watcher " + err.Error())
	}
	if w.prof.Mail {
		if err := w.mail(ctx, false); err != nil {
			w.sink.Log("google watcher " + err.Error())
		}
	}
}

// run is the watcher loop with watch-google.ts tick semantics: silent
// first-run mail before the loop, calendar every 5th tick, mail on later
// ticks, prune and save every tick, per-iteration errors logged without
// stopping. It returns nil when ctx is cancelled, and the pre-loop mail
// error otherwise (the TS script dies on it).
func (w *watcher) run(ctx context.Context) error {
	w.sink.Log(fmt.Sprintf("google watcher starting (profile %s)", w.prof.Name))
	if w.prof.Mail {
		if err := w.mail(ctx, w.firstRun); err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return err
		}
	}
	for tick := 0; ; tick++ {
		if ctx.Err() != nil {
			return nil
		}
		if err := w.tick(ctx, tick); err != nil && ctx.Err() == nil {
			w.sink.Log("google watcher " + err.Error())
		}
		if err := w.sleep(ctx, time.Minute); err != nil {
			return nil
		}
	}
}

// tick is one loop iteration; an error skips the rest of the iteration
// (mail, prune, save), matching the single TS try block.
func (w *watcher) tick(ctx context.Context, n int) error {
	if n%5 == 0 {
		if err := w.calendar(ctx); err != nil {
			return err
		}
	}
	if n > 0 && w.prof.Mail {
		if err := w.mail(ctx, false); err != nil {
			return err
		}
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	w.prune()
	return w.save()
}

// startOfString mirrors e.start?.dateTime ?? e.start?.date ?? e.start ?? ""
// from watch-google.ts: a map start yields dateTime then date, a scalar
// start yields itself, and anything else (or a map with neither key)
// degrades to the empty string, i.e. a NaN seen key.
func startOfString(e *core.OMap) string {
	v, ok := e.Get("start")
	if !ok {
		return ""
	}
	if m, ok := v.(*core.OMap); ok {
		if dt, ok := m.Get("dateTime"); ok {
			if s, ok := dt.(string); ok {
				return s
			}
		}
		if d, ok := m.Get("date"); ok {
			if s, ok := d.(string); ok {
				return s
			}
		}
		return ""
	}
	if s, ok := v.(string); ok {
		return s
	}
	return ""
}

func omapStr(m *core.OMap, key string) string {
	v, _ := m.Get(key)
	s, _ := v.(string)
	return s
}

func numberFloat(v any) (float64, bool) {
	switch x := v.(type) {
	case float64:
		return x, true
	case int:
		return float64(x), true
	case int64:
		return float64(x), true
	case json.Number:
		f, err := x.Float64()
		return f, err == nil
	}
	return 0, false
}

func containsCalendar(list []string, s string) bool {
	for _, c := range list {
		if c == s {
			return true
		}
	}
	return false
}
