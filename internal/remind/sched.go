package remind

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"github.com/DevNewbie1826/omosense/internal/core"
)

// Test hooks (plan: "the say executable path and the clock are injectable
// for tests"). Zero values select the production behavior.
var (
	hookNow    = time.Now
	hookSleep  = sleepCtx
	hookSayBin = ""
)

// sleepCtx sleeps for d, reporting false when ctx was cancelled first.
func sleepCtx(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}

// tickInterval is remind.ts's Bun.sleep(20_000).
const tickInterval = 20 * time.Second

// maxLateDelivery is remind.ts's MAX_LATE_DELIVERY_MS: a reminder more
// than 6h past due is skipped instead of sent.
const maxLateDelivery = 6 * time.Hour

// scheduler is the remind.ts tick loop over one profile's reminder file.
type scheduler struct {
	c    *core.Ctx
	sink core.Sink
	file string
}

func newScheduler(c *core.Ctx, sink core.Sink) *scheduler {
	return &scheduler{
		c:    c,
		sink: sink,
		file: reminderFile(c),
	}
}

func reminderFile(c *core.Ctx) string {
	return filepath.Join(c.State, "reminders-"+c.Profile.Name+".json")
}

// CancelPending marks every pending reminder of c's profile with
// "cancelled" set to core.ISO(now). An entry is pending when sent,
// skipped, failed and cancelled are all missing or falsy. Unknown fields
// and key order are preserved.
//
// The file is <State>/reminders-<profile>.json. A missing file returns
// 0, nil. A parse error is returned and the file is left untouched. The
// file is rewritten with marshalIndent2Array only when n > 0, where n is
// the number of entries marked.
//
// CancelPending does not take any lock. The caller owns the remind lock
// and must hold it across the call so a scheduler tick cannot race the
// rewrite.
func CancelPending(c *core.Ctx, now time.Time) (int, error) {
	b, err := os.ReadFile(reminderFile(c))
	if os.IsNotExist(err) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	v, err := core.ParseJSON(b)
	if err != nil {
		return 0, err
	}
	arr, ok := v.([]any)
	if !ok {
		return 0, fmt.Errorf("reminders: expected a JSON array")
	}
	iso := core.ISO(now)
	n := 0
	for _, ev := range arr {
		r, ok := ev.(*core.OMap)
		if !ok {
			return 0, fmt.Errorf("reminders: entry is not a JSON object")
		}
		if !pendingEntry(r) {
			continue
		}
		r.Set("cancelled", iso)
		n++
	}
	if n == 0 {
		return 0, nil
	}
	out, err := marshalIndent2Array(arr)
	if err != nil {
		return 0, err
	}
	if err := os.WriteFile(reminderFile(c), out, 0o644); err != nil {
		return 0, err
	}
	return n, nil
}

// run prints the startup LOG and then loops tick/sleep, logging every tick
// error as "LOG remind <err>" (remind.ts try/catch), until ctx is
// cancelled, which ends the loop with nil.
func (s *scheduler) run(ctx context.Context) error {
	s.sink.Log(fmt.Sprintf("reminder scheduler starting (profile %s)", s.c.Profile.Name))
	for {
		if ctx.Err() != nil {
			return nil
		}
		if err := s.tick(ctx); err != nil && ctx.Err() == nil {
			s.sink.Log("remind " + err.Error())
		}
		if !hookSleep(ctx, tickInterval) {
			return nil
		}
	}
}

// tick is remind.ts's tick(): a missing file is a no-op; entries holding a
// truthy sent, skipped, failed or cancelled are skipped (failed and
// cancelled are terminal); due entries send through the say executable;
// the file is rewritten in the bun indent layout only when an entry changed.
func (s *scheduler) tick(ctx context.Context) error {
	b, err := os.ReadFile(s.file)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	v, err := core.ParseJSON(b)
	if err != nil {
		return fmt.Errorf("parse reminders: %w", err)
	}
	arr, ok := v.([]any)
	if !ok {
		return fmt.Errorf("reminders: expected a JSON array")
	}
	now := hookNow()
	changed := false
	var pending []pendingLine
	for _, ev := range arr {
		if ctx.Err() != nil {
			break
		}
		r, ok := ev.(*core.OMap)
		if !ok {
			return fmt.Errorf("reminders: entry is not a JSON object")
		}
		if !pendingEntry(r) {
			continue
		}
		due, parsed := jsDateParse(fieldStr(r, "at"))
		if parsed {
			if due.After(now) {
				continue // not due yet: wait
			}
			if now.Sub(due) > maxLateDelivery {
				r.Set("skipped", core.ISO(hookNow()))
				changed = true
				pending = append(pending, entryLine("skipped-late", r))
				continue
			}
		}
		// An unparsed at is Date.parse NaN: remind.ts's two comparisons
		// are both false on NaN, so the entry sends immediately.
		code, out, errOut, err := s.sendViaSay(ctx, r)
		if ctx.Err() != nil {
			break
		}
		if err != nil {
			return err
		}
		if code != 0 {
			// Keep the TS LOG line (remind.ts:24) so existing LOG
			// handling still sees send failures.
			s.sink.Log(fmt.Sprintf("remind send failed %s: %s", fieldStr(r, "id"), core.Trunc(errOut, 200)))
			r.Set("failed", core.ISO(hookNow()))
			r.Set("error", core.Trunc(strings.TrimSpace(out+errOut), 200))
			changed = true
			pending = append(pending, entryLine("failed", r))
			continue
		}
		r.Set("sent", core.ISO(hookNow()))
		changed = true
		pending = append(pending, entryLine("sent", r))
	}
	if !changed {
		return nil
	}
	out, err := marshalIndent2Array(arr)
	if err == nil {
		err = os.WriteFile(s.file, out, 0o644)
	}
	// The REMIND lines print only once the state write is durable, so an
	// observer that saw a line can rely on the entry it names; when the
	// write fails the lines still print (the send happened) and the error
	// follows as "LOG remind <err>".
	for _, p := range pending {
		if p.prefix == "" { // entryLine failed to marshal; never emit junk
			continue
		}
		s.sink.Raw(p.prefix, p.text)
	}
	return err
}

// pendingLine is one buffered grammar line, flushed after the state
// write.
type pendingLine struct {
	prefix string
	text   string
}

// entryLine renders "<verb> <compact entry json>" for the REMIND grammar.
func entryLine(verb string, r *core.OMap) pendingLine {
	b, err := r.Marshal()
	if err != nil {
		return pendingLine{}
	}
	return pendingLine{prefix: "REMIND", text: verb + " " + string(b)}
}

// sendViaSay execs "<self> say --profile <profile> <platform> send <json>"
// with the target object plus text (remind.ts's argument list plus the
// profile flag: say's default bot comes from the profile, so the child
// must run under this scheduler's profile). It returns the exit code and
// the captured stdout/stderr; only a failure to spawn is an error.
func (s *scheduler) sendViaSay(ctx context.Context, r *core.OMap) (code int, out, errOut string, err error) {
	body := core.NewOMap()
	if t, _ := getOMap(r, "target"); t != nil {
		for _, k := range t.Keys() {
			v, _ := t.Get(k)
			body.Set(k, v)
		}
	}
	// {...r.target, text: r.text}: an absent text is dropped by
	// JSON.stringify(undefined) and so stays absent here too.
	if v, ok := r.Get("text"); ok {
		body.Set("text", v)
	}
	j, err := body.Marshal()
	if err != nil {
		return 0, "", "", err
	}
	bin := hookSayBin
	if bin == "" {
		if bin, err = os.Executable(); err != nil {
			return 0, "", "", err
		}
	}
	cmd := core.SourceCommand(ctx, bin, "say", "--profile", s.c.Profile.Name, fieldStr(r, "platform"), "send", string(j))
	var ob, eb bytes.Buffer
	cmd.Stdout, cmd.Stderr = &ob, &eb
	if err := core.RunSource(cmd); err != nil {
		if ctx.Err() != nil {
			return 0, ob.String(), eb.String(), ctx.Err()
		}
		var ee *exec.ExitError
		if errors.As(err, &ee) {
			return ee.ExitCode(), ob.String(), eb.String(), nil
		}
		return 0, ob.String(), eb.String(), err
	}
	return 0, ob.String(), eb.String(), nil
}

// marshalIndent2Array renders the reminder list the way remind.ts writes
// it: bun JSON.stringify(list, null, 2) — two-space indent, no trailing
// newline. core exposes indent2 rendering only for objects, so the array
// rides as the value of a one-key wrapper object and is then unwrapped and
// dedented by the wrapper's two spaces; the golden test pins the bytes.
func marshalIndent2Array(arr []any) ([]byte, error) {
	w := core.NewOMap()
	w.Set("v", arr)
	b, err := w.MarshalIndent2()
	if err != nil {
		return nil, err
	}
	s := string(b)
	const prefix = "{\n  \"v\": "
	if !strings.HasPrefix(s, prefix) || !strings.HasSuffix(s, "\n}") {
		return nil, fmt.Errorf("reminders: unexpected indent2 wrapper shape")
	}
	s = s[len(prefix) : len(s)-2]
	if s == "[]" {
		return []byte("[]"), nil
	}
	lines := strings.Split(s, "\n")
	for i, ln := range lines {
		lines[i] = strings.TrimPrefix(ln, "  ")
	}
	return []byte(strings.Join(lines, "\n")), nil
}

// doneEntry reports whether the terminal marker key holds a truthy value,
// mirroring remind.ts's `if (r.sent || r.skipped) continue` plus the
// terminal failed and cancelled markers.
func doneEntry(r *core.OMap, key string) bool {
	v, ok := r.Get(key)
	return ok && truthy(v)
}

// pendingEntry reports whether the reminder can still be sent. A truthy
// sent, skipped, failed or cancelled field is terminal.
func pendingEntry(r *core.OMap) bool {
	return !doneEntry(r, "sent") && !doneEntry(r, "skipped") && !doneEntry(r, "failed") && !doneEntry(r, "cancelled")
}

// truthy is ECMAScript truthiness (null, "" and 0 are falsy).
func truthy(v any) bool {
	switch x := v.(type) {
	case nil:
		return false
	case bool:
		return x
	case string:
		return x != ""
	case json.Number:
		f, err := x.Float64()
		return err == nil && f != 0
	default:
		return true
	}
}

// fieldStr renders an entry field the way a JS template literal
// interpolates it: strings as-is, numbers and booleans as text, and a
// missing, null or undefined value as "".
func fieldStr(r *core.OMap, key string) string {
	v, _ := r.Get(key)
	switch x := v.(type) {
	case string:
		return x
	case json.Number:
		return x.String()
	case bool:
		if x {
			return "true"
		}
		return "false"
	default:
		return ""
	}
}

func getOMap(m *core.OMap, key string) (*core.OMap, bool) {
	v, ok := m.Get(key)
	if !ok {
		return nil, false
	}
	om, is := v.(*core.OMap)
	return om, is
}

// jsDateParse covers the date forms Date.parse receives from the agent:
// ISO datetimes with Z or a numeric offset (optional milliseconds), naive
// ISO datetimes (local time, as in JS) and date-only strings (UTC
// midnight, as in JS). Anything else is Date.parse's NaN.
func jsDateParse(s string) (time.Time, bool) {
	for _, layout := range []string{
		"2006-01-02T15:04:05.999999999Z07:00",
		"2006-01-02T15:04Z07:00",
	} {
		if t, err := time.Parse(layout, s); err == nil {
			return t, true
		}
	}
	for _, layout := range []string{
		"2006-01-02T15:04:05.999999999",
		"2006-01-02T15:04",
	} {
		if t, err := time.ParseInLocation(layout, s, time.Local); err == nil {
			return t, true
		}
	}
	if t, err := time.Parse("2006-01-02", s); err == nil {
		return t, true
	}
	return time.Time{}, false
}
