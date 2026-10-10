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
	return filepath.Join(c.State, "reminders.json")
}

// CancelPending marks every pending reminder of c's profile with
// "cancelled" set to core.ISO(now). An entry is pending when sent,
// skipped, failed and cancelled are all missing or falsy. Unknown fields
// and key order are preserved.
//
// The file is <State>/reminders.json. A missing file returns
// 0, nil and is not created. A parse error is returned and the file is left
// untouched. The file is rewritten (temp file + rename) with
// marshalIndent2Array only when n > 0, where n is the number of entries
// marked.
//
// CancelPending takes reminders.lock itself, through withReminders like
// remind add, so a tick's read and per-result record and an add serialize
// with it. The entries cancelled are those pending when the file is read
// under the lock; a send already in flight finishes outside the lock and its
// result is then not recorded (record finds the entry changed). No caller
// holds reminders.lock around CancelPending (it has no production caller);
// one that did would deadlock, because the lock is opened per call and flock
// then blocks even within the same process.
func CancelPending(c *core.Ctx, now time.Time) (int, error) {
	n := 0
	err := withReminders(c.State, func(arr []any) ([]any, bool, error) {
		if cancelPendingLockedHook != nil {
			cancelPendingLockedHook()
		}
		iso := core.ISO(now)
		marked := 0
		for _, ev := range arr {
			r, ok := ev.(*core.OMap)
			if !ok {
				return nil, false, fmt.Errorf("reminders: entry is not a JSON object")
			}
			if !pendingEntry(r) {
				continue
			}
			r.Set("cancelled", iso)
			marked++
		}
		n = marked
		return arr, marked > 0, nil
	})
	if err != nil {
		return 0, err
	}
	return n, nil
}

// cancelPendingLockedHook, when set, runs inside CancelPending's lock right
// after the read and before any entry is marked; nil in production.
var cancelPendingLockedHook func()

// run prints the startup LOG and then loops tick/sleep, logging every tick
// error as "LOG remind <err>" (remind.ts try/catch), until ctx is
// cancelled, which ends the loop with nil.
func (s *scheduler) run(ctx context.Context) error {
	s.sink.Log("reminder scheduler starting")
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
//
// The read and every write take <state>/reminders.lock briefly, never across
// a send: phase 1 selects the due entries under the lock and releases it,
// every send then runs unlocked, and each result is recorded by a second
// short lock that re-reads the file before writing it back atomically. A
// concurrent `remind add` therefore waits for a read or a write, never for
// the network.
func (s *scheduler) tick(ctx context.Context) error {
	if _, err := os.Stat(s.file); os.IsNotExist(err) {
		return nil
	}
	arr, plan, err := s.planDue()
	if err != nil {
		return err
	}
	for _, it := range plan {
		if ctx.Err() != nil {
			break
		}
		sel, ok := arr[it.index].(*core.OMap)
		if !ok {
			return fmt.Errorf("reminders: entry is not a JSON object")
		}
		var o outcome
		if it.skip {
			o = outcome{verb: "skipped-late", apply: func(r *core.OMap) {
				r.Set("skipped", core.ISO(hookNow()))
			}}
		} else {
			code, out, errOut, err := s.sendViaSay(ctx, sel)
			if ctx.Err() != nil {
				break // cancelled mid-send: record nothing, start nothing more
			}
			if err != nil {
				return err
			}
			if code != 0 {
				// Keep the TS LOG line (remind.ts:24) so existing LOG
				// handling still sees send failures.
				s.sink.Log(fmt.Sprintf("remind send failed %s: %s", fieldStr(sel, "id"), core.Trunc(errOut, 200)))
				msg := core.Trunc(strings.TrimSpace(out+errOut), 200)
				o = outcome{verb: "failed", apply: func(r *core.OMap) {
					r.Set("failed", core.ISO(hookNow()))
					r.Set("error", msg)
				}}
			} else {
				o = outcome{verb: "sent", apply: func(r *core.OMap) {
					r.Set("sent", core.ISO(hookNow()))
				}}
			}
		}
		line, recorded, recErr := s.record(arr, it, o)
		// The REMIND line prints only once the state write is durable, so an
		// observer that saw a line can rely on the entry it names; when the
		// write fails the line still prints (the send happened) and the error
		// follows as "LOG remind <err>".
		if line.prefix != "" {
			s.sink.Raw(line.prefix, line.text)
		}
		if recErr != nil {
			return recErr
		}
		if !recorded {
			s.sink.Log(fmt.Sprintf("remind %s changed or removed during send; not recorded", entryID(sel)))
		}
	}
	return nil
}

// plannedSend is one pending entry selected in phase 1: where it sits in the
// list read under the lock and the compact JSON it had at that moment.
type plannedSend struct {
	index    int
	snapshot string
	skip     bool // more than maxLateDelivery past due: record skipped, never send
}

// outcome is the state a finished item records, plus how to set it.
type outcome struct {
	verb  string
	apply func(*core.OMap)
}

// planDue takes the short read lock, reads the reminder list and returns it
// with the entries to act on, in file order. A future entry is left alone
// (still pending, not due yet); an entry more than maxLateDelivery past due is
// planned as a skip, so it is recorded skipped-late and never sent. Nothing is
// written, so the lock is held only for the read.
func (s *scheduler) planDue() ([]any, []plannedSend, error) {
	unlock, err := lockReminders(filepath.Dir(s.file))
	if err != nil {
		return nil, nil, err
	}
	defer unlock()
	b, err := os.ReadFile(s.file)
	if os.IsNotExist(err) {
		return nil, nil, nil
	}
	if err != nil {
		return nil, nil, err
	}
	v, err := core.ParseJSON(b)
	if err != nil {
		return nil, nil, fmt.Errorf("parse reminders: %w", err)
	}
	arr, ok := v.([]any)
	if !ok {
		return nil, nil, fmt.Errorf("reminders: expected a JSON array")
	}
	now := hookNow()
	var plan []plannedSend
	for i, ev := range arr {
		r, ok := ev.(*core.OMap)
		if !ok {
			return nil, nil, fmt.Errorf("reminders: entry is not a JSON object")
		}
		if !pendingEntry(r) {
			continue
		}
		snap, err := r.Marshal()
		if err != nil {
			return nil, nil, err
		}
		due, parsed := jsDateParse(fieldStr(r, "at"))
		if parsed {
			if due.After(now) {
				continue // not due yet: wait
			}
			if now.Sub(due) > maxLateDelivery {
				plan = append(plan, plannedSend{index: i, snapshot: string(snap), skip: true})
				continue
			}
		}
		// An unparsed at is Date.parse NaN: remind.ts's two comparisons
		// are both false on NaN, so the entry sends immediately.
		plan = append(plan, plannedSend{index: i, snapshot: string(snap)})
	}
	return arr, plan, nil
}

// record writes one outcome back for the entry selected as it, and reports
// whether that entry was found.
//
// It takes the short write lock, re-reads the file - so an `add` that landed
// while the send was in flight is never overwritten - and matches the FIRST
// pending entry whose compact JSON equals the phase-1 snapshot. Matching on
// content is deliberately stricter than matching on the id: an entry edited or
// removed meanwhile is left alone and the caller reports it. Nothing is
// written when nothing matched, and a missing file is never created. The
// rendered line still comes back when the write failed, because the send
// already happened.
func (s *scheduler) record(stale []any, it plannedSend, o outcome) (pendingLine, bool, error) {
	var line pendingLine
	recorded := false
	err := withReminders(s.c.State, func(arr []any) ([]any, bool, error) {
		for _, ev := range arr {
			r, ok := ev.(*core.OMap)
			if !ok {
				return nil, false, fmt.Errorf("reminders: entry is not a JSON object")
			}
			if !pendingEntry(r) {
				continue
			}
			b, err := r.Marshal()
			if err != nil {
				return nil, false, err
			}
			if string(b) != it.snapshot {
				continue
			}
			o.apply(r)
			line = entryLine(o.verb, r)
			recorded = true
			return arr, true, nil
		}
		return arr, false, nil
	})
	if err != nil && !recorded {
		// The re-read or the write failed: the send already happened, so the
		// line still prints, rendered from the entry as it was selected.
		if sel, ok := stale[it.index].(*core.OMap); ok {
			line = applyLine(sel, o)
		}
	}
	return line, recorded, err
}

// applyLine applies o to e and renders its REMIND grammar line.
func applyLine(e *core.OMap, o outcome) pendingLine {
	o.apply(e)
	return entryLine(o.verb, e)
}

// entryID is the entry's id, or "(no id)" for a legacy entry without one.
func entryID(r *core.OMap) string {
	if id := fieldStr(r, "id"); id != "" {
		return id
	}
	return "(no id)"
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

// sendViaSay execs "<self> say <platform> send <json>" with the target
// object plus text, in the scheduler's own folder: the child inherits
// OMOSENSE_DIR/OMOSENSE_STATE so say reads the same config.json and resolves
// the same default bot. It returns the exit code and the captured
// stdout/stderr; only a failure to spawn is an error.
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
	cmd := core.SourceCommand(ctx, bin, "say", fieldStr(r, "platform"), "send", string(j))
	cmd.Env = append(os.Environ(), "OMOSENSE_DIR="+s.c.Dir, "OMOSENSE_STATE="+s.c.State)
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
