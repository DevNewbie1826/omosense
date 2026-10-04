package herdr

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

// interval is watch-herdr.ts's Bun.sleep(5_000).
const interval = 5 * time.Second

// sleepFn is replaced by tests so a tick loop can be stopped without
// waiting out the interval. Production calls sleepCtx.
var sleepFn = sleepCtx

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

// paneKind distinguishes the three JS states that `===` and template
// strings treat differently: a missing property (undefined), JSON null,
// and a present value. `??` collapses the first two, so the fields that
// use `??` are plain *string instead.
type paneKind int

const (
	kindMissing paneKind = iota
	kindNull
	kindValue
)

type paneID struct {
	kind paneKind
	s    string
}

func (p paneID) equal(o paneID) bool {
	if p.kind != o.kind {
		return false
	}
	if p.kind != kindValue {
		return true
	}
	return p.s == o.s
}

// interp is the JS template conversion: ${undefined} is "undefined",
// ${null} is "null".
func (p paneID) interp() string {
	switch p.kind {
	case kindNull:
		return "null"
	case kindMissing:
		return "undefined"
	default:
		return p.s
	}
}

func (p paneID) ptr() *string {
	if p.kind != kindValue {
		return nil
	}
	s := p.s
	return &s
}

type agent struct {
	pane    paneID
	tab     *string
	name    *string
	display *string
	status  *string
	cwd     *string
	title   *string
}

type seenRec struct {
	status string
	title  string
}

// herdrEvent is the HERDR payload. Every field is present; tab, agent,
// title, cwd and from are null when the TS expression used `?? null`.
type herdrEvent struct {
	Machine string  `json:"machine"`
	Pane    *string `json:"pane"`
	Tab     *string `json:"tab"`
	Agent   *string `json:"agent"`
	Title   *string `json:"title"`
	Cwd     *string `json:"cwd"`
	From    *string `json:"from"`
	To      string  `json:"to"`
}

type snapEntry struct {
	key     string
	machine string
	agent   agent
}

type target struct {
	machine string
	args    []string
}

// watcher is one profile's herdr source: seen statuses and the last logged
// error per machine survive across ticks, matching the module-level maps
// in watch-herdr.ts.
type watcher struct {
	profile string
	state   string
	sink    core.Sink
	own     paneID
	seen    map[string]seenRec
	errs    map[string]string
	sleep   func(context.Context, time.Duration) error
}

func newWatcher(c *core.Ctx, sink core.Sink) *watcher {
	return &watcher{
		profile: c.Profile.Name,
		state:   c.State,
		sink:    sink,
		own:     ownPane(),
		seen:    map[string]seenRec{},
		errs:    map[string]string{},
		sleep:   sleepFn,
	}
}

// ownPane reads HERDR_PANE_ID the way the TS module does: an unset variable
// is null (`?? null`), an empty string is a real value.
func ownPane() paneID {
	v, ok := os.LookupEnv("HERDR_PANE_ID")
	if !ok {
		return paneID{kind: kindNull}
	}
	return paneID{kind: kindValue, s: v}
}

// run prints the startup LOG and ticks until ctx is cancelled. A tick
// error is logged and the loop continues, matching the TS try/catch; the
// per-machine herdr failures are logged inside the snapshot instead.
func (w *watcher) run(ctx context.Context) error {
	skip := "none"
	if w.own.kind == kindValue {
		skip = w.own.s
	}
	w.sink.Log(fmt.Sprintf("herdr watcher starting (profile %s, every %ds, skip %s)", w.profile, int(interval/time.Second), skip))
	first := true
	for {
		if ctx.Err() != nil {
			return nil
		}
		if err := w.tick(ctx, first); err != nil && ctx.Err() == nil {
			w.sink.Log("herdr " + err.Error())
		}
		if ctx.Err() != nil {
			return nil
		}
		first = false
		if err := w.sleep(ctx, interval); err != nil {
			return nil
		}
	}
}

// once is the read-only --once path: the same snapshot as a tick, printed
// as SNAP lines, with no lock and no seen-state updates. Snapshot errors
// still log, because --once calls snapshot() in the TS source too.
func (w *watcher) once(ctx context.Context) {
	for _, e := range w.snapshot(ctx) {
		w.sink.Raw("SNAP", fmt.Sprintf("%s %s %s %s", e.key, statusOf(e.agent), snapAgent(e.agent), snapTitle(e.agent)))
	}
}

// tick applies watch-herdr.ts's rules. own-pane rows are skipped before
// seen is updated. blocked emits on the first observation; working→idle
// and working→done emit only for a job pane that is not the family pane
// and not on the first tick. Status that did not change is quiet, even
// when the title did. Panes that disappear are forgotten.
func (w *watcher) tick(ctx context.Context, first bool) error {
	fam := w.familyPane()
	jobs := w.jobPanes()
	snap := w.snapshot(ctx)
	if err := ctx.Err(); err != nil {
		return err
	}
	inSnap := make(map[string]bool, len(snap))
	for _, e := range snap {
		inSnap[e.key] = true
		if e.machine == "local" && e.agent.pane.equal(w.own) {
			continue
		}
		status := statusOf(e.agent)
		prev, had := w.seen[e.key]
		w.seen[e.key] = seenRec{status: status, title: snapTitle(e.agent)}
		if had && prev.status == status {
			continue
		}
		isFamily := e.machine == "local" && e.agent.pane.equal(fam)
		switch {
		case status == "blocked":
			var from *string
			if had {
				from = strPtr(prev.status)
			}
			w.emit(e.machine, e.agent, from, status)
		case !first && !isFamily && jobs[e.key] && had && prev.status == "working" && (status == "idle" || status == "done"):
			w.emit(e.machine, e.agent, strPtr(prev.status), status)
		}
	}
	for key := range w.seen {
		if !inSnap[key] {
			delete(w.seen, key)
		}
	}
	return nil
}

func (w *watcher) emit(machine string, a agent, from *string, to string) {
	w.sink.Emit("HERDR", herdrEvent{
		Machine: machine,
		Pane:    a.pane.ptr(),
		Tab:     a.tab,
		Agent:   coalesce(a.display, a.name),
		Title:   a.title,
		Cwd:     a.cwd,
		From:    from,
		To:      to,
	})
}

func (w *watcher) snapshot(ctx context.Context) []snapEntry {
	targets := []target{{machine: "local", args: []string{"agent", "list"}}}
	for _, m := range w.machineNames(ctx) {
		targets = append(targets, target{machine: m, args: []string{"--machine", m, "agent", "list"}})
	}
	var out []snapEntry
	index := map[string]int{}
	for _, t := range targets {
		if ctx.Err() != nil {
			break
		}
		w.collect(ctx, t, &out, index)
	}
	return out
}

// collect runs one herdr target. A repeated pane id keeps its first Map
// position and takes the later payload, matching Map.set. An error is
// logged once per distinct message until a later success clears it; the
// TS source stringifies the thrown Error, which prefixes "Error: ".
func (w *watcher) collect(ctx context.Context, t target, out *[]snapEntry, index map[string]int) {
	v, err := w.herdrJSON(ctx, t.args)
	if err != nil {
		if ctx.Err() != nil {
			return
		}
		w.noteError(t.machine, err)
		return
	}
	agents, err := resultAgents(v)
	if err != nil {
		w.noteError(t.machine, err)
		return
	}
	for _, raw := range agents {
		m, ok := raw.(*core.OMap)
		if !ok {
			w.noteError(t.machine, errors.New("TypeError: agent is not an object"))
			return
		}
		a := parseAgent(m)
		key := t.machine + "/" + a.pane.interp()
		e := snapEntry{key: key, machine: t.machine, agent: a}
		if i, exists := index[key]; exists {
			(*out)[i] = e
		} else {
			index[key] = len(*out)
			*out = append(*out, e)
		}
	}
	delete(w.errs, t.machine)
}

func (w *watcher) noteError(machine string, err error) {
	msg := err.Error()
	if prev, ok := w.errs[machine]; ok && prev == msg {
		return
	}
	w.errs[machine] = msg
	w.sink.Log("herdr " + machine + " " + msg)
}

// machineNames asks `herdr machine list --json` and swallows every failure
// (non-zero exit, bad JSON, a non-object row). watch-herdr.ts returns []
// in all of those cases and does not log.
func (w *watcher) machineNames(ctx context.Context) []string {
	v, err := w.herdrJSON(ctx, []string{"machine", "list", "--json"})
	if err != nil {
		return nil
	}
	list, err := machineEntries(v)
	if err != nil {
		return nil
	}
	var names []string
	for _, e := range list {
		m, ok := e.(*core.OMap)
		if !ok {
			return nil
		}
		if !machineEnabled(m) {
			continue
		}
		if name, ok := machineName(m); ok {
			names = append(names, name)
		}
	}
	return names
}

// herdrJSON runs herdr and parses stdout. A non-zero exit becomes
// `Error: <trimmed stderr>` (or `Error: exit N` when stderr is blank),
// truncated to 200 runes. A parse failure becomes `SyntaxError: ...`,
// which is what String(JSON.parse's throw) starts with.
func (w *watcher) herdrJSON(ctx context.Context, args []string) (any, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	stdout, stderr, code := execHerdr(ctx, args)
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if code != 0 {
		msg := strings.TrimSpace(stderr)
		if msg == "" {
			msg = fmt.Sprintf("exit %d", code)
		} else {
			msg = core.Trunc(msg, 200)
		}
		return nil, errors.New("Error: " + msg)
	}
	v, err := core.ParseJSON([]byte(stdout))
	if err != nil {
		return nil, errors.New("SyntaxError: " + err.Error())
	}
	return v, nil
}

func execHerdr(ctx context.Context, args []string) (stdout, stderr string, code int) {
	cmd := core.SourceCommand(ctx, "herdr", args...)
	var out, errb bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &errb
	err := cmd.Run()
	if err == nil {
		return out.String(), errb.String(), 0
	}
	var ee *exec.ExitError
	if errors.As(err, &ee) {
		return out.String(), errb.String(), ee.ExitCode()
	}
	return out.String(), errb.String() + err.Error(), 127
}

// familyPane reads <State>/sessions.json `.family.pane`, or null when the
// file is missing, unparsable, or the pane is absent. The TS expression
// ends in `?? null`, so missing and null are the same.
func (w *watcher) familyPane() paneID {
	b, err := os.ReadFile(filepath.Join(w.state, "sessions.json"))
	if err != nil {
		return paneID{kind: kindNull}
	}
	v, err := core.ParseJSON(b)
	if err != nil {
		return paneID{kind: kindNull}
	}
	m, ok := v.(*core.OMap)
	if !ok {
		return paneID{kind: kindNull}
	}
	raw, ok := m.Get("family")
	if !ok || raw == nil {
		return paneID{kind: kindNull}
	}
	fm, ok := raw.(*core.OMap)
	if !ok {
		return paneID{kind: kindNull}
	}
	return nullishPane(fm, "pane")
}

// jobPanes reads <State>/threads.json. A falsy pane is skipped. A null or
// missing machine becomes "local"; an empty string does not, because `??`
// only replaces null and undefined.
func (w *watcher) jobPanes() map[string]bool {
	out := map[string]bool{}
	b, err := os.ReadFile(filepath.Join(w.state, "threads.json"))
	if err != nil {
		return out
	}
	v, err := core.ParseJSON(b)
	if err != nil {
		return out
	}
	var vals []any
	switch x := v.(type) {
	case *core.OMap:
		for _, k := range x.Keys() {
			val, _ := x.Get(k)
			vals = append(vals, val)
		}
	case []any:
		vals = x
	default:
		return out
	}
	for _, val := range vals {
		m, ok := val.(*core.OMap)
		if !ok {
			continue
		}
		pane, ok := truthyField(m, "pane")
		if !ok {
			continue
		}
		out[jobMachine(m)+"/"+pane] = true
	}
	return out
}

func jobMachine(m *core.OMap) string {
	raw, ok := m.Get("machine")
	if !ok || raw == nil {
		return "local"
	}
	s, ok := jsPrimitive(raw)
	if !ok {
		return "local"
	}
	return s
}

func resultAgents(v any) ([]any, error) {
	m, ok := v.(*core.OMap)
	if !ok || m == nil {
		return []any{}, nil
	}
	raw, ok := m.Get("result")
	if !ok || raw == nil {
		return []any{}, nil
	}
	rm, ok := raw.(*core.OMap)
	if !ok {
		return []any{}, nil
	}
	a, ok := rm.Get("agents")
	if !ok || a == nil {
		return []any{}, nil
	}
	arr, ok := a.([]any)
	if !ok {
		return nil, errors.New("TypeError: agents is not iterable")
	}
	return arr, nil
}

// machineEntries mirrors `Array.isArray(d) ? d : d.result?.machines ?? d.machines ?? []`.
func machineEntries(v any) ([]any, error) {
	if arr, ok := v.([]any); ok {
		return arr, nil
	}
	m, ok := v.(*core.OMap)
	if !ok || m == nil {
		return []any{}, nil
	}
	if raw, ok := m.Get("result"); ok && raw != nil {
		if rm, ok := raw.(*core.OMap); ok {
			if a, ok := rm.Get("machines"); ok && a != nil {
				return asArray(a)
			}
		}
	}
	if a, ok := m.Get("machines"); ok && a != nil {
		return asArray(a)
	}
	return []any{}, nil
}

func asArray(v any) ([]any, error) {
	arr, ok := v.([]any)
	if !ok {
		return nil, errors.New("TypeError: not iterable")
	}
	return arr, nil
}

// machineEnabled is `enabled !== false`: only a boolean false drops a row.
func machineEnabled(m *core.OMap) bool {
	raw, ok := m.Get("enabled")
	if !ok || raw == nil {
		return true
	}
	b, isBool := raw.(bool)
	return !isBool || b
}

// machineName is `label ?? id`, then filter(Boolean). An empty label does
// not fall through to id.
func machineName(m *core.OMap) (string, bool) {
	if s, mode := lookupJS(m, "label"); mode != modeNullish {
		if mode != modeTruthy {
			return "", false
		}
		return s, true
	}
	if s, mode := lookupJS(m, "id"); mode == modeTruthy {
		return s, true
	}
	return "", false
}

type jsMode int

const (
	modeNullish jsMode = iota
	modeFalsy
	modeTruthy
)

// lookupJS reports JS nullish / falsy / truthy. String "0" and "false" are
// truthy; only "" , numeric 0 and boolean false are falsy. The string form
// is what a template would produce when the value is used as a name.
func lookupJS(m *core.OMap, key string) (string, jsMode) {
	raw, ok := m.Get(key)
	if !ok || raw == nil {
		return "", modeNullish
	}
	switch x := raw.(type) {
	case string:
		if x == "" {
			return "", modeFalsy
		}
		return x, modeTruthy
	case bool:
		if !x {
			return "false", modeFalsy
		}
		return "true", modeTruthy
	case json.Number:
		f, err := x.Float64()
		if err != nil || f == 0 {
			return x.String(), modeFalsy
		}
		return x.String(), modeTruthy
	default:
		return "", modeFalsy
	}
}

func truthyField(m *core.OMap, key string) (string, bool) {
	s, mode := lookupJS(m, key)
	if mode != modeTruthy {
		return "", false
	}
	return s, true
}

func jsPrimitive(v any) (string, bool) {
	switch x := v.(type) {
	case string:
		return x, true
	case json.Number:
		return x.String(), true
	case bool:
		if x {
			return "true", true
		}
		return "false", true
	default:
		return "", false
	}
}

func parseAgent(m *core.OMap) agent {
	return agent{
		pane:    fieldPane(m, "pane_id"),
		tab:     fieldPtr(m, "tab_id"),
		name:    fieldPtr(m, "agent"),
		display: fieldPtr(m, "display_agent"),
		status:  fieldPtr(m, "agent_status"),
		cwd:     fieldPtr(m, "cwd"),
		title:   fieldPtr(m, "terminal_title_stripped"),
	}
}

func fieldPane(m *core.OMap, key string) paneID {
	raw, ok := m.Get(key)
	if !ok {
		return paneID{kind: kindMissing}
	}
	if raw == nil {
		return paneID{kind: kindNull}
	}
	s, ok := jsPrimitive(raw)
	if !ok {
		return paneID{kind: kindMissing}
	}
	return paneID{kind: kindValue, s: s}
}

func nullishPane(m *core.OMap, key string) paneID {
	raw, ok := m.Get(key)
	if !ok || raw == nil {
		return paneID{kind: kindNull}
	}
	s, ok := jsPrimitive(raw)
	if !ok {
		return paneID{kind: kindNull}
	}
	return paneID{kind: kindValue, s: s}
}

func fieldPtr(m *core.OMap, key string) *string {
	raw, ok := m.Get(key)
	if !ok || raw == nil {
		return nil
	}
	s, ok := jsPrimitive(raw)
	if !ok {
		return nil
	}
	return &s
}

func statusOf(a agent) string {
	if a.status == nil {
		return "unknown"
	}
	return *a.status
}

func snapAgent(a agent) string {
	if a.display != nil {
		return *a.display
	}
	if a.name != nil {
		return *a.name
	}
	return ""
}

func snapTitle(a agent) string {
	if a.title == nil {
		return ""
	}
	return *a.title
}

func coalesce(ps ...*string) *string {
	for _, p := range ps {
		if p != nil {
			return p
		}
	}
	return nil
}

func strPtr(s string) *string { return &s }
