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
	"regexp"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
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
	rev     json.Number // IS-6: the row's revision (terminal content_seq)
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
	// IS-8: the opt-in done-verification verdict. Both are omitempty, so a
	// hook-less HERDR line is byte-identical to 0.1.0.
	Verify       string `json:"verify,omitempty"`
	VerifyDetail string `json:"verify_detail,omitempty"`
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

// watcher is the herdr source: seen statuses and the last logged error per
// machine survive across ticks, matching the module-level maps in
// watch-herdr.ts.
type watcher struct {
	state string
	sink  core.Sink
	own   paneID
	seen  map[string]seenRec
	errs  map[string]string
	sleep func(context.Context, time.Duration) error

	// IS-5: the 60-second dead-pane check, its pattern, and the panes already
	// reported dead.
	pattern  string
	agentRe  *regexp.Regexp
	deadAt   time.Time
	deadSeen map[string]bool

	// IS-6: the per-job-pane silence window.
	silent        map[string]silentState
	silentMinutes int

	// IS-12: whether the rpc source may own a job pane's session.
	rpcEnabled bool

	// IS-1: whether a blocked line is printed for every pane (herdr.blockedAll)
	// instead of only for registered job panes.
	blockedAll bool

	// IS-3/4: the quiet done batch and its persisted store.
	store    *doneStore
	inflight atomic.Int32

	// IS-8: the opt-in done-verification hook.
	verify        bool
	verifyCmd     []string
	verifyTimeout time.Duration
	hooks         sync.WaitGroup
}

func newWatcher(c *core.Ctx, sink core.Sink) *watcher {
	w := &watcher{
		state: c.State,
		sink:  sink,
		own:   ownPane(),
		seen:  map[string]seenRec{},
		errs:  map[string]string{},
		sleep: sleepFn,

		pattern:  c.Profile.Herdr.AgentPattern,
		agentRe:  c.Profile.Herdr.AgentRe,
		deadAt:   nowFn(),
		deadSeen: map[string]bool{},

		silent:        map[string]silentState{},
		silentMinutes: c.Profile.SilentMinutes,

		rpcEnabled: c.Profile.RPC.Enabled,

		blockedAll: c.Profile.Herdr.BlockedAll,
		store:      newDoneStore(c.State),

		verify:        len(c.Profile.Verify.Command) > 0 && c.Profile.Herdr.Verify,
		verifyCmd:     c.Profile.Verify.Command,
		verifyTimeout: verifyTimeout(c),
	}
	if w.agentRe == nil && w.pattern != "" {
		// core.Load always resolves a compiled pattern; a hand-built Ctx
		// (tests) may carry only the string.
		w.agentRe = regexp.MustCompile(w.pattern)
	}
	return w
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
	// IS-8: a held HERDR line is printed by its hook goroutine, so the source
	// never returns before every hook has finished.
	defer w.hooks.Wait()
	skip := "none"
	if w.own.kind == kindValue {
		skip = w.own.s
	}
	w.sink.Log(fmt.Sprintf("herdr watcher starting (every %ds, skip %s)", int(interval/time.Second), skip))
	// IS-4: the pending store is loaded here, not in newWatcher, so --once
	// stays read-only. A missing file creates nothing; a malformed one is
	// preserved aside before the watcher starts empty. Nothing else may start
	// a writable tick: a file that could not be read, or a malformed one that
	// could not be moved aside, stops the source with the error instead of
	// overwriting bytes it could not preserve.
	malformed, err := w.store.load()
	if err != nil {
		w.sink.Log("herdr pending: " + err.Error())
		if !malformed {
			return err
		}
		if qerr := w.store.quarantine(); qerr != nil {
			w.sink.Log("herdr pending: " + qerr.Error())
			return qerr
		}
	}
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
// seen is updated. blocked emits on the first observation, for a registered
// job pane or for every pane under herdr.blockedAll. A job pane's
// working→idle/done is recorded for the quiet batch (IS-3) instead of being
// printed. Status that did not change is quiet, even when the title did.
// Panes that disappear are forgotten.
func (w *watcher) tick(ctx context.Context, first bool) error {
	jobs := w.unowned(ctx, w.jobPanes())
	w.checkDeadPanes(ctx, jobs)
	snap := w.snapshot(ctx)
	if err := ctx.Err(); err != nil {
		return err
	}
	inSnap := make(map[string]bool, len(snap))
	silent := make(map[string]silentState, len(snap))
	for _, e := range snap {
		inSnap[e.key] = true
		if e.machine == "local" && e.agent.pane.equal(w.own) {
			continue
		}
		status := statusOf(e.agent)
		prev, had := w.seen[e.key]
		w.seen[e.key] = seenRec{status: status, title: snapTitle(e.agent)}
		jp, isJob := jobs[e.key]
		if isJob {
			// IS-6: a registered pane that stays working with no status
			// change and no revision growth is reported once per window.
			silent[e.key] = w.trackSilent(e, jp)
		}
		if had && prev.status == status {
			continue
		}
		switch {
		case status == "blocked":
			// IS-1: a blocked line is printed for a registered job pane (a
			// non-owned one), or for every pane under herdr.blockedAll. The
			// own pane was skipped above; seen is already updated either way.
			if !isJob && !w.blockedAll {
				break
			}
			var from *string
			if had {
				from = strPtr(prev.status)
			}
			w.emit(e.machine, e.agent, from, status)
		case !first && isJob && had && prev.status == "working" && (status == "idle" || status == "done"):
			// IS-3: the done is recorded for the quiet batch, not printed now.
			w.recordDone(ctx, e, jp, strPtr(prev.status), status)
		}
	}
	// IS-3: the flush runs after the snapshot loop, so a done recorded in this
	// tick resets the window. A cancelled context never prints the batch.
	if ctx.Err() == nil {
		w.flushBatch()
	}
	for key := range w.seen {
		if !inSnap[key] {
			delete(w.seen, key)
		}
	}
	w.silent = silent
	return nil
}

func (w *watcher) emit(machine string, a agent, from *string, to string) {
	w.sink.Emit("HERDR", w.event(machine, a, from, to))
}

func (w *watcher) event(machine string, a agent, from *string, to string) herdrEvent {
	return herdrEvent{
		Machine: machine,
		Pane:    a.pane.ptr(),
		Tab:     a.tab,
		Agent:   coalesce(a.display, a.name),
		Title:   a.title,
		Cwd:     a.cwd,
		From:    from,
		To:      to,
	}
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
	err := core.RunSource(cmd)
	if err == nil {
		return out.String(), errb.String(), 0
	}
	var ee *exec.ExitError
	if errors.As(err, &ee) {
		return out.String(), errb.String(), ee.ExitCode()
	}
	return out.String(), errb.String() + err.Error(), 127
}

// jobPanes reads <State>/threads.json. A closed entry (IS-4) and a falsy pane
// are skipped. A null or missing machine becomes "local"; an empty string does
// not, because `??` only replaces null and undefined.
func (w *watcher) jobPanes() map[string]jobPane {
	out := map[string]jobPane{}
	b, err := os.ReadFile(filepath.Join(w.state, "threads.json"))
	if err != nil {
		return out
	}
	v, err := core.ParseJSON(b)
	if err != nil {
		return out
	}
	add := func(key string, val any) {
		m, ok := val.(*core.OMap)
		if !ok || !core.ThreadActive(m) {
			return
		}
		pane, ok := truthyField(m, "pane")
		if !ok {
			return
		}
		machine := jobMachine(m)
		out[machine+"/"+pane] = jobPane{
			thread: key, machine: machine, pane: pane,
			session: sessionField(m), entry: m,
		}
	}
	switch x := v.(type) {
	case *core.OMap:
		for _, k := range x.Keys() {
			val, _ := x.Get(k)
			add(k, val)
		}
	case []any:
		for i, val := range x {
			add(strconv.Itoa(i), val)
		}
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
		rev:     fieldNumber(m, "revision"),
	}
}

// fieldNumber reads a JSON number field verbatim, so a revision survives the
// round trip without a float conversion. A missing, null or non-number value
// is the empty number.
func fieldNumber(m *core.OMap, key string) json.Number {
	raw, ok := m.Get(key)
	if !ok || raw == nil {
		return ""
	}
	n, isNum := raw.(json.Number)
	if !isNum {
		return ""
	}
	return n
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
