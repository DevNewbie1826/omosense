// IS-5 dead panes, IS-6 herdr silent sessions, IS-8 opt-in done verification
// and IS-12's rpc-ownership fallback. The 0.1.0 HERDR grammar is untouched:
// every new HERDR field is omitempty and every new line is a LOG line.
package herdr

import (
	"context"
	"encoding/json"
	"errors"
	"sort"
	"strings"
	"time"

	"github.com/DevNewbie1826/omosense/internal/core"
	"github.com/DevNewbie1826/omosense/internal/rpc"
)

// deadInterval is the IS-5 cadence: the pane checks run once a minute, not on
// every 5-second tick.
const deadInterval = time.Minute

// nowFn is the clock the IS-5, IS-6 and IS-3 windows read. Only the tick
// goroutine reads it; a hook never does. Tests replace it, so no check waits
// out real time.
var nowFn = time.Now

// runVerifyFn is the done-verification runner recordDone hands every hook to.
// Tests replace it to observe the timeout actually delivered to the hook; in
// production it is core.RunVerify unchanged.
var runVerifyFn = core.RunVerify

// jobPane is one registered thread that names a pane (IS-4): the threads.json
// key, the pane identity, and the session field IS-12 matches on.
type jobPane struct {
	thread  string
	machine string
	pane    string
	session string
	entry   *core.OMap
}

// sessionField is the entry's session identity: the first non-empty of the
// three field names the rpc matcher accepts (IS-12).
func sessionField(m *core.OMap) string {
	for _, key := range []string{"session_id", "session", "durable_session_id"} {
		if v, ok := m.Get(key); ok {
			if s, isStr := v.(string); isStr && s != "" {
				return s
			}
		}
	}
	return ""
}

// unowned drops the job panes the rpc source owns (IS-12): an ACTIVE entry
// with both a session field and a pane is left to rpc while a successful
// list_sessions names a session the entry matches. A socket error or no match
// falls back to herdr, so the pane is reported as usual; neither case logs.
func (w *watcher) unowned(ctx context.Context, jobs map[string]jobPane) map[string]jobPane {
	if !w.rpcEnabled || len(jobs) == 0 {
		return jobs
	}
	entries := map[string]*core.OMap{}
	for _, jp := range jobs {
		if jp.session != "" {
			entries[jp.thread] = jp.entry
		}
	}
	if len(entries) == 0 {
		return jobs
	}
	owned, err := rpc.OwnedThreads(ctx, entries)
	if err != nil {
		return jobs
	}
	for key, jp := range jobs {
		if owned[jp.thread] {
			delete(jobs, key)
		}
	}
	return jobs
}

// deadPaneLine is the IS-5 `LOG dead-pane` payload.
type deadPaneLine struct {
	Machine    string   `json:"machine"`
	Pane       string   `json:"pane"`
	Thread     string   `json:"thread"`
	Reason     string   `json:"reason"`
	Foreground []string `json:"foreground"`
	Pattern    string   `json:"pattern"`
}

// checkDeadPanes is IS-5: once per deadInterval, every active non-owned job
// pane is checked. `pane list` decides existence first - a pane absent from a
// successful list is "pane gone", with no process-info call and no LOG herdr -
// and only panes it lists get `pane process-info`, where the pane is dead when
// no foreground process's argv (joined by spaces) matches herdr.agentPattern.
// A failing herdr call is never dead: it goes to the once-per-message LOG
// herdr path. Each pane is reported once until it is alive again.
func (w *watcher) checkDeadPanes(ctx context.Context, jobs map[string]jobPane) {
	live := make(map[string]bool, len(jobs))
	for key := range jobs {
		live[key] = true
	}
	for key := range w.deadSeen {
		if !live[key] {
			delete(w.deadSeen, key)
		}
	}
	if len(jobs) == 0 || w.agentRe == nil {
		return
	}
	now := nowFn()
	if !w.deadAt.IsZero() && now.Sub(w.deadAt) < deadInterval {
		return
	}
	w.deadAt = now
	byMachine := map[string][]jobPane{}
	for _, jp := range jobs {
		byMachine[jp.machine] = append(byMachine[jp.machine], jp)
	}
	for _, machine := range machineOrder(byMachine) {
		if ctx.Err() != nil {
			return
		}
		w.checkDeadMachine(ctx, machine, byMachine[machine])
	}
}

func (w *watcher) checkDeadMachine(ctx context.Context, machine string, panes []jobPane) {
	v, err := w.herdrJSON(ctx, paneListArgs(machine))
	if err != nil {
		if ctx.Err() == nil {
			w.noteError(machine, err)
		}
		return
	}
	ids, err := paneListIDs(v)
	if err != nil {
		w.noteError(machine, err)
		return
	}
	for _, jp := range panes {
		if ctx.Err() != nil {
			return
		}
		if !ids[jp.pane] {
			w.reportDead(jp, "pane gone", []string{})
			continue
		}
		v, err := w.herdrJSON(ctx, processInfoArgs(machine, jp.pane))
		if err != nil {
			if ctx.Err() == nil {
				w.noteError(machine, err)
			}
			continue
		}
		procs, err := foregroundProcesses(v)
		if err != nil {
			w.noteError(machine, err)
			continue
		}
		if w.agentAlive(procs) {
			delete(w.deadSeen, jp.machine+"/"+jp.pane)
			continue
		}
		w.reportDead(jp, "no agent process", procTexts(procs))
	}
}

func (w *watcher) reportDead(jp jobPane, reason string, foreground []string) {
	key := jp.machine + "/" + jp.pane
	if w.deadSeen[key] {
		return
	}
	w.deadSeen[key] = true
	payload, err := json.Marshal(deadPaneLine{
		Machine:    jp.machine,
		Pane:       jp.pane,
		Thread:     jp.thread,
		Reason:     reason,
		Foreground: foreground,
		Pattern:    w.pattern,
	})
	if err != nil {
		w.sink.Log("dead-pane")
		return
	}
	w.sink.Log("dead-pane " + string(payload))
}

// agentAlive reports whether any foreground process still runs an agent: its
// argv joined by spaces matches herdr.agentPattern. An empty foreground list
// (the shell at its prompt) is not alive.
func (w *watcher) agentAlive(procs []foregroundProcess) bool {
	for _, p := range procs {
		if w.agentRe.MatchString(strings.Join(p.argv, " ")) {
			return true
		}
	}
	return false
}

// foregroundProcess is one `pane process-info` foreground_processes entry.
type foregroundProcess struct {
	argv    []string
	cmdline string
}

// text is the process line the dead-pane report carries: the reported cmdline
// when the row has one, else its argv joined by spaces.
func (p foregroundProcess) text() string {
	if p.cmdline != "" {
		return p.cmdline
	}
	return strings.Join(p.argv, " ")
}

func procTexts(procs []foregroundProcess) []string {
	out := make([]string, 0, len(procs))
	for _, p := range procs {
		out = append(out, p.text())
	}
	return out
}

func paneListArgs(machine string) []string {
	if machine == "local" {
		return []string{"pane", "list"}
	}
	return []string{"--machine", machine, "pane", "list"}
}

func processInfoArgs(machine, pane string) []string {
	if machine == "local" {
		return []string{"pane", "process-info", "--pane", pane}
	}
	return []string{"--machine", machine, "pane", "process-info", "--pane", pane}
}

// machineOrder returns the machines of one dead check with "local" first, like
// the snapshot, then the rest in a stable order.
func machineOrder(byMachine map[string][]jobPane) []string {
	names := make([]string, 0, len(byMachine))
	for m := range byMachine {
		if m != "local" {
			names = append(names, m)
		}
	}
	sort.Strings(names)
	if _, ok := byMachine["local"]; ok {
		names = append([]string{"local"}, names...)
	}
	return names
}

// paneListIDs reads result.panes. A response that is not the pane list shape
// is an error, never an empty list: "absent from a successful list" is what
// means pane gone, so a broken response must not report every pane dead.
func paneListIDs(v any) (map[string]bool, error) {
	m, ok := v.(*core.OMap)
	if !ok || m == nil {
		return nil, errors.New("TypeError: pane list is not an object")
	}
	raw, ok := m.Get("result")
	if !ok || raw == nil {
		return nil, errors.New("TypeError: pane list has no result")
	}
	rm, ok := raw.(*core.OMap)
	if !ok {
		return nil, errors.New("TypeError: pane list result is not an object")
	}
	raw, ok = rm.Get("panes")
	if !ok || raw == nil {
		return nil, errors.New("TypeError: pane list has no panes")
	}
	arr, ok := raw.([]any)
	if !ok {
		return nil, errors.New("TypeError: panes is not an array")
	}
	out := make(map[string]bool, len(arr))
	for _, e := range arr {
		em, ok := e.(*core.OMap)
		if !ok {
			continue
		}
		if id, ok := em.Get("pane_id"); ok {
			if s, isStr := id.(string); isStr && s != "" {
				out[s] = true
			}
		}
	}
	return out, nil
}

// foregroundProcesses reads result.process_info.foreground_processes. The same
// rule as paneListIDs applies: a response that is not this shape is an error,
// so a broken call is never read as "no agent process".
func foregroundProcesses(v any) ([]foregroundProcess, error) {
	m, ok := v.(*core.OMap)
	if !ok || m == nil {
		return nil, errors.New("TypeError: process-info is not an object")
	}
	raw, ok := m.Get("result")
	if !ok || raw == nil {
		return nil, errors.New("TypeError: process-info has no result")
	}
	rm, ok := raw.(*core.OMap)
	if !ok {
		return nil, errors.New("TypeError: process-info result is not an object")
	}
	raw, ok = rm.Get("process_info")
	if !ok || raw == nil {
		return nil, errors.New("TypeError: process-info has no process_info")
	}
	pim, ok := raw.(*core.OMap)
	if !ok {
		return nil, errors.New("TypeError: process_info is not an object")
	}
	raw, ok = pim.Get("foreground_processes")
	if !ok || raw == nil {
		return nil, errors.New("TypeError: process_info has no foreground_processes")
	}
	arr, ok := raw.([]any)
	if !ok {
		return nil, errors.New("TypeError: foreground_processes is not an array")
	}
	out := make([]foregroundProcess, 0, len(arr))
	for _, e := range arr {
		em, ok := e.(*core.OMap)
		if !ok {
			return nil, errors.New("TypeError: foreground process is not an object")
		}
		var p foregroundProcess
		if v, ok := em.Get("cmdline"); ok {
			if s, isStr := v.(string); isStr {
				p.cmdline = s
			}
		}
		if v, ok := em.Get("argv"); ok {
			if list, isArr := v.([]any); isArr {
				for _, a := range list {
					if s, isStr := a.(string); isStr {
						p.argv = append(p.argv, s)
					}
				}
			}
		}
		out = append(out, p)
	}
	return out, nil
}

// silentState tracks one job pane's silence window (IS-6): the last activity
// (a status change or revision growth), the last sampled status and revision,
// and whether the current window was already reported.
type silentState struct {
	last     time.Time
	status   string
	revision json.Number
	reported bool
}

// trackSilent updates the IS-6 window for one job pane present in the agent
// list. A pane that stays working with no status change and no revision growth
// for silentMinutes is reported once; a status change or a revision increase
// re-arms it. A pane missing from the agent list is not working, so it is
// never silent (dead-pane covers it).
func (w *watcher) trackSilent(e snapEntry, jp jobPane) silentState {
	now := nowFn()
	status := statusOf(e.agent)
	rev := e.agent.rev
	s, tracked := w.silent[e.key]
	switch {
	case !tracked:
		s = silentState{last: now, status: status, revision: rev}
	case status != s.status || revisionIncreased(rev, s.revision):
		s.last, s.status, s.revision, s.reported = now, status, rev, false
	case status == "working" && !s.reported && w.silentMinutes > 0 &&
		now.Sub(s.last) >= time.Duration(w.silentMinutes)*time.Minute:
		s.reported = true
		w.sink.Log(silentLine(e, jp, s.last, now))
	}
	s.revision = rev
	return s
}

// silentLine is the IS-6 herdr silent-session payload: a registered pane
// stayed working with no status change and no revision growth since `since`.
func silentLine(e snapEntry, jp jobPane, since, now time.Time) string {
	var signal any
	if e.agent.rev != "" {
		signal = e.agent.rev
	}
	payload, err := json.Marshal(struct {
		Source  string `json:"source"`
		Machine string `json:"machine"`
		Pane    string `json:"pane"`
		Thread  string `json:"thread"`
		Since   string `json:"since"`
		Minutes int    `json:"minutes"`
		Signal  any    `json:"signal"`
	}{"herdr", e.machine, jp.pane, jp.thread, core.ISO(since), int(now.Sub(since).Minutes()), signal})
	if err != nil {
		return "silent-session"
	}
	return "silent-session " + string(payload)
}

// revisionIncreased reports whether a revision moved forward: the IS-6 output
// growth signal. The JSON numbers compare by value, so "9" and "10" order
// correctly; a revision that is absent on one side counts as growth only when
// it newly appeared.
func revisionIncreased(cur, prev json.Number) bool {
	if cur == prev {
		return false
	}
	cf, cerr := cur.Float64()
	pf, perr := prev.Float64()
	if cerr != nil || perr != nil {
		return prev == "" && cur != ""
	}
	return cf > pf
}

// recordDone records a job pane's working->idle/done transition for the IS-3
// quiet batch. The record is written synchronously, at the transition time, so
// a crash between here and the flush keeps the done. With the opt-in
// herdr.done hook (IS-5) the entry carries verify "pending" and its sequence,
// and the hook goroutine only writes the verdict back onto that same
// transition; on cancel it writes unverified/cancelled, so no transition is
// lost. The goroutines are joined by run before the source returns.
func (w *watcher) recordDone(ctx context.Context, e snapEntry, jp jobPane, from *string, to string) {
	entry, err := w.store.record(e.key, w.event(e.machine, e.agent, from, to), nowFn(), w.verify)
	if err != nil {
		w.sink.Log("herdr pending: " + err.Error())
		return
	}
	if !w.verify {
		return
	}
	w.inflight.Add(1)
	w.hooks.Add(1)
	go func() {
		defer w.hooks.Done()
		defer w.inflight.Add(-1)
		res := runVerifyFn(ctx, w.verifyCmd, w.verifyTimeout, verifyEnv(e, jp), deref(e.agent.cwd))
		status, detail := res.Status, res.Detail
		if status == "cancelled" {
			status, detail = "unverified", "cancelled"
		}
		if err := w.store.setVerify(entry.key, entry.seq, status, detail); err != nil {
			w.sink.Log("herdr pending: " + err.Error())
		}
	}()
}

// flushBatch prints the recorded burst as ONE done-batch line once the quiet
// window has passed. A hook still in flight holds it back, so the line always
// carries the finished verdicts; the record itself is already durable, so
// holding costs nothing.
func (w *watcher) flushBatch() {
	if w.inflight.Load() != 0 {
		return
	}
	if _, err := w.store.flush(w.sink, nowFn(), batchQuiet); err != nil {
		w.sink.Log("herdr pending: " + err.Error())
	}
}

// verifyEnv is the IS-5 hook environment: the recorded transition's identity.
func verifyEnv(e snapEntry, jp jobPane) []string {
	return []string{
		"OMOSENSE_DONE_SOURCE=herdr",
		"OMOSENSE_DONE_PANE=" + jp.pane,
		"OMOSENSE_DONE_MACHINE=" + e.machine,
		"OMOSENSE_DONE_THREAD=" + jp.thread,
		"OMOSENSE_DONE_CWD=" + deref(e.agent.cwd),
	}
}

// verifyTimeout is the configured per-attempt hook timeout, falling back to
// the config default for a profile built without one.
func verifyTimeout(c *core.Ctx) time.Duration {
	if sec := c.Profile.Verify.TimeoutSec; sec > 0 {
		return time.Duration(sec) * time.Second
	}
	return 60 * time.Second
}

func deref(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}
