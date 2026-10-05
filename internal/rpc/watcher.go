package rpc

import (
	"context"
	"fmt"
	"time"

	"github.com/DevNewbie1826/omosense/internal/core"
)

const interval = 5 * time.Second

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

type record struct {
	entry                              entry
	status                             string
	active, pollArmed                  bool
	activeEpoch, startEpoch, doneEpoch int
	streamSeq                          uint64
}

type watcher struct {
	profile, stateDir, socket string
	all, first, ready         bool
	sink                      core.Sink
	seen                      map[string]record
	listed                    map[string]bool
	errs                      map[string]string
	sleep                     func(context.Context, time.Duration) error
	record                    func(durable string, ev rpcEvent)
	handles                   map[string]sessionInfo
	turns                     map[string]streamTurn
	closed                    map[string]string
	deferred                  map[string][]deferredDone
	streamErrors              map[string]bool
	streamUp, draining        bool
	snapshotHeld, upHeld      bool
	streamEpoch               int
	streamSeq                 uint64
	queue                     *streamFIFO
}

func newWatcher(c *core.Ctx, sink core.Sink) *watcher {
	return &watcher{
		profile: c.Profile.Name, stateDir: c.State, socket: socketPath(),
		all: watchAll(c), first: true, sink: sink, sleep: sleepFn,
		seen: map[string]record{}, listed: map[string]bool{}, errs: map[string]string{},
		handles: map[string]sessionInfo{}, turns: map[string]streamTurn{},
		closed: map[string]string{}, deferred: map[string][]deferredDone{},
		streamErrors: map[string]bool{},
	}
}

func (w *watcher) run(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	mode := "threads"
	if w.all {
		mode = "all"
	}
	w.sink.Log(fmt.Sprintf("rpc watcher starting (profile %s, every 5s, watch %s, sock %s)", w.profile, mode, w.socket))
	w.queue = newStreamFIFO()
	readerDone := make(chan struct{})
	go func() {
		defer close(readerDone)
		w.streamLoop(ctx, w.queue)
	}()
	// One sleeper at a time keeps the existing sleep hook, without blocking
	// stream application between ticks. Both auxiliaries are joined on return.
	timer := make(chan error, 1)
	var timerDone chan struct{}
	armTimer := func() {
		timerDone = make(chan struct{})
		go func() {
			defer close(timerDone)
			timer <- w.sleep(ctx, interval)
		}()
	}
	w.tick(ctx)
	armTimer()
loop:
	for ctx.Err() == nil {
		select {
		case <-ctx.Done():
			break loop
		case <-w.queue.wake:
			w.drainStream(ctx)
		case err := <-timer:
			<-timerDone
			if err != nil {
				break loop
			}
			w.tick(ctx)
			armTimer()
		}
	}
	cancel()
	<-timerDone
	<-readerDone
	return nil
}

func (w *watcher) noteError(key string, err error) {
	msg := err.Error()
	if w.errs[key] != msg {
		w.errs[key] = msg
		if key == "list" {
			w.sink.Log("rpc " + msg)
		} else {
			w.sink.Log("rpc " + key + " " + msg)
		}
		w.ready = false
	}
}

func (w *watcher) once(ctx context.Context) error {
	_, entries, _, err := w.snapshot(ctx, true)
	if err != nil {
		return err
	}
	for _, e := range entries {
		if e.valid {
			w.sink.Raw("SNAP", fmt.Sprintf("%s %s %s %s", e.info.id(), e.state.status(), snapText(e.thread), snapText(e.info.Name)))
		}
	}
	return nil
}

func snapText(s *string) string {
	if s == nil || *s == "" {
		return "-"
	}
	return *s
}

func (w *watcher) tick(ctx context.Context) {
	w.poll(ctx, false)
}

func (w *watcher) poll(ctx context.Context, reconcile bool) {
	w.drainStream(ctx)
	w.expireDeferred()
	seq := w.streamSeq
	list, entries, healthy, err := w.snapshot(ctx, false)
	// A connect drained while this older snapshot waits reconciles only after
	// it is applied: snapshots apply in the order they were taken, so this one
	// neither re-arms what the reconciliation concludes nor is dropped.
	w.snapshotHeld = true
	w.drainStream(ctx)
	w.snapshotHeld = false
	defer w.reconcileHeld(ctx)
	if ctx.Err() != nil {
		return
	}
	if err != nil {
		w.noteError("list", err)
		return
	}
	w.refreshHandles(list)
	listed := make(map[string]bool, len(list))
	present := make(map[string]bool, len(list))
	for _, s := range list {
		id := s.id()
		present[id] = true
		listed[id] = true
	}
	seen := make(map[string]record, len(entries))
	for _, e := range entries {
		id := e.info.id()
		if ended, ok := w.closed[e.info.Session]; ok && ended == id {
			continue
		}
		prev, observed := w.seen[id]
		if !e.valid {
			if observed {
				seen[id] = prev
			} else if !w.first && !w.listed[id] {
				// Defer a new watched session until its first valid state.
				delete(listed, id)
			}
			continue
		}
		next := prev
		status := e.state.status()
		var from *string
		if prev.status != "" {
			from = ptr(prev.status)
		}
		if prev.streamSeq <= seq {
			next.entry = e
			switch {
			case status == "blocked" && prev.status != "blocked":
				w.emit("blocked", e, from, status)
			case status == "idle" && prev.active && (!w.streamUp || prev.activeEpoch < w.streamEpoch || prev.pollArmed):
				w.conclude(&next, e)
			}
			if status == "working" && !next.active && (!w.streamUp || reconcile || (e.state.Compacting && !e.state.Streaming)) {
				next.active, next.activeEpoch = true, w.streamEpoch
				next.pollArmed = w.streamUp && e.state.Compacting && !e.state.Streaming
				if reconcile {
					// Bindings seen here belong to ended connections.
					w.turns[e.info.Session] = streamTurn{info: e.info}
				}
			}
			next.status = status
		}
		if !w.first && !w.listed[id] {
			w.emit("opened", e, nil, e.state.status())
		}
		seen[id] = next
	}
	for id, prev := range w.seen {
		if !present[id] {
			// A list captured before these records cannot erase their state.
			if prev.streamSeq > seq {
				seen[id] = prev
				continue
			}
			var from *string
			if prev.status != "" {
				from = ptr(prev.status)
			}
			if ended, ok := w.closed[prev.entry.info.Session]; !ok || ended != id {
				w.emit("closed", prev.entry, from, "closed")
			}
		}
	}
	w.seen, w.listed, w.first = seen, listed, false
	if healthy && !w.ready {
		w.sink.Log(fmt.Sprintf("rpc ready (%d sessions, %d watched)", len(list), len(entries)))
		w.ready = true
	}
}

type rpcEvent struct {
	Event     string   `json:"event"`
	Session   string   `json:"session"`
	ID        string   `json:"id"`
	Name      *string  `json:"name"`
	Cwd       *string  `json:"cwd"`
	Thread    *string  `json:"thread"`
	From      *string  `json:"from"`
	To        string   `json:"to"`
	Questions []string `json:"questions"`
}

func (w *watcher) emit(event string, e entry, from *string, to string) {
	questions := []string{}
	if event == "blocked" {
		for _, p := range e.state.Pending {
			for _, q := range p.Questions {
				text := q.Question
				if text == "" {
					text = q.Header
				}
				questions = append(questions, core.Trunc(text, 200))
			}
		}
	}
	ev := rpcEvent{event, e.info.Session, e.info.id(), e.info.Name, e.info.Cwd, e.thread, from, to, questions}
	w.sink.Emit("RPC", ev)
	if event == "done" {
		if e.info.Durable == "" {
			w.deferred[e.info.Session] = append(w.deferred[e.info.Session], deferredDone{ev, nowFn()})
		} else if w.record != nil {
			w.record(e.info.Durable, ev)
		}
	}
}

func ptr(s string) *string { return &s }

func (w *watcher) conclude(rec *record, e entry) {
	from := "working"
	if rec.status != "" {
		from = rec.status
	}
	w.emit("done", e, ptr(from), "idle")
	rec.active, rec.pollArmed, rec.doneEpoch = false, false, w.streamEpoch
}
