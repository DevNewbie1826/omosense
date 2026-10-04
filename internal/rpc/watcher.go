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
	entry  entry
	status string
	active bool
}

type watcher struct {
	profile, stateDir, socket string
	all, first, ready         bool
	sink                      core.Sink
	seen                      map[string]record
	listed                    map[string]bool
	errs                      map[string]string
	sleep                     func(context.Context, time.Duration) error
}

func newWatcher(c *core.Ctx, sink core.Sink) *watcher {
	return &watcher{
		profile: c.Profile.Name, stateDir: c.State, socket: socketPath(),
		all: watchAll(c), first: true, sink: sink, sleep: sleepFn,
		seen: map[string]record{}, listed: map[string]bool{}, errs: map[string]string{},
	}
}

func (w *watcher) run(ctx context.Context) error {
	mode := "threads"
	if w.all {
		mode = "all"
	}
	w.sink.Log(fmt.Sprintf("rpc watcher starting (profile %s, every 5s, watch %s, sock %s)", w.profile, mode, w.socket))
	for ctx.Err() == nil {
		w.tick(ctx)
		if ctx.Err() != nil || w.sleep(ctx, interval) != nil {
			break
		}
	}
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
	list, entries, healthy, err := w.snapshot(ctx, false)
	if ctx.Err() != nil {
		return
	}
	if err != nil {
		w.noteError("list", err)
		return
	}
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
		next.entry = e
		status := e.state.status()
		var from *string
		if prev.status != "" {
			from = ptr(prev.status)
		}
		switch {
		case status == "blocked" && prev.status != "blocked":
			w.emit("blocked", e, from, status)
		case status == "idle" && prev.active:
			w.emit("done", e, from, status)
			next.active = false
		}
		if status == "working" {
			next.active = true
		}
		next.status = status
		if !w.first && !w.listed[id] {
			w.emit("opened", e, nil, e.state.status())
		}
		seen[id] = next
	}
	for id, prev := range w.seen {
		if !present[id] {
			var from *string
			if prev.status != "" {
				from = ptr(prev.status)
			}
			w.emit("closed", prev.entry, from, "closed")
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
	w.sink.Emit("RPC", rpcEvent{event, e.info.Session, e.info.id(), e.info.Name, e.info.Cwd, e.thread, from, to, questions})
}

func ptr(s string) *string { return &s }
