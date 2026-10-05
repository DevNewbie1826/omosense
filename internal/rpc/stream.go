package rpc

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"time"
)

const streamBacklog = 65536

// Separate from tick dialing so tests can keep the stream unavailable without
// preventing snapshot connections.
var streamDialFn = dial

type streamFrame struct {
	Type    string          `json:"type"`
	ID      string          `json:"id"`
	Session string          `json:"sessionId"`
	Reason  string          `json:"reason"`
	Success bool            `json:"success"`
	Error   string          `json:"error"`
	Data    json.RawMessage `json:"data"`
}

type streamItem struct {
	frame  streamFrame
	lookup string
	up     bool
	err    error
	ack    chan struct{}
}

type streamFIFO struct {
	mu    sync.Mutex
	items []streamItem
	wake  chan struct{}
}

func newStreamFIFO() *streamFIFO { return &streamFIFO{wake: make(chan struct{}, 1)} }

func (q *streamFIFO) append(item streamItem) bool {
	q.mu.Lock()
	q.items = append(q.items, item)
	overflow := len(q.items) > streamBacklog
	q.mu.Unlock()
	select {
	case q.wake <- struct{}{}:
	default:
	}
	return overflow
}

func (q *streamFIFO) detach() []streamItem {
	q.mu.Lock()
	items := q.items
	q.items = nil
	q.mu.Unlock()
	return items
}

func (w *watcher) streamLoop(ctx context.Context, q *streamFIFO) {
	for ctx.Err() == nil {
		c, err := streamDialFn(ctx, w.socket)
		if err == nil {
			q.append(streamItem{up: true})
			err = readStream(c, q)
			c.close()
		}
		if ctx.Err() != nil {
			return
		}
		// Reconnect only after the owner has applied every preceding record.
		ack := make(chan struct{})
		q.append(streamItem{err: err, ack: ack})
		select {
		case <-ctx.Done():
			return
		case <-ack:
		}
		if w.sleep(ctx, interval) != nil {
			return
		}
	}
}

func observe(c *client, id string) error {
	// Only the reader writes this connection, including start lookups.
	if err := c.conn.SetWriteDeadline(time.Now().Add(callTimeout)); err != nil {
		return err
	}
	return json.NewEncoder(c.conn).Encode(struct {
		ID      string `json:"id"`
		Type    string `json:"type"`
		Observe bool   `json:"observe"`
	}{id, "list_sessions", true})
}

func readStream(c *client, q *streamFIFO) error {
	if err := observe(c, "s0"); err != nil {
		return err
	}
	var lookup uint64
	for c.scan.Scan() {
		if len(c.scan.Bytes()) > maxLine {
			return errors.New("RPC frame exceeds 8 MiB")
		}
		var frame streamFrame
		if json.Unmarshal(c.scan.Bytes(), &frame) != nil {
			continue
		}
		item := streamItem{frame: frame}
		switch frame.Type {
		case "agent_start":
			if frame.Session == "" {
				continue
			}
			lookup++
			item.lookup = fmt.Sprintf("l%d", lookup)
			if err := observe(c, item.lookup); err != nil {
				// Do not discard the start when its lookup write fails.
				q.append(item)
				return err
			}
		case "agent_settled", "session_closed", "session_parked":
			if frame.Session == "" {
				continue
			}
		case "response":
			if frame.ID != "s0" && !strings.HasPrefix(frame.ID, "l") {
				continue
			}
		default:
			continue
		}
		if q.append(item) {
			return errors.New("stream backlog overflow")
		}
	}
	if err := c.scan.Err(); err != nil {
		return err
	}
	return io.EOF
}

type streamTurn struct {
	info    sessionInfo
	lookup  string
	settled bool
}

type deferredDone struct {
	event rpcEvent
	at    time.Time
}

func (w *watcher) drainStream(ctx context.Context) {
	if w.queue == nil || w.draining {
		return
	}
	w.draining = true
	defer func() { w.draining = false }()
	for {
		items := w.queue.detach()
		if len(items) == 0 || ctx.Err() != nil {
			return
		}
		for _, item := range items {
			w.applyStream(ctx, item)
		}
	}
}

func (w *watcher) applyStream(ctx context.Context, item streamItem) {
	switch {
	case item.up:
		w.streamEpoch++
		w.streamUp = true
		w.sink.Log("rpc stream connected")
		if w.snapshotHeld {
			w.upHeld = true
		} else {
			w.poll(ctx, true)
		}
	case item.err != nil:
		w.streamUp = false
		msg := item.err.Error()
		if !w.streamErrors[msg] {
			w.streamErrors[msg] = true
			w.sink.Log("rpc stream " + msg)
		}
		// Lookups written on the ended connection are never answered.
		w.resolveLookups(ctx, func(string) bool { return true }, false)
		if item.ack != nil {
			close(item.ack)
		}
	default:
		w.streamSeq++
		f := item.frame
		switch f.Type {
		case "response":
			w.streamList(ctx, f)
		case "agent_start":
			turn := streamTurn{info: w.handles[f.Session], lookup: item.lookup}
			w.turns[f.Session] = turn
			w.armStream(turn.info)
		case "agent_settled":
			if f.Reason != "session_closed" {
				w.settleStream(ctx, f.Session)
			}
		case "session_closed", "session_parked":
			w.closeStream(f.Session)
		}
	}
}

// reconcileHeld runs the reconciliation of a connect that was drained while
// an older snapshot waited, once that snapshot has been applied.
func (w *watcher) reconcileHeld(ctx context.Context) {
	held := w.upHeld
	w.upHeld = false
	if held && ctx.Err() == nil {
		w.poll(ctx, true)
	}
}

func (w *watcher) streamEntry(info sessionInfo) (entry, bool) {
	if info.Session == "" {
		return entry{}, false
	}
	threads := w.threads([]sessionInfo{info})
	thread, watched := threads[info.id()]
	e := entry{info: info, valid: true}
	if watched {
		e.thread = ptr(thread)
	}
	return e, w.all || watched
}

func (w *watcher) armStream(info sessionInfo) {
	e, watched := w.streamEntry(info)
	if !watched {
		return
	}
	rec := w.seen[info.id()]
	rec.entry, rec.status = e, "working"
	rec.active, rec.pollArmed = true, false
	rec.activeEpoch, rec.startEpoch, rec.streamSeq = w.streamEpoch, w.streamEpoch, w.streamSeq
	w.seen[info.id()] = rec
}

func (w *watcher) streamList(ctx context.Context, f streamFrame) {
	var list struct {
		Sessions []sessionInfo `json:"sessions"`
	}
	listed := f.Success && json.Unmarshal(f.Data, &list) == nil
	if !f.Success {
		w.noteError("lookup", commandError(f.Error))
	}
	if listed {
		w.refreshHandles(list.Sessions)
	}
	w.resolveLookups(ctx, func(id string) bool { return id == f.ID }, listed)
}

// resolveLookups binds turns awaiting a matching lookup. A failed, lost or
// non-listing lookup keeps the pre-start identity; a held settle completes.
func (w *watcher) resolveLookups(ctx context.Context, match func(string) bool, listed bool) {
	for handle, turn := range w.turns {
		if turn.lookup == "" || !match(turn.lookup) {
			continue
		}
		if info, found := w.handles[handle]; listed && found {
			if old, ok := w.seen[turn.info.id()]; ok && turn.info.Session != "" && turn.info.id() != info.id() {
				old.active, old.pollArmed, old.streamSeq = false, false, w.streamSeq
				w.seen[turn.info.id()] = old
			}
			turn.info = info
		}
		turn.lookup = ""
		w.turns[handle] = turn
		w.armStream(turn.info)
		if turn.settled {
			w.settleStream(ctx, handle)
		}
	}
}

func (w *watcher) settleStream(ctx context.Context, handle string) {
	if turn, bound := w.turns[handle]; bound {
		if turn.info.Session == "" && turn.lookup != "" {
			turn.settled = true
			w.turns[handle] = turn
			return
		}
		delete(w.turns, handle)
		if turn.info.Session != "" {
			w.finishStream(handle, turn.info)
			return
		}
	}
	info, known := w.handles[handle]
	if !known {
		// Start unseen and no cached identity: one read-only fallback lookup.
		c, err := dial(ctx, w.socket)
		if err == nil {
			var data json.RawMessage
			data, err = c.call(ctx, "list_sessions", "")
			c.close()
			if err == nil {
				w.streamList(ctx, streamFrame{Success: true, Data: data})
				info, known = w.handles[handle]
			}
		}
		if err != nil {
			w.noteError("lookup", err)
		}
	}
	if !known {
		if w.errs["unknown:"+handle] == "" {
			w.errs["unknown:"+handle] = "unknown"
			w.sink.Log("rpc stream " + handle + " settled but unknown (not listed)")
		}
		return
	}
	w.finishStream(handle, info)
}

func (w *watcher) finishStream(handle string, info sessionInfo) {
	e, watched := w.streamEntry(info)
	if !watched {
		return
	}
	if id, closed := w.closed[handle]; closed && id == info.id() {
		return
	}
	rec := w.seen[info.id()]
	rec.entry, rec.streamSeq = e, w.streamSeq
	if rec.active || (rec.startEpoch != w.streamEpoch && rec.doneEpoch != w.streamEpoch) {
		w.conclude(&rec, e)
		rec.status = "idle"
	}
	w.seen[info.id()] = rec
}

func (w *watcher) closeStream(handle string) {
	w.clearDeferred(handle)
	turn := w.turns[handle]
	delete(w.turns, handle)
	info, known := w.handles[handle]
	if !known && turn.info.Session != "" {
		info, known = turn.info, true
	}
	delete(w.handles, handle)
	if !known {
		return
	}
	if id, closed := w.closed[handle]; closed && id == info.id() {
		return
	}
	w.closed[handle] = info.id()
	e, watched := w.streamEntry(info)
	if !watched {
		return
	}
	rec := w.seen[info.id()]
	var from *string
	if rec.status != "" {
		from = ptr(rec.status)
	}
	w.emit("closed", e, from, "closed")
	delete(w.seen, info.id())
}

func (w *watcher) refreshHandles(list []sessionInfo) {
	w.expireDeferred()
	handles := make(map[string]sessionInfo, len(list))
	for _, info := range list {
		handles[info.Session] = info
		if info.Durable != "" {
			// Preserve completion epochs when a formerly handle-only identity
			// becomes durable; a queued duplicate settle is still the same turn.
			// While down, the poll path stays exactly as today.
			if rec, ok := w.seen[info.Session]; ok && w.streamUp && rec.entry.info.Durable == "" {
				rec.entry.info = info
				w.seen[info.Durable] = rec
				delete(w.seen, info.Session)
			}
			for _, pending := range w.deferred[info.Session] {
				pending.event.ID = info.Durable
				if w.record != nil {
					w.record(info.Durable, pending.event)
				}
			}
			delete(w.deferred, info.Session)
		}
	}
	w.handles = handles
	for handle, id := range w.closed {
		if info, found := handles[handle]; !found || info.id() != id {
			delete(w.closed, handle)
		}
	}
}

func (w *watcher) expireDeferred() {
	for handle, events := range w.deferred {
		remaining := events[:0]
		for _, ev := range events {
			if nowFn().Sub(ev.at) >= 10*time.Minute {
				w.sink.Log("rpc done " + handle + " has no durable id; not pending")
			} else {
				remaining = append(remaining, ev)
			}
		}
		if len(remaining) == 0 {
			delete(w.deferred, handle)
		} else {
			w.deferred[handle] = remaining
		}
	}
}

func (w *watcher) clearDeferred(handle string) {
	for range w.deferred[handle] {
		w.sink.Log("rpc done " + handle + " has no durable id; not pending")
	}
	delete(w.deferred, handle)
}
