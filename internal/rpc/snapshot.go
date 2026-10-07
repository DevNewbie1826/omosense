package rpc

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strconv"

	"github.com/DevNewbie1826/omosense/internal/core"
)

type sessionInfo struct {
	Session string  `json:"sessionId"`
	Durable string  `json:"durableSessionId"`
	Name    *string `json:"name"`
	Cwd     *string `json:"cwd"`
}

func (s sessionInfo) id() string {
	if s.Durable != "" {
		return s.Durable
	}
	return s.Session
}

type sessionState struct {
	Pending []struct {
		Questions []struct {
			Question string `json:"question"`
			Header   string `json:"header"`
		} `json:"questions"`
	} `json:"pendingQuestions"`
	Streaming  bool `json:"isStreaming"`
	Compacting bool `json:"isCompacting"`
	Count      int  `json:"messageCount"`
}

func (s sessionState) status() string {
	if len(s.Pending) != 0 {
		return "blocked"
	}
	if s.Streaming || s.Compacting {
		return "working"
	}
	return "idle"
}

type entry struct {
	info   sessionInfo
	thread *string
	state  sessionState
	valid  bool
}

func socketPath() string {
	if p := os.Getenv("OMOSENSE_RPC_SOCK"); p != "" {
		return p
	}
	dir := os.Getenv("OMO_CODING_AGENT_DIR")
	if dir == "" {
		home, _ := os.UserHomeDir()
		dir = filepath.Join(home, ".omo", "agent")
	}
	return filepath.Join(dir, "rpc", "rpc.sock")
}

// watchAll reports whole-daemon mode: the --all flag or the profile's
// rpc.all setting watches every session, not only threads.json handles.
func watchAll(c *core.Ctx) bool {
	return c.Flags["--all"] || c.Profile.RPC.All
}

// threadSessionMatch reports whether a threads.json entry names session s by
// the rule the rpc source has always used: an equal durable id, or an equal
// session handle with an equal cleaned, non-empty cwd on both sides.
func threadSessionMatch(m *core.OMap, s sessionInfo) bool {
	if m == nil {
		return false
	}
	cwd, _ := m.Get("cwd")
	threadCwd, _ := cwd.(string)
	for _, field := range []string{"session_id", "session", "durable_session_id"} {
		v, _ := m.Get(field)
		id, ok := v.(string)
		if !ok || id == "" {
			continue
		}
		if id == s.Durable ||
			(id == s.Session && threadCwd != "" && s.Cwd != nil && *s.Cwd != "" && filepath.Clean(threadCwd) == filepath.Clean(*s.Cwd)) {
			return true
		}
	}
	return false
}

// OwnedThreads returns the thread keys the rpc source owns: the entries whose
// session field matches a session currently listed on the rpc socket, by the
// rule threads() applies. The herdr source uses it to fall back to a pane only
// when no live rpc session matches (IS-12). It dials socketPath() and calls
// list_sessions once, and reads no watcher state.
func OwnedThreads(ctx context.Context, entries map[string]*core.OMap) (map[string]bool, error) {
	out := map[string]bool{}
	if len(entries) == 0 {
		return out, nil
	}
	c, err := dial(ctx, socketPath())
	if err != nil {
		return nil, err
	}
	defer c.close()
	data, err := c.call(ctx, "list_sessions", "")
	if err != nil {
		return nil, err
	}
	var list struct {
		Sessions []sessionInfo `json:"sessions"`
	}
	if err := json.Unmarshal(data, &list); err != nil {
		return nil, err
	}
	for key, m := range entries {
		for _, s := range list.Sessions {
			if threadSessionMatch(m, s) {
				out[key] = true
				break
			}
		}
	}
	return out, nil
}

// threads preserves object order and uses array indices as thread keys.
func (w *watcher) threads(sessions []sessionInfo) map[string]string {
	out := map[string]string{}
	b, err := os.ReadFile(filepath.Join(w.stateDir, "threads.json"))
	if err != nil {
		return out
	}
	v, err := core.ParseJSON(b)
	if err != nil {
		return out
	}
	add := func(key string, v any) {
		m, ok := v.(*core.OMap)
		if !ok || !core.ThreadActive(m) {
			return
		}
		for _, s := range sessions {
			if threadSessionMatch(m, s) {
				if _, exists := out[s.id()]; !exists {
					out[s.id()] = key
				}
			}
		}
	}
	switch m := v.(type) {
	case *core.OMap:
		for _, key := range m.Keys() {
			v, _ := m.Get(key)
			add(key, v)
		}
	case []any:
		for i, v := range m {
			add(strconv.Itoa(i), v)
		}
	}
	return out
}

// snapshot never writes state and returns no list on whole-tick failure.
func (w *watcher) snapshot(ctx context.Context, once bool) ([]sessionInfo, []entry, bool, error) {
	c, err := dial(ctx, w.socket)
	if err != nil {
		return nil, nil, false, err
	}
	defer c.close()
	data, err := c.call(ctx, "list_sessions", "")
	if err != nil {
		return nil, nil, false, err
	}
	var list struct {
		Sessions []sessionInfo `json:"sessions"`
	}
	if err := json.Unmarshal(data, &list); err != nil {
		return nil, nil, false, err
	}
	threads := w.threads(list.Sessions)
	healthy := true
	var entries []entry
	for _, s := range list.Sessions {
		thread, watched := threads[s.id()]
		if !w.all && !watched {
			continue
		}
		e := entry{info: s}
		if watched {
			e.thread = ptr(thread)
		}
		data, err := c.call(ctx, "get_state", s.Session)
		var commandErr commandError
		if err != nil && !errors.As(err, &commandErr) {
			return nil, nil, false, err
		}
		if err == nil {
			err = json.Unmarshal(data, &e.state)
		}
		if err != nil {
			if !errors.Is(err, commandError("unknown_session")) {
				if once {
					return nil, nil, false, err
				}
				healthy = false
				w.noteError(s.id(), err)
			}
		} else {
			delete(w.errs, s.id())
			e.valid = true
		}
		entries = append(entries, e)
	}
	if err := ctx.Err(); err != nil {
		return nil, nil, false, err
	}
	delete(w.errs, "list")
	return list.Sessions, entries, healthy, nil
}
