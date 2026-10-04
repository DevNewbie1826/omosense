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

func watchAll(c *core.Ctx) bool {
	if c.Flags["--all"] {
		return true
	}
	if c.Cfg == nil || c.Cfg.Raw == nil {
		return false
	}
	v, _ := c.Cfg.Raw.Get("rpc")
	m, ok := v.(*core.OMap)
	if !ok {
		return false
	}
	v, _ = m.Get("all")
	return v == true
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
		if m, ok := v.(*core.OMap); ok {
			cwd, _ := m.Get("cwd")
			threadCwd, _ := cwd.(string)
			for _, field := range []string{"session_id", "session", "durable_session_id"} {
				v, _ := m.Get(field)
				if id, ok := v.(string); ok && id != "" {
					for _, s := range sessions {
						matches := id == s.Durable || (id == s.Session && threadCwd != "" && s.Cwd != nil && *s.Cwd != "" && filepath.Clean(threadCwd) == filepath.Clean(*s.Cwd))
						if matches {
							if _, exists := out[s.id()]; !exists {
								out[s.id()] = key
							}
						}
					}
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
