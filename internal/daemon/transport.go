package daemon

import (
	"encoding/json"
	"fmt"
	"net"
	"os"
	"slices"
	"time"
)

type sourceStatus struct {
	Profile string `json:"profile"`
	Name    string `json:"name"`
	State   string `json:"state"`
}

type clientStatus struct {
	Profile string   `json:"profile"`
	Name    string   `json:"name"`
	Sources []string `json:"sources"`
}

type status struct {
	PID      int            `json:"pid"`
	Version  string         `json:"version"`
	Features []string       `json:"features"`
	Sources  []sourceStatus `json:"sources"`
	Clients  []clientStatus `json:"clients"`
}

func (s *server) handle(conn net.Conn) {
	defer func() {
		conn.Close()
		s.mu.Lock()
		delete(s.conns, conn)
		s.mu.Unlock()
	}()
	conn.SetReadDeadline(time.Now().Add(10 * time.Second))
	reader := newFramer(conn)
	var raw json.RawMessage
	if err := reader.read(&raw); err != nil {
		return
	}
	var cmd command
	if err := json.Unmarshal(raw, &cmd); err != nil {
		return
	}
	if cmd.Cmd != "" {
		s.control(conn, cmd)
		return
	}
	var h hello
	if err := json.Unmarshal(raw, &h); err != nil {
		writeFrame(conn, reply{Error: "invalid hello"})
		return
	}
	s.mu.Lock()
	if s.stopping {
		s.mu.Unlock()
		return
	}
	p := s.profiles[h.Profile]
	rejection := ""
	if p == nil {
		rejection = "unknown profile " + h.Profile
	} else if h.Hello != 1 || h.Version == "" {
		rejection = "invalid hello"
	} else if p.stopping {
		rejection = "profile " + h.Profile + " is stopping"
	} else {
		for _, name := range h.Sources {
			if !slices.Contains(selections["all"], name) {
				rejection = "invalid source " + name
				break
			}
		}
	}
	if rejection == "" && p.stopped {
		if err := os.Remove(profileMarker(p.ctx)); err != nil && !os.IsNotExist(err) {
			rejection = "remove profile stop marker: " + err.Error()
		} else {
			p.stopped = false
			for _, w := range p.workers {
				w.resume()
			}
		}
	}
	if rejection != "" {
		s.mu.Unlock()
		conn.SetWriteDeadline(time.Now().Add(5 * time.Second))
		writeFrame(conn, reply{Error: rejection})
		return
	}
	c := newClient(conn)
	c.enqueue(reply{OK: true, Version: s.options.version})
	for _, e := range p.journal.pending(h) {
		c.enqueue(frame{Line: e.Line})
		if err := p.journal.deliver(e.Seq); err != nil {
			s.failLocked(err)
			s.mu.Unlock()
			c.close()
			return
		}
	}
	s.clients[c] = h
	s.interestLocked(h.Profile)
	// Test-only fake sources emit on exact attach events instead of a polling
	// timer. No real source implements this private hook.
	for _, name := range h.Sources {
		if w := p.workers[name]; w != nil {
			if fake, ok := w.source.(*fakeSource); ok {
				fake.attached()
			}
		}
	}
	s.mu.Unlock()
	conn.SetReadDeadline(time.Time{})
	written := make(chan struct{})
	go func() { c.write(); close(written) }()
	defer func() {
		c.close()
		<-written
		s.mu.Lock()
		delete(s.clients, c)
		s.interestLocked(h.Profile)
		s.mu.Unlock()
	}()
	for {
		var cmd command
		if err := reader.read(&cmd); err != nil {
			return
		}
		if cmd.Cmd == "stop" {
			s.requestStop(stopReason(cmd))
		}
	}
}

func (s *server) interestLocked(profile string) {
	for name, w := range s.profiles[profile].workers {
		wanted := false
		for c, h := range s.clients {
			if h.Profile == profile && slices.Contains(h.Sources, name) && !c.closed() {
				wanted = true
				break
			}
		}
		w.setWanted(wanted)
	}
}

func stopReason(c command) string {
	if c.Reason == "upgrade" {
		return "upgrade"
	}
	return "stop"
}

func (s *server) control(conn net.Conn, c command) {
	switch c.Cmd {
	case "status":
		conn.SetWriteDeadline(time.Now().Add(5 * time.Second))
		s.mu.Lock()
		st := status{PID: os.Getpid(), Version: s.options.version,
			Features: []string{"profile-stop"}, Sources: []sourceStatus{}, Clients: []clientStatus{}}
		for profile, p := range s.profiles {
			for name, w := range p.workers {
				state := w.state()
				if p.stopped {
					state = "stopped"
				}
				st.Sources = append(st.Sources, sourceStatus{profile, name, state})
			}
		}
		for _, h := range s.clients {
			st.Clients = append(st.Clients, clientStatus{h.Profile, h.Name, h.Sources})
		}
		s.mu.Unlock()
		writeFrame(conn, st)
	case "stop":
		if c.Profile != "" {
			// Joining workers can take longer than the control write budget.
			result, err := s.stopProfile(c.Profile)
			conn.SetWriteDeadline(time.Now().Add(5 * time.Second))
			r := profileStopReply{reply: reply{OK: err == nil}, profileStopResult: result}
			if err != nil {
				r.Error = err.Error()
			}
			writeFrame(conn, r)
			return
		}
		conn.SetWriteDeadline(time.Now().Add(5 * time.Second))
		writeFrame(conn, reply{OK: true, Version: s.options.version})
		s.requestStop(stopReason(c))
	default:
		conn.SetWriteDeadline(time.Now().Add(5 * time.Second))
		writeFrame(conn, reply{Error: fmt.Sprintf("unknown command %s", c.Cmd)})
	}
}
