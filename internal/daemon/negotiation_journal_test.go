package daemon

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/DevNewbie1826/omosense/internal/core"
)

func TestJournalNegotiationHelloDoesNotSubscribeOrConsume(t *testing.T) {
	for _, version := range []string{"test-v1", "test-v2"} {
		t.Run(version, func(t *testing.T) {
			// Given pending always-on messages and an idle pausable source.
			s := serverFixture(t, func(c *core.Ctx) []core.Source {
				return []core.Source{&testSource{name: "herdr", lock: "herdr-" + c.Profile.Name,
					run: func(ctx context.Context, _ core.Sink) error {
						<-ctx.Done()
						return ctx.Err()
					}}}
			})
			s.emit("main", "remind", true, "REMIND sent pending")
			s.emit("main", "discord", true, "EVENT pending")
			h := hello{Hello: 1, Version: version, Profile: "main",
				Sources: []string{"remind", "discord", "herdr"}}

			// When the client only negotiates, without accepting a version.
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			conn, _, gotVersion, err := handshake(ctx, s.paths, h)
			if err != nil {
				t.Fatal(err)
			}
			defer conn.Close()
			if gotVersion != "test-v1" {
				t.Fatalf("advertised version=%q", gotVersion)
			}

			// Then neither subscription interest nor delivery is committed.
			st := requestStatus(t, s)
			if len(st.Clients) != 0 {
				t.Errorf("hello-only probe registered clients: %+v", st.Clients)
			}
			for _, src := range st.Sources {
				if src.State != "paused" {
					t.Errorf("hello-only probe started source: %+v", src)
				}
			}
			s.mu.Lock()
			pending := s.profiles["main"].journal.pending(h)
			s.mu.Unlock()
			if len(pending) != 2 {
				t.Errorf("hello-only probe consumed journal: pending=%+v, want two entries", pending)
			}
		})
	}
}

func testUpgradeJournalReplay(t *testing.T, bin string) {
	// Given a real old-version daemon with both always-on prefixes pending.
	f := subprocessFixture(t, bin)
	peer := f.attach("v1", "main")
	old := f.streamPID(peer, 0)
	f.waitEvent("source:main:remind", old)
	f.waitEvent("source:main:discord", old)
	var state string
	for _, value := range f.env {
		if strings.HasPrefix(value, "OMOMEOW_STATE=") {
			state = strings.TrimPrefix(value, "OMOMEOW_STATE=")
		}
	}
	data, err := os.ReadFile(filepath.Join(state, "omosense-journal-main.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	var expected []string
	reader := newFramer(bytes.NewReader(data))
	for {
		var e journalEntry
		if err := reader.read(&e); err != nil {
			if err != io.EOF {
				t.Fatal(err)
			}
			break
		}
		if !e.Delivered && (strings.HasPrefix(e.Line, "REMIND ") || strings.HasPrefix(e.Line, "EVENT ")) {
			expected = append(expected, e.Line)
		}
	}
	if len(expected) != 2 {
		t.Fatalf("pending fixture=%q, want REMIND and EVENT", expected)
	}
	t.Logf("PENDING seq order: %q", expected)

	// When a new-version real attach upgrades and subscribes to both prefixes.
	a := f.attach("v2", "main", "all", "--only", "REMIND,EVENT")
	var output []string
	next := map[string]int{}
	for len(next) < 2 {
		var line string
		select {
		case value, ok := <-a.lines:
			if !ok {
				t.Fatal("attach exited before new live output")
			}
			line = value
		case <-time.After(15 * time.Second):
			t.Fatal("bounded upgrade-output await timed out")
		}
		t.Log("STDOUT " + line)
		output = append(output, line)
		prefix, raw, _ := strings.Cut(line, " ")
		var payload struct {
			PID int `json:"pid"`
		}
		if err := json.Unmarshal([]byte(raw), &payload); err != nil {
			t.Fatal(err)
		}
		if payload.PID != old {
			next[prefix] = payload.PID
		}
	}
	f.stop(peer, a)
	for line := range a.lines {
		t.Log("STDOUT " + line)
		output = append(output, line)
	}

	// Then every old entry appears once, in journal order, before new lines.
	if len(output) < len(expected) {
		t.Fatalf("lost replay: stdout=%q pending=%q", output, expected)
	}
	for i, want := range expected {
		count := 0
		for _, line := range output {
			if line == want {
				count++
			}
		}
		if output[i] != want || count != 1 {
			t.Errorf("upgrade lost/duplicated/reordered pending entry: want stdout[%d]=%q exactly once, got count=%d stdout=%q",
				i, want, count, output)
		}
	}
}
