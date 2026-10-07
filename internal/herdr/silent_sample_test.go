package herdr

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/DevNewbie1826/omosense/internal/core"
)

func TestSilentSessionGrowthAfterDecreaseAtReportBoundary(t *testing.T) {
	start := time.Date(2026, 10, 7, 0, 0, 0, 0, time.UTC)
	advance := fakeClock(t, start)
	var out bytes.Buffer
	w := &watcher{
		sink:          core.NewOut(&out),
		silent:        map[string]silentState{},
		silentMinutes: 1,
	}
	pane := jobPane{pane: "p", thread: "job"}
	frozen := jobPane{pane: "q", thread: "control"}
	sample := func(key string, rev json.Number, job jobPane) {
		e := snapEntry{
			key:     key,
			machine: "local",
			agent:   agent{status: strPtr("working"), rev: rev},
		}
		w.silent[key] = w.trackSilent(e, job)
	}

	// Given: both panes are working, with a one-minute silence window.
	sample("p", "100", pane)
	sample("q", "3", frozen)

	// When: the revision drops on the reporting tick, then grows from that
	// immediately preceding sample. The drop itself must not re-arm.
	advance(time.Minute)
	sample("p", "5", pane)
	sample("q", "3", frozen)
	if got := strings.Count(out.String(), `"thread":"job"`); got != 1 {
		t.Fatalf("revision decrease re-armed silence: job reports = %d, want 1", got)
	}
	advance(5 * time.Second)
	sample("p", "6", pane)
	sample("q", "3", frozen)
	advance(time.Minute)
	sample("p", "6", pane)
	sample("q", "3", frozen)

	// Then: the growing pane starts a new window; the frozen control does not.
	if got := strings.Count(out.String(), `"thread":"job"`); got != 2 {
		t.Fatalf("growth after the reporting-tick decrease did not re-arm: job reports = %d, want 2; output=%s", got, out.String())
	}
	if got := strings.Count(out.String(), `"thread":"control"`); got != 1 {
		t.Fatalf("frozen control reports = %d, want 1", got)
	}
	if !w.silent["p"].last.Equal(start.Add(65 * time.Second)) {
		t.Fatalf("new silence window starts at %s, want growth sample", w.silent["p"].last)
	}
}
