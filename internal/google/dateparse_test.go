package google

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/DevNewbie1826/omosense/internal/core"
)

func mustSeoul(t *testing.T) *time.Location {
	t.Helper()
	loc, err := time.LoadLocation("Asia/Seoul")
	if err != nil {
		t.Fatalf("load Asia/Seoul: %v", err)
	}
	old := time.Local
	time.Local = loc
	t.Cleanup(func() { time.Local = old })
	return loc
}

func goldenFloat(t *testing.T, v any) float64 {
	t.Helper()
	switch x := v.(type) {
	case float64:
		return x
	case int64:
		return float64(x)
	case int:
		return float64(x)
	case json.Number:
		f, err := x.Float64()
		if err != nil {
			t.Fatalf("golden value %q is not a number: %v", x.String(), err)
		}
		return f
	}
	t.Fatalf("golden value %#v is not a number", v)
	return 0
}

// TestSeenKeysGolden pins startMs to the committed TZ=Asia/Seoul Bun
// Date.parse golden: seen keys must stay byte-identical to the bun
// watcher's for the same zele output (IS-7).
func TestSeenKeysGolden(t *testing.T) {
	mustSeoul(t)
	gv, err := core.ParseJSON(fixture(t, "seen-keys.golden.json"))
	if err != nil {
		t.Fatalf("parse golden: %v", err)
	}
	g := gv.(*core.OMap)
	if n := g.Len(); n < 20 {
		t.Fatalf("golden has %d inputs, want >= 20", n)
	}
	for _, in := range g.Keys() {
		want, _ := g.Get(in)
		ms, ok := startMs(in)
		if s, isStr := want.(string); isStr {
			if s != "NaN" {
				t.Fatalf("golden[%q] = %q, expected only numbers or NaN", in, s)
			}
			if ok {
				t.Errorf("startMs(%q) = %v, want NaN", in, ms)
			}
			continue
		}
		wf := goldenFloat(t, want)
		if !ok {
			t.Errorf("startMs(%q) = NaN, want %v", in, wf)
			continue
		}
		if ms != wf {
			t.Errorf("startMs(%q) = %v, want %v", in, ms, wf)
		}
	}
}

// TestRealStartYearInference pins the SOON year correction: yearless timed
// zele starts pick the candidate year closest to now; date-only and
// all-day starts get no real start (no SOON).
func TestRealStartYearInference(t *testing.T) {
	loc := mustSeoul(t)
	now := time.Date(2026, 10, 3, 19, 0, 0, 0, loc)
	rs, ok := realStart("Oct 6, 9:30 AM", now)
	if !ok || !rs.Equal(time.Date(2026, 10, 6, 9, 30, 0, 0, loc)) {
		t.Errorf("realStart(Oct 6, 9:30 AM) = %v %v, want 2026-10-06 09:30 KST", rs, ok)
	}
	rs, ok = realStart("Jan 2, 9:00 AM", time.Date(2026, 12, 31, 12, 0, 0, 0, loc))
	if !ok || !rs.Equal(time.Date(2027, 1, 2, 9, 0, 0, 0, loc)) {
		t.Errorf("realStart(Jan 2) around year end = %v %v, want 2027-01-02", rs, ok)
	}
	rs, ok = realStart("Dec 30, 9:00 AM", time.Date(2026, 1, 1, 12, 0, 0, 0, loc))
	if !ok || !rs.Equal(time.Date(2025, 12, 30, 9, 0, 0, 0, loc)) {
		t.Errorf("realStart(Dec 30) around year start = %v %v, want 2025-12-30", rs, ok)
	}
	if _, ok := realStart("Oct 5", now); ok {
		t.Errorf("realStart(Oct 5) = ok, want no real start (all-day)")
	}
	if _, ok := realStart("2026-10-05", now); ok {
		t.Errorf("realStart(2026-10-05) = ok, want no real start (date-only)")
	}
	rs, ok = realStart("2026-10-05T15:04:05Z", now)
	if !ok || !rs.Equal(time.Date(2026, 10, 5, 15, 4, 5, 0, time.UTC)) {
		t.Errorf("realStart(RFC3339) = %v %v, want the parsed instant", rs, ok)
	}
}
