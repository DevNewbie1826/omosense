package herdr

import (
	"strings"
	"testing"
)

// TestBatchSortedByFirstAt guards the IS-3 entry order directly: the keys sort
// opposite to first_at, so an order that ignores first_at fails. Equal
// first_at falls back to the key.
func TestBatchSortedByFirstAt(t *testing.T) {
	s := newDoneStore(t.TempDir())
	s.entries = map[string]doneEntry{
		"a": {FirstAt: "2026-10-05T00:03:00.000Z", key: "a"},
		"b": {FirstAt: "2026-10-05T00:02:00.000Z", key: "b"},
		"c": {FirstAt: "2026-10-05T00:01:00.000Z", key: "c"},
		"d": {FirstAt: "2026-10-05T00:02:00.000Z", key: "d"},
	}
	var got []string
	for _, e := range s.sorted() {
		got = append(got, e.key)
	}
	if strings.Join(got, ",") != "c,b,d,a" {
		t.Fatalf("sorted keys = %v, want [c b d a]", got)
	}
}

// TestHelpNamesBlockedAllAndDoneBatch pins the config keys and line names
// the herdr help must point at.
func TestHelpNamesBlockedAllAndDoneBatch(t *testing.T) {
	for _, want := range []string{"herdr.blockedAll", "default false", "herdr.blockedCooldownSec", "default 0 = off", "done-batch", "5 quiet minutes"} {
		if !strings.Contains(Help, want) {
			t.Errorf("Help lacks %q:\n%s", want, Help)
		}
	}
}
