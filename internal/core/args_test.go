package core

import "testing"

func TestParseArgsDefaults(t *testing.T) {
	pa := ParseArgs(nil)
	if pa.ProfileSet {
		t.Error("--profile must not be reported for empty argv")
	}
	if len(pa.Flags) != 0 {
		t.Errorf("flags = %v, want empty", pa.Flags)
	}
	if len(pa.Rest) != 0 {
		t.Errorf("rest = %v, want empty", pa.Rest)
	}
}

// TestParseArgsProfileForms pins the removed-flag detection: both spellings
// set ProfileSet and neither leaks a value into Flags or Rest, so main can
// fail loudly on a stale caller instead of running another folder's config.
func TestParseArgsProfileForms(t *testing.T) {
	pa := ParseArgs([]string{"--profile", "family"})
	if !pa.ProfileSet || len(pa.Flags) != 0 || len(pa.Rest) != 0 {
		t.Errorf("--profile family: got %+v", pa)
	}
	pa = ParseArgs([]string{"--profile=family"})
	if !pa.ProfileSet {
		t.Errorf("--profile=family: got %+v", pa)
	}
	pa = ParseArgs([]string{"--profile=family", "--dry-run"})
	if !pa.ProfileSet || !pa.Flags["--dry-run"] {
		t.Errorf("--profile=family --dry-run: got %+v", pa)
	}
}

func TestParseArgsProfileConsumesNextVerbatim(t *testing.T) {
	// --profile eats the next arg even when it looks like a flag, and the
	// consumed value never lands in Flags.
	pa := ParseArgs([]string{"--profile", "--dry-run"})
	if !pa.ProfileSet {
		t.Error("--profile not reported")
	}
	if pa.Flags["--dry-run"] {
		t.Errorf("consumed value must not become a flag")
	}
}

func TestParseArgsTrailingProfile(t *testing.T) {
	pa := ParseArgs([]string{"listen", "--profile"})
	if !pa.ProfileSet {
		t.Error("trailing --profile not reported")
	}
	if len(pa.Rest) != 1 || pa.Rest[0] != "listen" {
		t.Errorf("rest = %v, want [listen]", pa.Rest)
	}
}

func TestParseArgsFlagsAndRest(t *testing.T) {
	pa := ParseArgs([]string{"a", "--dry-run", "b", "--once", "--x=1"})
	if len(pa.Rest) != 2 || pa.Rest[0] != "a" || pa.Rest[1] != "b" {
		t.Errorf("rest = %v, want [a b]", pa.Rest)
	}
	for _, f := range []string{"--dry-run", "--once", "--x=1"} {
		if !pa.Flags[f] {
			t.Errorf("flag %q not set", f)
		}
	}
	if len(pa.Flags) != 3 {
		t.Errorf("flags = %v, want 3 entries", pa.Flags)
	}
}
