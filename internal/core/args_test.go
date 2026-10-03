package core

import "testing"

func TestParseArgsDefaults(t *testing.T) {
	pa := ParseArgs(nil)
	if pa.Profile != "main" {
		t.Errorf("default profile = %q, want main", pa.Profile)
	}
	if len(pa.Flags) != 0 {
		t.Errorf("flags = %v, want empty", pa.Flags)
	}
	if len(pa.Rest) != 0 {
		t.Errorf("rest = %v, want empty", pa.Rest)
	}
}

func TestParseArgsProfileForms(t *testing.T) {
	pa := ParseArgs([]string{"--profile", "family"})
	if pa.Profile != "family" {
		t.Errorf("--profile family: got %q", pa.Profile)
	}
	pa = ParseArgs([]string{"--profile=family"})
	if pa.Profile != "family" {
		t.Errorf("--profile=family: got %q", pa.Profile)
	}
	pa = ParseArgs([]string{"--profile=family", "--dry-run"})
	if pa.Profile != "family" || !pa.Flags["--dry-run"] {
		t.Errorf("--profile=family --dry-run: got %+v", pa)
	}
}

func TestParseArgsProfileConsumesNextVerbatim(t *testing.T) {
	// profile.ts parity: --profile eats the next arg even when it looks like
	// a flag, and the consumed value never lands in Flags.
	pa := ParseArgs([]string{"--profile", "--dry-run"})
	if pa.Profile != "--dry-run" {
		t.Errorf("profile = %q, want --dry-run", pa.Profile)
	}
	if pa.Flags["--dry-run"] {
		t.Errorf("consumed value must not become a flag")
	}
}

func TestParseArgsTrailingProfile(t *testing.T) {
	pa := ParseArgs([]string{"listen", "--profile"})
	if pa.Profile != "" {
		t.Errorf("trailing --profile: got %q, want empty", pa.Profile)
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
