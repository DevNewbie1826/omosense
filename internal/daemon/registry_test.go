package daemon

import (
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"testing"

	"github.com/DevNewbie1826/omosense/internal/core"
)

// TestRegistryProfiles checks the resident source list for two profiles.
// main enables discord, names telegram bot "x", and turns mail on.
// family disables discord. Always-on is exactly remind and discord (IS-13);
// family has no discord source, so only remind stays always-on.
// Prefixes are the IS-3 grammar prefixes each source emits.
func TestRegistryProfiles(t *testing.T) {
	home := t.TempDir()
	dir := filepath.Join(home, "omomeow")
	state := filepath.Join(home, "state")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("HOME", home)
	t.Setenv("OMOMEOW_DIR", dir)
	t.Setenv("OMOMEOW_STATE", state)
	const cfg = `{
  "profiles": {
    "main": {"discord": true, "telegram": ["x"], "mail": true},
    "family": {"discord": false}
  }
}`
	if err := os.WriteFile(filepath.Join(dir, "config.json"), []byte(cfg), 0o644); err != nil {
		t.Fatal(err)
	}

	mainCtx := loadProfile(t, "main")
	familyCtx := loadProfile(t, "family")

	mainWant := []sourceExpect{
		{"telegram", []string{"EVENT"}, false, "listen-main", "listen"},
		{"discord", []string{"EVENT"}, true, "listen-main", "listen"},
		{"google", []string{"CAL", "SOON", "MAIL"}, false, "watch-google-main", "watch-google"},
		{"remind", []string{"REMIND"}, true, "remind-main", "remind"},
		{"herdr", []string{"HERDR"}, false, "watch-herdr-main", ""},
		{"tidy", []string{"TIDY"}, false, "memory-tidy-main", ""},
	}
	familyWant := []sourceExpect{
		{"telegram", []string{"EVENT"}, false, "listen-family", "listen"},
		{"google", []string{"CAL", "SOON", "MAIL"}, false, "watch-google-family", "watch-google"},
		{"remind", []string{"REMIND"}, true, "remind-family", "remind"},
		{"herdr", []string{"HERDR"}, false, "watch-herdr-family", ""},
		{"tidy", []string{"TIDY"}, false, "memory-tidy-family", ""},
	}

	checkProfileSources(t, Registry(mainCtx), mainWant, []string{"discord", "remind"})
	checkProfileSources(t, Registry(familyCtx), familyWant, []string{"remind"})
}

type sourceExpect struct {
	name     string
	prefixes []string
	alwaysOn bool
	lock     string
	legacy   string
}

func loadProfile(t *testing.T, name string) *core.Ctx {
	t.Helper()
	ctx, err := core.Load(core.ParseArgs([]string{"--profile", name}), false)
	if err != nil {
		t.Fatalf("load %s: %v", name, err)
	}
	return ctx
}

func checkProfileSources(t *testing.T, got []core.Source, want []sourceExpect, alwaysOn []string) {
	t.Helper()
	if len(got) != len(want) {
		names := make([]string, len(got))
		for i, s := range got {
			names[i] = s.Name()
		}
		t.Fatalf("sources = %v, want %d entries", names, len(want))
	}
	var on []string
	for i, s := range got {
		w := want[i]
		if s.Name() != w.name {
			t.Errorf("source[%d] name = %q, want %q", i, s.Name(), w.name)
		}
		if !reflect.DeepEqual(s.Prefixes(), w.prefixes) {
			t.Errorf("%s prefixes = %v, want %v", w.name, s.Prefixes(), w.prefixes)
		}
		if s.AlwaysOn() != w.alwaysOn {
			t.Errorf("%s AlwaysOn = %v, want %v", w.name, s.AlwaysOn(), w.alwaysOn)
		}
		if s.AlwaysOn() {
			on = append(on, s.Name())
		}
		lock, legacy := s.LockName()
		if lock != w.lock || legacy != w.legacy {
			t.Errorf("%s lock = %q/%q, want %q/%q", w.name, lock, legacy, w.lock, w.legacy)
		}
	}
	slices.Sort(on)
	if !reflect.DeepEqual(on, alwaysOn) {
		t.Errorf("AlwaysOn = %v, want %v", on, alwaysOn)
	}
}
