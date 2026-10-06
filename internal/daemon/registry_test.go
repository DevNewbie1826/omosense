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
// main enables discord and rpc, names telegram bot "x", and turns mail on.
// family keeps one telegram bot and disables discord. Always-on is exactly
// remind and discord (IS-13); family has no discord source, so only remind
// stays always-on. Prefixes are the IS-3 grammar prefixes each source emits.
//
// Correction (node n2-listen, IS-4b): the family fixture must name a
// telegram bot, because a profile with an empty telegram.bots list no
// longer registers a telegram source at all.
// Both profiles set tidy.enabled: tidy.Sources registers by that flag
// (IS-6, node n4-tidy), so the fixture names it explicitly.
func TestRegistryProfiles(t *testing.T) {
	home := t.TempDir()
	dir := filepath.Join(home, "omosense")
	state := filepath.Join(home, "state")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("HOME", home)
	t.Setenv("OMOSENSE_DIR", dir)
	t.Setenv("OMOSENSE_STATE", state)
	const cfg = `{
  "profiles": {
    "main": {"telegram": {"bots": ["x"]}, "discord": {"bots": ["d1"]}, "rpc": {"enabled": true}, "tidy": {"enabled": true}, "mail": true},
    "family": {"telegram": {"bots": ["y"]}, "discord": {"bots": []}, "tidy": {"enabled": true}}
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
		{"rpc", []string{"RPC"}, false, "watch-rpc-main", ""},
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

// TestRegistryRPCProfileFlag guards IS-5: the rpc source is registered by
// the profile's rpc.enabled flag, not by the profile name — a non-"main"
// profile with rpc.enabled gets the source, main without it does not.
func TestRegistryRPCProfileFlag(t *testing.T) {
	home := t.TempDir()
	dir := filepath.Join(home, "omosense")
	state := filepath.Join(home, "state")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("HOME", home)
	t.Setenv("OMOSENSE_DIR", dir)
	t.Setenv("OMOSENSE_STATE", state)
	const cfg = `{
  "profiles": {
    "main": {"telegram": {"bots": ["x"]}},
    "alpha": {"telegram": {"bots": ["a1"]}, "rpc": {"enabled": true}}
  }
}`
	if err := os.WriteFile(filepath.Join(dir, "config.json"), []byte(cfg), 0o644); err != nil {
		t.Fatal(err)
	}

	for _, tc := range []struct {
		profile string
		wantRPC bool
	}{
		{"alpha", true},
		{"main", false},
	} {
		ctx := loadProfile(t, tc.profile)
		var names []string
		for _, s := range Registry(ctx) {
			names = append(names, s.Name())
		}
		if slices.Contains(names, "rpc") != tc.wantRPC {
			t.Errorf("%s sources = %v, wantRPC = %v", tc.profile, names, tc.wantRPC)
		}
	}
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

// TestEnvironmentPathsOmosenseDir pins the daemon's directory resolution:
// start, attach and status derive every socket, lock and spawn path from
// OMOSENSE_DIR, with the same default as every subcommand.
func TestEnvironmentPathsOmosenseDir(t *testing.T) {
	home := t.TempDir()
	dir := filepath.Join(home, "run")
	t.Setenv("HOME", home)
	t.Setenv("OMOSENSE_DIR", dir)
	t.Setenv("OMOSENSE_SOCK", "")
	p, err := environmentPaths()
	if err != nil {
		t.Fatal(err)
	}
	if p.dir != dir || p.socket != filepath.Join(dir, "omosense.sock") {
		t.Fatalf("paths = %+v, want dir %s and its default socket", p, dir)
	}
}

// TestEnvironmentPathsStaleOmomeow guards against a stale OMOMEOW_DIR
// silently pointing the daemon at a different directory.
func TestEnvironmentPathsStaleOmomeow(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("OMOSENSE_DIR", "")
	t.Setenv("OMOMEOW_DIR", filepath.Join(home, "omomeow"))
	_, err := environmentPaths()
	if err == nil || err.Error() != "OMOMEOW_DIR is no longer read; set OMOSENSE_DIR" {
		t.Fatalf("err = %v, want OMOMEOW_DIR stale error", err)
	}
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
