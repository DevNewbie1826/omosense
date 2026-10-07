package core_test

import (
	"reflect"
	"testing"

	"github.com/DevNewbie1826/omosense/internal/core"
	"github.com/DevNewbie1826/omosense/internal/google"
	"github.com/DevNewbie1826/omosense/internal/herdr"
	"github.com/DevNewbie1826/omosense/internal/host"
	"github.com/DevNewbie1826/omosense/internal/listen"
	"github.com/DevNewbie1826/omosense/internal/remind"
	"github.com/DevNewbie1826/omosense/internal/say"
	"github.com/DevNewbie1826/omosense/internal/tidy"
)

// The compat subcommands and the session host code against these exact
// signatures.
var _ = []func(*core.Ctx, []string) int{listen.Run, google.Run, remind.Run, herdr.Run, tidy.Run, say.Run}
var _ = []func(*core.Ctx) []core.Source{listen.Sources, google.Sources, remind.Sources, herdr.Sources, tidy.Sources, say.Sources}
var _ = []func(*core.Ctx) int{host.Run}
var _ = []func(*core.Ctx) []core.Source{host.Sources}
var _ = []string{listen.Help, google.Help, remind.Help, herdr.Help, tidy.Help, say.Help}

func TestSubHelpPrintedBeforeConfigLoad(t *testing.T) {
	// Build before cliEnv points HOME at a temp dir: go build under a fresh
	// HOME would fill that dir with a read-only module cache.
	buildBinaries(t)
	// Deliberately no config.json: --help must not read it.
	cliEnv(t)

	helps := map[string]string{
		"listen": listen.Help,
		"google": google.Help,
		"remind": remind.Help,
		"herdr":  herdr.Help,
		"tidy":   tidy.Help,
		"say":    say.Help,
	}
	for sub, help := range helps {
		stdout, stderr, code := runBin(t, sub, "--help")
		if code != 0 || stdout != help || stderr != "" {
			t.Errorf("%s --help: code=%d stderr=%q exact-help=%v", sub, code, stderr, stdout == help)
		}
	}
	stdout, _, code := runBin(t, "listen", "-h")
	if code != 0 || stdout != listen.Help {
		t.Errorf("listen -h: code=%d exact-help=%v", code, stdout == listen.Help)
	}
	// The usage texts no longer advertise the removed --profile flag.
	for _, help := range helps {
		if got := help; len(got) > 0 && contains(got, "--profile") {
			t.Errorf("help still mentions --profile: %q", got)
		}
	}
}

func contains(s, sub string) bool {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}

// TestCLIHelpDoesNotRescueRemovedProfile pins the stale-caller rule: --help
// prints help, but a --profile anywhere is still an error.
func TestCLIHelpDoesNotRescueRemovedProfile(t *testing.T) {
	cliEnv(t)
	stdout, stderr, code := runBin(t, "listen", "--help", "--profile", "nope")
	if code != 2 {
		t.Errorf("exit = %d, want 2 (the removed flag fails loudly)", code)
	}
	if stdout != "" || !contains(stderr, "--profile was removed") {
		t.Errorf("stdout = %q stderr = %q, want the removal message", stdout, stderr)
	}
}

func TestSourceMetadata(t *testing.T) {
	_, dir, _ := cliEnv(t)
	writeCliConfig(t, dir, cliFlatCfg)
	ctx, err := core.Load(core.ParseArgs(nil), false)
	if err != nil {
		t.Fatalf("load: %v", err)
	}

	ls := listen.Sources(ctx)
	if len(ls) != 2 {
		t.Fatalf("listen.Sources = %d sources, want 2", len(ls))
	}
	checkSource(t, ls[0], "telegram", []string{"EVENT"}, false, "listen", "")
	checkSource(t, ls[1], "discord", []string{"EVENT"}, true, "listen", "")

	gs := google.Sources(ctx)
	if len(gs) != 1 {
		t.Fatalf("google.Sources = %d sources, want 1", len(gs))
	}
	checkSource(t, gs[0], "google", []string{"CAL", "SOON", "MAIL"}, false, "watch-google", "")

	rs := remind.Sources(ctx)
	if len(rs) != 1 {
		t.Fatalf("remind.Sources = %d sources, want 1", len(rs))
	}
	checkSource(t, rs[0], "remind", []string{"REMIND"}, true, "remind", "")

	hs := herdr.Sources(ctx)
	if len(hs) != 1 {
		t.Fatalf("herdr.Sources = %d sources, want 1", len(hs))
	}
	checkSource(t, hs[0], "herdr", []string{"HERDR"}, false, "watch-herdr", "")

	ts := tidy.Sources(ctx)
	if len(ts) != 1 {
		t.Fatalf("tidy.Sources = %d sources, want 1", len(ts))
	}
	checkSource(t, ts[0], "tidy", []string{"TIDY"}, false, "memory-tidy", "")

	if got := say.Sources(ctx); len(got) != 0 {
		t.Errorf("say.Sources = %v, want none (say is not a hosted source)", got)
	}
}

func checkSource(t *testing.T, s core.Source, name string, prefixes []string, alwaysOn bool, lock, legacy string) {
	t.Helper()
	if s.Name() != name {
		t.Errorf("name = %q, want %q", s.Name(), name)
	}
	if !reflect.DeepEqual(s.Prefixes(), prefixes) {
		t.Errorf("%s prefixes = %v, want %v", name, s.Prefixes(), prefixes)
	}
	if s.AlwaysOn() != alwaysOn {
		t.Errorf("%s alwaysOn = %v, want %v", name, s.AlwaysOn(), alwaysOn)
	}
	l, lg := s.LockName()
	if l != lock || lg != legacy {
		t.Errorf("%s lock = %q/%q, want %q/%q", name, l, lg, lock, legacy)
	}
}
