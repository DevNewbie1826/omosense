package core

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func testEnv(t *testing.T) (home, dir, state string) {
	t.Helper()
	home = t.TempDir()
	dir = filepath.Join(home, ".omosense")
	state = filepath.Join(dir, "state")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("HOME", home)
	t.Setenv("OMOSENSE_DIR", dir)
	t.Setenv("OMOSENSE_STATE", state)
	return home, dir, state
}

func writeConfig(t *testing.T, dir, cfg string) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(dir, "config.json"), []byte(cfg), 0o644); err != nil {
		t.Fatal(err)
	}
}

const mainCfg = `{"profiles":{"main":{"telegram":{"bots":["mb"],"roles":{}},"discord":{"bots":["db"]}}}}`

const profilesCfg = `{
  "profiles": {
    "main": {
      "telegram": {"bots": ["mb"], "roles": {"12": "owner", "13": "wife"}},
      "discord": {"bots": ["db"], "roles": {"44": "owner"}},
      "rpc": {"enabled": true, "session": null, "all": false},
      "tidy": {"enabled": true, "learnOthers": true, "exclude": ["repo1"]},
      "memory": "mem-main",
      "mail": true
    },
    "family": {
      "telegram": {"bots": ["fb"], "roles": {}},
      "discord": {"bots": [], "roles": {}},
      "rpc": {},
      "tidy": {"exclude": ["x"]},
      "calendars": ["cal1", "cal2"],
      "mail": false
    },
    "nocal": {
      "telegram": {"bots": []},
      "discord": {"bots": ["db2"]},
      "calendars": null,
      "mail": true
    }
  }
}`

func TestLoadNamedProfiles(t *testing.T) {
	_, dir, _ := testEnv(t)
	writeConfig(t, dir, profilesCfg)

	ctx, err := Load(ParseArgs([]string{"--profile", "main"}), false)
	if err != nil {
		t.Fatalf("load main: %v", err)
	}
	want := Profile{
		Name:     "main",
		Telegram: PlatformCfg{Bots: []string{"mb"}, Roles: map[string]string{"12": "owner", "13": "wife"}},
		Discord:  PlatformCfg{Bots: []string{"db"}, Roles: map[string]string{"44": "owner"}},
		RPC:      RPCCfg{Enabled: true},
		Tidy:     TidyCfg{Enabled: true, LearnOthers: true, Exclude: []string{"repo1"}},
		Memory:   "mem-main",
		Mail:     true,
	}
	if !reflect.DeepEqual(ctx.Profile, want) {
		t.Errorf("main profile = %+v, want %+v", ctx.Profile, want)
	}

	ctx, err = Load(ParseArgs([]string{"--profile=family"}), false)
	if err != nil {
		t.Fatalf("load family: %v", err)
	}
	if len(ctx.Profile.Telegram.Bots) != 1 || ctx.Profile.Telegram.Bots[0] != "fb" ||
		len(ctx.Profile.Discord.Bots) != 0 || ctx.Profile.RPC.Enabled || ctx.Profile.Mail {
		t.Errorf("family profile = %+v", ctx.Profile)
	}
	if ctx.Profile.Tidy.Enabled || ctx.Profile.Tidy.LearnOthers || !reflect.DeepEqual(ctx.Profile.Tidy.Exclude, []string{"x"}) {
		t.Errorf("family tidy = %+v", ctx.Profile.Tidy)
	}
	if ctx.Profile.Calendars == nil || len(*ctx.Profile.Calendars) != 2 || (*ctx.Profile.Calendars)[0] != "cal1" {
		t.Errorf("family calendars = %v", ctx.Profile.Calendars)
	}

	ctx, err = Load(ParseArgs([]string{"--profile", "nocal"}), false)
	if err != nil {
		t.Fatalf("load nocal: %v", err)
	}
	// An explicit calendars:null means all calendars, same as an absent key.
	if ctx.Profile.Calendars != nil {
		t.Errorf("calendars:null must resolve to nil (all), got %v", *ctx.Profile.Calendars)
	}
	if len(ctx.Profile.Telegram.Bots) != 0 || len(ctx.Profile.Discord.Bots) != 1 || !ctx.Profile.Mail {
		t.Errorf("nocal profile = %+v", ctx.Profile)
	}
}

func TestLoadUnknownProfile(t *testing.T) {
	_, dir, _ := testEnv(t)
	writeConfig(t, dir, profilesCfg)

	_, err := Load(ParseArgs([]string{"--profile", "nope"}), false)
	var up UnknownProfileError
	if !errors.As(err, &up) {
		t.Fatalf("err = %v, want UnknownProfileError", err)
	}
	if up.Name != "nope" || up.Error() != "unknown profile nope" {
		t.Errorf("error = %q / name %q", up.Error(), up.Name)
	}
}

func TestLoadLegacyTopLevelKeyRejected(t *testing.T) {
	// Guard: an old-shape config must never load, or the binary would run
	// silently with zero bots.
	for _, key := range []string{"telegram", "discord", "owner", "wife", "rpc"} {
		_, dir, _ := testEnv(t)
		writeConfig(t, dir, fmt.Sprintf(`{%q:{}, "profiles":{"main":{}}}`, key))
		_, err := Load(ParseArgs(nil), false)
		if err == nil {
			t.Fatalf("legacy key %s: config must not load", key)
		}
		want := fmt.Sprintf("config.json: legacy top-level key %q is no longer supported; move it into profiles.<name> (see new profile shape)", key)
		if err.Error() != want {
			t.Errorf("legacy key %s: err = %q, want %q", key, err, want)
		}
	}
}

func TestLoadRequiresProfiles(t *testing.T) {
	_, dir, _ := testEnv(t)
	writeConfig(t, dir, `{"mail": true}`)
	_, err := Load(ParseArgs(nil), false)
	if err == nil || err.Error() != `config.json: "profiles" is required` {
		t.Fatalf("err = %v, want profiles required", err)
	}
	writeConfig(t, dir, `{"profiles": ["main"]}`)
	_, err = Load(ParseArgs(nil), false)
	if err == nil || err.Error() != `config.json: "profiles" must be an object` {
		t.Fatalf("err = %v, want profiles must be an object", err)
	}
}

func TestResolveProfileShapeErrors(t *testing.T) {
	cases := []struct{ cfg, want string }{
		{`{"profiles":{"main":[]}}`, "profiles.main must be an object"},
		{`{"profiles":{"main":{"telegram":[]}}}`, "profiles.main.telegram must be an object"},
		{`{"profiles":{"main":{"discord":true}}}`, "profiles.main.discord must be an object"},
		{`{"profiles":{"main":{"telegram":{"bots":"mb"}}}}`, "profiles.main.telegram.bots must be an array of strings"},
		{`{"profiles":{"main":{"telegram":{"bots":[1]}}}}`, "profiles.main.telegram.bots must be an array of strings"},
		{`{"profiles":{"main":{"telegram":{"roles":[]}}}}`, "profiles.main.telegram.roles must be an object with string values"},
		{`{"profiles":{"main":{"telegram":{"roles":{"12":2}}}}}`, "profiles.main.telegram.roles must be an object with string values"},
		{`{"profiles":{"main":{"discord":{"roles":{"a":true}}}}}`, "profiles.main.discord.roles must be an object with string values"},
		{`{"profiles":{"main":{"rpc":false}}}`, "profiles.main.rpc must be an object"},
		{`{"profiles":{"main":{"rpc":{"enabled":"yes"}}}}`, "profiles.main.rpc.enabled must be a boolean"},
		{`{"profiles":{"main":{"rpc":{"session":7}}}}`, "profiles.main.rpc.session must be a string"},
		{`{"profiles":{"main":{"rpc":{"all":1}}}}`, "profiles.main.rpc.all must be a boolean"},
		{`{"profiles":{"main":{"tidy":[]}}}`, "profiles.main.tidy must be an object"},
		{`{"profiles":{"main":{"tidy":{"exclude":"x"}}}}`, "profiles.main.tidy.exclude must be an array of strings"},
		{`{"profiles":{"main":{"tidy":{"learnOthers":1}}}}`, "profiles.main.tidy.learnOthers must be a boolean"},
		{`{"profiles":{"main":{"memory":5}}}`, "profiles.main.memory must be a string"},
		{`{"profiles":{"main":{"mail":"yes"}}}`, "profiles.main.mail must be a boolean"},
		{`{"profiles":{"main":{"calendars":{"a":"b"}}}}`, "profiles.main.calendars must be an array of strings"},
	}
	for _, tc := range cases {
		_, dir, _ := testEnv(t)
		writeConfig(t, dir, tc.cfg)
		_, err := Load(ParseArgs(nil), false)
		if err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Errorf("cfg %s: err = %v, want %q", tc.cfg, err, tc.want)
		}
	}
}

func TestEnvDirsDefaultsAndOverrides(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("OMOSENSE_DIR", "")
	t.Setenv("OMOSENSE_STATE", "")
	t.Setenv("OMOMEOW_DIR", "")
	t.Setenv("OMOMEOW_STATE", "")
	dir, state, err := EnvDirs()
	if err != nil || dir != filepath.Join(home, ".omosense") || state != filepath.Join(home, ".omosense", "state") {
		t.Fatalf("EnvDirs = %q %q %v, want default ~/.omosense and <dir>/state", dir, state, err)
	}
	override := filepath.Join(home, "run")
	t.Setenv("OMOSENSE_DIR", override)
	t.Setenv("OMOSENSE_STATE", filepath.Join(home, "elsewhere"))
	dir, state, err = EnvDirs()
	if err != nil || dir != override || state != filepath.Join(home, "elsewhere") {
		t.Fatalf("EnvDirs = %q %q %v, want overrides honored", dir, state, err)
	}
}

func TestEnvDirsStaleOmomeowRejected(t *testing.T) {
	// Guard: a stale OMOMEOW_* variable must fail loudly naming its
	// replacement instead of silently running against another directory.
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("OMOSENSE_DIR", "")
	t.Setenv("OMOSENSE_STATE", "")
	t.Setenv("OMOMEOW_DIR", filepath.Join(home, "omomeow"))
	t.Setenv("OMOMEOW_STATE", "")
	_, _, err := EnvDirs()
	if err == nil || err.Error() != "OMOMEOW_DIR is no longer read; set OMOSENSE_DIR" {
		t.Fatalf("err = %v, want OMOMEOW_DIR stale error", err)
	}
	t.Setenv("OMOMEOW_DIR", "")
	t.Setenv("OMOMEOW_STATE", filepath.Join(home, "legacy-state"))
	_, _, err = EnvDirs()
	if err == nil || err.Error() != "OMOMEOW_STATE is no longer read; set OMOSENSE_STATE" {
		t.Fatalf("err = %v, want OMOMEOW_STATE stale error", err)
	}
	// A stale variable next to its replacement is simply ignored.
	t.Setenv("OMOMEOW_STATE", "")
	dir := filepath.Join(home, "new")
	t.Setenv("OMOSENSE_DIR", dir)
	gotDir, gotState, err := EnvDirs()
	if err != nil || gotDir != dir || gotState != filepath.Join(dir, "state") {
		t.Fatalf("EnvDirs = %q %q %v, want the OMOSENSE_ override to win", gotDir, gotState, err)
	}
}

func TestLoadEnvRejectsStaleOmomeow(t *testing.T) {
	// A valid config does not rescue a stale environment.
	_, dir, _ := testEnv(t)
	writeConfig(t, dir, mainCfg)
	t.Setenv("OMOSENSE_DIR", "")
	t.Setenv("OMOMEOW_DIR", dir)
	_, err := Load(ParseArgs(nil), false)
	if err == nil || err.Error() != "OMOMEOW_DIR is no longer read; set OMOSENSE_DIR" {
		t.Fatalf("err = %v, want OMOMEOW_DIR stale error", err)
	}
}

func TestLoadWritableCreatesState(t *testing.T) {
	_, dir, state := testEnv(t)
	writeConfig(t, dir, mainCfg)

	if _, err := Load(ParseArgs(nil), true); err != nil {
		t.Fatalf("load: %v", err)
	}
	if fi, err := os.Stat(state); err != nil || !fi.IsDir() {
		t.Errorf("state dir not created (err=%v)", err)
	}
}

func TestLoadReadOnlyLeavesStateAbsent(t *testing.T) {
	_, dir, state := testEnv(t)
	writeConfig(t, dir, mainCfg)

	if _, err := Load(ParseArgs(nil), false); err != nil {
		t.Fatalf("load: %v", err)
	}
	if _, err := os.Stat(state); !os.IsNotExist(err) {
		t.Errorf("read-only load created the state dir (err=%v)", err)
	}
}

func TestLoadMissingConfig(t *testing.T) {
	_, _, _ = testEnv(t)
	if _, err := Load(ParseArgs(nil), false); err == nil {
		t.Fatal("expected an error for a missing config.json")
	}
}

func TestLoadBadJSONConfig(t *testing.T) {
	_, dir, _ := testEnv(t)
	writeConfig(t, dir, "{nope")
	if _, err := Load(ParseArgs(nil), false); err == nil {
		t.Fatal("expected an error for unparsable config.json")
	}
}

func TestLoadCfgRawKeepsOrder(t *testing.T) {
	_, dir, _ := testEnv(t)
	writeConfig(t, dir, `{"profiles":{"main":{}},"zz":1,"aa":2}`)

	ctx, err := Load(ParseArgs(nil), false)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	got := ctx.Cfg.Raw.Keys()
	want := []string{"profiles", "zz", "aa"}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("raw keys = %v, want %v", got, want)
	}
}

func TestCred(t *testing.T) {
	home := t.TempDir()
	f := filepath.Join(home, ".config", "agent-messenger", "telegrambot-credentials.json")
	if err := os.MkdirAll(filepath.Dir(f), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(f, []byte(`{"bots":{"tb":{"token":"tk"}},"other":1}`), 0o644); err != nil {
		t.Fatal(err)
	}

	tok, err := Cred(home, "telegrambot-credentials.json", "tb")
	if err != nil {
		t.Fatalf("cred: %v", err)
	}
	if tok != "tk" {
		t.Errorf("token = %q, want tk", tok)
	}

	_, err = Cred(home, "telegrambot-credentials.json", "missing")
	if err == nil {
		t.Fatal("expected an error for an unknown bot")
	}
	if strings.Contains(err.Error(), "tk") {
		t.Errorf("error must never echo a token value: %v", err)
	}
}
