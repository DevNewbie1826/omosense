package core

import (
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

const mainCfg = `{"telegram":{"bot":"mb","roles":{}},"discord":{"bot":"db"}}`

// flatCfg is the whole flat document: every key of the single-session shape
// with a value, so one load pins the entire Profile.
const flatCfg = `{
  "telegram": {"bot": "mb", "roles": {"12": "owner", "13": "wife"}},
  "discord": {"bot": "db", "roles": {"44": "owner"}},
  "rpc": {"enabled": true, "all": false},
  "tidy": {"enabled": true, "learnOthers": true, "exclude": ["repo1"]},
  "herdr": {"enabled": true},
  "memory": "mem-main",
  "calendars": ["cal1", "cal2"],
  "mail": true
}`

func boolPtr(b bool) *bool { return &b }

func TestLoadFlatConfig(t *testing.T) {
	_, dir, _ := testEnv(t)
	writeConfig(t, dir, flatCfg)

	ctx, err := Load(ParseArgs(nil), false)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	want := Profile{
		Telegram:  PlatformCfg{Bot: "mb", Roles: map[string]string{"12": "owner", "13": "wife"}},
		Discord:   PlatformCfg{Bot: "db", Roles: map[string]string{"44": "owner"}},
		RPC:       RPCCfg{Enabled: true},
		Tidy:      TidyCfg{Enabled: true, LearnOthers: true, Exclude: []string{"repo1"}},
		Herdr:     HerdrCfg{Enabled: boolPtr(true)},
		Memory:    "mem-main",
		Calendars: &[]string{"cal1", "cal2"},
		Mail:      true,
	}
	if !reflect.DeepEqual(ctx.Profile, want) {
		t.Errorf("profile = %+v, want %+v", ctx.Profile, want)
	}
}

func TestLoadHerdrDefaultsOn(t *testing.T) {
	// herdr is a default source: an absent key (and an explicit null) means
	// on, and only an explicit false turns it off.
	for _, tc := range []struct {
		name string
		cfg  string
	}{{"absent", mainCfg}, {"null", `{"herdr":null}`}, {"false", `{"herdr":{"enabled":false}}`}} {
		t.Run(tc.name, func(t *testing.T) {
			_, dir, _ := testEnv(t)
			writeConfig(t, dir, tc.cfg)
			ctx, err := Load(ParseArgs(nil), false)
			if err != nil {
				t.Fatalf("load: %v", err)
			}
			want := tc.name != "false"
			if ctx.Profile.Herdr.On() != want {
				t.Errorf("herdr.On() = %v, want %v (enabled %v)", ctx.Profile.Herdr.On(), want, ctx.Profile.Herdr.Enabled)
			}
		})
	}
}

func TestLoadRejectsProfiles(t *testing.T) {
	// Guard: a stale profiles-shaped config must fail loudly instead of
	// loading with no bots at all.
	for _, cfg := range []string{
		`{"profiles":{"main":{"telegram":{"bots":["mb"]}}}}`,
		`{"profiles":["main"]}`,
		`{"profiles":null}`,
	} {
		_, dir, _ := testEnv(t)
		writeConfig(t, dir, cfg)
		_, err := Load(ParseArgs(nil), false)
		if err == nil || !strings.Contains(err.Error(), `"profiles" is no longer supported`) {
			t.Errorf("cfg %s: err = %v, want the profiles-removed message", cfg, err)
		}
	}
}

func TestLoadLegacyKeyRejected(t *testing.T) {
	// Guard: an old-shape config must never load, or the binary would run
	// silently with zero bots.
	for _, key := range []string{"owner", "wife"} {
		_, dir, _ := testEnv(t)
		writeConfig(t, dir, fmt.Sprintf(`{%q:"x"}`, key))
		_, err := Load(ParseArgs(nil), false)
		if err == nil {
			t.Fatalf("legacy key %s: config must not load", key)
		}
		want := fmt.Sprintf("config.json: legacy top-level key %q is no longer supported; config.json is flat now (telegram, discord, rpc, tidy, herdr, memory, calendars, mail)", key)
		if err.Error() != want {
			t.Errorf("legacy key %s: err = %q, want %q", key, err, want)
		}
	}
	_, dir, _ := testEnv(t)
	writeConfig(t, dir, `{"bots":["mb"]}`)
	_, err := Load(ParseArgs(nil), false)
	if err == nil || !strings.Contains(err.Error(), `"bots" is no longer supported`) {
		t.Fatalf("bots array: err = %v, want the bots-removed message", err)
	}
}

func TestLoadPlatformBotsArrayRejected(t *testing.T) {
	// Guard: the previous per-profile bots array must not load as a
	// bot-less config.
	for _, platform := range []string{"telegram", "discord"} {
		_, dir, _ := testEnv(t)
		writeConfig(t, dir, fmt.Sprintf(`{%q:{"bots":["mb"]}}`, platform))
		_, err := Load(ParseArgs(nil), false)
		if err == nil {
			t.Fatalf("%s.bots: config must not load", platform)
		}
		want := fmt.Sprintf("config.json: %s.bots is no longer supported; %s.bot holds one bot name", platform, platform)
		if err.Error() != want {
			t.Errorf("%s.bots: err = %q, want %q", platform, err, want)
		}
	}
}

func TestResolveProfileShapeErrors(t *testing.T) {
	cases := []struct{ cfg, want string }{
		{`{"telegram":[]}`, "config.json: telegram must be an object"},
		{`{"discord":true}`, "config.json: discord must be an object"},
		{`{"telegram":{"bot":1}}`, "config.json: telegram.bot must be a string"},
		{`{"discord":{"bot":["db"]}}`, "config.json: discord.bot must be a string"},
		{`{"telegram":{"roles":[]}}`, "config.json: telegram.roles must be an object with string values"},
		{`{"telegram":{"roles":{"12":2}}}`, "config.json: telegram.roles must be an object with string values"},
		{`{"discord":{"roles":{"a":true}}}`, "config.json: discord.roles must be an object with string values"},
		{`{"rpc":false}`, "config.json: rpc must be an object"},
		{`{"rpc":{"enabled":"yes"}}`, "config.json: rpc.enabled must be a boolean"},
		{`{"rpc":{"all":1}}`, "config.json: rpc.all must be a boolean"},
		{`{"tidy":[]}`, "config.json: tidy must be an object"},
		{`{"tidy":{"exclude":"x"}}`, "config.json: tidy.exclude must be an array of strings"},
		{`{"tidy":{"learnOthers":1}}`, "config.json: tidy.learnOthers must be a boolean"},
		{`{"herdr":[]}`, "config.json: herdr must be an object"},
		{`{"herdr":{"enabled":"yes"}}`, "config.json: herdr.enabled must be a boolean"},
		{`{"memory":5}`, "config.json: memory must be a string"},
		{`{"mail":"yes"}`, "config.json: mail must be a boolean"},
		{`{"calendars":{"a":"b"}}`, "config.json: calendars must be an array of strings"},
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
	cwd := t.TempDir()
	t.Chdir(cwd)
	dir, state, err := EnvDirs()
	if err != nil || dir != filepath.Join(cwd, ".omosense") || state != filepath.Join(cwd, ".omosense", "state") {
		t.Fatalf("EnvDirs = %q %q %v, want the cwd default <cwd>/.omosense and <dir>/state", dir, state, err)
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
	writeConfig(t, dir, `{"mail":true,"zz":1,"aa":2}`)

	ctx, err := Load(ParseArgs(nil), false)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	got := ctx.Cfg.Raw.Keys()
	want := []string{"mail", "zz", "aa"}
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
