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

// flatCfg is the whole 0.1.0 flat document: every key of the
// single-session shape with a value, so one load pins the entire Profile
// (the improve-8 keys stay absent here; TestLoadNewKeyDefaults pins them
// at their defaults).
const flatCfg = `{
  "telegram": {"bot": "mb", "roles": {"12": "owner", "13": "wife"}},
  "discord": {"bot": "db", "roles": {"44": "owner"}},
  "rpc": {"enabled": true, "all": false},
  "tidy": {"enabled": true, "learnOthers": true, "exclude": ["repo1"], "checkMin": 12, "quietMin": 240},
  "herdr": {"enabled": true},
  "memory": "mem-main",
  "calendars": ["cal1", "cal2"],
  "mail": true
}`

func boolPtr(b bool) *bool        { return &b }
func floatPtr(f float64) *float64 { return &f }

func TestLoadFlatConfig(t *testing.T) {
	_, dir, _ := testEnv(t)
	writeConfig(t, dir, flatCfg)

	ctx, err := Load(ParseArgs(nil), false)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	want := Profile{
		Telegram:      PlatformCfg{Bot: "mb", Roles: map[string]string{"12": "owner", "13": "wife"}},
		Discord:       PlatformCfg{Bot: "db", Roles: map[string]string{"44": "owner"}},
		RPC:           RPCCfg{Enabled: true},
		Tidy:          TidyCfg{Enabled: true, LearnOthers: true, Exclude: []string{"repo1"}, CheckMin: floatPtr(12), QuietMin: floatPtr(240)},
		Herdr:         HerdrCfg{Enabled: boolPtr(true), AgentPattern: "senpi|omo|claude|codex|opencode|(^|/)pi( |$)"},
		Memory:        "mem-main",
		Calendars:     &[]string{"cal1", "cal2"},
		Mail:          true,
		Verify:        VerifyCfg{TimeoutSec: 60},
		SilentMinutes: 30,
		Transcriber:   nil,
		Guard:         GuardCfg{StateFileBytes: 16 << 20},
	}
	// The compiled pattern is not comparable as a value; it is pinned
	// separately right after the DeepEqual pass.
	got := ctx.Profile
	re := got.Herdr.AgentRe
	got.Herdr.AgentRe = nil
	if !reflect.DeepEqual(got, want) {
		t.Errorf("profile = %+v, want %+v", got, want)
	}
	if re == nil || !re.MatchString("/Users/m/.bun/install/global/node_modules/@code-yeongyu/senpi/dist/bundle/cli.js") || re.MatchString("/bin/zsh -l") {
		t.Errorf("default agent pattern not compiled from the default source: %v", re)
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
		{`{"tidy":{"checkMin":"5"}}`, "config.json: tidy.checkMin must be a positive number"},
		{`{"tidy":{"checkMin":true}}`, "config.json: tidy.checkMin must be a positive number"},
		{`{"tidy":{"checkMin":0}}`, "config.json: tidy.checkMin must be a positive number"},
		{`{"tidy":{"checkMin":-1}}`, "config.json: tidy.checkMin must be a positive number"},
		{`{"tidy":{"quietMin":"60"}}`, "config.json: tidy.quietMin must be a positive number"},
		{`{"tidy":{"quietMin":false}}`, "config.json: tidy.quietMin must be a positive number"},
		{`{"tidy":{"quietMin":0}}`, "config.json: tidy.quietMin must be a positive number"},
		{`{"tidy":{"quietMin":-0.5}}`, "config.json: tidy.quietMin must be a positive number"},
		{`{"herdr":[]}`, "config.json: herdr must be an object"},
		{`{"herdr":{"enabled":"yes"}}`, "config.json: herdr.enabled must be a boolean"},
		{`{"memory":5}`, "config.json: memory must be a string"},
		{`{"mail":"yes"}`, "config.json: mail must be a boolean"},
		{`{"calendars":{"a":"b"}}`, "config.json: calendars must be an array of strings"},
		// improve-8 keys: a wrong type must never load silently (IS-15).
		{`{"verify":[]}`, "config.json: verify must be an object"},
		{`{"verify":{"command":"x"}}`, "config.json: verify.command must be a non-empty array of strings"},
		{`{"verify":{"command":[]}}`, "config.json: verify.command must be a non-empty array of strings"},
		{`{"verify":{"command":[""]}}`, "config.json: verify.command must be a non-empty array of strings"},
		{`{"verify":{"command":[1]}}`, "config.json: verify.command must be a non-empty array of strings"},
		{`{"verify":{"command":["sh"],"timeoutSec":0}}`, "config.json: verify.timeoutSec must be a positive integer"},
		{`{"verify":{"timeoutSec":-1}}`, "config.json: verify.timeoutSec must be a positive integer"},
		{`{"verify":{"timeoutSec":1.5}}`, "config.json: verify.timeoutSec must be a positive integer"},
		{`{"verify":{"timeoutSec":"60"}}`, "config.json: verify.timeoutSec must be a positive integer"},
		{`{"verify":{"timeoutSec":true}}`, "config.json: verify.timeoutSec must be a positive integer"},
		{`{"rpc":{"verify":"yes"}}`, "config.json: rpc.verify must be a boolean"},
		{`{"rpc":{"labels":[]}}`, "config.json: rpc.labels must be an object"},
		{`{"rpc":{"labels":{"bogus":"x"}}}`, "config.json: rpc.labels.bogus is not a known label (task, thread, cwd, id, seq, doneAt, count, ack, unverified, verifyPending, more)"},
		{`{"rpc":{"labels":{"task":2}}}`, "config.json: rpc.labels.task must be a string"},
		{`{"herdr":{"verify":"yes"}}`, "config.json: herdr.verify must be a boolean"},
		{`{"herdr":{"agentPattern":5}}`, "config.json: herdr.agentPattern must be a string"},
		{`{"herdr":{"agentPattern":"["}}`, "config.json: herdr.agentPattern is not a valid regular expression:"},
		{`{"silentMinutes":0}`, "config.json: silentMinutes must be a positive integer"},
		{`{"silentMinutes":-5}`, "config.json: silentMinutes must be a positive integer"},
		{`{"silentMinutes":1.5}`, "config.json: silentMinutes must be a positive integer"},
		{`{"silentMinutes":"30"}`, "config.json: silentMinutes must be a positive integer"},
		{`{"transcriber":"x"}`, "config.json: transcriber must be a non-empty array of strings"},
		{`{"transcriber":[]}`, "config.json: transcriber must be a non-empty array of strings"},
		{`{"transcriber":[""]}`, "config.json: transcriber must be a non-empty array of strings"},
		{`{"transcriber":[1]}`, "config.json: transcriber must be a non-empty array of strings"},
		{`{"guard":[]}`, "config.json: guard must be an object"},
		{`{"guard":{"stateFileBytes":0}}`, "config.json: guard.stateFileBytes must be a positive integer"},
		{`{"guard":{"stateFileBytes":-1}}`, "config.json: guard.stateFileBytes must be a positive integer"},
		{`{"guard":{"stateFileBytes":2.5}}`, "config.json: guard.stateFileBytes must be a positive integer"},
		{`{"guard":{"stateFileBytes":"16"}}`, "config.json: guard.stateFileBytes must be a positive integer"},
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

// TestLoadNewKeyDefaults pins IS-15: a 0.1.0 config (no improve-8 key)
// loads with every new key at its plan default and the default agent
// pattern compiled.
func TestLoadNewKeyDefaults(t *testing.T) {
	_, dir, _ := testEnv(t)
	writeConfig(t, dir, mainCfg)
	ctx, err := Load(ParseArgs(nil), false)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	p := ctx.Profile
	if p.Verify.Command != nil || p.Verify.TimeoutSec != 60 {
		t.Errorf("verify defaults = %+v, want no command and 60s", p.Verify)
	}
	if !p.RPC.VerifyOn() || p.RPC.Verify != nil {
		t.Errorf("rpc.verify default = %+v, want absent (on)", p.RPC.Verify)
	}
	if p.RPC.Labels != nil {
		t.Errorf("rpc.labels default = %v, want none", p.RPC.Labels)
	}
	for key, want := range map[string]string{
		"task": "작업", "thread": "thread", "cwd": "cwd", "id": "완료 id", "seq": "seq",
		"doneAt": "done_at", "count": "count", "ack": "확인 명령", "unverified": "미검증",
		"verifyPending": "검증이 끝나지 않음", "more": "more",
	} {
		if got := p.RPC.Label(key); got != want {
			t.Errorf("Label(%q) = %q, want %q", key, got, want)
		}
	}
	if p.Herdr.Verify {
		t.Errorf("herdr.verify default = true, want false")
	}
	const defPat = "senpi|omo|claude|codex|opencode|(^|/)pi( |$)"
	if p.Herdr.AgentPattern != defPat || p.Herdr.AgentRe == nil {
		t.Errorf("herdr pattern = %q %v, want the default compiled", p.Herdr.AgentPattern, p.Herdr.AgentRe)
	}
	if p.SilentMinutes != 30 {
		t.Errorf("silentMinutes default = %d, want 30", p.SilentMinutes)
	}
	if p.Tidy.CheckMin != nil || p.Tidy.QuietMin != nil {
		t.Errorf("tidy cadence defaults = %v/%v, want unset", p.Tidy.CheckMin, p.Tidy.QuietMin)
	}
	if p.Transcriber != nil {
		t.Errorf("transcriber default = %v, want none", p.Transcriber)
	}
	if p.Guard.StateFileBytes != 16777216 {
		t.Errorf("guard.stateFileBytes default = %d, want 16777216", p.Guard.StateFileBytes)
	}
}

// TestLoadNewKeysSet pins the new keys' happy path: every configured
// value resolves into the Profile and Label mixes overrides with
// defaults.
func TestLoadNewKeysSet(t *testing.T) {
	_, dir, _ := testEnv(t)
	writeConfig(t, dir, `{
	  "verify": {"command": ["/bin/sh", "-c", "exit 0"], "timeoutSec": 5},
	  "rpc": {"verify": false, "labels": {"task": "T", "ack": "A", "more": "M"}},
	  "herdr": {"verify": true, "agentPattern": "^agent-"},
	  "silentMinutes": 7,
	  "transcriber": ["/opt/tr.sh", "{audio}"],
	  "guard": {"stateFileBytes": 1048576}
	}`)
	ctx, err := Load(ParseArgs(nil), false)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	p := ctx.Profile
	if !reflect.DeepEqual(p.Verify.Command, []string{"/bin/sh", "-c", "exit 0"}) || p.Verify.TimeoutSec != 5 {
		t.Errorf("verify = %+v, want the configured command and 5s", p.Verify)
	}
	if p.RPC.VerifyOn() {
		t.Errorf("rpc.verify = %+v, want off", p.RPC.Verify)
	}
	for key, want := range map[string]string{"task": "T", "ack": "A", "more": "M", "thread": "thread", "id": "완료 id", "nope": ""} {
		if got := p.RPC.Label(key); got != want {
			t.Errorf("Label(%q) = %q, want %q", key, got, want)
		}
	}
	if !p.Herdr.Verify || p.Herdr.AgentPattern != "^agent-" || p.Herdr.AgentRe == nil ||
		!p.Herdr.AgentRe.MatchString("agent-x") || p.Herdr.AgentRe.MatchString("senpi bundle") {
		t.Errorf("herdr verify/pattern = %v %q %v, want true and the configured pattern compiled", p.Herdr.Verify, p.Herdr.AgentPattern, p.Herdr.AgentRe)
	}
	if p.SilentMinutes != 7 {
		t.Errorf("silentMinutes = %d, want 7", p.SilentMinutes)
	}
	if !reflect.DeepEqual(p.Transcriber, []string{"/opt/tr.sh", "{audio}"}) {
		t.Errorf("transcriber = %v, want the configured argv", p.Transcriber)
	}
	if p.Guard.StateFileBytes != 1048576 {
		t.Errorf("guard.stateFileBytes = %d, want 1048576", p.Guard.StateFileBytes)
	}
}

// TestLoadLiveMainShapeConfig pins IS-15: the live MAIN 0.1.0 key set
// loads unchanged with every new field at its default.
func TestLoadLiveMainShapeConfig(t *testing.T) {
	_, dir, _ := testEnv(t)
	writeConfig(t, dir, `{
	  "telegram": {"bot": "mb", "roles": {}},
	  "discord": {"bot": "db", "roles": {}},
	  "rpc": {"enabled": true, "all": true},
	  "tidy": {"enabled": true, "learnOthers": true, "exclude": []},
	  "herdr": {"enabled": true},
	  "memory": "main-id",
	  "calendars": [],
	  "mail": true
	}`)
	ctx, err := Load(ParseArgs(nil), false)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	p := ctx.Profile
	if !p.RPC.Enabled || !p.RPC.All || !p.Tidy.Enabled || !p.Tidy.LearnOthers ||
		p.Herdr.Enabled == nil || !*p.Herdr.Enabled || p.Memory != "main-id" ||
		p.Calendars == nil || len(*p.Calendars) != 0 || !p.Mail {
		t.Fatalf("old keys resolved wrong: %+v", p)
	}
	if p.Verify.Command != nil || p.Verify.TimeoutSec != 60 || p.RPC.Labels != nil || p.RPC.Verify != nil ||
		p.Herdr.Verify || p.Herdr.AgentRe == nil || p.SilentMinutes != 30 || p.Transcriber != nil || p.Guard.StateFileBytes != 16777216 {
		t.Errorf("new keys not at their defaults: %+v", p)
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

// TestLoadNullNewKeysKeepDefaults pins that an explicit null behaves as
// absent for every new key (the tri-state convention of the flat shape).
func TestLoadNullNewKeysKeepDefaults(t *testing.T) {
	_, dir, _ := testEnv(t)
	writeConfig(t, dir, `{"tidy":{"checkMin":null,"quietMin":null},"verify":null,"rpc":{"verify":null,"labels":null},"herdr":{"verify":null,"agentPattern":null},"silentMinutes":null,"transcriber":null,"guard":null}`)
	ctx, err := Load(ParseArgs(nil), false)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	p := ctx.Profile
	if p.Verify.Command != nil || p.Verify.TimeoutSec != 60 || p.RPC.Verify != nil || p.RPC.Labels != nil ||
		p.Herdr.Verify || p.Herdr.AgentRe == nil || p.SilentMinutes != 30 || p.Transcriber != nil || p.Guard.StateFileBytes != 16<<20 {
		t.Errorf("null new keys changed the defaults: %+v", p)
	}
	if p.Tidy.CheckMin != nil || p.Tidy.QuietMin != nil {
		t.Errorf("null tidy cadence keys changed the defaults: %+v", p.Tidy)
	}
}

// TestLoadTidyCadenceKeys pins the tidy cadence keys: configured numbers
// (fractions included) resolve into the Profile, absent stays unset.
func TestLoadTidyCadenceKeys(t *testing.T) {
	_, dir, _ := testEnv(t)
	writeConfig(t, dir, `{"tidy":{"checkMin":5,"quietMin":0.5}}`)
	ctx, err := Load(ParseArgs(nil), false)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if p := ctx.Profile.Tidy; p.CheckMin == nil || *p.CheckMin != 5 || p.QuietMin == nil || *p.QuietMin != 0.5 {
		t.Errorf("tidy cadence = %+v, want checkMin 5 and quietMin 0.5", p)
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
