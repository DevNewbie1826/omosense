package core

import (
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func testEnv(t *testing.T) (home, dir, state string) {
	t.Helper()
	home = t.TempDir()
	dir = filepath.Join(home, ".omomeow")
	state = filepath.Join(dir, "state")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("HOME", home)
	t.Setenv("OMOMEOW_DIR", dir)
	t.Setenv("OMOMEOW_STATE", state)
	return home, dir, state
}

func writeConfig(t *testing.T, dir, cfg string) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(dir, "config.json"), []byte(cfg), 0o644); err != nil {
		t.Fatal(err)
	}
}

const fallbackCfg = `{"telegram":{"bot":"b1","dm_bot":"b2"},"discord":{"bot":"d1"}}`

func TestLoadFallbackProfile(t *testing.T) {
	_, dir, state := testEnv(t)
	writeConfig(t, dir, fallbackCfg)

	ctx, err := Load(ParseArgs(nil), false)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if ctx.Dir != dir || ctx.State != state {
		t.Errorf("dirs: got %q/%q, want %q/%q", ctx.Dir, ctx.State, dir, state)
	}
	want := Profile{Name: "main", Discord: true, Telegram: []string{"b1", "b2"}, Calendars: nil, Mail: true}
	if !reflect.DeepEqual(ctx.Profile, want) {
		t.Errorf("profile = %+v, want %+v", ctx.Profile, want)
	}
	if ctx.Cfg.Telegram.Bot != "b1" || ctx.Cfg.Telegram.DmBot != "b2" {
		t.Errorf("cfg.telegram = %+v", ctx.Cfg.Telegram)
	}
	if ctx.Cfg.Discord.Bot != "d1" {
		t.Errorf("cfg.discord = %+v", ctx.Cfg.Discord)
	}
}

const profilesCfg = `{"telegram":{"bot":"b1","dm_bot":"b2"},"discord":{"bot":"d1"},
"profiles":{
  "main":{"discord":true,"telegram":["mb"],"mail":true},
  "family":{"discord":false,"telegram":["fb"],"calendars":["cal1","cal2"],"mail":false},
  "nocal":{"discord":true,"telegram":[],"calendars":null,"mail":true}
}}`

func TestLoadNamedProfiles(t *testing.T) {
	_, dir, _ := testEnv(t)
	writeConfig(t, dir, profilesCfg)

	ctx, err := Load(ParseArgs([]string{"--profile", "main"}), false)
	if err != nil {
		t.Fatalf("load main: %v", err)
	}
	if ctx.Profile.Discord != true || !reflect.DeepEqual(ctx.Profile.Telegram, []string{"mb"}) ||
		ctx.Profile.Calendars != nil || ctx.Profile.Mail != true {
		t.Errorf("main profile = %+v", ctx.Profile)
	}

	ctx, err = Load(ParseArgs([]string{"--profile=family"}), false)
	if err != nil {
		t.Fatalf("load family: %v", err)
	}
	if ctx.Profile.Discord || ctx.Profile.Mail || !reflect.DeepEqual(ctx.Profile.Telegram, []string{"fb"}) {
		t.Errorf("family profile = %+v", ctx.Profile)
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

func TestLoadUnknownProfileWithFallbackConfig(t *testing.T) {
	// With no profiles key only "main" exists (the profile.ts fallback).
	_, dir, _ := testEnv(t)
	writeConfig(t, dir, fallbackCfg)

	_, err := Load(ParseArgs([]string{"--profile", "nope"}), false)
	var up UnknownProfileError
	if !errors.As(err, &up) {
		t.Fatalf("err = %v, want UnknownProfileError", err)
	}
}

func TestLoadWritableCreatesState(t *testing.T) {
	_, dir, state := testEnv(t)
	writeConfig(t, dir, fallbackCfg)

	if _, err := Load(ParseArgs(nil), true); err != nil {
		t.Fatalf("load: %v", err)
	}
	if fi, err := os.Stat(state); err != nil || !fi.IsDir() {
		t.Errorf("state dir not created (err=%v)", err)
	}
}

func TestLoadReadOnlyLeavesStateAbsent(t *testing.T) {
	_, dir, state := testEnv(t)
	writeConfig(t, dir, fallbackCfg)

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
	writeConfig(t, dir, `{"telegram":{"bot":"b","dm_bot":"d"},"zz":1,"aa":2}`)

	ctx, err := Load(ParseArgs(nil), false)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	got := ctx.Cfg.Raw.Keys()
	want := []string{"telegram", "zz", "aa"}
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
