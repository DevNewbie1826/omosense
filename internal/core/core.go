// Package core is the shared foundation of omosense: the run context and
// profile loading, the single-instance lock, the stdout line grammar
// (Out/Sink), order-preserving JSON (OMap), and the Source interface that
// both the compat subcommands and the resident daemon host.
//
// The package is owned by the foundation node; every other package codes
// against this API and must not modify it.
package core

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// Profile is the resolved run profile, mirroring profile.ts.
type Profile struct {
	Name      string
	Discord   bool
	Telegram  []string
	Calendars *[]string // nil means all calendars
	Mail      bool
}

// TelegramCfg is the telegram section of config.json.
type TelegramCfg struct {
	Bot   string
	DmBot string
}

// DiscordCfg is the discord section of config.json.
type DiscordCfg struct {
	Bot string
}

// Cfg is the parsed config.json: the raw document kept in order plus the
// typed sections every source needs.
type Cfg struct {
	Raw      *OMap
	Telegram TelegramCfg
	Discord  DiscordCfg
	Profiles *OMap // cfg.profiles; nil when absent, so the fallback applies
}

// UnknownProfileError reports a profile name that config.json does not define.
type UnknownProfileError struct {
	Name string
}

func (e UnknownProfileError) Error() string { return "unknown profile " + e.Name }

// Ctx is the per-run context: directories, config, the resolved profile,
// parsed flags, positional args, and the stdout writer.
type Ctx struct {
	Dir     string
	State   string
	Cfg     *Cfg
	Profile Profile
	Flags   map[string]bool
	Args    []string // positional args with flags stripped
	Out     *Out
}

// Sink is where a Source emits its stdout grammar lines. The compat host
// passes the process Out; the daemon passes a client-routing sink.
type Sink interface {
	// Emit writes "PREFIX <json>\n" with the payload JSON-encoded without
	// HTML escaping.
	Emit(prefix string, payload any)
	// Raw writes "PREFIX text\n" without JSON-encoding the text, for lines
	// like "REMIND sent {json}".
	Raw(prefix, text string)
	// Log writes "LOG msg\n".
	Log(msg string)
}

// Source is one monitor hosted either by its compat subcommand or by the
// resident daemon. Every source may also emit LOG lines in addition to its
// Prefixes.
type Source interface {
	Name() string
	Prefixes() []string
	AlwaysOn() bool
	LockName() (name, legacy string)
	Run(ctx context.Context, sink Sink) error
}

// ParsedArgs is the result of ParseArgs.
type ParsedArgs struct {
	Profile string
	Flags   map[string]bool
	Rest    []string
}

// ParseArgs mirrors profile.ts argv handling: --profile NAME and
// --profile=NAME set the profile (default "main"; a trailing --profile
// yields the empty profile); every other --x becomes a value-less flag;
// remaining args are positional.
func ParseArgs(args []string) ParsedArgs {
	pa := ParsedArgs{Profile: "main", Flags: map[string]bool{}}
	for i := 0; i < len(args); i++ {
		arg := args[i]
		switch {
		case arg == "--profile":
			i++
			if i < len(args) {
				pa.Profile = args[i]
			} else {
				pa.Profile = ""
			}
		case strings.HasPrefix(arg, "--profile="):
			pa.Profile = strings.TrimPrefix(arg, "--profile=")
		case strings.HasPrefix(arg, "--"):
			pa.Flags[arg] = true
		default:
			pa.Rest = append(pa.Rest, arg)
		}
	}
	return pa
}

// Load performs the profile.ts logic: it resolves Dir ($OMOMEOW_DIR or
// ~/.omomeow) and State ($OMOMEOW_STATE or Dir/state), reads config.json,
// resolves the requested profile, and builds the Ctx with an stdout Out.
// When writable is true the state dir is created (main loops and lock
// holders); read-only paths must pass false and leave an absent state dir
// absent.
func Load(pa ParsedArgs, writable bool) (*Ctx, error) {
	home, err := os.UserHomeDir()
	if err != nil {
		return nil, fmt.Errorf("home dir: %w", err)
	}
	dir := os.Getenv("OMOMEOW_DIR")
	if dir == "" {
		dir = filepath.Join(home, ".omomeow")
	}
	state := os.Getenv("OMOMEOW_STATE")
	if state == "" {
		state = filepath.Join(dir, "state")
	}
	raw, err := os.ReadFile(filepath.Join(dir, "config.json"))
	if err != nil {
		return nil, fmt.Errorf("read config: %w", err)
	}
	v, err := ParseJSON(raw)
	if err != nil {
		return nil, fmt.Errorf("parse config: %w", err)
	}
	om, ok := v.(*OMap)
	if !ok {
		return nil, fmt.Errorf("config.json: expected a JSON object")
	}
	cfg := parseCfg(om)
	prof, err := cfg.Resolve(pa.Profile)
	if err != nil {
		return nil, err
	}
	if writable {
		if err := os.MkdirAll(state, 0o755); err != nil {
			return nil, fmt.Errorf("create state dir: %w", err)
		}
	}
	return &Ctx{
		Dir:     dir,
		State:   state,
		Cfg:     cfg,
		Profile: prof,
		Flags:   pa.Flags,
		Args:    pa.Rest,
		Out:     NewOut(os.Stdout),
	}, nil
}

func parseCfg(om *OMap) *Cfg {
	c := &Cfg{Raw: om}
	if tg := omapOf(om, "telegram"); tg != nil {
		c.Telegram = TelegramCfg{Bot: strOf(tg, "bot"), DmBot: strOf(tg, "dm_bot")}
	}
	if dc := omapOf(om, "discord"); dc != nil {
		c.Discord = DiscordCfg{Bot: strOf(dc, "bot")}
	}
	c.Profiles = omapOf(om, "profiles")
	return c
}

// Resolve returns the named profile. With no cfg.profiles key only "main"
// exists, resolved from the profile.ts fallback; an unknown name yields
// UnknownProfileError.
func (c *Cfg) Resolve(name string) (Profile, error) {
	if c.Profiles == nil {
		if name != "main" {
			return Profile{}, UnknownProfileError{Name: name}
		}
		return Profile{
			Name:     name,
			Discord:  true,
			Telegram: []string{c.Telegram.Bot, c.Telegram.DmBot},
			Mail:     true,
		}, nil
	}
	v, ok := c.Profiles.Get(name)
	if !ok {
		return Profile{}, UnknownProfileError{Name: name}
	}
	p := Profile{Name: name}
	m, _ := v.(*OMap)
	if m == nil {
		return p, nil
	}
	p.Discord = boolOf(m, "discord")
	p.Telegram = strsOf(m, "telegram")
	if cal, ok := m.Get("calendars"); ok && cal != nil {
		s := strsOfValue(cal)
		p.Calendars = &s
	}
	p.Mail = boolOf(m, "mail")
	return p, nil
}

// Cred reads the bot token stored at
// <home>/.config/agent-messenger/<file> under .bots[bot].token. The token
// must never be logged or echoed in an error.
func Cred(home, file, bot string) (string, error) {
	b, err := os.ReadFile(filepath.Join(home, ".config", "agent-messenger", file))
	if err != nil {
		return "", fmt.Errorf("cred %s: %w", file, err)
	}
	v, err := ParseJSON(b)
	if err != nil {
		return "", fmt.Errorf("cred %s: %w", file, err)
	}
	m, _ := v.(*OMap)
	bots := omapOf(m, "bots")
	if bots == nil {
		return "", fmt.Errorf("cred %s: no bots object", file)
	}
	entry := omapOf(bots, bot)
	if entry == nil {
		return "", fmt.Errorf("cred %s: unknown bot %q", file, bot)
	}
	tok := strOf(entry, "token")
	if tok == "" {
		return "", fmt.Errorf("cred %s: bot %q has no token", file, bot)
	}
	return tok, nil
}

func omapOf(m *OMap, key string) *OMap {
	if m == nil {
		return nil
	}
	v, _ := m.Get(key)
	om, _ := v.(*OMap)
	return om
}

func strOf(m *OMap, key string) string {
	v, _ := m.Get(key)
	s, _ := v.(string)
	return s
}

func boolOf(m *OMap, key string) bool {
	v, _ := m.Get(key)
	b, _ := v.(bool)
	return b
}

func strsOf(m *OMap, key string) []string {
	v, _ := m.Get(key)
	return strsOfValue(v)
}

func strsOfValue(v any) []string {
	arr, ok := v.([]any)
	if !ok {
		return nil
	}
	var out []string
	for _, e := range arr {
		if s, ok := e.(string); ok {
			out = append(out, s)
		}
	}
	return out
}
