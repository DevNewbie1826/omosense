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
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// Profile is the resolved run profile: one self-contained block of the
// config.json profiles object.
type Profile struct {
	Name      string
	Telegram  PlatformCfg
	Discord   PlatformCfg
	RPC       RPCCfg
	Tidy      TidyCfg
	Memory    string
	Calendars *[]string // nil means all calendars
	Mail      bool
}

// PlatformCfg is a platform section of one profile: the bots the profile
// listens on and the role of every known sender id. Role names are data;
// only "owner" is special in code, unknown ids resolve to "other".
type PlatformCfg struct {
	Bots  []string
	Roles map[string]string
}

// RPCCfg is the rpc section of one profile.
type RPCCfg struct {
	Enabled bool
	Session string // empty means the webchat sessions.json
	All     bool
}

// TidyCfg is the tidy section of one profile.
type TidyCfg struct {
	Enabled     bool
	LearnOthers bool
	Exclude     []string
}

// Cfg is the parsed config.json: the raw document kept in order plus the
// profiles object. parseCfg rejects legacy documents, so Profiles is never
// nil in a successfully loaded config.
type Cfg struct {
	Raw      *OMap
	Profiles *OMap // cfg.profiles
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

// EnvDirs resolves the run directory and state directory: $OMOSENSE_DIR
// else ~/.omosense, and $OMOSENSE_STATE else <dir>/state. A stale OMOMEOW_*
// variable set without its OMOSENSE_ counterpart is an error naming the
// replacement, so a stale environment never silently runs elsewhere.
func EnvDirs() (dir, state string, err error) {
	home, err := os.UserHomeDir()
	if err != nil {
		return "", "", fmt.Errorf("home dir: %w", err)
	}
	dir = os.Getenv("OMOSENSE_DIR")
	if dir == "" {
		if os.Getenv("OMOMEOW_DIR") != "" {
			return "", "", errors.New("OMOMEOW_DIR is no longer read; set OMOSENSE_DIR")
		}
		dir = filepath.Join(home, ".omosense")
	}
	state = os.Getenv("OMOSENSE_STATE")
	if state == "" {
		if os.Getenv("OMOMEOW_STATE") != "" {
			return "", "", errors.New("OMOMEOW_STATE is no longer read; set OMOSENSE_STATE")
		}
		state = filepath.Join(dir, "state")
	}
	return dir, state, nil
}

// Load resolves the run directories (see EnvDirs), reads config.json,
// resolves the requested profile, and builds the Ctx with an stdout Out.
// When writable is true the state dir is created (main loops and lock
// holders); read-only paths must pass false and leave an absent state dir
// absent.
func Load(pa ParsedArgs, writable bool) (*Ctx, error) {
	dir, state, err := EnvDirs()
	if err != nil {
		return nil, err
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
	cfg, err := parseCfg(om)
	if err != nil {
		return nil, err
	}
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

// legacyKeys are the pre-generalize top-level config sections. A config
// still carrying one is from the old shape and must fail loudly instead of
// silently running without bots.
var legacyKeys = []string{"telegram", "discord", "owner", "wife", "rpc"}

// parseCfg validates the document shape: no legacy top-level keys, and a
// profiles object. Per-profile validation happens in Resolve.
func parseCfg(om *OMap) (*Cfg, error) {
	for _, k := range legacyKeys {
		if _, ok := om.Get(k); ok {
			return nil, fmt.Errorf("config.json: legacy top-level key %q is no longer supported; move it into profiles.<name> (see new profile shape)", k)
		}
	}
	v, ok := om.Get("profiles")
	if !ok || v == nil {
		return nil, errors.New(`config.json: "profiles" is required`)
	}
	profiles, isObj := v.(*OMap)
	if !isObj {
		return nil, errors.New(`config.json: "profiles" must be an object`)
	}
	return &Cfg{Raw: om, Profiles: profiles}, nil
}

// Resolve returns the named profile. Every profile key is optional, but a
// present key must have the declared shape; violations name
// profiles.<name>.<key> so the offending entry is located at a glance. An
// unknown name yields UnknownProfileError.
func (c *Cfg) Resolve(name string) (Profile, error) {
	v, ok := c.Profiles.Get(name)
	if !ok {
		return Profile{}, UnknownProfileError{Name: name}
	}
	p := Profile{Name: name}
	m, isObj := v.(*OMap)
	if !isObj {
		return Profile{}, fmt.Errorf("config.json: profiles.%s must be an object", name)
	}
	var err error
	if p.Telegram, err = platformCfg(m, "telegram", name); err != nil {
		return Profile{}, err
	}
	if p.Discord, err = platformCfg(m, "discord", name); err != nil {
		return Profile{}, err
	}
	if p.RPC, err = sectionCfg(m, "rpc", name, func(section *OMap) (RPCCfg, error) {
		var out RPCCfg
		var err error
		if out.Enabled, err = boolKey(section, "enabled", name+".rpc.enabled"); err != nil {
			return out, err
		}
		if out.Session, err = strKey(section, "session", name+".rpc.session"); err != nil {
			return out, err
		}
		if out.All, err = boolKey(section, "all", name+".rpc.all"); err != nil {
			return out, err
		}
		return out, nil
	}); err != nil {
		return Profile{}, err
	}
	if p.Tidy, err = sectionCfg(m, "tidy", name, func(section *OMap) (TidyCfg, error) {
		var out TidyCfg
		var err error
		if out.Enabled, err = boolKey(section, "enabled", name+".tidy.enabled"); err != nil {
			return out, err
		}
		if out.LearnOthers, err = boolKey(section, "learnOthers", name+".tidy.learnOthers"); err != nil {
			return out, err
		}
		if out.Exclude, err = strsKey(section, "exclude", name+".tidy.exclude"); err != nil {
			return out, err
		}
		return out, nil
	}); err != nil {
		return Profile{}, err
	}
	if p.Memory, err = strKey(m, "memory", name+".memory"); err != nil {
		return Profile{}, err
	}
	if p.Calendars, err = calendarsCfg(m, name); err != nil {
		return Profile{}, err
	}
	if p.Mail, err = boolKey(m, "mail", name+".mail"); err != nil {
		return Profile{}, err
	}
	return p, nil
}

func platformCfg(m *OMap, platform, profile string) (PlatformCfg, error) {
	var out PlatformCfg
	v, ok := m.Get(platform)
	if !ok || v == nil {
		return out, nil
	}
	section, isObj := v.(*OMap)
	if !isObj {
		return out, fmt.Errorf("config.json: profiles.%s.%s must be an object", profile, platform)
	}
	var err error
	if out.Bots, err = strsKey(section, "bots", profile+"."+platform+".bots"); err != nil {
		return out, err
	}
	r, ok := section.Get("roles")
	if !ok || r == nil {
		return out, nil
	}
	roles, isObj := r.(*OMap)
	if !isObj {
		return out, fmt.Errorf("config.json: profiles.%s.%s.roles must be an object with string values", profile, platform)
	}
	out.Roles = make(map[string]string, roles.Len())
	for _, id := range roles.Keys() {
		role, _ := roles.Get(id)
		s, isStr := role.(string)
		if !isStr {
			return out, fmt.Errorf("config.json: profiles.%s.%s.roles must be an object with string values", profile, platform)
		}
		out.Roles[id] = s
	}
	return out, nil
}

func sectionCfg[T any](m *OMap, key, profile string, parse func(*OMap) (T, error)) (T, error) {
	var zero T
	v, ok := m.Get(key)
	if !ok || v == nil {
		return zero, nil
	}
	section, isObj := v.(*OMap)
	if !isObj {
		return zero, fmt.Errorf("config.json: profiles.%s.%s must be an object", profile, key)
	}
	return parse(section)
}

func calendarsCfg(m *OMap, profile string) (*[]string, error) {
	v, ok := m.Get("calendars")
	if !ok || v == nil {
		return nil, nil // nil means all calendars
	}
	s, err := strsKey(m, "calendars", profile+".calendars")
	if err != nil {
		return nil, err
	}
	return &s, nil
}

func boolKey(m *OMap, key, path string) (bool, error) {
	v, ok := m.Get(key)
	if !ok || v == nil {
		return false, nil
	}
	b, isBool := v.(bool)
	if !isBool {
		return false, fmt.Errorf("config.json: profiles.%s must be a boolean", path)
	}
	return b, nil
}

func strKey(m *OMap, key, path string) (string, error) {
	v, ok := m.Get(key)
	if !ok || v == nil {
		return "", nil
	}
	s, isStr := v.(string)
	if !isStr {
		return "", fmt.Errorf("config.json: profiles.%s must be a string", path)
	}
	return s, nil
}

func strsKey(m *OMap, key, path string) ([]string, error) {
	v, ok := m.Get(key)
	if !ok || v == nil {
		return nil, nil
	}
	arr, isArr := v.([]any)
	if !isArr {
		return nil, fmt.Errorf("config.json: profiles.%s must be an array of strings", path)
	}
	out := make([]string, 0, len(arr))
	for _, e := range arr {
		s, isStr := e.(string)
		if !isStr {
			return nil, fmt.Errorf("config.json: profiles.%s must be an array of strings", path)
		}
		out = append(out, s)
	}
	return out, nil
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
