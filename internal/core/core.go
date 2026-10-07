// Package core is the shared foundation of omosense: the run context and
// flat config loading, the single-instance lock, the stdout line grammar
// (Out/Sink), order-preserving JSON (OMap), and the Source interface that
// both the compat subcommands and the session host use.
//
// The package is owned by the foundation node; every other package codes
// against this API and must not modify it.
package core

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
)

// Profile is the resolved flat config.json of the folder omosense runs in.
// There is exactly one profile per folder: the profiles object is gone.
type Profile struct {
	Telegram      PlatformCfg
	Discord       PlatformCfg
	RPC           RPCCfg
	Tidy          TidyCfg
	Herdr         HerdrCfg
	Memory        string
	Calendars     *[]string // nil means all calendars
	Mail          bool
	Verify        VerifyCfg
	SilentMinutes int
	Transcriber   []string
	Guard         GuardCfg
}

// PlatformCfg is a platform section: the single bot this folder listens on
// and the role of every known sender id. Role names are data; only "owner"
// is special in code, unknown ids resolve to "other".
type PlatformCfg struct {
	Bot   string
	Roles map[string]string
}

// RPCCfg is the rpc section.
type RPCCfg struct {
	Enabled bool
	All     bool
	// Verify is the tri-state rpc.verify: absent (nil) and true are both
	// on, only an explicit false turns the done-verification hook off.
	Verify *bool
	// Labels holds the configured rpc.labels overrides; Label resolves a
	// key against them and the defaults.
	Labels map[string]string
}

// VerifyOn reports whether the rpc done-verification hook runs.
func (r RPCCfg) VerifyOn() bool { return r.Verify == nil || *r.Verify }

// Label returns the rpc batch label for key: the configured override,
// else the default. An unknown key has no default and returns "".
func (r RPCCfg) Label(key string) string {
	if v, ok := r.Labels[key]; ok {
		return v
	}
	return rpcLabelDefaults[key]
}

// rpcLabelList is the label table: key order fixes the unknown-key error
// text, def is the 0.1.0 default each label reproduces byte-for-byte.
var rpcLabelList = []struct{ key, def string }{
	{"task", "작업"},
	{"thread", "thread"},
	{"cwd", "cwd"},
	{"id", "완료 id"},
	{"seq", "seq"},
	{"doneAt", "done_at"},
	{"count", "count"},
	{"ack", "확인 명령"},
	{"unverified", "미검증"},
	{"verifyPending", "검증이 끝나지 않음"},
	{"more", "more"},
}

var rpcLabelDefaults = func() map[string]string {
	m := make(map[string]string, len(rpcLabelList))
	for _, e := range rpcLabelList {
		m[e.key] = e.def
	}
	return m
}()

// TidyCfg is the tidy section.
type TidyCfg struct {
	Enabled     bool
	LearnOthers bool
	Exclude     []string
}

// VerifyCfg is the verify section: the optional done-verification hook
// command (empty means no hook) and its per-attempt timeout.
type VerifyCfg struct {
	Command    []string
	TimeoutSec int
}

// GuardCfg is the guard section: host resource-guard thresholds.
type GuardCfg struct {
	StateFileBytes int64
}

// HerdrCfg is the herdr section. herdr is a default source: Enabled absent
// means on, and only an explicit false turns it off.
type HerdrCfg struct {
	Enabled *bool
	// Verify is the opt-in herdr.done hook: absent (false) keeps HERDR
	// lines byte-identical to 0.1.0.
	Verify bool
	// AgentPattern is the dead-pane foreground match source and AgentRe
	// its compiled form; after Load both always carry the configured or
	// default pattern, so AgentRe is never nil.
	AgentPattern string
	AgentRe      *regexp.Regexp
}

// On reports whether the herdr source runs: absent means yes.
func (h HerdrCfg) On() bool { return h.Enabled == nil || *h.Enabled }

// Cfg is the parsed config.json: the raw document kept in order. parseCfg
// rejects profiles-shaped and legacy documents, so a loaded Cfg is always
// the flat shape.
type Cfg struct {
	Raw *OMap
}

// Ctx is the per-run context: directories, config, the resolved flat
// profile, parsed flags, positional args, and the stdout writer.
type Ctx struct {
	Dir     string
	State   string
	Cfg     *Cfg
	Profile Profile
	Flags   map[string]bool
	Args    []string // positional args with flags stripped
	Out     *Out
}

// Sink is where a Source emits its stdout grammar lines. Both the compat
// subcommands and the session host pass the process Out.
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
// session host. Every source may also emit LOG lines in addition to its
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
	// ProfileSet reports that --profile or --profile=NAME appeared. The flag
	// was removed with the profiles object; callers reject it loudly so a
	// stale caller never silently runs against another folder's config.
	ProfileSet bool
	Flags      map[string]bool
	Rest       []string
}

// ParseArgs splits argv: --profile NAME and --profile=NAME are consumed and
// recorded in ProfileSet (the value is discarded), every other --x becomes
// a value-less flag, and remaining args are positional.
func ParseArgs(args []string) ParsedArgs {
	pa := ParsedArgs{Flags: map[string]bool{}}
	for i := 0; i < len(args); i++ {
		arg := args[i]
		switch {
		case arg == "--profile":
			pa.ProfileSet = true
			i++
		case strings.HasPrefix(arg, "--profile="):
			pa.ProfileSet = true
		case strings.HasPrefix(arg, "--"):
			pa.Flags[arg] = true
		default:
			pa.Rest = append(pa.Rest, arg)
		}
	}
	return pa
}

// EnvDirs resolves the run directory and state directory: $OMOSENSE_DIR
// else <cwd>/.omosense, and $OMOSENSE_STATE else <dir>/state. A stale
// OMOMEOW_* variable set without its OMOSENSE_ counterpart is an error
// naming the replacement, so a stale environment never silently runs
// elsewhere.
func EnvDirs() (dir, state string, err error) {
	dir = os.Getenv("OMOSENSE_DIR")
	if dir == "" {
		if os.Getenv("OMOMEOW_DIR") != "" {
			return "", "", errors.New("OMOMEOW_DIR is no longer read; set OMOSENSE_DIR")
		}
		cwd, err := os.Getwd()
		if err != nil {
			return "", "", fmt.Errorf("cwd: %w", err)
		}
		dir = filepath.Join(cwd, ".omosense")
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

// Load resolves the run directories (see EnvDirs), reads the flat
// config.json, parses it, and builds the Ctx with a stdout Out. When
// writable is true the state dir is created (host loops and lock holders);
// read-only paths must pass false and leave an absent state dir absent.
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
	prof, err := parseProfile(om)
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

// parseCfg rejects the shapes that would otherwise load silently without
// the bot or source the folder expects: the profiles object, the legacy
// owner/wife keys, and the bots array of the previous per-profile shape.
// A present key of the wrong shape is caught by parseProfile, which names
// the exact key path.
func parseCfg(om *OMap) (*Cfg, error) {
	if _, ok := om.Get("profiles"); ok {
		return nil, errors.New(`config.json: "profiles" is no longer supported; config.json is flat now (telegram, discord, rpc, tidy, herdr, memory, calendars, mail)`)
	}
	for _, k := range []string{"owner", "wife"} {
		if _, ok := om.Get(k); ok {
			return nil, fmt.Errorf("config.json: legacy top-level key %q is no longer supported; config.json is flat now (telegram, discord, rpc, tidy, herdr, memory, calendars, mail)", k)
		}
	}
	if _, ok := om.Get("bots"); ok {
		return nil, errors.New(`config.json: "bots" is no longer supported; telegram.bot and discord.bot hold one bot name each`)
	}
	return &Cfg{Raw: om}, nil
}

// defaultAgentPattern is the IS-5 dead-pane foreground default: the
// measured agent argvs (senpi/omo/claude/codex/opencode bundles, pi)
// against everything else a pane foreground can be.
const defaultAgentPattern = `senpi|omo|claude|codex|opencode|(^|/)pi( |$)`

// parseProfile validates the flat document. Every key is optional, but a
// present key must have the declared shape; violations name the key path
// (for example "config.json: telegram.bot must be a string") so the
// offending entry is located at a glance.
func parseProfile(om *OMap) (Profile, error) {
	var p Profile
	var err error
	if p.Telegram, err = platformCfg(om, "telegram"); err != nil {
		return Profile{}, err
	}
	if p.Discord, err = platformCfg(om, "discord"); err != nil {
		return Profile{}, err
	}
	if p.RPC, err = sectionCfg(om, "rpc", func(section *OMap) (RPCCfg, error) {
		var out RPCCfg
		var err error
		if out.Enabled, err = boolKey(section, "enabled", "rpc.enabled"); err != nil {
			return out, err
		}
		if out.All, err = boolKey(section, "all", "rpc.all"); err != nil {
			return out, err
		}
		if out.Verify, err = boolPtrKey(section, "verify", "rpc.verify"); err != nil {
			return out, err
		}
		if out.Labels, err = labelsKey(section); err != nil {
			return out, err
		}
		return out, nil
	}); err != nil {
		return Profile{}, err
	}
	if p.Tidy, err = sectionCfg(om, "tidy", func(section *OMap) (TidyCfg, error) {
		var out TidyCfg
		var err error
		if out.Enabled, err = boolKey(section, "enabled", "tidy.enabled"); err != nil {
			return out, err
		}
		if out.LearnOthers, err = boolKey(section, "learnOthers", "tidy.learnOthers"); err != nil {
			return out, err
		}
		if out.Exclude, err = strsKey(section, "exclude", "tidy.exclude"); err != nil {
			return out, err
		}
		return out, nil
	}); err != nil {
		return Profile{}, err
	}
	if p.Herdr, err = sectionCfg(om, "herdr", func(section *OMap) (HerdrCfg, error) {
		var out HerdrCfg
		var err error
		if out.Enabled, err = boolPtrKey(section, "enabled", "herdr.enabled"); err != nil {
			return out, err
		}
		if out.Verify, err = boolKey(section, "verify", "herdr.verify"); err != nil {
			return out, err
		}
		if out.AgentPattern, err = strKey(section, "agentPattern", "herdr.agentPattern"); err != nil {
			return out, err
		}
		if out.AgentPattern == "" {
			out.AgentPattern = defaultAgentPattern
		}
		re, err := regexp.Compile(out.AgentPattern)
		if err != nil {
			return HerdrCfg{}, fmt.Errorf("config.json: herdr.agentPattern is not a valid regular expression: %v", err)
		}
		out.AgentRe = re
		return out, nil
	}); err != nil {
		return Profile{}, err
	}
	if p.Herdr.AgentRe == nil {
		// herdr section absent or null: the default pattern still resolves,
		// so the compiled pattern is never nil after Load.
		p.Herdr.AgentPattern = defaultAgentPattern
		p.Herdr.AgentRe = regexp.MustCompile(defaultAgentPattern)
	}
	if p.Memory, err = strKey(om, "memory", "memory"); err != nil {
		return Profile{}, err
	}
	if p.Calendars, err = calendarsCfg(om); err != nil {
		return Profile{}, err
	}
	if p.Mail, err = boolKey(om, "mail", "mail"); err != nil {
		return Profile{}, err
	}
	if p.Verify, err = sectionCfg(om, "verify", func(section *OMap) (VerifyCfg, error) {
		var out VerifyCfg
		var err error
		if out.Command, err = cmdKey(section, "command", "verify.command"); err != nil {
			return out, err
		}
		n, err := posIntKey(section, "timeoutSec", "verify.timeoutSec")
		if err != nil {
			return out, err
		}
		out.TimeoutSec = int(n)
		return out, nil
	}); err != nil {
		return Profile{}, err
	}
	if p.Verify.TimeoutSec == 0 {
		p.Verify.TimeoutSec = 60
	}
	n, err := posIntKey(om, "silentMinutes", "silentMinutes")
	if err != nil {
		return Profile{}, err
	}
	if n == 0 {
		n = 30
	}
	p.SilentMinutes = int(n)
	if p.Transcriber, err = cmdKey(om, "transcriber", "transcriber"); err != nil {
		return Profile{}, err
	}
	if p.Guard, err = sectionCfg(om, "guard", func(section *OMap) (GuardCfg, error) {
		var out GuardCfg
		n, err := posIntKey(section, "stateFileBytes", "guard.stateFileBytes")
		if err != nil {
			return out, err
		}
		out.StateFileBytes = n
		return out, nil
	}); err != nil {
		return Profile{}, err
	}
	if p.Guard.StateFileBytes == 0 {
		p.Guard.StateFileBytes = 16 << 20
	}
	return p, nil
}

// platformCfg parses one platform section. A bots array is the previous
// per-profile shape and fails loudly instead of loading with no bot.
func platformCfg(m *OMap, platform string) (PlatformCfg, error) {
	var out PlatformCfg
	v, ok := m.Get(platform)
	if !ok || v == nil {
		return out, nil
	}
	section, isObj := v.(*OMap)
	if !isObj {
		return out, fmt.Errorf("config.json: %s must be an object", platform)
	}
	if _, ok := section.Get("bots"); ok {
		return out, fmt.Errorf("config.json: %s.bots is no longer supported; %s.bot holds one bot name", platform, platform)
	}
	var err error
	if out.Bot, err = strKey(section, "bot", platform+".bot"); err != nil {
		return out, err
	}
	r, ok := section.Get("roles")
	if !ok || r == nil {
		return out, nil
	}
	roles, isObj := r.(*OMap)
	if !isObj {
		return out, fmt.Errorf("config.json: %s.roles must be an object with string values", platform)
	}
	out.Roles = make(map[string]string, roles.Len())
	for _, id := range roles.Keys() {
		role, _ := roles.Get(id)
		s, isStr := role.(string)
		if !isStr {
			return out, fmt.Errorf("config.json: %s.roles must be an object with string values", platform)
		}
		out.Roles[id] = s
	}
	return out, nil
}

func sectionCfg[T any](m *OMap, key string, parse func(*OMap) (T, error)) (T, error) {
	var zero T
	v, ok := m.Get(key)
	if !ok || v == nil {
		return zero, nil
	}
	section, isObj := v.(*OMap)
	if !isObj {
		return zero, fmt.Errorf("config.json: %s must be an object", key)
	}
	return parse(section)
}

func calendarsCfg(m *OMap) (*[]string, error) {
	v, ok := m.Get("calendars")
	if !ok || v == nil {
		return nil, nil // nil means all calendars
	}
	s, err := strsKey(m, "calendars", "calendars")
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
		return false, fmt.Errorf("config.json: %s must be a boolean", path)
	}
	return b, nil
}

// boolPtrKey is boolKey for a tri-state key: absent and null both yield a
// nil pointer so the caller can tell "not set" from an explicit false.
func boolPtrKey(m *OMap, key, path string) (*bool, error) {
	v, ok := m.Get(key)
	if !ok || v == nil {
		return nil, nil
	}
	b, isBool := v.(bool)
	if !isBool {
		return nil, fmt.Errorf("config.json: %s must be a boolean", path)
	}
	return &b, nil
}

func strKey(m *OMap, key, path string) (string, error) {
	v, ok := m.Get(key)
	if !ok || v == nil {
		return "", nil
	}
	s, isStr := v.(string)
	if !isStr {
		return "", fmt.Errorf("config.json: %s must be a string", path)
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
		return nil, fmt.Errorf("config.json: %s must be an array of strings", path)
	}
	out := make([]string, 0, len(arr))
	for _, e := range arr {
		s, isStr := e.(string)
		if !isStr {
			return nil, fmt.Errorf("config.json: %s must be an array of strings", path)
		}
		out = append(out, s)
	}
	return out, nil
}

// cmdKey reads a command key: a non-empty array of strings whose first
// element (the program to run) is non-empty. Later elements may be empty
// strings: they are arguments the command receives verbatim.
func cmdKey(m *OMap, key, path string) ([]string, error) {
	v, ok := m.Get(key)
	if !ok || v == nil {
		return nil, nil
	}
	arr, isArr := v.([]any)
	if !isArr || len(arr) == 0 {
		return nil, fmt.Errorf("config.json: %s must be a non-empty array of strings", path)
	}
	out := make([]string, len(arr))
	for i, e := range arr {
		s, isStr := e.(string)
		if !isStr || (i == 0 && s == "") {
			return nil, fmt.Errorf("config.json: %s must be a non-empty array of strings", path)
		}
		out[i] = s
	}
	return out, nil
}

// posIntKey reads a positive integer key: a JSON number that is an
// integer greater than zero (fractions, zero and negatives are rejected).
// Absent and null return 0 so the caller applies its default.
func posIntKey(m *OMap, key, path string) (int64, error) {
	v, ok := m.Get(key)
	if !ok || v == nil {
		return 0, nil
	}
	n, isNum := v.(json.Number)
	if !isNum {
		return 0, fmt.Errorf("config.json: %s must be a positive integer", path)
	}
	i, err := n.Int64()
	if err != nil || i <= 0 {
		return 0, fmt.Errorf("config.json: %s must be a positive integer", path)
	}
	return i, nil
}

// labelsKey reads rpc.labels: string values under exactly the known
// label keys.
func labelsKey(section *OMap) (map[string]string, error) {
	v, ok := section.Get("labels")
	if !ok || v == nil {
		return nil, nil
	}
	lm, isObj := v.(*OMap)
	if !isObj {
		return nil, fmt.Errorf("config.json: rpc.labels must be an object")
	}
	out := make(map[string]string, lm.Len())
	for _, k := range lm.Keys() {
		if _, known := rpcLabelDefaults[k]; !known {
			keys := make([]string, len(rpcLabelList))
			for i, e := range rpcLabelList {
				keys[i] = e.key
			}
			return nil, fmt.Errorf("config.json: rpc.labels.%s is not a known label (%s)", k, strings.Join(keys, ", "))
		}
		val, _ := lm.Get(k)
		s, isStr := val.(string)
		if !isStr {
			return nil, fmt.Errorf("config.json: rpc.labels.%s must be a string", k)
		}
		out[k] = s
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
