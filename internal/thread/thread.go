// Package thread implements the `omosense thread` subcommand: the writer
// half of the threads.json lifecycle (register and close, plan IS-1..IS-3).
// The readers (herdr jobPanes, rpc threads) keep reading the same fields,
// so register sets only the fields the flags name and preserves every
// other key plus the key order of the file and of the entry.
package thread

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/DevNewbie1826/omosense/internal/core"
)

// Help is the usage text printed by omosense thread --help, and by usage
// errors (then on stderr, exit 2).
const Help = `Usage: omosense thread register <thread-id> [flags]
       omosense thread close <thread-id>

Writes the thread registry <state>/threads.json that the herdr and rpc
sources read; it is the writer half of the thread lifecycle.

Verbs:
  register  upsert the thread: set only the given fields, status "active",
            remove closed, set started (ISO) only when absent; every other
            key and the key order of the file and of the entry survive
  close     set status "done" and closed to now

Both print one THREAD {"id":<thread-id>,...} line (entry keys in file
order) and exit 0.

Flags (register only; --flag value and --flag=value both work):
  --session <id>       session_id the rpc source matches sessions by
  --pane <pane-id>     pane the herdr source watches
  --machine <name>     machine the pane lives on
  --cwd <dir>          working directory of the thread
  --name <text>        human-readable thread name
  --platform <name>    platform the thread runs on

register needs --session or --pane. A missing value, an unknown flag, a
repeated flag, a missing thread id or an unknown verb prints this help to
stderr and exits 2 without writing anything. A malformed or array-form
threads.json is never overwritten: the run exits 1 leaving the file
byte-identical.
`

// flagFields fixes the register flags and the entry field each sets, in
// the IS-1 order; the order also fixes the field order of a new entry.
var flagFields = []struct{ flag, field string }{
	{"--session", "session_id"},
	{"--pane", "pane"},
	{"--machine", "machine"},
	{"--cwd", "cwd"},
	{"--name", "name"},
	{"--platform", "platform"},
}

// errArrayForm marks an array-form threads.json; its pinned error text is
// composed at print time (it names the verb twice). errUnknownClose marks
// a close of an id the registry does not hold.
var (
	errArrayForm    = errors.New("threads.json is an array")
	errUnknownClose = errors.New("unknown thread")
)

// Run is the compat subcommand host for thread. args is the raw argv after
// the subcommand word: value flags are parsed here, not by core.ParseArgs
// (which treats every --x as a value-less flag).
func Run(c *core.Ctx, args []string) int {
	verb, id, fields, ok := parseArgs(args)
	if !ok {
		fmt.Fprint(os.Stderr, Help)
		return 2
	}
	var entry *core.OMap
	err := transact(c.State, func(root *core.OMap) error {
		if verb == "register" {
			entry = register(root, id, fields)
			return nil
		}
		e, exists := entryOf(root, id)
		if !exists {
			return fmt.Errorf("%w %q", errUnknownClose, id)
		}
		e.Set("status", "done")
		e.Set("closed", core.ISO(time.Now()))
		entry = e
		return nil
	})
	if err != nil {
		switch {
		case errors.Is(err, errUnknownClose):
			fmt.Fprintf(os.Stderr, "omosense: thread close: unknown thread %q\n", id)
		case errors.Is(err, errArrayForm):
			fmt.Fprintf(os.Stderr, "omosense: thread %s: threads.json is an array; thread %s needs the object form\n", verb, verb)
		default:
			fmt.Fprintf(os.Stderr, "omosense: thread %s: %v\n", verb, err)
		}
		return 1
	}
	c.Out.Emit("THREAD", lineMap(id, entry))
	return 0
}

// parseArgs validates the raw thread argv: the verb, exactly one thread
// id, and register's value flags in both --flag value and --flag=value
// forms. ok=false is a usage error: the caller prints Help and exits 2
// having written nothing.
func parseArgs(args []string) (verb, id string, fields map[string]string, ok bool) {
	fields = map[string]string{}
	if len(args) == 0 {
		return "", "", nil, false
	}
	verb = args[0]
	if verb != "register" && verb != "close" {
		return "", "", nil, false
	}
	known := map[string]bool{}
	for _, ff := range flagFields {
		known[ff.flag] = true
	}
	var pos []string
	rest := args[1:]
	for i := 0; i < len(rest); i++ {
		tok := rest[i]
		if !strings.HasPrefix(tok, "--") {
			pos = append(pos, tok)
			continue
		}
		name, val, hasVal := tok, "", false
		if eq := strings.IndexByte(tok, '='); eq >= 0 {
			name, val, hasVal = tok[:eq], tok[eq+1:], true
		}
		if !known[name] {
			return "", "", nil, false
		}
		if !hasVal {
			if i+1 >= len(rest) || strings.HasPrefix(rest[i+1], "--") {
				return "", "", nil, false
			}
			i++
			val = rest[i]
		}
		if _, dup := fields[name]; dup {
			return "", "", nil, false
		}
		fields[name] = val
	}
	if len(pos) != 1 || pos[0] == "" {
		return "", "", nil, false
	}
	if verb == "close" && len(fields) != 0 {
		return "", "", nil, false
	}
	if verb == "register" {
		if _, session := fields["--session"]; !session {
			if _, pane := fields["--pane"]; !pane {
				return "", "", nil, false
			}
		}
	}
	return verb, pos[0], fields, true
}

// register returns the upserted entry and stores it under id: only the
// fields the flags name are set, status is active, closed is removed and
// started is set only when absent (OMap.Set keeps every existing key's
// position, so the entry's and the file's key order survive).
func register(root *core.OMap, id string, fields map[string]string) *core.OMap {
	entry, _ := entryOf(root, id)
	if entry == nil {
		entry = core.NewOMap()
	}
	for _, ff := range flagFields {
		if v, ok := fields[ff.flag]; ok {
			entry.Set(ff.field, v)
		}
	}
	entry.Set("status", "active")
	entry.Delete("closed")
	if !entry.Has("started") {
		entry.Set("started", core.ISO(time.Now()))
	}
	root.Set(id, entry)
	return entry
}

// entryOf returns the entry for id when it is a JSON object. A missing id
// or a non-object value yields ok=false: register starts a fresh entry in
// its place, close refuses to touch it (an unknown thread).
func entryOf(root *core.OMap, id string) (*core.OMap, bool) {
	v, ok := root.Get(id)
	if !ok {
		return nil, false
	}
	entry, isMap := v.(*core.OMap)
	return entry, isMap
}

// lineMap builds the THREAD payload: {"id":<thread-id>} followed by the
// entry's keys in file order.
func lineMap(id string, entry *core.OMap) *core.OMap {
	m := core.NewOMap()
	m.Set("id", id)
	for _, k := range entry.Keys() {
		v, _ := entry.Get(k)
		m.Set(k, v)
	}
	return m
}

// transact runs fn under the exclusive <state>/threads.lock with the
// parsed registry: a nil return writes the registry back atomically, an
// error leaves the file untouched. The lock is opened per call so flock
// also serializes goroutines, not only separate processes.
func transact(state string, fn func(root *core.OMap) error) error {
	if err := os.MkdirAll(state, 0o755); err != nil {
		return err
	}
	path := filepath.Join(state, "threads.json")
	lock, err := os.OpenFile(filepath.Join(state, "threads.lock"), os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return err
	}
	defer lock.Close()
	if err := syscall.Flock(int(lock.Fd()), syscall.LOCK_EX); err != nil {
		return err
	}
	defer syscall.Flock(int(lock.Fd()), syscall.LOCK_UN)
	root, err := readRegistry(path)
	if err != nil {
		return err
	}
	if err := fn(root); err != nil {
		return err
	}
	return writeFile(path, root)
}

// readRegistry parses threads.json. A missing file is an empty registry
// (the write path then creates it); an array-form file keeps its pinned
// error; any other failure names the file and the underlying error.
func readRegistry(path string) (*core.OMap, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return core.NewOMap(), nil
		}
		return nil, fmt.Errorf("threads.json: %w", err)
	}
	v, err := core.ParseJSON(b)
	if err != nil {
		return nil, fmt.Errorf("threads.json: %w", err)
	}
	root, ok := v.(*core.OMap)
	if !ok {
		if _, isArr := v.([]any); isArr {
			return nil, errArrayForm
		}
		return nil, errors.New("threads.json: not a JSON object")
	}
	return root, nil
}

// writeFile replaces threads.json atomically: the bytes go to a temp file
// in the same directory (the bun JSON.stringify(v, null, 2) layout plus a
// trailing newline) that keeps the current file mode (0o600 when new) and
// renames over the target.
func writeFile(path string, root *core.OMap) error {
	b, err := root.MarshalIndent2()
	if err != nil {
		return err
	}
	b = append(b, '\n')
	mode := os.FileMode(0o600)
	if fi, err := os.Stat(path); err == nil {
		mode = fi.Mode().Perm()
	}
	tmp, err := os.CreateTemp(filepath.Dir(path), ".threads-*")
	if err != nil {
		return err
	}
	defer os.Remove(tmp.Name())
	if _, err = tmp.Write(b); err == nil {
		err = tmp.Sync()
	}
	closeErr := tmp.Close()
	if err != nil {
		return err
	}
	if closeErr != nil {
		return closeErr
	}
	if err := os.Chmod(tmp.Name(), mode); err != nil {
		return err
	}
	return os.Rename(tmp.Name(), path)
}
