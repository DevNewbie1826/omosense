package remind

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/DevNewbie1826/omosense/internal/core"
)

// AddUsage is printed with a usage error from remind add.
const AddUsage = `Usage: omosense remind add (--at TIME | --in DURATION) --platform telegram|discord --target JSON --text TEXT [--id ID]
`

// add appends one pending reminder to <State>/reminders.json in the schema
// the scheduler reads ({id, at, platform, target, text}) and prints
// "REMIND added <entry>". A bad argument exits 2 with the usage; a failed
// read or write exits 1.
func add(c *core.Ctx, args []string, stdout, stderr io.Writer) int {
	r, err := parseAdd(args, hookNow())
	if err != nil {
		fmt.Fprintln(stderr, "omosense: remind add:", err)
		fmt.Fprint(stderr, AddUsage)
		return 2
	}
	if err := withReminders(c.State, func(arr []any) ([]any, error) {
		return append(arr, r), nil
	}); err != nil {
		fmt.Fprintln(stderr, "omosense: remind add:", err)
		return 1
	}
	line := entryLine("added", r)
	fmt.Fprintf(stdout, "%s %s\n", line.prefix, line.text)
	return 0
}

// parseAdd validates the flags and builds the entry. --at accepts every form
// the scheduler parses; --in is a Go duration from now. The stored at is the
// normalized UTC ISO time. A time more than the scheduler's 6h late limit in
// the past is rejected, because the scheduler would only skip it.
func parseAdd(args []string, now time.Time) (*core.OMap, error) {
	fs := flag.NewFlagSet("remind add", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	at := fs.String("at", "", "")
	in := fs.String("in", "", "")
	platform := fs.String("platform", "", "")
	target := fs.String("target", "", "")
	text := fs.String("text", "", "")
	id := fs.String("id", "", "")
	if err := fs.Parse(args); err != nil {
		return nil, err
	}
	if fs.NArg() > 0 {
		return nil, fmt.Errorf("unexpected argument %q", fs.Arg(0))
	}
	var due time.Time
	switch {
	case (*at == "") == (*in == ""):
		return nil, errors.New("give exactly one of --at or --in")
	case *at != "":
		t, ok := jsDateParse(*at)
		if !ok {
			return nil, fmt.Errorf("--at %q is not an ISO time", *at)
		}
		due = t
	default:
		d, err := time.ParseDuration(*in)
		if err != nil || d < 0 {
			return nil, fmt.Errorf("--in %q is not a non-negative duration like 30m", *in)
		}
		due = now.Add(d)
	}
	if now.Sub(due) > maxLateDelivery {
		return nil, fmt.Errorf("--at is more than 6h in the past; the scheduler would skip it")
	}
	if *platform != "telegram" && *platform != "discord" {
		return nil, errors.New("--platform must be telegram or discord")
	}
	tv, err := core.ParseJSON([]byte(*target))
	tm, ok := tv.(*core.OMap)
	if err != nil || !ok {
		return nil, errors.New(`--target must be a JSON object, e.g. {"chat_id":123}`)
	}
	if strings.TrimSpace(*text) == "" {
		return nil, errors.New("--text is required")
	}
	if *id == "" {
		b := make([]byte, 4)
		if _, err := rand.Read(b); err != nil {
			return nil, err
		}
		*id = "r-" + hex.EncodeToString(b)
	}
	r := core.NewOMap()
	r.Set("id", *id)
	r.Set("at", core.ISO(due))
	r.Set("platform", *platform)
	r.Set("target", tm)
	r.Set("text", *text)
	return r, nil
}

// withReminders runs fn on the reminder list under the exclusive
// <state>/reminders.lock and writes the result back atomically (temp file +
// rename). The scheduler's tick takes the same lock, so an add and a tick
// never overwrite each other. The lock is opened per call so flock also
// serializes goroutines (the threads.lock / rpc-pending.lock pattern).
func withReminders(state string, fn func([]any) ([]any, error)) error {
	if err := os.MkdirAll(state, 0o755); err != nil {
		return err
	}
	unlock, err := lockReminders(state)
	if err != nil {
		return err
	}
	defer unlock()
	path := filepath.Join(state, "reminders.json")
	var arr []any
	b, err := os.ReadFile(path)
	switch {
	case errors.Is(err, os.ErrNotExist):
		arr = []any{}
	case err != nil:
		return err
	default:
		v, err := core.ParseJSON(b)
		if err != nil {
			return fmt.Errorf("parse reminders: %w", err)
		}
		var ok bool
		if arr, ok = v.([]any); !ok {
			return errors.New("reminders: expected a JSON array")
		}
	}
	arr, err = fn(arr)
	if err != nil {
		return err
	}
	out, err := marshalIndent2Array(arr)
	if err != nil {
		return err
	}
	tmp, err := os.CreateTemp(state, ".reminders-*")
	if err != nil {
		return err
	}
	defer os.Remove(tmp.Name())
	if _, err = tmp.Write(out); err == nil {
		err = tmp.Chmod(0o644)
	}
	if cerr := tmp.Close(); err == nil {
		err = cerr
	}
	if err != nil {
		return err
	}
	return os.Rename(tmp.Name(), path)
}

// lockReminders takes the exclusive <state>/reminders.lock.
func lockReminders(state string) (func(), error) {
	lock, err := os.OpenFile(filepath.Join(state, "reminders.lock"), os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, err
	}
	if err := syscall.Flock(int(lock.Fd()), syscall.LOCK_EX); err != nil {
		lock.Close()
		return nil, err
	}
	return func() {
		syscall.Flock(int(lock.Fd()), syscall.LOCK_UN)
		lock.Close()
	}, nil
}
