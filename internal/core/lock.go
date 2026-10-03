package core

import (
	"encoding/json"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"
)

func lockPath(state, name string) string {
	return filepath.Join(state, name+".lock.json")
}

// TryAcquire takes the <name>.lock.json lock in c.State, checking the
// legacy lock file first when legacy is non-empty (lock.ts order). A holder
// whose pid is alive (syscall.Kill(pid, 0) succeeds; any error counts as
// dead, matching the TS catch) and is not this process blocks the acquire:
// TryAcquire then returns the blocking lock's name in blockedBy (the legacy
// name for a live legacy lock) and its parsed content in held, with a nil
// release. A dead holder is overwritten. An unparsable lock file is an
// error. The daemon host uses TryAcquire so it can report
// ALREADY_RUNNING itself without exiting.
func (c *Ctx) TryAcquire(name, legacy string) (release func(), blockedBy string, held *OMap, err error) {
	if err := os.MkdirAll(c.State, 0o755); err != nil {
		return nil, "", nil, fmt.Errorf("create state dir: %w", err)
	}
	if legacy != "" {
		blocked, m, err := c.checkLock(legacy)
		if err != nil {
			return nil, "", nil, err
		}
		if blocked {
			return nil, legacy, m, nil
		}
	}
	blocked, m, err := c.checkLock(name)
	if err != nil {
		return nil, "", nil, err
	}
	if blocked {
		return nil, name, m, nil
	}
	if err := os.WriteFile(lockPath(c.State, name), ownerLock(), 0o644); err != nil {
		return nil, "", nil, fmt.Errorf("write lock: %w", err)
	}
	pid := os.Getpid()
	return func() {
		b, err := os.ReadFile(lockPath(c.State, name))
		if err != nil {
			return
		}
		v, err := ParseJSON(b)
		if err != nil {
			return
		}
		if m, ok := v.(*OMap); ok {
			if p, ok := lockPid(m); ok && int(p) == pid {
				_ = os.Remove(lockPath(c.State, name))
			}
		}
	}, "", nil, nil
}

// Acquire is the exiting lock.ts equivalent for compat hosts: on conflict
// it prints "LOG ALREADY_RUNNING <blockedBy> <held compact json>" to the
// ctx Out and exits 3; on an unparsable lock it prints to stderr and exits
// 1. On success it installs SIGINT/SIGTERM/SIGHUP handlers that release and
// exit 0, and returns the release func for deferring. Daemon-hosted
// sources must use TryAcquire instead (no process-global effects).
func (c *Ctx) Acquire(name, legacy string) func() {
	release, blockedBy, held, err := c.TryAcquire(name, legacy)
	if err != nil {
		fmt.Fprintf(os.Stderr, "omosense: lock %s: %v\n", name, err)
		os.Exit(1)
	}
	if blockedBy != "" {
		b, _ := held.Marshal()
		c.Out.Log(fmt.Sprintf("ALREADY_RUNNING %s %s", blockedBy, string(b)))
		os.Exit(3)
	}
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM, syscall.SIGHUP)
	go func() {
		<-sig
		release()
		os.Exit(0)
	}()
	return release
}

func (c *Ctx) checkLock(name string) (blocked bool, held *OMap, err error) {
	b, err := os.ReadFile(lockPath(c.State, name))
	if os.IsNotExist(err) {
		return false, nil, nil
	}
	if err != nil {
		return false, nil, fmt.Errorf("read lock %s: %w", name, err)
	}
	v, err := ParseJSON(b)
	if err != nil {
		return false, nil, fmt.Errorf("lock %s: %w", name, err)
	}
	m, ok := v.(*OMap)
	if !ok {
		return false, nil, fmt.Errorf("lock %s: expected a JSON object", name)
	}
	if pid, ok := lockPid(m); ok && pid != int64(os.Getpid()) && syscall.Kill(int(pid), 0) == nil {
		return true, m, nil
	}
	return false, m, nil
}

func ownerLock() []byte {
	cwd, err := os.Getwd()
	if err != nil {
		cwd = "."
	}
	if v, ok := os.LookupEnv("PI_SESSION_CWD"); ok {
		cwd = v
	}
	m := NewOMap()
	m.Set("pid", os.Getpid())
	m.Set("session", envOrNil("PI_SESSION_ID"))
	m.Set("pane", envOrNil("HERDR_PANE_ID"))
	m.Set("cwd", cwd)
	m.Set("started", ISO(time.Now()))
	b, err := m.Marshal()
	if err != nil {
		return []byte("{}")
	}
	return b
}

func envOrNil(key string) any {
	if v, ok := os.LookupEnv(key); ok {
		return v
	}
	return nil
}

func lockPid(m *OMap) (int64, bool) {
	v, ok := m.Get("pid")
	if !ok {
		return 0, false
	}
	n, ok := v.(json.Number)
	if !ok {
		return 0, false
	}
	i, err := n.Int64()
	if err != nil {
		return 0, false
	}
	return i, true
}
