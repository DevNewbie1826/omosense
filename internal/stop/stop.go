// Package stop implements the `omosense stop` subcommand: it signals this
// folder's running host and waits for it to exit. It reads only
// <state>/*.lock.json, never loads config.json, never creates the state dir
// and never deletes a lock file (the host removes its own on exit).
package stop

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/DevNewbie1826/omosense/internal/core"
)

// Help is the `omosense stop --help` text.
const Help = `Usage: omosense stop

Stop this folder's running omosense host. Reads <state>/*.lock.json (or
$OMOSENSE_STATE) and sends SIGTERM to every live pid whose command name (the
basename of argv[0]) starts with "omosense", then waits up to 10 seconds for
the process to exit and its locks to go away. It prints one result line and
does not need config.json.

Exit codes:
  0  not running, stopped, or the lock is stale or held by a foreign pid
  1  a verified omosense pid, or its lock file, still persists after the
     10 s wait (the message names the pid and the remaining lock), or a lock
     file is unreadable or malformed, or argv could not be read
  2  usage error
`

// waitTimeout is how long stop waits for every signaled pid to exit and
// every lock it saw to be released. Tests inject a shorter value.
var waitTimeout = 10 * time.Second

// pollEvery is the wait poll interval. Tests inject a smaller value.
var pollEvery = 100 * time.Millisecond

// procArgv0 returns a process's argv[0]. It is a var so tests can fake a
// failed read; the production reader is realArgv0.
var procArgv0 = realArgv0

// realArgv0 reads argv[0] for pid: on linux the first NUL-separated field of
// /proc/<pid>/cmdline, elsewhere what `ps -o comm=` prints (the executable
// path as exec'd). procps comm is never used on linux because it truncates.
func realArgv0(pid int) (string, error) {
	if runtime.GOOS == "linux" {
		b, err := os.ReadFile(fmt.Sprintf("/proc/%d/cmdline", pid))
		if err != nil {
			return "", err
		}
		if i := bytes.IndexByte(b, 0); i >= 0 {
			b = b[:i]
		}
		if s := string(b); s != "" {
			return s, nil
		}
		return "", fmt.Errorf("empty cmdline")
	}
	out, err := exec.Command("ps", "-o", "comm=", "-p", strconv.Itoa(pid)).Output()
	if err != nil {
		return "", err
	}
	if s := strings.TrimSpace(string(out)); s != "" {
		return s, nil
	}
	return "", fmt.Errorf("empty comm")
}

// isOmosense reports whether argv0 names an omosense process: the basename
// starts with "omosense" (the production host is npm/dist/omosense-<os>-<arch>).
func isOmosense(argv0 string) bool {
	return strings.HasPrefix(filepath.Base(argv0), "omosense")
}

// lockEntry is one parsed <state>/*.lock.json.
type lockEntry struct {
	file string
	pid  int
}

// Run executes `omosense stop`. It resolves the directories with
// core.EnvDirs only, so it works with no config.json and creates nothing.
func Run(stdout, stderr io.Writer) int {
	_, state, err := core.EnvDirs()
	if err != nil {
		fmt.Fprintln(stderr, "omosense: stop:", err)
		return 1
	}
	files, err := lockFiles(state)
	if err != nil {
		fmt.Fprintln(stderr, "omosense: stop:", err)
		return 1
	}
	if len(files) == 0 {
		fmt.Fprintln(stdout, "not running")
		return 0
	}
	sort.Strings(files)

	// Validate every lock before signaling anything: a malformed or
	// unreadable lock aborts the whole run without a single signal.
	var (
		verified []lockEntry
		reports  []string
	)
	for _, file := range files {
		e, err := parseLock(file)
		if err != nil {
			fmt.Fprintf(stderr, "omosense: stop: %v\n", err)
			return 1
		}
		if !alive(e.pid) {
			reports = append(reports, fmt.Sprintf("stale lock %s (pid %d is dead)", file, e.pid))
			continue
		}
		if e.pid == os.Getpid() {
			reports = append(reports, fmt.Sprintf("lock %s: pid %d is this process; remove the lock by hand", file, e.pid))
			continue
		}
		argv0, err := procArgv0(e.pid)
		if err != nil {
			fmt.Fprintf(stderr, "omosense: stop: pid %d: %v\n", e.pid, err)
			return 1
		}
		if !isOmosense(argv0) {
			reports = append(reports, fmt.Sprintf("lock %s: pid %d is not omosense; remove the lock by hand", file, e.pid))
			continue
		}
		verified = append(verified, e)
	}

	if len(verified) == 0 {
		fmt.Fprintln(stdout, resultLine("", reports))
		return 0
	}

	pids := distinctPids(verified)
	for _, pid := range pids {
		if err := syscall.Kill(pid, syscall.SIGTERM); err != nil && !errors.Is(err, syscall.ESRCH) {
			fmt.Fprintf(stderr, "omosense: stop: pid %d: %v\n", pid, err)
			return 1
		}
	}
	outstanding, ok := waitStopped(verified)
	if !ok {
		if len(outstanding) == 0 {
			outstanding = pids
		}
		parts := []string{fmt.Sprintf("could not stop omosense %s within %s", pidNoun(outstanding), waitTimeout)}
		parts = append(parts, remainingLockNotes(verified)...)
		parts = append(parts, reports...)
		fmt.Fprintln(stdout, strings.Join(parts, "; "))
		return 1
	}
	fmt.Fprintln(stdout, resultLine("stopped omosense "+pidNoun(pids), reports))
	return 0
}

// lockFiles returns every <state>/*.lock.json path, built from the literal
// state directory: the directory NAME is never interpreted as a glob pattern,
// so a folder called "project[1]" yields only its own locks and never the
// sibling "project1"'s. An absent state directory means nothing is running;
// any other read error is reported.
func lockFiles(state string) ([]string, error) {
	entries, err := os.ReadDir(state)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil, nil
		}
		return nil, err
	}
	files := make([]string, 0, len(entries))
	for _, e := range entries {
		if !e.Type().IsRegular() || !strings.HasSuffix(e.Name(), ".lock.json") {
			continue
		}
		files = append(files, filepath.Join(state, e.Name()))
	}
	sort.Strings(files)
	return files, nil
}

// parseLock reads and validates one lock file: it must be a JSON object
// whose pid is an integer greater than 1.
func parseLock(file string) (lockEntry, error) {
	b, err := os.ReadFile(file)
	if err != nil {
		return lockEntry{}, fmt.Errorf("lock %s: %w", file, err)
	}
	v, err := core.ParseJSON(b)
	if err != nil {
		return lockEntry{}, fmt.Errorf("lock %s: %w", file, err)
	}
	m, ok := v.(*core.OMap)
	if !ok {
		return lockEntry{}, fmt.Errorf("lock %s: expected a JSON object", file)
	}
	pid, ok := lockPid(m)
	if !ok || pid <= 1 {
		return lockEntry{}, fmt.Errorf("lock %s: pid must be an integer greater than 1", file)
	}
	return lockEntry{file: file, pid: pid}, nil
}

func lockPid(m *core.OMap) (int, bool) {
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
	return int(i), true
}

// alive reports whether pid exists: kill(pid, 0) succeeds for a live process
// and fails (ESRCH) for a dead one, matching the lock layer's convention.
func alive(pid int) bool { return syscall.Kill(pid, 0) == nil }

// waitStopped polls until every verified pid is gone and every lock file it
// saw is gone, or waitTimeout elapses. On timeout it reports the pids still
// outstanding: a pid whose process is alive, or one whose lock file was
// never released.
func waitStopped(entries []lockEntry) (outstanding []int, ok bool) {
	deadline := time.Now().Add(waitTimeout)
	for {
		if entriesGone(entries) {
			return nil, true
		}
		if !time.Now().Before(deadline) {
			return outstandingPids(entries), false
		}
		time.Sleep(pollEvery)
	}
}

func entriesGone(entries []lockEntry) bool {
	for _, e := range entries {
		if alive(e.pid) || fileExists(e.file) {
			return false
		}
	}
	return true
}

// outstandingPids returns every verified pid whose process is still alive or
// whose lock file is still present, ascending and without duplicates.
func outstandingPids(entries []lockEntry) []int {
	seen := map[int]bool{}
	out := make([]int, 0, len(entries))
	for _, e := range entries {
		if seen[e.pid] {
			continue
		}
		if alive(e.pid) || fileExists(e.file) {
			seen[e.pid] = true
			out = append(out, e.pid)
		}
	}
	sort.Ints(out)
	return out
}

// remainingLockNotes names every lock file that outlived the wait, so the
// timeout line says which locks are still on disk.
func remainingLockNotes(entries []lockEntry) []string {
	seen := map[string]bool{}
	var out []string
	for _, e := range entries {
		if seen[e.file] || !fileExists(e.file) {
			continue
		}
		seen[e.file] = true
		out = append(out, fmt.Sprintf("lock %s (pid %d) remains", e.file, e.pid))
	}
	return out
}

// resultLine joins the outcome and the per-lock reports into the single
// result line the contract requires.
func resultLine(outcome string, reports []string) string {
	parts := make([]string, 0, len(reports)+1)
	if outcome != "" {
		parts = append(parts, outcome)
	}
	parts = append(parts, reports...)
	if len(parts) == 0 {
		return "not running"
	}
	return strings.Join(parts, "; ")
}

func fileExists(p string) bool {
	_, err := os.Stat(p)
	return err == nil
}

func distinctPids(entries []lockEntry) []int {
	seen := map[int]bool{}
	out := make([]int, 0, len(entries))
	for _, e := range entries {
		if !seen[e.pid] {
			seen[e.pid] = true
			out = append(out, e.pid)
		}
	}
	sort.Ints(out)
	return out
}

func joinPids(pids []int) string {
	parts := make([]string, len(pids))
	for i, p := range pids {
		parts[i] = strconv.Itoa(p)
	}
	return strings.Join(parts, ", ")
}

// pidNoun renders "pid N" or "pids N, M" for the result line.
func pidNoun(pids []int) string {
	if len(pids) == 1 {
		return "pid " + strconv.Itoa(pids[0])
	}
	return "pids " + joinPids(pids)
}
