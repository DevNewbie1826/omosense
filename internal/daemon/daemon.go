// Package daemon implements the resident daemon hosting every source of
// every profile as goroutines, plus the attach client that streams a
// source's lines to stdout over a unix socket.
package daemon

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"slices"
	"strings"
	"syscall"
	"time"

	"github.com/DevNewbie1826/omosense/internal/core"
	"github.com/DevNewbie1826/omosense/internal/remind"
)

// DaemonHelp is the usage text printed by omosense daemon --help.
const DaemonHelp = `Usage: omosense daemon [status|stop [--profile P]]

Runs the resident daemon hosting every source of every profile as
goroutines, streaming lines to attached clients over a unix socket.
status prints the daemon state as one JSON line; stop stops a running
daemon. stop --profile P stops only P's sources, cancels its pending
reminders and discards queued replay, keeping the daemon and other
profiles running. It works offline too and prints one JSON result line.
The profile stays stopped across daemon restarts; the next attach of P
re-enables it.
`

// AttachHelp is the usage text printed by omosense attach --help.
const AttachHelp = `Usage: omosense attach <source> [--profile P] [--only PREFIXES] [--name N]

Streams a source's lines from the resident daemon to stdout, spawning
the daemon first when its socket is missing or refuses connections.
<source> is one of listen, google, remind, herdr, rpc, tidy, or all.
`

// RunDaemon runs the resident daemon in the foreground; the status and
// stop subcommands report or terminate a running daemon instead.
func RunDaemon(args []string) int {
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM, syscall.SIGHUP)
	defer cancel()
	p, err := environmentPaths()
	if err != nil {
		return report(err, 1)
	}
	if len(args) == 1 && (args[0] == "status" || args[0] == "stop") {
		if err := daemonControl(ctx, p, args[0]); err != nil {
			return report(err, 1)
		}
		return 0
	}
	if len(args) > 1 && args[0] == "stop" {
		profile := ""
		switch {
		case len(args) == 3 && args[1] == "--profile":
			profile = args[2]
		case len(args) == 2 && strings.HasPrefix(args[1], "--profile="):
			profile = strings.TrimPrefix(args[1], "--profile=")
		}
		if profile == "" || strings.HasPrefix(profile, "--") {
			return report(fmt.Errorf("expected daemon stop --profile P"), 2)
		}
		result, err := daemonProfileStop(ctx, p, profile)
		if err != nil {
			var missing core.UnknownProfileError
			if errors.As(err, &missing) {
				return report(err, 2)
			}
			return report(err, 1)
		}
		if err := writeFrame(os.Stdout, result); err != nil {
			return report(err, 1)
		}
		return 0
	}
	if len(args) != 0 {
		return report(fmt.Errorf("unexpected daemon arguments"), 2)
	}
	if err := testSpawnLog(); err != nil {
		return report(err, 1)
	}
	base, err := loadDaemonContext()
	if err != nil {
		return report(err, 1)
	}
	version, err := cliVersion()
	if err != nil {
		return report(err, 1)
	}
	registry := Registry
	if os.Getenv("OMOSENSE_TEST_REGISTRY") == "fake" {
		registry = fakeRegistry
	}
	s, err := newServer(base, p, serverOptions{version: version, registry: registry, clock: realClock{}})
	if err == nil {
		err = s.serve(ctx)
	}
	if err != nil {
		if errors.Is(err, context.Canceled) {
			return 0
		}
		var running *alreadyRunningError
		if errors.As(err, &running) {
			return report(err, 3)
		}
		return report(err, 1)
	}
	return 0
}

// RunAttach streams matching lines from the daemon to stdout and keeps
// running across daemon restarts, respawning the daemon when needed.
func RunAttach(args []string) int {
	h, err := parseAttach(args, "")
	if err != nil {
		return report(err, 2)
	}
	h.Version, err = cliVersion()
	if err != nil {
		return report(err, 1)
	}
	p, err := environmentPaths()
	if err != nil {
		return report(err, 1)
	}
	exe, err := os.Executable()
	if err != nil {
		return report(err, 1)
	}
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()
	code, err := streamAttach(ctx, os.Stdout, func(c context.Context) (net.Conn, *framer, error) {
		return ensureDaemon(c, p, h, exe)
	})
	if err != nil {
		return report(err, code)
	}
	return code
}

func report(err error, code int) int {
	fmt.Fprintln(os.Stderr, err)
	return code
}

func environmentPaths() (paths, error) {
	dir, _, err := core.EnvDirs()
	if err != nil {
		return paths{}, err
	}
	return pathsFor(dir, os.Getenv("OMOSENSE_SOCK")), nil
}

func loadDaemonContext() (*core.Ctx, error) {
	base, err := core.Load(core.ParseArgs(nil), true)
	var missing core.UnknownProfileError
	if !errors.As(err, &missing) {
		return base, err
	}
	p, pathErr := environmentPaths()
	if pathErr != nil {
		return nil, pathErr
	}
	b, readErr := os.ReadFile(filepath.Join(p.dir, "config.json"))
	if readErr != nil {
		return nil, readErr
	}
	raw, parseErr := core.ParseJSON(b)
	if parseErr != nil {
		return nil, parseErr
	}
	if object, ok := raw.(*core.OMap); ok {
		if v, ok := object.Get("profiles"); ok {
			if profiles, ok := v.(*core.OMap); ok && profiles.Len() > 0 {
				return core.Load(core.ParseArgs([]string{"--profile", profiles.Keys()[0]}), true)
			}
		}
	}
	return nil, err
}

func daemonControl(parent context.Context, p paths, cmd string) error {
	ctx, cancel := context.WithTimeout(parent, 10*time.Second)
	defer cancel()
	conn, err := (&net.Dialer{}).DialContext(ctx, "unix", p.socket)
	if err != nil {
		return err
	}
	defer conn.Close()
	end, _ := ctx.Deadline()
	conn.SetDeadline(end)
	if err := writeFrame(conn, command{Cmd: cmd}); err != nil {
		return err
	}
	var raw json.RawMessage
	if err := newFramer(conn).read(&raw); err != nil {
		return err
	}
	if cmd == "status" {
		_, err = fmt.Fprintln(os.Stdout, string(raw))
		return err
	}
	var r reply
	if err := json.Unmarshal(raw, &r); err != nil {
		return err
	}
	if !r.OK {
		return fmt.Errorf("daemon stop: %s", r.Error)
	}
	lock, err := acquireSpawn(ctx, p.lock)
	if err == nil {
		err = lock.Close()
	}
	return err
}

func daemonProfileStop(parent context.Context, p paths, profile string) (profileStopResult, error) {
	ctx, cancel := context.WithTimeout(parent, 40*time.Second)
	defer cancel()
	result := profileStopResult{Profile: profile, Stopped: []string{}, Daemon: "offline"}
	conn, err := (&net.Dialer{}).DialContext(ctx, "unix", p.socket)
	if err == nil {
		return onlineProfileStop(ctx, conn, profile)
	}
	if !errors.Is(err, syscall.ENOENT) && !errors.Is(err, syscall.ECONNREFUSED) {
		return result, err
	}
	c, err := core.Load(core.ParseArgs([]string{"--profile", profile}), true)
	if err != nil {
		return result, err
	}
	spawn, err := acquireSpawn(ctx, p.spawn)
	if err != nil {
		return result, fmt.Errorf("profile stop spawn flock: %w", err)
	}
	defer spawn.Close()
	// Do not use acquireLifetime: an offline operation must never write
	// or remove a daemon pid file.
	lifetime, err := os.OpenFile(p.lock, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return result, err
	}
	defer lifetime.Close()
	err = syscall.Flock(int(lifetime.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
	if errors.Is(err, syscall.EWOULDBLOCK) {
		lifetime.Close()
		spawn.Close()
		// A foreground daemon can own lifetime before its socket is bound.
		// Release spawn so an auto-spawn owner can also complete readiness.
		if err := testNotify("profile-stop-online-wait"); err != nil {
			return result, err
		}
		readyCtx, readyCancel := context.WithTimeout(ctx, 30*time.Second)
		defer readyCancel()
		backoff := 50 * time.Millisecond
		for {
			conn, err := (&net.Dialer{}).DialContext(readyCtx, "unix", p.socket)
			if err == nil {
				return onlineProfileStop(ctx, conn, profile)
			}
			if !errors.Is(err, syscall.ENOENT) && !errors.Is(err, syscall.ECONNREFUSED) {
				return result, err
			}
			if err := retryDelay(readyCtx, backoff); err != nil {
				return result, fmt.Errorf("profile stop daemon readiness: %w", err)
			}
			backoff = min(backoff*2, time.Second)
		}
	}
	if err != nil {
		return result, fmt.Errorf("profile stop lifetime flock: %w", err)
	}
	registry := Registry
	if os.Getenv("OMOSENSE_TEST_REGISTRY") == "fake" {
		registry = fakeRegistry
	}
	for _, source := range registry(c) {
		result.Stopped = append(result.Stopped, source.Name())
	}
	slices.Sort(result.Stopped)
	stopErr := writeProfileMarker(c, time.Now())
	// Lifetime ownership also excludes the daemon's journal writer. Discard
	// pre-crash pending replay even if reminder cancellation is blocked.
	journalPath := filepath.Join(c.State, "omosense-journal-"+profile+".jsonl")
	if _, err := os.Stat(journalPath); err == nil {
		j, err := openJournal(journalPath, time.Now())
		if err != nil {
			stopErr = errors.Join(stopErr, err)
		} else {
			stopErr = errors.Join(stopErr, j.discardPending(), j.close())
		}
	} else if !os.IsNotExist(err) {
		stopErr = errors.Join(stopErr, err)
	}
	release, blocked, metadata, err := c.TryAcquire("remind-"+profile, "remind")
	if err != nil {
		return result, errors.Join(stopErr, err)
	}
	if blocked != "" {
		b, err := metadata.Marshal()
		if err != nil {
			return result, errors.Join(stopErr, err)
		}
		return result, errors.Join(stopErr, fmt.Errorf("cancel reminders: locked by %s %s", blocked, b))
	}
	defer release()
	result.CancelledReminders, err = remind.CancelPending(c, time.Now())
	return result, errors.Join(stopErr, err)
}

func onlineProfileStop(ctx context.Context, conn net.Conn, profile string) (profileStopResult, error) {
	defer conn.Close()
	stopClose := context.AfterFunc(ctx, func() { conn.Close() })
	defer stopClose()
	end, _ := ctx.Deadline()
	conn.SetDeadline(end)
	// A legacy peer rejects this opcode rather than stopping the daemon.
	// Keep the request and reply on this connection: no capability probe
	// can protect a later connection from daemon replacement.
	if err := writeFrame(conn, command{Cmd: "stop-profile", Profile: profile}); err != nil {
		return profileStopResult{}, err
	}
	var r profileStopReply
	if err := newFramer(conn).read(&r); err != nil {
		return profileStopResult{}, err
	}
	if !r.OK {
		if strings.Contains(r.Error, "unknown command") {
			version := r.Version
			if version == "" {
				version = "unknown"
			}
			return r.profileStopResult, fmt.Errorf("omosense daemon (version %s) does not support stop --profile; upgrade the running daemon first (attach any source with the new binary, which negotiates the upgrade), then retry", version)
		}
		if r.Error == "unknown profile "+profile {
			return r.profileStopResult, core.UnknownProfileError{Name: profile}
		}
		return r.profileStopResult, fmt.Errorf("daemon stop: %s", r.Error)
	}
	if r.Profile != profile {
		return r.profileStopResult, fmt.Errorf("daemon stop: reply profile %q does not match requested profile %q", r.Profile, profile)
	}
	return r.profileStopResult, nil
}
