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
	"syscall"
	"time"

	"github.com/DevNewbie1826/omosense/internal/core"
)

// DaemonHelp is the usage text printed by omosense daemon --help.
const DaemonHelp = `Usage: omosense daemon [status|stop]

Runs the resident daemon hosting every source of every profile as
goroutines, streaming lines to attached clients over a unix socket.
status prints the daemon state as one JSON line; stop stops a running
daemon.
`

// AttachHelp is the usage text printed by omosense attach --help.
const AttachHelp = `Usage: omosense attach <source> [--profile P] [--only PREFIXES] [--name N]

Streams a source's lines from the resident daemon to stdout, spawning
the daemon first when its socket is missing or refuses connections.
<source> is one of listen, google, remind, herdr, tidy, or all.
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
	dir := os.Getenv("OMOMEOW_DIR")
	if dir == "" {
		home, err := os.UserHomeDir()
		if err != nil {
			return paths{}, err
		}
		dir = filepath.Join(home, ".omomeow")
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
