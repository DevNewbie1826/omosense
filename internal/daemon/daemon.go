// Package daemon implements the resident daemon hosting every source of
// every profile as goroutines, plus the attach client that streams a
// source's lines to stdout over a unix socket.
package daemon

import (
	"fmt"
	"os"
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
	fmt.Fprintln(os.Stderr, "omosense daemon: not implemented yet")
	return 1
}

// RunAttach streams matching lines from the daemon to stdout and keeps
// running across daemon restarts, respawning the daemon when needed.
func RunAttach(args []string) int {
	fmt.Fprintln(os.Stderr, "omosense attach: not implemented yet")
	return 1
}
