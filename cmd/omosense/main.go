// Command omosense is the Go rewrite of the ~/.omomeow bun monitors
// (listen, watch-google, remind, watch-herdr, memory-tidy, say). Each
// subcommand runs one source in-process with the TS stdout grammar; the
// resident daemon and its attach clients are the default long-term mode.
package main

import (
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/DevNewbie1826/omosense/internal/core"
	"github.com/DevNewbie1826/omosense/internal/daemon"
	"github.com/DevNewbie1826/omosense/internal/google"
	"github.com/DevNewbie1826/omosense/internal/herdr"
	"github.com/DevNewbie1826/omosense/internal/listen"
	"github.com/DevNewbie1826/omosense/internal/remind"
	"github.com/DevNewbie1826/omosense/internal/rpc"
	"github.com/DevNewbie1826/omosense/internal/say"
	"github.com/DevNewbie1826/omosense/internal/tidy"
)

func main() {
	os.Exit(run(os.Args[1:]))
}

func run(args []string) int {
	if len(args) == 0 {
		usage(os.Stderr)
		return 2
	}
	sub, rest := args[0], args[1:]
	if sub == "--help" || sub == "-h" {
		usage(os.Stdout)
		return 0
	}

	switch sub {
	case "daemon":
		if hasHelp(rest) {
			fmt.Print(daemon.DaemonHelp)
			return 0
		}
		return daemon.RunDaemon(rest)
	case "attach":
		if hasHelp(rest) {
			fmt.Print(daemon.AttachHelp)
			return 0
		}
		return daemon.RunAttach(rest)
	case "listen", "google", "remind", "herdr", "rpc", "tidy", "say":
		if hasHelp(rest) {
			fmt.Print(subHelp(sub))
			return 0
		}
		pa := core.ParseArgs(rest)
		// Read-only paths (--once/--now/--dry-run) and say must never create
		// the state dir; every writable host creates it before locking.
		writable := sub != "say" && !pa.Flags["--once"] && !pa.Flags["--now"] && !pa.Flags["--dry-run"]
		ctx, err := core.Load(pa, writable)
		if err != nil {
			var up core.UnknownProfileError
			if errors.As(err, &up) {
				fmt.Fprintln(os.Stderr, err)
				return 2
			}
			fmt.Fprintln(os.Stderr, "omosense:", err)
			return 1
		}
		switch sub {
		case "listen":
			return listen.Run(ctx, rest)
		case "google":
			return google.Run(ctx, rest)
		case "remind":
			return remind.Run(ctx, rest)
		case "herdr":
			return herdr.Run(ctx, rest)
		case "rpc":
			return rpc.Run(ctx, rest)
		case "tidy":
			return tidy.Run(ctx, rest)
		default:
			return say.Run(ctx, rest)
		}
	default:
		usage(os.Stderr)
		return 2
	}
}

func hasHelp(args []string) bool {
	for _, a := range args {
		if a == "--help" || a == "-h" {
			return true
		}
	}
	return false
}

func subHelp(sub string) string {
	switch sub {
	case "listen":
		return listen.Help
	case "google":
		return google.Help
	case "remind":
		return remind.Help
	case "herdr":
		return herdr.Help
	case "rpc":
		return rpc.Help
	case "tidy":
		return tidy.Help
	default:
		return say.Help
	}
}

func usage(w io.Writer) {
	fmt.Fprint(w, `Usage: omosense <subcommand> [--profile main|family] [flags]

Subcommands:
  listen    Telegram long-poll and Discord gateway listener (EVENT)
  google    calendar and mail watcher (CAL, SOON, MAIL)
  remind    reminder scheduler (REMIND)
  herdr     herdr pane watcher (HERDR)
  rpc       webchat rpc.sock session watcher (RPC)
  tidy      memory tidy watcher (TIDY)
  say       outbound message sender
  daemon    resident daemon hosting all sources
  attach    stream a source's lines from the daemon

Run omosense <subcommand> --help for per-subcommand help.
`)
}
