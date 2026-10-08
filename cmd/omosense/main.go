// Command omosense is the Go rewrite of the bun monitors (listen,
// watch-google, remind, watch-herdr, memory-tidy, say). Bare `omosense`
// runs one foreground session host; each subcommand runs one source
// in-process with the TS stdout grammar.
package main

import (
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/DevNewbie1826/omosense/internal/core"
	"github.com/DevNewbie1826/omosense/internal/google"
	"github.com/DevNewbie1826/omosense/internal/herdr"
	"github.com/DevNewbie1826/omosense/internal/host"
	"github.com/DevNewbie1826/omosense/internal/listen"
	"github.com/DevNewbie1826/omosense/internal/remind"
	"github.com/DevNewbie1826/omosense/internal/rpc"
	"github.com/DevNewbie1826/omosense/internal/say"
	"github.com/DevNewbie1826/omosense/internal/stop"
	"github.com/DevNewbie1826/omosense/internal/thread"
	"github.com/DevNewbie1826/omosense/internal/tidy"
)

func main() {
	os.Exit(run(os.Args[1:]))
}

// profileRemoved is the stale-caller message: --profile belonged to the
// profiles object, and a caller still passing it must fail loudly instead
// of silently reading another folder's config.
const profileRemoved = "omosense: --profile was removed; config.json is per folder now (run omosense in the folder that holds .omosense/config.json)"

func run(args []string) int {
	for _, a := range args {
		if a == "--profile" || strings.HasPrefix(a, "--profile=") {
			fmt.Fprintln(os.Stderr, profileRemoved)
			return 2
		}
	}
	if len(args) == 0 {
		return runHost()
	}
	sub, rest := args[0], args[1:]
	if sub == "--help" || sub == "-h" {
		usage(os.Stdout)
		return 0
	}

	switch sub {
	case "stop":
		// stop reads only this folder's lock files, so it is dispatched
		// before core.Load and needs no config.json.
		if hasHelp(rest) {
			fmt.Print(subHelp("stop"))
			return 0
		}
		if len(rest) > 0 {
			fmt.Fprintln(os.Stderr, "omosense: stop takes no arguments")
			return 2
		}
		return stop.Run(os.Stdout, os.Stderr)
	case "listen", "google", "remind", "herdr", "rpc", "tidy", "say", "thread":
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
		case "thread":
			return thread.Run(ctx, rest)
		default:
			return say.Run(ctx, rest)
		}
	default:
		usage(os.Stderr)
		return 2
	}
}

// runHost is bare `omosense`: one foreground process hosting every source
// the folder's flat config enables. The memory-repo check (IS-14) runs
// after Load and before any lock or source: a configured memory id whose
// repo is missing refuses the start.
func runHost() int {
	ctx, err := core.Load(core.ParseArgs(nil), true)
	if err != nil {
		fmt.Fprintln(os.Stderr, "omosense:", err)
		return 1
	}
	if err := core.CheckMemory(ctx); err != nil {
		fmt.Fprintln(os.Stderr, "omosense:", err)
		return 1
	}
	return host.Run(ctx)
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
	case "thread":
		return thread.Help
	case "stop":
		return stop.Help
	default:
		return say.Help
	}
}

func usage(w io.Writer) {
	fmt.Fprint(w, `Usage: omosense [<subcommand> [flags]]

Bare omosense runs the session host: one foreground process hosting every
source this folder's .omosense/config.json enables, printing EVENT/CAL/
SOON/MAIL/REMIND/HERDR/RPC/TIDY/LOG lines to stdout.

Subcommands:
  listen    Telegram and Discord listener (EVENT)
  google    calendar and mail watcher (CAL, SOON, MAIL)
  remind    reminder scheduler (REMIND)
  herdr     herdr pane watcher (HERDR)
  rpc       webchat rpc.sock session watcher (RPC)
  tidy      memory tidy watcher (TIDY)
  say       outbound message sender
  thread    register or close a job thread in threads.json
  stop      stop this folder's running host

Run omosense <subcommand> --help for per-subcommand help.
`)
}
