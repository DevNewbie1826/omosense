// Package say sends outbound messages through the Telegram and Discord bot
// APIs and prints the raw platform response JSON with no prefix.
package say

import (
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/DevNewbie1826/omosense/internal/core"
)

// Help is the usage text printed by omosense say --help.
const Help = `Usage: omosense say <platform> <action> <json>

Sends a message through the platform bot API and prints the response
JSON to stdout. The folder's config.json provides the default bot
(<platform>.bot). Pass {"bot":"name"} in the json to override the bot
on either platform (the field is stripped and never sent to the API).
With no configured bot and no override, say exits 2.
`

// Sources returns no host sources because say is a one-shot command.
func Sources(*core.Ctx) []core.Source {
	return nil
}

// client bounds a single one-shot request; bun fetch has no timeout, but a
// hung API must never wedge the remind tick that execs say.
var client = &http.Client{Timeout: 60 * time.Second}

// Run is the compat subcommand host for say. The positional
// platform/action/json are read from ctx.Args (the host parsed the flags).
func Run(ctx *core.Ctx, args []string) int {
	return run(ctx, os.Stdout, os.Stderr)
}

type env struct {
	stdout io.Writer
	stderr io.Writer
	token  string
}

func run(ctx *core.Ctx, stdout, stderr io.Writer) int {
	e := &env{stdout: stdout, stderr: stderr}
	platform, action, raw := positionals(ctx.Args)
	if platform != "telegram" && platform != "discord" {
		fmt.Fprintf(stderr, "unknown %s %s\n", platform, action)
		return 2
	}
	a, err := argsJSON(raw)
	if err != nil {
		return e.fail(err)
	}
	bot := botFor(ctx, a, platform)
	if bot == "" {
		fmt.Fprintf(stderr, "no bot: config.json has no %s.bot; pass {\"bot\":\"name\"} to choose one\n", platform)
		return 2
	}
	if platform == "telegram" {
		return e.telegram(action, a, bot)
	}
	return e.discord(action, a, bot)
}

// botFor resolves the sending bot: the {"bot":"name"} override from the
// args when present, else the folder's configured bot for the platform. The
// override key is deleted from the payload on both platforms so it never
// reaches the API. An empty result means no bot is configured and none was
// overridden.
func botFor(ctx *core.Ctx, a *core.OMap, platform string) string {
	bot := ctx.Profile.Telegram.Bot
	if platform == "discord" {
		bot = ctx.Profile.Discord.Bot
	}
	if v, ok := a.Get("bot"); ok {
		if s, is := v.(string); is {
			bot = s
		}
	}
	a.Delete("bot")
	return bot
}

func positionals(argv []string) (platform, action, raw string) {
	raw = "{}"
	if len(argv) > 0 {
		platform = argv[0]
	}
	if len(argv) > 1 {
		action = argv[1]
	}
	if len(argv) > 2 {
		raw = argv[2]
	}
	return platform, action, raw
}

// argsJSON parses the json-args document; a non-object document behaves as
// an empty args object (say.ts reads undefined from it, same net effect).
func argsJSON(raw string) (*core.OMap, error) {
	v, err := core.ParseJSON([]byte(raw))
	if err != nil {
		return nil, fmt.Errorf("parse json args: %w", err)
	}
	om, _ := v.(*core.OMap)
	if om == nil {
		om = core.NewOMap()
	}
	return om, nil
}

// redactJSON returns the JSON bytes with every occurrence of the bot
// token replaced by "[redacted]". API failure bodies can echo the
// credential-bearing request back (the Telegram /bot<token>/ URL path or
// the Discord Authorization value), and that body is printed verbatim
// (review-1 P1 #2); JSON structure, status and exit code are preserved
// because the token never appears outside string values.
func (e *env) redactJSON(b []byte) []byte {
	if e.token == "" {
		return b
	}
	return []byte(strings.ReplaceAll(string(b), e.token, "[redacted]"))
}

// fail prints an error JSON to stderr and yields exit 1; say.ts crashes on
// these paths (unhandled rejection), the port reports the failure instead
// (plan IS-8: exit 1 on network/JSON errors).
func (e *env) fail(err error) int {
	m := core.NewOMap()
	message := err.Error()
	if e.token != "" {
		message = strings.ReplaceAll(message, e.token, "[redacted]")
	}
	m.Set("error", message)
	b, mErr := m.Marshal()
	if mErr != nil {
		b = []byte(`{"error":"unprintable failure"}`)
	}
	fmt.Fprintln(e.stderr, string(b))
	return 1
}
