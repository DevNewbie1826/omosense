// Package say sends outbound messages through the Telegram and Discord bot
// APIs and prints the raw platform response JSON with no prefix.
package say

import (
	"fmt"
	"os"

	"github.com/DevNewbie1826/omosense/internal/core"
)

// Help is the usage text printed by omosense say --help.
const Help = `Usage: omosense say <platform> <action> <json> [--profile P]

Sends a message through the platform bot API and prints the response
JSON to stdout. --profile is validated but does not select the bot;
pass {"bot":"name"} in the json to override it.
`

// Run is the compat subcommand host for say.
func Run(ctx *core.Ctx, args []string) int {
	fmt.Fprintln(os.Stderr, "omosense say: not implemented yet")
	return 1
}

// Sources returns nil: say is a sender, not a daemon-hosted source.
func Sources(ctx *core.Ctx) []core.Source {
	return nil
}
