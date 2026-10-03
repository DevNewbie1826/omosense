package daemon

import (
	"github.com/DevNewbie1826/omosense/internal/core"
	"github.com/DevNewbie1826/omosense/internal/google"
	"github.com/DevNewbie1826/omosense/internal/herdr"
	"github.com/DevNewbie1826/omosense/internal/listen"
	"github.com/DevNewbie1826/omosense/internal/remind"
	"github.com/DevNewbie1826/omosense/internal/tidy"
)

// Registry returns the resident sources of one profile: listen (telegram,
// plus discord when the profile enables it), google, remind, herdr and tidy.
// say is a one-shot sender, not a resident source, so it is not registered.
// Set it before starting a daemon; do not mutate it while a daemon is running.
// Tests can instead pass a registry to newServer without global mutation.
var Registry = profileSources

func profileSources(c *core.Ctx) []core.Source {
	out := make([]core.Source, 0, 6)
	out = append(out, listen.Sources(c)...)
	out = append(out, google.Sources(c)...)
	out = append(out, remind.Sources(c)...)
	out = append(out, herdr.Sources(c)...)
	out = append(out, tidy.Sources(c)...)
	return out
}
