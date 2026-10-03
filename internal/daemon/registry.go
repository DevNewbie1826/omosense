package daemon

import "github.com/DevNewbie1826/omosense/internal/core"

// Registry is replaced by the integration node with the real Sources factories.
// Set it before starting a daemon; do not mutate it while a daemon is running.
// Tests can instead pass a registry to newServer without global mutation.
var Registry = func(*core.Ctx) []core.Source { return nil }
