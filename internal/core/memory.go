package core

import (
	"fmt"
	"os"
	"path/filepath"
)

// MemoryAgentsDir resolves the omosense memory agents directory:
// $OMO_MEMORY_AGENTS, else <home>/.omo/memory/agents. The host's
// memory-repo check (CheckMemory) and tidy's repo discovery share this
// one rule, so they can never disagree (IS-14).
func MemoryAgentsDir() (string, error) {
	if d := os.Getenv("OMO_MEMORY_AGENTS"); d != "" {
		return d, nil
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return "", fmt.Errorf("memory agents dir: %w", err)
	}
	return filepath.Join(home, ".omo", "memory", "agents"), nil
}

// MemoryRepoPath returns <agents>/<id>/repo, the directory a configured
// memory id must have for tidy's self-exclusion to work.
func MemoryRepoPath(id string) (string, error) {
	agents, err := MemoryAgentsDir()
	if err != nil {
		return "", err
	}
	return filepath.Join(agents, id, "repo"), nil
}

// CheckMemory enforces IS-14 at host start (bare `omosense` only;
// subcommands never call it): a configured memory id whose repo
// directory is missing refuses the start naming the key and the checked
// path; a memory unset with tidy enabled logs one warning and continues;
// otherwise there is nothing to check.
func CheckMemory(c *Ctx) error {
	id := c.Profile.Memory
	if id == "" {
		if c.Profile.Tidy.Enabled {
			c.Out.Log("memory is not set; tidy has no own memory repo to skip")
		}
		return nil
	}
	p, err := MemoryRepoPath(id)
	if err != nil {
		return err
	}
	fi, statErr := os.Stat(p)
	if statErr != nil || !fi.IsDir() {
		return fmt.Errorf("config.json: memory %q: repo not found at %s", id, p)
	}
	return nil
}
