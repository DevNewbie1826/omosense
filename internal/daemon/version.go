package daemon

import (
	"crypto/sha256"
	"fmt"
	"io"
	"os"
	"runtime/debug"
)

// Version identifies the build, including uncommitted edits. Without VCS build
// metadata it identifies the actual executable bytes, not a package constant.
func Version() (string, error) {
	info, _ := debug.ReadBuildInfo()
	exe, err := os.Executable()
	if err != nil {
		return "", fmt.Errorf("executable: %w", err)
	}
	return buildVersion(info, exe)
}

func buildVersion(info *debug.BuildInfo, exe string) (string, error) {
	revision, modified := "", false
	if info != nil {
		for _, s := range info.Settings {
			switch s.Key {
			case "vcs.revision":
				revision = s.Value
			case "vcs.modified":
				modified = s.Value == "true"
			}
		}
	}
	if revision != "" {
		if modified {
			revision += "+modified"
		}
		return revision, nil
	}
	f, err := os.Open(exe)
	if err != nil {
		return "", fmt.Errorf("open executable: %w", err)
	}
	defer f.Close()
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return "", fmt.Errorf("hash executable: %w", err)
	}
	return fmt.Sprintf("%x", h.Sum(nil)), nil
}
