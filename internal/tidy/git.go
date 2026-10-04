package tidy

import (
	"bytes"
	"context"
	"errors"
	"os/exec"

	"github.com/DevNewbie1826/omosense/internal/core"
)

// runGit runs git, reporting a non-zero exit as code without an error and
// a spawn-level failure as an "Error: ..." (which the TS $ shell throws,
// so callers treat it as an abort).
func runGit(ctx context.Context, args ...string) (stdout, stderr string, code int, err error) {
	cmd := core.SourceCommand(ctx, "git", args...)
	var out, errb bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &errb
	if err := core.RunSource(cmd); err != nil {
		if ctx.Err() != nil {
			return out.String(), errb.String(), 0, ctx.Err()
		}
		var ee *exec.ExitError
		if errors.As(err, &ee) {
			return out.String(), errb.String(), ee.ExitCode(), nil
		}
		return "", "", 0, errors.New("Error: " + err.Error())
	}
	return out.String(), errb.String(), 0, nil
}
