package tidy

import (
	"bytes"
	"errors"
	"os/exec"
)

// runGit runs git, reporting a non-zero exit as code without an error and
// a spawn-level failure as an "Error: ..." (which the TS $ shell throws,
// so callers treat it as an abort).
func runGit(args ...string) (stdout, stderr string, code int, err error) {
	cmd := exec.Command("git", args...)
	var out, errb bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &errb
	if err := cmd.Run(); err != nil {
		var ee *exec.ExitError
		if errors.As(err, &ee) {
			return out.String(), errb.String(), ee.ExitCode(), nil
		}
		return "", "", 0, errors.New("Error: " + err.Error())
	}
	return out.String(), errb.String(), 0, nil
}
