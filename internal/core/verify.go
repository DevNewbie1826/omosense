package core

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"time"
)

// VerifyResult is the outcome of a done-verification hook run: Status is
// "verified" (exit 0), "unverified" (failed both attempts or never
// started), or "cancelled" (the caller's ctx ended); Detail carries the
// unverified reason.
type VerifyResult struct {
	Status string
	Detail string
}

// RunVerify runs command with a per-attempt timeout: exit 0 verifies,
// anything else gets exactly one retry (two attempts ever). A cancelled
// ctx returns cancelled with no retry, the running process killed;
// WaitDelay bounds the return even when an escaped child holds the
// pipes. env is appended to os.Environ(); dir becomes the working
// directory when it is an existing directory, else it is inherited.
func RunVerify(ctx context.Context, command []string, timeout time.Duration, env []string, dir string) VerifyResult {
	var res VerifyResult
	for attempt := 0; attempt < 2; attempt++ {
		res = runVerifyOnce(ctx, command, timeout, env, dir)
		if res.Status != "unverified" {
			return res
		}
		if ctx.Err() != nil {
			return VerifyResult{Status: "cancelled"}
		}
	}
	return res
}

func runVerifyOnce(ctx context.Context, command []string, timeout time.Duration, env []string, dir string) VerifyResult {
	cctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	cmd := exec.CommandContext(cctx, command[0], command[1:]...)
	cmd.Env = append(os.Environ(), env...)
	if fi, err := os.Stat(dir); err == nil && fi.IsDir() {
		cmd.Dir = dir
	}
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	cmd.WaitDelay = 2 * time.Second
	err := cmd.Run()
	if err == nil {
		return VerifyResult{Status: "verified"}
	}
	if ctx.Err() != nil {
		return VerifyResult{Status: "cancelled"}
	}
	tail := Trunc(strings.TrimSpace(stderr.String()), 200)
	var ee *exec.ExitError
	if errors.As(err, &ee) {
		if cctx.Err() != nil {
			return VerifyResult{Status: "unverified", Detail: verifyDetail("timeout", tail)}
		}
		return VerifyResult{Status: "unverified", Detail: verifyDetail(fmt.Sprintf("exit %d", ee.ExitCode()), tail)}
	}
	return VerifyResult{Status: "unverified", Detail: verifyDetail("start error", err.Error())}
}

// verifyDetail renders "<kind>: <text>", dropping the ": <text>" part
// when text is empty (for example a hook that fails with no stderr).
func verifyDetail(kind, text string) string {
	if text == "" {
		return kind
	}
	return kind + ": " + text
}
