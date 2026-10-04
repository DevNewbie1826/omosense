package core

import (
	"context"
	"os/exec"
	"syscall"
	"time"
)

// SourceCommand owns a process group so cancellation also kills descendants.
// WaitDelay bounds pipe draining if a descendant escapes that group. Callers
// must wait for Run to return before releasing the source lock.
func SourceCommand(ctx context.Context, name string, args ...string) *exec.Cmd {
	cmd := exec.CommandContext(ctx, name, args...)
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	cmd.Cancel = func() error {
		return syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
	}
	cmd.WaitDelay = 2 * time.Second
	return cmd
}
