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

// RunSource runs a SourceCommand and then kills whatever is left of its
// process group, so a descendant outliving its parent cannot survive the
// source that started it.
func RunSource(cmd *exec.Cmd) error {
	err := cmd.Run()
	if cmd.Process != nil {
		_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
	}
	return err
}
