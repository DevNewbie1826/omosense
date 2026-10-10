//go:build !darwin

package remind

import (
	"errors"
	"syscall"
	"time"
)

func awaitExit(pid int, _ time.Duration) error {
	if err := syscall.Kill(pid, 0); !errors.Is(err, syscall.ESRCH) {
		return errors.New("process still exists")
	}
	return nil
}
