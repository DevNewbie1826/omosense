package remind

import (
	"errors"
	"syscall"
	"time"
)

// awaitExit returns nil once pid has exited. A killed descendant is orphaned
// and reaped by launchd later, so kill(pid, 0) can still succeed on its
// zombie; kqueue NOTE_EXIT fires at exit itself, independent of reaping.
func awaitExit(pid int, timeout time.Duration) error {
	kq, err := syscall.Kqueue()
	if err != nil {
		return err
	}
	defer syscall.Close(kq)
	change := syscall.Kevent_t{Ident: uint64(pid), Filter: syscall.EVFILT_PROC, Flags: syscall.EV_ADD | syscall.EV_ONESHOT, Fflags: syscall.NOTE_EXIT}
	out := make([]syscall.Kevent_t, 1)
	ts := syscall.NsecToTimespec(timeout.Nanoseconds())
	n, err := syscall.Kevent(kq, []syscall.Kevent_t{change}, out, &ts)
	if errors.Is(err, syscall.ESRCH) {
		return nil
	}
	if err != nil {
		return err
	}
	if n == 0 {
		return errors.New("no exit within timeout")
	}
	if out[0].Flags&syscall.EV_ERROR != 0 {
		if syscall.Errno(out[0].Data) == syscall.ESRCH {
			return nil
		}
		return syscall.Errno(out[0].Data)
	}
	return nil
}
