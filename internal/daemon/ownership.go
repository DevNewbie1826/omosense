package daemon

import (
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"
)

type paths struct {
	dir, socket, spawn, lock, pid, log string
}

func pathsFor(dir, socket string) paths {
	if socket == "" {
		socket = filepath.Join(dir, "omosense.sock")
	}
	return paths{
		dir: dir, socket: socket,
		spawn: filepath.Join(dir, "omosense.spawn.lock"),
		lock:  filepath.Join(dir, "omosense.daemon.lock"),
		pid:   filepath.Join(dir, "omosense-daemon.pid"),
		log:   filepath.Join(dir, "omosense-daemon.log"),
	}
}

type alreadyRunningError struct{ PID int }

func (e *alreadyRunningError) Error() string {
	return fmt.Sprintf("omosense daemon already running (pid %d)", e.PID)
}

type lifetime struct {
	paths
	lockFile *os.File
	listener *net.UnixListener
	socketID os.FileInfo
}

func acquireLifetime(p paths) (*lifetime, error) {
	f, err := os.OpenFile(p.lock, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, fmt.Errorf("daemon lock: %w", err)
	}
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		f.Close()
		if errors.Is(err, syscall.EWOULDBLOCK) {
			b, _ := os.ReadFile(p.pid)
			pid, _ := strconv.Atoi(strings.TrimSpace(string(b)))
			return nil, &alreadyRunningError{PID: pid}
		}
		return nil, fmt.Errorf("daemon flock: %w", err)
	}
	l := &lifetime{paths: p, lockFile: f}
	if err := os.WriteFile(p.pid, []byte(strconv.Itoa(os.Getpid())+"\n"), 0o600); err != nil {
		l.close()
		return nil, fmt.Errorf("daemon pid: %w", err)
	}
	return l, nil
}

func (l *lifetime) bind() (*net.UnixListener, error) {
	fi, err := os.Lstat(l.socket)
	if err == nil {
		if fi.Mode()&os.ModeSocket == 0 {
			return nil, fmt.Errorf("refusing non-socket path %s", l.socket)
		}
		c, dialErr := net.DialTimeout("unix", l.socket, time.Second)
		if dialErr == nil {
			c.Close()
			return nil, fmt.Errorf("socket already serves a live endpoint")
		}
		if !errors.Is(dialErr, syscall.ECONNREFUSED) && !errors.Is(dialErr, syscall.ENOENT) {
			return nil, fmt.Errorf("probe stale socket: %w", dialErr)
		}
		if err := os.Remove(l.socket); err != nil && !os.IsNotExist(err) {
			return nil, fmt.Errorf("remove stale socket: %w", err)
		}
	} else if !os.IsNotExist(err) {
		return nil, fmt.Errorf("stat socket: %w", err)
	}
	ln, err := net.ListenUnix("unix", &net.UnixAddr{Name: l.socket, Net: "unix"})
	if err != nil {
		return nil, fmt.Errorf("bind socket: %w", err)
	}
	ln.SetUnlinkOnClose(false)
	l.listener = ln
	l.socketID, err = os.Stat(l.socket)
	if err != nil {
		return nil, fmt.Errorf("socket identity: %w", err)
	}
	if err := os.Chmod(l.socket, 0o600); err != nil {
		return nil, fmt.Errorf("socket permissions: %w", err)
	}
	return ln, nil
}

func (l *lifetime) close() error {
	if l.lockFile == nil {
		return nil
	}
	var errs []error
	if l.listener != nil {
		if err := l.listener.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
			errs = append(errs, err)
		}
	}
	if l.socketID != nil {
		if current, err := os.Stat(l.socket); err == nil && os.SameFile(l.socketID, current) {
			if err := os.Remove(l.socket); err != nil && !os.IsNotExist(err) {
				errs = append(errs, err)
			}
		} else if err != nil && !os.IsNotExist(err) {
			errs = append(errs, err)
		}
	}
	if err := os.Remove(l.pid); err != nil && !os.IsNotExist(err) {
		errs = append(errs, err)
	}
	errs = append(errs, l.lockFile.Close()) // Closing the descriptor releases flock.
	l.lockFile = nil
	return errors.Join(errs...)
}
