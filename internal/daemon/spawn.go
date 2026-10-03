package daemon

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"syscall"
	"time"
)

func handshake(ctx context.Context, p paths, h hello) (net.Conn, *framer, string, error) {
	conn, err := (&net.Dialer{}).DialContext(ctx, "unix", p.socket)
	if err != nil {
		return nil, nil, "", err
	}
	stopClose := context.AfterFunc(ctx, func() { conn.Close() })
	defer stopClose()
	deadline := time.Now().Add(time.Second)
	if end, ok := ctx.Deadline(); ok && end.Before(deadline) {
		deadline = end
	}
	conn.SetDeadline(deadline)
	if err := writeFrame(conn, h); err != nil {
		conn.Close()
		return nil, nil, "", err
	}
	reader := newFramer(conn)
	var r reply
	if err := reader.read(&r); err != nil {
		conn.Close()
		return nil, nil, "", err
	}
	if !r.OK {
		conn.Close()
		return nil, nil, "", &rejectedHello{r.Error}
	}
	conn.SetDeadline(time.Time{})
	return conn, reader, r.Version, nil
}

func retryDelay(ctx context.Context, d time.Duration) error {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func acquireSpawn(ctx context.Context, path string) (*os.File, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, err
	}
	notified := false
	for {
		if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err == nil {
			return f, nil
		} else if !errors.Is(err, syscall.EWOULDBLOCK) {
			f.Close()
			return nil, err
		}
		if !notified {
			if err := testNotify("spawn-wait"); err != nil {
				f.Close()
				return nil, err
			}
			notified = true
		}
		if err := retryDelay(ctx, 50*time.Millisecond); err != nil {
			f.Close()
			return nil, err
		}
	}
}

func spawnProcess(p paths, exe string) (*exec.Cmd, <-chan error, error) {
	log, err := os.OpenFile(p.log, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		return nil, nil, err
	}
	defer log.Close()
	null, err := os.Open(os.DevNull)
	if err != nil {
		return nil, nil, err
	}
	defer null.Close()
	cmd := exec.Command(exe, "daemon")
	cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
	cmd.Stdin, cmd.Stdout, cmd.Stderr = null, log, log
	if err := cmd.Start(); err != nil {
		return nil, nil, fmt.Errorf("spawn daemon: %w", err)
	}
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	return cmd, done, nil
}

// ensureDaemon holds the spawn flock from re-dial until a hello proves the
// spawned daemon is ready. The daemon's own lifetime flock is separate.
func ensureDaemon(parent context.Context, p paths, h hello, exe string) (net.Conn, *framer, error) {
	ctx, cancel := context.WithTimeout(parent, 10*time.Second)
	defer cancel()
	acceptDifferent, _ := parent.Value(acceptVersionKey{}).(bool)
	conn, reader, v, err := handshake(ctx, p, h)
	if err == nil && (v == h.Version || acceptDifferent) {
		return conn, reader, nil
	}
	if conn != nil {
		conn.Close()
	}
	var rejected *rejectedHello
	if errors.As(err, &rejected) {
		return nil, nil, err
	}
	lock, err := acquireSpawn(ctx, p.spawn)
	if err != nil {
		return nil, nil, fmt.Errorf("spawn flock: %w", err)
	}
	defer lock.Close()
	var child *exec.Cmd
	var exited <-chan error
	ready, upgraded := false, false
	defer func() {
		if child != nil && !ready {
			child.Process.Kill()
			<-exited
		}
	}()
	backoff := 50 * time.Millisecond
	for {
		conn, reader, v, err = handshake(ctx, p, h)
		if err == nil {
			if v == h.Version || acceptDifferent {
				ready = true
				return conn, reader, nil
			}
			if upgraded {
				conn.Close()
				return nil, nil, fmt.Errorf("daemon version still differs after upgrade")
			}
			upgraded = true
			end, _ := ctx.Deadline()
			conn.SetDeadline(end)
			if err := writeFrame(conn, command{Cmd: "stop", Reason: "upgrade"}); err != nil {
				conn.Close()
				return nil, nil, err
			}
			var f frame
			for {
				err = reader.read(&f)
				if err != nil {
					break
				}
			}
			conn.Close()
			if !errors.Is(err, io.EOF) {
				return nil, nil, fmt.Errorf("wait for upgrade closure: %w", err)
			}
			// EOF can precede lifetime release. Never spawn into that gap.
			lifetime, err := acquireSpawn(ctx, p.lock)
			if err != nil {
				return nil, nil, err
			}
			lifetime.Close()
			continue
		}
		if errors.As(err, &rejected) {
			// A rejection still proves readiness. The daemon must keep its
			// always-on jobs; only this invalid client exits.
			ready = true
			return nil, nil, err
		}
		missing := errors.Is(err, syscall.ENOENT) || errors.Is(err, syscall.ECONNREFUSED)
		if child == nil && missing {
			child, exited, err = spawnProcess(p, exe)
			if err != nil {
				return nil, nil, err
			}
		} else if !missing && !errors.Is(err, io.EOF) {
			return nil, nil, fmt.Errorf("daemon handshake: %w", err)
		}
		if err := retryDelay(ctx, backoff); err != nil {
			return nil, nil, fmt.Errorf("daemon readiness: %w", err)
		}
		backoff = min(backoff*2, time.Second)
	}
}
