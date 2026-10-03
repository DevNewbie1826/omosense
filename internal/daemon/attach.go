package daemon

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
)

type rejectedHello struct{ message string }

func (e *rejectedHello) Error() string { return e.message }

// An upgrade notice is permission to reconnect to the replacement version.
// Older peers must not immediately downgrade it to their own binary.
type acceptVersionKey struct{}

func streamAttach(ctx context.Context, out io.Writer, dial func(context.Context) (net.Conn, *framer, error)) (int, error) {
	for {
		conn, reader, err := dial(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return 0, nil
			}
			var rejected *rejectedHello
			if errors.As(err, &rejected) {
				return 2, err
			}
			return 1, err
		}
		stopClose := context.AfterFunc(ctx, func() { conn.Close() })
		reason := ""
		for {
			var f frame
			if err = reader.read(&f); err != nil {
				break
			}
			if f.Ctl == "shutdown" {
				reason = f.Reason
				break
			}
			if f.Line != "" {
				if _, err := fmt.Fprintln(out, f.Line); err != nil {
					stopClose()
					conn.Close()
					return 1, fmt.Errorf("attach stdout: %w", err)
				}
			}
		}
		stopClose()
		conn.Close()
		if ctx.Err() != nil || reason == "stop" {
			return 0, nil
		}
		if reason == "upgrade" {
			ctx = context.WithValue(ctx, acceptVersionKey{}, true)
		}
		// EOF without ctl is unexpected death, not permission to exit.
	}
}
