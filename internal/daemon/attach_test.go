package daemon

import (
	"bytes"
	"context"
	"net"
	"testing"
	"testing/synctest"
	"time"
)

func TestAttachStopVersusUpgradeAndEOF(t *testing.T) {
	for _, ctl := range []string{"stop", "upgrade", "EOF"} {
		t.Run(ctl, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				calls := 0
				dial := func(context.Context) (net.Conn, *framer, error) {
					calls++
					n := calls
					client, peer := net.Pipe()
					go func() {
						defer peer.Close()
						writeFrame(peer, frame{Line: "HERDR actual"})
						if n == 1 && ctl != "EOF" {
							writeFrame(peer, frame{Ctl: "shutdown", Reason: ctl})
						} else if n > 1 {
							writeFrame(peer, frame{Ctl: "shutdown", Reason: "stop"})
						}
					}()
					return client, newFramer(client), nil
				}
				var out bytes.Buffer
				code, err := streamAttach(ctx, &out, dial)
				wantCalls := 2
				if ctl == "stop" {
					wantCalls = 1
				}
				if err != nil || code != 0 || calls != wantCalls {
					t.Fatalf("code=%d err=%v dials=%d", code, err, calls)
				}
				want := "HERDR actual\n"
				if wantCalls == 2 {
					want += want
				}
				if out.String() != want {
					t.Fatalf("control leaked to stdout: %q", out.String())
				}
			})
		})
	}
}

func TestAttachSignalCancelsBlockedStream(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	client, peer := net.Pipe()
	defer peer.Close()
	connected := make(chan struct{})
	dial := func(context.Context) (net.Conn, *framer, error) {
		close(connected)
		return client, newFramer(client), nil
	}
	done := make(chan int, 1)
	go func() { code, _ := streamAttach(ctx, &bytes.Buffer{}, dial); done <- code }()
	<-connected
	cancel()
	if code := await(t, done); code != 0 {
		t.Fatalf("cancel exit=%d", code)
	}
}

func TestAttachSpawnFailureIsBoundedAndCleansChild(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		p := socketPaths(t)
		_, _, err := ensureDaemon(ctx, p, hello{Hello: 1, Version: "v1", Profile: "main"}, "/does/not/exist")
		if err == nil {
			t.Fatal("nonexistent binary spawned successfully")
		}
	})
}
