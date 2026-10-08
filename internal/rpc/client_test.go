package rpc

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"net"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

func transportServer(t *testing.T, reply func(net.Conn, string)) string {
	t.Helper()
	path := shortSocket(t)
	ln, err := net.Listen("unix", path)
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	var conn net.Conn
	accepted := make(chan struct{})
	go func() {
		defer close(done)
		var err error
		conn, err = ln.Accept()
		close(accepted)
		if err != nil {
			return
		}
		defer conn.Close()
		sc := bufio.NewScanner(conn)
		for sc.Scan() {
			var req struct {
				ID string `json:"id"`
			}
			if json.Unmarshal(sc.Bytes(), &req) != nil {
				return
			}
			reply(conn, req.ID)
		}
		if err := sc.Err(); err != nil && !errors.Is(err, net.ErrClosed) {
			t.Errorf("transport server scan: %v", err)
		}
	}()
	t.Cleanup(func() {
		ln.Close()
		<-accepted
		if conn != nil {
			conn.Close()
		}
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Error("transport server did not stop")
		}
	})
	return path
}

func TestClientCancellation(t *testing.T) {
	received := make(chan struct{})
	path := transportServer(t, func(conn net.Conn, id string) { close(received) })
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c, err := dial(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	defer c.close()
	done := make(chan error, 1)
	go func() {
		_, err := c.call(ctx, "list_sessions", "")
		done <- err
	}()
	select {
	case <-received:
	case <-done:
		t.Fatal("call returned without sending request")
	case <-time.After(5 * time.Second):
		t.Fatal("request not received")
	}
	cancel()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("canceled call succeeded")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("cancellation failed to unblock socket")
	}
}

func TestClientFrameLimitAndFragmentation(t *testing.T) {
	for _, size := range []int{5 << 20, 8 << 20} {
		t.Run(strconv.Itoa(size/(1<<20))+"MiB", func(t *testing.T) {
			path := transportServer(t, func(conn net.Conn, id string) {
				b, _ := json.Marshal(map[string]any{"id": id, "type": "response", "success": true, "data": map[string]any{"text": strings.Repeat("x", size)}})
				// Two writes also exercise fragmented NDJSON frames.
				conn.Write(b[:len(b)/2])
				conn.Write(append(b[len(b)/2:], '\n'))
			})
			c, err := dial(context.Background(), path)
			if err != nil {
				t.Fatal(err)
			}
			defer c.close()
			data, err := c.call(context.Background(), "list_sessions", "")
			if size == 8<<20 {
				if err == nil {
					t.Fatal("oversized frame accepted")
				}
			} else {
				var got struct {
					Text string `json:"text"`
				}
				if err != nil || json.Unmarshal(data, &got) != nil || len(got.Text) != size {
					t.Fatalf("large fragmented frame failed: length %d, err %v", len(got.Text), err)
				}
			}
		})
	}
}

func TestClientRequestIDsAndDeadline(t *testing.T) {
	var ids []string
	var mu sync.Mutex
	path := transportServer(t, func(conn net.Conn, id string) {
		mu.Lock()
		ids = append(ids, id)
		mu.Unlock()
		json.NewEncoder(conn).Encode(map[string]any{"type": "response", "id": id, "success": true, "data": map[string]any{}})
	})
	c, err := dial(context.Background(), path)
	if err != nil {
		t.Fatal(err)
	}
	defer c.close()
	for i := 0; i < 2; i++ {
		if _, err := c.call(context.Background(), "list_sessions", ""); err != nil {
			t.Fatal(err)
		}
	}
	c.close()
	mu.Lock()
	defer mu.Unlock()
	if len(ids) != 2 || ids[0] == "" || ids[0] == ids[1] {
		t.Fatalf("request IDs = %v", ids)
	}
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancel()
	if _, err := dial(ctx, path); err == nil {
		t.Fatal("expired dial context accepted")
	}
}
