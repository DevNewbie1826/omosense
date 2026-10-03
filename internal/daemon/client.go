package daemon

import (
	"net"
	"sync"
	"time"
)

// A client queues replay and live frames in the same order without holding the
// server mutex across socket I/O. Its writer has bounded I/O and owns closure.
type client struct {
	conn  net.Conn
	mu    sync.Mutex
	queue []any
	wake  chan struct{}
	dead  bool
}

func newClient(c net.Conn) *client { return &client{conn: c, wake: make(chan struct{}, 1)} }

func (c *client) enqueue(v any) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.dead {
		return false
	}
	c.queue = append(c.queue, v)
	select {
	case c.wake <- struct{}{}:
	default:
	}
	return true
}

func (c *client) closed() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.dead
}

func (c *client) close() {
	c.mu.Lock()
	c.dead = true
	c.queue = nil
	c.mu.Unlock()
	c.conn.Close()
	select {
	case c.wake <- struct{}{}:
	default:
	}
}

func (c *client) write() {
	defer c.close()
	for range c.wake {
		c.mu.Lock()
		batch, dead := c.queue, c.dead
		c.queue = nil
		c.mu.Unlock()
		if dead {
			return
		}
		for _, v := range batch {
			c.conn.SetWriteDeadline(time.Now().Add(5 * time.Second))
			if err := writeFrame(c.conn, v); err != nil {
				return
			}
			if f, ok := v.(frame); ok && f.Ctl == "shutdown" {
				return
			}
		}
	}
}
