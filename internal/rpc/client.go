package rpc

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"time"
)

const maxLine = 8 << 20
const callTimeout = 10 * time.Second

type commandError string

func (e commandError) Error() string { return string(e) }

type client struct {
	conn net.Conn
	scan *bufio.Scanner
	next uint64
	stop func() bool
}

func dial(ctx context.Context, path string) (*client, error) {
	dctx, cancel := context.WithTimeout(ctx, callTimeout)
	defer cancel()
	conn, err := (&net.Dialer{}).DialContext(dctx, "unix", path)
	if err != nil {
		return nil, err
	}
	scan := bufio.NewScanner(conn)
	scan.Buffer(make([]byte, 4096), maxLine+1)
	return &client{conn: conn, scan: scan, stop: context.AfterFunc(ctx, func() { conn.Close() })}, nil
}

func (c *client) close() {
	c.stop()
	c.conn.Close()
}

// call is sequential on a tick-local connection. Only read-only command
// shapes are emitted, and unrelated response IDs/events never settle a call.
func (c *client) call(ctx context.Context, command, session string) (json.RawMessage, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if command != "list_sessions" && command != "get_state" {
		return nil, fmt.Errorf("unsupported RPC command %q", command)
	}
	deadline := time.Now().Add(callTimeout)
	if d, ok := ctx.Deadline(); ok && d.Before(deadline) {
		deadline = d
	}
	if err := c.conn.SetDeadline(deadline); err != nil {
		return nil, err
	}
	stop := context.AfterFunc(ctx, func() { c.conn.Close() })
	defer stop()
	c.next++
	id := strconv.FormatUint(c.next, 10)
	req := struct {
		ID      string `json:"id"`
		Type    string `json:"type"`
		Session string `json:"sessionId,omitempty"`
	}{id, command, session}
	if err := json.NewEncoder(c.conn).Encode(req); err != nil {
		return nil, err
	}
	for c.scan.Scan() {
		if len(c.scan.Bytes()) > maxLine {
			return nil, errors.New("RPC frame exceeds 8 MiB")
		}
		var response struct {
			ID      string          `json:"id"`
			Type    string          `json:"type"`
			Success bool            `json:"success"`
			Error   string          `json:"error"`
			Data    json.RawMessage `json:"data"`
		}
		if err := json.Unmarshal(c.scan.Bytes(), &response); err != nil {
			return nil, err
		}
		if response.Type != "response" || response.ID != id {
			continue
		}
		if !response.Success {
			return nil, commandError(response.Error)
		}
		return response.Data, nil
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := c.scan.Err(); err != nil {
		return nil, err
	}
	return nil, io.EOF
}
