// Package rpc watches webchat sessions through rpc.sock events and snapshots.
package rpc

import (
	"context"
	"fmt"
	"os"
	"strconv"

	"github.com/DevNewbie1826/omosense/internal/core"
)

// Help is the compat subcommand's usage.
const Help = `Usage: omosense rpc [--profile P] [--once] [--all]
       omosense rpc ack <id> [<seq>] [--profile P]
       omosense rpc pending [--profile P]

Watches registered webchat sessions through rpc.sock push events plus 5-second
snapshots, printing RPC and LOG lines. Each done is kept in
rpc-pending-<profile>.json and sent to the profile's agent with omo thread send,
then re-sent every 10 minutes until acked.

Commands:
  ack       clear a pending completion by durable id; a positive seq leaves a
            newer completion of the same session pending
  pending   print pending completions ordered by seq
  Both work without a daemon or rpc socket and never take the watch lock.

Flags:
  --once    read-only single snapshot: prints SNAP lines, no lock, no delivery
  --all     watch all sessions, not only threads.json registrations
`

// Run hosts the compat watcher; snapshots never acquire a lock.
func Run(c *core.Ctx, args []string) int {
	_ = args
	if len(c.Args) > 0 {
		switch c.Args[0] {
		case "ack":
			return ack(c)
		case "pending":
			return pending(c)
		}
	}
	w := newWatcher(c, c.Out)
	if c.Flags["--once"] {
		if err := w.once(context.Background()); err != nil {
			c.Out.Log("rpc " + err.Error())
			return 1
		}
		return 0
	}
	release := c.Acquire("watch-rpc-"+c.Profile.Name, "")
	defer release()
	if err := run(context.Background(), c, c.Out, w); err != nil {
		c.Out.Log("rpc " + err.Error())
		return 1
	}
	return 0
}

func ack(c *core.Ctx) int {
	if len(c.Args) < 2 || len(c.Args) > 3 || c.Args[1] == "" {
		fmt.Fprint(os.Stderr, Help)
		return 2
	}
	var seq *uint64
	if len(c.Args) == 3 {
		n, err := strconv.ParseUint(c.Args[2], 10, 64)
		if err != nil || n == 0 {
			fmt.Fprint(os.Stderr, Help)
			return 2
		}
		seq = &n
	}
	var n uint64
	if seq != nil {
		n = *seq
	}
	result, err := newPendingStore(c.State, c.Profile.Name).Ack(c.Args[1], n, seq != nil)
	if err != nil {
		fmt.Fprintln(os.Stderr, "omosense: rpc ack:", err)
		return 1
	}
	c.Out.Emit("ACK", struct {
		ID     string  `json:"id"`
		Seq    *uint64 `json:"seq"`
		Result string  `json:"result"`
	}{c.Args[1], seq, result})
	return 0
}

func pending(c *core.Ctx) int {
	if len(c.Args) != 1 {
		fmt.Fprint(os.Stderr, Help)
		return 2
	}
	entries, err := newPendingStore(c.State, c.Profile.Name).List()
	if err != nil {
		fmt.Fprintln(os.Stderr, "omosense: rpc pending:", err)
		return 1
	}
	for _, e := range entries {
		c.Out.Emit("PENDING", e)
	}
	return 0
}

// Sources supplies the pause-while-idle source. The daemon owns its lock.
func Sources(c *core.Ctx) []core.Source {
	return []core.Source{src{c: c}}
}

type src struct{ c *core.Ctx }

func (s src) Name() string               { return "rpc" }
func (s src) Prefixes() []string         { return []string{"RPC"} }
func (s src) AlwaysOn() bool             { return false }
func (s src) LockName() (string, string) { return "watch-rpc-" + s.c.Profile.Name, "" }
func (s src) Run(ctx context.Context, sink core.Sink) error {
	return run(ctx, s.c, sink, newWatcher(s.c, sink))
}

func run(ctx context.Context, c *core.Ctx, sink core.Sink, w *watcher) error {
	store := newPendingStore(c.State, c.Profile.Name)
	d := newDeliverer(c, store, sink)
	w.record = func(id string, ev rpcEvent) {
		e, err := store.Record(id, ev)
		if err != nil {
			sink.Log("rpc pending " + err.Error())
			return
		}
		sink.Log(fmt.Sprintf("rpc pending %s seq %d", id, e.Seq))
		d.Notify()
	}
	ctx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() {
		defer close(done)
		d.Run(ctx)
	}()
	err := w.run(ctx)
	cancel()
	<-done
	return err
}
