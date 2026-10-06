package rpc

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/DevNewbie1826/omosense/internal/core"
)

var (
	deliverPoll        = 30 * time.Second
	deliverRemind      = 10 * time.Minute
	deliverRetry       = time.Minute
	deliverExecTimeout = time.Minute
)

func omoBin() string {
	if bin := os.Getenv("OMOSENSE_OMO"); bin != "" {
		return bin
	}
	return "omo"
}

type deliverer struct {
	c     *core.Ctx
	store *pendingStore
	sink  core.Sink
	wake  chan struct{}
	errs  map[string]bool
}

func newDeliverer(c *core.Ctx, store *pendingStore, sink core.Sink) *deliverer {
	return &deliverer{c: c, store: store, sink: sink, wake: make(chan struct{}, 1), errs: map[string]bool{}}
}

func (d *deliverer) Notify() {
	select {
	case d.wake <- struct{}{}:
	default:
	}
}

func (d *deliverer) Run(ctx context.Context) {
	for ctx.Err() == nil {
		d.pass(ctx)
		timer := time.NewTimer(deliverPoll)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-d.wake:
			timer.Stop()
		case <-timer.C:
		}
	}
}

func (d *deliverer) note(msg string) {
	if !d.errs[msg] {
		d.errs[msg] = true
		d.sink.Log(msg)
	}
}

func omoExec(ctx context.Context, args ...string) ([]byte, error) {
	ctx, cancel := context.WithTimeout(ctx, deliverExecTimeout)
	defer cancel()
	cmd := exec.CommandContext(ctx, omoBin(), args...)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	out, err := cmd.Output()
	if err != nil {
		msg := strings.TrimSpace(stderr.String())
		if msg == "" {
			msg = err.Error()
		}
		return nil, fmt.Errorf("%s", msg)
	}
	return out, nil
}

func (d *deliverer) target(ctx context.Context) (string, error) {
	// The profile's rpc.session pins the delivery target; empty falls
	// back to the webchat sessions.json of the state dir.
	if id := d.c.Profile.RPC.Session; id != "" {
		return id, nil
	}
	b, err := os.ReadFile(filepath.Join(d.c.State, "sessions.json"))
	if err != nil {
		return "", err
	}
	var profiles map[string]struct {
		SessionID string `json:"session_id"`
		Cwd       string `json:"cwd"`
	}
	if err := json.Unmarshal(b, &profiles); err != nil {
		return "", err
	}
	p := profiles[d.c.Profile.Name]
	if p.SessionID != "" {
		return p.SessionID, nil
	}
	if p.Cwd == "" {
		return "", fmt.Errorf("profile %s has no cwd or session_id", d.c.Profile.Name)
	}
	b, err = omoExec(ctx, "thread", "list", "--all-scope", "--json")
	if err != nil {
		return "", err
	}
	var threads []struct {
		ThreadID  string `json:"thread_id"`
		SessionID string `json:"sessionId"`
		Alive     bool   `json:"alive"`
		Kind      string `json:"kind"`
		Surface   string `json:"surface"`
		Cwd       string `json:"cwd"`
	}
	if err := json.Unmarshal(b, &threads); err != nil {
		return "", err
	}
	var candidates []string
	for _, t := range threads {
		if t.Alive && t.Kind == "interactive" && t.Surface == "tui" && t.Cwd != "" &&
			filepath.Clean(t.Cwd) == filepath.Clean(p.Cwd) {
			id := t.ThreadID
			if id == "" {
				id = t.SessionID
			}
			if id != "" {
				candidates = append(candidates, id)
			}
		}
	}
	if len(candidates) != 1 {
		return "", fmt.Errorf("profile %s has %d cwd candidates", d.c.Profile.Name, len(candidates))
	}
	return candidates[0], nil
}

func (d *deliverer) pass(ctx context.Context) {
	entries, err := d.store.List()
	if err != nil {
		d.note("rpc pending: " + err.Error())
		return
	}
	if len(entries) == 0 || ctx.Err() != nil {
		return
	}
	target, err := d.target(ctx)
	if err != nil {
		d.note("rpc deliver target: " + err.Error())
		return
	}
	for _, e := range entries {
		if ctx.Err() != nil {
			return
		}
		next, _ := time.Parse(time.RFC3339Nano, e.NextAt)
		if e.Attempts != 0 && nowFn().Before(next) {
			continue
		}
		if e.ID == target {
			result, err := d.store.Ack(e.ID, e.Seq, true)
			if err != nil {
				d.note("rpc pending: " + err.Error())
			} else if result == "acked" {
				d.sink.Log(fmt.Sprintf("rpc pending %s is the delivery target; dropped", e.ID))
			}
			continue
		}
		key := fmt.Sprintf("omosense-rpc-%s-%s-%d-%d", d.store.profile, e.ID, e.Seq, e.Attempts+1)
		_, sendErr := omoExec(ctx, "thread", "send", target, deliveryText(e, d.store.profile),
			"--all-scope", "--idempotency-key", key, "--json")
		applied, err := d.store.update(e, func(current *pendingEntry) {
			now := nowFn()
			current.Attempts++
			if sendErr == nil {
				current.DeliveredAt = core.ISO(now)
				current.NextAt = core.ISO(now.Add(deliverRemind))
				current.LastError = ""
			} else {
				current.NextAt = core.ISO(now.Add(deliverRetry))
				current.LastError = strings.TrimSpace(sendErr.Error())
			}
		})
		if err != nil {
			d.note("rpc pending: " + err.Error())
		} else if applied {
			if sendErr == nil {
				d.sink.Log(fmt.Sprintf("rpc delivered %s seq %d attempt %d", e.ID, e.Seq, e.Attempts+1))
			} else {
				d.note(fmt.Sprintf("rpc deliver %s: %s", e.ID, sendErr))
			}
		}
	}
}

func deliveryText(e pendingEntry, profile string) string {
	// Reserve the mandatory metadata and ACK command before bounding the
	// display fields; truncation must never cut off the machine-consumed ACK.
	tail := fmt.Sprintf("\n완료 id: %s\nseq: %d\ndone_at: %s\ncount: %d\n확인 명령: omosense rpc ack %s %d --profile %s\n확인할 때까지 10분마다 다시 알립니다.",
		e.ID, e.Seq, e.DoneAt, e.Count, e.ID, e.Seq, profile)
	fields := []*string{e.Name, e.Thread, e.Cwd}
	values := make([]string, len(fields))
	budget := (32768 - len(tail) - len("작업: \nthread: \ncwd: ")) / len(fields)
	for i, field := range fields {
		values[i] = snapText(field)
		if len(values[i]) > budget {
			values[i] = values[i][:budget]
			for !utf8.ValidString(values[i]) {
				values[i] = values[i][:len(values[i])-1]
			}
		}
	}
	return fmt.Sprintf("작업: %s\nthread: %s\ncwd: %s%s", values[0], values[1], values[2], tail)
}
