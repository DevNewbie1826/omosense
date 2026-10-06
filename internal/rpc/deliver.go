package rpc

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"time"
	"unicode/utf8"

	"github.com/DevNewbie1826/omosense/internal/core"
)

// The batcher's knobs are package-level so tests drive it with an injected
// clock and no real sleeps (the nowFn/deliverPoll pattern).
var (
	deliverPoll        = 30 * time.Second
	deliverRetry       = time.Minute
	deliverExecTimeout = time.Minute
	batchQuiet         = 5 * time.Minute
	batchLimit         = 32 * 1024
	omoExecFn          = omoExec
)

func omoBin() string {
	if bin := os.Getenv("OMOSENSE_OMO"); bin != "" {
		return bin
	}
	return "omo"
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

// configName is the folder's own name, the diagnostics label of this folder's
// watcher.
func configName(c *core.Ctx) string {
	if c.Dir == "" {
		return ""
	}
	return filepath.Base(c.Dir)
}

// batcher turns recorded completions into ONE debounced notification. Every
// Record wakes it; it fires five quiet minutes after the newest un-notified
// done and pushes a single batch to the subscribed session, or drops the batch
// and leaves the entries for `omosense rpc pending` (IS-6..IS-9).
type batcher struct {
	c       *core.Ctx
	store   *pendingStore
	sink    core.Sink
	wake    chan struct{}
	errs    map[string]bool
	retryAt time.Time
}

func newBatcher(c *core.Ctx, store *pendingStore, sink core.Sink) *batcher {
	return &batcher{c: c, store: store, sink: sink, wake: make(chan struct{}, 1), errs: map[string]bool{}}
}

// Notify re-arms the timer without blocking: a coalesced wake is enough, the
// next pass recomputes the quiet window from the pending file.
func (b *batcher) Notify() {
	select {
	case b.wake <- struct{}{}:
	default:
	}
}

func (b *batcher) Run(ctx context.Context) {
	for ctx.Err() == nil {
		b.pass(ctx)
		timer := time.NewTimer(deliverPoll)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-b.wake:
			timer.Stop()
		case <-timer.C:
		}
	}
}

// note logs a message once per process, so a repeated failure does not flood
// the session's stdout.
func (b *batcher) note(msg string) {
	if !b.errs[msg] {
		b.errs[msg] = true
		b.sink.Log(msg)
	}
}

// pass attempts one batch. It is due at retryAt after a failed attempt and at
// five quiet minutes after the newest un-notified done, whichever is later: one
// rule for a live process and for a restart, so a restart with un-notified
// entries fires at max(start, lastDoneAt+5m) (B3), and a done recorded while a
// retry is pending still waits its own quiet window (IS-6).
func (b *batcher) pass(ctx context.Context) {
	entries, err := b.store.Unnotified()
	if err != nil {
		b.note("rpc pending: " + err.Error())
		return
	}
	if len(entries) == 0 {
		b.retryAt = time.Time{}
		return
	}
	now := nowFn()
	due := lastDoneAt(entries).Add(batchQuiet)
	if !b.retryAt.IsZero() && b.retryAt.After(due) {
		due = b.retryAt
	}
	if now.Before(due) {
		return
	}
	b.retryAt = time.Time{}
	sub, err := readSubscription(b.c.State)
	if err != nil {
		// A damaged subscription must not silently swallow the batch.
		b.note("rpc subscription: " + err.Error())
		b.retryAt = now.Add(deliverRetry)
		return
	}
	if sub == nil {
		b.drop(entries, "rpc batch dropped: no subscriber")
		return
	}
	alive, err := subscriberAlive(ctx, sub.Session)
	if err != nil {
		// Only a successful list that lacks the id is "not alive"; an omo
		// outage keeps the subscription and retries (B4).
		b.note("rpc batch list: " + err.Error())
		b.retryAt = now.Add(deliverRetry)
		return
	}
	if !alive {
		removed, err := removeSubscriptionIf(b.c.State, sub)
		if err != nil {
			b.note("rpc subscription: " + err.Error())
			b.retryAt = now.Add(deliverRetry)
			return
		}
		if !removed {
			// The subscription changed while its liveness was being checked:
			// the stale result is discarded, the batch is not consumed, and the
			// next pass re-evaluates the replacement - even when the replacement
			// renews the same session (IS-7).
			b.retryAt = now.Add(deliverRetry)
			return
		}
		b.drop(entries, fmt.Sprintf("rpc batch dropped: subscriber %s not alive; unsubscribed", sub.Session))
		return
	}
	// The liveness answer belongs to the subscription that was examined: if a
	// replacement (or an unsubscribe) landed while `thread list` ran, the batch
	// must not be sent to the old session. Re-read under the subscription lock;
	// a renewal of the same session is a different instance too.
	unchanged, err := subscriptionUnchanged(b.c.State, sub)
	if err != nil {
		b.note("rpc subscription: " + err.Error())
		b.retryAt = now.Add(deliverRetry)
		return
	}
	if !unchanged {
		b.retryAt = now.Add(deliverRetry)
		return
	}
	// The subscriber's own completion is acked, never sent to itself. Its
	// durable id and its session handle both identify the subscriber.
	send := make([]pendingEntry, 0, len(entries))
	for _, e := range entries {
		if e.ID == sub.Session || e.Session == sub.Session {
			if result, err := b.store.Ack(e.ID, e.Seq, true); err != nil {
				b.note("rpc pending: " + err.Error())
			} else if result == "acked" {
				b.sink.Log(fmt.Sprintf("rpc batch: %s is the subscriber; dropped", e.ID))
			}
			continue
		}
		send = append(send, e)
	}
	// notified_seq covers every sequence this attempt consumed; an entry
	// recorded while the send is in flight keeps a greater seq and re-arms.
	maxSeq := entries[len(entries)-1].Seq
	if len(send) == 0 {
		b.markNotified(maxSeq)
		return
	}
	key := fmt.Sprintf("omosense-rpc-batch-%d", maxSeq)
	if _, err := omoExecFn(ctx, "thread", "send", sub.Session, batchText(send, b.c.Dir, b.c.State), "--all-scope", "--idempotency-key", key, "--json"); err != nil {
		b.note("rpc batch send: " + err.Error())
		b.retryAt = now.Add(deliverRetry)
		return
	}
	if b.markNotified(maxSeq) {
		b.sink.Log(fmt.Sprintf("rpc batch sent %d entries (seq <= %d) to %s", len(send), maxSeq, sub.Session))
	} else {
		// The watermark could not be persisted: back off instead of re-sending
		// on every poll (the key keeps a repeat idempotent anyway).
		b.retryAt = now.Add(deliverRetry)
	}
}

// drop abandons the batch: every un-notified entry becomes notified, so a
// dropped batch is never pushed later - the entries stay readable through
// `omosense rpc pending` (IS-7).
func (b *batcher) drop(entries []pendingEntry, msg string) {
	if b.markNotified(entries[len(entries)-1].Seq) {
		b.note(msg)
	}
}

func (b *batcher) markNotified(seq uint64) bool {
	changed, err := b.store.MarkNotified(seq)
	if err != nil {
		b.note("rpc pending: " + err.Error())
		return false
	}
	return changed
}

// lastDoneAt is the newest done_at among the un-notified entries. An
// unparsable timestamp contributes the zero time, so a damaged file notifies
// immediately instead of never.
func lastDoneAt(entries []pendingEntry) time.Time {
	var out time.Time
	for _, e := range entries {
		t, err := time.Parse(time.RFC3339Nano, e.DoneAt)
		if err != nil {
			continue
		}
		if t.After(out) {
			out = t
		}
	}
	return out
}

// subscriberAlive reports whether the subscription names a live omo thread.
// Every failure is returned so the caller retries without unsubscribing.
func subscriberAlive(ctx context.Context, id string) (bool, error) {
	b, err := omoExecFn(ctx, "thread", "list", "--all-scope", "--json")
	if err != nil {
		return false, err
	}
	var threads []struct {
		ThreadID  string `json:"thread_id"`
		SessionID string `json:"sessionId"`
		Alive     bool   `json:"alive"`
	}
	if err := json.Unmarshal(b, &threads); err != nil {
		return false, fmt.Errorf("thread list: %w", err)
	}
	for _, t := range threads {
		if t.Alive && (t.ThreadID == id || t.SessionID == id) {
			return true, nil
		}
	}
	return false, nil
}

// subscription is the folder's single rpc delivery target (IS-7). Instance is
// the record's identity: it is regenerated on every publish, so a renewal of
// the same session is a new instance even when SubscribedAt repeats (core.ISO
// has millisecond resolution).
type subscription struct {
	Session      string `json:"session"`
	SubscribedAt string `json:"subscribed_at"`
	Instance     string `json:"instance"`
}

// newSubscription builds the record `omosense rpc subscribe` publishes. The
// instance token is a fresh 128-bit random value, so every publish - including
// a renewal of the same session id - is distinguishable from the record it
// replaces.
func newSubscription(session string, at time.Time) subscription {
	return subscription{Session: session, SubscribedAt: core.ISO(at), Instance: rand.Text()}
}

func subscriptionPath(stateDir string) string {
	return filepath.Join(stateDir, "rpc-subscription.json")
}

// subscriptionLockPath serializes subscription updates with the batcher's
// compare-and-remove, so a stale liveness result can never delete a
// replacement published while its check was in flight (IS-7).
func subscriptionLockPath(stateDir string) string {
	return filepath.Join(stateDir, "rpc-subscription.lock")
}

// lockSubscription takes the exclusive subscription lock, creating the state
// dir and the lock file when needed.
func lockSubscription(stateDir string) (*os.File, error) {
	if err := os.MkdirAll(stateDir, 0o755); err != nil {
		return nil, err
	}
	f, err := os.OpenFile(subscriptionLockPath(stateDir), os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, err
	}
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX); err != nil {
		f.Close()
		return nil, err
	}
	return f, nil
}

func unlockSubscription(f *os.File) {
	if f == nil {
		return
	}
	syscall.Flock(int(f.Fd()), syscall.LOCK_UN)
	f.Close()
}

// readSubscription returns nil when nothing is subscribed. A malformed file is
// an error so the caller retries rather than dropping the batch.
func readSubscription(stateDir string) (*subscription, error) {
	b, err := os.ReadFile(subscriptionPath(stateDir))
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var s subscription
	if err := json.Unmarshal(b, &s); err != nil {
		return nil, fmt.Errorf("rpc-subscription.json: %w", err)
	}
	if s.Session == "" {
		return nil, errors.New("rpc-subscription.json: no session")
	}
	return &s, nil
}

// writeSubscription publishes the subscription with a temp file and a rename,
// so the batcher never reads a half-written file. It holds the subscription
// lock so it cannot interleave with a compare-and-remove (IS-7).
func writeSubscription(stateDir string, s subscription) error {
	b, err := json.Marshal(s)
	if err != nil {
		return err
	}
	lock, err := lockSubscription(stateDir)
	if err != nil {
		return err
	}
	defer unlockSubscription(lock)
	tmp, err := os.CreateTemp(stateDir, ".rpc-subscription-*")
	if err != nil {
		return err
	}
	defer os.Remove(tmp.Name())
	if _, err = tmp.Write(b); err == nil {
		err = tmp.Sync()
	}
	closeErr := tmp.Close()
	if err != nil {
		return err
	}
	if closeErr != nil {
		return closeErr
	}
	return os.Rename(tmp.Name(), subscriptionPath(stateDir))
}

// sameInstance reports whether cur is the very record examined: the same
// session, the same publish time and the same instance token. Comparing the
// whole record is what keeps a renewal of the same session (unsubscribe then
// subscribe) distinct from the record whose liveness was checked (IS-7).
func sameInstance(cur, examined *subscription) bool {
	return cur != nil && examined != nil &&
		cur.Session == examined.Session &&
		cur.SubscribedAt == examined.SubscribedAt &&
		cur.Instance == examined.Instance
}

// removeSubscriptionIf removes the subscription only when it is still the
// record examined: a subscription replaced while the liveness check was in
// flight - including a renewal of the same session - is left untouched. It
// reports whether a removal happened (IS-7).
func removeSubscriptionIf(stateDir string, examined *subscription) (bool, error) {
	lock, err := lockSubscription(stateDir)
	if err != nil {
		return false, err
	}
	defer unlockSubscription(lock)
	cur, err := readSubscription(stateDir)
	if err != nil {
		return false, err
	}
	if !sameInstance(cur, examined) {
		return false, nil
	}
	if err := os.Remove(subscriptionPath(stateDir)); err != nil && !errors.Is(err, os.ErrNotExist) {
		return false, err
	}
	return true, nil
}

// subscriptionUnchanged reports whether the stored subscription is still the
// record examined, read under the subscription lock so a replacement published
// during the liveness check cannot be sent the old subscriber's batch (IS-7).
func subscriptionUnchanged(stateDir string, examined *subscription) (bool, error) {
	lock, err := lockSubscription(stateDir)
	if err != nil {
		return false, err
	}
	defer unlockSubscription(lock)
	cur, err := readSubscription(stateDir)
	if err != nil {
		return false, err
	}
	return sameInstance(cur, examined), nil
}

// takeSubscription removes and returns the current subscription under the
// subscription lock, so `omosense rpc unsubscribe` cannot interleave with the
// batcher's compare-and-remove.
func takeSubscription(stateDir string) (*subscription, error) {
	lock, err := lockSubscription(stateDir)
	if err != nil {
		return nil, err
	}
	defer unlockSubscription(lock)
	sub, err := readSubscription(stateDir)
	if err != nil {
		return nil, err
	}
	if sub == nil {
		return nil, nil
	}
	if err := os.Remove(subscriptionPath(stateDir)); err != nil && !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}
	return sub, nil
}

// batchText renders the IS-9 batch: one block per entry, bounded to batchLimit
// with a single overflow line naming `omosense rpc pending`. A listed entry's
// ACK command is never truncated.
func batchText(entries []pendingEntry, dir, state string) string {
	absDir, absState := absPath(dir), absPath(state)
	blocks := make([]string, len(entries))
	for i, e := range entries {
		blocks[i] = entryBlock(e, absDir, absState, len(entries))
	}
	total, kept := 0, 0
	for i := range blocks {
		add := len(blocks[i])
		if kept > 0 {
			add++ // the newline joining the previous block
		}
		extra := 0
		if rest := len(blocks) - kept - 1; rest > 0 {
			extra = 1 + len(overflowLine(rest, absDir, absState))
		}
		if total+add+extra > batchLimit {
			break
		}
		total += add
		kept++
	}
	if kept == 0 {
		return overflowLine(len(blocks), absDir, absState)
	}
	out := strings.Join(blocks[:kept], "\n")
	if kept < len(blocks) {
		out += "\n" + overflowLine(len(blocks)-kept, absDir, absState)
	}
	return out
}

func overflowLine(n int, dir, state string) string {
	return fmt.Sprintf("+%d more: OMOSENSE_DIR=%s OMOSENSE_STATE=%s omosense rpc pending", n, shellQuote(dir), shellQuote(state))
}

// ackCommand is the machine-consumed command the batch carries: absolute
// env-pinned paths, so it is valid in every configuration (B1).
func ackCommand(e pendingEntry, dir, state string) string {
	return fmt.Sprintf("OMOSENSE_DIR=%s OMOSENSE_STATE=%s omosense rpc ack %s %d", shellQuote(dir), shellQuote(state), e.ID, e.Seq)
}

// shellQuote renders s as one POSIX shell word: single-quoted, with every
// embedded single quote escaped as '\” so a folder named quote'project still
// yields a command that runs (IS-9/B1).
func shellQuote(s string) string {
	return "'" + strings.ReplaceAll(s, "'", `'\''`) + "'"
}

// entryBlock renders one entry. The display fields share the entry's slice of
// the budget; the metadata and the ACK command are mandatory.
func entryBlock(e pendingEntry, dir, state string, n int) string {
	labels := "작업: \nthread: \ncwd: "
	ack := "확인 명령: " + ackCommand(e, dir, state)
	meta := fmt.Sprintf("완료 id: %s\nseq: %d\ndone_at: %s\ncount: %d", e.ID, e.Seq, e.DoneAt, e.Count)
	display := (batchLimit/n - len(labels) - len(meta) - len(ack) - 2) / 3
	if display < 0 {
		display = 0
	}
	return fmt.Sprintf("작업: %s\nthread: %s\ncwd: %s\n%s\n%s",
		truncField(snapText(e.Name), display), truncField(snapText(e.Thread), display),
		truncField(snapText(e.Cwd), display), meta, ack)
}

// truncField cuts s to at most budget bytes without splitting a rune.
func truncField(s string, budget int) string {
	if budget <= 0 {
		return ""
	}
	if len(s) <= budget {
		return s
	}
	s = s[:budget]
	for !utf8.ValidString(s) {
		s = s[:len(s)-1]
	}
	return s
}

func absPath(p string) string {
	if p == "" {
		return p
	}
	abs, err := filepath.Abs(p)
	if err != nil {
		return p
	}
	return abs
}
