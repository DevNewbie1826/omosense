# rpc watcher: completion ordering invariants

This is the proof behind the watcher's reconnect/poll ordering (`watcher.go` `poll`, `stream.go`).
It replaces the case-by-case fixes 0491cd9, f844703 and 1e6cf72 with one rule.
Every ordering case below is derived from that rule, and each has a regression test in `stream_test.go`.
Plan: `.omo/plans/omosense-rpc-done-pending.md` (Revision 4 is amended by I3b below, by owner decision).

## The rule

> **Newest observation wins, per key, in arrival order. A list's silence about a handle never discards a known completion. While a connection is live, its stream owns every turn binding.**

The single owner applies every observation exactly once, in arrival order. An observation replaces only the state of the keys it observes, and only when it is newer than the observation that set that state.

**Keys**

- `w.seen[id]`: record activity per durable id (active, epochs, status).
- `w.turns[H]`: turn binding per routing handle, meaning the identity to which H's next settle is attributed.

**Observations**

- Stream items, in reader FIFO order:
  - `agent_start` plus its FIFO lookup response
  - `agent_settled`
  - `session_closed` and `session_parked`
  - connect (`up`) and connection end (`down`)
- Poll snapshots: list plus get_state on a tick-local connection. A reconciliation is the snapshot a connect runs.

**What a list observes**

- It observes which sessions exist, so a session absent from a successful list is closed (today's poll rule).
- It does not observe which turn a queued settle belongs to, so absence never removes a turn binding, and never removes a deferred completion either.

## Invariants

### I1 Arrival order: no observation is dropped or reordered

- (a) Stream items apply in FIFO order.
- (b) A snapshot is positioned at its request.
  - Items read before the request apply before it.
  - A connect read after the request applies after the snapshot. It is requeued at the FIFO head and never dropped.
  - The snapshot is therefore evaluated with the connection state in force during its I/O.
- (c) A connect's reconciliation is positioned at the connect. It applies before any item read after the connect, because drains nested inside it are no-ops.
- (d) Items read during a snapshot's I/O that precede the next connect apply before the snapshot. Per I2 they are newer than it for the keys they touch.

### I2 Records: newest wins per durable id

A snapshot never arms, concludes, re-statuses or erases a record that a stream item touched after the snapshot's request (`record.streamSeq`).

### I3 Turn bindings: newest wins per handle, only while no live connection owns the handle

- **(a) The live connection owns bindings.** While the stream is up, only these change a binding:
  - its stream items: `agent_start` with its FIFO lookup sets it, and settle or close consumes it;
  - its own reconciliation (b).

  Ordinary polls never change a binding (plan Revision 4). That includes a binding its reconciliation kept under (c): that turn's settle can still arrive on the live connection.
- **(b) When no live connection owns bindings, a successful poll observes every handle it lists.** This covers a reconciliation (its connection has delivered nothing yet, I1c) and any poll applied while the stream is down. The only exception is a handle whose binding a stream item set after this poll's request (`streamTurn.seq`; I2 applied to bindings). For each observed handle:
  - a valid in-progress observation (working or blocked) binds H to it;
  - a valid idle observation removes H's binding;
  - a different listed identity removes the old binding, because the host never replaces a session mid-turn, so the old turn has ended;
  - an unwatched or invalid observation of the same identity leaves the binding.

  "In progress" means "not idle". That is exactly I4's notion of a turn end, where only idle concludes a record.
- **(c) A handle the list omits keeps its binding.** Silence is not an observation.
- **(d) A binding is consumed only by its handle's settle or close.**

### I4 Exactly once per turn (plan Revisions 1-3)

- `conclude` emits done, clears `active`/`pollArmed`, and stamps `doneEpoch`.
- A poll concludes only an active, non-stale record whose activity the current connection may have missed: `!streamUp || activeEpoch < streamEpoch || pollArmed`.
- A stream settle concludes an active record. It concludes an inactive one only when neither its start nor a done belongs to the current epoch.

### Derived properties

- **G1** Exactly one done per turn. I4 holds, and I1 never feeds a record an observation older than one already applied for it. Exception: the known limit L1(ii).
- **G2** A known completion is never discarded. "Known" means one of:
  - a settle for a handle whose turn was observed in progress by a binding observation (I3);
  - a settle for a listed handle;
  - a poll idle for an active record.
- **G3** Attribution. A settle goes to the newest observation of its handle's turn, so a replacement never takes the completion of the turn it replaced. Exception: the known limit L1(i).

## Realization

| Invariant | Code |
|---|---|
| I1a | `streamFIFO`, `drainStream` detach loop |
| I1b | `poll`: `snapshotHeld` around the post-I/O drain; `drainStream` requeues `items[i:]` at a held `up`; `defer w.drainStream(ctx)` |
| I1c | `applyStream(up)` calls `poll(ctx, true)` inside `drainStream` while `w.draining` is set |
| I1d, I2 | `seq := w.streamSeq` before the I/O, then the `prev.streamSeq <= seq` guard |
| I3a | `applyStream` `agent_start` (`streamTurn.seq`) and `resolveLookups`; `poll` does nothing to bindings while `w.streamUp && !reconcile` |
| I3b | `poll`: `rebind` for listed handles when `reconcile \|\| !w.streamUp` and the binding is not newer than the request; the list loop removes another identity's binding; the entries loop binds a non-idle observation and removes on idle |
| I3c | `poll` touches bindings only for listed handles |
| I3d | `settleStream`, `closeStream` |
| I4 | `conclude`, `finishStream`, `poll`'s conclude condition |

## Why I3b is safe (the Revision 4 amendment)

Revision 4 forbids a poll from rewriting a binding for one reason:
- A poll is answered on another connection.
- While the turn's connection is live, its settle may already be in flight on that stream.
- A poll that shows the handle's next session would then take that settle.

I3b rewrites a binding only when no live connection owns it.

- **Stream down.**
  - Every binding's connection has ended. Its `down` item followed all of its items in FIFO order, so every item it delivered has been applied (a settle consumed its binding).
  - An ended connection delivers nothing more, so no settle for any binding is in flight.
  - A turn still running settles on a later connection. That connection's reconciliation applies first (I1c) and re-observes every handle it lists, and that observation is newest.
- **Reconciliation.** The same holds at the connect's FIFO position, before any item of the new connection.
- **Seq guard.** A start applied during the poll's I/O, before the `down`, is newer than the poll, so its binding stays.
- **After the reconnect.**
  - A binding the reconciliation kept (I3c) is owned by the live connection, because its settle can only arrive there.
  - So I3a applies again, and no ordinary poll can take that settle.

## The five cases, derived from the rule

### Case 1: nested reconciliation. `TestStreamNestedReconciliation`

Ticks: working, then outer working (its get_state queues `up` and `EOF`), then idle.

1. Tick 1 (stream down) arms D7 (`activeEpoch` 0) and binds H (I3b).
2. The outer snapshot was requested before the connect. So its drain stops at `up` and requeues `[up, EOF]` (I1b), and the snapshot applies with the stream down. D7 is already active, and working is not a conclusion.
3. The reconciliation applies at the connect (I1c):
   - D7 is idle and active with `activeEpoch` 0 < 1, so it gets one done (I4).
   - D7's binding is removed because D7 is idle (I3b).
4. `EOF` applies, and the next idle poll finds D7 inactive.

Everything applied after the reconciliation is newer than it, so nothing re-arms D7. Result: done [D7], pending seq 1.

### Case 2: flapping stream, poll fallback (IS-4). `TestStreamFlappingPollFallback`

1. Each connect read during a poll's I/O is requeued, not dropped (I1b).
2. The working observation applies with the stream down, which arms D7.
3. The reconciliation sees idle and gives one done.
4. Later polls and reconciliations see idle on an inactive record.

Result: `streamEpoch` 2, stream down, done [D7], pending seq 1.

### Case 3: settle before rebind. `TestStreamHeldReconciliationRebindsTurn`, `TestStreamReconciliationDropsEndedTurn`, `TestStreamReconnectRebindsTurn`, `TestStreamReconciliationBindsBeforeLaterSettle`

**3a, Held.**
1. A is bound and its connection ends.
2. An outage poll sees B working. It removes A's binding (different identity) and binds B, with the stream down (I3b).
3. The connect is requeued behind that poll (I1b).
4. The reconciliation applies at the connect (I1c). B's settle, queued during the reconciliation's I/O, waits behind it.
5. The reconciliation sees B working and binds B.
6. The settle resolves to B, which is active, so one done B.

**3b, DropsEnded.**
1. The outage poll sees B working and binds B. The connect and B's settle are requeued behind it.
2. The reconciliation sees B idle. It concludes B (`activeEpoch` 1 < 2, so done B with `doneEpoch` 2) and removes the binding.
3. The settle resolves through `w.handles` to B. `doneEpoch` is the current epoch, so nothing more (I4).

Result: done [B].

**3c, ReconnectRebinds, with no outage poll.** The reconciliation sees B working, which replaces A (I3b), so done B.

**3d, with no outage poll, B settling during the reconciliation's I/O.**
1. Only the reconciliation can replace A before the settle.
2. The settle waits behind it (I1c).
3. The reconciliation binds B, so done B.

In 3a, the outage poll already binds B before the connect, so 3a no longer depends on I1c alone; 3d isolates I1c.

**Not reopened.**
- Every settle applied after a reconnect is preceded by that connection's reconciliation (I1c).
- I3b adds only earlier observations: outage polls, which are positioned before the connect (I1b) and are older than the reconciliation. So the reconciliation still has the last word for every handle it lists.
- I3b never acts while the stream is up (I3a), so a live start binding and its settle are untouched.

### Case 4: a known completion is never dropped. `TestStreamReconnectRemovedKnownTurn`, `TestStreamOutagePollKeepsKnownTurn`, `TestStreamStaleOutagePollKeepsBinding`

**4a, no outage poll** (review 3's case).
1. A's start and FIFO lookup bind H=A, and the connection ends.
2. The reconciliation omits H, so binding A is kept (I3c). A's record is closed (absent; I2 allows it).
3. The queued settle consumes binding A. The record is gone, `startEpoch` 0 and `doneEpoch` 0 differ from 2, so one done A and pending A seq 1.

**4b, an outage poll sees A still in progress** (working or blocked).
- That poll is the newest observation and is in progress, so it binds A again (I3b).
- The reconciliation omits H, so the binding is kept, and the settle gives done A.
- If the outage poll saw A idle instead, A's turn ended during the outage, and that poll concludes A itself (I4 with the stream down). The completion is reported by the poll, and the binding is retired so that the finished turn takes no later settle.

**4c, a poll requested before A's start, applied after the connection ended.**
- A stream item set A's binding after that poll's request, so the poll does not observe H (I3b exception).
- Its stale H=Z cannot remove A, so the settle gives done A.

**Not reopened.**
- Before the reconnect, I3b only rebinds an in-progress A to A, or retires A after its turn visibly ended (idle, or replaced).
- After the reconnect, I3a forbids ordinary polls from touching the kept binding, so the queued settle consumes it.
- A blocked turn is in progress (I4 concludes only on idle). Treating blocked as ended would retire A's binding and drop its completion, which `TestStreamOutagePollKeepsKnownTurn/blocked` guards.

### Case 5: a replacement seen only by outage polls. `TestStreamOutageReplacementRemoved`

**5a, bound A, B seen working.**
1. A is bound and its connection ends.
2. The outage poll lists H=B working. That removes A's binding (different identity) and binds B (I3b).
3. The reconciliation omits H, so binding B is kept (I3c).
4. B's settle gives done B and pending B seq 1. A gets only `closed`: its turn ended during the outage, and its settle was never delivered.

**5b, bound A, B seen idle then working.**
1. The first poll removes A's binding (different identity, and B is idle).
2. The second poll binds B, because I3b applies to the handle whether or not a binding exists.
3. The rest is as in 5a.

**5c, B unbound and seen working.** This is the state 5b passes through, with no earlier binding. The outage poll binds B, and the rest is as in 5a.

**Pre-fix result** (e638932, only the test added): FAIL in all three subtests.
```text
--- FAIL: TestStreamOutageReplacementRemoved/bound_A,_B_seen_working
    pending entries = [{ID:A ... Seq:1 Count:1 ...}], want B seq 1 count 1
    done ids = [A], want [B]
--- FAIL: TestStreamOutageReplacementRemoved/bound_A,_B_seen_idle_then_working
    done ids = [A], want [B]
--- FAIL: TestStreamOutageReplacementRemoved/unbound_B_seen_working
    pending entries = [], want B seq 1 count 1
    done ids = [], want [B]
```

**Why "only an existing binding is replaced" is not enough.** If only existing bindings could be replaced, 5b would still fail: the first outage poll removes A's binding, and nothing would then bind B.

## Existing guards under the rule

The amendment changes bindings only for polls that run while the stream is down. Every row keeps its outcome.

| # | Test | Why it still holds |
|---|---|---|
| 1 | TestStreamShortTurn | Stream up: the start binds and the settle consumes, so one done. |
| 2 | TestStreamPollExactlyOnce | Stream up: polls change no binding (I3a). A current-epoch turn is concluded only by its settle. |
| 3 | TestStreamCloseTombstone | Close and tombstone paths. Polls run with the stream up, so no binding change. |
| 4 | TestTransitions | The stream never connects. Polls bind and remove bindings, but no stream item ever reads them, so output is identical. |
| 5 | TestStreamObserveFirst | Reader wire format only. |
| 6 | TestStreamOutageReconnect | The outage poll binds D7. The reconciliation sees idle, concludes D7 and removes the binding. The queued settle finds `doneEpoch` current. |
| 7 | TestStreamRemovedTurn | Stream up: the live binding survives a newer absent list. |
| 8 | TestStreamReopenedHandle | Stream down, poll path. rpc-9 is bound while working and removed when idle; the poll concludes D7. |
| 9 | TestStreamReplacement | The start lookup is the binding observation, so B. |
| 10 | TestStreamDeferredDurable | Deferral rules; stream up. |
| 11 | TestStreamStaleWorking | The settle applied during the I/O consumed the binding and makes D7 newer than the snapshot (I2), so there is no re-arm and no re-bind. |
| 12 | TestStreamQueuedSettle | The outage poll binds D7. While up, polls keep it (I3a). The settle consumes it with `doneEpoch` current, so no second done. |
| 13 | TestStreamMidturnEarlySettle | The reconciliation list fails before any binding step. The settle resolves through `w.handles` to D7, so one done. |
| 14 | TestStreamCompaction | Stream up; `pollArmed` path. |
| 15 | TestStreamStartIdentity | Stream up: later lists never rewrite the live binding. |
| 16 | TestStreamStaleListDeferral | Absence never clears a deferral. |
| 17 | TestStreamReconciliationIdentity | The outage poll (true) or the reconciliation binds A. The later live list H=B does not rewrite it, so A. |
| 18 | TestStreamReconciliationOnce | The reconciliation arms and binds. While up, the poll neither concludes nor rebinds. The settle concludes once. |
| 19 | TestStreamLookupWhileTickHeld | No binding at the connect. The FIFO lookup binds B. |
| 20 | TestStreamOverflow | Every queued item applies before `streamDown` and the reconnect. |
| 21 | TestStreamSettleBeforeLookup | The settle uses the pre-start identity in FIFO order. |
| 22 | TestStreamCloseRemovedTurn | Stream up: the live binding survives an absent stream list. |
| 23 | TestStreamDeferredExpiryAndClose | Deferral end rules. |
| 24 | TestStreamLookupLostOnDown | `down` resolves the pending lookup. The held settle completes through the fallback lookup. |
| 25 | TestStreamReconnectRebindsTurn | Case 3c. |
| 26 | TestStreamReplacementFromUnwatched | Start-lookup replacement; stream up. |
| 27 | TestStreamDownDurableAssignment | Stream down; idle polls bind nothing. |

Case tests: 1, 2, 3 (Held, DropsEnded), 4 (RemovedKnownTurn, OutagePollKeepsKnownTurn working and blocked, StaleOutagePollKeepsBinding) and 5 (OutageReplacementRemoved, three subtests).

## Known limit L1: polls are answered on another connection

A poll is ordered by its request, but answered on a different connection than the stream. Host events between a new connection's registration and a poll's answer can therefore be seen in another order. This applies to a reconciliation, and to an outage poll whose I/O spans a connect (I1b requeues the connect behind it).

No rule over these inputs can tell the two histories apart. That is why I1 picks one fixed order.

- **(i) Attribution.**
  - The failing timeline: a turn A settles just after the connect, and H changes to B before the poll is answered. The newest list observation (B) then takes A's settle.
  - The inputs are identical to "B ran and settled after the connect" (3b without its outage poll). Review 3 required listed replacements to win.
  - Lifecycle records carry only handles, and `session_replaced` is not broadcast (plan Revisions 3 and 4).
- **(ii) Duplicate done.**
  - The failing timeline: an outage poll whose I/O spans a connect sees A idle and concludes it. A's own settle, emitted after the new registration, then arrives after the connect and reports A again.
  - The inputs are identical to "a new A turn began during the outage and settled on the new connection". The rule reports rather than drops it (G2).
  - It involves no binding. Probe result on e638932: `[done A, done A]`, unchanged by I3b.
- **Not attributable.** A turn never observed in progress has no binding: its start fell in an outage and no poll saw it running. If its handle leaves the list before the reconciliation is answered, its settle stays unattributed. This is the plan's never-listed exclusion (Revision 3).

**Cost of the rule.** While the stream is down, a binding for a session that vanished while in progress stays until its handle is observed again. One small entry per such session.

## INV-1 (documented only; follow-up, not part of this change)

**What happens.**
1. A poll whose list no longer shows A emits `closed A` and drops A's record, while A's settle is still unread.
2. The settle resolves A's binding. `finishStream` emits the single done and writes `w.seen[A]` back.
3. The next poll emits `closed A` a second time.
4. A stream `session_closed` that resolves the same binding can repeat `closed` in the same way.

**Conflict.** This conflicts with IS-3's "closed exactly once": a record re-created by a later observation is closed again.

**Why it is outside this change.**
- It is identical on 37678e8, 1e6cf72 and e638932, and it needs no reconnect.
- done and pending stay exactly once.
- It concerns the closed lifecycle, not completion ordering.

It is tracked as follow-up INV-1.
