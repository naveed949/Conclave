# 0014. Opt-in linearizable reads via ReadIndex

- Status: Accepted
- Date: 2026-06-19

## Context

ADR-0006 serves reads from each node's local replica: fast and always available,
but only eventually consistent — a read may miss a write that is committed but
not yet applied on the node that answered, and a follower may simply be behind.
Some operations (e.g. read-after-write, a balance check before an action) need a
**linearizable** read: a guarantee that the response reflects every write that
completed before the read began. We want this guarantee available without giving
up the fast local read for the common case, and without writing to the log (a
read should not bloat the log or the audit trail).

## Decision

Add an **opt-in** linearizable read path using Raft's **ReadIndex** technique
(§6.4), exposed via `?consistency=strong` (or an `X-Consistency: strong` header).
The default remains the local, eventually-consistent read of ADR-0006.

`RaftNode.readBarrier()` implements the barrier:

1. Capture `readIndex = commitIndex`. The leader commits a no-op on election, so
   its commit index reflects all writes from prior terms.
2. **Confirm leadership** by exchanging one round of heartbeats and requiring a
   majority to still acknowledge us for the current term — proving no newer
   leader has superseded this one (a reply that carries a higher term steps us
   down). This is what prevents a deposed leader from serving a stale read.
3. Wait until the state machine has **applied through `readIndex`**, then serve
   the read from the local state machine.

Strong reads are leader-only; a follower throws `NotLeaderError`, so the HTTP
adapter forwards them to the leader exactly as it does writes (ADR-0005). No log
entry is created, so the audit trail and log size are unaffected.

## Consequences

- Clients can choose per request between fast/available (default) and
  linearizable (`?consistency=strong`) reads.
- A strong read costs one heartbeat round-trip to a majority but **no log write**.
- If the leader cannot confirm a quorum (partition), the strong read fails closed
  with `NotLeaderError` rather than risk returning stale data.
- Strong reads concentrate on the leader (no follower read offloading); read
  leases (longer-lived leadership confirmation) could amortise the round-trip but
  add clock assumptions — deferred.
- Surfaced as `raft_read_barriers_total` for observability.

## Alternatives considered

- **Always linearizable** — simplest mental model, but every read pays
  coordination cost and loses follower availability. Rejected; ADR-0006's default
  is the right one for a read-heavy workload.
- **Leader lease reads** — avoid the per-read round-trip by trusting leadership
  for a lease window, but depend on bounded clock drift. Deferred.
- **Follower read with ReadIndex** (follower asks leader for the read index) —
  offloads reads to followers; more moving parts than needed for the POC.
