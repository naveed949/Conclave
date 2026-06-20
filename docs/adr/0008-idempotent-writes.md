# 0008. Idempotent writes via request IDs

- Status: Accepted
- Date: 2026-06-18

## Context

Fault-tolerant clients retry on timeout or leader change. Without
de-duplication, a retried write can be applied twice (e.g. two identical books, a
double decrement of copies). The system delivers commands at-least-once; we want
their *effects* to be exactly-once.

## Decision

Every write carries a `requestId` (from an inbound `X-Request-Id` header or
generated). The replicated state machine records the result for each applied
`requestId`; a command whose `requestId` has already been applied returns the
**cached result without re-applying**. Because the dedup table is part of the
deterministic state machine, it is identical on every node and survives via
snapshots.

The dedup table is **bounded** to `DEDUP_LIMIT` entries (default 10,000) so it —
and the snapshots it is folded into — cannot grow without limit. Eviction is
**insertion-order FIFO**: when the cap is exceeded, the oldest remembered
`requestId` is dropped. The eviction policy must be deterministic for the same
reason the state machine must be (ADR-0003): every node applies the same commands
in the same order, so each evicts exactly the same entries and the replicas stay
identical. A wall-clock TTL would *not* be deterministic — two nodes could evict
at different moments, so one would re-apply a replay while another served the
cached result, silently diverging the cluster — so it is explicitly rejected.

## Consequences

- Client retries become safe: at-least-once delivery yields exactly-once effects.
- Ties together with tracing (ADR-0010) and audit (ADR-0009), which reuse the same
  `requestId`.
- Memory and snapshot size are bounded; the cache size is exposed for monitoring
  (`raft_dedup_cache_size`, `/raft/status`).
- The window is finite: a retry of a request older than `DEDUP_LIMIT` distinct
  intervening writes is no longer deduped and will re-apply. This is acceptable —
  realistic retries are recent — and is the standard Raft "client session"
  trade-off. Operators size `DEDUP_LIMIT` to cover their expected retry window.

## Alternatives considered

- **No idempotency** — simplest, but unsafe under realistic retry behavior.
- **Idempotency only at the HTTP layer** — wouldn't survive leader changes or
  replays through the log; must live in the replicated state machine.
- **Unbounded dedup table** — the original POC approach; simple but grows without
  limit (memory and snapshot bloat) for a long-running cluster.
- **Wall-clock TTL eviction** — natural for a cache, but non-deterministic across
  nodes and would diverge the replicated state machine. Rejected.
- **Per-client session tracking (Raft §6.3)** — bounds by client rather than by
  global count; more precise but heavier. Deferred — global FIFO suffices for the POC.
