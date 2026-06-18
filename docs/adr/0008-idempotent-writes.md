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

## Consequences

- Client retries become safe: at-least-once delivery yields exactly-once effects.
- Ties together with tracing (ADR-0010) and audit (ADR-0009), which reuse the same
  `requestId`.
- The dedup table grows unbounded (no TTL/eviction) — a documented POC limitation.

## Alternatives considered

- **No idempotency** — simplest, but unsafe under realistic retry behavior.
- **Idempotency only at the HTTP layer** — wouldn't survive leader changes or
  replays through the log; must live in the replicated state machine.
