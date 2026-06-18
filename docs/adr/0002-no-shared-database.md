# 0002. No shared database; per-node replicated state machine

- Status: Accepted
- Date: 2026-06-12

## Context

The original design persisted all state in a single MongoDB instance shared by
the service. A shared database is a single point of failure and control — exactly
what a decentralized system must avoid. If consensus replicates commands but all
nodes still read/write one database, the database, not consensus, remains the
source of truth.

## Decision

Remove the shared database entirely. Each node owns an **in-memory state machine**
that is rebuilt by applying committed log entries. State is replicated *through
consensus*, not through shared storage. Mongoose/MongoDB and the dead `db.ts`
were deleted.

## Consequences

- True decentralization: there is no central store to fail or to trust.
- The source of truth is the replicated log; the state machine is a derived view.
- Durability now depends on the Raft log/snapshot persistence (see ADR-0007), not
  on a database.
- Trade-off: we forgo a mature database's query capabilities, indexing, and
  storage management — acceptable for a POC whose point is the consensus model.

## Alternatives considered

- **One database per node, replicated by consensus** — heavier, and still couples
  the demo to external infrastructure; an in-memory state machine demonstrates the
  idea more cleanly.
- **Keep MongoDB as the state store** — re-centralizes the system and defeats the
  project's purpose.
