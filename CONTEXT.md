# Project Context

Orientation for anyone (or any agent) picking up this codebase. It explains
*what* the project is, the mental model, how the pieces fit, and where to look.
Companion documents:

- [`README.md`](./README.md) — how to run it, the API, env vars.
- [`docs/PHILOSOPHY.md`](./docs/PHILOSOPHY.md) — the core idea and *why*.
- [`docs/adr/`](./docs/adr/README.md) — the decisions, with rationale & alternatives.
- [`CLAUDE.md`](./CLAUDE.md) — concise working guidance, commands, invariants.

## In one paragraph

`backend-poc` is a proof-of-concept for a **decentralized backend**: a cluster of
equal peer nodes that agree on every change via the **Raft consensus protocol**,
with **no shared database**. Each node holds its own in-memory state machine that
converges by applying the same committed log entries in the same order. On top of
this substrate, three classic backend concerns are built in: **observability**,
**fault tolerance**, and a tamper-evident **audit** trail. The demo application is
a library book service (add / list / update / delete / borrow / return) — but it is
deliberately incidental; consensus is the real subject.

## Mental model

```
client ──HTTP──> any node
                   │  writes: proposed to the leader (followers forward)
                   ▼
            ┌──────────────┐   Raft RPCs (HTTP)   ┌──────────────┐
            │  leader      │◀────────────────────▶│  follower(s) │
            │  append log  │  RequestVote          │  replicate   │
            │  replicate   │  AppendEntries        │  apply       │
            │  commit      │  InstallSnapshot      │              │
            └──────┬───────┘                       └──────┬───────┘
                   │ apply committed entries               │
                   ▼                                        ▼
        ReplicatedStateMachine (per node)      ReplicatedStateMachine (per node)
        = books + audit chain + dedup          (converges to identical state)
```

**The source of truth is the replicated log, not any single node or store.** A
command becomes "real" only once a majority of nodes hold it (the commit point).

## Lifecycle of a request

**Write** (`POST/PUT/DELETE`):
1. Request hits any node; middleware assigns a `requestId` + `actor`.
2. If the node isn't the leader, it forwards to the leader (or replies `421`).
3. The leader builds a **deterministic command** — generating ids/timestamps *now*
   so all replicas apply identical data — and appends it to its log.
4. The entry is replicated; once a majority acknowledge, it is **committed**.
5. Each node **applies** the command to its state machine, recording an audit
   entry (hash-chained) and the result for idempotency. The leader resolves the
   client's pending request with the applied result.

**Read** (`GET`): by default served directly from the receiving node's local
replica (eventually consistent — fast and available on any node). With
`?consistency=strong` it instead goes through the leader's **ReadIndex barrier**
(leadership confirmed via a heartbeat quorum, then it waits until the read index
is applied) for a linearizable result; followers forward strong reads to the
leader just like writes.

## Key concepts (glossary)

- **Replicated state machine** — the book store; deterministic, rebuilt from the
  log, identical on every node.
- **Command** — the unit of replication (ADD/UPDATE/DELETE/BORROW/RETURN/NOOP);
  carries leader-resolved values and metadata (`requestId`, `actor`, `timestamp`).
- **Term / leader election** — Raft's logical clock and how a single leader is
  chosen per term.
- **Commit** — an entry replicated to a majority; only then is it applied.
- **Snapshot / compaction** — periodic state capture that lets old log entries be
  discarded; a far-behind follower is caught up via `InstallSnapshot`.
- **Audit chain** — append-only, hash-linked record of every committed change;
  tamper-evident and replicated.
- **Idempotency** — a replayed `requestId` returns the cached result, so retries
  have exactly-once effect.

## Where things live

| Concern | Path |
|---------|------|
| Raft algorithm | `src/consensus/raftNode.ts` |
| State machine + audit + idempotency | `src/consensus/{stateMachine,replicatedStateMachine}.ts` |
| Peer transport | `src/consensus/transport.ts` |
| Durable storage | `src/consensus/storage.ts` |
| Observability / tracing / forwarding | `src/platform/` |
| HTTP API | `src/{controllers,routes}`, `src/app.ts` |
| Process entry point | `src/server.ts` |
| Tests | `tests/` |

## Current status

- Implemented: Raft (election + replication), deterministic state machine,
  persistence, snapshotting/compaction, idempotency, leader forwarding,
  structured logging, Prometheus metrics, request tracing, hash-chained audit, CI.
- All work is on branch `claude/codebase-review-g2rts3` (PR #1). 27 tests pass.

## Known limitations / likely next steps

(See the "Limitations" section of the README and the relevant ADRs.)

- No dynamic cluster membership changes (fixed peer list).
- Reads are eventually consistent by default; linearizable reads are opt-in via
  `?consistency=strong` (leader-routed, no follower offloading or leases).
- Audit log grows unbounded (kept in full inside snapshots by design); the
  idempotency cache is bounded (`DEDUP_LIMIT`, deterministic FIFO eviction).
- Snapshots transfer in a single RPC (no chunking).

For production, the abstractions (`Transport`, `RaftStorage`, `MetricsRegistry`)
are the seams for swapping in mature libraries (gRPC, an embedded log store,
prom-client/OpenTelemetry) — see ADR-0013.
