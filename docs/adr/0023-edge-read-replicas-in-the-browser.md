# 0023. Edge read replicas in the browser

- Status: Accepted
- Date: 2026-06-22 (accepted and implemented — see Implementation status)

## Context

Reads in this system are served from a node's local, eventually-consistent state
machine (ADR-0006), and an opt-in linearizable path goes through the leader's
ReadIndex barrier (ADR-0014). ADR-0014's follower-read offloading (M17) lets a
*follower* serve a linearizable read after confirming a safe read index with the
leader, so read serving can scale past the leader.

A natural question is whether we can push that one tier further — past the server
cluster entirely — and run a **read-only replica inside the client (browser)**, so
a read-heavy, interactive UI answers reads from a local in-memory copy with **no
network round-trip at all**, and updates reactively as new commits arrive.

The building blocks already exist and make this unusually tractable here:

- The state machine is a **deterministic, domain-agnostic `StateMachine<C, T>`**
  (ADR-0017) — the same code that runs on a server node can apply commands in a
  browser and converge to identical state.
- State is a **replicated, totally-ordered log** with snapshots and an
  `InstallSnapshot` catch-up path (ADR-0011, M16 chunking) — exactly what a late
  joiner needs to bootstrap then tail.
- The log is a **tamper-evident hash-chain** (ADR-0009) with `/audit/verify`, so a
  client can *verify* the integrity of the history it was served — an end-to-end
  guarantee most local-first sync stacks cannot offer.

Two facts constrain any design:

1. **Browsers cannot accept inbound RPCs.** Replication today is push
   (`AppendEntries`, leader→follower). A browser replica must instead **pull** a
   committed-log stream ("from index N") over SSE/WebSocket/long-poll, with
   snapshot handoff and resume-on-gap.
2. **A browser replica must be non-voting.** A flaky, untrusted client must never
   count toward a quorum or it would wreck liveness and safety. It is a **learner**
   (non-voting replica) — it never participates in elections, commit counting, or
   the write path.

So an "edge read replica" is precisely a **non-voting learner pushed to the client
over a pull-based stream**, serving reads locally.

## Decision

Record this as a **proposed, not-yet-built** capability, and define the shape and
the dependency order so it can be pursued deliberately rather than bolted on.

A browser edge replica would:

- **Bootstrap** from a (chunked) snapshot, then **tail** the committed log from the
  snapshot index via a resumable server-side stream endpoint.
- **Apply** committed commands to a local instance of the application's
  `StateMachine` and serve all reads from it — eventually consistent by default.
- Be **strictly read-only and non-voting**: writes still go through the
  authenticated leader path (ADR-0005); the replica never votes or acks.
- Offer **read-your-writes** via a session read-index token: after a write commits
  at index *i*, the client withholds the affected view until its local replica has
  applied through *i* (a strong read still costs a round-trip and is the exception).
- Optionally **verify** the audit hash-chain locally for end-to-end integrity.

This is deliberately gated behind three prerequisites, in order:

1. **A non-voting learner role** in the consensus core (also the prerequisite for
   the membership catch-up phase). The replica must be replicate-to-but-don't-count.
2. **A pull/streaming transport** + a resumable "log stream from index N" endpoint
   with snapshot handoff, gap detection, and backpressure.
3. **Scoped / partial replication + per-client authorization** — the make-or-break.
   A browser cannot hold the whole dataset or log, and must not receive other
   tenants' data. This needs per-key/per-tenant/per-shard scoped streams or CQRS
   projections, plus row-level authorization on the stream.

A thin browser SDK over the existing `StateMachine` is comparatively easy once
those exist.

## Consequences

Positive:

- **Reads become local memory access** — no leader/follower hop, no ReadIndex
  barrier — ideal for read-heavy interactive UIs.
- **Reactive UI for free**: the client already tails the log, so views update live
  as commits arrive (a built-in change-feed; no polling).
- **Read scaling at zero consensus cost**: the voting set stays small and fast
  while reads fan out to unlimited clients.
- **Offline / weak-connectivity reads** of last-known state; natural optimistic UI.
- **End-to-end tamper-evidence** down to the client via the audit chain — a genuine
  differentiator versus typical local-first sync.
- **Incremental bandwidth**: after the initial snapshot, only committed deltas flow.

Negative / risks:

- **Eventual consistency & read-your-writes** require explicit session/read-index
  handling; truly strong reads still need a round-trip, partially defeating the goal.
- **Partial replication is mandatory and does not exist today** (the state machine
  is whole-state). This is the largest gap.
- **Authorization vs. determinism tension**: "every replica applies the same log"
  is what gives convergence *and* client-side chain verification, but you cannot
  stream one user's data to another. Per-client filtered streams weaken both the
  uniform-log model and the integrity story; server-side projections become real,
  load-bearing design work.
- **Determinism now spans client builds (ADR-0003)**: the reducer runs across many
  browser versions at once, so state-machine changes must roll out without skew —
  an operational hazard, not just a coding rule.
- **Cold-start cost**: the initial snapshot can be large, worsened by the audit log
  living inside snapshots (the unbounded-audit limitation); M16 chunking helps the
  transfer, not the size.
- **Untrusted, low-control edge** multiplies failure modes (reconnection, duplicate
  delivery, compaction outrunning a slow client → forced re-bootstrap).

## Alternatives considered

- **Follower read offloading only (ADR-0014 / M17)** — keep read serving on server
  followers, not clients. Simpler, no partial-replication/authz problem, but still
  one network round-trip and no offline/reactive benefit. The pragmatic default.
- **Plain client-side cache (HTTP caching / SWR / a query cache)** — far simpler and
  needs no consensus changes, but no ordering guarantees, no integrity verification,
  and staleness is ad hoc rather than a principled read-index. Adequate for many UIs.
- **Adopt an existing local-first sync engine** (e.g. Replicache / ElectricSQL /
  PouchDB–CouchDB style) on top of, or instead of, this log — mature partial-sync
  and conflict handling, but discards the Raft-backed ordering and the audit-chain
  integrity that motivate this project. Worth referencing for the hard parts
  (partial replication, per-client authz) we would otherwise reinvent.
- **Do nothing** — reads stay server-side. No new attack surface or operational
  burden; loses the local-first latency/offline/reactivity upside. The status quo.

## Implementation status

Built across milestones M20–M24 (the three prerequisites, in the order above,
plus the client SDK):

- **M20 — committed-log read stream.** A non-voting, read-only tap on the
  committed log on `RaftNode` (`onCommitted` / `getCommittedEntries` /
  `getStreamSnapshot`) and a resumable SSE endpoint `GET /raft/stream` served by
  **any** node (snapshot handoff → committed tail → live tail).
- **M21 — edge replica SDK** (`src/edge/`). `EdgeReplica` bootstraps, applies
  committed commands to a local `StateMachine`, serves local reads, and provides
  read-your-writes (`waitForIndex`) and a change-feed, over a `LogStreamSource`
  seam (Node `HttpStreamSource`, browser `EventSourceStreamSource`).
- **M23 — per-client authorization + partial replication** (`StreamGuard` /
  `ScopedFilter`). A connection presents a token (bearer header or `?token=`);
  the snapshot and entry feed are restricted to its scope. The book example
  scopes by publisher. This closes the "make-or-break" gap above.
- **M24 — compiled browser SDK.** The browser imports the *same* compiled
  `StateMachine` the server runs (`yarn build:browser`), resolving the
  determinism-across-builds hazard. Worked browser + Node demos in
  `examples/edge-replica/`.

Observability: `raft_stream_subscribers` gauges active edge replicas per node.

**Deferred / production hardening** (intentionally out of scope here): backpressure
and connection caps for slow/abundant consumers (a slow SSE consumer currently
buffers server-side); JWT/session auth in place of the demo token registry, with
TLS/`wss` and short-lived tokens (the `?token=` form can leak via URLs); and
client-side audit-chain verification for end-to-end tamper-evidence (the stream
sends application state only today, so the chain is not re-derivable on the
client). See `docs/OPERATIONS.md` → "Edge read replicas in production".
