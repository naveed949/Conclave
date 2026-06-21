# 0020. Multi-Raft sharding for write scaling

- Status: Proposed
- Date: 2026-06-21

## Context

ADR-0019 pillar 4 names a hard limit of the prototype: **all writes serialize
through a single Raft leader.** One leader orders every committed entry, so write
throughput is bounded by one node, and the entire state must fit on every member.
M6 (CQRS read projections) and M8 (pluggable key-oriented state store) scale
*reads* and *state size*, but neither scales *writes* — a single Raft group is
still one ordering bottleneck.

The standard answer, used by every production system built on Raft (CockroachDB,
TiKV, YugabyteDB, etcd-at-scale), is **multi-Raft**: partition the keyspace into
shards, give each shard its own independent Raft group with its own leader, and
let writes to different shards proceed in parallel. The cost is that an operation
spanning two shards is no longer a single atomic log append — it needs a
cross-shard transaction protocol.

This ADR records the design for a multi-Raft *prototype* on top of the existing
runtime, and the decision to demonstrate cross-shard atomicity with a **saga**
(try/compensate) rather than blocking two-phase commit.

## Decision

Introduce a **shard router + per-shard Raft groups + a saga coordinator**, built
additively on the existing consensus core and runtime (no changes to `RaftNode`):

- **Sharding key.** A deterministic function maps each command to exactly one
  shard. For keyed modules (M8) the shard is derived from the record key
  (`hash(key) mod N`, or an explicit range map); for whole-state modules the
  whole module is pinned to one shard. The mapping is configuration, identical on
  every participant, so routing is deterministic.
- **Per-shard groups.** Each shard is an independent Raft cluster — its own set of
  `RaftNode`s, its own leader election, its own replicated log and `ModuleHost`.
  A write to shard *s* is proposed only to *s*'s leader; shards commit in parallel.
  Within a shard, every guarantee from M1–M9 still holds unchanged.
- **Shard router.** A thin front door maps an incoming command to its shard and
  submits to that shard's current leader (forwarding as today). It holds no state
  of its own — it is derivable from the shard map and each group's leadership.
- **Cross-shard transactions via saga.** An operation touching shards *A* and *B*
  (e.g. transfer from an account on *A* to one on *B*) runs as an ordered set of
  per-shard local steps, each with a **compensating** step. The coordinator
  executes forward; on a step failure it runs the compensations for the steps that
  already committed, in reverse. Each step and each compensation is an ordinary,
  idempotent, single-shard command (so it inherits exactly-once via the requestId
  dedup). This yields **atomicity by eventual compensation**, not isolation.

### Why saga, not 2PC

- Two-phase commit holds locks across shards for the duration of the transaction
  and blocks if the coordinator fails mid-commit — it trades availability for
  isolation, which is at odds with the project's availability-first thesis.
- A saga keeps each shard independently available and uses the runtime's existing
  idempotency + audit to make steps safe to retry and compensations safe to
  replay. It surfaces intermediate states (no isolation), which is the accepted
  trade-off and is exactly what the hash-chained audit is there to record.

## Consequences

### Positive

- Writes to different shards commit in parallel — throughput scales with shard
  count instead of being pinned to one leader.
- State is partitioned, so no single node must hold the whole dataset.
- Built additively: each shard is just an existing single-group cluster, so all
  per-shard correctness (determinism, snapshots, signed commands, sandbox, audit)
  carries over untouched.
- The saga steps are ordinary signed/audited module commands, so a cross-shard
  transaction is itself fully audit-traceable across shards.

### Negative

- **No cross-shard isolation.** A saga exposes intermediate states (the debit is
  visible before the credit). Reads spanning shards can observe a partially
  applied transaction; only per-shard linearizability is preserved.
- **Compensation, not rollback.** A failed cross-shard transaction is undone by
  forward compensating actions, which must be designed per operation and be
  idempotent — more application-author burden than a single-shard command.
- **Cross-shard ordering is not globally defined.** There is no single log across
  shards, so there is no global total order; the per-shard audit roots no longer
  compose into one root (a cross-shard transaction appears as correlated entries
  in two audits). A global audit needs a separate cross-shard correlation id.
- **Resharding is out of scope.** Changing the shard count/map (rebalancing) is a
  hard problem (key migration between groups) and is not addressed here.
- **Routing/membership complexity.** The router must track each shard's current
  leader, and a client must retry across leader changes per shard.

## Alternatives considered

- **Stay single-group (status quo).** Simplest and globally ordered, but the write
  bottleneck and whole-state-on-every-node limit are exactly what this addresses.
- **Two-phase commit / Percolator-style transactions.** Gives cross-shard
  isolation (snapshot/serializable), but blocks on coordinator failure and holds
  locks — rejected for the prototype in favor of availability; a real system might
  layer a transaction protocol on top of the same per-shard groups later.
- **Single big Raft group with parallel apply.** Doesn't help: the bottleneck is
  *ordering/commit* at one leader, not apply.
- **Hash- vs range-sharding.** Hash spreads load evenly but kills range scans;
  range keeps locality but risks hotspots. The prototype uses a simple,
  configurable map and treats the choice as orthogonal.

## Prototype scope (tracked under ADR-0019 milestone M10)

A runtime-level prototype: a deterministic `ShardRouter` over N independent
in-process groups, a `Saga` coordinator with compensations, and a demonstrated
cross-shard account transfer (debit on shard A, credit on shard B, with
compensation when the credit leg fails). Resharding, range scans, and a global
cross-shard audit root are explicitly out of scope.
