# Architecture Decision Records

This directory records the significant architectural decisions made on this
project, using lightweight [ADRs](https://adr.github.io/). Each record captures
the **context** (the forces at play), the **decision** taken, its
**consequences** (good and bad), and the **alternatives** considered — so the
reasoning survives even when the people don't.

ADRs are immutable once accepted: to change a decision, add a new ADR that
supersedes the old one rather than editing history.

## Index

| # | Title | Status |
|---|-------|--------|
| [0001](./0001-adopt-raft-consensus.md) | Adopt Raft consensus to decentralize the backend | Accepted |
| [0002](./0002-no-shared-database.md) | No shared database; per-node replicated state machine | Accepted |
| [0003](./0003-deterministic-state-machine.md) | Deterministic state machine; leader resolves non-determinism | Accepted |
| [0004](./0004-pluggable-transport.md) | Pluggable transport (HTTP in prod, in-process in tests) | Accepted |
| [0005](./0005-leader-writes-with-forwarding.md) | Leader-only writes with follower forwarding | Accepted |
| [0006](./0006-local-eventually-consistent-reads.md) | Local, eventually-consistent reads | Accepted |
| [0007](./0007-durable-persistence.md) | Durable persistence of Raft state and snapshots | Accepted |
| [0008](./0008-idempotent-writes.md) | Idempotent writes via request IDs | Accepted |
| [0009](./0009-log-as-audit-trail.md) | The replicated log as a tamper-evident audit trail | Accepted |
| [0010](./0010-observability-in-substrate.md) | Observability built into the consensus substrate | Accepted |
| [0011](./0011-log-compaction-snapshotting.md) | Log compaction via snapshotting and InstallSnapshot | Accepted |
| [0012](./0012-platform-layer.md) | A reusable platform layer for cross-cutting concerns | Accepted |
| [0013](./0013-minimal-dependencies.md) | Minimal external dependencies (implement primitives in-house) | Accepted |
| [0014](./0014-opt-in-linearizable-reads.md) | Opt-in linearizable reads via ReadIndex | Accepted |
| [0015](./0015-dynamic-cluster-membership.md) | Dynamic cluster membership via single-server changes | Superseded by ADR-0022 |
| [0016](./0016-crash-consistent-snapshot-persistence.md) | Crash-consistent snapshot/log persistence and snapshot transfer | Accepted |
| [0017](./0017-generic-pluggable-state-machine.md) | Generic pluggable state machine (framework core) | Accepted |
| [0018](./0018-application-logic-as-smart-contracts.md) | Deploying application logic as smart contracts on this framework | Proposed |
| [0019](./0019-native-backend-blockchain-benefits.md) | Bringing blockchain benefits to backend development natively | Proposed |
| [0020](./0020-multi-raft-sharding.md) | Multi-Raft sharding for write scaling | Proposed |
| [0021](./0021-pluggable-bft-consensus-seam.md) | Pluggable BFT consensus — the seam, and why it is not built | Proposed |
| [0022](./0022-joint-consensus-membership.md) | Dynamic cluster membership via joint consensus | Accepted |
| [0023](./0023-edge-read-replicas-in-the-browser.md) | Edge read replicas in the browser | Proposed |

## Template

```markdown
# NNNN. Title

- Status: Proposed | Accepted | Superseded by ADR-XXXX
- Date: YYYY-MM-DD

## Context
What is the problem and the forces at play?

## Decision
What we decided to do.

## Consequences
The results — positive, negative, and follow-ups.

## Alternatives considered
What else we looked at, and why we didn't choose it.
```
