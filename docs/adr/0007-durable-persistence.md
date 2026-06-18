# 0007. Durable persistence of Raft state and snapshots

- Status: Accepted
- Date: 2026-06-18

## Context

Raft's safety guarantees depend on certain state surviving crashes:
`currentTerm`, `votedFor`, and the log must be durable, or a restarted node can
vote twice in a term or lose committed entries — violating correctness. The
initial implementation kept everything in memory, so a restart was unsafe.

## Decision

Introduce a `RaftStorage` abstraction with two implementations:

- **`FileStorage`** — persists term/vote/log (and snapshots, per ADR-0011) to
  per-node JSON files, written via atomic temp-file rename. Reloaded on startup.
- **`MemoryStorage`** — a no-op implementation used by tests and ephemeral runs.

Persistence happens on every mutation of the durable state (term change, vote,
log append/truncate, snapshot).

## Consequences

- A node can crash and restart without violating Raft safety; verified by tests.
- The storage interface keeps durability decoupled from the algorithm, so tests
  stay fast (in-memory) while production is durable.
- Synchronous writes on every mutation are simple but not the highest-throughput
  option — acceptable for a POC.

## Alternatives considered

- **Write-ahead log / embedded KV (e.g. LevelDB)** — more robust and performant
  but adds a dependency and complexity beyond POC needs (see ADR-0013).
- **No persistence** — simplest, but makes restarts unsafe; rejected.
