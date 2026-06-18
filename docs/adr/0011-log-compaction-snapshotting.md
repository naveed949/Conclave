# 0011. Log compaction via snapshotting and InstallSnapshot

- Status: Accepted
- Date: 2026-06-18

## Context

A Raft log grows without bound as writes accumulate. With state held in memory and
the log replayed on restart, an ever-growing log means unbounded memory, disk, and
restart time. Followers that fall far behind would also need the entire log
re-sent.

## Decision

Implement log compaction by snapshotting (Raft paper §7). Once the in-memory log
exceeds `SNAPSHOT_THRESHOLD`, a node snapshots its state machine (books + audit
chain + dedup cache) and discards the covered entries. The log is stored
relative to the snapshot boundary (`log[0]` is a sentinel at `lastIncludedIndex`).
A leader brings a lagging follower up to date with an **InstallSnapshot** RPC when
the follower's `nextIndex` precedes the leader's compacted log. Snapshots are
persisted separately from the log.

## Consequences

- The in-memory/on-disk log stays bounded; restart replays only the post-snapshot
  tail on top of a restored snapshot.
- A wiped or far-behind follower is recoverable purely via a snapshot transfer.
- Indexing logic is more complex (absolute index ↔ array position helpers).
- Snapshots are sent in a single RPC (no chunking) — fine for POC-sized state, a
  documented limitation for very large state.

## Alternatives considered

- **No compaction** — simplest, but unbounded growth makes long-running clusters
  impractical.
- **Incremental/chunked snapshot transfer** — needed for large state; deferred as
  unnecessary complexity for the POC.
