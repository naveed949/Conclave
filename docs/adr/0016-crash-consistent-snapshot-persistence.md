# 0016. Crash-consistent snapshot/log persistence and snapshot transfer

- Status: Accepted
- Date: 2026-06-19

## Context

The log and the snapshot are persisted to two separate files (ADR-0007, ADR-0011),
written with independent atomic renames — but not atomically *with respect to each
other*. A code review found three correctness gaps that the in-process tests
(which use `MemoryStorage` and never crash mid-write) did not exercise:

1. `takeSnapshot` wrote the compacted log first, then the snapshot. A crash in
   between left a compacted log whose sentinel sat at the *new* boundary while the
   snapshot file still recorded the *old* one. On restart the log base and the
   snapshot disagreed, and the snapshot-relative index math silently corrupted —
   dropping committed entries.
2. `handleInstallSnapshot` discarded the follower's entire log unconditionally and
   reset `commitIndex`/`lastApplied` even when the follower was already ahead,
   violating Raft figure 13 step 6 (retain a matching suffix) and risking rollback.
3. `sendSnapshot` built a *live* snapshot at `lastApplied` and labelled it with a
   term looked up from a possibly-compacted entry, so the shipped
   `lastIncludedTerm` could disagree with the real entry and corrupt the
   follower's later AppendEntries consistency checks.

## Decision

- **Write order is snapshot-first, then the compacted log**, in both `takeSnapshot`
  and `handleInstallSnapshot`. This makes the failure modes one-directional: the
  durable log base can only ever *lag* the snapshot, never lead it (which would
  lose the state the discarded entries produced).
- **Persist the log's base index/term** (`baseIndex`/`baseTerm`) alongside the log.
  On restart `reconcileLog()` compares them with the snapshot: if the log base lags
  the snapshot (the crash window above), it trims the now-covered prefix and keeps
  only the tail, instead of trusting the stale base.
- **InstallSnapshot follows figure 13 step 6**: ignore a snapshot already covered
  (`<= lastIncludedIndex` or `<= commitIndex`), retain the log tail when the entry
  at the snapshot boundary matches by term, and advance `commitIndex`/`lastApplied`
  monotonically (never roll back).
- **Ship the durable snapshot**: `sendSnapshot` forwards the persisted snapshot's
  `lastIncludedIndex`/`lastIncludedTerm`/`data` verbatim, not a freshly-taken one
  at `lastApplied`.
- A node **removed from the configuration stops campaigning** (it can never win a
  real quorum and would only disrupt the survivors as a zombie leader).

## Consequences

- A crash between the snapshot and log writes is now recoverable without data loss
  or index corruption; covered by `tests/crashConsistency.test.ts`.
- Followers no longer lose a valid log suffix or roll state back on InstallSnapshot.
- Snapshot transfers always carry a self-consistent boundary.
- `PersistentState` grew two optional fields; old persisted files (without them)
  still load and are treated as base 0.

## Alternatives considered

- **A single combined state file** (log + snapshot written atomically together) —
  simplest to reason about, but couples the two and rewrites the whole snapshot on
  every log append. The separate-files + ordering + reconciliation approach keeps
  the frequent log write cheap while remaining crash-safe.
- **A write-ahead intent record / fsync barrier** — stronger durability but more
  machinery than a POC warrants; the ordering invariant achieves correctness.
