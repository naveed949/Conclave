# 0015. Dynamic cluster membership via single-server changes

- Status: Superseded by [ADR-0022](./0022-joint-consensus-membership.md)
- Date: 2026-06-19

## Context

The cluster's configuration (its set of voting members) was fixed at process
start from the `PEERS` environment variable. Growing, shrinking, or replacing a
node meant restarting the whole cluster — unworkable for a system that claims to
tolerate faults and scale. We need to add and remove nodes while the cluster
keeps serving, without ever allowing two disjoint majorities to form (which would
split-brain the log).

## Decision

Implement runtime membership changes using the Raft dissertation's
**single-server change** approach (§4.1): add or remove **one** voting member at
a time. Because consecutive single-server configurations overlap in a majority,
no two configurations can elect leaders or commit entries independently, so a
joint-consensus (C-old,new) phase is unnecessary.

- The configuration is a **`CONFIG` log entry** carrying the full new member set.
  Like all log entries it replicates through the normal path, but a node adopts
  the configuration the **moment the entry is appended** (not when committed) —
  this is the Raft rule that makes the overlap argument hold.
- All quorum/peer logic derives from the current configuration: election votes,
  commit counting, leadership confirmation, and replication targets.
- Configuration survives compaction: snapshots store the member set as of their
  `lastIncludedIndex`, and `InstallSnapshot` carries it, so a node restoring or
  catching up via a snapshot still learns the cluster shape.
- A leader **removed** from a committed new configuration **steps down**.
- A removed or partitioned server is prevented from disrupting the cluster with
  needless elections by the **leader-stickiness** rule (§4.2.3): a server ignores
  a `RequestVote` (and does not adopt its term) if it heard from a leader within
  the minimum election timeout.
- Exposed over HTTP: `GET /raft/members`, `POST /raft/members {id,url}`,
  `DELETE /raft/members/:id` (leader-routed, with follower forwarding), and
  surfaced as `raft_cluster_size`.

## Consequences

- The cluster can grow, shrink, and replace nodes with no downtime; a new node
  catches up via normal replication or an InstallSnapshot.
- Only one change may be in flight at a time (a second is refused until the first
  commits) — the safety condition for single-server changes.
- A newly added node joins as a full voting member immediately (no separate
  non-voting catch-up phase). If it is far behind *and* another member is down at
  the same time, commits can briefly stall on the larger quorum until it catches
  up — a documented limitation; the catch-up phase (§4.2.1) is the refinement.
- Configuration is now derived state (from the log + snapshot base), adding a
  little indexing complexity, but keeps membership consistent across replicas by
  the same mechanism as everything else: the replicated log.

## Alternatives considered

- **Joint consensus (C-old,new → C-new, §6)** — the original Raft membership
  algorithm; handles arbitrary multi-node changes at once but is materially more
  complex (two overlapping configurations active simultaneously). Overkill for a
  POC where one-at-a-time changes suffice.
- **Static configuration + full restart** — the prior behaviour; simple but
  defeats the availability goals of the project.
- **Non-voting learner phase before adding** — strictly better availability during
  catch-up; deferred as an optimisation on top of this decision.
