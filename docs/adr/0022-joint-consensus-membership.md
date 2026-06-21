# 0022. Dynamic cluster membership via joint consensus

- Status: Accepted
- Date: 2026-06-21
- Supersedes: [ADR-0015](./0015-dynamic-cluster-membership.md)

## Context

ADR-0015 implemented runtime membership changes with the Raft dissertation's
**single-server change** approach (§4.1): add or remove exactly one voting member
at a time, relying on the fact that consecutive single-server configurations
always overlap in a majority, so two configurations can never independently elect
a leader or commit conflicting entries. That kept the implementation small, but it
buys safety by *constraint*, not by mechanism:

- It can only ever express **one-node** changes. Replacing a node is two changes
  (add then remove); re-sharding or swapping several nodes is a slow sequence.
- The overlap argument is *assumed* by the single-node restriction rather than
  *enforced* by the vote/commit math — nothing in the quorum code would stop an
  arbitrary (non-overlapping) change from splitting the log if one were ever
  appended. The safety property lived in a comment, not in the predicate.
- Diehl/Ongaro later showed even single-server changes have a subtle corner case
  around the initial configuration; the robust, fully-general answer Raft actually
  specifies is joint consensus.

For a project whose whole subject is the consensus layer, the membership change
should be safe by *construction* and able to express arbitrary reconfigurations.

## Decision

Implement membership changes using Raft's **joint consensus** (§6 / dissertation
§4.3). A change from configuration `C-old` to `C-new` transitions through a
**joint configuration `C-old,new`** in which every quorum decision requires a
majority of **both** `C-old` and `C-new` *separately*. Two phases, each adopted
on append (not on commit) and each awaited to commit:

1. Append a **joint** `CONFIG` entry (`members: C-new`, `oldMembers: C-old`);
   replicate and await its commit under dual majority.
2. Append a **final** simple `CONFIG` entry (`members: C-new`); replicate and
   await its commit.

`changeMembership` runs both phases and resolves when the final config commits.

The dual-majority rule is the single gate for **every** quorum decision, so the
safety property is now enforced by the math rather than assumed by a restriction:

- **One predicate, `inMajority(ids)`** — for a simple config it is the ordinary
  `|ids ∩ C-new| ≥ ⌊|C-new|/2⌋ + 1`; for a joint config it additionally requires
  `|ids ∩ C-old| ≥ ⌊|C-old|/2⌋ + 1`. Election tally, commit advance, and
  leadership confirmation all route through it. During the joint phase no decision
  can be carried by a majority of only one configuration — making even an
  **arbitrary** change (one whose `C-old` and `C-new` do not overlap in a
  majority) safe.
- **Replication is driven by the voting UNION** `C-old ∪ C-new`: the leader
  replicates to, and confirms leadership against, every node in either
  configuration; the dual-majority predicate then decides. (`members` is the
  union; `configOld`/`configNew` are the two decision sets.)
- **Configuration remains derived from the log** (latest `CONFIG` over the
  snapshot base) and is adopted the instant an entry is **appended**, exactly as
  before — so every replica agrees on the shape without out-of-band tracking.
- **Joint configs survive compaction.** Snapshots and `InstallSnapshot` carry an
  optional `oldMembers` alongside `members`, so a node that snapshots, restarts,
  or catches up while a transition is in flight reconstructs the joint config and
  keeps enforcing dual majority rather than silently collapsing to a simple one.
- **One transition at a time** is still enforced (`hasUncommittedConfig` refuses a
  second change while one is uncommitted), and a leader **not in `C-new`** steps
  down once the final config commits (leader self-removal).
- If leadership is lost between the two phases, the leader does not append the
  final config from a stale term; a crash leaves the joint `CONFIG` in the log,
  where the next leader observes it and can drive the transition forward.

The HTTP surface and `raft_cluster_size` metric are unchanged from ADR-0015.

## Consequences

- Membership safety is enforced by the quorum math, not by a one-node restriction:
  the dual-majority predicate makes it impossible for `C-old` and `C-new` to elect
  leaders or commit entries independently during a transition, for *any* change.
- The API still exposes single add/remove operations (the common case), but the
  underlying mechanism now generalizes to arbitrary changes — a follow-up can
  expose a batch reconfiguration with no further consensus work.
- More moving parts than single-server changes: two voting sets, a union for
  replication, and `oldMembers` threaded through `CONFIG`, `Snapshot`, and
  `InstallSnapshot`. This is the price of being correct by construction and is
  covered by tests (including a partition test proving a `C-old`-only majority can
  neither elect nor commit during the joint phase).
- Only one transition may be in flight at a time, as before. A new voting member
  still joins immediately (no non-voting learner phase) — the same documented
  catch-up caveat as ADR-0015 applies and remains a future refinement.

## Alternatives considered

- **Keep single-server changes (ADR-0015)** — simpler, but limited to one-node
  changes and safe only by constraint; the corner cases and the inability to
  express arbitrary reconfigurations motivated this supersession.
- **Non-voting learner phase before promotion** — strictly better availability
  during catch-up; orthogonal to joint vs. single-server and still deferred as an
  optimisation on top of this decision.
