# CLAUDE.md

Guidance for AI agents (and humans) working in this repository. Keep it short,
current, and actionable. For *why* the system is built this way, read
[`docs/PHILOSOPHY.md`](./docs/PHILOSOPHY.md) and the ADRs in
[`docs/adr/`](./docs/adr/README.md).

## What this is

A POC of a **decentralized backend built on the Raft consensus protocol**. A
library book service is the demo; the consensus layer is the real subject. State
is a replicated log applied to a per-node in-memory state machine — **there is no
shared database**.

## Commands

```bash
yarn install            # install deps
yarn build              # tsc -> dist/
npx tsc --noEmit        # type-check only
yarn test               # jest (use LOG_SILENT=true to mute logs in dev)
LOG_SILENT=true npx jest snapshot   # run a single suite
```

Run a local cluster (each node is one process):

```bash
NODE_ID=node1 PORT=3001 PEERS="node2@http://localhost:3002,node3@http://localhost:3003" node dist/server.js
```

## Architecture map

- `src/consensus/` — the Raft algorithm (the core):
  - `raftNode.ts` — elections, replication, commit, snapshotting, dynamic
    membership. **Log is snapshot-relative**: `log[0]` is a sentinel at
    `lastIncludedIndex`; use the `pos()/termAt()/entryAt()/lastLogIndex()`
    helpers, never raw `log[i]`. **Configuration is derived from the log** (the
    latest `CONFIG` entry over `baseConfig`); change quorum/peer logic via
    `members`/`quorum()`/`otherMembers()`, never a fixed peer list.
  - `stateMachine.ts` — the generic `StateMachine<C, T>` **interface** an
    application implements; the consensus core is domain-agnostic (ADR-0017).
  - `replicatedStateMachine.ts` — wraps any application state machine, adding the
    audit hash-chain + idempotency on the apply path.
  - `transport.ts` — `HttpTransport` (prod) / `LocalTransport` (tests).
  - `storage.ts` — `FileStorage` / `MemoryStorage` (term, vote, log, snapshots).
- `src/platform/` — cross-cutting concerns: `logger`, `metrics`,
  `requestContext` (tracing), `forward` (leader forwarding).
- `src/models/book.ts`, `src/models/bookStateMachine.ts` — the **example app**: a
  `StateMachine` for books + its command builders (not part of the framework).
- `src/controllers`, `src/routes`, `src/app.ts` — thin HTTP adapter over the node
  for the book example (audit/raft routes are generic over any node).
- `src/edge/` — **edge read replica SDK (ADR-0023):** `EdgeReplica` tails a node's
  committed-log stream (`GET /raft/stream`, served by any node) and applies commits
  to a local `StateMachine` for local reads (Node `HttpStreamSource` / browser
  `EventSourceStreamSource`). Read-only, non-voting. The streaming primitives live
  on `RaftNode` (`onCommitted`/`getCommittedEntries`/`getStreamSnapshot`); worked
  demo in `examples/edge-replica/`.
- `src/server.ts` — wires a node (with `BookStateMachine`) from env and starts it.
- `src/index.ts` — public library surface (embedded-library use).
- `tests/` — `consensus`, `bookApi`, `platform`, `snapshot`, `readBarrier`,
  `membership`, `crashConsistency`, `logBacktracking` (+ `helpers.ts`).

## Invariants — do not break these

- **Determinism (ADR-0003):** the state machine must be deterministic. NEVER
  generate ids, timestamps, or randomness inside `apply()`/the state machine.
  Resolve them on the leader in the command builders (`src/models/book.ts`) so the
  value is baked into the command before it enters the log. Diverging replicas is
  a silent, severe bug.
- **Snapshot-relative indexing:** when touching the log, go through the index
  helpers; off-by-`lastIncludedIndex` errors corrupt replication.
- **Writes go through the leader** (followers forward); **reads are local** and
  eventually consistent by default. `?consistency=strong` reads go through the
  leader's ReadIndex barrier (`node.readBarrier()`) and must never be served from
  a node that can't confirm leadership — fail closed (`NotLeaderError`) instead.
- **Membership: joint consensus, one transition at a time (ADR-0022, supersedes
  ADR-0015).** A change C-old→C-new goes through a **joint** `CONFIG` (`members`=
  C-new, `oldMembers`=C-old) then a **final** `CONFIG` (`members`=C-new). A `CONFIG`
  entry takes effect on *append*, not commit. During the joint phase EVERY quorum
  decision (election, commit, leadership confirmation) needs a **dual majority** —
  a majority of BOTH C-old and C-new separately; route every decision through
  `inMajority()`, never a single count. Replicate to the voting **union**
  (`members` / `otherMembers()`). Never start a second change while one is in
  flight (the code refuses while a `CONFIG` is uncommitted *or* the config is still
  joint). Membership must be derived from the log so every replica agrees — don't
  track it out of band. A new leader that inherits an uncompleted joint config must
  finish the transition (`inheritedJoint` → finalize in `applyCommitted`).

## Conventions

- TypeScript, `strict: true`, 4-space indent. Match the surrounding style.
- **Minimal dependencies (ADR-0013):** prefer the Node stdlib (`http`, `crypto`,
  `fs`, `async_hooks`) over new packages. Don't add deps casually.
- Use the structured `Logger`, not `console.log` (errors may use `console.error`
  only where a logger isn't available).
- Adding a new command type (to an application, e.g. the book example): extend
  that app's command union (`models/book.ts`), handle it in the app's
  `StateMachine.apply` (`models/bookStateMachine.ts`; the `switch` is
  exhaustiveness-checked), and add a leader-side builder. The framework reserves
  the `NOOP` and `CONFIG` command types — app command types must not use them.
- Building a new application: implement `StateMachine<C, T>`
  (`consensus/stateMachine.ts`) and wire it via
  `new RaftNode({ stateMachine, … })`. Don't add domain types to `consensus/`.

## Testing notes

- Tests use `LocalTransport` + `MemoryStorage` — no sockets, no DB. Always
  `node.stop()` in teardown to clear timers.
- Use the `waitFor(...)` helper for election/commit timing; don't assert on fixed
  sleeps. Tests use short Raft timers via `tests/helpers.ts`.
- Keep `tsc --noEmit` and `yarn test` green before committing.

## Git / workflow

- Active branch: `claude/codebase-review-g2rts3`. Do not push elsewhere without
  explicit permission. Open a PR only when asked.
- Don't commit the runtime `data/` directory (git-ignored) or `dist/`.
- Record significant decisions as a new ADR in `docs/adr/` (don't edit accepted
  ones — supersede them).
