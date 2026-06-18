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
  - `raftNode.ts` — elections, replication, commit, snapshotting. **Log is
    snapshot-relative**: `log[0]` is a sentinel at `lastIncludedIndex`; use the
    `pos()/termAt()/entryAt()/lastLogIndex()` helpers, never raw `log[i]`.
  - `stateMachine.ts` / `replicatedStateMachine.ts` — deterministic book store;
    the latter adds the audit hash-chain + idempotency on the apply path.
  - `transport.ts` — `HttpTransport` (prod) / `LocalTransport` (tests).
  - `storage.ts` — `FileStorage` / `MemoryStorage` (term, vote, log, snapshots).
- `src/platform/` — cross-cutting concerns: `logger`, `metrics`,
  `requestContext` (tracing), `forward` (leader forwarding).
- `src/controllers`, `src/routes`, `src/app.ts` — thin HTTP adapter over the node.
- `src/server.ts` — wires a node from env and starts it.
- `tests/` — `consensus`, `bookApi`, `platform`, `snapshot` (+ `helpers.ts`).

## Invariants — do not break these

- **Determinism (ADR-0003):** the state machine must be deterministic. NEVER
  generate ids, timestamps, or randomness inside `apply()`/the state machine.
  Resolve them on the leader in the command builders (`src/models/book.ts`) so the
  value is baked into the command before it enters the log. Diverging replicas is
  a silent, severe bug.
- **Snapshot-relative indexing:** when touching the log, go through the index
  helpers; off-by-`lastIncludedIndex` errors corrupt replication.
- **Writes go through the leader** (followers forward); **reads are local** and
  eventually consistent.

## Conventions

- TypeScript, `strict: true`, 4-space indent. Match the surrounding style.
- **Minimal dependencies (ADR-0013):** prefer the Node stdlib (`http`, `crypto`,
  `fs`, `async_hooks`) over new packages. Don't add deps casually.
- Use the structured `Logger`, not `console.log` (errors may use `console.error`
  only where a logger isn't available).
- Adding a new command type: extend the `Command` union in `consensus/types.ts`,
  handle it in `stateMachine.ts` (the `switch` is exhaustiveness-checked), and add
  a leader-side builder in `models/book.ts`.

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
