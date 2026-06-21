# 0017. Generic pluggable state machine (framework core)

- Status: Accepted
- Date: 2026-06-20

## Context

The project set out to demonstrate consensus, but the application — the library
book service — was welded into the consensus core. The book `Command` union,
the `Book` type, and `ApplyResult.book` lived inside `consensus/types.ts`;
`BookStateMachine` was hard-wired into `ReplicatedStateMachine`; and `RaftNode`
called book-specific methods. To use the consensus layer for anything else you
had to fork it.

The decision was taken (with the project owner) to make this usable as a
**framework** in real backend systems, consumed as an **embedded library**: a
team installs it, defines their own domain, and runs it on the consensus core.
That requires the core to know nothing about books — or any specific domain.

## Decision

Invert the dependency: the consensus layer becomes generic over an application
**`StateMachine<C, T>`** that the consumer supplies.

- `consensus/stateMachine.ts` now defines the `StateMachine<C, T>` *interface*
  (`apply`, `snapshot`, `restore`, optional `size`) — the contract an
  application implements. Determinism remains the one hard rule (ADR-0003).
- The replicated log carries a **flat command union**,
  `Command<C> = { type: 'NOOP' } | { type: 'CONFIG'; members } | C`. Application
  commands ride the log directly (no envelope), so the wire/persistence format
  is unchanged and `node.submit(domainCommand)` stays ergonomic. The framework
  reserves the `NOOP` and `CONFIG` type discriminators.
- `RaftNode<C, T, SM>` and `ReplicatedStateMachine<C, T>` are generic. The
  cross-cutting substrate concerns — audit hash-chain, idempotency/dedup,
  snapshotting, observability — stay in the substrate and now apply to *any*
  state machine, which is the whole point of ADR-0012/ADR-0009/ADR-0008.
- `node.app` exposes the concrete application state machine for domain reads;
  `node.stateMachine` exposes the substrate wrapper (audit, size, dedup).
- The book service moves to the example/application layer
  (`models/book.ts`, `models/bookStateMachine.ts`) and plugs in via
  `new RaftNode({ stateMachine: new BookStateMachine(), … })`. The HTTP layer
  (`controllers/`, `routes/bookRoutes`, `app.ts`) is the example's adapter; the
  audit and raft routes are generic over any node.
- A public entry point (`src/index.ts`) re-exports the framework surface for
  embedded-library use.

## Consequences

- The consensus core is genuinely reusable: swap `BookStateMachine` for
  payments, inventory, a feature-flag store, etc., and inherit replication,
  audit, idempotency, and observability for free.
- Type inference flows from the supplied state machine: `RaftConfig.stateMachine`
  is typed `SM & StateMachine<C, T>` so `C`/`T` are inferred at the construction
  site without explicit type arguments.
- Casts appear at the two inherently untyped boundaries — the transport
  (incoming `AppendEntries` entries) and durable storage (the persisted log) —
  where JSON crosses back into typed code. These are localized and commented.
- The book-specific metric `books_total` became the generic
  `state_machine_entries`; node status reports `stateSize` rather than `books`.
- All existing behaviour is preserved: the full test suite passes unchanged in
  intent (only construction now supplies a state machine, and domain reads go
  through `node.app`).

## Alternatives considered

- **Wrap user commands in an `{ type: 'APP'; command: C }` envelope** — cleaner
  discriminated union, but changes the wire/persistence format and the audit
  type extraction, and makes `submit`/builders less direct. The flat union with
  two reserved type names is simpler and backward-compatible.
- **Keep books in the core, expose extension points** — would leave the core
  domain-coupled and the "framework" claim hollow.
- **Standalone sidecar (language-agnostic) instead of an embedded library** —
  deferred: the embedded core is the harder, foundational piece and a sidecar
  can be layered on top of it later (see the consumption-model decision).
