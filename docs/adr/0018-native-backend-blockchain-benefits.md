# 0018. Bringing blockchain benefits to backend development natively

- Status: Proposed
- Date: 2026-06-20

## Context

ADR-0017 concluded that this framework provides the *form* of smart contracts
(deterministic, replicated, audited execution) but not the *substance* of
trustlessness, and is best scoped to a single trust domain (CFT, not BFT). It
also enumerated the gaps that make "application logic as contracts" awkward for
real backends: determinism is a convention not a guarantee, there is no story for
external I/O, the single leader and in-memory state limit scale, the audit proves
data but not logic, and there is no resource bound.

This ADR answers the follow-up: **how can the framework deliver blockchain
benefits (CFT availability, exactly-once, tamper-evident/provable audit,
deterministic replay) natively in everyday backend development, without
compromising the fundamental principles of backend systems** (developer
productivity, side effects / external I/O, rich queries and large datasets,
horizontal scale, schema evolution, a standard API contract, security and
observability)?

The decisive insight: backends are inherently *effectful* while a replicated
state machine must be *pure and deterministic*. Rather than forcing backends to
become pure, **split the system in two and connect the halves with the log**:

```
  Effectful EDGE          Deterministic CORE         Effectful EDGE (out)
  (oracle resolution,  →  (consensus + audit +   →   (committed-intent
   auth, validation)       exactly-once, state)       executor / outbox)
                                   │
                                   ▼
                          Read projections (CQRS)
                          rich queries, big data
```

The deterministic core gets the blockchain properties; the edges do the real
backend work; the committed log is the exactly-once, auditable boundary between
them. The existing seams (`Command`/`apply()`, the `Transport` and `RaftStorage`
interfaces, leader-side builders, `CommandMeta`, the audit chain, snapshots) are
the foundation this builds on.

## Decision

Adopt a layered "deterministic core + effectful edge" architecture and evolve the
framework along the following pillars. Each closes a specific ADR-0017 gap while
preserving a backend principle.

1. **Module SDK (developer productivity).** Replace the four-touchpoint workflow
   (extend the `Command` union, add an `apply()` case, add a leader-side builder,
   wire a route) with one declarative unit: `defineModule({ state, commands,
   queries, effects })`. Developers write **pure reducers** plus a schema; the
   framework generates the namespaced command variants, the `apply()` dispatch,
   REST routes + OpenAPI, and validation. The REST surface stays standard so
   clients remain unaware they talk to a consensus cluster.

2. **Determinism as a guarantee, not a convention.** Inject a deterministic `ctx`
   (`ctx.now`, `ctx.random()`, `ctx.id()`) into every reducer, resolved on the
   leader and baked into `CommandMeta` before the entry enters the log
   (generalizing what `models/book.ts` already does for ids/timestamps). Run
   reducers in a sandbox with frozen globals shadowing `Date`/`Math.random`/
   `crypto`, add a lint rule, and add a CI check that replays the log on two
   nodes and diffs state hashes.

3. **Effects and external I/O via committed intents (the unblocker).** Reducers
   never perform I/O; they **return declarative effect intents** that commit to
   the log as part of the deterministic result. After commit, a single designated
   executor (leader or leased worker) performs the effect exactly-once keyed by
   `requestId`, then feeds the outcome back as a new command. Consequences: the
   replicated log *is* a transactional outbox (exactly-once, for free), and
   inbound external data is resolved on the leader before entering the log
   (leader-as-oracle), keeping replicas identical.

4. **Scale without compromising consensus.**
   - *Reads / rich queries / big data:* project the committed log into a queryable
     read model (CQRS) — a derived, rebuildable cache, not a central database, so
     ADR-0002 still holds and the log remains source of truth.
   - *Writes:* partition into multiple Raft groups (multi-Raft), one per keyspace/
     aggregate, each with its own leader; cross-shard operations use a saga or 2PC
     over groups. Small services stay single-group.
   - *State > RAM:* swap the in-memory `Map` for a pluggable embedded LSM/KV store
     behind the same interface (the `RaftStorage` seam); snapshots become store
     checkpoints.

5. **Auditability upgraded to real tamper-evidence.** Replace the linear hash
   chain with a Merkle tree/accumulator (O(log n) inclusion proofs and a compact
   root that can be externally anchored), and record a hash of the deployed module
   code in a `DEPLOY` log entry referenced by each command's meta — so the audit
   proves *which logic version* produced each result, closing ADR-0017's
   data-but-not-logic gap.

6. **Resource safety (gas analog).** Bound reducer execution with a deterministic
   step/instruction budget (count operations, not wall-clock), validated on the
   leader so over-budget commands are rejected before entering the log, plus input
   size limits at the edge. No gas market — just a guard against a runaway command
   halting the single-threaded apply path on every node.

7. **Stay CFT, add accountability cheaply.** Sign commands with the originating
   actor's key so the leader cannot forge `actor` in `CommandMeta`. Keep full BFT
   (PBFT/Tendermint) as an optional swap behind the consensus seam, only if
   multi-party trustlessness is ever required — not paid for by default.

### Phased roadmap

1. Determinism runtime + module SDK (pillars 1–2) — biggest DX/safety win, no
   architecture change.
2. Effect intents + post-commit executor/outbox (pillar 3) — unblocks real
   backend use cases.
3. CQRS read projections + pluggable state store (pillar 4) — queries and big data.
4. Merkle audit + committed code version + signed commands (pillars 5, 7) — real
   tamper-evidence.
5. Multi-Raft sharding + step budget (pillars 4, 6) — scale and safety hardening.

## Consequences

### Positive

- Blockchain benefits — CFT availability, exactly-once, provable/tamper-evident
  audit with committed code versions, deterministic replay, a built-in outbox —
  become available through ordinary-looking backend code.
- Each fundamental backend principle is preserved: productivity (module SDK),
  side effects (committed intents), rich queries/big data (CQRS + pluggable
  store), horizontal scale (read replicas + multi-Raft), schema evolution
  (logged, auditable `DEPLOY` migrations), standard API contract (unchanged REST),
  security/observability (kept at the edge, off the deterministic path).
- Builds on existing abstractions rather than replacing them; several pillars are
  additive against current seams.

### Negative

- Materially larger surface area: a module runtime/sandbox, an effect executor,
  projection infrastructure, multi-Raft routing, and a Merkle audit are each
  non-trivial subsystems — well beyond a POC's current scope.
- The committed-intent/outbox model adds eventual-consistency semantics for
  side effects (an effect runs after commit, asynchronously), which application
  authors must reason about.
- Multi-Raft introduces cross-shard transactions (saga/2PC), the hardest part of
  distributed data systems.
- Sandboxing and step-budgeting add per-command overhead and constrain what
  reducer code may do.
- Signed commands and Merkle proofs add cryptographic machinery and key
  management that the current trust-domain-internal model does without.

## Alternatives considered

- **Keep the framework as a focused POC and document the gaps only (ADR-0017).**
  Lowest effort, but leaves the framework unusable for mainstream backend work;
  this ADR is the constructive counterpart.
- **Adopt an existing platform instead of evolving this one** — an event-sourcing
  framework (Axon, EventStoreDB) for the core, or a permissioned ledger
  (Hyperledger Fabric) for the contract/audit properties. Pragmatic, but forfeits
  the single-substrate integration (consensus + audit + exactly-once + REST in one
  place) that is this project's thesis, and Fabric pays BFT-class cost unneeded in
  a single trust domain.
- **All-at-once rewrite rather than the phased roadmap.** Higher risk; the phases
  are independently valuable and each ships a usable increment, so incremental is
  preferred.
- **Make backends pure instead of splitting core/edge.** Rejected outright:
  forbidding external I/O contradicts the fundamental nature of backend systems;
  the core/edge split is precisely what avoids that compromise.

## Prototype status (2026-06-20)

A working prototype of the deterministic-core pillars lives under `src/runtime/`
(see [`src/runtime/README.md`](../../src/runtime/README.md)). It is layered on
top of the existing consensus core without modifying it, and was built in three
reviewed milestones:

- **M1 — Module SDK + deterministic runtime (pillars 1–2).** `defineModule`, a
  `ModuleHost` that dispatches commands to pure reducers, and a deterministic
  `ReducerContext` (`now`/`random`/`id`) derived entirely from a leader-resolved
  `Seed`. A convergence test proves two independent hosts fed the same seeded
  command stream reach byte-identical state.
- **M2 — Committed-intent effects + outbox executor (pillar 3).** Reducers emit
  declarative `EffectIntent`s into a deterministic outbox; a post-commit
  `EffectExecutor` runs each effect and feeds the result back as a committed
  `EffectResultEntry`. Exactly-once at the state level is enforced by outbox
  dedup + idempotent result application + an in-flight guard; handler execution
  is honestly at-least-once.
- **M3 — Merkle audit + committed code version (pillar 5).** A Merkle
  accumulator with O(log n) inclusion proofs and a domain-separated root, plus a
  per-command module code-version hash so the audit proves which logic version
  produced each result.

**Deferred (not in the prototype):** consensus wiring of a generic module command
into `RaftNode` (the runtime is exercised via deterministic replay, which proves
the replicated-state-machine property without touching the core); pluggable
state-store backend and CQRS read projections (pillar 4); multi-Raft sharding and
the step-budget "gas" guard (pillars 4, 6); signed commands and an optional BFT
swap (pillar 7); and a `vm`-level determinism sandbox (the prototype relies on
`ctx` injection plus the determinism convergence tests). These remain the natural
next increments of the roadmap above.
