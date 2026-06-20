# 0017. Deploying application logic as smart contracts on this framework

- Status: Proposed
- Date: 2026-06-20

## Context

This project is, structurally, a small **smart-contract execution substrate**:
application logic is expressed as `Command` types (`consensus/types.ts`) that flow
through an ordered, replicated log and are applied by a deterministic state
machine (`stateMachine.ts`), with a tamper-evident hash-chained audit trail
(`replicatedStateMachine.ts`) and exactly-once semantics. That is the same shape
as "deterministic state transitions over an append-only, agreed-upon log" that a
contract platform provides.

A recurring question is therefore whether a conventional ("formal") API server's
application logic should be deployed *as smart contracts on this framework* —
i.e. modelling each mutating endpoint as a `Command` variant + an `apply()` case
+ a leader-side builder (`models/book.ts`), instead of an imperative handler over
a database. This ADR records the analysis and the conditions under which that is
(and is not) the right model.

The decisive fact shaping the whole analysis: **Raft is Crash-Fault-Tolerant
(CFT), not Byzantine-Fault-Tolerant (BFT).** It assumes peers are honest but may
crash; it does not tolerate malicious participants. The framework gives the
*form* of smart contracts (deterministic, replicated, audited execution) without
the *substance* of trustlessness (agreement among mutually-distrusting parties).
The "trustless agreement" framing in `docs/PHILOSOPHY.md` is aspirational in this
sense.

## Decision

Characterize the framework as a **CFT replicated-state-machine contract
substrate, scoped to a single trust domain**, and adopt the following guidance
for modelling application logic as contracts on it:

- **Recommended** where the goal is replication + tamper-evidence + exactly-once
  within *one organization that controls all nodes*: compliance ledgers,
  financial-ops journals, inventory/settlement, audit-critical CRUD. This is
  effectively "event-sourcing + Raft + a hash-chained audit," which the codebase
  already demonstrates.
- **Not recommended** where the motivation is trust-minimization across
  distrusting parties, public/permissionless participation, adversarial node
  operators, large state, or logic requiring external I/O. Those are a category
  mismatch for CFT consensus; use an actual BFT/blockchain platform.

When logic *is* modelled this way, it must respect the existing invariants:
determinism in `apply()` (ADR-0003), snapshot-relative indexing, leader-resolved
non-determinism, and side effects kept off the apply path (leader/edge "oracle"
pattern).

## Consequences

### Positive

- **The execution model is already enforced.** Ordered, replicated, reproducible
  state transitions come for free; the `apply()` switch is exhaustiveness-checked
  and non-determinism is pushed to leader-side builders.
- **Tamper-evident history = on-chain ledger.** `GET /audit` / `/audit/verify`
  give a verifiable, replicated execution log per call, with no extra
  event-sourcing infrastructure.
- **Exactly-once semantics = transaction nonce.** A replayed `requestId` returns
  the cached result, preventing double-application of stateful operations.
- **Fault tolerance with no central DB.** Logic and state survive a minority of
  node failures; there is no single database to lose.
- **Determinism forces clean architecture.** Pure state transitions separated
  from effects mirror smart-contract best practice and are trivially unit-testable.
- **Cheaper and faster than a real blockchain.** No PoW/PoS, no gas market, no
  global network; sub-second commits and optional linearizable reads.

### Negative

- **Not trustless / not BFT — the dealbreaker if trust-minimization is the goal.**
  The leader builds every command and could forge `actor`/`timestamp`/`id`;
  followers largely trust it. Any operator running modified `apply()` code
  diverges silently. The `matchIndex` clamp in `replicateTo()` is the only
  adversarial hardening.
- **Determinism is a convention, not a sandbox.** Nothing in the runtime prevents
  `Date.now()`/`Math.random()` inside `apply()`; it would silently diverge
  replicas (the worst class of bug). No metering, no static guarantee.
- **No gas / no resource metering.** A looping or over-allocating command blocks
  the single-threaded apply path on every node, with no bound — it can halt the
  cluster.
- **Mutable logic undermines immutability.** Deployed TypeScript can be changed
  and redeployed across a restart. The audit chain records *that* a command ran
  and its result, but not *which code version* produced it — "tamper-evident"
  covers the data, not the logic.
- **Determinism excludes most real API work.** No external calls, randomness, or
  wall-clock in `apply()`. Payment gateways, email, third-party reads need an
  off-chain/leader-side oracle pattern — the genuinely awkward part of contract
  development.
- **Single-leader writes + full replication.** Mutating calls serialize through
  one leader (no parallel execution); every node holds the full in-memory state
  and an unbounded audit log. Same scaling pain as a full blockchain node.
- **Schema/state migration friction.** State serializes as `RsmSnapshot`;
  evolving its shape requires careful snapshot-format migration or `restore()`
  breaks.

## Alternatives considered

- **Keep the conventional API-server model (imperative handlers over a shared
  database).** Simpler for arbitrary side-effecting logic and large state, but
  re-centralizes on one store and forfeits the replication, tamper-evident audit,
  and exactly-once guarantees the substrate provides — the very properties that
  motivate the contract framing.
- **Deploy on an actual BFT / smart-contract platform (EVM, Cosmos/Tendermint,
  Hyperledger Fabric).** The right choice when trust-minimization across
  distrusting parties is required, with structural determinism (sandbox), gas
  metering, and code-committing immutability. Rejected as default here because it
  is far heavier and unnecessary within a single trust domain, where CFT + audit
  already meets the need at a fraction of the cost.
- **Hybrid: model only audit-critical, deterministic core logic as contracts and
  keep side-effecting logic at the edge.** The pragmatic middle ground implied by
  the guidance above; preferred over an all-or-nothing migration.
