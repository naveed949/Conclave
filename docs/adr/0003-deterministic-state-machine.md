# 0003. Deterministic state machine; leader resolves non-determinism

- Status: Accepted
- Date: 2026-06-12

## Context

A replicated state machine only converges if every node applies the same commands
and produces the same result. Many operations involve non-deterministic values:
generated ids, "now" timestamps, due dates. If each node computed these at apply
time, every node would produce a *different* id/timestamp and state would diverge —
silently breaking consensus.

## Decision

The **leader resolves all non-deterministic values before a command enters the
log**. Command builders (`src/models/book.ts`) run only on the leader and bake in
the generated `id`, borrow timestamp, and due date. The state machine
(`stateMachine.ts`) is strictly deterministic: given a command, every node
produces identical state.

## Consequences

- State provably converges across nodes; this is verified by tests asserting all
  replicas hold identical books/log.
- Command payloads are self-contained and replayable (important for snapshots and
  restarts).
- Application logic must be written deterministically — a discipline enforced by
  keeping all randomness/time in the leader-side builders.

## Alternatives considered

- **Generate ids/timestamps in the state machine** — simplest to write but
  produces divergent replicas; rejected outright.
- **Seed a shared PRNG / logical clock** — workable but more machinery than a POC
  needs; baking values into commands is simpler and obviously correct.
