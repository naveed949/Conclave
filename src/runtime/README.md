# Runtime — ADR-0018 prototype

A prototype of the **deterministic core + effectful edge** design from
[ADR-0018](../../docs/adr/0018-native-backend-blockchain-benefits.md): a way to
write ordinary-looking backend modules that nonetheless inherit the blockchain
benefits of the consensus substrate — deterministic replicated execution,
exactly-once effects, and a provable tamper-evident audit.

This layer sits **on top of** the consensus core (`src/consensus/`) and does not
modify it. It is exercised by deterministic replay in `tests/runtime/`, which
demonstrates the replicated-state-machine property without wiring a generic
module command through `RaftNode` (a deferred next step — see the ADR).

## The model

```
  EDGE (effectful)          CORE (deterministic)          EDGE (effectful)
  resolveSeed(),       →    ModuleHost.apply():       →   EffectExecutor.drain():
  oracle reads,             pure reducers over             runs handlers once,
  validation                replicated state +             feeds results back as
                            outbox + Merkle audit          committed entries
```

Non-determinism (time, randomness, ids, external data) is resolved **once on the
edge** and baked into the committed command/seed, so every replica applies the
exact same values — the same discipline `src/models/book.ts` already uses for the
book demo.

## Pieces

| File | Role |
|------|------|
| `types.ts` | `ModuleDefinition`, `Reducer`, `ReducerContext`, `EffectIntent`, `Seed`, audit/outbox types |
| `defineModule.ts` | Declarative module definition + validation (rejects `__`-reserved names) |
| `context.ts` | `resolveSeed()` (the sole edge entropy site) and the deterministic `ReducerContext` |
| `moduleHost.ts` | `ModuleHost`: dispatch commands to pure reducers, queries, outbox, Merkle audit, snapshot/restore |
| `effectExecutor.ts` | Post-commit executor: runs effect handlers exactly-once and submits results back |
| `merkleAudit.ts` | Append-only Merkle accumulator: inclusion proofs + domain-separated root |
| `codeHash.ts` | Deterministic module code-version hash (POC stand-in for hashing the built artifact) |
| `modules/` | Demo modules: `counter`, `notes` (leader-resolved id/time via `ctx`), `payments` (effect → settle) |

## Writing a module

```ts
import { defineModule } from '../defineModule';

export const counter = defineModule<{ value: number }>({
    name: 'counter',
    version: '1',
    initialState: () => ({ value: 0 }),
    commands: {
        // Pure: state in, { state, result?, effects? } out. ctx is the only
        // source of "now"/randomness/ids, all derived from the leader's seed.
        increment: (state, input: any, _ctx) => ({
            state: { value: state.value + (input?.by ?? 1) },
        }),
    },
    queries: {
        value: (state) => state.value,
    },
});
```

## Invariants (do not break)

- **Reducers are pure.** Never read `Date`/`Math.random`/`crypto`/network inside a
  reducer — use `ctx.now` / `ctx.random()` / `ctx.id()`. Divergence is silent and
  severe (same rule as ADR-0003 for the core state machine).
- **Effects are described, not performed, in reducers.** Return `EffectIntent`s;
  the `EffectExecutor` performs them after commit and feeds results back as
  committed entries.
- **Everything hashed uses canonical (sorted-key) serialization** so replicas
  compute identical audit roots.

## Tests

```bash
LOG_SILENT=true npx jest tests/runtime
```

Covers determinism/convergence, snapshot-restore fidelity, exactly-once effects,
and Merkle proof/verify + audit convergence.
