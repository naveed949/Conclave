import { defineModule } from '../defineModule';

/**
 * A demo SANDBOXED whole-state module (ADR-0018 pillars 2, 6 — M9). Defined with
 * `{ sandbox: true }`, so the host re-compiles each reducer into a frozen `vm`
 * context at registration: the reducers below run with NO ambient `Date`,
 * `Math.random`, `crypto`, timers, etc. — determinism is enforced STRUCTURALLY,
 * not merely by the static lint.
 *
 * Reducers MUST be self-contained arrow/`function` expressions that reference
 * only their parameters, `ctx`, and the curated safe globals (`Object`, `Array`,
 * `Math` without `random`, `JSON`, …). They derive any time/randomness/ids from
 * `ctx`, exactly like the non-sandboxed demos — the sandbox merely makes a
 * regression (reaching for `Date.now()`) impossible to apply silently.
 */
interface ComputeState {
    /** Last computed value (deterministic). */
    last: number;
    /** A deterministic id stamped on the last computation, from `ctx.id()`. */
    lastId: string;
}

/** Input to `sumTo`: compute the bounded sum 1..n. */
interface SumToInput {
    n: number;
}

export const compute = defineModule<ComputeState>(
    {
        name: 'compute',
        initialState: () => ({ last: 0, lastId: '' }),
        commands: {
            // A terminating arithmetic reducer: the triangular number 1..n. Bounded
            // by a hard cap so it always halts well within any step budget. Uses
            // ctx.id() (deterministic) — never crypto/Date — to stamp the result.
            sumTo: (state, input, ctx) => {
                const n = Math.max(0, Math.min(((input ?? {}) as SumToInput).n | 0, 100000));
                let total = 0;
                for (let i = 1; i <= n; i++) {
                    total += i;
                }
                return {
                    state: { last: total, lastId: ctx.id() },
                    result: { sum: total },
                };
            },
            // TEST FIXTURE ONLY — do not call on a real apply path. `spin` loops
            // forever; it exists solely to prove the leader-side admission step
            // meter (`ModuleHost.admit`) interrupts a runaway reducer via the vm
            // timeout and rejects it BEFORE it could enter the log. On the apply
            // path (no timeout) this would hang, which is precisely why admission
            // must reject it first.
            spin: (state) => {
                // eslint-disable-next-line no-constant-condition
                while (true) {
                    // busy-loop; the vm wall-clock budget interrupts this during
                    // the leader's admission dry-run.
                }
                // Unreachable, but keeps the reducer a well-formed expression.
                return { state };
            },
        },
        queries: {
            last: (state) => state.last,
        },
    },
    { sandbox: true },
);
