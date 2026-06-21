/**
 * Saga coordinator for cross-shard transactions (ADR-0020, M10).
 *
 * A multi-Raft cluster has NO single log spanning shards, so an operation that
 * touches two shards (e.g. debit an account on shard A, credit one on shard B)
 * cannot be one atomic append. ADR-0020 chooses a SAGA over two-phase commit:
 * run the per-shard steps forward, and if one fails, run the COMPENSATING action
 * for each step that already succeeded, in reverse order.
 *
 * This yields **atomicity by eventual compensation, NOT isolation**: intermediate
 * states are visible (the debit lands before the credit; on failure the credit
 * never happens and the debit is undone). That exposure is the accepted trade-off
 * for keeping each shard independently available (ADR-0020 "Why saga, not 2PC").
 *
 * Idempotency requirement: each `invoke` and each `compensate` is an ordinary,
 * single-shard MODULE command carrying a `requestId`, so the runtime's exactly-
 * once dedup makes steps safe to RETRY and compensations safe to REPLAY. Authors
 * MUST give distinct requestIds to the forward and compensating legs (they are
 * different commands) but keep each leg's requestId stable across retries.
 */

/**
 * One step of a saga. `invoke` performs the forward action; `compensate` undoes
 * it. `compensate` runs ONLY for steps whose `invoke` already resolved, and only
 * when a LATER step fails — so it must semantically reverse a committed `invoke`
 * (e.g. `invoke` = withdraw, `compensate` = deposit the same amount back).
 */
export interface SagaStep {
    name: string;
    invoke: () => Promise<unknown>;
    compensate: () => Promise<void>;
}

/** Outcome of {@link runSaga}: a discriminated union on `ok`. */
export type SagaResult =
    | { ok: true; results: unknown[] }
    | {
          ok: false;
          failedAt: string;
          error: Error;
          /** Steps whose compensation RAN SUCCESSFULLY (in compensation order). */
          compensated: string[];
          /**
           * Steps whose compensation itself THREW — their forward effect was NOT
           * undone, so this is the real "funds may be lost" signal a conservation-
           * critical coordinator must not hide. Empty on a clean compensation.
           */
          compensationFailures: { step: string; error: Error }[];
      };

/**
 * Run `steps` forward in order. On the FIRST failing `invoke`, stop and run
 * `compensate` for every step that already SUCCEEDED, in REVERSE order, then
 * report which steps were compensated.
 *
 * - Success: `{ ok: true, results }` with one entry per step (in step order).
 * - Failure: `{ ok: false, failedAt, error, compensated, compensationFailures }`.
 *   `failedAt` is the step whose `invoke` threw; `compensated` lists the steps
 *   whose compensation RAN SUCCESSFULLY (in the order their compensations ran —
 *   reverse of success). The failing step itself is NOT compensated (its `invoke`
 *   did not commit).
 *
 * A compensation that itself throws does NOT block the remaining compensations
 * (best-effort unwind), but it is NOT silently swallowed: the step is recorded in
 * `compensationFailures` and kept OUT of `compensated`, so a conservation-critical
 * caller can detect that a forward effect was never undone (real fund loss) and
 * park it for retry. A non-empty `compensationFailures` means the saga did not
 * fully unwind.
 */
export async function runSaga(steps: SagaStep[]): Promise<SagaResult> {
    const results: unknown[] = [];
    // Steps whose invoke committed, in execution order; compensated in reverse.
    const succeeded: SagaStep[] = [];

    for (const step of steps) {
        try {
            const result = await step.invoke();
            results.push(result);
            succeeded.push(step);
        } catch (err) {
            const error = err instanceof Error ? err : new Error(String(err));
            const compensated: string[] = [];
            const compensationFailures: { step: string; error: Error }[] = [];
            // Undo committed steps in REVERSE order so dependencies unwind safely.
            for (let i = succeeded.length - 1; i >= 0; i--) {
                const done = succeeded[i];
                try {
                    await done.compensate();
                    compensated.push(done.name);
                } catch (cerr) {
                    // A failed compensation must not block the others, but it means
                    // this step's effect was NOT undone — surface it, don't hide it.
                    compensationFailures.push({
                        step: done.name,
                        error: cerr instanceof Error ? cerr : new Error(String(cerr)),
                    });
                }
            }
            return { ok: false, failedAt: step.name, error, compensated, compensationFailures };
        }
    }

    return { ok: true, results };
}
