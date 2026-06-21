import { resolveSeed } from './context';
import { EffectHandler, EffectIntent, EffectResultEntry } from './types';

/**
 * The effectful EDGE of the core/edge split (ADR-0019 pillar 3). The deterministic
 * core records `pending` effects in its outbox; this executor runs them post-commit
 * — the ONE non-deterministic actor in the system — and feeds each outcome back as
 * a committed `EffectResultEntry` that replicas apply identically.
 *
 * Exactly-once is a collaboration, not a single mechanism:
 *  - HANDLER execution is at-least-once across executor restarts: a crash after the
 *    side effect but before `submit` lands forces a retry on the next drain.
 *  - COMMITTED STATE is exactly-once regardless, because (a) the outbox dedups
 *    enqueue on `idempotencyKey`, (b) `applyEffectResult` is idempotent on that key,
 *    and (c) the in-flight guard below stops a single executor double-running an
 *    effect concurrently. Handlers should therefore be idempotent where the external
 *    system allows, but the core stays correct even if a handler runs twice.
 */
export class EffectExecutor {
    /**
     * Keys currently being executed by THIS executor. Guards against a concurrent
     * double-drain firing the same handler twice before the first `submit` has
     * marked the outbox `done`. Cleared in a `finally` so a failed handler can be
     * retried by a later drain.
     */
    private readonly inFlight = new Set<string>();

    constructor(
        private readonly handlers: Record<string, EffectHandler>,
        private readonly submit: (entry: EffectResultEntry) => void | Promise<void>,
    ) {}

    /**
     * Run every pending intent whose key is not already in flight. On success,
     * resolve a `Seed` here on the edge (committed verbatim so any `onResult`
     * reducer stays deterministic on every replica), package the handler's result
     * into an `EffectResultEntry`, and `submit` it back
     * into the log. On handler failure, leave the effect pending (do NOT submit) so a
     * later drain retries it.
     */
    async drain(pending: EffectIntent[]): Promise<void> {
        await Promise.all(pending.map((intent) => this.run(intent)));
    }

    private async run(intent: EffectIntent): Promise<void> {
        const key = intent.idempotencyKey;
        // In-flight guard: a second concurrent drain seeing the same pending key
        // skips it rather than re-running the handler.
        if (this.inFlight.has(key)) {
            return;
        }

        const handler = this.handlers[intent.kind];
        if (!handler) {
            // No handler for this kind: leave it pending. A handler may be
            // registered later; we never drop the intent or fabricate a result.
            return;
        }

        this.inFlight.add(key);
        try {
            const result = await handler(intent);
            // The seed is resolved once on the edge by the executor, then committed
            // verbatim so every replica applies the same value (convergence comes
            // from committing the value, not from where it was generated). That
            // deterministic seed keeps any consuming `onResult` reducer's `ctx`
            // identical on every replica.
            const entry: EffectResultEntry = { idempotencyKey: key, result, seed: resolveSeed() };
            await this.submit(entry);
        } catch {
            // Handler failed (e.g. network error): swallow and leave the effect
            // pending so the next drain retries. Submitting nothing keeps the
            // outbox entry `pending`, preserving at-least-once handler semantics.
        } finally {
            this.inFlight.delete(key);
        }
    }
}
