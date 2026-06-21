import { CommandMeta } from '../consensus/types';
import { NotLeaderError } from '../consensus/raftNode';
import { ConsensusOf } from '../consensus/consensus';
import { MetricsRegistry } from '../platform/metrics';
import { buildEffectResultCommand } from './command';
import { EffectExecutor } from './effectExecutor';
import { ModuleStateMachine } from './moduleStateMachine';
import { EffectHandler, ModuleAppCommand } from './types';

/** Default tick interval (ms) for the leader's outbox drain loop. */
const DEFAULT_INTERVAL_MS = 25;

/**
 * The LIVE, leadership-aware driver of the committed-intent effect loop (M12).
 * It is the running-cluster counterpart of what `tests/runtime/effects.test.ts`
 * exercises by hand: on the LEADER only, it drains the `ModuleHost` outbox after
 * commits, runs each pending effect's handler at the edge (the ONE place I/O is
 * allowed), and feeds the resolved {@link EffectResultEntry} back through the
 * replicated log (`MODULE_EFFECT_RESULT`) so every node applies it.
 *
 * EXACTLY-ONCE STATE, AT-LEAST-ONCE HANDLERS. Handler execution is at-least-once
 * across leader changes and restarts: if leadership is lost mid-drain (or a node
 * crashes after the side effect but before the result commits), the effect stays
 * `pending` and the new leader's driver retries it. The COMMITTED STATE effect is
 * nonetheless exactly-once because three guards collaborate:
 *  - leader-only ticks: followers never run handlers, so only one node acts;
 *  - the outbox dedups enqueue on `idempotencyKey` and `applyEffectResult` is
 *    idempotent on it (a redelivered/replayed result never re-dispatches);
 *  - the result command's stable `requestId` (`effect:<key>`) lets the substrate
 *    dedup cache treat a re-submitted result as a replay.
 * Handlers should therefore be idempotent where the external system allows, but
 * the replicated state stays correct even if a handler runs twice.
 *
 * NO consensus-core changes: this is an additive, post-commit poller. It does
 * NOT register a commit hook on the node — it polls `pendingEffects()` on a
 * timer, which is sufficient because the outbox is durable replicated state.
 */
export class EffectDriver {
    private readonly executor: EffectExecutor;
    private readonly intervalMs: number;
    private timer: ReturnType<typeof setInterval> | undefined;
    /** Re-entrancy guard: a tick whose drain is still running skips the next. */
    private draining = false;

    constructor(
        // Depends on the `Consensus` seam (ADR-0021/M13), not the concrete node:
        // it only uses `submit`/`isLeader`/`app`, so any engine implementing
        // `Consensus` drives effects unchanged.
        private readonly node: ConsensusOf<ModuleAppCommand, unknown, ModuleStateMachine>,
        handlers: Record<string, EffectHandler>,
        opts: { intervalMs?: number; metrics?: MetricsRegistry } = {},
    ) {
        this.intervalMs = opts.intervalMs ?? DEFAULT_INTERVAL_MS;
        // The executor's `submit` rides the resolved result back through the log.
        // A `NotLeaderError` (leadership lost between the leader check and submit)
        // is swallowed: leave the effect pending so the new leader retries. The
        // outbox dedup + idempotent `applyEffectResult` keep state exactly-once.
        this.executor = new EffectExecutor(handlers, async (entry) => {
            const command = buildEffectResultCommand(entry);
            const meta: CommandMeta = {
                requestId: command.requestId,
                actor: command.actor,
                timestamp: new Date().toISOString(),
            };
            try {
                await this.node.submit(command, meta);
            } catch (err) {
                if (err instanceof NotLeaderError) return;
                throw err;
            }
        }, opts.metrics);
    }

    /**
     * Start the drain loop. Each tick: if this node is NOT the leader, do nothing
     * (followers never execute effects). Otherwise, if a drain is not already in
     * flight and there are pending effects, drain them. The `draining` flag makes
     * overlapping ticks a no-op so a slow handler is not double-fired by the timer.
     */
    start(): void {
        if (this.timer) return;
        this.timer = setInterval(() => {
            void this.tick();
        }, this.intervalMs);
        // Don't let the loop keep the process alive on its own.
        if (typeof this.timer.unref === 'function') this.timer.unref();
    }

    private async tick(): Promise<void> {
        if (this.draining) return;
        if (!this.node.isLeader()) return;
        const pending = this.node.app.host.pendingEffects();
        if (pending.length === 0) return;
        this.draining = true;
        try {
            await this.executor.drain(pending);
        } finally {
            this.draining = false;
        }
    }

    /** Stop the drain loop and clear the timer (no leaks). */
    stop(): void {
        if (this.timer) {
            clearInterval(this.timer);
            this.timer = undefined;
        }
    }
}
