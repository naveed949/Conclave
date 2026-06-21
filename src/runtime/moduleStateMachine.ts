import { RaftNode } from '../consensus/raftNode';
import { StateMachine } from '../consensus/stateMachine';
import { ApplyResult } from '../consensus/types';
import { MetricsRegistry } from '../platform/metrics';
import { ModuleHost } from './moduleHost';
import { ModuleAppCommand } from './types';

/**
 * Adapts a {@link ModuleHost} to the framework's {@link StateMachine} contract
 * (ADR-0017) so the whole module runtime — pure reducers, effects/outbox, the
 * Merkle audit, the keyed store, signing — plugs into a `RaftNode` as an ordinary
 * pluggable application:
 *
 *   const sm = new ModuleStateMachine();
 *   sm.host.registerModules([counter, notes]);
 *   const node = new RaftNode({ id, peers, stateMachine: sm }, transport);
 *
 * This is the seam ADR-0021 calls the real pluggability point — the
 * commit-ordered-log contract above the log. The module runtime no longer needs
 * a bespoke `MODULE` command baked into the consensus core: a `ModuleAppCommand`
 * is just an application command `C`, and this adapter is the application.
 *
 * The only impedance to bridge is that `StateMachine.apply(command)` receives no
 * separate `CommandMeta` (audit/idempotency are the substrate's concern), whereas
 * `ModuleHost.apply(cmd, meta)` needs `actor`/`requestId` (for the audit leaf and
 * signature verification). We therefore carry those two fields ON the command
 * (see {@link ModuleAppCommand}) and split them back out here. `ModuleHost`
 * itself is untouched.
 */
export class ModuleStateMachine implements StateMachine<ModuleAppCommand, unknown> {
    /** The wrapped runtime — use it to register modules/keys and to read state. */
    readonly host: ModuleHost;
    /**
     * Optional metrics sink (Milestone 15). When set, the adapter records command
     * throughput/latency on the apply path and exposes a scrape-time collector for
     * the outbox/audit/module gauges. PURE OBSERVABILITY: a counter incremented on
     * `apply` is identical on every replica (the same committed command stream),
     * touches NO replicated state/snapshot/audit, and is fully optional — an
     * undefined registry is a no-op, so `buildModuleCluster` (no metrics) is
     * unaffected and determinism/convergence are untouched.
     */
    private readonly metrics?: MetricsRegistry;

    constructor(host: ModuleHost = new ModuleHost(), metrics?: MetricsRegistry) {
        this.host = host;
        this.metrics = metrics;
    }

    apply(command: ModuleAppCommand): ApplyResult<unknown> {
        // Route on the application `type` discriminator. A caller invoke
        // (`'MODULE'`) runs the reducer; a committed effect result
        // (`'MODULE_EFFECT_RESULT'`, M12) routes to `applyEffectResult` so the
        // edge-resolved outcome folds back into state identically on every node.
        // Time the host call and label by module/command for the metrics below; a
        // `MODULE_EFFECT_RESULT` has no module/command, so it gets a synthetic label.
        const isEffect = command.type === 'MODULE_EFFECT_RESULT';
        const moduleLabel = isEffect ? '__effect' : command.module;
        const commandLabel = isEffect ? 'result' : command.command;
        const start = Date.now();

        let result;
        if (isEffect) {
            result = this.host.applyEffectResult(command.entry, {
                actor: command.actor,
                requestId: command.requestId,
            });
        } else {
            result = this.host.apply(
                {
                    module: command.module,
                    command: command.command,
                    input: command.input,
                    seed: command.seed,
                    sig: command.sig,
                },
                { actor: command.actor, requestId: command.requestId },
            );
        }

        // Pure observability — never affects state/snapshot/audit/convergence and a
        // no-op when no registry is wired.
        if (this.metrics) {
            const labels = { module: moduleLabel, command: commandLabel };
            this.metrics.moduleCommands.inc({ ...labels, status: result.status });
            this.metrics.moduleCommandDuration.observe(Date.now() - start, labels);
        }

        // The runtime's `effects` stay in the host's outbox (drained post-commit
        // by the EffectDriver); the substrate only needs status/data/message.
        return { status: result.status, data: result.result, message: result.message };
    }

    /**
     * Push scrape-time module-runtime gauges into `metrics` (Milestone 15) — the
     * runtime analog of `RaftNode.collectMetrics()`. Read-only over the host
     * (outbox status counts, audit size, module count), so it never perturbs
     * replicated state. Wired via `metrics.registerCollector(() => sm.collectMetrics(m))`.
     */
    collectMetrics(metrics: MetricsRegistry): void {
        let pending = 0;
        let done = 0;
        for (const entry of this.host.getOutbox()) {
            if (entry.status === 'done') done += 1;
            else pending += 1;
        }
        // Single-series gauges (no label dimension), so — unlike raft's per-peer
        // labelled gauges — they need no `reset()`: each scrape overwrites the one
        // series with the current count.
        metrics.moduleOutboxPending.set(pending);
        metrics.moduleOutboxDone.set(done);
        metrics.moduleAuditSize.set(this.host.auditSize());
        metrics.moduleRegistered.set(this.host.moduleCount());
    }

    snapshot(): unknown {
        return this.host.snapshot();
    }

    restore(data: unknown): void {
        this.host.restore(data as Record<string, unknown>);
    }
}

/**
 * A Raft node whose application is the {@link ModuleStateMachine} runtime — the
 * concrete node type the generic module HTTP adapter wires (the runtime analog of
 * `BookNode` in `models/bookStateMachine.ts`). `node.app` is the
 * `ModuleStateMachine`, so `node.app.host` is the live {@link ModuleHost} the
 * controller reads queries/state from.
 */
export type ModuleNode = RaftNode<ModuleAppCommand, unknown, ModuleStateMachine>;
