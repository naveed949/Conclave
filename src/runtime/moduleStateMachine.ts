import { RaftNode } from '../consensus/raftNode';
import { StateMachine } from '../consensus/stateMachine';
import { ApplyResult } from '../consensus/types';
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

    constructor(host: ModuleHost = new ModuleHost()) {
        this.host = host;
    }

    apply(command: ModuleAppCommand): ApplyResult<unknown> {
        const result = this.host.apply(
            {
                module: command.module,
                command: command.command,
                input: command.input,
                seed: command.seed,
                sig: command.sig,
            },
            { actor: command.actor, requestId: command.requestId },
        );
        // The runtime's `effects` stay in the host's outbox (drained post-commit
        // by the EffectExecutor); the substrate only needs status/data/message.
        return { status: result.status, data: result.result, message: result.message };
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
