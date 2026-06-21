import { Request, Response } from 'express';
import { Consensus } from '../consensus/consensus';
import { NotLeaderError } from '../consensus/raftNode';
import { CommandMeta } from '../consensus/types';
import { getContext } from '../platform/requestContext';
import { forwardToLeader, isForwarded } from '../platform/forward';
import { buildModuleCommand } from '../runtime/command';
import { ModuleStateMachine } from '../runtime/moduleStateMachine';
import { ModuleAppCommand } from '../runtime/types';

/**
 * The consensus seam this controller depends on, specialized to the module
 * runtime: `node.app` is the {@link ModuleStateMachine}, so `node.app.host`
 * resolves to the live `ModuleHost` the controller reads queries/state from.
 */
type ModuleConsensus = Consensus<ModuleAppCommand, unknown, ModuleStateMachine>;

/** A linearizable read is requested via `?consistency=strong` or `X-Consistency: strong`. */
function wantsStrongRead(req: Request): boolean {
    return req.query.consistency === 'strong' || req.header('x-consistency') === 'strong';
}

/**
 * Generic HTTP adapter between REST and a node whose application is the module
 * runtime ({@link ModuleNode}), the runtime analog of `bookController.ts`. Writes
 * (module commands) are proposed to the Raft log via the leader; a follower
 * transparently forwards them (falling back to 421 if the leader is
 * unknown/unreachable). Reads (queries / raw state) are served from the local
 * replica by default (eventually consistent); a client can opt into a
 * **linearizable** query with `?consistency=strong`, which goes through the
 * leader's ReadIndex barrier before serving.
 *
 * SIGNING OVER HTTP (ADR-0019 pillar 7): the server NEVER holds an actor's
 * private key. The client signs the LOGICAL command
 * (`{ module, command, input, actor, requestId }`) with its own key and sends the
 * base64 signature in the `x-signature` header. This adapter only RELAYS that
 * signature onto the command (`sig`); the leader resolves the seed (which is
 * deliberately outside the signed payload) and every replica verifies the
 * signature on the deterministic apply path. The adapter does no signing itself.
 */
export function createModuleController(node: ModuleConsensus) {
    // Forward to the leader (or reply 421) when this node isn't the leader.
    const onNotLeader = async (req: Request, res: Response, err: NotLeaderError): Promise<void> => {
        const leaderUrl = node.getLeaderUrl();
        if (leaderUrl && !isForwarded(req)) {
            const ok = await forwardToLeader(req, res, leaderUrl);
            if (ok) return;
        }
        res.status(421).json({ message: 'Not the leader — retry against the leader', leader: err.leaderId });
    };

    // Run `serve` only after the linearizable read barrier resolves. On a
    // follower this obtains a ReadIndex from the leader and applies through it,
    // then serves LOCALLY (no forwarding); only if no confirmed read index can be
    // obtained does it fall back to forwarding/421 (fail closed — never stale).
    const strongRead = async (req: Request, res: Response, serve: () => void): Promise<void> => {
        try {
            await node.readBarrierLocal();
            serve();
        } catch (err) {
            if (err instanceof NotLeaderError) return onNotLeader(req, res, err);
            console.error(err);
            res.status(503).json({ message: 'Read barrier failed', error: (err as Error).message });
        }
    };

    return {
        /**
         * `POST /modules/:module/:command` — propose a module command. The body is
         * the command `input`. The deterministic seed is resolved on the leader by
         * `buildModuleCommand`; `actor`/`requestId` come from the request context.
         * An `x-signature` header (a client-produced signature over the logical
         * command) is relayed onto the command's `sig` for apply-path verification.
         */
        runCommand: async (req: Request, res: Response): Promise<void> => {
            const ctx = getContext();
            if (!ctx) {
                res.status(500).json({ message: 'Missing request context' });
                return;
            }
            const command = buildModuleCommand(req.params.module, req.params.command, req.body, {
                actor: ctx.actor,
                requestId: ctx.requestId,
            });
            // Relay (never produce) the client's signature. The private key stays
            // with the client; the server only carries the signature to the apply
            // path, where every replica verifies it against the actor's public key.
            const signature = req.header('x-signature');
            if (signature) command.sig = signature;

            const meta: CommandMeta = {
                requestId: ctx.requestId,
                actor: ctx.actor,
                timestamp: new Date().toISOString(),
            };
            try {
                const result = await node.submit(command, meta);
                res.status(result.status).json(result.data ?? { message: result.message });
            } catch (err) {
                if (err instanceof NotLeaderError) return onNotLeader(req, res, err);
                console.error(err);
                res.status(500).json({ message: 'Server error' });
            }
        },

        /**
         * `GET /modules/:module/query/:name` — run a read-only query against the
         * local replica. The whole `req.query` object is passed as the query args.
         * `?consistency=strong` (or `X-Consistency: strong`) routes through the
         * leader's ReadIndex barrier first, forwarding to the leader on a follower.
         */
        query: async (req: Request, res: Response): Promise<void> => {
            const serve = () => {
                try {
                    const value = node.app.host.query(req.params.module, req.params.name, req.query);
                    res.json(value);
                } catch (err) {
                    // Unknown module/query is a client error, not a server crash.
                    res.status(404).json({ message: (err as Error).message });
                }
            };
            if (wantsStrongRead(req)) return strongRead(req, res, serve);
            serve();
        },

        /**
         * `GET /modules/:module/state` — the raw current state of a module from the
         * local replica (eventually consistent). A convenience read for inspection.
         */
        getState: (req: Request, res: Response): void => {
            const state = node.app.host.getState(req.params.module);
            res.json(state ?? null);
        },
    };
}

export type ModuleController = ReturnType<typeof createModuleController>;
