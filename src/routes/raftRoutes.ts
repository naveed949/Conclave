import express, { Request, Response } from 'express';
import { RaftNode, NotLeaderError, MembershipError } from '../consensus/raftNode';
import { StateMachine } from '../consensus/stateMachine';
import { AppCommand, CommandMeta } from '../consensus/types';
import { getContext } from '../platform/requestContext';
import { forwardToLeader, isForwarded } from '../platform/forward';

/**
 * Internal cluster endpoints: peer RPCs, membership admin, and a status view.
 *
 * This is the Raft-SPECIFIC adapter: it exposes the protocol RPC endpoints
 * (`handleRequestVote`/`handleAppendEntries`/`handleInstallSnapshot`, the
 * `RpcHandler` surface) which are Raft-shaped and would be replaced *differently*
 * by a BFT engine. It is therefore intentionally typed to the concrete
 * {@link RaftNode}, NOT the engine-agnostic {@link Consensus} seam (ADR-0021).
 */
export default function raftRoutes<C extends AppCommand, T, SM extends StateMachine<C, T>>(
    node: RaftNode<C, T, SM>,
) {
    const router = express.Router();

    router.post('/request-vote', (req, res) => {
        res.json(node.handleRequestVote(req.body));
    });

    router.post('/append-entries', (req, res) => {
        res.json(node.handleAppendEntries(req.body));
    });

    router.post('/install-snapshot', (req, res) => {
        res.json(node.handleInstallSnapshot(req.body));
    });

    // ReadIndex (Raft §6.4): a follower asks the leader to confirm leadership and
    // return a safe read index, so the follower can serve a linearizable read
    // locally (follower read offloading) instead of forwarding the whole read.
    router.post('/read-index', async (req, res) => {
        res.json(await node.handleReadIndex(req.body));
    });

    router.get('/status', (_req, res) => {
        res.json(node.status());
    });

    // ---- cluster membership administration (Raft dissertation §4) ----

    router.get('/members', (_req, res) => {
        res.json(node.getMembers());
    });

    // Add a voting member: { "id": "node4", "url": "http://host:port" }.
    router.post('/members', (req, res) =>
        applyMembership(node, req, res, { add: { id: req.body.id, url: req.body.url } }),
    );

    // Remove a voting member by id.
    router.delete('/members/:id', (req, res) =>
        applyMembership(node, req, res, { remove: req.params.id }),
    );

    return router;
}

/** Run a membership change on the leader (forwarding from a follower) and relay the outcome. */
async function applyMembership<C extends AppCommand, T, SM extends StateMachine<C, T>>(
    node: RaftNode<C, T, SM>,
    req: Request,
    res: Response,
    change: { add?: { id: string; url: string }; remove?: string },
): Promise<void> {
    if (change.add && (!change.add.id || !change.add.url)) {
        res.status(400).json({ message: 'add requires { id, url }' });
        return;
    }
    const ctx = getContext();
    const meta: CommandMeta | undefined = ctx
        ? { requestId: ctx.requestId, actor: ctx.actor, timestamp: new Date().toISOString() }
        : undefined;
    try {
        await node.changeMembership(change, meta);
        res.json({ ok: true, members: node.getMembers() });
    } catch (err) {
        if (err instanceof NotLeaderError) {
            const leaderUrl = node.getLeaderUrl();
            if (leaderUrl && !isForwarded(req)) {
                const ok = await forwardToLeader(req, res, leaderUrl);
                if (ok) return;
            }
            res.status(421).json({ message: 'Not the leader — retry against the leader', leader: err.leaderId });
            return;
        }
        if (err instanceof MembershipError) {
            res.status(409).json({ message: err.message });
            return;
        }
        console.error(err);
        res.status(500).json({ message: 'Server error' });
    }
}
