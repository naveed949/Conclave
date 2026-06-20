import express, { Request, Response } from 'express';
import { RaftNode, NotLeaderError, MembershipError } from '../consensus/raftNode';
import { CommandMeta } from '../consensus/types';
import { getContext } from '../platform/requestContext';
import { forwardToLeader, isForwarded } from '../platform/forward';

/** Internal cluster endpoints: peer RPCs, membership admin, and a status view. */
export default function raftRoutes(node: RaftNode) {
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
async function applyMembership(
    node: RaftNode,
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
