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

    // ---- committed-log read stream (ADR-0023: edge read replicas) ----
    //
    // Server-Sent Events stream of the committed log, served from THIS node's
    // local, eventually-consistent state (ADR-0006) — any node can serve it, so
    // read serving fans out across the cluster and, via a browser EventSource,
    // past it. A consumer connects with `?fromIndex=N` (the last index it has);
    // we bootstrap it from the snapshot if its entries were compacted away, replay
    // the committed tail, then live-tail new commits. The consumer is a read-only,
    // non-voting learner — it never participates in consensus.
    //
    // Events: `snapshot` { lastIncludedIndex, lastIncludedTerm, members, data },
    //         `entry`    { index, entry },
    //         `caughtup` { index }  (caller has replayed through the live head).
    router.get('/stream', (req, res) => {
        const fromIndex = Math.max(0, Number.parseInt(String(req.query.fromIndex ?? '0'), 10) || 0);

        res.writeHead(200, {
            'Content-Type': 'text/event-stream',
            'Cache-Control': 'no-cache, no-transform',
            Connection: 'keep-alive',
            // Defeat proxy buffering so events flush promptly.
            'X-Accel-Buffering': 'no',
        });
        // Tell an EventSource client how long to wait before reconnecting.
        res.write('retry: 2000\n\n');

        const send = (event: string, data: unknown): void => {
            res.write(`event: ${event}\ndata: ${JSON.stringify(data)}\n\n`);
        };

        // The whole setup below runs synchronously in one tick, so no new commit
        // can interleave between the catch-up read and the subscription — `cursor`
        // is a consistent handoff point and the `onCommitted` guard dedupes the seam.
        let cursor = fromIndex;

        // Bootstrap from the snapshot if the consumer's next entry was compacted away
        // (or it is brand new and a snapshot exists). Otherwise tail from where it left off.
        if (node.needsSnapshot(fromIndex)) {
            const snap = node.getStreamSnapshot();
            if (snap) {
                send('snapshot', snap);
                cursor = snap.lastIncludedIndex;
            }
        }

        // Replay the already-committed tail the consumer hasn't seen.
        for (const item of node.getCommittedEntries(cursor)) {
            send('entry', item);
            cursor = item.index;
        }
        send('caughtup', { index: cursor });

        // Live-tail: forward each newly-committed entry, skipping anything already
        // replayed above (the synchronous seam) and refusing to leave a gap.
        const unsubscribe = node.onCommitted((index, entry) => {
            if (index <= cursor) return;
            send('entry', { index, entry });
            cursor = index;
        });

        // Keepalive comments stop idle intermediaries from dropping the connection.
        const keepalive = setInterval(() => res.write(': keepalive\n\n'), 15_000);

        const close = (): void => {
            clearInterval(keepalive);
            unsubscribe();
        };
        req.on('close', close);
        res.on('close', close);
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
