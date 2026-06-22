import express, { Request, Response } from 'express';
import { RaftNode, NotLeaderError, MembershipError } from '../consensus/raftNode';
import { StateMachine } from '../consensus/stateMachine';
import { AppCommand, CommandMeta } from '../consensus/types';
import { getContext } from '../platform/requestContext';
import { forwardToLeader, isForwarded } from '../platform/forward';
import { StreamGuard, ScopedFilter, extractStreamToken } from '../edge/streamGuard';
import { MetricsRegistry } from '../platform/metrics';

/**
 * Limits that protect a node from slow or abundant `/raft/stream` consumers
 * (M27). `maxClients` caps concurrent connections per node; `maxBufferBytes`
 * caps how many bytes may sit unflushed in one connection's socket send buffer
 * before we drop it. Both have sensible defaults so omitting them is safe.
 */
export interface StreamLimits {
    /** Max concurrent stream connections on this node. Default 10000. */
    maxClients?: number;
    /** Per-connection server-side send-buffer ceiling, in bytes. Default 1 MiB. */
    maxBufferBytes?: number;
}

const DEFAULT_MAX_CLIENTS = 10_000;
const DEFAULT_MAX_BUFFER_BYTES = 1024 * 1024; // 1 MiB

/**
 * Internal cluster endpoints: peer RPCs, membership admin, and a status view.
 *
 * This is the Raft-SPECIFIC adapter: it exposes the protocol RPC endpoints
 * (`handleRequestVote`/`handleAppendEntries`/`handleInstallSnapshot`, the
 * `RpcHandler` surface) which are Raft-shaped and would be replaced *differently*
 * by a BFT engine. It is therefore intentionally typed to the concrete
 * {@link RaftNode}, NOT the engine-agnostic {@link Consensus} seam (ADR-0021).
 *
 * `streamGuard` (optional) enforces per-client authorization + partial
 * replication on `GET /raft/stream` (ADR-0023). When omitted the stream is open
 * and unfiltered (dev/trusted use); when supplied, a connection must present a
 * valid token and only ever receives its authorized scope.
 */
export default function raftRoutes<C extends AppCommand, T, SM extends StateMachine<C, T>>(
    node: RaftNode<C, T, SM>,
    streamGuard?: StreamGuard<C>,
    streamLimits?: StreamLimits,
    metrics?: MetricsRegistry,
) {
    const router = express.Router();

    const maxClients = streamLimits?.maxClients ?? DEFAULT_MAX_CLIENTS;
    const maxBufferBytes = streamLimits?.maxBufferBytes ?? DEFAULT_MAX_BUFFER_BYTES;
    // Active `/raft/stream` connections on this node — the cap is enforced
    // against this and it's decremented exactly once per connection on cleanup.
    let activeStreams = 0;

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

        // Authorize + scope the connection (ADR-0023 prereq 3). With a guard, an
        // invalid/absent token is rejected and the snapshot + entry feed are
        // restricted to the client's scope; without one, the stream is open.
        let filter: ScopedFilter<C> | null = null;
        if (streamGuard) {
            filter = streamGuard.authorize(extractStreamToken(req));
            if (!filter) {
                res.status(401).json({ message: 'Unauthorized stream' });
                return;
            }
        }

        // Per-node connection cap (M27). Check AFTER auth so a rejected token
        // never consumes a slot. At capacity we fail closed with 503 + Retry-After
        // and never open the SSE stream; the client backs off and retries (and can
        // resume from its applied index).
        if (activeStreams >= maxClients) {
            metrics?.raftStreamRejected.inc({ node: node.id });
            res.setHeader('Retry-After', '5');
            res.status(503).json({ message: 'Stream connection limit reached' });
            return;
        }
        activeStreams += 1;

        res.writeHead(200, {
            'Content-Type': 'text/event-stream',
            'Cache-Control': 'no-cache, no-transform',
            Connection: 'keep-alive',
            // Defeat proxy buffering so events flush promptly.
            'X-Accel-Buffering': 'no',
        });
        // Tell an EventSource client how long to wait before reconnecting.
        res.write('retry: 2000\n\n');

        // Cleanup runs exactly once even though it's wired to several triggers
        // (backpressure drop, `req`/`res` close): clear the keepalive, unsubscribe
        // from the commit feed, and release the connection slot.
        let closed = false;
        let unsubscribe: () => void = () => {};
        let keepalive: ReturnType<typeof setInterval> | undefined;
        const close = (): void => {
            if (closed) return;
            closed = true;
            if (keepalive) clearInterval(keepalive);
            unsubscribe();
            activeStreams -= 1;
        };

        // Drop a consumer that can't keep up: if `res.writableLength` (bytes
        // buffered server-side but not yet handed to the OS) exceeds the ceiling,
        // the client is too slow — tear the connection down rather than buffer
        // unboundedly. It can reconnect and resume from its applied index.
        const dropIfBackpressured = (): boolean => {
            if (closed) return true;
            if (res.writableLength > maxBufferBytes) {
                metrics?.raftStreamDropped.inc({ node: node.id });
                close();
                res.destroy();
                return true;
            }
            return false;
        };

        const send = (event: string, data: unknown): void => {
            if (closed) return;
            res.write(`event: ${event}\ndata: ${JSON.stringify(data)}\n\n`);
            dropIfBackpressured();
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
                if (filter) {
                    // Restrict the bootstrap state to the client's scope, and drop the
                    // RSM internals (audit/dedup) — a scoped client gets state only.
                    const scopedState = filter.filterSnapshotState((snap.data as { state?: unknown }).state);
                    send('snapshot', { ...snap, data: { state: scopedState } });
                } else {
                    send('snapshot', snap);
                }
                cursor = snap.lastIncludedIndex;
            }
        }

        // Replay the already-committed tail. The cursor advances past EVERY entry
        // (so the client stays "current"), but only IN-SCOPE entries are sent.
        for (const item of node.getCommittedEntries(cursor)) {
            if (!filter || filter.includes(item.entry)) send('entry', item);
            cursor = item.index;
        }
        send('caughtup', { index: cursor });

        // Live-tail: advance the cursor past each newly-committed entry, forwarding
        // only the in-scope ones. (SSE rides one ordered TCP connection, so no
        // in-scope entry is dropped mid-stream; on reconnect the client resumes
        // from its applied index and the server replays from there.)
        //
        // Guard on `!closed`: a backpressure drop during the synchronous catch-up
        // above already ran `close()` (with `unsubscribe` still the no-op), so
        // subscribing now would leak a listener into the node's committed feed that
        // nothing ever removes (and inflate raft_stream_subscribers forever).
        if (!closed) {
            unsubscribe = node.onCommitted((index, entry) => {
                if (closed || index <= cursor) return;
                cursor = index;
                if (!filter || filter.includes(entry)) send('entry', { index, entry });
            });
        }

        // If the catch-up replay above already overran the buffer, the connection
        // is gone — don't arm a keepalive on a dead socket.
        if (!closed) {
            // Keepalive comments stop idle intermediaries from dropping the connection.
            keepalive = setInterval(() => {
                if (closed) return;
                res.write(': keepalive\n\n');
                dropIfBackpressured();
            }, 15_000);
        }

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
