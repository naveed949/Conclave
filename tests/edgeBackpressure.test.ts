import http from 'http';
import { Server } from 'http';
import { AddressInfo } from 'net';
import { createApp } from '../src/app';
import { RaftNode } from '../src/consensus/raftNode';
import { LocalTransport } from '../src/consensus/transport';
import { Book, BookCommand, buildAddCommand } from '../src/models/book';
import { BookStateMachine } from '../src/models/bookStateMachine';
import { waitFor } from './helpers';

// Real sockets + an SSE stream. Generous ceiling; every wait polls a real
// condition, so the ceiling only bites when something is genuinely wrong.
jest.setTimeout(30000);

/** Poll an async predicate until it returns true or the timeout elapses. */
async function waitForAsync(predicate: () => boolean | Promise<boolean>, timeoutMs = 5000, stepMs = 25): Promise<void> {
    const start = Date.now();
    while (Date.now() - start < timeoutMs) {
        if (await predicate()) return;
        await new Promise((r) => setTimeout(r, stepMs));
    }
    throw new Error('waitForAsync: condition not met within timeout');
}

const TIMERS = { electionMinMs: 150, electionMaxMs: 300, heartbeatMs: 50 };

const listen = (s: Server): Promise<void> =>
    new Promise((r) => s.listen(0, '127.0.0.1', () => r()));

/** A book whose title is `size` bytes, so a handful of entries overrun a small buffer. */
const bigBook = (n: number, size: number) =>
    buildAddCommand({
        title: 'x'.repeat(size),
        author: 'y'.repeat(size),
        publisher: 'p',
        isbn: `isbn-${n}`,
        copies: 1,
    });

/**
 * M27: protect a node from slow / abundant `/raft/stream` consumers — a per-node
 * connection cap (503 + Retry-After when full) and a backpressure drop (the server
 * tears down a connection whose send buffer overruns the ceiling).
 */
describe('Edge stream backpressure + connection cap (M27)', () => {
    let node: RaftNode<BookCommand, Book, BookStateMachine>;
    let server: Server;
    let url: string;
    const openSockets: http.ClientRequest[] = [];

    const startNode = async (streamLimits?: { maxClients?: number; maxBufferBytes?: number }): Promise<void> => {
        node = new RaftNode<BookCommand, Book, BookStateMachine>(
            { id: 'solo', peers: [], stateMachine: new BookStateMachine(), ...TIMERS },
            new LocalTransport(new Map()),
        );
        server = http.createServer(createApp(node, { streamLimits }));
        await listen(server);
        url = `http://127.0.0.1:${(server.address() as AddressInfo).port}`;
        node.start();
        await waitFor(() => node.isLeader(), 3000);
    };

    afterEach(async () => {
        for (const req of openSockets) req.destroy();
        openSockets.length = 0;
        node.stop();
        await new Promise<void>((r) => server.close(() => r()));
    });

    /** Open a long-lived SSE connection; resolve with the response once headers arrive. */
    const openStream = (consume: boolean): Promise<http.IncomingMessage> =>
        new Promise((resolve, reject) => {
            const req = http.get(`${url}/raft/stream?fromIndex=0`, (res) => {
                if (consume) res.resume();
                else res.pause();
                // The server may abruptly destroy the socket (backpressure drop);
                // swallow the resulting client-side error so it doesn't surface as
                // an unhandled 'error' once we've already resolved with the response.
                res.on('error', () => undefined);
                resolve(res);
            });
            req.on('error', () => undefined);
            openSockets.push(req);
        });

    /** Raw GET resolving just the status code. */
    const statusOf = (): Promise<number> =>
        new Promise((resolve, reject) => {
            const req = http.get(`${url}/raft/stream?fromIndex=0`, (res) => {
                res.resume();
                resolve(res.statusCode ?? 0);
                req.destroy();
            });
            req.on('error', reject);
        });

    it('caps concurrent connections (503 + Retry-After) and frees a slot on close', async () => {
        await startNode({ maxClients: 2 });

        // Two long-lived streams fill the cap.
        const s1 = await openStream(true);
        const s2 = await openStream(true);
        expect(s1.statusCode).toBe(200);
        expect(s2.statusCode).toBe(200);

        // A third connection is rejected with 503 + Retry-After.
        const rejected: { status: number; retryAfter?: string } = await new Promise((resolve, reject) => {
            const req = http.get(`${url}/raft/stream?fromIndex=0`, (res) => {
                res.resume();
                resolve({ status: res.statusCode ?? 0, retryAfter: res.headers['retry-after'] as string | undefined });
                req.destroy();
            });
            req.on('error', reject);
        });
        expect(rejected.status).toBe(503);
        expect(rejected.retryAfter).toBeDefined();

        // Close one connection; the slot is freed and a new connection succeeds.
        const closedSocket = openSockets.shift()!;
        closedSocket.destroy();
        // Server-side decrement happens on the socket 'close' event; poll until a slot frees.
        await waitForAsync(async () => (await statusOf()) === 200, 5000);
    });

    it('drops a slow consumer whose send buffer overruns the ceiling', async () => {
        await startNode({ maxBufferBytes: 1024 });

        // Open a stream but never read it: the client socket stays paused, so the
        // server's writableLength grows as we push entries through.
        const res = await openStream(false);
        expect(res.statusCode).toBe(200);

        let serverClosed = false;
        res.on('close', () => { serverClosed = true; });
        res.on('end', () => { serverClosed = true; });

        // Commit large entries until the server's per-connection send buffer
        // overruns the ceiling. The client never reads, so once the kernel socket
        // buffer fills, `res.writableLength` grows past 1 KiB and the server drops
        // the connection. We push generously (large payloads, many of them) so the
        // drop is deterministic rather than timing-dependent.
        let n = 0;
        await waitForAsync(async () => {
            if (!serverClosed) {
                for (let i = 0; i < 5; i++) await node.submit(bigBook(n++, 64 * 1024));
            }
            return serverClosed;
        }, 15000);

        expect(serverClosed).toBe(true);
    });

    it('does not leak a committed-log listener when dropped DURING catch-up (B1 regression)', async () => {
        // A negative ceiling makes the FIRST write during catch-up overrun
        // (`res.writableLength` is >= 0 > -1), so the connection is dropped
        // synchronously DURING catch-up — before the live-tail subscription is set
        // up. This deterministically exercises the path where a missing `if (!closed)`
        // guard would register an onCommitted listener that nothing ever removes.
        await startNode({ maxBufferBytes: -1 });

        // At least one committed entry so the catch-up replay actually sends (and so
        // triggers the drop) before the subscription.
        await node.submit(bigBook(0, 16));
        expect(node.streamSubscriberCount()).toBe(0);

        // Fire the request. The server drops the connection during catch-up — so
        // abruptly it may reset the socket around the headers — so we settle on
        // EITHER a response-close OR a connection error; the server has run the
        // handler (and made its subscribe-or-not decision) by the time either fires.
        await new Promise<void>((resolve) => {
            const req = http.get(`${url}/raft/stream?fromIndex=0`, (res) => {
                res.resume();
                res.on('error', () => undefined);
                res.on('close', () => resolve());
                res.on('end', () => resolve());
            });
            req.on('error', () => resolve());
            openSockets.push(req);
        });

        // It must NOT have left a committed-log listener behind (which would also
        // inflate the subscribers gauge forever). Poll briefly to let any (incorrect)
        // late subscription settle, then assert the feed is empty.
        await waitForAsync(() => node.streamSubscriberCount() === 0, 2000);
        expect(node.streamSubscriberCount()).toBe(0);
    });
});
