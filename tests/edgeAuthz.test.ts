import http from 'http';
import { Server } from 'http';
import { AddressInfo } from 'net';
import { createApp } from '../src/app';
import { RaftNode } from '../src/consensus/raftNode';
import { LocalTransport } from '../src/consensus/transport';
import { Book, BookCommand, buildAddCommand } from '../src/models/book';
import { BookStateMachine } from '../src/models/bookStateMachine';
import { buildBookStreamGuard } from '../src/models/bookStreamGuard';
import { EdgeReplica } from '../src/edge/edgeReplica';
import { HttpStreamSource } from '../src/edge/httpStreamSource';
import { waitFor } from './helpers';

jest.setTimeout(30000);
const TIMERS = { electionMinMs: 150, electionMaxMs: 300, heartbeatMs: 50 };

const listen = (s: Server): Promise<void> => new Promise((r) => s.listen(0, '127.0.0.1', () => r()));

const book = (isbn: string, publisher: string) =>
    buildAddCommand({ title: `t-${isbn}`, author: 'a', publisher, isbn, copies: 1 });

/** Raw GET that resolves the HTTP status (for the 401 assertion). */
function statusOf(url: string): Promise<number> {
    return new Promise((resolve, reject) => {
        const req = http.get(url, (res) => {
            res.resume();
            resolve(res.statusCode ?? 0);
            req.destroy();
        });
        req.on('error', reject);
    });
}

/**
 * Per-client authorization + partial replication on the read stream (ADR-0023,
 * prerequisite 3). A StreamGuard rejects unauthorized connections and restricts a
 * scoped client to exactly its slice — snapshot AND live tail.
 */
describe('EdgeReplica authorization + partial replication (ADR-0023)', () => {
    let node: RaftNode<BookCommand, Book, BookStateMachine>;
    let server: Server;
    let url: string;
    let replica: EdgeReplica<BookCommand, Book> | null = null;

    beforeEach(async () => {
        node = new RaftNode<BookCommand, Book, BookStateMachine>(
            { id: 'solo', peers: [], stateMachine: new BookStateMachine(), ...TIMERS },
            new LocalTransport(new Map()),
        );
        // reader = all books; acme = only "Acme Press" books.
        const streamGuard = buildBookStreamGuard('reader=*,acme=Acme Press');
        server = http.createServer(createApp(node, { streamGuard }));
        await listen(server);
        url = `http://127.0.0.1:${(server.address() as AddressInfo).port}`;
        node.start();
        await waitFor(() => node.isLeader(), 3000);

        // Two Acme books, one Penguin book.
        await node.submit(book('a1', 'Acme Press'));
        await node.submit(book('a2', 'Acme Press'));
        await node.submit(book('p1', 'Penguin'));
    });

    afterEach(async () => {
        replica?.stop();
        replica = null;
        node.stop();
        await new Promise<void>((r) => server.close(() => r()));
    });

    it('rejects a stream with a missing or invalid token (401)', async () => {
        expect(await statusOf(`${url}/raft/stream?fromIndex=0`)).toBe(401);
        expect(await statusOf(`${url}/raft/stream?fromIndex=0&token=nope`)).toBe(401);
        // A valid token is accepted (200 then we drop the connection).
        expect(await statusOf(`${url}/raft/stream?fromIndex=0&token=reader`)).toBe(200);
    });

    it('serves a scoped client ONLY its publisher (snapshot + live tail)', async () => {
        const local = new BookStateMachine();
        replica = new EdgeReplica<BookCommand, Book>({
            app: local,
            source: new HttpStreamSource(url, { token: 'acme' }),
        });
        replica.start();

        // Bootstrap: only the two Acme books, never the Penguin one.
        await waitFor(() => replica!.isCaughtUp() && local.size() === 2, 5000);
        expect(local.getAll().map((b) => b.isbn).sort()).toEqual(['a1', 'a2']);
        expect(local.getAll().every((b) => b.publisher === 'Acme Press')).toBe(true);

        // Live: a new Acme book streams through; a Penguin book does NOT.
        await node.submit(book('a3', 'Acme Press'));
        await node.submit(book('p2', 'Penguin'));
        await waitFor(() => local.size() === 3, 5000);
        // Give any (incorrect) leak a chance to arrive, then assert it didn't.
        await new Promise((r) => setTimeout(r, 150));
        expect(local.size()).toBe(3);
        expect(local.getAll().map((b) => b.isbn).sort()).toEqual(['a1', 'a2', 'a3']);
    });

    it('serves an all-scope client every book', async () => {
        const local = new BookStateMachine();
        replica = new EdgeReplica<BookCommand, Book>({
            app: local,
            source: new HttpStreamSource(url, { token: 'reader' }),
        });
        replica.start();
        await waitFor(() => replica!.isCaughtUp() && local.size() === 3, 5000);
        expect(local.getAll().map((b) => b.isbn).sort()).toEqual(['a1', 'a2', 'p1']);
    });
});
