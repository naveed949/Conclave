import http from 'http';
import { Server } from 'http';
import { AddressInfo } from 'net';
import { createApp } from '../src/app';
import { RaftNode } from '../src/consensus/raftNode';
import { HttpTransport, LocalTransport } from '../src/consensus/transport';
import { Book, BookCommand, buildAddCommand } from '../src/models/book';
import { BookStateMachine } from '../src/models/bookStateMachine';
import { PeerInfo } from '../src/consensus/types';
import { EdgeReplica } from '../src/edge/edgeReplica';
import { HttpStreamSource } from '../src/edge/httpStreamSource';
import { waitFor } from './helpers';

// Real sockets + an SSE stream — generous ceiling; every wait polls a real
// condition, so the ceiling only bites when something is genuinely wrong.
jest.setTimeout(30000);

const TIMERS = { electionMinMs: 150, electionMaxMs: 300, heartbeatMs: 50 };

const listen = (s: Server): Promise<void> =>
    new Promise((r) => s.listen(0, '127.0.0.1', () => r()));

const addBook = (n: number) =>
    buildAddCommand({ title: `t${n}`, author: 'a', publisher: 'p', isbn: `isbn-${n}`, copies: 1 });

/**
 * The edge read replica (ADR-0023) end to end: a real HTTP node serving
 * `GET /raft/stream`, an {@link EdgeReplica} tailing it over the stdlib SSE
 * source, applying committed book commands to a LOCAL `BookStateMachine`, and
 * serving reads from it. Proves bootstrap, live tail, read-your-writes, snapshot
 * handoff, and follower-served read fan-out.
 */
describe('EdgeReplica over /raft/stream (ADR-0023)', () => {
    describe('single node', () => {
        let node: RaftNode<BookCommand, Book, BookStateMachine>;
        let server: Server;
        let url: string;
        let replica: EdgeReplica<BookCommand, Book> | null = null;

        const startNode = async (snapshotThreshold?: number): Promise<void> => {
            node = new RaftNode<BookCommand, Book, BookStateMachine>(
                {
                    id: 'solo',
                    peers: [],
                    stateMachine: new BookStateMachine(),
                    snapshotThreshold,
                    ...TIMERS,
                },
                new LocalTransport(new Map()),
            );
            server = http.createServer(createApp(node));
            await listen(server);
            url = `http://127.0.0.1:${(server.address() as AddressInfo).port}`;
            node.start();
            await waitFor(() => node.isLeader(), 3000);
        };

        afterEach(async () => {
            replica?.stop();
            replica = null;
            node.stop();
            await new Promise<void>((r) => server.close(() => r()));
        });

        it('bootstraps and converges to the node state, then live-tails', async () => {
            await startNode();
            for (let i = 0; i < 3; i++) await node.submit(addBook(i));

            const local = new BookStateMachine();
            replica = new EdgeReplica<BookCommand, Book>({ app: local, source: new HttpStreamSource(url) });
            replica.start();

            await waitFor(() => replica!.isCaughtUp() && local.size() === 3, 5000);
            expect(local.getAll().map((b) => b.isbn).sort()).toEqual(['isbn-0', 'isbn-1', 'isbn-2']);

            // A write after catch-up streams through and updates the local view live.
            await node.submit(addBook(99));
            await waitFor(() => local.size() === 4, 5000);
            expect(local.get(node.app.getAll().find((b) => b.isbn === 'isbn-99')!.id)).toBeDefined();
        });

        it('fires onChange and supports read-your-writes via waitForIndex', async () => {
            await startNode();
            const local = new BookStateMachine();
            replica = new EdgeReplica<BookCommand, Book>({ app: local, source: new HttpStreamSource(url) });
            let changes = 0;
            replica.onChange(() => {
                changes += 1;
            });
            replica.start();
            await waitFor(() => replica!.isCaughtUp(), 5000);

            await node.submit(addBook(1));
            // A solo node commits synchronously on submit, so commitIndex is the
            // index of the write we just made — the read-your-writes target.
            const writeIndex = node.getCommitIndex();
            await replica.waitForIndex(writeIndex, 5000);
            expect(local.size()).toBe(1);
            expect(changes).toBeGreaterThan(0);
        });

        it('bootstraps a fresh replica from a snapshot after compaction', async () => {
            await startNode(5); // low snapshot threshold
            for (let i = 0; i < 14; i++) await node.submit(addBook(i));
            expect(node.getSnapshotIndex()).toBeGreaterThan(0);

            // The replica connects AFTER compaction, so it must bootstrap from the
            // snapshot (its early entries are gone) then replay the tail.
            const local = new BookStateMachine();
            replica = new EdgeReplica<BookCommand, Book>({ app: local, source: new HttpStreamSource(url) });
            replica.start();

            await waitFor(() => replica!.isCaughtUp() && local.size() === 14, 5000);
            expect(replica.lastIndex()).toBeGreaterThanOrEqual(node.getSnapshotIndex());
            expect(local.getAll().map((b) => b.isbn).sort()).toEqual(node.app.getAll().map((b) => b.isbn).sort());
        });
    });

    describe('three-node cluster', () => {
        interface CN {
            node: RaftNode<BookCommand, Book, BookStateMachine>;
            server: Server;
            url: string;
        }
        let cluster: CN[] = [];
        let replica: EdgeReplica<BookCommand, Book> | null = null;

        afterEach(async () => {
            replica?.stop();
            replica = null;
            for (const c of cluster) c.node.stop();
            await Promise.all(cluster.map((c) => new Promise<void>((r) => c.server.close(() => r()))));
            cluster = [];
        });

        it('serves the stream from a FOLLOWER (read fan-out past the leader)', async () => {
            const ids = ['node1', 'node2', 'node3'];
            const servers = await Promise.all(
                ids.map(async () => {
                    const s = http.createServer();
                    await listen(s);
                    return s;
                }),
            );
            const urls = servers.map((s) => `http://127.0.0.1:${(s.address() as AddressInfo).port}`);
            cluster = ids.map((id, i) => {
                const peers: PeerInfo[] = ids
                    .map((pid, j) => ({ id: pid, url: urls[j] }))
                    .filter((p) => p.id !== id);
                const node = new RaftNode<BookCommand, Book, BookStateMachine>(
                    { id, peers, selfUrl: urls[i], stateMachine: new BookStateMachine(), ...TIMERS },
                    new HttpTransport(),
                );
                servers[i].on('request', createApp(node));
                return { node, server: servers[i], url: urls[i] };
            });
            cluster.forEach((c) => c.node.start());

            await waitFor(() => cluster.some((c) => c.node.isLeader()), 8000);
            const leader = cluster.find((c) => c.node.isLeader())!;
            for (let i = 0; i < 3; i++) await leader.node.submit(addBook(i));

            // Point the replica at a FOLLOWER's stream — it must still converge,
            // because committed reads are served locally from any node.
            const follower = cluster.find((c) => !c.node.isLeader())!;
            const local = new BookStateMachine();
            replica = new EdgeReplica<BookCommand, Book>({ app: local, source: new HttpStreamSource(follower.url) });
            replica.start();

            await waitFor(() => replica!.isCaughtUp() && local.size() === 3, 8000);
            expect(local.getAll().map((b) => b.isbn).sort()).toEqual(['isbn-0', 'isbn-1', 'isbn-2']);
        });
    });
});
