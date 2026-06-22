import request from 'supertest';
import { Application } from 'express';
import { createApp } from '../src/app';
import { createModuleApp } from '../src/moduleApp';
import { RaftNode } from '../src/consensus/raftNode';
import { LocalTransport, RpcHandler } from '../src/consensus/transport';
import { ModuleStateMachine, ModuleNode } from '../src/runtime/moduleStateMachine';
import { counter } from '../src/runtime/modules/counter';
import { BookNode, BookStateMachine } from '../src/models/bookStateMachine';
import { PeerInfo } from '../src/consensus/types';
import { buildCluster, waitFor } from './helpers';

const aBook = { title: 'T', author: 'A', publisher: 'P', copies: 1 };

/**
 * HTTP adapter behaviour that the happy-path single-node suites don't reach: the
 * not-leader routing on a follower (forward fails over LocalTransport, so the
 * controller falls back to 421), strong reads on the leader, the Raft membership
 * admin routes, and the app-level error/health endpoints.
 */
describe('HTTP routing: not-leader, strong reads, and error paths', () => {
    describe('book controller across a 3-node cluster', () => {
        let nodes: BookNode[];
        let leaderApp: Application;
        let followerApp: Application;

        beforeAll(async () => {
            nodes = buildCluster(3);
            nodes.forEach((n) => n.start());
            await waitFor(() => nodes.some((n) => n.isLeader()));
            leaderApp = createApp(nodes.find((n) => n.isLeader())!);
            followerApp = createApp(nodes.find((n) => !n.isLeader())!);
        });
        afterAll(() => nodes.forEach((n) => n.stop()));

        it('replies 421 with the leader id when a write hits a follower', async () => {
            // Forwarding over LocalTransport's `local://` URL fails, so the
            // controller falls back to 421 rather than silently dropping the write.
            const res = await request(followerApp).post('/books').send({ ...aBook, isbn: 'f1' });
            expect(res.status).toBe(421);
            expect(res.body.leader).toBeTruthy();
        });

        it('does not re-forward an already-forwarded write (X-Forwarded-By) — straight to 421', async () => {
            const res = await request(followerApp)
                .post('/books')
                .set('X-Forwarded-By', 'cluster')
                .send({ ...aBook, isbn: 'f2' });
            expect(res.status).toBe(421);
        });

        it('serves a strong (linearizable) read on the leader', async () => {
            const res = await request(leaderApp).get('/books').query({ consistency: 'strong' });
            expect(res.status).toBe(200);
            expect(Array.isArray(res.body)).toBe(true);
        });

        it('returns 404 for a missing book on the strong-read path', async () => {
            const res = await request(leaderApp).get('/books/missing').set('X-Consistency', 'strong');
            expect(res.status).toBe(404);
        });
    });

    describe('module controller across a 3-node cluster', () => {
        let nodes: ModuleNode[];
        let leaderApp: Application;
        let followerApp: Application;

        beforeAll(async () => {
            const registry = new Map<string, RpcHandler>();
            const transport = new LocalTransport(registry, 1);
            const ids = ['node1', 'node2', 'node3'];
            nodes = ids.map((id) => {
                const peers: PeerInfo[] = ids
                    .filter((p) => p !== id)
                    .map((p) => ({ id: p, url: `local://${p}` }));
                const sm = new ModuleStateMachine();
                sm.host.register(counter);
                return new RaftNode(
                    { id, peers, stateMachine: sm, electionMinMs: 50, electionMaxMs: 100, heartbeatMs: 20 },
                    transport,
                );
            });
            nodes.forEach((n) => registry.set(n.id, n));
            nodes.forEach((n) => n.start());
            await waitFor(() => nodes.some((n) => n.isLeader()));
            leaderApp = createModuleApp(nodes.find((n) => n.isLeader())!);
            followerApp = createModuleApp(nodes.find((n) => !n.isLeader())!);
        });
        afterAll(() => nodes.forEach((n) => n.stop()));

        it('replies 421 when a module command hits a follower', async () => {
            const res = await request(followerApp).post('/modules/counter/increment').send({ by: 1 });
            expect(res.status).toBe(421);
            expect(res.body.leader).toBeTruthy();
        });

        it('serves a strong query on the leader', async () => {
            const res = await request(leaderApp)
                .get('/modules/counter/query/value')
                .query({ consistency: 'strong' });
            expect(res.status).toBe(200);
        });

        it('returns 404 for an unknown query', async () => {
            const res = await request(leaderApp).get('/modules/counter/query/nope');
            expect(res.status).toBe(404);
        });
    });

    describe('raft membership admin routes', () => {
        let nodes: BookNode[];
        let leaderApp: Application;
        let followerApp: Application;

        beforeAll(async () => {
            nodes = buildCluster(3);
            nodes.forEach((n) => n.start());
            await waitFor(() => nodes.some((n) => n.isLeader()));
            leaderApp = createApp(nodes.find((n) => n.isLeader())!);
            followerApp = createApp(nodes.find((n) => !n.isLeader())!);
        });
        afterAll(() => nodes.forEach((n) => n.stop()));

        it('GET /raft/members lists the current members', async () => {
            const res = await request(leaderApp).get('/raft/members');
            expect(res.status).toBe(200);
            expect(Array.isArray(res.body)).toBe(true);
            expect(res.body.length).toBe(3);
        });

        it('rejects POST /raft/members without id/url (400)', async () => {
            const res = await request(leaderApp).post('/raft/members').send({ id: 'node4' });
            expect(res.status).toBe(400);
        });

        it('rejects removing an unknown member with a 409 MembershipError', async () => {
            const res = await request(leaderApp).delete('/raft/members/ghost');
            expect(res.status).toBe(409);
            expect(res.body.message).toMatch(/not a member/);
        });

        it('replies 421 for a membership change on a follower', async () => {
            const res = await request(followerApp).delete('/raft/members/node1');
            expect(res.status).toBe(421);
        });
    });

    describe('app-level health and metrics endpoints', () => {
        // A standalone, NOT-started node: it has no known leader yet.
        function freshNode(): BookNode {
            const registry = new Map<string, RpcHandler>();
            const transport = new LocalTransport(registry, 1);
            const node = new RaftNode({ id: 'solo', peers: [], stateMachine: new BookStateMachine() }, transport);
            registry.set(node.id, node);
            return node;
        }

        it('GET /ready is 503 before a leader is known', async () => {
            const node = freshNode();
            const res = await request(createApp(node)).get('/ready');
            expect(res.status).toBe(503);
            expect(res.body.ready).toBe(false);
        });

        it('GET /metrics is 404 when metrics are not configured', async () => {
            const node = freshNode();
            const res = await request(createApp(node)).get('/metrics');
            expect(res.status).toBe(404);
        });
    });
});
