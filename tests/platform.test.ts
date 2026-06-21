import request from 'supertest';
import { RaftNode } from '../src/consensus/raftNode';
import { LocalTransport, RpcHandler } from '../src/consensus/transport';
import { MemoryStorage, FileStorage } from '../src/consensus/storage';
import { ReplicatedStateMachine } from '../src/consensus/replicatedStateMachine';
import { LogEntry } from '../src/consensus/types';
import { MetricsRegistry } from '../src/platform/metrics';
import { createApp } from '../src/app';
import { BookCommand, buildAddCommand } from '../src/models/book';
import { BookNode, BookStateMachine } from '../src/models/bookStateMachine';
import { waitFor } from './helpers';
import fs from 'fs';
import os from 'os';
import path from 'path';

/** A book ReplicatedStateMachine (the substrate wrapping the book application). */
const bookRsm = (dedupLimit?: number) =>
    new ReplicatedStateMachine(new BookStateMachine(), dedupLimit);

const addEntry = (term: number, isbn: string, requestId: string): LogEntry<BookCommand> => ({
    term,
    command: buildAddCommand({ title: 'T', author: 'A', publisher: 'P', isbn, copies: 1 }),
    meta: { requestId, actor: 'tester', timestamp: '2026-01-01T00:00:00.000Z' },
});

describe('Audit log (hash-chained, tamper-evident)', () => {
    it('records every committed change and verifies intact', () => {
        const sm = bookRsm();
        sm.apply(1, addEntry(1, 'a-1', 'r1'));
        sm.apply(2, addEntry(1, 'a-2', 'r2'));

        const log = sm.getAuditLog();
        expect(log).toHaveLength(2);
        expect(log[0].hash).toMatch(/^[0-9a-f]{64}$/);
        expect(log[1].prevHash).toBe(log[0].hash); // chained
        expect(sm.verifyAudit()).toEqual({ valid: true, length: 2 });
    });

    it('detects tampering with a historical entry', () => {
        const sm = bookRsm();
        sm.apply(1, addEntry(1, 'b-1', 'r1'));
        sm.apply(2, addEntry(1, 'b-2', 'r2'));

        // Forge a record after the fact.
        (sm as unknown as { audit: { status: number }[] }).audit[0].status = 999;

        const result = sm.verifyAudit();
        expect(result.valid).toBe(false);
        expect(result.brokenAt).toBe(1);
    });

    it('does not audit internal NOOP entries', () => {
        const sm = bookRsm();
        sm.apply(1, { term: 1, command: { type: 'NOOP' } });
        expect(sm.getAuditLog()).toHaveLength(0);
    });
});

describe('Idempotency (exactly-once on retried requestId)', () => {
    it('returns the cached result without re-applying', () => {
        const sm = bookRsm();
        const entry = addEntry(1, 'dup-1', 'same-request');
        const first = sm.apply(1, entry);
        const second = sm.apply(2, entry); // replay

        expect(second).toEqual(first);
        expect(sm.size()).toBe(1); // not added twice
        expect(sm.getAuditLog()).toHaveLength(1); // not audited twice
    });
});

describe('Idempotency cache is bounded (deterministic FIFO eviction)', () => {
    it('caps the cache and re-applies a requestId evicted past the limit', () => {
        const sm = bookRsm(2); // remember at most 2 requestIds
        expect(sm.apply(1, addEntry(1, 'a-1', 'r1')).status).toBe(201);
        expect(sm.apply(2, addEntry(1, 'a-2', 'r2')).status).toBe(201);
        expect(sm.apply(3, addEntry(1, 'a-3', 'r3')).status).toBe(201);

        // FIFO: r1 (oldest) was evicted, the cache stays at the cap.
        expect(sm.dedupCacheSize()).toBe(2);

        // r3 is still cached -> replay returns the original result, no re-apply.
        expect(sm.apply(4, addEntry(1, 'a-3', 'r3')).status).toBe(201);

        // r1 was evicted -> its replay is NOT deduped, so it re-applies the ADD
        // and now hits the duplicate-ISBN guard (proving it went through apply()).
        expect(sm.apply(5, addEntry(1, 'a-1', 'r1')).status).toBe(400);

        expect(sm.size()).toBe(3); // still only the 3 distinct books
        expect(sm.dedupCacheSize()).toBe(2); // still bounded
    });

    it('evicts identically on every node (same sequence -> same cache)', () => {
        const a = bookRsm(3);
        const b = bookRsm(3);
        for (let i = 1; i <= 6; i++) {
            const entry = addEntry(1, `k-${i}`, `req-${i}`);
            a.apply(i, entry);
            b.apply(i, entry);
        }
        // Both kept exactly the last 3 requestIds, in the same order.
        expect(a.snapshot().seen).toEqual(b.snapshot().seen);
        expect(a.snapshot().seen.map(([id]) => id)).toEqual(['req-4', 'req-5', 'req-6']);
    });
});

describe('Persistence (state survives restart)', () => {
    const buildNode = (storage: MemoryStorage): BookNode => {
        const registry = new Map<string, RpcHandler>();
        const node = new RaftNode(
            { id: 'n1', peers: [], stateMachine: new BookStateMachine(), storage, electionMinMs: 50, electionMaxMs: 100, heartbeatMs: 20 },
            new LocalTransport(registry),
        );
        registry.set('n1', node);
        return node;
    };

    it('reloads term and log after a restart and replays state', async () => {
        const storage = new MemoryStorage();

        const node = buildNode(storage);
        node.start();
        await waitFor(() => node.isLeader());
        await node.submit(buildAddCommand({ title: 'X', author: 'A', publisher: 'P', isbn: 'persist-1', copies: 1 }), {
            requestId: 'r1', actor: 'a', timestamp: 't',
        });
        const termBefore = node.status().term;
        node.stop();

        // "Restart": a fresh node backed by the same durable storage.
        const restarted = buildNode(storage);
        restarted.start();
        expect(restarted.status().term).toBeGreaterThanOrEqual(termBefore);
        await waitFor(() => restarted.isLeader());
        await waitFor(() => restarted.stateMachine.size() === 1); // log replayed
        restarted.stop();
    });

    it('FileStorage round-trips persistent state atomically', () => {
        const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'raft-'));
        const store = new FileStorage('node1', dir);
        store.save({ currentTerm: 7, votedFor: 'node2', log: [{ term: 0, command: { type: 'NOOP' } }] });
        expect(store.load()).toEqual({ currentTerm: 7, votedFor: 'node2', log: [{ term: 0, command: { type: 'NOOP' } }] });
        fs.rmSync(dir, { recursive: true, force: true });
    });
});

describe('Observability & audit HTTP endpoints', () => {
    let node: BookNode;
    let metrics: MetricsRegistry;
    let app: ReturnType<typeof createApp>;

    beforeAll(async () => {
        const registry = new Map<string, RpcHandler>();
        node = new RaftNode(
            { id: 'n1', peers: [], stateMachine: new BookStateMachine(), electionMinMs: 50, electionMaxMs: 100, heartbeatMs: 20 },
            new LocalTransport(registry),
        );
        registry.set('n1', node);
        node.start();
        metrics = new MetricsRegistry();
        metrics.registerCollector(() => node.collectMetrics());
        app = createApp(node, { metrics });
        await waitFor(() => node.isLeader());
    });

    afterAll(() => node.stop());

    it('exposes /ready once a leader exists', async () => {
        const res = await request(app).get('/ready');
        expect(res.status).toBe(200);
        expect(res.body.ready).toBe(true);
    });

    it('serves Prometheus metrics including raft + http series', async () => {
        await request(app).get('/books'); // generate an http metric
        const res = await request(app).get('/metrics');
        expect(res.status).toBe(200);
        expect(res.text).toContain('http_requests_total');
        expect(res.text).toContain('raft_is_leader');
    });

    it('records writes in the audit log with the request actor', async () => {
        await request(app).post('/books').set('X-Actor', 'alice')
            .send({ title: 'Audited', author: 'A', publisher: 'P', isbn: 'audit-1', copies: 1 });
        const res = await request(app).get('/audit');
        expect(res.status).toBe(200);
        const entry = res.body.find((e: { type: string }) => e.type === 'ADD');
        expect(entry).toBeDefined();
        expect(entry.actor).toBe('alice');
        expect((await request(app).get('/audit/verify')).body.valid).toBe(true);
    });

    it('treats a replayed X-Request-Id as idempotent', async () => {
        const body = { title: 'Once', author: 'A', publisher: 'P', isbn: 'idem-1', copies: 1 };
        const first = await request(app).post('/books').set('X-Request-Id', 'fixed-id').send(body);
        const second = await request(app).post('/books').set('X-Request-Id', 'fixed-id').send(body);
        expect(first.status).toBe(201);
        expect(second.status).toBe(201); // not a 400 duplicate-isbn error
        expect(second.body.id).toBe(first.body.id);
        const list = await request(app).get('/books');
        expect(list.body.filter((b: { isbn: string }) => b.isbn === 'idem-1')).toHaveLength(1);
    });

    it('serves a linearizable read via ?consistency=strong on the leader', async () => {
        await request(app).post('/books')
            .send({ title: 'Strong', author: 'A', publisher: 'P', isbn: 'strong-1', copies: 1 });
        const res = await request(app).get('/books?consistency=strong');
        expect(res.status).toBe(200);
        // A single-node cluster is its own majority, so the barrier resolves and
        // the just-committed write is guaranteed visible.
        expect(res.body.some((b: { isbn: string }) => b.isbn === 'strong-1')).toBe(true);
    });
});
