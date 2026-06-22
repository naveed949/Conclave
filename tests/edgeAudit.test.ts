import http from 'http';
import { Server } from 'http';
import { AddressInfo } from 'net';
import { createApp } from '../src/app';
import { RaftNode } from '../src/consensus/raftNode';
import { LocalTransport } from '../src/consensus/transport';
import { ReplicatedStateMachine } from '../src/consensus/replicatedStateMachine';
import { Book, BookCommand, buildAddCommand } from '../src/models/book';
import { BookStateMachine } from '../src/models/bookStateMachine';
import { buildBookStreamGuard } from '../src/models/bookStreamGuard';
import { EdgeReplica } from '../src/edge/edgeReplica';
import { HttpStreamSource } from '../src/edge/httpStreamSource';
import { waitFor } from './helpers';

jest.setTimeout(30000);
const TIMERS = { electionMinMs: 150, electionMaxMs: 300, heartbeatMs: 50 };

const listen = (s: Server): Promise<void> =>
    new Promise((r) => s.listen(0, '127.0.0.1', () => r()));

const book = (isbn: string, publisher = 'Acme Press') =>
    buildAddCommand({ title: `t-${isbn}`, author: 'a', publisher, isbn, copies: 1 });

/** Server-side audit head (last chained hash) for the node's replicated log. */
const serverAuditHead = (node: RaftNode<BookCommand, Book, BookStateMachine>): string => {
    const log = node.stateMachine.getAuditLog();
    return log[log.length - 1].hash;
};

/**
 * Client-side audit-chain verification on an edge replica (M28, ADR-0023). A
 * replica in `verifyAudit: true` mode rebuilds the SHA-256 audit hash-chain as it
 * applies the committed-log stream and can prove the served history is internally
 * consistent — but only on a FULL (unfiltered) stream that carries the audit data.
 */
describe('EdgeReplica audit verification (ADR-0023, M28)', () => {
    describe('full (unfiltered) stream', () => {
        let node: RaftNode<BookCommand, Book, BookStateMachine>;
        let server: Server;
        let url: string;
        let replica: EdgeReplica<BookCommand, Book> | null = null;

        beforeEach(async () => {
            node = new RaftNode<BookCommand, Book, BookStateMachine>(
                { id: 'solo', peers: [], stateMachine: new BookStateMachine(), ...TIMERS },
                new LocalTransport(new Map()),
            );
            // No streamGuard ⇒ the stream is unfiltered and the snapshot carries audit.
            server = http.createServer(createApp(node));
            await listen(server);
            url = `http://127.0.0.1:${(server.address() as AddressInfo).port}`;
            node.start();
            await waitFor(() => node.isLeader(), 3000);
        });

        afterEach(async () => {
            replica?.stop();
            replica = null;
            node.stop();
            await new Promise<void>((r) => server.close(() => r()));
        });

        it('rebuilds the chain, verifies valid, and matches the server head', async () => {
            for (let i = 0; i < 4; i++) await node.submit(book(`isbn-${i}`));

            const local = new BookStateMachine();
            replica = new EdgeReplica<BookCommand, Book>({
                app: local,
                source: new HttpStreamSource(url),
                verifyAudit: true,
            });
            replica.start();

            await waitFor(() => replica!.isCaughtUp() && local.size() === 4, 5000);
            // Reads converge.
            expect(local.getAll().map((b) => b.isbn).sort()).toEqual(
                node.app.getAll().map((b) => b.isbn).sort(),
            );

            // The replica re-derived a valid, complete chain.
            const result = replica.verifyAudit();
            expect(result).not.toBeNull();
            expect(result!.valid).toBe(true);
            expect(result!.length).toBe(node.stateMachine.getAuditLog().length);

            // And it is the SAME chain the server holds (heads match).
            expect(replica.auditHead()).not.toBeNull();
            expect(replica.auditHead()).toBe(serverAuditHead(node));
            expect(node.stateMachine.verifyAudit().valid).toBe(true);
        });

        it('keeps the chain valid and advances the head on live tail', async () => {
            await node.submit(book('isbn-0'));

            const local = new BookStateMachine();
            replica = new EdgeReplica<BookCommand, Book>({
                app: local,
                source: new HttpStreamSource(url),
                verifyAudit: true,
            });
            replica.start();
            await waitFor(() => replica!.isCaughtUp() && local.size() === 1, 5000);

            const headBefore = replica.auditHead();
            expect(headBefore).toBe(serverAuditHead(node));

            // Submit more after catch-up; the live tail extends the chain.
            await node.submit(book('isbn-1'));
            await node.submit(book('isbn-2'));
            await waitFor(() => local.size() === 3, 5000);

            const after = replica.verifyAudit();
            expect(after!.valid).toBe(true);
            expect(replica.auditHead()).not.toBe(headBefore); // head advanced
            expect(replica.auditHead()).toBe(serverAuditHead(node));
        });
    });

    /**
     * Tamper-evidence is exercised at the {@link ReplicatedStateMachine} level: the
     * edge replica's auditing mode IS a `ReplicatedStateMachine`, and a tampered
     * snapshot is far cleaner to construct directly than to inject mid-SSE-stream
     * (which would require a malicious server). We mutate one audit field WITHOUT
     * recomputing downstream hashes and assert the chain no longer verifies — the
     * exact property the client's `verifyAudit()` delegates to.
     */
    it('detects tampering: a mutated audit entry breaks verification', async () => {
        const node = new RaftNode<BookCommand, Book, BookStateMachine>(
            { id: 'solo', peers: [], stateMachine: new BookStateMachine(), ...TIMERS },
            new LocalTransport(new Map()),
        );
        node.start();
        await waitFor(() => node.isLeader(), 3000);
        for (let i = 0; i < 3; i++) await node.submit(book(`isbn-${i}`));

        // A genuine, valid RSM snapshot from the node.
        const snap = node.stateMachine.snapshot();
        expect(snap.audit.length).toBeGreaterThan(1);

        // Tamper: flip one field of a middle audit entry, leaving its `hash` (and all
        // downstream prevHash links) untouched — exactly what a forger can't fix up.
        const victim = snap.audit[1];
        const tampered = {
            ...snap,
            audit: snap.audit.map((e, i) =>
                i === 1 ? { ...e, actor: `${victim.actor}-tampered` } : { ...e },
            ),
        };

        const fresh = new ReplicatedStateMachine<BookCommand, Book>(new BookStateMachine());
        fresh.restore(tampered);
        const result = fresh.verifyAudit();
        expect(result.valid).toBe(false);
        expect(result.brokenAt).toBe(victim.index);

        node.stop();
    });

    describe('scoped stream ⇒ verification unavailable', () => {
        let node: RaftNode<BookCommand, Book, BookStateMachine>;
        let server: Server;
        let url: string;
        let replica: EdgeReplica<BookCommand, Book> | null = null;

        beforeEach(async () => {
            node = new RaftNode<BookCommand, Book, BookStateMachine>(
                { id: 'solo', peers: [], stateMachine: new BookStateMachine(), ...TIMERS },
                new LocalTransport(new Map()),
            );
            // A streamGuard ⇒ the snapshot is scoped and STRIPS audit/dedup.
            const streamGuard = buildBookStreamGuard('acme=Acme Press');
            server = http.createServer(createApp(node, { streamGuard }));
            await listen(server);
            url = `http://127.0.0.1:${(server.address() as AddressInfo).port}`;
            node.start();
            await waitFor(() => node.isLeader(), 3000);

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

        it('converges for its scope but reports verification unavailable (null)', async () => {
            const local = new BookStateMachine();
            replica = new EdgeReplica<BookCommand, Book>({
                app: local,
                source: new HttpStreamSource(url, { token: 'acme' }),
                verifyAudit: true,
            });
            replica.start();

            // It still converges to its scope (the two Acme books).
            await waitFor(() => replica!.isCaughtUp() && local.size() === 2, 5000);
            expect(local.getAll().map((b) => b.isbn).sort()).toEqual(['a1', 'a2']);

            // But verification is correctly UNAVAILABLE — not a false "valid".
            expect(replica.verifyAudit()).toBeNull();
            expect(replica.auditHead()).toBeNull();
            expect(replica.getAuditLog()).toBeNull();
        });
    });

    describe('non-auditing replica', () => {
        let node: RaftNode<BookCommand, Book, BookStateMachine>;
        let server: Server;
        let url: string;
        let replica: EdgeReplica<BookCommand, Book> | null = null;

        beforeEach(async () => {
            node = new RaftNode<BookCommand, Book, BookStateMachine>(
                { id: 'solo', peers: [], stateMachine: new BookStateMachine(), ...TIMERS },
                new LocalTransport(new Map()),
            );
            server = http.createServer(createApp(node));
            await listen(server);
            url = `http://127.0.0.1:${(server.address() as AddressInfo).port}`;
            node.start();
            await waitFor(() => node.isLeader(), 3000);
        });

        afterEach(async () => {
            replica?.stop();
            replica = null;
            node.stop();
            await new Promise<void>((r) => server.close(() => r()));
        });

        it('returns null from the audit API when verifyAudit is off', async () => {
            await node.submit(book('isbn-0'));
            const local = new BookStateMachine();
            replica = new EdgeReplica<BookCommand, Book>({ app: local, source: new HttpStreamSource(url) });
            replica.start();
            await waitFor(() => replica!.isCaughtUp() && local.size() === 1, 5000);

            expect(replica.verifyAudit()).toBeNull();
            expect(replica.auditHead()).toBeNull();
            expect(replica.getAuditLog()).toBeNull();
        });
    });
});
