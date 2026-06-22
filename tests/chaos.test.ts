import http from 'http';
import { AddressInfo } from 'net';
import { Server } from 'http';
import { Application } from 'express';
import { createApp } from '../src/app';
import { RaftNode } from '../src/consensus/raftNode';
import { HttpTransport } from '../src/consensus/transport';
import { BookStateMachine } from '../src/models/bookStateMachine';
import { PeerInfo, AppendEntriesArgs, AppendEntriesReply, InstallSnapshotArgs, InstallSnapshotReply, ReadIndexArgs, ReadIndexReply, RequestVoteArgs, RequestVoteReply } from '../src/consensus/types';
import { waitFor } from './helpers';

// Real sockets + real elections are slower than LocalTransport — give the suite
// plenty of headroom. Each scenario polls on real conditions (never fixed sleeps),
// so the generous ceiling only matters when something is genuinely wrong.
jest.setTimeout(30000);

// Fast-but-realistic Raft timers. The election window sits comfortably above the
// heartbeat and the HttpTransport's 100ms RPC timeout, so elections converge over
// real sockets without thrashing.
const TIMERS = {
    electionMinMs: 150,
    electionMaxMs: 300,
    heartbeatMs: 50,
};

/**
 * An HttpTransport that can drop traffic to specific peers, simulating a network
 * partition. Blocking is keyed by peer id; a blocked send resolves to `null` (the
 * same "no reply" the real transport returns on a network error), so the node
 * treats the peer as unreachable without throwing.
 */
class PartitionableHttpTransport extends HttpTransport {
    private readonly blocked = new Set<string>();

    block(peerId: string): void {
        this.blocked.add(peerId);
    }

    unblock(peerId: string): void {
        this.blocked.delete(peerId);
    }

    sendRequestVote(peer: PeerInfo, args: RequestVoteArgs): Promise<RequestVoteReply | null> {
        if (this.blocked.has(peer.id)) return Promise.resolve(null);
        return super.sendRequestVote(peer, args);
    }

    sendAppendEntries(peer: PeerInfo, args: AppendEntriesArgs): Promise<AppendEntriesReply | null> {
        if (this.blocked.has(peer.id)) return Promise.resolve(null);
        return super.sendAppendEntries(peer, args);
    }

    sendInstallSnapshot(peer: PeerInfo, args: InstallSnapshotArgs): Promise<InstallSnapshotReply | null> {
        if (this.blocked.has(peer.id)) return Promise.resolve(null);
        return super.sendInstallSnapshot(peer, args);
    }

    sendReadIndex(peer: PeerInfo, args: ReadIndexArgs): Promise<ReadIndexReply | null> {
        if (this.blocked.has(peer.id)) return Promise.resolve(null);
        return super.sendReadIndex(peer, args);
    }
}

/** One node of the real-socket cluster. */
interface ClusterNode {
    id: string;
    node: RaftNode<any, any, BookStateMachine>;
    app: Application;
    server: Server;
    transport: PartitionableHttpTransport;
    port: number;
    url: string;
}

/**
 * Build a real `size`-node cluster over actual HTTP sockets. Two passes: first
 * open listeners on OS-assigned ephemeral ports and read back the real ports, THEN
 * construct the nodes with peer URLs pointing at those ports. Nodes are NOT started
 * here — the caller starts them so each scenario controls election timing.
 */
async function buildHttpCluster(size: number): Promise<ClusterNode[]> {
    const ids = Array.from({ length: size }, (_, i) => `node${i + 1}`);

    // Pass 1: open a listener per node and discover its port.
    const servers = await Promise.all(
        ids.map(
            () =>
                new Promise<Server>((resolve) => {
                    const server = http.createServer();
                    server.listen(0, '127.0.0.1', () => resolve(server));
                }),
        ),
    );
    const ports = servers.map((s) => (s.address() as AddressInfo).port);
    const urls = ports.map((p) => `http://127.0.0.1:${p}`);

    // Pass 2: build nodes with peer lists pointing at the now-known ports, then
    // mount the app onto each pre-opened server (attach request handler).
    const cluster: ClusterNode[] = ids.map((id, i) => {
        const peers: PeerInfo[] = ids
            .map((pid, j) => ({ id: pid, url: urls[j] }))
            .filter((p) => p.id !== id);
        const transport = new PartitionableHttpTransport();
        const node = new RaftNode(
            { id, peers, selfUrl: urls[i], stateMachine: new BookStateMachine(), ...TIMERS },
            transport,
        );
        const app = createApp(node);
        // The server is already listening; attach the express app as its handler.
        servers[i].on('request', app);
        return { id, node, app, server: servers[i], transport, port: ports[i], url: urls[i] };
    });

    return cluster;
}

/** Start every node's Raft loop. */
function startAll(cluster: ClusterNode[]): void {
    for (const c of cluster) c.node.start();
}

/** The current leaders in the (sub)cluster. */
function leaders(cluster: ClusterNode[]): ClusterNode[] {
    return cluster.filter((c) => c.node.isLeader());
}

/** Stop a node's Raft loop and close its HTTP server (awaiting the close). */
async function tearDownNode(c: ClusterNode): Promise<void> {
    c.node.stop();
    await new Promise<void>((resolve) => c.server.close(() => resolve()));
}

/** Minimal JSON HTTP client over Node's http module (no supertest, real sockets). */
function httpRequest(
    method: string,
    url: string,
    body?: unknown,
): Promise<{ status: number; body: any }> {
    return new Promise((resolve, reject) => {
        const target = new URL(url);
        const payload = body === undefined ? undefined : JSON.stringify(body);
        const req = http.request(
            {
                hostname: target.hostname,
                port: target.port,
                path: target.pathname + target.search,
                method,
                headers: {
                    'Content-Type': 'application/json',
                    ...(payload ? { 'Content-Length': Buffer.byteLength(payload) } : {}),
                },
                timeout: 5000,
            },
            (res) => {
                let data = '';
                res.on('data', (chunk) => (data += chunk));
                res.on('end', () => {
                    let parsed: any = undefined;
                    try {
                        parsed = data.length ? JSON.parse(data) : undefined;
                    } catch {
                        parsed = data;
                    }
                    resolve({ status: res.statusCode ?? 0, body: parsed });
                });
            },
        );
        req.on('error', reject);
        req.on('timeout', () => req.destroy(new Error('request timeout')));
        if (payload) req.write(payload);
        req.end();
    });
}

const SAMPLE_BOOK = {
    title: 'In Search of an Understandable Consensus Algorithm',
    author: 'Ongaro & Ousterhout',
    publisher: 'USENIX',
    isbn: 'RAFT-2014',
    copies: 3,
};

describe('chaos: real 3-node HTTP cluster', () => {
    let cluster: ClusterNode[];

    beforeEach(async () => {
        cluster = await buildHttpCluster(3);
        startAll(cluster);
    });

    afterEach(async () => {
        // Stop every Raft loop and close every server so no timers/sockets leak
        // (the suite runs under --detectOpenHandles --forceExit).
        await Promise.all(cluster.map((c) => tearDownNode(c).catch(() => undefined)));
    });

    it('1. elects exactly one leader', async () => {
        await waitFor(() => leaders(cluster).length === 1, 10000);
        expect(leaders(cluster)).toHaveLength(1);

        // The whole cluster agrees on the leader and shares a single term.
        const leader = leaders(cluster)[0];
        await waitFor(() => cluster.every((c) => c.node.getLeaderId() === leader.id), 10000);
        const terms = new Set(cluster.map((c) => c.node.status().term));
        expect(terms.size).toBe(1);
    });

    it('2. replicates a write so it is readable on a different node', async () => {
        await waitFor(() => leaders(cluster).length === 1, 10000);
        const leader = leaders(cluster)[0];

        const res = await httpRequest('POST', `${leader.url}/books`, SAMPLE_BOOK);
        expect(res.status).toBe(201);
        const id = res.body.id;
        expect(id).toBeDefined();

        // Read it back from a DIFFERENT node (eventual consistency via replication).
        const follower = cluster.find((c) => c.id !== leader.id)!;
        await waitFor(() => follower.node.app.get(id) !== undefined, 10000);
        const got = await httpRequest('GET', `${follower.url}/books/${id}`);
        expect(got.status).toBe(200);
        expect(got.body).toMatchObject({ title: SAMPLE_BOOK.title, copies: SAMPLE_BOOK.copies });
    });

    it('3. recovers from leader failure and commits a new write', async () => {
        await waitFor(() => leaders(cluster).length === 1, 10000);
        const oldLeader = leaders(cluster)[0];

        // Commit one write through the original leader first.
        const first = await httpRequest('POST', `${oldLeader.url}/books`, SAMPLE_BOOK);
        expect(first.status).toBe(201);

        // Crash the leader: stop its Raft loop and close its HTTP server.
        await tearDownNode(oldLeader);
        const survivors = cluster.filter((c) => c.id !== oldLeader.id);

        // A majority (2 of 3) remains, so a new leader must emerge among survivors.
        await waitFor(() => leaders(survivors).length === 1, 10000);
        const newLeader = leaders(survivors)[0];
        expect(newLeader.id).not.toBe(oldLeader.id);

        // A NEW write commits through the 2-node majority.
        const second = await httpRequest('POST', `${newLeader.url}/books`, {
            ...SAMPLE_BOOK,
            isbn: 'RAFT-AFTER-FAILOVER',
        });
        expect(second.status).toBe(201);
        const id = second.body.id;
        const otherSurvivor = survivors.find((c) => c.id !== newLeader.id)!;
        await waitFor(() => otherSurvivor.node.app.get(id) !== undefined, 10000);
    });

    it('4. tolerates a partition: majority commits, minority cannot, then heals', async () => {
        await waitFor(() => leaders(cluster).length === 1, 10000);
        const leader = leaders(cluster)[0];

        // Isolate a FOLLOWER into the minority so the leader keeps its majority.
        const minority = cluster.find((c) => c.id !== leader.id)!;
        const majority = cluster.filter((c) => c.id !== minority.id);

        // Two-way partition: nobody talks to the minority node, and it talks to nobody.
        for (const c of cluster) {
            if (c.id === minority.id) {
                for (const other of cluster) if (other.id !== minority.id) c.transport.block(other.id);
            } else {
                c.transport.block(minority.id);
            }
        }

        // The majority side still has a leader and commits a write.
        await waitFor(() => leaders(majority).length === 1, 10000);
        const majLeader = leaders(majority)[0];
        const res = await httpRequest('POST', `${majLeader.url}/books`, {
            ...SAMPLE_BOOK,
            isbn: 'RAFT-PARTITION',
        });
        expect(res.status).toBe(201);
        const id = res.body.id;

        // The isolated minority node cannot have applied the write (no majority).
        expect(minority.node.app.get(id)).toBeUndefined();

        // Heal the partition; the minority node catches up and converges.
        for (const c of cluster) {
            for (const other of cluster) if (other.id !== c.id) c.transport.unblock(other.id);
        }
        await waitFor(() => minority.node.app.get(id) !== undefined, 10000);
        expect(minority.node.app.get(id)).toMatchObject({ isbn: 'RAFT-PARTITION' });
    });

    it('5. serves a strong (linearizable) read against the leader', async () => {
        await waitFor(() => leaders(cluster).length === 1, 10000);
        const leader = leaders(cluster)[0];

        const created = await httpRequest('POST', `${leader.url}/books`, {
            ...SAMPLE_BOOK,
            isbn: 'RAFT-STRONG',
        });
        expect(created.status).toBe(201);
        const id = created.body.id;

        // A ?consistency=strong read goes through the leader's ReadIndex barrier
        // (heartbeat-confirmed leadership) over real HTTP and reflects the write.
        const strong = await httpRequest('GET', `${leader.url}/books/${id}?consistency=strong`);
        expect(strong.status).toBe(200);
        expect(strong.body).toMatchObject({ isbn: 'RAFT-STRONG', title: SAMPLE_BOOK.title });
    });
});
