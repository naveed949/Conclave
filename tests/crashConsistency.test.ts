import { RaftNode } from '../src/consensus/raftNode';
import { LocalTransport, RpcHandler, Transport } from '../src/consensus/transport';
import { MemoryStorage, RaftStorage } from '../src/consensus/storage';
import {
    InstallSnapshotArgs,
    InstallSnapshotReply,
    PeerInfo,
    RequestVoteArgs,
    RequestVoteReply,
    AppendEntriesArgs,
    AppendEntriesReply,
} from '../src/consensus/types';
import { buildAddCommand } from '../src/models/book';
import { BookNode, BookStateMachine } from '../src/models/bookStateMachine';
import { waitFor } from './helpers';

const TIMERS = { electionMinMs: 50, electionMaxMs: 100, heartbeatMs: 20 };

function makeNode(
    id: string,
    peerIds: string[],
    registry: Map<string, RpcHandler>,
    opts: { snapshotThreshold?: number; storage?: RaftStorage } = {},
): BookNode {
    const peers: PeerInfo[] = peerIds.filter((p) => p !== id).map((p) => ({ id: p, url: `local://${p}` }));
    return new RaftNode({ id, peers, stateMachine: new BookStateMachine(), ...TIMERS, ...opts }, new LocalTransport(registry));
}

const add = (n: RaftNode, isbn: string) =>
    n.submit(buildAddCommand({ title: isbn, author: 'A', publisher: 'P', isbn, copies: 1 }), {
        requestId: isbn, actor: 'tester', timestamp: 't',
    });

describe('Crash consistency between snapshot and log files', () => {
    it('reconciles a stale log when the snapshot landed but the compacted log did not', async () => {
        const storage = new MemoryStorage();
        const registry = new Map<string, RpcHandler>();
        const node = makeNode('n1', ['n1'], registry, { snapshotThreshold: 4, storage });
        registry.set('n1', node);
        node.start();
        await waitFor(() => node.isLeader());
        for (let i = 0; i < 10; i++) await add(node, `cc-${i}`);
        expect(node.status().snapshotIndex).toBeGreaterThan(0);
        node.stop();

        // Simulate a crash *after* the newest snapshot was written but *before* the
        // compacted log replaced the old one: keep the durable snapshot, but roll the
        // persisted log back to a pre-compaction state (sentinel at base 0, full log).
        const realSnap = storage.loadSnapshot()!;
        const staleLog = {
            currentTerm: node.status().term,
            votedFor: 'n1',
            // A full, uncompacted log whose sentinel base (0) lags the snapshot.
            log: [{ term: 0, command: { type: 'NOOP' as const } }],
            baseIndex: 0,
            baseTerm: 0,
        };
        storage.save(staleLog);
        expect(realSnap.lastIncludedIndex).toBeGreaterThan(0);

        // On restart the node must trust the (newer) snapshot and not corrupt its
        // index math by blindly adopting the stale log base.
        const registry2 = new Map<string, RpcHandler>();
        const restarted = makeNode('n1', ['n1'], registry2, { snapshotThreshold: 4, storage });
        registry2.set('n1', restarted);
        restarted.start();

        expect(restarted.status().snapshotIndex).toBe(realSnap.lastIncludedIndex);
        // Index math is consistent: lastLogIndex never precedes the snapshot boundary.
        expect(restarted.status().lastLogIndex).toBeGreaterThanOrEqual(realSnap.lastIncludedIndex);
        await waitFor(() => restarted.isLeader());
        // State from the snapshot survives; new writes still work on top.
        expect(restarted.stateMachine.size()).toBeGreaterThan(0);
        await add(restarted, 'after-recovery');
        expect(restarted.app.get(restarted.app.getAll().find((b) => b.isbn === 'after-recovery')!.id)).toBeDefined();
        restarted.stop();
    });
});

describe('InstallSnapshot follower safety', () => {
    it('ignores a snapshot the follower has already covered (no rollback)', () => {
        const registry = new Map<string, RpcHandler>();
        const node = makeNode('n1', ['n1', 'n2'], registry, {});
        registry.set('n1', node);
        node.start();

        const before = node.status();
        const reply = node.handleInstallSnapshot({
            term: 0,
            leaderId: 'n2',
            lastIncludedIndex: 0, // <= our boundary/commit: stale
            lastIncludedTerm: 0,
            members: [{ id: 'n1', url: 'local://n1' }, { id: 'n2', url: 'local://n2' }],
            offset: 0,
            data: JSON.stringify({ books: [], audit: [], seen: [], lastHash: '0'.repeat(64) }),
            done: true,
        } as InstallSnapshotArgs);

        expect(reply.term).toBe(before.term);
        // commitIndex/lastApplied were not rolled back.
        expect(node.status().commitIndex).toBe(before.commitIndex);
        node.stop();
    });
});

/** Transport that records the InstallSnapshot args the leader actually sends. */
class CapturingTransport implements Transport {
    sent: InstallSnapshotArgs[] = [];
    constructor(private readonly registry: Map<string, RpcHandler>) {}
    private deliver<T>(peerId: string, fn: (h: RpcHandler) => T): Promise<T | null> {
        const h = this.registry.get(peerId);
        return Promise.resolve(h ? fn(h) : null);
    }
    sendRequestVote(p: PeerInfo, a: RequestVoteArgs): Promise<RequestVoteReply | null> {
        return this.deliver(p.id, (h) => h.handleRequestVote(a));
    }
    sendAppendEntries(p: PeerInfo, a: AppendEntriesArgs): Promise<AppendEntriesReply | null> {
        return this.deliver(p.id, (h) => h.handleAppendEntries(a));
    }
    sendInstallSnapshot(p: PeerInfo, a: InstallSnapshotArgs): Promise<InstallSnapshotReply | null> {
        this.sent.push(a);
        return this.deliver(p.id, (h) => h.handleInstallSnapshot(a));
    }
}

describe('sendSnapshot ships the durable boundary', () => {
    it('labels the snapshot with lastIncludedIndex/term, not lastApplied', async () => {
        const registry = new Map<string, RpcHandler>();
        const transport = new CapturingTransport(registry);
        // n3 absent so the leader will need to InstallSnapshot it once it returns.
        const leader = new RaftNode(
            { id: 'n1', peers: [{ id: 'n2', url: 'local://n2' }, { id: 'n3', url: 'local://n3' }], stateMachine: new BookStateMachine(), ...TIMERS, snapshotThreshold: 4 },
            transport,
        );
        const n2 = new RaftNode(
            { id: 'n2', peers: [{ id: 'n1', url: 'local://n1' }, { id: 'n3', url: 'local://n3' }], stateMachine: new BookStateMachine(), ...TIMERS, snapshotThreshold: 4 },
            transport,
        );
        registry.set('n1', leader);
        registry.set('n2', n2);
        leader.start();
        n2.start();
        await waitFor(() => leader.isLeader() || n2.isLeader());
        const boss = leader.isLeader() ? leader : n2;

        for (let i = 0; i < 12; i++) await add(boss, `dur-${i}`);
        await waitFor(() => boss.status().snapshotIndex > 0);

        // Bring n3 online; it forces snapshot transfers.
        const n3 = new RaftNode(
            { id: 'n3', peers: [{ id: 'n1', url: 'local://n1' }, { id: 'n2', url: 'local://n2' }], stateMachine: new BookStateMachine(), ...TIMERS, snapshotThreshold: 4 },
            transport,
        );
        registry.set('n3', n3);
        n3.start();
        await waitFor(() => transport.sent.length > 0, 4000);

        const boundary = boss.status().snapshotIndex;
        for (const a of transport.sent) {
            // Every snapshot we shipped is labelled at a real durable boundary…
            expect(a.lastIncludedIndex).toBeLessThanOrEqual(boundary);
            // …and never at a bare lastApplied past the snapshot with a fallback term.
            expect(a.lastIncludedTerm).toBeGreaterThan(0);
        }
        await waitFor(() => n3.stateMachine.size() === 12, 4000);
        [leader, n2, n3].forEach((n) => n.stop());
    });
});
