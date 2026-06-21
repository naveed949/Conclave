import { RaftNode } from '../src/consensus/raftNode';
import { LocalTransport, RpcHandler } from '../src/consensus/transport';
import { MemoryStorage, RaftStorage } from '../src/consensus/storage';
import { InstallSnapshotArgs, PeerInfo } from '../src/consensus/types';
import { buildAddCommand } from '../src/models/book';
import { BookNode, BookStateMachine } from '../src/models/bookStateMachine';
import { waitFor } from './helpers';

const TIMERS = { electionMinMs: 50, electionMaxMs: 100, heartbeatMs: 20 };

function makeNode(
    id: string,
    peerIds: string[],
    registry: Map<string, RpcHandler>,
    opts: { snapshotThreshold?: number; storage?: RaftStorage; snapshotChunkBytes?: number } = {},
): BookNode {
    const peers: PeerInfo[] = peerIds.filter((p) => p !== id).map((p) => ({ id: p, url: `local://${p}` }));
    return new RaftNode({ id, peers, stateMachine: new BookStateMachine(), ...TIMERS, ...opts }, new LocalTransport(registry));
}

const add = (n: RaftNode, isbn: string) =>
    n.submit(buildAddCommand({ title: isbn, author: 'A', publisher: 'P', isbn, copies: 1 }), {
        requestId: isbn, actor: 'tester', timestamp: 't',
    });

describe('Log compaction (snapshotting)', () => {
    it('compacts the leader log once it exceeds the threshold, keeping state intact', async () => {
        const registry = new Map<string, RpcHandler>();
        const node = makeNode('n1', ['n1'], registry, { snapshotThreshold: 5 });
        registry.set('n1', node);
        node.start();
        await waitFor(() => node.isLeader());

        for (let i = 0; i < 12; i++) await add(node, `cmp-${i}`);

        const s = node.status();
        expect(node.stateMachine.size()).toBe(12); // all data still present
        expect(s.snapshotIndex).toBeGreaterThan(0); // a snapshot happened
        expect(s.logEntries).toBeLessThanOrEqual(6); // log was truncated near the threshold
        expect(s.commitIndex).toBe(s.lastLogIndex);
        node.stop();
    });

    it('catches a lagging follower up via InstallSnapshot', async () => {
        const registry = new Map<string, RpcHandler>();
        const ids = ['n1', 'n2', 'n3'];
        const n1 = makeNode('n1', ids, registry, { snapshotThreshold: 4 });
        const n2 = makeNode('n2', ids, registry, { snapshotThreshold: 4 });
        const n3 = makeNode('n3', ids, registry, { snapshotThreshold: 4 });

        // Start only a 2-of-3 majority; n3 is "offline" (absent from the registry).
        registry.set('n1', n1);
        registry.set('n2', n2);
        n1.start();
        n2.start();
        await waitFor(() => [n1, n2].some((n) => n.isLeader()));
        const leader = [n1, n2].find((n) => n.isLeader())!;

        // Write enough that the leader compacts past where n3 would need entries.
        for (let i = 0; i < 12; i++) await add(leader, `lag-${i}`);
        expect(leader.status().snapshotIndex).toBeGreaterThan(0);

        // n3 comes online and must be brought up to date via a snapshot.
        registry.set('n3', n3);
        n3.start();

        await waitFor(() => n3.stateMachine.size() === 12, 4000);
        expect(n3.status().snapshotIndex).toBeGreaterThan(0); // received a snapshot
        [n1, n2, n3].forEach((n) => n.stop());
    });

    it('catches a lagging follower up via a MULTI-CHUNK snapshot', async () => {
        const registry = new Map<string, RpcHandler>();
        const ids = ['n1', 'n2', 'n3'];
        // Tiny chunk size forces the snapshot to span many InstallSnapshot RPCs.
        const chunked = { snapshotThreshold: 4, snapshotChunkBytes: 64 };
        const n1 = makeNode('n1', ids, registry, chunked);
        const n2 = makeNode('n2', ids, registry, chunked);
        const n3 = makeNode('n3', ids, registry, chunked);

        registry.set('n1', n1);
        registry.set('n2', n2);
        n1.start();
        n2.start();
        await waitFor(() => [n1, n2].some((n) => n.isLeader()));
        const leader = [n1, n2].find((n) => n.isLeader())!;

        for (let i = 0; i < 12; i++) await add(leader, `multi-${i}`);
        expect(leader.status().snapshotIndex).toBeGreaterThan(0);

        // n3 comes online far behind; it must reassemble a multi-chunk snapshot.
        registry.set('n3', n3);
        n3.start();

        await waitFor(() => n3.stateMachine.size() === 12, 4000);
        expect(n3.status().snapshotIndex).toBeGreaterThan(0);
        // Converges on commit index and boundary with the leader.
        await waitFor(() => n3.status().commitIndex === leader.status().commitIndex, 4000);
        expect(n3.status().snapshotIndex).toBe(leader.status().snapshotIndex);
        [n1, n2, n3].forEach((n) => n.stop());
    });

    it('installs cleanly on retry after an interrupted (offset===0-restarted) stream', async () => {
        // A transport that drops the leader's snapshot chunks the FIRST time it
        // reaches the follower (simulating an interruption mid-stream), then lets
        // every subsequent attempt through. The leader retries from offset 0, and
        // the follower must discard its stale partial buffer and install cleanly.
        let dropFirstStream = true;
        let sawPartial = false;
        class InterruptingTransport extends LocalTransport {
            async sendInstallSnapshot(peer: PeerInfo, args: InstallSnapshotArgs) {
                if (dropFirstStream && peer.id === 'n3') {
                    // Let the first chunk reach the follower (seeding a partial
                    // buffer), then drop the rest of this stream and disable the
                    // drop so the next full attempt succeeds.
                    if (args.offset === 0 && !args.done) {
                        sawPartial = true;
                        await super.sendInstallSnapshot(peer, args);
                    }
                    dropFirstStream = false;
                    return null; // "lost" reply — leader aborts and retries
                }
                return super.sendInstallSnapshot(peer, args);
            }
        }

        const registry = new Map<string, RpcHandler>();
        const ids = ['n1', 'n2', 'n3'];
        const chunked = { snapshotThreshold: 4, snapshotChunkBytes: 64 };
        const transport = new InterruptingTransport(registry);
        const peersOf = (id: string): PeerInfo[] =>
            ids.filter((p) => p !== id).map((p) => ({ id: p, url: `local://${p}` }));
        const n1 = new RaftNode({ id: 'n1', peers: peersOf('n1'), stateMachine: new BookStateMachine(), ...TIMERS, ...chunked }, transport);
        const n2 = new RaftNode({ id: 'n2', peers: peersOf('n2'), stateMachine: new BookStateMachine(), ...TIMERS, ...chunked }, transport);
        const n3 = new RaftNode({ id: 'n3', peers: peersOf('n3'), stateMachine: new BookStateMachine(), ...TIMERS, ...chunked }, transport);

        registry.set('n1', n1);
        registry.set('n2', n2);
        n1.start();
        n2.start();
        await waitFor(() => [n1, n2].some((n) => n.isLeader()));
        const leader = [n1, n2].find((n) => n.isLeader())!;

        for (let i = 0; i < 12; i++) await add(leader, `retry-${i}`);
        await waitFor(() => leader.status().snapshotIndex > 0);

        // n3 comes online; the first snapshot stream is interrupted, the retry
        // installs cleanly with no corrupt partial state.
        registry.set('n3', n3);
        n3.start();

        await waitFor(() => n3.stateMachine.size() === 12, 6000);
        expect(sawPartial).toBe(true); // we really did interrupt a partial stream
        expect(n3.status().snapshotIndex).toBe(leader.status().snapshotIndex);
        [n1, n2, n3].forEach((n) => n.stop());
    });

    it('restores from a snapshot after restart', async () => {
        const storage = new MemoryStorage();
        const registry = new Map<string, RpcHandler>();

        const node = makeNode('n1', ['n1'], registry, { snapshotThreshold: 4, storage });
        registry.set('n1', node);
        node.start();
        await waitFor(() => node.isLeader());
        for (let i = 0; i < 10; i++) await add(node, `snap-${i}`);
        const snapIndexBefore = node.status().snapshotIndex;
        expect(snapIndexBefore).toBeGreaterThan(0);
        node.stop();

        // Restart against the same durable storage (log + snapshot files).
        const registry2 = new Map<string, RpcHandler>();
        const restarted = makeNode('n1', ['n1'], registry2, { snapshotThreshold: 4, storage });
        registry2.set('n1', restarted);
        restarted.start();
        // State machine is rebuilt from the snapshot immediately, before re-election:
        // a partial state (the snapshot subset), with the snapshot boundary preserved.
        expect(restarted.status().snapshotIndex).toBe(snapIndexBefore);
        const restoredSize = restarted.stateMachine.size();
        expect(restoredSize).toBeGreaterThan(0);
        expect(restoredSize).toBeLessThan(10);

        // After re-election it replays the post-snapshot log tail, recovering everything.
        await waitFor(() => restarted.isLeader());
        await waitFor(() => restarted.stateMachine.size() === 10);
        restarted.stop();
    });
});
