import { RaftNode, RaftConfig } from '../src/consensus/raftNode';
import { LocalTransport, RpcHandler } from '../src/consensus/transport';
import { MemoryStorage } from '../src/consensus/storage';
import { PeerInfo } from '../src/consensus/types';
import { buildAddCommand } from '../src/models/book';
import { waitFor } from './helpers';

const TIMERS: Partial<RaftConfig> = { electionMinMs: 50, electionMaxMs: 100, heartbeatMs: 20 };

function makeNode(
    id: string,
    peerIds: string[],
    registry: Map<string, RpcHandler>,
    opts: Partial<RaftConfig> = {},
): RaftNode {
    const peers: PeerInfo[] = peerIds.filter((p) => p !== id).map((p) => ({ id: p, url: `local://${p}` }));
    return new RaftNode({ id, peers, ...TIMERS, ...opts }, new LocalTransport(registry));
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
