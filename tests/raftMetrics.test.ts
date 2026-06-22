import { RaftNode } from '../src/consensus/raftNode';
import { LocalTransport, RpcHandler } from '../src/consensus/transport';
import { PeerInfo } from '../src/consensus/types';
import { MetricsRegistry } from '../src/platform/metrics';
import { buildAddCommand } from '../src/models/book';
import { BookNode, BookStateMachine } from '../src/models/bookStateMachine';
import { waitFor } from './helpers';

const TIMERS = { electionMinMs: 50, electionMaxMs: 100, heartbeatMs: 20 };

/**
 * Exercises `RaftNode.collectMetrics()` — the scrape-time gauge updater — and in
 * particular the leader-only per-peer replication-lag series and its reset, which
 * the existing single-node metrics test does not reach (no peers) and the
 * membership tests do not reach (no metrics attached).
 */
describe('RaftNode.collectMetrics', () => {
    let nodes: BookNode[];
    let metrics: MetricsRegistry;

    function lagPeers(text: string): string[] {
        return [...text.matchAll(/raft_replication_lag\{[^}]*peer="([^"]+)"/g)].map((m) => m[1]);
    }

    beforeEach(async () => {
        const registry = new Map<string, RpcHandler>();
        const transport = new LocalTransport(registry, 1);
        const ids = ['node1', 'node2', 'node3'];
        metrics = new MetricsRegistry();
        nodes = ids.map((id) => {
            const peers: PeerInfo[] = ids.filter((p) => p !== id).map((p) => ({ id: p, url: `local://${p}` }));
            // Only the leader's series matter here, but every node carries a registry
            // so whichever wins the election has one.
            return new RaftNode(
                { id, peers, stateMachine: new BookStateMachine(), metrics, ...TIMERS },
                transport,
            );
        });
        nodes.forEach((n) => registry.set(n.id, n));
        nodes.forEach((n) => n.start());
        await waitFor(() => nodes.filter((n) => n.isLeader()).length === 1);
    });

    afterEach(() => nodes.forEach((n) => n.stop()));

    it('emits a per-peer replication-lag series for each follower on the leader', async () => {
        const leader = nodes.find((n) => n.isLeader())!;
        await leader.submit(buildAddCommand({ title: 'M', author: 'A', publisher: 'P', isbn: 'm-1', copies: 1 }));

        leader.collectMetrics(); // production wires this as a scrape-time collector
        const text = metrics.expose();
        const peers = lagPeers(text).sort();
        // Two followers, each with its own lag series, none referring to the leader.
        expect(peers.length).toBe(2);
        expect(peers).not.toContain(leader.id);
        expect(text).toMatch(/raft_cluster_size\S* 3/);
    });

    it('drops the lag series for a member that has been removed', async () => {
        const leader = nodes.find((n) => n.isLeader())!;
        const victim = nodes.find((n) => !n.isLeader())!;

        await leader.submit(buildAddCommand({ title: 'M', author: 'A', publisher: 'P', isbn: 'm-2', copies: 1 }));
        leader.collectMetrics();
        expect(lagPeers(metrics.expose())).toContain(victim.id);

        // Remove a follower; the lag gauge is rebuilt each scrape from the current
        // membership, so the departed peer's series must disappear (no stale gauge).
        await leader.changeMembership({ remove: victim.id });
        await waitFor(() => leader.getMembers().every((m) => m.id !== victim.id));
        victim.stop();

        leader.collectMetrics();
        const peers = lagPeers(metrics.expose());
        expect(peers).not.toContain(victim.id);
        expect(peers.length).toBe(1);
    });
});
