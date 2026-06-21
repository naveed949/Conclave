import { RaftNode, NotLeaderError } from '../src/consensus/raftNode';
import { LocalTransport, RpcHandler } from '../src/consensus/transport';
import { PeerInfo } from '../src/consensus/types';
import { buildAddCommand } from '../src/models/book';
import { BookNode, BookStateMachine } from '../src/models/bookStateMachine';
import { waitFor } from './helpers';

const TIMERS = { electionMinMs: 50, electionMaxMs: 100, heartbeatMs: 20 };

/** A cluster whose registry the test controls, so peers can be "partitioned". */
function cluster(size: number) {
    const registry = new Map<string, RpcHandler>();
    const transport = new LocalTransport(registry, 1);
    const ids = Array.from({ length: size }, (_, i) => `node${i + 1}`);
    const nodes = ids.map((id) => {
        const peers: PeerInfo[] = ids.filter((p) => p !== id).map((p) => ({ id: p, url: `local://${p}` }));
        return new RaftNode({ id, peers, stateMachine: new BookStateMachine(), ...TIMERS }, transport);
    });
    nodes.forEach((n) => registry.set(n.id, n));
    return { nodes, registry };
}

const leaderOf = (nodes: BookNode[]) => nodes.find((n) => n.isLeader())!;

describe('Linearizable reads (ReadIndex barrier)', () => {
    let nodes: BookNode[];
    let registry: Map<string, RpcHandler>;

    beforeEach(async () => {
        ({ nodes, registry } = cluster(3));
        nodes.forEach((n) => n.start());
        await waitFor(() => nodes.filter((n) => n.isLeader()).length === 1);
    });

    afterEach(() => nodes.forEach((n) => n.stop()));

    it('resolves on a healthy leader so the latest committed write is visible', async () => {
        const leader = leaderOf(nodes);
        const { data } = await leader.submit(
            buildAddCommand({ title: 'R', author: 'O', publisher: 'S', isbn: 'rb-1', copies: 1 }),
        );

        await expect(leader.readBarrier()).resolves.toBeUndefined();
        expect(leader.app.get(data!.id)).toBeDefined();
    });

    it('rejects with NotLeaderError when the leader cannot reach a quorum', async () => {
        const leader = leaderOf(nodes);

        // Partition the leader away from both followers (RPCs now find no peer).
        for (const n of nodes) if (n !== leader) registry.delete(n.id);

        // It still believes it is leader, but cannot confirm leadership, so a
        // linearizable read must refuse rather than risk serving a stale value.
        await expect(leader.readBarrier()).rejects.toThrow('NOT_LEADER');
    });

    it('rejects immediately on a follower (caller forwards to the leader)', async () => {
        const follower = nodes.find((n) => !n.isLeader())!;
        await expect(follower.readBarrier()).rejects.toBeInstanceOf(NotLeaderError);
    });
});
