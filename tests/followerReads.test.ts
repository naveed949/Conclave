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
const followersOf = (nodes: BookNode[]) => nodes.filter((n) => !n.isLeader());

describe('Follower read offloading (ReadIndex on a follower)', () => {
    let nodes: BookNode[];
    let registry: Map<string, RpcHandler>;

    beforeEach(async () => {
        ({ nodes, registry } = cluster(3));
        nodes.forEach((n) => n.start());
        await waitFor(() => nodes.filter((n) => n.isLeader()).length === 1);
    });

    afterEach(() => nodes.forEach((n) => n.stop()));

    it('serves the latest committed write LOCALLY from a follower (no forwarding)', async () => {
        const leader = leaderOf(nodes);
        const { data } = await leader.submit(
            buildAddCommand({ title: 'F', author: 'O', publisher: 'S', isbn: 'fr-1', copies: 1 }),
        );

        // The write replicates to followers asynchronously; the follower barrier
        // is responsible for waiting until this follower has applied through it.
        const follower = followersOf(nodes)[0];
        expect(follower.isLeader()).toBe(false);

        await expect(follower.readBarrierLocal()).resolves.toBeUndefined();

        // Served from THIS follower's own local state — it was never the leader,
        // yet it returns the fresh value, proving it obtained a confirmed
        // ReadIndex and applied through it.
        expect(follower.isLeader()).toBe(false);
        expect(follower.app.get(data!.id)).toBeDefined();
    });

    it('leader path still resolves locally (readBarrierLocal delegates to readBarrier)', async () => {
        const leader = leaderOf(nodes);
        const { data } = await leader.submit(
            buildAddCommand({ title: 'L', author: 'O', publisher: 'S', isbn: 'fr-2', copies: 1 }),
        );
        await expect(leader.readBarrierLocal()).resolves.toBeUndefined();
        expect(leader.app.get(data!.id)).toBeDefined();
    });

    it('a briefly-behind follower waits until applied before serving (no stale read)', async () => {
        const leader = leaderOf(nodes);
        const follower = followersOf(nodes)[0];

        // Cut this follower off so it falls behind, commit a write through the
        // remaining majority (leader + the other follower), then reconnect.
        registry.delete(follower.id);
        const { data } = await leader.submit(
            buildAddCommand({ title: 'B', author: 'O', publisher: 'S', isbn: 'fr-3', copies: 1 }),
        );
        // Confirm the cluster committed it without this follower.
        await waitFor(() => leader.app.get(data!.id) !== undefined);
        expect(follower.app.get(data!.id)).toBeUndefined(); // still behind

        // Reconnect and issue the strong read at the stale follower. The barrier
        // must block until the follower has caught up through the read index.
        registry.set(follower.id, follower);
        await expect(follower.readBarrierLocal()).resolves.toBeUndefined();
        expect(follower.app.get(data!.id)).toBeDefined();
    });

    it('fails closed when the leader is unreachable (caller would forward/421)', async () => {
        const leader = leaderOf(nodes);
        const follower = followersOf(nodes)[0];

        // Remove the leader from the registry: the follower still thinks `leader`
        // is leader, but the ReadIndex RPC now finds no peer (returns null).
        registry.delete(leader.id);

        // Fail closed: throw rather than serve a possibly-stale local value.
        await expect(follower.readBarrierLocal()).rejects.toBeInstanceOf(NotLeaderError);
    });

    it('fails closed on a node with no known leader', async () => {
        // A freshly-built, never-started node has leaderId === null: it is not the
        // leader and knows of none, so a strong read must throw (the controller
        // would 421), never serve from empty/stale local state.
        const { nodes: fresh } = cluster(3);
        const lone = fresh[0];
        try {
            await expect(lone.readBarrierLocal()).rejects.toBeInstanceOf(NotLeaderError);
        } finally {
            fresh.forEach((n) => n.stop());
        }
    });
});
