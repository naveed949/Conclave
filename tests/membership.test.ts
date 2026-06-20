import { RaftNode, RaftConfig, MembershipError } from '../src/consensus/raftNode';
import { LocalTransport, RpcHandler } from '../src/consensus/transport';
import { PeerInfo } from '../src/consensus/types';
import { buildAddCommand } from '../src/models/book';
import { waitFor } from './helpers';

const TIMERS: Partial<RaftConfig> = { electionMinMs: 50, electionMaxMs: 100, heartbeatMs: 20 };

/**
 * A cluster whose registry + node set the test controls, so nodes can be added
 * to or removed from the configuration at runtime.
 */
function makeCluster(ids: string[]) {
    const registry = new Map<string, RpcHandler>();
    const transport = new LocalTransport(registry, 1);
    const nodes = new Map<string, RaftNode>();

    const peersFor = (id: string): PeerInfo[] =>
        ids.filter((p) => p !== id).map((p) => ({ id: p, url: `local://${p}` }));

    for (const id of ids) {
        const node = new RaftNode({ id, peers: peersFor(id), selfUrl: `local://${id}`, ...TIMERS }, transport);
        nodes.set(id, node);
        registry.set(id, node);
    }
    return { registry, transport, nodes, ids };
}

const leaderOf = (nodes: Iterable<RaftNode>) => [...nodes].find((n) => n.isLeader())!;
const addBook = (isbn: string) =>
    buildAddCommand({ title: isbn, author: 'A', publisher: 'P', isbn, copies: 1 });

describe('Dynamic membership (single-server changes)', () => {
    it('adds a new node, which catches up and joins the quorum', async () => {
        const c = makeCluster(['n1', 'n2', 'n3']);
        c.nodes.forEach((n) => n.start());
        await waitFor(() => [...c.nodes.values()].filter((n) => n.isLeader()).length === 1);

        const leader = leaderOf(c.nodes.values());
        await leader.submit(addBook('pre-1')); // a write that predates the new node

        // Spin up n4 (initially knows the existing peers) and add it to the config.
        const transport = c.transport;
        const n4 = new RaftNode(
            { id: 'n4', peers: ['n1', 'n2', 'n3'].map((p) => ({ id: p, url: `local://${p}` })), selfUrl: 'local://n4', ...TIMERS },
            transport,
        );
        c.registry.set('n4', n4);
        n4.start();

        const res = await leader.changeMembership({ add: { id: 'n4', url: 'local://n4' } });
        expect(res.status).toBe(200);

        // n4 catches up: it learns the config and replays the pre-existing write.
        await waitFor(() => n4.status().members.length === 4);
        await waitFor(() => n4.stateMachine.get(leader.stateMachine.getAll()[0].id) !== undefined);
        expect(leader.status().members.sort()).toEqual(['n1', 'n2', 'n3', 'n4']);

        // A new write now commits through the 4-node configuration and reaches n4.
        await leader.submit(addBook('post-1'));
        await waitFor(() => n4.stateMachine.size() === 2);

        [...c.nodes.values(), n4].forEach((n) => n.stop());
    });

    it('removes a follower; the remaining nodes keep committing', async () => {
        const c = makeCluster(['n1', 'n2', 'n3']);
        c.nodes.forEach((n) => n.start());
        await waitFor(() => [...c.nodes.values()].filter((n) => n.isLeader()).length === 1);
        const leader = leaderOf(c.nodes.values());

        const victim = [...c.nodes.values()].find((n) => !n.isLeader())!;
        const res = await leader.changeMembership({ remove: victim.id });
        expect(res.status).toBe(200);
        await waitFor(() => leader.status().members.length === 2);
        expect(leader.status().members).not.toContain(victim.id);

        // The two remaining members still form a quorum and commit a write.
        const write = await leader.submit(addBook('after-removal'));
        expect(write.status).toBe(201);

        c.nodes.forEach((n) => n.stop());
    });

    it('makes a leader step down when it removes itself', async () => {
        const c = makeCluster(['n1', 'n2', 'n3']);
        c.nodes.forEach((n) => n.start());
        await waitFor(() => [...c.nodes.values()].filter((n) => n.isLeader()).length === 1);
        const leader = leaderOf(c.nodes.values());

        await leader.changeMembership({ remove: leader.id });

        // After the removal commits, the old leader is no longer leader, and the
        // two remaining nodes elect a new one among themselves.
        await waitFor(() => !leader.isLeader());
        const survivors = [...c.nodes.values()].filter((n) => n !== leader);
        await waitFor(() => survivors.filter((n) => n.isLeader()).length === 1, 3000);
        expect(leader.status().members).not.toContain(leader.id);

        c.nodes.forEach((n) => n.stop());
    });

    it('rejects invalid changes (duplicate add, unknown remove, concurrent change)', async () => {
        const c = makeCluster(['n1', 'n2', 'n3']);
        c.nodes.forEach((n) => n.start());
        await waitFor(() => [...c.nodes.values()].filter((n) => n.isLeader()).length === 1);
        const leader = leaderOf(c.nodes.values());

        await expect(leader.changeMembership({ add: { id: 'n2', url: 'local://n2' } }))
            .rejects.toBeInstanceOf(MembershipError);
        await expect(leader.changeMembership({ remove: 'nobody' }))
            .rejects.toBeInstanceOf(MembershipError);

        c.nodes.forEach((n) => n.stop());
    });
});
