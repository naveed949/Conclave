import { RaftNode, RaftConfig } from '../src/consensus/raftNode';
import { LocalTransport, RpcHandler } from '../src/consensus/transport';
import { AppendEntriesArgs, PeerInfo } from '../src/consensus/types';
import { buildAddCommand } from '../src/models/book';
import { waitFor } from './helpers';

const TIMERS: Partial<RaftConfig> = { electionMinMs: 50, electionMaxMs: 100, heartbeatMs: 20 };

function makeNode(id: string, peerIds: string[], registry: Map<string, RpcHandler>): RaftNode {
    const peers: PeerInfo[] = peerIds.filter((p) => p !== id).map((p) => ({ id: p, url: `local://${p}` }));
    return new RaftNode({ id, peers, ...TIMERS }, new LocalTransport(registry));
}

const add = (n: RaftNode, isbn: string) =>
    n.submit(buildAddCommand({ title: isbn, author: 'A', publisher: 'P', isbn, copies: 1 }), {
        requestId: isbn, actor: 'tester', timestamp: 't',
    });

describe('Accelerated log backtracking (conflict hints)', () => {
    it('returns conflictIndex when the follower log is too short', () => {
        const registry = new Map<string, RpcHandler>();
        const node = makeNode('n1', ['n1', 'n2'], registry);
        registry.set('n1', node);
        node.start();

        // prevLogIndex far beyond the follower's (empty) log → "too short" hint.
        const reply = node.handleAppendEntries({
            term: 5, leaderId: 'n2', prevLogIndex: 9, prevLogTerm: 3, entries: [], leaderCommit: 0,
        } as AppendEntriesArgs);

        expect(reply.success).toBe(false);
        expect(reply.conflictTerm).toBeUndefined();
        expect(reply.conflictIndex).toBe(node.status().lastLogIndex + 1);
        node.stop();
    });

    it('brings a follower with a divergent log back into sync', async () => {
        const registry = new Map<string, RpcHandler>();
        const ids = ['n1', 'n2', 'n3'];
        const nodes = ids.map((id) => makeNode(id, ids, registry));
        nodes.forEach((n) => registry.set(n.id, n));
        nodes.forEach((n) => n.start());
        await waitFor(() => nodes.filter((n) => n.isLeader()).length === 1);
        const leader = nodes.find((n) => n.isLeader())!;

        // A burst of writes across (likely) multiple heartbeat terms.
        for (let i = 0; i < 20; i++) await add(leader, `bt-${i}`);

        await waitFor(() => nodes.every((n) => n.stateMachine.size() === 20), 4000);
        const sizes = new Set(nodes.map((n) => n.stateMachine.size()));
        expect(sizes).toEqual(new Set([20]));
        nodes.forEach((n) => n.stop());
    });
});
