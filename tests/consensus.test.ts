import { RaftNode } from '../src/consensus/raftNode';
import { buildAddCommand, buildBorrowCommand } from '../src/models/book';
import { buildCluster, leaders, waitFor } from './helpers';

describe('Raft consensus cluster', () => {
    let nodes: RaftNode[];

    beforeEach(() => {
        nodes = buildCluster(3);
        nodes.forEach((n) => n.start());
    });

    afterEach(() => {
        nodes.forEach((n) => n.stop());
    });

    it('elects exactly one leader that the whole cluster agrees on', async () => {
        await waitFor(() => leaders(nodes).length === 1);

        const leader = leaders(nodes)[0];
        expect(leader).toBeDefined();
        // Followers should learn who the leader is and share its term.
        await waitFor(() => nodes.every((n) => n.getLeaderId() === leader.id));
        const terms = new Set(nodes.map((n) => n.status().term));
        expect(terms.size).toBe(1);
    });

    it('replicates a committed write to every node', async () => {
        await waitFor(() => leaders(nodes).length === 1);
        const leader = leaders(nodes)[0];

        const cmd = buildAddCommand({
            title: 'Distributed Systems',
            author: 'Tanenbaum',
            publisher: 'Pearson',
            isbn: 'ISBN-001',
            copies: 3,
        });
        const result = await leader.submit(cmd);
        expect(result.status).toBe(201);
        const id = result.book!.id;

        // Every node's state machine converges on the same book.
        await waitFor(() => nodes.every((n) => n.stateMachine.get(id) !== undefined));
        for (const n of nodes) {
            expect(n.stateMachine.get(id)).toMatchObject({ title: 'Distributed Systems', copies: 3 });
        }
    });

    it('keeps logs identical across nodes after multiple writes', async () => {
        await waitFor(() => leaders(nodes).length === 1);
        const leader = leaders(nodes)[0];

        for (let i = 0; i < 5; i++) {
            await leader.submit(
                buildAddCommand({ title: `Book ${i}`, author: 'A', publisher: 'P', isbn: `ISBN-${i}`, copies: 1 }),
            );
        }

        await waitFor(() => nodes.every((n) => n.stateMachine.size() === 5));
        const sizes = new Set(nodes.map((n) => n.stateMachine.size()));
        expect(sizes).toEqual(new Set([5]));
    });

    it('elects a new leader after the current leader fails', async () => {
        await waitFor(() => leaders(nodes).length === 1);
        const oldLeader = leaders(nodes)[0];

        // Simulate a crash of the leader.
        oldLeader.stop();
        const survivors = nodes.filter((n) => n !== oldLeader);

        // A majority (2 of 3) remains, so a new leader must emerge.
        await waitFor(() => leaders(survivors).length === 1, 3000);
        const newLeader = leaders(survivors)[0];
        expect(newLeader).toBeDefined();
        expect(newLeader.id).not.toBe(oldLeader.id);

        // The new leader can still commit writes.
        const result = await newLeader.submit(
            buildBorrowCommand('missing-id', 'someone'), // book absent -> 404, but still commits through the log
        );
        expect(result.status).toBe(404);
    });
});
