import { RaftNode } from '../src/consensus/raftNode';
import { LocalTransport, RpcHandler } from '../src/consensus/transport';
import { LogEntry } from '../src/consensus/types';
import { BookCommand, buildAddCommand } from '../src/models/book';
import { Book } from '../src/models/book';
import { BookNode, BookStateMachine } from '../src/models/bookStateMachine';
import { buildCluster, waitFor, leaders } from './helpers';

/**
 * The committed-log read stream (ADR-0023) — the consensus-core primitives an
 * edge read replica consumes: a non-voting, read-only tap on the committed log
 * with snapshot-boundary handoff and live tail. These exercise the node API
 * directly over LocalTransport; the SSE endpoint and the browser SDK that ride on
 * top are covered in the edge-replica integration tests.
 */
describe('committed-log read stream (ADR-0023)', () => {
    let cluster: BookNode[];

    afterEach(() => {
        for (const n of cluster ?? []) n.stop();
    });

    /** ISBNs unique per add so the state machine doesn't reject duplicates. */
    const addBook = (n: number) =>
        buildAddCommand({ title: `t${n}`, author: 'a', publisher: 'p', isbn: `isbn-${n}`, copies: 1 });

    it('live-tails committed entries to a subscriber, in commit order', async () => {
        cluster = buildCluster(3);
        cluster.forEach((n) => n.start());
        await waitFor(() => leaders(cluster).length === 1, 3000);
        const leader = leaders(cluster)[0];

        const seen: { index: number; type: string }[] = [];
        leader.onCommitted((index, entry: LogEntry<BookCommand>) => {
            seen.push({ index, type: entry.command.type });
        });

        for (let i = 0; i < 5; i++) await leader.submit(addBook(i));

        // The five application commands arrive in strictly increasing index order.
        const adds = seen.filter((e) => e.type === 'ADD');
        expect(adds).toHaveLength(5);
        const indices = adds.map((e) => e.index);
        expect(indices).toEqual([...indices].sort((a, b) => a - b));
        expect(new Set(indices).size).toBe(5);
    });

    it('replays the already-committed tail to a late consumer via getCommittedEntries', async () => {
        cluster = buildCluster(3);
        cluster.forEach((n) => n.start());
        await waitFor(() => leaders(cluster).length === 1, 3000);
        const leader = leaders(cluster)[0];

        for (let i = 0; i < 4; i++) await leader.submit(addBook(i));

        // A consumer connecting fresh (afterIndex 0) gets every committed entry,
        // contiguous from 1, ending at the live head.
        const backlog = leader.getCommittedEntries(0);
        const addCount = backlog.filter((e) => e.entry.command.type === 'ADD').length;
        expect(addCount).toBe(4);
        const idxs = backlog.map((e) => e.index);
        expect(idxs[0]).toBe(1);
        expect(idxs).toEqual(idxs.slice().sort((a, b) => a - b));
        expect(Math.max(...idxs)).toBe(leader.getCommitIndex());

        // A consumer that already has through the head gets nothing more (no gap, no dupes).
        expect(leader.getCommittedEntries(leader.getCommitIndex())).toHaveLength(0);
    });

    it('a follower serves the same committed stream (read fan-out)', async () => {
        cluster = buildCluster(3);
        cluster.forEach((n) => n.start());
        await waitFor(() => leaders(cluster).length === 1, 3000);
        const leader = leaders(cluster)[0];
        const follower = cluster.find((n) => !n.isLeader())!;

        const followerSeen: number[] = [];
        follower.onCommitted((index, entry: LogEntry<BookCommand>) => {
            if (entry.command.type === 'ADD') followerSeen.push(index);
        });

        for (let i = 0; i < 3; i++) await leader.submit(addBook(i));

        // The follower applies the same committed entries as the leader commits them,
        // so a stream served from a follower converges to identical state.
        await waitFor(() => followerSeen.length === 3, 3000);
        expect(followerSeen).toEqual(followerSeen.slice().sort((a, b) => a - b));
    });

    it('hands off via a snapshot once the log has been compacted', async () => {
        // A single-node cluster is its own majority and commits immediately, so we
        // can drive enough writes to cross a low snapshot threshold deterministically.
        const registry = new Map<string, RpcHandler>();
        const transport = new LocalTransport(registry, 1);
        const node = new RaftNode<BookCommand, Book, BookStateMachine>(
            {
                id: 'solo',
                peers: [],
                stateMachine: new BookStateMachine(),
                snapshotThreshold: 5,
                electionMinMs: 50,
                electionMaxMs: 100,
                heartbeatMs: 20,
            },
            transport,
        );
        registry.set('solo', node);
        cluster = [node];
        node.start();
        await waitFor(() => node.isLeader(), 2000);

        for (let i = 0; i < 12; i++) await node.submit(addBook(i));

        // The log was compacted, so a fresh consumer (afterIndex 0) can no longer be
        // served entry-by-entry — it must bootstrap from the snapshot first.
        expect(node.getSnapshotIndex()).toBeGreaterThan(0);
        expect(node.needsSnapshot(0)).toBe(true);

        const snap = node.getStreamSnapshot();
        expect(snap).not.toBeNull();
        expect(snap!.lastIncludedIndex).toBe(node.getSnapshotIndex());
        // The snapshot carries the application state an edge replica restores from.
        const state = (snap!.data as { state: Book[] }).state;
        expect(Array.isArray(state)).toBe(true);
        expect(state.length).toBeGreaterThan(0);

        // After the boundary, the remaining committed tail is still replayable.
        const tail = node.getCommittedEntries(snap!.lastIncludedIndex);
        for (const item of tail) expect(item.index).toBeGreaterThan(snap!.lastIncludedIndex);
        expect(Math.max(node.getSnapshotIndex(), ...tail.map((t) => t.index))).toBe(node.getCommitIndex());
    });
});
