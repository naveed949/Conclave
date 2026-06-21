import { RaftNode, MembershipError } from '../src/consensus/raftNode';
import {
    LocalTransport,
    RpcHandler,
    Transport,
} from '../src/consensus/transport';
import {
    AppendEntriesArgs,
    InstallSnapshotArgs,
    PeerInfo,
    ReadIndexArgs,
    RequestVoteArgs,
} from '../src/consensus/types';
import { buildAddCommand } from '../src/models/book';
import { BookNode, BookStateMachine } from '../src/models/bookStateMachine';
import { waitFor } from './helpers';

/**
 * A LocalTransport whose deliveries can be partitioned at runtime. `blocked` holds
 * unordered "a|b" pairs; an RPC between two endpoints in a blocked pair (either
 * direction) resolves to null, simulating a network partition. Used to prove the
 * dual-majority property: a candidate that can only reach ONE config's majority
 * during a joint transition must not win.
 */
class PartitionableTransport implements Transport {
    private readonly inner: LocalTransport;
    readonly blocked = new Set<string>();

    constructor(private readonly registry: Map<string, RpcHandler>) {
        this.inner = new LocalTransport(registry, 1);
    }

    static key(a: string, b: string): string {
        return [a, b].sort().join('|');
    }

    /** Block all traffic between every node in `groupA` and every node in `groupB`. */
    partition(groupA: string[], groupB: string[]): void {
        for (const a of groupA) for (const b of groupB) this.blocked.add(PartitionableTransport.key(a, b));
    }

    private reachable(from: string, to: string): boolean {
        return !this.blocked.has(PartitionableTransport.key(from, to));
    }

    sendRequestVote(peer: PeerInfo, args: RequestVoteArgs) {
        if (!this.reachable(args.candidateId, peer.id)) return Promise.resolve(null);
        return this.inner.sendRequestVote(peer, args);
    }
    sendAppendEntries(peer: PeerInfo, args: AppendEntriesArgs) {
        if (!this.reachable(args.leaderId, peer.id)) return Promise.resolve(null);
        return this.inner.sendAppendEntries(peer, args);
    }
    sendInstallSnapshot(peer: PeerInfo, args: InstallSnapshotArgs) {
        if (!this.reachable(args.leaderId, peer.id)) return Promise.resolve(null);
        return this.inner.sendInstallSnapshot(peer, args);
    }
    sendReadIndex(peer: PeerInfo, _args: ReadIndexArgs) {
        // ReadIndex carries no source id; the partition tests don't exercise it.
        return this.inner.sendReadIndex(peer, _args);
    }
}

/** Append a raw CONFIG entry (joint or final) via the node's internal two-phase helper. */
type ConfigSubmitter = {
    submitConfigEntry(command: { type: 'CONFIG'; members: PeerInfo[]; oldMembers?: PeerInfo[] }): Promise<unknown>;
    recomputeMembers(): void;
};
const asInternal = (node: BookNode) => node as unknown as ConfigSubmitter;

const TIMERS = { electionMinMs: 50, electionMaxMs: 100, heartbeatMs: 20 };

// Several tests here run multiple membership changes + elections back-to-back,
// each gated by its own `waitFor` (up to 2–3s apiece). Those internal budgets are
// the real guards — they throw a clear "condition not met" if anything genuinely
// stalls. Their SUM, however, can exceed Jest's 5s default test timeout when
// workers run in parallel under CPU contention (the suite passes in isolation),
// which is the only reason these tests flaked. The joint-consensus tests below
// run 5–6 node clusters with forced elections + partitions, so they get extra
// headroom (30s) so a slow-but-correct run under parallel load is never mistaken
// for a hang. The SAFETY assertions in those tests are negative (assert nothing
// happened) and so are load-insensitive regardless.
jest.setTimeout(30000);

/**
 * A cluster whose registry + node set the test controls, so nodes can be added
 * to or removed from the configuration at runtime.
 */
function makeCluster(ids: string[]) {
    const registry = new Map<string, RpcHandler>();
    const transport = new LocalTransport(registry, 1);
    const nodes = new Map<string, BookNode>();

    const peersFor = (id: string): PeerInfo[] =>
        ids.filter((p) => p !== id).map((p) => ({ id: p, url: `local://${p}` }));

    for (const id of ids) {
        const node = new RaftNode(
            { id, peers: peersFor(id), selfUrl: `local://${id}`, stateMachine: new BookStateMachine(), ...TIMERS },
            transport,
        );
        nodes.set(id, node);
        registry.set(id, node);
    }
    return { registry, transport, nodes, ids };
}

const leaderOf = (nodes: Iterable<BookNode>) => [...nodes].find((n) => n.isLeader())!;
const addBook = (isbn: string) =>
    buildAddCommand({ title: isbn, author: 'A', publisher: 'P', isbn, copies: 1 });

describe('Dynamic membership (joint consensus, ADR-0022)', () => {
    it('adds a new node, which catches up and joins the quorum', async () => {
        const c = makeCluster(['n1', 'n2', 'n3']);
        c.nodes.forEach((n) => n.start());
        await waitFor(() => [...c.nodes.values()].filter((n) => n.isLeader()).length === 1);

        const leader = leaderOf(c.nodes.values());
        await leader.submit(addBook('pre-1')); // a write that predates the new node

        // Spin up n4 (initially knows the existing peers) and add it to the config.
        const transport = c.transport;
        const n4 = new RaftNode(
            {
                id: 'n4',
                peers: ['n1', 'n2', 'n3'].map((p) => ({ id: p, url: `local://${p}` })),
                selfUrl: 'local://n4',
                stateMachine: new BookStateMachine(),
                ...TIMERS,
            },
            transport,
        );
        c.registry.set('n4', n4);
        n4.start();

        const res = await leader.changeMembership({ add: { id: 'n4', url: 'local://n4' } });
        expect(res.status).toBe(200);

        // n4 catches up: it learns the config and replays the pre-existing write.
        await waitFor(() => n4.status().members.length === 4);
        await waitFor(() => n4.app.get(leader.app.getAll()[0].id) !== undefined);
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
        await waitFor(() => survivors.filter((n) => n.isLeader()).length === 1, 6000);
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

    it('commits an arbitrary change whose C-old and C-new do NOT overlap in a majority', async () => {
        // Replace two of three nodes: C-old = {n1,n2,n3}, C-new = {n1,n4,n5}.
        // The two configs share only {n1} — a majority of NEITHER. A single-server
        // change could never express this; only joint consensus (dual majority)
        // commits it safely. Drive the two phases directly through the node.
        const c = makeCluster(['n1', 'n2', 'n3']);
        c.nodes.forEach((n) => n.start());
        await waitFor(() => [...c.nodes.values()].filter((n) => n.isLeader()).length === 1);
        const leader = leaderOf(c.nodes.values());
        const peer = (p: string): PeerInfo => ({ id: p, url: `local://${p}` });

        // Bring up the two new members n4/n5, knowing the existing peers.
        const extra = ['n4', 'n5'].map((id) => {
            const node = new RaftNode(
                { id, peers: ['n1', 'n2', 'n3'].map(peer), selfUrl: `local://${id}`, stateMachine: new BookStateMachine(), ...TIMERS },
                c.transport,
            );
            c.registry.set(id, node);
            node.start();
            return node;
        });
        const nodes = new Map<string, BookNode>(c.nodes);
        nodes.set('n4', extra[0]);
        nodes.set('n5', extra[1]);

        await leader.submit(addBook('pre'));

        // Keep the current leader in C-new (so it survives the transition), and
        // replace the other two C-old nodes with n4/n5. C-old = {n1,n2,n3}, C-new =
        // {<leader>,n4,n5} share only {<leader>} — a majority of NEITHER config.
        const cOldIds = ['n1', 'n2', 'n3'];
        const cNewIds = [leader.id, 'n4', 'n5'];
        const cOld = cOldIds.map(peer);
        const cNew = cNewIds.map(peer);

        // Phase 1: joint C-old,new. Commits only on a majority of BOTH configs.
        await asInternal(leader).submitConfigEntry({ type: 'CONFIG', members: cNew, oldMembers: cOld });
        // Phase 2: final C-new.
        await asInternal(leader).submitConfigEntry({ type: 'CONFIG', members: cNew });

        // The cluster converges to C-new on the surviving leader and the new members.
        await waitFor(() => nodes.get('n4')!.status().members.length === 3, 6000);
        await waitFor(() => nodes.get('n4')!.app.get(leader.app.getAll()[0].id) !== undefined, 6000);

        // A new write commits through C-new (which now excludes two old nodes).
        const w = await leader.submit(addBook('post'));
        expect(w.status).toBe(201);
        await waitFor(() => nodes.get('n5')!.stateMachine.size() === 2, 6000);

        nodes.forEach((n) => n.stop());
    });

    it('enforces dual majority during the joint phase: a C-old-only majority can neither elect nor commit', async () => {
        // Two DISJOINT configs: C-old = {n1,n2,n3}, C-new = {n4,n5,n6}. A majority of
        // one is never a majority of the other, so dual majority is impossible to
        // satisfy from a single side. We pin every node into the joint config, then
        // sever C-old from C-new and watch a C-old-only group: it can reach a C-old
        // majority but ZERO of C-new, so it must neither win an election nor commit.
        const registry = new Map<string, RpcHandler>();
        const transport = new PartitionableTransport(registry);
        const all = ['n1', 'n2', 'n3', 'n4', 'n5', 'n6'];
        const cOldIds = ['n1', 'n2', 'n3'];
        const cNewIds = ['n4', 'n5', 'n6'];
        const peer = (p: string): PeerInfo => ({ id: p, url: `local://${p}` });
        // DETERMINISTIC LEADERSHIP via ASYMMETRIC election timers: n1 alone gets a
        // short timeout; n2..n6 get timeouts so long they never spontaneously
        // campaign. Only n1 ever *initiates* an election, so n1 is the only node
        // that can win one — making n1 the joint leader regardless of CPU load.
        //
        // This replaces an earlier symmetric-timer setup that flaked under
        // parallel-worker contention: forcing *only* n1 to `becomeCandidate` does
        // NOT guarantee n1 wins — once the prior leader steps down, no heartbeats
        // flow, every node's election timer fires, and under CPU starvation some
        // OTHER node (e.g. n4) wins the storm, so `n1.isLeader()` never holds. With
        // n1 the sole initiator that race is gone. The dual-majority SAFETY
        // assertions below FORCE n2/n3 to campaign explicitly (so their long
        // timeouts don't matter there) and assert they still cannot win.
        const FAST = { electionMinMs: 50, electionMaxMs: 100, heartbeatMs: 20 };
        const SLOW = { electionMinMs: 2000, electionMaxMs: 4000, heartbeatMs: 20 };
        const nodes = new Map<string, BookNode>();
        for (const id of all) {
            const node = new RaftNode(
                { id, peers: all.filter((p) => p !== id).map(peer), selfUrl: `local://${id}`, stateMachine: new BookStateMachine(), ...(id === 'n1' ? FAST : SLOW) },
                transport,
            );
            nodes.set(id, node);
            registry.set(id, node);
        }
        all.forEach((id) => nodes.get(id)!.start());
        // n1 is the only node that campaigns, so it is deterministically the leader.
        const n1 = nodes.get('n1')!;
        await waitFor(() => n1.isLeader(), 6000);

        // Pin EVERY node into the joint config C-old,new = ({n1,n2,n3},{n4,n5,n6}) and
        // leave it joint (no final config follows).
        await asInternal(n1).submitConfigEntry({
            type: 'CONFIG',
            members: cNewIds.map(peer),
            oldMembers: cOldIds.map(peer),
        });
        await waitFor(() => all.every((id) => nodes.get(id)!.status().members.length === 6), 6000);

        // n1 (a C-old node) is the joint leader: with the cluster still fully
        // connected it satisfies BOTH majorities and keeps its leadership.
        expect(n1.isLeader()).toBe(true);
        // Commit one write through the intact joint cluster as a baseline.
        await n1.submit(addBook('joint-pre'));
        const sizeBefore = n1.stateMachine.size();

        // Now sever C-old from C-new entirely. n1 (a C-old node) can still reach a
        // C-old majority {n1,n2,n3} but ZERO of C-new {n4,n5,n6}.
        transport.partition(cOldIds, cNewIds);

        // SAFETY (commits): n1 is still leader and accepts the proposal into its log,
        // but committing it needs a C-new majority it can no longer reach — so it
        // must NOT commit (its state machine never grows) within the partial window.
        const pending = n1.submit(addBook('joint-stalled')).catch(() => undefined);
        await new Promise((r) => setTimeout(r, 500));
        expect(n1.stateMachine.size()).toBe(sizeBefore);

        // SAFETY (elections): force the isolated C-old group to campaign. A C-old
        // majority grants votes among themselves, but winning the JOINT config also
        // needs a C-new majority, which is unreachable — so no C-old node wins.
        cOldIds.forEach((id) => (nodes.get(id)! as unknown as { becomeCandidate(): void }).becomeCandidate());
        await new Promise((r) => setTimeout(r, 500));
        expect(cOldIds.filter((id) => nodes.get(id)!.isLeader())).toEqual([]);

        nodes.forEach((n) => n.stop());
        await pending;
    });

    it('a new leader completes a joint transition its crashed predecessor left unfinished', async () => {
        // Raft §4.3: a leader that appends the joint C-old,new but crashes before
        // installing the final C-new must not wedge the cluster — the next leader
        // adopts the joint config on election and finishes the transition. We drive
        // ONLY the joint phase directly, crash the leader, and assert a survivor
        // converges the cluster to the simple C-new on its own.
        const c = makeCluster(['n1', 'n2', 'n3']);
        c.nodes.forEach((n) => n.start());
        await waitFor(() => [...c.nodes.values()].filter((n) => n.isLeader()).length === 1);
        const leader = leaderOf(c.nodes.values());
        const peer = (p: string): PeerInfo => ({ id: p, url: `local://${p}` });

        // A fresh node n4 that joins as part of C-new.
        const n4 = new RaftNode(
            { id: 'n4', peers: ['n1', 'n2', 'n3'].map(peer), selfUrl: 'local://n4', stateMachine: new BookStateMachine(), ...TIMERS },
            c.transport,
        );
        c.registry.set('n4', n4);
        n4.start();

        // Joint transition C-old = {n1,n2,n3} → C-new = the two non-leader originals
        // plus n4 (the leader removes itself). Append ONLY the joint config; the
        // joint UNION is 4 nodes, so a survivor reaching members.length === 4 proves
        // the joint config replicated before we crash the leader.
        const keep = [...c.nodes.values()].filter((n) => n !== leader).map((n) => n.id);
        const cNew = [...keep, 'n4'];
        const survivors = [...keep.map((id) => c.nodes.get(id)!), n4];
        await asInternal(leader).submitConfigEntry({
            type: 'CONFIG',
            members: cNew.map(peer),
            oldMembers: ['n1', 'n2', 'n3'].map(peer),
        });
        await waitFor(() => survivors.some((n) => n.status().members.length === 4), 6000);

        // The originating leader "crashes" before installing the final config.
        leader.stop();

        // A survivor wins (dual majority among the reachable {keep ∪ n4}) and, seeing
        // it inherited a joint config, completes the transition to the simple C-new.
        await waitFor(() => survivors.some((n) => n.isLeader()), 6000);
        await waitFor(() => survivors.every((n) => n.status().members.length === 3), 6000);
        survivors.forEach((n) => expect(n.status().members.slice().sort()).toEqual(cNew.slice().sort()));

        // The finalized simple-majority cluster still commits writes.
        const newLeader = survivors.find((n) => n.isLeader())!;
        const w = await newLeader.submit(addBook('post-finalize'));
        expect(w.status).toBe(201);

        survivors.forEach((n) => n.stop());
    });
});
