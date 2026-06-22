import { RaftNode, NotLeaderError } from '../src/consensus/raftNode';
import { isControlCommand } from '../src/consensus/types';
import type { Command } from '../src/consensus/types';
import {
    buildAddCommand,
    buildBorrowCommand,
    buildReturnCommand,
    buildUpdateCommand,
    buildDeleteCommand,
    BookCommand,
    Book,
} from '../src/models/book';
import { BookNode, BookStateMachine } from '../src/models/bookStateMachine';
import { buildCluster, leaders, waitFor } from './helpers';

/**
 * Property-based / generative safety tests for the consensus core.
 *
 * Each iteration builds a fresh in-process LocalTransport book cluster, drives a
 * randomized sequence of valid book commands through the current leader (re-finding
 * it each step and tolerating transient NotLeader), periodically crashes/restarts a
 * node or the leader to force re-elections, then — after the cluster re-settles —
 * asserts the Raft SAFETY invariants:
 *
 *   1. State convergence — every live node's app.getAll() is deep-equal.
 *   2. Log agreement — committed entries are identical across nodes up to the
 *      shared (minimum) commit index; no divergence below the commit point.
 *   3. Determinism — replaying the committed command sequence into a fresh
 *      BookStateMachine reproduces the converged state exactly.
 *
 * Randomness is a hand-rolled seeded mulberry32 PRNG (no new deps), and every
 * iteration prints its seed up front so any failure is reproducible: re-run with
 * SEED=<n> to replay a single failing case.
 */

// ---- seeded PRNG (mulberry32) — deterministic, reproducible ----

function mulberry32(seed: number): () => number {
    let a = seed >>> 0;
    return () => {
        a |= 0;
        a = (a + 0x6d2b79f5) | 0;
        let t = Math.imul(a ^ (a >>> 15), 1 | a);
        t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t;
        return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
    };
}

class Rng {
    constructor(private readonly next: () => number) {}
    float(): number {
        return this.next();
    }
    int(maxExclusive: number): number {
        return Math.floor(this.next() * maxExclusive);
    }
    pick<T>(items: T[]): T {
        return items[this.int(items.length)];
    }
    bool(p = 0.5): boolean {
        return this.next() < p;
    }
}

// ---- helpers ----

const liveLeader = (nodes: BookNode[]): BookNode | undefined => leaders(nodes)[0];

/**
 * Submit `cmd` to whichever live node is currently leader, retrying across
 * transient NotLeader / re-election windows. Returns true if it committed.
 */
async function submitToLeader(nodes: BookNode[], cmd: BookCommand): Promise<boolean> {
    for (let attempt = 0; attempt < 40; attempt++) {
        const leader = liveLeader(nodes);
        if (leader) {
            try {
                await leader.submit(cmd);
                return true;
            } catch (err) {
                // Transient under churn: the leader stepped down mid-submit
                // (NotLeaderError) or the leader we awaited was crashed (its pending
                // entries reject with 'node stopped'). Re-find the leader and retry.
                // Any OTHER error is a real failure and must surface — never let a
                // genuine regression hide as a silently-retried low-commit run.
                if (!(err instanceof NotLeaderError) && (err as Error).message !== 'node stopped') {
                    throw err;
                }
            }
        }
        await new Promise((r) => setTimeout(r, 15));
    }
    return false;
}

/** Pick a valid, well-typed book command given the leader's current state. */
function nextCommand(rng: Rng, leader: BookNode, isbnCounter: { n: number }): BookCommand {
    const books = leader.app.getAll();
    // With no books yet, the only sensible command is ADD.
    if (books.length === 0 || rng.bool(0.45)) {
        const isbn = `ISBN-${isbnCounter.n++}`;
        return buildAddCommand({
            title: `Title ${isbn}`,
            author: rng.pick(['Lamport', 'Ongaro', 'Tanenbaum', 'Lampson']),
            publisher: rng.pick(['ACM', 'USENIX', 'Pearson']),
            isbn,
            copies: 1 + rng.int(3),
        });
    }

    const book: Book = rng.pick(books);
    const choice = rng.int(4);
    switch (choice) {
        case 0:
            return buildUpdateCommand(book.id, { title: `Retitled ${rng.int(1000)}` });
        case 1:
            // BORROW may legitimately fail (no copies) — still a valid log entry.
            return buildBorrowCommand(book.id, rng.pick(['alice', 'bob', 'carol']));
        case 2:
            // RETURN may legitimately fail (nothing borrowed) — still valid.
            return buildReturnCommand(book.id);
        default:
            return buildDeleteCommand(book.id);
    }
}

/** Committed application (non-control) commands from a node, in log order. */
function committedAppCommands(node: BookNode): BookCommand[] {
    const out: BookCommand[] = [];
    for (const { entry } of node.getCommittedEntries(0)) {
        const command = entry.command as Command<BookCommand>;
        if (!isControlCommand(command)) out.push(command as BookCommand);
    }
    return out;
}

/** Stable, order-independent comparison key for a set of books. */
function bookKey(books: Book[]): string {
    return JSON.stringify([...books].sort((a, b) => a.id.localeCompare(b.id)));
}

// ---- invariant assertions ----

function assertConvergence(live: BookNode[], seed: number): void {
    if (live.length < 2) return;
    const reference = bookKey(live[0].app.getAll());
    for (const node of live.slice(1)) {
        expect({ seed, node: node.id, state: bookKey(node.app.getAll()) }).toEqual({
            seed,
            node: node.id,
            state: reference,
        });
    }
}

function assertLogAgreement(live: BookNode[], seed: number): void {
    if (live.length < 2) return;
    // Compare committed entries up to the SHARED (minimum) commit index: no two
    // nodes may disagree on any committed entry below the commit point.
    const minCommit = Math.min(...live.map((n) => n.getCommitIndex()));
    const serialize = (node: BookNode): string[] => {
        const rows: string[] = [];
        for (const { index, entry } of node.getCommittedEntries(0)) {
            if (index > minCommit) break;
            rows.push(`${index}:${entry.term}:${JSON.stringify(entry.command)}`);
        }
        return rows;
    };
    const reference = serialize(live[0]);
    for (const node of live.slice(1)) {
        expect({ seed, node: node.id, log: serialize(node) }).toEqual({
            seed,
            node: node.id,
            log: reference,
        });
    }
}

function assertDeterminism(live: BookNode[], seed: number): void {
    // Replaying the converged node's committed command sequence into a fresh state
    // machine must reproduce the same state, confirming apply() is a pure function
    // of the committed log.
    const reference = live[0];
    const replay = new BookStateMachine();
    for (const cmd of committedAppCommands(reference)) replay.apply(cmd);
    expect({ seed, state: bookKey(replay.getAll()) }).toEqual({
        seed,
        state: bookKey(reference.app.getAll()),
    });
}

// ---- the generative scenario ----

async function runScenario(seed: number, size: number): Promise<void> {
    const rng = new Rng(mulberry32(seed));
    const nodes = buildCluster(size);
    nodes.forEach((n) => n.start());
    const stopped = new Set<BookNode>();
    const live = () => nodes.filter((n) => !stopped.has(n));

    try {
        await waitFor(() => leaders(nodes).length === 1, 3000);

        const isbnCounter = { n: 0 };
        const steps = 6 + rng.int(6); // 6..11 commands per iteration

        for (let step = 0; step < steps; step++) {
            const leader = liveLeader(live());
            if (!leader) {
                await waitFor(() => leaders(live()).length === 1, 3000).catch(() => undefined);
                continue;
            }
            const cmd = nextCommand(rng, leader, isbnCounter);
            await submitToLeader(live(), cmd);

            // Periodically induce churn: crash a node (often the leader) to force a
            // re-election, or a second one on a 5-node cluster. A 3-node cluster
            // tolerates exactly one failure, so never stop more than one there.
            if (rng.bool(0.35)) {
                if (stopped.size === 0) {
                    const victim = rng.bool(0.6) ? liveLeader(nodes) ?? rng.pick(nodes) : rng.pick(nodes);
                    victim.stop();
                    stopped.add(victim);
                    await waitFor(() => leaders(live()).length === 1, 3000).catch(() => undefined);
                } else if (size >= 5 && stopped.size < size - 3 && rng.bool(0.4)) {
                    // Only a 5-node cluster can afford a second concurrent failure
                    // (5 tolerates 2). Bounded so a quorum always remains.
                    const candidate = live().find((n) => !n.isLeader());
                    if (candidate) {
                        candidate.stop();
                        stopped.add(candidate);
                        await waitFor(() => leaders(live()).length === 1, 3000).catch(() => undefined);
                    }
                }
            }
        }

        // Restart every crashed node so the cluster heals to full membership.
        for (const node of [...stopped]) {
            node.start();
            stopped.delete(node);
        }

        // Wait for the healed cluster to re-settle: one leader, and every node
        // caught up to the leader's commit index (so convergence is a fair check).
        await waitFor(() => leaders(nodes).length === 1, 4000);
        const targetCommit = liveLeader(nodes)!.getCommitIndex();
        await waitFor(() => nodes.every((n) => n.getCommitIndex() >= targetCommit), 4000);
        // Give the apply loop a beat to drain lastApplied up to commitIndex everywhere.
        await waitFor(() => nodes.every((n) => n.status().lastApplied >= targetCommit), 4000);

        // ---- SAFETY INVARIANTS ----
        assertLogAgreement(nodes, seed);
        assertConvergence(nodes, seed);
        assertDeterminism(nodes, seed);
    } finally {
        nodes.forEach((n) => n.stop());
    }
}

describe('Consensus safety invariants (generative)', () => {
    // Generous headroom: an iteration can chain several 3-4s settle waits, and CI
    // boxes are slower than dev. The real timing is bounded by the fast test timers.
    jest.setTimeout(45000);

    // A fixed base seed keeps CI runs reproducible while still covering many
    // distinct randomized scenarios. Override with SEED=<n> to replay one case.
    const BASE_SEED = process.env.SEED ? Number(process.env.SEED) : 0xc0ffee;
    const ITERATIONS = process.env.SEED ? 1 : 10;

    for (let i = 0; i < ITERATIONS; i++) {
        const seed = (BASE_SEED + i * 0x9e3779b1) >>> 0;
        const size = i % 3 === 2 ? 5 : 3; // mix in 5-node clusters

        it(`converges and stays consistent under churn [seed=${seed}, ${size} nodes]`, async () => {
            // eslint-disable-next-line no-console
            console.log(`[consensusProperties] iteration ${i} seed=${seed} size=${size}`);
            await runScenario(seed, size);
        });
    }
});
