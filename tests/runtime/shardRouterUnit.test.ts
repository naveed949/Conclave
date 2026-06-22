import { ApplyResult, CommandMeta } from '../../src/consensus/types';
import { MetricsRegistry } from '../../src/platform/metrics';
import { NoShardLeaderError, ShardNode, ShardRouter } from '../../src/runtime/shardRouter';

/**
 * Unit coverage of `ShardRouter` over FAKE shard nodes. `sharding.test.ts` drives
 * the router with real `buildModuleCluster` shards (the happy path), but its error
 * and edge branches — empty-shard guard, an out-of-range shard index, the
 * no-known-leader rejection, and `collectMetrics` when a shard has NO leader — are
 * easier and faster to pin down with stubbed `{ isLeader, submit }` handles. No
 * sockets, no timers, no teardown needed.
 */

interface FakeNode extends ShardNode<any, unknown> {
    leader: boolean;
    submitted: Array<{ command: any; meta?: CommandMeta }>;
}

/** A stub shard node whose leadership is a settable flag. */
function fakeNode(leader: boolean): FakeNode {
    const submitted: Array<{ command: any; meta?: CommandMeta }> = [];
    return {
        leader,
        submitted,
        isLeader() {
            return this.leader;
        },
        async submit(command: any, meta?: CommandMeta): Promise<ApplyResult<unknown>> {
            submitted.push({ command, meta });
            return { status: 200, result: { ok: true } } as ApplyResult<unknown>;
        },
    };
}

const META: CommandMeta = { actor: 'tester', requestId: 'req-1' };

describe('ShardRouter construction', () => {
    it('rejects an empty shard list', () => {
        expect(() => new ShardRouter([])).toThrow(/at least one shard/);
    });

    it('exposes shardCount equal to the number of handles', () => {
        const router = new ShardRouter([{ nodes: [fakeNode(true)] }, { nodes: [fakeNode(false)] }]);
        expect(router.shardCount).toBe(2);

        const single = new ShardRouter([{ nodes: [fakeNode(true)] }]);
        expect(single.shardCount).toBe(1);
    });
});

describe('ShardRouter.shardFor (deterministic routing)', () => {
    it('maps a key into [0, shardCount) and is stable across instances', () => {
        const mk = () =>
            new ShardRouter([{ nodes: [fakeNode(true)] }, { nodes: [fakeNode(true)] }, { nodes: [fakeNode(true)] }]);
        const a = mk();
        const b = mk();
        for (let i = 0; i < 50; i++) {
            const key = `k-${i}`;
            const shard = a.shardFor(key);
            expect(shard).toBeGreaterThanOrEqual(0);
            expect(shard).toBeLessThan(3);
            expect(b.shardFor(key)).toBe(shard); // pure function of key + count
        }
    });

    it('a single-shard router always routes to shard 0', () => {
        const router = new ShardRouter([{ nodes: [fakeNode(true)] }]);
        for (const key of ['a', 'b', 'anything']) {
            expect(router.shardFor(key)).toBe(0);
        }
    });
});

describe('ShardRouter.leaderOf', () => {
    it('returns the current leader node when one exists', () => {
        const follower = fakeNode(false);
        const leader = fakeNode(true);
        const router = new ShardRouter([{ nodes: [follower, leader] }]);
        expect(router.leaderOf(0)).toBe(leader);
    });

    it('returns null when the shard has no known leader', () => {
        const router = new ShardRouter([{ nodes: [fakeNode(false), fakeNode(false)] }]);
        expect(router.leaderOf(0)).toBeNull();
    });

    it('throws RangeError for an out-of-range shard index', () => {
        const router = new ShardRouter([{ nodes: [fakeNode(true)] }]);
        expect(() => router.leaderOf(5)).toThrow(RangeError);
        expect(() => router.leaderOf(5)).toThrow(/no such shard: 5/);
    });
});

describe('ShardRouter.submit', () => {
    it("routes a command to the owning shard's leader and resolves its ApplyResult", async () => {
        // Two shards, each with a distinct leader; find a key landing on each.
        const leaderA = fakeNode(true);
        const leaderB = fakeNode(true);
        const router = new ShardRouter([{ nodes: [leaderA] }, { nodes: [leaderB] }]);

        let keyA: string | undefined;
        let keyB: string | undefined;
        for (let i = 0; i < 1000 && (keyA === undefined || keyB === undefined); i++) {
            const key = `acct-${i}`;
            const shard = router.shardFor(key);
            if (shard === 0 && keyA === undefined) keyA = key;
            if (shard === 1 && keyB === undefined) keyB = key;
        }
        expect(keyA).toBeDefined();
        expect(keyB).toBeDefined();

        const resA = await router.submit(keyA!, { type: 'X' } as any, META);
        expect(resA.status).toBe(200);
        // The command reached ONLY the owning shard's leader.
        expect(leaderA.submitted).toHaveLength(1);
        expect(leaderA.submitted[0].command).toEqual({ type: 'X' });
        expect(leaderA.submitted[0].meta).toBe(META);
        expect(leaderB.submitted).toHaveLength(0);

        await router.submit(keyB!, { type: 'Y' } as any, META);
        expect(leaderB.submitted).toHaveLength(1);
        expect(leaderA.submitted).toHaveLength(1); // unchanged
    });

    it('rejects with NoShardLeaderError when the owning shard has no leader', async () => {
        // Every node is a follower => no leader on any shard.
        const router = new ShardRouter([{ nodes: [fakeNode(false), fakeNode(false)] }]);
        await expect(router.submit('any-key', { type: 'X' } as any, META)).rejects.toBeInstanceOf(NoShardLeaderError);
        await expect(router.submit('any-key', { type: 'X' } as any, META)).rejects.toMatchObject({ shard: 0 });
    });

    it('NoShardLeaderError carries the shard number and a descriptive message', () => {
        const err = new NoShardLeaderError(3);
        expect(err.shard).toBe(3);
        expect(err.name).toBe('NoShardLeaderError');
        expect(err.message).toMatch(/shard 3 has no known leader/);
        expect(err).toBeInstanceOf(Error);
    });
});

describe('ShardRouter.collectMetrics', () => {
    it('sets shard_count and shard_has_leader=1 for every shard that has a leader', () => {
        const metrics = new MetricsRegistry();
        const router = new ShardRouter([{ nodes: [fakeNode(true)] }, { nodes: [fakeNode(true)] }]);
        router.collectMetrics(metrics);

        const text = metrics.expose();
        expect(sampleValue(text, 'shard_count ')).toBe(2);
        expect(sampleValue(text, 'shard_has_leader{shard="0"}')).toBe(1);
        expect(sampleValue(text, 'shard_has_leader{shard="1"}')).toBe(1);
    });

    it('sets shard_has_leader=0 for a shard mid-election (no known leader)', () => {
        const metrics = new MetricsRegistry();
        // Shard 0 has a leader; shard 1 is leaderless (both followers).
        const router = new ShardRouter([
            { nodes: [fakeNode(true)] },
            { nodes: [fakeNode(false), fakeNode(false)] },
        ]);
        router.collectMetrics(metrics);

        const text = metrics.expose();
        expect(sampleValue(text, 'shard_count ')).toBe(2);
        expect(sampleValue(text, 'shard_has_leader{shard="0"}')).toBe(1);
        expect(sampleValue(text, 'shard_has_leader{shard="1"}')).toBe(0);
    });
});

/** Parse a single Prometheus sample line's value by exact `name{labels}` prefix. */
function sampleValue(text: string, prefix: string): number | undefined {
    const line = text.split('\n').find((l) => l.startsWith(prefix));
    if (!line) return undefined;
    return Number(line.slice(line.lastIndexOf(' ') + 1));
}
