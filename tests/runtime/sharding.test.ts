import { ApplyResult, CommandMeta } from '../../src/consensus/types';
import { buildModuleCommand } from '../../src/runtime/command';
import { accounts } from '../../src/runtime/modules/accounts';
import { runSaga, SagaStep } from '../../src/runtime/saga';
import { ShardRouter } from '../../src/runtime/shardRouter';
import { ModuleAppCommand } from '../../src/runtime/types';
import { buildModuleCluster, leaders, ModuleNode, waitFor } from '../helpers';

/**
 * M10 (ADR-0020): write scaling via multiple INDEPENDENT Raft groups (shards), a
 * deterministic shard router, and cross-shard transactions via a saga
 * (try/compensate). Each shard is an ordinary `buildCluster(3)`, so per-shard
 * correctness from M1-M9 is unchanged; cross-shard atomicity is by COMPENSATION,
 * not isolation.
 */
describe('Multi-Raft sharding (ADR-0020)', () => {
    // Two genuinely independent clusters: each buildCluster makes its own
    // registry + transport, so the shards share nothing but the router's view.
    let shardA: ModuleNode[];
    let shardB: ModuleNode[];
    let router: ShardRouter<ModuleAppCommand>;

    /** All nodes across all shards, for blanket replication asserts + teardown. */
    const allNodes = (): ModuleNode[] => [...shardA, ...shardB];

    beforeEach(async () => {
        // Each shard is its own independent cluster running the keyed `accounts`
        // module, so a MODULE command applies identically within each group.
        shardA = buildModuleCluster(3, [accounts]);
        shardB = buildModuleCluster(3, [accounts]);
        allNodes().forEach((n) => n.start());

        router = new ShardRouter([{ nodes: shardA }, { nodes: shardB }]);

        // Wait for each shard to elect its OWN leader (independent elections).
        await waitFor(() => leaders(shardA).length === 1 && leaders(shardB).length === 1);
    });

    afterEach(() => {
        // Stop ALL nodes of ALL shards to clear timers (no open-handle leaks).
        allNodes().forEach((n) => n.stop());
    });

    /** A unique requestId per logical command keeps idempotency dedup honest. */
    const meta = (requestId: string, actor = 'tester'): CommandMeta => ({
        requestId,
        actor,
        timestamp: new Date().toISOString(),
    });

    /**
     * Submit through the router and THROW on a non-200 status. `RaftNode.submit`
     * resolves with an ApplyResult even for a business rejection (a thrown reducer
     * maps to status 500), so a saga step must convert non-200 into a throw to
     * trigger compensation.
     */
    const routeOrThrow = async (
        key: string,
        module: string,
        command: string,
        input: unknown,
        requestId: string,
    ): Promise<ApplyResult> => {
        const m = meta(requestId);
        const res = await router.submit(key, buildModuleCommand(module, command, input, m), m);
        if (res.status !== 200) {
            throw new Error(`command ${module}.${command} failed (${res.status}): ${res.message}`);
        }
        return res;
    };

    /** Balance of `id` as seen by `node` (undefined if the account is unknown). */
    const balanceOn = (node: ModuleNode, id: string): number | undefined =>
        node.app.host.query('accounts', 'balance', { id }) as number | undefined;

    /**
     * Pick two account ids that the router maps to DIFFERENT shards, so each demo
     * account genuinely lives in a different Raft group. Deterministic search.
     */
    const pickKeysOnDifferentShards = (): { onA: string; onB: string } => {
        let onA: string | undefined;
        let onB: string | undefined;
        for (let i = 0; i < 1000 && (onA === undefined || onB === undefined); i++) {
            const key = `acct-${i}`;
            const shard = router.shardFor(key);
            if (shard === 0 && onA === undefined) onA = key;
            if (shard === 1 && onB === undefined) onB = key;
        }
        if (onA === undefined || onB === undefined) {
            throw new Error('could not find keys on both shards');
        }
        return { onA, onB };
    };

    it('routes keys deterministically and stably across router instances', () => {
        // Determinism: a fresh router over the same shard count maps identically.
        const other = new ShardRouter([{ nodes: shardA }, { nodes: shardB }]);
        for (let i = 0; i < 50; i++) {
            const key = `acct-${i}`;
            expect(router.shardFor(key)).toBe(other.shardFor(key));
            expect(router.shardFor(key)).toBeGreaterThanOrEqual(0);
            expect(router.shardFor(key)).toBeLessThan(2);
        }

        // The two demo keys must land on DIFFERENT shards for the cross-shard tests.
        const { onA, onB } = pickKeysOnDifferentShards();
        expect(router.shardFor(onA)).not.toBe(router.shardFor(onB));
    });

    it('writes to different shards proceed independently (parallel, isolated state)', async () => {
        const { onA, onB } = pickKeysOnDifferentShards();

        // Open + deposit each account on ITS OWN shard, in parallel.
        await Promise.all([
            routeOrThrow(onA, 'accounts', 'open', { id: onA }, `open-${onA}`),
            routeOrThrow(onB, 'accounts', 'open', { id: onB }, `open-${onB}`),
        ]);
        await Promise.all([
            routeOrThrow(onA, 'accounts', 'deposit', { id: onA, amount: 100 }, `dep-${onA}`),
            routeOrThrow(onB, 'accounts', 'deposit', { id: onB, amount: 250 }, `dep-${onB}`),
        ]);

        // Each converges within its OWN 3-node cluster.
        await waitFor(() => shardA.every((n) => balanceOn(n, onA) === 100));
        await waitFor(() => shardB.every((n) => balanceOn(n, onB) === 250));

        // Shards are independent: A's account is unknown on shard B and vice-versa.
        expect(shardB.every((n) => balanceOn(n, onA) === undefined)).toBe(true);
        expect(shardA.every((n) => balanceOn(n, onB) === undefined)).toBe(true);
    });

    it('completes a cross-shard transfer via a saga (withdraw on X, deposit on Y)', async () => {
        const { onA: alice, onB: bob } = pickKeysOnDifferentShards();

        // Set up: alice has 100 on shard X, bob has 0 on shard Y.
        await routeOrThrow(alice, 'accounts', 'open', { id: alice }, `open-${alice}`);
        await routeOrThrow(bob, 'accounts', 'open', { id: bob }, `open-${bob}`);
        await routeOrThrow(alice, 'accounts', 'deposit', { id: alice, amount: 100 }, `dep-${alice}`);
        await waitFor(() => shardA.every((n) => balanceOn(n, alice) === 100));
        await waitFor(() => shardB.every((n) => balanceOn(n, bob) === 0));

        const amount = 40;
        const steps: SagaStep[] = [
            {
                name: 'debit-alice',
                invoke: () => routeOrThrow(alice, 'accounts', 'withdraw', { id: alice, amount }, `sx-wd-${alice}`),
                // Compensation: deposit the same amount back (distinct requestId).
                compensate: async () => {
                    await routeOrThrow(alice, 'accounts', 'deposit', { id: alice, amount }, `sx-comp-${alice}`);
                },
            },
            {
                name: 'credit-bob',
                invoke: () => routeOrThrow(bob, 'accounts', 'deposit', { id: bob, amount }, `sx-dep-${bob}`),
                compensate: async () => {
                    await routeOrThrow(bob, 'accounts', 'withdraw', { id: bob, amount }, `sx-comp-${bob}`);
                },
            },
        ];

        const result = await runSaga(steps);
        expect(result.ok).toBe(true);

        // Alice debited on every node of shard X; bob credited on every node of Y.
        await waitFor(() => shardA.every((n) => balanceOn(n, alice) === 60));
        await waitFor(() => shardB.every((n) => balanceOn(n, bob) === 40));

        // Funds conserved across both shards — checked against EVERY node (the
        // shards converged above), not just one sampled replica.
        expect(shardA.every((n) => balanceOn(n, alice) === 60)).toBe(true);
        expect(shardB.every((n) => balanceOn(n, bob) === 40)).toBe(true);
        expect(balanceOn(shardA[0], alice)! + balanceOn(shardB[0], bob)!).toBe(100);
    });

    it('compensates the source debit when the credit leg fails (no funds lost)', async () => {
        const { onA: alice } = pickKeysOnDifferentShards();
        // A target account that DOES NOT EXIST: depositing into it fails, forcing
        // compensation of the already-committed debit.
        const ghost = 'ghost-target-never-opened';

        await routeOrThrow(alice, 'accounts', 'open', { id: alice }, `open2-${alice}`);
        await routeOrThrow(alice, 'accounts', 'deposit', { id: alice, amount: 100 }, `dep2-${alice}`);
        await waitFor(() => shardA.every((n) => balanceOn(n, alice) === 100));

        const amount = 30;
        const steps: SagaStep[] = [
            {
                name: 'debit-alice',
                invoke: () => routeOrThrow(alice, 'accounts', 'withdraw', { id: alice, amount }, `fail-wd-${alice}`),
                compensate: async () => {
                    await routeOrThrow(alice, 'accounts', 'deposit', { id: alice, amount }, `fail-comp-${alice}`);
                },
            },
            {
                name: 'credit-ghost',
                // ghost does not exist on its shard ⇒ deposit rejects ⇒ step fails.
                invoke: () => routeOrThrow(ghost, 'accounts', 'deposit', { id: ghost, amount }, `fail-dep-${ghost}`),
                compensate: async () => {
                    await routeOrThrow(ghost, 'accounts', 'withdraw', { id: ghost, amount }, `fail-comp-${ghost}`);
                },
            },
        ];

        const result = await runSaga(steps);
        expect(result.ok).toBe(false);
        if (result.ok === false) {
            expect(result.failedAt).toBe('credit-ghost');
            expect(result.compensated).toEqual(['debit-alice']);
            // The compensation succeeded, so nothing was stranded (no fund loss).
            expect(result.compensationFailures).toEqual([]);
        }

        // Source balance restored on every node of shard X — no funds lost.
        await waitFor(() => shardA.every((n) => balanceOn(n, alice) === 100));

        // Funds conserved on EVERY node: the ghost never received anything; alice whole.
        expect(shardA.every((n) => balanceOn(n, alice) === 100)).toBe(true);
    });
});

/**
 * Pure saga semantics (no clusters) — in particular the case the conservation
 * coordinator must never hide: a COMPENSATION that itself throws leaves a forward
 * effect un-undone, so it is reported in `compensationFailures`, NOT `compensated`.
 */
describe('saga compensation failure reporting', () => {
    it('surfaces a throwing compensation instead of marking it compensated', async () => {
        const order: string[] = [];
        const steps: SagaStep[] = [
            {
                name: 's1',
                invoke: async () => order.push('inv-s1'),
                // s1's compensation FAILS — its forward effect is not undone.
                compensate: async () => {
                    order.push('comp-s1');
                    throw new Error('compensation boom');
                },
            },
            {
                name: 's2',
                invoke: async () => order.push('inv-s2'),
                compensate: async () => {
                    order.push('comp-s2');
                },
            },
            {
                name: 's3',
                invoke: async () => {
                    throw new Error('s3 failed');
                },
                compensate: async () => order.push('comp-s3'),
            },
        ];

        const result = await runSaga(steps);
        expect(result.ok).toBe(false);
        if (result.ok === false) {
            expect(result.failedAt).toBe('s3');
            // s2 compensated cleanly; s1's compensation threw — reported separately.
            expect(result.compensated).toEqual(['s2']);
            expect(result.compensationFailures.map((f) => f.step)).toEqual(['s1']);
            expect(result.compensationFailures[0].error.message).toBe('compensation boom');
        }
        // Compensations ran in reverse order (s2 before s1); s3 never committed.
        expect(order).toEqual(['inv-s1', 'inv-s2', 'comp-s2', 'comp-s1']);
    });
});
