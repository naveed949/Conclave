import { CommandMeta } from '../../src/consensus/types';
import { buildModuleCommand } from '../../src/runtime/command';
import { EffectDriver } from '../../src/runtime/effectDriver';
import { payments } from '../../src/runtime/modules/payments';
import { EffectHandler } from '../../src/runtime/types';
import { buildModuleCluster, leaders, ModuleNode, waitFor } from '../helpers';

/**
 * M12: the LIVE committed-intent effect loop on a real Raft cluster. A
 * `payments.charge` enqueues an effect into every node's outbox; the LEADER's
 * `EffectDriver` (only the leader acts) drains it, runs the handler ONCE at the
 * edge, and submits the result through the log so `settle` applies on EVERY node.
 * Exactly-once committed state with at-least-once handlers.
 */

/** A driver-friendly handler that counts its invocations and reports the order paid. */
function okHandler(): EffectHandler & { calls: number } {
    const fn = (async (intent) => {
        fn.calls += 1;
        const { orderId } = intent.payload as { orderId: string };
        return { orderId, ok: true };
    }) as EffectHandler & { calls: number };
    fn.calls = 0;
    return fn;
}

/** Submit a charge on the current leader and assert it was accepted. */
async function charge(leader: ModuleNode, orderId: string, amount: number, requestId: string): Promise<void> {
    const meta: CommandMeta = { requestId, actor: 'alice', timestamp: new Date().toISOString() };
    const res = await leader.submit(buildModuleCommand('payments', 'charge', { orderId, amount }, meta), meta);
    expect(res.status).toBe(200);
}

/** The order status as seen by a node's local host. */
function orderStatus(node: ModuleNode, orderId: string): string | undefined {
    const order = node.app.host.query('payments', 'order', { orderId }) as { status: string } | undefined;
    return order?.status;
}

describe('EffectDriver: live committed-intent effect loop over Raft', () => {
    let nodes: ModuleNode[];
    let drivers: EffectDriver[];

    afterEach(() => {
        drivers.forEach((d) => d.stop());
        nodes.forEach((n) => n.stop());
    });

    it('leader runs the handler exactly once and settle converges to paid on every node', async () => {
        nodes = buildModuleCluster(3, [payments]);
        const handler = okHandler();
        // A driver on EVERY node, but only the leader should ever run the handler.
        drivers = nodes.map((n) => new EffectDriver(n, { http: handler }, { intervalMs: 10 }));
        nodes.forEach((n) => n.start());
        drivers.forEach((d) => d.start());

        await waitFor(() => leaders(nodes).length === 1);
        const leader = leaders(nodes)[0];

        await charge(leader, 'o1', 100, 'req-charge-1');

        // The leader's driver drains, the handler runs, the result rides the log,
        // and `settle` flips the order to paid on every node.
        await waitFor(() => nodes.every((n) => orderStatus(n, 'o1') === 'paid'));

        // Exactly-once execution: a SINGLE handler invocation across the cluster
        // (only the leader ran it, even though all 3 nodes have a driver).
        expect(handler.calls).toBe(1);

        // Convergence: identical module state + outbox on every node.
        const states = new Set(nodes.map((n) => JSON.stringify(n.app.host.getState('payments'))));
        expect(states.size).toBe(1);
        const outboxes = new Set(nodes.map((n) => JSON.stringify(n.app.host.getOutbox())));
        expect(outboxes.size).toBe(1);
        // The single outbox entry is done.
        const leaderOutbox = leader.app.host.getOutbox();
        expect(leaderOutbox).toHaveLength(1);
        expect(leaderOutbox[0].status).toBe('done');
    });

    it('followers do not run the handler (only the leader acts)', async () => {
        nodes = buildModuleCluster(3, [payments]);
        const handler = okHandler();
        drivers = nodes.map((n) => new EffectDriver(n, { http: handler }, { intervalMs: 10 }));
        nodes.forEach((n) => n.start());
        drivers.forEach((d) => d.start());

        await waitFor(() => leaders(nodes).length === 1);
        const leader = leaders(nodes)[0];

        await charge(leader, 'o1', 100, 'req-charge-1');
        await waitFor(() => nodes.every((n) => orderStatus(n, 'o1') === 'paid'));

        // Even with a driver on all 3 nodes, the handler ran exactly once: the two
        // followers' drivers no-op every tick because `isLeader()` is false.
        expect(handler.calls).toBe(1);
    });

    it('a failing handler leaves the effect pending; a later tick completes it', async () => {
        nodes = buildModuleCluster(3, [payments]);

        // A handler that fails until `succeed` is flipped, then reports paid.
        let attempts = 0;
        let succeed = false;
        const handler: EffectHandler = async (intent) => {
            attempts += 1;
            if (!succeed) throw new Error('gateway down');
            const { orderId } = intent.payload as { orderId: string };
            return { orderId, ok: true };
        };
        drivers = nodes.map((n) => new EffectDriver(n, { http: handler }, { intervalMs: 10 }));
        nodes.forEach((n) => n.start());
        drivers.forEach((d) => d.start());

        await waitFor(() => leaders(nodes).length === 1);
        const leader = leaders(nodes)[0];

        await charge(leader, 'o1', 100, 'req-charge-1');

        // While failing: the handler is attempted (≥1) but the effect stays pending
        // and no node settles the order.
        await waitFor(() => attempts >= 1);
        expect(leader.app.host.getOutbox()[0].status).toBe('pending');
        expect(orderStatus(leader, 'o1')).toBe('pending');

        // Make the handler succeed; a later tick drains and completes the effect.
        succeed = true;
        await waitFor(() => nodes.every((n) => orderStatus(n, 'o1') === 'paid'));

        // Convergence holds; outbox entry is done on every node (single settle).
        const outboxes = new Set(nodes.map((n) => JSON.stringify(n.app.host.getOutbox())));
        expect(outboxes.size).toBe(1);
        expect(leader.app.host.getOutbox()).toHaveLength(1);
        expect(leader.app.host.getOutbox()[0].status).toBe('done');
    });

    it('idempotency: the committed result applies once and the cluster converges (no double settle)', async () => {
        nodes = buildModuleCluster(3, [payments]);
        const handler = okHandler();
        drivers = nodes.map((n) => new EffectDriver(n, { http: handler }, { intervalMs: 10 }));
        nodes.forEach((n) => n.start());
        drivers.forEach((d) => d.start());

        await waitFor(() => leaders(nodes).length === 1);
        const leader = leaders(nodes)[0];

        await charge(leader, 'o1', 100, 'req-charge-1');
        await waitFor(() => nodes.every((n) => orderStatus(n, 'o1') === 'paid'));

        // Let several more driver ticks fire: the now-`done` outbox entry is no
        // longer pending, so nothing re-drains and the handler is not re-invoked.
        await new Promise((r) => setTimeout(r, 60));
        expect(handler.calls).toBe(1);

        // Exactly one outbox entry (done), and all nodes are byte-identical:
        // structurally proves no double-apply of the effect result.
        const leaderOutbox = leader.app.host.getOutbox();
        expect(leaderOutbox).toHaveLength(1);
        expect(leaderOutbox[0].status).toBe('done');
        const snapshots = new Set(nodes.map((n) => JSON.stringify(n.app.host.snapshot())));
        expect(snapshots.size).toBe(1);
    });
});
