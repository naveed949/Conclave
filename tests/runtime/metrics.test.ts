import { RaftNode } from '../../src/consensus/raftNode';
import { LocalTransport, RpcHandler } from '../../src/consensus/transport';
import { CommandMeta, PeerInfo } from '../../src/consensus/types';
import { MetricsRegistry } from '../../src/platform/metrics';
import { buildModuleCommand } from '../../src/runtime/command';
import { EffectDriver } from '../../src/runtime/effectDriver';
import { EffectExecutor } from '../../src/runtime/effectExecutor';
import { ModuleHost } from '../../src/runtime/moduleHost';
import { ModuleStateMachine, ModuleNode } from '../../src/runtime/moduleStateMachine';
import { counter } from '../../src/runtime/modules/counter';
import { payments } from '../../src/runtime/modules/payments';
import { ShardRouter } from '../../src/runtime/shardRouter';
import { EffectHandler, ModuleAppCommand } from '../../src/runtime/types';
import { AnyModuleDefinition } from '../../src/runtime/moduleHost';
import { buildModuleCluster, leaders, waitFor } from '../helpers';

/**
 * Milestone 15: runtime observability over the existing Prometheus registry. These
 * tests assert the module-runtime series surface by parsing `metrics.expose()` text
 * (the same style as `tests/platform.test.ts`), exercise the adapter/driver/scrape
 * collectors, and confirm a metrics-less runtime is a pure no-op.
 */

const TEST_TIMERS = { electionMinMs: 50, electionMaxMs: 100, heartbeatMs: 20 };

/** A single-node module cluster whose ModuleStateMachine has `metrics` wired. */
function buildMeteredNode(
    modules: AnyModuleDefinition[],
    metrics: MetricsRegistry,
): ModuleNode {
    const registry = new Map<string, RpcHandler>();
    const transport = new LocalTransport(registry, 1);
    const peers: PeerInfo[] = [];
    const sm = new ModuleStateMachine(new ModuleHost(), metrics);
    sm.host.registerModules(modules);
    const node: ModuleNode = new RaftNode(
        { id: 'node1', peers, stateMachine: sm, ...TEST_TIMERS },
        transport,
    );
    registry.set(node.id, node);
    metrics.registerCollector(() => sm.collectMetrics(metrics));
    return node;
}

const meta = (requestId: string, actor = 'tester'): CommandMeta => ({
    requestId,
    actor,
    timestamp: new Date().toISOString(),
});

/** Parse a single Prometheus sample line's value by exact `name{labels}` prefix. */
function sampleValue(text: string, prefix: string): number | undefined {
    const line = text.split('\n').find((l) => l.startsWith(prefix));
    if (!line) return undefined;
    return Number(line.slice(line.lastIndexOf(' ') + 1));
}

describe('Module runtime metrics (Milestone 15)', () => {
    describe('command throughput by status (apply path)', () => {
        let node: ModuleNode;
        let metrics: MetricsRegistry;

        beforeEach(async () => {
            metrics = new MetricsRegistry();
            node = buildMeteredNode([counter], metrics);
            node.start();
            await waitFor(() => node.isLeader());
        });

        afterEach(() => node.stop());

        it('counts 200s by module/command and a non-200 under its status label', async () => {
            await node.submit(buildModuleCommand('counter', 'increment', { by: 1 }, meta('r1')), meta('r1'));
            await node.submit(buildModuleCommand('counter', 'increment', { by: 2 }, meta('r2')), meta('r2'));
            // An unknown command resolves with status 404 from the host (not a throw).
            await node.submit(buildModuleCommand('counter', 'nope', {}, meta('r3')), meta('r3'));

            const text = metrics.expose();
            expect(text).toContain('module_commands_total');
            expect(
                sampleValue(text, 'module_commands_total{command="increment",module="counter",status="200"}'),
            ).toBe(2);
            expect(
                sampleValue(text, 'module_commands_total{command="nope",module="counter",status="404"}'),
            ).toBe(1);
            // Latency histogram present for the increment command.
            expect(text).toContain('module_command_duration_ms_count{command="increment",module="counter"}');
        });
    });

    describe('scrape-time outbox/audit gauges', () => {
        let nodes: ModuleNode[];
        let drivers: EffectDriver[];
        let metrics: MetricsRegistry;

        afterEach(() => {
            drivers.forEach((d) => d.stop());
            nodes.forEach((n) => n.stop());
        });

        it('reflects outbox pending/done and audit size through the collector', async () => {
            metrics = new MetricsRegistry();
            // A 3-node cluster whose LEADER's state machine has metrics wired (the
            // collector reads the leader's host outbox/audit at scrape time).
            const registry = new Map<string, RpcHandler>();
            const transport = new LocalTransport(registry, 1);
            const ids = ['node1', 'node2', 'node3'];
            const machines = new Map<string, ModuleStateMachine>();
            nodes = ids.map((id) => {
                const peers: PeerInfo[] = ids.filter((p) => p !== id).map((p) => ({ id: p, url: `local://${p}` }));
                const sm = new ModuleStateMachine(new ModuleHost(), metrics);
                sm.host.registerModules([payments]);
                machines.set(id, sm);
                const n: ModuleNode = new RaftNode({ id, peers, stateMachine: sm, ...TEST_TIMERS }, transport);
                registry.set(id, n);
                return n;
            });
            const handler: EffectHandler = async (intent) => {
                const { orderId } = intent.payload as { orderId: string };
                return { orderId, ok: true };
            };
            drivers = nodes.map((n) => new EffectDriver(n, { http: handler }, { intervalMs: 10, metrics }));
            nodes.forEach((n) => n.start());

            await waitFor(() => leaders(nodes).length === 1);
            const leader = leaders(nodes)[0];
            // The scrape collector must read the leader's host, so register it now.
            metrics.registerCollector(() => machines.get(leader.id)!.collectMetrics(metrics));

            const m = meta('charge-1', 'alice');
            const res = await leader.submit(buildModuleCommand('payments', 'charge', { orderId: 'o1', amount: 100 }, m), m);
            expect(res.status).toBe(200);

            // After the charge (before the driver settles) the effect is pending.
            let text = metrics.expose();
            expect(sampleValue(text, 'module_outbox_pending ')).toBeGreaterThanOrEqual(1);
            expect(sampleValue(text, 'module_audit_size ')).toBe(leader.app.host.auditSize());
            expect(sampleValue(text, 'module_registered ')).toBe(1);

            // Start the drivers; the leader drains the effect and `settle` commits.
            drivers.forEach((d) => d.start());
            await waitFor(() => leader.app.host.getOutbox()[0]?.status === 'done');

            text = metrics.expose();
            expect(sampleValue(text, 'module_outbox_done ')).toBeGreaterThanOrEqual(1);
            expect(sampleValue(text, 'module_audit_size ')).toBe(leader.app.host.auditSize());
        });

        it('effect_runs_total{outcome="success"} increments when a handler runs', async () => {
            metrics = new MetricsRegistry();
            nodes = buildModuleCluster(3, [payments]);
            const handler: EffectHandler = async (intent) => {
                const { orderId } = intent.payload as { orderId: string };
                return { orderId, ok: true };
            };
            drivers = nodes.map((n) => new EffectDriver(n, { http: handler }, { intervalMs: 10, metrics }));
            nodes.forEach((n) => n.start());
            drivers.forEach((d) => d.start());

            await waitFor(() => leaders(nodes).length === 1);
            const leader = leaders(nodes)[0];

            const m = meta('charge-1', 'alice');
            await leader.submit(buildModuleCommand('payments', 'charge', { orderId: 'o1', amount: 100 }, m), m);
            await waitFor(() => leader.app.host.getOutbox()[0]?.status === 'done');

            const text = metrics.expose();
            expect(sampleValue(text, 'effect_runs_total{kind="http",outcome="success"}')).toBe(1);
        });
    });

    describe('ShardRouter.collectMetrics', () => {
        let shardA: ModuleNode[];
        let shardB: ModuleNode[];

        afterEach(() => {
            [...shardA, ...shardB].forEach((n) => n.stop());
        });

        it('sets shard_count and shard_has_leader for a 2-shard setup', async () => {
            const metrics = new MetricsRegistry();
            shardA = buildModuleCluster(3, [counter]);
            shardB = buildModuleCluster(3, [counter]);
            [...shardA, ...shardB].forEach((n) => n.start());
            const router = new ShardRouter<ModuleAppCommand>([{ nodes: shardA }, { nodes: shardB }]);
            await waitFor(() => leaders(shardA).length === 1 && leaders(shardB).length === 1);

            router.collectMetrics(metrics);
            const text = metrics.expose();
            expect(sampleValue(text, 'shard_count ')).toBe(2);
            expect(sampleValue(text, 'shard_has_leader{shard="0"}')).toBe(1);
            expect(sampleValue(text, 'shard_has_leader{shard="1"}')).toBe(1);
        });
    });

    describe('metrics-less runtime is a no-op', () => {
        it('a ModuleStateMachine with no registry applies commands without throwing', async () => {
            const nodes = buildModuleCluster(1, [counter]);
            nodes.forEach((n) => n.start());
            try {
                await waitFor(() => nodes[0].isLeader());
                const m = meta('r1');
                const res = await nodes[0].submit(buildModuleCommand('counter', 'increment', { by: 5 }, m), m);
                expect(res.status).toBe(200);
                expect(nodes[0].app.host.query('counter', 'value')).toBe(5);
                // No metrics wired and no collectMetrics call: nothing to assert beyond
                // the apply path behaving exactly as before.
            } finally {
                nodes.forEach((n) => n.stop());
            }
        });

        it('attributes handler outcome exactly once: failure on throw, success even if submit fails', async () => {
            const intent = { kind: 'http', idempotencyKey: 'k1', payload: {} };

            // A throwing handler → exactly one `failure`, no `success`.
            const failMetrics = new MetricsRegistry();
            const failExec = new EffectExecutor(
                { http: async () => { throw new Error('network down'); } },
                async () => {},
                failMetrics,
            );
            await failExec.drain([intent]);
            const failText = failMetrics.expose();
            expect(sampleValue(failText, 'effect_runs_total{kind="http",outcome="failure"}')).toBe(1);
            expect(sampleValue(failText, 'effect_runs_total{kind="http",outcome="success"}')).toBeUndefined();

            // Handler succeeds but `submit` throws (e.g. lost leadership) → exactly
            // one `success`, NO `failure` (submit failure must not double-count).
            const okMetrics = new MetricsRegistry();
            const okExec = new EffectExecutor(
                { http: async () => ({ ok: true }) },
                async () => { throw new Error('submit boom'); },
                okMetrics,
            );
            await okExec.drain([intent]);
            const okText = okMetrics.expose();
            expect(sampleValue(okText, 'effect_runs_total{kind="http",outcome="success"}')).toBe(1);
            expect(sampleValue(okText, 'effect_runs_total{kind="http",outcome="failure"}')).toBeUndefined();
        });

        it('an EffectExecutor/driver with no metrics runs handlers normally', async () => {
            const nodes = buildModuleCluster(3, [payments]);
            const handler: EffectHandler = async (intent) => {
                const { orderId } = intent.payload as { orderId: string };
                return { orderId, ok: true };
            };
            // No `metrics` in opts: the driver's executor never touches a registry.
            const drivers = nodes.map((n) => new EffectDriver(n, { http: handler }, { intervalMs: 10 }));
            nodes.forEach((n) => n.start());
            drivers.forEach((d) => d.start());
            try {
                await waitFor(() => leaders(nodes).length === 1);
                const leader = leaders(nodes)[0];
                const m = meta('charge-1', 'alice');
                await leader.submit(buildModuleCommand('payments', 'charge', { orderId: 'o1', amount: 1 }, m), m);
                await waitFor(() => leader.app.host.getOutbox()[0]?.status === 'done');
                expect(leader.app.host.getOutbox()[0].status).toBe('done');
            } finally {
                drivers.forEach((d) => d.stop());
                nodes.forEach((n) => n.stop());
            }
        });
    });
});
