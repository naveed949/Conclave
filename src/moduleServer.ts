import dotenv from 'dotenv';
import { createModuleApp } from './moduleApp';
import { RaftNode } from './consensus/raftNode';
import { HttpTransport } from './consensus/transport';
import { FileStorage } from './consensus/storage';
import { getPort, loadRaftConfig } from './consensus/config';
import { createLogger } from './platform/logger';
import { metrics } from './platform/metrics';
import { ModuleStateMachine, ModuleNode } from './runtime/moduleStateMachine';
import { AnyModuleDefinition } from './runtime/moduleHost';
import { counter } from './runtime/modules/counter';
import { notes } from './runtime/modules/notes';
import { accounts } from './runtime/modules/accounts';
import { payments } from './runtime/modules/payments';
import { EffectDriver } from './runtime/effectDriver';
import { EffectHandler } from './runtime/types';

dotenv.config();

const logger = createLogger();
const config = loadRaftConfig();
const storage = new FileStorage(config.id, process.env.DATA_DIR || './data');

// Plug the module runtime into the consensus core (the analog of `server.ts`
// wiring `BookStateMachine`). Register a demo module set; because modules are
// registered identically on every node BEFORE start, MODULE commands apply
// deterministically and the cluster converges.
const stateMachine = new ModuleStateMachine(undefined, metrics);
// A `Reducer<S>` is INVARIANT in its state type `S` (it both consumes and
// produces `S`), so a strongly-typed `ModuleDefinition<CounterState>` is not
// assignable to the host's erased `ModuleDefinition<unknown>` slot — even though
// the host treats state as opaque. Erase the state type at this registration
// boundary; the host only ever round-trips it through the reducers, so this is
// sound. (`accounts` is keyed and needs no erasure.)
const demoModules: AnyModuleDefinition[] = [
    counter as AnyModuleDefinition,
    notes as AnyModuleDefinition,
    accounts,
    payments as AnyModuleDefinition,
];
stateMachine.host.registerModules(demoModules);

const node: ModuleNode = new RaftNode(
    { ...config, stateMachine, logger, metrics, storage },
    new HttpTransport(),
);

// Refresh Raft + module-runtime gauges whenever /metrics is scraped (M15).
metrics.registerCollector(() => node.collectMetrics());
metrics.registerCollector(() => stateMachine.collectMetrics(metrics));

const app = createModuleApp(node, { logger, metrics });
const PORT = getPort();

// The committed-intent effect loop (M12). The driver polls the leader's outbox
// post-commit and runs each pending effect's handler at the edge, feeding the
// result back through the log. The `http` handler is the demo effect for the
// `payments` module: it resolves a deterministic-SHAPED outcome (no real
// network) so the `settle` follow-up can flip the order to paid on every node.
const effectHandlers: Record<string, EffectHandler> = {
    http: async (intent) => {
        const { orderId } = (intent.payload ?? {}) as { orderId: string };
        return { orderId, ok: true };
    },
};
const effectDriver = new EffectDriver(node, effectHandlers, { metrics });

const server = app.listen(PORT, () => {
    logger.info('module node started', { node: config.id, port: PORT, peers: config.peers.length });
    node.start();
    // Start after the node so the driver only ever acts once a leader is known.
    effectDriver.start();
});

const shutdown = () => {
    logger.info('shutting down', { node: config.id });
    effectDriver.stop();
    node.stop();
    server.close(() => process.exit(0));
};
process.on('SIGINT', shutdown);
process.on('SIGTERM', shutdown);

export { app, node };
export default app;
