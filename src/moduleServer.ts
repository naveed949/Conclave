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

dotenv.config();

const logger = createLogger();
const config = loadRaftConfig();
const storage = new FileStorage(config.id, process.env.DATA_DIR || './data');

// Plug the module runtime into the consensus core (the analog of `server.ts`
// wiring `BookStateMachine`). Register a demo module set; because modules are
// registered identically on every node BEFORE start, MODULE commands apply
// deterministically and the cluster converges.
const stateMachine = new ModuleStateMachine();
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
];
stateMachine.host.registerModules(demoModules);

const node: ModuleNode = new RaftNode(
    { ...config, stateMachine, logger, metrics, storage },
    new HttpTransport(),
);

// Refresh Raft gauges whenever /metrics is scraped.
metrics.registerCollector(() => node.collectMetrics());

const app = createModuleApp(node, { logger, metrics });
const PORT = getPort();

const server = app.listen(PORT, () => {
    logger.info('module node started', { node: config.id, port: PORT, peers: config.peers.length });
    node.start();
});

const shutdown = () => {
    logger.info('shutting down', { node: config.id });
    node.stop();
    server.close(() => process.exit(0));
};
process.on('SIGINT', shutdown);
process.on('SIGTERM', shutdown);

export { app, node };
export default app;
