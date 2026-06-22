import dotenv from 'dotenv';
import { createApp } from './app';
import { RaftNode } from './consensus/raftNode';
import { HttpTransport } from './consensus/transport';
import { FileStorage } from './consensus/storage';
import { getPort, loadRaftConfig } from './consensus/config';
import { createLogger } from './platform/logger';
import { metrics } from './platform/metrics';
import { BookStateMachine } from './models/bookStateMachine';
import { buildBookStreamGuard } from './models/bookStreamGuard';

dotenv.config();

const logger = createLogger();
const config = loadRaftConfig();
const storage = new FileStorage(config.id, process.env.DATA_DIR || './data');

// Plug the book application into the consensus core. Swap in any other
// StateMachine here to run a different domain on the same substrate.
const node = new RaftNode(
    { ...config, stateMachine: new BookStateMachine(), logger, metrics, storage },
    new HttpTransport(),
);

// Refresh Raft gauges whenever /metrics is scraped.
metrics.registerCollector(() => node.collectMetrics());

// Per-client authorization + partial replication for the edge read stream
// (ADR-0023). The token→scope registry comes from STREAM_TOKENS (e.g.
// "reader=*,acme=Acme Press"); it defaults to a single public `demo=*` token so
// the worked example runs out of the box. Swap this for JWT verification in prod.
const streamGuard = buildBookStreamGuard(process.env.STREAM_TOKENS);

const app = createApp(node, { logger, metrics, streamGuard });
const PORT = getPort();

const server = app.listen(PORT, () => {
    logger.info('node started', { node: config.id, port: PORT, peers: config.peers.length });
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
