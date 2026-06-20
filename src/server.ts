import dotenv from 'dotenv';
import { createApp } from './app';
import { RaftNode } from './consensus/raftNode';
import { HttpTransport } from './consensus/transport';
import { FileStorage } from './consensus/storage';
import { getPort, loadRaftConfig } from './consensus/config';
import { createLogger } from './platform/logger';
import { metrics } from './platform/metrics';

dotenv.config();

const logger = createLogger();
const config = loadRaftConfig();
const storage = new FileStorage(config.id, process.env.DATA_DIR || './data');

const node = new RaftNode({ ...config, logger, metrics, storage }, new HttpTransport());

// Refresh Raft gauges whenever /metrics is scraped.
metrics.registerCollector(() => node.collectMetrics());

const app = createApp(node, { logger, metrics });
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
