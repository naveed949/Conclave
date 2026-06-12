import dotenv from 'dotenv';
import { createApp } from './app';
import { RaftNode } from './consensus/raftNode';
import { HttpTransport } from './consensus/transport';
import { getPort, loadRaftConfig } from './consensus/config';

dotenv.config();

const config = loadRaftConfig();
const node = new RaftNode(config, new HttpTransport());
const app = createApp(node);

const PORT = getPort();

const server = app.listen(PORT, () => {
    console.log(`Node "${config.id}" listening on port ${PORT} with ${config.peers.length} peer(s)`);
    node.start();
});

const shutdown = () => {
    node.stop();
    server.close(() => process.exit(0));
};
process.on('SIGINT', shutdown);
process.on('SIGTERM', shutdown);

export { app, node };
export default app;
