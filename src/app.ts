import express, { Application } from 'express';
import cors from 'cors';
import { RaftNode } from './consensus/raftNode';
import bookRoutes from './routes/bookRoutes';
import raftRoutes from './routes/raftRoutes';

/**
 * Build an Express app bound to a given Raft node. Kept as a factory (rather
 * than a module-level singleton) so tests can wire up isolated nodes/clusters
 * without any global state or external database.
 */
export function createApp(node: RaftNode): Application {
    const app: Application = express();

    app.use(express.json());
    app.use(cors());

    app.use('/books', bookRoutes(node));
    app.use('/raft', raftRoutes(node));

    app.get('/health', (_req, res) => res.json({ status: 'ok', node: node.status() }));

    return app;
}
