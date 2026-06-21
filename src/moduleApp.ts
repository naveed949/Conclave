import express, { Application, NextFunction, Request, Response } from 'express';
import cors from 'cors';
import { ModuleNode } from './runtime/moduleStateMachine';
import { Logger } from './platform/logger';
import { MetricsRegistry } from './platform/metrics';
import { requestContextMiddleware, getContext } from './platform/requestContext';
import moduleRoutes from './routes/moduleRoutes';
import raftRoutes from './routes/raftRoutes';
import auditRoutes from './routes/auditRoutes';

export interface ModuleAppDeps {
    logger?: Logger;
    metrics?: MetricsRegistry;
}

/**
 * Build an Express app bound to a node whose application is the module runtime
 * ({@link ModuleNode}) — the runtime analog of `createApp` in `app.ts`. It wires
 * the SAME platform middleware (request context, structured access logs, HTTP
 * metrics) and mounts the generic module surface under `/modules`. The
 * `/raft`, `/audit`, `/health`, `/ready`, `/metrics` routes are application-
 * agnostic (generic over any `RaftNode`), so they are reused verbatim.
 */
export function createModuleApp(node: ModuleNode, deps: ModuleAppDeps = {}): Application {
    const app: Application = express();
    const { logger, metrics } = deps;

    app.use(express.json());
    app.use(cors());
    app.use(requestContextMiddleware);

    // Access logging + HTTP metrics, recorded once the response is sent.
    app.use((req: Request, res: Response, next: NextFunction) => {
        const start = Date.now();
        res.on('finish', () => {
            const durationMs = Date.now() - start;
            // Use the matched route template (e.g. /modules/:module/:command), not the
            // raw path, so per-module requests don't explode metric label cardinality.
            const route = req.route ? `${req.baseUrl}${req.route.path}` : req.baseUrl || 'unmatched';
            metrics?.httpRequests.inc({ method: req.method, route, status: res.statusCode });
            metrics?.httpDuration.observe(durationMs, { method: req.method, route });
            logger?.info('http_request', {
                method: req.method,
                path: req.originalUrl,
                status: res.statusCode,
                durationMs,
            });
        });
        next();
    });

    app.use('/modules', moduleRoutes(node));
    app.use('/raft', raftRoutes(node));
    app.use('/audit', auditRoutes(node));

    // Liveness: process is up.
    app.get('/health', (_req, res) => res.json({ status: 'ok', node: node.status() }));

    // Readiness: the cluster has a leader this node recognises (safe to route writes).
    app.get('/ready', (_req, res) => {
        const ready = node.getLeaderId() !== null;
        res.status(ready ? 200 : 503).json({ ready, leader: node.getLeaderId() });
    });

    // Prometheus scrape endpoint.
    app.get('/metrics', (_req, res) => {
        if (!metrics) {
            res.status(404).json({ message: 'metrics disabled' });
            return;
        }
        res.type('text/plain').send(metrics.expose());
    });

    // Centralized error handler — structured, with the request id for tracing.
    app.use((err: Error, _req: Request, res: Response, _next: NextFunction) => {
        logger?.error('unhandled_error', { error: err.message, requestId: getContext()?.requestId });
        res.status(500).json({ message: 'Server error' });
    });

    return app;
}
