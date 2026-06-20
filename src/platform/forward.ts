import http from 'http';
import { URL } from 'url';
import { Request, Response } from 'express';
import { getContext } from './requestContext';

/**
 * Transparently proxy a write request to the current leader so any node can
 * accept writes (the client needn't know who the leader is). The leader's
 * response is relayed verbatim. Returns false if forwarding failed so the
 * caller can fall back to a 421.
 */
export function forwardToLeader(req: Request, res: Response, leaderUrl: string): Promise<boolean> {
    return new Promise((resolve) => {
        const target = new URL(req.originalUrl, leaderUrl);
        const body = JSON.stringify(req.body ?? {});
        const ctx = getContext();

        const proxy = http.request(
            {
                hostname: target.hostname,
                port: target.port,
                path: target.pathname + target.search,
                method: req.method,
                headers: {
                    'Content-Type': 'application/json',
                    'Content-Length': Buffer.byteLength(body),
                    ...(ctx ? { 'X-Request-Id': ctx.requestId, 'X-Actor': ctx.actor } : {}),
                    // Mark as forwarded so a stale leader won't bounce it back into a loop.
                    'X-Forwarded-By': 'cluster',
                },
                timeout: 2000,
            },
            (upstream) => {
                let data = '';
                upstream.on('data', (c) => (data += c));
                upstream.on('end', () => {
                    // Relay the upstream content-type verbatim rather than forcing JSON —
                    // an error body from the leader may be HTML/plain text.
                    const contentType = upstream.headers['content-type'];
                    if (contentType) res.type(contentType);
                    res.status(upstream.statusCode || 502).send(data);
                    resolve(true);
                });
            },
        );
        proxy.on('error', () => resolve(false));
        proxy.on('timeout', () => { proxy.destroy(); resolve(false); });
        proxy.write(body);
        proxy.end();
    });
}

/** A request already forwarded once must not be forwarded again. */
export function isForwarded(req: Request): boolean {
    return req.header('x-forwarded-by') === 'cluster';
}
