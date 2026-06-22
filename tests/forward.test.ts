import http from 'http';
import { AddressInfo } from 'net';
import { forwardToLeader, isForwarded } from '../src/platform/forward';
import { runWithContext } from '../src/platform/requestContext';

/**
 * Unit tests for the leader-forwarding proxy (`src/platform/forward.ts`) — the
 * write path any non-leader node uses to relay a request to the current leader.
 * We stand up a stub HTTP server as the "leader" and drive `forwardToLeader`
 * with minimal Express-shaped req/res doubles, covering: a successful relay,
 * verbatim content-type/body passthrough for a non-JSON error body, request
 * context propagation + the anti-loop header, and the two failure fallbacks
 * (connection error and socket timeout) that must resolve `false`.
 */

/** Capture what `forwardToLeader` writes back through the Express Response. */
function mockRes() {
    const captured: { status?: number; type?: string; body?: string } = {};
    const res = {
        type(t: string) {
            captured.type = t;
            return res;
        },
        status(c: number) {
            captured.status = c;
            return res;
        },
        send(d: string) {
            captured.body = d;
            return res;
        },
    };
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    return { res: res as any, captured };
}

/** Minimal Express-shaped Request double. */
function mockReq(opts: {
    originalUrl?: string;
    method?: string;
    body?: unknown;
    headers?: Record<string, string>;
}) {
    const headers = opts.headers ?? {};
    return {
        originalUrl: opts.originalUrl ?? '/books',
        method: opts.method ?? 'POST',
        body: opts.body,
        header(name: string): string | undefined {
            return headers[name.toLowerCase()];
        },
        // eslint-disable-next-line @typescript-eslint/no-explicit-any
    } as any;
}

function listen(server: http.Server): Promise<string> {
    return new Promise((resolve) => {
        server.listen(0, '127.0.0.1', () => {
            const port = (server.address() as AddressInfo).port;
            resolve(`http://127.0.0.1:${port}`);
        });
    });
}

describe('forwardToLeader', () => {
    const servers: http.Server[] = [];

    afterEach(async () => {
        await Promise.all(
            servers.splice(0).map((s) => new Promise<void>((r) => s.close(() => r()))),
        );
    });

    it('relays the request to the leader and returns its response verbatim', async () => {
        let seen: { method?: string; url?: string; body?: string; forwardedBy?: string } = {};
        const server = http.createServer((req, res) => {
            let body = '';
            req.on('data', (c) => (body += c));
            req.on('end', () => {
                seen = {
                    method: req.method,
                    url: req.url,
                    body,
                    forwardedBy: req.headers['x-forwarded-by'] as string,
                };
                res.writeHead(201, { 'Content-Type': 'application/json' });
                res.end(JSON.stringify({ ok: true }));
            });
        });
        servers.push(server);
        const leaderUrl = await listen(server);

        const { res, captured } = mockRes();
        const req = mockReq({ originalUrl: '/books?x=1', method: 'POST', body: { title: 'Dune' } });
        const ok = await forwardToLeader(req, res, leaderUrl);

        expect(ok).toBe(true);
        expect(captured.status).toBe(201);
        expect(captured.type).toMatch(/application\/json/);
        expect(captured.body).toBe(JSON.stringify({ ok: true }));
        // The leader saw the original method/url, the JSON body, and the anti-loop mark.
        expect(seen.method).toBe('POST');
        expect(seen.url).toBe('/books?x=1');
        expect(JSON.parse(seen.body!)).toEqual({ title: 'Dune' });
        expect(seen.forwardedBy).toBe('cluster');
    });

    it('relays a non-JSON error body with its content-type intact', async () => {
        const server = http.createServer((_req, res) => {
            res.writeHead(503, { 'Content-Type': 'text/plain' });
            res.end('no leader yet');
        });
        servers.push(server);
        const leaderUrl = await listen(server);

        const { res, captured } = mockRes();
        const ok = await forwardToLeader(mockReq({}), res, leaderUrl);

        expect(ok).toBe(true);
        expect(captured.status).toBe(503);
        expect(captured.type).toMatch(/text\/plain/);
        expect(captured.body).toBe('no leader yet');
    });

    it('propagates the request context as X-Request-Id / X-Actor headers', async () => {
        let headers: http.IncomingHttpHeaders = {};
        const server = http.createServer((req, res) => {
            headers = req.headers;
            req.resume();
            res.writeHead(200).end('{}');
        });
        servers.push(server);
        const leaderUrl = await listen(server);

        const { res } = mockRes();
        await runWithContext({ requestId: 'req-42', actor: 'alice' }, () =>
            forwardToLeader(mockReq({}), res, leaderUrl),
        );

        expect(headers['x-request-id']).toBe('req-42');
        expect(headers['x-actor']).toBe('alice');
    });

    it('returns false (does not write a response) when the leader is unreachable', async () => {
        // Bind then immediately close to obtain a port nothing is listening on.
        const probe = http.createServer();
        const leaderUrl = await listen(probe);
        await new Promise<void>((r) => probe.close(() => r()));

        const { res, captured } = mockRes();
        const ok = await forwardToLeader(mockReq({}), res, leaderUrl);

        expect(ok).toBe(false);
        expect(captured.status).toBeUndefined();
        expect(captured.body).toBeUndefined();
    });

    it('returns false when the leader accepts but never responds (socket timeout)', async () => {
        // Accept the connection but never reply, so the 2s request timeout fires.
        const server = http.createServer(() => {
            /* intentionally hang */
        });
        servers.push(server);
        const leaderUrl = await listen(server);

        const { res, captured } = mockRes();
        const ok = await forwardToLeader(mockReq({}), res, leaderUrl);

        expect(ok).toBe(false);
        expect(captured.status).toBeUndefined();
    }, 5000);
});

describe('isForwarded', () => {
    it('is true only for a request already marked by the cluster', () => {
        expect(isForwarded(mockReq({ headers: { 'x-forwarded-by': 'cluster' } }))).toBe(true);
        expect(isForwarded(mockReq({}))).toBe(false);
        expect(isForwarded(mockReq({ headers: { 'x-forwarded-by': 'someone-else' } }))).toBe(false);
    });
});
