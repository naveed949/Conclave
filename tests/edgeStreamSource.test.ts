import http from 'http';
import { Server } from 'http';
import { AddressInfo } from 'net';
import { HttpStreamSource } from '../src/edge/httpStreamSource';
import {
    EventSourceStreamSource,
    EventSourceCtor,
    EventSourceLike,
    MessageEventLike,
} from '../src/edge/eventSourceStreamSource';
import { StreamHandlers, StreamSnapshot, StreamEntry } from '../src/edge/types';
import { AppCommand } from '../src/consensus/types';
import { waitFor } from './helpers';

/**
 * Unit-level coverage for the edge stream-source internals that the HTTP-backed
 * EdgeReplica integration suites don't exercise directly: SSE frame parsing,
 * malformed-payload handling, mid-stream disconnect -> onError, the close()
 * teardown contract for {@link HttpStreamSource}, and the open/message/error/
 * close paths of {@link EventSourceStreamSource} via an injected double.
 */

jest.setTimeout(15000);

interface Cmd extends AppCommand {
    type: string;
}

/** A recording StreamHandlers: every callback pushes onto an ordered event log. */
function recordingHandlers(): {
    handlers: StreamHandlers<Cmd>;
    events: string[];
    snapshots: StreamSnapshot[];
    entries: StreamEntry<Cmd>[];
    caughtUp: number[];
    errors: Error[];
} {
    const events: string[] = [];
    const snapshots: StreamSnapshot[] = [];
    const entries: StreamEntry<Cmd>[] = [];
    const caughtUp: number[] = [];
    const errors: Error[] = [];
    const handlers: StreamHandlers<Cmd> = {
        onOpen() {
            events.push('open');
        },
        onSnapshot(snap) {
            events.push('snapshot');
            snapshots.push(snap);
        },
        onEntry(item) {
            events.push('entry');
            entries.push(item);
        },
        onCaughtUp(index) {
            events.push('caughtup');
            caughtUp.push(index);
        },
        onError(err) {
            events.push('error');
            errors.push(err);
        },
    };
    return { handlers, events, snapshots, entries, caughtUp, errors };
}

const sse = (event: string, data: unknown): string => `event: ${event}\ndata: ${JSON.stringify(data)}\n\n`;

describe('HttpStreamSource (Node SSE client)', () => {
    let server: Server;
    let baseUrl: string;
    const sockets = new Set<import('net').Socket>();
    // Per-test hook: receives the request + response so each test scripts the stream.
    let onRequest: (req: http.IncomingMessage, res: http.ServerResponse) => void;

    beforeEach(async () => {
        onRequest = (_req, res) => res.end();
        server = http.createServer((req, res) => onRequest(req, res));
        // Track live sockets so teardown can force-close any held-open SSE stream;
        // otherwise server.close() would block on the long-lived connection.
        server.on('connection', (s) => {
            sockets.add(s);
            s.on('close', () => sockets.delete(s));
        });
        await new Promise<void>((r) => server.listen(0, '127.0.0.1', () => r()));
        baseUrl = `http://127.0.0.1:${(server.address() as AddressInfo).port}`;
    });

    afterEach(async () => {
        for (const s of sockets) s.destroy();
        sockets.clear();
        await new Promise<void>((r) => server.close(() => r()));
    });

    it('parses a snapshot -> entry -> caughtup sequence in order and decodes payloads', async () => {
        const snapshot: StreamSnapshot = {
            lastIncludedIndex: 5,
            lastIncludedTerm: 2,
            members: [{ id: 'n1', url: 'http://n1' }],
            data: { state: { count: 7 } },
        };
        const streamEntry: StreamEntry<Cmd> = { index: 6, entry: { term: 2, command: { type: 'op' } } };

        onRequest = (req, res) => {
            // The fromIndex query is propagated onto the URL.
            expect(req.url).toContain('fromIndex=5');
            res.writeHead(200, { 'Content-Type': 'text/event-stream' });
            res.write(': keepalive comment\n\n');
            res.write(sse('snapshot', snapshot));
            res.write(sse('entry', streamEntry));
            res.write(sse('caughtup', { index: 6 }));
            // Leave the connection open; close() will tear it down.
        };

        const source = new HttpStreamSource<Cmd>(baseUrl);
        const rec = recordingHandlers();
        const close = source.connect(5, rec.handlers);

        await waitFor(() => rec.caughtUp.length === 1, 5000);
        close();

        expect(rec.events).toEqual(['open', 'snapshot', 'entry', 'caughtup']);
        expect(rec.snapshots[0]).toEqual(snapshot);
        expect(rec.entries[0]).toEqual(streamEntry);
        expect(rec.caughtUp[0]).toBe(6);
        expect(rec.errors).toHaveLength(0);
    });

    it('handles a frame split across multiple TCP chunks', async () => {
        const streamEntry: StreamEntry<Cmd> = { index: 1, entry: { term: 1, command: { type: 'op' } } };
        const full = sse('entry', streamEntry);
        const mid = Math.floor(full.length / 2);

        onRequest = (_req, res) => {
            res.writeHead(200, { 'Content-Type': 'text/event-stream' });
            res.write(full.slice(0, mid));
            setTimeout(() => res.write(full.slice(mid)), 20);
        };

        const source = new HttpStreamSource<Cmd>(baseUrl);
        const rec = recordingHandlers();
        const close = source.connect(0, rec.handlers);

        await waitFor(() => rec.entries.length === 1, 5000);
        close();
        expect(rec.entries[0]).toEqual(streamEntry);
        expect(rec.errors).toHaveLength(0);
    });

    it('surfaces a malformed (non-JSON) data frame via onError without crashing', async () => {
        onRequest = (_req, res) => {
            res.writeHead(200, { 'Content-Type': 'text/event-stream' });
            res.write('event: entry\ndata: {not valid json}\n\n');
            // A valid frame after the bad one still gets dispatched -> consumer survives.
            res.write(sse('caughtup', { index: 3 }));
        };

        const source = new HttpStreamSource<Cmd>(baseUrl);
        const rec = recordingHandlers();
        const close = source.connect(0, rec.handlers);

        await waitFor(() => rec.caughtUp.length === 1, 5000);
        close();

        expect(rec.errors).toHaveLength(1);
        expect(rec.caughtUp[0]).toBe(3);
    });

    it('ignores a comment-only frame and an unknown event type (forward-compatible)', async () => {
        onRequest = (_req, res) => {
            res.writeHead(200, { 'Content-Type': 'text/event-stream' });
            res.write(': just a comment\n\n');
            res.write('event: futurething\ndata: {"hello":"world"}\n\n');
            res.write('event: entry\n\n'); // no data line -> ignored
            res.write(sse('caughtup', { index: 1 }));
        };

        const source = new HttpStreamSource<Cmd>(baseUrl);
        const rec = recordingHandlers();
        const close = source.connect(0, rec.handlers);

        await waitFor(() => rec.caughtUp.length === 1, 5000);
        close();

        // Only open + caughtup; the comment/unknown/empty frames produced nothing.
        expect(rec.events).toEqual(['open', 'caughtup']);
        expect(rec.errors).toHaveLength(0);
    });

    it('reports onError when the server disconnects mid-stream (after delivering entries)', async () => {
        onRequest = (_req, res) => {
            res.writeHead(200, { 'Content-Type': 'text/event-stream' });
            res.write(sse('entry', { index: 1, entry: { term: 1, command: { type: 'op' } } }));
            // The server tears the stream down after the first frame; the client sees
            // end-of-response and surfaces it as an error for the replica to reconnect.
            setTimeout(() => res.end(), 20);
        };

        const source = new HttpStreamSource<Cmd>(baseUrl);
        const rec = recordingHandlers();
        const close = source.connect(0, rec.handlers);

        await waitFor(() => rec.errors.length === 1, 5000);
        close();
        expect(rec.entries).toHaveLength(1);
        expect(rec.errors[0]).toBeInstanceOf(Error);
        expect(rec.errors[0].message).toMatch(/stream ended/);
    });

    it('does not crash on an abrupt socket reset (currently swallowed; see report)', async () => {
        onRequest = (_req, res) => {
            res.writeHead(200, { 'Content-Type': 'text/event-stream' });
            res.write(sse('entry', { index: 1, entry: { term: 1, command: { type: 'op' } } }));
            // Abrupt RST: Node fires 'aborted'/'close' on the response, NOT 'end' on the
            // response nor 'error' on the request — which the source does not listen for,
            // so no onError fires. We assert the consumer survives (no throw/crash).
            setTimeout(() => res.socket?.destroy(), 20);
        };

        const source = new HttpStreamSource<Cmd>(baseUrl);
        const rec = recordingHandlers();
        const close = source.connect(0, rec.handlers);

        await waitFor(() => rec.entries.length === 1, 5000);
        // Allow the abrupt reset to propagate; with the current source it produces no event.
        await new Promise((r) => setTimeout(r, 100));
        close();
        expect(rec.entries).toHaveLength(1);
        expect(rec.events).not.toContain('error');
    });

    it('reports onError when the server ends the stream cleanly', async () => {
        onRequest = (_req, res) => {
            res.writeHead(200, { 'Content-Type': 'text/event-stream' });
            res.end(sse('caughtup', { index: 1 }));
        };

        const source = new HttpStreamSource<Cmd>(baseUrl);
        const rec = recordingHandlers();
        const close = source.connect(0, rec.handlers);

        await waitFor(() => rec.errors.length === 1, 5000);
        close();
        // The clean end still surfaces as an error: the consumer (EdgeReplica) decides
        // whether to reconnect.
        expect(rec.caughtUp[0]).toBe(1);
        expect(rec.errors[0].message).toMatch(/stream ended/);
    });

    it('reports onError on a non-200 status (e.g. 500)', async () => {
        onRequest = (_req, res) => {
            res.writeHead(500);
            res.end('boom');
        };

        const source = new HttpStreamSource<Cmd>(baseUrl);
        const rec = recordingHandlers();
        const close = source.connect(0, rec.handlers);

        await waitFor(() => rec.errors.length === 1, 5000);
        close();
        expect(rec.events).not.toContain('open');
        expect(rec.errors[0].message).toMatch(/HTTP 500/);
    });

    it('reports an unauthorized stream (401/403) distinctly and does not open', async () => {
        onRequest = (req, res) => {
            // The bearer token rides the Authorization header.
            expect(req.headers.authorization).toBe('Bearer secret-tok');
            res.writeHead(403);
            res.end();
        };

        const source = new HttpStreamSource<Cmd>(baseUrl, { token: 'secret-tok' });
        const rec = recordingHandlers();
        const close = source.connect(0, rec.handlers);

        await waitFor(() => rec.errors.length === 1, 5000);
        close();
        expect(rec.events).not.toContain('open');
        expect(rec.errors[0].message).toMatch(/unauthorized.*403/);
    });

    it('close() stops the stream: no events fire after teardown', async () => {
        let res!: http.ServerResponse;
        const opened = new Promise<void>((resolve) => {
            onRequest = (_req, r) => {
                res = r;
                res.writeHead(200, { 'Content-Type': 'text/event-stream' });
                // Flush a keepalive comment so the response headers reach the client and
                // onOpen fires (writeHead alone may buffer under keep-alive).
                res.write(': open\n\n');
                resolve();
            };
        });

        const source = new HttpStreamSource<Cmd>(baseUrl);
        const rec = recordingHandlers();
        const close = source.connect(0, rec.handlers);

        await opened;
        await waitFor(() => rec.events.includes('open'), 5000);
        close();

        // After close(), a server-side end must NOT surface as an error (closed guard).
        res.end(sse('caughtup', { index: 1 }));
        // Give any in-flight 'end'/'error' callbacks a chance to (wrongly) fire.
        await new Promise((r) => setTimeout(r, 50));

        expect(rec.events).toEqual(['open']);
        expect(rec.errors).toHaveLength(0);
        expect(rec.caughtUp).toHaveLength(0);
    });

    it('routes to https for an https:// base URL (connection fails fast, no crash)', async () => {
        // We don't stand up TLS; we only assert the https branch is taken and the
        // resulting connection error is surfaced through onError (not thrown).
        const source = new HttpStreamSource<Cmd>('https://127.0.0.1:1/');
        const rec = recordingHandlers();
        const close = source.connect(0, rec.handlers);
        await waitFor(() => rec.errors.length === 1, 5000);
        close();
        // The connection error is surfaced (not thrown). Assert on shape rather than
        // `instanceof Error`, which can fail across the https module's internal realm.
        expect(typeof rec.errors[0].message).toBe('string');
        expect(rec.events).not.toContain('open');
    });
});

/**
 * A fully in-memory EventSource double. Unlike the socket-backed polyfill in
 * eventSourceSource.test.ts, this lets us drive open/message/error directly so we
 * can exercise EventSourceStreamSource's branches (parse failure, post-close
 * suppression, close() idempotency) deterministically.
 */
class FakeEventSource implements EventSourceLike {
    static last: FakeEventSource | null = null;
    onopen: ((ev: unknown) => void) | null = null;
    onerror: ((ev: unknown) => void) | null = null;
    closed = 0;
    readonly url: string;
    private readonly listeners = new Map<string, ((ev: MessageEventLike) => void)[]>();

    constructor(url: string) {
        this.url = url;
        FakeEventSource.last = this;
    }

    addEventListener(type: string, listener: (ev: MessageEventLike) => void): void {
        const arr = this.listeners.get(type) ?? [];
        arr.push(listener);
        this.listeners.set(type, arr);
    }

    close(): void {
        this.closed += 1;
    }

    // --- test drivers ---
    emitOpen(): void {
        this.onopen?.({});
    }
    emit(type: string, data: unknown): void {
        for (const cb of this.listeners.get(type) ?? []) cb({ data: JSON.stringify(data) });
    }
    emitRaw(type: string, data: string): void {
        for (const cb of this.listeners.get(type) ?? []) cb({ data });
    }
    emitError(): void {
        this.onerror?.({});
    }
}

describe('EventSourceStreamSource (browser path) via an injected double', () => {
    const ctor = FakeEventSource as unknown as EventSourceCtor;

    beforeEach(() => {
        FakeEventSource.last = null;
    });

    it('builds the stream URL with fromIndex and a token query param', () => {
        const source = new EventSourceStreamSource<Cmd>('http://node:3001/', ctor, { token: 'tok 1' });
        const rec = recordingHandlers();
        const close = source.connect(7, rec.handlers);

        const es = FakeEventSource.last!;
        expect(es.url).toBe('http://node:3001/raft/stream?fromIndex=7&token=tok%201');
        close();
    });

    it('uses & as the query separator when baseUrl already has a query string', () => {
        const source = new EventSourceStreamSource<Cmd>('http://node:3001/?x=1', ctor);
        const rec = recordingHandlers();
        const close = source.connect(0, rec.handlers);
        expect(FakeEventSource.last!.url).toBe('http://node:3001/?x=1/raft/stream&fromIndex=0');
        close();
    });

    it('dispatches open/snapshot/entry/caughtup and decodes payloads', () => {
        const source = new EventSourceStreamSource<Cmd>('http://node', ctor);
        const rec = recordingHandlers();
        source.connect(0, rec.handlers);
        const es = FakeEventSource.last!;

        const snapshot: StreamSnapshot = {
            lastIncludedIndex: 3,
            lastIncludedTerm: 1,
            members: [],
            data: { state: { count: 1 } },
        };
        const streamEntry: StreamEntry<Cmd> = { index: 4, entry: { term: 1, command: { type: 'op' } } };

        es.emitOpen();
        es.emit('snapshot', snapshot);
        es.emit('entry', streamEntry);
        es.emit('caughtup', { index: 4 });

        expect(rec.events).toEqual(['open', 'snapshot', 'entry', 'caughtup']);
        expect(rec.snapshots[0]).toEqual(snapshot);
        expect(rec.entries[0]).toEqual(streamEntry);
        expect(rec.caughtUp[0]).toBe(4);
    });

    it('surfaces a malformed message payload via onError and skips dispatch', () => {
        const source = new EventSourceStreamSource<Cmd>('http://node', ctor);
        const rec = recordingHandlers();
        source.connect(0, rec.handlers);
        const es = FakeEventSource.last!;

        es.emitRaw('entry', '{bad json');

        expect(rec.entries).toHaveLength(0);
        expect(rec.errors).toHaveLength(1);
        expect(rec.errors[0].message).toMatch(/bad SSE payload/);
    });

    it('on error: closes the EventSource and reports exactly once', () => {
        const source = new EventSourceStreamSource<Cmd>('http://node', ctor);
        const rec = recordingHandlers();
        source.connect(0, rec.handlers);
        const es = FakeEventSource.last!;

        es.emitError();
        es.emitError(); // second error after self-close is suppressed

        expect(rec.errors).toHaveLength(1);
        expect(rec.errors[0].message).toMatch(/EventSource error/);
        expect(es.closed).toBe(1);
    });

    it('close() closes the EventSource and suppresses a later error', () => {
        const source = new EventSourceStreamSource<Cmd>('http://node', ctor);
        const rec = recordingHandlers();
        const close = source.connect(0, rec.handlers);
        const es = FakeEventSource.last!;

        close();
        expect(es.closed).toBe(1);

        // An error arriving after an explicit close must not reach the handler.
        es.emitError();
        expect(rec.errors).toHaveLength(0);
    });

    it('throws when no EventSource is available and none is injected', () => {
        const g = globalThis as unknown as { EventSource?: unknown };
        const had = 'EventSource' in g;
        const prev = g.EventSource;
        delete g.EventSource;
        try {
            expect(() => new EventSourceStreamSource<Cmd>('http://node')).toThrow(/No EventSource/);
        } finally {
            if (had) g.EventSource = prev;
        }
    });

    it('falls back to a global EventSource when no ctor is passed', () => {
        const g = globalThis as unknown as { EventSource?: unknown };
        const had = 'EventSource' in g;
        const prev = g.EventSource;
        g.EventSource = FakeEventSource;
        try {
            const source = new EventSourceStreamSource<Cmd>('http://node');
            const rec = recordingHandlers();
            source.connect(0, rec.handlers);
            FakeEventSource.last!.emitOpen();
            expect(rec.events).toEqual(['open']);
        } finally {
            if (had) g.EventSource = prev;
            else delete g.EventSource;
        }
    });
});
