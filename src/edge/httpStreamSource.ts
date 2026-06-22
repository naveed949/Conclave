import http from 'http';
import https from 'https';
import { URL } from 'url';
import { AppCommand } from '../consensus/types';
import { LogStreamSource, StreamHandlers } from './types';

/**
 * A {@link LogStreamSource} for Node (tests, server-side replicas, CLIs) that
 * consumes the `GET /raft/stream` Server-Sent Events endpoint over the stdlib
 * `http`/`https` modules — no external dependency (ADR-0013). It parses SSE
 * frames (`event:` / `data:` lines separated by a blank line) and dispatches the
 * `snapshot` / `entry` / `caughtup` events to the handlers. It does NOT retry:
 * the {@link EdgeReplica} owns reconnection and resume.
 *
 * Browsers use the native `EventSource` instead — see `eventSourceStreamSource.ts`.
 */
export class HttpStreamSource<C extends AppCommand = AppCommand> implements LogStreamSource<C> {
    private readonly token?: string;

    /**
     * @param baseUrl Base URL of any cluster node (e.g. `http://127.0.0.1:3001`).
     *   A follower works fine — reads are local and eventually consistent, so the
     *   stream fans read serving out across the cluster.
     * @param opts.token Bearer token for an authorized/scoped stream (ADR-0023).
     *   Sent as `Authorization: Bearer <token>`; required when the node has a
     *   StreamGuard configured.
     */
    constructor(private readonly baseUrl: string, opts: { token?: string } = {}) {
        this.token = opts.token;
    }

    connect(fromIndex: number, handlers: StreamHandlers<C>): () => void {
        const url = new URL('/raft/stream', this.baseUrl);
        url.searchParams.set('fromIndex', String(fromIndex));
        const client = url.protocol === 'https:' ? https : http;

        let closed = false;
        let buffer = '';

        const headers: Record<string, string> = { Accept: 'text/event-stream' };
        if (this.token) headers.Authorization = `Bearer ${this.token}`;

        const req = client.get(
            url,
            { headers },
            (res) => {
                if (res.statusCode === 401 || res.statusCode === 403) {
                    res.resume();
                    if (!closed) handlers.onError(new Error(`stream unauthorized (HTTP ${res.statusCode})`));
                    return;
                }
                if (res.statusCode !== 200) {
                    res.resume(); // drain
                    if (!closed) handlers.onError(new Error(`stream HTTP ${res.statusCode}`));
                    return;
                }
                handlers.onOpen?.();
                res.setEncoding('utf8');
                res.on('data', (chunk: string) => {
                    buffer += chunk;
                    // SSE frames are separated by a blank line.
                    let sep: number;
                    while ((sep = buffer.indexOf('\n\n')) !== -1) {
                        const frame = buffer.slice(0, sep);
                        buffer = buffer.slice(sep + 2);
                        dispatchFrame(frame, handlers);
                    }
                });
                res.on('end', () => {
                    if (!closed) handlers.onError(new Error('stream ended'));
                });
            },
        );

        req.on('error', (err) => {
            if (!closed) handlers.onError(err);
        });

        return () => {
            closed = true;
            req.destroy();
        };
    }
}

/** Parse one SSE frame and dispatch it. Comment-only frames (`: …`) are ignored. */
function dispatchFrame<C extends AppCommand>(frame: string, handlers: StreamHandlers<C>): void {
    let event = 'message';
    const dataLines: string[] = [];
    for (const line of frame.split('\n')) {
        if (line.startsWith(':')) continue; // keepalive / comment
        if (line.startsWith('event:')) event = line.slice('event:'.length).trim();
        else if (line.startsWith('data:')) dataLines.push(line.slice('data:'.length).trim());
        // `retry:` and other fields are not needed here (reconnect is the replica's job).
    }
    if (dataLines.length === 0) return;

    let payload: unknown;
    try {
        payload = JSON.parse(dataLines.join('\n'));
    } catch (err) {
        handlers.onError(err instanceof Error ? err : new Error('bad SSE payload'));
        return;
    }

    switch (event) {
        case 'snapshot':
            handlers.onSnapshot(payload as Parameters<StreamHandlers<C>['onSnapshot']>[0]);
            break;
        case 'entry':
            handlers.onEntry(payload as Parameters<StreamHandlers<C>['onEntry']>[0]);
            break;
        case 'caughtup':
            handlers.onCaughtUp((payload as { index: number }).index);
            break;
        default:
            // Unknown event types are ignored (forward-compatible).
            break;
    }
}
