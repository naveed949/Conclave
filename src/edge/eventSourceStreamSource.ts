import { AppCommand } from '../consensus/types';
import { LogStreamSource, StreamHandlers } from './types';

/**
 * Minimal structural shape of the browser `EventSource` API. Declared locally so
 * this module compiles without the DOM lib (the project targets ES2017/Node for
 * the server). In a browser, pass the global `EventSource` constructor.
 */
export interface MessageEventLike {
    data: string;
}
export interface EventSourceLike {
    addEventListener(type: string, listener: (ev: MessageEventLike) => void): void;
    onerror: ((ev: unknown) => void) | null;
    onopen: ((ev: unknown) => void) | null;
    close(): void;
}
export type EventSourceCtor = new (url: string) => EventSourceLike;

/**
 * A {@link LogStreamSource} backed by the browser's native `EventSource`, for the
 * in-browser edge replica (ADR-0023). The native client handles the SSE framing;
 * we just map the named events to handler calls.
 *
 * Reconnection is the {@link EdgeReplica}'s job (it must resume from a NEW
 * `fromIndex`, which the URL encodes), so on error we close and report rather
 * than letting `EventSource` silently re-fetch the original `fromIndex`.
 *
 * Usage (browser):
 * ```js
 * const source = new EventSourceStreamSource('http://node:3001', EventSource);
 * const replica = new EdgeReplica({ app: new BookStateMachine(), source });
 * replica.start();
 * ```
 */
export class EventSourceStreamSource<C extends AppCommand = AppCommand> implements LogStreamSource<C> {
    private readonly ctor: EventSourceCtor;
    private readonly token?: string;

    /**
     * @param opts.token Bearer token for an authorized/scoped stream (ADR-0023).
     *   The native `EventSource` cannot set headers, so it rides the URL as
     *   `?token=` — prefer a short-lived token (see `extractStreamToken`).
     */
    constructor(private readonly baseUrl: string, eventSourceCtor?: EventSourceCtor, opts: { token?: string } = {}) {
        const ctor =
            eventSourceCtor ??
            (globalThis as unknown as { EventSource?: EventSourceCtor }).EventSource;
        if (!ctor) {
            throw new Error('No EventSource available; pass one to EventSourceStreamSource');
        }
        this.ctor = ctor;
        this.token = opts.token;
    }

    connect(fromIndex: number, handlers: StreamHandlers<C>): () => void {
        const sep = this.baseUrl.includes('?') ? '&' : '?';
        const tokenParam = this.token ? `&token=${encodeURIComponent(this.token)}` : '';
        const url = `${this.baseUrl.replace(/\/$/, '')}/raft/stream${sep}fromIndex=${fromIndex}${tokenParam}`;
        const es = new this.ctor(url);
        let closed = false;

        const parse = (ev: MessageEventLike): unknown | undefined => {
            try {
                return JSON.parse(ev.data);
            } catch {
                handlers.onError(new Error('bad SSE payload'));
                return undefined;
            }
        };

        es.onopen = () => handlers.onOpen?.();
        es.addEventListener('snapshot', (ev) => {
            const p = parse(ev);
            if (p) handlers.onSnapshot(p as Parameters<StreamHandlers<C>['onSnapshot']>[0]);
        });
        es.addEventListener('entry', (ev) => {
            const p = parse(ev);
            if (p) handlers.onEntry(p as Parameters<StreamHandlers<C>['onEntry']>[0]);
        });
        es.addEventListener('caughtup', (ev) => {
            const p = parse(ev) as { index: number } | undefined;
            if (p) handlers.onCaughtUp(p.index);
        });
        es.onerror = () => {
            if (closed) return;
            // Close and surface — the replica reconnects from its applied index.
            closed = true;
            es.close();
            handlers.onError(new Error('EventSource error'));
        };

        return () => {
            closed = true;
            es.close();
        };
    }
}
