import http from 'http';
import { Server } from 'http';
import { AddressInfo } from 'net';
import { createApp } from '../src/app';
import { RaftNode } from '../src/consensus/raftNode';
import { LocalTransport } from '../src/consensus/transport';
import { Book, BookCommand, buildAddCommand } from '../src/models/book';
import { BookStateMachine } from '../src/models/bookStateMachine';
import { buildBookStreamGuard } from '../src/models/bookStreamGuard';
import { EdgeReplica } from '../src/edge/edgeReplica';
import { EventSourceStreamSource, EventSourceCtor, MessageEventLike } from '../src/edge/eventSourceStreamSource';
import { waitFor } from './helpers';

jest.setTimeout(30000);
const TIMERS = { electionMinMs: 150, electionMaxMs: 300, heartbeatMs: 50 };
const listen = (s: Server): Promise<void> => new Promise((r) => s.listen(0, '127.0.0.1', () => r()));
const book = (isbn: string, publisher: string) =>
    buildAddCommand({ title: `t-${isbn}`, author: 'a', publisher, isbn, copies: 1 });

/**
 * Minimal Node stand-in for the browser `EventSource`, matching the structural
 * `EventSourceLike` the browser source targets. Lets us exercise the BROWSER
 * code path (`EventSourceStreamSource`, token-in-URL) against a real node in CI,
 * where the global `EventSource` doesn't exist.
 */
class NodeEventSource {
    onopen: ((ev: unknown) => void) | null = null;
    onerror: ((ev: unknown) => void) | null = null;
    private readonly listeners = new Map<string, ((ev: MessageEventLike) => void)[]>();
    private readonly req: http.ClientRequest;
    private buffer = '';

    constructor(url: string) {
        this.req = http.get(url, (res) => {
            if (res.statusCode !== 200) {
                res.resume();
                this.onerror?.(new Error(`HTTP ${res.statusCode}`));
                return;
            }
            this.onopen?.({});
            res.setEncoding('utf8');
            res.on('data', (chunk: string) => {
                this.buffer += chunk;
                let i: number;
                while ((i = this.buffer.indexOf('\n\n')) !== -1) {
                    const frame = this.buffer.slice(0, i);
                    this.buffer = this.buffer.slice(i + 2);
                    this.dispatch(frame);
                }
            });
            res.on('end', () => this.onerror?.(new Error('end')));
        });
        this.req.on('error', (e) => this.onerror?.(e));
    }

    addEventListener(type: string, cb: (ev: MessageEventLike) => void): void {
        const arr = this.listeners.get(type) ?? [];
        arr.push(cb);
        this.listeners.set(type, arr);
    }

    private dispatch(frame: string): void {
        let event = 'message';
        const data: string[] = [];
        for (const line of frame.split('\n')) {
            if (line.startsWith(':')) continue;
            if (line.startsWith('event:')) event = line.slice('event:'.length).trim();
            else if (line.startsWith('data:')) data.push(line.slice('data:'.length).trim());
        }
        if (data.length === 0) return;
        for (const cb of this.listeners.get(event) ?? []) cb({ data: data.join('\n') });
    }

    close(): void {
        this.req.destroy();
    }
}

/**
 * The browser stream source (ADR-0023) over a real node: `EventSourceStreamSource`
 * feeding an `EdgeReplica`, exercised with a Node `EventSource` polyfill. Proves
 * the browser path converges and honors the token (carried in the URL).
 */
describe('EventSourceStreamSource + EdgeReplica (browser path)', () => {
    let node: RaftNode<BookCommand, Book, BookStateMachine>;
    let server: Server;
    let url: string;
    let replica: EdgeReplica<BookCommand, Book> | null = null;

    beforeEach(async () => {
        node = new RaftNode<BookCommand, Book, BookStateMachine>(
            { id: 'solo', peers: [], stateMachine: new BookStateMachine(), ...TIMERS },
            new LocalTransport(new Map()),
        );
        server = http.createServer(createApp(node, { streamGuard: buildBookStreamGuard('reader=*,acme=Acme Press') }));
        await listen(server);
        url = `http://127.0.0.1:${(server.address() as AddressInfo).port}`;
        node.start();
        await waitFor(() => node.isLeader(), 3000);
        await node.submit(book('a1', 'Acme Press'));
        await node.submit(book('p1', 'Penguin'));
    });

    afterEach(async () => {
        replica?.stop();
        replica = null;
        node.stop();
        await new Promise<void>((r) => server.close(() => r()));
    });

    const connect = (token: string): BookStateMachine => {
        const local = new BookStateMachine();
        replica = new EdgeReplica<BookCommand, Book>({
            app: local,
            source: new EventSourceStreamSource(url, NodeEventSource as unknown as EventSourceCtor, { token }),
        });
        replica.start();
        return local;
    };

    it('converges via EventSource and live-tails (all scope)', async () => {
        const local = connect('reader');
        await waitFor(() => replica!.isCaughtUp() && local.size() === 2, 5000);
        expect(local.getAll().map((b) => b.isbn).sort()).toEqual(['a1', 'p1']);

        await node.submit(book('a2', 'Acme Press'));
        await waitFor(() => local.size() === 3, 5000);
    });

    it('honors the token: a scoped client sees only its publisher', async () => {
        const local = connect('acme');
        await waitFor(() => replica!.isCaughtUp() && local.size() === 1, 5000);
        expect(local.getAll().map((b) => b.isbn)).toEqual(['a1']);
    });
});
