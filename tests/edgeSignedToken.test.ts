import http from 'http';
import { Server } from 'http';
import { AddressInfo } from 'net';
import { createApp } from '../src/app';
import { RaftNode } from '../src/consensus/raftNode';
import { LocalTransport } from '../src/consensus/transport';
import { Book, BookCommand, buildAddCommand } from '../src/models/book';
import { BookStateMachine } from '../src/models/bookStateMachine';
import { buildSignedBookStreamGuard } from '../src/models/bookStreamGuard';
import { mintStreamToken, verifyStreamToken } from '../src/edge/signedToken';
import { EdgeReplica } from '../src/edge/edgeReplica';
import { HttpStreamSource } from '../src/edge/httpStreamSource';
import { waitFor } from './helpers';

jest.setTimeout(30000);
const TIMERS = { electionMinMs: 150, electionMaxMs: 300, heartbeatMs: 50 };
const SECRET = 'test-stream-secret';

const listen = (s: Server): Promise<void> => new Promise((r) => s.listen(0, '127.0.0.1', () => r()));

const book = (isbn: string, publisher: string) =>
    buildAddCommand({ title: `t-${isbn}`, author: 'a', publisher, isbn, copies: 1 });

/** Raw GET that resolves the HTTP status (for the 401 assertions). */
function statusOf(url: string): Promise<number> {
    return new Promise((resolve, reject) => {
        const req = http.get(url, (res) => {
            res.resume();
            resolve(res.statusCode ?? 0);
            req.destroy();
        });
        req.on('error', reject);
    });
}

/**
 * Cryptographically-verified, short-lived, scoped stream tokens (M26). The
 * signed-token guard authenticates an HS256 JWT-shaped token and derives the
 * scope from its claims — no static registry, no guessable token.
 */
describe('signed stream tokens (M26)', () => {
    describe('verifyStreamToken (unit)', () => {
        it('returns the claims for a freshly minted token', () => {
            const token = mintStreamToken(SECRET, { scope: 'Acme Press' }, 60);
            const claims = verifyStreamToken(SECRET, token);
            expect(claims).not.toBeNull();
            expect(claims!.scope).toBe('Acme Press');
            expect(typeof claims!.iat).toBe('number');
            expect(typeof claims!.exp).toBe('number');
        });

        it('rejects a tampered payload', () => {
            const token = mintStreamToken(SECRET, { scope: 'Acme Press' }, 60);
            const [header, , signature] = token.split('.');
            const forgedPayload = Buffer.from(JSON.stringify({ scope: '*', exp: 9999999999 }))
                .toString('base64')
                .replace(/\+/g, '-')
                .replace(/\//g, '_')
                .replace(/=+$/, '');
            const tampered = `${header}.${forgedPayload}.${signature}`;
            expect(verifyStreamToken(SECRET, tampered)).toBeNull();
        });

        it('rejects a tampered signature', () => {
            const token = mintStreamToken(SECRET, { scope: '*' }, 60);
            const [header, payload] = token.split('.');
            const tampered = `${header}.${payload}.AAAA`;
            expect(verifyStreamToken(SECRET, tampered)).toBeNull();
        });

        it('rejects a token signed with a different secret', () => {
            const token = mintStreamToken('other-secret', { scope: '*' }, 60);
            expect(verifyStreamToken(SECRET, token)).toBeNull();
        });

        it('rejects an expired token', () => {
            const token = mintStreamToken(SECRET, { scope: '*' }, -1);
            expect(verifyStreamToken(SECRET, token)).toBeNull();
        });

        it('never throws on malformed input', () => {
            expect(verifyStreamToken(SECRET, 'not-a-token')).toBeNull();
            expect(verifyStreamToken(SECRET, 'a.b')).toBeNull();
            expect(verifyStreamToken(SECRET, '...')).toBeNull();
        });

        // base64url-encode a JSON object the way the minter does (test helper).
        const b64 = (obj: unknown) =>
            Buffer.from(JSON.stringify(obj))
                .toString('base64')
                .replace(/\+/g, '-')
                .replace(/\//g, '_')
                .replace(/=+$/, '');

        it('rejects algorithm confusion (alg: none / a non-HS256 header)', () => {
            const payload = b64({ scope: '*', exp: 9999999999 });
            // alg:"none" with an empty signature — the classic JWT bypass.
            expect(verifyStreamToken(SECRET, `${b64({ alg: 'none', typ: 'JWT' })}.${payload}.`)).toBeNull();
            // A forged alg the verifier must not honor (it always computes HS256).
            expect(verifyStreamToken(SECRET, `${b64({ alg: 'RS256', typ: 'JWT' })}.${payload}.AAAA`)).toBeNull();
        });

        it('rejects a token with no exp (fail closed — no perpetual tokens)', () => {
            // A correctly HS256-SIGNED token that simply omits exp must still be
            // rejected: a missing/non-numeric exp would be a perpetual credential.
            const header = b64({ alg: 'HS256', typ: 'JWT' });
            const payload = b64({ scope: '*' }); // no exp
            const signingInput = `${header}.${payload}`;
            const sig = require('crypto')
                .createHmac('sha256', SECRET)
                .update(signingInput)
                .digest('base64')
                .replace(/\+/g, '-')
                .replace(/\//g, '_')
                .replace(/=+$/, '');
            expect(verifyStreamToken(SECRET, `${signingInput}.${sig}`)).toBeNull();
        });
    });

    describe('integration over GET /raft/stream', () => {
        let node: RaftNode<BookCommand, Book, BookStateMachine>;
        let server: Server;
        let url: string;
        let replica: EdgeReplica<BookCommand, Book> | null = null;

        beforeEach(async () => {
            node = new RaftNode<BookCommand, Book, BookStateMachine>(
                { id: 'solo', peers: [], stateMachine: new BookStateMachine(), ...TIMERS },
                new LocalTransport(new Map()),
            );
            const streamGuard = buildSignedBookStreamGuard(SECRET);
            server = http.createServer(createApp(node, { streamGuard }));
            await listen(server);
            url = `http://127.0.0.1:${(server.address() as AddressInfo).port}`;
            node.start();
            await waitFor(() => node.isLeader(), 3000);

            await node.submit(book('a1', 'Acme Press'));
            await node.submit(book('a2', 'Acme Press'));
            await node.submit(book('p1', 'Penguin'));
        });

        afterEach(async () => {
            replica?.stop();
            replica = null;
            node.stop();
            await new Promise<void>((r) => server.close(() => r()));
        });

        it('rejects missing, expired, and tampered tokens (401)', async () => {
            expect(await statusOf(`${url}/raft/stream?fromIndex=0`)).toBe(401);
            const expired = mintStreamToken(SECRET, { scope: '*' }, -1);
            expect(await statusOf(`${url}/raft/stream?fromIndex=0&token=${expired}`)).toBe(401);
            const valid = mintStreamToken(SECRET, { scope: '*' }, 60);
            const tampered = `${valid.split('.').slice(0, 2).join('.')}.AAAA`;
            expect(await statusOf(`${url}/raft/stream?fromIndex=0&token=${tampered}`)).toBe(401);
            // A valid signed token is accepted (200 then we drop the connection).
            expect(await statusOf(`${url}/raft/stream?fromIndex=0&token=${valid}`)).toBe(200);
        });

        it('serves a publisher-scoped token ONLY its publisher (snapshot + live tail)', async () => {
            const token = mintStreamToken(SECRET, { scope: 'Acme Press' }, 60);
            const local = new BookStateMachine();
            replica = new EdgeReplica<BookCommand, Book>({
                app: local,
                source: new HttpStreamSource(url, { token }),
            });
            replica.start();

            await waitFor(() => replica!.isCaughtUp() && local.size() === 2, 5000);
            expect(local.getAll().map((b) => b.isbn).sort()).toEqual(['a1', 'a2']);
            expect(local.getAll().every((b) => b.publisher === 'Acme Press')).toBe(true);

            await node.submit(book('a3', 'Acme Press'));
            await node.submit(book('p2', 'Penguin'));
            await waitFor(() => local.size() === 3, 5000);
            await new Promise((r) => setTimeout(r, 150));
            expect(local.size()).toBe(3);
            expect(local.getAll().map((b) => b.isbn).sort()).toEqual(['a1', 'a2', 'a3']);
        });

        it('serves an all-scope token every book', async () => {
            const token = mintStreamToken(SECRET, { scope: '*' }, 60);
            const local = new BookStateMachine();
            replica = new EdgeReplica<BookCommand, Book>({
                app: local,
                source: new HttpStreamSource(url, { token }),
            });
            replica.start();
            await waitFor(() => replica!.isCaughtUp() && local.size() === 3, 5000);
            expect(local.getAll().map((b) => b.isbn).sort()).toEqual(['a1', 'a2', 'p1']);
        });
    });
});
