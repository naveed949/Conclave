import { AppCommand, LogEntry } from '../consensus/types';

/**
 * Per-client authorization + partial replication for the committed-log read
 * stream (ADR-0023, prerequisite 3 — the "make-or-break").
 *
 * Exposing the whole committed log to an untrusted edge client is the dealbreaker
 * the ADR calls out: a browser must not be able to read another tenant's data,
 * and cannot hold the whole dataset anyway. A {@link StreamGuard} closes that gap.
 * It authenticates a connection's credential and returns a {@link ScopedFilter}
 * that restricts BOTH the bootstrap snapshot and the live entry feed to exactly
 * what that client may see — or `null` to reject the connection (401).
 *
 * The framework stays domain-agnostic: it knows how to authenticate-and-filter,
 * but WHAT a scope means (which rows, which tenant, which shard) is supplied by
 * the application (see `models/bookStreamGuard.ts` for the book example).
 */

/**
 * A per-connection view restriction. Stateful: `includes` may track which
 * entities have entered the client's scope as entries stream by (e.g. remember
 * the ids of in-scope books seen via ADD, so later UPDATE/DELETE by id resolve).
 * A FRESH instance is created per connection by {@link StreamGuard.authorize}.
 */
export interface ScopedFilter<C extends AppCommand = AppCommand> {
    /**
     * Restrict the bootstrap application state to this scope. `appState` is the
     * application's own snapshot shape (e.g. `Book[]`); return the scoped subset.
     * Also the place to seed any in-scope membership the filter tracks.
     */
    filterSnapshotState(appState: unknown): unknown;

    /**
     * Whether a committed entry is in this client's scope (and may be streamed).
     * Out-of-scope entries are simply not sent; the client's cursor still advances
     * past them (so it stays "current") but its local state never sees them.
     */
    includes(entry: LogEntry<C>): boolean;
}

/**
 * Authenticates a stream connection and produces its {@link ScopedFilter}.
 * Implementations resolve the token (a bearer token, a signed JWT, a session id,
 * …) to a scope. Return `null` for an invalid/expired/absent credential — the
 * endpoint replies 401 and serves nothing.
 */
export interface StreamGuard<C extends AppCommand = AppCommand> {
    authorize(token: string | undefined): ScopedFilter<C> | null;
}

/**
 * Extract a bearer token from a request: `Authorization: Bearer <token>` (used by
 * the Node client, which can set headers) or a `token` query parameter (used by
 * the browser, whose `EventSource` cannot set headers). Returns undefined if neither.
 *
 * SECURITY NOTE: a token in the URL can leak via logs/referrers — in production
 * prefer a short-lived token, a cookie, or `wss`. The query form exists because
 * the native browser `EventSource` API has no header option.
 */
export function extractStreamToken(req: {
    headers: Record<string, unknown>;
    query: Record<string, unknown>;
}): string | undefined {
    const auth = req.headers['authorization'];
    if (typeof auth === 'string' && auth.toLowerCase().startsWith('bearer ')) {
        return auth.slice('bearer '.length).trim();
    }
    const q = req.query['token'];
    if (typeof q === 'string' && q.length > 0) return q;
    return undefined;
}
