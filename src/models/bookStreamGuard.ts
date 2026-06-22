import { LogEntry, isControlCommand } from '../consensus/types';
import { ScopedFilter, StreamGuard } from '../edge/streamGuard';
import { createSignedTokenGuard } from '../edge/signedToken';
import { Book, BookCommand } from './book';

/**
 * Per-client authorization + partial replication for the BOOK example stream
 * (ADR-0023). This is application code — it decides what a scope MEANS — wired to
 * the framework's domain-agnostic {@link StreamGuard} seam.
 *
 * A token resolves to a scope: either every book (`{ all: true }`) or only a
 * given publisher's books (`{ publisher }`) — think a publisher-portal client
 * that may see only its own catalogue. The matching `ScopedFilter` restricts both
 * the bootstrap snapshot and the live entry feed to that scope, so a scoped client
 * never receives another publisher's data.
 *
 * Production note: this maps opaque bearer tokens via a static registry for the
 * demo. Swap `BookStreamGuard` for one that verifies a signed JWT (or a session)
 * and derives the scope from its claims — the seam is unchanged.
 */
export type BookScope = { all: true } | { publisher: string };

/**
 * Map a {@link BookScope} to a fresh {@link ScopedFilter} (one per connection).
 * Shared by every book guard — the static-registry one and the signed-token one —
 * so the scope→view-restriction logic lives in exactly one place.
 */
export function bookScopeToFilter(scope: BookScope): ScopedFilter<BookCommand> {
    return new BookScopedFilter(scope);
}

/**
 * Read a {@link BookScope} from verified token claims. The `scope` claim is `"*"`
 * for every book, or a publisher string for that publisher's catalogue only.
 * Returns `null` (⇒ 401) for a missing or non-string `scope` claim.
 */
export function claimsToBookScope(claims: Record<string, unknown>): BookScope | null {
    const scope = claims['scope'];
    if (typeof scope !== 'string' || scope.length === 0) return null;
    return scope === '*' ? { all: true } : { publisher: scope };
}

/** Scoped, stateful filter over book commands (one per connection). */
class BookScopedFilter implements ScopedFilter<BookCommand> {
    /** Ids of books that have entered this client's scope (for id-based commands). */
    private readonly inScope = new Set<string>();

    constructor(private readonly scope: BookScope) {}

    private matchesBook(book: Book): boolean {
        return 'all' in this.scope || book.publisher === this.scope.publisher;
    }

    filterSnapshotState(appState: unknown): Book[] {
        const books = (appState as Book[]) ?? [];
        const scoped = books.filter((b) => this.matchesBook(b));
        for (const b of scoped) this.inScope.add(b.id);
        return scoped;
    }

    includes(entry: LogEntry<BookCommand>): boolean {
        const cmd = entry.command;
        // Control entries (NOOP/CONFIG) carry no application data — and CONFIG would
        // leak cluster topology — so a scoped client never receives them.
        if (isControlCommand(cmd)) return false;

        switch (cmd.type) {
            case 'ADD': {
                const inScope = this.matchesBook(cmd.book);
                if (inScope) this.inScope.add(cmd.book.id);
                return inScope;
            }
            case 'DELETE': {
                const inScope = 'all' in this.scope || this.inScope.has(cmd.id);
                this.inScope.delete(cmd.id);
                return inScope;
            }
            case 'UPDATE':
            case 'BORROW':
            case 'RETURN':
                return 'all' in this.scope || this.inScope.has(cmd.id);
            default: {
                // Exhaustiveness: a new in-scope command type must be classified above.
                const _never: never = cmd;
                return Boolean(_never);
            }
        }
    }
}

/**
 * A {@link StreamGuard} for books backed by a token→scope registry. With no
 * registry entry, a token is rejected (the connection gets 401).
 */
export class BookStreamGuard implements StreamGuard<BookCommand> {
    constructor(private readonly tokens: Map<string, BookScope>) {}

    authorize(token: string | undefined): ScopedFilter<BookCommand> | null {
        if (!token) return null;
        const scope = this.tokens.get(token);
        if (!scope) return null;
        return bookScopeToFilter(scope);
    }
}

/**
 * Build a demo guard from a `TOKEN=scope` spec (env-friendly). `scope` is `*`
 * for all books or a publisher name. Example:
 *   "reader=*,acme=Acme Press,penguin=Penguin"
 * Falls back to a single public `demo=*` token so the examples work out of the box.
 */
export function buildBookStreamGuard(spec?: string): BookStreamGuard {
    const tokens = new Map<string, BookScope>();
    const source = spec && spec.trim().length > 0 ? spec : 'demo=*';
    for (const pair of source.split(',')) {
        const idx = pair.indexOf('=');
        if (idx === -1) continue;
        const token = pair.slice(0, idx).trim();
        const value = pair.slice(idx + 1).trim();
        if (!token) continue;
        tokens.set(token, value === '*' ? { all: true } : { publisher: value });
    }
    return new BookStreamGuard(tokens);
}

/**
 * Build a book {@link StreamGuard} backed by cryptographically-signed, scoped,
 * short-lived tokens (M26) instead of a guessable static registry. A connection
 * presents a token minted with `mintStreamToken(secret, { scope }, ttl)`; the
 * guard verifies the HS256 signature + expiry under `secret` and derives the
 * scope from the `scope` claim (`"*"` ⇒ all books, else a publisher). Mint tokens
 * with `scripts/mint-token.js` (`yarn mint-token`). The seam is unchanged — wire
 * this via `createApp(node, { streamGuard })` exactly like the demo guard.
 */
export function buildSignedBookStreamGuard(secret: string): StreamGuard<BookCommand> {
    return createSignedTokenGuard<BookCommand>({
        secret,
        toFilter: (claims) => {
            const scope = claimsToBookScope(claims);
            return scope ? bookScopeToFilter(scope) : null;
        },
    });
}
