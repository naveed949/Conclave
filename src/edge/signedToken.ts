import { createHmac, timingSafeEqual } from 'crypto';
import { AppCommand } from '../consensus/types';
import { ScopedFilter, StreamGuard } from './streamGuard';

/**
 * Cryptographically-verified, short-lived, scoped stream tokens (M26).
 *
 * The demo `StreamGuard`s map *guessable* opaque tokens via a static registry —
 * fine for the worked example, not for production. This module mints and verifies
 * a compact, JWT-shaped HS256 token instead: a self-describing credential whose
 * claims (scope, expiry) are tamper-evident under a shared secret. The stream
 * endpoint can then authorize a connection with NO server-side session lookup —
 * verify the signature, read the scope from the claims, expire on `exp`.
 *
 * Dependency-free by design (ADR-0013): Node `crypto` only — `createHmac` for the
 * signature, `timingSafeEqual` for a constant-time compare. The token format is a
 * deliberate subset of JWT (HS256, base64url, three dot-separated segments) so it
 * is recognisable and inspectable, without pulling in a JWT library.
 */

/** JWT header for the only algorithm we support. */
const HEADER = { alg: 'HS256', typ: 'JWT' } as const;

/** base64url-encode a buffer/string (no `=` padding, URL-safe alphabet). */
function base64urlEncode(input: Buffer | string): string {
    const buf = typeof input === 'string' ? Buffer.from(input, 'utf8') : input;
    return buf.toString('base64').replace(/\+/g, '-').replace(/\//g, '_').replace(/=+$/, '');
}

/** base64url-decode to a Buffer. Restores padding and the standard alphabet. */
function base64urlDecode(input: string): Buffer {
    const normalized = input.replace(/-/g, '+').replace(/_/g, '/');
    const pad = normalized.length % 4 === 0 ? '' : '='.repeat(4 - (normalized.length % 4));
    return Buffer.from(normalized + pad, 'base64');
}

/** Compute the base64url HMAC-SHA256 signature over `header.payload`. */
function sign(secret: string, signingInput: string): string {
    return base64urlEncode(createHmac('sha256', secret).update(signingInput).digest());
}

/**
 * Mint a signed stream token:
 *   base64url(header).base64url(payload).base64url(HMAC_SHA256(secret, header+"."+payload))
 *
 * `claims` is merged with `iat` (issued-at) and `exp` (now + `ttlSeconds`), both
 * unix seconds. A `ttlSeconds` of 0 or negative yields an already-expired token
 * (useful in tests). The caller owns the claim shape — the book guard reads a
 * `scope` claim (see `models/bookStreamGuard.ts`).
 */
export function mintStreamToken(
    secret: string,
    claims: Record<string, unknown>,
    ttlSeconds: number,
): string {
    const now = Math.floor(Date.now() / 1000);
    const payload = { ...claims, iat: now, exp: now + ttlSeconds };
    const encodedHeader = base64urlEncode(JSON.stringify(HEADER));
    const encodedPayload = base64urlEncode(JSON.stringify(payload));
    const signingInput = `${encodedHeader}.${encodedPayload}`;
    return `${signingInput}.${sign(secret, signingInput)}`;
}

/**
 * Verify a signed stream token and return its claims, or `null` if the token is
 * malformed, uses an unsupported algorithm, has a bad signature, is expired, or
 * lacks a valid numeric `exp` (a missing expiry fails closed — see below).
 *
 * NEVER throws on bad input — every failure path returns `null`, so an attacker
 * cannot distinguish causes via exceptions. The signature compare is
 * constant-time (`timingSafeEqual`, length-guarded) to avoid leaking the secret
 * byte-by-byte through timing.
 */
export function verifyStreamToken(secret: string, token: string): Record<string, unknown> | null {
    if (typeof token !== 'string') return null;
    const parts = token.split('.');
    if (parts.length !== 3) return null;
    const [encodedHeader, encodedPayload, encodedSignature] = parts;
    if (!encodedHeader || !encodedPayload || !encodedSignature) return null;

    try {
        const header = JSON.parse(base64urlDecode(encodedHeader).toString('utf8'));
        if (!header || header.alg !== 'HS256') return null;

        // Constant-time signature check. A length mismatch means the signatures
        // differ — bail before `timingSafeEqual` (which throws on unequal lengths).
        const expected = base64urlDecode(sign(secret, `${encodedHeader}.${encodedPayload}`));
        const actual = base64urlDecode(encodedSignature);
        if (expected.length !== actual.length) return null;
        if (!timingSafeEqual(expected, actual)) return null;

        const claims = JSON.parse(base64urlDecode(encodedPayload).toString('utf8'));
        if (!claims || typeof claims !== 'object') return null;

        // Expiry: `exp` is REQUIRED and must be a finite number of unix seconds.
        // Fail closed — a token without a usable `exp` would be perpetual, which
        // defeats the whole point of a short-lived credential. Reject once it is in
        // the past (now >= exp). `mintStreamToken` always sets a numeric `exp`.
        const exp = (claims as { exp?: unknown }).exp;
        if (typeof exp !== 'number' || !Number.isFinite(exp) || Math.floor(Date.now() / 1000) >= exp) {
            return null;
        }

        return claims as Record<string, unknown>;
    } catch {
        // Malformed base64url or JSON — treat as an invalid token, never throw.
        return null;
    }
}

/**
 * Build a {@link StreamGuard} backed by signed tokens. `authorize` verifies the
 * presented token under `secret`, then maps the verified claims to a
 * {@link ScopedFilter} via `toFilter` (return `null` to reject — the endpoint
 * 401s). This is the generic, domain-agnostic seam: the application supplies
 * `toFilter` to decide what a claim set may see (see `buildSignedBookStreamGuard`).
 */
export function createSignedTokenGuard<C extends AppCommand = AppCommand>(opts: {
    secret: string;
    toFilter: (claims: Record<string, unknown>) => ScopedFilter<C> | null;
}): StreamGuard<C> {
    const { secret, toFilter } = opts;
    return {
        authorize(token: string | undefined): ScopedFilter<C> | null {
            if (!token) return null;
            const claims = verifyStreamToken(secret, token);
            if (!claims) return null;
            return toFilter(claims);
        },
    };
}
