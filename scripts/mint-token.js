#!/usr/bin/env node
/*
 * Mint a signed, scoped, short-lived edge-stream token (M26, ADR-0023).
 *
 * Usage:
 *   STREAM_TOKEN_SECRET=... node scripts/mint-token.js <scope> [ttlSeconds]
 *   yarn mint-token "*" 3600                 # all books, valid 1h
 *   yarn mint-token "Acme Press"             # Acme catalogue, default ttl
 *
 * The secret comes from STREAM_TOKEN_SECRET (the same value the server verifies
 * against — see src/server.ts / buildSignedBookStreamGuard). Prints the token to
 * stdout. Requires a prior `yarn build` (it loads the compiled minter from dist/).
 */
const { mintStreamToken } = require('../dist/edge/signedToken');

const DEFAULT_TTL_SECONDS = 3600;

const secret = process.env.STREAM_TOKEN_SECRET;
if (!secret) {
    console.error('STREAM_TOKEN_SECRET is required (the shared HS256 secret).');
    process.exit(1);
}

const scope = process.argv[2];
if (!scope) {
    console.error('Usage: node scripts/mint-token.js <scope> [ttlSeconds]');
    console.error('  <scope> is "*" for all books or a publisher name.');
    process.exit(1);
}

const ttlArg = process.argv[3];
const ttlSeconds = ttlArg !== undefined ? Number.parseInt(ttlArg, 10) : DEFAULT_TTL_SECONDS;
if (!Number.isFinite(ttlSeconds)) {
    console.error(`Invalid ttlSeconds: ${ttlArg}`);
    process.exit(1);
}

console.log(mintStreamToken(secret, { scope }, ttlSeconds));
