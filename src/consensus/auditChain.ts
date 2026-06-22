// Shared audit-hash-chain payload format (ADR-0023, M29).
//
// The single source of truth for HOW an audit entry is serialized before it is
// hashed. Both the server's `ReplicatedStateMachine` (Node `crypto`, sync) and
// the browser-safe edge replica (WebCrypto, async) import this one function, so
// the two hash chains CANNOT drift: change the format here and both sides change
// together.
//
// This module is intentionally PURE — no `crypto`, no Node builtins, no DOM — so
// it is safe to pull into the browser bundle.

/** Hash that precedes the first audit entry (the empty-chain seed). */
export const GENESIS_HASH = '0'.repeat(64);

/**
 * An audit record's own fields (everything in the preimage EXCEPT the chaining
 * `prevHash`). The edge replica collects these per live entry and supplies
 * `prevHash` only when it folds the chain — so the field set is shared and can't
 * drift from {@link AuditPayloadInput}.
 */
export interface AuditRecord {
    index: number;
    term: number;
    type: string;
    actor: string;
    requestId: string;
    timestamp: string;
    status: number;
}

/** The fields that make up an audit-hash preimage: an {@link AuditRecord} + `prevHash`. */
export interface AuditPayloadInput extends AuditRecord {
    prevHash: string;
}

/**
 * Build the exact byte string that gets SHA-256'd into an audit entry's `hash`.
 * Keeping it here (and importing it everywhere) guarantees the server and the
 * browser edge replica produce identical chains for identical histories.
 */
export function auditEntryPayload(r: AuditPayloadInput): string {
    return `${r.prevHash}|${r.index}|${r.term}|${r.type}|${r.actor}|${r.requestId}|${r.timestamp}|${r.status}`;
}
