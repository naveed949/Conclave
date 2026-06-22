// Edge read replica SDK (ADR-0023): a read-only, non-voting replica of the
// application state machine, kept current by tailing a node's committed-log
// stream and serving reads locally. Environment-agnostic core (`EdgeReplica`)
// plus a Node stream source and a browser (EventSource) stream source.

export { EdgeReplica } from './edgeReplica';
export type { EdgeReplicaOptions } from './edgeReplica';
// Browser-safe async SHA-256 hex (WebCrypto) used for client-side audit
// verification, plus the shared audit-hash payload format (single source of truth
// with the server's ReplicatedStateMachine).
export { webcryptoSha256Hex } from './sha256';
export type { Sha256Hex } from './sha256';
export { GENESIS_HASH, auditEntryPayload } from '../consensus/auditChain';
export { HttpStreamSource } from './httpStreamSource';
export { EventSourceStreamSource } from './eventSourceStreamSource';
export type { EventSourceLike, EventSourceCtor, MessageEventLike } from './eventSourceStreamSource';
export type { LogStreamSource, StreamHandlers, StreamSnapshot, StreamEntry } from './types';
// Per-client authorization + partial replication (ADR-0023 prereq 3).
export { extractStreamToken } from './streamGuard';
export type { StreamGuard, ScopedFilter } from './streamGuard';
// Cryptographically-signed, short-lived, scoped stream tokens (M26).
export { mintStreamToken, verifyStreamToken, createSignedTokenGuard } from './signedToken';
