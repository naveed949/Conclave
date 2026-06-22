// Browser entry point for the edge read replica SDK (ADR-0023).
//
// Re-exports only the browser-safe pieces — the environment-agnostic
// `EdgeReplica`, the `EventSourceStreamSource`, and (crucially) the SHARED
// `BookStateMachine` — so a browser runs the EXACT reducer a server node runs.
// No hand-port, no determinism drift across builds (the hazard ADR-0023 names):
// the one deterministic StateMachine is compiled and shipped to the client.
//
// Deliberately excludes the Node-only `HttpStreamSource` (which imports `http`)
// and the server-side `StreamGuard`, so the emitted graph touches no Node builtin.
//
// Build with `yarn build:browser` → emits ESM under examples/edge-replica/lib/.

export { EdgeReplica } from './edgeReplica';
export type { EdgeReplicaOptions } from './edgeReplica';
// Browser-safe async SHA-256 (WebCrypto) backing client-side audit verification.
export { webcryptoSha256Hex } from './sha256';
export type { Sha256Hex } from './sha256';
export { EventSourceStreamSource } from './eventSourceStreamSource';
export type { LogStreamSource, StreamHandlers, StreamSnapshot, StreamEntry } from './types';

// The shared application state machine — the same code the server applies.
export { BookStateMachine } from '../models/bookStateMachine';
export type { Book, BookCommand } from '../models/book';
