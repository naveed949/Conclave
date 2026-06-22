import { AppCommand, LogEntry, PeerInfo } from '../consensus/types';

/**
 * Wire types for the committed-log read stream (ADR-0023). These mirror the
 * Server-Sent Events emitted by `GET /raft/stream` (see `routes/raftRoutes.ts`)
 * and are what an {@link EdgeReplica} consumes to bootstrap then live-tail.
 */

/** Snapshot-boundary handoff: the state to restore before tailing by entry. */
export interface StreamSnapshot {
    lastIncludedIndex: number;
    lastIncludedTerm: number;
    members: PeerInfo[];
    /** The replicated state machine snapshot (`{ state, audit, … }`). */
    data: unknown;
}

/** One committed log entry at its absolute index. */
export interface StreamEntry<C extends AppCommand = AppCommand> {
    index: number;
    entry: LogEntry<C>;
}

/** Callbacks a {@link LogStreamSource} drives as stream events arrive. */
export interface StreamHandlers<C extends AppCommand = AppCommand> {
    /** Connection established (before any events). */
    onOpen?(): void;
    /** A snapshot to bootstrap from (consumer was behind the compaction boundary). */
    onSnapshot(snap: StreamSnapshot): void;
    /** A committed entry to apply. */
    onEntry(item: StreamEntry<C>): void;
    /** The consumer has replayed through the live head and is now tailing. */
    onCaughtUp(index: number): void;
    /** The connection failed or closed; the consumer decides whether to reconnect. */
    onError(err: Error): void;
}

/**
 * A single connection to a node's committed-log stream. Implementations are
 * environment-specific (Node `http`, browser `EventSource`, …) but dumb: they
 * open ONE connection from just after `fromIndex` and report events/errors. The
 * {@link EdgeReplica} owns reconnection/backoff and resume bookkeeping, so retry
 * policy lives in exactly one place.
 *
 * `connect` returns a function that closes the connection.
 */
export interface LogStreamSource<C extends AppCommand = AppCommand> {
    connect(fromIndex: number, handlers: StreamHandlers<C>): () => void;
}
