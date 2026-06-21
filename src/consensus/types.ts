// Core type definitions for the Raft consensus framework.
//
// These types are domain-agnostic: the consensus layer knows nothing about the
// application riding on top of it. An application supplies its own command union
// (any object with a string `type` discriminator) and a `StateMachine` that
// applies those commands — see `consensus/stateMachine.ts` and the book example
// in `models/`.

/** Role of a node in the Raft cluster at any given moment. */
export type Role = 'follower' | 'candidate' | 'leader';

/**
 * The contract every application command must satisfy: a JSON-serializable
 * object discriminated by a string `type`. The framework reserves the type
 * values `NOOP` and `CONFIG` for its own control entries (see {@link Command}),
 * so an application's command types must not collide with those.
 */
export interface AppCommand {
    type: string;
}

/**
 * What actually rides in the replicated log: either one of the framework's two
 * control commands, or an application command `C`. The leader resolves every
 * non-deterministic value (ids, timestamps) BEFORE a command enters the log, so
 * every node applies the exact same command and converges to the exact same
 * state. That determinism is what makes the cluster a replicated state machine.
 *
 * - `NOOP`   — internal Raft bookkeeping (e.g. the no-op a new leader commits to
 *              make prior-term entries safely committable). Never audited.
 * - `CONFIG` — a cluster membership change (Raft dissertation §4). Consumed by
 *              the Raft node, which adopts `members` as its configuration the
 *              moment the entry is appended (not when committed). A no-op for the
 *              application state machine. `members` is the full new voting set.
 */
export type Command<C extends AppCommand = AppCommand> =
    | { type: 'NOOP' }
    | { type: 'CONFIG'; members: PeerInfo[] }
    | C;

/** A single entry in the replicated log. */
export interface LogEntry<C extends AppCommand = AppCommand> {
    term: number;
    command: Command<C>;
    /** Leader-assigned metadata, replicated with the command (audit + idempotency). */
    meta?: CommandMeta;
}

/**
 * Metadata attached by the leader before a command enters the log. Replicated
 * verbatim so every node sees the same actor/requestId/timestamp — the basis
 * for the audit trail and idempotent retries.
 */
export interface CommandMeta {
    requestId: string;
    actor: string;
    timestamp: string; // ISO, leader-assigned
}

/** One tamper-evident record in the replicated audit log. */
export interface AuditEntry {
    index: number;
    term: number;
    /** The command's discriminator (the application's type, or `CONFIG`). */
    type: string;
    actor: string;
    requestId: string;
    timestamp: string;
    status: number;
    prevHash: string;
    hash: string;
}

/**
 * The outcome of applying a command to the state machine. `status` is an
 * HTTP-style code the framework records in the audit trail and an HTTP adapter
 * can relay; `data` is the application-defined payload (e.g. the affected
 * entity), and `message` an optional human-readable note.
 */
export interface ApplyResult<T = unknown> {
    status: number;
    data?: T;
    message?: string;
}

// ---- RPC payloads (Raft figure 2) ----
//
// Replicated entries cross the wire as JSON, so the RPC payloads use the default
// `AppCommand` form rather than threading the application's command type through
// the transport. A node casts incoming entries back to its own command type at
// the (inherently untyped) transport boundary.

export interface RequestVoteArgs {
    term: number;
    candidateId: string;
    lastLogIndex: number;
    lastLogTerm: number;
}

export interface RequestVoteReply {
    term: number;
    voteGranted: boolean;
}

export interface AppendEntriesArgs {
    term: number;
    leaderId: string;
    prevLogIndex: number;
    prevLogTerm: number;
    entries: LogEntry[];
    leaderCommit: number;
}

export interface AppendEntriesReply {
    term: number;
    success: boolean;
    /** Index of the last entry the follower now has in sync (success only). */
    matchIndex?: number;
    /**
     * Accelerated log backtracking hint (failure only): the term of the conflicting
     * entry at `prevLogIndex` (or omitted if the follower's log is simply too short),
     * and the first log index the leader should retry from. Lets the leader skip a
     * whole conflicting term per round trip instead of decrementing by one.
     */
    conflictTerm?: number;
    conflictIndex?: number;
}

/**
 * Sent by a leader to a follower that has fallen behind the leader's log
 * snapshot. Snapshots are streamed in bounded-size chunks (Raft figure 13): the
 * leader serializes the snapshot's `data` to a JSON string once and ships
 * successive slices. `lastIncludedIndex`/`lastIncludedTerm`/`members` ride every
 * chunk (the follower uses them when the final chunk arrives).
 */
export interface InstallSnapshotArgs {
    term: number;
    leaderId: string;
    lastIncludedIndex: number;
    lastIncludedTerm: number;
    /** Cluster configuration as of the snapshot point (membership survives compaction). */
    members: PeerInfo[];
    /** Code-unit (UTF-16) offset of this chunk within the serialized snapshot string. */
    offset: number;
    /** A slice of the JSON-serialized state-machine snapshot at `offset`. */
    data: string;
    /** True for the final chunk: the follower reassembles and installs on `done`. */
    done: boolean;
}

export interface InstallSnapshotReply {
    term: number;
}

/** A point-in-time snapshot that replaces the log up to lastIncludedIndex. */
export interface Snapshot {
    lastIncludedIndex: number;
    lastIncludedTerm: number;
    /** Cluster configuration as of the snapshot point (the compacted log's base config). */
    members?: PeerInfo[];
    data: unknown;
}

/** Identity + address of a peer node. */
export interface PeerInfo {
    id: string;
    url: string;
}

/** True for the framework's reserved control commands (never application commands). */
export function isControlCommand(command: { type: string }): command is { type: 'NOOP' } | { type: 'CONFIG'; members: PeerInfo[] } {
    return command.type === 'NOOP' || command.type === 'CONFIG';
}

/** Narrow a log command to a CONFIG membership entry. */
export function isConfigCommand(command: { type: string }): command is { type: 'CONFIG'; members: PeerInfo[] } {
    return command.type === 'CONFIG';
}
