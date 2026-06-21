// Core type definitions for the Raft consensus implementation.

/** Role of a node in the Raft cluster at any given moment. */
export type Role = 'follower' | 'candidate' | 'leader';

/** A book as stored in the replicated state machine. */
export interface Book {
    id: string;
    title: string;
    author: string;
    publisher: string;
    isbn: string;
    copies: number;
    totalCopies: number;
    borrowedBy: string | null;
    borrowedDate: string | null; // ISO timestamp
    dueDate: string | null; // ISO timestamp
}

/**
 * Commands are the unit of replication. The leader resolves all
 * non-deterministic values (ids, timestamps) BEFORE a command enters the
 * log, so every node applies the exact same command and converges to the
 * exact same state. This is what makes the cluster a deterministic
 * replicated state machine.
 */
export type Command =
    | { type: 'NOOP' }
    | { type: 'ADD'; book: Book }
    | { type: 'UPDATE'; id: string; fields: Partial<Omit<Book, 'id'>> }
    | { type: 'DELETE'; id: string }
    | { type: 'BORROW'; id: string; borrowedBy: string; borrowedDate: string; dueDate: string }
    | { type: 'RETURN'; id: string }
    /**
     * Cluster membership change (Raft dissertation §4). Consumed by the Raft
     * node (which adopts `members` as its configuration the moment the entry is
     * appended), and a no-op for the book state machine. `members` is the full
     * new voting set, including the leader and any node being added/removed.
     */
    | { type: 'CONFIG'; members: PeerInfo[] }
    /**
     * A generic module command routed to the runtime `ModuleHost` (ADR-0018,
     * pillars 1–2) instead of the book state machine. `seed` is leader-resolved
     * up front — the deterministic-runtime analog of resolving ids/timestamps
     * before a command enters the log (see `models/book.ts`) — so every replica
     * derives the identical reducer context and converges. The inline `seed`
     * shape is structurally compatible with `runtime/types.ts` `Seed`; declaring
     * it here (rather than importing from `src/runtime/`) keeps the consensus
     * core free of any dependency on the runtime layer.
     */
    | {
          type: 'MODULE';
          module: string;
          command: string;
          input: unknown;
          seed: { timestamp: string; nonce: string };
          /**
           * Optional ed25519 signature (base64) by the ORIGINATING ACTOR's key
           * over the LOGICAL command only — `{ module, command, input, actor,
           * requestId }`, NOT the leader-resolved `seed` (the actor signs before
           * the leader picks the seed). Verified on the apply path against an
           * actor->public-key registry when one is configured, so a malicious
           * leader cannot forge `actor` (ADR-0018 pillar 7). Purely ADDITIVE:
           * when no registry is configured, the signature is ignored and unsigned
           * commands remain valid — existing behavior is unchanged.
           */
          sig?: string;
      };

/** A single entry in the replicated log. */
export interface LogEntry {
    term: number;
    command: Command;
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
    type: Command['type'];
    actor: string;
    requestId: string;
    timestamp: string;
    status: number;
    prevHash: string;
    hash: string;
}

/** The outcome of applying a command to the state machine. */
export interface ApplyResult {
    status: number; // HTTP-style status for the controller to relay
    book?: Book;
    message?: string;
    /** A module command's return value, surfaced to the controller alongside book/message. */
    result?: unknown;
}

// ---- RPC payloads (Raft figure 2) ----

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

/** Sent by a leader to a follower that has fallen behind the leader's log snapshot. */
export interface InstallSnapshotArgs {
    term: number;
    leaderId: string;
    lastIncludedIndex: number;
    lastIncludedTerm: number;
    /** Cluster configuration as of the snapshot point (membership survives compaction). */
    members: PeerInfo[];
    /** Serialized state-machine snapshot (opaque to the transport). */
    data: unknown;
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
