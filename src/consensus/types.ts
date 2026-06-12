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
    | { type: 'RETURN'; id: string };

/** A single entry in the replicated log. */
export interface LogEntry {
    term: number;
    command: Command;
}

/** The outcome of applying a command to the state machine. */
export interface ApplyResult {
    status: number; // HTTP-style status for the controller to relay
    book?: Book;
    message?: string;
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
}

/** Identity + address of a peer node. */
export interface PeerInfo {
    id: string;
    url: string;
}
