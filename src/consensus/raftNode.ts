import { BookStateMachine } from './stateMachine';
import { RpcHandler, Transport } from './transport';
import {
    AppendEntriesArgs,
    AppendEntriesReply,
    ApplyResult,
    Command,
    LogEntry,
    PeerInfo,
    RequestVoteArgs,
    RequestVoteReply,
    Role,
} from './types';

export interface RaftConfig {
    id: string;
    peers: PeerInfo[];
    electionMinMs?: number;
    electionMaxMs?: number;
    heartbeatMs?: number;
    /** When false, suppress per-event logging (used by tests). */
    debug?: boolean;
}

/** Raised when a write is attempted on a non-leader node. */
export class NotLeaderError extends Error {
    constructor(public readonly leaderId: string | null) {
        super('NOT_LEADER');
        this.name = 'NotLeaderError';
    }
}

interface PendingProposal {
    term: number;
    resolve: (result: ApplyResult) => void;
    reject: (err: Error) => void;
}

/**
 * A single Raft node. Implements leader election and log replication
 * (Raft paper, figure 2) on top of a pluggable transport, and applies
 * committed entries to a {@link BookStateMachine}.
 */
export class RaftNode implements RpcHandler {
    readonly id: string;
    private readonly peers: PeerInfo[];
    private readonly transport: Transport;
    readonly stateMachine = new BookStateMachine();

    // Persistent state (would be on disk in a real system).
    private currentTerm = 0;
    private votedFor: string | null = null;
    /** Log is 1-indexed; index 0 is a sentinel so real entries start at 1. */
    private log: LogEntry[] = [{ term: 0, command: { type: 'NOOP' } }];

    // Volatile state.
    private commitIndex = 0;
    private lastApplied = 0;
    private role: Role = 'follower';
    private leaderId: string | null = null;

    // Leader-only volatile state.
    private nextIndex = new Map<string, number>();
    private matchIndex = new Map<string, number>();

    // Timers.
    private electionTimer: NodeJS.Timeout | null = null;
    private heartbeatTimer: NodeJS.Timeout | null = null;

    // Proposals awaiting commit, keyed by log index.
    private pending = new Map<number, PendingProposal>();

    private readonly electionMinMs: number;
    private readonly electionMaxMs: number;
    private readonly heartbeatMs: number;
    private readonly debug: boolean;
    private running = false;

    constructor(config: RaftConfig, transport: Transport) {
        this.id = config.id;
        this.peers = config.peers;
        this.transport = transport;
        this.electionMinMs = config.electionMinMs ?? 150;
        this.electionMaxMs = config.electionMaxMs ?? 300;
        this.heartbeatMs = config.heartbeatMs ?? 50;
        this.debug = config.debug ?? false;
    }

    // ---- lifecycle ----

    start(): void {
        if (this.running) return;
        this.running = true;
        this.becomeFollower(this.currentTerm);
    }

    stop(): void {
        this.running = false;
        this.clearElectionTimer();
        this.clearHeartbeatTimer();
        for (const p of this.pending.values()) p.reject(new Error('node stopped'));
        this.pending.clear();
    }

    // ---- public accessors (status / controller) ----

    isLeader(): boolean {
        return this.role === 'leader';
    }

    status() {
        return {
            id: this.id,
            role: this.role,
            term: this.currentTerm,
            leaderId: this.leaderId,
            logLength: this.log.length - 1,
            commitIndex: this.commitIndex,
            lastApplied: this.lastApplied,
            books: this.stateMachine.size(),
        };
    }

    getLeaderId(): string | null {
        return this.leaderId;
    }

    /**
     * Propose a command. Resolves once the entry is committed and applied.
     * Throws {@link NotLeaderError} if this node is not the leader.
     */
    submit(command: Command): Promise<ApplyResult> {
        if (this.role !== 'leader') throw new NotLeaderError(this.leaderId);

        const entry: LogEntry = { term: this.currentTerm, command };
        this.log.push(entry);
        const index = this.log.length - 1;
        this.matchIndex.set(this.id, index);

        const promise = new Promise<ApplyResult>((resolve, reject) => {
            this.pending.set(index, { term: entry.term, resolve, reject });
        });

        // A single-node cluster is its own majority, so try to commit right away.
        this.advanceCommitIndex();
        // Push to followers immediately rather than waiting for the next heartbeat.
        this.broadcastAppendEntries();
        return promise;
    }

    // ---- role transitions ----

    private becomeFollower(term: number): void {
        this.role = 'follower';
        if (term > this.currentTerm) {
            this.currentTerm = term;
            this.votedFor = null;
        }
        this.clearHeartbeatTimer();
        this.resetElectionTimer();
    }

    private becomeCandidate(): void {
        this.currentTerm += 1;
        this.role = 'candidate';
        this.votedFor = this.id;
        this.leaderId = null;
        this.log_(`became candidate for term ${this.currentTerm}`);
        this.resetElectionTimer();
        void this.runElection(this.currentTerm);
    }

    private becomeLeader(): void {
        this.role = 'leader';
        this.leaderId = this.id;
        this.clearElectionTimer();
        const nextIdx = this.log.length;
        for (const peer of this.peers) {
            this.nextIndex.set(peer.id, nextIdx);
            this.matchIndex.set(peer.id, 0);
        }
        this.matchIndex.set(this.id, this.log.length - 1);
        this.log_(`became LEADER for term ${this.currentTerm}`);
        // Commit a no-op for this term so prior entries can be safely committed.
        this.log.push({ term: this.currentTerm, command: { type: 'NOOP' } });
        this.matchIndex.set(this.id, this.log.length - 1);
        this.advanceCommitIndex();
        this.startHeartbeat();
    }

    // ---- elections ----

    private async runElection(term: number): Promise<void> {
        const lastLogIndex = this.log.length - 1;
        const args: RequestVoteArgs = {
            term,
            candidateId: this.id,
            lastLogIndex,
            lastLogTerm: this.log[lastLogIndex].term,
        };

        let votes = 1; // vote for self
        const majority = Math.floor((this.peers.length + 1) / 2) + 1;
        if (votes >= majority) {
            this.becomeLeader();
            return;
        }

        await Promise.all(
            this.peers.map(async (peer) => {
                const reply = await this.transport.sendRequestVote(peer, args);
                if (!reply || this.role !== 'candidate' || this.currentTerm !== term) return;
                if (reply.term > this.currentTerm) {
                    this.becomeFollower(reply.term);
                    return;
                }
                if (reply.voteGranted) {
                    votes += 1;
                    if (votes >= majority && this.role === 'candidate') {
                        this.becomeLeader();
                    }
                }
            }),
        );
    }

    handleRequestVote(args: RequestVoteArgs): RequestVoteReply {
        if (args.term > this.currentTerm) this.becomeFollower(args.term);

        const lastLogIndex = this.log.length - 1;
        const lastLogTerm = this.log[lastLogIndex].term;
        const logOk =
            args.lastLogTerm > lastLogTerm ||
            (args.lastLogTerm === lastLogTerm && args.lastLogIndex >= lastLogIndex);

        const grant =
            args.term >= this.currentTerm &&
            (this.votedFor === null || this.votedFor === args.candidateId) &&
            logOk;

        if (grant) {
            this.votedFor = args.candidateId;
            this.resetElectionTimer();
        }
        return { term: this.currentTerm, voteGranted: grant };
    }

    // ---- log replication ----

    private startHeartbeat(): void {
        this.clearHeartbeatTimer();
        this.broadcastAppendEntries();
        this.heartbeatTimer = setInterval(() => this.broadcastAppendEntries(), this.heartbeatMs);
    }

    private broadcastAppendEntries(): void {
        if (this.role !== 'leader') return;
        for (const peer of this.peers) void this.replicateTo(peer);
    }

    private async replicateTo(peer: PeerInfo): Promise<void> {
        if (this.role !== 'leader') return;
        const term = this.currentTerm;
        const nextIdx = this.nextIndex.get(peer.id) ?? this.log.length;
        const prevLogIndex = nextIdx - 1;
        const args: AppendEntriesArgs = {
            term,
            leaderId: this.id,
            prevLogIndex,
            prevLogTerm: this.log[prevLogIndex]?.term ?? 0,
            entries: this.log.slice(nextIdx),
            leaderCommit: this.commitIndex,
        };

        const reply = await this.transport.sendAppendEntries(peer, args);
        if (!reply || this.role !== 'leader' || this.currentTerm !== term) return;

        if (reply.term > this.currentTerm) {
            this.becomeFollower(reply.term);
            return;
        }

        if (reply.success) {
            const match = reply.matchIndex ?? prevLogIndex + args.entries.length;
            this.matchIndex.set(peer.id, match);
            this.nextIndex.set(peer.id, match + 1);
            this.advanceCommitIndex();
        } else {
            // Log inconsistency: back off and retry on the next tick.
            this.nextIndex.set(peer.id, Math.max(1, nextIdx - 1));
        }
    }

    handleAppendEntries(args: AppendEntriesArgs): AppendEntriesReply {
        if (args.term < this.currentTerm) {
            return { term: this.currentTerm, success: false };
        }

        // Valid leader for this term — recognise it and reset our election clock.
        if (args.term > this.currentTerm) this.becomeFollower(args.term);
        this.role = 'follower';
        this.leaderId = args.leaderId;
        this.resetElectionTimer();

        // Consistency check on the entry preceding the new ones.
        const prev = this.log[args.prevLogIndex];
        if (!prev || prev.term !== args.prevLogTerm) {
            return { term: this.currentTerm, success: false };
        }

        // Append new entries, overwriting any conflicting suffix.
        let insertAt = args.prevLogIndex + 1;
        for (const entry of args.entries) {
            const existing = this.log[insertAt];
            if (existing && existing.term !== entry.term) {
                this.log.length = insertAt; // truncate conflicting tail
            }
            if (insertAt >= this.log.length) this.log.push(entry);
            insertAt += 1;
        }

        if (args.leaderCommit > this.commitIndex) {
            this.commitIndex = Math.min(args.leaderCommit, this.log.length - 1);
            this.applyCommitted();
        }

        return { term: this.currentTerm, success: true, matchIndex: args.prevLogIndex + args.entries.length };
    }

    /** Leader: advance commitIndex to the highest index replicated on a majority. */
    private advanceCommitIndex(): void {
        for (let n = this.log.length - 1; n > this.commitIndex; n--) {
            // Raft safety: only commit entries from the current term by counting.
            if (this.log[n].term !== this.currentTerm) continue;
            let count = 0;
            for (const idx of this.matchIndex.values()) {
                if (idx >= n) count += 1;
            }
            if (count >= Math.floor((this.peers.length + 1) / 2) + 1) {
                this.commitIndex = n;
                this.applyCommitted();
                break;
            }
        }
    }

    /** Apply every newly-committed entry and resolve any waiting proposals. */
    private applyCommitted(): void {
        while (this.lastApplied < this.commitIndex) {
            this.lastApplied += 1;
            const entry = this.log[this.lastApplied];
            const result = this.stateMachine.apply(entry.command);

            const waiter = this.pending.get(this.lastApplied);
            if (waiter) {
                this.pending.delete(this.lastApplied);
                if (waiter.term === entry.term) waiter.resolve(result);
                else waiter.reject(new NotLeaderError(this.leaderId));
            }
        }
    }

    // ---- timers ----

    private randomElectionTimeout(): number {
        return this.electionMinMs + Math.floor(Math.random() * (this.electionMaxMs - this.electionMinMs));
    }

    private resetElectionTimer(): void {
        if (!this.running) return;
        this.clearElectionTimer();
        this.electionTimer = setTimeout(() => {
            if (this.role !== 'leader') this.becomeCandidate();
        }, this.randomElectionTimeout());
    }

    private clearElectionTimer(): void {
        if (this.electionTimer) {
            clearTimeout(this.electionTimer);
            this.electionTimer = null;
        }
    }

    private clearHeartbeatTimer(): void {
        if (this.heartbeatTimer) {
            clearInterval(this.heartbeatTimer);
            this.heartbeatTimer = null;
        }
    }

    private log_(msg: string): void {
        if (this.debug) console.log(`[raft ${this.id}] ${msg}`);
    }
}
