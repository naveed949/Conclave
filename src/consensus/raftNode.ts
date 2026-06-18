import { ReplicatedStateMachine, RsmSnapshot } from './replicatedStateMachine';
import { MemoryStorage, RaftStorage } from './storage';
import { RpcHandler, Transport } from './transport';
import { Logger } from '../platform/logger';
import { MetricsRegistry } from '../platform/metrics';
import {
    AppendEntriesArgs,
    AppendEntriesReply,
    ApplyResult,
    Command,
    CommandMeta,
    InstallSnapshotArgs,
    InstallSnapshotReply,
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
    /** Take a snapshot once the in-memory log holds this many entries. */
    snapshotThreshold?: number;
    /** Cap on the idempotency dedup cache (remembered requestIds). FIFO eviction. */
    dedupLimit?: number;
    /** Optional observability/durability collaborators (tests omit them). */
    logger?: Logger;
    metrics?: MetricsRegistry;
    storage?: RaftStorage;
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
 * A single Raft node: leader election, log replication, and log compaction via
 * snapshotting (Raft paper, figures 2 & 13), on a pluggable transport.
 *
 * The log is stored relative to the latest snapshot: `log[0]` is a sentinel
 * representing the snapshot boundary (absolute index = lastIncludedIndex), and
 * `log[p]` has absolute index `lastIncludedIndex + p`. Entries up to and
 * including a snapshot are discarded, so the in-memory log stays bounded.
 */
export class RaftNode implements RpcHandler {
    readonly id: string;
    private readonly peers: PeerInfo[];
    private readonly transport: Transport;
    private readonly storage: RaftStorage;
    private readonly logger?: Logger;
    private readonly metrics?: MetricsRegistry;
    readonly stateMachine: ReplicatedStateMachine;

    // Persistent state.
    private currentTerm = 0;
    private votedFor: string | null = null;
    /** log[0] is a sentinel at absolute index `lastIncludedIndex`. */
    private log: LogEntry[] = [{ term: 0, command: { type: 'NOOP' } }];
    private lastIncludedIndex = 0;
    private lastIncludedTerm = 0;

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

    // Proposals awaiting commit, keyed by absolute log index.
    private pending = new Map<number, PendingProposal>();

    private readonly electionMinMs: number;
    private readonly electionMaxMs: number;
    private readonly heartbeatMs: number;
    private readonly snapshotThreshold: number;
    private running = false;

    constructor(config: RaftConfig, transport: Transport) {
        this.id = config.id;
        this.peers = config.peers;
        this.transport = transport;
        this.storage = config.storage ?? new MemoryStorage();
        this.logger = config.logger?.child({ component: 'raft', node: config.id });
        this.metrics = config.metrics;
        this.electionMinMs = config.electionMinMs ?? 150;
        this.electionMaxMs = config.electionMaxMs ?? 300;
        this.heartbeatMs = config.heartbeatMs ?? 50;
        this.snapshotThreshold = config.snapshotThreshold ?? 1000;
        this.stateMachine = new ReplicatedStateMachine(config.dedupLimit);
    }

    // ---- log index helpers (translate absolute index <-> array position) ----

    private lastLogIndex(): number {
        return this.lastIncludedIndex + this.log.length - 1;
    }

    private pos(absIndex: number): number {
        return absIndex - this.lastIncludedIndex;
    }

    private termAt(absIndex: number): number | undefined {
        const p = this.pos(absIndex);
        if (p < 0 || p >= this.log.length) return undefined;
        return this.log[p].term;
    }

    private entryAt(absIndex: number): LogEntry | undefined {
        const p = this.pos(absIndex);
        if (p < 0 || p >= this.log.length) return undefined;
        return this.log[p];
    }

    // ---- lifecycle ----

    start(): void {
        if (this.running) return;
        this.running = true;

        // Restore a snapshot first (rebuilds the state machine), then the log.
        const snap = this.storage.loadSnapshot();
        if (snap) {
            this.stateMachine.restore(snap.data as RsmSnapshot);
            this.lastIncludedIndex = snap.lastIncludedIndex;
            this.lastIncludedTerm = snap.lastIncludedTerm;
            this.commitIndex = snap.lastIncludedIndex;
            this.lastApplied = snap.lastIncludedIndex;
            this.log = [{ term: snap.lastIncludedTerm, command: { type: 'NOOP' } }];
            this.logger?.info('restored snapshot', { lastIncludedIndex: this.lastIncludedIndex });
        }

        const persisted = this.storage.load();
        if (persisted) {
            this.currentTerm = persisted.currentTerm;
            this.votedFor = persisted.votedFor;
            this.log = persisted.log;
            this.logger?.info('restored persistent state', {
                term: this.currentTerm,
                logEntries: this.log.length - 1,
                lastIncludedIndex: this.lastIncludedIndex,
            });
        }
        this.becomeFollower(this.currentTerm);
    }

    /** Persist the durable subset of state (term, vote, log). */
    private persist(): void {
        this.storage.save({ currentTerm: this.currentTerm, votedFor: this.votedFor, log: this.log });
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
            lastLogIndex: this.lastLogIndex(),
            logEntries: this.log.length - 1,
            snapshotIndex: this.lastIncludedIndex,
            commitIndex: this.commitIndex,
            lastApplied: this.lastApplied,
            books: this.stateMachine.size(),
            dedupCacheSize: this.stateMachine.dedupCacheSize(),
        };
    }

    getLeaderId(): string | null {
        return this.leaderId;
    }

    /** URL of the current leader (for write forwarding), or null if unknown/self. */
    getLeaderUrl(): string | null {
        if (!this.leaderId || this.leaderId === this.id) return null;
        return this.peers.find((p) => p.id === this.leaderId)?.url ?? null;
    }

    /**
     * Propose a command. Resolves once the entry is committed and applied.
     * Throws {@link NotLeaderError} if this node is not the leader.
     */
    submit(command: Command, meta?: CommandMeta): Promise<ApplyResult> {
        if (this.role !== 'leader') throw new NotLeaderError(this.leaderId);

        const entry: LogEntry = { term: this.currentTerm, command, meta };
        this.log.push(entry);
        this.persist();
        const index = this.lastLogIndex();
        this.matchIndex.set(this.id, index);
        this.logger?.debug('proposed command', { index, type: command.type, requestId: meta?.requestId });

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
            this.persist();
        }
        this.clearHeartbeatTimer();
        this.resetElectionTimer();
    }

    private becomeCandidate(): void {
        this.currentTerm += 1;
        this.role = 'candidate';
        this.votedFor = this.id;
        this.leaderId = null;
        this.persist();
        this.metrics?.raftElections.inc({ node: this.id });
        this.logger?.info('became candidate', { term: this.currentTerm });
        this.resetElectionTimer();
        void this.runElection(this.currentTerm);
    }

    private becomeLeader(): void {
        this.role = 'leader';
        this.leaderId = this.id;
        this.clearElectionTimer();
        const nextIdx = this.lastLogIndex() + 1;
        for (const peer of this.peers) {
            this.nextIndex.set(peer.id, nextIdx);
            this.matchIndex.set(peer.id, 0);
        }
        this.logger?.info('became LEADER', { term: this.currentTerm });
        // Commit a no-op for this term so prior entries can be safely committed.
        this.log.push({ term: this.currentTerm, command: { type: 'NOOP' } });
        this.persist();
        this.matchIndex.set(this.id, this.lastLogIndex());
        this.advanceCommitIndex();
        this.startHeartbeat();
    }

    // ---- elections ----

    private async runElection(term: number): Promise<void> {
        const args: RequestVoteArgs = {
            term,
            candidateId: this.id,
            lastLogIndex: this.lastLogIndex(),
            lastLogTerm: this.termAt(this.lastLogIndex())!,
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

        const lastLogIndex = this.lastLogIndex();
        const lastLogTerm = this.termAt(lastLogIndex)!;
        const logOk =
            args.lastLogTerm > lastLogTerm ||
            (args.lastLogTerm === lastLogTerm && args.lastLogIndex >= lastLogIndex);

        const grant =
            args.term >= this.currentTerm &&
            (this.votedFor === null || this.votedFor === args.candidateId) &&
            logOk;

        if (grant) {
            this.votedFor = args.candidateId;
            this.persist();
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
        const nextIdx = this.nextIndex.get(peer.id) ?? this.lastLogIndex() + 1;

        // The entry the follower needs has been compacted away — ship a snapshot.
        if (nextIdx <= this.lastIncludedIndex) {
            await this.sendSnapshot(peer);
            return;
        }

        const term = this.currentTerm;
        const prevLogIndex = nextIdx - 1;
        const args: AppendEntriesArgs = {
            term,
            leaderId: this.id,
            prevLogIndex,
            prevLogTerm: this.termAt(prevLogIndex) ?? this.lastIncludedTerm,
            entries: this.log.slice(this.pos(nextIdx)),
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
            // Log inconsistency: back off and retry (may fall through to a snapshot).
            this.nextIndex.set(peer.id, Math.max(1, nextIdx - 1));
        }
    }

    private async sendSnapshot(peer: PeerInfo): Promise<void> {
        const term = this.currentTerm;
        const snapIndex = this.lastApplied;
        const args: InstallSnapshotArgs = {
            term,
            leaderId: this.id,
            lastIncludedIndex: snapIndex,
            lastIncludedTerm: this.termAt(snapIndex) ?? this.lastIncludedTerm,
            data: this.stateMachine.snapshot(),
        };
        this.logger?.info('sending snapshot', { peer: peer.id, lastIncludedIndex: snapIndex });

        const reply = await this.transport.sendInstallSnapshot(peer, args);
        if (!reply || this.role !== 'leader' || this.currentTerm !== term) return;
        if (reply.term > this.currentTerm) {
            this.becomeFollower(reply.term);
            return;
        }
        this.matchIndex.set(peer.id, snapIndex);
        this.nextIndex.set(peer.id, snapIndex + 1);
        this.advanceCommitIndex();
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

        // Everything up to our snapshot is already durably applied.
        if (args.prevLogIndex < this.lastIncludedIndex) {
            return { term: this.currentTerm, success: true, matchIndex: this.lastLogIndex() };
        }

        // Consistency check on the entry preceding the new ones.
        const prevTerm = this.termAt(args.prevLogIndex);
        if (prevTerm === undefined || prevTerm !== args.prevLogTerm) {
            return { term: this.currentTerm, success: false };
        }

        // Append new entries, overwriting any conflicting suffix.
        let p = this.pos(args.prevLogIndex + 1);
        let mutated = false;
        for (const entry of args.entries) {
            const existing = this.log[p];
            if (existing && existing.term !== entry.term) {
                this.log.length = p; // truncate conflicting tail
                mutated = true;
            }
            if (p >= this.log.length) {
                this.log.push(entry);
                mutated = true;
            }
            p += 1;
        }
        if (mutated) this.persist();

        if (args.leaderCommit > this.commitIndex) {
            this.commitIndex = Math.min(args.leaderCommit, this.lastLogIndex());
            this.applyCommitted();
        }

        return { term: this.currentTerm, success: true, matchIndex: args.prevLogIndex + args.entries.length };
    }

    handleInstallSnapshot(args: InstallSnapshotArgs): InstallSnapshotReply {
        if (args.term < this.currentTerm) {
            return { term: this.currentTerm };
        }
        if (args.term > this.currentTerm) this.becomeFollower(args.term);
        this.role = 'follower';
        this.leaderId = args.leaderId;
        this.resetElectionTimer();

        // Ignore a stale snapshot we've already surpassed.
        if (args.lastIncludedIndex <= this.lastIncludedIndex) {
            return { term: this.currentTerm };
        }

        this.stateMachine.restore(args.data as RsmSnapshot);
        this.lastIncludedIndex = args.lastIncludedIndex;
        this.lastIncludedTerm = args.lastIncludedTerm;
        this.commitIndex = args.lastIncludedIndex;
        this.lastApplied = args.lastIncludedIndex;
        // Discard the entire log; the leader will re-replicate anything newer.
        this.log = [{ term: args.lastIncludedTerm, command: { type: 'NOOP' } }];
        this.persist();
        this.storage.saveSnapshot({
            lastIncludedIndex: this.lastIncludedIndex,
            lastIncludedTerm: this.lastIncludedTerm,
            data: args.data,
        });
        this.logger?.info('installed snapshot', { lastIncludedIndex: this.lastIncludedIndex });
        return { term: this.currentTerm };
    }

    /** Leader: advance commitIndex to the highest index replicated on a majority. */
    private advanceCommitIndex(): void {
        for (let n = this.lastLogIndex(); n > this.commitIndex; n--) {
            // Raft safety: only commit entries from the current term by counting.
            if (this.termAt(n) !== this.currentTerm) continue;
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

    /** Apply every newly-committed entry, resolve waiting proposals, maybe snapshot. */
    private applyCommitted(): void {
        while (this.lastApplied < this.commitIndex) {
            this.lastApplied += 1;
            const entry = this.entryAt(this.lastApplied)!;
            const result = this.stateMachine.apply(this.lastApplied, entry);

            const waiter = this.pending.get(this.lastApplied);
            if (waiter) {
                this.pending.delete(this.lastApplied);
                if (waiter.term === entry.term) waiter.resolve(result);
                else waiter.reject(new NotLeaderError(this.leaderId));
            }
        }
        this.maybeSnapshot();
    }

    // ---- log compaction ----

    private maybeSnapshot(): void {
        if (this.log.length - 1 < this.snapshotThreshold) return;
        if (this.lastApplied <= this.lastIncludedIndex) return;
        this.takeSnapshot();
    }

    private takeSnapshot(): void {
        const snapIndex = this.lastApplied;
        const snapTerm = this.termAt(snapIndex)!;
        const data = this.stateMachine.snapshot();

        // Keep the sentinel + everything after the snapshot point.
        const tail = this.log.slice(this.pos(snapIndex) + 1);
        this.log = [{ term: snapTerm, command: { type: 'NOOP' } }, ...tail];
        this.lastIncludedIndex = snapIndex;
        this.lastIncludedTerm = snapTerm;

        this.persist();
        this.storage.saveSnapshot({ lastIncludedIndex: snapIndex, lastIncludedTerm: snapTerm, data });
        this.logger?.info('took snapshot', { lastIncludedIndex: snapIndex, remainingEntries: this.log.length - 1 });
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

    /** Push current Raft state into the metrics registry (called at scrape time). */
    collectMetrics(): void {
        const m = this.metrics;
        if (!m) return;
        const node = this.id;
        m.raftTerm.set(this.currentTerm, { node });
        m.raftIsLeader.set(this.role === 'leader' ? 1 : 0, { node });
        m.raftCommitIndex.set(this.commitIndex, { node });
        m.raftLastApplied.set(this.lastApplied, { node });
        m.raftLogLength.set(this.log.length - 1, { node });
        m.raftSnapshotIndex.set(this.lastIncludedIndex, { node });
        m.raftDedupCacheSize.set(this.stateMachine.dedupCacheSize(), { node });
        m.booksTotal.set(this.stateMachine.size(), { node });
        if (this.role === 'leader') {
            const last = this.lastLogIndex();
            for (const peer of this.peers) {
                m.raftReplicationLag.set(last - (this.matchIndex.get(peer.id) ?? 0), { node, peer: peer.id });
            }
        }
    }
}
