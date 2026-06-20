import { ReplicatedStateMachine, RsmSnapshot } from './replicatedStateMachine';
import { MemoryStorage, RaftStorage, PersistentState } from './storage';
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
    /** This node's own address, advertised to peers in membership configs. */
    selfUrl?: string;
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

/** Raised when a membership change is invalid or cannot be started right now. */
export class MembershipError extends Error {
    constructor(message: string) {
        super(message);
        this.name = 'MembershipError';
    }
}

interface PendingProposal {
    term: number;
    resolve: (result: ApplyResult) => void;
    reject: (err: Error) => void;
}

interface ReadWaiter {
    index: number;
    resolve: () => void;
    reject: (err: Error) => void;
    timer: NodeJS.Timeout;
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
    private readonly selfPeer: PeerInfo;
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

    // Cluster membership (Raft dissertation §4). The configuration lives in the
    // log and takes effect the moment an entry is appended (not when committed).
    // `members` is the current voting set (including self), derived from the log;
    // `baseConfig` is the configuration as of the snapshot boundary, the base the
    // log's CONFIG entries are layered on top of.
    private members = new Map<string, PeerInfo>();
    private baseConfig: PeerInfo[];
    /** Wall-clock of the last AppendEntries/InstallSnapshot from a current leader. */
    private lastLeaderContact = 0;

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

    // Linearizable-read barriers awaiting the state machine to apply through an index.
    private readWaiters: ReadWaiter[] = [];

    private readonly electionMinMs: number;
    private readonly electionMaxMs: number;
    private readonly heartbeatMs: number;
    private readonly snapshotThreshold: number;
    private running = false;

    constructor(config: RaftConfig, transport: Transport) {
        this.id = config.id;
        this.selfPeer = { id: config.id, url: config.selfUrl ?? `local://${config.id}` };
        this.transport = transport;
        this.storage = config.storage ?? new MemoryStorage();
        this.logger = config.logger?.child({ component: 'raft', node: config.id });
        this.metrics = config.metrics;
        this.electionMinMs = config.electionMinMs ?? 150;
        this.electionMaxMs = config.electionMaxMs ?? 300;
        this.heartbeatMs = config.heartbeatMs ?? 50;
        this.snapshotThreshold = config.snapshotThreshold ?? 1000;
        this.stateMachine = new ReplicatedStateMachine(config.dedupLimit);
        // Bootstrap configuration: the peers from env plus this node itself.
        this.baseConfig = [this.selfPeer, ...config.peers];
        this.recomputeMembers();
    }

    // ---- cluster membership ----

    /** Other voting members (the current configuration minus this node). */
    private otherMembers(): PeerInfo[] {
        return [...this.members.values()].filter((p) => p.id !== this.id);
    }

    /** Votes/acks needed for a decision: a strict majority of the current config. */
    private quorum(): number {
        return Math.floor(this.members.size / 2) + 1;
    }

    /** Configuration effective at `absIndex`: baseConfig + CONFIG entries up to it. */
    private configAt(absIndex: number): PeerInfo[] {
        let cfg = this.baseConfig;
        const upto = this.pos(Math.min(absIndex, this.lastLogIndex()));
        for (let p = 1; p <= upto; p++) {
            const cmd = this.log[p]?.command;
            if (cmd?.type === 'CONFIG') cfg = cmd.members;
        }
        return cfg;
    }

    /** Recompute `members` from the latest configuration in the log. */
    private recomputeMembers(): void {
        const cfg = this.configAt(this.lastLogIndex());
        const next = new Map<string, PeerInfo>();
        for (const p of cfg) next.set(p.id, p);
        this.members = next;
    }

    /** True if the log holds a not-yet-committed CONFIG entry (a change in flight). */
    private hasUncommittedConfig(): boolean {
        for (let i = this.commitIndex + 1; i <= this.lastLogIndex(); i++) {
            if (this.entryAt(i)?.command.type === 'CONFIG') return true;
        }
        return false;
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
            // The config compacted into the snapshot becomes the new base config.
            if (snap.members) this.baseConfig = snap.members;
            this.logger?.info('restored snapshot', { lastIncludedIndex: this.lastIncludedIndex });
        }

        const persisted = this.storage.load();
        if (persisted) {
            this.currentTerm = persisted.currentTerm;
            this.votedFor = persisted.votedFor;
            this.reconcileLog(persisted);
            this.logger?.info('restored persistent state', {
                term: this.currentTerm,
                logEntries: this.log.length - 1,
                lastIncludedIndex: this.lastIncludedIndex,
            });
        }
        // Adopt whatever configuration the restored log/snapshot implies.
        this.recomputeMembers();
        this.becomeFollower(this.currentTerm);
    }

    /**
     * Reconcile the persisted log with the (separately-persisted) snapshot. The
     * two files are written non-atomically, so a crash can leave them out of step.
     * We always write the snapshot before the compacted log, so the persisted log
     * base can only lag the snapshot, never lead it; repair that case here instead
     * of trusting the log's base blindly (which would corrupt the index math).
     */
    private reconcileLog(persisted: PersistentState): void {
        const persistedBase = persisted.baseIndex ?? 0;
        const persistedBaseTerm = persisted.baseTerm ?? 0;

        if (persistedBase >= this.lastIncludedIndex) {
            // Log is aligned with, or newer than, the snapshot — trust it as-is.
            this.log = persisted.log;
            this.lastIncludedIndex = persistedBase;
            this.lastIncludedTerm = persistedBaseTerm;
            return;
        }

        // Snapshot is newer than the persisted log base (crash after the snapshot
        // landed but before the log was compacted). Drop entries the snapshot now
        // covers, keeping only the tail strictly after `lastIncludedIndex`.
        const tailStart = this.lastIncludedIndex - persistedBase + 1;
        const tail = persisted.log.slice(tailStart);
        this.log = [{ term: this.lastIncludedTerm, command: { type: 'NOOP' } }, ...tail];
        this.logger?.warn('reconciled stale log against newer snapshot', {
            persistedBase,
            snapshotIndex: this.lastIncludedIndex,
            keptTail: tail.length,
        });
    }

    /** Persist the durable subset of state (term, vote, log + its snapshot base). */
    private persist(): void {
        this.storage.save({
            currentTerm: this.currentTerm,
            votedFor: this.votedFor,
            log: this.log,
            baseIndex: this.lastIncludedIndex,
            baseTerm: this.lastIncludedTerm,
        });
    }

    stop(): void {
        this.running = false;
        this.clearElectionTimer();
        this.clearHeartbeatTimer();
        for (const p of this.pending.values()) p.reject(new Error('node stopped'));
        this.pending.clear();
        this.rejectReadWaiters(new Error('node stopped'));
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
            members: [...this.members.keys()],
        };
    }

    /** The current cluster configuration (voting members, including self). */
    getMembers(): PeerInfo[] {
        return [...this.members.values()];
    }

    getLeaderId(): string | null {
        return this.leaderId;
    }

    /** URL of the current leader (for write forwarding), or null if unknown/self. */
    getLeaderUrl(): string | null {
        if (!this.leaderId || this.leaderId === this.id) return null;
        return this.members.get(this.leaderId)?.url ?? null;
    }

    /**
     * Propose a command. Resolves once the entry is committed and applied, and
     * rejects with {@link NotLeaderError} if this node is not the leader.
     */
    submit(command: Command, meta?: CommandMeta): Promise<ApplyResult> {
        if (this.role !== 'leader') return Promise.reject(new NotLeaderError(this.leaderId));

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

    /**
     * Add or remove a single voting member (Raft dissertation §4.1). Changing one
     * server at a time keeps the old and new majorities overlapping, so no split
     * decision is possible without a joint-consensus phase. The new configuration
     * is appended as a CONFIG entry and adopted immediately; the returned promise
     * resolves once that entry commits. Leader-only.
     */
    changeMembership(change: { add?: PeerInfo; remove?: string }, meta?: CommandMeta): Promise<ApplyResult> {
        if (this.role !== 'leader') throw new NotLeaderError(this.leaderId);
        // One change at a time: refuse while a previous change is still uncommitted.
        if (this.hasUncommittedConfig()) {
            return Promise.reject(new MembershipError('A membership change is already in progress'));
        }

        const next = new Map(this.members);
        if (change.add) {
            if (next.has(change.add.id)) {
                return Promise.reject(new MembershipError(`${change.add.id} is already a member`));
            }
            next.set(change.add.id, change.add);
        } else if (change.remove) {
            if (!next.has(change.remove)) {
                return Promise.reject(new MembershipError(`${change.remove} is not a member`));
            }
            if (next.size === 1) {
                return Promise.reject(new MembershipError('Cannot remove the last member'));
            }
            next.delete(change.remove);
        } else {
            return Promise.reject(new MembershipError('Specify add or remove'));
        }

        return this.submitConfig([...next.values()], meta);
    }

    /** Append a CONFIG entry, adopt it immediately, and replicate it. */
    private submitConfig(members: PeerInfo[], meta?: CommandMeta): Promise<ApplyResult> {
        const entry: LogEntry = { term: this.currentTerm, command: { type: 'CONFIG', members }, meta };
        this.log.push(entry);
        // Adopt the new configuration the instant it is in the log (Raft §4.1).
        this.recomputeMembers();
        this.persist();
        const index = this.lastLogIndex();
        this.matchIndex.set(this.id, index);
        this.nextIndex.set(this.id, index + 1);
        this.logger?.info('membership change proposed', { index, members: members.map((m) => m.id) });

        const promise = new Promise<ApplyResult>((resolve, reject) => {
            this.pending.set(index, { term: entry.term, resolve, reject });
        });
        this.advanceCommitIndex();
        this.broadcastAppendEntries();
        return promise;
    }

    /**
     * Linearizable read barrier (Raft §6.4, "ReadIndex"). Resolving it
     * guarantees a subsequent local read reflects every write committed before
     * the barrier was requested — without writing to the log:
     *
     *   1. Capture `readIndex = commitIndex` (the leader has committed a no-op
     *      from its current term on election, so its commitIndex is current).
     *   2. Confirm we are *still* the leader by exchanging a round of heartbeats
     *      with a majority — proving no newer leader has superseded us.
     *   3. Wait until the state machine has applied through `readIndex`.
     *
     * Throws {@link NotLeaderError} if this node is not (or ceases to be) the
     * leader, so the caller can forward the read to the leader like a write.
     */
    async readBarrier(): Promise<void> {
        if (this.role !== 'leader') throw new NotLeaderError(this.leaderId);
        const readIndex = this.commitIndex;
        const confirmed = await this.confirmLeadership();
        if (!confirmed || this.role !== 'leader') throw new NotLeaderError(this.leaderId);
        this.metrics?.raftReadBarriers.inc({ node: this.id });
        await this.waitForApplied(readIndex);
    }

    /**
     * Exchange one round of AppendEntries with the peers and report whether a
     * majority still acknowledge our leadership for the current term. A reply
     * that doesn't carry a higher term counts as an acknowledgement even if it
     * reports a log mismatch — the follower still recognises us as leader.
     */
    private async confirmLeadership(): Promise<boolean> {
        const majority = this.quorum();
        if (majority <= 1) return true; // single-node cluster is its own majority
        const term = this.currentTerm;
        const acks = await Promise.all(this.otherMembers().map((peer) => this.replicateTo(peer)));
        if (this.role !== 'leader' || this.currentTerm !== term) return false;
        const confirmed = 1 + acks.filter(Boolean).length; // +1 for self
        return confirmed >= majority;
    }

    private waitForApplied(index: number): Promise<void> {
        if (this.lastApplied >= index) return Promise.resolve();
        return new Promise<void>((resolve, reject) => {
            const timer = setTimeout(() => {
                this.readWaiters = this.readWaiters.filter((w) => w.timer !== timer);
                reject(new Error('READ_BARRIER_TIMEOUT'));
            }, 2000);
            this.readWaiters.push({ index, resolve, reject, timer });
        });
    }

    private resolveReadWaiters(): void {
        if (this.readWaiters.length === 0) return;
        const remaining: ReadWaiter[] = [];
        for (const w of this.readWaiters) {
            if (this.lastApplied >= w.index) {
                clearTimeout(w.timer);
                w.resolve();
            } else {
                remaining.push(w);
            }
        }
        this.readWaiters = remaining;
    }

    private rejectReadWaiters(err: Error): void {
        if (this.readWaiters.length === 0) return;
        const waiters = this.readWaiters;
        this.readWaiters = [];
        for (const w of waiters) {
            clearTimeout(w.timer);
            w.reject(err);
        }
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
        // Reads in flight can no longer be served linearizably from this node.
        this.rejectReadWaiters(new NotLeaderError(this.leaderId));
    }

    private becomeCandidate(): void {
        // A node removed from the configuration must not campaign: it can never win
        // a real quorum and would only disrupt the remaining cluster (zombie leader).
        if (!this.members.has(this.id)) {
            this.logger?.debug('not campaigning: not a cluster member');
            return;
        }
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
        this.nextIndex.clear();
        this.matchIndex.clear();
        for (const peer of this.otherMembers()) {
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
        const majority = this.quorum();
        if (votes >= majority) {
            this.becomeLeader();
            return;
        }

        await Promise.all(
            this.otherMembers().map(async (peer) => {
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
        // Disruption avoidance (Raft dissertation §4.2.3): if we have heard from a
        // leader within the minimum election timeout, ignore the vote request and
        // do not adopt its term. This stops a removed or partitioned server — which
        // keeps timing out and bumping its term — from forcing needless elections.
        const sinceLeader = Date.now() - this.lastLeaderContact;
        if (this.leaderId !== null && this.leaderId !== args.candidateId && sinceLeader < this.electionMinMs) {
            return { term: this.currentTerm, voteGranted: false };
        }

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
        for (const peer of this.otherMembers()) void this.replicateTo(peer);
    }

    /** Returns true if the peer acknowledged our leadership for the current term. */
    private async replicateTo(peer: PeerInfo): Promise<boolean> {
        if (this.role !== 'leader') return false;
        const nextIdx = this.nextIndex.get(peer.id) ?? this.lastLogIndex() + 1;

        // The entry the follower needs has been compacted away — ship a snapshot.
        if (nextIdx <= this.lastIncludedIndex) {
            return this.sendSnapshot(peer);
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
        if (!reply || this.role !== 'leader' || this.currentTerm !== term) return false;

        if (reply.term > this.currentTerm) {
            this.becomeFollower(reply.term);
            return false;
        }

        if (reply.success) {
            // Never trust a follower-reported matchIndex beyond what we actually sent,
            // so a buggy/malicious follower can't inflate matchIndex and prematurely
            // advance the commit index.
            const expected = prevLogIndex + args.entries.length;
            const match = Math.min(reply.matchIndex ?? expected, expected);
            this.matchIndex.set(peer.id, match);
            this.nextIndex.set(peer.id, match + 1);
            this.advanceCommitIndex();
        } else {
            // Log inconsistency: back off and retry (may fall through to a snapshot).
            this.nextIndex.set(peer.id, Math.max(1, nextIdx - 1));
        }
        // Either way the follower recognised us as leader for this term.
        return true;
    }

    /** Returns true if the peer acknowledged our leadership for the current term. */
    private async sendSnapshot(peer: PeerInfo): Promise<boolean> {
        const term = this.currentTerm;
        // Ship the DURABLE snapshot (its persisted lastIncludedIndex/Term/data), not
        // a live snapshot taken at lastApplied. A live snapshot reflects state through
        // lastApplied but would be labelled with a boundary whose term may already be
        // compacted away — the term wouldn't match the entry, corrupting the follower's
        // later AppendEntries consistency checks. This path is only reached once a
        // snapshot exists (nextIndex <= lastIncludedIndex >= 1).
        const durable = this.storage.loadSnapshot();
        const snapIndex = durable?.lastIncludedIndex ?? this.lastIncludedIndex;
        const args: InstallSnapshotArgs = {
            term,
            leaderId: this.id,
            lastIncludedIndex: snapIndex,
            lastIncludedTerm: durable?.lastIncludedTerm ?? this.lastIncludedTerm,
            members: durable?.members ?? this.configAt(snapIndex),
            data: durable?.data ?? this.stateMachine.snapshot(),
        };
        this.logger?.info('sending snapshot', { peer: peer.id, lastIncludedIndex: snapIndex });

        const reply = await this.transport.sendInstallSnapshot(peer, args);
        if (!reply || this.role !== 'leader' || this.currentTerm !== term) return false;
        if (reply.term > this.currentTerm) {
            this.becomeFollower(reply.term);
            return false;
        }
        this.matchIndex.set(peer.id, snapIndex);
        this.nextIndex.set(peer.id, snapIndex + 1);
        this.advanceCommitIndex();
        return true;
    }

    handleAppendEntries(args: AppendEntriesArgs): AppendEntriesReply {
        if (args.term < this.currentTerm) {
            return { term: this.currentTerm, success: false };
        }

        // Valid leader for this term — recognise it and reset our election clock.
        if (args.term > this.currentTerm) this.becomeFollower(args.term);
        this.role = 'follower';
        this.leaderId = args.leaderId;
        this.lastLeaderContact = Date.now();
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
        if (mutated) {
            this.persist();
            // A CONFIG entry may have arrived/changed; adopt it immediately (Raft §4.1).
            this.recomputeMembers();
        }

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
        this.lastLeaderContact = Date.now();
        this.resetElectionTimer();

        // Ignore a snapshot we've already covered (don't roll back our own state).
        if (args.lastIncludedIndex <= this.lastIncludedIndex || args.lastIncludedIndex <= this.commitIndex) {
            return { term: this.currentTerm };
        }

        this.stateMachine.restore(args.data as RsmSnapshot);

        // Raft figure 13, step 6: if we already have the entry at the snapshot's
        // last-included index with a matching term, the snapshot is just a prefix
        // of our log — keep the tail after it. Otherwise our log conflicts or falls
        // short, so discard it entirely and let the leader re-replicate.
        const existingTerm = this.termAt(args.lastIncludedIndex);
        const tail =
            existingTerm === args.lastIncludedTerm
                ? this.log.slice(this.pos(args.lastIncludedIndex) + 1)
                : [];
        this.log = [{ term: args.lastIncludedTerm, command: { type: 'NOOP' } }, ...tail];

        this.lastIncludedIndex = args.lastIncludedIndex;
        this.lastIncludedTerm = args.lastIncludedTerm;
        // commitIndex/lastApplied only move forward (guarded above against rollback).
        this.commitIndex = Math.max(this.commitIndex, args.lastIncludedIndex);
        this.lastApplied = Math.max(this.lastApplied, args.lastIncludedIndex);
        // Adopt the configuration carried with the snapshot as the new base config.
        if (args.members) this.baseConfig = args.members;
        this.recomputeMembers();

        // Persist snapshot before the compacted log (same crash-ordering as takeSnapshot).
        this.storage.saveSnapshot({
            lastIncludedIndex: this.lastIncludedIndex,
            lastIncludedTerm: this.lastIncludedTerm,
            members: args.members,
            data: args.data,
        });
        this.persist();
        this.logger?.info('installed snapshot', { lastIncludedIndex: this.lastIncludedIndex, keptTail: tail.length });
        return { term: this.currentTerm };
    }

    /** Leader: advance commitIndex to the highest index replicated on a majority. */
    private advanceCommitIndex(): void {
        for (let n = this.lastLogIndex(); n > this.commitIndex; n--) {
            // Raft safety: only commit entries from the current term by counting.
            if (this.termAt(n) !== this.currentTerm) continue;
            // Count only current voting members (a removed node mustn't count).
            let count = 0;
            for (const id of this.members.keys()) {
                if ((this.matchIndex.get(id) ?? 0) >= n) count += 1;
            }
            if (count >= this.quorum()) {
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
        this.resolveReadWaiters();
        // If a committed config removed us, step down: we are no longer a member.
        if (this.role === 'leader' && !this.members.has(this.id)) {
            this.logger?.info('stepping down: removed from cluster configuration');
            this.leaderId = null;
            this.becomeFollower(this.currentTerm);
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
        // Capture the configuration at the snapshot point before compacting — the
        // CONFIG entries that produced it are about to be discarded.
        const members = this.configAt(snapIndex);

        // Keep the sentinel + everything after the snapshot point.
        const tail = this.log.slice(this.pos(snapIndex) + 1);

        // Order matters for crash safety: persist the snapshot FIRST, then the
        // compacted log. That way the durable log base can only ever lag the
        // snapshot (reconciled on restart), never lead it (which would lose the
        // state the discarded entries produced).
        this.storage.saveSnapshot({ lastIncludedIndex: snapIndex, lastIncludedTerm: snapTerm, members, data });

        this.log = [{ term: snapTerm, command: { type: 'NOOP' } }, ...tail];
        this.lastIncludedIndex = snapIndex;
        this.lastIncludedTerm = snapTerm;
        this.baseConfig = members;
        this.persist();
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
        m.raftClusterSize.set(this.members.size, { node });
        m.booksTotal.set(this.stateMachine.size(), { node });
        // Rebuild the per-peer lag series each scrape so a removed peer (or one we
        // no longer lead) doesn't leave a stale gauge behind forever.
        m.raftReplicationLag.reset();
        if (this.role === 'leader') {
            const last = this.lastLogIndex();
            for (const peer of this.otherMembers()) {
                m.raftReplicationLag.set(last - (this.matchIndex.get(peer.id) ?? 0), { node, peer: peer.id });
            }
        }
    }
}
