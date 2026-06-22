import { Consensus } from './consensus';
import { ReplicatedStateMachine, RsmSnapshot } from './replicatedStateMachine';
import { StateMachine } from './stateMachine';
import { MemoryStorage, RaftStorage, PersistentState } from './storage';
import { RpcHandler, Transport } from './transport';
import { Logger } from '../platform/logger';
import { MetricsRegistry } from '../platform/metrics';
import {
    AppCommand,
    AppendEntriesArgs,
    AppendEntriesReply,
    ApplyResult,
    Command,
    CommandMeta,
    InstallSnapshotArgs,
    InstallSnapshotReply,
    isConfigCommand,
    LogEntry,
    PeerInfo,
    ReadIndexArgs,
    ReadIndexReply,
    RequestVoteArgs,
    RequestVoteReply,
    Role,
} from './types';

export interface RaftConfig<
    C extends AppCommand = AppCommand,
    T = unknown,
    SM extends StateMachine<C, T> = StateMachine<C, T>,
> {
    id: string;
    peers: PeerInfo[];
    /**
     * The application state machine this node replicates. Typed as the
     * intersection so that, given a concrete state machine, TypeScript infers the
     * command (`C`) and result (`T`) types from it at the construction site.
     */
    stateMachine: SM & StateMachine<C, T>;
    /** This node's own address, advertised to peers in membership configs. */
    selfUrl?: string;
    electionMinMs?: number;
    electionMaxMs?: number;
    heartbeatMs?: number;
    /** Take a snapshot once the in-memory log holds this many entries. */
    snapshotThreshold?: number;
    /** Max size (in characters) of a single InstallSnapshot chunk on the wire. */
    snapshotChunkBytes?: number;
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

/**
 * A configuration as derived from a CONFIG log entry (or a snapshot base).
 * `members` is C-new (the target voting set). When `oldMembers` is present the
 * configuration is **joint** (C-old,new) and every decision needs a majority of
 * BOTH sets separately (Raft §6 / ADR-0022).
 */
interface ConfigState {
    members: PeerInfo[];
    oldMembers?: PeerInfo[];
}

/** True iff `acks` contains a strict majority (floor(n/2)+1) of `config`. */
function majorityOf(acks: Set<string>, config: Set<string>): boolean {
    if (config.size === 0) return true; // an empty config imposes no constraint
    let count = 0;
    for (const id of config) if (acks.has(id)) count += 1;
    return count >= Math.floor(config.size / 2) + 1;
}

interface PendingProposal<T> {
    term: number;
    resolve: (result: ApplyResult<T>) => void;
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
export class RaftNode<
    C extends AppCommand = AppCommand,
    T = unknown,
    SM extends StateMachine<C, T> = StateMachine<C, T>,
> implements Consensus<C, T, SM>, RpcHandler {
    readonly id: string;
    private readonly selfPeer: PeerInfo;
    private readonly transport: Transport;
    private readonly storage: RaftStorage;
    private readonly logger?: Logger;
    private readonly metrics?: MetricsRegistry;
    /** The application state machine plugged into this node (for domain reads). */
    readonly app: SM;
    /** Substrate wrapper adding audit + idempotency over the application state machine. */
    private readonly rsm: ReplicatedStateMachine<C, T>;

    // Persistent state.
    private currentTerm = 0;
    private votedFor: string | null = null;
    /** log[0] is a sentinel at absolute index `lastIncludedIndex`. */
    private log: LogEntry<C>[] = [{ term: 0, command: { type: 'NOOP' } }];
    private lastIncludedIndex = 0;
    private lastIncludedTerm = 0;

    // Cluster membership (Raft §6, joint consensus; ADR-0022). The configuration
    // lives in the log and takes effect the moment an entry is appended (not when
    // committed). `members` is the current voting UNION (including self), derived
    // from the log — during a joint transition that's C-old ∪ C-new, otherwise
    // just C-new; it is the set of replication targets. The majority PREDICATE,
    // however, is `inMajority`, which during a joint transition requires a majority
    // of C-old AND of C-new separately (the two sets below). `baseConfig` is the
    // configuration as of the snapshot boundary — the base the log's CONFIG entries
    // are layered on top of; it may itself be joint if the snapshot boundary fell
    // inside a transition.
    private members = new Map<string, PeerInfo>();
    /** During a joint transition: the two voting sets whose majorities are BOTH required. */
    private configOld: Set<string> | null = null;
    private configNew = new Set<string>();
    private baseConfig: ConfigState;
    /**
     * Set when this node becomes leader while the adopted config is already joint —
     * i.e. it INHERITED an in-progress transition a previous leader appended but
     * never finished. Such a leader must complete the transition (Raft §4.3) by
     * appending the final C-new once the joint config commits (see `applyCommitted`),
     * or the cluster stays wedged in joint consensus. A leader that ORIGINATES a
     * change (via `changeMembership`) finalizes it itself, so this stays false for
     * those — the two finalization paths are mutually exclusive.
     */
    private inheritedJoint = false;
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
    // Peers we are currently streaming a (multi-chunk) snapshot to. Prevents a
    // heartbeat from starting a second, interleaving stream to the same peer —
    // concurrent offset===0 chunks would reset each other's reassembly buffer.
    private snapshotInFlight = new Set<string>();

    // Timers.
    private electionTimer: NodeJS.Timeout | null = null;
    private heartbeatTimer: NodeJS.Timeout | null = null;

    // Proposals awaiting commit, keyed by absolute log index.
    private pending = new Map<number, PendingProposal<T>>();

    // Linearizable-read barriers awaiting the state machine to apply through an index.
    private readWaiters: ReadWaiter[] = [];

    // Committed-entry subscribers (the read-stream / edge-replica fan-out; ADR-0023).
    // Each is invoked, in commit order, with every entry as it is applied — a
    // read-only, non-voting tap on the committed log. Subscribers never enter any
    // voting set, never ack, and never affect quorum/commit; they only observe.
    private committedListeners = new Set<(index: number, entry: LogEntry<C>) => void>();

    private readonly electionMinMs: number;
    private readonly electionMaxMs: number;
    private readonly heartbeatMs: number;
    private readonly snapshotThreshold: number;
    private readonly snapshotChunkBytes: number;
    private running = false;

    // Per-node reassembly buffer for an in-flight chunked InstallSnapshot stream.
    // Keyed by snapshot identity (lastIncludedIndex+term); reset on a fresh
    // offset===0 chunk, a gap, or a mismatched identity (fail closed — the leader
    // retries from offset 0). Bounded by the size of one snapshot.
    private snapshotBuffer: {
        lastIncludedIndex: number;
        lastIncludedTerm: number;
        nextOffset: number;
        data: string;
    } | null = null;

    constructor(config: RaftConfig<C, T, SM>, transport: Transport) {
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
        this.snapshotChunkBytes = config.snapshotChunkBytes ?? 64 * 1024;
        this.app = config.stateMachine;
        this.rsm = new ReplicatedStateMachine<C, T>(this.app, config.dedupLimit);
        // Bootstrap configuration: the peers from env plus this node itself (a
        // simple, non-joint config).
        this.baseConfig = { members: [this.selfPeer, ...config.peers] };
        this.recomputeMembers();
    }

    // ---- cluster membership ----

    /**
     * Other voting members (the current voting UNION minus this node) — the set
     * of replication targets. During a joint transition this is C-old ∪ C-new, so
     * the leader replicates to (and confirms leadership against) every node in
     * either configuration; the dual-majority predicate then decides separately.
     */
    private otherMembers(): PeerInfo[] {
        return [...this.members.values()].filter((p) => p.id !== this.id);
    }

    /**
     * Dual-majority predicate (Raft §6 / ADR-0022) — the single gate for EVERY
     * quorum decision (election tally, commit advance, leadership confirmation):
     *
     *  - Simple config: `|ids ∩ C-new| >= maj(C-new)`.
     *  - Joint config:  `|ids ∩ C-old| >= maj(C-old)` AND `|ids ∩ C-new| >= maj(C-new)`.
     *
     * During the joint phase this means no decision can be carried by a majority
     * of only ONE configuration — the property that makes arbitrary membership
     * changes safe (it is impossible for C-old and C-new to independently elect
     * leaders or commit conflicting entries).
     */
    private inMajority(ids: Iterable<string>): boolean {
        const set = ids instanceof Set ? ids : new Set(ids);
        if (this.configOld && !majorityOf(set, this.configOld)) return false;
        return majorityOf(set, this.configNew);
    }

    /** True iff the current configuration is the joint (transitional) C-old,new. */
    private isJoint(): boolean {
        return this.configOld !== null;
    }

    /** Configuration effective at `absIndex`: baseConfig + CONFIG entries up to it. */
    private configAt(absIndex: number): ConfigState {
        let cfg = this.baseConfig;
        const upto = this.pos(Math.min(absIndex, this.lastLogIndex()));
        for (let p = 1; p <= upto; p++) {
            const cmd = this.log[p]?.command;
            if (cmd && isConfigCommand(cmd)) cfg = { members: cmd.members, oldMembers: cmd.oldMembers };
        }
        return cfg;
    }

    /**
     * Recompute the derived configuration from the latest CONFIG entry in the log
     * (over the snapshot base): the voting union `members` (replication targets)
     * plus the two sets `configOld`/`configNew` the dual-majority predicate uses.
     */
    private recomputeMembers(): void {
        this.adoptConfig(this.configAt(this.lastLogIndex()));
    }

    /** Adopt a (possibly joint) configuration as the current derived membership. */
    private adoptConfig(cfg: ConfigState): void {
        const union = new Map<string, PeerInfo>();
        for (const p of cfg.members) union.set(p.id, p);
        this.configNew = new Set(cfg.members.map((p) => p.id));
        if (cfg.oldMembers) {
            for (const p of cfg.oldMembers) union.set(p.id, p);
            this.configOld = new Set(cfg.oldMembers.map((p) => p.id));
        } else {
            this.configOld = null;
        }
        this.members = union;
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

    private entryAt(absIndex: number): LogEntry<C> | undefined {
        const p = this.pos(absIndex);
        if (p < 0 || p >= this.log.length) return undefined;
        return this.log[p];
    }

    /** Earliest index in our log that shares `term` with `absIndex` (for backtracking). */
    private firstIndexOfTerm(absIndex: number, term: number): number {
        let i = absIndex;
        while (i > this.lastIncludedIndex + 1 && this.termAt(i - 1) === term) i -= 1;
        return i;
    }

    /** Last index in our log whose term is `term`, or undefined if none. */
    private lastIndexOfTerm(term: number): number | undefined {
        for (let i = this.lastLogIndex(); i > this.lastIncludedIndex; i--) {
            if (this.termAt(i) === term) return i;
        }
        return this.lastIncludedTerm === term ? this.lastIncludedIndex : undefined;
    }

    // ---- lifecycle ----

    start(): void {
        if (this.running) return;
        this.running = true;

        // Restore a snapshot first (rebuilds the state machine), then the log.
        const snap = this.storage.loadSnapshot();
        if (snap) {
            this.rsm.restore(snap.data as RsmSnapshot<T>);
            this.lastIncludedIndex = snap.lastIncludedIndex;
            this.lastIncludedTerm = snap.lastIncludedTerm;
            this.commitIndex = snap.lastIncludedIndex;
            this.lastApplied = snap.lastIncludedIndex;
            this.log = [{ term: snap.lastIncludedTerm, command: { type: 'NOOP' } }];
            // The config compacted into the snapshot becomes the new base config.
            // Preserve a joint config if the snapshot boundary fell inside one.
            if (snap.members) this.baseConfig = { members: snap.members, oldMembers: snap.oldMembers };
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

        // The persisted log crosses the (untyped JSON) storage boundary, so cast
        // it back to this node's application command type.
        const persistedLog = persisted.log as LogEntry<C>[];
        if (persistedBase >= this.lastIncludedIndex) {
            // Log is aligned with, or newer than, the snapshot — trust it as-is.
            this.log = persistedLog;
            this.lastIncludedIndex = persistedBase;
            this.lastIncludedTerm = persistedBaseTerm;
            return;
        }

        // Snapshot is newer than the persisted log base (crash after the snapshot
        // landed but before the log was compacted). Drop entries the snapshot now
        // covers, keeping only the tail strictly after `lastIncludedIndex`.
        const tailStart = this.lastIncludedIndex - persistedBase + 1;
        const tail = persistedLog.slice(tailStart);
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

    /** The replicated state machine wrapper (audit, idempotency, snapshots, size). */
    get stateMachine(): ReplicatedStateMachine<C, T> {
        return this.rsm;
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
            stateSize: this.rsm.size(),
            dedupCacheSize: this.rsm.dedupCacheSize(),
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
    submit(command: C, meta?: CommandMeta): Promise<ApplyResult<T>> {
        if (this.role !== 'leader') return Promise.reject(new NotLeaderError(this.leaderId));

        const entry: LogEntry<C> = { term: this.currentTerm, command, meta };
        this.log.push(entry);
        this.persist();
        const index = this.lastLogIndex();
        this.matchIndex.set(this.id, index);
        this.logger?.debug('proposed command', { index, type: command.type, requestId: meta?.requestId });

        const promise = new Promise<ApplyResult<T>>((resolve, reject) => {
            this.pending.set(index, { term: entry.term, resolve, reject });
        });

        // A single-node cluster is its own majority, so try to commit right away.
        this.advanceCommitIndex();
        // Push to followers immediately rather than waiting for the next heartbeat.
        this.broadcastAppendEntries();
        return promise;
    }

    /**
     * Add or remove a voting member via **joint consensus** (Raft §6 / ADR-0022,
     * superseding the single-server changes of ADR-0015). A change from C-old to
     * C-new transitions through a joint configuration C-old,new in which every
     * decision needs a majority of BOTH configurations separately — which makes
     * even an arbitrary change (one that does NOT overlap C-old in a majority)
     * safe. Two phases, each adopted on append and awaited to commit:
     *
     *   1. Append a JOINT CONFIG (`members: C-new, oldMembers: C-old`); await its
     *      commit (dual majority).
     *   2. Append a FINAL CONFIG (`members: C-new`); await its commit.
     *
     * The returned promise resolves when the FINAL config commits. A leader not in
     * C-new steps down once the final config commits. Leader-only.
     */
    async changeMembership(change: { add?: PeerInfo; remove?: string }, meta?: CommandMeta): Promise<ApplyResult<T>> {
        if (this.role !== 'leader') throw new NotLeaderError(this.leaderId);
        // One transition at a time: refuse while a previous change is still in
        // flight. That means both an uncommitted CONFIG entry AND a still-joint
        // configuration (a committed joint awaiting its final C-new — e.g. one this
        // leader inherited and is finalizing): starting a new change from either
        // state would overlap two transitions and break the dual-majority argument.
        if (this.hasUncommittedConfig() || this.isJoint()) {
            throw new MembershipError('A membership change is already in progress');
        }

        const oldMembers = [...this.members.values()];
        const next = new Map(this.members);
        if (change.add) {
            if (next.has(change.add.id)) {
                throw new MembershipError(`${change.add.id} is already a member`);
            }
            next.set(change.add.id, change.add);
        } else if (change.remove) {
            if (!next.has(change.remove)) {
                throw new MembershipError(`${change.remove} is not a member`);
            }
            if (next.size === 1) {
                throw new MembershipError('Cannot remove the last member');
            }
            next.delete(change.remove);
        } else {
            throw new MembershipError('Specify add or remove');
        }
        const newMembers = [...next.values()];

        // Phase 1: joint config C-old,new. Adopt on append, replicate, await commit
        // under dual majority. (A leader crash here leaves the joint CONFIG in the
        // log; the next leader adopts it on election and completes the transition —
        // see `inheritedJoint` / the finalization in `applyCommitted`.)
        const term = this.currentTerm;
        await this.submitConfigEntry({ type: 'CONFIG', members: newMembers, oldMembers }, meta);

        // We may have lost leadership (or moved terms) while awaiting the joint
        // commit — don't append the final config from a stale leadership.
        if (this.role !== 'leader' || this.currentTerm !== term) {
            throw new NotLeaderError(this.leaderId);
        }

        // Phase 2: final simple config C-new. Adopt on append, replicate, await
        // commit; the leader-self-removal step-down (if we're not in C-new) fires
        // from applyCommitted once this commits.
        return this.submitConfigEntry({ type: 'CONFIG', members: newMembers }, meta);
    }

    /** Append a CONFIG entry (joint or final), adopt it immediately, replicate it. */
    private submitConfigEntry(
        command: { type: 'CONFIG'; members: PeerInfo[]; oldMembers?: PeerInfo[] },
        meta?: CommandMeta,
    ): Promise<ApplyResult<T>> {
        // A configuration's target set C-new must never be empty: an empty config
        // makes its majority vacuously satisfiable (see `majorityOf`), which would
        // silently drop the dual-majority requirement and let a single side decide.
        if (command.members.length === 0) {
            return Promise.reject(new MembershipError('a configuration must have at least one member'));
        }
        const entry: LogEntry<C> = { term: this.currentTerm, command, meta };
        this.log.push(entry);
        // Adopt the new configuration the instant it is in the log (Raft §6).
        this.recomputeMembers();
        this.persist();
        const index = this.lastLogIndex();
        this.matchIndex.set(this.id, index);
        this.nextIndex.set(this.id, index + 1);
        this.logger?.info('membership change proposed', {
            index,
            members: command.members.map((m) => m.id),
            joint: command.oldMembers !== undefined,
        });

        const promise = new Promise<ApplyResult<T>>((resolve, reject) => {
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
     * Linearizable read barrier that can be satisfied on ANY node, so a strong
     * read arriving at a FOLLOWER is served LOCALLY rather than forwarded whole
     * to the leader (Raft §6.4 ReadIndex, follower read offloading).
     *
     * - On the leader, this is exactly {@link readBarrier} (unchanged path).
     * - On a follower, it obtains a confirmed `readIndex` from the leader via the
     *   {@link ReadIndexArgs} RPC, then waits until this node has APPLIED through
     *   that index. Only then may the caller serve from this follower's local
     *   state, guaranteeing the read reflects every write committed before the
     *   request.
     *
     * Fails closed: any uncertainty (no known leader, RPC lost, the leader can't
     * confirm a quorum or reports a higher term, or the local apply times out)
     * throws {@link NotLeaderError} (or a barrier-timeout error), so the caller
     * forwards/421s instead of serving a possibly-stale value. A candidate (no
     * known leader) likewise throws.
     */
    async readBarrierLocal(): Promise<void> {
        if (this.role === 'leader') return this.readBarrier();

        // Follower (or candidate): we need a confirmed read index from the leader.
        const leaderId = this.leaderId;
        if (!leaderId || leaderId === this.id) throw new NotLeaderError(leaderId);
        const leader = this.members.get(leaderId);
        if (!leader) throw new NotLeaderError(leaderId);

        const term = this.currentTerm;
        const reply = await this.transport.sendReadIndex(leader, { term });
        // No reply / leader couldn't confirm / a newer term exists — fail closed.
        if (!reply || !reply.success || reply.readIndex === undefined) {
            if (reply && reply.term > this.currentTerm) this.becomeFollower(reply.term);
            throw new NotLeaderError(this.leaderId);
        }
        if (reply.term > this.currentTerm) {
            // The leader has moved to a newer term: our view is stale, don't serve.
            this.becomeFollower(reply.term);
            throw new NotLeaderError(this.leaderId);
        }
        // A `reply.term < currentTerm` with success can't be unsafe: handleReadIndex
        // steps down (success:false) when the requester's term is higher than its
        // own, so a success here means the leader confirmed a quorum at a term >=
        // ours — its `readIndex` is a valid lower bound on writes committed before
        // this read. No special handling needed for the strict-less-than case.

        this.metrics?.raftFollowerReads.inc({ node: this.id });
        // Block until THIS node has applied through the confirmed read index, so
        // the subsequent local read can't observe state older than the barrier.
        await this.waitForApplied(reply.readIndex);
    }

    /**
     * Leader half of the ReadIndex protocol, exposed as an RPC for follower read
     * offloading. If we are not the leader, fail closed (`success: false`) so the
     * follower forwards. Otherwise capture `readIndex = commitIndex` and run the
     * EXISTING heartbeat-quorum {@link confirmLeadership} round; only on a
     * confirmed quorum (and still leader at the same term) do we return the index.
     * This is precisely the leader half of {@link readBarrier} — the leader's own
     * `readBarrier` semantics are unchanged.
     */
    async handleReadIndex(args: ReadIndexArgs): Promise<ReadIndexReply> {
        // A requester at a higher term means a newer term exists somewhere: step
        // down (we cannot be a legitimate leader) and refuse to vouch for an index.
        if (args.term > this.currentTerm) {
            this.becomeFollower(args.term);
            return { term: this.currentTerm, success: false };
        }
        if (this.role !== 'leader') return { term: this.currentTerm, success: false };
        const readIndex = this.commitIndex;
        const term = this.currentTerm;
        const confirmed = await this.confirmLeadership();
        if (!confirmed || this.role !== 'leader' || this.currentTerm !== term) {
            return { term: this.currentTerm, success: false };
        }
        this.metrics?.raftReadBarriers.inc({ node: this.id });
        return { term: this.currentTerm, success: true, readIndex };
    }

    /**
     * Exchange one round of AppendEntries with the peers and report whether a
     * majority still acknowledge our leadership for the current term. A reply
     * that doesn't carry a higher term counts as an acknowledgement even if it
     * reports a log mismatch — the follower still recognises us as leader.
     */
    private async confirmLeadership(): Promise<boolean> {
        // Single-node config (no peers in any voting set) is its own majority.
        if (this.otherMembers().length === 0) return this.inMajority([this.id]);
        const term = this.currentTerm;
        const peers = this.otherMembers();
        const acks = await Promise.all(peers.map((peer) => this.replicateTo(peer)));
        if (this.role !== 'leader' || this.currentTerm !== term) return false;
        // Tally the set of ids that acknowledged our leadership (plus self), then
        // require a DUAL majority during a joint transition (Raft §6 / ADR-0022):
        // a single-config majority must never confirm a read on its own.
        const confirmed = new Set<string>([this.id]);
        peers.forEach((peer, i) => {
            if (acks[i]) confirmed.add(peer.id);
        });
        return this.inMajority(confirmed);
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
        this.snapshotInFlight.clear();
        for (const peer of this.otherMembers()) {
            this.nextIndex.set(peer.id, nextIdx);
            this.matchIndex.set(peer.id, 0);
        }
        this.logger?.info('became LEADER', { term: this.currentTerm });
        // If we inherited an in-progress joint configuration (a previous leader
        // appended the joint C-old,new but crashed before installing the final
        // C-new), remember to complete that transition once it commits (Raft §4.3;
        // see `applyCommitted`). A leader that originates a change is NOT joint at
        // election, so this is false for those.
        this.inheritedJoint = this.isJoint();
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

        // Tally granting voter ids (vote for self) and decide via the dual-majority
        // predicate, so during a joint transition a candidate that can only reach a
        // majority of ONE configuration can never win (Raft §6 / ADR-0022).
        const votes = new Set<string>([this.id]);
        if (this.inMajority(votes)) {
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
                    votes.add(peer.id);
                    if (this.role === 'candidate' && this.inMajority(votes)) {
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
            // Skip if a snapshot stream to this peer is already in progress, so a
            // heartbeat can't start an interleaving second stream from offset 0.
            if (this.snapshotInFlight.has(peer.id)) return true;
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
            // Log inconsistency: use the follower's conflict hint to skip a whole
            // term per round trip (accelerated backtracking), falling back to a
            // single-step decrement. May ultimately fall through to a snapshot.
            let next: number;
            if (reply.conflictTerm !== undefined) {
                const lastWithTerm = this.lastIndexOfTerm(reply.conflictTerm);
                next = lastWithTerm !== undefined ? lastWithTerm + 1 : reply.conflictIndex ?? nextIdx - 1;
            } else if (reply.conflictIndex !== undefined) {
                next = reply.conflictIndex;
            } else {
                next = nextIdx - 1;
            }
            this.nextIndex.set(peer.id, Math.max(1, Math.min(next, this.lastLogIndex() + 1)));
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
        const lastIncludedTerm = durable?.lastIncludedTerm ?? this.lastIncludedTerm;
        // The config at the snapshot point — may be joint; carry both sets so the
        // follower reconstructs the joint config and keeps enforcing dual majority.
        // Prefer the durable snapshot's config; fall back to the live config at the
        // snapshot point rather than an empty set (an empty config a follower adopted
        // as its base would be constraint-free — see `majorityOf`).
        const cfg: ConfigState = durable && durable.members
            ? { members: durable.members, oldMembers: durable.oldMembers }
            : this.configAt(snapIndex);
        const members = cfg.members;
        const oldMembers = cfg.oldMembers;
        const data = durable?.data ?? this.rsm.snapshot();

        // Serialize the snapshot's data ONCE, then stream byte/character slices.
        // Reassembled on the follower this yields a byte-identical string, so
        // JSON.parse recovers exactly the durable snapshot object.
        const serialized = JSON.stringify(data);
        const chunkSize = this.snapshotChunkBytes;
        this.logger?.info('sending snapshot', {
            peer: peer.id,
            lastIncludedIndex: snapIndex,
            bytes: serialized.length,
        });

        // Send chunks SEQUENTIALLY, awaiting each reply before the next so the
        // follower reassembles in order. A snapshot whose serialized data fits in
        // one chunk sends exactly one `done` chunk (parity with the single RPC).
        // `snapshotInFlight` prevents a heartbeat from launching a second,
        // interleaving stream to the same peer while this one is mid-flight.
        this.snapshotInFlight.add(peer.id);
        try {
            let offset = 0;
            do {
                const slice = serialized.slice(offset, offset + chunkSize);
                const done = offset + slice.length >= serialized.length;
                const args: InstallSnapshotArgs = {
                    term,
                    leaderId: this.id,
                    lastIncludedIndex: snapIndex,
                    lastIncludedTerm,
                    members,
                    oldMembers,
                    offset,
                    data: slice,
                    done,
                };

                const reply = await this.transport.sendInstallSnapshot(peer, args);
                // Abort if the reply is lost, we lost leadership, or the term moved
                // on mid-stream — leader (or its successor) restarts from offset 0.
                if (!reply || this.role !== 'leader' || this.currentTerm !== term) return false;
                if (reply.term > this.currentTerm) {
                    this.becomeFollower(reply.term);
                    return false;
                }

                if (done) {
                    this.matchIndex.set(peer.id, snapIndex);
                    this.nextIndex.set(peer.id, snapIndex + 1);
                    this.advanceCommitIndex();
                    return true;
                }
                offset += slice.length;
            } while (true);
        } finally {
            this.snapshotInFlight.delete(peer.id);
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
        this.lastLeaderContact = Date.now();
        this.resetElectionTimer();

        // Everything up to our snapshot is already durably applied.
        if (args.prevLogIndex < this.lastIncludedIndex) {
            return { term: this.currentTerm, success: true, matchIndex: this.lastLogIndex() };
        }

        // Consistency check on the entry preceding the new ones. On failure, return
        // an accelerated-backtracking hint so the leader can skip a whole term.
        const prevTerm = this.termAt(args.prevLogIndex);
        if (prevTerm === undefined) {
            // Our log is too short to reach prevLogIndex.
            return { term: this.currentTerm, success: false, conflictIndex: this.lastLogIndex() + 1 };
        }
        if (prevTerm !== args.prevLogTerm) {
            // We have a conflicting term here — report it and where it first appears.
            return {
                term: this.currentTerm,
                success: false,
                conflictTerm: prevTerm,
                conflictIndex: this.firstIndexOfTerm(args.prevLogIndex, prevTerm),
            };
        }

        // Append new entries, overwriting any conflicting suffix. Entries arrive
        // as JSON over the (untyped) transport, so cast them back to this node's
        // application command type at the boundary.
        let p = this.pos(args.prevLogIndex + 1);
        let mutated = false;
        for (const entry of args.entries as LogEntry<C>[]) {
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
            this.snapshotBuffer = null;
            return { term: this.currentTerm };
        }

        // --- Chunk reassembly (Raft figure 13) ---
        // A new offset===0 chunk starts (or restarts) a buffer, superseding any
        // partial stream. Subsequent chunks must contiguously extend the buffer for
        // the SAME snapshot identity; any gap, mismatched identity, or out-of-order
        // offset is rejected — we drop the buffer and reply success without
        // installing, so the leader retries cleanly from offset 0 (fail closed:
        // never install a corrupt, partially-reassembled snapshot).
        // The buffer is bounded by the size of one legitimate snapshot: under the
        // CFT (non-Byzantine) trust model the leader is honest, so it always sends
        // `done` and never streams unbounded chunks. A Byzantine leader is out of
        // scope (ADR-0021).
        if (args.offset === 0) {
            this.snapshotBuffer = {
                lastIncludedIndex: args.lastIncludedIndex,
                lastIncludedTerm: args.lastIncludedTerm,
                nextOffset: 0,
                data: '',
            };
        }
        const buf = this.snapshotBuffer;
        if (
            !buf ||
            buf.lastIncludedIndex !== args.lastIncludedIndex ||
            buf.lastIncludedTerm !== args.lastIncludedTerm ||
            buf.nextOffset !== args.offset
        ) {
            // Out-of-order / mismatched chunk: discard the partial buffer.
            this.snapshotBuffer = null;
            return { term: this.currentTerm };
        }
        buf.data += args.data;
        buf.nextOffset += args.data.length;

        // A non-final chunk is acked without installing.
        if (!args.done) {
            return { term: this.currentTerm };
        }

        // Final chunk: reassembly complete. Parse the full string back into the
        // snapshot data object. A corrupt reassembly (which JSON.parse rejects)
        // resets the buffer and acks without installing, so the leader retries.
        let snapshotData: RsmSnapshot<T>;
        try {
            snapshotData = JSON.parse(buf.data) as RsmSnapshot<T>;
        } catch {
            this.snapshotBuffer = null;
            return { term: this.currentTerm };
        }
        this.snapshotBuffer = null;

        this.rsm.restore(snapshotData);

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
        // Adopt the configuration carried with the snapshot as the new base config,
        // preserving a joint config if the snapshot boundary fell inside one.
        if (args.members) this.baseConfig = { members: args.members, oldMembers: args.oldMembers };
        this.recomputeMembers();

        // Persist snapshot before the compacted log (same crash-ordering as takeSnapshot).
        this.storage.saveSnapshot({
            lastIncludedIndex: this.lastIncludedIndex,
            lastIncludedTerm: this.lastIncludedTerm,
            members: args.members,
            oldMembers: args.oldMembers,
            data: snapshotData,
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
            // The set of voting members (incl. self) that have replicated through n.
            // An index is committed iff that set forms a DUAL majority during a joint
            // transition (Raft §6 / ADR-0022) — i.e. it never commits on a single
            // configuration's majority alone.
            const acked = new Set<string>();
            for (const id of this.members.keys()) {
                if ((this.matchIndex.get(id) ?? 0) >= n) acked.add(id);
            }
            if (this.inMajority(acked)) {
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
            const result = this.rsm.apply(this.lastApplied, entry);

            const waiter = this.pending.get(this.lastApplied);
            if (waiter) {
                this.pending.delete(this.lastApplied);
                if (waiter.term === entry.term) waiter.resolve(result);
                else waiter.reject(new NotLeaderError(this.leaderId));
            }

            // Fan the committed entry out to read-stream subscribers (ADR-0023).
            // This is the live-tail feed an edge replica consumes after bootstrap.
            this.emitCommitted(this.lastApplied, entry);
        }
        this.resolveReadWaiters();
        // Complete an INHERITED joint transition (Raft §4.3): a previous leader
        // appended the joint C-old,new but crashed before installing the final
        // C-new. Once we (the new leader) have COMMITTED that joint config (it is
        // joint and no CONFIG entry is still uncommitted), append the final C-new so
        // the cluster doesn't stay wedged in joint consensus. `changeMembership`
        // finalizes the transitions it originates itself, so `inheritedJoint` gates
        // this to inherited ones only and is cleared so it fires at most once.
        if (this.role === 'leader' && this.inheritedJoint && this.isJoint() && !this.hasUncommittedConfig()) {
            this.inheritedJoint = false;
            const cNew = [...this.configNew]
                .map((id) => this.members.get(id))
                .filter((p): p is PeerInfo => p !== undefined);
            this.logger?.info('completing inherited joint transition', { members: cNew.map((m) => m.id) });
            void this.submitConfigEntry({ type: 'CONFIG', members: cNew }).catch(() => undefined);
        }
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
        const data = this.rsm.snapshot();
        // Capture the configuration at the snapshot point before compacting — the
        // CONFIG entries that produced it are about to be discarded. This may be a
        // JOINT config (the snapshot boundary fell inside a transition): preserve
        // both sets so the compacted base still enforces dual majority (ADR-0022).
        const cfg = this.configAt(snapIndex);

        // Keep the sentinel + everything after the snapshot point.
        const tail = this.log.slice(this.pos(snapIndex) + 1);

        // Order matters for crash safety: persist the snapshot FIRST, then the
        // compacted log. That way the durable log base can only ever lag the
        // snapshot (reconciled on restart), never lead it (which would lose the
        // state the discarded entries produced).
        this.storage.saveSnapshot({
            lastIncludedIndex: snapIndex,
            lastIncludedTerm: snapTerm,
            members: cfg.members,
            oldMembers: cfg.oldMembers,
            data,
        });

        this.log = [{ term: snapTerm, command: { type: 'NOOP' } }, ...tail];
        this.lastIncludedIndex = snapIndex;
        this.lastIncludedTerm = snapTerm;
        this.baseConfig = cfg;
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
        m.raftDedupCacheSize.set(this.rsm.dedupCacheSize(), { node });
        m.raftClusterSize.set(this.members.size, { node });
        m.raftStreamSubscribers.set(this.committedListeners.size, { node });
        m.stateMachineEntries.set(this.rsm.size(), { node });
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

    // ---- committed-log read stream (ADR-0023: edge read replicas) ----
    //
    // A read-only, eventually-consistent view of the committed log, served from
    // THIS node's local state (ADR-0006). Any node — leader or follower — can
    // serve it, so read serving fans out across the cluster (and, via a browser
    // SDK, past it). A consumer bootstraps from the snapshot boundary, replays the
    // already-committed tail, then live-tails new commits via `onCommitted`. None
    // of this participates in consensus: a stream consumer is a non-voting learner.

    /** Highest committed index (the live tail's current head). */
    getCommitIndex(): number {
        return this.commitIndex;
    }

    /** The snapshot boundary: entries at or below this live only in the snapshot. */
    getSnapshotIndex(): number {
        return this.lastIncludedIndex;
    }

    /**
     * True iff an entry the consumer still needs (everything after `afterIndex`)
     * has already been compacted into the snapshot — so the consumer must first
     * bootstrap from {@link getStreamSnapshot} before it can tail by entry.
     */
    needsSnapshot(afterIndex: number): boolean {
        return afterIndex < this.lastIncludedIndex;
    }

    /**
     * Committed entries with absolute index in `(afterIndex, commitIndex]` that are
     * still in the in-memory log — the catch-up batch a freshly-connected consumer
     * replays before live-tailing. Entries at or below the snapshot boundary are
     * skipped (the consumer gets those from {@link getStreamSnapshot}).
     */
    getCommittedEntries(afterIndex: number): { index: number; entry: LogEntry<C> }[] {
        const start = Math.max(afterIndex, this.lastIncludedIndex) + 1;
        const out: { index: number; entry: LogEntry<C> }[] = [];
        for (let i = start; i <= this.commitIndex; i++) {
            const entry = this.entryAt(i);
            if (entry) out.push({ index: i, entry });
        }
        return out;
    }

    /**
     * The durable snapshot at the compaction boundary, for stream bootstrap, or
     * `null` if nothing has been compacted yet (the whole committed log is still
     * in memory, so a consumer can tail from index 1 with no snapshot). The `data`
     * is the replicated state machine's snapshot (`{ state, audit, … }`); an edge
     * replica restores its local application state from `data.state`.
     */
    getStreamSnapshot(): { lastIncludedIndex: number; lastIncludedTerm: number; members: PeerInfo[]; data: unknown } | null {
        if (this.lastIncludedIndex === 0) return null;
        const durable = this.storage.loadSnapshot();
        if (!durable) return null;
        return {
            lastIncludedIndex: durable.lastIncludedIndex,
            lastIncludedTerm: durable.lastIncludedTerm,
            members: durable.members ?? [...this.members.values()],
            data: durable.data,
        };
    }

    /**
     * Subscribe to committed entries as they are applied, in commit order, for
     * live-tailing the read stream (ADR-0023). Returns an unsubscribe function.
     * The subscriber is a read-only, non-voting observer: it never acks, votes, or
     * affects commit/quorum. Listener exceptions are isolated so one slow/broken
     * consumer can't wedge the apply loop or the other subscribers.
     */
    onCommitted(listener: (index: number, entry: LogEntry<C>) => void): () => void {
        this.committedListeners.add(listener);
        return () => {
            this.committedListeners.delete(listener);
        };
    }

    private emitCommitted(index: number, entry: LogEntry<C>): void {
        if (this.committedListeners.size === 0) return;
        for (const listener of this.committedListeners) {
            try {
                listener(index, entry);
            } catch (err) {
                this.logger?.warn('committed-stream listener threw', {
                    error: err instanceof Error ? err.message : String(err),
                });
            }
        }
    }
}
