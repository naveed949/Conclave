import { ReplicatedStateMachine } from './replicatedStateMachine';
import { StateMachine } from './stateMachine';
import { AppCommand, ApplyResult, CommandMeta, PeerInfo } from './types';

/**
 * The ordering/commit seam ABOVE the log (ADR-0021).
 *
 * This is exactly the application-facing contract the rest of the system depends
 * on from a consensus node: propose a command and learn the committed, ordered,
 * applied result (`submit`); read linearizably (`readBarrier`); change cluster
 * membership; and inspect leadership/membership status. It is the
 * "commit-ordered-log contract" — everything in `src/runtime/`, the HTTP
 * controllers, and the audit routes depend only on *this*, never on Raft
 * internals.
 *
 * Naming this seam is the prerequisite ADR-0021 identifies for a future
 * Byzantine-Fault-Tolerant (BFT) engine: such an engine would implement
 * `Consensus` and the controllers/audit routes would not change. Crucially, the
 * Raft RPCs (`RpcHandler` in `transport.ts` — `handleRequestVote` /
 * `handleAppendEntries` / `handleInstallSnapshot`) are the SEPARATE,
 * protocol-specific surface: a BFT protocol uses an entirely different
 * multi-phase, signed message exchange, so those RPCs are intentionally NOT part
 * of this interface. The `Transport`/`RpcHandler` boundary is replaced
 * *differently* (and not 1:1) by a BFT engine; this `Consensus` boundary is the
 * one the application rides unchanged.
 *
 * @typeParam C - the application's command union (each a string-`type` object).
 * @typeParam T - the payload type carried back in {@link ApplyResult}.
 * @typeParam A - the concrete application state machine type, so consumers can
 *   reach domain reads via {@link Consensus.app} (e.g. `node.app.host`).
 */
export interface Consensus<C extends AppCommand, T = unknown, A = unknown> {
    /** The application state machine plugged into this node (for domain reads). */
    readonly app: A;

    /** The replicated state machine wrapper (audit, idempotency, snapshots, size). */
    get stateMachine(): ReplicatedStateMachine<C, T>;

    /**
     * Propose a command. Resolves once the entry is committed and applied, and
     * rejects with a `NotLeaderError` if this node is not the leader.
     */
    submit(command: C, meta?: CommandMeta): Promise<ApplyResult<T>>;

    /**
     * Linearizable read barrier (Raft §6.4, "ReadIndex"): resolving it guarantees
     * a subsequent local read reflects every write committed before the barrier
     * was requested. Rejects with a `NotLeaderError` if this node is not (or
     * ceases to be) the leader, so the caller can forward the read like a write.
     */
    readBarrier(): Promise<void>;

    /**
     * Linearizable read barrier that can be satisfied on ANY node (Raft §6.4
     * follower read offloading). On the leader it is exactly {@link readBarrier};
     * on a follower it obtains a confirmed ReadIndex from the leader and waits
     * until this node has applied through it, so the caller may then serve the
     * read from THIS node's local state. Rejects with `NotLeaderError` if no
     * confirmed read index can be obtained (no leader / RPC fails / leader can't
     * confirm a quorum), so the caller can forward the read like a write.
     */
    readBarrierLocal(): Promise<void>;

    /**
     * Add or remove a single voting member (one change at a time). The returned
     * promise resolves once the change commits; leader-only.
     */
    changeMembership(change: { add?: PeerInfo; remove?: string }, meta?: CommandMeta): Promise<ApplyResult<T>>;

    /** Whether this node currently believes itself to be the leader. */
    isLeader(): boolean;

    /** The current leader's id, or null if unknown. */
    getLeaderId(): string | null;

    /** URL of the current leader (for write forwarding), or null if unknown/self. */
    getLeaderUrl(): string | null;

    /** A status view of this node (role, term, indices, membership). */
    status(): {
        id: string;
        role: string;
        term: number;
        leaderId: string | null;
        lastLogIndex: number;
        logEntries: number;
        snapshotIndex: number;
        commitIndex: number;
        lastApplied: number;
        stateSize: number;
        dedupCacheSize: number;
        members: string[];
    };

    /** The current cluster configuration (voting members, including self). */
    getMembers(): PeerInfo[];

    /** Begin participating in consensus (restores durable state, arms timers). */
    start(): void;

    /** Stop participating in consensus (clears timers, rejects in-flight work). */
    stop(): void;
}

/**
 * Convenience alias for a {@link Consensus} whose application is a concrete
 * {@link StateMachine}. Mirrors how `RaftNode<C, T, SM>` is parameterized, so a
 * consumer can write `ConsensusOf<BookCommand, Book, BookStateMachine>`.
 */
export type ConsensusOf<C extends AppCommand, T, SM extends StateMachine<C, T>> = Consensus<C, T, SM>;
