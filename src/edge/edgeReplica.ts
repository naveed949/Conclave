import { ReplicatedStateMachine, RsmSnapshot } from '../consensus/replicatedStateMachine';
import { StateMachine } from '../consensus/stateMachine';
import { AppCommand, AuditEntry, isControlCommand, LogEntry } from '../consensus/types';
import { LogStreamSource, StreamEntry, StreamSnapshot } from './types';

export interface EdgeReplicaOptions<C extends AppCommand, T> {
    /**
     * The application state machine to feed committed commands into — the SAME
     * deterministic `StateMachine` implementation a server node runs (ADR-0017),
     * so the replica converges to identical state. Reads are served from it.
     */
    app: StateMachine<C, T>;
    /** How to open a committed-log stream (Node http, browser EventSource, …). */
    source: LogStreamSource<C>;
    /** Resume from just after this index (e.g. a persisted cursor). Default 0. */
    fromIndex?: number;
    /** Reconnect backoff bounds (ms). Defaults: 250 / 5000. */
    reconnectMinMs?: number;
    reconnectMaxMs?: number;
    /** Optional structured log sink. */
    logger?: (msg: string, meta?: Record<string, unknown>) => void;
    /**
     * Opt in to **client-side audit-chain verification** (M28, ADR-0023). When set,
     * the replica applies committed entries through a {@link ReplicatedStateMachine}
     * (wrapping `app`), re-deriving the tamper-evident SHA-256 audit hash-chain the
     * server maintains — so it can prove the served history is internally consistent
     * via {@link EdgeReplica.verifyAudit}.
     *
     * Requires the FULL (unfiltered) stream: the bootstrap snapshot must carry the
     * audit data. On a SCOPED stream the audit is stripped (the uniform-log-vs-authz
     * tension), so verification becomes **unavailable** — {@link EdgeReplica.verifyAudit}
     * returns `null` rather than falsely reporting success. Node-only: the chain uses
     * Node `crypto` (sync); browser verification needs async WebCrypto (future work).
     */
    verifyAudit?: boolean;
}

/**
 * Internal seam isolating HOW committed entries land in local state, so the
 * audit-verifying mode is a clean swap of strategy rather than conditionals
 * scattered through the stream-handling code.
 */
interface Applier<C extends AppCommand, T> {
    /** Restore from a bootstrap snapshot payload (the `data` of a `snapshot` event). */
    restore(data: unknown): void;
    /** Apply one committed entry at its absolute index. */
    applyEntry(index: number, entry: LogEntry<C>): void;
}

/** Default applier: today's behavior — apply commands to the bare application SM. */
class DirectApplier<C extends AppCommand, T> implements Applier<C, T> {
    constructor(private readonly app: StateMachine<C, T>) {}

    restore(data: unknown): void {
        // The stream carries the replicated-state-machine snapshot; an edge replica
        // only needs the application state slice to restore its local view.
        const obj = data as { state?: unknown } | undefined;
        const state = obj && typeof obj === 'object' && 'state' in obj ? obj.state : data;
        this.app.restore(state);
    }

    applyEntry(_index: number, entry: LogEntry<C>): void {
        // Control commands (NOOP/CONFIG) have no application effect — advance past
        // them just like the server's replicated state machine does.
        if (!isControlCommand(entry.command)) {
            this.app.apply(entry.command as C);
        }
    }
}

/**
 * Audit-verifying applier: wraps the same `app` in a {@link ReplicatedStateMachine}
 * so applying re-derives the audit hash-chain. Reads still come from `app` (the
 * RSM delegates `apply` to it), so `replica.app` stays correct.
 *
 * `available` is false when bootstrapped from a snapshot WITHOUT audit data (a
 * scoped/partial stream): the chain cannot be rebuilt, so verification is reported
 * unavailable rather than silently "valid".
 */
class AuditingApplier<C extends AppCommand, T> implements Applier<C, T> {
    readonly rsm: ReplicatedStateMachine<C, T>;
    available = true;

    constructor(app: StateMachine<C, T>) {
        this.rsm = new ReplicatedStateMachine<C, T>(app);
    }

    restore(data: unknown): void {
        const obj = data as Partial<RsmSnapshot<T>> | undefined;
        // A full-stream snapshot carries `audit` + `lastHash`; a scoped one is just
        // `{ state }`. Without the audit we cannot rebuild/verify the chain.
        if (obj && typeof obj === 'object' && 'audit' in obj && 'lastHash' in obj) {
            this.rsm.restore(obj as RsmSnapshot<T>);
            this.available = true;
        } else {
            // Restore application state only (so reads still work) and mark
            // verification unavailable.
            const state = obj && typeof obj === 'object' && 'state' in obj ? obj.state : data;
            this.rsm.application.restore(state);
            this.available = false;
        }
    }

    applyEntry(index: number, entry: LogEntry<C>): void {
        // `apply` chains the audit hash for non-NOOP entries and delegates the
        // application effect to the wrapped SM — exactly what the server does.
        this.rsm.apply(index, entry);
    }
}

/**
 * A read-only, non-voting replica of the application state machine, kept current
 * by tailing a node's committed-log stream (ADR-0023). It bootstraps from the
 * snapshot boundary, applies committed commands to a local `StateMachine`, and
 * serves reads from that local state with **no network round-trip** — ideal for
 * read-heavy, reactive UIs. It never writes, votes, or acks; writes still go
 * through the authenticated leader path.
 *
 * Consistency is eventual by default. {@link waitForIndex} provides read-your-
 * writes: after a write commits at index *i* (the leader returns it), await the
 * replica catching up through *i* before reading the affected view.
 *
 * Environment-agnostic: give it a {@link LogStreamSource} for your runtime. The
 * replica owns reconnection with backoff and resumes from its applied index, so a
 * dropped connection (or a compaction that outran it) transparently re-bootstraps.
 */
export class EdgeReplica<C extends AppCommand, T = unknown> {
    /** The local application state machine — read from it directly (e.g. `app.getAll()`). */
    readonly app: StateMachine<C, T>;

    private readonly applier: Applier<C, T>;
    private readonly auditing: AuditingApplier<C, T> | null;
    private readonly source: LogStreamSource<C>;
    private readonly reconnectMinMs: number;
    private readonly reconnectMaxMs: number;
    private readonly logger?: (msg: string, meta?: Record<string, unknown>) => void;

    private appliedIndex: number;
    private caughtUp = false;
    private running = false;
    private closeConn: (() => void) | null = null;
    private reconnectTimer: ReturnType<typeof setTimeout> | null = null;
    private attempt = 0;

    private readonly changeListeners = new Set<() => void>();
    private indexWaiters: {
        index: number;
        resolve: () => void;
        reject: (err: Error) => void;
        timer: ReturnType<typeof setTimeout>;
    }[] = [];

    constructor(opts: EdgeReplicaOptions<C, T>) {
        this.app = opts.app;
        if (opts.verifyAudit) {
            this.auditing = new AuditingApplier<C, T>(opts.app);
            this.applier = this.auditing;
        } else {
            this.auditing = null;
            this.applier = new DirectApplier<C, T>(opts.app);
        }
        this.source = opts.source;
        this.appliedIndex = opts.fromIndex ?? 0;
        this.reconnectMinMs = opts.reconnectMinMs ?? 250;
        this.reconnectMaxMs = opts.reconnectMaxMs ?? 5000;
        this.logger = opts.logger;
    }

    /** Begin tailing the stream (idempotent). */
    start(): void {
        if (this.running) return;
        this.running = true;
        this.connect();
    }

    /** Stop tailing and reject any pending read-your-writes barriers. */
    stop(): void {
        this.running = false;
        if (this.reconnectTimer) {
            clearTimeout(this.reconnectTimer);
            this.reconnectTimer = null;
        }
        if (this.closeConn) {
            this.closeConn();
            this.closeConn = null;
        }
        this.rejectWaiters(new Error('edge replica stopped'));
    }

    /** Highest committed index this replica has applied. */
    lastIndex(): number {
        return this.appliedIndex;
    }

    /** True once the replica has replayed through the live head at least once. */
    isCaughtUp(): boolean {
        return this.caughtUp;
    }

    /**
     * Subscribe to local state changes (a built-in change-feed: the replica
     * already tails the log, so a reactive UI re-renders with no polling).
     * Returns an unsubscribe function.
     */
    onChange(listener: () => void): () => void {
        this.changeListeners.add(listener);
        return () => {
            this.changeListeners.delete(listener);
        };
    }

    /**
     * Resolve once this replica has applied through `index` — the read-your-writes
     * primitive. After a write commits at index *i*, `await replica.waitForIndex(i)`
     * before serving the affected view so the user sees their own change. Rejects
     * on timeout (default 5s) or if the replica is stopped.
     */
    waitForIndex(index: number, timeoutMs = 5000): Promise<void> {
        if (this.appliedIndex >= index) return Promise.resolve();
        return new Promise<void>((resolve, reject) => {
            const timer = setTimeout(() => {
                this.indexWaiters = this.indexWaiters.filter((w) => w.timer !== timer);
                reject(new Error('WAIT_FOR_INDEX_TIMEOUT'));
            }, timeoutMs);
            this.indexWaiters.push({ index, resolve, reject, timer });
        });
    }

    // ---- audit verification (M28, ADR-0023) ----

    /**
     * Recompute the audit hash-chain over the history this replica rebuilt and
     * report whether it is intact — end-to-end tamper-evidence for the served log.
     *
     * Returns `null` when verification is **unavailable**: either the replica is not
     * in auditing mode (`verifyAudit` was not set), or it bootstrapped from a SCOPED
     * snapshot that carried no audit data (the uniform-log-vs-authz tension). A
     * scoped client therefore gets `null`, never a false `{ valid: true }`.
     */
    verifyAudit(): { valid: boolean; brokenAt?: number; length: number } | null {
        if (!this.auditing || !this.auditing.available) return null;
        return this.auditing.rsm.verifyAudit();
    }

    /**
     * The current head of the rebuilt audit hash-chain (the last entry's hash), or
     * `null` if verification is unavailable. Equals the server's audit head once the
     * replica has caught up — proving it re-derived the SAME chain the node holds.
     */
    auditHead(): string | null {
        if (!this.auditing || !this.auditing.available) return null;
        const log = this.auditing.rsm.getAuditLog();
        return log.length > 0 ? log[log.length - 1].hash : null;
    }

    /** The rebuilt audit entries, or `null` if verification is unavailable. */
    getAuditLog(): AuditEntry[] | null {
        if (!this.auditing || !this.auditing.available) return null;
        return this.auditing.rsm.getAuditLog();
    }

    // ---- connection lifecycle ----

    private connect(): void {
        if (!this.running) return;
        this.closeConn = this.source.connect(this.appliedIndex, {
            onOpen: () => {
                this.attempt = 0;
            },
            onSnapshot: (snap) => this.handleSnapshot(snap),
            onEntry: (item) => this.handleEntry(item),
            onCaughtUp: (idx) => {
                if (!this.caughtUp) this.caughtUp = true;
                // The server has streamed every in-scope entry through `idx`, so we
                // are current through that absolute index even if some were filtered
                // out — advance the cursor (efficient resume + read-your-writes).
                if (idx > this.appliedIndex) this.appliedIndex = idx;
                this.logger?.('edge replica caught up', { index: idx });
                this.afterApply();
            },
            onError: (err) => {
                this.logger?.('edge stream error', { error: err.message });
                this.scheduleReconnect();
            },
        });
    }

    private scheduleReconnect(): void {
        if (!this.running || this.reconnectTimer) return;
        if (this.closeConn) {
            this.closeConn();
            this.closeConn = null;
        }
        // Exponential backoff with full jitter, capped — resumes from appliedIndex.
        const ceiling = Math.min(this.reconnectMaxMs, this.reconnectMinMs * 2 ** this.attempt);
        const delay = this.reconnectMinMs + Math.floor(Math.random() * Math.max(1, ceiling - this.reconnectMinMs));
        this.attempt += 1;
        this.reconnectTimer = setTimeout(() => {
            this.reconnectTimer = null;
            this.connect();
        }, delay);
    }

    // ---- applying the stream ----

    private handleSnapshot(snap: StreamSnapshot): void {
        // The applier owns how the snapshot lands: the default takes the application
        // state slice; the auditing one restores the full RSM snapshot (state + audit
        // + lastHash) so it can rebuild and verify the hash-chain.
        this.applier.restore(snap.data);
        this.appliedIndex = snap.lastIncludedIndex;
        this.logger?.('edge replica bootstrapped from snapshot', { index: snap.lastIncludedIndex });
        this.afterApply();
    }

    private handleEntry(item: StreamEntry<C>): void {
        // Idempotent + monotonic: ignore anything at or below our cursor (a reconnect
        // replay). We do NOT require strictly contiguous indices: a scoped stream
        // legitimately skips out-of-scope entries (ADR-0023 partial replication), and
        // SSE rides one ordered TCP connection so an in-scope entry is never dropped
        // mid-stream. The cursor simply advances to each entry the server sends us.
        if (item.index <= this.appliedIndex) return;

        // The applier knows whether to skip control commands and whether to chain the
        // audit hash; pass the absolute index so the auditing applier can record it.
        this.applier.applyEntry(item.index, item.entry);
        this.appliedIndex = item.index;
        this.afterApply();
    }

    private afterApply(): void {
        this.resolveWaiters();
        this.notifyChange();
    }

    private notifyChange(): void {
        for (const listener of this.changeListeners) {
            try {
                listener();
            } catch (err) {
                this.logger?.('edge change listener threw', {
                    error: err instanceof Error ? err.message : String(err),
                });
            }
        }
    }

    private resolveWaiters(): void {
        if (this.indexWaiters.length === 0) return;
        const remaining: typeof this.indexWaiters = [];
        for (const w of this.indexWaiters) {
            if (this.appliedIndex >= w.index) {
                clearTimeout(w.timer);
                w.resolve();
            } else {
                remaining.push(w);
            }
        }
        this.indexWaiters = remaining;
    }

    private rejectWaiters(err: Error): void {
        const waiters = this.indexWaiters;
        this.indexWaiters = [];
        for (const w of waiters) {
            clearTimeout(w.timer);
            w.reject(err);
        }
    }
}
