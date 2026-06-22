import { StateMachine } from '../consensus/stateMachine';
import { AppCommand, isControlCommand } from '../consensus/types';
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
                this.logger?.('edge replica caught up', { index: idx });
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
        // The stream carries the replicated-state-machine snapshot; an edge replica
        // only needs the application state slice to restore its local view.
        const data = snap.data as { state?: unknown } | undefined;
        const state = data && typeof data === 'object' && 'state' in data ? data.state : snap.data;
        this.app.restore(state);
        this.appliedIndex = snap.lastIncludedIndex;
        this.logger?.('edge replica bootstrapped from snapshot', { index: snap.lastIncludedIndex });
        this.afterApply();
    }

    private handleEntry(item: StreamEntry<C>): void {
        // Idempotent: ignore anything we've already applied (e.g. a reconnect replay).
        if (item.index <= this.appliedIndex) return;
        // A gap means we somehow skipped entries (dropped frame / bad resume): drop
        // the connection and reconnect from appliedIndex, which re-bootstraps cleanly.
        if (item.index !== this.appliedIndex + 1) {
            this.logger?.('edge stream gap; re-bootstrapping', {
                expected: this.appliedIndex + 1,
                got: item.index,
            });
            this.scheduleReconnect();
            return;
        }

        const command = item.entry.command;
        // Control commands (NOOP/CONFIG) have no application effect — advance past
        // them just like the server's replicated state machine does.
        if (!isControlCommand(command)) {
            this.app.apply(command as C);
        }
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
