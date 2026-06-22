import { createHash } from 'crypto';
import { auditEntryPayload, GENESIS_HASH } from './auditChain';
import { StateMachine } from './stateMachine';
import { AppCommand, ApplyResult, AuditEntry, LogEntry } from './types';

/** Default cap on the idempotency dedup cache (number of remembered requestIds). */
export const DEFAULT_DEDUP_LIMIT = 10_000;

/** Serializable snapshot of the full replicated state (app state + audit + dedup). */
export interface RsmSnapshot<T = unknown> {
    /** The application state machine's own serialized state. */
    state: unknown;
    audit: AuditEntry[];
    seen: [string, ApplyResult<T>][];
    lastHash: string;
}

/**
 * Wraps an application {@link StateMachine} with two cross-cutting, generic
 * backend concerns that every node gets for free because they ride the same
 * deterministic, replicated apply path — regardless of which application is
 * plugged in:
 *
 *  - **Audit**: an append-only, hash-chained record of every committed change.
 *    Replicated across the cluster and tamper-evident (alter one entry and the
 *    chain no longer verifies), so it doubles as a forgery-resistant history.
 *  - **Idempotency**: commands carry a requestId; a replayed requestId returns
 *    the original result without re-applying, turning at-least-once delivery
 *    into exactly-once effects (key for fault-tolerant client retries).
 *
 * The framework's control commands (`NOOP`, `CONFIG`) have no application
 * effect: they are not forwarded to the wrapped state machine (`NOOP` is also
 * kept out of the audit). Everything else is an application command, delegated
 * to `app.apply`.
 *
 * The dedup cache is bounded ({@link DEFAULT_DEDUP_LIMIT}) so it — and therefore
 * the snapshots it is folded into — cannot grow without limit. Eviction is
 * **insertion-order FIFO**, which is deterministic: every node applies the same
 * commands in the same order, so each evicts the same entries and replicas stay
 * identical. (A wall-clock TTL would *not* be deterministic and would diverge
 * the cluster.) The trade-off: a retry of a request older than the window is no
 * longer deduped and re-applies — acceptable, since realistic retries are recent.
 */
export class ReplicatedStateMachine<C extends AppCommand, T = unknown> {
    private readonly audit: AuditEntry[] = [];
    private readonly seen = new Map<string, ApplyResult<T>>();
    private lastHash = GENESIS_HASH;
    private readonly dedupLimit: number;

    constructor(
        private readonly app: StateMachine<C, T>,
        dedupLimit: number = DEFAULT_DEDUP_LIMIT,
    ) {
        this.dedupLimit = dedupLimit > 0 ? dedupLimit : DEFAULT_DEDUP_LIMIT;
    }

    /** Apply a committed log entry at `index`. Deterministic across nodes. */
    apply(index: number, entry: LogEntry<C>): ApplyResult<T> {
        const { command, meta } = entry;

        // Idempotency: a replayed requestId yields the cached result, no re-apply.
        if (meta?.requestId && this.seen.has(meta.requestId)) {
            return this.seen.get(meta.requestId)!;
        }

        // Control commands (NOOP/CONFIG) have no application effect; everything
        // else is an application command handled by the wrapped state machine.
        const result: ApplyResult<T> =
            command.type === 'NOOP' || command.type === 'CONFIG'
                ? { status: 200 }
                : this.app.apply(command as C);

        // NOOP entries are internal Raft bookkeeping — keep them out of the audit.
        if (command.type !== 'NOOP') {
            const record: AuditEntry = {
                index,
                term: entry.term,
                type: command.type,
                actor: meta?.actor ?? 'system',
                requestId: meta?.requestId ?? '',
                timestamp: meta?.timestamp ?? '',
                status: result.status,
                prevHash: this.lastHash,
                hash: '',
            };
            record.hash = this.hashOf(record);
            this.lastHash = record.hash;
            this.audit.push(record);
        }

        if (meta?.requestId) this.remember(meta.requestId, result);
        return result;
    }

    /** Record a result, evicting the oldest entries (FIFO) past the cap. */
    private remember(requestId: string, result: ApplyResult<T>): void {
        this.seen.set(requestId, result);
        while (this.seen.size > this.dedupLimit) {
            // Map preserves insertion order, so the first key is the oldest.
            const oldest = this.seen.keys().next().value as string;
            this.seen.delete(oldest);
        }
    }

    private hashOf(r: AuditEntry): string {
        return createHash('sha256').update(auditEntryPayload(r)).digest('hex');
    }

    // ---- audit access ----

    getAuditLog(): AuditEntry[] {
        return this.audit.map((e) => ({ ...e }));
    }

    /** Recompute the chain and report whether it is intact. */
    verifyAudit(): { valid: boolean; brokenAt?: number; length: number } {
        let prev = GENESIS_HASH;
        for (const e of this.audit) {
            const expected = this.hashOf({ ...e, prevHash: prev });
            if (e.prevHash !== prev || e.hash !== expected) {
                return { valid: false, brokenAt: e.index, length: this.audit.length };
            }
            prev = e.hash;
        }
        return { valid: true, length: this.audit.length };
    }

    // ---- snapshot / restore (for log compaction) ----

    snapshot(): RsmSnapshot<T> {
        return {
            state: this.app.snapshot(),
            audit: this.getAuditLog(),
            seen: [...this.seen.entries()],
            lastHash: this.lastHash,
        };
    }

    restore(snap: RsmSnapshot<T>): void {
        this.app.restore(snap.state);
        this.audit.length = 0;
        this.audit.push(...snap.audit);
        this.seen.clear();
        for (const [k, v] of snap.seen) this.seen.set(k, v);
        this.lastHash = snap.lastHash;
    }

    // ---- application access ----

    /** The wrapped application state machine (for domain reads). */
    get application(): StateMachine<C, T> {
        return this.app;
    }

    /** Entity count from the application state machine (0 if it doesn't track one). */
    size(): number {
        return this.app.size?.() ?? 0;
    }

    /** Current number of remembered requestIds (bounded by the dedup limit). */
    dedupCacheSize(): number {
        return this.seen.size;
    }
}
