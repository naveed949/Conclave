import { createHash } from 'crypto';
import { BookStateMachine } from './stateMachine';
import { ApplyResult, AuditEntry, Book, LogEntry } from './types';

const GENESIS_HASH = '0'.repeat(64);

/** Serializable snapshot of the full replicated state (books + audit + dedup). */
export interface RsmSnapshot {
    books: Book[];
    audit: AuditEntry[];
    seen: [string, ApplyResult][];
    lastHash: string;
}

/**
 * Wraps the domain {@link BookStateMachine} with two cross-cutting, generic
 * backend concerns that every node gets for free because they ride the same
 * deterministic, replicated apply path:
 *
 *  - **Audit**: an append-only, hash-chained record of every committed change.
 *    Replicated across the cluster and tamper-evident (alter one entry and the
 *    chain no longer verifies), so it doubles as a forgery-resistant history.
 *  - **Idempotency**: commands carry a requestId; a replayed requestId returns
 *    the original result without re-applying, turning at-least-once delivery
 *    into exactly-once effects (key for fault-tolerant client retries).
 */
export class ReplicatedStateMachine {
    private readonly books = new BookStateMachine();
    private readonly audit: AuditEntry[] = [];
    private readonly seen = new Map<string, ApplyResult>();
    private lastHash = GENESIS_HASH;

    /** Apply a committed log entry at `index`. Deterministic across nodes. */
    apply(index: number, entry: LogEntry): ApplyResult {
        const { command, meta } = entry;

        // Idempotency: a replayed requestId yields the cached result, no re-apply.
        if (meta?.requestId && this.seen.has(meta.requestId)) {
            return this.seen.get(meta.requestId)!;
        }

        const result = this.books.apply(command);

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

        if (meta?.requestId) this.seen.set(meta.requestId, result);
        return result;
    }

    private hashOf(r: AuditEntry): string {
        const payload = `${r.prevHash}|${r.index}|${r.term}|${r.type}|${r.actor}|${r.requestId}|${r.timestamp}|${r.status}`;
        return createHash('sha256').update(payload).digest('hex');
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

    snapshot(): RsmSnapshot {
        return {
            books: this.books.getAll(),
            audit: this.getAuditLog(),
            seen: [...this.seen.entries()],
            lastHash: this.lastHash,
        };
    }

    restore(snap: RsmSnapshot): void {
        this.books.load(snap.books);
        this.audit.length = 0;
        this.audit.push(...snap.audit);
        this.seen.clear();
        for (const [k, v] of snap.seen) this.seen.set(k, v);
        this.lastHash = snap.lastHash;
    }

    // ---- domain access (delegated) ----

    getAll(): Book[] { return this.books.getAll(); }
    get(id: string): Book | undefined { return this.books.get(id); }
    size(): number { return this.books.size(); }
}
