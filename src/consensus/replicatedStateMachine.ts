import { createHash } from 'crypto';
import { BookStateMachine } from './stateMachine';
import { ApplyResult, AuditEntry, Book, LogEntry } from './types';
import { ModuleHost } from '../runtime/moduleHost';
import { ModuleDefinition } from '../runtime/types';
import { EMPTY_ROOT } from '../runtime/merkleAudit';

const GENESIS_HASH = '0'.repeat(64);

/** Default cap on the idempotency dedup cache (number of remembered requestIds). */
export const DEFAULT_DEDUP_LIMIT = 10_000;

/** Serializable snapshot of the full replicated state (books + audit + dedup). */
export interface RsmSnapshot {
    books: Book[];
    audit: AuditEntry[];
    seen: [string, ApplyResult][];
    lastHash: string;
    /**
     * The embedded {@link ModuleHost} snapshot (module state + outbox + Merkle
     * audit). Optional for back-compat: snapshots taken before module wiring (or
     * by a node with no modules registered) omit it, and `restore()` tolerates
     * its absence.
     */
    modules?: Record<string, unknown>;
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
 *
 * The dedup cache is bounded ({@link DEFAULT_DEDUP_LIMIT}) so it — and therefore
 * the snapshots it is folded into — cannot grow without limit. Eviction is
 * **insertion-order FIFO**, which is deterministic: every node applies the same
 * commands in the same order, so each evicts the same entries and replicas stay
 * identical. (A wall-clock TTL would *not* be deterministic and would diverge
 * the cluster.) The trade-off: a retry of a request older than the window is no
 * longer deduped and re-applies — acceptable, since realistic retries are recent.
 */
export class ReplicatedStateMachine {
    private readonly books = new BookStateMachine();
    /**
     * The embedded module runtime (ADR-0018 pillars 1–2). Lazily created when the
     * first module is registered, so a node that never uses modules carries no
     * host and emits no `modules` field in its snapshot (back-compat).
     */
    private moduleHost?: ModuleHost;
    private readonly audit: AuditEntry[] = [];
    private readonly seen = new Map<string, ApplyResult>();
    private lastHash = GENESIS_HASH;
    private readonly dedupLimit: number;

    constructor(dedupLimit: number = DEFAULT_DEDUP_LIMIT) {
        this.dedupLimit = dedupLimit > 0 ? dedupLimit : DEFAULT_DEDUP_LIMIT;
    }

    // ---- module registration (run on every node BEFORE start) ----

    /** Lazily create and return the embedded ModuleHost. */
    private host(): ModuleHost {
        if (!this.moduleHost) this.moduleHost = new ModuleHost();
        return this.moduleHost;
    }

    /** Register one module so its reducers can apply on this node's MODULE path. */
    registerModule(def: ModuleDefinition<any>): void {
        this.host().register(def);
    }

    /** Register several modules at once. */
    registerModules(defs: ModuleDefinition<any>[]): void {
        for (const def of defs) this.host().register(def);
    }

    /** Apply a committed log entry at `index`. Deterministic across nodes. */
    apply(index: number, entry: LogEntry): ApplyResult {
        const { command, meta } = entry;

        // Idempotency FIRST — covers MODULE commands too: a replayed requestId
        // yields the cached result without re-running the reducer (so a retried
        // increment never double-applies).
        if (meta?.requestId && this.seen.has(meta.requestId)) {
            return this.seen.get(meta.requestId)!;
        }

        // MODULE commands are a runtime concern: dispatch them to the embedded
        // ModuleHost. Everything else applies to the book store. Both paths share
        // the hash-chain audit + idempotency bookkeeping below for uniformity.
        const result: ApplyResult =
            command.type === 'MODULE'
                ? this.applyModule(command, meta)
                : this.books.apply(command);

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

    /**
     * Dispatch a MODULE command to the embedded ModuleHost and map its
     * `ModuleApplyResult` onto the generic `ApplyResult` the apply path expects.
     * The ModuleHost keeps its own richer Merkle audit; the hash-chain audit in
     * `apply()` still records the MODULE envelope (type + status) for uniformity.
     */
    private applyModule(
        command: Extract<LogEntry['command'], { type: 'MODULE' }>,
        meta: LogEntry['meta'],
    ): ApplyResult {
        const host = this.host();
        const moduleResult = host.apply(
            {
                module: command.module,
                command: command.command,
                input: command.input,
                seed: command.seed,
            },
            { actor: meta?.actor ?? 'system', requestId: meta?.requestId ?? '' },
        );
        return {
            status: moduleResult.status,
            result: moduleResult.result,
            message: moduleResult.message,
        };
    }

    /** Record a result, evicting the oldest entries (FIFO) past the cap. */
    private remember(requestId: string, result: ApplyResult): void {
        this.seen.set(requestId, result);
        while (this.seen.size > this.dedupLimit) {
            // Map preserves insertion order, so the first key is the oldest.
            const oldest = this.seen.keys().next().value as string;
            this.seen.delete(oldest);
        }
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
        const snap: RsmSnapshot = {
            books: this.books.getAll(),
            audit: this.getAuditLog(),
            seen: [...this.seen.entries()],
            lastHash: this.lastHash,
        };
        // Fold the module runtime into the snapshot so module state, outbox, and
        // Merkle audit survive log compaction/restart. Omit the field entirely
        // when no host exists, keeping snapshots of module-free nodes unchanged.
        if (this.moduleHost) snap.modules = this.moduleHost.snapshot();
        return snap;
    }

    restore(snap: RsmSnapshot): void {
        this.books.load(snap.books);
        this.audit.length = 0;
        this.audit.push(...snap.audit);
        this.seen.clear();
        for (const [k, v] of snap.seen) this.seen.set(k, v);
        this.lastHash = snap.lastHash;
        // Rehydrate the module runtime. Guard back-compat: older snapshots (and
        // those from module-free nodes) carry no `modules` field, so only restore
        // when both the snapshot has it and modules are registered on this node.
        if (snap.modules && this.moduleHost) this.moduleHost.restore(snap.modules);
    }

    // ---- domain access (delegated) ----

    getAll(): Book[] { return this.books.getAll(); }
    get(id: string): Book | undefined { return this.books.get(id); }
    size(): number { return this.books.size(); }

    /** Current number of remembered requestIds (bounded by the dedup limit). */
    dedupCacheSize(): number { return this.seen.size; }

    // ---- module access (ADR-0018 runtime) ----

    /** Run a read query against a module's state (undefined if no host/module). */
    moduleQuery(module: string, name: string, args?: unknown): unknown {
        return this.moduleHost?.query(module, name, args);
    }

    /** Current live state of a module (undefined if no host/module). */
    moduleState(module: string): unknown {
        return this.moduleHost?.getState(module);
    }

    /** The module runtime's Merkle audit root (the empty-tree root if no host). */
    moduleAuditRoot(): string {
        // A read must never allocate a host: a module-free node stays provably
        // host-free (and its snapshots provably unchanged) even if queried.
        return this.moduleHost ? this.moduleHost.auditRoot() : EMPTY_ROOT;
    }
}
