/**
 * Pluggable, key-oriented state store (ADR-0018 pillar 4, the "state larger than
 * RAM" half).
 *
 * The existing module model (`defineModule`) treats a module's state as ONE
 * whole-state blob: every command loads, transforms, and replaces the entire
 * value. That is simple but caps a module's state at what fits in RAM and what is
 * cheap to clone per command. This module adds the alternative: a module's state
 * is a COLLECTION OF RECORDS addressed by key, and a reducer touches only the
 * keys it needs (read one account, write it back) — never the whole dataset.
 *
 * `StateStore` is the seam. The default `MemoryStateStore` keeps records in a
 * `Map`, but the interface is deliberately the shape a persistent embedded KV/LSM
 * store (LevelDB, RocksDB, …) would implement: there snapshots become store
 * checkpoints and the working set need not fit in memory. Swapping the backend
 * changes nothing above this seam.
 *
 * DETERMINISM (the non-negotiable, same rule as the consensus state machine):
 * every observable store operation a reducer can see MUST be identical on every
 * replica. Two consequences enforced here:
 *  - ITERATION ORDER is sorted by key, always. A `Map` iterates in insertion
 *    order, which differs between replicas that applied the same logical writes
 *    in a different incidental order — so `keys()/entries()/snapshot()` sort.
 *  - READS RETURN DEEP CLONES. A reducer must never receive a shared mutable
 *    reference into stored state: mutating it in place would (a) bypass the
 *    transactional commit/discard guarantee and (b) leak insertion-time identity.
 *    Every read clones, so the store is the sole owner of its records.
 */

import { canonicalJson } from './canonical';

/**
 * Deep-clone a serializable value. Reuses the shared `canonicalJson` round-trip
 * (DROP-`undefined` mode, matching `JSON.stringify` semantics) rather than
 * `structuredClone` so the clone is consistent with the byte-sizing / hashing
 * paths and does not depend on `structuredClone` typings under the lib target —
 * the same assumption `moduleHost.ts`'s `deepClone` makes. Records are plain
 * serializable data, so a JSON round-trip is sufficient.
 *
 * Using `canonicalJson` (sorted keys) additionally NORMALIZES key order in the
 * clone, so a stored record can never leak its insertion-time key order to a
 * reader — one more determinism guard for free.
 */
function cloneValue<T>(value: T): T {
    return JSON.parse(canonicalJson(value, { onUndefined: 'drop' })) as T;
}

/**
 * Per-module record store: a key→value map with deterministic iteration and
 * clone-on-read. This is the seam a persistent embedded KV/LSM store drops into
 * (snapshots become store checkpoints there); the default below is in-memory.
 *
 * VALUE SEMANTICS: values round-trip through canonical JSON (the clone path), so
 * store plain JSON-safe records — a property explicitly set to `undefined` is
 * DROPPED (matching `JSON.stringify`, identical across replicas, so no
 * divergence) and `Date`/`Map`/`Set`/class instances are NOT preserved (a `Date`
 * becomes its ISO string, a `Map` becomes `{}`).
 */
export interface StateStore {
    /** The value at `key`, deep-cloned, or `undefined` if absent. */
    get(key: string): unknown | undefined;
    /** Store (a deep clone of) `value` at `key`, replacing any existing record. */
    put(key: string, value: unknown): void;
    /** Remove the record at `key` (no-op if absent). */
    delete(key: string): void;
    /** Whether a record exists at `key`. */
    has(key: string): boolean;
    /** All keys, SORTED ascending (deterministic iteration). */
    keys(): string[];
    /** All [key, value] pairs, SORTED by key, values deep-cloned. */
    entries(): [string, unknown][];
    /** Number of records. */
    size(): number;
    /** Full SORTED dump for snapshotting; values deep-cloned. */
    snapshot(): [string, unknown][];
    /** Replace all records with `entries` (deep-cloned). */
    restore(entries: [string, unknown][]): void;
}

/**
 * Default in-memory `StateStore` over a `Map`.
 *
 * This is the default backend; the `StateStore` interface is the seam a
 * persistent embedded KV/LSM store would implement instead (where `snapshot()`
 * becomes a store checkpoint and the dataset need not fit in RAM). Determinism is
 * guaranteed here by SORTING keys on every iteration (a `Map` is insertion-
 * ordered, which is not replica-stable) and DEEP-CLONING values on read/dump so a
 * caller can never mutate a stored record in place.
 *
 * VALUE SEMANTICS (same as the interface): records round-trip through canonical
 * JSON, so a property set to `undefined` is dropped and `Date`/`Map`/etc. are
 * not preserved — store plain JSON-safe records.
 */
export class MemoryStateStore implements StateStore {
    private readonly map = new Map<string, unknown>();

    get(key: string): unknown | undefined {
        if (!this.map.has(key)) return undefined;
        // Clone on read: the caller must never get a mutable reference into the
        // store, or it could mutate committed state out of band and diverge.
        return cloneValue(this.map.get(key));
    }

    put(key: string, value: unknown): void {
        // Clone on write too: the caller keeps no shared handle to what it stored,
        // so a later mutation of its local object cannot reach into the store.
        this.map.set(key, cloneValue(value));
    }

    delete(key: string): void {
        this.map.delete(key);
    }

    has(key: string): boolean {
        return this.map.has(key);
    }

    keys(): string[] {
        // SORTED: a Map iterates in insertion order, which is not replica-stable.
        return [...this.map.keys()].sort();
    }

    entries(): [string, unknown][] {
        return this.keys().map((k) => [k, cloneValue(this.map.get(k))]);
    }

    size(): number {
        return this.map.size;
    }

    snapshot(): [string, unknown][] {
        // Same as entries(): sorted + cloned. Named distinctly so a persistent
        // backend can implement it as a checkpoint without overloading entries().
        return this.entries();
    }

    restore(entries: [string, unknown][]): void {
        this.map.clear();
        for (const [k, v] of entries) {
            this.map.set(k, cloneValue(v));
        }
    }
}

/**
 * A transactional COPY-ON-WRITE view over a `StateStore`, handed to a keyed
 * reducer (ADR-0018 pillar 4). The atomicity story mirrors what whole-state
 * modules get from the host's clone-and-swap: nothing the reducer writes touches
 * the underlying store until the host explicitly `commit()`s, and a reducer that
 * throws (or is rejected by a budget/lint check) is simply discarded — the view
 * is dropped and the store is untouched.
 *
 * READ SEMANTICS (reads-your-writes): a `get`/`has`/`keys`/`entries` reflects
 * this view's own buffered `put`/`delete` FIRST, falling through to the
 * underlying store only for keys the view hasn't touched. Fall-through reads
 * return clones (from the underlying store), so the reducer still cannot mutate
 * committed records in place. Iteration stays SORTED and reflects buffered
 * puts/deletes — deterministic regardless of how the reducer interleaved writes.
 */
export class StoreView {
    /** Buffered puts, keyed; values are already cloned on entry. */
    private readonly writes = new Map<string, unknown>();
    /** Keys buffered for deletion. A delete shadows the underlying store. */
    private readonly deletes = new Set<string>();

    constructor(private readonly store: StateStore) {}

    get(key: string): unknown | undefined {
        if (this.deletes.has(key)) return undefined;
        if (this.writes.has(key)) {
            // Clone the buffered value so the reducer cannot mutate the buffer in
            // place behind its own write (consistent with the store's read clone).
            return cloneValue(this.writes.get(key));
        }
        return this.store.get(key); // already cloned by the underlying store
    }

    put(key: string, value: unknown): void {
        // Buffer only — nothing reaches the underlying store until commit().
        // Clone on entry so the reducer's later mutation of its local object
        // cannot retroactively change what the view will commit.
        this.writes.set(key, cloneValue(value));
        this.deletes.delete(key); // a put cancels a prior buffered delete
    }

    delete(key: string): void {
        this.writes.delete(key);
        this.deletes.add(key);
    }

    has(key: string): boolean {
        if (this.deletes.has(key)) return false;
        if (this.writes.has(key)) return true;
        return this.store.has(key);
    }

    /**
     * SORTED keys reflecting buffered changes: underlying keys, minus buffered
     * deletes, plus buffered puts (which may be new keys). Computed fresh each
     * call so it always mirrors the current buffer.
     */
    keys(): string[] {
        const set = new Set<string>();
        for (const k of this.store.keys()) {
            if (!this.deletes.has(k)) set.add(k);
        }
        for (const k of this.writes.keys()) set.add(k);
        return [...set].sort();
    }

    entries(): [string, unknown][] {
        return this.keys().map((k) => [k, this.get(k)]);
    }

    size(): number {
        return this.keys().length;
    }

    /** Number of buffered writes (puts + deletes) — the host's `maxWrites` axis. */
    pendingWriteCount(): number {
        return this.writes.size + this.deletes.size;
    }

    /** The buffered puts, sorted by key — what the host bytes-sizes for `maxResultBytes`. */
    pendingPuts(): [string, unknown][] {
        return [...this.writes.keys()].sort().map((k) => [k, this.writes.get(k)] as [string, unknown]);
    }

    /**
     * Apply the buffer to the underlying store atomically: all buffered puts and
     * deletes land together. The host calls this ONLY on a successful, in-budget
     * reducer; on any failure the view is simply dropped and nothing here runs.
     */
    commit(): void {
        // Deterministic apply order (sorted) — the result is order-independent
        // since each key appears at most once, but a stable order keeps any
        // backend's write log reproducible.
        for (const k of [...this.deletes].sort()) {
            this.store.delete(k);
        }
        for (const k of [...this.writes.keys()].sort()) {
            this.store.put(k, this.writes.get(k));
        }
    }
}
