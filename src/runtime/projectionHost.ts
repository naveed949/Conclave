import { canonicalJson } from './canonical';
import { ProjectionDefinition, ProjectionEvent } from './projection';

/**
 * The READ side of CQRS (ADR-0018 pillar 4). A registry of projections plus their
 * live, folded views, fed by the stream of committed module commands.
 *
 * It is architecturally SEPARATE from the authoritative state:
 *  - It never participates in the consensus apply path — `ModuleHost` and the
 *    replicated log remain the single source of truth.
 *  - It is a DERIVED cache: every view here can be discarded and reconstructed
 *    from the committed command stream via `rebuild`. That is the proof the read
 *    model is not authoritative — if the cache and a from-scratch replay disagree,
 *    the replay (the log) wins, by construction.
 *  - Folds are pure and deterministic (`ProjectionDefinition.on`), so two hosts
 *    fed the same committed stream converge to deep-equal views — the same
 *    discipline the deterministic core uses, applied to the read model so read
 *    replicas stay consistent.
 */
export class ProjectionHost {
    private readonly defs = new Map<string, ProjectionDefinition<unknown>>();
    /** Live per-projection view, keyed by projection name. */
    private readonly views = new Map<string, unknown>();

    /** Register a projection and initialize its view. Throws on a duplicate name. */
    register<V>(def: ProjectionDefinition<V>): void {
        if (!def.name || def.name.trim() === '') {
            throw new Error('Projection definition requires a non-empty name');
        }
        if (this.defs.has(def.name)) {
            throw new Error(`Projection "${def.name}" is already registered`);
        }
        this.defs.set(def.name, def as ProjectionDefinition<unknown>);
        this.views.set(def.name, def.init());
    }

    /**
     * Fold one committed event into every registered projection's view. Each
     * projection's `on` decides whether the event is relevant (it filters on
     * `event.module` / `event.command` internally) and returns the next view,
     * which we adopt. Because `on` is pure, applying the same event on every
     * replica produces the same next view.
     */
    applyEvent(event: ProjectionEvent): void {
        for (const [name, def] of this.defs) {
            const next = def.on(this.views.get(name), event);
            this.views.set(name, next);
        }
    }

    /** Run a named query against a projection's current view. Never mutates. */
    query(projection: string, name: string, args?: unknown): unknown {
        const def = this.defs.get(projection);
        if (!def) {
            throw new Error(`Unknown projection: ${projection}`);
        }
        const q = def.queries[name];
        if (!q) {
            throw new Error(`Unknown query "${name}" on projection "${projection}"`);
        }
        return q(this.views.get(projection), args);
    }

    /**
     * Discard ALL views and rebuild them from scratch by replaying the committed
     * event stream in order. This is the heart of CQRS: it proves the read model
     * is DERIVED, not authoritative. A node can drop its cache (or join fresh) and
     * reconstruct byte-identical views purely from the log, because each `on` fold
     * is pure and deterministic.
     *
     * Every registered projection is reset to its `init()` view first, so the
     * rebuilt state depends ONLY on the supplied events, never on whatever the
     * cache happened to hold before.
     */
    rebuild(events: ProjectionEvent[]): void {
        for (const [name, def] of this.defs) {
            this.views.set(name, def.init());
        }
        for (const event of events) {
            this.applyEvent(event);
        }
    }

    /**
     * A serializable, deep-cloned map of projection name -> current view, with
     * keys emitted in sorted order so a `JSON.stringify` over the snapshot is
     * stable regardless of registration order (mirrors `ModuleHost.snapshot`).
     *
     * NOTE: the snapshot is a CACHE CONVENIENCE — a way to warm-start a read
     * replica without replaying the whole stream. It is NOT a source of truth.
     * Correctness of the read model comes from `rebuild` (replay from the log);
     * a snapshot is only ever as trustworthy as the stream that produced it.
     */
    snapshot(): Record<string, unknown> {
        const out: Record<string, unknown> = {};
        for (const name of [...this.views.keys()].sort()) {
            out[name] = clone(this.views.get(name));
        }
        return out;
    }

    /**
     * Replace the views of registered projections from a snapshot. Only
     * projections that are both registered AND present in the snapshot are
     * restored; unknown snapshot keys are ignored and absent projections keep
     * their `init()` view. As with `snapshot`, this is a cache warm-start — the
     * authoritative path remains `rebuild` from the committed stream.
     */
    restore(snap: Record<string, unknown>): void {
        for (const name of this.defs.keys()) {
            if (Object.prototype.hasOwnProperty.call(snap, name)) {
                this.views.set(name, clone(snap[name]));
            }
        }
    }

    /** Current live view of a projection (for tests/inspection). */
    getView(projection: string): unknown {
        return this.views.get(projection);
    }
}

/**
 * Deep-clone a view through the shared canonical serializer. Routing the clone
 * through `canonicalJson` (rather than a second `JSON.stringify`) keeps this
 * layer on the ONE sanctioned serializer — the same reason `canonical.ts` exists
 * — and yields a sorted-key, drift-free copy decoupled from the live view, so a
 * caller that mutates a returned snapshot cannot reach back into host state.
 */
function clone<T>(value: T): T {
    return JSON.parse(canonicalJson(value, { onUndefined: 'drop' })) as T;
}
