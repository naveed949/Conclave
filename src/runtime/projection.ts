/**
 * CQRS read-projection layer (ADR-0018 pillar 4: "Reads / rich queries / big
 * data").
 *
 * The authoritative state is the replicated log applied by `ModuleHost` — that
 * is the source of truth and the only thing consensus commits. This layer is the
 * OTHER half of CQRS: a DERIVED, indexed read model built by folding the stream
 * of committed module commands. It exists to answer rich queries the raw module
 * state does not index (e.g. "all note ids by a given actor") without bloating
 * the deterministic apply path or the snapshots the consensus core replicates.
 *
 * WHY SEPARATE: a projection is a CACHE, not a database. It is never consulted
 * to decide whether a command is valid, never feeds back into the log, and can
 * be thrown away and rebuilt from the committed command stream at any time
 * (`ProjectionHost.rebuild`). Because the fold is PURE and DETERMINISTIC, every
 * node that replays the same committed stream reconstructs an identical read
 * model — read-scaling that preserves ADR-0002 (the log stays source of truth).
 */

/**
 * A committed module command as seen by the READ side. This is the projection
 * layer's input event: the same information the write path produced when it
 * applied the command, captured AFTER commit so the read model only ever folds
 * facts that consensus already agreed on.
 *
 * It is derived from a `ModuleHost.apply` outcome plus the command's meta — see
 * the projection tests for how a host apply result maps onto one of these. The
 * read side intentionally does not depend on `ModuleHost` internals; it consumes
 * this flat, serializable event so a projection could equally be fed from a log
 * reader, a replication stream, or a test harness.
 */
export interface ProjectionEvent {
    /** Monotonic position of the command in the committed stream (its log order). */
    seq: number;
    /** Target module name. */
    module: string;
    /** Command name dispatched on the module. */
    command: string;
    /** The command input that was committed. */
    input: unknown;
    /** The explicit result the reducer surfaced (e.g. the created entity). */
    result: unknown;
    /** Who issued the command (from `CommandMeta`). */
    actor: string;
    /** Request correlation id (from `CommandMeta`). */
    requestId: string;
    /** Leader-resolved commit timestamp, if the caller chose to carry it. */
    timestamp?: string;
}

/**
 * A declarative read projection over the committed command stream.
 *
 * `on` is a PURE fold: given the current view and the next committed event, it
 * returns the NEXT view. It is responsible for its own filtering — it inspects
 * `event.module` / `event.command` and ignores events it does not care about by
 * returning the view unchanged. Keeping the fold pure (no `Date`, no
 * `Math.random`, no I/O) is what makes the read model rebuildable and convergent
 * across nodes: the same committed stream always folds to the same view.
 *
 * `queries` are read-only views over the folded state — the rich, indexed answers
 * the projection exists to provide. They never mutate the view.
 */
export interface ProjectionDefinition<V> {
    name: string;
    /** Builds the empty initial view. A factory so each host gets its own copy. */
    init: () => V;
    /** Pure fold: next view given the current view and the next committed event. */
    on: (view: V, event: ProjectionEvent) => V;
    /** Named read-only queries over the folded view. */
    queries: Record<string, (view: V, args: unknown) => unknown>;
}

/**
 * Validate and return a projection definition. Like `defineModule`, this is an
 * identity with light validation so a malformed projection fails loud at
 * definition time rather than as a confusing fold/query error later.
 *
 * Requirements: a non-empty name (projections are registered by name and
 * duplicates are rejected) and at least one query (a projection with no queries
 * is dead weight — it folds a view nothing can read).
 */
export function defineProjection<V>(def: ProjectionDefinition<V>): ProjectionDefinition<V> {
    if (!def.name || def.name.trim() === '') {
        throw new Error('Projection definition requires a non-empty name');
    }
    if (typeof def.init !== 'function') {
        throw new Error(`Projection "${def.name}" requires an init() factory`);
    }
    if (typeof def.on !== 'function') {
        throw new Error(`Projection "${def.name}" requires an on() fold`);
    }
    const queryNames = Object.keys(def.queries ?? {});
    if (queryNames.length === 0) {
        throw new Error(`Projection "${def.name}" must define at least one query`);
    }
    return def;
}
