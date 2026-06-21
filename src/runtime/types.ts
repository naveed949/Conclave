/**
 * Core type definitions for the module runtime (ADR-0018, pillars 1–2).
 *
 * This layer sits ON TOP of the consensus core. Where the book demo wires
 * non-determinism out by hand in `models/book.ts`, the runtime generalizes that
 * discipline: developers write **pure reducers** and the framework injects a
 * deterministic `ReducerContext`. Every value a reducer could be tempted to pull
 * from the ambient environment (clock, randomness, ids) instead arrives through
 * `ctx`, and `ctx` is derived entirely from a leader-resolved `Seed`. That is
 * what lets two fresh hosts replay the same commands to byte-identical state.
 */

/**
 * A declarative request for a side effect (ADR-0018 pillar 3). Reducers never
 * perform I/O; they return intents that a later milestone will commit to the log
 * and hand to a post-commit executor. Minimal for now — fleshed out later.
 */
export interface EffectIntent {
    kind: string;
    /** Stable key so the executor can run the effect exactly-once. */
    idempotencyKey: string;
    payload: unknown;
}

/**
 * The deterministic capabilities handed to a reducer. These are the ONLY
 * sanctioned source of non-determinism: a reducer that reads `Date.now()`,
 * `Math.random()`, or `crypto` directly breaks the determinism contract.
 *
 * All values are reproducible from the command's `Seed` alone, so a replica
 * applying the same command rebuilds the identical context.
 */
export interface ReducerContext {
    /** Leader-resolved wall-clock as an ISO string (the analog of `seed.timestamp`). */
    now: string;
    /** Deterministic PRNG: a float in [0, 1). Successive calls advance the stream. */
    random(): number;
    /** Deterministic unique id. Successive calls return distinct values. */
    id(): string;
    /** Who issued the command (carried from `CommandMeta`, off the deterministic path). */
    actor: string;
    /** Request correlation id (carried from `CommandMeta`). */
    requestId: string;
}

/**
 * What a reducer returns: the next state, an optional explicit result value to
 * surface to the caller, and any effect intents it wants run. `result` is kept
 * distinct from `state` so the host can hand callers a purpose-built value (e.g.
 * the created entity) without exposing the entire next-state object.
 */
export interface ReducerResult<S> {
    state: S;
    result?: unknown;
    effects?: EffectIntent[];
}

/**
 * A pure state transition. Given the current state, a command input, and the
 * deterministic context, it returns the next state. MUST NOT mutate `state`
 * in place in a way that depends on anything outside its arguments, and MUST
 * NOT read ambient clocks/randomness — use `ctx`.
 */
export type Reducer<S> = (state: S, input: unknown, ctx: ReducerContext) => ReducerResult<S>;

/** A read-only projection over state. Never mutates; not part of the log. */
export type Query<S> = (state: S, args: unknown) => unknown;

/** A self-contained unit of application logic: its state, commands, and queries. */
export interface ModuleDefinition<S> {
    name: string;
    /** Builds the empty initial state. A factory (not a value) so each host gets its own. */
    initialState: () => S;
    commands: Record<string, Reducer<S>>;
    queries?: Record<string, Query<S>>;
}

/**
 * The leader-resolved deterministic seed for a single command. Resolved once on
 * the leader (real clock + real randomness) and replicated verbatim, so every
 * replica derives the same context from it. The consensus-core analog is the
 * leader baking ids/timestamps into a `Command` before it enters the log.
 */
export interface Seed {
    /** ISO timestamp captured on the leader. */
    timestamp: string;
    /** Hex-encoded random nonce captured on the leader; seeds the PRNG and id stream. */
    nonce: string;
}

/** A command targeted at a module's reducer, carrying its leader-resolved seed. */
export interface ModuleCommand {
    module: string;
    command: string;
    input: unknown;
    seed: Seed;
}

/** The outcome of applying a `ModuleCommand` to a `ModuleHost`. */
export interface ModuleApplyResult {
    status: number;
    result?: unknown;
    effects: EffectIntent[];
    message?: string;
}
