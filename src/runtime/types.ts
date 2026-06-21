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
 * perform I/O; they return intents that the host records into a deterministic
 * outbox and a post-commit `EffectExecutor` runs at the edge. The replicated log
 * thus IS a transactional outbox: the intent commits as part of the reducer's
 * result, and the effect's outcome re-enters the log as a committed follow-up.
 */
export interface EffectIntent {
    kind: string;
    /**
     * Stable key that makes execution exactly-once: the outbox dedups on it (a
     * replayed command never re-enqueues) and `applyEffectResult` is idempotent
     * on it (a redelivered result never double-dispatches).
     */
    idempotencyKey: string;
    payload: unknown;
    /**
     * Optional module command that consumes the effect's result. After the edge
     * resolves the effect, `applyEffectResult` dispatches this command so the
     * reducer can fold the outcome back into module state deterministically.
     */
    onResult?: { module: string; command: string };
}

/** Lifecycle of an outbox entry: enqueued (`pending`) or executed (`done`). */
export type OutboxStatus = 'pending' | 'done';

/**
 * One slot in the deterministic outbox: the emitted intent, its execution
 * status, and (once executed) the result the edge produced. Keyed in the outbox
 * by `intent.idempotencyKey`.
 */
export interface OutboxEntry {
    intent: EffectIntent;
    status: OutboxStatus;
    result?: unknown;
}

/**
 * A committed log entry carrying an effect's outcome back into the deterministic
 * core. The `result` was resolved ONCE on the effectful edge (the executor) and
 * is now replicated verbatim; likewise the `seed` is resolved once on the edge by
 * the executor, then committed verbatim so every replica applies the same value
 * (convergence comes from committing the value, not from where it was generated).
 * That deterministic `seed` gives any `onResult` reducer that consumes the result
 * a deterministic `ctx`. Applying this entry on every replica yields identical
 * state.
 */
export interface EffectResultEntry {
    idempotencyKey: string;
    result: unknown;
    /**
     * Resolved once on the edge by the executor, then committed verbatim so every
     * replica applies the same value. It seeds the `ctx` of the `onResult`
     * reducer, keeping that follow-up dispatch deterministic on every replica.
     */
    seed: Seed;
}

/**
 * Performs the real side effect at the edge (ADR-0018 pillar 3). This is the ONE
 * place non-determinism (network, clock, randomness) is allowed: the executor
 * runs it post-commit, off the deterministic apply path, and bakes the returned
 * value into an `EffectResultEntry` so replicas never re-run it.
 */
export type EffectHandler = (intent: EffectIntent) => Promise<unknown>;

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
    /**
     * Semantic version of the module's LOGIC (ADR-0018 pillar 5). Optional;
     * defaults to `'0'` when unset. It participates in the module's code hash, so
     * bumping it produces a distinct `codeHash` even if the reducer source is
     * byte-identical — letting an operator force a recorded version boundary.
     */
    version?: string;
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

/**
 * Construction options for `ModuleHost` (ADR-0018 pillar 6 "resource safety").
 *
 * These bound the AMPLIFICATION a single command may cause: how many effects it
 * may enqueue, and how large the next-state it may produce. They are a
 * DETERMINISTIC resource bound, NOT a CPU/step meter — every replica computes
 * the same effect count and the same canonical byte size from the same reducer
 * output, so an over-budget command is rejected IDENTICALLY on every node and
 * the bound never causes divergence. A preemptive CPU/instruction meter (which
 * would need a vm/worker to interrupt a runaway reducer mid-execution) is
 * explicitly DEFERRED per ADR-0018; this guards the cheap, deterministic axes
 * (fan-out and state size) that can be measured purely after the reducer returns.
 */
export interface ModuleHostOptions {
    /**
     * Max number of effect intents one command may emit. Caps outbox fan-out so
     * a single command cannot flood the post-commit executor. Default: 16.
     */
    maxEffects?: number;
    /**
     * Max canonical-JSON byte size of the next-state a reducer may produce. Caps
     * state amplification so a single command cannot bloat replicated state (and
     * thus every snapshot) without bound. Default: 64 KiB.
     */
    maxResultBytes?: number;
}
