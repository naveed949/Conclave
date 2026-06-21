import { createContext } from './context';
import {
    EffectIntent,
    EffectResultEntry,
    ModuleApplyResult,
    ModuleCommand,
    ModuleDefinition,
    OutboxEntry,
} from './types';

/**
 * Deep-clone via JSON round-trip. Used so snapshots are decoupled from live
 * state (mutating a host after snapshotting must not retroactively change the
 * snapshot, and vice versa). Module state is plain serializable data — the same
 * assumption the consensus-core snapshots already make — so JSON is sufficient
 * and avoids depending on `structuredClone` typings under the ES2017 lib target.
 */
function deepClone<T>(value: T): T {
    return JSON.parse(JSON.stringify(value)) as T;
}

/**
 * Registry of modules plus their live state (ADR-0018 pillars 1–2). This is the
 * deterministic runtime that a later milestone will drive from the replicated
 * `apply()` path. Today it stands alone: register modules, then `apply` commands
 * whose seeds were resolved on the leader. Because the host derives every
 * context from the command's seed, two hosts fed the same command stream reach
 * identical state — the property the convergence test asserts.
 */
export class ModuleHost {
    private readonly modules = new Map<string, ModuleDefinition<unknown>>();
    /** Live per-module state, keyed by module name. */
    private readonly states = new Map<string, unknown>();
    /**
     * The deterministic outbox (ADR-0018 pillar 3), keyed by `idempotencyKey`.
     * Every host derives it from the same committed command stream, so it is
     * itself replicated state — part of `snapshot()`/`restore()`. Keying on the
     * idempotency key is what makes enqueue idempotent: a replayed command can
     * re-run its reducer but never re-enqueue the same effect.
     */
    private readonly outbox = new Map<string, OutboxEntry>();

    /** Register a module and initialize its state. Throws on a duplicate name. */
    register(def: ModuleDefinition<any>): void {
        if (!def.name || def.name.trim() === '') {
            throw new Error('Module definition requires a non-empty name');
        }
        // Reject reserved `__`-prefixed names: the snapshot stores the outbox
        // under the reserved `__outbox` key in the same flat module-states object,
        // so a `__`-prefixed module would silently collide (its state overwritten
        // on snapshot, misread as an outbox map on restore). Fail closed.
        if (def.name.startsWith('__')) {
            throw new Error(
                `Module name "${def.name}" is reserved: names starting with "__" are reserved for runtime internals`,
            );
        }
        if (this.modules.has(def.name)) {
            throw new Error(`Module "${def.name}" is already registered`);
        }
        this.modules.set(def.name, def as ModuleDefinition<unknown>);
        this.states.set(def.name, def.initialState());
    }

    /**
     * Dispatch a command: look up the module + reducer, build a deterministic
     * context from the command's seed, run the (pure) reducer against current
     * state, and adopt the returned state. Failures are returned as status codes
     * rather than thrown, mirroring the state machine's HTTP-style results, so a
     * single bad command never crashes the apply loop.
     */
    apply(cmd: ModuleCommand, meta: { actor: string; requestId: string }): ModuleApplyResult {
        const result = this.dispatch(cmd, meta);
        if (result.status === 200) {
            // Record each emitted effect into the outbox as `pending`, but only if
            // its key is unknown — a replayed command must re-run deterministically
            // without re-enqueuing the same effect (the exactly-once dedup point).
            for (const intent of result.effects) {
                if (!this.outbox.has(intent.idempotencyKey)) {
                    this.outbox.set(intent.idempotencyKey, { intent, status: 'pending' });
                }
            }
        }
        return result;
    }

    /**
     * The reducer dispatch path, shared by `apply` (caller commands) and
     * `applyEffectResult` (the committed `onResult` follow-up). It runs a pure
     * reducer against a clone of current state and adopts the result atomically;
     * it does NOT touch the outbox so callers can layer their own bookkeeping.
     */
    private dispatch(cmd: ModuleCommand, meta: { actor: string; requestId: string }): ModuleApplyResult {
        const def = this.modules.get(cmd.module);
        if (!def) {
            return { status: 404, effects: [], message: `Unknown module: ${cmd.module}` };
        }

        const reducer = def.commands[cmd.command];
        if (!reducer) {
            return {
                status: 404,
                effects: [],
                message: `Unknown command "${cmd.command}" on module "${cmd.module}"`,
            };
        }

        const ctx = createContext(cmd.seed, meta);
        // Hand the reducer a deep clone, never the live reference: a reducer that
        // mutates `state` in place and then throws must not corrupt committed
        // host state. Live state is replaced atomically only on a clean return.
        const working = deepClone(this.states.get(cmd.module));

        try {
            const result = reducer(working, cmd.input, ctx);
            this.states.set(cmd.module, result.state);
            // Surface the reducer's explicit `result` value, not the whole
            // ReducerResult — callers get a purpose-built value, not next-state.
            return { status: 200, result: result.result, effects: result.effects ?? [] };
        } catch (err) {
            // A throwing reducer must not corrupt state or halt the host; report it.
            const message = err instanceof Error ? err.message : String(err);
            return { status: 500, effects: [], message };
        }
    }

    /** All outbox entries still awaiting execution at the edge. */
    pendingEffects(): EffectIntent[] {
        const out: EffectIntent[] = [];
        for (const name of [...this.outbox.keys()].sort()) {
            const entry = this.outbox.get(name)!;
            if (entry.status === 'pending') {
                // Deep-clone (like getOutbox) so a handler that mutates the intent
                // it receives cannot mutate the committed outbox state behind it.
                out.push(deepClone(entry.intent));
            }
        }
        return out;
    }

    /**
     * Apply a committed `EffectResultEntry` — the follow-up entry the executor
     * fed back after performing the effect at the edge. This runs on the
     * deterministic apply path on EVERY replica, so it must be a pure function of
     * the entry + current state, and it MUST be idempotent: a redelivered or
     * replayed result must not re-dispatch the `onResult` reducer.
     *
     * Idempotency rule: if the key is unknown or already `done`, no-op (200, no
     * dispatch). Otherwise mark the entry `done`, store the edge-resolved
     * `result`, and — if the intent named an `onResult` command — dispatch it
     * through the normal reducer path with the entry's leader-resolved `seed`, so
     * the consuming reducer stays deterministic.
     */
    applyEffectResult(entry: EffectResultEntry, meta: { actor: string; requestId: string }): ModuleApplyResult {
        // First-applied wins. A handler retry that fires before the first result
        // is applied can commit multiple `EffectResultEntry` values for one key
        // (each drain that completes submits one). That is fine: the FIRST entry
        // applied flips the key to `done`; every later entry for that key hits the
        // `done` guard below and no-ops. Convergence holds because the committed
        // log order is identical on every replica, so all replicas apply the same
        // "first" entry and discard the same rest.
        const slot = this.outbox.get(entry.idempotencyKey);
        if (!slot || slot.status === 'done') {
            // Unknown or already-applied: exactly-once at the state level means
            // this is a harmless no-op, never a second `onResult` dispatch.
            return { status: 200, effects: [] };
        }

        slot.status = 'done';
        slot.result = entry.result;

        const onResult = slot.intent.onResult;
        if (!onResult) {
            return { status: 200, effects: [] };
        }

        // Feed the edge-resolved result to the consuming reducer. We go through
        // `apply` (not bare `dispatch`) so any effects the onResult reducer emits
        // are themselves enqueued into the outbox.
        return this.apply(
            {
                module: onResult.module,
                command: onResult.command,
                input: { idempotencyKey: entry.idempotencyKey, result: entry.result },
                seed: entry.seed,
            },
            meta,
        );
    }

    /**
     * The outbox as a list in deterministic (sorted-by-key) order. For tests and
     * inspection; the canonical store is the keyed map.
     */
    getOutbox(): OutboxEntry[] {
        return [...this.outbox.keys()].sort().map((k) => deepClone(this.outbox.get(k)!));
    }

    /** Run a read query against current state. Never mutates. */
    query(module: string, name: string, args?: unknown): unknown {
        const def = this.modules.get(module);
        if (!def) {
            throw new Error(`Unknown module: ${module}`);
        }
        const q = def.queries?.[name];
        if (!q) {
            throw new Error(`Unknown query "${name}" on module "${module}"`);
        }
        return q(this.states.get(module), args);
    }

    /** Current live state of a module (for tests/snapshots). */
    getState(module: string): unknown {
        return this.states.get(module);
    }

    /**
     * Serializable map of module name -> deep-cloned state. Keys are emitted in
     * sorted order so a `JSON.stringify` over the snapshot is stable regardless
     * of module registration order (helps the later audit/hash-chain milestone).
     */
    snapshot(): Record<string, unknown> {
        const states: Record<string, unknown> = {};
        for (const name of [...this.states.keys()].sort()) {
            states[name] = deepClone(this.states.get(name));
        }
        // The outbox is replicated state too: emit it under a reserved key, also
        // in sorted-key order so `JSON.stringify` over the snapshot is stable.
        const outbox: Record<string, OutboxEntry> = {};
        for (const key of [...this.outbox.keys()].sort()) {
            outbox[key] = deepClone(this.outbox.get(key)!);
        }
        return { ...states, __outbox: outbox };
    }

    /**
     * Replace the state of registered modules from a snapshot. Only modules that
     * are both registered AND present in the snapshot are restored; unknown keys
     * in the snapshot are ignored (a module may have been removed), and modules
     * absent from the snapshot keep their initialized state.
     */
    restore(snap: Record<string, unknown>): void {
        for (const name of this.modules.keys()) {
            if (Object.prototype.hasOwnProperty.call(snap, name)) {
                this.states.set(name, deepClone(snap[name]));
            }
        }
        // Rebuild the outbox from its reserved key. Replacing wholesale (not
        // merging) keeps restore a faithful point-in-time reconstruction.
        this.outbox.clear();
        const saved = snap.__outbox;
        if (saved && typeof saved === 'object') {
            for (const [key, entry] of Object.entries(saved as Record<string, OutboxEntry>)) {
                this.outbox.set(key, deepClone(entry));
            }
        }
    }
}
