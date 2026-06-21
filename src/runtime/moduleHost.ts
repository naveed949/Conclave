import { createContext } from './context';
import { ModuleApplyResult, ModuleCommand, ModuleDefinition } from './types';

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

    /** Register a module and initialize its state. Throws on a duplicate name. */
    register(def: ModuleDefinition<any>): void {
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
        const out: Record<string, unknown> = {};
        for (const name of [...this.states.keys()].sort()) {
            out[name] = deepClone(this.states.get(name));
        }
        return out;
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
    }
}
