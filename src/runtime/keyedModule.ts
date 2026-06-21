/**
 * Keyed (key-oriented, transactional) module model (ADR-0018 pillar 4, the
 * "state larger than RAM" half).
 *
 * A WHOLE-STATE module (`defineModule`) hands each reducer the entire module
 * state and takes back the next whole state. A KEYED module instead hands the
 * reducer a transactional {@link StoreView} over a per-module {@link StateStore}:
 * the reducer READS and WRITES individual records by key and returns no state
 * blob at all. The host commits the view's buffered writes only on success, so a
 * keyed reducer gets the SAME atomicity guarantee as a whole-state one — without
 * ever loading the whole dataset.
 *
 * This is ADDITIVE. Keyed modules carry `kind: 'keyed'` so the host can route
 * them separately; whole-state modules are unchanged and keep `kind` absent
 * (treated as `'whole'`).
 */

import { lintReducer } from './determinism';
import { StateStore, StoreView } from './stateStore';
import { EffectIntent, ReducerContext } from './types';

/**
 * A keyed reducer. Mutations go through `store` (a transactional {@link StoreView}
 * buffering its writes); the reducer returns NO next-state — only an optional
 * explicit `result` to surface to the caller and any `effects` to enqueue. Same
 * purity contract as a whole-state reducer: read `now`/randomness/ids from `ctx`
 * only, never ambient globals.
 */
export type KeyedReducer = (
    store: StoreView,
    input: unknown,
    ctx: ReducerContext,
) => { result?: unknown; effects?: EffectIntent[] };

/**
 * A read query over a keyed module's store. Receives the module's
 * {@link StateStore} (read surface; reads return clones) and the query args.
 */
export type KeyedQuery = (store: StateStore, args: unknown) => unknown;

/**
 * A keyed module definition. The `kind: 'keyed'` discriminant lets the
 * {@link ModuleHost} tell it apart from a whole-state `ModuleDefinition` (which
 * has no `kind`).
 */
export interface KeyedModuleDefinition {
    name: string;
    version?: string;
    kind: 'keyed';
    commands: Record<string, KeyedReducer>;
    queries?: Record<string, KeyedQuery>;
}

/**
 * A keyed module that failed the determinism lint with `strict: false`. As with
 * `defineModule`, the violations are recorded under the reserved `__lint` field
 * rather than thrown.
 */
export type LintedKeyedModuleDefinition = KeyedModuleDefinition & { __lint?: string[] };

/** Options for {@link defineKeyedModule}, mirroring `defineModule`. */
export interface DefineKeyedModuleOptions {
    /**
     * Enforce the determinism lint at definition time (ADR-0018 pillar 2).
     * Defaults to TRUE — identical semantics to `defineModule`: a violation
     * throws, or with `{ strict: false }` is recorded under `__lint` instead.
     */
    strict?: boolean;
}

/**
 * Validate and return a keyed module definition. Mirrors `defineModule`'s
 * validation exactly (non-empty name, reject `__`-reserved names, ≥1 command,
 * run the determinism lint strict-by-default over each reducer) and stamps
 * `kind: 'keyed'` so the host routes it through the keyed (StoreView) path.
 */
export function defineKeyedModule(
    def: Omit<KeyedModuleDefinition, 'kind'> & { kind?: 'keyed' },
    opts: DefineKeyedModuleOptions = {},
): LintedKeyedModuleDefinition {
    const strict = opts.strict ?? true;
    if (!def.name || def.name.trim() === '') {
        throw new Error('Module definition requires a non-empty name');
    }
    // Same reserved-name guard as defineModule/register: `__`-prefixed names
    // collide with the snapshot's reserved keys (`__outbox`/`__audit`).
    if (def.name.startsWith('__')) {
        throw new Error(
            `Module name "${def.name}" is reserved: names starting with "__" are reserved for runtime internals`,
        );
    }

    const commandNames = Object.keys(def.commands ?? {});
    if (commandNames.length === 0) {
        throw new Error(`Module "${def.name}" must define at least one command`);
    }
    for (const name of commandNames) {
        if (name === '') {
            throw new Error(`Module "${def.name}" has a command with an empty name`);
        }
    }

    // Determinism lint (ADR-0018 pillar 2) over every keyed reducer — the same
    // static stand-in for the deferred sandbox that whole-state modules get.
    const violations: string[] = [];
    for (const name of commandNames) {
        violations.push(...lintReducer(name, def.commands[name]));
    }

    const result: KeyedModuleDefinition = { ...def, kind: 'keyed' };
    if (violations.length > 0) {
        if (strict) {
            throw new Error(
                `Module "${def.name}" failed the determinism lint (ADR-0018 pillar 2):\n` +
                    violations.map((v) => `  - ${v}`).join('\n') +
                    `\nReducers must be pure: use ctx.now / ctx.random() / ctx.id() instead of ambient ` +
                    `globals, or pass { strict: false } to defineKeyedModule for a vetted reducer.`,
            );
        }
        return Object.assign(result, { __lint: violations });
    }
    return result;
}
