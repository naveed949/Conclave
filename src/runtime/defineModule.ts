import { lintReducer } from './determinism';
import { ModuleDefinition } from './types';

/**
 * Options for {@link defineModule}.
 */
export interface DefineModuleOptions {
    /**
     * Enforce the determinism lint at definition time (ADR-0018 pillar 2).
     * Defaults to TRUE: a reducer that references a non-deterministic global
     * (`Date.now`, `Math.random`, `crypto`, network, timers, …) is REJECTED with
     * a thrown error before the module can ever be registered.
     *
     * OPT-OUT: pass `{ strict: false }` for a vetted reducer whose flagged token
     * is a false positive (e.g. a banned word appearing only in a string/comment)
     * or that has been audited by other means. Opting out does not silence the
     * lint — the violations are attached to the returned definition under the
     * reserved `__lint` field for inspection/logging — it only skips the throw.
     */
    strict?: boolean;
    /**
     * Opt into the `vm` determinism sandbox (ADR-0018 pillars 2, 6 — M9). When
     * set here it is stamped onto the returned definition so the host compiles the
     * reducers into a frozen `vm` context at registration. Additive and
     * default-off; the static lint stays on as defense-in-depth (a sandboxed
     * reducer is self-contained and still passes the lint). See `sandbox.ts` for
     * the honest "not a security boundary" scoping. WHOLE-STATE modules only.
     */
    sandbox?: boolean;
    /**
     * Wall-clock budget (ms) for the leader-side admission step meter. Only
     * meaningful with `sandbox: true`. Stamped onto the returned definition.
     * Default applied by the host: 50.
     */
    stepBudgetMs?: number;
}

/**
 * A module definition that failed the determinism lint with `strict: false`.
 * The violations are recorded under the reserved `__lint` field so an operator
 * can still surface them, even though registration was allowed to proceed.
 */
export type LintedModuleDefinition<S> = ModuleDefinition<S> & { __lint?: string[] };

/**
 * Validate and return a module definition (ADR-0018 pillar 1: the single
 * declarative unit that replaces the four-touchpoint command workflow).
 *
 * Validation happens here, at definition time, so a malformed module fails loud
 * on startup rather than surfacing as a confusing dispatch error later. The
 * function is otherwise an identity — it returns the same object, typed — which
 * keeps authoring a module a single `defineModule({ ... })` call.
 *
 * Determinism lint (ADR-0018 pillar 2): every command reducer is statically
 * linted for non-deterministic globals. With `strict` (the default), any
 * violation THROWS, turning the "reducers must be pure" convention into an
 * enforced guarantee. With `{ strict: false }` the violations are recorded on
 * the returned definition's `__lint` field instead of throwing — the documented
 * escape hatch for a vetted reducer.
 */
export function defineModule<S>(
    rawDef: ModuleDefinition<S>,
    opts: DefineModuleOptions = {},
): LintedModuleDefinition<S> {
    const strict = opts.strict ?? true;
    // Stamp the sandbox opt-in (ADR-0018 M9) from the options onto the definition
    // so the host sees it at registration. A field set directly on `def` wins over
    // the option (explicit on the definition is the more specific intent); options
    // are the ergonomic place to pass it alongside `strict`.
    const def: ModuleDefinition<S> = { ...rawDef };
    const sandbox = rawDef.sandbox ?? opts.sandbox;
    if (sandbox !== undefined) def.sandbox = sandbox;
    const stepBudgetMs = rawDef.stepBudgetMs ?? opts.stepBudgetMs;
    if (stepBudgetMs !== undefined) def.stepBudgetMs = stepBudgetMs;
    if (!def.name || def.name.trim() === '') {
        throw new Error('Module definition requires a non-empty name');
    }

    // Names starting with `__` are reserved for runtime internals — the snapshot
    // stores the outbox under the reserved `__outbox` key in the same flat
    // module-states object, so a `__`-prefixed module would silently collide.
    // Fail closed at definition time rather than corrupt a snapshot later.
    if (def.name.startsWith('__')) {
        throw new Error(
            `Module name "${def.name}" is reserved: names starting with "__" are reserved for runtime internals`,
        );
    }

    const commandNames = Object.keys(def.commands ?? {});
    if (commandNames.length === 0) {
        throw new Error(`Module "${def.name}" must define at least one command`);
    }

    // Validate names exactly as they will be dispatched (untrimmed keys are what
    // `moduleHost.apply` looks up), so validation can't diverge from dispatch.
    // `Object.keys` already collapses duplicate literal keys, so only the
    // empty-name case needs rejecting here; an empty name makes dispatch ambiguous.
    for (const name of commandNames) {
        if (name === '') {
            throw new Error(`Module "${def.name}" has a command with an empty name`);
        }
    }

    // Determinism lint (ADR-0018 pillar 2): scan every reducer's source for
    // non-deterministic globals. This is the always-on static stand-in for the
    // deferred vm/worker sandbox — it catches the common footgun at definition
    // time, before a single command can be applied.
    const violations: string[] = [];
    for (const name of commandNames) {
        violations.push(...lintReducer(name, def.commands[name]));
    }

    if (violations.length > 0) {
        if (strict) {
            throw new Error(
                `Module "${def.name}" failed the determinism lint (ADR-0018 pillar 2):\n` +
                    violations.map((v) => `  - ${v}`).join('\n') +
                    `\nReducers must be pure: use ctx.now / ctx.random() / ctx.id() instead of ambient ` +
                    `globals, or pass { strict: false } to defineModule for a vetted reducer.`,
            );
        }
        // strict: false — record the violations rather than throw, so a vetted
        // module can opt out while the findings remain inspectable.
        return Object.assign({}, def, { __lint: violations });
    }

    return def;
}
