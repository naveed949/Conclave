/**
 * `vm`-based determinism sandbox + leader-side step/CPU meter (ADR-0019
 * pillars 2 and 6). This is the M9 hardening the prototype-status note flagged
 * as missing: M5 enforced determinism with a STATIC lint (bypassable by
 * obfuscation) and a deterministic size/write bound; this module adds the
 * STRUCTURAL enforcement the lint stands in for, plus the preemptive CPU/step
 * gas meter the deterministic size bound could not provide.
 *
 * Two distinct guarantees, two execution entry points:
 *
 *  1. STRUCTURAL DETERMINISM (apply path). `compileReducer` re-evaluates a
 *     self-contained reducer source inside a fresh `vm` context whose globals are
 *     a curated, frozen SAFE set — `Date`, `Intl`, `Math.random`, `process`,
 *     `require`, timers, `crypto`, `console`, `performance` are ABSENT, and the
 *     locale-sensitive `toLocale*` / `localeCompare` prototype methods on the kept
 *     `Number`/`String`/`Array`/`Object` intrinsics are replaced by throwers. A
 *     reducer that reaches for `Date.now()` or `(1).toLocaleString()` therefore
 *     throws at runtime, even if it slipped past the static lint. This list is the
 *     set of footguns the sandbox ENFORCES, not a proof of total purity (the file
 *     header below is explicit that this is a determinism aid, not a security
 *     boundary). `runReducer` executes this on the
 *     apply path with NO timeout: the call must be a pure, deterministic function
 *     of (state, input, ctx) so every replica computes the identical result. A
 *     wall-clock timeout there would be non-deterministic (it could trip on one
 *     replica and not another) and is therefore forbidden on the apply path.
 *
 *  2. CPU/STEP GAS (admission, leader-only). `runReducerWithBudget` executes the
 *     SAME compiled reducer but inside the vm with `{ timeout: budgetMs }`, so a
 *     runaway synchronous reducer (an infinite loop) is interrupted and rejected
 *     as `BudgetExceededError`. The leader runs this as a pre-log DRY RUN against
 *     a clone of current state; an over-budget command is rejected BEFORE it ever
 *     enters the log. Followers never re-run the meter — under the CFT trust
 *     model they trust that a committed entry was already admitted by the leader,
 *     so the apply path stays timeout-free and deterministic.
 *
 * HONEST SCOPING — NOT A SECURITY BOUNDARY. Node's own docs are explicit that
 * the `vm` module is "not a security mechanism. Do not use it to run untrusted
 * code." A vm context still exposes `Function`/`eval` (and a sufficiently
 * determined script can break out), so this sandbox does NOT defend against
 * MALICIOUS reducers. Its goal is to enforce determinism against HONEST-BUT-
 * CARELESS reducers — the author who reflexively reaches for `Date.now()` — by
 * making the non-deterministic globals structurally unavailable. This mirrors
 * the honest caveats already stated for the M3 code-hash and the M5 static lint.
 */

import * as vm from 'vm';
import { ReducerContext, ReducerResult } from './types';

/**
 * Thrown when the admission step meter (`runReducerWithBudget`) interrupts a
 * reducer that ran past its wall-clock budget. Distinct error type so the host's
 * `admit()` can map it to a dedicated rejection status (503) rather than lumping
 * it in with an ordinary reducer throw.
 */
export class BudgetExceededError extends Error {
    constructor(message: string) {
        super(message);
        this.name = 'BudgetExceededError';
    }
}

/**
 * A compiled, sandboxed reducer handle. Holds the vm context (with its frozen
 * safe globals) and the reducer function evaluated INSIDE that context, so every
 * free identifier the reducer references resolves against the sandbox globals —
 * a banned global is simply absent and throws `ReferenceError` when touched.
 */
export interface CompiledReducer {
    /** Run on the apply path: no timeout, deterministic. */
    run(state: unknown, input: unknown, ctx: ReducerContext): ReducerResult<unknown>;
    /** Run on the admission/dry-run path: vm-enforced wall-clock budget. */
    runWithBudget(
        state: unknown,
        input: unknown,
        ctx: ReducerContext,
        budgetMs: number,
    ): ReducerResult<unknown>;
}

/**
 * The curated safe-global posture, enforced by REMOVING non-deterministic
 * intrinsics from a fresh `vm` context rather than by an allowlist.
 *
 * IMPORTANT IMPLEMENTATION NOTE: `vm.createContext` produces a fresh global
 * object that V8 STILL populates with the standard built-in intrinsics
 * (`Date`, `Math`, `Object`, …) — passing only an allowlist object does NOT
 * unset them. So determinism is enforced by explicitly `delete`-ing the banned
 * globals from the context's `globalThis` and neutralizing `Math.random`, run
 * once per context as the setup script below.
 *
 * KEPT (deterministic builtins a reducer legitimately needs): `Object, Array,
 * String, Number, Boolean, JSON, Math` (random removed), `isNaN, isFinite,
 * parseInt, parseFloat` — these are left in place as standard intrinsics. Their
 * locale-sensitive methods are neutralized in place (see NEUTRALIZED below) so
 * the deterministic methods stay usable while the non-deterministic ones throw.
 *
 * REMOVED (so referencing them throws `ReferenceError`):
 *  - `Date` — wall-clock; use `ctx.now`.
 *  - `Intl` — locale/ICU formatting & collation are non-deterministic (vary by
 *    host locale and ICU build); the global is deleted entirely.
 *  - `Math.random` — randomness (the property is replaced by a thrower; the rest
 *    of `Math` stays); use `ctx.random()`.
 *  - `crypto` — entropy; use `ctx.id()` / `ctx.random()`.
 *  - `process`, `require`, `module` — ambient environment / dynamic load.
 *  - `setTimeout`, `setInterval`, `setImmediate`, `clearTimeout`,
 *    `clearInterval`, `queueMicrotask` — timers / async scheduling have no place
 *    on the synchronous deterministic path.
 *  - `console` — I/O; reducers must be pure.
 *  - `Reflect`, `Proxy` — common indirection paths to reach removed globals.
 *  - `performance` — high-resolution wall clock.
 *
 * NEUTRALIZED (kept intrinsics whose locale-sensitive methods are replaced by a
 * thrower, since deleting `Intl` alone does not close the gap — these methods
 * ride on the kept `Number`/`String`/`Array`/`Object` prototypes):
 *  - `Number.prototype.toLocaleString`, `Array.prototype.toLocaleString`,
 *    `Object.prototype.toLocaleString` (and `BigInt.prototype.toLocaleString`
 *    when present) — locale-dependent formatting; format deterministically.
 *  - `String.prototype.localeCompare` — locale-dependent collation; compare with
 *    deterministic ordering (`<`/`>`) instead.
 *  - `String.prototype.toLocaleLowerCase`, `String.prototype.toLocaleUpperCase`
 *    — locale-dependent case mapping; use `toLowerCase`/`toUpperCase`.
 * The non-locale methods on these prototypes are untouched and remain usable.
 *
 * This is the documented footgun set the sandbox enforces, NOT a proof of total
 * determinism — see the file header's HONEST SCOPING note.
 *
 * `globalThis` itself, `Function`, and `eval` CANNOT be meaningfully removed
 * (they are intrinsic to any vm context and a script can re-derive them), which
 * is exactly why this is a DETERMINISM aid and NOT a security boundary — see the
 * file header.
 */

/** Globals deleted from each sandbox context's `globalThis`. */
const BANNED_GLOBALS = [
    'Date',
    'Intl',
    'crypto',
    'process',
    'require',
    'module',
    'setTimeout',
    'setInterval',
    'setImmediate',
    'clearTimeout',
    'clearInterval',
    'queueMicrotask',
    'console',
    'Reflect',
    'Proxy',
    'performance',
];

/**
 * Setup script run ONCE per context: delete every banned intrinsic from
 * `globalThis`, replace `Math.random` with a thrower (leaving the rest of `Math`
 * — all pure functions — intact), and neutralize the locale-sensitive `toLocale*`
 * / `localeCompare` prototype methods that ride on the KEPT `Number`/`String`/
 * `Array`/`Object` intrinsics. After this runs, a reducer body that references a
 * banned global resolves it as a free identifier against the (now-missing) global
 * and throws `ReferenceError`; `Math.random()` throws too; and any call to a
 * locale-formatting method throws a clear `Error`.
 *
 * WHY the locale methods: removing the `Intl` global is not enough — methods like
 * `Number.prototype.toLocaleString`, `String.prototype.localeCompare`, and
 * `Array.prototype.toLocaleString` perform locale/ICU-dependent formatting and
 * comparison that VARY across Node builds and host locales, so two replicas could
 * compute different strings/orderings from the same input. That is exactly the
 * non-determinism this sandbox must block, so each is replaced by a thrower while
 * the deterministic methods on those prototypes stay intact.
 */
const SANDBOX_SETUP = `
(() => {
    const banned = ${JSON.stringify(BANNED_GLOBALS)};
    for (const name of banned) {
        try { delete globalThis[name]; } catch (_e) { /* non-configurable: ignore */ }
    }
    // Replace Math.random with a loud thrower (do NOT touch the shared real Math
    // object outside this context — this mutates only this context's Math).
    try {
        Object.defineProperty(Math, 'random', {
            configurable: true,
            get() {
                throw new ReferenceError(
                    'Math.random is not available in the determinism sandbox (use ctx.random())',
                );
            },
        });
    } catch (_e) { /* ignore */ }
    // Neutralize locale-sensitive prototype methods: replace each with a thrower
    // so a reducer cannot reach locale/ICU-dependent formatting through the kept
    // Number/String/Array/Object intrinsics. The deterministic methods on those
    // prototypes are untouched.
    const localeThrower = (label) => function () {
        throw new Error(
            label + ' is not available in the determinism sandbox: locale-aware ' +
            'formatting/comparison is non-deterministic (it varies by host locale ' +
            'and ICU build). Format via deterministic logic instead.',
        );
    };
    const neutralize = (proto, method, label) => {
        try {
            Object.defineProperty(proto, method, {
                configurable: true,
                writable: true,
                enumerable: false,
                value: localeThrower(label),
            });
        } catch (_e) { /* non-configurable: ignore */ }
    };
    neutralize(Number.prototype, 'toLocaleString', 'Number.prototype.toLocaleString');
    neutralize(String.prototype, 'localeCompare', 'String.prototype.localeCompare');
    neutralize(String.prototype, 'toLocaleLowerCase', 'String.prototype.toLocaleLowerCase');
    neutralize(String.prototype, 'toLocaleUpperCase', 'String.prototype.toLocaleUpperCase');
    neutralize(Array.prototype, 'toLocaleString', 'Array.prototype.toLocaleString');
    neutralize(Object.prototype, 'toLocaleString', 'Object.prototype.toLocaleString');
    // BigInt may be absent depending on context shape; guard it. Its
    // toLocaleString is equally locale-dependent.
    try {
        if (typeof BigInt !== 'undefined' && BigInt.prototype) {
            neutralize(BigInt.prototype, 'toLocaleString', 'BigInt.prototype.toLocaleString');
        }
    } catch (_e) { /* ignore */ }
})();
`;

/**
 * Coverage tools (Istanbul/`nyc`, as used by `jest --coverage`) rewrite every
 * instrumented function body to call a per-file counter, e.g.
 * `cov_15m9dncfb6().f[1]++` / `(cov_15m9dncfb6().s[5]++, expr)`. Because the
 * sandbox compiles a reducer from `fn.toString()`, that injected counter
 * identifier becomes a FREE global reference inside the vm context — where it
 * does not exist — so an otherwise-pure reducer would throw `ReferenceError`
 * purely as an artifact of running under coverage (and the apply path would
 * surface it as a 500). We detect those counter identifiers and bind each to a
 * harmless no-op sink so instrumented reducers still execute deterministically.
 *
 * Only names matching Istanbul's `cov_<hash>` shape are stubbed, so this never
 * widens the sandbox surface for a reducer's own free identifiers — a banned
 * global like `Date` is untouched and still throws.
 */
const COVERAGE_COUNTER_RE = /\bcov_[0-9a-zA-Z_$]+/g;

/**
 * Bind a no-op sink for each coverage-counter identifier referenced by `source`.
 * MUST run BEFORE `SANDBOX_SETUP` deletes `Proxy` from the context — the sink is
 * a single Proxy that absorbs any call/get/set/construct and returns itself, so
 * `cov_x().f[1]++`, `cov_x().b[2][0]++`, etc. are all harmless no-ops. The sink
 * itself survives `SANDBOX_SETUP` because its bound names are not banned globals.
 */
function installCoverageStubs(context: vm.Context, source: string): void {
    const names = Array.from(new Set(source.match(COVERAGE_COUNTER_RE) ?? []));
    if (names.length === 0) return;
    vm.runInContext(
        `(() => {
            // A single self-returning Proxy absorbs the counter's whole access
            // shape — call (cov_x()), property (.f/.s/.b), and index ([i][j]).
            // It coerces to 0 for the well-known conversion hooks so the trailing
            // '++' (which does ToNumber on the leaf) succeeds instead of throwing
            // 'Cannot convert object to primitive value'.
            const sink = new Proxy(function () {}, {
                get(_t, prop) {
                    if (prop === Symbol.toPrimitive) return () => 0;
                    if (prop === 'valueOf') return () => 0;
                    if (prop === 'toString') return () => '0';
                    return sink;
                },
                set() { return true; },
                apply() { return sink; },
                construct() { return sink; },
            });
            for (const name of ${JSON.stringify(names)}) {
                globalThis[name] = sink;
            }
        })();`,
        context,
    );
}

/**
 * Detect whether a stringified reducer is a STANDALONE expression we can wrap in
 * parentheses and evaluate — i.e. an arrow function (`(s, i) => ...`) or a
 * `function` expression (`function (s) { ... }`, named or anonymous). Method
 * shorthand from an object literal stringifies as `name(args) { ... }`, which is
 * NOT a valid standalone expression; we detect that and reject with a clear
 * message rather than producing a confusing `SyntaxError` deep in the vm.
 */
function isStandaloneFunctionSource(source: string): boolean {
    const trimmed = source.trim();
    // Arrow function: contains `=>` before any `{` body and starts with `(` or an
    // identifier/`async`. A function expression starts with `function`/`async`.
    if (/^async\s+function\b/.test(trimmed)) return true;
    if (/^function\b/.test(trimmed)) return true;
    // Arrow: `(...) =>` or `ident =>` or `async (...) =>`.
    if (/^async\s*\(/.test(trimmed) || /^\(/.test(trimmed) || /^[A-Za-z_$][\w$]*\s*=>/.test(trimmed)) {
        // Must actually be an arrow (has `=>`), not a parenthesized non-function.
        if (trimmed.includes('=>')) return true;
    }
    return false;
}

/**
 * Compile a SELF-CONTAINED reducer source into the determinism sandbox.
 *
 * `source` is typically `fn.toString()` of an arrow or `function` expression
 * that references ONLY its parameters, the safe globals, and `ctx` — it must not
 * close over helpers defined outside itself (the vm re-evaluates the text in a
 * fresh context, so any free identifier that is not a safe global is undefined /
 * throws). The demo `compute` module uses arrow expressions, satisfying this.
 *
 * The returned handle exposes both execution modes; the host picks `run` for the
 * apply path and `runWithBudget` for leader-side admission.
 */
export function compileReducer(source: string): CompiledReducer {
    if (!isStandaloneFunctionSource(source)) {
        throw new Error(
            'Sandboxed reducer source is not a standalone function expression. ' +
                'Sandboxed reducers must be written as arrow functions or `function` ' +
                'expressions (not object method-shorthand), and must be self-contained ' +
                '(reference only their parameters, ctx, and the safe globals).',
        );
    }

    // Fresh context, then STRIP the banned intrinsics (createContext leaves the
    // standard globals in place, so removal — not an allowlist — is what makes
    // them absent). After setup, a banned global referenced in the reducer body
    // throws `ReferenceError`.
    const context = vm.createContext({});
    // Bind no-op stubs for any coverage-counter identifiers BEFORE the setup
    // script removes `Proxy`, so a reducer instrumented by `jest --coverage`
    // still runs instead of throwing `ReferenceError` on `cov_<hash>`.
    installCoverageStubs(context, source);
    vm.runInContext(SANDBOX_SETUP, context);

    // Evaluate the reducer expression and stash it on the context as `__reducer`.
    // Parenthesize the source so an arrow/function expression is an expression,
    // not a statement (a bare `function foo(){}` would be a declaration).
    vm.runInContext(`globalThis.__reducer = (${source});`, context);

    /**
     * Build the per-call invocation. We pass `state`/`input`/`ctx` INTO the
     * context as globals and call `__reducer` there, so the reducer body runs
     * with the sandbox's resolution rules. `ctx` carries closures (`random`,
     * `id`) created OUTSIDE the context; calling them is fine — they execute in
     * their own (deterministic) closure, the sandbox only governs the free
     * identifiers in the reducer's own body.
     */
    const invoke = (
        state: unknown,
        input: unknown,
        ctx: ReducerContext,
        timeoutMs?: number,
    ): ReducerResult<unknown> => {
        // Publish the call arguments as context globals. They are plain data /
        // the ctx capability object; assigning them does not widen the sandbox's
        // ambient surface for the reducer's free identifiers.
        (context as Record<string, unknown>).__state = state;
        (context as Record<string, unknown>).__input = input;
        (context as Record<string, unknown>).__ctx = ctx;

        const options: vm.RunningScriptOptions = {};
        // Only the admission path passes a timeout: a positive budget arms the
        // vm's wall-clock interrupt. The apply path passes none, so the call runs
        // to completion deterministically.
        if (timeoutMs !== undefined) {
            options.timeout = timeoutMs;
        }

        try {
            return vm.runInContext(
                '__reducer(__state, __input, __ctx)',
                context,
                options,
            ) as ReducerResult<unknown>;
        } catch (err) {
            // The vm throws a timeout error whose `code` is
            // `ERR_SCRIPT_EXECUTION_TIMEOUT` and whose message includes "timed
            // out" when the budget trips. We deliberately DO NOT use
            // `instanceof Error` to detect it: the vm fabricates that error in a
            // DIFFERENT V8 realm, so `instanceof Error` is FALSE for it in this
            // realm (cross-realm prototype identity). We sniff `code`/`message`
            // duck-typed instead. Normalize that single case to
            // `BudgetExceededError`; every other throw (including a reducer's
            // `ReferenceError` for a banned global) propagates as-is so the apply
            // path can audit it as an ordinary 500.
            const e = err as { code?: string; message?: string };
            if (
                timeoutMs !== undefined &&
                (e?.code === 'ERR_SCRIPT_EXECUTION_TIMEOUT' ||
                    (typeof e?.message === 'string' && /timed out/i.test(e.message)))
            ) {
                throw new BudgetExceededError(
                    `Reducer exceeded the ${timeoutMs}ms step budget`,
                );
            }
            throw err;
        }
    };

    return {
        run: (state, input, ctx) => invoke(state, input, ctx),
        runWithBudget: (state, input, ctx, budgetMs) => invoke(state, input, ctx, budgetMs),
    };
}

/**
 * Execute a compiled reducer on the APPLY path: NO timeout, fully deterministic.
 * Every replica runs this and must reach the identical result, so a wall-clock
 * interrupt is deliberately not used here.
 */
export function runReducer(
    compiled: CompiledReducer,
    state: unknown,
    input: unknown,
    ctx: ReducerContext,
): ReducerResult<unknown> {
    return compiled.run(state, input, ctx);
}

/**
 * Execute a compiled reducer on the ADMISSION/dry-run path with a wall-clock
 * `budgetMs`. A runaway synchronous reducer is interrupted by the vm and surfaced
 * as `BudgetExceededError`. LEADER-ONLY: this is the pre-log gas check; followers
 * never call it (CFT trust). Because it runs against a CLONE of state and its
 * result is discarded, the non-determinism of the timeout never touches
 * replicated state.
 */
export function runReducerWithBudget(
    compiled: CompiledReducer,
    state: unknown,
    input: unknown,
    ctx: ReducerContext,
    budgetMs: number,
): ReducerResult<unknown> {
    return compiled.runWithBudget(state, input, ctx, budgetMs);
}
