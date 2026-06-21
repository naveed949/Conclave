/**
 * Determinism lint + size accounting (ADR-0019 pillar 2 "determinism as a
 * guarantee, not a convention", and pillar 6 "resource safety / gas").
 *
 * Pillar 2 calls for turning the *convention* "reducers must be pure" into an
 * enforced *guarantee*. The ideal enforcement is a `vm`/worker sandbox with
 * frozen globals; that needs a separate execution context and is explicitly
 * DEFERRED (see the ADR's prototype-status note). This module is the cheap,
 * always-on first line of defence instead: a STATIC lint that inspects a
 * reducer's source for the common non-determinism footguns and rejects them at
 * registration, before any command is ever applied.
 *
 * Pillar 6 wants a deterministic resource bound. `canonicalBytes` below is the
 * size-accounting primitive the host uses to cap result/state amplification —
 * every replica computes the identical byte length and rejects identically, so
 * the bound never causes divergence (unlike a wall-clock CPU meter would).
 */

import { canonicalJson } from './canonical';

/**
 * Non-deterministic access patterns to detect in reducer source.
 *
 * Each entry is a targeted regex chosen to match the ACCESS to a banned global,
 * not an innocent identifier that merely shares a name. For example we match
 * `Math.random` (the property access), not a local variable or object field
 * literally named `random` — so a module that has a `random` field in its state
 * is not flagged. `ctx.now` / `ctx.id()` / `ctx.random()` are the SANCTIONED
 * deterministic substitutes and go through `ctx`, never these globals, so the
 * demo modules (counter/notes/payments) pass cleanly.
 *
 * This list is the documented footgun set, not an exhaustive proof of purity.
 */
export const BANNED: { pattern: RegExp; reason: string }[] = [
    // Clock + randomness: the two classic determinism breakers.
    { pattern: /\bMath\s*\.\s*random\b/, reason: 'Math.random (use ctx.random())' },
    { pattern: /\bDate\s*\.\s*now\b/, reason: 'Date.now (use ctx.now)' },
    // Any `new Date(` or bare `Date(` is suspicious: even `new Date(arg)` is a
    // footgun magnet, and we cannot statically prove the arg is deterministic.
    // Authors should derive time from ctx.now instead of constructing Dates.
    { pattern: /\bnew\s+Date\s*\(/, reason: 'new Date( (use ctx.now)' },
    { pattern: /(?<!\.)\bDate\s*\(/, reason: 'Date( call (use ctx.now)' },
    // Ambient crypto / entropy. Match ACCESS patterns, not the bare identifier:
    // a property access (`crypto.`), the global (`globalThis.crypto`), and a
    // direct require — never a legitimate local named `crypto` (or `cryptoKey` /
    // `cryptocurrency`). Dynamic `require('crypto')` is also caught by the
    // general `require(` rule below; these are the targeted crypto-specific hits.
    { pattern: /\bcrypto\s*\./, reason: 'crypto. (non-deterministic; use ctx.id()/ctx.random())' },
    {
        pattern: /\brequire\s*\(\s*['"]crypto['"]\s*\)/,
        reason: "require('crypto') (non-deterministic; use ctx.id()/ctx.random())",
    },
    { pattern: /\bglobalThis\s*\.\s*crypto\b/, reason: 'globalThis.crypto (non-deterministic; use ctx.id()/ctx.random())' },
    // Dynamic code / module loading: opens an unbounded escape hatch.
    { pattern: /\brequire\s*\(/, reason: 'require( (dynamic module load)' },
    { pattern: /\bimport\s*\(/, reason: 'import( (dynamic module load)' },
    // Ambient process / global object: env, argv, hrtime, etc.
    { pattern: /\bprocess\s*\./, reason: 'process. (ambient environment)' },
    { pattern: /\bglobalThis\b/, reason: 'globalThis (ambient global access)' },
    // Network / IO: must go through an effect intent, never a reducer.
    { pattern: /\bfetch\s*\(/, reason: 'fetch( (network I/O; emit an EffectIntent)' },
    // Timers: wall-clock scheduling has no place on the deterministic path.
    { pattern: /\bsetTimeout\b/, reason: 'setTimeout (wall-clock timer)' },
    { pattern: /\bsetInterval\b/, reason: 'setInterval (wall-clock timer)' },
    // High-resolution clock.
    { pattern: /\bperformance\s*\./, reason: 'performance. (wall-clock timing)' },
    // Reflection: a common way to reach banned globals indirectly.
    { pattern: /\bReflect\s*\./, reason: 'Reflect. (reflective global access)' },
    // Locale / ICU formatting & collation: results depend on the host locale and
    // ICU build, so two replicas can format/compare differently from the same
    // input. Match the METHOD access (`.toLocaleString`, `.localeCompare`, any
    // `.toLocale*(`) and the `Intl` namespace access — never a bare identifier
    // that merely shares a name. The sandbox neutralizes these structurally; this
    // is the defense-in-depth lint for non-sandboxed modules.
    { pattern: /\.\s*toLocaleString\b/, reason: '.toLocaleString (locale-dependent formatting; format deterministically)' },
    { pattern: /\.\s*localeCompare\b/, reason: '.localeCompare (locale-dependent collation; compare deterministically)' },
    { pattern: /\.\s*toLocale[A-Za-z]*\s*\(/, reason: '.toLocale*( (locale-dependent; non-deterministic)' },
    { pattern: /\bIntl\s*\./, reason: 'Intl. (locale/ICU formatting; non-deterministic)' },
];

/**
 * Statically lint a single reducer's source for non-deterministic globals,
 * returning a list of human-readable violation strings (empty = clean).
 *
 * HONEST LIMITATION: this is a STATIC stand-in for a real sandbox. It operates
 * on `fn.toString()` — the reducer's source text — so it is trivially
 * bypassable by obfuscation or indirection (e.g. `globalThis['Ma'+'th']`,
 * aliasing a global, or pulling entropy through a closed-over helper defined
 * elsewhere), and it can produce false positives on source that merely mentions
 * a banned token in a string/comment. It catches the COMMON footgun — a reducer
 * author reflexively reaching for `Date.now()` or `Math.random()` — at zero
 * runtime cost. It is NOT a security boundary; the real boundary (a `vm`/worker
 * sandbox with frozen globals) is deferred per ADR-0019.
 *
 * SCOPE: this lint deliberately covers COMMAND REDUCERS only — they are the code
 * on the convergence path, run on every replica for every command. It is NOT
 * applied to `queries` (read-only and off the convergence path, so impurity
 * there cannot diverge replicated state) nor to `initialState` (runs once at
 * registration, not per command).
 */
export function lintReducer(commandName: string, fn: Function): string[] {
    const source = fn.toString();
    const violations: string[] = [];
    for (const { pattern, reason } of BANNED) {
        if (pattern.test(source)) {
            violations.push(`command "${commandName}" references ${reason}`);
        }
    }
    return violations;
}

/**
 * Byte length of the canonical (sorted-key) JSON serialization of `value`. The
 * size primitive behind the host's deterministic result/state bound: every
 * replica computes the identical byte count from the identical state, so an
 * over-budget reducer is rejected uniformly on every node — no divergence. Uses
 * UTF-8 byte length (not string length) so multi-byte content is measured
 * honestly.
 *
 * Uses the shared `canonicalJson` in DROP-`undefined` mode: this is a
 * byte-length bound, not a hash preimage, so a property whose value is
 * `undefined` is dropped exactly as `JSON.stringify` would — lenient on purpose,
 * and it never throws on the state shapes reducers return.
 */
export function canonicalBytes(value: unknown): number {
    return Buffer.byteLength(canonicalJson(value, { onUndefined: 'drop' }), 'utf8');
}
