/**
 * Single shared canonical-JSON serializer (ADR-0019 runtime).
 *
 * Canonical JSON means: stringify with object keys recursively SORTED, so two
 * logically-equal values serialize to the SAME bytes regardless of property
 * insertion order. This is the determinism linchpin for both consumers below —
 * without sorted keys, two replicas building the same object with different key
 * order would produce different bytes and diverge.
 *
 * WHY ONE MODULE: there were previously two near-identical copies of this logic
 * (one for audit-leaf hash preimages in `merkleAudit.ts`, one for the size bound
 * in `determinism.ts`). Two copies of a determinism-critical serializer are a
 * drift hazard — a fix or feature applied to one but not the other could make a
 * leaf hash and a size measurement disagree about the same value. This module is
 * the single source of truth; the only sanctioned variation is how `undefined`
 * is handled, expressed as an explicit option (see below).
 *
 * THE TWO `undefined` MODES (and why they differ):
 *  - `'throw'` (default) — for HASH PREIMAGES (audit leaves). A hash preimage
 *    must be LOSSLESS and unambiguous: bare `JSON.stringify(undefined)` yields
 *    the value `undefined` (not a string), and a property whose value is
 *    `undefined` is silently dropped. Either would corrupt a leaf hash or make
 *    it ambiguous, so we refuse `undefined` loudly rather than hash garbage.
 *  - `'drop'` — for the SIZE bound. A byte-length measurement is not a preimage,
 *    so it can be lenient: a property whose value is `undefined` is dropped and
 *    an `undefined` array element is normalized to `null`, exactly mirroring
 *    `JSON.stringify`. This never throws on the state shapes reducers return.
 */

/** Controls how a `undefined` value is treated during serialization. */
export interface CanonicalOptions {
    /**
     * `'throw'` (default): reject `undefined` anywhere — required for lossless
     * hash preimages. `'drop'`: omit `undefined` object properties and normalize
     * `undefined` array elements to `null`, matching `JSON.stringify` — used for
     * the lenient size bound.
     */
    onUndefined?: 'throw' | 'drop';
}

/**
 * Serialize `value` to canonical (recursively sorted-key) JSON.
 *
 * The byte output is identical to the previous per-module serializers for each
 * caller's chosen `onUndefined` mode — `'throw'` reproduces `merkleAudit.ts`'s
 * old `canonical()` exactly (so audit-leaf hashes and the Merkle root are
 * unchanged), and `'drop'` reproduces `determinism.ts`'s old `canonicalJson()`.
 */
export function canonicalJson(value: unknown, opts: CanonicalOptions = {}): string {
    const onUndefined = opts.onUndefined ?? 'throw';

    if (value === undefined) {
        if (onUndefined === 'throw') {
            throw new Error('canonical: undefined is not serializable (would corrupt the hash preimage)');
        }
        // 'drop' mode only reaches here for a top-level/array `undefined`; array
        // callers normalize to null below, so this serves the top-level case.
        return JSON.stringify(null);
    }

    if (value === null || typeof value !== 'object') {
        return JSON.stringify(value);
    }

    if (Array.isArray(value)) {
        return `[${value
            .map((v) => {
                // In 'drop' mode, `undefined` array holes/elements become `null`
                // exactly as `JSON.stringify` would; in 'throw' mode the recursive
                // call rejects them.
                if (v === undefined && onUndefined === 'drop') {
                    return 'null';
                }
                return canonicalJson(v, opts);
            })
            .join(',')}]`;
    }

    const obj = value as Record<string, unknown>;
    const keys = Object.keys(obj).sort();
    const parts: string[] = [];
    for (const k of keys) {
        // In 'drop' mode, skip `undefined`-valued properties (matching
        // `JSON.stringify`); in 'throw' mode the recursive call rejects them.
        if (obj[k] === undefined && onUndefined === 'drop') {
            continue;
        }
        parts.push(`${JSON.stringify(k)}:${canonicalJson(obj[k], opts)}`);
    }
    return `{${parts.join(',')}}`;
}
