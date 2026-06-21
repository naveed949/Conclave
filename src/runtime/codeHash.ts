import { createHash } from 'crypto';
import { ModuleDefinition } from './types';

/**
 * Module code-version hashing (ADR-0018 pillar 5). The audit records a hash of
 * the LOGIC that produced each result, so the history proves not just *what* the
 * data became but *which version of the code* computed it — closing ADR-0017's
 * "tamper-evident data but not logic" gap.
 *
 * PROTOTYPE CAVEAT (stated honestly): `fn.toString()` is a STAND-IN for hashing
 * the built/deployed artifact. It captures a reducer's source text, which is
 * enough to detect that the logic changed (a different body or a bumped
 * `version` yields a different hash) and to bind that change into the audit. It
 * is NOT a production code-provenance mechanism: source text omits captured
 * closure variables and imported helpers, can be defeated by semantically-equal
 * rewrites, and is sensitive to incidental formatting. A real deployment would
 * hash the immutable build output recorded in a `DEPLOY` log entry. For this
 * POC, source-text identity is sufficient to demonstrate the audit captures
 * which logic version ran.
 *
 * DETERMINISM: command names are sorted and the material is assembled with no
 * clock/randomness, so every replica computes the identical code hash for a
 * given module definition.
 */

/** sha256 hex of a string. */
function sha256(s: string): string {
    return createHash('sha256').update(s).digest('hex');
}

/**
 * Deterministic identity hash of a module's logic. Built from the module name,
 * its `version` (defaulting to `'0'` when unset), and each command name paired
 * with its reducer's source text, with command names SORTED for a stable order.
 */
export function moduleCodeHash(def: ModuleDefinition<any>): string {
    const commandNames = Object.keys(def.commands).sort();
    const commandMaterial = commandNames
        // `JSON.stringify` on the name + body keeps the delimiter unambiguous so
        // distinct (name, body) pairs cannot alias into the same concatenation.
        .map((name) => `${JSON.stringify(name)}:${JSON.stringify(def.commands[name].toString())}`)
        .join(',');
    const material = [
        `name:${JSON.stringify(def.name)}`,
        `version:${JSON.stringify(def.version ?? '0')}`,
        `commands:[${commandMaterial}]`,
    ].join('|');
    return sha256(material);
}
