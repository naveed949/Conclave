import { createHash, randomBytes } from 'crypto';
import { ReducerContext, Seed } from './types';

/**
 * Seed resolution and deterministic context construction (ADR-0018 pillar 2).
 *
 * `resolveSeed` is the ONLY place real non-determinism enters; it runs on the
 * leader. `createContext` then rebuilds a fully deterministic `ReducerContext`
 * from that seed, so any replica (or a fresh host during replay) reconstructs
 * the identical clock/PRNG/id stream and converges byte-for-byte.
 */

/**
 * LEADER-SIDE ONLY. Capture the ambient clock and a fresh random nonce, exactly
 * as `models/book.ts` resolves ids/timestamps up front before a command enters
 * the log. The resulting seed is replicated verbatim; replicas never call this.
 */
export function resolveSeed(): Seed {
    return {
        timestamp: new Date().toISOString(),
        nonce: randomBytes(16).toString('hex'),
    };
}

/** Derive a 32-bit unsigned seed integer from the nonce via sha256. */
function seedStateFromNonce(nonce: string): number {
    const digest = createHash('sha256').update(nonce).digest();
    // Take the first 4 bytes as a big-endian uint32. `>>> 0` forces unsigned.
    return digest.readUInt32BE(0) >>> 0;
}

/**
 * Build a deterministic context from a seed. Every capability below is a pure
 * function of `seed` (plus per-context call counters), so the same seed always
 * yields the same `now`/`random()` stream/`id()` stream.
 */
export function createContext(seed: Seed, meta: { actor: string; requestId: string }): ReducerContext {
    // mulberry32: a tiny, fast, well-distributed PRNG. Deterministic given its
    // 32-bit state, which we derive from the nonce. We keep `randomState` in a
    // closure so successive `random()` calls advance the same stream.
    let randomState = seedStateFromNonce(seed.nonce);
    const random = (): number => {
        // mulberry32 step.
        randomState = (randomState + 0x6d2b79f5) >>> 0;
        let t = randomState;
        t = Math.imul(t ^ (t >>> 15), t | 1);
        t ^= t + Math.imul(t ^ (t >>> 7), t | 61);
        return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
    };

    // Deterministic id stream: sha256(nonce:counter) hex, truncated to 32 chars.
    // The counter is independent of the PRNG so the two streams don't interfere.
    let idCounter = 0;
    const id = (): string => {
        const hex = createHash('sha256')
            .update(`${seed.nonce}:${idCounter}`)
            .digest('hex');
        idCounter += 1;
        return hex.slice(0, 32);
    };

    return {
        now: seed.timestamp,
        random,
        id,
        actor: meta.actor,
        requestId: meta.requestId,
    };
}
