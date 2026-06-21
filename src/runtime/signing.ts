/**
 * Actor-signed module commands (ADR-0019 pillar 7: "Sign commands with the
 * originating actor's key so the leader cannot forge `actor`").
 *
 * This adds ACCOUNTABILITY without paying for full BFT. The originating actor
 * signs the LOGICAL command with its private key; every replica verifies the
 * signature on the deterministic apply path against an actor->public-key
 * registry. A command whose `actor` was forged by a malicious leader (or whose
 * input/requestId was tampered with) fails verification IDENTICALLY on every
 * node and is rejected (401) before its reducer runs — so the leader cannot put
 * words in an actor's mouth.
 *
 * WHY THE SIGNATURE EXCLUDES `seed`: the leader resolves the deterministic
 * `seed` (clock + nonce) AFTER the client signs, so the seed cannot be part of
 * the signed payload — the actor never sees it. The actor signs only the
 * LOGICAL command (`module`/`command`/`input`/`actor`/`requestId`). The seed is
 * a NON-SECURITY, convergence-only value the leader bakes in afterward; it does
 * not need to be authenticated. Verification recomputes the exact same logical
 * payload from the committed command + meta and checks the signature.
 *
 * WHY THIS IS CONVERGENCE-SAFE: ed25519 sign and verify are deterministic, and
 * the verification input (canonical JSON of the logical payload) is a pure
 * function of replicated state (the committed command fields + meta). Every
 * replica computes the same boolean, so an invalid signature is rejected on all
 * nodes or none — never some — and the cluster never diverges.
 *
 * Stdlib only: Node `crypto` ed25519 (ADR-0013, minimal dependencies).
 */

import { createPrivateKey, createPublicKey, generateKeyPairSync, sign, verify } from 'crypto';
import { canonicalJson } from './canonical';

/**
 * The LOGICAL command an actor signs. Deliberately EXCLUDES the leader-resolved
 * `seed`: the actor signs before the leader exists in the flow, so the seed is
 * not available and (being non-security) does not need to be authenticated.
 */
export interface SignablePayload {
    module: string;
    command: string;
    input: unknown;
    actor: string;
    requestId: string;
}

/**
 * Generate an ed25519 keypair for an actor, returned as PEM strings. The public
 * half is registered (per actor) in a {@link KeyRegistry} on every node; the
 * private half stays with the actor and signs its commands.
 */
export function generateActorKeypair(): { publicKey: string; privateKey: string } {
    const { publicKey, privateKey } = generateKeyPairSync('ed25519');
    return {
        publicKey: publicKey.export({ type: 'spki', format: 'pem' }).toString(),
        privateKey: privateKey.export({ type: 'pkcs8', format: 'pem' }).toString(),
    };
}

/**
 * Sign the canonical serialization of the logical payload with `privateKeyPem`.
 * Canonical (sorted-key) JSON guarantees the signer and every verifier hash the
 * SAME bytes regardless of property insertion order. ed25519 uses `null` as the
 * algorithm argument (the digest is built in). Returns a base64 signature.
 */
export function signCommand(privateKeyPem: string, payload: SignablePayload): string {
    const key = createPrivateKey(privateKeyPem);
    const data = Buffer.from(canonicalJson(payload));
    return sign(null, data, key).toString('base64');
}

/**
 * Verify `signatureB64` over the canonical logical payload against
 * `publicKeyPem`. Pure and deterministic, so it is safe to run on the apply
 * path: every replica computes the same boolean from the same committed inputs.
 * Returns `false` (never throws) on a malformed key/signature so a bad command
 * is a deterministic rejection, not a host crash.
 */
export function verifyCommand(publicKeyPem: string, payload: SignablePayload, signatureB64: string): boolean {
    try {
        const key = createPublicKey(publicKeyPem);
        const data = Buffer.from(canonicalJson(payload));
        return verify(null, data, key, Buffer.from(signatureB64, 'base64'));
    } catch {
        return false;
    }
}

/**
 * The prototype's PKI / allowlist: a map from `actor` -> the public key
 * AUTHORIZED to sign on that actor's behalf. Binding actor->authorized-key here
 * is the whole point of pillar 7 — it is what makes a forged-`actor` command
 * detectable: a command claiming `actor: 'alice'` only verifies if it was signed
 * by the key the registry holds for `alice`. A leader that fabricates an
 * `actor` it has no key for cannot produce a matching signature.
 *
 * The registry is configured per node BEFORE start (like module registration)
 * and is identical on every node, so verification converges. When NO registry is
 * configured on a host, verification is skipped entirely (back-compat).
 */
export class KeyRegistry {
    private readonly keys = new Map<string, string>();

    /** Authorize `publicKeyPem` as the sole signer for `actor`. */
    registerActor(actor: string, publicKeyPem: string): void {
        this.keys.set(actor, publicKeyPem);
    }

    /** The public key authorized for `actor`, or `undefined` if none is registered. */
    get(actor: string): string | undefined {
        return this.keys.get(actor);
    }

    /** Whether `actor` has an authorized key on file. */
    has(actor: string): boolean {
        return this.keys.has(actor);
    }
}
