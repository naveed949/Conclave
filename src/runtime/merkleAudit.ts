import { createHash } from 'crypto';

/**
 * Merkle accumulator audit (ADR-0018 pillar 5). Upgrades the consensus core's
 * LINEAR hash chain (`replicatedStateMachine.ts`) to a Merkle tree: instead of
 * an O(n) walk to prove an entry, a verifier needs only a compact root plus an
 * O(log n) sibling path. The root is a single hash that can be externally
 * anchored, and any inclusion proof verifies against it without the full log —
 * the property the linear chain could not offer.
 *
 * DETERMINISM: the audit is replicated state, so every leaf hash and the root
 * MUST be a pure function of the applied command stream. We therefore hash a
 * CANONICAL serialization (object keys sorted) of each leaf — no `Date`, no
 * `Math.random`, no key-order dependence — so all replicas compute byte-identical
 * hashes and converge on the same root.
 */

/**
 * One audited fact: which logic version (`codeHash`) produced which result
 * (`status`) for which command, and on whose behalf. `seq` is the leaf's index
 * in the accumulator (and its position in the tree). `codeHash` is what closes
 * ADR-0017's "data but not logic" gap — see {@link moduleCodeHash}.
 */
export interface AuditLeaf {
    seq: number;
    module: string;
    command: string;
    actor: string;
    requestId: string;
    status: number;
    codeHash: string;
}

/**
 * An inclusion proof: the leaf's own hash plus the ordered sibling hashes from
 * leaf level up to the root. `side` says whether the sibling sits to the LEFT or
 * RIGHT of the running hash at that level, so a verifier knows the concatenation
 * order (`sha256(0x01 + left + right)`, the internal-node domain). A level where
 * this node was promoted unchanged
 * (odd-count tail — see below) contributes NO sibling, exactly as the tree built.
 */
export interface MerkleProof {
    leafHash: string;
    siblings: { hash: string; side: 'left' | 'right' }[];
}

/** sha256 hex of the given string. The single hashing primitive used throughout. */
function sha256(s: string): string {
    return createHash('sha256').update(s).digest('hex');
}

/**
 * RFC-6962-style domain-separation tags. Leaves and internal nodes are hashed in
 * DISTINCT domains (`0x00` for leaves, `0x01` for internal nodes) so a hash can
 * never be valid as both. Without this, an internal-node hash (a bare 64-hex
 * string) could be presented as a `MerkleProof.leafHash` and verify against the
 * root — the classic second-preimage ambiguity. The prefix byte closes that gap.
 */
const LEAF_TAG = '\x00';
const NODE_TAG = '\x01';

/** Hash an internal node from its two child hashes, tagged in the node domain. */
function hashNode(left: string, right: string): string {
    return sha256(NODE_TAG + left + right);
}

/**
 * Root of the EMPTY tree: a fixed, documented constant `sha256('')`. Returning a
 * stable sentinel (rather than throwing) lets `auditRoot()` be called before any
 * command is applied and keeps the root well-defined for the zero-leaf case.
 * Deliberately UNTAGGED (neither leaf nor node domain): an empty tree has no
 * leaf, so it cannot collide with any real leaf or internal hash.
 */
export const EMPTY_ROOT = sha256('');

/**
 * Canonical JSON: stringify with object keys recursively sorted, so logically
 * equal leaves serialize to the SAME bytes regardless of property insertion
 * order. This is the determinism linchpin — without it, two replicas building a
 * leaf object with different key order would hash differently and diverge.
 *
 * `undefined` is REJECTED (we throw rather than normalize). Bare
 * `JSON.stringify(undefined)` yields the value `undefined` (not a string), and a
 * property whose value is `undefined` is silently dropped — both produce output
 * that is non-deterministic or invalid JSON, which would corrupt a leaf hash.
 * No current `AuditLeaf` field is ever `undefined`; throwing here keeps that
 * guarantee enforced and loud for any future reuse, rather than hashing garbage.
 */
function canonical(value: unknown): string {
    if (value === undefined) {
        throw new Error('canonical: undefined is not serializable (would corrupt the leaf hash)');
    }
    if (value === null || typeof value !== 'object') {
        return JSON.stringify(value);
    }
    if (Array.isArray(value)) {
        return `[${value.map(canonical).join(',')}]`;
    }
    const obj = value as Record<string, unknown>;
    const parts = Object.keys(obj)
        .sort()
        .map((k) => `${JSON.stringify(k)}:${canonical(obj[k])}`);
    return `{${parts.join(',')}}`;
}

/**
 * Hash a leaf via its canonical serialization, in the LEAF domain (`0x00`
 * prefix). The tag keeps leaf hashes disjoint from internal-node hashes so a
 * proof's `leafHash` can never be a smuggled internal node. Pure and
 * replica-stable.
 */
function hashLeaf(leaf: AuditLeaf): string {
    return sha256(LEAF_TAG + canonical(leaf));
}

/**
 * Append-only Merkle accumulator over audit leaves.
 *
 * DOMAIN SEPARATION (RFC-6962-style): leaves and internal nodes are hashed in
 * disjoint domains via a one-byte prefix — leaves as `sha256(0x00 + canonical)`,
 * internal nodes as `sha256(0x01 + left + right)`. Because the two domains can
 * never collide, an internal-node hash can never be passed off as a leaf hash
 * (or vice versa), which closes the second-preimage ambiguity for proofs.
 *
 * ODD-NODE SCHEME (documented, and applied identically in build and proof): when
 * a tree level has an odd number of nodes, the unpaired LAST node is **promoted
 * unchanged** to the next level (it is NOT duplicated and re-hashed with itself).
 * Combined with the leaf/internal domain tags above, this avoids the classic
 * "duplicate-last" second-preimage ambiguity (where a tree and a tampered variant
 * could share a root). A promoted level contributes no proof element, because no
 * concatenation happened there.
 *
 * ATTESTATION SCOPE (read this before trusting the root for more than it offers):
 * the audit attests *which command + logic version (`codeHash`) + status* was
 * applied, and in what ORDER. It does NOT by itself attest STATE agreement: two
 * replicas that processed an identical command stream share the same audit root
 * even if a non-determinism bug left their materialized state divergent. State
 * convergence is a separate property, verified elsewhere — the audit root is a
 * cross-check on the command/logic stream, not a proof of equal state.
 */
export class MerkleAudit {
    /** Leaves in append order; `leaf.seq` equals its index here. */
    private readonly storedLeaves: AuditLeaf[] = [];
    /** Cached leaf hashes, parallel to `storedLeaves`. */
    private readonly leafHashes: string[] = [];

    /**
     * Append a leaf, returning its index (which equals `leaf.seq`). The leaf is
     * canonical-hashed once and cached; the tree itself is recomputed on demand
     * by `root()`/`proof()`, which keeps append O(1) and the structure trivially
     * correct for any leaf count.
     */
    append(leaf: AuditLeaf): number {
        const index = this.storedLeaves.length;
        this.storedLeaves.push({ ...leaf });
        this.leafHashes.push(hashLeaf(leaf));
        return index;
    }

    /**
     * Build all tree levels bottom-up from the cached leaf hashes. `levels[0]` is
     * the leaf level; the last entry is the single-node root level. Returns an
     * empty array for an empty tree (callers handle that as {@link EMPTY_ROOT}).
     */
    private buildLevels(): string[][] {
        if (this.leafHashes.length === 0) {
            return [];
        }
        const levels: string[][] = [this.leafHashes.slice()];
        let current = levels[0];
        while (current.length > 1) {
            const next: string[] = [];
            for (let i = 0; i < current.length; i += 2) {
                if (i + 1 < current.length) {
                    next.push(hashNode(current[i], current[i + 1]));
                } else {
                    // Odd tail: promote the last node unchanged (no self-pairing).
                    next.push(current[i]);
                }
            }
            levels.push(next);
            current = next;
        }
        return levels;
    }

    /**
     * Merkle root over all leaf hashes. Empty tree → {@link EMPTY_ROOT}
     * (`sha256('')`). Stable for a given leaf set regardless of how the tree is
     * cached or recomputed.
     */
    root(): string {
        const levels = this.buildLevels();
        if (levels.length === 0) {
            return EMPTY_ROOT;
        }
        return levels[levels.length - 1][0];
    }

    /**
     * Inclusion proof for the leaf at `index`. Walks level by level, recording
     * the sibling hash and which side it sits on. Where this node is the promoted
     * odd tail (no sibling at that level), it records nothing and simply rises —
     * the SAME rule `buildLevels` used, so `verify` reconstructs the identical
     * root. Throws on an out-of-range index.
     */
    proof(index: number): MerkleProof {
        if (index < 0 || index >= this.storedLeaves.length) {
            throw new Error(`Audit proof index out of range: ${index}`);
        }
        const levels = this.buildLevels();
        const siblings: { hash: string; side: 'left' | 'right' }[] = [];
        let pos = index;
        // Walk from the leaf level up to (but not including) the root level.
        for (let level = 0; level < levels.length - 1; level += 1) {
            const nodes = levels[level];
            const isRightChild = pos % 2 === 1;
            const siblingPos = isRightChild ? pos - 1 : pos + 1;
            if (siblingPos < nodes.length) {
                siblings.push({
                    hash: nodes[siblingPos],
                    // If we are the right child, the sibling is to our left.
                    side: isRightChild ? 'left' : 'right',
                });
            }
            // else: we are the promoted odd tail — no sibling, no concatenation.
            pos = Math.floor(pos / 2);
        }
        return { leafHash: this.leafHashes[index], siblings };
    }

    /**
     * Recompute the root from a proof and compare to `root`. Folds each sibling
     * into the running hash on the recorded side, then checks equality. Returns
     * false on any mismatch, so a tampered leaf or wrong-index proof fails.
     */
    static verify(root: string, proof: MerkleProof): boolean {
        let running = proof.leafHash;
        for (const sib of proof.siblings) {
            running = sib.side === 'left' ? hashNode(sib.hash, running) : hashNode(running, sib.hash);
        }
        return running === root;
    }

    /** A defensive copy of every leaf in append order. */
    leaves(): AuditLeaf[] {
        return this.storedLeaves.map((l) => ({ ...l }));
    }

    /** Number of appended leaves. */
    size(): number {
        return this.storedLeaves.length;
    }

    /**
     * Snapshot the accumulator as just its leaves — the tree (and every hash) is
     * a pure function of them, so storing the leaves alone is sufficient and
     * keeps the snapshot minimal. `restore` rebuilds the hashes deterministically.
     */
    snapshot(): { leaves: AuditLeaf[] } {
        return { leaves: this.leaves() };
    }

    /**
     * Rebuild from a snapshot: drop all state, then re-append each leaf so the
     * cached hashes are recomputed canonically. Two hosts restoring the same
     * snapshot therefore reproduce the identical root.
     */
    restore(snap: { leaves: AuditLeaf[] }): void {
        this.storedLeaves.length = 0;
        this.leafHashes.length = 0;
        for (const leaf of snap.leaves) {
            this.append(leaf);
        }
    }
}
