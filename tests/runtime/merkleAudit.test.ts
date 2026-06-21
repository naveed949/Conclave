import { AuditLeaf, EMPTY_ROOT, MerkleAudit, MerkleProof } from '../../src/runtime/merkleAudit';

/** Build a leaf with a varying `seq`/`command` so leaf hashes differ. */
const leaf = (seq: number): AuditLeaf => ({
    seq,
    module: 'm',
    command: `cmd-${seq}`,
    actor: 'tester',
    requestId: `req-${seq}`,
    status: 200,
    codeHash: 'abc',
});

/** An accumulator pre-filled with `n` distinct leaves. */
function filled(n: number): MerkleAudit {
    const audit = new MerkleAudit();
    for (let i = 0; i < n; i += 1) audit.append(leaf(i));
    return audit;
}

describe('MerkleAudit tree', () => {
    it('empty tree returns the documented sha256("") root', () => {
        const audit = new MerkleAudit();
        expect(audit.size()).toBe(0);
        expect(audit.root()).toBe(EMPTY_ROOT);
    });

    // Cover 1, 2 (even), 3 and 5 (ODD — exercise the promote-unchanged tail), and
    // a larger set, since proof/build differ across these shapes.
    for (const n of [1, 2, 3, 5, 16, 17]) {
        it(`inclusion proofs verify against the root for ${n} leaves`, () => {
            const audit = filled(n);
            const root = audit.root();
            for (let i = 0; i < n; i += 1) {
                const proof = audit.proof(i);
                expect(MerkleAudit.verify(root, proof)).toBe(true);
            }
        });
    }

    it('a tampered leaf changes the root and invalidates the old proof', () => {
        const audit = filled(5);
        const rootBefore = audit.root();
        const proofBefore = audit.proof(2);
        expect(MerkleAudit.verify(rootBefore, proofBefore)).toBe(true);

        // Rebuild a tampered tree: same leaves, but leaf 2 mutated.
        const tampered = new MerkleAudit();
        for (let i = 0; i < 5; i += 1) {
            tampered.append(i === 2 ? { ...leaf(i), status: 500 } : leaf(i));
        }
        const rootAfter = tampered.root();

        expect(rootAfter).not.toBe(rootBefore);
        // The pre-tamper proof no longer verifies against the new root.
        expect(MerkleAudit.verify(rootAfter, proofBefore)).toBe(false);
        // And the genuine new proof for index 2 has a different leaf hash.
        expect(tampered.proof(2).leafHash).not.toBe(proofBefore.leafHash);
    });

    it('a proof for one index does not verify a different leaf', () => {
        const audit = filled(5);
        const root = audit.root();
        const proofFor1 = audit.proof(1);

        // Splice leaf 3's hash into index 1's sibling path — verification must fail.
        const forged: MerkleProof = { leafHash: audit.proof(3).leafHash, siblings: proofFor1.siblings };
        expect(MerkleAudit.verify(root, forged)).toBe(false);
    });

    it('snapshot -> restore reproduces the identical root and proofs', () => {
        const audit = filled(7);
        const snap = audit.snapshot();

        const restored = new MerkleAudit();
        restored.restore(snap);

        expect(restored.size()).toBe(7);
        expect(restored.root()).toBe(audit.root());
        for (let i = 0; i < 7; i += 1) {
            expect(restored.proof(i)).toEqual(audit.proof(i));
            expect(MerkleAudit.verify(restored.root(), restored.proof(i))).toBe(true);
        }
    });

    it('append returns the leaf index (== seq)', () => {
        const audit = new MerkleAudit();
        expect(audit.append(leaf(0))).toBe(0);
        expect(audit.append(leaf(1))).toBe(1);
        expect(audit.append(leaf(2))).toBe(2);
    });

    it('proof throws on an out-of-range index', () => {
        const audit = filled(3);
        expect(() => audit.proof(3)).toThrow(/out of range/);
        expect(() => audit.proof(-1)).toThrow(/out of range/);
    });
});
