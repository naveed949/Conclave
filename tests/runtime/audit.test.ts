import { MerkleAudit } from '../../src/runtime/merkleAudit';
import { ModuleHost } from '../../src/runtime/moduleHost';
import { defineModule } from '../../src/runtime/defineModule';
import { counter } from '../../src/runtime/modules/counter';
import { notes } from '../../src/runtime/modules/notes';
import { ModuleCommand, Seed } from '../../src/runtime/types';

const META = { actor: 'tester', requestId: 'req-1' };

const seed = (nonce: string, timestamp = '2026-06-21T00:00:00.000Z'): Seed => ({ timestamp, nonce });

const cmd = (module: string, command: string, input: unknown, s: Seed): ModuleCommand => ({
    module,
    command,
    input,
    seed: s,
});

function freshHost(): ModuleHost {
    const host = new ModuleHost();
    host.register(counter);
    host.register(notes);
    return host;
}

describe('ModuleHost Merkle audit', () => {
    it('grows the audit and stamps each leaf with module/command/status/codeHash', () => {
        const host = freshHost();
        expect(host.auditSize()).toBe(0);
        expect(host.auditRoot()).toBe(new MerkleAudit().root()); // empty-tree constant

        host.apply(cmd('counter', 'increment', { by: 2 }, seed('a1')), META);
        host.apply(cmd('notes', 'create', { text: 'hi' }, seed('a2')), META);

        expect(host.auditSize()).toBe(2);
        const entries = host.auditEntries();

        expect(entries[0]).toMatchObject({
            seq: 0,
            module: 'counter',
            command: 'increment',
            actor: 'tester',
            requestId: 'req-1',
            status: 200,
            codeHash: host.moduleCodeHash('counter'),
        });
        expect(entries[1]).toMatchObject({
            seq: 1,
            module: 'notes',
            command: 'create',
            status: 200,
            codeHash: host.moduleCodeHash('notes'),
        });
        // Different modules -> different code hashes.
        expect(entries[0].codeHash).not.toBe(entries[1].codeHash);
    });

    it('auditProof(seq) verifies against auditRoot()', () => {
        const host = freshHost();
        for (let i = 0; i < 5; i += 1) {
            host.apply(cmd('counter', 'increment', { by: 1 }, seed(`s${i}`)), META);
        }
        const root = host.auditRoot();
        for (let seq = 0; seq < host.auditSize(); seq += 1) {
            expect(MerkleAudit.verify(root, host.auditProof(seq))).toBe(true);
        }
    });

    it('two independent hosts applying the same stream converge on the same auditRoot', () => {
        const stream: ModuleCommand[] = [
            cmd('counter', 'increment', { by: 3 }, seed('s1')),
            cmd('notes', 'create', { text: 'first' }, seed('s2', '2026-01-01T00:00:00.000Z')),
            cmd('counter', 'reset', undefined, seed('s3')),
            cmd('notes', 'create', { text: 'second' }, seed('s4', '2026-02-02T00:00:00.000Z')),
            cmd('counter', 'increment', undefined, seed('s5')),
        ];

        const h1 = freshHost();
        const h2 = freshHost();
        for (const c of stream) {
            h1.apply(c, META);
            h2.apply(c, META);
        }

        expect(h1.auditRoot()).toBe(h2.auditRoot());
        expect(h1.auditEntries()).toEqual(h2.auditEntries());
    });

    it('captures which logic version ran: a changed version/body yields a new codeHash on its leaves', () => {
        // Two modules, same shape, different reducer BODY -> different codeHash.
        const v1 = defineModule<{ n: number }>({
            name: 'widget',
            initialState: () => ({ n: 0 }),
            commands: { bump: (s) => ({ state: { n: s.n + 1 } }) },
        });
        const v2 = defineModule<{ n: number }>({
            name: 'widget',
            initialState: () => ({ n: 0 }),
            commands: { bump: (s) => ({ state: { n: s.n + 2 } }) }, // changed logic
        });

        const hostV1 = new ModuleHost();
        hostV1.register(v1);
        hostV1.apply(cmd('widget', 'bump', undefined, seed('w1')), META);

        const hostV2 = new ModuleHost();
        hostV2.register(v2);
        hostV2.apply(cmd('widget', 'bump', undefined, seed('w1')), META);

        const hash1 = hostV1.moduleCodeHash('widget')!;
        const hash2 = hostV2.moduleCodeHash('widget')!;
        expect(hash1).not.toBe(hash2);

        // Each version's leaf records its own code hash -> the audit proves which
        // logic version produced the result, and the two audits diverge.
        expect(hostV1.auditEntries()[0].codeHash).toBe(hash1);
        expect(hostV2.auditEntries()[0].codeHash).toBe(hash2);
        expect(hostV1.auditRoot()).not.toBe(hostV2.auditRoot());

        // A bumped `version` alone (identical body) also shifts the hash.
        const sameBodyV3 = defineModule<{ n: number }>({
            name: 'widget',
            version: '2.0.0',
            initialState: () => ({ n: 0 }),
            commands: { bump: (s) => ({ state: { n: s.n + 1 } }) },
        });
        const hostV3 = new ModuleHost();
        hostV3.register(sameBodyV3);
        expect(hostV3.moduleCodeHash('widget')).not.toBe(hash1);
    });

    it('audit survives snapshot/restore: root and proofs stay stable', () => {
        const host = freshHost();
        for (let i = 0; i < 6; i += 1) {
            host.apply(cmd('counter', 'increment', { by: 1 }, seed(`r${i}`)), META);
        }
        const rootBefore = host.auditRoot();
        const snap = host.snapshot();

        const restored = freshHost();
        restored.restore(snap);

        expect(restored.auditSize()).toBe(host.auditSize());
        expect(restored.auditRoot()).toBe(rootBefore);
        for (let seq = 0; seq < restored.auditSize(); seq += 1) {
            expect(MerkleAudit.verify(restored.auditRoot(), restored.auditProof(seq))).toBe(true);
        }

        // Audit continues seamlessly after restore (seq keeps climbing).
        restored.apply(cmd('counter', 'increment', { by: 1 }, seed('after')), META);
        expect(restored.auditSize()).toBe(host.auditSize() + 1);
        expect(restored.auditEntries()[host.auditSize()].seq).toBe(host.auditSize());
    });

    it('audits business failures and thrown reducers; does NOT audit unknown commands', () => {
        // A module whose reducer throws to signal a business failure (-> status 500).
        const flaky = defineModule<{ ok: boolean }>({
            name: 'flaky',
            initialState: () => ({ ok: true }),
            commands: {
                fail: () => {
                    throw new Error('business rule violated');
                },
                ok: (s) => ({ state: s }),
            },
        });

        const host = new ModuleHost();
        host.register(flaky);

        // Thrown reducer -> 500, still audited with that status.
        const failed = host.apply(cmd('flaky', 'fail', undefined, seed('f1')), META);
        expect(failed.status).toBe(500);
        expect(host.auditSize()).toBe(1);
        expect(host.auditEntries()[0]).toMatchObject({ command: 'fail', status: 500 });

        // A successful command is audited too.
        host.apply(cmd('flaky', 'ok', undefined, seed('f2')), META);
        expect(host.auditSize()).toBe(2);

        // Unknown module / unknown command: no logic ran -> NOT audited.
        expect(host.apply(cmd('ghost', 'whatever', {}, seed('f3')), META).status).toBe(404);
        expect(host.apply(cmd('flaky', 'nope', {}, seed('f4')), META).status).toBe(404);
        expect(host.auditSize()).toBe(2);
    });
});
