import { RaftNode } from '../../src/consensus/raftNode';
import { CommandMeta } from '../../src/consensus/types';
import { buildModuleCommand, buildSignedModuleCommand } from '../../src/runtime/command';
import { ModuleHost } from '../../src/runtime/moduleHost';
import { counter } from '../../src/runtime/modules/counter';
import {
    generateActorKeypair,
    KeyRegistry,
    signCommand,
    SignablePayload,
    verifyCommand,
} from '../../src/runtime/signing';
import { buildCluster, leaders, waitFor } from '../helpers';

/**
 * Milestone 7 (ADR-0018 pillar 7): actor-signed module commands. The actor signs
 * the LOGICAL command (NOT the leader-resolved seed); every node verifies on the
 * apply path against an actor->public-key registry. A forged/tampered command is
 * rejected DETERMINISTICALLY (401) on every node, so a malicious leader cannot
 * forge `actor`. With NO registry configured, behavior is unchanged (back-compat).
 */

/** A fixed leader-resolved seed; the seed is excluded from the signature. */
const SEED = { timestamp: '2026-06-21T00:00:00.000Z', nonce: 'deadbeef' };

describe('actor command signing primitives', () => {
    it('round-trips sign + verify with the matching key', () => {
        const { publicKey, privateKey } = generateActorKeypair();
        const payload: SignablePayload = {
            module: 'counter',
            command: 'increment',
            input: { by: 5 },
            actor: 'alice',
            requestId: 'req-1',
        };
        const sig = signCommand(privateKey, payload);
        expect(verifyCommand(publicKey, payload, sig)).toBe(true);
    });

    it('fails verification when the input is tampered', () => {
        const { publicKey, privateKey } = generateActorKeypair();
        const payload: SignablePayload = {
            module: 'counter', command: 'increment', input: { by: 5 }, actor: 'alice', requestId: 'req-1',
        };
        const sig = signCommand(privateKey, payload);
        expect(verifyCommand(publicKey, { ...payload, input: { by: 6 } }, sig)).toBe(false);
    });

    it('fails verification when the actor is tampered', () => {
        const { publicKey, privateKey } = generateActorKeypair();
        const payload: SignablePayload = {
            module: 'counter', command: 'increment', input: { by: 5 }, actor: 'alice', requestId: 'req-1',
        };
        const sig = signCommand(privateKey, payload);
        expect(verifyCommand(publicKey, { ...payload, actor: 'mallory' }, sig)).toBe(false);
    });

    it('fails verification when the requestId is tampered', () => {
        const { publicKey, privateKey } = generateActorKeypair();
        const payload: SignablePayload = {
            module: 'counter', command: 'increment', input: { by: 5 }, actor: 'alice', requestId: 'req-1',
        };
        const sig = signCommand(privateKey, payload);
        expect(verifyCommand(publicKey, { ...payload, requestId: 'req-2' }, sig)).toBe(false);
    });

    it('fails verification under the wrong key', () => {
        const alice = generateActorKeypair();
        const mallory = generateActorKeypair();
        const payload: SignablePayload = {
            module: 'counter', command: 'increment', input: { by: 5 }, actor: 'alice', requestId: 'req-1',
        };
        const sig = signCommand(alice.privateKey, payload);
        expect(verifyCommand(mallory.publicKey, payload, sig)).toBe(false);
    });
});

describe('ModuleHost signature enforcement', () => {
    const alice = generateActorKeypair();

    /** A host with a registry binding alice -> alice's public key. */
    function signingHost(): ModuleHost {
        const host = new ModuleHost();
        host.register(counter);
        const reg = new KeyRegistry();
        reg.registerActor('alice', alice.publicKey);
        host.setKeyRegistry(reg);
        return host;
    }

    it('applies a correctly-signed command (200)', () => {
        const host = signingHost();
        const sig = signCommand(alice.privateKey, {
            module: 'counter', command: 'increment', input: { by: 4 }, actor: 'alice', requestId: 'r1',
        });
        const res = host.apply(
            { module: 'counter', command: 'increment', input: { by: 4 }, seed: SEED, sig },
            { actor: 'alice', requestId: 'r1' },
        );
        expect(res.status).toBe(200);
        expect(host.query('counter', 'value')).toBe(4);
    });

    it('rejects an unsigned command (401) and leaves state/outbox unchanged', () => {
        const host = signingHost();
        const res = host.apply(
            { module: 'counter', command: 'increment', input: { by: 4 }, seed: SEED },
            { actor: 'alice', requestId: 'r1' },
        );
        expect(res.status).toBe(401);
        expect(host.query('counter', 'value')).toBe(0);
        expect(host.getOutbox()).toEqual([]);
    });

    it('rejects a command signed by the wrong actor (401)', () => {
        const host = signingHost();
        const mallory = generateActorKeypair();
        // Signed by mallory but CLAIMING actor: 'alice' (the forgery attempt).
        const sig = signCommand(mallory.privateKey, {
            module: 'counter', command: 'increment', input: { by: 4 }, actor: 'alice', requestId: 'r1',
        });
        const res = host.apply(
            { module: 'counter', command: 'increment', input: { by: 4 }, seed: SEED, sig },
            { actor: 'alice', requestId: 'r1' },
        );
        expect(res.status).toBe(401);
        expect(host.query('counter', 'value')).toBe(0);
    });

    it('rejects a tampered input (401)', () => {
        const host = signingHost();
        // Signed over by:4, but the command carries by:99 (leader altered input).
        const sig = signCommand(alice.privateKey, {
            module: 'counter', command: 'increment', input: { by: 4 }, actor: 'alice', requestId: 'r1',
        });
        const res = host.apply(
            { module: 'counter', command: 'increment', input: { by: 99 }, seed: SEED, sig },
            { actor: 'alice', requestId: 'r1' },
        );
        expect(res.status).toBe(401);
        expect(host.query('counter', 'value')).toBe(0);
    });

    it('rejects an unknown actor with no registered key (401)', () => {
        const host = signingHost();
        const eve = generateActorKeypair();
        const sig = signCommand(eve.privateKey, {
            module: 'counter', command: 'increment', input: { by: 1 }, actor: 'eve', requestId: 'r1',
        });
        const res = host.apply(
            { module: 'counter', command: 'increment', input: { by: 1 }, seed: SEED, sig },
            { actor: 'eve', requestId: 'r1' },
        );
        expect(res.status).toBe(401);
        expect(host.query('counter', 'value')).toBe(0);
    });

    it('converges: two hosts with the same registry accept/reject identically', () => {
        const mallory = generateActorKeypair();
        const goodSig = signCommand(alice.privateKey, {
            module: 'counter', command: 'increment', input: { by: 2 }, actor: 'alice', requestId: 'g1',
        });
        const forgedSig = signCommand(mallory.privateKey, {
            module: 'counter', command: 'increment', input: { by: 2 }, actor: 'alice', requestId: 'f1',
        });

        const run = (host: ModuleHost): number[] => {
            const a = host.apply(
                { module: 'counter', command: 'increment', input: { by: 2 }, seed: SEED, sig: goodSig },
                { actor: 'alice', requestId: 'g1' },
            ).status;
            const b = host.apply(
                { module: 'counter', command: 'increment', input: { by: 2 }, seed: SEED, sig: forgedSig },
                { actor: 'alice', requestId: 'f1' },
            ).status;
            return [a, b];
        };

        const h1 = signingHost();
        const h2 = signingHost();
        expect(run(h1)).toEqual([200, 401]);
        expect(run(h2)).toEqual([200, 401]);
        // Deep-equal snapshots prove byte-identical state AND audit after the
        // same accept/reject sequence.
        expect(h1.snapshot()).toEqual(h2.snapshot());
        expect(h1.auditRoot()).toBe(h2.auditRoot());
    });

    it('back-compat: a host with NO registry applies an unsigned command unchanged', () => {
        const host = new ModuleHost();
        host.register(counter);
        const res = host.apply(
            { module: 'counter', command: 'increment', input: { by: 7 }, seed: SEED },
            { actor: 'alice', requestId: 'r1' },
        );
        expect(res.status).toBe(200);
        expect(host.query('counter', 'value')).toBe(7);
    });
});

describe('signed module commands over Raft consensus', () => {
    const alice = generateActorKeypair();
    let nodes: RaftNode[];

    beforeEach(() => {
        nodes = buildCluster(3);
        nodes.forEach((n) => {
            n.stateMachine.registerModules([counter]);
            // Every node configured with the SAME registry (alice's pubkey).
            n.stateMachine.registerActorKey('alice', alice.publicKey);
        });
        nodes.forEach((n) => n.start());
    });

    afterEach(() => {
        nodes.forEach((n) => n.stop());
    });

    it('replicates a signed command from alice and converges on all nodes (200)', async () => {
        await waitFor(() => leaders(nodes).length === 1);
        const leader = leaders(nodes)[0];

        const meta: CommandMeta = { requestId: 'req-signed', actor: 'alice', timestamp: SEED.timestamp };
        const cmd = buildSignedModuleCommand('counter', 'increment', { by: 5 }, {
            actor: 'alice',
            requestId: 'req-signed',
            privateKeyPem: alice.privateKey,
        });
        const res = await leader.submit(cmd, meta);
        expect(res.status).toBe(200);

        await waitFor(() => nodes.every((n) => n.stateMachine.moduleQuery('counter', 'value') === 5));
        const values = new Set(nodes.map((n) => n.stateMachine.moduleQuery('counter', 'value')));
        expect(values).toEqual(new Set([5]));
        const roots = new Set(nodes.map((n) => n.stateMachine.moduleAuditRoot()));
        expect(roots.size).toBe(1);
    });

    it('FORGERY: a command claiming actor alice but signed by another key is rejected (401) on every node; counter unchanged; nodes converged', async () => {
        await waitFor(() => leaders(nodes).length === 1);
        const leader = leaders(nodes)[0];

        // Mallory forges actor: 'alice' but signs with her own key.
        const mallory = generateActorKeypair();
        const meta: CommandMeta = { requestId: 'req-forge', actor: 'alice', timestamp: SEED.timestamp };
        const forged = buildSignedModuleCommand('counter', 'increment', { by: 100 }, {
            actor: 'alice',
            requestId: 'req-forge',
            privateKeyPem: mallory.privateKey,
        });
        const res = await leader.submit(forged, meta);
        // Rejected deterministically with 401 (the command still committed to the
        // log and applied, but the reducer never ran).
        expect(res.status).toBe(401);

        // Also submit an unsigned forgery for good measure.
        const unsignedMeta: CommandMeta = { requestId: 'req-unsigned', actor: 'alice', timestamp: SEED.timestamp };
        const unsigned = buildModuleCommand('counter', 'increment', { by: 50 });
        const res2 = await leader.submit(unsigned, unsignedMeta);
        expect(res2.status).toBe(401);

        // Wait until both forgery entries have committed/applied on every node by
        // gating on their audit appearing, then assert the counter never moved.
        await waitFor(() =>
            nodes.every((n) =>
                n.stateMachine.getAuditLog().some((e) => e.requestId === 'req-unsigned'),
            ),
        );
        const values = new Set(nodes.map((n) => n.stateMachine.moduleQuery('counter', 'value')));
        expect(values).toEqual(new Set([0]));
        // Nodes stay converged (identical module audit roots).
        const roots = new Set(nodes.map((n) => n.stateMachine.moduleAuditRoot()));
        expect(roots.size).toBe(1);
    });
});
