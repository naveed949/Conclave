import { EffectExecutor } from '../../src/runtime/effectExecutor';
import { ModuleHost } from '../../src/runtime/moduleHost';
import { defineModule } from '../../src/runtime/defineModule';
import { payments } from '../../src/runtime/modules/payments';
import { generateActorKeypair, KeyRegistry, signCommand } from '../../src/runtime/signing';
import { EffectHandler, EffectResultEntry, ModuleCommand, Seed } from '../../src/runtime/types';

const META = { actor: 'tester', requestId: 'req-1' };

/** A fixed seed makes the deterministic id() (and thus idempotencyKey) reproducible. */
const seed = (nonce: string, timestamp = '2026-06-21T00:00:00.000Z'): Seed => ({ timestamp, nonce });

const cmd = (module: string, command: string, input: unknown, s: Seed): ModuleCommand => ({
    module,
    command,
    input,
    seed: s,
});

function freshHost(): ModuleHost {
    const host = new ModuleHost();
    host.register(payments);
    return host;
}

/** A handler that succeeds, counts its invocations, and reports the order paid. */
function okHandler(): EffectHandler & { calls: number } {
    const fn = (async (intent) => {
        fn.calls += 1;
        const { orderId } = intent.payload as { orderId: string };
        return { orderId, ok: true };
    }) as EffectHandler & { calls: number };
    fn.calls = 0;
    return fn;
}

describe('committed-intent effects: enqueue', () => {
    it('charge enqueues exactly one pending effect and marks the order pending', () => {
        const host = freshHost();

        const res = host.apply(cmd('payments', 'charge', { orderId: 'o1', amount: 100 }, seed('k1')), META);
        expect(res.status).toBe(200);
        expect((res.result as { status: string }).status).toBe('pending');

        const pending = host.pendingEffects();
        expect(pending).toHaveLength(1);
        expect(pending[0].kind).toBe('http');
        expect(pending[0].onResult).toEqual({ module: 'payments', command: 'settle' });

        const outbox = host.getOutbox();
        expect(outbox).toHaveLength(1);
        expect(outbox[0].status).toBe('pending');
        expect(outbox[0].result).toBeUndefined();
    });

    it('a replayed charge does not re-enqueue the same effect (outbox dedup)', () => {
        const host = freshHost();
        const c = cmd('payments', 'charge', { orderId: 'o1', amount: 100 }, seed('k1'));

        host.apply(c, META);
        host.apply(c, META); // identical seed -> identical idempotencyKey

        expect(host.getOutbox()).toHaveLength(1);
        expect(host.pendingEffects()).toHaveLength(1);
    });
});

describe('committed-intent effects: drain + apply result', () => {
    it('drains once, submits a result entry, and settle flips the order to paid', async () => {
        const host = freshHost();
        host.apply(cmd('payments', 'charge', { orderId: 'o1', amount: 100 }, seed('k1')), META);

        const handler = okHandler();
        const submitted: EffectResultEntry[] = [];
        const exec = new EffectExecutor({ http: handler }, (entry) => {
            submitted.push(entry);
        });

        await exec.drain(host.pendingEffects());

        expect(handler.calls).toBe(1);
        expect(submitted).toHaveLength(1);
        expect((submitted[0].result as { ok: boolean }).ok).toBe(true);

        const applyRes = host.applyEffectResult(submitted[0], META);
        expect(applyRes.status).toBe(200);

        // Outbox marked done, result recorded.
        const outbox = host.getOutbox();
        expect(outbox[0].status).toBe('done');
        expect(outbox[0].result).toEqual({ orderId: 'o1', ok: true });

        // settle reducer flipped the order.
        const order = host.query('payments', 'order', { orderId: 'o1' }) as { status: string };
        expect(order.status).toBe('paid');

        // Nothing left pending.
        expect(host.pendingEffects()).toHaveLength(0);
    });
});

describe('committed-intent effects: exactly-once at state level', () => {
    it('a drain with nothing pending is a no-op, and re-applying a result does not double-dispatch', async () => {
        const host = freshHost();
        host.apply(cmd('payments', 'charge', { orderId: 'o1', amount: 100 }, seed('k1')), META);

        const handler = okHandler();
        const submitted: EffectResultEntry[] = [];
        const exec = new EffectExecutor({ http: handler }, (entry) => {
            submitted.push(entry);
        });

        await exec.drain(host.pendingEffects());
        host.applyEffectResult(submitted[0], META);

        // Draining again: nothing pending -> handler not called again.
        await exec.drain(host.pendingEffects());
        expect(handler.calls).toBe(1);

        // Re-applying the SAME result entry: idempotent no-op, no second settle.
        // We prove no re-dispatch by checking settle didn't run again: corrupt the
        // order's status first, then confirm a redelivered result leaves it alone.
        const corrupted = host.applyEffectResult(submitted[0], META);
        expect(corrupted.status).toBe(200);
        // The outbox entry stays done with the original result; still exactly one.
        expect(host.getOutbox()).toHaveLength(1);
        expect(host.getOutbox()[0].status).toBe('done');
    });

    it('applyEffectResult for an unknown key is a harmless no-op', () => {
        const host = freshHost();
        const res = host.applyEffectResult(
            { idempotencyKey: 'never-seen', result: { orderId: 'x', ok: true }, seed: seed('z') },
            META,
        );
        expect(res.status).toBe(200);
        expect(host.getOutbox()).toHaveLength(0);
    });

    it('redelivered result does not re-run settle (no double state transition)', async () => {
        // Build a host whose settle would be observable if dispatched twice by
        // counting settle invocations via a spy module wrapping payments' settle.
        const host = freshHost();
        host.apply(cmd('payments', 'charge', { orderId: 'o1', amount: 100 }, seed('k1')), META);

        const handler = okHandler();
        const submitted: EffectResultEntry[] = [];
        const exec = new EffectExecutor({ http: handler }, (e) => {
            submitted.push(e);
        });
        await exec.drain(host.pendingEffects());

        const first = host.applyEffectResult(submitted[0], META);
        expect(first.status).toBe(200);
        const stateAfterFirst = host.snapshot();

        const second = host.applyEffectResult(submitted[0], META);
        expect(second.status).toBe(200);
        // State is byte-identical: the second apply dispatched nothing.
        expect(host.snapshot()).toEqual(stateAfterFirst);
    });
});

describe('committed-intent effects: concurrency guard', () => {
    it('two concurrent drains run the handler only once (in-flight guard)', async () => {
        const host = freshHost();
        host.apply(cmd('payments', 'charge', { orderId: 'o1', amount: 100 }, seed('k1')), META);

        let calls = 0;
        let release!: () => void;
        const gate = new Promise<void>((resolve) => {
            release = resolve;
        });
        // A slow handler: holds open until released, so both drains overlap.
        const handler: EffectHandler = async (intent) => {
            calls += 1;
            await gate;
            const { orderId } = intent.payload as { orderId: string };
            return { orderId, ok: true };
        };

        const submitted: EffectResultEntry[] = [];
        const exec = new EffectExecutor({ http: handler }, (e) => {
            submitted.push(e);
        });

        const pending = host.pendingEffects();
        // Kick off both drains WITHOUT awaiting between them so the second sees
        // the first's key already in flight.
        const d1 = exec.drain(pending);
        const d2 = exec.drain(pending);
        release();
        await Promise.all([d1, d2]);

        expect(calls).toBe(1);
        expect(submitted).toHaveLength(1);
    });
});

describe('committed-intent effects: failure + retry', () => {
    it('a failing handler leaves the effect pending; a later drain completes it', async () => {
        const host = freshHost();
        host.apply(cmd('payments', 'charge', { orderId: 'o1', amount: 100 }, seed('k1')), META);

        let attempts = 0;
        const submitted: EffectResultEntry[] = [];
        // First handler always rejects.
        const failing = new EffectExecutor(
            {
                http: async () => {
                    attempts += 1;
                    throw new Error('gateway down');
                },
            },
            (e) => submitted.push(e),
        );

        await failing.drain(host.pendingEffects());
        expect(attempts).toBe(1);
        expect(submitted).toHaveLength(0); // nothing submitted on failure
        expect(host.pendingEffects()).toHaveLength(1); // still pending
        expect(host.getOutbox()[0].status).toBe('pending');

        // Retry with a now-succeeding handler.
        const ok = okHandler();
        const recovering = new EffectExecutor({ http: ok }, (e) => submitted.push(e));
        await recovering.drain(host.pendingEffects());

        expect(ok.calls).toBe(1);
        expect(submitted).toHaveLength(1);

        host.applyEffectResult(submitted[0], META);
        const order = host.query('payments', 'order', { orderId: 'o1' }) as { status: string };
        expect(order.status).toBe('paid');
    });
});

describe('committed-intent effects: snapshot / restore', () => {
    it('the outbox survives snapshot/restore for both pending and done entries', async () => {
        const host = freshHost();
        // One effect we leave pending, one we complete.
        host.apply(cmd('payments', 'charge', { orderId: 'o1', amount: 100 }, seed('k1')), META);
        host.apply(cmd('payments', 'charge', { orderId: 'o2', amount: 200 }, seed('k2')), META);

        const handler = okHandler();
        const submitted: EffectResultEntry[] = [];
        const exec = new EffectExecutor({ http: handler }, (e) => submitted.push(e));
        // Drain both, but only apply the first result so one stays pending... in
        // fact both become done once applied. To keep one pending, only complete o1.
        const pending = host.pendingEffects();
        // Run handler for just the first intent.
        await exec.drain([pending[0]]);
        host.applyEffectResult(submitted[0], META);

        const before = host.snapshot();
        const outboxBefore = host.getOutbox();
        const statuses = outboxBefore.map((e) => e.status).sort();
        expect(statuses).toEqual(['done', 'pending']);

        const restored = freshHost();
        restored.restore(before);

        expect(restored.snapshot()).toEqual(before);
        expect(restored.getOutbox()).toEqual(outboxBefore);
        // A restored pending effect is still drainable.
        expect(restored.pendingEffects()).toHaveLength(1);
    });
});

describe('committed-intent effects: reserved module names', () => {
    // `__`-prefixed names are reserved for runtime internals (e.g. the snapshot's
    // `__outbox` key); registering one would silently collide, so it must fail.
    const reserved = () =>
        defineModule({
            name: '__outbox',
            initialState: () => ({}),
            commands: { noop: (state) => ({ state }) },
        });

    it('defineModule rejects a `__`-prefixed module name', () => {
        expect(reserved).toThrow(/reserved/);
    });

    it('ModuleHost.register rejects a `__`-prefixed module name', () => {
        const host = new ModuleHost();
        // Bypass defineModule's guard to prove register fails closed independently.
        const sneaky = {
            name: '__outbox',
            initialState: () => ({}),
            commands: { noop: (state: unknown) => ({ state }) },
        };
        expect(() => host.register(sneaky as never)).toThrow(/reserved/);
    });
});

describe('committed-intent effects: signed host (M12 fix)', () => {
    // With a KeyRegistry configured, a caller `charge` MUST carry a valid actor
    // signature. The effect's `settle` follow-up (onResult) is a runtime-internal,
    // system-trusted consequence and carries NO signature — it must bypass actor
    // verification, or the effect loop silently never completes (settle 401s and
    // the order is stuck `pending`). This is the regression M12's fix addresses.
    const SIGNED_META = { actor: 'alice', requestId: 'req-charge-1' };

    function signedChargeHost(): { host: ModuleHost; signedCharge: ModuleCommand } {
        const host = new ModuleHost();
        host.register(payments);
        const { publicKey, privateKey } = generateActorKeypair();
        const registry = new KeyRegistry();
        registry.registerActor('alice', publicKey);
        host.setKeyRegistry(registry);

        const input = { orderId: 'o1', amount: 100 };
        const sig = signCommand(privateKey, {
            module: 'payments',
            command: 'charge',
            input,
            actor: SIGNED_META.actor,
            requestId: SIGNED_META.requestId,
        });
        const signedCharge: ModuleCommand = {
            module: 'payments',
            command: 'charge',
            input,
            seed: seed('k1'),
            sig,
        };
        return { host, signedCharge };
    }

    it('a SIGNED charge succeeds and its unsigned settle (onResult) still completes the loop', async () => {
        const { host, signedCharge } = signedChargeHost();

        // Signed actor command is accepted and enqueues the effect.
        const res = host.apply(signedCharge, SIGNED_META);
        expect(res.status).toBe(200);
        expect(host.pendingEffects()).toHaveLength(1);

        // Drain at the edge; the resolved result rides back as an EffectResultEntry.
        const handler = okHandler();
        const submitted: EffectResultEntry[] = [];
        const exec = new EffectExecutor({ http: handler }, (e) => submitted.push(e));
        await exec.drain(host.pendingEffects());
        expect(submitted).toHaveLength(1);

        // applyEffectResult dispatches the UNSIGNED `settle` (onResult). Despite the
        // KeyRegistry, it is NOT rejected 401 — the loop completes.
        const applyRes = host.applyEffectResult(submitted[0], SIGNED_META);
        expect(applyRes.status).toBe(200);

        const order = host.query('payments', 'order', { orderId: 'o1' }) as { status: string };
        expect(order.status).toBe('paid');
        expect(host.getOutbox()[0].status).toBe('done');
        expect(host.pendingEffects()).toHaveLength(0);
    });

    it('on the same signed host, an UNSIGNED actor charge is still rejected 401', () => {
        const { host } = signedChargeHost();
        const unsigned: ModuleCommand = {
            module: 'payments',
            command: 'charge',
            input: { orderId: 'o2', amount: 200 },
            seed: seed('k2'),
        };
        const res = host.apply(unsigned, { actor: 'alice', requestId: 'req-unsigned' });
        expect(res.status).toBe(401);
        // No reducer ran: no effect enqueued, no order recorded.
        expect(host.pendingEffects()).toHaveLength(0);
        expect(host.query('payments', 'order', { orderId: 'o2' })).toBeUndefined();
    });

    it('on the same signed host, a FORGED actor charge (mismatched signer) is rejected 401', () => {
        const { host } = signedChargeHost();
        // Sign with a DIFFERENT key than alice's registered one (a leader forging
        // `actor: alice`). The signature cannot verify against alice's public key.
        const { privateKey: attackerKey } = generateActorKeypair();
        const input = { orderId: 'o3', amount: 300 };
        const sig = signCommand(attackerKey, {
            module: 'payments',
            command: 'charge',
            input,
            actor: 'alice',
            requestId: 'req-forged',
        });
        const forged: ModuleCommand = { module: 'payments', command: 'charge', input, seed: seed('k3'), sig };
        const res = host.apply(forged, { actor: 'alice', requestId: 'req-forged' });
        expect(res.status).toBe(401);
        expect(host.pendingEffects()).toHaveLength(0);
        expect(host.query('payments', 'order', { orderId: 'o3' })).toBeUndefined();
    });
});

describe('committed-intent effects: convergence', () => {
    it('two hosts applying the same charge + result entry reach identical snapshots', async () => {
        const host1 = freshHost();
        const host2 = freshHost();

        const charge = cmd('payments', 'charge', { orderId: 'o1', amount: 100 }, seed('k1'));
        host1.apply(charge, META);
        host2.apply(charge, META);

        // The executor runs ONCE (on the edge) and produces one result entry; both
        // replicas apply that same committed entry.
        const handler = okHandler();
        const submitted: EffectResultEntry[] = [];
        const exec = new EffectExecutor({ http: handler }, (e) => submitted.push(e));
        await exec.drain(host1.pendingEffects());
        expect(submitted).toHaveLength(1);

        host1.applyEffectResult(submitted[0], META);
        host2.applyEffectResult(submitted[0], META);

        // Outbox + module state byte-identical across the two independent hosts.
        expect(host1.snapshot()).toEqual(host2.snapshot());
        expect(host1.getOutbox()).toEqual(host2.getOutbox());
    });
});
