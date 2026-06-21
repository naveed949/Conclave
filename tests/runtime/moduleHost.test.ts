import { ModuleHost } from '../../src/runtime/moduleHost';
import { counter } from '../../src/runtime/modules/counter';
import { notes } from '../../src/runtime/modules/notes';
import { ModuleCommand, Seed } from '../../src/runtime/types';

const META = { actor: 'tester', requestId: 'req-1' };

/** A fixed seed makes assertions on ids/timestamps reproducible. */
const seed = (nonce: string, timestamp = '2026-06-21T00:00:00.000Z'): Seed => ({ timestamp, nonce });

/** Helper to build a module command tersely. */
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

describe('ModuleHost dispatch', () => {
    it('dispatches counter increment/reset and answers the value query', () => {
        const host = freshHost();

        expect(host.apply(cmd('counter', 'increment', { by: 5 }, seed('a1')), META).status).toBe(200);
        expect(host.apply(cmd('counter', 'increment', undefined, seed('a2')), META).status).toBe(200); // +1
        expect(host.query('counter', 'value')).toBe(6);

        expect(host.apply(cmd('counter', 'reset', undefined, seed('a3')), META).status).toBe(200);
        expect(host.query('counter', 'value')).toBe(0);
    });

    it('notes create assigns a deterministic id + createdAt from the seed', () => {
        const host = freshHost();
        const ts = '2026-06-21T12:34:56.000Z';

        const res = host.apply(cmd('notes', 'create', { text: 'hello' }, seed('beef', ts)), META);
        expect(res.status).toBe(200);

        // The host surfaces the created note directly as the explicit result.
        const created = res.result as { id: string; text: string; createdAt: string };
        expect(created.text).toBe('hello');
        expect(created.createdAt).toBe(ts); // came from ctx.now == seed.timestamp
        expect(created.id).toMatch(/^[0-9a-f]{32}$/); // deterministic 32-char hex id

        const list = host.query('notes', 'list') as Array<{ id: string; text: string; createdAt: string }>;
        expect(list).toHaveLength(1);
        expect(list[0]).toEqual(created);

        // The id is reproducible: a fresh host with the same seed yields the same id.
        const host2 = freshHost();
        host2.apply(cmd('notes', 'create', { text: 'hello' }, seed('beef', ts)), META);
        const list2 = host2.query('notes', 'list') as Array<{ id: string }>;
        expect(list2[0].id).toBe(list[0].id);
    });

    it('returns 404 for an unknown module or command without throwing', () => {
        const host = freshHost();

        const noModule = host.apply(cmd('ghost', 'increment', {}, seed('z1')), META);
        expect(noModule.status).toBe(404);
        expect(noModule.effects).toEqual([]);
        expect(noModule.message).toMatch(/Unknown module/);

        const noCommand = host.apply(cmd('counter', 'nope', {}, seed('z2')), META);
        expect(noCommand.status).toBe(404);
        expect(noCommand.message).toMatch(/Unknown command/);

        // State is untouched by the failed dispatches.
        expect(host.query('counter', 'value')).toBe(0);
    });

    it('a reducer that mutates the passed-in state then throws leaves committed state intact', () => {
        // The reducer mutates `state` in place AND throws. Because the host hands
        // the reducer a deep clone and only swaps live state on a clean return,
        // the committed state must be exactly what it was before the call.
        const corrupting = {
            name: 'corrupting',
            initialState: () => ({ items: ['safe'] as string[] }),
            commands: {
                wreck: (state: { items: string[] }) => {
                    state.items.push('corrupted'); // mutate the live-looking reference
                    throw new Error('boom after mutation');
                },
            },
            queries: {
                items: (state: { items: string[] }) => state.items,
            },
        };

        const host = new ModuleHost();
        host.register(corrupting as any);

        const before = host.getState('corrupting');
        const res = host.apply(cmd('corrupting', 'wreck', undefined, seed('m1')), META);

        expect(res.status).toBe(500);
        // Committed state unchanged: the in-place mutation hit only the clone.
        expect(host.query('corrupting', 'items')).toEqual(['safe']);
        expect(host.getState('corrupting')).toEqual(before);
    });
});

describe('ModuleHost determinism / convergence', () => {
    it('two independent hosts reach byte-identical state from the same command stream', () => {
        // A module whose reducer pulls from BOTH ctx.random() and ctx.id(), to
        // prove both deterministic streams are reproducible across hosts.
        const roller = {
            name: 'roller',
            initialState: () => ({ rolls: [] as Array<{ id: string; r: number; at: string }> }),
            commands: {
                roll: (state: { rolls: Array<{ id: string; r: number; at: string }> }, _input: unknown, ctx: any) => ({
                    state: { rolls: [...state.rolls, { id: ctx.id(), r: ctx.random(), at: ctx.now }] },
                }),
            },
        };

        const build = (): ModuleHost => {
            const h = new ModuleHost();
            h.register(counter);
            h.register(notes);
            h.register(roller as any);
            return h;
        };

        const host1 = build();
        const host2 = build();

        // Identical command stream (identical seeds) applied to both hosts.
        const stream: ModuleCommand[] = [
            cmd('counter', 'increment', { by: 3 }, seed('s1')),
            cmd('notes', 'create', { text: 'first' }, seed('s2', '2026-01-01T00:00:00.000Z')),
            cmd('roller', 'roll', undefined, seed('s3')),
            cmd('roller', 'roll', undefined, seed('s3')), // same seed -> same id+random again
            cmd('counter', 'increment', undefined, seed('s4')),
            cmd('notes', 'create', { text: 'second' }, seed('s5', '2026-02-02T00:00:00.000Z')),
        ];

        for (const c of stream) {
            host1.apply(c, META);
            host2.apply(c, META);
        }

        // Byte-identical state, including ids, timestamps, and random() outputs.
        expect(host1.snapshot()).toEqual(host2.snapshot());

        // Sanity: the random()/id() values actually landed in state.
        const rollerState = host1.getState('roller') as { rolls: Array<{ id: string; r: number }> };
        expect(rollerState.rolls).toHaveLength(2);
        expect(typeof rollerState.rolls[0].r).toBe('number');
        expect(rollerState.rolls[0].r).toBeGreaterThanOrEqual(0);
        expect(rollerState.rolls[0].r).toBeLessThan(1);
        // Same seed reused for both rolls -> identical id and random output.
        expect(rollerState.rolls[0]).toEqual(rollerState.rolls[1]);
    });

    it('successive ctx.random()/ctx.id() calls within one command advance deterministically', () => {
        // Two calls in the SAME reducer invocation must differ from each other,
        // but be identical across hosts for the same seed.
        const multi = {
            name: 'multi',
            initialState: () => ({ ids: [] as string[], rs: [] as number[] }),
            commands: {
                go: (_s: { ids: string[]; rs: number[] }, _i: unknown, ctx: any) => ({
                    state: { ids: [ctx.id(), ctx.id()], rs: [ctx.random(), ctx.random()] },
                }),
            },
        };

        const mk = () => {
            const h = new ModuleHost();
            h.register(multi as any);
            return h;
        };

        const h1 = mk();
        const h2 = mk();
        h1.apply(cmd('multi', 'go', undefined, seed('seed-x')), META);
        h2.apply(cmd('multi', 'go', undefined, seed('seed-x')), META);

        const s1 = h1.getState('multi') as { ids: string[]; rs: number[] };
        expect(s1.ids[0]).not.toBe(s1.ids[1]); // successive ids differ
        expect(s1.rs[0]).not.toBe(s1.rs[1]); // successive randoms differ
        expect(h1.snapshot()).toEqual(h2.snapshot()); // but reproducible across hosts
    });
});

describe('ModuleHost snapshot / restore', () => {
    it('round-trips state into a fresh host', () => {
        const host = freshHost();
        host.apply(cmd('counter', 'increment', { by: 9 }, seed('r1')), META);
        host.apply(cmd('notes', 'create', { text: 'persist me' }, seed('r2')), META);

        const snap = host.snapshot();

        const restored = freshHost();
        restored.restore(snap);

        expect(restored.snapshot()).toEqual(snap);
        expect(restored.query('counter', 'value')).toBe(9);
        expect((restored.query('notes', 'list') as unknown[]).length).toBe(1);
        expect(restored.query('notes', 'list')).toEqual(host.query('notes', 'list'));
    });

    it('snapshot is decoupled from live state (deep clone)', () => {
        const host = freshHost();
        host.apply(cmd('counter', 'increment', { by: 1 }, seed('d1')), META);

        const snap = host.snapshot();
        host.apply(cmd('counter', 'increment', { by: 1 }, seed('d2')), META); // mutate after snapshot

        expect((snap.counter as { value: number }).value).toBe(1); // snapshot unaffected
        expect(host.query('counter', 'value')).toBe(2);
    });
});

describe('ModuleHost registration', () => {
    it('throws on duplicate module registration', () => {
        const host = new ModuleHost();
        host.register(counter);
        expect(() => host.register(counter)).toThrow(/already registered/);
    });
});
