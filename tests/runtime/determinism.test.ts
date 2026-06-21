import { defineModule } from '../../src/runtime/defineModule';
import { canonicalBytes, lintReducer } from '../../src/runtime/determinism';
import { ModuleHost } from '../../src/runtime/moduleHost';
import { counter } from '../../src/runtime/modules/counter';
import { notes } from '../../src/runtime/modules/notes';
import { payments } from '../../src/runtime/modules/payments';
import { ModuleCommand, Seed } from '../../src/runtime/types';

const META = { actor: 'tester', requestId: 'req-1' };

const seed = (nonce: string, timestamp = '2026-06-21T00:00:00.000Z'): Seed => ({ timestamp, nonce });

const cmd = (module: string, command: string, input: unknown, s: Seed): ModuleCommand => ({
    module,
    command,
    input,
    seed: s,
});

describe('determinism lint at registration', () => {
    it('rejects a reducer that calls Math.random() (strict default)', () => {
        expect(() =>
            defineModule<{ v: number }>({
                name: 'bad-random',
                initialState: () => ({ v: 0 }),
                commands: {
                    go: (state) => ({ state: { v: Math.random() } }),
                },
            }),
        ).toThrow(/Math\.random/);
    });

    it('rejects a reducer that calls Date.now() (strict default)', () => {
        expect(() =>
            defineModule<{ v: number }>({
                name: 'bad-date',
                initialState: () => ({ v: 0 }),
                commands: {
                    go: (state) => ({ state: { v: Date.now() } }),
                },
            }),
        ).toThrow(/Date\.now/);
    });

    it('allows a non-deterministic reducer when strict: false, recording the violations', () => {
        const mod = defineModule<{ v: number }>(
            {
                name: 'vetted',
                initialState: () => ({ v: 0 }),
                commands: {
                    go: (state) => ({ state: { v: Math.random() } }),
                },
            },
            { strict: false },
        );
        expect(mod.name).toBe('vetted');
        expect(mod.__lint).toBeDefined();
        expect(mod.__lint!.some((v) => /Math\.random/.test(v))).toBe(true);
    });

    it('lintReducer returns empty for the clean demo reducers (no false positives)', () => {
        // The demo modules use ctx.now / ctx.id() / ctx.random() — through ctx,
        // never ambient globals — so they MUST pass the lint cleanly.
        for (const mod of [counter, notes, payments]) {
            for (const [name, fn] of Object.entries(mod.commands)) {
                expect(lintReducer(name, fn)).toEqual([]);
            }
        }
    });

    it('does not flag a state field merely named "random"', () => {
        // Guard against a false positive: a field named `random` is not access to
        // the Math.random global, so the module must define cleanly.
        const mod = defineModule<{ random: number }>({
            name: 'has-random-field',
            initialState: () => ({ random: 0 }),
            commands: {
                set: (state, input) => ({ state: { random: (input as { v: number }).v } }),
            },
        });
        expect(mod.name).toBe('has-random-field');
    });
});

describe('canonicalBytes', () => {
    it('is independent of key insertion order', () => {
        expect(canonicalBytes({ a: 1, b: 2 })).toBe(canonicalBytes({ b: 2, a: 1 }));
    });

    it('grows with content size', () => {
        const small = canonicalBytes({ items: ['x'] });
        const large = canonicalBytes({ items: Array.from({ length: 100 }, () => 'x') });
        expect(large).toBeGreaterThan(small);
    });
});

describe('deterministic resource bound: maxEffects', () => {
    /** A module whose `spam` command emits `input.n` effect intents. */
    const spammer = defineModule<{ count: number }>({
        name: 'spammer',
        initialState: () => ({ count: 0 }),
        commands: {
            spam: (state, input) => {
                const n = (input as { n: number }).n;
                return {
                    state: { count: state.count + 1 },
                    effects: Array.from({ length: n }, (_v, i) => ({
                        kind: 'noop',
                        idempotencyKey: `k-${i}`,
                        payload: {},
                    })),
                };
            },
        },
        queries: { count: (state) => state.count },
    });

    function host(): ModuleHost {
        const h = new ModuleHost({ maxEffects: 3 });
        h.register(spammer);
        return h;
    }

    it('rejects a command emitting more than maxEffects with 413, leaving state + outbox unchanged', () => {
        const h1 = host();
        const h2 = host();

        const res1 = h1.apply(cmd('spammer', 'spam', { n: 4 }, seed('s1')), META);
        const res2 = h2.apply(cmd('spammer', 'spam', { n: 4 }, seed('s1')), META);

        expect(res1.status).toBe(413);
        expect(res1.message).toMatch(/exceeding the limit of 3/);
        // State not adopted, no effects enqueued — identical on both hosts.
        expect(h1.query('spammer', 'count')).toBe(0);
        expect(h1.getOutbox()).toEqual([]);
        expect(res1).toEqual(res2);
        expect(h1.snapshot()).toEqual(h2.snapshot());
    });

    it('applies a command at the maxEffects boundary normally', () => {
        const h = host();
        const res = h.apply(cmd('spammer', 'spam', { n: 3 }, seed('s2')), META);
        expect(res.status).toBe(200);
        expect(res.effects).toHaveLength(3);
        expect(h.query('spammer', 'count')).toBe(1);
        expect(h.getOutbox()).toHaveLength(3);
    });
});

describe('deterministic resource bound: maxResultBytes', () => {
    /** A module whose `grow` command produces a state of roughly `input.n` bytes. */
    const grower = defineModule<{ blob: string }>({
        name: 'grower',
        initialState: () => ({ blob: '' }),
        commands: {
            grow: (state, input) => ({ state: { blob: 'x'.repeat((input as { n: number }).n) } }),
        },
        queries: { size: (state) => state.blob.length },
    });

    function host(): ModuleHost {
        const h = new ModuleHost({ maxResultBytes: 256 });
        h.register(grower);
        return h;
    }

    it('rejects an over-budget next-state with 413, leaving state unchanged (deterministic across hosts)', () => {
        const h1 = host();
        const h2 = host();

        const res1 = h1.apply(cmd('grower', 'grow', { n: 1000 }, seed('g1')), META);
        const res2 = h2.apply(cmd('grower', 'grow', { n: 1000 }, seed('g1')), META);

        expect(res1.status).toBe(413);
        expect(res1.message).toMatch(/exceeding the limit of 256 bytes/);
        expect(h1.query('grower', 'size')).toBe(0); // state not adopted
        expect(res1).toEqual(res2);
        expect(h1.snapshot()).toEqual(h2.snapshot());
    });

    it('applies an under-budget next-state normally', () => {
        const h = host();
        const res = h.apply(cmd('grower', 'grow', { n: 50 }, seed('g2')), META);
        expect(res.status).toBe(200);
        expect(h.query('grower', 'size')).toBe(50);
    });
});

describe('clean module under both budgets', () => {
    it('applies normally with default budgets', () => {
        const h = new ModuleHost();
        h.register(counter);
        const res = h.apply(cmd('counter', 'increment', { by: 5 }, seed('c1')), META);
        expect(res.status).toBe(200);
        expect(h.query('counter', 'value')).toBe(5);
    });
});
