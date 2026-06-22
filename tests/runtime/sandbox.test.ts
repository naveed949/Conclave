import { defineModule } from '../../src/runtime/defineModule';
import { lintReducer } from '../../src/runtime/determinism';
import { ModuleHost } from '../../src/runtime/moduleHost';
import { counter } from '../../src/runtime/modules/counter';
import { compute } from '../../src/runtime/modules/compute';
import { compileReducer, runReducer } from '../../src/runtime/sandbox';
import { ModuleCommand, ReducerContext, Seed } from '../../src/runtime/types';

const META = { actor: 'tester', requestId: 'req-1' };

const seed = (nonce: string, timestamp = '2026-06-21T00:00:00.000Z'): Seed => ({ timestamp, nonce });

const cmd = (module: string, command: string, input: unknown, s: Seed): ModuleCommand => ({
    module,
    command,
    input,
    seed: s,
});

describe('determinism sandbox: structural enforcement on the apply path', () => {
    it('a sandboxed reducer that touches Date throws at apply (no lint reliance)', () => {
        // Register WITHOUT the static lint (strict: false) so the only thing that
        // can catch the Date access is the vm sandbox itself. This proves the
        // sandbox is the structural guarantee, not the lint.
        const badDate = defineModule<{ v: number }>(
            {
                name: 'bad-date-sbx',
                initialState: () => ({ v: 0 }),
                commands: {
                    // References the ambient Date global, which is ABSENT in the
                    // sandbox context -> ReferenceError at runtime.
                    go: (state) => ({ state: { v: (Date as any).now() } }),
                },
            },
            { strict: false, sandbox: true },
        );

        const host = new ModuleHost();
        host.register(badDate);

        const res = host.apply(cmd('bad-date-sbx', 'go', undefined, seed('d1')), META);
        // The thrown ReferenceError is caught by dispatch as a 500; state untouched.
        expect(res.status).toBe(500);
        expect(res.message).toMatch(/Date is not defined|not defined/);
        expect(host.getState('bad-date-sbx')).toEqual({ v: 0 });
    });

    it('a sandboxed reducer that calls Math.random() throws at apply', () => {
        const badRandom = defineModule<{ v: number }>(
            {
                name: 'bad-random-sbx',
                initialState: () => ({ v: 0 }),
                commands: {
                    // Math IS available (deterministic members), but `random` is
                    // removed from the sandbox's Math, so this throws.
                    go: (state) => ({ state: { v: (Math as any).random() } }),
                },
            },
            { strict: false, sandbox: true },
        );

        const host = new ModuleHost();
        host.register(badRandom);

        const res = host.apply(cmd('bad-random-sbx', 'go', undefined, seed('r1')), META);
        expect(res.status).toBe(500);
        // Either the getter throw (ReferenceError) or a not-a-function depending
        // on engine; both indicate Math.random was unavailable.
        expect(res.message).toMatch(/Math\.random|not a function|not available/);
        expect(host.getState('bad-random-sbx')).toEqual({ v: 0 });
    });

    it('a sandboxed reducer calling toLocaleString throws at apply (locale non-determinism blocked)', () => {
        // Register WITHOUT the lint (strict: false) so ONLY the sandbox can catch
        // the locale call. `Number.prototype.toLocaleString` is neutralized in the
        // sandbox context, so this throws at apply -> 500, state untouched.
        const badLocale = defineModule<{ v: string }>(
            {
                name: 'bad-locale-sbx',
                initialState: () => ({ v: '' }),
                commands: {
                    go: (state) => ({ state: { v: (1234.5).toLocaleString() } }),
                },
            },
            { strict: false, sandbox: true },
        );

        const host = new ModuleHost();
        host.register(badLocale);

        const res = host.apply(cmd('bad-locale-sbx', 'go', undefined, seed('l1')), META);
        expect(res.status).toBe(500);
        expect(res.message).toMatch(/locale|not a function|not available/i);
        expect(host.getState('bad-locale-sbx')).toEqual({ v: '' });
    });

    it('a sandboxed reducer using Intl throws at apply (Intl removed from the sandbox)', () => {
        const badIntl = defineModule<{ v: string }>(
            {
                name: 'bad-intl-sbx',
                initialState: () => ({ v: '' }),
                commands: {
                    // `Intl` is ABSENT in the sandbox -> ReferenceError at runtime.
                    go: (state) => ({ state: { v: new (Intl as any).NumberFormat().format(1234.5) } }),
                },
            },
            { strict: false, sandbox: true },
        );

        const host = new ModuleHost();
        host.register(badIntl);

        const res = host.apply(cmd('bad-intl-sbx', 'go', undefined, seed('i1')), META);
        expect(res.status).toBe(500);
        expect(res.message).toMatch(/Intl is not defined|not defined/);
        expect(host.getState('bad-intl-sbx')).toEqual({ v: '' });
    });

    it('the lint also flags a toLocaleString reducer (defense-in-depth)', () => {
        const violations = lintReducer('go', (state: { v: string }) => ({
            state: { v: (1234.5).toLocaleString() },
        }));
        expect(violations.some((v) => /toLocaleString/.test(v))).toBe(true);
    });

    it('rejects a sandboxed keyed module at registration (whole-state only this milestone)', () => {
        // Build a keyed-shaped definition with a sandbox flag and confirm the host
        // refuses it rather than silently ignoring the flag.
        const keyedish = { name: 'k', kind: 'keyed', initialState: () => ({}), commands: {}, sandbox: true };
        const host = new ModuleHost();
        expect(() => host.register(keyedish as any)).toThrow(/whole-state modules only/);
    });
});

describe('determinism sandbox: ctx still works inside the sandbox', () => {
    /** A sandboxed reducer pulling from ctx.now / ctx.id() / ctx.random(). */
    const ctxMod = defineModule<{ rows: Array<{ id: string; r: number; at: string }> }>(
        {
            name: 'ctx-sbx',
            initialState: () => ({ rows: [] }),
            commands: {
                go: (state, _input, ctx) => ({
                    state: { rows: [...state.rows, { id: ctx.id(), r: ctx.random(), at: ctx.now }] },
                }),
            },
        },
        { sandbox: true },
    );

    /** The same logic, NON-sandboxed, to compare values against. */
    const ctxModPlain = defineModule<{ rows: Array<{ id: string; r: number; at: string }> }>({
        name: 'ctx-plain',
        initialState: () => ({ rows: [] }),
        commands: {
            go: (state, _input, ctx) => ({
                state: { rows: [...state.rows, { id: ctx.id(), r: ctx.random(), at: ctx.now }] },
            }),
        },
    });

    it('produces the same deterministic ctx values as the non-sandboxed path', () => {
        const sandboxedHost = new ModuleHost();
        sandboxedHost.register(ctxMod);
        const plainHost = new ModuleHost();
        plainHost.register(ctxModPlain);

        const s = seed('beef', '2026-06-21T12:00:00.000Z');
        sandboxedHost.apply(cmd('ctx-sbx', 'go', undefined, s), META);
        plainHost.apply(cmd('ctx-plain', 'go', undefined, s), META);

        const sRow = (sandboxedHost.getState('ctx-sbx') as any).rows[0];
        const pRow = (plainHost.getState('ctx-plain') as any).rows[0];
        // Byte-identical id / random / timestamp from the same seed.
        expect(sRow).toEqual(pRow);
        expect(sRow.at).toBe('2026-06-21T12:00:00.000Z');
        expect(sRow.id).toMatch(/^[0-9a-f]{32}$/);
        expect(sRow.r).toBeGreaterThanOrEqual(0);
        expect(sRow.r).toBeLessThan(1);
    });

    it('two sandboxed hosts converge on identical state from the same command stream', () => {
        const build = (): ModuleHost => {
            const h = new ModuleHost();
            h.register(ctxMod);
            return h;
        };
        const h1 = build();
        const h2 = build();

        const stream = [
            cmd('ctx-sbx', 'go', undefined, seed('s1')),
            cmd('ctx-sbx', 'go', undefined, seed('s2')),
            cmd('ctx-sbx', 'go', undefined, seed('s1')), // reuse -> same id+random
        ];
        for (const c of stream) {
            h1.apply(c, META);
            h2.apply(c, META);
        }
        expect(h1.snapshot()).toEqual(h2.snapshot());
        const rows = (h1.getState('ctx-sbx') as any).rows;
        expect(rows).toHaveLength(3);
        // First and third reused the same seed -> identical row.
        expect(rows[0]).toEqual(rows[2]);
    });
});

describe('compute module: apply path is deterministic and timeout-free', () => {
    function host(): ModuleHost {
        const h = new ModuleHost();
        h.register(compute);
        return h;
    }

    it('sumTo computes the triangular number deterministically and converges', () => {
        const h1 = host();
        const h2 = host();
        const res1 = h1.apply(cmd('compute', 'sumTo', { n: 100 }, seed('c1')), META);
        const res2 = h2.apply(cmd('compute', 'sumTo', { n: 100 }, seed('c1')), META);

        expect(res1.status).toBe(200);
        expect((res1.result as { sum: number }).sum).toBe(5050); // 1..100
        expect(h1.query('compute', 'last')).toBe(5050);
        // Apply path uses NO timeout; both hosts agree byte-for-byte.
        expect(res1).toEqual(res2);
        expect(h1.snapshot()).toEqual(h2.snapshot());
    });
});

describe('leader-side step meter: admit()', () => {
    function host(): ModuleHost {
        // A ~100ms budget gives headroom on a slow CI box while still tripping
        // quickly: `spin` never terminates, so the vm interrupt fires at the
        // budget regardless of its size. A tighter budget (~25ms) risked flaking
        // close to the observed runtime; the assertion (ok === false) is unchanged.
        const h = new ModuleHost({ stepBudgetMs: 100 });
        h.register(compute);
        return h;
    }

    it('rejects the runaway `spin` command (budget exceeded) and leaves state UNCHANGED', () => {
        const h = host();
        const before = h.snapshot();

        const verdict = h.admit(cmd('compute', 'spin', undefined, seed('x1')), META);
        expect(verdict.ok).toBe(false);
        if (verdict.ok === false) {
            expect(verdict.status).toBe(503);
            expect(verdict.message).toMatch(/step budget/);
        }

        // Admission is a pure dry-run: no mutation, no audit, no outbox change.
        expect(h.snapshot()).toEqual(before);
        expect(h.auditSize()).toBe(0);
    });

    it('admits a normal command, which then applies', () => {
        const h = host();
        const verdict = h.admit(cmd('compute', 'sumTo', { n: 10 }, seed('x2')), META);
        expect(verdict.ok).toBe(true);
        // admit() did not mutate state.
        expect(h.query('compute', 'last')).toBe(0);
        expect(h.auditSize()).toBe(0);

        // The leader would now submit; apply runs without a timeout.
        const res = h.apply(cmd('compute', 'sumTo', { n: 10 }, seed('x2')), META);
        expect(res.status).toBe(200);
        expect(h.query('compute', 'last')).toBe(55); // 1..10
        expect(h.auditSize()).toBe(1);
    });
});

describe('coverage instrumentation: injected counter identifiers are tolerated', () => {
    const ctx: ReducerContext = {
        now: '2026-06-21T00:00:00.000Z',
        random: () => 0.5,
        id: () => 'id-1',
        actor: 'tester',
        requestId: 'req-1',
    };

    // When the suite runs under `jest --coverage`, Istanbul rewrites reducer
    // bodies to call a per-file counter (`cov_<hash>().f[0]++`). Because the
    // sandbox compiles from `fn.toString()`, that identifier reaches the vm as a
    // free global. We simulate that instrumentation explicitly here so the
    // regression is caught even when coverage is OFF.
    it('a reducer source that references a cov_<hash> counter still applies', () => {
        const source = `(state, input, ctx) => {
            cov_abc123def().f[0]++;
            cov_abc123def().s[1]++;
            const next = (cov_abc123def().b[0][0]++, ((state && state.n) || 0) + 1);
            return { state: { n: next }, result: { n: next } };
        }`;
        const compiled = compileReducer(source);
        const res = runReducer(compiled, { n: 41 }, undefined, ctx);
        expect((res.state as { n: number }).n).toBe(42);
        expect((res.result as { n: number }).n).toBe(42);
    });

    it('still throws for a genuinely banned global (the stub does not widen the sandbox)', () => {
        const source = `(state, input, ctx) => {
            cov_abc123def().f[0]++;
            return { state: { t: Date.now() } };
        }`;
        const compiled = compileReducer(source);
        expect(() => runReducer(compiled, {}, undefined, ctx)).toThrow();
    });
});

describe('back-compat: non-sandboxed modules and admit()', () => {
    it('a non-sandboxed module behaves exactly as before', () => {
        const h = new ModuleHost();
        h.register(counter);
        const res = h.apply(cmd('counter', 'increment', { by: 7 }, seed('b1')), META);
        expect(res.status).toBe(200);
        expect(h.query('counter', 'value')).toBe(7);
    });

    it('admit() returns ok for a non-sandboxed module (admission is a sandbox feature)', () => {
        const h = new ModuleHost();
        h.register(counter);
        expect(h.admit(cmd('counter', 'increment', { by: 1 }, seed('b2')), META)).toEqual({ ok: true });
        // It did not run the reducer / mutate state.
        expect(h.query('counter', 'value')).toBe(0);
    });

    it('admit() returns ok for an unknown module/command (404 surfaces on apply, not admission)', () => {
        const h = new ModuleHost();
        h.register(counter);
        expect(h.admit(cmd('ghost', 'go', {}, seed('b3')), META)).toEqual({ ok: true });
        expect(h.admit(cmd('counter', 'nope', {}, seed('b4')), META)).toEqual({ ok: true });
    });
});
