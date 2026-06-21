import { defineKeyedModule } from '../../src/runtime/keyedModule';
import { ModuleHost } from '../../src/runtime/moduleHost';
import { accounts } from '../../src/runtime/modules/accounts';
import { counter } from '../../src/runtime/modules/counter';
import { MemoryStateStore, StoreView } from '../../src/runtime/stateStore';
import { ModuleCommand, Seed } from '../../src/runtime/types';

const META = { actor: 'tester', requestId: 'req-1' };

const seed = (nonce: string, timestamp = '2026-06-21T00:00:00.000Z'): Seed => ({ timestamp, nonce });

const cmd = (module: string, command: string, input: unknown, s: Seed): ModuleCommand => ({
    module,
    command,
    input,
    seed: s,
});

describe('MemoryStateStore', () => {
    it('get/put/delete/has round-trip', () => {
        const store = new MemoryStateStore();
        expect(store.has('a')).toBe(false);
        expect(store.get('a')).toBeUndefined();

        store.put('a', { v: 1 });
        expect(store.has('a')).toBe(true);
        expect(store.get('a')).toEqual({ v: 1 });
        expect(store.size()).toBe(1);

        store.delete('a');
        expect(store.has('a')).toBe(false);
        expect(store.size()).toBe(0);
        store.delete('a'); // delete of absent key is a no-op
    });

    it('keys/entries are SORTED regardless of insertion order', () => {
        const store = new MemoryStateStore();
        store.put('c', 3);
        store.put('a', 1);
        store.put('b', 2);

        expect(store.keys()).toEqual(['a', 'b', 'c']);
        expect(store.entries()).toEqual([
            ['a', 1],
            ['b', 2],
            ['c', 3],
        ]);
        expect(store.snapshot()).toEqual([
            ['a', 1],
            ['b', 2],
            ['c', 3],
        ]);
    });

    it('deep-clones on read: mutating a returned record does not change the store', () => {
        const store = new MemoryStateStore();
        store.put('rec', { nested: { count: 0 } });

        const got = store.get('rec') as { nested: { count: number } };
        got.nested.count = 999;
        expect((store.get('rec') as { nested: { count: number } }).nested.count).toBe(0);

        // entries() and snapshot() also return clones.
        const entry = store.entries()[0][1] as { nested: { count: number } };
        entry.nested.count = 7;
        expect((store.get('rec') as { nested: { count: number } }).nested.count).toBe(0);
    });

    it('clones on write: mutating the stored object afterward does not change the store', () => {
        const store = new MemoryStateStore();
        const obj = { v: 1 };
        store.put('k', obj);
        obj.v = 2;
        expect(store.get('k')).toEqual({ v: 1 });
    });

    it('snapshot/restore round-trips', () => {
        const store = new MemoryStateStore();
        store.put('b', { x: 2 });
        store.put('a', { x: 1 });
        const dump = store.snapshot();

        const restored = new MemoryStateStore();
        restored.restore(dump);
        expect(restored.snapshot()).toEqual(dump);
        expect(restored.keys()).toEqual(['a', 'b']);

        // Restore is a wholesale replace, not a merge.
        restored.put('c', { x: 3 });
        restored.restore(dump);
        expect(restored.keys()).toEqual(['a', 'b']);
    });
});

describe('StoreView (transactional copy-on-write)', () => {
    it('reads-your-writes: buffered puts/deletes are visible before commit', () => {
        const store = new MemoryStateStore();
        store.put('a', 1);
        const view = new StoreView(store);

        expect(view.get('a')).toBe(1); // falls through to underlying store
        view.put('a', 10);
        expect(view.get('a')).toBe(10); // sees its own buffered write
        view.put('b', 2);
        expect(view.has('b')).toBe(true);

        view.delete('a');
        expect(view.get('a')).toBeUndefined();
        expect(view.has('a')).toBe(false);

        // Underlying store is still untouched (nothing committed).
        expect(store.get('a')).toBe(1);
        expect(store.has('b')).toBe(false);
    });

    it('keys/entries stay SORTED and reflect buffered puts and deletes', () => {
        const store = new MemoryStateStore();
        store.put('a', 1);
        store.put('c', 3);
        const view = new StoreView(store);

        view.put('b', 2); // new key
        view.delete('a'); // remove an underlying key

        expect(view.keys()).toEqual(['b', 'c']);
        expect(view.entries()).toEqual([
            ['b', 2],
            ['c', 3],
        ]);
        expect(view.size()).toBe(2);
    });

    it('a put cancels a prior buffered delete', () => {
        const store = new MemoryStateStore();
        store.put('a', 1);
        const view = new StoreView(store);
        view.delete('a');
        expect(view.has('a')).toBe(false);
        view.put('a', 5);
        expect(view.get('a')).toBe(5);
        expect(view.has('a')).toBe(true);
    });

    it('commit applies the buffer to the underlying store atomically', () => {
        const store = new MemoryStateStore();
        store.put('a', 1);
        const view = new StoreView(store);
        view.put('a', 10);
        view.put('b', 2);
        view.delete('c'); // no-op delete

        view.commit();
        expect(store.get('a')).toBe(10);
        expect(store.get('b')).toBe(2);
        expect(store.keys()).toEqual(['a', 'b']);
    });

    it('abandoning the view (no commit) writes nothing', () => {
        const store = new MemoryStateStore();
        store.put('a', 1);
        const view = new StoreView(store);
        view.put('a', 99);
        view.put('z', 100);
        // Never call commit(): the view is dropped.
        expect(store.get('a')).toBe(1);
        expect(store.has('z')).toBe(false);
    });

    it('clones on read so a reducer cannot mutate committed state via a fall-through read', () => {
        const store = new MemoryStateStore();
        store.put('rec', { n: 1 });
        const view = new StoreView(store);
        const got = view.get('rec') as { n: number };
        got.n = 42;
        expect((store.get('rec') as { n: number }).n).toBe(1);
        expect((view.get('rec') as { n: number }).n).toBe(1);
    });
});

describe('keyed module via ModuleHost: accounts', () => {
    function host(): ModuleHost {
        const h = new ModuleHost();
        h.register(accounts);
        return h;
    }

    it('open creates an account; deposit and transfer move funds across keys', () => {
        const h = host();

        const opened = h.apply(cmd('accounts', 'open', { id: 'alice' }, seed('o1')), META);
        expect(opened.status).toBe(200);
        expect(opened.result).toEqual({ id: 'alice', balance: 0, openedAt: '2026-06-21T00:00:00.000Z' });

        h.apply(cmd('accounts', 'open', { id: 'bob' }, seed('o2')), META);
        expect(h.query('accounts', 'count')).toBe(2);

        expect(h.apply(cmd('accounts', 'deposit', { id: 'alice', amount: 100 }, seed('d1')), META).status).toBe(200);
        expect(h.query('accounts', 'balance', { id: 'alice' })).toBe(100);

        expect(h.apply(cmd('accounts', 'transfer', { from: 'alice', to: 'bob', amount: 30 }, seed('t1')), META).status).toBe(200);
        expect(h.query('accounts', 'balance', { id: 'alice' })).toBe(70);
        expect(h.query('accounts', 'balance', { id: 'bob' })).toBe(30);
    });

    it('rejects opening an existing account, depositing/transferring on missing accounts, and insufficient funds', () => {
        const h = host();
        h.apply(cmd('accounts', 'open', { id: 'alice' }, seed('o1')), META);

        expect(h.apply(cmd('accounts', 'open', { id: 'alice' }, seed('o1b')), META).status).toBe(500); // already exists
        expect(h.apply(cmd('accounts', 'deposit', { id: 'ghost', amount: 5 }, seed('d2')), META).status).toBe(500);

        h.apply(cmd('accounts', 'open', { id: 'bob' }, seed('o2')), META);
        h.apply(cmd('accounts', 'deposit', { id: 'alice', amount: 10 }, seed('d3')), META);

        // Insufficient funds: transfer rejected and NEITHER side changed.
        const res = h.apply(cmd('accounts', 'transfer', { from: 'alice', to: 'bob', amount: 50 }, seed('t2')), META);
        expect(res.status).toBe(500);
        expect(res.message).toMatch(/insufficient/);
        expect(h.query('accounts', 'balance', { id: 'alice' })).toBe(10);
        expect(h.query('accounts', 'balance', { id: 'bob' })).toBe(0);
    });

    it('a throwing keyed reducer commits NOTHING (store unchanged)', () => {
        const thrower = defineKeyedModule({
            name: 'thrower',
            commands: {
                wreck: (store) => {
                    store.put('a', 1);
                    store.put('b', 2);
                    throw new Error('boom after buffered writes');
                },
            },
            queries: { count: (store) => store.size() },
        });
        const h = new ModuleHost();
        h.register(thrower);

        const res = h.apply(cmd('thrower', 'wreck', undefined, seed('w1')), META);
        expect(res.status).toBe(500);
        // The buffered puts never reached the store.
        expect(h.query('thrower', 'count')).toBe(0);
        expect(h.getStore('thrower')!.keys()).toEqual([]);
    });

    it('determinism/convergence: two hosts running the same keyed command stream reach deep-equal snapshots', () => {
        const build = (): ModuleHost => {
            const h = new ModuleHost();
            h.register(accounts);
            return h;
        };
        const h1 = build();
        const h2 = build();

        const stream: ModuleCommand[] = [
            cmd('accounts', 'open', { id: 'alice' }, seed('s1', '2026-01-01T00:00:00.000Z')),
            cmd('accounts', 'open', { id: 'bob' }, seed('s2', '2026-02-02T00:00:00.000Z')),
            cmd('accounts', 'deposit', { id: 'alice', amount: 100 }, seed('s3')),
            cmd('accounts', 'transfer', { from: 'alice', to: 'bob', amount: 40 }, seed('s4')),
            cmd('accounts', 'deposit', { id: 'bob', amount: 5 }, seed('s5')),
        ];
        // Apply in a DIFFERENT incidental order would still converge, but here we
        // simply assert identical stream -> identical sorted snapshot.
        for (const c of stream) {
            h1.apply(c, META);
            h2.apply(c, META);
        }
        expect(h1.snapshot()).toEqual(h2.snapshot());
        expect(h1.auditRoot()).toBe(h2.auditRoot());
    });

    it('cross-order convergence: independent keyed commands in DIFFERENT incidental orders reach the same store state', () => {
        // The keyed store's sorted-iteration + clone-on-read design exists so that
        // the INCIDENTAL apply order of INDEPENDENT commands does not affect the
        // committed record state. This is the stronger claim the same-stream test
        // above does not exercise: here the two hosts see the same SET of commands
        // but in DIFFERENT orders.
        //
        // The commands are mutually commutative: two `open`s for distinct ids and
        // two `deposit`s into distinct accounts never touch a shared key, so no
        // ordering creates a read-write conflict. (We deliberately avoid `transfer`
        // or a second deposit to the same id — those are order-SENSITIVE on the
        // business level, not just incidentally.) Each command carries the same
        // leader-resolved seed in both orders, so any timestamp/id is identical.
        const cOpenA = cmd('accounts', 'open', { id: 'alice' }, seed('s-a', '2026-01-01T00:00:00.000Z'));
        const cOpenB = cmd('accounts', 'open', { id: 'bob' }, seed('s-b', '2026-02-02T00:00:00.000Z'));
        const cDepA = cmd('accounts', 'deposit', { id: 'alice', amount: 100 }, seed('s-da'));
        const cDepB = cmd('accounts', 'deposit', { id: 'bob', amount: 250 }, seed('s-db'));

        // Host 1: opens first, then deposits.
        const h1 = new ModuleHost();
        h1.register(accounts);
        for (const c of [cOpenA, cOpenB, cDepA, cDepB]) expect(h1.apply(c, META).status).toBe(200);

        // Host 2: a DIFFERENT incidental order — interleave open/deposit per id, and
        // process bob before alice. Every command still succeeds because each id's
        // open precedes its own deposit; the cross-id order is free to vary.
        const h2 = new ModuleHost();
        h2.register(accounts);
        for (const c of [cOpenB, cDepB, cOpenA, cDepA]) expect(h2.apply(c, META).status).toBe(200);

        // The STATE STORE converges: the accounts module's record store is
        // deep-equal across the two hosts despite the different apply order. The
        // store sorts keys and clones values, so insertion order leaves no trace.
        expect(h1.getStore('accounts')!.snapshot()).toEqual(h2.getStore('accounts')!.snapshot());
        // Both hosts agree on the per-account balances, too.
        expect(h1.query('accounts', 'balance', { id: 'alice' })).toBe(100);
        expect(h2.query('accounts', 'balance', { id: 'alice' })).toBe(100);
        expect(h1.query('accounts', 'balance', { id: 'bob' })).toBe(250);
        expect(h2.query('accounts', 'balance', { id: 'bob' })).toBe(250);

        // The AUDIT root legitimately DIFFERS: the audit is an ordered hash-chain
        // (each leaf records the command in APPLY order), so feeding the same
        // commands in a different order produces a different sequence of leaves and
        // thus a different root. That is by design — the audit is order-SENSITIVE
        // (it records "what ran, in what order"), whereas the keyed STORE state is
        // order-INDEPENDENT for commuting commands. Asserting equality on the full
        // host `snapshot()` would fail on the `__audit`/`__outbox`-free state only
        // if order mattered; we scope to the store precisely because the audit must
        // NOT be expected to match here.
        expect(h1.auditRoot()).not.toBe(h2.auditRoot());
    });

    it('snapshot/restore of a keyed module round-trips', () => {
        const h = host();
        h.apply(cmd('accounts', 'open', { id: 'alice' }, seed('o1')), META);
        h.apply(cmd('accounts', 'deposit', { id: 'alice', amount: 50 }, seed('d1')), META);

        const snap = h.snapshot();
        // The keyed module's dump is the SORTED [key, value][] entries.
        expect(snap.accounts).toEqual([['alice', { id: 'alice', balance: 50, openedAt: '2026-06-21T00:00:00.000Z' }]]);

        const restored = host();
        restored.restore(snap);
        expect(restored.snapshot()).toEqual(snap);
        expect(restored.query('accounts', 'balance', { id: 'alice' })).toBe(50);
        expect(restored.query('accounts', 'count')).toBe(1);
    });

    it('the determinism lint rejects a keyed reducer that calls Date.now()', () => {
        expect(() =>
            defineKeyedModule({
                name: 'bad-keyed',
                commands: {
                    go: (store) => {
                        store.put('t', Date.now());
                        return {};
                    },
                },
            }),
        ).toThrow(/Date\.now/);
    });

    it('the write-count budget rejects an over-maxWrites reducer with 413 and no writes', () => {
        const bulk = defineKeyedModule({
            name: 'bulk',
            commands: {
                fill: (store, input) => {
                    const n = (input as { n: number }).n;
                    for (let i = 0; i < n; i += 1) store.put(`k-${i}`, i);
                    return {};
                },
            },
            queries: { count: (store) => store.size() },
        });
        const h1 = new ModuleHost({ maxWrites: 3 });
        const h2 = new ModuleHost({ maxWrites: 3 });
        h1.register(bulk);
        h2.register(bulk);

        const res1 = h1.apply(cmd('bulk', 'fill', { n: 4 }, seed('b1')), META);
        const res2 = h2.apply(cmd('bulk', 'fill', { n: 4 }, seed('b1')), META);
        expect(res1.status).toBe(413);
        expect(res1.message).toMatch(/exceeding the limit of 3/);
        expect(h1.query('bulk', 'count')).toBe(0); // nothing committed
        expect(res1).toEqual(res2);
        expect(h1.snapshot()).toEqual(h2.snapshot());

        // At the boundary it applies normally.
        const ok = h1.apply(cmd('bulk', 'fill', { n: 3 }, seed('b2')), META);
        expect(ok.status).toBe(200);
        expect(h1.query('bulk', 'count')).toBe(3);
    });

    it('the write-size budget rejects an over-maxResultBytes keyed reducer with 413 and no writes', () => {
        const grower = defineKeyedModule({
            name: 'keyed-grower',
            commands: {
                grow: (store, input) => {
                    store.put('blob', 'x'.repeat((input as { n: number }).n));
                    return {};
                },
            },
            queries: { has: (store) => store.has('blob') },
        });
        const h = new ModuleHost({ maxResultBytes: 256 });
        h.register(grower);

        const res = h.apply(cmd('keyed-grower', 'grow', { n: 1000 }, seed('g1')), META);
        expect(res.status).toBe(413);
        expect(res.message).toMatch(/bytes/);
        expect(h.query('keyed-grower', 'has')).toBe(false);
    });
});

describe('back-compat: whole-state and keyed modules coexist', () => {
    it('a host with both counter (whole-state) and accounts (keyed) snapshots/restores both', () => {
        const h = new ModuleHost();
        h.registerModules([counter, accounts]);

        h.apply(cmd('counter', 'increment', { by: 7 }, seed('c1')), META);
        h.apply(cmd('accounts', 'open', { id: 'alice' }, seed('a1')), META);
        h.apply(cmd('accounts', 'deposit', { id: 'alice', amount: 12 }, seed('a2')), META);

        const snap = h.snapshot();

        const restored = new ModuleHost();
        restored.registerModules([counter, accounts]);
        restored.restore(snap);

        expect(restored.snapshot()).toEqual(snap);
        expect(restored.query('counter', 'value')).toBe(7);
        expect(restored.query('accounts', 'balance', { id: 'alice' })).toBe(12);
        expect(restored.auditRoot()).toBe(h.auditRoot());
    });
});
