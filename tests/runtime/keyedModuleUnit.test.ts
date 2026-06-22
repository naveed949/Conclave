import { defineKeyedModule } from '../../src/runtime/keyedModule';
import { StoreView } from '../../src/runtime/stateStore';

/**
 * Unit coverage of `defineKeyedModule`'s VALIDATION branches. `keyedStore.test.ts`
 * exercises keyed modules end-to-end through `ModuleHost` (and the strict-default
 * lint via Date.now), but not the definition-time guards themselves: empty name,
 * the `__`-reserved-name rejection, the no-commands / empty-command-name guards,
 * and the non-strict `{ strict: false }` path that records `__lint` instead of
 * throwing.
 */

/** A trivially clean keyed reducer (passes the determinism lint). */
const ok = (store: StoreView, input: unknown) => {
    store.put('k', input);
    return {};
};

describe('defineKeyedModule: name validation', () => {
    it('rejects an empty name', () => {
        expect(() => defineKeyedModule({ name: '', commands: { go: ok } })).toThrow(/non-empty name/);
    });

    it('rejects a whitespace-only name', () => {
        expect(() => defineKeyedModule({ name: '   ', commands: { go: ok } })).toThrow(/non-empty name/);
    });

    it('rejects a "__"-prefixed name (reserved for runtime internals)', () => {
        expect(() => defineKeyedModule({ name: '__outbox', commands: { go: ok } })).toThrow(
            /reserved.*"__"|"__".*reserved/,
        );
    });
});

describe('defineKeyedModule: command validation', () => {
    it('rejects a module with no commands', () => {
        expect(() => defineKeyedModule({ name: 'empty', commands: {} })).toThrow(
            /must define at least one command/,
        );
    });

    it('rejects a command with an empty name', () => {
        expect(() => defineKeyedModule({ name: 'bad-cmd', commands: { '': ok } })).toThrow(
            /command with an empty name/,
        );
    });

    it('treats an entirely absent commands map as no commands', () => {
        // `commands` omitted: the `?? {}` default yields zero command names.
        expect(() => defineKeyedModule({ name: 'no-commands-field' } as never)).toThrow(
            /must define at least one command/,
        );
    });
});

describe('defineKeyedModule: success', () => {
    it('stamps kind: "keyed" and returns the definition for a valid module', () => {
        const mod = defineKeyedModule({
            name: 'good',
            version: '1',
            commands: { go: ok },
            queries: { count: (store) => store.size() },
        });
        expect(mod.name).toBe('good');
        expect(mod.kind).toBe('keyed');
        expect(mod.version).toBe('1');
        expect(mod.__lint).toBeUndefined();
        expect(Object.keys(mod.commands)).toEqual(['go']);
    });

    it('accepts an explicit kind: "keyed" without complaint', () => {
        const mod = defineKeyedModule({ name: 'explicit', kind: 'keyed', commands: { go: ok } });
        expect(mod.kind).toBe('keyed');
    });
});

describe('defineKeyedModule: determinism lint', () => {
    it('throws on a non-deterministic reducer under the strict default', () => {
        expect(() =>
            defineKeyedModule({
                name: 'bad-random',
                commands: {
                    go: (store) => {
                        store.put('k', Math.random());
                        return {};
                    },
                },
            }),
        ).toThrow(/determinism lint/);
    });

    it('records violations under __lint instead of throwing when strict: false', () => {
        const mod = defineKeyedModule(
            {
                name: 'vetted-keyed',
                commands: {
                    go: (store) => {
                        store.put('t', Date.now());
                        return {};
                    },
                },
            },
            { strict: false },
        );
        expect(mod.name).toBe('vetted-keyed');
        expect(mod.kind).toBe('keyed');
        expect(mod.__lint).toBeDefined();
        expect(mod.__lint!.some((v) => /Date\.now/.test(v))).toBe(true);
    });
});
