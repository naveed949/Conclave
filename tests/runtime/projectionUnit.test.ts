import { defineProjection, ProjectionDefinition, ProjectionEvent } from '../../src/runtime/projection';

/**
 * Unit coverage of `defineProjection`'s VALIDATION branches. `projection.test.ts`
 * drives a pre-built projection (`noteIndex`) through `ProjectionHost`, but never
 * exercises the definition-time guards in `projection.ts`: empty name, a missing
 * `init()` factory, a missing `on()` fold, and the no-queries guard. A valid
 * definition is returned as-is (identity with light validation).
 */

type View = { total: number };

const validDef = (over: Partial<ProjectionDefinition<View>> = {}): ProjectionDefinition<View> => ({
    name: 'p',
    init: () => ({ total: 0 }),
    on: (view, _event: ProjectionEvent) => view,
    queries: { total: (view) => view.total },
    ...over,
});

describe('defineProjection: name validation', () => {
    it('rejects an empty name', () => {
        expect(() => defineProjection(validDef({ name: '' }))).toThrow(/non-empty name/);
    });

    it('rejects a whitespace-only name', () => {
        expect(() => defineProjection(validDef({ name: '   ' }))).toThrow(/non-empty name/);
    });
});

describe('defineProjection: shape validation', () => {
    it('rejects a definition missing the init() factory', () => {
        expect(() => defineProjection(validDef({ init: undefined as unknown as () => View }))).toThrow(
            /requires an init\(\) factory/,
        );
    });

    it('rejects a definition missing the on() fold', () => {
        expect(() =>
            defineProjection(validDef({ on: undefined as unknown as ProjectionDefinition<View>['on'] })),
        ).toThrow(/requires an on\(\) fold/);
    });

    it('rejects a definition with no queries', () => {
        expect(() => defineProjection(validDef({ queries: {} }))).toThrow(/must define at least one query/);
    });

    it('treats an entirely absent queries map as no queries', () => {
        // `queries` omitted: the `?? {}` default yields zero query names.
        const def = validDef();
        delete (def as { queries?: unknown }).queries;
        expect(() => defineProjection(def)).toThrow(/must define at least one query/);
    });
});

describe('defineProjection: success', () => {
    it('returns a valid definition unchanged (identity)', () => {
        const def = validDef();
        const result = defineProjection(def);
        expect(result).toBe(def);
        expect(result.name).toBe('p');
        // The returned fold/init/queries are the same callables it was given.
        expect(result.init()).toEqual({ total: 0 });
        expect(Object.keys(result.queries)).toEqual(['total']);
    });
});
