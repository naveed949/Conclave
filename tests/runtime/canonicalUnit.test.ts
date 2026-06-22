import { canonicalJson } from '../../src/runtime/canonical';

/**
 * Direct unit coverage of the shared canonical-JSON serializer (`canonical.ts`).
 * `determinism.test.ts` exercises it only via `canonicalBytes` (drop mode, size
 * bound); these tests hit the serializer's own branches head-on: the default
 * `'throw'` undefined mode (hash-preimage path), the lenient `'drop'` mode, nested
 * structures, and key-order independence — the determinism linchpin the module
 * exists to guarantee.
 */
describe('canonicalJson: undefined handling', () => {
    describe("'throw' mode (default — hash preimages)", () => {
        it('throws on a bare top-level undefined', () => {
            expect(() => canonicalJson(undefined)).toThrow(/undefined is not serializable/);
        });

        it('throws on an undefined-valued object property (would be silently dropped)', () => {
            expect(() => canonicalJson({ a: 1, b: undefined })).toThrow(/undefined is not serializable/);
        });

        it('throws on an undefined array element', () => {
            expect(() => canonicalJson([1, undefined, 3])).toThrow(/undefined is not serializable/);
        });

        it('throws on a nested undefined deep in the structure', () => {
            expect(() => canonicalJson({ outer: { inner: [{ x: undefined }] } })).toThrow(
                /undefined is not serializable/,
            );
        });

        it('is the default when opts is omitted or onUndefined is unset', () => {
            expect(() => canonicalJson(undefined, {})).toThrow(/undefined/);
        });
    });

    describe("'drop' mode (lenient — size bound)", () => {
        it('returns null for a bare top-level undefined (matches JSON.stringify(null))', () => {
            expect(canonicalJson(undefined, { onUndefined: 'drop' })).toBe('null');
        });

        it('omits undefined-valued object properties, mirroring JSON.stringify', () => {
            expect(canonicalJson({ a: 1, b: undefined, c: 3 }, { onUndefined: 'drop' })).toBe('{"a":1,"c":3}');
            expect(canonicalJson({ a: 1, b: undefined, c: 3 }, { onUndefined: 'drop' })).toBe(
                JSON.stringify({ a: 1, c: 3 }),
            );
        });

        it('normalizes an undefined array element to null, mirroring JSON.stringify', () => {
            expect(canonicalJson([1, undefined, 3], { onUndefined: 'drop' })).toBe('[1,null,3]');
            expect(canonicalJson([1, undefined, 3], { onUndefined: 'drop' })).toBe(JSON.stringify([1, undefined, 3]));
        });

        it('drops nested undefined object props but keeps the surrounding structure', () => {
            const v = { keep: 'yes', gone: undefined, nested: { a: undefined, b: 2 } };
            expect(canonicalJson(v, { onUndefined: 'drop' })).toBe('{"keep":"yes","nested":{"b":2}}');
        });
    });
});

describe('canonicalJson: primitives and structure', () => {
    it('serializes scalars exactly like JSON.stringify', () => {
        expect(canonicalJson(null)).toBe('null');
        expect(canonicalJson(42)).toBe('42');
        expect(canonicalJson(true)).toBe('true');
        expect(canonicalJson('hi')).toBe('"hi"');
        expect(canonicalJson('a "quoted" \n value')).toBe(JSON.stringify('a "quoted" \n value'));
    });

    it('serializes an empty object and empty array', () => {
        expect(canonicalJson({})).toBe('{}');
        expect(canonicalJson([])).toBe('[]');
    });

    it('quotes object keys (including keys needing escaping)', () => {
        expect(canonicalJson({ 'a"b': 1 })).toBe('{"a\\"b":1}');
    });

    it('serializes nested objects and arrays recursively', () => {
        const v = { z: [1, { q: 2 }], a: { b: { c: [3, 4] } } };
        expect(canonicalJson(v)).toBe('{"a":{"b":{"c":[3,4]}},"z":[1,{"q":2}]}');
    });
});

describe('canonicalJson: key-order independence (determinism linchpin)', () => {
    it('produces identical bytes regardless of object key insertion order', () => {
        expect(canonicalJson({ a: 1, b: 2, c: 3 })).toBe(canonicalJson({ c: 3, a: 1, b: 2 }));
    });

    it('sorts keys recursively at every depth', () => {
        const a = { outer: { z: 1, a: 2 }, list: [{ y: 1, x: 2 }] };
        const b = { list: [{ x: 2, y: 1 }], outer: { a: 2, z: 1 } };
        expect(canonicalJson(a)).toBe(canonicalJson(b));
        expect(canonicalJson(a)).toBe('{"list":[{"x":2,"y":1}],"outer":{"a":2,"z":1}}');
    });

    it('preserves array element order (arrays are positional, not sorted)', () => {
        expect(canonicalJson([3, 1, 2])).toBe('[3,1,2]');
        expect(canonicalJson([3, 1, 2])).not.toBe(canonicalJson([1, 2, 3]));
    });
});
