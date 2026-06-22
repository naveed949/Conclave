// Browser-safe async SHA-256 (ADR-0023, M29).
//
// The audit hash-chain on the server uses Node's sync `crypto`. An edge replica
// must re-derive the SAME chain in the browser, where the only hashing primitive
// is the async WebCrypto API. `globalThis.crypto.subtle` and `TextEncoder` are
// globals in BOTH Node 20+ and modern browsers, so this one implementation runs
// in either environment with no Node builtin and no extra dependency.
//
// The project's tsconfig has no DOM lib, so we declare the minimal structural
// shape of the WebCrypto bits we touch rather than depend on `lib.dom`.

/** Hashes a string to a lowercase hex SHA-256 digest. Async (WebCrypto). */
export type Sha256Hex = (input: string) => Promise<string>;

interface SubtleLike {
    digest(algorithm: string, data: Uint8Array): Promise<ArrayBuffer>;
}
interface CryptoLike {
    subtle: SubtleLike;
}

const HEX = '0123456789abcdef';

function toHex(buf: ArrayBuffer): string {
    const bytes = new Uint8Array(buf);
    let out = '';
    for (let i = 0; i < bytes.length; i++) {
        const b = bytes[i];
        out += HEX[(b >> 4) & 0xf] + HEX[b & 0xf];
    }
    return out;
}

/**
 * Default {@link Sha256Hex}: hashes via `globalThis.crypto.subtle` (WebCrypto).
 * Works unchanged in Node 20+ and the browser. Produces the identical lowercase
 * hex digest the server's Node `crypto` produces for the same input, so the two
 * audit chains match byte-for-byte.
 */
export const webcryptoSha256Hex: Sha256Hex = async (input: string): Promise<string> => {
    const subtle = (globalThis as unknown as { crypto?: CryptoLike }).crypto?.subtle;
    if (!subtle) {
        throw new Error('WebCrypto (globalThis.crypto.subtle) is unavailable in this environment');
    }
    const data = new TextEncoder().encode(input);
    const digest = await subtle.digest('SHA-256', data);
    return toHex(digest);
};
