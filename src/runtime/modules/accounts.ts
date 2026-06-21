import { defineKeyedModule } from '../keyedModule';

/**
 * A demo KEYED module (ADR-0019 pillar 4, "state larger than RAM"): a ledger of
 * accounts where state is ONE RECORD PER ACCOUNT, addressed by account id. Every
 * command touches only the keys it needs — `deposit` reads/writes a single
 * account, `transfer` exactly two — so the whole ledger is NEVER loaded as a
 * blob. That is the per-key access the in-memory `Map` model could not express;
 * it is the property a persistent embedded KV/LSM backend exploits to scale state
 * past RAM behind the same `StateStore` seam.
 *
 * Determinism: `openedAt` comes from `ctx.now` (the leader-resolved seed), never
 * an ambient `Date` — the same purity contract every reducer obeys.
 */

/** One account record, stored at key = its id. */
interface Account {
    id: string;
    balance: number;
    openedAt: string;
}

export const accounts = defineKeyedModule({
    name: 'accounts',
    version: '1',
    commands: {
        /**
         * Open an account at key = id. Idempotent-ish: rejects 400 if the key is
         * already present rather than silently overwriting an existing balance.
         */
        open: (store, input, ctx) => {
            const { id } = (input ?? {}) as { id: string };
            if (!id) {
                throw new Error('open requires an id');
            }
            if (store.has(id)) {
                throw new Error(`account "${id}" already exists`);
            }
            const account: Account = { id, balance: 0, openedAt: ctx.now };
            store.put(id, account);
            return { result: account };
        },

        /** Read the single account by key, add `amount`, write it back. */
        deposit: (store, input) => {
            const { id, amount } = (input ?? {}) as { id: string; amount: number };
            if (!(amount > 0)) {
                throw new Error('deposit amount must be positive');
            }
            const account = store.get(id) as Account | undefined;
            if (!account) {
                throw new Error(`account "${id}" not found`);
            }
            account.balance += amount;
            store.put(id, account);
            return { result: account };
        },

        /**
         * Read the single account by key, subtract `amount`, write it back. The
         * single-key debit half of a CROSS-SHARD transfer (ADR-0020): a cross-shard
         * transfer cannot be one `transfer` command (the two accounts live in
         * different Raft groups), so the saga composes `withdraw` on the source
         * shard with `deposit` on the target shard. Rejects (non-200) on a missing
         * account or insufficient balance so the saga can compensate; because the
         * host commits the StoreView only on a clean return, a rejection writes
         * nothing — the balance is unchanged, never partially debited.
         */
        withdraw: (store, input) => {
            const { id, amount } = (input ?? {}) as { id: string; amount: number };
            if (!(amount > 0)) {
                throw new Error('withdraw amount must be positive');
            }
            const account = store.get(id) as Account | undefined;
            if (!account) {
                throw new Error(`account "${id}" not found`);
            }
            if (account.balance < amount) {
                throw new Error(`insufficient balance in "${id}"`);
            }
            account.balance -= amount;
            store.put(id, account);
            return { result: account };
        },

        /**
         * Move funds between two accounts, touching ONLY the two involved keys.
         * Rejects on a missing account or insufficient balance; because the host
         * commits the StoreView atomically, a rejection (thrown) writes neither
         * side — no partial transfer.
         */
        transfer: (store, input) => {
            const { from, to, amount } = (input ?? {}) as { from: string; to: string; amount: number };
            if (!(amount > 0)) {
                throw new Error('transfer amount must be positive');
            }
            if (from === to) {
                throw new Error('cannot transfer to the same account');
            }
            const src = store.get(from) as Account | undefined;
            if (!src) {
                throw new Error(`account "${from}" not found`);
            }
            const dst = store.get(to) as Account | undefined;
            if (!dst) {
                throw new Error(`account "${to}" not found`);
            }
            if (src.balance < amount) {
                throw new Error(`insufficient balance in "${from}"`);
            }
            src.balance -= amount;
            dst.balance += amount;
            store.put(from, src);
            store.put(to, dst);
            return { result: { from: src, to: dst } };
        },
    },
    queries: {
        /** Balance of one account, or `undefined` if it does not exist. */
        balance: (store, args) => {
            const { id } = (args ?? {}) as { id: string };
            const account = store.get(id) as Account | undefined;
            return account?.balance;
        },
        /** Number of accounts in the ledger. */
        count: (store) => store.size(),
    },
});
