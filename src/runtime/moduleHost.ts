import { createContext } from './context';
import { moduleCodeHash } from './codeHash';
import { canonicalBytes } from './determinism';
import { KeyedModuleDefinition } from './keyedModule';
import { AuditLeaf, MerkleAudit, MerkleProof } from './merkleAudit';
import { KeyRegistry, SignablePayload, verifyCommand } from './signing';
import { MemoryStateStore, StateStore, StoreView } from './stateStore';
import {
    EffectIntent,
    EffectResultEntry,
    ModuleApplyResult,
    ModuleCommand,
    ModuleDefinition,
    ModuleHostOptions,
    OutboxEntry,
} from './types';

/** Default ceiling on effects emitted by a single command. See `ModuleHostOptions`. */
const DEFAULT_MAX_EFFECTS = 16;
/** Default ceiling on the canonical byte size of a command's next-state (64 KiB). */
const DEFAULT_MAX_RESULT_BYTES = 64 * 1024;
/** Default ceiling on the buffered writes a single KEYED command may make. */
const DEFAULT_MAX_WRITES = 256;

/**
 * A registrable module: either a whole-state `ModuleDefinition` (no `kind`, the
 * original model) or a key-oriented `KeyedModuleDefinition` (`kind: 'keyed'`,
 * ADR-0018 pillar 4). The host discriminates on `kind`, defaulting absent to
 * whole-state for back-compat.
 */
export type AnyModuleDefinition = ModuleDefinition<unknown> | KeyedModuleDefinition;

/** Narrow an `AnyModuleDefinition` to the keyed variant. */
function isKeyed(def: AnyModuleDefinition): def is KeyedModuleDefinition {
    return (def as KeyedModuleDefinition).kind === 'keyed';
}

/**
 * Deep-clone via JSON round-trip. Used so snapshots are decoupled from live
 * state (mutating a host after snapshotting must not retroactively change the
 * snapshot, and vice versa). Module state is plain serializable data — the same
 * assumption the consensus-core snapshots already make — so JSON is sufficient
 * and avoids depending on `structuredClone` typings under the ES2017 lib target.
 */
function deepClone<T>(value: T): T {
    return JSON.parse(JSON.stringify(value)) as T;
}

/**
 * Registry of modules plus their live state (ADR-0018 pillars 1–2). This is the
 * deterministic runtime that a later milestone will drive from the replicated
 * `apply()` path. Today it stands alone: register modules, then `apply` commands
 * whose seeds were resolved on the leader. Because the host derives every
 * context from the command's seed, two hosts fed the same command stream reach
 * identical state — the property the convergence test asserts.
 */
export class ModuleHost {
    private readonly modules = new Map<string, AnyModuleDefinition>();
    /** Live per-module WHOLE-STATE blob, keyed by module name. */
    private readonly states = new Map<string, unknown>();
    /**
     * Per-module record store for KEYED modules (ADR-0018 pillar 4), keyed by
     * module name. A keyed module has an entry here and NONE in `states`; a
     * whole-state module is the reverse. The default `MemoryStateStore` is the
     * in-memory backend; the `StateStore` seam is where a persistent embedded
     * KV/LSM store would drop in (snapshots becoming store checkpoints).
     */
    private readonly stores = new Map<string, StateStore>();
    /**
     * The deterministic outbox (ADR-0018 pillar 3), keyed by `idempotencyKey`.
     * Every host derives it from the same committed command stream, so it is
     * itself replicated state — part of `snapshot()`/`restore()`. Keying on the
     * idempotency key is what makes enqueue idempotent: a replayed command can
     * re-run its reducer but never re-enqueue the same effect.
     */
    private readonly outbox = new Map<string, OutboxEntry>();
    /**
     * Per-module code-version hash (ADR-0018 pillar 5), computed at `register()`
     * from the module's logic. Every audited leaf stamps the hash of the module
     * that produced it, so the history proves WHICH logic version ran.
     */
    private readonly codeHashes = new Map<string, string>();
    /**
     * The Merkle audit accumulator (ADR-0018 pillar 5). Replicated state: it is
     * derived purely from the applied command stream, so two hosts fed the same
     * stream produce the same `auditRoot()`. Folded into `snapshot()`/`restore()`.
     */
    private readonly audit = new MerkleAudit();
    /**
     * Monotonic audit sequence, advanced once per audited leaf. Deterministic
     * (driven only by the command stream), it is each leaf's `seq` / index.
     */
    private auditSeq = 0;

    /**
     * Deterministic resource bound (ADR-0018 pillar 6): the max effects one
     * command may emit and the max canonical byte size of the next-state it may
     * produce. Computed identically on every replica, so over-budget commands
     * are rejected uniformly — NOT a CPU meter (that is deferred; needs a vm).
     */
    private readonly maxEffects: number;
    private readonly maxResultBytes: number;
    private readonly maxWrites: number;

    /**
     * Optional actor->public-key registry (ADR-0018 pillar 7). When SET, every
     * caller `apply()` must carry a valid actor signature over its logical
     * command or it is rejected (401) before its reducer runs. When UNSET
     * (default), no verification happens and behavior is exactly as before —
     * the back-compat guarantee that keeps all existing tests green. Configured
     * per node before start (like modules), identical on every node, so
     * verification converges.
     */
    private keyRegistry?: KeyRegistry;

    constructor(opts: ModuleHostOptions = {}) {
        this.maxEffects = opts.maxEffects ?? DEFAULT_MAX_EFFECTS;
        this.maxResultBytes = opts.maxResultBytes ?? DEFAULT_MAX_RESULT_BYTES;
        this.maxWrites = opts.maxWrites ?? DEFAULT_MAX_WRITES;
    }

    /**
     * Configure the actor signature registry (ADR-0018 pillar 7). Call before
     * start, identically on every node. Once set, caller commands are verified
     * on the apply path; until set, verification is skipped (back-compat).
     */
    setKeyRegistry(reg: KeyRegistry): void {
        this.keyRegistry = reg;
    }

    /** Authorize `publicKeyPem` as the signer for `actor`, creating the registry if needed. */
    registerActorKey(actor: string, publicKeyPem: string): void {
        if (!this.keyRegistry) this.keyRegistry = new KeyRegistry();
        this.keyRegistry.registerActor(actor, publicKeyPem);
    }

    /**
     * Register a module and initialize its state. Accepts BOTH a whole-state
     * `ModuleDefinition` and a key-oriented `KeyedModuleDefinition` (discriminated
     * on `kind`, ADR-0018 pillar 4); a keyed module gets its own `StateStore`
     * instead of a whole-state blob. Throws on a duplicate name.
     */
    register(def: AnyModuleDefinition): void {
        if (!def.name || def.name.trim() === '') {
            throw new Error('Module definition requires a non-empty name');
        }
        // Reject reserved `__`-prefixed names: the snapshot stores the outbox
        // under the reserved `__outbox` key in the same flat module-states object,
        // so a `__`-prefixed module would silently collide (its state overwritten
        // on snapshot, misread as an outbox map on restore). Fail closed.
        if (def.name.startsWith('__')) {
            throw new Error(
                `Module name "${def.name}" is reserved: names starting with "__" are reserved for runtime internals`,
            );
        }
        if (this.modules.has(def.name)) {
            throw new Error(`Module "${def.name}" is already registered`);
        }
        this.modules.set(def.name, def);
        if (isKeyed(def)) {
            // Keyed module: give it an empty record store. The default in-memory
            // backend; the StateStore interface is the persistent-KV seam.
            this.stores.set(def.name, new MemoryStateStore());
        } else {
            this.states.set(def.name, def.initialState());
        }
        // Stamp the module's logic version now; every leaf it later produces
        // records this hash (ADR-0018 pillar 5).
        this.codeHashes.set(def.name, moduleCodeHash(def));
    }

    /** Register several modules (whole-state and/or keyed) in order. */
    registerModules(defs: AnyModuleDefinition[]): void {
        for (const def of defs) this.register(def);
    }

    /**
     * Dispatch a command: look up the module + reducer, build a deterministic
     * context from the command's seed, run the (pure) reducer against current
     * state, and adopt the returned state. Failures are returned as status codes
     * rather than thrown, mirroring the state machine's HTTP-style results, so a
     * single bad command never crashes the apply loop.
     */
    apply(cmd: ModuleCommand, meta: { actor: string; requestId: string }): ModuleApplyResult {
        // Actor signature verification (ADR-0018 pillar 7), if a registry is
        // configured. This runs FIRST so a forged/tampered/unsigned command never
        // reaches its reducer and never touches state or the outbox. Verification
        // is a pure, deterministic function of the committed command + meta
        // (canonical JSON + ed25519 verify, both deterministic), so every replica
        // rejects identically — the cluster cannot diverge on a bad signature.
        const denied = this.verifySignature(cmd, meta);
        if (denied) {
            // Audit the rejection at 401 for uniformity with other failure
            // statuses (a forged command IS part of the history), but do NOT run
            // the reducer and do NOT mutate state/outbox.
            this.recordAudit(cmd, meta, denied.status);
            return denied;
        }

        const result = this.dispatch(cmd, meta);
        if (result.status === 200) {
            // Record each emitted effect into the outbox as `pending`, but only if
            // its key is unknown — a replayed command must re-run deterministically
            // without re-enqueuing the same effect (the exactly-once dedup point).
            for (const intent of result.effects) {
                if (!this.outbox.has(intent.idempotencyKey)) {
                    this.outbox.set(intent.idempotencyKey, { intent, status: 'pending' });
                }
            }
        }
        return result;
    }

    /**
     * Verify a caller command's actor signature (ADR-0018 pillar 7). Returns a
     * deterministic 401 `ModuleApplyResult` to REJECT, or `undefined` to ALLOW.
     *
     * Back-compat: with no registry configured, always allows (returns
     * `undefined`) — verification is opt-in, so existing unsigned flows are
     * untouched. With a registry configured, a command is allowed ONLY if it
     * carries a `sig` AND the actor has a registered key AND the signature
     * verifies over the canonical LOGICAL payload (excluding `seed`). A missing
     * signature, an unknown actor, or an invalid signature all reject — this is
     * what stops a leader forging `actor`.
     */
    private verifySignature(
        cmd: ModuleCommand,
        meta: { actor: string; requestId: string },
    ): ModuleApplyResult | undefined {
        if (!this.keyRegistry) return undefined; // back-compat: no verification

        if (!cmd.sig) {
            return { status: 401, effects: [], message: 'Missing actor signature' };
        }
        const publicKey = this.keyRegistry.get(meta.actor);
        if (!publicKey) {
            return { status: 401, effects: [], message: `No registered key for actor "${meta.actor}"` };
        }
        // Reconstruct the EXACT logical payload the actor signed — the seed is
        // deliberately excluded (the leader adds it after signing).
        const payload: SignablePayload = {
            module: cmd.module,
            command: cmd.command,
            input: cmd.input,
            actor: meta.actor,
            requestId: meta.requestId,
        };
        if (!verifyCommand(publicKey, payload, cmd.sig)) {
            return { status: 401, effects: [], message: 'Invalid actor signature' };
        }
        return undefined; // verified
    }

    /**
     * The reducer dispatch path, shared by `apply` (caller commands) and
     * `applyEffectResult` (the committed `onResult` follow-up). It runs a pure
     * reducer against a clone of current state and adopts the result atomically;
     * it does NOT touch the outbox so callers can layer their own bookkeeping.
     */
    private dispatch(cmd: ModuleCommand, meta: { actor: string; requestId: string }): ModuleApplyResult {
        const def = this.modules.get(cmd.module);
        if (!def) {
            return { status: 404, effects: [], message: `Unknown module: ${cmd.module}` };
        }

        if (!def.commands[cmd.command]) {
            return {
                status: 404,
                effects: [],
                message: `Unknown command "${cmd.command}" on module "${cmd.module}"`,
            };
        }

        // Keyed modules take a separate, transactional (StoreView) path.
        if (isKeyed(def)) {
            return this.dispatchKeyed(def, cmd, meta);
        }

        const reducer = def.commands[cmd.command];
        const ctx = createContext(cmd.seed, meta);
        // Hand the reducer a deep clone, never the live reference: a reducer that
        // mutates `state` in place and then throws must not corrupt committed
        // host state. Live state is replaced atomically only on a clean return.
        const working = deepClone(this.states.get(cmd.module));

        // The reducer is about to run, so this command WILL be audited regardless
        // of its outcome (success, business-failure status, or thrown→500). The
        // earlier unknown-module/unknown-command 404s returned before this point
        // and are intentionally NOT audited — no logic ran for them.
        let outcome: ModuleApplyResult;
        try {
            const result = reducer(working, cmd.input, ctx);
            const effects = result.effects ?? [];

            // Deterministic resource bound (ADR-0018 pillar 6). Computed AFTER the
            // reducer returns, from its output alone — every replica derives the
            // same effect count and the same canonical byte size, so an
            // over-budget command is rejected identically everywhere (no
            // divergence). On rejection we DO NOT adopt the next-state and DO NOT
            // surface the effects: it is treated as a failed apply, audited with a
            // 413 status for uniformity (consistent with a business-failure
            // status), leaving committed state and the outbox untouched.
            if (effects.length > this.maxEffects) {
                outcome = {
                    status: 413,
                    effects: [],
                    message:
                        `Command "${cmd.command}" on module "${cmd.module}" emitted ${effects.length} ` +
                        `effects, exceeding the limit of ${this.maxEffects}`,
                };
            } else {
                const stateBytes = canonicalBytes(result.state);
                if (stateBytes > this.maxResultBytes) {
                    outcome = {
                        status: 413,
                        effects: [],
                        message:
                            `Command "${cmd.command}" on module "${cmd.module}" produced a ${stateBytes}-byte ` +
                            `state, exceeding the limit of ${this.maxResultBytes} bytes`,
                    };
                } else {
                    // Under budget: adopt the next-state and surface the reducer's
                    // explicit `result` value (not the whole ReducerResult).
                    this.states.set(cmd.module, result.state);
                    outcome = { status: 200, result: result.result, effects };
                }
            }
        } catch (err) {
            // A throwing reducer must not corrupt state or halt the host; report it.
            const message = err instanceof Error ? err.message : String(err);
            outcome = { status: 500, effects: [], message };
        }

        this.recordAudit(cmd, meta, outcome.status);
        return outcome;
    }

    /**
     * The KEYED reducer dispatch path (ADR-0018 pillar 4). Mirrors `dispatch`'s
     * atomicity and resource-bound envelope, but the reducer mutates a
     * transactional {@link StoreView} instead of returning a next-state blob:
     *
     *  - Build a copy-on-write `StoreView` over the module's `StateStore`. The
     *    reducer reads through it (clones, reads-its-own-writes) and buffers any
     *    `put`/`delete` — nothing reaches the store yet.
     *  - On a CLEAN, in-budget return, `commit()` the view atomically. On a thrown
     *    reducer OR a budget rejection, DISCARD the view (drop it) so the store is
     *    untouched — the same all-or-nothing guarantee whole-state modules get.
     *  - Resource bounds (deterministic, computed from the buffer after return):
     *    `maxEffects` (fan-out), `maxWrites` (records touched), and `maxResultBytes`
     *    over the canonical bytes of the buffered puts (write amplification). Over
     *    budget ⇒ 413, view discarded.
     *  - Audit exactly as the whole-state path (one envelope leaf per run).
     */
    private dispatchKeyed(
        def: KeyedModuleDefinition,
        cmd: ModuleCommand,
        meta: { actor: string; requestId: string },
    ): ModuleApplyResult {
        const reducer = def.commands[cmd.command];
        const ctx = createContext(cmd.seed, meta);
        const store = this.stores.get(cmd.module)!;
        // The view buffers all writes; the live store is touched only on commit().
        const view = new StoreView(store);

        let outcome: ModuleApplyResult;
        try {
            const result = reducer(view, cmd.input, ctx);
            const effects = result.effects ?? [];

            if (effects.length > this.maxEffects) {
                // Over fan-out budget: discard the view (no commit), 413.
                outcome = {
                    status: 413,
                    effects: [],
                    message:
                        `Command "${cmd.command}" on module "${cmd.module}" emitted ${effects.length} ` +
                        `effects, exceeding the limit of ${this.maxEffects}`,
                };
            } else if (view.pendingWriteCount() > this.maxWrites) {
                // Over write-count budget: discard the view, 413.
                outcome = {
                    status: 413,
                    effects: [],
                    message:
                        `Command "${cmd.command}" on module "${cmd.module}" buffered ` +
                        `${view.pendingWriteCount()} writes, exceeding the limit of ${this.maxWrites}`,
                };
            } else {
                const writeBytes = canonicalBytes(view.pendingPuts());
                if (writeBytes > this.maxResultBytes) {
                    // Over write-size budget: discard the view, 413.
                    outcome = {
                        status: 413,
                        effects: [],
                        message:
                            `Command "${cmd.command}" on module "${cmd.module}" buffered ${writeBytes} bytes ` +
                            `of writes, exceeding the limit of ${this.maxResultBytes} bytes`,
                    };
                } else {
                    // Clean + in budget: commit the buffer atomically and surface
                    // the reducer's explicit result value.
                    view.commit();
                    outcome = { status: 200, result: result.result, effects };
                }
            }
        } catch (err) {
            // A throwing keyed reducer commits NOTHING: the view is dropped here
            // with its buffer unapplied, so the store is exactly as before.
            const message = err instanceof Error ? err.message : String(err);
            outcome = { status: 500, effects: [], message };
        }

        this.recordAudit(cmd, meta, outcome.status);
        return outcome;
    }

    /**
     * Append one audit leaf for a command whose reducer ran. The leaf is a pure
     * function of the command + meta + outcome status + the producing module's
     * code hash, so every replica appends an identical leaf and the Merkle root
     * stays convergent. `seq` is the monotonic, deterministic audit index.
     */
    private recordAudit(cmd: ModuleCommand, meta: { actor: string; requestId: string }, status: number): void {
        const leaf: AuditLeaf = {
            seq: this.auditSeq,
            module: cmd.module,
            command: cmd.command,
            actor: meta.actor,
            requestId: meta.requestId,
            status,
            codeHash: this.codeHashes.get(cmd.module) ?? '',
        };
        this.audit.append(leaf);
        this.auditSeq += 1;
    }

    /** All outbox entries still awaiting execution at the edge. */
    pendingEffects(): EffectIntent[] {
        const out: EffectIntent[] = [];
        for (const name of [...this.outbox.keys()].sort()) {
            const entry = this.outbox.get(name)!;
            if (entry.status === 'pending') {
                // Deep-clone (like getOutbox) so a handler that mutates the intent
                // it receives cannot mutate the committed outbox state behind it.
                out.push(deepClone(entry.intent));
            }
        }
        return out;
    }

    /**
     * Apply a committed `EffectResultEntry` — the follow-up entry the executor
     * fed back after performing the effect at the edge. This runs on the
     * deterministic apply path on EVERY replica, so it must be a pure function of
     * the entry + current state, and it MUST be idempotent: a redelivered or
     * replayed result must not re-dispatch the `onResult` reducer.
     *
     * Idempotency rule: if the key is unknown or already `done`, no-op (200, no
     * dispatch). Otherwise mark the entry `done`, store the edge-resolved
     * `result`, and — if the intent named an `onResult` command — dispatch it
     * through the normal reducer path with the entry's leader-resolved `seed`, so
     * the consuming reducer stays deterministic.
     */
    applyEffectResult(entry: EffectResultEntry, meta: { actor: string; requestId: string }): ModuleApplyResult {
        // First-applied wins. A handler retry that fires before the first result
        // is applied can commit multiple `EffectResultEntry` values for one key
        // (each drain that completes submits one). That is fine: the FIRST entry
        // applied flips the key to `done`; every later entry for that key hits the
        // `done` guard below and no-ops. Convergence holds because the committed
        // log order is identical on every replica, so all replicas apply the same
        // "first" entry and discard the same rest.
        const slot = this.outbox.get(entry.idempotencyKey);
        if (!slot || slot.status === 'done') {
            // Unknown or already-applied: exactly-once at the state level means
            // this is a harmless no-op, never a second `onResult` dispatch.
            return { status: 200, effects: [] };
        }

        slot.status = 'done';
        slot.result = entry.result;

        const onResult = slot.intent.onResult;
        if (!onResult) {
            return { status: 200, effects: [] };
        }

        // Feed the edge-resolved result to the consuming reducer. We go through
        // `apply` (not bare `dispatch`) so any effects the onResult reducer emits
        // are themselves enqueued into the outbox.
        return this.apply(
            {
                module: onResult.module,
                command: onResult.command,
                input: { idempotencyKey: entry.idempotencyKey, result: entry.result },
                seed: entry.seed,
            },
            meta,
        );
    }

    /**
     * The outbox as a list in deterministic (sorted-by-key) order. For tests and
     * inspection; the canonical store is the keyed map.
     */
    getOutbox(): OutboxEntry[] {
        return [...this.outbox.keys()].sort().map((k) => deepClone(this.outbox.get(k)!));
    }

    /** Run a read query against current state. Never mutates. */
    query(module: string, name: string, args?: unknown): unknown {
        const def = this.modules.get(module);
        if (!def) {
            throw new Error(`Unknown module: ${module}`);
        }
        const q = def.queries?.[name];
        if (!q) {
            throw new Error(`Unknown query "${name}" on module "${module}"`);
        }
        if (isKeyed(def)) {
            // A keyed query reads the module's StateStore (reads return clones).
            return q(this.stores.get(module)!, args);
        }
        return (q as (state: unknown, args: unknown) => unknown)(this.states.get(module), args);
    }

    /** Current live state of a module (for tests/snapshots). */
    getState(module: string): unknown {
        return this.states.get(module);
    }

    /**
     * The `StateStore` backing a keyed module (for tests/inspection). Returns the
     * LIVE store, not a copy — callers must NOT mutate it in production code
     * (writes must go through `apply()` so they are audited and replicated);
     * direct mutation here would diverge a replica.
     */
    getStore(module: string): StateStore | undefined {
        return this.stores.get(module);
    }

    /**
     * Serializable map of module name -> deep-cloned state. Keys are emitted in
     * sorted order so a `JSON.stringify` over the snapshot is stable regardless
     * of module registration order (helps the later audit/hash-chain milestone).
     */
    snapshot(): Record<string, unknown> {
        const states: Record<string, unknown> = {};
        for (const name of [...this.states.keys()].sort()) {
            states[name] = deepClone(this.states.get(name));
        }
        // KEYED modules contribute their store dump (SORTED entries) under the
        // module name, alongside whole-state blobs. `store.snapshot()` already
        // returns sorted, cloned entries — deterministic across replicas.
        for (const name of [...this.stores.keys()].sort()) {
            states[name] = this.stores.get(name)!.snapshot();
        }
        // The outbox is replicated state too: emit it under a reserved key, also
        // in sorted-key order so `JSON.stringify` over the snapshot is stable.
        const outbox: Record<string, OutboxEntry> = {};
        for (const key of [...this.outbox.keys()].sort()) {
            outbox[key] = deepClone(this.outbox.get(key)!);
        }
        // The Merkle audit + its seq are replicated state too: emit them under the
        // reserved `__audit` key (collision-safe — `__`-prefixed module names are
        // rejected at registration). Storing the leaves alone is sufficient; the
        // tree and root are a pure function of them, rebuilt on restore.
        const auditSnap = { seq: this.auditSeq, ...this.audit.snapshot() };
        return { ...states, __outbox: outbox, __audit: auditSnap };
    }

    /**
     * Replace the state of registered modules from a snapshot. Only modules that
     * are both registered AND present in the snapshot are restored; unknown keys
     * in the snapshot are ignored (a module may have been removed), and modules
     * absent from the snapshot keep their initialized state.
     */
    restore(snap: Record<string, unknown>): void {
        for (const [name, def] of this.modules) {
            if (!Object.prototype.hasOwnProperty.call(snap, name)) continue;
            if (isKeyed(def)) {
                // Route by registered kind: a keyed module restores its store from
                // the dumped [key, value][] entries (cloned by the store on restore).
                const entries = (snap[name] as [string, unknown][]) ?? [];
                this.stores.get(name)!.restore(entries);
            } else {
                this.states.set(name, deepClone(snap[name]));
            }
        }
        // Rebuild the outbox from its reserved key. Replacing wholesale (not
        // merging) keeps restore a faithful point-in-time reconstruction.
        this.outbox.clear();
        const saved = snap.__outbox;
        if (saved && typeof saved === 'object') {
            for (const [key, entry] of Object.entries(saved as Record<string, OutboxEntry>)) {
                this.outbox.set(key, deepClone(entry));
            }
        }
        // Rebuild the Merkle audit from its reserved key. The accumulator
        // recomputes all leaf hashes (and thus the root) deterministically, so a
        // restored host reports the identical `auditRoot()`.
        const auditSaved = snap.__audit as { seq?: number; leaves?: AuditLeaf[] } | undefined;
        this.audit.restore({ leaves: auditSaved?.leaves ?? [] });
        this.auditSeq = auditSaved?.seq ?? this.audit.size();
    }

    // ---- audit access (ADR-0018 pillar 5) ----

    /**
     * The compact Merkle root over all audited commands. A single hash that
     * summarizes the entire history and can be externally anchored; identical on
     * every replica that applied the same command stream.
     */
    auditRoot(): string {
        return this.audit.root();
    }

    /**
     * An O(log n) inclusion proof that the leaf at `seq` is part of the audited
     * history. Verifies against `auditRoot()` via `MerkleAudit.verify`.
     */
    auditProof(seq: number): MerkleProof {
        return this.audit.proof(seq);
    }

    /** Every audit leaf in append order (for inspection/tests). */
    auditEntries(): AuditLeaf[] {
        return this.audit.leaves();
    }

    /** Number of audited commands. */
    auditSize(): number {
        return this.audit.size();
    }

    /**
     * The code-version hash recorded for a module (or `undefined` if the module
     * is not registered). The same value every audited leaf from that module
     * carries — letting a verifier confirm which logic version produced a result.
     */
    moduleCodeHash(module: string): string | undefined {
        return this.codeHashes.get(module);
    }
}
