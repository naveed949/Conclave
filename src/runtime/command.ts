import { resolveSeed } from './context';
import { signCommand } from './signing';
import { EffectResultEntry, ModuleEffectResultCommand, ModuleInvokeCommand } from './types';

/**
 * LEADER-SIDE builder for a module application command (ADR-0019). This is where
 * the runtime's non-determinism is resolved up front: {@link resolveSeed}
 * captures the ambient clock + a fresh random nonce ONCE on the leader, and that
 * seed is baked into the command before it enters the replicated log. Every
 * replica then derives the identical `ReducerContext` from the committed seed, so
 * module reducers stay pure and the cluster converges — exactly the discipline
 * `src/models/book.ts` uses to resolve ids/timestamps for the book demo,
 * generalized to the module runtime.
 *
 * The returned command is an ordinary `AppCommand` (type `'MODULE'`): it plugs
 * into a `RaftNode` whose state machine is a {@link ModuleStateMachine}, with no
 * bespoke consensus-core support. `actor`/`requestId` are carried on the command
 * (the adapter has no separate meta) and MUST match the `CommandMeta` it is
 * submitted with. Followers/replicas never call this; they apply the committed
 * command as-is.
 */
export function buildModuleCommand(
    module: string,
    command: string,
    input: unknown,
    meta: { actor: string; requestId: string },
): ModuleInvokeCommand {
    return {
        type: 'MODULE',
        module,
        command,
        input,
        seed: resolveSeed(),
        actor: meta.actor,
        requestId: meta.requestId,
    };
}

/**
 * Build a SIGNED module command (ADR-0019 pillar 7). The originating actor signs
 * the LOGICAL command — `{ module, command, input, actor, requestId }` — with its
 * private key BEFORE this reaches the leader; the leader then resolves the
 * deterministic `seed` and forwards the signature unchanged.
 *
 * The `seed` is INTENTIONALLY excluded from the signed payload: the leader picks
 * it after the actor signs, so the actor cannot have signed over it, and (being
 * a non-security convergence value) it does not need to be authenticated. On
 * apply, every node recomputes this same logical payload from the committed
 * command and verifies the signature against the actor's registered key, so a
 * leader that forged `actor` could not have produced a matching signature.
 */
export function buildSignedModuleCommand(
    module: string,
    command: string,
    input: unknown,
    opts: { actor: string; requestId: string; privateKeyPem: string },
): ModuleInvokeCommand {
    const sig = signCommand(opts.privateKeyPem, {
        module,
        command,
        input,
        actor: opts.actor,
        requestId: opts.requestId,
    });
    return {
        type: 'MODULE',
        module,
        command,
        input,
        seed: resolveSeed(),
        actor: opts.actor,
        requestId: opts.requestId,
        sig,
    };
}

/**
 * Build the committed effect-result command (M12). The leader's `EffectDriver`
 * calls this after a handler resolves a pending effect, so the resolved
 * {@link EffectResultEntry} replicates and routes to `ModuleHost.applyEffectResult`
 * on every node.
 *
 * The actor is the system identity `'system'` (no human originated this — the
 * runtime did) and the `requestId` is STABLE, derived from the entry's
 * idempotency key (`effect:${entry.idempotencyKey}`). That stable id means the
 * SUBSTRATE's dedup cache treats a re-submitted result for the same effect as a
 * replay (returns the cached apply result without re-routing), layering on top of
 * `applyEffectResult` itself being idempotent. The entry already carries its own
 * leader/edge-resolved `seed`; this does NOT re-resolve one.
 */
export function buildEffectResultCommand(entry: EffectResultEntry): ModuleEffectResultCommand {
    return {
        type: 'MODULE_EFFECT_RESULT',
        entry,
        actor: 'system',
        requestId: `effect:${entry.idempotencyKey}`,
    };
}
