import { Command } from '../consensus/types';
import { resolveSeed } from './context';
import { signCommand } from './signing';

/**
 * LEADER-SIDE builder for a generic `MODULE` command (ADR-0018, M4 consensus
 * wiring). This is where the runtime's non-determinism is resolved up front:
 * {@link resolveSeed} captures the ambient clock + a fresh random nonce ONCE on
 * the leader, and that seed is baked into the command before it enters the
 * replicated log. Every replica then derives the identical `ReducerContext` from
 * the committed seed, so module reducers stay pure and the cluster converges —
 * exactly the discipline `src/models/book.ts` uses to resolve ids/timestamps for
 * the book demo, generalized to the module runtime.
 *
 * Followers/replicas never call this; they apply the seed already in the log.
 */
export function buildModuleCommand(module: string, command: string, input: unknown): Command {
    return { type: 'MODULE', module, command, input, seed: resolveSeed() };
}

/**
 * Build a SIGNED `MODULE` command (ADR-0018 pillar 7). The originating actor
 * signs the LOGICAL command — `{ module, command, input, actor, requestId }` —
 * with its private key BEFORE this reaches the leader; the leader then resolves
 * the deterministic `seed` and forwards the signature unchanged.
 *
 * The `seed` is INTENTIONALLY excluded from the signed payload: the leader picks
 * it after the actor signs, so the actor cannot have signed over it, and (being
 * a non-security convergence value) it does not need to be authenticated. On
 * apply, every node recomputes this same logical payload from the committed
 * command + meta and verifies the signature against the actor's registered key,
 * so a leader that forged `actor` could not have produced a matching signature.
 *
 * The `actor`/`requestId` here MUST match the `CommandMeta` the command is
 * submitted with — verification rebuilds the payload from that meta, so a
 * mismatch (or a meta tampered by the leader) fails verification.
 */
export function buildSignedModuleCommand(
    module: string,
    command: string,
    input: unknown,
    opts: { actor: string; requestId: string; privateKeyPem: string },
): Command {
    const sig = signCommand(opts.privateKeyPem, {
        module,
        command,
        input,
        actor: opts.actor,
        requestId: opts.requestId,
    });
    return { type: 'MODULE', module, command, input, seed: resolveSeed(), sig };
}
