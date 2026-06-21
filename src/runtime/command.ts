import { Command } from '../consensus/types';
import { resolveSeed } from './context';

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
