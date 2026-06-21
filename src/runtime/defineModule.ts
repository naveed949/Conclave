import { ModuleDefinition } from './types';

/**
 * Validate and return a module definition (ADR-0018 pillar 1: the single
 * declarative unit that replaces the four-touchpoint command workflow).
 *
 * Validation happens here, at definition time, so a malformed module fails loud
 * on startup rather than surfacing as a confusing dispatch error later. The
 * function is otherwise an identity — it returns the same object, typed — which
 * keeps authoring a module a single `defineModule({ ... })` call.
 */
export function defineModule<S>(def: ModuleDefinition<S>): ModuleDefinition<S> {
    if (!def.name || def.name.trim() === '') {
        throw new Error('Module definition requires a non-empty name');
    }

    const commandNames = Object.keys(def.commands ?? {});
    if (commandNames.length === 0) {
        throw new Error(`Module "${def.name}" must define at least one command`);
    }

    // Validate names exactly as they will be dispatched (untrimmed keys are what
    // `moduleHost.apply` looks up), so validation can't diverge from dispatch.
    // `Object.keys` already collapses duplicate literal keys, so only the
    // empty-name case needs rejecting here; an empty name makes dispatch ambiguous.
    for (const name of commandNames) {
        if (name === '') {
            throw new Error(`Module "${def.name}" has a command with an empty name`);
        }
    }

    return def;
}
