import { defineProjection, ProjectionEvent } from '../projection';

/**
 * The note created by the `notes` module's `create` reducer — the shape that
 * arrives on `event.result` for a `notes.create` command. We only need the id
 * here; the rest of the note lives in the authoritative module state.
 */
interface CreatedNote {
    id: string;
}

/**
 * The folded read model: note ids grouped by the actor who created them, plus a
 * running total. This is exactly the index the authoritative `notes` module does
 * NOT keep — its state is a flat `notes` array, so answering "which notes did
 * actor X create?" against the module would be an O(n) scan on every call. The
 * projection maintains the grouping incrementally so the query is O(1) lookup.
 */
interface NoteIndexView {
    byActor: Record<string, string[]>;
    total: number;
}

/**
 * A demo CQRS projection over the `notes` module (ADR-0018 pillar 4).
 *
 * It proves the value of the read side: a derived, indexed view that answers
 * rich queries the raw module state cannot, built purely by folding the committed
 * command stream. The fold is pure and deterministic — no `Date`, no
 * `Math.random` — so the index is rebuildable from the log and convergent across
 * nodes.
 */
export const noteIndex = defineProjection<NoteIndexView>({
    name: 'noteIndex',
    init: () => ({ byActor: {}, total: 0 }),
    on: (view, event: ProjectionEvent) => {
        // Filter inside the fold: ignore everything but a successful note creation.
        if (event.module !== 'notes' || event.command !== 'create') {
            return view;
        }
        const note = event.result as CreatedNote | undefined;
        // Defensive: a create whose result lacks an id (e.g. a rejected apply that
        // still reached the read side) contributes nothing rather than corrupting
        // the index. The fold stays total and pure.
        if (!note || typeof note.id !== 'string') {
            return view;
        }

        // Build the NEXT view immutably — never mutate the input view in place, so
        // the fold is a clean pure function and replay is reproducible.
        const existing = view.byActor[event.actor] ?? [];
        return {
            byActor: { ...view.byActor, [event.actor]: [...existing, note.id] },
            total: view.total + 1,
        };
    },
    queries: {
        /** Note ids created by `actor`, in creation order (empty if none). */
        byActor: (view, args): string[] => {
            const actor = args as string;
            return view.byActor[actor] ?? [];
        },
        /** Total number of notes created across all actors. */
        total: (view): number => view.total,
        /** The set of actors that have created at least one note, sorted. */
        actors: (view): string[] => Object.keys(view.byActor).sort(),
    },
});
