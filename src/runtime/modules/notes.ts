import { defineModule } from '../defineModule';

/** A single note. Its `id` and `createdAt` come from the deterministic ctx. */
interface Note {
    id: string;
    text: string;
    createdAt: string;
}

interface NotesState {
    notes: Note[];
}

/** Input to `create`. */
interface CreateInput {
    text: string;
}

/**
 * A demo module that mints ids and timestamps. It exists to prove the central
 * runtime contract: the id and createdAt below are NOT pulled from `crypto` or
 * `Date` inside the reducer — they flow from `ctx`, which is rebuilt identically
 * on every replica from the leader-resolved seed. Replace ctx with ambient
 * randomness here and the convergence test would fail.
 */
export const notes = defineModule<NotesState>({
    name: 'notes',
    initialState: () => ({ notes: [] }),
    commands: {
        create: (state, input, ctx) => {
            const { text } = (input ?? {}) as CreateInput;
            const note: Note = {
                id: ctx.id(),
                text,
                createdAt: ctx.now,
            };
            // Return the created note as the explicit `result` so callers get it
            // directly at `apply().result`, decoupled from the next-state object.
            return { state: { notes: [...state.notes, note] }, result: note };
        },
    },
    queries: {
        list: (state) => state.notes,
    },
});
