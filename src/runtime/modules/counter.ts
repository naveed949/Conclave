import { defineModule } from '../defineModule';

/** A trivial demo module: a single integer with increment/reset commands. */
interface CounterState {
    value: number;
}

/** Input to `increment`: an optional step (defaults to 1). */
interface IncrementInput {
    by?: number;
}

export const counter = defineModule<CounterState>({
    name: 'counter',
    initialState: () => ({ value: 0 }),
    commands: {
        increment: (state, input) => {
            const { by = 1 } = (input ?? {}) as IncrementInput;
            return { state: { value: state.value + by } };
        },
        reset: () => ({ state: { value: 0 } }),
    },
    queries: {
        value: (state) => state.value,
    },
});
