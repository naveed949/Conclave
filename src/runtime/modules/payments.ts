import { defineModule } from '../defineModule';

/** An order tracked through the charge lifecycle. */
interface Order {
    id: string;
    amount: number;
    status: 'pending' | 'paid' | 'failed';
}

interface PaymentsState {
    orders: Record<string, Order>;
}

/** Input to `charge`. */
interface ChargeInput {
    orderId: string;
    amount: number;
}

/**
 * Input to `settle`, the `onResult` target. The host feeds it the effect's key
 * plus the edge-resolved `result`; here `result` carries the gateway outcome.
 */
interface SettleInput {
    idempotencyKey: string;
    result: { orderId: string; ok: boolean };
}

/**
 * A demo module for the committed-intent effect model (ADR-0019 pillar 3).
 *
 * `charge` is a pure reducer: it records the order as `pending` and EMITS an
 * effect intent rather than calling a payment gateway itself. The intent commits
 * to the log via the outbox; the edge executor performs the real charge and feeds
 * the outcome back, which `settle` folds into state. Both reducers are pure — the
 * only side effect lives in the executor's handler, off the deterministic path.
 */
export const payments = defineModule<PaymentsState>({
    name: 'payments',
    initialState: () => ({ orders: {} }),
    commands: {
        charge: (state, input, ctx) => {
            const { orderId, amount } = (input ?? {}) as ChargeInput;
            const order: Order = { id: orderId, amount, status: 'pending' };
            return {
                state: { orders: { ...state.orders, [orderId]: order } },
                result: order,
                // `ctx.id()` is deterministic, so the idempotency key is identical
                // on every replica — the outbox dedups consistently across hosts.
                effects: [
                    {
                        kind: 'http',
                        idempotencyKey: ctx.id(),
                        payload: { orderId, amount },
                        onResult: { module: 'payments', command: 'settle' },
                    },
                ],
            };
        },
        settle: (state, input) => {
            const { result } = (input ?? {}) as SettleInput;
            const existing = state.orders[result.orderId];
            if (!existing) {
                // Nothing to settle; return state unchanged. Pure no-op.
                return { state };
            }
            const settled: Order = { ...existing, status: result.ok ? 'paid' : 'failed' };
            return {
                state: { orders: { ...state.orders, [result.orderId]: settled } },
                result: settled,
            };
        },
    },
    queries: {
        order: (state, args) => state.orders[(args as { orderId: string }).orderId],
        list: (state) => Object.values(state.orders),
    },
});
