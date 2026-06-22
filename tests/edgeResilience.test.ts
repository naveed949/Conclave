import { EdgeReplica } from '../src/edge/edgeReplica';
import { LogStreamSource, StreamHandlers } from '../src/edge/types';
import { StateMachine } from '../src/consensus/stateMachine';
import { ApplyResult, LogEntry } from '../src/consensus/types';
import { waitFor } from './helpers';

/**
 * Drives {@link EdgeReplica}'s reconnect/resume/idempotency lifecycle through a
 * fully controllable fake stream source — the parts the HTTP-backed edge suites
 * don't reach: reconnect-after-error resumes from the applied cursor, replayed
 * entries are not re-applied, onCaughtUp advances past filtered (scoped) entries,
 * snapshot bootstrap, and waitForIndex resolve/reject.
 */

interface Cmd {
    type: string;
}

/** Counts the commands it applies, so a double-apply is observable. */
class CountingSM implements StateMachine<Cmd, number> {
    count = 0;
    apply(_cmd: Cmd): ApplyResult<number> {
        this.count += 1;
        return { status: 200, data: this.count };
    }
    snapshot(): unknown {
        return { count: this.count };
    }
    restore(data: unknown): void {
        this.count = (data as { count?: number })?.count ?? 0;
    }
}

/** A hand-driven LogStreamSource: the test feeds events and inspects connects. */
class FakeSource implements LogStreamSource<Cmd> {
    fromIndexes: number[] = [];
    closes = 0;
    handlers!: StreamHandlers<Cmd>;

    connect(fromIndex: number, handlers: StreamHandlers<Cmd>): () => void {
        this.fromIndexes.push(fromIndex);
        this.handlers = handlers;
        return () => {
            this.closes += 1;
        };
    }
}

const entry = (index: number, type = 'op'): { index: number; entry: LogEntry<Cmd> } => ({
    index,
    entry: { term: 1, command: { type } },
});

function makeReplica(source: FakeSource) {
    const app = new CountingSM();
    const replica = new EdgeReplica<Cmd, number>({
        app,
        source,
        // Tiny, tight bounds so reconnect fires within a few ms and deterministically.
        reconnectMinMs: 5,
        reconnectMaxMs: 5,
    });
    return { app, replica };
}

describe('EdgeReplica resilience over a fake stream source', () => {
    it('reconnects after an error and resumes from the applied cursor', async () => {
        const source = new FakeSource();
        const { replica } = makeReplica(source);
        replica.start();
        expect(source.fromIndexes).toEqual([0]);

        source.handlers.onOpen?.();
        source.handlers.onEntry(entry(1));
        source.handlers.onCaughtUp(1);
        expect(replica.lastIndex()).toBe(1);

        source.handlers.onError(new Error('socket dropped'));
        await waitFor(() => source.fromIndexes.length === 2);
        // Resumed from the cursor, and the previous connection was closed.
        expect(source.fromIndexes).toEqual([0, 1]);
        expect(source.closes).toBeGreaterThanOrEqual(1);

        replica.stop();
    });

    it('ignores replayed entries after a reconnect (no double-apply)', async () => {
        const source = new FakeSource();
        const { app, replica } = makeReplica(source);
        replica.start();

        source.handlers.onEntry(entry(1));
        source.handlers.onEntry(entry(2));
        expect(app.count).toBe(2);

        source.handlers.onError(new Error('drop'));
        await waitFor(() => source.fromIndexes.length === 2);

        // The server replays 1 and 2 (at/below the cursor) then sends 3.
        source.handlers.onEntry(entry(1));
        source.handlers.onEntry(entry(2));
        expect(app.count).toBe(2); // unchanged — replays ignored
        source.handlers.onEntry(entry(3));
        expect(app.count).toBe(3);
        expect(replica.lastIndex()).toBe(3);

        replica.stop();
    });

    it('advances the cursor on onCaughtUp even when entries were filtered out', async () => {
        const source = new FakeSource();
        const { app, replica } = makeReplica(source);
        replica.start();

        // A scoped stream skips out-of-scope entries but still reports it is current
        // through an absolute index.
        source.handlers.onCaughtUp(9);
        expect(replica.lastIndex()).toBe(9);
        expect(replica.isCaughtUp()).toBe(true);
        expect(app.count).toBe(0); // nothing applied, just the cursor advanced

        await expect(replica.waitForIndex(9)).resolves.toBeUndefined();
        replica.stop();
    });

    it('bootstraps from a snapshot, restoring state and the cursor', async () => {
        const source = new FakeSource();
        const { app, replica } = makeReplica(source);
        replica.start();

        source.handlers.onSnapshot({
            lastIncludedIndex: 10,
            lastIncludedTerm: 2,
            members: [],
            data: { state: { count: 7 } },
        });
        expect(app.count).toBe(7);
        expect(replica.lastIndex()).toBe(10);

        // A post-snapshot tail entry applies on top.
        source.handlers.onEntry(entry(11));
        expect(app.count).toBe(8);

        replica.stop();
    });

    it('stop() closes the connection and rejects pending read-your-writes barriers', async () => {
        const source = new FakeSource();
        const { replica } = makeReplica(source);
        replica.start();

        const pending = replica.waitForIndex(99);
        replica.stop();
        await expect(pending).rejects.toThrow(/stopped/);
        expect(source.closes).toBeGreaterThanOrEqual(1);
    });

    it('waitForIndex rejects on timeout when the index is never reached', async () => {
        const source = new FakeSource();
        const { replica } = makeReplica(source);
        replica.start();

        await expect(replica.waitForIndex(50, 20)).rejects.toThrow('WAIT_FOR_INDEX_TIMEOUT');
        replica.stop();
    });
});
