import { ModuleHost } from '../../src/runtime/moduleHost';
import { notes } from '../../src/runtime/modules/notes';
import { ProjectionHost } from '../../src/runtime/projectionHost';
import { ProjectionEvent } from '../../src/runtime/projection';
import { noteIndex } from '../../src/runtime/projections/noteIndex';
import { ModuleCommand, Seed } from '../../src/runtime/types';

/** A fixed seed makes ids/timestamps reproducible (mirrors moduleHost.test). */
const seed = (nonce: string, timestamp = '2026-06-21T00:00:00.000Z'): Seed => ({ timestamp, nonce });

const cmd = (module: string, command: string, input: unknown, s: Seed): ModuleCommand => ({
    module,
    command,
    input,
    seed: s,
});

/**
 * Build a flat stream of committed `notes.create` events from several actors,
 * deriving each event's note id deterministically from its seed (the same id the
 * `notes` reducer would mint). Building the events directly keeps these tests
 * focused on the read side, without coupling to `ModuleHost` internals — the
 * realistic wiring is exercised separately below in "derived from ModuleHost".
 */
function noteCreateEvent(seq: number, actor: string, text: string, noteId: string): ProjectionEvent {
    return {
        seq,
        module: 'notes',
        command: 'create',
        input: { text },
        result: { id: noteId, text, createdAt: '2026-06-21T00:00:00.000Z' },
        actor,
        requestId: `req-${seq}`,
    };
}

function freshHost(): ProjectionHost {
    const host = new ProjectionHost();
    host.register(noteIndex);
    return host;
}

/** A representative multi-actor stream reused across several tests. */
function sampleStream(): ProjectionEvent[] {
    return [
        noteCreateEvent(0, 'alice', 'a1', 'id-a1'),
        noteCreateEvent(1, 'bob', 'b1', 'id-b1'),
        noteCreateEvent(2, 'alice', 'a2', 'id-a2'),
        noteCreateEvent(3, 'carol', 'c1', 'id-c1'),
        noteCreateEvent(4, 'alice', 'a3', 'id-a3'),
        noteCreateEvent(5, 'bob', 'b2', 'id-b2'),
    ];
}

describe('Projection: noteIndex rich queries', () => {
    it('answers byActor / total / actors that the flat module state does not index', () => {
        const host = freshHost();
        for (const e of sampleStream()) {
            host.applyEvent(e);
        }

        // Indexed O(1) lookup the raw `notes` array would need an O(n) scan for.
        expect(host.query('noteIndex', 'byActor', 'alice')).toEqual(['id-a1', 'id-a2', 'id-a3']);
        expect(host.query('noteIndex', 'byActor', 'bob')).toEqual(['id-b1', 'id-b2']);
        expect(host.query('noteIndex', 'byActor', 'carol')).toEqual(['id-c1']);

        // Unknown actor -> empty, never undefined.
        expect(host.query('noteIndex', 'byActor', 'nobody')).toEqual([]);

        expect(host.query('noteIndex', 'total')).toBe(6);
        expect(host.query('noteIndex', 'actors')).toEqual(['alice', 'bob', 'carol']);
    });

    it('ignores events for other modules/commands inside the fold', () => {
        const host = freshHost();
        host.applyEvent(noteCreateEvent(0, 'alice', 'a1', 'id-a1'));
        // A non-notes event and a non-create notes event must not touch the index.
        host.applyEvent({
            seq: 1,
            module: 'counter',
            command: 'increment',
            input: { by: 1 },
            result: undefined,
            actor: 'alice',
            requestId: 'req-1',
        });
        host.applyEvent({
            seq: 2,
            module: 'notes',
            command: 'delete',
            input: { id: 'id-a1' },
            result: undefined,
            actor: 'alice',
            requestId: 'req-2',
        });

        expect(host.query('noteIndex', 'total')).toBe(1);
        expect(host.query('noteIndex', 'actors')).toEqual(['alice']);
    });

    it('rejects an unknown projection or query', () => {
        const host = freshHost();
        expect(() => host.query('ghost', 'total')).toThrow(/Unknown projection/);
        expect(() => host.query('noteIndex', 'nope')).toThrow(/Unknown query/);
    });
});

describe('Projection: rebuildable (derived, not authoritative)', () => {
    it('rebuild from the same stream reconstructs the identical incremental view', () => {
        const events = sampleStream();

        // Incremental host: folded event-by-event.
        const incremental = freshHost();
        for (const e of events) {
            incremental.applyEvent(e);
        }
        const incrementalSnap = incremental.snapshot();

        // Fresh host: drop everything and rebuild purely from the log stream.
        const rebuilt = freshHost();
        rebuilt.rebuild(events);

        // The cache (incremental) and the from-scratch replay agree => the read
        // model is DERIVED, reconstructible from the committed stream alone.
        expect(rebuilt.snapshot()).toEqual(incrementalSnap);
    });

    it('rebuild resets prior state so the result depends only on the supplied events', () => {
        const host = freshHost();
        // Pollute the cache with one stream...
        host.applyEvent(noteCreateEvent(0, 'mallory', 'x', 'id-x'));
        // ...then rebuild from a DIFFERENT stream; the old event must vanish.
        const events = [noteCreateEvent(0, 'alice', 'a1', 'id-a1')];
        host.rebuild(events);

        expect(host.query('noteIndex', 'actors')).toEqual(['alice']);
        expect(host.query('noteIndex', 'total')).toBe(1);
        expect(host.query('noteIndex', 'byActor', 'mallory')).toEqual([]);
    });
});

describe('Projection: convergence across nodes', () => {
    it('two independent hosts fed the same stream produce deep-equal snapshots', () => {
        const events = sampleStream();

        const node1 = freshHost();
        const node2 = freshHost();
        for (const e of events) {
            node1.applyEvent(e);
            node2.applyEvent(e);
        }

        // Pure deterministic fold => identical read model on every replica.
        expect(node1.snapshot()).toEqual(node2.snapshot());
    });

    it('deterministic replay: same events in the same committed order => identical view', () => {
        const events = sampleStream();
        const a = freshHost();
        const b = freshHost();
        a.rebuild(events);
        b.rebuild(events);
        expect(a.snapshot()).toEqual(b.snapshot());
    });
});

describe('Projection: snapshot / restore', () => {
    it('round-trips the view into a fresh host', () => {
        const host = freshHost();
        for (const e of sampleStream()) {
            host.applyEvent(e);
        }
        const snap = host.snapshot();

        const restored = freshHost();
        restored.restore(snap);

        expect(restored.snapshot()).toEqual(snap);
        expect(restored.query('noteIndex', 'byActor', 'alice')).toEqual(['id-a1', 'id-a2', 'id-a3']);
        expect(restored.query('noteIndex', 'total')).toBe(6);
    });

    it('snapshot is decoupled from live state (deep clone)', () => {
        const host = freshHost();
        host.applyEvent(noteCreateEvent(0, 'alice', 'a1', 'id-a1'));
        const snap = host.snapshot();
        host.applyEvent(noteCreateEvent(1, 'alice', 'a2', 'id-a2')); // mutate after snapshot

        const snapView = snap.noteIndex as { total: number };
        expect(snapView.total).toBe(1); // snapshot unaffected by later folds
        expect(host.query('noteIndex', 'total')).toBe(2);
    });
});

describe('Projection: realistic wiring derived from a ModuleHost apply result', () => {
    it('builds ProjectionEvents from committed module commands and indexes them', () => {
        // The authoritative write side: a real ModuleHost applying notes.create.
        const moduleHost = new ModuleHost();
        moduleHost.register(notes);

        const projections = freshHost();

        const commands: Array<{ actor: string; c: ModuleCommand }> = [
            { actor: 'alice', c: cmd('notes', 'create', { text: 'hello' }, seed('n1')) },
            { actor: 'bob', c: cmd('notes', 'create', { text: 'world' }, seed('n2')) },
            { actor: 'alice', c: cmd('notes', 'create', { text: 'again' }, seed('n3')) },
        ];

        commands.forEach(({ actor, c }, seq) => {
            const meta = { actor, requestId: `req-${seq}` };
            const res = moduleHost.apply(c, meta);
            expect(res.status).toBe(200);
            // Derive the read-side event from the authoritative apply outcome.
            projections.applyEvent({
                seq,
                module: c.module,
                command: c.command,
                input: c.input,
                result: res.result,
                actor: meta.actor,
                requestId: meta.requestId,
            });
        });

        // The projection indexes the real, leader-minted note ids by actor.
        const aliceNotes = moduleHost.query('notes', 'list') as Array<{ id: string }>;
        const aliceIds = aliceNotes.filter((_n, i) => i === 0 || i === 2).map((n) => n.id);
        expect(projections.query('noteIndex', 'byActor', 'alice')).toEqual(aliceIds);
        expect(projections.query('noteIndex', 'total')).toBe(3);
        expect(projections.query('noteIndex', 'actors')).toEqual(['alice', 'bob']);
    });
});
