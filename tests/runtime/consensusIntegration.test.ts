import { CommandMeta } from '../../src/consensus/types';
import { buildModuleCommand } from '../../src/runtime/command';
import { counter } from '../../src/runtime/modules/counter';
import { notes } from '../../src/runtime/modules/notes';
import { buildModuleCluster, leaders, ModuleNode, waitFor } from '../helpers';

/**
 * M4: the module runtime rides REAL Raft as a pluggable application state machine
 * (ADR-0017 seam). The leader resolves the seed up front (via `buildModuleCommand`),
 * the command replicates as an ordinary application log entry, and every node
 * applies it to its own `ModuleStateMachine`/`ModuleHost` — converging on identical
 * module state AND identical Merkle audit roots, with idempotency (substrate dedup)
 * and leader-resolved seeds carried through the log.
 */
describe('Module commands over Raft consensus', () => {
    let nodes: ModuleNode[];

    beforeEach(() => {
        // Each node gets its own ModuleStateMachine registered with the same modules.
        nodes = buildModuleCluster(3, [counter, notes]);
        nodes.forEach((n) => n.start());
    });

    afterEach(() => {
        nodes.forEach((n) => n.stop());
    });

    it('replicates module commands and converges state + audit root on every node', async () => {
        await waitFor(() => leaders(nodes).length === 1);
        const leader = leaders(nodes)[0];

        const incMeta: CommandMeta = {
            requestId: 'req-inc-1',
            actor: 'alice',
            timestamp: new Date().toISOString(),
        };
        const incResult = await leader.submit(buildModuleCommand('counter', 'increment', { by: 5 }, incMeta), incMeta);
        expect(incResult.status).toBe(200);

        const noteMeta: CommandMeta = {
            requestId: 'req-note-1',
            actor: 'bob',
            timestamp: new Date().toISOString(),
        };
        const noteResult = await leader.submit(buildModuleCommand('notes', 'create', { text: 'hello' }, noteMeta), noteMeta);
        expect(noteResult.status).toBe(200);

        // Wait for replication of BOTH commands: gate on the LAST submitted entry
        // (the note) as well as the counter, so we never read mid-replication while
        // the later entry is still in flight on a follower.
        await waitFor(() =>
            nodes.every(
                (n) =>
                    n.app.host.query('counter', 'value') === 5 &&
                    (n.app.host.query('notes', 'list') as unknown[]).length === 1,
            ),
        );

        // All three nodes converge on identical module state AND audit root.
        const counterValues = new Set(nodes.map((n) => n.app.host.query('counter', 'value')));
        expect(counterValues).toEqual(new Set([5]));

        const noteLists = nodes.map((n) => JSON.stringify(n.app.host.getState('notes')));
        expect(new Set(noteLists).size).toBe(1);

        const auditRoots = new Set(nodes.map((n) => n.app.host.auditRoot()));
        expect(auditRoots.size).toBe(1);
    });

    it('does not double-apply a module command whose requestId is replayed (idempotency)', async () => {
        await waitFor(() => leaders(nodes).length === 1);
        const leader = leaders(nodes)[0];

        const meta: CommandMeta = {
            requestId: 'req-dup',
            actor: 'alice',
            timestamp: new Date().toISOString(),
        };

        await leader.submit(buildModuleCommand('counter', 'increment', { by: 3 }, meta), meta);
        await waitFor(() => nodes.every((n) => n.app.host.query('counter', 'value') === 3));

        // Re-submit with the SAME requestId: the substrate dedup cache returns the
        // cached result without re-running the reducer, so the counter must NOT advance.
        await leader.submit(buildModuleCommand('counter', 'increment', { by: 3 }, meta), meta);

        // Give the second command a chance to commit/apply, then assert no change.
        await waitFor(() => leader.status().lastApplied >= 0); // settle
        await new Promise((r) => setTimeout(r, 50));
        const values = new Set(nodes.map((n) => n.app.host.query('counter', 'value')));
        expect(values).toEqual(new Set([3]));
    });

    it('carries the leader-resolved seed through the log (same id/timestamp on every node)', async () => {
        await waitFor(() => leaders(nodes).length === 1);
        const leader = leaders(nodes)[0];

        const meta: CommandMeta = {
            requestId: 'req-seed',
            actor: 'carol',
            timestamp: new Date().toISOString(),
        };
        await leader.submit(buildModuleCommand('notes', 'create', { text: 'seeded' }, meta), meta);

        await waitFor(() =>
            nodes.every((n) => {
                const list = n.app.host.query('notes', 'list') as Array<{ id: string }>;
                return list.length === 1;
            }),
        );

        // The note's id and createdAt come from ctx, derived from the seed baked
        // into the log on the leader. If each node resolved its own seed, these
        // would differ — identical values prove the seed flowed through the log.
        const noteJsons = nodes.map((n) => JSON.stringify(n.app.host.query('notes', 'list')));
        expect(new Set(noteJsons).size).toBe(1);

        const firstList = nodes[0].app.host.query('notes', 'list') as Array<{ id: string; createdAt: string }>;
        expect(firstList[0].id).toBeTruthy();
        expect(firstList[0].createdAt).toBeTruthy();
    });
});
