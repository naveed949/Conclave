import { RaftNode } from '../../src/consensus/raftNode';
import { CommandMeta } from '../../src/consensus/types';
import { buildModuleCommand } from '../../src/runtime/command';
import { counter } from '../../src/runtime/modules/counter';
import { notes } from '../../src/runtime/modules/notes';
import { buildCluster, leaders, waitFor } from '../helpers';

/**
 * M4: a generic MODULE command flows through REAL Raft. The leader resolves the
 * seed up front (via `buildModuleCommand`), the command replicates as a log
 * entry, and every node applies it to its embedded ModuleHost — converging on
 * identical module state AND identical Merkle audit roots, with idempotency and
 * leader-resolved seeds carried through the log.
 */
describe('Module commands over Raft consensus', () => {
    let nodes: RaftNode[];

    beforeEach(() => {
        nodes = buildCluster(3);
        // Register the SAME modules on every node BEFORE start, so each node's
        // embedded ModuleHost can apply MODULE entries identically.
        nodes.forEach((n) => n.stateMachine.registerModules([counter, notes]));
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
        const incResult = await leader.submit(buildModuleCommand('counter', 'increment', { by: 5 }), incMeta);
        expect(incResult.status).toBe(200);

        const noteMeta: CommandMeta = {
            requestId: 'req-note-1',
            actor: 'bob',
            timestamp: new Date().toISOString(),
        };
        const noteResult = await leader.submit(buildModuleCommand('notes', 'create', { text: 'hello' }), noteMeta);
        expect(noteResult.status).toBe(200);

        // Wait for replication of BOTH commands: gate on the LAST submitted entry
        // (the note) as well as the counter, so we never read mid-replication while
        // the later entry is still in flight on a follower.
        await waitFor(() =>
            nodes.every(
                (n) =>
                    n.stateMachine.moduleQuery('counter', 'value') === 5 &&
                    (n.stateMachine.moduleQuery('notes', 'list') as unknown[]).length === 1,
            ),
        );

        // All three nodes converge on identical module state AND audit root.
        const counterValues = new Set(nodes.map((n) => n.stateMachine.moduleQuery('counter', 'value')));
        expect(counterValues).toEqual(new Set([5]));

        const noteLists = nodes.map((n) => JSON.stringify(n.stateMachine.moduleState('notes')));
        expect(new Set(noteLists).size).toBe(1);

        const auditRoots = new Set(nodes.map((n) => n.stateMachine.moduleAuditRoot()));
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

        await leader.submit(buildModuleCommand('counter', 'increment', { by: 3 }), meta);
        await waitFor(() => nodes.every((n) => n.stateMachine.moduleQuery('counter', 'value') === 3));

        // Re-submit with the SAME requestId: the dedup cache returns the cached
        // result without re-running the reducer, so the counter must NOT advance.
        await leader.submit(buildModuleCommand('counter', 'increment', { by: 3 }), meta);

        // Give the second command a chance to commit/apply, then assert no change.
        await waitFor(() => leader.status().lastApplied >= 0); // settle
        await new Promise((r) => setTimeout(r, 50));
        const values = new Set(nodes.map((n) => n.stateMachine.moduleQuery('counter', 'value')));
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
        await leader.submit(buildModuleCommand('notes', 'create', { text: 'seeded' }), meta);

        await waitFor(() =>
            nodes.every((n) => {
                const list = n.stateMachine.moduleQuery('notes', 'list') as Array<{ id: string }>;
                return list.length === 1;
            }),
        );

        // The note's id and createdAt come from ctx, derived from the seed baked
        // into the log on the leader. If each node resolved its own seed, these
        // would differ — identical values prove the seed flowed through the log.
        const noteJsons = nodes.map((n) => JSON.stringify(n.stateMachine.moduleQuery('notes', 'list')));
        expect(new Set(noteJsons).size).toBe(1);

        const firstList = nodes[0].stateMachine.moduleQuery('notes', 'list') as Array<{ id: string; createdAt: string }>;
        expect(firstList[0].id).toBeTruthy();
        expect(firstList[0].createdAt).toBeTruthy();
    });
});
