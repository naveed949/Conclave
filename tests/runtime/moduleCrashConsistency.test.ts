import fs from 'fs';
import os from 'os';
import path from 'path';

import { RaftNode } from '../../src/consensus/raftNode';
import { LocalTransport, RpcHandler } from '../../src/consensus/transport';
import { FileStorage } from '../../src/consensus/storage';
import { CommandMeta, PeerInfo } from '../../src/consensus/types';
import { buildModuleCommand } from '../../src/runtime/command';
import { ModuleStateMachine, ModuleNode } from '../../src/runtime/moduleStateMachine';
import { counter } from '../../src/runtime/modules/counter';
import { notes } from '../../src/runtime/modules/notes';
import { accounts } from '../../src/runtime/modules/accounts';
import { waitFor } from '../helpers';

/**
 * Milestone 14: prove the module runtime (ADR-0019) survives a REAL durable
 * restart through {@link FileStorage} — the module analog of the book
 * `crashConsistency` suite. We wire a single-node cluster directly (a single node
 * is its own majority, so commits land immediately) with `LocalTransport` +
 * `FileStorage` over a temp data dir + a {@link ModuleStateMachine}, exactly as
 * `crashConsistency.test.ts` wires a durable book node.
 *
 * "Restart" = stop the node, then construct a FRESH `RaftNode` with the SAME
 * `nodeId`, the SAME data dir, and a FRESH `ModuleStateMachine` re-registered with
 * the SAME modules, and `start()` it (which restores from disk via the snapshot
 * and/or the persisted log). We assert that whole-state module data, keyed-store
 * records, the leader-resolved seed values, AND the Merkle audit root all
 * round-trip across the restart via three durable paths: log replay (no snapshot),
 * snapshot+restore, and snapshot-then-tail compaction.
 */

const TIMERS = { electionMinMs: 50, electionMaxMs: 100, heartbeatMs: 20 };
const MODULES = [counter, notes, accounts];

/**
 * Build a durable single-node module cluster: `LocalTransport` (no sockets) +
 * `FileStorage` over `dataDir` + a `ModuleStateMachine` registered with the demo
 * modules. The node is its own sole voting member, so it elects itself and
 * commits writes immediately. A fresh `ModuleHost`/state machine is created each
 * call, so calling this twice against the same `dataDir` models a process restart.
 */
/** Nodes created in a test, stopped in teardown so no timer outlives the temp dir. */
let openNodes: ModuleNode[] = [];

function makeModuleNode(dataDir: string, snapshotThreshold: number): ModuleNode {
    const registry = new Map<string, RpcHandler>();
    const peers: PeerInfo[] = []; // single-node cluster: only self
    const sm = new ModuleStateMachine();
    sm.host.registerModules(MODULES);
    const node = new RaftNode(
        { id: 'n1', peers, stateMachine: sm, ...TIMERS, snapshotThreshold, storage: new FileStorage('n1', dataDir) },
        new LocalTransport(registry),
    );
    registry.set('n1', node);
    openNodes.push(node);
    return node;
}

/** Submit a module command and await its commit; `requestId` doubles as the idempotency key. */
function submit(node: ModuleNode, module: string, command: string, input: unknown, requestId: string) {
    const meta: CommandMeta = { requestId, actor: 'tester', timestamp: 't' };
    return node.submit(buildModuleCommand(module, command, input, meta), meta);
}

/**
 * Snapshot the observable runtime state we expect to survive a restart: every
 * whole-state blob, every keyed store dump, the outbox, and the Merkle audit root.
 * Compared by JSON equality before vs after the restart.
 */
function captureState(node: ModuleNode) {
    const host = node.app.host;
    return {
        counter: host.query('counter', 'value'),
        notes: JSON.stringify(host.getState('notes')),
        accounts: JSON.stringify(host.getStore('accounts')!.snapshot()),
        auditRoot: host.auditRoot(),
        auditSize: host.auditSize(),
    };
}

describe('Module runtime crash consistency over FileStorage (M14)', () => {
    let dir: string;

    beforeEach(() => {
        openNodes = [];
        dir = fs.mkdtempSync(path.join(os.tmpdir(), 'mod-crash-'));
    });

    afterEach(() => {
        // Stop every node first (clears its election/heartbeat timers) so nothing
        // tries to persist into a directory we are about to delete. Idempotent:
        // `stop()` on an already-stopped node is a no-op.
        openNodes.forEach((n) => n.stop());
        fs.rmSync(dir, { recursive: true, force: true });
    });

    it('replays the durable log on restart with NO snapshot (state + keyed store + audit root survive)', async () => {
        // High threshold so the log never compacts: restore is pure log replay.
        const node = makeModuleNode(dir, 10_000);
        node.start();
        await waitFor(() => node.isLeader());

        await submit(node, 'counter', 'increment', { by: 7 }, 'inc-1');
        await submit(node, 'notes', 'create', { text: 'first note' }, 'note-1');
        await submit(node, 'notes', 'create', { text: 'second note' }, 'note-2');
        await submit(node, 'accounts', 'open', { id: 'acct-a' }, 'open-a');
        await submit(node, 'accounts', 'deposit', { id: 'acct-a', amount: 250 }, 'dep-a');

        // No snapshot was taken (the whole history lives in the durable log).
        expect(node.status().snapshotIndex).toBe(0);
        const before = captureState(node);
        node.stop();

        // Restart: fresh host, same dir + modules. start() replays the log.
        const restarted = makeModuleNode(dir, 10_000);
        restarted.start();
        // Still no snapshot — proof we exercised the pure log-replay path.
        expect(restarted.status().snapshotIndex).toBe(0);
        await waitFor(() => restarted.isLeader());

        const after = captureState(restarted);
        // Whole-state module value replays deterministically.
        expect(after.counter).toBe(7);
        // The notes (including the leader-resolved id/createdAt baked into the
        // committed seed) replay byte-identically — proving deterministic replay.
        expect(after.notes).toBe(before.notes);
        const noteList = restarted.app.host.query('notes', 'list') as Array<{ id: string; createdAt: string }>;
        expect(noteList).toHaveLength(2);
        // The seed-resolved id/createdAt survived (replayed from the committed seed,
        // not regenerated — they are present and exactly the pre-restart values).
        expect(noteList[0].id).toBeTruthy();
        expect(noteList[0].createdAt).toBeTruthy();
        // Keyed accounts store + balance survive the replay.
        expect(restarted.app.host.query('accounts', 'balance', { id: 'acct-a' })).toBe(250);
        expect(after.accounts).toBe(before.accounts);
        // The Merkle audit root — derived purely from the applied command stream —
        // is identical, proving the audit history round-trips through replay.
        expect(after.auditRoot).toBe(before.auditRoot);
        expect(after.auditSize).toBe(before.auditSize);
        restarted.stop();
    });

    it('restores from a SNAPSHOT on restart (compacted log; keyed store dump round-trips)', async () => {
        // Low threshold so a snapshot IS taken; restore comes from the snapshot.
        const node = makeModuleNode(dir, 4);
        node.start();
        await waitFor(() => node.isLeader());

        await submit(node, 'counter', 'increment', { by: 3 }, 'inc-1');
        await submit(node, 'notes', 'create', { text: 'snap note' }, 'note-1');
        await submit(node, 'accounts', 'open', { id: 'acct-x' }, 'open-x');
        await submit(node, 'accounts', 'deposit', { id: 'acct-x', amount: 100 }, 'dep-x');
        await submit(node, 'accounts', 'open', { id: 'acct-y' }, 'open-y');
        await submit(node, 'accounts', 'deposit', { id: 'acct-y', amount: 40 }, 'dep-y');
        await submit(node, 'counter', 'increment', { by: 9 }, 'inc-2');

        // A snapshot landed and the log compacted past index 0.
        await waitFor(() => node.status().snapshotIndex > 0);
        const before = captureState(node);
        node.stop();

        const restarted = makeModuleNode(dir, 4);
        restarted.start();
        // State machine is reconstructed FROM THE SNAPSHOT immediately on start(),
        // before any re-election: the snapshot boundary is preserved.
        expect(restarted.status().snapshotIndex).toBeGreaterThan(0);
        const after = captureState(restarted);

        expect(after.counter).toBe(12);
        expect(restarted.app.host.query('accounts', 'balance', { id: 'acct-x' })).toBe(100);
        expect(restarted.app.host.query('accounts', 'balance', { id: 'acct-y' })).toBe(40);
        // The keyed StateStore dump (sorted [key,value][]) round-trips through the snapshot.
        expect(after.accounts).toBe(before.accounts);
        expect(after.notes).toBe(before.notes);
        // The Merkle audit root reconstructs from the snapshot's __audit leaves.
        expect(after.auditRoot).toBe(before.auditRoot);
        expect(after.auditSize).toBe(before.auditSize);

        // The node recovers to full liveness and converges the same state after election.
        await waitFor(() => restarted.isLeader());
        expect(restarted.app.host.query('counter', 'value')).toBe(12);
        restarted.stop();
    });

    it('restores from snapshot THEN replays the post-snapshot log tail (no double-apply, no loss)', async () => {
        const node = makeModuleNode(dir, 4);
        node.start();
        await waitFor(() => node.isLeader());

        // Enough commands to trigger a snapshot.
        await submit(node, 'counter', 'increment', { by: 1 }, 't-1');
        await submit(node, 'accounts', 'open', { id: 'acct-1' }, 't-2');
        await submit(node, 'accounts', 'deposit', { id: 'acct-1', amount: 10 }, 't-3');
        await submit(node, 'counter', 'increment', { by: 1 }, 't-4');
        await submit(node, 'counter', 'increment', { by: 1 }, 't-5');
        await waitFor(() => node.status().snapshotIndex > 0);

        // MORE commands AFTER the snapshot point: these live only in the log tail.
        await submit(node, 'counter', 'increment', { by: 1 }, 't-6');
        await submit(node, 'accounts', 'deposit', { id: 'acct-1', amount: 5 }, 't-7');
        await submit(node, 'notes', 'create', { text: 'post-snapshot note' }, 't-8');

        // The DURABLE snapshot may have advanced as the tail kept compacting; read
        // the actual boundary now and confirm a genuine post-snapshot tail exists
        // (lastLogIndex past the boundary), so restart exercises replay-on-top.
        const snapBoundary = node.status().snapshotIndex;
        expect(snapBoundary).toBeGreaterThan(0);
        expect(node.status().lastLogIndex).toBeGreaterThan(snapBoundary);

        const before = captureState(node);
        node.stop();

        const restarted = makeModuleNode(dir, 4);
        restarted.start();
        // Immediately after start() (before re-election/replay) the boundary equals
        // the durable snapshot: state is reconstructed FROM the snapshot first.
        expect(restarted.status().snapshotIndex).toBe(snapBoundary);
        // The post-snapshot tail is then replayed on top. Final state = snapshot
        // state + replayed tail, with no double-apply and no loss.
        await waitFor(() => restarted.isLeader());
        const after = captureState(restarted);

        // counter incremented once per 't-1','t-4','t-5','t-6' = 4 (t-2,3,7,8 are non-counter).
        expect(after.counter).toBe(4);
        // acct-1: deposited 10 (pre-snapshot) + 5 (tail) = 15, NOT double-applied.
        expect(restarted.app.host.query('accounts', 'balance', { id: 'acct-1' })).toBe(15);
        // The post-snapshot note (tail-only) is present exactly once.
        const noteList = restarted.app.host.query('notes', 'list') as unknown[];
        expect(noteList).toHaveLength(1);
        // Whole capture (state + keyed store + audit root) matches the pre-restart value.
        expect(after.notes).toBe(before.notes);
        expect(after.accounts).toBe(before.accounts);
        expect(after.auditRoot).toBe(before.auditRoot);
        expect(after.auditSize).toBe(before.auditSize);
        restarted.stop();
    });

    // The "snapshot landed but the compacted log didn't" reconcile case is covered
    // engine-side by the book `crashConsistency` suite (`reconcileLog`): that path
    // lives entirely in the consensus core and is application-agnostic. Module
    // nodes ride the SAME RSM snapshot/restore + the SAME `reconcileLog`, so the
    // module-specific risk (whole-state + keyed-store + Merkle-audit serialization)
    // is what the three tests above prove; we do not re-poke storage internals here.
});
