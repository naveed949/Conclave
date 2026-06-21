import { RaftNode } from '../src/consensus/raftNode';
import { LocalTransport, RpcHandler } from '../src/consensus/transport';
import { PeerInfo } from '../src/consensus/types';
import { BookNode, BookStateMachine } from '../src/models/bookStateMachine';
import { ModuleStateMachine } from '../src/runtime/moduleStateMachine';
import { ModuleAppCommand } from '../src/runtime/types';
import { AnyModuleDefinition } from '../src/runtime/moduleHost';
import { KeyRegistry } from '../src/runtime/signing';

// Fast timers keep the test suite snappy and deterministic.
const TEST_TIMERS = {
    electionMinMs: 50,
    electionMaxMs: 100,
    heartbeatMs: 20,
};

/** Build an in-process cluster of `size` book nodes wired with LocalTransport. */
export function buildCluster(size: number): BookNode[] {
    const registry = new Map<string, RpcHandler>();
    const transport = new LocalTransport(registry, 1);
    const ids = Array.from({ length: size }, (_, i) => `node${i + 1}`);

    const nodes = ids.map((id) => {
        const peers: PeerInfo[] = ids.filter((p) => p !== id).map((p) => ({ id: p, url: `local://${p}` }));
        return new RaftNode({ id, peers, stateMachine: new BookStateMachine(), ...TEST_TIMERS }, transport);
    });

    nodes.forEach((n) => registry.set(n.id, n));
    return nodes;
}

/** A node whose application is the {@link ModuleStateMachine} runtime (ADR-0019). */
export type ModuleNode = RaftNode<ModuleAppCommand, unknown, ModuleStateMachine>;

/**
 * Build an in-process cluster of `size` nodes running the module runtime. Each
 * node gets its OWN {@link ModuleStateMachine} (independent replica), registered
 * with the same `modules` and the same optional `keyRegistry`, so MODULE commands
 * apply identically and converge.
 */
export function buildModuleCluster(
    size: number,
    modules: AnyModuleDefinition[],
    opts: { keyRegistry?: KeyRegistry } = {},
): ModuleNode[] {
    const registry = new Map<string, RpcHandler>();
    const transport = new LocalTransport(registry, 1);
    const ids = Array.from({ length: size }, (_, i) => `node${i + 1}`);

    const nodes = ids.map((id) => {
        const peers: PeerInfo[] = ids.filter((p) => p !== id).map((p) => ({ id: p, url: `local://${p}` }));
        const sm = new ModuleStateMachine();
        sm.host.registerModules(modules);
        if (opts.keyRegistry) sm.host.setKeyRegistry(opts.keyRegistry);
        return new RaftNode({ id, peers, stateMachine: sm, ...TEST_TIMERS }, transport);
    });

    nodes.forEach((n) => registry.set(n.id, n));
    return nodes;
}

/** Poll until `predicate` is true or `timeoutMs` elapses. */
export async function waitFor(predicate: () => boolean, timeoutMs = 2000, stepMs = 10): Promise<void> {
    const start = Date.now();
    while (Date.now() - start < timeoutMs) {
        if (predicate()) return;
        await new Promise((r) => setTimeout(r, stepMs));
    }
    throw new Error('waitFor: condition not met within timeout');
}

/** The current leaders among `nodes` (generic over the node's command type). */
export function leaders<N extends { isLeader(): boolean }>(nodes: N[]): N[] {
    return nodes.filter((n) => n.isLeader());
}
