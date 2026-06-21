import request from 'supertest';
import { Application } from 'express';
import { createModuleApp } from '../src/moduleApp';
import { RaftNode } from '../src/consensus/raftNode';
import { LocalTransport, RpcHandler } from '../src/consensus/transport';
import { ModuleStateMachine, ModuleNode } from '../src/runtime/moduleStateMachine';
import { counter } from '../src/runtime/modules/counter';
import { notes } from '../src/runtime/modules/notes';
import { accounts } from '../src/runtime/modules/accounts';
import { generateActorKeypair, signCommand } from '../src/runtime/signing';
import { waitFor } from './helpers';

const TEST_TIMERS = { electionMinMs: 50, electionMaxMs: 100, heartbeatMs: 20 };

/**
 * Build a 1-node module-runtime cluster with the demo modules registered. The
 * node elects itself leader, so writes commit immediately. `setup` runs against
 * the fresh `ModuleStateMachine.host` before `start()` (e.g. to register actor
 * keys), so signature verification is configured identically on every replica.
 */
function buildSingleNode(setup?: (sm: ModuleStateMachine) => void): ModuleNode {
    const registry = new Map<string, RpcHandler>();
    const transport = new LocalTransport(registry, 1);
    const sm = new ModuleStateMachine();
    sm.host.register(counter);
    sm.host.register(notes);
    sm.host.register(accounts);
    setup?.(sm);
    const node: ModuleNode = new RaftNode({ id: 'node1', peers: [], stateMachine: sm, ...TEST_TIMERS }, transport);
    registry.set(node.id, node);
    return node;
}

describe('Module API (single-node cluster)', () => {
    describe('unsigned commands and queries', () => {
        let node: ModuleNode;
        let app: Application;

        beforeAll(async () => {
            node = buildSingleNode();
            node.start();
            app = createModuleApp(node);
            await waitFor(() => node.isLeader());
        });

        afterAll(() => {
            node.stop();
        });

        it('increments the counter and reads the value back via a query', async () => {
            const res = await request(app).post('/modules/counter/increment').send({ by: 5 });
            expect(res.status).toBe(200);

            const value = await request(app).get('/modules/counter/query/value');
            expect(value.status).toBe(200);
            expect(value.body).toBe(5);
        });

        it('creates a note (id/createdAt minted from ctx) and lists it', async () => {
            const created = await request(app).post('/modules/notes/create').send({ text: 'hi' });
            expect(created.status).toBe(200);
            expect(created.body.id).toBeDefined();
            expect(created.body.createdAt).toBeDefined();
            expect(created.body.text).toBe('hi');

            const list = await request(app).get('/modules/notes/query/list');
            expect(list.status).toBe(200);
            expect(list.body).toEqual([created.body]);
        });

        it('exposes raw module state', async () => {
            const state = await request(app).get('/modules/counter/state');
            expect(state.status).toBe(200);
            expect(state.body).toEqual({ value: 5 });
        });

        it('serves a linearizable (strong) query through the read barrier', async () => {
            const value = await request(app).get('/modules/counter/query/value?consistency=strong');
            expect(value.status).toBe(200);
            expect(value.body).toBe(5);
        });

        it('returns 404 for an unknown module without crashing', async () => {
            const res = await request(app).post('/modules/ghost/poke').send({});
            expect(res.status).toBe(404);
        });

        it('returns 404 for an unknown query', async () => {
            const res = await request(app).get('/modules/counter/query/nope');
            expect(res.status).toBe(404);
        });
    });

    describe('signed commands (ADR-0019 pillar 7) end-to-end over HTTP', () => {
        let node: ModuleNode;
        let app: Application;
        const actor = 'alice';
        const keys = generateActorKeypair();

        beforeAll(async () => {
            // Authorize alice's PUBLIC key on the host before start; the registry is
            // now configured, so every command must carry a valid signature.
            node = buildSingleNode((sm) => sm.host.registerActorKey(actor, keys.publicKey));
            node.start();
            app = createModuleApp(node);
            await waitFor(() => node.isLeader());
        });

        afterAll(() => {
            node.stop();
        });

        it('accepts a command signed by the actor and relayed via x-signature', async () => {
            const requestId = 'req-signed-1';
            const input = { by: 3 };
            // The CLIENT signs the logical payload with its PRIVATE key (the server
            // never holds it); the seed is excluded (the leader adds it later).
            const sig = signCommand(keys.privateKey, {
                module: 'counter',
                command: 'increment',
                input,
                actor,
                requestId,
            });

            const res = await request(app)
                .post('/modules/counter/increment')
                .set('x-actor', actor)
                .set('x-request-id', requestId)
                .set('x-signature', sig)
                .send(input);
            expect(res.status).toBe(200);

            const value = await request(app).get('/modules/counter/query/value');
            expect(value.body).toBe(3);
        });

        it('rejects a command signed by the wrong key with 401', async () => {
            const requestId = 'req-forged-1';
            const input = { by: 99 };
            // A DIFFERENT keypair (not the one registered for alice) forges the sig.
            const forged = generateActorKeypair();
            const sig = signCommand(forged.privateKey, {
                module: 'counter',
                command: 'increment',
                input,
                actor,
                requestId,
            });

            const res = await request(app)
                .post('/modules/counter/increment')
                .set('x-actor', actor)
                .set('x-request-id', requestId)
                .set('x-signature', sig)
                .send(input);
            expect(res.status).toBe(401);

            // State unchanged — the forged command never reached its reducer.
            const value = await request(app).get('/modules/counter/query/value');
            expect(value.body).toBe(3);
        });

        it('rejects a command with no signature when a registry is configured', async () => {
            const res = await request(app)
                .post('/modules/counter/increment')
                .set('x-actor', actor)
                .send({ by: 1 });
            expect(res.status).toBe(401);
        });
    });
});
