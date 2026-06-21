import express from 'express';
import { RaftNode } from '../consensus/raftNode';
import { StateMachine } from '../consensus/stateMachine';
import { AppCommand } from '../consensus/types';

/**
 * The replicated, hash-chained audit log — a built-in, tamper-evident history
 * of every committed state change, available on every node. Application-agnostic.
 */
export default function auditRoutes<C extends AppCommand, T, SM extends StateMachine<C, T>>(
    node: RaftNode<C, T, SM>,
) {
    const router = express.Router();

    // Full audit trail (optionally filtered by ?actor= or ?type=).
    router.get('/', (req, res) => {
        const { actor, type } = req.query;
        let entries = node.stateMachine.getAuditLog();
        if (typeof actor === 'string') entries = entries.filter((e) => e.actor === actor);
        if (typeof type === 'string') entries = entries.filter((e) => e.type === type);
        res.json(entries);
    });

    // Verify the integrity of the hash chain on this node.
    router.get('/verify', (_req, res) => {
        res.json(node.stateMachine.verifyAudit());
    });

    return router;
}
