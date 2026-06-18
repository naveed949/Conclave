import express from 'express';
import { RaftNode } from '../consensus/raftNode';

/**
 * The replicated, hash-chained audit log — a built-in, tamper-evident history
 * of every committed state change, available on every node.
 */
export default function auditRoutes(node: RaftNode) {
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
