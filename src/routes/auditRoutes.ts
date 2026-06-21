import express from 'express';
import { Consensus } from '../consensus/consensus';
import { AppCommand } from '../consensus/types';

/**
 * The replicated, hash-chained audit log — a built-in, tamper-evident history
 * of every committed state change, available on every node. Application-agnostic.
 *
 * Depends only on the {@link Consensus} seam (`node.stateMachine`), not on Raft:
 * the audit trail is a property of the committed-ordered log, so it works
 * unchanged under any engine that implements `Consensus` (ADR-0021).
 */
export default function auditRoutes<C extends AppCommand, T, A>(
    node: Consensus<C, T, A>,
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
