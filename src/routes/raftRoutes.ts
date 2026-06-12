import express from 'express';
import { RaftNode } from '../consensus/raftNode';

/** Internal cluster endpoints: peer RPCs plus a human-friendly status view. */
export default function raftRoutes(node: RaftNode) {
    const router = express.Router();

    router.post('/request-vote', (req, res) => {
        res.json(node.handleRequestVote(req.body));
    });

    router.post('/append-entries', (req, res) => {
        res.json(node.handleAppendEntries(req.body));
    });

    router.get('/status', (_req, res) => {
        res.json(node.status());
    });

    return router;
}
