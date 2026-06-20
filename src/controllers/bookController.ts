import { Request, Response } from 'express';
import { RaftNode, NotLeaderError } from '../consensus/raftNode';
import { Command, CommandMeta } from '../consensus/types';
import { getContext } from '../platform/requestContext';
import { forwardToLeader, isForwarded } from '../platform/forward';
import {
    buildAddCommand,
    buildBorrowCommand,
    buildDeleteCommand,
    buildReturnCommand,
    buildUpdateCommand,
} from '../models/book';

/** A linearizable read is requested via `?consistency=strong` or `X-Consistency: strong`. */
function wantsStrongRead(req: Request): boolean {
    return req.query.consistency === 'strong' || req.header('x-consistency') === 'strong';
}

/**
 * Thin adapter between HTTP and the consensus layer. Reads are served from the
 * local replicated state machine (eventually consistent by default); a client
 * can opt into a **linearizable** read with `?consistency=strong`, which goes
 * through the leader's ReadIndex barrier. Writes are proposed to the Raft log
 * via the leader; a follower transparently forwards writes (and strong reads)
 * to the leader (falling back to 421 if the leader is unknown/unreachable).
 */
export function createBookController(node: RaftNode) {
    // Forward to the leader (or reply 421) when this node isn't the leader.
    const onNotLeader = async (req: Request, res: Response, err: NotLeaderError): Promise<void> => {
        const leaderUrl = node.getLeaderUrl();
        if (leaderUrl && !isForwarded(req)) {
            const ok = await forwardToLeader(req, res, leaderUrl);
            if (ok) return;
        }
        res.status(421).json({ message: 'Not the leader — retry against the leader', leader: err.leaderId });
    };

    const propose = async (req: Request, res: Response, build: () => Command) => {
        const ctx = getContext();
        const meta: CommandMeta | undefined = ctx
            ? { requestId: ctx.requestId, actor: ctx.actor, timestamp: new Date().toISOString() }
            : undefined;
        try {
            const result = await node.submit(build(), meta);
            if (result.book) res.status(result.status).json(result.book);
            else res.status(result.status).json({ message: result.message });
        } catch (err) {
            if (err instanceof NotLeaderError) return onNotLeader(req, res, err);
            console.error(err);
            res.status(500).json({ message: 'Server error' });
        }
    };

    // Run `serve` only after the leader's linearizable read barrier resolves.
    const strongRead = async (req: Request, res: Response, serve: () => void): Promise<void> => {
        try {
            await node.readBarrier();
            serve();
        } catch (err) {
            if (err instanceof NotLeaderError) return onNotLeader(req, res, err);
            console.error(err);
            res.status(503).json({ message: 'Read barrier failed', error: (err as Error).message });
        }
    };

    return {
        getBooks: async (req: Request, res: Response): Promise<void> => {
            const serve = () => res.json(node.stateMachine.getAll());
            if (wantsStrongRead(req)) return strongRead(req, res, serve);
            serve();
        },

        getBook: async (req: Request, res: Response): Promise<void> => {
            const serve = () => {
                const book = node.stateMachine.get(req.params.id);
                if (!book) {
                    res.status(404).json({ message: 'Book not found' });
                    return;
                }
                res.json(book);
            };
            if (wantsStrongRead(req)) return strongRead(req, res, serve);
            serve();
        },

        addBook: async (req: Request, res: Response): Promise<void> => {
            const { title, author, publisher, isbn, copies } = req.body;
            await propose(req, res, () => buildAddCommand({ title, author, publisher, isbn, copies }));
        },

        updateBook: async (req: Request, res: Response): Promise<void> => {
            const { title, author, publisher, isbn, copies } = req.body;
            await propose(req, res, () => buildUpdateCommand(req.params.id, { title, author, publisher, isbn, copies }));
        },

        deleteBook: async (req: Request, res: Response): Promise<void> => {
            await propose(req, res, () => buildDeleteCommand(req.params.id));
        },

        borrowBook: async (req: Request, res: Response): Promise<void> => {
            await propose(req, res, () => buildBorrowCommand(req.params.id, req.body.borrowedBy));
        },

        returnBook: async (req: Request, res: Response): Promise<void> => {
            await propose(req, res, () => buildReturnCommand(req.params.id));
        },
    };
}

export type BookController = ReturnType<typeof createBookController>;
