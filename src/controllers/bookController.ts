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

/**
 * Thin adapter between HTTP and the consensus layer. Reads are served from the
 * local replicated state machine (eventually consistent). Writes are proposed
 * to the Raft log via the leader; a follower transparently forwards the write
 * to the leader (falling back to 421 if the leader is unknown/unreachable).
 */
export function createBookController(node: RaftNode) {
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
            if (err instanceof NotLeaderError) {
                const leaderUrl = node.getLeaderUrl();
                if (leaderUrl && !isForwarded(req)) {
                    const ok = await forwardToLeader(req, res, leaderUrl);
                    if (ok) return;
                }
                res.status(421).json({ message: 'Not the leader — retry against the leader', leader: err.leaderId });
                return;
            }
            console.error(err);
            res.status(500).json({ message: 'Server error' });
        }
    };

    return {
        getBooks: async (_req: Request, res: Response): Promise<void> => {
            res.json(node.stateMachine.getAll());
        },

        getBook: async (req: Request, res: Response): Promise<void> => {
            const book = node.stateMachine.get(req.params.id);
            if (!book) {
                res.status(404).json({ message: 'Book not found' });
                return;
            }
            res.json(book);
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
