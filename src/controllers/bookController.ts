import { Request, Response } from 'express';
import { RaftNode, NotLeaderError } from '../consensus/raftNode';
import {
    buildAddCommand,
    buildBorrowCommand,
    buildDeleteCommand,
    buildReturnCommand,
    buildUpdateCommand,
} from '../models/book';

/**
 * The controller is a thin adapter between HTTP and the consensus layer.
 * Reads are served from the local replicated state machine (eventually
 * consistent). Writes are proposed to the Raft log via the leader; if this
 * node is a follower, the client is told where the leader is (421).
 */
export function createBookController(node: RaftNode) {
    /** Propose a write command, await commit, and relay the result. */
    const propose = async (res: Response, build: () => ReturnType<typeof buildDeleteCommand>) => {
        try {
            const result = await node.submit(build());
            if (result.book) {
                res.status(result.status).json(result.book);
            } else {
                res.status(result.status).json({ message: result.message });
            }
        } catch (err) {
            if (err instanceof NotLeaderError) {
                res.status(421).json({ message: 'Not the leader — retry against the leader', leader: err.leaderId });
                return;
            }
            console.error(err);
            res.status(500).json({ message: 'Server error' });
        }
    };

    return {
        // GET /books — served locally from this node's state machine.
        getBooks: async (_req: Request, res: Response): Promise<void> => {
            res.json(node.stateMachine.getAll());
        },

        // GET /books/:id
        getBook: async (req: Request, res: Response): Promise<void> => {
            const book = node.stateMachine.get(req.params.id);
            if (!book) {
                res.status(404).json({ message: 'Book not found' });
                return;
            }
            res.json(book);
        },

        // POST /books
        addBook: async (req: Request, res: Response): Promise<void> => {
            const { title, author, publisher, isbn, copies } = req.body;
            await propose(res, () => buildAddCommand({ title, author, publisher, isbn, copies }));
        },

        // PUT /books/:id
        updateBook: async (req: Request, res: Response): Promise<void> => {
            const { title, author, publisher, isbn, copies } = req.body;
            await propose(res, () => buildUpdateCommand(req.params.id, { title, author, publisher, isbn, copies }));
        },

        // DELETE /books/:id
        deleteBook: async (req: Request, res: Response): Promise<void> => {
            await propose(res, () => buildDeleteCommand(req.params.id));
        },

        // PUT /books/borrow/:id
        borrowBook: async (req: Request, res: Response): Promise<void> => {
            await propose(res, () => buildBorrowCommand(req.params.id, req.body.borrowedBy));
        },

        // PUT /books/return/:id
        returnBook: async (req: Request, res: Response): Promise<void> => {
            await propose(res, () => buildReturnCommand(req.params.id));
        },
    };
}

export type BookController = ReturnType<typeof createBookController>;
