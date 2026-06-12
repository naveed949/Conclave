import express from 'express';
import { RaftNode } from '../consensus/raftNode';
import { createBookController } from '../controllers/bookController';

export default function bookRoutes(node: RaftNode) {
    const router = express.Router();
    const c = createBookController(node);

    // @route   GET /books            @desc  Get all books
    router.get('/', c.getBooks);
    // @route   GET /books/:id        @desc  Get one book
    router.get('/:id', c.getBook);
    // @route   POST /books           @desc  Add a book
    router.post('/', c.addBook);
    // @route   PUT /books/:id        @desc  Update a book
    router.put('/:id', c.updateBook);
    // @route   DELETE /books/:id     @desc  Delete a book
    router.delete('/:id', c.deleteBook);
    // @route   PUT /books/borrow/:id @desc  Borrow a book
    router.put('/borrow/:id', c.borrowBook);
    // @route   PUT /books/return/:id @desc  Return a borrowed book
    router.put('/return/:id', c.returnBook);

    return router;
}
