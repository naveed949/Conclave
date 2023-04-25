import express from 'express';
import {
    addBook,
    getBooks,
    updateBook,
    deleteBook,
    borrowBook,
    returnBook,
} from '../controllers/bookController';

const router = express.Router();

// @route   GET /books
// @desc    Get all books
router.get('/', getBooks);

// @route   POST /books
// @desc    Add a book
router.post('/', addBook);

// @route   PUT /books/:id
// @desc    Update a book
router.put('/:id', updateBook);

// @route   DELETE /books/:id
// @desc    Delete a book
router.delete('/:id', deleteBook);

// @route   PUT /books/borrow/:id
// @desc    Borrow a book
router.put('/borrow/:id', borrowBook);

// @route   PUT /books/return/:id
// @desc    Return a borrowed book
router.put('/return/:id', returnBook);

export default router;
