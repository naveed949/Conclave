import { Request, Response } from 'express';
import Book, { IBook } from '../models/book';

// @desc    Get all books
// @route   GET /books
// @access  Public
export const getBooks = async (req: Request, res: Response): Promise<void> => {
    try {
        const books: IBook[] = await Book.find();
        res.json(books);
    } catch (err) {
        console.log(err);
        res.status(500).json({ message: 'Server error' });
    }
};

// @desc    Add a book
// @route   POST /books
// @access  Public
export const addBook = async (req: Request, res: Response): Promise<void> => {
    const { title, author, publisher, isbn, copies } = req.body;

    try {
        const newBook: IBook = new Book({
            title,
            author,
            publisher,
            isbn,
            copies,
        });

        await newBook.save();
        res.json(newBook);
    } catch (err) {
        console.log(err);
        res.status(500).json({ message: 'Server error' });
    }
};

// @desc    Update a book
// @route   PUT /books/:id
// @access  Public
export const updateBook = async (req: Request, res: Response): Promise<void> => {
    const { title, author, publisher, isbn, copies } = req.body;

    try {
        const book: IBook | null = await Book.findById(req.params.id);

        if (!book) {
            res.status(404).json({ message: 'Book not found' });
            return;
        }

        book.title = title || book.title;
        book.author = author || book.author;
        book.publisher = publisher || book.publisher;
        book.isbn = isbn || book.isbn;
        book.copies = copies || book.copies;

        await book.save();
        res.json(book);
    } catch (err) {
        console.log(err);
        res.status(500).json({ message: 'Server error' });
    }
};

// @desc    Delete a book
// @route   DELETE /books/:id
// @access  Public
export const deleteBook = async (req: Request, res: Response): Promise<void> => {
    try {
        const book: IBook | null = await Book.findById(req.params.id);

        if (!book) {
            res.status(404).json({ message: 'Book not found' });
            return;
        }

        await book.deleteOne()
        res.json({ message: 'Book removed' });
    } catch (err) {
        console.log(err);
        res.status(500).json({ message: 'Server error' });
    }
};

// @desc    Borrow a book
// @route   PUT /books/borrow/:id
// @access  Public
export const borrowBook = async (req: Request, res: Response): Promise<void> => {
    try {
        const book: IBook | null = await Book.findById(req.params.id);

        if (!book) {
            res.status(404).json({ message: 'Book not found' });
            return;
        }

        if (book.copies <= 0) {
            res.status(400).json({ message: 'All copies of this book are currently borrowed' });
            return;
        }

        const borrowedBy: string = req.body.borrowedBy;
        const borrowedDate: Date = new Date();
        const dueDate: Date = new Date();
        dueDate.setDate(dueDate.getDate() + 7); // set due date to 7 days from today

        book.copies--;
        book.borrowedBy = borrowedBy;
        book.borrowedDate = borrowedDate;
        book.dueDate = dueDate;

        await book.save();
        res.json(book);
    } catch (err) {
        console.log(err);
        res.status(500).json({ message: 'Server error' });
    }
};

// @desc    Return a borrowed book
// @route   PUT /books/return/:id
// @access  Public
export const returnBook = async (req: Request, res: Response): Promise<void> => {
    try {
        const book: IBook | null = await Book.findById(req.params.id);

        if (!book) {
            res.status(404).json({ message: 'Book not found' });
            return;
        }

        if (book.copies >= 10) { // assuming maximum number of copies is 10
            res.status(400).json({ message: 'Cannot return book as all copies have been returned' });
            return;
        }

        book.copies++;
        book.borrowedBy = null;
        book.borrowedDate = null;
        book.dueDate = null;

        await book.save();
        res.json(book);
    } catch (err) {
        console.log(err);
        res.status(500).json({ message: 'Server error' });
    }
};

