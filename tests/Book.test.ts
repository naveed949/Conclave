import request from 'supertest';
import mongoose from 'mongoose';
import app from '../src/server';
import Book from '../src/models/book';

const testBook = {
    title: 'Test Book',
    author: 'Test Author',
    publisher: 'Test Publisher',
    isbn: '1234567890',
    copies: 5,
};

describe('Book Routes', () => {
    beforeAll(async () => {
        await mongoose.connect(process.env.MONGO_URI_TEST);
    });

    afterAll(async () => {
        await mongoose.connection.close();
    });

    afterEach(async () => {
        await Book.deleteMany({});
    });

    describe('GET /books', () => {
        it('should return an empty array when there are no books in the database', async () => {
            const response = await request(app).get('/books');
            expect(response.status).toBe(200);
            expect(response.body).toEqual([]);
        });

        it('should return an array of books when there are books in the database', async () => {
            await Book.create(testBook);
            const response = await request(app).get('/books');
            expect(response.status).toBe(200);
            expect(response.body).toEqual([expect.objectContaining(testBook)]);
        });
    });

    describe('POST /books', () => {
        it('should add a new book to the database', async () => {
            const response = await request(app)
                .post('/books')
                .send(testBook)
                .set('Content-Type', 'application/json');
            expect(response.status).toBe(200);
            expect(response.body).toEqual(expect.objectContaining(testBook));
            const book = await Book.findById(response.body._id);
            expect(book).not.toBeNull();
            expect(book).toEqual(expect.objectContaining(testBook));
        });
    });

    describe('PUT /books/:id', () => {
        it('should update a book in the database', async () => {
            const book = await Book.create(testBook);
            const newBookData = {
                title: 'New Title',
                author: 'New Author',
                publisher: 'New Publisher',
                isbn: '0987654321',
                copies: 10,
            };
            const response = await request(app)
                .put(`/books/${book._id}`)
                .send(newBookData)
                .set('Content-Type', 'application/json');
            expect(response.status).toBe(200);
            expect(response.body).toEqual(expect.objectContaining(newBookData));
            const updatedBook = await Book.findById(book._id);
            expect(updatedBook).not.toBeNull();
            expect(updatedBook).toEqual(expect.objectContaining(newBookData));
        });

        it('should return a 404 error if the book is not found', async () => {
            const fakeId = '123456789012';
            const response = await request(app)
                .put(`/books/${fakeId}`)
                .send(testBook)
                .set('Content-Type', 'application/json');
            expect(response.status).toBe(404);
            expect(response.body).toEqual({ message: 'Book not found' });
        });
    });

    describe('DELETE /books/:id', () => {
        it('should delete a book from the database', async () => {
            const book = await Book.create(testBook);
            const response = await request(app).delete(`/books/${book._id}`);
            expect(response.status).toBe
            const deletedBook = await Book.findById(book._id);
            expect(deletedBook).toBeNull();
        });

        it('should return a 404 error if the book is not found', async () => {
            const fakeId = '123456789012';
            const response = await request(app).delete(`/books/${fakeId}`);
            expect(response.status).toBe(404);
            expect(response.body).toEqual({ message: 'Book not found' });
        });
    });

    describe('PUT /books/borrow/:id', () => {
        it('should borrow a book', async () => {
            const book = await Book.create(testBook);
            const borrower = 'John Doe';
            const response = await request(app)
                .put(`/books/borrow/${book._id}`)
                .send({ borrowedBy: borrower })
                .set('Content-Type', 'application/json');
            expect(response.status).toBe(200);
            expect(response.body.borrowedBy).toBe(borrower);
            expect(response.body.copies).toBe(testBook.copies - 1);
            expect(response.body.borrowedDate).toBeDefined();
            expect(response.body.dueDate).toBeDefined();
        });

        it('should return a 404 error if the book is not found', async () => {
            const fakeId = '123456789012';
            const response = await request(app)
                .put(`/books/borrow/${fakeId}`)
                .send({ borrowedBy: 'John Doe' })
                .set('Content-Type', 'application/json');
            expect(response.status).toBe(404);
            expect(response.body).toEqual({ message: 'Book not found' });
        });

        it('should return a 400 error if all copies of the book are borrowed', async () => {
            const book = await Book.create({
                ...testBook,
                copies: 0,
                borrowedBy: 'Jane Doe',
                borrowedDate: new Date(),
                dueDate: new Date(),
            });
            const response = await request(app)
                .put(`/books/borrow/${book._id}`)
                .send({ borrowedBy: 'John Doe' })
                .set('Content-Type', 'application/json');
            expect(response.status).toBe(400);
            expect(response.body).toEqual({ message: 'All copies of this book are currently borrowed' });
        });
    });

    describe('PUT /books/return/:id', () => {
        it('should return a borrowed book', async () => {
            const book = await Book.create({
                ...testBook,
                copies: 0,
                borrowedBy: 'John Doe',
                borrowedDate: new Date(),
                dueDate: new Date(),
            });
            const response = await request(app).put(`/books/return/${book._id}`);
            expect(response.status).toBe(200);
            expect(response.body.borrowedBy).toBeNull();
            expect(response.body.copies).toBe(1);
            expect(response.body.borrowedDate).toBeNull();
            expect(response.body.dueDate).toBeNull();
        });

        it('should return a 404 error if the book is not found', async () => {
            const fakeId = '123456789012';
            const response = await request(app).put(`/books/return/${fakeId}`);
            expect(response.status).toBe(404);
            expect(response.body).toEqual({ message: 'Book not found' });
        });

        it('should return a 400 error if all copies of the book have been returned', async () => {
            const book = await Book.create(testBook);
            const response = await request(app).put(`/books/return/${book._id}`);
            expect(response.status).toBe(400);
            expect(response.body).toEqual({ message: 'Cannot return book as all copies have been returned' });
        });
    });
});
