import request from 'supertest';
import { Application } from 'express';
import { createApp } from '../src/app';
import { RaftNode } from '../src/consensus/raftNode';
import { buildCluster, waitFor } from './helpers';

const testBook = {
    title: 'Test Book',
    author: 'Test Author',
    publisher: 'Test Publisher',
    isbn: '1234567890',
    copies: 5,
};

describe('Book API (single-node cluster)', () => {
    let node: RaftNode;
    let app: Application;

    beforeAll(async () => {
        // A 1-node cluster: the node elects itself leader, so writes commit immediately.
        [node] = buildCluster(1);
        node.start();
        app = createApp(node);
        await waitFor(() => node.isLeader());
    });

    afterAll(() => {
        node.stop();
    });

    describe('POST /books', () => {
        it('adds a book and returns it with a generated id', async () => {
            const res = await request(app).post('/books').send(testBook);
            expect(res.status).toBe(201);
            expect(res.body).toMatchObject(testBook);
            expect(res.body.id).toBeDefined();
            expect(res.body.totalCopies).toBe(testBook.copies);
        });

        it('rejects a duplicate ISBN', async () => {
            const res = await request(app).post('/books').send({ ...testBook, isbn: '1234567890' });
            expect(res.status).toBe(400);
        });
    });

    describe('GET /books', () => {
        it('lists books from the replicated state machine', async () => {
            const res = await request(app).get('/books');
            expect(res.status).toBe(200);
            expect(Array.isArray(res.body)).toBe(true);
            expect(res.body.length).toBeGreaterThanOrEqual(1);
        });
    });

    describe('borrow / return flow', () => {
        let id: string;
        beforeAll(async () => {
            const res = await request(app).post('/books').send({ ...testBook, isbn: 'borrow-1', copies: 1 });
            id = res.body.id;
        });

        it('borrows a book, decrementing copies and recording the borrower', async () => {
            const res = await request(app).put(`/books/borrow/${id}`).send({ borrowedBy: 'John Doe' });
            expect(res.status).toBe(200);
            expect(res.body.copies).toBe(0);
            expect(res.body.borrowedBy).toBe('John Doe');
            expect(res.body.dueDate).toBeDefined();
        });

        it('rejects borrowing when no copies remain', async () => {
            const res = await request(app).put(`/books/borrow/${id}`).send({ borrowedBy: 'Jane' });
            expect(res.status).toBe(400);
        });

        it('returns the book, restoring copies and clearing the borrower', async () => {
            const res = await request(app).put(`/books/return/${id}`);
            expect(res.status).toBe(200);
            expect(res.body.copies).toBe(1);
            expect(res.body.borrowedBy).toBeNull();
        });

        it('rejects returning when all copies are already present', async () => {
            const res = await request(app).put(`/books/return/${id}`);
            expect(res.status).toBe(400);
        });
    });

    describe('update / delete', () => {
        let id: string;
        beforeAll(async () => {
            const res = await request(app).post('/books').send({ ...testBook, isbn: 'upd-1' });
            id = res.body.id;
        });

        it('updates only the provided fields', async () => {
            const res = await request(app).put(`/books/${id}`).send({ title: 'New Title' });
            expect(res.status).toBe(200);
            expect(res.body.title).toBe('New Title');
            expect(res.body.author).toBe(testBook.author); // untouched
        });

        it('returns 404 when updating a missing book', async () => {
            const res = await request(app).put('/books/does-not-exist').send({ title: 'x' });
            expect(res.status).toBe(404);
        });

        it('deletes a book', async () => {
            const res = await request(app).delete(`/books/${id}`);
            expect(res.status).toBe(200);
            const after = await request(app).get(`/books/${id}`);
            expect(after.status).toBe(404);
        });
    });
});
