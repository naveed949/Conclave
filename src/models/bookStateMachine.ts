import type { RaftNode } from '../consensus/raftNode';
import type { StateMachine } from '../consensus/stateMachine';
import type { ApplyResult } from '../consensus/types';
import type { Book, BookCommand } from './book';

/**
 * The book application as a deterministic {@link StateMachine}: an in-memory
 * book store. Every node owns its own copy; because Raft guarantees all nodes
 * apply the same committed commands in the same order, every copy converges to
 * the same state without any shared/central database.
 *
 * This is example/application code, not part of the framework — it shows what a
 * consumer writes to run their domain on the consensus core.
 */
export class BookStateMachine implements StateMachine<BookCommand, Book> {
    private books = new Map<string, Book>();
    /** Secondary index isbn -> id, so duplicate-ISBN checks are O(1), not O(n). */
    private idByIsbn = new Map<string, string>();

    /** Apply a committed command. Must be deterministic. */
    apply(command: BookCommand): ApplyResult<Book> {
        switch (command.type) {
            case 'ADD': {
                const { book } = command;
                if (this.idByIsbn.has(book.isbn)) {
                    return { status: 400, message: 'A book with this ISBN already exists' };
                }
                this.books.set(book.id, { ...book });
                this.idByIsbn.set(book.isbn, book.id);
                return { status: 201, data: { ...book } };
            }

            case 'UPDATE': {
                const book = this.books.get(command.id);
                if (!book) return { status: 404, message: 'Book not found' };
                const updated: Book = { ...book, ...command.fields, id: book.id };
                // Keep the isbn index in sync if the ISBN changed.
                if (updated.isbn !== book.isbn) {
                    this.idByIsbn.delete(book.isbn);
                    this.idByIsbn.set(updated.isbn, book.id);
                }
                this.books.set(book.id, updated);
                return { status: 200, data: { ...updated } };
            }

            case 'DELETE': {
                const book = this.books.get(command.id);
                if (!book) return { status: 404, message: 'Book not found' };
                this.books.delete(command.id);
                this.idByIsbn.delete(book.isbn);
                return { status: 200, message: 'Book removed' };
            }

            case 'BORROW': {
                const book = this.books.get(command.id);
                if (!book) return { status: 404, message: 'Book not found' };
                if (book.copies <= 0) {
                    return { status: 400, message: 'All copies of this book are currently borrowed' };
                }
                book.copies -= 1;
                book.borrowedBy = command.borrowedBy;
                book.borrowedDate = command.borrowedDate;
                book.dueDate = command.dueDate;
                return { status: 200, data: { ...book } };
            }

            case 'RETURN': {
                const book = this.books.get(command.id);
                if (!book) return { status: 404, message: 'Book not found' };
                if (book.copies >= book.totalCopies) {
                    return { status: 400, message: 'Cannot return book as all copies have been returned' };
                }
                book.copies += 1;
                book.borrowedBy = null;
                book.borrowedDate = null;
                book.dueDate = null;
                return { status: 200, data: { ...book } };
            }

            default: {
                // Exhaustiveness guard — a new command type must be handled above.
                const _never: never = command;
                return { status: 500, message: `Unknown command: ${JSON.stringify(_never)}` };
            }
        }
    }

    // ---- snapshot / restore (framework log-compaction contract) ----

    snapshot(): Book[] {
        return this.getAll();
    }

    restore(data: unknown): void {
        this.books.clear();
        this.idByIsbn.clear();
        for (const b of (data as Book[]) ?? []) {
            this.books.set(b.id, { ...b });
            this.idByIsbn.set(b.isbn, b.id);
        }
    }

    // ---- domain reads (used by the HTTP controller) ----

    getAll(): Book[] {
        return [...this.books.values()];
    }

    get(id: string): Book | undefined {
        const book = this.books.get(id);
        return book ? { ...book } : undefined;
    }

    /** Number of books — handy for tests asserting convergence. */
    size(): number {
        return this.books.size;
    }
}

/** A Raft node running the book application — the concrete type the HTTP layer wires. */
export type BookNode = RaftNode<BookCommand, Book, BookStateMachine>;
