import { ApplyResult, Book, Command } from './types';

/**
 * The replicated state machine: a deterministic, in-memory book store.
 *
 * Every node owns its own copy. Because Raft guarantees all nodes apply the
 * same committed commands in the same order, every copy converges to the same
 * state without any shared/central database. This is the "decentralized"
 * part of the POC.
 */
export class BookStateMachine {
    private books = new Map<string, Book>();

    /** Apply a committed command. Must be deterministic. */
    apply(command: Command): ApplyResult {
        switch (command.type) {
            case 'NOOP':
                return { status: 200 };

            case 'ADD': {
                const { book } = command;
                const duplicate = [...this.books.values()].some((b) => b.isbn === book.isbn);
                if (duplicate) {
                    return { status: 400, message: 'A book with this ISBN already exists' };
                }
                this.books.set(book.id, { ...book });
                return { status: 201, book: { ...book } };
            }

            case 'UPDATE': {
                const book = this.books.get(command.id);
                if (!book) return { status: 404, message: 'Book not found' };
                const updated: Book = { ...book, ...command.fields, id: book.id };
                this.books.set(book.id, updated);
                return { status: 200, book: { ...updated } };
            }

            case 'DELETE': {
                if (!this.books.has(command.id)) return { status: 404, message: 'Book not found' };
                this.books.delete(command.id);
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
                return { status: 200, book: { ...book } };
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
                return { status: 200, book: { ...book } };
            }

            default: {
                // Exhaustiveness guard — a new command type must be handled above.
                const _never: never = command;
                return { status: 500, message: `Unknown command: ${JSON.stringify(_never)}` };
            }
        }
    }

    getAll(): Book[] {
        return [...this.books.values()];
    }

    /** Replace all books (used when restoring from a snapshot). */
    load(books: Book[]): void {
        this.books.clear();
        for (const b of books) this.books.set(b.id, { ...b });
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
