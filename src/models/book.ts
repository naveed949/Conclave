import { randomUUID } from 'crypto';
import { Book, Command } from '../consensus/types';

export { Book } from '../consensus/types';

/** Shape of a create-book request body. */
export interface BookInput {
    title: string;
    author: string;
    publisher: string;
    isbn: string;
    copies: number;
}

const LOAN_PERIOD_DAYS = 7;

/**
 * Command builders. These run ONLY on the leader and resolve every
 * non-deterministic value (id, timestamps) up front, so the command that
 * enters the replicated log applies identically on every node.
 */

export function buildAddCommand(input: BookInput): Command {
    const book: Book = {
        id: randomUUID(),
        title: input.title,
        author: input.author,
        publisher: input.publisher,
        isbn: input.isbn,
        copies: input.copies,
        totalCopies: input.copies,
        borrowedBy: null,
        borrowedDate: null,
        dueDate: null,
    };
    return { type: 'ADD', book };
}

export function buildUpdateCommand(id: string, fields: Partial<BookInput>): Command {
    // Drop undefined keys so absent fields don't overwrite existing values.
    const clean: Partial<Book> = {};
    for (const [k, v] of Object.entries(fields)) {
        if (v !== undefined) (clean as Record<string, unknown>)[k] = v;
    }
    return { type: 'UPDATE', id, fields: clean };
}

export function buildDeleteCommand(id: string): Command {
    return { type: 'DELETE', id };
}

export function buildBorrowCommand(id: string, borrowedBy: string): Command {
    const borrowedDate = new Date();
    const dueDate = new Date(borrowedDate);
    dueDate.setDate(dueDate.getDate() + LOAN_PERIOD_DAYS);
    return {
        type: 'BORROW',
        id,
        borrowedBy,
        borrowedDate: borrowedDate.toISOString(),
        dueDate: dueDate.toISOString(),
    };
}

export function buildReturnCommand(id: string): Command {
    return { type: 'RETURN', id };
}
