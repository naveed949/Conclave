import { randomUUID } from 'crypto';

/**
 * The library-book example application. This is NOT part of the framework — it
 * is a sample {@link StateMachine} (in `bookStateMachine.ts`) plus the command
 * types and builders that drive it, showing how an application plugs into the
 * consensus core. Swap this out for payments, inventory, a feature-flag store,
 * or anything else with a deterministic state machine.
 */

/** A book as stored in the replicated state machine. */
export interface Book {
    id: string;
    title: string;
    author: string;
    publisher: string;
    isbn: string;
    copies: number;
    totalCopies: number;
    borrowedBy: string | null;
    borrowedDate: string | null; // ISO timestamp
    dueDate: string | null; // ISO timestamp
}

/**
 * The book application's command union. Each command is a plain object with a
 * string `type` (the framework's only requirement, {@link AppCommand}). The
 * leader bakes every non-deterministic value into the command up front, so all
 * replicas apply identical data.
 */
export type BookCommand =
    | { type: 'ADD'; book: Book }
    | { type: 'UPDATE'; id: string; fields: Partial<Omit<Book, 'id'>> }
    | { type: 'DELETE'; id: string }
    | { type: 'BORROW'; id: string; borrowedBy: string; borrowedDate: string; dueDate: string }
    | { type: 'RETURN'; id: string };

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

export function buildAddCommand(input: BookInput): BookCommand {
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

export function buildUpdateCommand(id: string, fields: Partial<BookInput>): BookCommand {
    // Drop undefined keys so absent fields don't overwrite existing values.
    const clean: Partial<Book> = {};
    for (const [k, v] of Object.entries(fields)) {
        if (v !== undefined) (clean as Record<string, unknown>)[k] = v;
    }
    return { type: 'UPDATE', id, fields: clean };
}

export function buildDeleteCommand(id: string): BookCommand {
    return { type: 'DELETE', id };
}

export function buildBorrowCommand(id: string, borrowedBy: string): BookCommand {
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

export function buildReturnCommand(id: string): BookCommand {
    return { type: 'RETURN', id };
}
