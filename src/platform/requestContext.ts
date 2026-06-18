import { randomUUID } from 'crypto';
import { AsyncLocalStorage } from 'async_hooks';
import { NextFunction, Request, Response } from 'express';

/**
 * Per-request context propagated implicitly via AsyncLocalStorage, so the
 * logger (and anything else) can tag output with the requestId/actor without
 * threading them through every function call.
 */
export interface RequestContext {
    requestId: string;
    actor: string;
}

const storage = new AsyncLocalStorage<RequestContext>();

export function getContext(): RequestContext | undefined {
    return storage.getStore();
}

export function runWithContext<T>(ctx: RequestContext, fn: () => T): T {
    return storage.run(ctx, fn);
}

/**
 * Express middleware: derive a requestId (honouring an inbound `X-Request-Id`
 * for cross-service tracing) and actor, echo the id back, and run the rest of
 * the request inside that context.
 */
export function requestContextMiddleware(req: Request, res: Response, next: NextFunction): void {
    const requestId = req.header('x-request-id') || randomUUID();
    const actor = req.header('x-actor') || 'anonymous';
    res.setHeader('x-request-id', requestId);
    runWithContext({ requestId, actor }, () => next());
}
