import express from 'express';
import { ModuleNode } from '../runtime/moduleStateMachine';
import { createModuleController } from '../controllers/moduleController';

/**
 * Generic HTTP routes for a node running the module runtime (ADR-0019), the
 * runtime analog of `bookRoutes`. Mounted under `/modules`.
 *
 * Route ordering is deliberate: the static read subpaths (`.../query/:name`,
 * `.../state`) are registered BEFORE the catch-all write path (`.../:command`) so
 * a literal `query`/`state` segment can never be mis-bound as a command name.
 * (They are also GET vs POST, but ordering keeps the intent unambiguous.)
 */
export default function moduleRoutes(node: ModuleNode) {
    const router = express.Router();
    const c = createModuleController(node);

    // @route GET  /modules/:module/query/:name  @desc Run a read-only query (local; ?consistency=strong for linearizable)
    router.get('/:module/query/:name', c.query);
    // @route GET  /modules/:module/state        @desc Raw current state of a module (local read)
    router.get('/:module/state', c.getState);
    // @route POST /modules/:module/:command     @desc Propose a module command (body = input)
    router.post('/:module/:command', c.runCommand);

    return router;
}
