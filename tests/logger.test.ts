import { Logger, createLogger } from '../src/platform/logger';
import { runWithContext } from '../src/platform/requestContext';

/**
 * Unit tests for the structured Logger: level filtering, JSON vs pretty output,
 * base/child field enrichment, request-context enrichment, and the env-driven
 * `createLogger` factory. We capture `console.log` rather than asserting on the
 * terminal.
 */
describe('Logger', () => {
    let lines: string[];
    let spy: jest.SpyInstance;

    beforeEach(() => {
        lines = [];
        spy = jest.spyOn(console, 'log').mockImplementation((...args: unknown[]) => {
            lines.push(args.map(String).join(' '));
        });
    });
    afterEach(() => spy.mockRestore());

    it('emits one JSON object per line with ts/level/msg', () => {
        new Logger({}, 'info').info('hello', { a: 1 });
        expect(lines).toHaveLength(1);
        const rec = JSON.parse(lines[0]);
        expect(rec).toMatchObject({ level: 'info', msg: 'hello', a: 1 });
        expect(typeof rec.ts).toBe('string');
    });

    it('suppresses records below the configured level', () => {
        const log = new Logger({}, 'warn');
        log.debug('d');
        log.info('i');
        log.warn('w');
        log.error('e');
        const levels = lines.map((l) => JSON.parse(l).level);
        expect(levels).toEqual(['warn', 'error']);
    });

    it('writes nothing when silent', () => {
        new Logger({}, 'debug', false, true).error('boom');
        expect(lines).toHaveLength(0);
    });

    it('merges base fields and child fields, child overriding nothing it should not', () => {
        const log = new Logger({ node: 'n1' }, 'info').child({ component: 'raft' });
        log.info('m');
        expect(JSON.parse(lines[0])).toMatchObject({ node: 'n1', component: 'raft', msg: 'm' });
    });

    it('enriches with the active request context (requestId/actor)', () => {
        runWithContext({ requestId: 'req-7', actor: 'bob' }, () => new Logger().info('within'));
        expect(JSON.parse(lines[0])).toMatchObject({ requestId: 'req-7', actor: 'bob' });
    });

    it('pretty mode prints a human line with the extra fields as JSON', () => {
        new Logger({ node: 'n1' }, 'info', true).warn('careful', { code: 42 });
        expect(lines[0]).toMatch(/WARN /);
        expect(lines[0]).toMatch(/careful/);
        expect(lines[0]).toMatch(/"node":"n1"/);
        expect(lines[0]).toMatch(/"code":42/);
    });

    describe('createLogger from env', () => {
        it('defaults to info level, JSON, not silent', () => {
            createLogger({} as NodeJS.ProcessEnv).debug('hidden');
            createLogger({} as NodeJS.ProcessEnv).info('shown');
            expect(lines.map((l) => JSON.parse(l).msg)).toEqual(['shown']);
        });

        it('honours LOG_LEVEL, LOG_FORMAT=pretty and LOG_SILENT=true', () => {
            createLogger({ LOG_LEVEL: 'debug', LOG_FORMAT: 'pretty' } as NodeJS.ProcessEnv).debug('dbg');
            expect(lines[0]).toMatch(/DEBUG/);
            lines.length = 0;
            createLogger({ LOG_SILENT: 'true' } as NodeJS.ProcessEnv).error('nope');
            expect(lines).toHaveLength(0);
        });
    });
});
