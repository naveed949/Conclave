import { getContext } from './requestContext';

export type LogLevel = 'debug' | 'info' | 'warn' | 'error';

const ORDER: Record<LogLevel, number> = { debug: 10, info: 20, warn: 30, error: 40 };

type Fields = Record<string, unknown>;

/**
 * Minimal structured logger (no external deps). Emits one JSON object per line
 * by default — easy to ship to any log aggregator — and automatically enriches
 * every line with the active request's id/actor and any bound base fields.
 */
export class Logger {
    constructor(
        private readonly base: Fields = {},
        private readonly level: LogLevel = 'info',
        private readonly pretty = false,
        private readonly silent = false,
    ) {}

    /** Derive a logger with extra always-on fields (e.g. node id, component). */
    child(fields: Fields): Logger {
        return new Logger({ ...this.base, ...fields }, this.level, this.pretty, this.silent);
    }

    debug(msg: string, fields?: Fields): void { this.write('debug', msg, fields); }
    info(msg: string, fields?: Fields): void { this.write('info', msg, fields); }
    warn(msg: string, fields?: Fields): void { this.write('warn', msg, fields); }
    error(msg: string, fields?: Fields): void { this.write('error', msg, fields); }

    private write(level: LogLevel, msg: string, fields?: Fields): void {
        if (this.silent || ORDER[level] < ORDER[this.level]) return;
        const ctx = getContext();
        const record: Fields = {
            ts: new Date().toISOString(),
            level,
            msg,
            ...this.base,
            ...(ctx ? { requestId: ctx.requestId, actor: ctx.actor } : {}),
            ...fields,
        };
        if (this.pretty) {
            const extra = { ...record };
            delete extra.ts; delete extra.level; delete extra.msg;
            console.log(`${record.ts} ${level.toUpperCase().padEnd(5)} ${msg} ${JSON.stringify(extra)}`);
        } else {
            console.log(JSON.stringify(record));
        }
    }
}

export function createLogger(env: NodeJS.ProcessEnv = process.env): Logger {
    const level = (env.LOG_LEVEL as LogLevel) || 'info';
    const pretty = env.LOG_FORMAT === 'pretty';
    const silent = env.LOG_SILENT === 'true';
    return new Logger({}, level, pretty, silent);
}
