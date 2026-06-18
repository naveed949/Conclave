import fs from 'fs';
import path from 'path';
import { LogEntry } from './types';

/** The subset of Raft state that MUST survive a restart for safety. */
export interface PersistentState {
    currentTerm: number;
    votedFor: string | null;
    log: LogEntry[];
}

export interface RaftStorage {
    load(): PersistentState | null;
    save(state: PersistentState): void;
}

/** No-op storage (used by tests and single-process runs that don't need durability). */
export class MemoryStorage implements RaftStorage {
    private state: PersistentState | null = null;
    load(): PersistentState | null {
        return this.state;
    }
    save(state: PersistentState): void {
        this.state = state;
    }
}

/**
 * Durable storage: one JSON file per node under `dataDir`. Written
 * synchronously on every persisted mutation so a crash can't lose an
 * acknowledged term/vote/log entry (the correctness guarantee Raft needs).
 */
export class FileStorage implements RaftStorage {
    private readonly file: string;

    constructor(nodeId: string, dataDir = './data') {
        fs.mkdirSync(dataDir, { recursive: true });
        this.file = path.join(dataDir, `${nodeId}.json`);
    }

    load(): PersistentState | null {
        try {
            return JSON.parse(fs.readFileSync(this.file, 'utf8')) as PersistentState;
        } catch {
            return null;
        }
    }

    save(state: PersistentState): void {
        const tmp = `${this.file}.tmp`;
        fs.writeFileSync(tmp, JSON.stringify(state));
        fs.renameSync(tmp, this.file); // atomic replace
    }
}
