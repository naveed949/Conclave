import fs from 'fs';
import path from 'path';
import { LogEntry, Snapshot } from './types';

/** The subset of Raft state that MUST survive a restart for safety. */
export interface PersistentState {
    currentTerm: number;
    votedFor: string | null;
    log: LogEntry[];
}

export interface RaftStorage {
    load(): PersistentState | null;
    save(state: PersistentState): void;
    loadSnapshot(): Snapshot | null;
    saveSnapshot(snapshot: Snapshot): void;
}

/** No-op storage (used by tests and single-process runs that don't need durability). */
export class MemoryStorage implements RaftStorage {
    private state: PersistentState | null = null;
    private snapshot: Snapshot | null = null;

    load(): PersistentState | null {
        return this.state;
    }
    save(state: PersistentState): void {
        this.state = state;
    }
    loadSnapshot(): Snapshot | null {
        return this.snapshot;
    }
    saveSnapshot(snapshot: Snapshot): void {
        this.snapshot = snapshot;
    }
}

/**
 * Durable storage: one JSON file per node under `dataDir` for the log, and a
 * separate file for the snapshot (written less often, since it's larger).
 * Both are written via atomic rename so a crash can't leave a half-written file.
 */
export class FileStorage implements RaftStorage {
    private readonly logFile: string;
    private readonly snapFile: string;

    constructor(nodeId: string, dataDir = './data') {
        fs.mkdirSync(dataDir, { recursive: true });
        this.logFile = path.join(dataDir, `${nodeId}.json`);
        this.snapFile = path.join(dataDir, `${nodeId}.snapshot.json`);
    }

    private static read<T>(file: string): T | null {
        try {
            return JSON.parse(fs.readFileSync(file, 'utf8')) as T;
        } catch {
            return null;
        }
    }

    private static write(file: string, value: unknown): void {
        const tmp = `${file}.tmp`;
        fs.writeFileSync(tmp, JSON.stringify(value));
        fs.renameSync(tmp, file); // atomic replace
    }

    load(): PersistentState | null {
        return FileStorage.read<PersistentState>(this.logFile);
    }
    save(state: PersistentState): void {
        FileStorage.write(this.logFile, state);
    }
    loadSnapshot(): Snapshot | null {
        return FileStorage.read<Snapshot>(this.snapFile);
    }
    saveSnapshot(snapshot: Snapshot): void {
        FileStorage.write(this.snapFile, snapshot);
    }
}
