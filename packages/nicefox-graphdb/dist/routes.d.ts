import { Hono } from "hono";
import { DatabaseManager } from "./db.js";
import { BackupManager } from "./backup.js";
import { ApiKeyStore } from "./auth.js";
export interface QueryRequest {
    cypher: string;
    params?: Record<string, unknown>;
}
export interface AppContext {
    dbManager: DatabaseManager;
}
export declare function createApp(dbManager: DatabaseManager, dataPath?: string, backupManager?: BackupManager, apiKeyStore?: ApiKeyStore): Hono;
export interface ServerOptions {
    port?: number;
    dataPath?: string;
    backupPath?: string;
    apiKeys?: Record<string, {
        project?: string;
        env?: string;
        admin?: boolean;
    }>;
}
export declare function createServer(options?: ServerOptions): {
    app: Hono<import("hono/types").BlankEnv, import("hono/types").BlankSchema, "/">;
    dbManager: DatabaseManager;
    backupManager: BackupManager | undefined;
    apiKeyStore: ApiKeyStore | undefined;
    port: number;
    fetch: (request: Request, Env?: unknown, executionCtx?: import("hono").ExecutionContext) => Response | Promise<Response>;
};
//# sourceMappingURL=routes.d.ts.map