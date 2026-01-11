// LeanGraph - Unified Package
// A lightweight graph database with Cypher query support, powered by SQLite.

import { createRequire } from "module";
import { createRemoteClient } from "./remote.js";
import type { LeanGraphOptions, LeanGraphClient } from "./types.js";

const require = createRequire(import.meta.url);
const pkg = require("../package.json");

// ============================================================================
// Re-export Types
// ============================================================================

export type {
  LeanGraphOptions,
  LeanGraphClient,
  QueryResponse,
  HealthResponse,
  NodeResult,
} from "./types.js";

export { LeanGraphError } from "./types.js";

// ============================================================================
// Re-export Server Components (for advanced usage)
// ============================================================================

// Parser
export { parse } from "./parser.js";
export type {
  Query,
  Clause,
  CreateClause,
  MatchClause,
  MergeClause,
  SetClause,
  DeleteClause,
  ReturnClause,
  NodePattern,
  RelationshipPattern,
  EdgePattern,
  WhereCondition,
  Expression,
  PropertyValue,
  ParameterRef,
  ParseResult,
  ParseError,
} from "./parser.js";

// Translator
export { translate, Translator } from "./translator.js";
export type { SqlStatement, TranslationResult } from "./translator.js";

// Database
export { GraphDatabase, DatabaseManager } from "./db.js";
export type { Node, Edge, NodeRow, EdgeRow, QueryResult } from "./db.js";

// Executor
export { Executor, executeQuery } from "./executor.js";
export type {
  ExecutionResult,
  ExecutionError,
  QueryResponse as ServerQueryResponse,
} from "./executor.js";

// Routes / Server
export { createApp, createServer } from "./routes.js";
export type { QueryRequest, ServerOptions } from "./routes.js";

// Backup
export { BackupManager } from "./backup.js";
export type { BackupResult, BackupStatus, BackupAllOptions } from "./backup.js";

// Auth
export { ApiKeyStore, authMiddleware, generateApiKey } from "./auth.js";
export type { ApiKeyConfig, ValidationResult, KeyInfo } from "./auth.js";

// ============================================================================
// Version
// ============================================================================

export const VERSION: string = pkg.version;

// ============================================================================
// Main Factory Function
// ============================================================================

/**
 * Create a LeanGraph client.
 *
 * @example
 * ```typescript
 * import { LeanGraph } from 'leangraph';
 *
 * const db = await LeanGraph({ project: 'myapp' });
 *
 * await db.execute('CREATE (n:User {name: "Alice"})');
 * const users = await db.query('MATCH (n:User) RETURN n');
 *
 * db.close();
 * ```
 */
export async function LeanGraph(options: LeanGraphOptions = {}): Promise<LeanGraphClient> {
  const mode = options.mode ?? (process.env.LEANGRAPH_MODE as "local" | "remote" | "test") ?? "local";

  if (mode === "remote") {
    return createRemoteClient(options);
  } else {
    // local or test - lazy-load to avoid requiring better-sqlite3 when not needed
    try {
      const { createLocalClient } = await import("./local.js");
      return createLocalClient({ ...options, mode });
    } catch (err) {
      if (err instanceof Error && err.message.includes("better-sqlite3")) {
        throw new Error(
          "Local/test mode requires better-sqlite3. Install it with: npm install better-sqlite3\n" +
          "Or set LEANGRAPH_MODE=remote to use remote mode instead."
        );
      }
      throw err;
    }
  }
}

// Default export
export default LeanGraph;
