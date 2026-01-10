// LeanGraph - Unified Package
// A lightweight graph database with Cypher query support, powered by SQLite.

import { createRequire } from "module";
import { createRemoteClient } from "./remote.js";
import type { GraphDBOptions, GraphDBClient } from "./types.js";

const require = createRequire(import.meta.url);
const pkg = require("../package.json");

// ============================================================================
// Re-export Types
// ============================================================================

export type {
  GraphDBOptions,
  GraphDBClient,
  QueryResponse,
  HealthResponse,
  NodeResult,
} from "./types.js";

export { GraphDBError } from "./types.js";

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
 * All options support environment variable defaults:
 * - `url`: LEANGRAPH_URL (default: 'https://leangraph.io')
 * - `project`: LEANGRAPH_PROJECT (required)
 * - `env`: NODE_ENV (default: 'production')
 * - `apiKey`: LEANGRAPH_API_KEY
 * - `dataPath`: LEANGRAPH_DATA_PATH (default: './data')
 *
 * **Development Mode** (NODE_ENV=development):
 * - Uses a local SQLite database
 * - `url` and `apiKey` are ignored
 * - Data is stored at `dataPath/{env}/{project}.db`
 *
 * **Production Mode** (NODE_ENV=production or unset):
 * - Connects to a remote server via HTTP
 *
 * @example
 * ```typescript
 * import { LeanGraph } from 'leangraph';
 *
 * const db = await LeanGraph({ project: 'myapp' });
 *
 * // Create nodes
 * await db.execute('CREATE (n:User {name: "Alice"})');
 *
 * // Query
 * const users = await db.query('MATCH (n:User) RETURN n');
 *
 * // Always close when done
 * db.close();
 * ```
 */
export async function LeanGraph(options: GraphDBOptions = {}): Promise<GraphDBClient> {
  const isProduction = process.env.NODE_ENV === "production";

  if (isProduction) {
    return createRemoteClient(options);
  } else {
    // Lazy-load local client to avoid requiring better-sqlite3 when not needed
    try {
      const { createLocalClient } = await import("./local.js");
      return createLocalClient(options);
    } catch (err) {
      if (err instanceof Error && err.message.includes("better-sqlite3")) {
        throw new Error(
          "Local mode requires better-sqlite3. Install it with: npm install better-sqlite3\n" +
          "Or set NODE_ENV=production to use remote mode instead."
        );
      }
      throw err;
    }
  }
}

// Default export
export default LeanGraph;
