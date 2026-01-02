// NiceFox GraphDB - Unified Package
// A lightweight graph database with Cypher query support, powered by SQLite.

import { createLocalClient } from "./local.js";
import { createRemoteClient } from "./remote.js";
import type { GraphDBOptions, GraphDBClient } from "./types.js";

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

export const VERSION = "0.1.0";

// ============================================================================
// Main Factory Function
// ============================================================================

/**
 * Create a GraphDB client.
 *
 * **Development Mode** (NODE_ENV=development):
 * - Uses a local SQLite database
 * - `url` and `apiKey` are ignored
 * - Data is stored at `dataPath/{env}/{project}.db`
 *
 * **Production Mode** (NODE_ENV=production or unset):
 * - Connects to a remote server via HTTP
 * - `url` and `apiKey` are required
 *
 * @example
 * ```typescript
 * import { GraphDB } from 'nicefox-graphdb';
 *
 * // Same code works in both development and production!
 * const db = await GraphDB({
 *   url: 'https://my-graphdb.example.com',
 *   project: 'myapp',
 *   apiKey: process.env.GRAPHDB_API_KEY,
 * });
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
export async function GraphDB(options: GraphDBOptions): Promise<GraphDBClient> {
  const isDevelopment = process.env.NODE_ENV === "development";

  if (isDevelopment) {
    return createLocalClient(options);
  } else {
    return createRemoteClient(options);
  }
}

// Default export
export default GraphDB;
