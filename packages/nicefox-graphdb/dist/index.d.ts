import type { GraphDBOptions, GraphDBClient } from "./types.js";
export type { GraphDBOptions, GraphDBClient, QueryResponse, HealthResponse, NodeResult, } from "./types.js";
export { GraphDBError } from "./types.js";
export { parse } from "./parser.js";
export type { Query, Clause, CreateClause, MatchClause, MergeClause, SetClause, DeleteClause, ReturnClause, NodePattern, RelationshipPattern, EdgePattern, WhereCondition, Expression, PropertyValue, ParameterRef, ParseResult, ParseError, } from "./parser.js";
export { translate, Translator } from "./translator.js";
export type { SqlStatement, TranslationResult } from "./translator.js";
export { GraphDatabase, DatabaseManager } from "./db.js";
export type { Node, Edge, NodeRow, EdgeRow, QueryResult } from "./db.js";
export { Executor, executeQuery } from "./executor.js";
export type { ExecutionResult, ExecutionError, QueryResponse as ServerQueryResponse, } from "./executor.js";
export { createApp, createServer } from "./routes.js";
export type { QueryRequest, ServerOptions } from "./routes.js";
export { BackupManager } from "./backup.js";
export type { BackupResult, BackupStatus, BackupAllOptions } from "./backup.js";
export { ApiKeyStore, authMiddleware, generateApiKey } from "./auth.js";
export type { ApiKeyConfig, ValidationResult, KeyInfo } from "./auth.js";
export declare const VERSION = "0.1.0";
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
export declare function GraphDB(options: GraphDBOptions): Promise<GraphDBClient>;
export default GraphDB;
//# sourceMappingURL=index.d.ts.map