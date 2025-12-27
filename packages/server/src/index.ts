// NiceFox GraphDB Server - Entry Point

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

export { translate, Translator } from "./translator.js";
export type { SqlStatement, TranslationResult } from "./translator.js";

export { GraphDatabase, DatabaseManager } from "./db.js";
export type { Node, Edge, NodeRow, EdgeRow, QueryResult } from "./db.js";

export { Executor, executeQuery } from "./executor.js";
export type { ExecutionResult, ExecutionError, QueryResponse } from "./executor.js";

export { createApp, createServer } from "./routes.js";
export type { QueryRequest, ServerOptions } from "./routes.js";

export { BackupManager } from "./backup.js";
export type { BackupResult, BackupStatus, BackupAllOptions } from "./backup.js";

export { ApiKeyStore, authMiddleware, generateApiKey } from "./auth.js";
export type { ApiKeyConfig, ValidationResult, KeyInfo } from "./auth.js";

export const VERSION = "0.1.0";

// If this file is run directly, start the server
if (import.meta.url === `file://${process.argv[1]}`) {
  const { serve } = await import("@hono/node-server");
  const { createServer } = await import("./routes");

  const port = parseInt(process.env.PORT || "3000", 10);
  const dataPath = process.env.DATA_PATH || "./data";

  const { app, dbManager } = createServer({ port, dataPath });

  console.log(`NiceFox GraphDB Server v${VERSION}`);
  console.log(`Starting on http://localhost:${port}`);
  console.log(`Data path: ${dataPath}`);

  serve({
    fetch: app.fetch,
    port,
  });
}
