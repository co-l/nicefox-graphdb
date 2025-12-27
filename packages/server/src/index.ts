// NiceFox GraphDB Server - Entry Point

export { parse } from "./parser";
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
} from "./parser";

export { translate, Translator } from "./translator";
export type { SqlStatement, TranslationResult } from "./translator";

export { GraphDatabase, DatabaseManager } from "./db";
export type { Node, Edge, NodeRow, EdgeRow, QueryResult } from "./db";

export { Executor, executeQuery } from "./executor";
export type { ExecutionResult, ExecutionError, QueryResponse } from "./executor";

export { createApp, createServer } from "./routes";
export type { QueryRequest, ServerOptions } from "./routes";

export { BackupManager } from "./backup";
export type { BackupResult, BackupStatus, BackupAllOptions } from "./backup";

export { ApiKeyStore, authMiddleware, generateApiKey } from "./auth";
export type { ApiKeyConfig, ValidationResult, KeyInfo } from "./auth";

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
