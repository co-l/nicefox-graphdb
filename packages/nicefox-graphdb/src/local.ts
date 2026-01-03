// NiceFox GraphDB - Local Embedded Client
// Uses SQLite directly without HTTP

import * as fs from "fs";
import * as path from "path";
import { GraphDatabase } from "./db.js";
import { Executor } from "./executor.js";
import type {
  GraphDBClient,
  GraphDBOptions,
  QueryResponse,
  HealthResponse,
  NodeResult,
} from "./types.js";
import { GraphDBError } from "./types.js";

/**
 * Create a local embedded GraphDB client.
 * This client uses SQLite directly without any HTTP layer.
 */
export function createLocalClient(options: GraphDBOptions = {}): GraphDBClient {
  const dataPath = options.dataPath ?? process.env.GRAPHDB_DATA_PATH ?? "./data";
  const project = options.project ?? process.env.GRAPHDB_PROJECT;
  const env = options.env ?? process.env.NODE_ENV ?? "production";

  if (!project) {
    throw new Error("Project is required. Set via options.project or GRAPHDB_PROJECT env var.");
  }

  // Determine database path
  let dbPath: string;
  if (dataPath === ":memory:") {
    dbPath = ":memory:";
  } else {
    // Ensure directory exists
    const dir = path.join(dataPath, env);
    if (!fs.existsSync(dir)) {
      fs.mkdirSync(dir, { recursive: true });
    }
    dbPath = path.join(dir, `${project}.db`);
  }

  // Create and initialize database
  const db = new GraphDatabase(dbPath);
  db.initialize();
  const executor = new Executor(db);

  return {
    async query<T = Record<string, unknown>>(
      cypher: string,
      params: Record<string, unknown> = {}
    ): Promise<T[]> {
      const result = executor.execute(cypher, params);
      if (!result.success) {
        throw new GraphDBError(result.error.message, {
          position: result.error.position,
          line: result.error.line,
          column: result.error.column,
        });
      }
      return result.data as T[];
    },

    async queryRaw<T = Record<string, unknown>>(
      cypher: string,
      params: Record<string, unknown> = {}
    ): Promise<QueryResponse<T>> {
      const result = executor.execute(cypher, params);
      if (!result.success) {
        throw new GraphDBError(result.error.message, {
          position: result.error.position,
          line: result.error.line,
          column: result.error.column,
        });
      }
      return {
        success: true,
        data: result.data as T[],
        meta: result.meta,
      };
    },

    async execute(
      cypher: string,
      params: Record<string, unknown> = {}
    ): Promise<void> {
      const result = executor.execute(cypher, params);
      if (!result.success) {
        throw new GraphDBError(result.error.message, {
          position: result.error.position,
          line: result.error.line,
          column: result.error.column,
        });
      }
    },

    async createNode(
      label: string,
      properties: Record<string, unknown> = {}
    ): Promise<string> {
      const propKeys = Object.keys(properties);
      const propAssignments = propKeys.map((k) => `${k}: $${k}`).join(", ");

      const cypher = `CREATE (n:${label} {${propAssignments}}) RETURN id(n) as id`;
      const result = await this.query<{ id: string }>(cypher, properties);

      return result[0]?.id;
    },

    async createEdge(
      sourceId: string,
      type: string,
      targetId: string,
      properties: Record<string, unknown> = {}
    ): Promise<void> {
      const propKeys = Object.keys(properties);
      const propAssignments =
        propKeys.length > 0
          ? ` {${propKeys.map((k) => `${k}: $${k}`).join(", ")}}`
          : "";

      const cypher = `
        MATCH (source), (target)
        WHERE id(source) = $sourceId AND id(target) = $targetId
        MERGE (source)-[:${type}${propAssignments}]->(target)
      `;

      await this.execute(cypher, { sourceId, targetId, ...properties });
    },

    async getNode(
      label: string,
      filter: Record<string, unknown>
    ): Promise<NodeResult | null> {
      const filterKeys = Object.keys(filter);
      const filterProps = filterKeys.map((k) => `${k}: $${k}`).join(", ");

      const cypher = `MATCH (n:${label} {${filterProps}}) RETURN n LIMIT 1`;
      const result = await this.query<{ n: NodeResult }>(cypher, filter);

      return result.length > 0 ? result[0].n : null;
    },

    async deleteNode(id: string): Promise<void> {
      await this.execute("MATCH (n) WHERE id(n) = $id DETACH DELETE n", { id });
    },

    async updateNode(
      id: string,
      properties: Record<string, unknown>
    ): Promise<void> {
      const propKeys = Object.keys(properties);
      const setClause = propKeys.map((k) => `n.${k} = $${k}`).join(", ");

      const cypher = `MATCH (n) WHERE id(n) = $id SET ${setClause}`;
      await this.execute(cypher, { id, ...properties });
    },

    async health(): Promise<HealthResponse> {
      return {
        status: "ok",
        timestamp: new Date().toISOString(),
      };
    },

    close(): void {
      db.close();
    },
  };
}
