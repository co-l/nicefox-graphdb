// NiceFox GraphDB - Remote HTTP Client
// Connects to a remote GraphDB server

import type {
  GraphDBClient,
  GraphDBOptions,
  QueryResponse,
  HealthResponse,
  NodeResult,
} from "./types.js";
import { GraphDBError } from "./types.js";

/**
 * Create a remote GraphDB client that connects via HTTP.
 */
export function createRemoteClient(options: GraphDBOptions = {}): GraphDBClient {
  // Resolve options with environment variable defaults
  const rawUrl = options.url ?? process.env.GRAPHDB_URL ?? "https://graphdb.nicefox.net";
  const project = options.project ?? process.env.GRAPHDB_PROJECT;
  const env = options.env ?? process.env.NODE_ENV ?? "production";
  const apiKey = options.apiKey ?? process.env.GRAPHDB_API_KEY;

  if (!project) {
    throw new Error("Project is required. Set via options.project or GRAPHDB_PROJECT env var.");
  }

  // Normalize URL (remove trailing slash)
  const url = rawUrl.replace(/\/$/, "");

  return {
    async query<T = Record<string, unknown>>(
      cypher: string,
      params: Record<string, unknown> = {}
    ): Promise<T[]> {
      const response = await this.queryRaw<T>(cypher, params);
      return response.data;
    },

    async queryRaw<T = Record<string, unknown>>(
      cypher: string,
      params: Record<string, unknown> = {}
    ): Promise<QueryResponse<T>> {
      const endpoint = `${url}/query/${env}/${project}`;

      const headers: Record<string, string> = {
        "Content-Type": "application/json",
      };

      if (apiKey) {
        headers["Authorization"] = `Bearer ${apiKey}`;
      }

      const response = await fetch(endpoint, {
        method: "POST",
        headers,
        body: JSON.stringify({ cypher, params }),
      });

      const data = (await response.json()) as QueryResponse<T>;

      if (!data.success || data.error) {
        throw new GraphDBError(data.error?.message || "Query failed", {
          position: data.error?.position,
          line: data.error?.line,
          column: data.error?.column,
        });
      }

      return data;
    },

    async execute(
      cypher: string,
      params: Record<string, unknown> = {}
    ): Promise<void> {
      await this.queryRaw(cypher, params);
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
      const response = await fetch(`${url}/health`);
      return response.json() as Promise<HealthResponse>;
    },

    close(): void {
      // No-op for remote client (no resources to release)
    },
  };
}
