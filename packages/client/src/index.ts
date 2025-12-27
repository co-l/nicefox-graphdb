// NiceFox GraphDB TypeScript Client

// ============================================================================
// Types
// ============================================================================

export interface ClientOptions {
  /** Base URL of the GraphDB server */
  url: string;
  /** Project name */
  project: string;
  /** Environment: 'production' or 'test' (default: 'production') */
  env?: "production" | "test";
  /** API key for authentication */
  apiKey?: string;
}

export interface QueryResponse<T = Record<string, unknown>> {
  success: boolean;
  data: T[];
  meta: {
    count: number;
    time_ms: number;
  };
  error?: {
    message: string;
    position?: number;
    line?: number;
    column?: number;
  };
}

export interface HealthResponse {
  status: string;
  timestamp: string;
}

export interface NodeResult {
  id: string;
  label: string;
  properties: Record<string, unknown>;
}

// ============================================================================
// Error Class
// ============================================================================

export class GraphDBError extends Error {
  public readonly position?: number;
  public readonly line?: number;
  public readonly column?: number;

  constructor(
    message: string,
    options?: {
      position?: number;
      line?: number;
      column?: number;
    }
  ) {
    super(message);
    this.name = "GraphDBError";
    this.position = options?.position;
    this.line = options?.line;
    this.column = options?.column;
  }
}

// ============================================================================
// Client Class
// ============================================================================

export class NiceFoxGraphDB {
  private url: string;
  private project: string;
  private env: "production" | "test";
  private apiKey?: string;

  constructor(options: ClientOptions) {
    // Normalize URL (remove trailing slash)
    this.url = options.url.replace(/\/$/, "");
    this.project = options.project;
    this.env = options.env || "production";
    this.apiKey = options.apiKey;
  }

  // ==========================================================================
  // Core Query Methods
  // ==========================================================================

  /**
   * Execute a Cypher query and return the data array.
   * Throws GraphDBError if the query fails.
   */
  async query<T = Record<string, unknown>>(
    cypher: string,
    params: Record<string, unknown> = {}
  ): Promise<T[]> {
    const response = await this.queryRaw<T>(cypher, params);
    return response.data;
  }

  /**
   * Execute a Cypher query and return the full response including metadata.
   * Throws GraphDBError if the query fails.
   */
  async queryRaw<T = Record<string, unknown>>(
    cypher: string,
    params: Record<string, unknown> = {}
  ): Promise<QueryResponse<T>> {
    const endpoint = `${this.url}/query/${this.env}/${this.project}`;

    const headers: Record<string, string> = {
      "Content-Type": "application/json",
    };

    if (this.apiKey) {
      headers["Authorization"] = `Bearer ${this.apiKey}`;
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
  }

  /**
   * Execute a mutating query (CREATE, SET, DELETE, MERGE) without expecting return data.
   * Throws GraphDBError if the query fails.
   */
  async execute(
    cypher: string,
    params: Record<string, unknown> = {}
  ): Promise<void> {
    await this.queryRaw(cypher, params);
  }

  // ==========================================================================
  // Convenience Methods
  // ==========================================================================

  /**
   * Create a node with the given label and properties.
   * Returns the generated node ID.
   */
  async createNode(
    label: string,
    properties: Record<string, unknown> = {}
  ): Promise<string> {
    // Build property assignments for the query
    const propKeys = Object.keys(properties);
    const propAssignments = propKeys.map((k) => `${k}: $${k}`).join(", ");

    const cypher = `CREATE (n:${label} {${propAssignments}}) RETURN id(n) as id`;
    const result = await this.query<{ id: string }>(cypher, properties);

    return result[0]?.id;
  }

  /**
   * Create an edge between two nodes.
   */
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
      MATCH (source {id: $sourceId}), (target {id: $targetId})
      MERGE (source)-[:${type}${propAssignments}]->(target)
    `;

    await this.execute(cypher, { sourceId, targetId, ...properties });
  }

  /**
   * Get a node by label and property filter.
   * Returns null if not found.
   */
  async getNode(
    label: string,
    filter: Record<string, unknown>
  ): Promise<NodeResult | null> {
    const filterKeys = Object.keys(filter);
    const filterProps = filterKeys.map((k) => `${k}: $${k}`).join(", ");

    const cypher = `MATCH (n:${label} {${filterProps}}) RETURN n LIMIT 1`;
    const result = await this.query<{ n: NodeResult }>(cypher, filter);

    return result.length > 0 ? result[0].n : null;
  }

  /**
   * Delete a node by ID (with DETACH to remove connected edges).
   */
  async deleteNode(id: string): Promise<void> {
    await this.execute("MATCH (n {id: $id}) DETACH DELETE n", { id });
  }

  /**
   * Update properties on a node.
   */
  async updateNode(
    id: string,
    properties: Record<string, unknown>
  ): Promise<void> {
    const propKeys = Object.keys(properties);
    const setClause = propKeys.map((k) => `n.${k} = $${k}`).join(", ");

    const cypher = `MATCH (n {id: $id}) SET ${setClause}`;
    await this.execute(cypher, { id, ...properties });
  }

  // ==========================================================================
  // Utility Methods
  // ==========================================================================

  /**
   * Check server health.
   */
  async health(): Promise<HealthResponse> {
    const response = await fetch(`${this.url}/health`);
    return response.json() as Promise<HealthResponse>;
  }
}

// Default export for convenience
export default NiceFoxGraphDB;
