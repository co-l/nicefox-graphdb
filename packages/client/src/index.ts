// NiceFox GraphDB TypeScript Client

// ============================================================================
// Types
// ============================================================================

export interface TestClientOptions {
  /** Project name (default: 'test') */
  project?: string;
}

export interface TestClient {
  /** Execute a Cypher query and return the data array */
  query<T = Record<string, unknown>>(cypher: string, params?: Record<string, unknown>): Promise<T[]>;
  /** Execute a Cypher query and return the full response including metadata */
  queryRaw<T = Record<string, unknown>>(cypher: string, params?: Record<string, unknown>): Promise<QueryResponse<T>>;
  /** Execute a mutating query without expecting return data */
  execute(cypher: string, params?: Record<string, unknown>): Promise<void>;
  /** Create a node with the given label and properties, returns the generated ID */
  createNode(label: string, properties?: Record<string, unknown>): Promise<string>;
  /** Create an edge between two nodes */
  createEdge(sourceId: string, type: string, targetId: string, properties?: Record<string, unknown>): Promise<void>;
  /** Get a node by label and property filter */
  getNode(label: string, filter: Record<string, unknown>): Promise<NodeResult | null>;
  /** Delete a node by ID */
  deleteNode(id: string): Promise<void>;
  /** Update properties on a node */
  updateNode(id: string, properties: Record<string, unknown>): Promise<void>;
  /** Check server health */
  health(): Promise<HealthResponse>;
  /** Stop the test server and clean up resources */
  close(): void;
}

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

/**
 * Result type for node queries.
 * In Neo4j 3.5 format, nodes return their properties directly.
 * Use id(n), labels(n), type(r) functions to access metadata.
 */
export type NodeResult = Record<string, unknown>;

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
   * Returns the node's properties directly, or null if not found.
   * 
   * @example
   * ```ts
   * const user = await graph.getNode('User', { id: 'abc123' });
   * // user = { id: 'abc123', name: 'Alice', email: 'alice@example.com' }
   * console.log(user?.name);  // 'Alice'
   * ```
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

// ============================================================================
// Test Client Factory
// ============================================================================

/**
 * Create an in-memory test client for unit testing.
 * This spins up an embedded server with an in-memory SQLite database.
 * 
 * Requires the server package to be installed: `npm install github:co-l/nicefox-graphdb#main`
 * 
 * @example
 * ```ts
 * import { createTestClient } from 'nicefox-graphdb/packages/client/src/index.ts';
 * 
 * const client = await createTestClient();
 * 
 * // Use like a normal client
 * await client.createNode('User', { name: 'Alice' });
 * const users = await client.query('MATCH (u:User) RETURN u');
 * 
 * // Clean up when done
 * client.close();
 * ```
 */
export async function createTestClient(options: TestClientOptions = {}): Promise<TestClient> {
  const { project = 'test' } = options;

  // Type for the server factory function
  type CreateServerFn = (options?: { dataPath?: string }) => {
    app: { request: (path: string, init?: RequestInit) => Response | Promise<Response> };
    dbManager: { closeAll: () => void };
  };

  // Dynamically import the server package
  // Users install via: "nicefox-graphdb": "github:user/nicefox-graphdb#main"
  let createServer: CreateServerFn | undefined;
  let lastError: Error | undefined;
  
  // Try different import paths to support various installation methods
  const importPaths = [
    'nicefox-graphdb/packages/server/src/index.js',  // GitHub dependency
    '../../server/src/index.js',                      // Local workspace (relative)
  ];
  
  for (const importPath of importPaths) {
    try {
      const serverModule = await import(/* @vite-ignore */ importPath);
      createServer = serverModule.createServer as CreateServerFn;
      break;
    } catch (e) {
      lastError = e as Error;
    }
  }
  
  if (!createServer) {
    throw new Error(
      'Could not import nicefox-graphdb server. Make sure it is installed:\n' +
      '  Add to package.json dependencies:\n' +
      '  "nicefox-graphdb": "github:your-org/nicefox-graphdb#main"\n' +
      (lastError ? `\nOriginal error: ${lastError.message}` : '')
    );
  }

  // Create an in-memory server
  const server = createServer({ dataPath: ':memory:' });

  // Create a wrapper that uses app.request() instead of fetch
  const testClient = {
    server,
    project,

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
      const response = await this.server.app.request(`/query/test/${this.project}`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ cypher, params }),
      });

      const data = (await response.json()) as QueryResponse<T>;

      if (!data.success || data.error) {
        throw new GraphDBError(data.error?.message || 'Query failed', {
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
        MATCH (source {id: $sourceId}), (target {id: $targetId})
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
      await this.execute("MATCH (n {id: $id}) DETACH DELETE n", { id });
    },

    async updateNode(
      id: string,
      properties: Record<string, unknown>
    ): Promise<void> {
      const propKeys = Object.keys(properties);
      const setClause = propKeys.map((k) => `n.${k} = $${k}`).join(", ");
      const cypher = `MATCH (n {id: $id}) SET ${setClause}`;
      await this.execute(cypher, { id, ...properties });
    },

    async health(): Promise<HealthResponse> {
      const response = await this.server.app.request('/health');
      return response.json() as Promise<HealthResponse>;
    },

    close(): void {
      this.server.dbManager.closeAll();
    },
  };

  return testClient as TestClient;
}

// Default export for convenience
export default NiceFoxGraphDB;
