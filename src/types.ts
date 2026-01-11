// LeanGraph - Shared Types

// ============================================================================
// Configuration Options
// ============================================================================

/**
 * Options for creating a LeanGraph client.
 */
export interface LeanGraphOptions {
  /**
   * Connection mode.
   * - local: Embedded SQLite, persisted to disk
   * - remote: HTTP connection to server
   * - test: Embedded SQLite, in-memory (resets on restart)
   * @default LEANGRAPH_MODE env var or 'local'
   */
  mode?: "local" | "remote" | "test";

  /**
   * Base URL of the LeanGraph server.
   * Only used in remote mode.
   * @default LEANGRAPH_URL env var or 'https://leangraph.io'
   */
  url?: string;

  /**
   * Project name. Used as the database filename in local mode.
   * @default LEANGRAPH_PROJECT env var (required)
   */
  project?: string;

  /**
   * API key for authentication.
   * Only used in remote mode.
   * @default LEANGRAPH_API_KEY env var
   */
  apiKey?: string;

  /**
   * Path for local data storage.
   * Only used in local mode. Database stored at {dataPath}/{project}.db
   * @default LEANGRAPH_DATA_PATH env var or './data'
   */
  dataPath?: string;
}

// ============================================================================
// Response Types
// ============================================================================

/**
 * Full response from a query, including metadata.
 */
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

/**
 * Health check response.
 */
export interface HealthResponse {
  status: string;
  timestamp: string;
}

/**
 * Result type for node queries.
 * In Neo4j 3.5 format, nodes return their properties directly.
 */
export type NodeResult = Record<string, unknown>;

// ============================================================================
// Client Interface
// ============================================================================

/**
 * GraphDB client interface.
 * Both local and remote clients implement this interface.
 */
export interface LeanGraphClient {
  /**
   * Execute a Cypher query and return the data array.
   * @throws LeanGraphError if the query fails
   */
  query<T = Record<string, unknown>>(
    cypher: string,
    params?: Record<string, unknown>
  ): Promise<T[]>;

  /**
   * Execute a Cypher query and return the full response including metadata.
   * @throws LeanGraphError if the query fails
   */
  queryRaw<T = Record<string, unknown>>(
    cypher: string,
    params?: Record<string, unknown>
  ): Promise<QueryResponse<T>>;

  /**
   * Execute a mutating query (CREATE, SET, DELETE, MERGE) without expecting return data.
   * @throws LeanGraphError if the query fails
   */
  execute(cypher: string, params?: Record<string, unknown>): Promise<void>;

  /**
   * Create a node with the given label and properties.
   * @returns The generated node ID
   */
  createNode(
    label: string,
    properties?: Record<string, unknown>
  ): Promise<string>;

  /**
   * Create an edge between two nodes.
   */
  createEdge(
    sourceId: string,
    type: string,
    targetId: string,
    properties?: Record<string, unknown>
  ): Promise<void>;

  /**
   * Get a node by label and property filter.
   * @returns The node's properties, or null if not found
   */
  getNode(
    label: string,
    filter: Record<string, unknown>
  ): Promise<NodeResult | null>;

  /**
   * Delete a node by ID (with DETACH to remove connected edges).
   */
  deleteNode(id: string): Promise<void>;

  /**
   * Update properties on a node.
   */
  updateNode(id: string, properties: Record<string, unknown>): Promise<void>;

  /**
   * Check health.
   * Local: always returns ok.
   * Remote: calls the server health endpoint.
   */
  health(): Promise<HealthResponse>;

  /**
   * Close the client and release resources.
   * Important: Always call this when done to prevent resource leaks.
   */
  close(): void;
}

// ============================================================================
// Error Class
// ============================================================================

/**
 * Error thrown by GraphDB operations.
 * Contains optional position information for Cypher parse errors.
 */
export class LeanGraphError extends Error {
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
    this.name = "LeanGraphError";
    this.position = options?.position;
    this.line = options?.line;
    this.column = options?.column;
  }
}
