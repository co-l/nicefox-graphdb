/**
 * Options for creating a GraphDB client.
 */
export interface GraphDBOptions {
    /**
     * Base URL of the GraphDB server.
     * Used in production mode. Ignored in development mode.
     * @example 'https://my-graphdb.example.com'
     */
    url: string;
    /**
     * Project name.
     * In production: used as part of the API endpoint path.
     * In development: used as the database filename.
     */
    project: string;
    /**
     * API key for authentication.
     * Used in production mode. Ignored in development mode.
     */
    apiKey?: string;
    /**
     * Environment: 'production' or 'test'.
     * @default 'production'
     */
    env?: "production" | "test";
    /**
     * Path for local data storage.
     * Only used in development mode (when NODE_ENV=development).
     * - Default: './data'
     * - Use ':memory:' for an in-memory database (resets on restart)
     * @default './data'
     */
    dataPath?: string;
}
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
/**
 * GraphDB client interface.
 * Both local and remote clients implement this interface.
 */
export interface GraphDBClient {
    /**
     * Execute a Cypher query and return the data array.
     * @throws GraphDBError if the query fails
     */
    query<T = Record<string, unknown>>(cypher: string, params?: Record<string, unknown>): Promise<T[]>;
    /**
     * Execute a Cypher query and return the full response including metadata.
     * @throws GraphDBError if the query fails
     */
    queryRaw<T = Record<string, unknown>>(cypher: string, params?: Record<string, unknown>): Promise<QueryResponse<T>>;
    /**
     * Execute a mutating query (CREATE, SET, DELETE, MERGE) without expecting return data.
     * @throws GraphDBError if the query fails
     */
    execute(cypher: string, params?: Record<string, unknown>): Promise<void>;
    /**
     * Create a node with the given label and properties.
     * @returns The generated node ID
     */
    createNode(label: string, properties?: Record<string, unknown>): Promise<string>;
    /**
     * Create an edge between two nodes.
     */
    createEdge(sourceId: string, type: string, targetId: string, properties?: Record<string, unknown>): Promise<void>;
    /**
     * Get a node by label and property filter.
     * @returns The node's properties, or null if not found
     */
    getNode(label: string, filter: Record<string, unknown>): Promise<NodeResult | null>;
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
/**
 * Error thrown by GraphDB operations.
 * Contains optional position information for Cypher parse errors.
 */
export declare class GraphDBError extends Error {
    readonly position?: number;
    readonly line?: number;
    readonly column?: number;
    constructor(message: string, options?: {
        position?: number;
        line?: number;
        column?: number;
    });
}
//# sourceMappingURL=types.d.ts.map