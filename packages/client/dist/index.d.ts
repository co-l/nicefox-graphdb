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
export declare class NiceFoxGraphDB {
    private url;
    private project;
    private env;
    private apiKey?;
    constructor(options: ClientOptions);
    /**
     * Execute a Cypher query and return the data array.
     * Throws GraphDBError if the query fails.
     */
    query<T = Record<string, unknown>>(cypher: string, params?: Record<string, unknown>): Promise<T[]>;
    /**
     * Execute a Cypher query and return the full response including metadata.
     * Throws GraphDBError if the query fails.
     */
    queryRaw<T = Record<string, unknown>>(cypher: string, params?: Record<string, unknown>): Promise<QueryResponse<T>>;
    /**
     * Execute a mutating query (CREATE, SET, DELETE, MERGE) without expecting return data.
     * Throws GraphDBError if the query fails.
     */
    execute(cypher: string, params?: Record<string, unknown>): Promise<void>;
    /**
     * Create a node with the given label and properties.
     * Returns the generated node ID.
     */
    createNode(label: string, properties?: Record<string, unknown>): Promise<string>;
    /**
     * Create an edge between two nodes.
     */
    createEdge(sourceId: string, type: string, targetId: string, properties?: Record<string, unknown>): Promise<void>;
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
     * Check server health.
     */
    health(): Promise<HealthResponse>;
}
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
export declare function createTestClient(options?: TestClientOptions): Promise<TestClient>;
export default NiceFoxGraphDB;
//# sourceMappingURL=index.d.ts.map