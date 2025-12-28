import Database from "better-sqlite3";
export interface NodeRow {
    id: string;
    label: string;
    properties: string;
}
export interface EdgeRow {
    id: string;
    type: string;
    source_id: string;
    target_id: string;
    properties: string;
}
export interface Node {
    id: string;
    label: string | string[];
    properties: Record<string, unknown>;
}
export interface Edge {
    id: string;
    type: string;
    source_id: string;
    target_id: string;
    properties: Record<string, unknown>;
}
export interface QueryResult {
    rows: Record<string, unknown>[];
    changes: number;
    lastInsertRowid: number | bigint;
}
export declare class GraphDatabase {
    private db;
    private initialized;
    constructor(path?: string);
    /**
     * Initialize the database schema
     */
    initialize(): void;
    /**
     * Execute a SQL statement and return results
     */
    execute(sql: string, params?: unknown[]): QueryResult;
    /**
     * Execute multiple statements in a transaction
     */
    transaction<T>(fn: () => T): T;
    /**
     * Insert a node
     */
    insertNode(id: string, label: string | string[], properties?: Record<string, unknown>): void;
    /**
     * Insert an edge
     */
    insertEdge(id: string, type: string, sourceId: string, targetId: string, properties?: Record<string, unknown>): void;
    /**
     * Get a node by ID
     */
    getNode(id: string): Node | null;
    /**
     * Get an edge by ID
     */
    getEdge(id: string): Edge | null;
    /**
     * Get all nodes with a given label
     */
    getNodesByLabel(label: string): Node[];
    /**
     * Get all edges with a given type
     */
    getEdgesByType(type: string): Edge[];
    /**
     * Delete a node by ID
     */
    deleteNode(id: string): boolean;
    /**
     * Delete an edge by ID
     */
    deleteEdge(id: string): boolean;
    /**
     * Update node properties
     */
    updateNodeProperties(id: string, properties: Record<string, unknown>): boolean;
    /**
     * Count nodes
     */
    countNodes(): number;
    /**
     * Count edges
     */
    countEdges(): number;
    /**
     * Close the database connection
     */
    close(): void;
    /**
     * Get the underlying database instance (for advanced operations)
     */
    getRawDatabase(): Database.Database;
    private ensureInitialized;
}
export declare class DatabaseManager {
    private databases;
    private basePath;
    constructor(basePath?: string);
    /**
     * Get or create a database for a project/environment
     */
    getDatabase(project: string, env?: string): GraphDatabase;
    /**
     * Close all database connections
     */
    closeAll(): void;
    /**
     * List all open databases
     */
    listDatabases(): string[];
}
//# sourceMappingURL=db.d.ts.map