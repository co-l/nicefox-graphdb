// Database Wrapper for SQLite
import Database from "better-sqlite3";
// ============================================================================
// Schema
// ============================================================================
const SCHEMA = `
CREATE TABLE IF NOT EXISTS nodes (
    id TEXT PRIMARY KEY,
    label JSON NOT NULL,
    properties JSON DEFAULT '{}'
);

CREATE TABLE IF NOT EXISTS edges (
    id TEXT PRIMARY KEY,
    type TEXT NOT NULL,
    source_id TEXT NOT NULL,
    target_id TEXT NOT NULL,
    properties JSON DEFAULT '{}',
    FOREIGN KEY (source_id) REFERENCES nodes(id) ON DELETE CASCADE,
    FOREIGN KEY (target_id) REFERENCES nodes(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_edges_type ON edges(type);
CREATE INDEX IF NOT EXISTS idx_edges_source ON edges(source_id);
CREATE INDEX IF NOT EXISTS idx_edges_target ON edges(target_id);
`;
// ============================================================================
// Database Class
// ============================================================================
export class GraphDatabase {
    db;
    initialized = false;
    constructor(path = ":memory:") {
        this.db = new Database(path);
        this.db.pragma("journal_mode = WAL");
        this.db.pragma("foreign_keys = ON");
    }
    /**
     * Initialize the database schema
     */
    initialize() {
        if (this.initialized)
            return;
        this.db.exec(SCHEMA);
        this.initialized = true;
    }
    /**
     * Execute a SQL statement and return results
     */
    execute(sql, params = []) {
        this.ensureInitialized();
        const stmt = this.db.prepare(sql);
        const trimmedSql = sql.trim().toUpperCase();
        // Check if it's a query (SELECT or WITH for CTEs)
        const isQuery = trimmedSql.startsWith("SELECT") || trimmedSql.startsWith("WITH");
        if (isQuery) {
            const rows = stmt.all(...params);
            return { rows, changes: 0, lastInsertRowid: 0 };
        }
        else {
            const result = stmt.run(...params);
            return {
                rows: [],
                changes: result.changes,
                lastInsertRowid: result.lastInsertRowid,
            };
        }
    }
    /**
     * Execute multiple statements in a transaction
     */
    transaction(fn) {
        this.ensureInitialized();
        return this.db.transaction(fn)();
    }
    /**
     * Insert a node
     */
    insertNode(id, label, properties = {}) {
        // Normalize label to array format for storage
        const labelArray = Array.isArray(label) ? label : [label];
        this.execute("INSERT INTO nodes (id, label, properties) VALUES (?, ?, ?)", [id, JSON.stringify(labelArray), JSON.stringify(properties)]);
    }
    /**
     * Insert an edge
     */
    insertEdge(id, type, sourceId, targetId, properties = {}) {
        this.execute("INSERT INTO edges (id, type, source_id, target_id, properties) VALUES (?, ?, ?, ?, ?)", [id, type, sourceId, targetId, JSON.stringify(properties)]);
    }
    /**
     * Get a node by ID
     */
    getNode(id) {
        const result = this.execute("SELECT * FROM nodes WHERE id = ?", [id]);
        if (result.rows.length === 0)
            return null;
        const row = result.rows[0];
        const labelArray = JSON.parse(row.label);
        return {
            id: row.id,
            label: labelArray,
            properties: JSON.parse(row.properties),
        };
    }
    /**
     * Get an edge by ID
     */
    getEdge(id) {
        const result = this.execute("SELECT * FROM edges WHERE id = ?", [id]);
        if (result.rows.length === 0)
            return null;
        const row = result.rows[0];
        return {
            id: row.id,
            type: row.type,
            source_id: row.source_id,
            target_id: row.target_id,
            properties: JSON.parse(row.properties),
        };
    }
    /**
     * Get all nodes with a given label
     */
    getNodesByLabel(label) {
        const result = this.execute("SELECT * FROM nodes WHERE EXISTS (SELECT 1 FROM json_each(label) WHERE value = ?)", [label]);
        return result.rows.map((row) => {
            const r = row;
            const labelArray = JSON.parse(r.label);
            return {
                id: r.id,
                label: labelArray,
                properties: JSON.parse(r.properties),
            };
        });
    }
    /**
     * Get all edges with a given type
     */
    getEdgesByType(type) {
        const result = this.execute("SELECT * FROM edges WHERE type = ?", [type]);
        return result.rows.map((row) => {
            const r = row;
            return {
                id: r.id,
                type: r.type,
                source_id: r.source_id,
                target_id: r.target_id,
                properties: JSON.parse(r.properties),
            };
        });
    }
    /**
     * Delete a node by ID
     */
    deleteNode(id) {
        const result = this.execute("DELETE FROM nodes WHERE id = ?", [id]);
        return result.changes > 0;
    }
    /**
     * Delete an edge by ID
     */
    deleteEdge(id) {
        const result = this.execute("DELETE FROM edges WHERE id = ?", [id]);
        return result.changes > 0;
    }
    /**
     * Update node properties
     */
    updateNodeProperties(id, properties) {
        const result = this.execute("UPDATE nodes SET properties = ? WHERE id = ?", [JSON.stringify(properties), id]);
        return result.changes > 0;
    }
    /**
     * Count nodes
     */
    countNodes() {
        const result = this.execute("SELECT COUNT(*) as count FROM nodes");
        return result.rows[0].count;
    }
    /**
     * Count edges
     */
    countEdges() {
        const result = this.execute("SELECT COUNT(*) as count FROM edges");
        return result.rows[0].count;
    }
    /**
     * Close the database connection
     */
    close() {
        this.db.close();
    }
    /**
     * Get the underlying database instance (for advanced operations)
     */
    getRawDatabase() {
        return this.db;
    }
    ensureInitialized() {
        if (!this.initialized) {
            this.initialize();
        }
    }
}
// ============================================================================
// Database Manager (for multi-project support)
// ============================================================================
export class DatabaseManager {
    databases = new Map();
    basePath;
    constructor(basePath = ":memory:") {
        this.basePath = basePath;
    }
    /**
     * Get or create a database for a project/environment
     */
    getDatabase(project, env = "production") {
        const key = `${env}/${project}`;
        if (!this.databases.has(key)) {
            const path = this.basePath === ":memory:"
                ? ":memory:"
                : `${this.basePath}/${env}/${project}.db`;
            const db = new GraphDatabase(path);
            db.initialize();
            this.databases.set(key, db);
        }
        return this.databases.get(key);
    }
    /**
     * Close all database connections
     */
    closeAll() {
        for (const db of this.databases.values()) {
            db.close();
        }
        this.databases.clear();
    }
    /**
     * List all open databases
     */
    listDatabases() {
        return Array.from(this.databases.keys());
    }
}
//# sourceMappingURL=db.js.map