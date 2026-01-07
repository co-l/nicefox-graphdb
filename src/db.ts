// Database Wrapper for SQLite

import Database from "better-sqlite3";

// ============================================================================
// Types
// ============================================================================

export interface NodeRow {
  id: string;
  label: string;
  properties: string; // JSON string
}

export interface EdgeRow {
  id: string;
  type: string;
  source_id: string;
  target_id: string;
  properties: string; // JSON string
}

export interface Node {
  id: string;
  label: string | string[]; // Support both single and multiple labels
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
// Helpers
// ============================================================================

/**
 * Convert a parameter value for SQLite binding.
 * Large integers (outside JavaScript's safe integer range) are converted to BigInt
 * to ensure SQLite treats them as INTEGER rather than REAL (which loses precision).
 * 
 * Important: We convert via string representation to preserve the value that JavaScript
 * would serialize (e.g., to JSON), rather than the internal floating-point representation
 * which may differ for large integers.
 */
function convertParamForSqlite(value: unknown): unknown {
  if (typeof value === "number" && Number.isInteger(value) && !Number.isSafeInteger(value)) {
    // Large integer: convert to BigInt via string to preserve the serialized representation
    // This ensures consistency with JSON.stringify() behavior
    return BigInt(String(value));
  }
  return value;
}

/**
 * Convert all params in an array for SQLite binding.
 */
function convertParamsForSqlite(params: unknown[]): unknown[] {
  return params.map(convertParamForSqlite);
}

// ============================================================================
// Database Class
// ============================================================================

// ============================================================================
// Custom SQL Functions for Cypher Semantics
// ============================================================================

/**
 * Deep equality comparison with Cypher's three-valued logic.
 * Returns: 1 (true), 0 (false), or null (unknown when comparing with null)
 */
function deepCypherEquals(a: unknown, b: unknown): number | null {
  // Both null/undefined -> null (unknown if null equals null)
  if (a === null && b === null) return null;
  // One null -> null (unknown)
  if (a === null || b === null) return null;

  // Arrays
  if (Array.isArray(a) && Array.isArray(b)) {
    if (a.length !== b.length) return 0; // false
    if (a.length === 0) return 1; // true

    let hasNull = false;
    for (let i = 0; i < a.length; i++) {
      const cmp = deepCypherEquals(a[i], b[i]);
      if (cmp === null) hasNull = true;
      else if (cmp === 0) return 0; // false
    }
    return hasNull ? null : 1;
  }

  // Objects (maps)
  if (typeof a === "object" && typeof b === "object" && a !== null && b !== null && !Array.isArray(a) && !Array.isArray(b)) {
    const keysA = Object.keys(a as Record<string, unknown>).sort();
    const keysB = Object.keys(b as Record<string, unknown>).sort();
    if (keysA.length !== keysB.length) return 0;
    if (keysA.join(",") !== keysB.join(",")) return 0;

    let hasNull = false;
    for (const k of keysA) {
      const cmp = deepCypherEquals((a as Record<string, unknown>)[k], (b as Record<string, unknown>)[k]);
      if (cmp === null) hasNull = true;
      else if (cmp === 0) return 0;
    }
    return hasNull ? null : 1;
  }

  // Primitives
  return a === b ? 1 : 0;
}

/**
 * Register custom SQL functions for Cypher semantics on a database instance.
 */
function registerCypherFunctions(db: Database.Database): void {
  // cypher_equals: Null-aware deep equality for lists and maps
  db.function("cypher_equals", { deterministic: true }, (a: unknown, b: unknown) => {
    // Handle SQL NULL
    if (a === null && b === null) return null;
    if (a === null || b === null) return null;

    // Try to parse as JSON (for arrays/objects stored as JSON strings)
    let parsedA: unknown, parsedB: unknown;
    try {
      parsedA = typeof a === "string" ? JSON.parse(a) : a;
    } catch {
      parsedA = a;
    }
    try {
      parsedB = typeof b === "string" ? JSON.parse(b) : b;
    } catch {
      parsedB = b;
    }

    return deepCypherEquals(parsedA, parsedB);
  });
}

export class GraphDatabase {
  private db: Database.Database;
  private initialized: boolean = false;

  constructor(path: string = ":memory:") {
    this.db = new Database(path);
    this.db.pragma("journal_mode = WAL");
    this.db.pragma("foreign_keys = ON");
    // Register custom Cypher functions
    registerCypherFunctions(this.db);
  }

  /**
   * Initialize the database schema
   */
  initialize(): void {
    if (this.initialized) return;

    this.db.exec(SCHEMA);
    this.initialized = true;
  }

  /**
   * Execute a SQL statement and return results
   */
  execute(sql: string, params: unknown[] = []): QueryResult {
    this.ensureInitialized();

    // Convert large integers to BigInt for proper SQLite INTEGER binding
    const convertedParams = convertParamsForSqlite(params);

    const stmt = this.db.prepare(sql);
    const trimmedSql = sql.trim().toUpperCase();
    // Check if it's a query (SELECT or WITH for CTEs)
    const isQuery = trimmedSql.startsWith("SELECT") || trimmedSql.startsWith("WITH");

    if (isQuery) {
      const rows = stmt.all(...convertedParams) as Record<string, unknown>[];
      return { rows, changes: 0, lastInsertRowid: 0 };
    } else {
      const result = stmt.run(...convertedParams);
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
  transaction<T>(fn: () => T): T {
    this.ensureInitialized();
    return this.db.transaction(fn)();
  }

  /**
   * Insert a node
   */
  insertNode(id: string, label: string | string[], properties: Record<string, unknown> = {}): void {
    // Normalize label to array format for storage
    const labelArray = Array.isArray(label) ? label : [label];
    this.execute(
      "INSERT INTO nodes (id, label, properties) VALUES (?, ?, ?)",
      [id, JSON.stringify(labelArray), JSON.stringify(properties)]
    );
  }

  /**
   * Insert an edge
   */
  insertEdge(
    id: string,
    type: string,
    sourceId: string,
    targetId: string,
    properties: Record<string, unknown> = {}
  ): void {
    this.execute(
      "INSERT INTO edges (id, type, source_id, target_id, properties) VALUES (?, ?, ?, ?, ?)",
      [id, type, sourceId, targetId, JSON.stringify(properties)]
    );
  }

  /**
   * Get a node by ID
   */
  getNode(id: string): Node | null {
    const result = this.execute("SELECT * FROM nodes WHERE id = ?", [id]);
    if (result.rows.length === 0) return null;

    const row = result.rows[0] as unknown as NodeRow;
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
  getEdge(id: string): Edge | null {
    const result = this.execute("SELECT * FROM edges WHERE id = ?", [id]);
    if (result.rows.length === 0) return null;

    const row = result.rows[0] as unknown as EdgeRow;
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
  getNodesByLabel(label: string): Node[] {
    const result = this.execute(
      "SELECT * FROM nodes WHERE EXISTS (SELECT 1 FROM json_each(label) WHERE value = ?)",
      [label]
    );
    return result.rows.map((row) => {
      const r = row as unknown as NodeRow;
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
  getEdgesByType(type: string): Edge[] {
    const result = this.execute("SELECT * FROM edges WHERE type = ?", [type]);
    return result.rows.map((row) => {
      const r = row as unknown as EdgeRow;
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
  deleteNode(id: string): boolean {
    const result = this.execute("DELETE FROM nodes WHERE id = ?", [id]);
    return result.changes > 0;
  }

  /**
   * Delete an edge by ID
   */
  deleteEdge(id: string): boolean {
    const result = this.execute("DELETE FROM edges WHERE id = ?", [id]);
    return result.changes > 0;
  }

  /**
   * Update node properties
   */
  updateNodeProperties(id: string, properties: Record<string, unknown>): boolean {
    const result = this.execute(
      "UPDATE nodes SET properties = ? WHERE id = ?",
      [JSON.stringify(properties), id]
    );
    return result.changes > 0;
  }

  /**
   * Count nodes
   */
  countNodes(): number {
    const result = this.execute("SELECT COUNT(*) as count FROM nodes");
    return (result.rows[0] as { count: number }).count;
  }

  /**
   * Count edges
   */
  countEdges(): number {
    const result = this.execute("SELECT COUNT(*) as count FROM edges");
    return (result.rows[0] as { count: number }).count;
  }

  /**
   * Close the database connection
   */
  close(): void {
    this.db.close();
  }

  /**
   * Get the underlying database instance (for advanced operations)
   */
  getRawDatabase(): Database.Database {
    return this.db;
  }

  private ensureInitialized(): void {
    if (!this.initialized) {
      this.initialize();
    }
  }
}

// ============================================================================
// Database Manager (for multi-project support)
// ============================================================================

export class DatabaseManager {
  private databases: Map<string, GraphDatabase> = new Map();
  private basePath: string;

  constructor(basePath: string = ":memory:") {
    this.basePath = basePath;
  }

  /**
   * Get or create a database for a project/environment
   */
  getDatabase(project: string, env: string = "production"): GraphDatabase {
    const key = `${env}/${project}`;

    if (!this.databases.has(key)) {
      const path = this.basePath === ":memory:" 
        ? ":memory:" 
        : `${this.basePath}/${env}/${project}.db`;
      
      const db = new GraphDatabase(path);
      db.initialize();
      this.databases.set(key, db);
    }

    return this.databases.get(key)!;
  }

  /**
   * Close all database connections
   */
  closeAll(): void {
    for (const db of this.databases.values()) {
      db.close();
    }
    this.databases.clear();
  }

  /**
   * List all open databases
   */
  listDatabases(): string[] {
    return Array.from(this.databases.keys());
  }
}
