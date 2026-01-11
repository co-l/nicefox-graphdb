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
CREATE INDEX IF NOT EXISTS idx_edges_source_type ON edges(source_id, type);
CREATE INDEX IF NOT EXISTS idx_edges_target_type ON edges(target_id, type);
CREATE INDEX IF NOT EXISTS idx_nodes_primary_label ON nodes(json_extract(label, '$[0]'));
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
 * Get the Cypher type category for ordering comparisons.
 * Returns a type string for values that can be ordered, or null for non-orderable types.
 * 
 * When using SQLite's -> operator for JSON extraction, values come as JSON-formatted strings:
 * - 'true' / 'false' for booleans
 * - '"string"' for strings (with quotes)
 * - '123' or '3.14' for numbers (no quotes)
 * - '[...]' for arrays
 * - '{...}' for objects
 */
// Regex patterns for temporal types
const TIME_PATTERN = /^\d{2}:\d{2}(:\d{2})?(\.\d+)?(Z|[+-]\d{2}:\d{2})$/;
const LOCALTIME_PATTERN = /^\d{2}:\d{2}(:\d{2})?(\.\d+)?$/;
const DATE_PATTERN = /^\d{4}-\d{2}-\d{2}$/;
const DATETIME_PATTERN = /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}(:\d{2})?(\.\d+)?(Z|[+-]\d{2}:\d{2})?(\[.+\])?$/;
const LOCALDATETIME_PATTERN = /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}(:\d{2})?(\.\d+)?$/;

function getCypherTypeForOrdering(value: unknown): string | null {
  if (value === null) return null;
  
  const jsType = typeof value;
  
  // Numbers (integer and real) are in the same ordering category
  if (jsType === "number" || jsType === "bigint") return "number";
  
  // Strings - could be raw strings OR JSON-formatted values from -> operator
  if (jsType === "string") {
    const s = value as string;
    
    // Check for JSON boolean literals (from -> operator)
    if (s === "true" || s === "false") return "boolean";
    
    // Check for JSON null
    if (s === "null") return null;
    
    // Check for JSON array
    if (s.startsWith("[") && s.endsWith("]")) {
      try {
        JSON.parse(s);
        return "array"; // arrays are not orderable
      } catch {
        // Not valid JSON, treat as string
      }
    }
    
    // Check for JSON object  
    if (s.startsWith("{") && s.endsWith("}")) {
      try {
        JSON.parse(s);
        return "object"; // objects are not orderable
      } catch {
        // Not valid JSON, treat as string
      }
    }
    
    // Check for JSON string literal (starts and ends with quotes)
    if (s.startsWith('"') && s.endsWith('"') && s.length >= 2) {
      return "string";
    }
    
    // Check for JSON number (no quotes, valid number)
    if (/^-?\d+(\.\d+)?([eE][+-]?\d+)?$/.test(s)) {
      return "number";
    }
    
    // Check for temporal types (times with timezone need special comparison)
    if (TIME_PATTERN.test(s)) return "time";
    if (DATETIME_PATTERN.test(s)) return "datetime";
    if (DATE_PATTERN.test(s)) return "date";
    if (LOCALTIME_PATTERN.test(s)) return "localtime";
    if (LOCALDATETIME_PATTERN.test(s)) return "localdatetime";
    
    // Otherwise treat as a plain string
    return "string";
  }
  
  // Booleans - SQLite stores these as integers, but if we somehow get a JS boolean
  if (jsType === "boolean") return "boolean";
  
  // Objects and arrays in JS form (shouldn't normally happen with SQLite)
  if (Array.isArray(value)) return "array";
  if (jsType === "object") return "object";
  
  return null;
}

/**
 * Check if two Cypher types are compatible for ordering comparisons (<, <=, >, >=).
 */
function areCypherTypesOrderable(typeA: string | null, typeB: string | null): boolean {
  if (typeA === null || typeB === null) return false;
  
  // Arrays, objects, nodes, relationships are not orderable
  if (typeA === "array" || typeB === "array") return false;
  if (typeA === "object" || typeB === "object") return false;
  
  // Same type is always orderable
  if (typeA === typeB) return true;
  
  // Numbers (integer/real) are orderable with each other - already handled by "number" category
  
  return false;
}

/**
 * Parse timezone offset to minutes from UTC
 */
function parseTimezoneOffset(tz: string): number {
  if (tz === "Z" || tz === "+00:00") return 0;
  const sign = tz[0] === "-" ? -1 : 1;
  const hours = parseInt(tz.slice(1, 3), 10);
  const minutes = parseInt(tz.slice(4, 6), 10);
  return sign * (hours * 60 + minutes);
}

/**
 * Convert time string to nanoseconds from midnight UTC for comparison
 */
function timeToNanosUTC(timeStr: string): number {
  // Format: HH:MM or HH:MM:SS or HH:MM:SS.nnnnnnnnn followed by Z or +HH:MM or -HH:MM
  // May also have [timezone] suffix - strip it
  const withoutTzName = timeStr.replace(/\[.+\]$/, "");
  
  // Find timezone part
  let tzOffset = 0;
  let timePart = withoutTzName;
  
  if (withoutTzName.endsWith("Z")) {
    timePart = withoutTzName.slice(0, -1);
    tzOffset = 0;
  } else {
    const tzMatch = withoutTzName.match(/([+-]\d{2}:\d{2})$/);
    if (tzMatch) {
      tzOffset = parseTimezoneOffset(tzMatch[1]);
      timePart = withoutTzName.slice(0, -6);
    }
  }
  
  // Parse time components
  const parts = timePart.split(":");
  const hours = parseInt(parts[0], 10);
  const minutes = parseInt(parts[1], 10);
  let seconds = 0;
  let nanos = 0;
  
  if (parts[2]) {
    const secParts = parts[2].split(".");
    seconds = parseInt(secParts[0], 10);
    if (secParts[1]) {
      // Pad or truncate to 9 digits
      const fracStr = secParts[1].padEnd(9, "0").slice(0, 9);
      nanos = parseInt(fracStr, 10);
    }
  }
  
  // Convert to total nanoseconds from midnight, then adjust for timezone
  const totalMinutes = hours * 60 + minutes - tzOffset;
  const totalNanos = (totalMinutes * 60 + seconds) * 1_000_000_000 + nanos;
  
  // Normalize to 24-hour range (handle negative from timezone adjustment)
  const dayInNanos = 24 * 60 * 60 * 1_000_000_000;
  return ((totalNanos % dayInNanos) + dayInNanos) % dayInNanos;
}

/**
 * Convert a value to its comparable form.
 * For JSON-formatted strings from -> operator, parse to get actual value.
 * For temporal types, convert to a form suitable for comparison.
 */
function toComparableValue(value: unknown, type: string): number | string | boolean {
  if (typeof value === "string") {
    if (type === "number") {
      return parseFloat(value);
    }
    if (type === "boolean") {
      return value === "true";
    }
    if (type === "string") {
      // JSON string literal - remove outer quotes
      if (value.startsWith('"') && value.endsWith('"')) {
        return value.slice(1, -1);
      }
      return value;
    }
    if (type === "time") {
      // Convert to nanoseconds from midnight UTC for comparison
      return timeToNanosUTC(value);
    }
    if (type === "datetime") {
      // Strip [timezone] suffix and compare lexically (ISO format is naturally sortable when in UTC or same TZ)
      // For proper comparison, we'd need to convert to UTC, but for same-offset datetimes, lexical works
      // TODO: Full timezone-aware datetime comparison
      return value.replace(/\[.+\]$/, "");
    }
    // date, localtime, localdatetime can be compared lexically (ISO format is sortable)
    if (type === "date" || type === "localtime" || type === "localdatetime") {
      return value;
    }
  }
  return value as number | string | boolean;
}

/**
 * Helper to convert SQLite boolean representation to JavaScript boolean.
 * SQLite can represent booleans as: 1, 0, 'true', 'false'
 */
function toBoolValue(x: unknown): boolean | null {
  if (x === null || x === undefined) return null;
  if (x === 1 || x === true || x === 'true') return true;
  if (x === 0 || x === false || x === 'false') return false;
  return null;
}

/**
 * Register custom SQL functions for Cypher semantics on a database instance.
 */
function registerCypherFunctions(db: Database.Database): void {
  // cypher_not: Proper boolean negation that works with both JSON booleans and integers
  // Converts json('true')/1 -> 0, json('false')/0 -> 1, null -> null
  // Returns integers for SQLite compatibility in WHERE clauses
  db.function("cypher_not", { deterministic: true }, (x: unknown) => {
    const b = toBoolValue(x);
    if (b === null) return null;
    return b ? 0 : 1;
  });
  
  // cypher_and: Proper boolean AND that works with both JSON booleans and integers
  // Returns integers for SQLite compatibility in WHERE clauses
  db.function("cypher_and", { deterministic: true }, (a: unknown, b: unknown) => {
    const boolA = toBoolValue(a);
    const boolB = toBoolValue(b);
    // Cypher AND with NULL: false AND NULL = false, true AND NULL = NULL
    if (boolA === false || boolB === false) return 0;
    if (boolA === null || boolB === null) return null;
    return boolA && boolB ? 1 : 0;
  });
  
  // cypher_or: Proper boolean OR that works with both JSON booleans and integers
  // Returns integers for SQLite compatibility in WHERE clauses
  db.function("cypher_or", { deterministic: true }, (a: unknown, b: unknown) => {
    const boolA = toBoolValue(a);
    const boolB = toBoolValue(b);
    // Cypher OR with NULL: true OR NULL = true, false OR NULL = NULL
    if (boolA === true || boolB === true) return 1;
    if (boolA === null || boolB === null) return null;
    return boolA || boolB ? 1 : 0;
  });

  // cypher_compare: Type-aware comparison for ordering operators (<, <=, >, >=)
  // Returns: 1 if condition is true, 0 if false, null if types are incompatible
  db.function("cypher_lt", { deterministic: true }, (a: unknown, b: unknown) => {
    if (a === null || a === undefined || b === null || b === undefined) return null;
    const typeA = getCypherTypeForOrdering(a);
    const typeB = getCypherTypeForOrdering(b);
    if (!areCypherTypesOrderable(typeA, typeB)) return null;
    const valA = toComparableValue(a, typeA!);
    const valB = toComparableValue(b, typeB!);
    return valA < valB ? 1 : 0;
  });
  
  db.function("cypher_lte", { deterministic: true }, (a: unknown, b: unknown) => {
    if (a === null || a === undefined || b === null || b === undefined) return null;
    const typeA = getCypherTypeForOrdering(a);
    const typeB = getCypherTypeForOrdering(b);
    if (!areCypherTypesOrderable(typeA, typeB)) return null;
    const valA = toComparableValue(a, typeA!);
    const valB = toComparableValue(b, typeB!);
    return valA <= valB ? 1 : 0;
  });
  
  db.function("cypher_gt", { deterministic: true }, (a: unknown, b: unknown) => {
    if (a === null || a === undefined || b === null || b === undefined) return null;
    const typeA = getCypherTypeForOrdering(a);
    const typeB = getCypherTypeForOrdering(b);
    if (!areCypherTypesOrderable(typeA, typeB)) return null;
    const valA = toComparableValue(a, typeA!);
    const valB = toComparableValue(b, typeB!);
    return valA > valB ? 1 : 0;
  });
  
  db.function("cypher_gte", { deterministic: true }, (a: unknown, b: unknown) => {
    if (a === null || a === undefined || b === null || b === undefined) return null;
    const typeA = getCypherTypeForOrdering(a);
    const typeB = getCypherTypeForOrdering(b);
    if (!areCypherTypesOrderable(typeA, typeB)) return null;
    const valA = toComparableValue(a, typeA!);
    const valB = toComparableValue(b, typeB!);
    return valA >= valB ? 1 : 0;
  });

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
  
  // cypher_case_eq: Type-aware equality for CASE expressions
  // Takes value+type pairs to preserve type information across SQLite's type coercion
  // Returns: 1 if equal (same type and value), 0 if not equal
  db.function("cypher_case_eq", { deterministic: true }, (val1: unknown, type1: string, val2: unknown, type2: string) => {
    // NULL handling: if either is null, return null (unknown)
    if (val1 === null || val2 === null) return null;
    if (type1 === "null" || type2 === "null") return null;
    
    // Helper to get runtime type from a value
    const getRuntimeType = (val: unknown): string => {
      if (val === null) return "null";
      const jsType = typeof val;
      if (jsType === "boolean" || val === 0 || val === 1) {
        // SQLite stores booleans as 0/1, so we can't distinguish at runtime
        // We rely on the compile-time type info for this
        return "unknown_number_or_boolean";
      }
      if (jsType === "number" || jsType === "bigint") return "number";
      if (jsType === "string") {
        // Check if it's a JSON array/object
        const str = val as string;
        if (str.startsWith("[")) return "list";
        if (str.startsWith("{")) return "map";
        return "string";
      }
      return "unknown";
    };
    
    // Resolve "dynamic" types using runtime type detection
    let resolvedType1 = type1;
    let resolvedType2 = type2;
    
    if (type1 === "dynamic") {
      resolvedType1 = getRuntimeType(val1);
    }
    if (type2 === "dynamic") {
      resolvedType2 = getRuntimeType(val2);
    }
    
    // Normalize numeric types: integer, float, and number are all comparable
    const normalizeNumericType = (t: string): string => {
      if (t === "integer" || t === "float" || t === "number") return "numeric";
      return t;
    };
    
    const normType1 = normalizeNumericType(resolvedType1);
    const normType2 = normalizeNumericType(resolvedType2);
    
    // Different types are never equal in CASE expressions
    // (except numeric types which are comparable)
    if (normType1 !== normType2) return 0;
    
    // Same type - compare values
    // For lists/maps, use deep comparison
    if (normType1 === "list" || normType1 === "map") {
      let parsed1: unknown, parsed2: unknown;
      try {
        parsed1 = typeof val1 === "string" ? JSON.parse(val1) : val1;
      } catch {
        parsed1 = val1;
      }
      try {
        parsed2 = typeof val2 === "string" ? JSON.parse(val2) : val2;
      } catch {
        parsed2 = val2;
      }
      const result = deepCypherEquals(parsed1, parsed2);
      return result === null ? null : result;
    }
    
    // For numeric types, compare as numbers
    if (normType1 === "numeric") {
      const num1 = Number(val1);
      const num2 = Number(val2);
      return num1 === num2 ? 1 : 0;
    }
    
    // For primitives (boolean, string), direct comparison
    return val1 === val2 ? 1 : 0;
  });
}

export class GraphDatabase {
  private db: Database.Database;
  private initialized: boolean = false;
  private stmtCache: Map<string, Database.Statement> = new Map();
  private readonly STMT_CACHE_MAX = 100;

  constructor(path: string = ":memory:") {
    this.db = new Database(path);
    this.db.pragma("journal_mode = WAL");
    this.db.pragma("foreign_keys = ON");
    this.db.pragma("synchronous = NORMAL"); // Safe with WAL, faster writes
    this.db.pragma("cache_size = -64000"); // 64MB cache (default is 2MB)
    this.db.pragma("temp_store = MEMORY"); // Temp tables in RAM
    this.db.pragma("mmap_size = 268435456"); // 256MB memory-mapped I/O
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
   * Get a cached prepared statement, or create and cache a new one
   * Uses LRU eviction: recently accessed entries are moved to end of Map
   */
  private getCachedStatement(sql: string): Database.Statement {
    let stmt = this.stmtCache.get(sql);
    if (stmt) {
      // Move to end for LRU (delete and re-add)
      this.stmtCache.delete(sql);
      this.stmtCache.set(sql, stmt);
      return stmt;
    }
    
    // Not cached - prepare and add
    stmt = this.db.prepare(sql);
    if (this.stmtCache.size >= this.STMT_CACHE_MAX) {
      // Evict least recently used (first entry)
      const firstKey = this.stmtCache.keys().next().value;
      if (firstKey) this.stmtCache.delete(firstKey);
    }
    this.stmtCache.set(sql, stmt);
    return stmt;
  }

  /**
   * Execute a SQL statement and return results
   */
  execute(sql: string, params: unknown[] = []): QueryResult {
    this.ensureInitialized();

    // Convert large integers to BigInt for proper SQLite INTEGER binding
    const convertedParams = convertParamsForSqlite(params);

    const stmt = this.getCachedStatement(sql);
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
    // Use index for primary label, fallback for secondary labels
    const result = this.execute(
      `SELECT * FROM nodes WHERE json_extract(label, '$[0]') = ? OR EXISTS (SELECT 1 FROM json_each(label) WHERE value = ? AND json_extract(label, '$[0]') != ?)`,
      [label, label, label]
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
    this.stmtCache.clear();
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
   * Get or create a database for a project
   */
  getDatabase(project: string): GraphDatabase {
    if (!this.databases.has(project)) {
      const path =
        this.basePath === ":memory:"
          ? ":memory:"
          : `${this.basePath}/${project}.db`;

      const db = new GraphDatabase(path);
      db.initialize();
      this.databases.set(project, db);
    }

    return this.databases.get(project)!;
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
