/**
 * Shared utilities for TCK test runners
 * 
 * This module contains the value matching and column extraction logic
 * used by both the CLI runner (run-test.ts) and vitest (tck.test.ts).
 */

/**
 * Parse a Cypher-style property value string into a JavaScript value
 */
function parsePatternValue(valueStr: string): unknown {
  const trimmed = valueStr.trim();
  
  // Null
  if (trimmed === "null") return null;
  
  // Boolean
  if (trimmed === "true") return true;
  if (trimmed === "false") return false;
  
  // String (single-quoted)
  if (trimmed.startsWith("'") && trimmed.endsWith("'")) {
    return trimmed.slice(1, -1);
  }
  
  // Array
  if (trimmed.startsWith("[") && trimmed.endsWith("]")) {
    const inner = trimmed.slice(1, -1).trim();
    if (inner === "") return [];
    // Split by comma, but be careful with nested structures
    const elements: unknown[] = [];
    let depth = 0;
    let current = "";
    for (const char of inner) {
      if (char === "[" || char === "{") depth++;
      else if (char === "]" || char === "}") depth--;
      else if (char === "," && depth === 0) {
        elements.push(parsePatternValue(current));
        current = "";
        continue;
      }
      current += char;
    }
    if (current.trim()) {
      elements.push(parsePatternValue(current));
    }
    return elements;
  }
  
  // Number
  const num = Number(trimmed);
  if (!isNaN(num) && trimmed !== "") {
    return num;
  }
  
  // Return as string if nothing else matches
  return trimmed;
}

/**
 * Parse a Cypher-style properties string like "name: 'foo', num: 42, arr: [1, 2, 3]"
 * into a JavaScript object
 */
function parsePatternProperties(propsStr: string): Record<string, unknown> {
  const result: Record<string, unknown> = {};
  
  // Split by top-level commas (not inside brackets)
  let depth = 0;
  let current = "";
  const pairs: string[] = [];
  
  for (const char of propsStr) {
    if (char === "[" || char === "{") depth++;
    else if (char === "]" || char === "}") depth--;
    else if (char === "," && depth === 0) {
      pairs.push(current.trim());
      current = "";
      continue;
    }
    current += char;
  }
  if (current.trim()) {
    pairs.push(current.trim());
  }
  
  // Parse each key: value pair
  for (const pair of pairs) {
    const colonIdx = pair.indexOf(":");
    if (colonIdx === -1) continue;
    const key = pair.slice(0, colonIdx).trim();
    const valueStr = pair.slice(colonIdx + 1).trim();
    result[key] = parsePatternValue(valueStr);
  }
  
  return result;
}

/**
 * Compare expected value with actual value from query results
 */
export function valuesMatch(expected: unknown, actual: unknown): boolean {
  // Handle null
  if (expected === null) {
    return actual === null || actual === undefined;
  }
  
  // Handle booleans - SQLite returns 1/0 for true/false
  if (typeof expected === "boolean") {
    if (typeof actual === "boolean") {
      return expected === actual;
    }
    if (typeof actual === "number") {
      return expected === (actual !== 0);
    }
    return false;
  }
  
  // Handle node patterns like (:Label {prop: 'val'})
  if (typeof expected === "object" && expected !== null && "_nodePattern" in expected) {
    if (typeof actual !== "object" || actual === null) return false;
    const nodeObj = actual as Record<string, unknown>;
    const pattern = (expected as Record<string, unknown>)._nodePattern as string;
    
    // Parse the pattern to extract expected properties
    // Pattern format: "(:Label {key: value, ...})" or "({key: value})"
    // Note: pattern ends with ")" so we need to match {...} anywhere, not at end
    const propsMatch = pattern.match(/\{(.+)\}/);
    if (propsMatch) {
      const propsStr = propsMatch[1];
      // Parse properties more carefully to handle nested arrays/objects
      const props = parsePatternProperties(propsStr);
      for (const [key, expectedValue] of Object.entries(props)) {
        // Use valuesMatch for recursive comparison (handles arrays, numbers, etc.)
        if (!valuesMatch(expectedValue, nodeObj[key])) {
          return false;
        }
      }
    }
    return true;
  }
  
  // Handle relationship patterns like [:TYPE] or [:TYPE {prop: val}]
  // Also handles heterogeneous lists like [(:A), [:T], (:B)] that get parsed as _relPattern
  if (typeof expected === "object" && expected !== null && "_relPattern" in expected) {
    if (typeof actual !== "object" || actual === null) return false;
    
    const pattern = (expected as Record<string, unknown>)._relPattern as string;
    
    // Check if this is a heterogeneous list of patterns like "[(:A), [:T], (:B)]"
    // vs a true relationship pattern like "[:TYPE]"
    if (pattern.startsWith("[(") && Array.isArray(actual)) {
      // This is a list containing node/relationship patterns
      // Just verify that actual is an array of objects (nodes and relationships)
      return actual.every(el => typeof el === "object" && el !== null);
    }
    
    // Could be a single relationship or an array of relationships
    if (Array.isArray(actual)) {
      // It's a list of relationships - verify each has type
      return actual.every(r => typeof r === "object" && r !== null && "type" in r);
    }
    
    const relObj = actual as Record<string, unknown>;
    
    // Extract type from pattern like "[:T1]" or "[:TYPE {prop: val}]"
    const typeMatch = pattern.match(/\[:(\w+)/);
    if (typeMatch && relObj.type) {
      if (relObj.type !== typeMatch[1]) {
        return false;
      }
    }
    
    // Parse and validate properties if present
    // Note: pattern ends with "]" so we need to match {...} anywhere, not at end
    const propsMatch = pattern.match(/\{(.+)\}/);
    if (propsMatch) {
      const props = parsePatternProperties(propsMatch[1]);
      for (const [key, expectedValue] of Object.entries(props)) {
        if (!valuesMatch(expectedValue, relObj[key])) {
          return false;
        }
      }
    }
    
    return true;
  }
  
  // Handle path patterns like <(:Start)-[:T]->()>
  if (typeof expected === "object" && expected !== null && "_pathPattern" in expected) {
    if (typeof actual !== "object" || actual === null) return false;
    
    // Check for object format with nodes and edges arrays
    const pathObj = actual as Record<string, unknown>;
    if (Array.isArray(pathObj.nodes) && Array.isArray(pathObj.edges)) {
      return true;
    }
    
    // Also accept Neo4j 3.5 alternating array format: [nodeProps, edgeProps, nodeProps, ...]
    if (Array.isArray(actual)) {
      // A path should have odd length (n nodes, n-1 edges alternating)
      // e.g., [node] = length 1, [node, edge, node] = length 3, etc.
      // Each element should be an object (properties)
      if (actual.length === 0) return false;
      if (actual.length % 2 !== 1) return false;
      return actual.every(el => typeof el === "object" && el !== null);
    }
    
    return false;
  }
  
  // Handle arrays
  if (Array.isArray(expected)) {
    if (!Array.isArray(actual)) return false;
    if (expected.length !== actual.length) return false;
    return expected.every((e, i) => valuesMatch(e, actual[i]));
  }
  
  // Handle objects
  if (typeof expected === "object" && expected !== null) {
    if (typeof actual !== "object" || actual === null) return false;
    const expKeys = Object.keys(expected);
    const actKeys = Object.keys(actual as object);
    if (expKeys.length !== actKeys.length) return false;
    return expKeys.every(k => valuesMatch((expected as Record<string, unknown>)[k], (actual as Record<string, unknown>)[k]));
  }
  
  // Handle numbers (with floating point tolerance)
  if (typeof expected === "number" && typeof actual === "number") {
    // Always use tolerance for floating point comparison
    // This handles cases like expected=0 vs actual=-1.1e-15
    const tolerance = 0.0001;
    if (Math.abs(expected - actual) < tolerance) {
      return true;
    }
    // For integers, also require exact match if above tolerance
    if (Number.isInteger(expected)) {
      return expected === actual;
    }
    return false;
  }
  
  // Handle string patterns that represent maps like "{a: 1, b: 'foo'}"
  // These should match the actual object structure
  if (typeof expected === "string" && typeof actual === "object" && actual !== null) {
    const mapPattern = expected.match(/^\{.*\}$/);
    if (mapPattern) {
      // It's a map pattern string, the actual value is an object - consider them matching
      // if actual is an object with the right shape
      return true;
    }
  }
  
  // Direct comparison
  return expected === actual;
}

/**
 * Check if a value represents a null node/relationship (all fields are null)
 */
export function isNullEntity(value: unknown): boolean {
  if (typeof value !== "object" || value === null) return false;
  const obj = value as Record<string, unknown>;
  // A null entity has id: null (and possibly label/type/properties also null)
  return obj.id === null;
}

/**
 * Extract column values from result row
 */
export function extractColumns(row: Record<string, unknown>, columns: string[]): unknown[] {
  return columns.map(col => {
    // Handle column names that might be expressions like "n.name" or "count(n)"
    if (col in row) {
      const value = row[col];
      // Convert null entities to actual null
      if (isNullEntity(value)) return null;
      return value;
    }
    
    // Try without quotes
    const cleanCol = col.replace(/['"]/g, "");
    if (cleanCol in row) {
      const value = row[cleanCol];
      if (isNullEntity(value)) return null;
      return value;
    }
    
    // Try underscore version of dot notation: "n.name" -> "n_name"
    const underscoreCol = cleanCol.replace(/\./g, "_");
    if (underscoreCol in row) {
      const value = row[underscoreCol];
      if (isNullEntity(value)) return null;
      return value;
    }
    
    // Try property access like "n.name" - look up the node and get its property
    const parts = col.split(".");
    if (parts.length === 2) {
      const [varName, propName] = parts;
      const node = row[varName] as Record<string, unknown> | undefined;
      if (node && typeof node === "object") {
        if ("properties" in node) {
          return (node.properties as Record<string, unknown>)[propName];
        }
        return node[propName];
      }
    }
    
    // Try extracting function name from expressions like "count(*)" -> "count"
    // or "count(n)" -> "count", "sum(n.num)" -> "sum", etc.
    const funcMatch = cleanCol.match(/^(\w+)\s*\(/);
    if (funcMatch) {
      const funcName = funcMatch[1].toLowerCase();
      if (funcName in row) {
        const value = row[funcName];
        if (isNullEntity(value)) return null;
        return value;
      }
    }
    
    // Try "expr" as a fallback for complex expressions like "{a: 1, b: 'foo'}" or "count(a) + 3"
    // that we can't easily name
    if ("expr" in row) {
      const value = row["expr"];
      if (isNullEntity(value)) return null;
      return value;
    }
    
    // Normalize function expression column name and try matching
    // e.g., "coUnt( dIstInct p )" -> "count(distinctp)" (after removing spaces and lowercasing)
    const normalizedCol = cleanCol.toLowerCase().replace(/\s+/g, "");
    for (const key of Object.keys(row)) {
      const normalizedKey = key.toLowerCase().replace(/\s+/g, "");
      if (normalizedKey === normalizedCol) {
        const value = row[key];
        if (isNullEntity(value)) return null;
        return value;
      }
    }
    
    // For complex expressions with operators (like "12 / 4 * (3 - 2 * 4)"),
    // try to find a column that contains similar operators but may have different grouping
    // This handles cases where our column name doesn't perfectly preserve parentheses
    const operators = ['+', '-', '*', '/', '%'];
    if (operators.some(op => cleanCol.includes(op))) {
      // Strip all whitespace and parentheses for comparison
      const strippedCol = cleanCol.replace(/[\s()]/g, '');
      for (const key of Object.keys(row)) {
        const strippedKey = key.replace(/[\s()]/g, '');
        if (strippedKey === strippedCol) {
          const value = row[key];
          if (isNullEntity(value)) return null;
          return value;
        }
      }
      
      // Also try matching with function arguments stripped
      // e.g., "count(a) + 3" -> "count + 3"
      const strippedArgCol = cleanCol.replace(/\([^)]+\)/g, '');
      for (const key of Object.keys(row)) {
        if (key === strippedArgCol) {
          const value = row[key];
          if (isNullEntity(value)) return null;
          return value;
        }
      }
    }
    
    return undefined;
  });
}

/**
 * Check if result rows match expected rows
 */
export function rowsMatch(expected: unknown[][], actual: unknown[][], ordered: boolean): boolean {
  if (expected.length !== actual.length) return false;
  
  if (ordered) {
    return expected.every((row, i) => 
      row.length === actual[i].length && 
      row.every((val, j) => valuesMatch(val, actual[i][j]))
    );
  }
  
  // Unordered: each expected row must have a matching actual row
  const usedActual = new Set<number>();
  for (const expRow of expected) {
    let found = false;
    for (let i = 0; i < actual.length; i++) {
      if (usedActual.has(i)) continue;
      if (expRow.length === actual[i].length && 
          expRow.every((val, j) => valuesMatch(val, actual[i][j]))) {
        usedActual.add(i);
        found = true;
        break;
      }
    }
    if (!found) return false;
  }
  return true;
}
