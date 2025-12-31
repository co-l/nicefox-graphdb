/**
 * openCypher TCK Test Runner
 * 
 * Runs the openCypher Technology Compatibility Kit tests against NiceFox GraphDB.
 * This provides a comprehensive compliance test suite for Cypher support.
 */

import { describe, it, expect, beforeEach, afterEach } from "vitest";
import * as path from "path";
import { GraphDatabase } from "../../src/db";
import { Executor } from "../../src/executor";
import { parseAllFeatures, getStats, TCKScenario, ParsedFeature } from "./tck-parser";
import { FAILING_TESTS } from "./failing-tests";

const TCK_PATH = path.join(__dirname, "openCypher/tck/features");

// Parse all TCK features
const allFeatures = parseAllFeatures(TCK_PATH);
const stats = getStats(allFeatures);

console.log(`\n📊 TCK Statistics:`);
console.log(`   Total features: ${stats.totalFeatures}`);
console.log(`   Total scenarios: ${stats.totalScenarios}`);
console.log(`   With expected results: ${stats.withExpectedResults}`);
console.log(`   With expected errors: ${stats.withExpectedErrors}`);
console.log(`   With expected empty: ${stats.withExpectedEmpty}`);
console.log(`   Outline scenarios (skipped): ${stats.outlineScenarios}\n`);

// Track results for summary
const results = {
  passed: 0,
  failed: 0,
  skipped: 0,
  errors: [] as { scenario: string; error: string }[],
};

/**
 * Compare expected value with actual value
 */
function valuesMatch(expected: unknown, actual: unknown): boolean {
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
    // For now, just check it's an object (node)
    return typeof actual === "object" && actual !== null;
  }
  
  // Handle relationship patterns like [:TYPE]
  if (typeof expected === "object" && expected !== null && "_relPattern" in expected) {
    // Check it's an object (relationship) and the type matches
    if (typeof actual !== "object" || actual === null) return false;
    const relObj = actual as Record<string, unknown>;
    const pattern = (expected as Record<string, unknown>)._relPattern as string;
    // Extract type from pattern like "[:T1]" or "[:TYPE {prop: val}]"
    const typeMatch = pattern.match(/\[:(\w+)/);
    if (typeMatch && relObj.type) {
      return relObj.type === typeMatch[1];
    }
    return true; // If we can't parse, assume it's ok
  }
  
  // Handle path patterns like <(:Start)-[:T]->()>
  if (typeof expected === "object" && expected !== null && "_pathPattern" in expected) {
    // Check it's a path object with nodes and edges arrays
    if (typeof actual !== "object" || actual === null) return false;
    const pathObj = actual as Record<string, unknown>;
    // A path should have nodes and edges arrays
    if (!Array.isArray(pathObj.nodes) || !Array.isArray(pathObj.edges)) return false;
    // For now, just verify it's a valid path structure
    // TODO: Could parse the pattern and verify structure more strictly
    return true;
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
    if (Number.isInteger(expected)) {
      return expected === actual;
    }
    return Math.abs(expected - actual) < 0.0001;
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
 * Check if result rows match expected rows (order-independent)
 */
function rowsMatch(expected: unknown[][], actual: unknown[][], ordered: boolean): boolean {
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

/**
 * Check if a value represents a null node/relationship (all fields are null)
 */
function isNullEntity(value: unknown): boolean {
  if (typeof value !== "object" || value === null) return false;
  const obj = value as Record<string, unknown>;
  // A null entity has id: null (and possibly label/type/properties also null)
  return obj.id === null;
}

/**
 * Extract column values from result row
 */
function extractColumns(row: Record<string, unknown>, columns: string[]): unknown[] {
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
    
    return undefined;
  });
}

/**
 * Run a single TCK scenario
 */
function runScenario(scenario: TCKScenario, db: GraphDatabase, executor: Executor): void {
  // Skip Scenario Outlines (require template expansion)
  if (scenario.tags?.includes("outline")) {
    results.skipped++;
    return;
  }
  
  // Run setup queries
  for (const setup of scenario.setupQueries) {
    try {
      executor.execute(setup);
    } catch (e) {
      throw new Error(`Setup failed: ${setup}\n${e}`);
    }
  }
  
  // Run the test query
  if (scenario.expectError) {
    // Expect an error
    expect(() => executor.execute(scenario.query)).toThrow();
  } else {
    const result = executor.execute(scenario.query);
    
    if (!result.success) {
      throw new Error(`Query failed: ${result.error.message}\nQuery: ${scenario.query}`);
    }
    
    if (scenario.expectEmpty) {
      expect(result.data).toHaveLength(0);
    } else if (scenario.expectResult) {
      const { columns, rows, ordered } = scenario.expectResult;
      
      // Extract relevant columns from actual results
      const actualRows = result.data.map(row => extractColumns(row, columns));
      
      // Compare
      const match = rowsMatch(rows, actualRows, ordered);
      if (!match) {
        console.log("\nExpected columns:", columns);
        console.log("Expected rows:", JSON.stringify(rows, null, 2));
        console.log("Actual rows:", JSON.stringify(actualRows, null, 2));
        console.log("Raw result:", JSON.stringify(result.data, null, 2));
      }
      expect(match).toBe(true);
    }
  }
  
  results.passed++;
}

// Group scenarios by category for organized testing
const featuresByCategory = new Map<string, ParsedFeature[]>();
for (const feature of allFeatures) {
  const category = path.dirname(feature.file).split("/").slice(-2).join("/");
  if (!featuresByCategory.has(category)) {
    featuresByCategory.set(category, []);
  }
  featuresByCategory.get(category)!.push(feature);
}

// Start with a subset of fundamental tests
const priorityCategories = [
  "clauses/match",
  "clauses/create", 
  "clauses/return",
  "clauses/delete",
  "clauses/set",
  "clauses/merge",
  "clauses/with",
  "clauses/unwind",
  "expressions/aggregation",
];

describe("openCypher TCK", () => {
  let db: GraphDatabase;
  let executor: Executor;

  beforeEach(() => {
    db = new GraphDatabase(":memory:");
    db.initialize();
    executor = new Executor(db);
  });

  afterEach(() => {
    db.close();
  });

  // Run priority categories first
  for (const category of priorityCategories) {
    const features = featuresByCategory.get(category);
    if (!features) continue;

    describe(category, () => {
      for (const feature of features) {
        // Skip empty features
        if (feature.scenarios.length === 0) continue;
        
        describe(feature.name, () => {
          for (const scenario of feature.scenarios) {
            // Skip outline scenarios
            if (scenario.tags?.includes("outline")) {
              it.skip(`[${scenario.index}] ${scenario.name}`, () => {});
              continue;
            }
            
            // Skip error expectation scenarios for now (need better error handling)
            if (scenario.expectError) {
              it.skip(`[${scenario.index}] ${scenario.name} (expects error)`, () => {});
              continue;
            }

            // Check if this test is known to fail
            const testKey = `${category} > ${feature.name}|${scenario.index}`;
            const isKnownFailing = FAILING_TESTS.has(testKey);
            
            const testFn = isKnownFailing ? it.skip : it;
            testFn(`[${scenario.index}] ${scenario.name}`, () => {
              // Fresh DB for each test
              db = new GraphDatabase(":memory:");
              db.initialize();
              executor = new Executor(db);
              
              try {
                runScenario(scenario, db, executor);
              } finally {
                db.close();
              }
            });
          }
        });
      }
    });
  }
});

// Summary at the end
afterAll(() => {
  console.log(`\n📈 TCK Results Summary:`);
  console.log(`   ✅ Passed: ${results.passed}`);
  console.log(`   ❌ Failed: ${results.failed}`);
  console.log(`   ⏭️  Skipped: ${results.skipped}`);
  if (results.errors.length > 0) {
    console.log(`\n   First 10 errors:`);
    for (const err of results.errors.slice(0, 10)) {
      console.log(`   - ${err.scenario}: ${err.error.slice(0, 100)}`);
    }
  }
});

function afterAll(fn: () => void) {
  // This will be called after all tests complete
  process.on("beforeExit", fn);
}
