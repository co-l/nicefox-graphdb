/**
 * openCypher TCK Test Runner
 * 
 * Runs the openCypher Technology Compatibility Kit tests against NiceFox GraphDB.
 * This provides a comprehensive compliance test suite for Cypher support.
 */

import { describe, it, expect, beforeEach, afterEach, afterAll } from "vitest";
import * as path from "path";
import { GraphDatabase } from "../../src/db";
import { Executor } from "../../src/executor";
import { parseAllFeatures, getStats, TCKScenario, ParsedFeature } from "./tck-parser";
import { FAILING_TESTS } from "./failing-tests";
import { NEO4J35_BASELINE } from "./neo4j35-baseline";
import { QUERY_OVERRIDES } from "./query-overrides";
import { valuesMatch, extractColumns, rowsMatch } from "./tck-utils";

const TCK_PATH = path.join(__dirname, "openCypher/tck/features");

// Environment variable to run all tests including known failing ones
// Usage: TCK_TEST_ALL=1 pnpm test -- --run
const TCK_TEST_ALL = process.env.TCK_TEST_ALL === "1";

// Parse all TCK features
const allFeatures = parseAllFeatures(TCK_PATH);
const stats = getStats(allFeatures);

console.log(`\n📊 TCK Statistics (Neo4j 3.5 baseline):`);
console.log(`   Target: ${NEO4J35_BASELINE.size} tests`);
if (TCK_TEST_ALL) {
  console.log(`   Mode: Testing ALL (including known failing tests)`);
}
console.log("");

// Track results for summary
const results = {
  passed: 0,
  failed: 0,
  skipped: 0,
  errors: [] as { scenario: string; error: string }[],
};

// Hierarchical stats tracking for detailed summary
interface FeatureStats {
  passed: number;
  failed: number;
  skipped: number;
  total: number;
}

interface SubcategoryStats extends FeatureStats {
  features: Map<string, FeatureStats>;
}

interface CategoryStats extends FeatureStats {
  subcategories: Map<string, SubcategoryStats>;
}

// Track results by category hierarchy
const categoryResults = new Map<string, CategoryStats>();

/**
 * Parse a test key into its hierarchical components
 * e.g., "clauses/match > Match1 - Match nodes|5" =>
 *   { topLevel: "clauses", subCategory: "match", feature: "Match1 - Match nodes" }
 */
function parseTestKey(testKey: string): { 
  topLevel: string; 
  subCategory: string; 
  feature: string;
} {
  // Format: "category/subcategory > FeatureName - Description|testNum"
  const categoryMatch = testKey.match(/^([^/]+)\/([^>]+)\s*>\s*([^|]+)/);
  if (!categoryMatch) {
    return { topLevel: "unknown", subCategory: "unknown", feature: "unknown" };
  }
  return {
    topLevel: categoryMatch[1].trim(),
    subCategory: categoryMatch[2].trim(),
    feature: categoryMatch[3].trim(),
  };
}

/**
 * Record a test result in the hierarchical tracking structure
 */
function recordResult(
  testKey: string, 
  status: "passed" | "failed" | "skipped"
): void {
  const { topLevel, subCategory, feature } = parseTestKey(testKey);
  
  // Initialize category if needed
  if (!categoryResults.has(topLevel)) {
    categoryResults.set(topLevel, {
      passed: 0, failed: 0, skipped: 0, total: 0,
      subcategories: new Map(),
    });
  }
  const cat = categoryResults.get(topLevel)!;
  
  // Initialize subcategory if needed
  if (!cat.subcategories.has(subCategory)) {
    cat.subcategories.set(subCategory, {
      passed: 0, failed: 0, skipped: 0, total: 0,
      features: new Map(),
    });
  }
  const subcat = cat.subcategories.get(subCategory)!;
  
  // Initialize feature if needed
  if (!subcat.features.has(feature)) {
    subcat.features.set(feature, { passed: 0, failed: 0, skipped: 0, total: 0 });
  }
  const feat = subcat.features.get(feature)!;
  
  // Update counts at all levels
  cat[status]++;
  cat.total++;
  subcat[status]++;
  subcat.total++;
  feat[status]++;
  feat.total++;
}

/**
 * Format stats as "passed/total" with optional checkmark or skip info
 */
function formatStats(stats: FeatureStats): string {
  const passRate = stats.total > 0 ? stats.passed / stats.total : 0;
  const base = `${stats.passed}/${stats.total}`;
  
  if (passRate === 1 && stats.total > 0) {
    return `${base} ✓`;
  }
  if (stats.skipped > 0 && stats.failed === 0) {
    return `${base} (${stats.skipped} skipped)`;
  }
  return base;
}

/**
 * Print the detailed category breakdown with tree structure
 */
function printDetailedSummary(): void {
  console.log(`\n📊 Detailed Results:\n`);
  
  // Sort categories for consistent output
  const sortedCategories = [...categoryResults.entries()].sort((a, b) => 
    a[0].localeCompare(b[0])
  );
  
  for (const [catName, catStats] of sortedCategories) {
    const passRate = catStats.total > 0 
      ? ((catStats.passed / catStats.total) * 100).toFixed(1) 
      : "0.0";
    console.log(`${catName} (${catStats.passed}/${catStats.total} passed, ${passRate}%)`);
    
    const subcats = [...catStats.subcategories.entries()].sort((a, b) => 
      a[0].localeCompare(b[0])
    );
    
    for (let i = 0; i < subcats.length; i++) {
      const [subName, subStats] = subcats[i];
      const isLastSubcat = i === subcats.length - 1;
      const prefix = isLastSubcat ? "└── " : "├── ";
      
      // Pad subcategory name for alignment
      const paddedName = (subName + ":").padEnd(22);
      console.log(`${prefix}${paddedName} ${formatStats(subStats)}`);
    }
    console.log(""); // Blank line between categories
  }
}

// Track which tests from FAILING_TESTS actually passed (only when TCK_TEST_ALL is set)
const unexpectedlyPassed: string[] = [];

/**
 * Run a single TCK scenario
 */
function runScenario(scenario: TCKScenario, db: GraphDatabase, executor: Executor, testKey: string): void {
  // Skip Scenario Outlines (require template expansion)
  if (scenario.tags?.includes("outline")) {
    // Note: This case shouldn't happen as outlines are expanded in tck-parser
    return;
  }
  
  // Check for query overrides (used for JavaScript limitations like large integers)
  const override = QUERY_OVERRIDES.get(testKey);
  
  // Get setup queries, query, and expected result (with override support)
  const setupQueries = override?.setup ?? scenario.setupQueries;
  const query = override?.query ?? scenario.query;
  const expectResult = override?.expectResult ?? scenario.expectResult;
  
  // Run setup queries
  for (const setup of setupQueries) {
    try {
      executor.execute(setup);
    } catch (e) {
      throw new Error(`Setup failed: ${setup}\n${e}`);
    }
  }
  
  // Run the test query
  if (scenario.expectError) {
    // Expect an error - executor returns { success: false } instead of throwing
    const result = executor.execute(query, scenario.params);
    expect(result.success).toBe(false);
  } else {
    const result = executor.execute(query, scenario.params);
    
    if (!result.success) {
      throw new Error(`Query failed: ${result.error.message}\nQuery: ${query}`);
    }
    
    if (scenario.expectEmpty) {
      expect(result.data).toHaveLength(0);
    } else if (expectResult) {
      const { columns, rows, ordered } = expectResult;
      
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
  
  // Note: results.passed is now tracked in the test function after runScenario completes
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

// Run all TCK categories
const priorityCategories = [
  // Core clauses
  "clauses/match",
  "clauses/match-where",
  "clauses/create", 
  "clauses/return",
  "clauses/return-orderby",
  "clauses/return-skip-limit",
  "clauses/delete",
  "clauses/set",
  "clauses/remove",
  "clauses/merge",
  "clauses/with",
  "clauses/with-where",
  "clauses/with-orderBy",
  "clauses/with-skip-limit",
  "clauses/unwind",
  "clauses/union",
  // "clauses/call", // Not yet supported - CALL procedures
  // Expressions
  "expressions/aggregation",
  "expressions/boolean",
  "expressions/comparison",
  "expressions/conditional",
  "expressions/existentialSubqueries",
  "expressions/graph",
  "expressions/list",
  "expressions/literals",
  "expressions/map",
  "expressions/mathematical",
  "expressions/null",
  "expressions/path",
  "expressions/pattern",
  "expressions/precedence",
  "expressions/quantifier",
  "expressions/string",
  "expressions/temporal",
  "expressions/typeConversion",
  // Use cases
  "useCases/countingSubgraphMatches",
  "useCases/triadicSelection",
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
        
        // Check if any scenarios are in the baseline
        const hasBaselineTests = feature.scenarios.some(scenario => {
          const testKey = scenario.exampleIndex !== undefined
            ? `${category} > ${feature.name}|${scenario.index}:${scenario.exampleIndex}`
            : `${category} > ${feature.name}|${scenario.index}`;
          return NEO4J35_BASELINE.has(testKey);
        });
        
        if (!hasBaselineTests) continue;
        
        describe(feature.name, () => {
          for (const scenario of feature.scenarios) {
            // Build test key - include example index for expanded outline scenarios
            const testKey = scenario.exampleIndex !== undefined
              ? `${category} > ${feature.name}|${scenario.index}:${scenario.exampleIndex}`
              : `${category} > ${feature.name}|${scenario.index}`;
            
            // Not in Neo4j 3.5 baseline? Don't create a test at all
            if (!NEO4J35_BASELINE.has(testKey)) {
              continue;
            }
            
            const isKnownFailing = FAILING_TESTS.has(testKey);
            
            // Build test name - include example index for expanded outlines
            const testName = scenario.exampleIndex !== undefined
              ? `[${scenario.index}:${scenario.exampleIndex}] ${scenario.name}`
              : `[${scenario.index}] ${scenario.name}`;
            
            // Skip known failing tests unless TCK_TEST_ALL is set
            const shouldSkip = isKnownFailing && !TCK_TEST_ALL;
            const testFn = shouldSkip ? it.skip : it;
            
            // Record skipped tests for detailed summary
            if (shouldSkip) {
              recordResult(testKey, "skipped");
              results.skipped++;
            }
            
            testFn(testName, () => {
              // Fresh DB for each test
              db = new GraphDatabase(":memory:");
              db.initialize();
              executor = new Executor(db);
              
              try {
                runScenario(scenario, db, executor, testKey);
                // Record successful test
                recordResult(testKey, "passed");
                // If this test was in the failing list but passed, track it
                if (TCK_TEST_ALL && isKnownFailing) {
                  unexpectedlyPassed.push(testKey);
                }
              } catch (error) {
                // Record failed test
                recordResult(testKey, "failed");
                results.failed++;
                results.errors.push({
                  scenario: testKey,
                  error: error instanceof Error ? error.message : String(error),
                });
                throw error; // Re-throw to fail the test
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
  // Calculate totals from category results for accurate counting
  let totalPassed = 0;
  let totalFailed = 0;
  let totalSkipped = 0;
  for (const cat of categoryResults.values()) {
    totalPassed += cat.passed;
    totalFailed += cat.failed;
    totalSkipped += cat.skipped;
  }
  
  // Print detailed category breakdown first
  printDetailedSummary();
  
  // Then print the summary
  console.log(`📈 TCK Results Summary:`);
  console.log(`   ✅ Passed: ${totalPassed}`);
  console.log(`   ❌ Failed: ${totalFailed}`);
  console.log(`   ⏭️  Skipped: ${totalSkipped}`);
  
  if (results.errors.length > 0) {
    console.log(`\n   First 10 errors:`);
    for (const err of results.errors.slice(0, 10)) {
      console.log(`   - ${err.scenario}: ${err.error.slice(0, 100)}`);
    }
  }
  
  // Report tests that were in FAILING_TESTS but actually passed
  if (TCK_TEST_ALL && unexpectedlyPassed.length > 0) {
    console.log(`\n🎉 Tests from FAILING_TESTS that now PASS (${unexpectedlyPassed.length}):`);
    console.log(`   These can be removed from failing-tests.ts:\n`);
    for (const testKey of unexpectedlyPassed) {
      console.log(`   // "${testKey}",`);
    }
    console.log("");
  }
});
