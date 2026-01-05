#!/usr/bin/env npx tsx
/**
 * List all TCK tests that are currently in the FAILING_TESTS list.
 * 
 * Useful for TDD workflow to see what tests need to be fixed next.
 * 
 * Usage:
 *   npx tsx test/tck/list-failing-tests.ts [options]
 *   npm run tck:failing [-- options]
 * 
 * Options:
 *   --summary      Show only summary counts by category (default)
 *   --full         Show all failing tests with details
 *   --category X   Filter by category (e.g., "match", "return", "expressions")
 *   --errors       Show only tests that expect errors
 */

import * as path from "path";
import { fileURLToPath } from "url";
import { parseAllFeatures } from "./tck-parser";
import { FAILING_TESTS } from "./failing-tests";
import { NEO4J35_BASELINE } from "./neo4j35-baseline";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const TCK_PATH = path.join(__dirname, "openCypher/tck/features");

// Parse args
const args = process.argv.slice(2);
const showFull = args.includes("--full");
const showSummary = args.includes("--summary") || !showFull;
const errorsOnly = args.includes("--errors");
const categoryFilter = args.find((_, i) => args[i - 1] === "--category");

// Parse all features
const allFeatures = parseAllFeatures(TCK_PATH);

// Collect failing tests with their details
interface FailingTest {
  testKey: string;
  scenarioName: string;
  query: string;
  category: string;
  subcategory: string;
  expectsError: boolean;
  errorType?: string;
  errorPhase?: string;
}

const failingTests: FailingTest[] = [];

for (const feature of allFeatures) {
  // Match the test key format used in tck.test.ts
  // e.g., "clauses/match > Match8 - Match clause interoperation with other clauses|2"
  const featurePath = feature.file.replace(TCK_PATH + "/", "").replace(".feature", "");
  const pathParts = featurePath.split("/");
  // Category is first two parts: "clauses/match", "expressions/list", etc.
  const category = pathParts.slice(0, 2).join("/");
  const subcategory = pathParts[1] || pathParts[0];
  const topCategory = pathParts[0];
  
  for (const scenario of feature.scenarios) {
    const indexPart = scenario.exampleIndex !== undefined 
      ? `${scenario.index}:${scenario.exampleIndex}`
      : `${scenario.index}`;
    // Use category (not full featurePath) to match tck.test.ts format
    const testKey = `${category} > ${feature.name}|${indexPart}`;
    
    // Only include tests in failing list
    // Note: Don't filter by baseline - expected error tests may not be in baseline
    if (!FAILING_TESTS.has(testKey)) {
      continue;
    }
    
    // Apply filters
    if (errorsOnly && !scenario.expectError) {
      continue;
    }
    if (categoryFilter && !testKey.toLowerCase().includes(categoryFilter.toLowerCase())) {
      continue;
    }
    
    failingTests.push({
      testKey,
      scenarioName: scenario.name,
      query: scenario.query,
      category: topCategory,
      subcategory,
      expectsError: !!scenario.expectError,
      errorType: scenario.expectError?.type,
      errorPhase: scenario.expectError?.phase,
    });
  }
}

// Sort by test key
failingTests.sort((a, b) => a.testKey.localeCompare(b.testKey));

// Group by category/subcategory
const byCategory = new Map<string, Map<string, FailingTest[]>>();
for (const test of failingTests) {
  if (!byCategory.has(test.category)) {
    byCategory.set(test.category, new Map());
  }
  const subcats = byCategory.get(test.category)!;
  if (!subcats.has(test.subcategory)) {
    subcats.set(test.subcategory, []);
  }
  subcats.get(test.subcategory)!.push(test);
}

// Print header
const filterDesc = [
  errorsOnly ? "error tests only" : null,
  categoryFilter ? `category "${categoryFilter}"` : null,
].filter(Boolean).join(", ");

console.log(`\n📋 Failing TCK Tests (${failingTests.length} total)${filterDesc ? ` [${filterDesc}]` : ""}\n`);

if (failingTests.length === 0) {
  console.log("No failing tests match the criteria!\n");
  process.exit(0);
}

// Print summary
console.log("By category:\n");
const sortedCategories = [...byCategory.entries()].sort((a, b) => a[0].localeCompare(b[0]));

for (const [category, subcats] of sortedCategories) {
  const categoryTotal = [...subcats.values()].reduce((sum, tests) => sum + tests.length, 0);
  console.log(`${category}/ (${categoryTotal} failing)`);
  
  const sortedSubcats = [...subcats.entries()].sort((a, b) => a[0].localeCompare(b[0]));
  for (let i = 0; i < sortedSubcats.length; i++) {
    const [subcat, tests] = sortedSubcats[i];
    const isLast = i === sortedSubcats.length - 1;
    const prefix = isLast ? "└── " : "├── ";
    const errorCount = tests.filter(t => t.expectsError).length;
    const errorNote = errorCount > 0 ? ` (${errorCount} error tests)` : "";
    console.log(`${prefix}${subcat}: ${tests.length}${errorNote}`);
  }
  console.log("");
}

// Print full list if requested
if (showFull) {
  console.log(`${"=".repeat(70)}`);
  console.log("Detailed list:\n");
  
  for (const test of failingTests) {
    console.log(`${test.testKey}`);
    console.log(`  "${test.scenarioName}"`);
    if (test.expectsError) {
      console.log(`  Expects: ${test.errorPhase}/${test.errorType} error`);
    }
    const queryPreview = test.query.replace(/\n/g, " ").slice(0, 70);
    console.log(`  Query: ${queryPreview}${test.query.length > 70 ? "..." : ""}`);
    console.log("");
  }
}

// Print quick-run examples
console.log(`${"=".repeat(70)}`);
console.log("Quick run examples:\n");

// Pick first few from different categories for variety
const examples = new Set<string>();
for (const [, subcats] of sortedCategories) {
  for (const [, tests] of subcats) {
    if (examples.size >= 5) break;
    const test = tests[0];
    const match = test.testKey.match(/> ([^|]+)\|(\d+(?::\d+)?)/);
    if (match) {
      const featureName = match[1].split(" - ")[0].split(" ")[0];
      examples.add(`npm run tck '${featureName}|${match[2]}' -- --force`);
    }
  }
}
for (const ex of examples) {
  console.log(`  ${ex}`);
}
console.log("");
