#!/usr/bin/env npx tsx
/**
 * TCK Test Runner CLI
 * 
 * Quickly run a single TCK test by name or pattern.
 * 
 * Usage:
 *   npx tsx packages/server/test/tck/run-test.ts "Return6|11"
 *   npx tsx packages/server/test/tck/run-test.ts "Return6|11" --verbose
 *   npx tsx packages/server/test/tck/run-test.ts "Match3" --list
 * 
 * Or via pnpm script:
 *   pnpm tck "Return6|11"
 *   pnpm tck "Return6|11" -v
 */

import * as path from "path";
import { fileURLToPath } from "url";
import { GraphDatabase } from "../../src/db";
import { Executor } from "../../src/executor";
import { parseAllFeatures, TCKScenario } from "./tck-parser";
import { FAILING_TESTS } from "./failing-tests";
import { QUERY_OVERRIDES } from "./query-overrides";
import { valuesMatch, extractColumns, rowsMatch } from "./tck-utils";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const TCK_PATH = path.join(__dirname, "openCypher/tck/features");

// Parse command line args
const args = process.argv.slice(2);
const pattern = args.find(a => !a.startsWith("-")) || "";
const verbose = args.includes("-v") || args.includes("--verbose");
const listOnly = args.includes("-l") || args.includes("--list");
const showSql = args.includes("--sql");
const ignoreFailingList = args.includes("--force") || args.includes("-f");

if (!pattern) {
  console.log(`
TCK Test Runner - Run individual openCypher TCK tests

Usage:
  pnpm tck <pattern> [options]

Examples:
  pnpm tck "Return6|11"           # Run specific test
  pnpm tck "Return6" --list       # List all Return6 tests  
  pnpm tck "Match3|15" -v         # Run with verbose output
  pnpm tck "Return6|11" --sql     # Show generated SQL
  pnpm tck "Return6|11" --force   # Run even if in failing list

Options:
  -v, --verbose    Show detailed test information
  -l, --list       List matching tests without running
  --sql            Show generated SQL for the query
  -f, --force      Run test even if it's in the failing list
`);
  process.exit(0);
}

// Parse all features
const allFeatures = parseAllFeatures(TCK_PATH);

// Find matching scenarios
const matches: Array<{ scenario: TCKScenario; testKey: string }> = [];

for (const feature of allFeatures) {
  for (const scenario of feature.scenarios) {
    // Build test key like "clauses/return > Return6 - Implicit grouping|11"
    // For expanded outline scenarios, include example index: "|11:1", "|11:2", etc.
    const featurePath = feature.file.replace(TCK_PATH + "/", "").replace(".feature", "");
    // Use category (e.g., "expressions/comparison") not full path (e.g., "expressions/comparison/Comparison1")
    // This matches the format used in tck.test.ts and QUERY_OVERRIDES
    const category = path.dirname(featurePath);
    const indexPart = scenario.exampleIndex !== undefined 
      ? `${scenario.index}:${scenario.exampleIndex}`
      : `${scenario.index}`;
    const testKey = `${category} > ${feature.name}|${indexPart}`;
    const shortKey = `${feature.name}|${indexPart}`;
    
    // Extract feature base name (e.g., "Return6" from "Return6 - Implicit grouping...")
    const featureBaseName = feature.name.split(" - ")[0].split(" ")[0];
    const tinyKey = `${featureBaseName}|${indexPart}`;
    
    // Match against pattern (case insensitive)
    if (
      testKey.toLowerCase().includes(pattern.toLowerCase()) ||
      shortKey.toLowerCase().includes(pattern.toLowerCase()) ||
      tinyKey.toLowerCase() === pattern.toLowerCase() ||
      scenario.name.toLowerCase().includes(pattern.toLowerCase())
    ) {
      matches.push({ scenario, testKey });
    }
  }
}

if (matches.length === 0) {
  console.log(`No tests found matching "${pattern}"`);
  process.exit(1);
}

// List mode
if (listOnly) {
  console.log(`\nFound ${matches.length} matching test(s):\n`);
  for (const { scenario, testKey } of matches) {
    const inFailingList = FAILING_TESTS.has(testKey);
    const status = inFailingList ? "❌ (in failing list)" : "✅";
    console.log(`  ${status} ${testKey}`);
    console.log(`     "${scenario.name}"`);
  }
  process.exit(0);
}

// Run tests
console.log(`\nRunning ${matches.length} test(s) matching "${pattern}":\n`);

let passed = 0;
let failed = 0;

for (const { scenario, testKey } of matches) {
  const inFailingList = FAILING_TESTS.has(testKey);
  
  if (inFailingList && !ignoreFailingList) {
    console.log(`⏭️  SKIP: ${testKey} (in failing list, use --force to run)`);
    continue;
  }
  
  console.log(`\n${"=".repeat(70)}`);
  console.log(`TEST: ${testKey}`);
  console.log(`NAME: ${scenario.name}`);
  console.log(`${"=".repeat(70)}`);
  
  // Check for query overrides (used for JavaScript limitations like large integers)
  const override = QUERY_OVERRIDES.get(testKey);
  if (override) {
    console.log(`\n⚠️  Query override applied: ${override.reason}`);
  }
  
  // Get setup queries, query, and expected result (with override support)
  const setupQueries = override?.setup ?? scenario.setupQueries;
  const query = override?.query ?? scenario.query;
  const expectResult = override?.expectResult ?? scenario.expectResult;
  
    if (verbose) {
      console.log(`\nSetup queries:`);
      for (const q of setupQueries) {
        console.log(`  ${q.replace(/\n/g, "\n  ")}`);
      }
      console.log(`\nTest query:`);
      console.log(`  ${query.replace(/\n/g, "\n  ")}`);
      if (scenario.params && Object.keys(scenario.params).length > 0) {
        console.log(`\nParameters:`);
        console.log(`  ${JSON.stringify(scenario.params, null, 2)}`);
      }
      
      if (expectResult) {
      console.log(`\nExpected columns: ${JSON.stringify(expectResult.columns)}`);
      console.log(`Expected rows (${expectResult.rows.length}):`);
      for (const row of expectResult.rows) {
        console.log(`  ${JSON.stringify(row)}`);
      }
    } else if (scenario.expectEmpty) {
      console.log(`\nExpected: empty result`);
    } else if (scenario.expectError) {
      console.log(`\nExpected error: ${scenario.expectError.type} at ${scenario.expectError.phase}`);
    }
  }
  
  // Create fresh database
  const db = new GraphDatabase(":memory:");
  db.initialize();
  const executor = new Executor(db);
  
  try {
    // Run setup queries
    for (const setupQuery of setupQueries) {
      const result = executor.execute(setupQuery);
      if (!result.success) {
        console.log(`\n❌ Setup failed: ${(result as any).error?.message}`);
        console.log(`   Query: ${setupQuery}`);
        failed++;
        db.close();
        continue;
      }
    }
    
    // Show SQL if requested
    if (showSql) {
      try {
        const { parse } = await import("../../src/parser");
        const { Translator } = await import("../../src/translator");
        const parsed = parse(query);
        if (parsed.success) {
          const translator = new Translator({});
          const translated = translator.translate(parsed.query);
          console.log(`\nGenerated SQL:`);
          for (const stmt of translated.statements) {
            console.log(`  ${stmt.sql}`);
            if (stmt.params.length > 0) {
              console.log(`  Params: ${JSON.stringify(stmt.params)}`);
            }
          }
        }
      } catch (e) {
        console.log(`\nCould not generate SQL: ${e}`);
      }
    }
    
    // Run the test query
    const result = executor.execute(query, scenario.params);
    
    console.log(`\nResult:`);
    if (result.success) {
      console.log(`  Success: true`);
      console.log(`  Rows: ${(result as any).data?.length || 0}`);
      if ((result as any).data && (result as any).data.length > 0) {
        console.log(`  Data:`);
        for (const row of (result as any).data.slice(0, 10)) {
          console.log(`    ${JSON.stringify(row)}`);
        }
        if ((result as any).data.length > 10) {
          console.log(`    ... and ${(result as any).data.length - 10} more rows`);
        }
      }
    } else {
      console.log(`  Success: false`);
      console.log(`  Error: ${(result as any).error?.message}`);
    }
    
    // Check result
    if (scenario.expectError) {
      if (!result.success) {
        console.log(`\n✅ PASS: Got expected error`);
        passed++;
      } else {
        console.log(`\n❌ FAIL: Expected error but got success`);
        failed++;
      }
    } else if (scenario.expectEmpty) {
      if (result.success && ((result as any).data?.length === 0)) {
        console.log(`\n✅ PASS: Got expected empty result`);
        passed++;
      } else {
        console.log(`\n❌ FAIL: Expected empty result`);
        failed++;
      }
    } else if (expectResult) {
      if (!result.success) {
        console.log(`\n❌ FAIL: Query failed`);
        failed++;
      } else {
        const data = (result as any).data || [];
        const columns = expectResult.columns;
        const expectedRows = expectResult.rows;
        const actualRows = data.map((row: Record<string, unknown>) => extractColumns(row, columns));
        
        // Check row count first
        if (actualRows.length !== expectedRows.length) {
          console.log(`\n❌ FAIL: Expected ${expectedRows.length} rows, got ${actualRows.length}`);
          failed++;
        } else {
          // Check values match (unordered comparison)
          const match = rowsMatch(expectedRows, actualRows, false);
          if (match) {
            console.log(`\n✅ PASS: All ${actualRows.length} rows match`);
            passed++;
          } else {
            console.log(`\n❌ FAIL: Row values don't match`);
            if (verbose) {
              console.log(`  Expected: ${JSON.stringify(expectedRows)}`);
              console.log(`  Actual:   ${JSON.stringify(actualRows)}`);
            }
            failed++;
          }
        }
      }
    } else {
      console.log(`\n⚠️  No expected result defined`);
    }
    
  } catch (e) {
    console.log(`\n❌ FAIL: ${e}`);
    failed++;
  }
  
  db.close();
}

console.log(`\n${"=".repeat(70)}`);
console.log(`Summary: ${passed} passed, ${failed} failed`);
console.log(`${"=".repeat(70)}\n`);

process.exit(failed > 0 ? 1 : 0);
