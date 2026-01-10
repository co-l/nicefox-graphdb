#!/usr/bin/env node

import { parseArgs } from "util";
import { fileURLToPath } from "url";
import * as fs from "fs";
import * as path from "path";
import type { BenchmarkResult, QueryResult } from "./types.js";
import { formatMs, formatBytes } from "./measure.js";

// Get benchmark directory path
const __dirname = path.dirname(fileURLToPath(import.meta.url));
const BENCHMARK_DIR = path.resolve(__dirname, "..");

// ANSI color codes
const colors = {
  reset: "\x1b[0m",
  bold: "\x1b[1m",
  dim: "\x1b[2m",
  green: "\x1b[32m",
  red: "\x1b[31m",
  yellow: "\x1b[33m",
  cyan: "\x1b[36m",
  gray: "\x1b[90m",
  magenta: "\x1b[35m",
};

// Get LeanGraph version from package.json
function getLeanGraphVersion(): string {
  try {
    const pkgPath = path.resolve(__dirname, "../../package.json");
    const pkg = JSON.parse(fs.readFileSync(pkgPath, "utf-8"));
    return pkg.version;
  } catch {
    return "unknown";
  }
}

// Find a benchmark result file by name
function findResultFile(name: string): string | null {
  const version = getLeanGraphVersion();
  const resultsDir = path.join(BENCHMARK_DIR, "results", version);

  if (!fs.existsSync(resultsDir)) {
    return null;
  }

  const exactPath = path.join(resultsDir, `${name}.json`);
  if (fs.existsSync(exactPath)) {
    return exactPath;
  }

  return null;
}

// List available benchmark names
function listAvailableNames(): string[] {
  const version = getLeanGraphVersion();
  const resultsDir = path.join(BENCHMARK_DIR, "results", version);

  if (!fs.existsSync(resultsDir)) {
    return [];
  }

  return fs
    .readdirSync(resultsDir)
    .filter((f) => f.endsWith(".json"))
    .map((f) => f.replace(".json", ""));
}

// Query pattern to optimization hints mapping
interface OptimizationHint {
  pattern: string;
  cause: string;
  files: string[];
  suggestions: string[];
}

const QUERY_HINTS: Record<string, OptimizationHint> = {
  // Traversals
  related_items_depth3: {
    pattern: "Variable-length path (*1..3)",
    cause: "Exponential path expansion without early termination",
    files: ["src/translator.ts (translateVarLengthPath)", "src/executor.ts"],
    suggestions: [
      "LIMIT pushdown into recursive CTE",
      "Early termination when LIMIT reached",
      "Path deduplication optimization",
    ],
  },
  related_items_depth2: {
    pattern: "Variable-length path (*1..2)",
    cause: "Path expansion grows with graph density",
    files: ["src/translator.ts (translateVarLengthPath)"],
    suggestions: [
      "Bounded recursion optimization",
      "Index on RELATED_TO edges",
    ],
  },
  related_items_depth1: {
    pattern: "Single-hop traversal with DISTINCT",
    cause: "DISTINCT on large result sets",
    files: ["src/translator.ts"],
    suggestions: ["Push DISTINCT into subquery", "Use EXISTS for dedup"],
  },

  // Pattern matching
  items_by_category: {
    pattern: "Property filter on i.category",
    cause: "No index on JSON properties - full table scan",
    files: ["src/db.ts (schema)", "src/translator.ts"],
    suggestions: [
      "Add computed column + index for category",
      "Property indexing system",
    ],
  },
  user_items: {
    pattern: "Pattern match with property filter",
    cause: "Property lookup then edge traversal",
    files: ["src/translator.ts"],
    suggestions: ["Index on User(id)", "Optimize join order"],
  },
  items_owned_by_multiple: {
    pattern: "Self-join on edges (diamond pattern)",
    cause: "N^2 join on OWNS edges",
    files: ["src/translator.ts"],
    suggestions: ["Add covering index", "Rewrite as semi-join"],
  },

  // Lookups
  lookup_user_by_id: {
    pattern: "Point lookup by property",
    cause: "JSON property extraction without index",
    files: ["src/db.ts", "src/translator.ts"],
    suggestions: ["Add index on nodes(json_extract(properties, '$.id'))"],
  },
  lookup_item_by_id: {
    pattern: "Point lookup with WHERE clause",
    cause: "Full scan with JSON extraction",
    files: ["src/translator.ts"],
    suggestions: ["Property index", "Prepared statement caching"],
  },
  lookup_user_by_email: {
    pattern: "Lookup by non-ID property",
    cause: "Full table scan on email property",
    files: ["src/db.ts"],
    suggestions: ["Selective property indexing"],
  },

  // Aggregations
  user_item_counts: {
    pattern: "Aggregation with ORDER BY",
    cause: "Full scan + sort for top-N",
    files: ["src/translator.ts"],
    suggestions: ["Partial aggregation", "Indexed count column"],
  },
  category_stats: {
    pattern: "GROUP BY on JSON property",
    cause: "JSON extraction for every row",
    files: ["src/translator.ts"],
    suggestions: ["Computed column for category", "Materialized view"],
  },

  // Writes
  create_user: {
    pattern: "Node creation",
    cause: "UUID generation + JSON serialization",
    files: ["src/executor.ts"],
    suggestions: ["Batch inserts", "Prepared statements"],
  },
  update_user_name: {
    pattern: "Property update with lookup",
    cause: "Lookup + JSON modification",
    files: ["src/executor.ts"],
    suggestions: ["json_set optimization", "Index for lookup"],
  },
};

// Cypher queries for reference
const QUERY_CYPHER: Record<string, string> = {
  lookup_user_by_id: "MATCH (u:User {id: $id}) RETURN u",
  lookup_item_by_id: "MATCH (i:Item) WHERE i.id = $id RETURN i",
  lookup_user_by_email: "MATCH (u:User) WHERE u.email = $email RETURN u",
  user_items: "MATCH (u:User {id: $id})-[:OWNS]->(i:Item) RETURN i",
  items_by_category: `MATCH (u:User)-[:OWNS]->(i:Item {category: $cat}) 
               RETURN u.id, i.title LIMIT 100`,
  user_events: "MATCH (u:User {id: $id})-[:TRIGGERED]->(e:Event) RETURN e",
  items_owned_by_multiple: `MATCH (u1:User)-[:OWNS]->(i:Item)<-[:OWNS]-(u2:User) 
               WHERE u1.id < u2.id 
               RETURN i.id, u1.id, u2.id LIMIT 50`,
  user_item_counts: `MATCH (u:User)-[:OWNS]->(i:Item) 
               RETURN u.id, COUNT(i) AS cnt 
               ORDER BY cnt DESC LIMIT 10`,
  category_stats: `MATCH (i:Item) 
               RETURN i.category, AVG(i.price) AS avg_price, COUNT(*) AS cnt`,
  event_type_counts: `MATCH (e:Event) 
               RETURN e.type, COUNT(*) AS cnt 
               ORDER BY cnt DESC`,
  user_event_summary: `MATCH (u:User {id: $id})-[:TRIGGERED]->(e:Event) 
               RETURN e.type, COUNT(*) AS cnt`,
  related_items_depth1: `MATCH (u:User {id: $id})-[:OWNS]->(i:Item)-[:RELATED_TO]->(r:Item) 
               RETURN DISTINCT r LIMIT 50`,
  related_items_depth2: `MATCH (u:User {id: $id})-[:OWNS]->(i:Item)-[:RELATED_TO*1..2]->(r:Item) 
               RETURN DISTINCT r LIMIT 50`,
  related_items_depth3: `MATCH (u:User {id: $id})-[:OWNS]->(i:Item)-[:RELATED_TO*1..3]->(r:Item) 
               RETURN DISTINCT r LIMIT 50`,
  create_user: `CREATE (u:User {id: $id, name: $name, email: $email, created_at: $ts})`,
  update_user_name: `MATCH (u:User {id: $id}) SET u.name = $name`,
  create_item: `CREATE (i:Item {id: $id, title: $title, category: $cat, price: $price})`,
};

function printHeader(text: string): void {
  console.log();
  console.log(`${colors.bold}${text}${colors.reset}`);
  console.log("━".repeat(50));
}

function analyzeSingleRun(name: string, topN: number = 5): void {
  const file = findResultFile(name);
  if (!file) {
    console.error(`${colors.red}Error: Benchmark '${name}' not found.${colors.reset}`);
    const available = listAvailableNames();
    if (available.length > 0) {
      console.error(`Available: ${available.join(", ")}`);
    }
    process.exit(1);
  }

  const result: BenchmarkResult = JSON.parse(fs.readFileSync(file, "utf-8"));
  const lg = result.databases.find((d) => d.database === "leangraph");

  if (!lg) {
    console.error(`${colors.red}Error: No LeanGraph results in '${name}'.${colors.reset}`);
    process.exit(1);
  }

  console.log();
  console.log(`${colors.bold}${colors.cyan}Performance Analysis: ${name}${colors.reset}`);
  console.log(`${colors.dim}Scale: ${result.scale} (${result.totalNodes.toLocaleString()} nodes, ${result.totalEdges.toLocaleString()} edges)${colors.reset}`);
  console.log(`${colors.dim}LeanGraph ${lg.version}${colors.reset}`);

  // Sort queries by p50 descending
  const sorted = [...lg.queries].sort((a, b) => b.timing.p50 - a.timing.p50);
  const topQueries = sorted.slice(0, topN);

  // Show top N slowest queries
  printHeader(`SLOWEST QUERIES (top ${topN} by p50)`);

  for (let i = 0; i < topQueries.length; i++) {
    const q = topQueries[i];
    const rank = `${i + 1}.`.padEnd(3);
    const name = q.name.padEnd(28);
    const p50 = formatMs(q.timing.p50).padStart(10);
    const cat = `(${q.category})`;

    console.log(`  ${colors.yellow}${rank}${colors.reset} ${name} ${colors.bold}${p50}${colors.reset}  ${colors.dim}${cat}${colors.reset}`);

    // Show cypher query for top 3
    if (i < 3 && QUERY_CYPHER[q.name]) {
      const cypher = QUERY_CYPHER[q.name].replace(/\s+/g, " ").trim();
      const truncated = cypher.length > 70 ? cypher.slice(0, 67) + "..." : cypher;
      console.log(`      ${colors.gray}${truncated}${colors.reset}`);
    }
  }

  // Investigation hints for top 3
  printHeader("INVESTIGATION HINTS");

  for (let i = 0; i < Math.min(3, topQueries.length); i++) {
    const q = topQueries[i];
    const hint = QUERY_HINTS[q.name];

    console.log(`${colors.magenta}${q.name}${colors.reset}`);

    if (hint) {
      console.log(`  ${colors.dim}Pattern:${colors.reset} ${hint.pattern}`);
      console.log(`  ${colors.dim}Cause:${colors.reset} ${hint.cause}`);
      console.log(`  ${colors.dim}Files:${colors.reset}`);
      for (const f of hint.files) {
        console.log(`    - ${f}`);
      }
      console.log(`  ${colors.dim}Suggestions:${colors.reset}`);
      for (const s of hint.suggestions) {
        console.log(`    ${colors.green}→${colors.reset} ${s}`);
      }
    } else {
      console.log(`  ${colors.dim}No specific hints available${colors.reset}`);
      console.log(`  ${colors.dim}Check: src/translator.ts, src/executor.ts${colors.reset}`);
    }
    console.log();
  }

  // Category summary
  printHeader("CATEGORY SUMMARY");

  const byCategory = new Map<string, number[]>();
  for (const q of lg.queries) {
    if (!byCategory.has(q.category)) byCategory.set(q.category, []);
    byCategory.get(q.category)!.push(q.timing.p50);
  }

  const categories = ["lookup", "pattern", "aggregation", "traversal", "write"];
  for (const cat of categories) {
    const times = byCategory.get(cat);
    if (!times) continue;

    const avg = times.reduce((a, b) => a + b, 0) / times.length;
    const max = Math.max(...times);
    const catName = cat.padEnd(12);

    console.log(
      `  ${catName} avg: ${formatMs(avg).padStart(8)}  max: ${formatMs(max).padStart(8)}`
    );
  }

  // Resource usage
  printHeader("RESOURCES");
  console.log(`  Disk:       ${formatBytes(lg.afterQueries.diskBytes)}`);
  console.log(`  RAM:        ${formatBytes(lg.afterQueries.ramBytes)}`);
  console.log(`  Cold start: ${formatMs(lg.coldStartMs)}`);
  console.log(`  Load time:  ${lg.load.timeSeconds.toFixed(1)}s`);

  // Next steps
  printHeader("RECOMMENDED NEXT STEPS");
  const slowestQuery = topQueries[0]?.name || "query";
  const queryText = QUERY_CYPHER[slowestQuery] || "";
  console.log(`  1. Debug slowest query (${slowestQuery}):`);
  console.log(`     See agents/PERFORMANCE_OPTIMIZATION.md "Debugging Queries" section`);
  if (queryText) {
    console.log(`     Query: ${colors.dim}${queryText.replace(/\s+/g, " ").slice(0, 60)}...${colors.reset}`);
  }
  console.log();
  console.log(`  2. Check generated SQL and EXPLAIN plan`);
  console.log();
  console.log(`  3. After optimization, run:`);
  console.log(`     ${colors.cyan}npm run benchmark -- -N 30K -d leangraph --name iter-1${colors.reset}`);
  console.log(`     ${colors.cyan}npm run benchmark:compare ${name} iter-1${colors.reset}`);
  console.log();
}

function compareRuns(baseName: string, targetName: string): void {
  const baseFile = findResultFile(baseName);
  const targetFile = findResultFile(targetName);

  if (!baseFile) {
    console.error(`${colors.red}Error: Benchmark '${baseName}' not found.${colors.reset}`);
    process.exit(1);
  }
  if (!targetFile) {
    console.error(`${colors.red}Error: Benchmark '${targetName}' not found.${colors.reset}`);
    process.exit(1);
  }

  const base: BenchmarkResult = JSON.parse(fs.readFileSync(baseFile, "utf-8"));
  const target: BenchmarkResult = JSON.parse(fs.readFileSync(targetFile, "utf-8"));

  const baseLg = base.databases.find((d) => d.database === "leangraph");
  const targetLg = target.databases.find((d) => d.database === "leangraph");

  if (!baseLg || !targetLg) {
    console.error(`${colors.red}Error: Missing LeanGraph results.${colors.reset}`);
    process.exit(1);
  }

  console.log();
  console.log(`${colors.bold}${colors.cyan}Performance Comparison${colors.reset}`);
  console.log(`${colors.dim}Baseline: ${baseName} → Target: ${targetName}${colors.reset}`);
  console.log();

  // Calculate improvements
  const baseQueryMap = new Map(baseLg.queries.map((q) => [q.name, q]));
  const targetQueryMap = new Map(targetLg.queries.map((q) => [q.name, q]));

  let totalImprovement = 0;
  let improvedCount = 0;
  let regressedCount = 0;

  const comparisons: { name: string; baseP50: number; targetP50: number; change: number }[] = [];

  for (const [name, baseQ] of baseQueryMap) {
    const targetQ = targetQueryMap.get(name);
    if (!targetQ) continue;

    const change = ((targetQ.timing.p50 - baseQ.timing.p50) / baseQ.timing.p50) * 100;
    comparisons.push({ name, baseP50: baseQ.timing.p50, targetP50: targetQ.timing.p50, change });

    if (change < -5) {
      improvedCount++;
      totalImprovement += -change;
    } else if (change > 5) {
      regressedCount++;
    }
  }

  // Sort by improvement (most improved first)
  comparisons.sort((a, b) => a.change - b.change);

  printHeader("QUERY CHANGES (sorted by improvement)");

  for (const c of comparisons) {
    const name = c.name.padEnd(28);
    const baseP50 = formatMs(c.baseP50).padStart(8);
    const targetP50 = formatMs(c.targetP50).padStart(8);

    let changeStr: string;
    if (c.change < -5) {
      changeStr = `${colors.green}${c.change.toFixed(1)}%${colors.reset}`;
    } else if (c.change > 5) {
      changeStr = `${colors.red}+${c.change.toFixed(1)}%${colors.reset}`;
    } else {
      changeStr = `${colors.gray}${c.change > 0 ? "+" : ""}${c.change.toFixed(1)}%${colors.reset}`;
    }

    console.log(`  ${name} ${baseP50} → ${targetP50}  ${changeStr}`);
  }

  // Summary
  printHeader("SUMMARY");

  const avgImprovement = improvedCount > 0 ? totalImprovement / improvedCount : 0;

  console.log(`  Improved:  ${colors.green}${improvedCount}${colors.reset} queries`);
  console.log(`  Regressed: ${colors.red}${regressedCount}${colors.reset} queries`);
  console.log(`  Unchanged: ${comparisons.length - improvedCount - regressedCount} queries`);
  console.log();

  if (avgImprovement > 5) {
    console.log(`  ${colors.green}Average improvement: ${avgImprovement.toFixed(1)}%${colors.reset}`);
    console.log(`  ${colors.bold}Continue optimizing!${colors.reset}`);
  } else if (regressedCount > improvedCount) {
    console.log(`  ${colors.red}More regressions than improvements.${colors.reset}`);
    console.log(`  Consider reverting or investigating regressions.`);
  } else {
    console.log(`  ${colors.yellow}Diminishing returns detected (<5% avg improvement).${colors.reset}`);
    console.log(`  Consider stopping optimization or trying different approach.`);
  }

  // Log recommendation
  console.log();
  console.log(`  ${colors.dim}Log this iteration:${colors.reset}`);
  console.log(`  ${colors.cyan}→ Update benchmark/OPTIMIZATION_LOG.md${colors.reset}`);
}

// Parse arguments
const { values, positionals } = parseArgs({
  options: {
    help: { type: "boolean", short: "h" },
    list: { type: "boolean", short: "l" },
    top: { type: "string", short: "n", default: "5" },
  },
  allowPositionals: true,
});

if (values.help) {
  console.log(`
${colors.bold}Benchmark Analysis Tool${colors.reset}

Usage:
  npm run benchmark:analyze <name>              Analyze single benchmark
  npm run benchmark:analyze <base> <target>    Compare two benchmarks

Options:
  -n, --top <N>    Show top N slowest queries (default: 5)
  -l, --list       List available benchmarks
  -h, --help       Show this help

Examples:
  npm run benchmark:analyze baseline
  npm run benchmark:analyze baseline iter-1
  npm run benchmark:analyze baseline -n 10
`);
  process.exit(0);
}

if (values.list) {
  const available = listAvailableNames();
  if (available.length === 0) {
    console.log("No benchmark results found. Run a benchmark first:");
    console.log("  npm run benchmark -- -N 30K -d leangraph --name baseline");
  } else {
    console.log("Available benchmarks:");
    for (const name of available) {
      console.log(`  ${name}`);
    }
  }
  process.exit(0);
}

if (positionals.length === 0) {
  console.error("Error: Expected benchmark name(s).");
  console.error("Usage: npm run benchmark:analyze <name> [target]");
  console.error("Run with --list to see available benchmarks.");
  process.exit(1);
}

const topN = parseInt(values.top as string, 10) || 5;

if (positionals.length === 1) {
  analyzeSingleRun(positionals[0], topN);
} else {
  compareRuns(positionals[0], positionals[1]);
}
