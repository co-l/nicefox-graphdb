#!/usr/bin/env node

import { parseArgs } from "util";
import { fileURLToPath } from "url";
import { SCALES, BENCHMARK_CONFIG, getTotalNodes, getTotalEdges } from "./config.js";
import { createQueries, getReadQueries, getWriteQueries } from "./queries.js";
import { resetQueryRng } from "./generator.js";
import { loadLeanGraph, LeanGraphRunner } from "./loaders/leangraph.js";
import { loadNeo4j, Neo4jRunner } from "./loaders/neo4j.js";
import { loadMemgraph, MemgraphRunner } from "./loaders/memgraph.js";
import {
  checkDockerAvailable,
  ensureContainerReady,
  stopAndCleanup,
} from "./docker.js";
import {
  getDiskUsage,
  getProcessRam,
  getDockerRam,
  calculateStats,
  formatBytes,
  formatMs,
  formatSeconds,
} from "./measure.js";
import type {
  Scale,
  ScaleConfig,
  DatabaseType,
  BenchmarkResult,
  DatabaseResult,
  QueryResult,
  Runner,
} from "./types.js";
import * as fs from "fs";
import * as path from "path";

// Get benchmark directory path (works regardless of CWD)
const __dirname = path.dirname(fileURLToPath(import.meta.url));
const BENCHMARK_DIR = path.resolve(__dirname, "..");

// Parse CLI arguments
const { values } = parseArgs({
  options: {
    scale: { type: "string", short: "s", default: "quick" },
    databases: { type: "string", short: "d", default: "leangraph,neo4j,memgraph" },
    skipLoad: { type: "boolean", default: false },
    output: { type: "string", short: "o" },
    help: { type: "boolean", short: "h" },
  },
});

if (values.help) {
  console.log(`
LeanGraph Benchmark Suite

Usage: npm run benchmark [options]

Options:
  -s, --scale <scale>          Dataset scale: micro, quick, full (default: quick)
  -d, --databases <list>       Comma-separated databases (default: leangraph,neo4j,memgraph)
  --skipLoad                   Skip data loading (use existing data)
  -o, --output <file>          Output file for results JSON
  -h, --help                   Show this help

Scales:
  micro   8K nodes, 8K edges      (fast sanity check)
  quick   170K nodes, 180K edges  (development benchmark)
  full    17M nodes, 18M edges    (production benchmark)

Examples:
  npm run benchmark -- -s micro                    # Fast test with all databases
  npm run benchmark -- -s micro -d leangraph       # Fast test, LeanGraph only
  npm run benchmark -- -s full -o results.json     # Full benchmark
`);
  process.exit(0);
}

const scale = (values.scale as Scale) || "quick";
const databases = (values.databases as string).split(",").map((d) => d.trim()) as DatabaseType[];
const skipLoad = values.skipLoad as boolean;
const outputFile = values.output as string | undefined;

const config: ScaleConfig = SCALES[scale];
const queries = createQueries(config);

console.log("=".repeat(60));
console.log("LeanGraph Benchmark Suite");
console.log("=".repeat(60));
console.log(`Scale: ${scale}`);
console.log(`Nodes: ${getTotalNodes(config).toLocaleString()}`);
console.log(`Edges: ${getTotalEdges(config).toLocaleString()}`);
console.log(`Databases: ${databases.join(", ")}`);
console.log(`Warmup: ${BENCHMARK_CONFIG.warmupIterations}, Measured: ${BENCHMARK_CONFIG.measuredIterations}`);
console.log("=".repeat(60));
console.log();

// Check Docker availability upfront if needed
const needsDocker = databases.includes("neo4j") || databases.includes("memgraph");
if (needsDocker) {
  checkDockerAvailable();
}

async function benchmarkDatabase(
  db: DatabaseType,
  index: number,
  total: number
): Promise<DatabaseResult | null> {
  console.log(`[${index}/${total}] ${db.toUpperCase()}`);
  console.log("-".repeat(60));

  const isDockerDb = db === "neo4j" || db === "memgraph";

  try {
    // Start Docker container if needed
    if (isDockerDb) {
      await ensureContainerReady(db);
    }

    // Load data
    let loadTimeSeconds = 0;
    if (!skipLoad) {
      console.log("  Loading data...");
      const loadStart = performance.now();

      if (db === "leangraph") {
        await loadLeanGraph(config, undefined, (msg) => console.log(`  ${msg}`));
      } else if (db === "neo4j") {
        await loadNeo4j(config, (msg) => console.log(`  ${msg}`));
      } else if (db === "memgraph") {
        await loadMemgraph(config, (msg) => console.log(`  ${msg}`));
      }

      loadTimeSeconds = (performance.now() - loadStart) / 1000;
      console.log(`  Load completed in ${formatSeconds(loadTimeSeconds)}`);
    }

    // Create runner and connect
    let runner: Runner;
    let diskBytes = 0;
    let ramBytes = 0;

    const connectStart = performance.now();
    if (db === "leangraph") {
      runner = new LeanGraphRunner();
      await runner.connect();
      diskBytes = getDiskUsage(BENCHMARK_CONFIG.leangraphDataPath);
      ramBytes = getProcessRam();
    } else if (db === "neo4j") {
      runner = new Neo4jRunner();
      await runner.connect();
      ramBytes = await getDockerRam("benchmark-neo4j");
    } else if (db === "memgraph") {
      runner = new MemgraphRunner();
      await runner.connect();
      ramBytes = await getDockerRam("benchmark-memgraph");
    } else {
      throw new Error(`Unknown database: ${db}`);
    }

    const coldStartMs = performance.now() - connectStart;
    const version = await runner.getVersion();

    console.log(`  Version: ${version}`);
    console.log(`  Disk: ${formatBytes(diskBytes)}, RAM: ${formatBytes(ramBytes)}`);
    console.log(`  Cold start: ${formatMs(coldStartMs)}`);

    // Run queries
    const queryResults: QueryResult[] = [];
    const readQueries = getReadQueries(queries);
    const writeQueries = getWriteQueries(queries);

    // Read queries
    console.log("  Running read queries...");
    for (const query of readQueries) {
      resetQueryRng();
      const times: number[] = [];

      // Warmup
      for (let i = 0; i < BENCHMARK_CONFIG.warmupIterations; i++) {
        try {
          await runner.execute(query.cypher, query.params());
        } catch {
          // Ignore warmup errors
        }
      }

      // Measured iterations
      for (let i = 0; i < BENCHMARK_CONFIG.measuredIterations; i++) {
        const params = query.params();
        const start = performance.now();
        try {
          await runner.execute(query.cypher, params);
          times.push(performance.now() - start);
        } catch (err) {
          if (i === 0) {
            console.log(`    ${query.name}: Error - ${err instanceof Error ? err.message : err}`);
          }
        }
      }

      if (times.length > 0) {
        const stats = calculateStats(times);
        queryResults.push({
          name: query.name,
          category: query.category,
          timing: stats,
        });
        console.log(`    ${query.name}: p50=${formatMs(stats.p50)}, p95=${formatMs(stats.p95)}`);
      }
    }

    // Write queries
    console.log("  Running write queries...");
    for (const query of writeQueries) {
      resetQueryRng();
      const times: number[] = [];

      // Warmup
      for (let i = 0; i < BENCHMARK_CONFIG.warmupIterations; i++) {
        try {
          await runner.execute(query.cypher, query.params());
        } catch {
          // Ignore warmup errors
        }
      }

      // Measured iterations
      for (let i = 0; i < BENCHMARK_CONFIG.measuredIterations; i++) {
        const params = query.params();
        const start = performance.now();
        try {
          await runner.execute(query.cypher, params);
          times.push(performance.now() - start);
        } catch (err) {
          if (i === 0) {
            console.log(`    ${query.name}: Error - ${err instanceof Error ? err.message : err}`);
          }
        }
      }

      if (times.length > 0) {
        const stats = calculateStats(times);
        queryResults.push({
          name: query.name,
          category: query.category,
          timing: stats,
        });
        console.log(`    ${query.name}: p50=${formatMs(stats.p50)}, p95=${formatMs(stats.p95)}`);
      }
    }

    // Update RAM measurement after queries (for LeanGraph)
    if (db === "leangraph") {
      ramBytes = getProcessRam();
    }

    // Close connection
    console.log("  Closing connection...");
    await runner.disconnect();

    // Stop Docker container and cleanup volumes
    if (isDockerDb) {
      await stopAndCleanup(db);
    }

    console.log();

    return {
      database: db,
      version,
      load: {
        timeSeconds: loadTimeSeconds,
        nodesLoaded: getTotalNodes(config),
        edgesLoaded: getTotalEdges(config),
      },
      diskBytes,
      ramBytes,
      coldStartMs,
      queries: queryResults,
    };
  } catch (err) {
    console.error(`  Failed: ${err instanceof Error ? err.message : err}`);
    console.log();

    // Cleanup on failure
    if (isDockerDb) {
      try {
        await stopAndCleanup(db);
      } catch {
        // Ignore cleanup errors
      }
    }

    return null;
  }
}

async function runBenchmark(): Promise<BenchmarkResult> {
  const results: DatabaseResult[] = [];

  for (let i = 0; i < databases.length; i++) {
    const db = databases[i];
    const result = await benchmarkDatabase(db, i + 1, databases.length);
    if (result) {
      results.push(result);
    }
  }

  return {
    timestamp: new Date().toISOString(),
    scale,
    config,
    totalNodes: getTotalNodes(config),
    totalEdges: getTotalEdges(config),
    databases: results,
  };
}

// Main execution
runBenchmark()
  .then((results) => {
    console.log("=".repeat(60));
    console.log("BENCHMARK COMPLETE");
    console.log("=".repeat(60));

    // Print summary
    console.log();
    console.log("Summary:");
    for (const db of results.databases) {
      console.log(`  ${db.database}:`);
      console.log(`    Version: ${db.version}`);
      console.log(`    Load time: ${formatSeconds(db.load.timeSeconds)}`);
      console.log(`    Disk: ${formatBytes(db.diskBytes)}`);
      console.log(`    RAM: ${formatBytes(db.ramBytes)}`);
      console.log(`    Cold start: ${formatMs(db.coldStartMs)}`);

      // Average p50 by category
      const byCategory = new Map<string, number[]>();
      for (const q of db.queries) {
        if (!byCategory.has(q.category)) byCategory.set(q.category, []);
        byCategory.get(q.category)!.push(q.timing.p50);
      }
      for (const [cat, times] of byCategory) {
        const avg = times.reduce((a, b) => a + b, 0) / times.length;
        console.log(`    ${cat} avg p50: ${formatMs(avg)}`);
      }
    }

    // Save results
    const outputPath = outputFile || path.join(BENCHMARK_DIR, `results/benchmark-${Date.now()}.json`);
    const dir = path.dirname(outputPath);
    if (!fs.existsSync(dir)) {
      fs.mkdirSync(dir, { recursive: true });
    }
    fs.writeFileSync(outputPath, JSON.stringify(results, null, 2));
    console.log();
    console.log(`Results saved to: ${outputPath}`);
  })
  .catch((err) => {
    console.error("Benchmark failed:", err);
    process.exit(1);
  });
