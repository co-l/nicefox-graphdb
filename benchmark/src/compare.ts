#!/usr/bin/env node

import { parseArgs } from "util";
import { fileURLToPath } from "url";
import * as fs from "fs";
import * as path from "path";
import type { BenchmarkResult, DatabaseResult, QueryResult } from "./types.js";
import { formatMs, formatBytes, formatSeconds } from "./measure.js";

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
  
  // Try exact match first
  const exactPath = path.join(resultsDir, `${name}.json`);
  if (fs.existsSync(exactPath)) {
    return exactPath;
  }
  
  // List available files for error message
  return null;
}

// List available benchmark names
function listAvailableNames(): string[] {
  const version = getLeanGraphVersion();
  const resultsDir = path.join(BENCHMARK_DIR, "results", version);
  
  if (!fs.existsSync(resultsDir)) {
    return [];
  }
  
  return fs.readdirSync(resultsDir)
    .filter(f => f.endsWith(".json"))
    .map(f => f.replace(".json", ""));
}

// Calculate percentage change
function percentChange(before: number, after: number): number {
  if (before === 0) return after === 0 ? 0 : Infinity;
  return ((after - before) / before) * 100;
}

// Format percentage change with color
function formatChange(change: number, lowerIsBetter: boolean = true): string {
  if (!isFinite(change)) {
    return lowerIsBetter ? `${colors.green}new${colors.reset}` : `${colors.red}new${colors.reset}`;
  }
  
  const sign = change > 0 ? "+" : "";
  const formatted = `${sign}${change.toFixed(1)}%`;
  
  if (Math.abs(change) < 1) {
    return `${colors.gray}${formatted}${colors.reset}`;
  }
  
  const isImprovement = lowerIsBetter ? change < 0 : change > 0;
  const color = isImprovement ? colors.green : colors.red;
  return `${color}${formatted}${colors.reset}`;
}

// Pad string to width
function pad(str: string, width: number, align: "left" | "right" = "left"): string {
  // Strip ANSI codes for length calculation
  const stripped = str.replace(/\x1b\[[0-9;]*m/g, "");
  const padding = Math.max(0, width - stripped.length);
  
  if (align === "right") {
    return " ".repeat(padding) + str;
  }
  return str + " ".repeat(padding);
}

// Print a table row
function printRow(cols: string[], widths: number[], aligns: ("left" | "right")[]): void {
  const cells = cols.map((col, i) => pad(col, widths[i], aligns[i]));
  console.log(`| ${cells.join(" | ")} |`);
}

// Print separator
function printSeparator(widths: number[]): void {
  const cells = widths.map(w => "-".repeat(w));
  console.log(`|-${cells.join("-|-")}-|`);
}

// Compare two benchmark results
function compare(baseName: string, targetName: string): void {
  const baseFile = findResultFile(baseName);
  const targetFile = findResultFile(targetName);
  
  if (!baseFile) {
    console.error(`Error: Benchmark '${baseName}' not found.`);
    const available = listAvailableNames();
    if (available.length > 0) {
      console.error(`Available benchmarks: ${available.join(", ")}`);
    }
    process.exit(1);
  }
  
  if (!targetFile) {
    console.error(`Error: Benchmark '${targetName}' not found.`);
    const available = listAvailableNames();
    if (available.length > 0) {
      console.error(`Available benchmarks: ${available.join(", ")}`);
    }
    process.exit(1);
  }
  
  const base: BenchmarkResult = JSON.parse(fs.readFileSync(baseFile, "utf-8"));
  const target: BenchmarkResult = JSON.parse(fs.readFileSync(targetFile, "utf-8"));
  
  // Validate same scale
  if (base.scale !== target.scale) {
    console.warn(`${colors.yellow}Warning: Comparing different scales (${base.scale} vs ${target.scale})${colors.reset}`);
  }
  
  console.log();
  console.log(`${colors.bold}Comparing: ${baseName} -> ${targetName}${colors.reset}`);
  console.log(`${colors.dim}Scale: ${target.scale} (${target.totalNodes.toLocaleString()} nodes, ${target.totalEdges.toLocaleString()} edges)${colors.reset}`);
  console.log();
  
  // Find common databases
  const baseDbMap = new Map(base.databases.map(d => [d.database, d]));
  const targetDbMap = new Map(target.databases.map(d => [d.database, d]));
  const commonDbs = base.databases
    .filter(d => targetDbMap.has(d.database))
    .map(d => d.database);
  
  if (commonDbs.length === 0) {
    console.error("Error: No common databases to compare.");
    process.exit(1);
  }
  
  // Compare each database
  for (const dbName of commonDbs) {
    const baseDb = baseDbMap.get(dbName)!;
    const targetDb = targetDbMap.get(dbName)!;
    
    console.log(`${colors.bold}${colors.cyan}${dbName.toUpperCase()}${colors.reset}`);
    console.log();
    
    // Summary metrics
    console.log(`${colors.dim}Summary:${colors.reset}`);
    const summaryData = [
      ["Total Duration", formatSeconds(baseDb.totalDurationSeconds), formatSeconds(targetDb.totalDurationSeconds), percentChange(baseDb.totalDurationSeconds, targetDb.totalDurationSeconds)],
      ["Load Time", formatSeconds(baseDb.load.timeSeconds), formatSeconds(targetDb.load.timeSeconds), percentChange(baseDb.load.timeSeconds, targetDb.load.timeSeconds)],
      ["Disk Usage", formatBytes(baseDb.afterQueries.diskBytes), formatBytes(targetDb.afterQueries.diskBytes), percentChange(baseDb.afterQueries.diskBytes, targetDb.afterQueries.diskBytes)],
      ["RAM Usage", formatBytes(baseDb.afterQueries.ramBytes), formatBytes(targetDb.afterQueries.ramBytes), percentChange(baseDb.afterQueries.ramBytes, targetDb.afterQueries.ramBytes)],
      ["Cold Start", formatMs(baseDb.coldStartMs), formatMs(targetDb.coldStartMs), percentChange(baseDb.coldStartMs, targetDb.coldStartMs)],
    ];
    
    const summaryWidths = [16, 12, 12, 10];
    const summaryAligns: ("left" | "right")[] = ["left", "right", "right", "right"];
    
    printRow(["Metric", baseName, targetName, "Change"], summaryWidths, summaryAligns);
    printSeparator(summaryWidths);
    for (const [metric, baseVal, targetVal, change] of summaryData) {
      printRow([metric as string, baseVal as string, targetVal as string, formatChange(change as number)], summaryWidths, summaryAligns);
    }
    console.log();
    
    // Query performance
    console.log(`${colors.dim}Query Performance (p50):${colors.reset}`);
    
    const baseQueryMap = new Map(baseDb.queries.map(q => [q.name, q]));
    const targetQueryMap = new Map(targetDb.queries.map(q => [q.name, q]));
    
    // Get all query names, preserving order from target
    const allQueryNames = new Set([
      ...targetDb.queries.map(q => q.name),
      ...baseDb.queries.map(q => q.name),
    ]);
    
    // Group by category
    const categories = ["lookup", "pattern", "aggregation", "traversal", "write"];
    
    let improved = 0;
    let regressed = 0;
    let unchanged = 0;
    
    const queryWidths = [28, 12, 12, 10];
    const queryAligns: ("left" | "right")[] = ["left", "right", "right", "right"];
    
    printRow(["Query", baseName, targetName, "Change"], queryWidths, queryAligns);
    printSeparator(queryWidths);
    
    for (const category of categories) {
      const categoryQueries = [...allQueryNames].filter(name => {
        const q = targetQueryMap.get(name) || baseQueryMap.get(name);
        return q?.category === category;
      });
      
      if (categoryQueries.length === 0) continue;
      
      for (const queryName of categoryQueries) {
        const baseQ = baseQueryMap.get(queryName);
        const targetQ = targetQueryMap.get(queryName);
        
        const baseP50 = baseQ?.timing.p50;
        const targetP50 = targetQ?.timing.p50;
        
        const baseStr = baseP50 !== undefined ? formatMs(baseP50) : "N/A";
        const targetStr = targetP50 !== undefined ? formatMs(targetP50) : "N/A";
        
        let changeStr: string;
        if (baseP50 !== undefined && targetP50 !== undefined) {
          const change = percentChange(baseP50, targetP50);
          changeStr = formatChange(change);
          
          if (change < -5) improved++;
          else if (change > 5) regressed++;
          else unchanged++;
        } else if (baseP50 === undefined && targetP50 !== undefined) {
          changeStr = `${colors.cyan}new${colors.reset}`;
        } else if (baseP50 !== undefined && targetP50 === undefined) {
          changeStr = `${colors.yellow}removed${colors.reset}`;
        } else {
          changeStr = "-";
        }
        
        printRow([queryName, baseStr, targetStr, changeStr], queryWidths, queryAligns);
      }
    }
    
    console.log();
    
    // Summary line
    const parts: string[] = [];
    if (improved > 0) parts.push(`${colors.green}${improved} improved${colors.reset}`);
    if (regressed > 0) parts.push(`${colors.red}${regressed} regressed${colors.reset}`);
    if (unchanged > 0) parts.push(`${colors.gray}${unchanged} unchanged${colors.reset}`);
    
    console.log(`Summary: ${parts.join(", ")}`);
    console.log();
  }
}

// Parse arguments
const { values, positionals } = parseArgs({
  options: {
    help: { type: "boolean", short: "h" },
    list: { type: "boolean", short: "l" },
  },
  allowPositionals: true,
});

if (values.help) {
  console.log(`
Benchmark Comparison Tool

Usage: npm run benchmark:compare <baseline> <target>

Compare two named benchmark runs and show performance differences.

Options:
  -l, --list    List available benchmark names
  -h, --help    Show this help

Examples:
  npm run benchmark:compare baseline optimized
  npm run benchmark:compare before-refactor after-refactor
`);
  process.exit(0);
}

if (values.list) {
  const available = listAvailableNames();
  if (available.length === 0) {
    console.log("No benchmark results found. Run a benchmark first:");
    console.log("  npm run benchmark -- -s micro -d leangraph --name baseline");
  } else {
    console.log("Available benchmarks:");
    for (const name of available) {
      console.log(`  ${name}`);
    }
  }
  process.exit(0);
}

if (positionals.length !== 2) {
  console.error("Error: Expected exactly two benchmark names to compare.");
  console.error("Usage: npm run benchmark:compare <baseline> <target>");
  console.error("Run with --list to see available benchmarks.");
  process.exit(1);
}

compare(positionals[0], positionals[1]);
