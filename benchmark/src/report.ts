#!/usr/bin/env node

import { parseArgs } from "util";
import { fileURLToPath } from "url";
import * as fs from "fs";
import * as path from "path";
import type { BenchmarkResult } from "./types.js";
import { writeReports } from "./report-generators.js";

// Get benchmark directory path (works regardless of CWD)
const __dirname = path.dirname(fileURLToPath(import.meta.url));
const BENCHMARK_DIR = path.resolve(__dirname, "..");

const { values } = parseArgs({
  options: {
    input: { type: "string", short: "i" },
    format: { type: "string", short: "f", default: "all" },
    output: { type: "string", short: "o" },
    help: { type: "boolean", short: "h" },
  },
});

if (values.help) {
  console.log(`
Benchmark Report Generator

Usage: npm run benchmark:report [options]

Options:
  -i, --input <file>     Input JSON results file (default: latest in results/)
  -f, --format <type>    Output format: json, html, markdown, all (default: all)
  -o, --output <prefix>  Output file prefix (default: same as input file)
  -h, --help             Show this help
`);
  process.exit(0);
}

// Find latest results file in version folders
function findLatestResults(): string | null {
  const resultsDir = path.join(BENCHMARK_DIR, "results");
  if (!fs.existsSync(resultsDir)) return null;

  // Look for version folders first, then JSON files
  const entries = fs.readdirSync(resultsDir, { withFileTypes: true });
  
  // Collect all JSON files from version folders and root
  const jsonFiles: { path: string; mtime: number }[] = [];
  
  for (const entry of entries) {
    if (entry.isDirectory()) {
      // Version folder - look for JSON files inside
      const versionDir = path.join(resultsDir, entry.name);
      const files = fs.readdirSync(versionDir).filter((f) => f.endsWith(".json"));
      for (const file of files) {
        const fullPath = path.join(versionDir, file);
        const stat = fs.statSync(fullPath);
        jsonFiles.push({ path: fullPath, mtime: stat.mtimeMs });
      }
    } else if (entry.name.endsWith(".json")) {
      // Legacy JSON file in root
      const fullPath = path.join(resultsDir, entry.name);
      const stat = fs.statSync(fullPath);
      jsonFiles.push({ path: fullPath, mtime: stat.mtimeMs });
    }
  }

  if (jsonFiles.length === 0) return null;

  // Return most recent
  jsonFiles.sort((a, b) => b.mtime - a.mtime);
  return jsonFiles[0].path;
}

const inputFile = (values.input as string) || findLatestResults();
if (!inputFile || !fs.existsSync(inputFile)) {
  console.error("No results file found. Run the benchmark first.");
  process.exit(1);
}

const results: BenchmarkResult = JSON.parse(fs.readFileSync(inputFile, "utf-8"));
const format = values.format as string;

// Default output prefix is same as input file (without .json)
const defaultOutputPrefix = inputFile.replace(/\.json$/, "");
const outputPrefix = (values.output as string) || defaultOutputPrefix;

console.log(`Generating report from: ${inputFile}`);
console.log(`Format: ${format}`);

const options = {
  json: format === "all" || format === "json",
  markdown: format === "all" || format === "markdown",
  html: format === "all" || format === "html",
  snippet: format === "all" || format === "html",
};

const written = writeReports(results, outputPrefix, options);
for (const file of written) {
  console.log(`Written: ${file}`);
}

console.log("Done!");
