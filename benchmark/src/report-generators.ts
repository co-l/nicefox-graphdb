import * as fs from "fs";
import * as path from "path";
import type { BenchmarkResult } from "./types.js";
import { formatBytes, formatMs } from "./measure.js";

/**
 * Generate Markdown report
 */
export function generateMarkdown(results: BenchmarkResult): string {
  const lines: string[] = [];

  lines.push("# LeanGraph Benchmark Results");
  lines.push("");
  lines.push(`**Date:** ${new Date(results.timestamp).toLocaleString()}`);
  lines.push(`**Scale:** ${results.scale} (${results.totalNodes.toLocaleString()} nodes, ${results.totalEdges.toLocaleString()} edges)`);
  lines.push("");

  // Summary table
  lines.push("## Summary");
  lines.push("");
  lines.push("| Metric | " + results.databases.map((d) => d.database).join(" | ") + " |");
  lines.push("|--------|" + results.databases.map(() => "--------").join("|") + "|");

  lines.push("| Version | " + results.databases.map((d) => d.version).join(" | ") + " |");
  lines.push("| Disk (before) | " + results.databases.map((d) => formatBytes(d.beforeQueries.diskBytes)).join(" | ") + " |");
  lines.push("| Disk (after) | " + results.databases.map((d) => formatBytes(d.afterQueries.diskBytes)).join(" | ") + " |");
  lines.push("| RAM (before) | " + results.databases.map((d) => formatBytes(d.beforeQueries.ramBytes)).join(" | ") + " |");
  lines.push("| RAM (after) | " + results.databases.map((d) => formatBytes(d.afterQueries.ramBytes)).join(" | ") + " |");
  lines.push("| Cold Start | " + results.databases.map((d) => formatMs(d.coldStartMs)).join(" | ") + " |");
  lines.push("");

  // Query performance by category
  const categories = ["lookup", "pattern", "aggregation", "traversal", "write"];
  for (const cat of categories) {
    lines.push(`## ${cat.charAt(0).toUpperCase() + cat.slice(1)} Queries`);
    lines.push("");

    // Find queries in this category
    const queryNames = new Set<string>();
    for (const db of results.databases) {
      for (const q of db.queries) {
        if (q.category === cat) queryNames.add(q.name);
      }
    }

    if (queryNames.size === 0) continue;

    lines.push("| Query | " + results.databases.map((d) => `${d.database} p50`).join(" | ") + " | " + results.databases.map((d) => `${d.database} p95`).join(" | ") + " |");
    lines.push("|-------|" + results.databases.map(() => "--------").join("|") + "|" + results.databases.map(() => "--------").join("|") + "|");

    for (const qName of queryNames) {
      const p50s = results.databases.map((d) => {
        const q = d.queries.find((q) => q.name === qName);
        return q ? formatMs(q.timing.p50) : "N/A";
      });
      const p95s = results.databases.map((d) => {
        const q = d.queries.find((q) => q.name === qName);
        return q ? formatMs(q.timing.p95) : "N/A";
      });
      lines.push(`| ${qName} | ${p50s.join(" | ")} | ${p95s.join(" | ")} |`);
    }
    lines.push("");
  }

  return lines.join("\n");
}

/**
 * Generate HTML report
 */
export function generateHtml(results: BenchmarkResult): string {
  // Find best values for highlighting (use afterQueries for final state)
  const bestDiskBefore = Math.min(...results.databases.map((d) => d.beforeQueries.diskBytes || Infinity));
  const bestDiskAfter = Math.min(...results.databases.map((d) => d.afterQueries.diskBytes || Infinity));
  const bestRamBefore = Math.min(...results.databases.map((d) => d.beforeQueries.ramBytes || Infinity));
  const bestRamAfter = Math.min(...results.databases.map((d) => d.afterQueries.ramBytes || Infinity));

  let html = `<!DOCTYPE html>
<html>
<head>
  <meta charset="utf-8">
  <title>LeanGraph Benchmark Results</title>
  <style>
    body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; max-width: 1200px; margin: 0 auto; padding: 2rem; background: #0d1117; color: #e6edf3; }
    h1, h2 { color: #58a6ff; }
    table { width: 100%; border-collapse: collapse; margin: 1rem 0; background: #161b22; border-radius: 8px; overflow: hidden; }
    th, td { padding: 0.75rem 1rem; text-align: left; border-bottom: 1px solid #30363d; }
    th { background: #21262d; color: #8b949e; font-weight: 600; }
    tr:last-child td { border-bottom: none; }
    .best { color: #3fb950; font-weight: 600; }
    .metric { color: #8b949e; }
    code { background: #21262d; padding: 0.2rem 0.4rem; border-radius: 4px; font-size: 0.9em; }
  </style>
</head>
<body>
  <h1>LeanGraph Benchmark Results</h1>
  <p><strong>Date:</strong> ${new Date(results.timestamp).toLocaleString()}</p>
  <p><strong>Scale:</strong> ${results.scale} (${results.totalNodes.toLocaleString()} nodes, ${results.totalEdges.toLocaleString()} edges)</p>

  <h2>Summary</h2>
  <table>
    <thead>
      <tr>
        <th>Metric</th>
        ${results.databases.map((d) => `<th>${d.database}</th>`).join("\n        ")}
      </tr>
    </thead>
    <tbody>
      <tr>
        <td class="metric">Version</td>
        ${results.databases.map((d) => `<td>${d.version}</td>`).join("\n        ")}
      </tr>
      <tr>
        <td class="metric">Disk (before)</td>
        ${results.databases.map((d) => `<td${d.beforeQueries.diskBytes === bestDiskBefore ? ' class="best"' : ''}>${formatBytes(d.beforeQueries.diskBytes)}</td>`).join("\n        ")}
      </tr>
      <tr>
        <td class="metric">Disk (after)</td>
        ${results.databases.map((d) => `<td${d.afterQueries.diskBytes === bestDiskAfter ? ' class="best"' : ''}>${formatBytes(d.afterQueries.diskBytes)}</td>`).join("\n        ")}
      </tr>
      <tr>
        <td class="metric">RAM (before)</td>
        ${results.databases.map((d) => `<td${d.beforeQueries.ramBytes === bestRamBefore ? ' class="best"' : ''}>${formatBytes(d.beforeQueries.ramBytes)}</td>`).join("\n        ")}
      </tr>
      <tr>
        <td class="metric">RAM (after)</td>
        ${results.databases.map((d) => `<td${d.afterQueries.ramBytes === bestRamAfter ? ' class="best"' : ''}>${formatBytes(d.afterQueries.ramBytes)}</td>`).join("\n        ")}
      </tr>
      <tr>
        <td class="metric">Cold Start</td>
        ${results.databases.map((d) => `<td>${formatMs(d.coldStartMs)}</td>`).join("\n        ")}
      </tr>
    </tbody>
  </table>
`;

  // Query tables by category
  const categories = ["lookup", "pattern", "aggregation", "traversal", "write"];
  for (const cat of categories) {
    const queryNames = new Set<string>();
    for (const db of results.databases) {
      for (const q of db.queries) {
        if (q.category === cat) queryNames.add(q.name);
      }
    }
    if (queryNames.size === 0) continue;

    html += `
  <h2>${cat.charAt(0).toUpperCase() + cat.slice(1)} Queries</h2>
  <table>
    <thead>
      <tr>
        <th>Query</th>
        ${results.databases.map((d) => `<th>${d.database} p50</th>`).join("\n        ")}
        ${results.databases.map((d) => `<th>${d.database} p95</th>`).join("\n        ")}
      </tr>
    </thead>
    <tbody>`;

    for (const qName of queryNames) {
      const p50Values = results.databases.map((d) => {
        const q = d.queries.find((q) => q.name === qName);
        return q?.timing.p50 ?? Infinity;
      });
      const bestP50 = Math.min(...p50Values);

      html += `
      <tr>
        <td><code>${qName}</code></td>`;

      for (let i = 0; i < results.databases.length; i++) {
        const q = results.databases[i].queries.find((q) => q.name === qName);
        const isBest = p50Values[i] === bestP50;
        html += `
        <td${isBest ? ' class="best"' : ''}>${q ? formatMs(q.timing.p50) : "N/A"}</td>`;
      }
      for (let i = 0; i < results.databases.length; i++) {
        const q = results.databases[i].queries.find((q) => q.name === qName);
        html += `
        <td>${q ? formatMs(q.timing.p95) : "N/A"}</td>`;
      }
      html += `
      </tr>`;
    }

    html += `
    </tbody>
  </table>`;
  }

  html += `
</body>
</html>`;

  return html;
}

/**
 * Generate landing page HTML snippet
 */
export function generateSnippet(results: BenchmarkResult): string {
  // Find best values (use afterQueries for final state)
  const findBest = (getter: (d: typeof results.databases[0]) => number) => {
    let best = Infinity;
    let bestDb = "";
    for (const d of results.databases) {
      const val = getter(d);
      if (val > 0 && val < best) {
        best = val;
        bestDb = d.database;
      }
    }
    return bestDb;
  };

  const bestDisk = findBest((d) => d.afterQueries.diskBytes);
  const bestRam = findBest((d) => d.afterQueries.ramBytes);

  // Get average p50 for lookups
  const getAvgLookupP50 = (db: typeof results.databases[0]) => {
    const lookups = db.queries.filter((q) => q.category === "lookup");
    if (lookups.length === 0) return 0;
    return lookups.reduce((sum, q) => sum + q.timing.p50, 0) / lookups.length;
  };
  const bestLookup = findBest((d) => getAvgLookupP50(d) || Infinity);

  const html = `<!-- Auto-generated by benchmark - ${new Date().toISOString()} -->
<!-- Scale: ${results.scale} (${results.totalNodes.toLocaleString()} nodes, ${results.totalEdges.toLocaleString()} edges) -->
<table>
  <thead>
    <tr>
      <th>Metric</th>
${results.databases.map((d) => `      <th>${d.database.charAt(0).toUpperCase() + d.database.slice(1)}</th>`).join("\n")}
    </tr>
  </thead>
  <tbody>
    <tr>
      <td>Disk Usage (${results.scale})</td>
${results.databases.map((d) => `      <td${d.database === bestDisk ? ' class="check"' : ''}>${formatBytes(d.afterQueries.diskBytes)}</td>`).join("\n")}
    </tr>
    <tr>
      <td>RAM Usage</td>
${results.databases.map((d) => `      <td${d.database === bestRam ? ' class="check"' : ''}>${formatBytes(d.afterQueries.ramBytes)}</td>`).join("\n")}
    </tr>
    <tr>
      <td>Lookup Query p50</td>
${results.databases.map((d) => {
  const avg = getAvgLookupP50(d);
  return `      <td${d.database === bestLookup ? ' class="check"' : ''}>${avg > 0 ? formatMs(avg) : "N/A"}</td>`;
}).join("\n")}
    </tr>
    <tr>
      <td>Cold Start</td>
${results.databases.map((d) => `      <td>${formatMs(d.coldStartMs)}</td>`).join("\n")}
    </tr>
  </tbody>
</table>`;

  return html;
}

/**
 * Write all reports to disk
 */
export function writeReports(
  results: BenchmarkResult,
  outputPrefix: string,
  options: { json?: boolean; markdown?: boolean; html?: boolean; snippet?: boolean } = {}
): string[] {
  const { json = true, markdown = true, html = true, snippet = true } = options;
  const written: string[] = [];

  const dir = path.dirname(outputPrefix);
  if (!fs.existsSync(dir)) {
    fs.mkdirSync(dir, { recursive: true });
  }

  if (json) {
    const jsonPath = `${outputPrefix}.json`;
    fs.writeFileSync(jsonPath, JSON.stringify(results, null, 2));
    written.push(jsonPath);
  }

  if (markdown) {
    const mdPath = `${outputPrefix}.md`;
    fs.writeFileSync(mdPath, generateMarkdown(results));
    written.push(mdPath);
  }

  if (html) {
    const htmlPath = `${outputPrefix}.html`;
    fs.writeFileSync(htmlPath, generateHtml(results));
    written.push(htmlPath);
  }

  if (snippet) {
    const snippetPath = `${outputPrefix}-snippet.html`;
    fs.writeFileSync(snippetPath, generateSnippet(results));
    written.push(snippetPath);
  }

  return written;
}

/**
 * Generate timestamp string for filenames: YYYYMMDDHHmm
 */
export function formatTimestamp(date: Date = new Date()): string {
  const pad = (n: number) => n.toString().padStart(2, "0");
  return `${date.getFullYear()}${pad(date.getMonth() + 1)}${pad(date.getDate())}${pad(date.getHours())}${pad(date.getMinutes())}`;
}
