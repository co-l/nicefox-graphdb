// CLI Helper Functions

import * as fs from "fs";
import * as path from "path";

// ============================================================================
// Types
// ============================================================================

export interface ApiKeyConfig {
  project?: string;
  env?: string;
  admin?: boolean;
}

// ============================================================================
// Formatting Helpers
// ============================================================================

export function formatBytes(bytes: number): string {
  if (bytes === 0) return "0 B";
  const k = 1024;
  const sizes = ["B", "KB", "MB", "GB"];
  const i = Math.floor(Math.log(bytes) / Math.log(k));
  return `${parseFloat((bytes / Math.pow(k, i)).toFixed(2))} ${sizes[i]}`;
}

export function formatValue(val: unknown): string {
  if (val === null) return "null";
  if (val === undefined) return "";
  if (typeof val === "object") return JSON.stringify(val);
  return String(val);
}

// ============================================================================
// API Key Helpers
// ============================================================================

export function getApiKeysPath(dataPath: string): string {
  return path.join(dataPath, "api-keys.json");
}

export function loadApiKeys(dataPath: string): Record<string, ApiKeyConfig> {
  const keysFile = getApiKeysPath(dataPath);
  if (fs.existsSync(keysFile)) {
    try {
      return JSON.parse(fs.readFileSync(keysFile, "utf-8"));
    } catch {
      return {};
    }
  }
  return {};
}

export function saveApiKeys(dataPath: string, keys: Record<string, ApiKeyConfig>): void {
  const keysFile = getApiKeysPath(dataPath);
  fs.writeFileSync(keysFile, JSON.stringify(keys, null, 2) + "\n");
}

// ============================================================================
// Directory Helpers
// ============================================================================

export function ensureDataDir(dataPath: string): void {
  if (!fs.existsSync(dataPath)) {
    fs.mkdirSync(dataPath, { recursive: true });
  }
  // Ensure env subdirs exist
  for (const env of ["production", "test"]) {
    const envPath = path.join(dataPath, env);
    if (!fs.existsSync(envPath)) {
      fs.mkdirSync(envPath, { recursive: true });
    }
  }
}

// ============================================================================
// Table Formatting
// ============================================================================

export function formatTableRow(
  columns: string[],
  row: Record<string, unknown>,
  widths: Record<string, number>
): string {
  return columns
    .map((col) => {
      const val = formatValue(row[col]);
      return val.slice(0, widths[col]).padEnd(widths[col]);
    })
    .join(" | ");
}

export function calculateColumnWidths(
  columns: string[],
  rows: Record<string, unknown>[],
  maxWidth: number = 40
): Record<string, number> {
  const widths: Record<string, number> = {};
  
  for (const col of columns) {
    widths[col] = col.length;
  }
  
  for (const row of rows) {
    for (const col of columns) {
      const val = formatValue(row[col]);
      widths[col] = Math.max(widths[col], val.length);
    }
  }
  
  // Cap max width
  for (const col of columns) {
    widths[col] = Math.min(widths[col], maxWidth);
  }
  
  return widths;
}

// ============================================================================
// Project Helpers
// ============================================================================

export function listProjects(dataPath: string): Map<string, string[]> {
  const projects = new Map<string, string[]>();

  for (const env of ["production", "test"]) {
    const envPath = path.join(dataPath, env);
    if (fs.existsSync(envPath)) {
      const files = fs.readdirSync(envPath).filter((f) => f.endsWith(".db"));
      for (const file of files) {
        const project = file.replace(".db", "");
        if (!projects.has(project)) {
          projects.set(project, []);
        }
        projects.get(project)!.push(env);
      }
    }
  }

  return projects;
}

export function projectExists(dataPath: string, project: string, env?: string): boolean {
  if (env) {
    const dbPath = path.join(dataPath, env, `${project}.db`);
    return fs.existsSync(dbPath);
  }
  
  // Check both environments
  return (
    fs.existsSync(path.join(dataPath, "production", `${project}.db`)) ||
    fs.existsSync(path.join(dataPath, "test", `${project}.db`))
  );
}

export function getProjectKeyCount(
  keys: Record<string, ApiKeyConfig>,
  project: string
): number {
  return Object.values(keys).filter((config) => config.project === project).length;
}
