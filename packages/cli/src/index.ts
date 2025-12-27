#!/usr/bin/env node

import { Command } from "commander";
import { serve } from "@hono/node-server";
import * as fs from "fs";
import * as path from "path";
import {
  createServer,
  GraphDatabase,
  Executor,
  BackupManager,
  VERSION,
} from "@nicefox/graphdb";

const program = new Command();

program
  .name("nicefox-graphdb")
  .description("NiceFox GraphDB - SQLite-based graph database with Cypher queries")
  .version(VERSION);

// ============================================================================
// serve - Start the HTTP server
// ============================================================================

program
  .command("serve")
  .description("Start the NiceFox GraphDB HTTP server")
  .option("-p, --port <port>", "Port to listen on", "3000")
  .option("-d, --data <path>", "Data directory for databases", "./data")
  .option("-H, --host <host>", "Host to bind to", "localhost")
  .option("-b, --backup <path>", "Backup directory (enables backup endpoints)")
  .option("-k, --api-keys <file>", "JSON file containing API keys")
  .action(async (options: { port: string; data: string; host: string; backup?: string; apiKeys?: string }) => {
    const port = parseInt(options.port, 10);
    const dataPath = path.resolve(options.data);
    const host = options.host;
    const backupPath = options.backup ? path.resolve(options.backup) : undefined;

    // Ensure data directory exists
    ensureDataDir(dataPath);

    // Load API keys from file or environment
    let apiKeys: Record<string, { project?: string; env?: string; admin?: boolean }> | undefined;
    
    if (options.apiKeys) {
      try {
        const keyFile = path.resolve(options.apiKeys);
        const content = fs.readFileSync(keyFile, "utf-8");
        apiKeys = JSON.parse(content);
        console.log(`Loaded ${Object.keys(apiKeys!).length} API key(s) from ${keyFile}`);
      } catch (err) {
        console.error(`Failed to load API keys from ${options.apiKeys}:`, err);
        process.exit(1);
      }
    } else if (process.env.API_KEYS) {
      // Load from environment variable (JSON format)
      try {
        apiKeys = JSON.parse(process.env.API_KEYS);
        console.log(`Loaded ${Object.keys(apiKeys || {}).length} API key(s) from environment`);
      } catch (err) {
        console.error("Failed to parse API_KEYS environment variable:", err);
        process.exit(1);
      }
    }

    const { app, dbManager } = createServer({ 
      port, 
      dataPath,
      backupPath,
      apiKeys,
    });

    const authStatus = apiKeys ? "enabled" : "disabled";
    const backupStatus = backupPath ? backupPath.slice(0, 30) : "disabled";

    console.log(`
╔═══════════════════════════════════════════════════════════╗
║              NiceFox GraphDB Server v${VERSION}                ║
╠═══════════════════════════════════════════════════════════╣
║  Endpoint:  http://${host}:${port.toString().padEnd(5)}                         ║
║  Data:      ${dataPath.slice(0, 43).padEnd(43)} ║
║  Backups:   ${backupStatus.padEnd(43)} ║
║  Auth:      ${authStatus.padEnd(43)} ║
║                                                           ║
║  Routes:                                                  ║
║    POST /query/:env/:project  - Execute Cypher queries    ║
║    GET  /health               - Health check              ║
║    GET  /admin/list           - List all projects         ║
║    GET  /admin/backup         - Backup status             ║
║    POST /admin/backup         - Trigger backup            ║
╚═══════════════════════════════════════════════════════════╝
`);

    serve({
      fetch: app.fetch,
      port,
      hostname: host,
    });

    // Handle graceful shutdown
    process.on("SIGINT", () => {
      console.log("\nShutting down...");
      dbManager.closeAll();
      process.exit(0);
    });

    process.on("SIGTERM", () => {
      console.log("\nShutting down...");
      dbManager.closeAll();
      process.exit(0);
    });
  });

// ============================================================================
// create - Create a new project (both production and test DBs)
// ============================================================================

program
  .command("create <project>")
  .description("Create a new project (creates both production and test databases)")
  .option("-d, --data <path>", "Data directory for databases", "./data")
  .action((project: string, options: { data: string }) => {
    const dataPath = path.resolve(options.data);
    ensureDataDir(dataPath);

    const envs = ["production", "test"];

    for (const env of envs) {
      const dbPath = path.join(dataPath, env, `${project}.db`);
      const dbDir = path.dirname(dbPath);

      if (!fs.existsSync(dbDir)) {
        fs.mkdirSync(dbDir, { recursive: true });
      }

      if (fs.existsSync(dbPath)) {
        console.log(`  [skip] ${env}/${project}.db already exists`);
      } else {
        const db = new GraphDatabase(dbPath);
        db.initialize();
        db.close();
        console.log(`  [created] ${env}/${project}.db`);
      }
    }

    console.log(`\nProject '${project}' is ready.`);
  });

// ============================================================================
// delete - Delete a project (both production and test DBs)
// ============================================================================

program
  .command("delete <project>")
  .description("Delete a project (removes both production and test databases)")
  .option("-d, --data <path>", "Data directory for databases", "./data")
  .option("-f, --force", "Skip confirmation prompt", false)
  .action((project: string, options: { data: string; force: boolean }) => {
    const dataPath = path.resolve(options.data);
    const envs = ["production", "test"];

    // Check what exists
    const existing: string[] = [];
    for (const env of envs) {
      const dbPath = path.join(dataPath, env, `${project}.db`);
      if (fs.existsSync(dbPath)) {
        existing.push(`${env}/${project}.db`);
      }
    }

    if (existing.length === 0) {
      console.log(`Project '${project}' does not exist.`);
      process.exit(1);
    }

    if (!options.force) {
      console.log(`This will delete:`);
      for (const file of existing) {
        console.log(`  - ${file}`);
      }
      console.log(`\nUse --force to confirm deletion.`);
      process.exit(1);
    }

    // Delete files
    for (const env of envs) {
      const dbPath = path.join(dataPath, env, `${project}.db`);
      if (fs.existsSync(dbPath)) {
        fs.unlinkSync(dbPath);
        console.log(`  [deleted] ${env}/${project}.db`);
      }
    }

    console.log(`\nProject '${project}' has been deleted.`);
  });

// ============================================================================
// list - List all projects
// ============================================================================

program
  .command("list")
  .description("List all projects and their environments")
  .option("-d, --data <path>", "Data directory for databases", "./data")
  .action((options: { data: string }) => {
    const dataPath = path.resolve(options.data);

    if (!fs.existsSync(dataPath)) {
      console.log("No data directory found. Run 'nicefox-graphdb create <project>' first.");
      return;
    }

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

    if (projects.size === 0) {
      console.log("No projects found.");
      return;
    }

    console.log("\nProjects:\n");
    for (const [project, envs] of projects) {
      const envList = envs.map((e) => (e === "production" ? "prod" : "test")).join(", ");
      console.log(`  ${project} [${envList}]`);
    }
    console.log("");
  });

// ============================================================================
// query - Execute a Cypher query
// ============================================================================

program
  .command("query <env> <project> <cypher>")
  .description("Execute a Cypher query against a project database")
  .option("-d, --data <path>", "Data directory for databases", "./data")
  .option("-p, --params <json>", "Query parameters as JSON", "{}")
  .option("--json", "Output raw JSON", false)
  .action((env: string, project: string, cypher: string, options: { data: string; params: string; json: boolean }) => {
    const dataPath = path.resolve(options.data);

    if (env !== "production" && env !== "test") {
      console.error(`Invalid environment: ${env}. Must be 'production' or 'test'.`);
      process.exit(1);
    }

    const dbPath = path.join(dataPath, env, `${project}.db`);

    if (!fs.existsSync(dbPath)) {
      console.error(`Database not found: ${dbPath}`);
      console.error(`Run 'nicefox-graphdb create ${project}' first.`);
      process.exit(1);
    }

    let params: Record<string, unknown> = {};
    try {
      params = JSON.parse(options.params);
    } catch {
      console.error("Invalid JSON in --params");
      process.exit(1);
    }

    const db = new GraphDatabase(dbPath);
    db.initialize();

    const executor = new Executor(db);
    const result = executor.execute(cypher, params);

    db.close();

    if (!result.success) {
      console.error(`Query failed: ${result.error.message}`);
      if (result.error.position !== undefined) {
        console.error(`  at position ${result.error.position} (line ${result.error.line}, column ${result.error.column})`);
      }
      process.exit(1);
    }

    if (options.json) {
      console.log(JSON.stringify(result, null, 2));
    } else {
      console.log(`\nResults (${result.meta.count} rows, ${result.meta.time_ms}ms):\n`);

      if (result.data.length === 0) {
        console.log("  (no results)");
      } else {
        // Print as table
        const columns = Object.keys(result.data[0]);
        printTable(columns, result.data);
      }
      console.log("");
    }
  });

// ============================================================================
// wipe - Wipe a test database (refuses on production)
// ============================================================================

program
  .command("wipe <project>")
  .description("Wipe test database for a project (production is protected)")
  .option("-d, --data <path>", "Data directory for databases", "./data")
  .option("-f, --force", "Skip confirmation prompt", false)
  .action((project: string, options: { data: string; force: boolean }) => {
    const dataPath = path.resolve(options.data);
    const dbPath = path.join(dataPath, "test", `${project}.db`);

    if (!fs.existsSync(dbPath)) {
      console.error(`Test database not found: ${dbPath}`);
      process.exit(1);
    }

    if (!options.force) {
      console.log(`This will delete all data in test/${project}.db`);
      console.log(`\nUse --force to confirm.`);
      process.exit(1);
    }

    const db = new GraphDatabase(dbPath);
    db.initialize();
    db.execute("DELETE FROM edges");
    db.execute("DELETE FROM nodes");
    db.close();

    console.log(`Wiped test/${project}.db`);
  });

// ============================================================================
// clone - Clone production to test
// ============================================================================

program
  .command("clone <project>")
  .description("Clone production database to test (overwrites test)")
  .option("-d, --data <path>", "Data directory for databases", "./data")
  .option("-f, --force", "Skip confirmation prompt", false)
  .action((project: string, options: { data: string; force: boolean }) => {
    const dataPath = path.resolve(options.data);
    const prodPath = path.join(dataPath, "production", `${project}.db`);
    const testPath = path.join(dataPath, "test", `${project}.db`);

    if (!fs.existsSync(prodPath)) {
      console.error(`Production database not found: ${prodPath}`);
      process.exit(1);
    }

    if (!options.force) {
      console.log(`This will overwrite test/${project}.db with production/${project}.db`);
      console.log(`\nUse --force to confirm.`);
      process.exit(1);
    }

    // Ensure test directory exists
    const testDir = path.dirname(testPath);
    if (!fs.existsSync(testDir)) {
      fs.mkdirSync(testDir, { recursive: true });
    }

    // Copy file
    fs.copyFileSync(prodPath, testPath);

    console.log(`Cloned production/${project}.db → test/${project}.db`);
  });

// ============================================================================
// backup - Backup databases
// ============================================================================

program
  .command("backup")
  .description("Backup production databases")
  .option("-d, --data <path>", "Data directory for databases", "./data")
  .option("-o, --output <path>", "Backup output directory", "./backups")
  .option("-p, --project <name>", "Backup specific project only")
  .option("--include-test", "Also backup test databases", false)
  .option("--keep <count>", "Number of backups to keep per project", "5")
  .option("--status", "Show backup status only", false)
  .action(async (options: { data: string; output: string; project?: string; includeTest: boolean; keep: string; status: boolean }) => {
    const dataPath = path.resolve(options.data);
    const backupPath = path.resolve(options.output);
    const keepCount = parseInt(options.keep, 10);

    const manager = new BackupManager(backupPath);

    // Status only mode
    if (options.status) {
      const status = manager.getBackupStatus();
      console.log("\nBackup Status:\n");
      console.log(`  Total backups:  ${status.totalBackups}`);
      console.log(`  Total size:     ${formatBytes(status.totalSizeBytes)}`);
      console.log(`  Projects:       ${status.projects.join(", ") || "(none)"}`);
      if (status.oldestBackup) {
        console.log(`  Oldest backup:  ${status.oldestBackup}`);
      }
      if (status.newestBackup) {
        console.log(`  Newest backup:  ${status.newestBackup}`);
      }
      console.log("");
      return;
    }

    // Check data directory exists
    if (!fs.existsSync(dataPath)) {
      console.error(`Data directory not found: ${dataPath}`);
      process.exit(1);
    }

    // Single project backup
    if (options.project) {
      const sourcePath = path.join(dataPath, "production", `${options.project}.db`);
      if (!fs.existsSync(sourcePath)) {
        console.error(`Project not found: ${options.project}`);
        process.exit(1);
      }

      console.log(`Backing up ${options.project}...`);
      const result = await manager.backupDatabase(sourcePath, options.project);

      if (result.success) {
        console.log(`  [success] ${result.backupPath}`);
        console.log(`  Size: ${formatBytes(result.sizeBytes || 0)}, Duration: ${result.durationMs}ms`);
        
        // Cleanup old backups
        const deleted = manager.cleanOldBackups(options.project, keepCount);
        if (deleted > 0) {
          console.log(`  Cleaned up ${deleted} old backup(s)`);
        }
      } else {
        console.error(`  [failed] ${result.error}`);
        process.exit(1);
      }
      return;
    }

    // Backup all databases
    console.log(`\nBacking up databases from ${dataPath}...\n`);
    const results = await manager.backupAll(dataPath, { includeTest: options.includeTest });

    if (results.length === 0) {
      console.log("No databases found to backup.");
      return;
    }

    let successCount = 0;
    let failCount = 0;

    for (const result of results) {
      if (result.success) {
        console.log(`  [success] ${result.project} → ${path.basename(result.backupPath!)}`);
        successCount++;

        // Cleanup old backups
        const deleted = manager.cleanOldBackups(result.project, keepCount);
        if (deleted > 0) {
          console.log(`            Cleaned up ${deleted} old backup(s)`);
        }
      } else {
        console.log(`  [failed]  ${result.project}: ${result.error}`);
        failCount++;
      }
    }

    console.log(`\nBackup complete: ${successCount} succeeded, ${failCount} failed`);
    if (failCount > 0) {
      process.exit(1);
    }
  });

// ============================================================================
// Helpers
// ============================================================================

function formatBytes(bytes: number): string {
  if (bytes === 0) return "0 B";
  const k = 1024;
  const sizes = ["B", "KB", "MB", "GB"];
  const i = Math.floor(Math.log(bytes) / Math.log(k));
  return `${parseFloat((bytes / Math.pow(k, i)).toFixed(2))} ${sizes[i]}`;
}

function ensureDataDir(dataPath: string): void {
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

function printTable(columns: string[], rows: Record<string, unknown>[]): void {
  // Calculate column widths
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
    widths[col] = Math.min(widths[col], 40);
  }

  // Print header
  const header = columns.map((col) => col.padEnd(widths[col])).join(" | ");
  console.log(`  ${header}`);
  console.log(`  ${columns.map((col) => "-".repeat(widths[col])).join("-+-")}`);

  // Print rows
  for (const row of rows) {
    const line = columns
      .map((col) => {
        const val = formatValue(row[col]);
        return val.slice(0, widths[col]).padEnd(widths[col]);
      })
      .join(" | ");
    console.log(`  ${line}`);
  }
}

function formatValue(val: unknown): string {
  if (val === null) return "null";
  if (val === undefined) return "";
  if (typeof val === "object") return JSON.stringify(val);
  return String(val);
}

// Parse and run
program.parse();
