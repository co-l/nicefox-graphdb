import { GraphDatabase, Executor } from "leangraph";
import { BENCHMARK_CONFIG } from "../config.js";
import {
  generateUsers,
  generateItems,
  generateEvents,
  generateOwnsEdges,
  generateTriggeredEdges,
  generateRelatedToEdges,
  batch,
} from "../generator.js";
import type { ScaleConfig, LoadResult, Runner } from "../types.js";
import * as fs from "fs";
import * as path from "path";

export class LeanGraphRunner implements Runner {
  name = "leangraph" as const;
  private db: GraphDatabase | null = null;
  private executor: Executor | null = null;
  private dbPath: string;

  constructor(dbPath?: string) {
    this.dbPath = dbPath ?? BENCHMARK_CONFIG.leangraphDataPath;
  }

  async connect(): Promise<void> {
    // Ensure directory exists
    const dir = path.dirname(this.dbPath);
    if (dir !== "." && !fs.existsSync(dir)) {
      fs.mkdirSync(dir, { recursive: true });
    }

    this.db = new GraphDatabase(this.dbPath);
    this.db.initialize();
    this.executor = new Executor(this.db);
  }

  async disconnect(): Promise<void> {
    if (this.db) {
      this.db.close();
      this.db = null;
      this.executor = null;
    }
  }

  async execute(cypher: string, params?: Record<string, unknown>): Promise<unknown> {
    if (!this.executor) throw new Error("Not connected");
    const result = this.executor.execute(cypher, params ?? {});
    if (!result.success) {
      throw new Error(result.error.message);
    }
    return result.data;
  }

  async clear(): Promise<void> {
    if (!this.executor) throw new Error("Not connected");
    // Delete all nodes and relationships
    this.executor.execute("MATCH (n) DETACH DELETE n");
  }

  async getVersion(): Promise<string> {
    // Read from package.json
    try {
      const pkgPath = path.resolve(import.meta.dirname, "../../../package.json");
      const pkg = JSON.parse(fs.readFileSync(pkgPath, "utf-8"));
      return pkg.version;
    } catch {
      return "unknown";
    }
  }

  getDatabase(): GraphDatabase | null {
    return this.db;
  }

  getDbPath(): string {
    return this.dbPath;
  }
}

export async function loadLeanGraph(
  config: ScaleConfig,
  dbPath?: string,
  onProgress?: (msg: string) => void
): Promise<LoadResult> {
  const log = onProgress ?? console.log;
  const actualDbPath = dbPath ?? BENCHMARK_CONFIG.leangraphDataPath;

  // Remove existing database file if exists
  if (fs.existsSync(actualDbPath)) {
    fs.unlinkSync(actualDbPath);
    // Also remove WAL files if they exist
    if (fs.existsSync(actualDbPath + "-wal")) fs.unlinkSync(actualDbPath + "-wal");
    if (fs.existsSync(actualDbPath + "-shm")) fs.unlinkSync(actualDbPath + "-shm");
  }

  // Ensure directory exists
  const dir = path.dirname(actualDbPath);
  if (dir !== "." && !fs.existsSync(dir)) {
    fs.mkdirSync(dir, { recursive: true });
  }

  log("LeanGraph: Initializing database...");
  const db = new GraphDatabase(actualDbPath);
  db.initialize();

  const startTime = performance.now();
  let nodesLoaded = 0;
  let edgesLoaded = 0;

  // Use the transaction wrapper for batched inserts
  const batchSize = BENCHMARK_CONFIG.batchSize;

  // Load Users
  log("LeanGraph: Loading Users...");
  let userBatchCount = 0;
  for (const userBatch of batch(generateUsers(config.users), batchSize)) {
    db.transaction(() => {
      for (const user of userBatch) {
        db.insertNode(`user-${user.id}`, "User", {
          id: user.id,
          name: user.name,
          email: user.email,
          created_at: user.created_at,
        });
        nodesLoaded++;
      }
    });
    userBatchCount++;
    if (userBatchCount % 10 === 0) {
      log(`  Users: ${nodesLoaded.toLocaleString()} / ${config.users.toLocaleString()}`);
    }
  }

  // Load Items
  log("LeanGraph: Loading Items...");
  let itemBatchCount = 0;
  let itemsLoaded = 0;
  for (const itemBatch of batch(generateItems(config.items), batchSize)) {
    db.transaction(() => {
      for (const item of itemBatch) {
        db.insertNode(`item-${item.id}`, "Item", {
          id: item.id,
          title: item.title,
          category: item.category,
          price: item.price,
        });
        nodesLoaded++;
        itemsLoaded++;
      }
    });
    itemBatchCount++;
    if (itemBatchCount % 10 === 0) {
      log(`  Items: ${itemsLoaded.toLocaleString()} / ${config.items.toLocaleString()}`);
    }
  }

  // Load Events
  log("LeanGraph: Loading Events...");
  let eventBatchCount = 0;
  let eventsLoaded = 0;
  for (const eventBatch of batch(generateEvents(config.events), batchSize)) {
    db.transaction(() => {
      for (const event of eventBatch) {
        db.insertNode(`event-${event.id}`, "Event", {
          id: event.id,
          type: event.type,
          timestamp: event.timestamp,
        });
        nodesLoaded++;
        eventsLoaded++;
      }
    });
    eventBatchCount++;
    if (eventBatchCount % 10 === 0) {
      log(`  Events: ${eventsLoaded.toLocaleString()} / ${config.events.toLocaleString()}`);
    }
  }

  // Load OWNS edges
  log("LeanGraph: Loading OWNS edges...");
  let ownsBatchCount = 0;
  let ownsLoaded = 0;
  for (const edgeBatch of batch(
    generateOwnsEdges(config.users, config.items, config.ownsEdges),
    batchSize
  )) {
    db.transaction(() => {
      for (const edge of edgeBatch) {
        db.insertEdge(
          `owns-${ownsLoaded}`,
          "OWNS",
          `user-${edge.fromId}`,
          `item-${edge.toId}`,
          {}
        );
        edgesLoaded++;
        ownsLoaded++;
      }
    });
    ownsBatchCount++;
    if (ownsBatchCount % 10 === 0) {
      log(`  OWNS: ${ownsLoaded.toLocaleString()} / ${config.ownsEdges.toLocaleString()}`);
    }
  }

  // Load TRIGGERED edges
  log("LeanGraph: Loading TRIGGERED edges...");
  let triggeredBatchCount = 0;
  let triggeredLoaded = 0;
  for (const edgeBatch of batch(
    generateTriggeredEdges(config.users, config.events, config.triggeredEdges),
    batchSize
  )) {
    db.transaction(() => {
      for (const edge of edgeBatch) {
        db.insertEdge(
          `triggered-${triggeredLoaded}`,
          "TRIGGERED",
          `user-${edge.fromId}`,
          `event-${edge.toId}`,
          {}
        );
        edgesLoaded++;
        triggeredLoaded++;
      }
    });
    triggeredBatchCount++;
    if (triggeredBatchCount % 10 === 0) {
      log(`  TRIGGERED: ${triggeredLoaded.toLocaleString()} / ${config.triggeredEdges.toLocaleString()}`);
    }
  }

  // Load RELATED_TO edges
  log("LeanGraph: Loading RELATED_TO edges...");
  let relatedBatchCount = 0;
  let relatedLoaded = 0;
  for (const edgeBatch of batch(
    generateRelatedToEdges(config.items, config.relatedToEdges),
    batchSize
  )) {
    db.transaction(() => {
      for (const edge of edgeBatch) {
        db.insertEdge(
          `related-${relatedLoaded}`,
          "RELATED_TO",
          `item-${edge.fromId}`,
          `item-${edge.toId}`,
          {}
        );
        edgesLoaded++;
        relatedLoaded++;
      }
    });
    relatedBatchCount++;
    if (relatedBatchCount % 10 === 0) {
      log(`  RELATED_TO: ${relatedLoaded.toLocaleString()} / ${config.relatedToEdges.toLocaleString()}`);
    }
  }

  db.close();

  const timeSeconds = (performance.now() - startTime) / 1000;

  return {
    timeSeconds,
    nodesLoaded,
    edgesLoaded,
  };
}
