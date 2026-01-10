import neo4j, { Driver } from "neo4j-driver";
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

export class MemgraphRunner implements Runner {
  name = "memgraph" as const;
  private driver: Driver | null = null;

  async connect(): Promise<void> {
    // Memgraph uses the same Bolt protocol as Neo4j
    this.driver = neo4j.driver(
      BENCHMARK_CONFIG.memgraph.uri,
      BENCHMARK_CONFIG.memgraph.user
        ? neo4j.auth.basic(BENCHMARK_CONFIG.memgraph.user, BENCHMARK_CONFIG.memgraph.password)
        : undefined
    );
    // Verify connection
    await this.driver.verifyConnectivity();
  }

  async disconnect(): Promise<void> {
    if (this.driver) {
      await this.driver.close();
      this.driver = null;
    }
  }

  async execute(cypher: string, params?: Record<string, unknown>): Promise<unknown> {
    if (!this.driver) throw new Error("Not connected");
    const session = this.driver.session();
    try {
      const result = await session.run(cypher, params ?? {});
      return result.records.map((r) => r.toObject());
    } finally {
      await session.close();
    }
  }

  async clear(): Promise<void> {
    if (!this.driver) throw new Error("Not connected");
    const session = this.driver.session();
    try {
      // Memgraph supports DROP GRAPH for faster clearing
      try {
        await session.run("DROP GRAPH");
      } catch {
        // Fall back to MATCH DELETE if DROP GRAPH not available
        let deleted = 1;
        while (deleted > 0) {
          const result = await session.run(
            "MATCH (n) WITH n LIMIT 10000 DETACH DELETE n RETURN count(*) as deleted"
          );
          deleted = result.records[0]?.get("deleted")?.toNumber() ?? 0;
        }
      }
    } finally {
      await session.close();
    }
  }

  async getVersion(): Promise<string> {
    if (!this.driver) throw new Error("Not connected");
    const session = this.driver.session();
    try {
      // Memgraph version query
      const result = await session.run("CALL mg.procedures() YIELD name RETURN 'memgraph' AS version LIMIT 1");
      // Try to get actual version from storage info
      try {
        const infoResult = await session.run("CALL mg.info() YIELD key, value WHERE key = 'version' RETURN value");
        if (infoResult.records.length > 0) {
          return infoResult.records[0].get("value");
        }
      } catch {
        // Ignore - version query not available
      }
      return "memgraph";
    } catch {
      return "unknown";
    } finally {
      await session.close();
    }
  }

  getDriver(): Driver | null {
    return this.driver;
  }
}

export async function loadMemgraph(
  config: ScaleConfig,
  onProgress?: (msg: string) => void
): Promise<LoadResult> {
  const log = onProgress ?? console.log;
  const runner = new MemgraphRunner();

  log("Memgraph: Connecting...");
  await runner.connect();

  const driver = runner.getDriver();
  if (!driver) throw new Error("Failed to get driver");

  log("Memgraph: Clearing existing data...");
  await runner.clear();

  const startTime = performance.now();
  let nodesLoaded = 0;
  let edgesLoaded = 0;

  // Create indexes first
  log("Memgraph: Creating indexes...");
  const indexSession = driver.session();
  try {
    await indexSession.run("CREATE INDEX ON :User(id)");
    await indexSession.run("CREATE INDEX ON :Item(id)");
    await indexSession.run("CREATE INDEX ON :Event(id)");
    await indexSession.run("CREATE INDEX ON :Item(category)");
  } catch (e) {
    // Indexes might already exist, ignore errors
    log(`  Index creation note: ${e instanceof Error ? e.message : "unknown error"}`);
  } finally {
    await indexSession.close();
  }

  // Load Users using UNWIND for batch inserts
  log("Memgraph: Loading Users...");
  let batchCount = 0;
  for (const userBatch of batch(generateUsers(config.users), BENCHMARK_CONFIG.batchSize)) {
    const session = driver.session();
    try {
      await session.run(
        `UNWIND $batch AS row
         CREATE (u:User {id: row.id, name: row.name, email: row.email, created_at: row.created_at})`,
        { batch: userBatch }
      );
      nodesLoaded += userBatch.length;
      batchCount++;
      if (batchCount % 10 === 0) {
        log(`  Users: ${nodesLoaded.toLocaleString()} / ${config.users.toLocaleString()}`);
      }
    } finally {
      await session.close();
    }
  }

  // Load Items
  log("Memgraph: Loading Items...");
  batchCount = 0;
  let itemsLoaded = 0;
  for (const itemBatch of batch(generateItems(config.items), BENCHMARK_CONFIG.batchSize)) {
    const session = driver.session();
    try {
      await session.run(
        `UNWIND $batch AS row
         CREATE (i:Item {id: row.id, title: row.title, category: row.category, price: row.price})`,
        { batch: itemBatch }
      );
      nodesLoaded += itemBatch.length;
      itemsLoaded += itemBatch.length;
      batchCount++;
      if (batchCount % 10 === 0) {
        log(`  Items: ${itemsLoaded.toLocaleString()} / ${config.items.toLocaleString()}`);
      }
    } finally {
      await session.close();
    }
  }

  // Load Events
  log("Memgraph: Loading Events...");
  batchCount = 0;
  let eventsLoaded = 0;
  for (const eventBatch of batch(generateEvents(config.events), BENCHMARK_CONFIG.batchSize)) {
    const session = driver.session();
    try {
      await session.run(
        `UNWIND $batch AS row
         CREATE (e:Event {id: row.id, type: row.type, timestamp: row.timestamp})`,
        { batch: eventBatch }
      );
      nodesLoaded += eventBatch.length;
      eventsLoaded += eventBatch.length;
      batchCount++;
      if (batchCount % 10 === 0) {
        log(`  Events: ${eventsLoaded.toLocaleString()} / ${config.events.toLocaleString()}`);
      }
    } finally {
      await session.close();
    }
  }

  // Load OWNS edges
  log("Memgraph: Loading OWNS edges...");
  batchCount = 0;
  let ownsLoaded = 0;
  for (const edgeBatch of batch(
    generateOwnsEdges(config.users, config.items, config.ownsEdges),
    BENCHMARK_CONFIG.batchSize
  )) {
    const session = driver.session();
    try {
      await session.run(
        `UNWIND $batch AS row
         MATCH (u:User {id: row.fromId})
         MATCH (i:Item {id: row.toId})
         CREATE (u)-[:OWNS]->(i)`,
        { batch: edgeBatch }
      );
      edgesLoaded += edgeBatch.length;
      ownsLoaded += edgeBatch.length;
      batchCount++;
      if (batchCount % 10 === 0) {
        log(`  OWNS: ${ownsLoaded.toLocaleString()} / ${config.ownsEdges.toLocaleString()}`);
      }
    } finally {
      await session.close();
    }
  }

  // Load TRIGGERED edges
  log("Memgraph: Loading TRIGGERED edges...");
  batchCount = 0;
  let triggeredLoaded = 0;
  for (const edgeBatch of batch(
    generateTriggeredEdges(config.users, config.events, config.triggeredEdges),
    BENCHMARK_CONFIG.batchSize
  )) {
    const session = driver.session();
    try {
      await session.run(
        `UNWIND $batch AS row
         MATCH (u:User {id: row.fromId})
         MATCH (e:Event {id: row.toId})
         CREATE (u)-[:TRIGGERED]->(e)`,
        { batch: edgeBatch }
      );
      edgesLoaded += edgeBatch.length;
      triggeredLoaded += edgeBatch.length;
      batchCount++;
      if (batchCount % 10 === 0) {
        log(`  TRIGGERED: ${triggeredLoaded.toLocaleString()} / ${config.triggeredEdges.toLocaleString()}`);
      }
    } finally {
      await session.close();
    }
  }

  // Load RELATED_TO edges
  log("Memgraph: Loading RELATED_TO edges...");
  batchCount = 0;
  let relatedLoaded = 0;
  for (const edgeBatch of batch(
    generateRelatedToEdges(config.items, config.relatedToEdges),
    BENCHMARK_CONFIG.batchSize
  )) {
    const session = driver.session();
    try {
      await session.run(
        `UNWIND $batch AS row
         MATCH (i1:Item {id: row.fromId})
         MATCH (i2:Item {id: row.toId})
         CREATE (i1)-[:RELATED_TO]->(i2)`,
        { batch: edgeBatch }
      );
      edgesLoaded += edgeBatch.length;
      relatedLoaded += edgeBatch.length;
      batchCount++;
      if (batchCount % 10 === 0) {
        log(`  RELATED_TO: ${relatedLoaded.toLocaleString()} / ${config.relatedToEdges.toLocaleString()}`);
      }
    } finally {
      await session.close();
    }
  }

  await runner.disconnect();

  const timeSeconds = (performance.now() - startTime) / 1000;

  return {
    timeSeconds,
    nodesLoaded,
    edgesLoaded,
  };
}
