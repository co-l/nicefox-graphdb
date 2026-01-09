// Test utilities for running tests against local or remote GraphDB

import { GraphDatabase } from "../src/db.js";
import { Executor, QueryResponse, ExecutionResult } from "../src/executor.js";

// Environment variable to control test mode: "local" (default) or "remote"
export const TEST_MODE = process.env.TEST_MODE ?? "local";

// Remote server configuration (only used when TEST_MODE=remote)
export const REMOTE_URL = process.env.TEST_REMOTE_URL ?? "http://localhost:3000";
export const REMOTE_API_KEY = process.env.TEST_REMOTE_API_KEY;

/**
 * Test client interface - abstracts over local Executor and remote HTTP client.
 * Returns the same QueryResponse structure as Executor.execute().
 */
export interface TestClient {
  /**
   * Execute a Cypher query and return the full response.
   * Returns: { success: true, data: [...], meta: { count, time_ms } }
   *      or: { success: false, error: { message, position?, line?, column? } }
   */
  execute(cypher: string, params?: Record<string, unknown>): Promise<QueryResponse>;

  /**
   * Close the client and clean up resources.
   */
  close(): void;

  /**
   * Get the underlying GraphDatabase (local mode only).
   * Returns null in remote mode.
   * Useful for tests that need direct DB access for setup.
   */
  getDatabase(): GraphDatabase | null;
}

/**
 * Create a test client based on the TEST_MODE environment variable.
 *
 * Local mode: Creates an in-memory GraphDatabase + Executor
 * Remote mode: Creates a client that connects to a running server
 */
export async function createTestClient(): Promise<TestClient> {
  if (TEST_MODE === "remote") {
    return createRemoteTestClient();
  }
  return createLocalTestClient();
}

/**
 * Create a local test client using in-memory SQLite
 */
function createLocalTestClient(): TestClient {
  const db = new GraphDatabase(":memory:");
  db.initialize();
  const executor = new Executor(db);

  return {
    async execute(
      cypher: string,
      params: Record<string, unknown> = {}
    ): Promise<QueryResponse> {
      // Executor.execute() is synchronous but we return Promise for interface consistency
      return executor.execute(cypher, params);
    },

    close(): void {
      db.close();
    },

    getDatabase(): GraphDatabase {
      return db;
    },
  };
}

/**
 * Create a remote test client that connects to a test server.
 * Each client gets a unique project for test isolation.
 */
function createRemoteTestClient(): TestClient {
  // Generate unique project name for test isolation
  const testId = `test-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
  const project = testId;
  const env = "test";

  const baseUrl = REMOTE_URL.replace(/\/$/, "");
  const endpoint = `${baseUrl}/query/${env}/${project}`;

  const headers: Record<string, string> = {
    "Content-Type": "application/json",
  };
  if (REMOTE_API_KEY) {
    headers["Authorization"] = `Bearer ${REMOTE_API_KEY}`;
  }

  // JSON replacer to handle types that JSON.stringify can't serialize
  const jsonReplacer = (_key: string, value: unknown) => {
    if (typeof value === "bigint") {
      return Number(value); // Convert BigInt to number (may lose precision for very large values)
    }
    return value;
  };

  return {
    async execute(
      cypher: string,
      params: Record<string, unknown> = {}
    ): Promise<QueryResponse> {
      const response = await fetch(endpoint, {
        method: "POST",
        headers,
        body: JSON.stringify({ cypher, params }, jsonReplacer),
      });

      const data = await response.json();
      return data as QueryResponse;
    },

    close(): void {
      // No-op for remote client (could clean up test project if needed)
    },

    getDatabase(): null {
      return null;
    },
  };
}

/**
 * Helper function to assert query success and return the result.
 * Throws if the query failed.
 */
export function expectSuccess(result: QueryResponse): ExecutionResult {
  if (!result.success) {
    throw new Error(`Query failed: ${result.error.message}`);
  }
  return result;
}

/**
 * Get the underlying database from a test client.
 * Throws if running in remote mode (database not available).
 * Use this for tests that need direct database access for setup.
 */
export function requireDatabase(client: TestClient): GraphDatabase {
  const db = client.getDatabase();
  if (!db) {
    throw new Error(
      "Direct database access not available in remote mode. " +
      "Convert this test to use Cypher queries or skip in remote mode."
    );
  }
  return db;
}
