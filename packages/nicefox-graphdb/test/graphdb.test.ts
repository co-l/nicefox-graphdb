import { describe, it, expect, beforeEach, afterEach, vi } from "vitest";
import { GraphDB, GraphDBError } from "../src/index.js";
import type { GraphDBClient } from "../src/types.js";

describe("GraphDB Factory", () => {
  describe("Development Mode (Local Client)", () => {
    let db: GraphDBClient;

    beforeEach(async () => {
      // Set NODE_ENV to development
      vi.stubEnv("NODE_ENV", "development");

      db = await GraphDB({
        url: "https://example.com", // ignored in dev mode
        project: "test-project",
        apiKey: "ignored-key", // ignored in dev mode
        dataPath: ":memory:", // use in-memory for tests
      });
    });

    afterEach(() => {
      if (db) {
        db.close();
      }
      vi.unstubAllEnvs();
    });

    it("should create a local client in development mode", async () => {
      expect(db).toBeDefined();
      expect(typeof db.query).toBe("function");
      expect(typeof db.execute).toBe("function");
      expect(typeof db.close).toBe("function");
    });

    it("should execute CREATE and RETURN queries", async () => {
      await db.execute('CREATE (n:User {name: "Alice", age: 30})');
      
      const results = await db.query<{ name: string; age: number }>(
        'MATCH (n:User) RETURN n.name AS name, n.age AS age'
      );
      
      expect(results).toHaveLength(1);
      expect(results[0].name).toBe("Alice");
      expect(results[0].age).toBe(30);
    });

    it("should support parameterized queries", async () => {
      await db.execute(
        'CREATE (n:User {name: $name, email: $email})',
        { name: "Bob", email: "bob@example.com" }
      );
      
      const results = await db.query<{ email: string }>(
        'MATCH (n:User {name: $name}) RETURN n.email AS email',
        { name: "Bob" }
      );
      
      expect(results).toHaveLength(1);
      expect(results[0].email).toBe("bob@example.com");
    });

    it("should create relationships", async () => {
      await db.execute(`
        CREATE (a:User {name: 'Alice'})-[:FOLLOWS]->(b:User {name: 'Bob'})
      `);
      
      const results = await db.query<{ follower: string; followed: string }>(
        'MATCH (a:User)-[:FOLLOWS]->(b:User) RETURN a.name AS follower, b.name AS followed'
      );
      
      expect(results).toHaveLength(1);
      expect(results[0].follower).toBe("Alice");
      expect(results[0].followed).toBe("Bob");
    });

    it("should return query metadata via queryRaw", async () => {
      await db.execute('CREATE (n:User {name: "Test"})');
      
      const response = await db.queryRaw('MATCH (n:User) RETURN n');
      
      expect(response.success).toBe(true);
      expect(response.meta.count).toBe(1);
      expect(typeof response.meta.time_ms).toBe("number");
      expect(response.data).toHaveLength(1);
    });

    it("should throw GraphDBError on invalid query", async () => {
      await expect(db.execute("INVALID CYPHER QUERY")).rejects.toThrow(GraphDBError);
    });

    it("should return health status", async () => {
      const health = await db.health();
      
      expect(health.status).toBe("ok");
      expect(health.timestamp).toBeDefined();
    });

    describe("Convenience Methods", () => {
      it("createNode should create and return node ID", async () => {
        const id = await db.createNode("User", { name: "Charlie", active: true });
        
        expect(id).toBeDefined();
        expect(typeof id).toBe("string");
        
        const results = await db.query<{ name: string }>(
          'MATCH (n:User {name: "Charlie"}) RETURN n.name AS name'
        );
        expect(results).toHaveLength(1);
      });

      it("getNode should find node by filter", async () => {
        await db.execute('CREATE (n:User {name: "Diana", email: "diana@example.com"})');
        
        const user = await db.getNode("User", { name: "Diana" });
        
        expect(user).not.toBeNull();
        expect(user!.email).toBe("diana@example.com");
      });

      it("getNode should return null for non-existent node", async () => {
        const user = await db.getNode("User", { name: "NonExistent" });
        
        expect(user).toBeNull();
      });

      it("updateNode should modify properties", async () => {
        await db.execute('CREATE (n:User {id: "user1", name: "Eve", status: "inactive"})');
        
        await db.updateNode("user1", { status: "active", verified: true });
        
        const results = await db.query<{ status: string; verified: boolean }>(
          'MATCH (n:User {id: "user1"}) RETURN n.status AS status, n.verified AS verified'
        );
        
        expect(results[0].status).toBe("active");
        expect(results[0].verified).toBe(true);
      });

      it("deleteNode should remove node and relationships", async () => {
        await db.execute(`
          CREATE (a:User {id: "del1", name: "ToDelete"})-[:KNOWS]->(b:User {name: "Friend"})
        `);
        
        await db.deleteNode("del1");
        
        const results = await db.query('MATCH (n:User {id: "del1"}) RETURN n');
        expect(results).toHaveLength(0);
      });

      it("createEdge should create relationship between nodes", async () => {
        await db.execute(`
          CREATE (a:User {id: "edge1", name: "User1"}),
                 (b:User {id: "edge2", name: "User2"})
        `);
        
        await db.createEdge("edge1", "FRIENDS_WITH", "edge2", { since: "2024" });
        
        const results = await db.query<{ type: string }>(
          'MATCH (a {id: "edge1"})-[r]->(b {id: "edge2"}) RETURN type(r) AS type'
        );
        
        expect(results).toHaveLength(1);
        expect(results[0].type).toBe("FRIENDS_WITH");
      });
    });
  });

  describe("Production Mode Detection", () => {
    afterEach(() => {
      vi.unstubAllEnvs();
    });

    it("should use remote client when NODE_ENV is not development", async () => {
      vi.stubEnv("NODE_ENV", "production");
      
      // Remote client will fail to connect, but we can verify it tries HTTP
      await expect(
        GraphDB({
          url: "http://localhost:99999", // non-existent server
          project: "test",
        })
      ).resolves.toBeDefined();
      
      // The client is created but will fail on first query
      const db = await GraphDB({
        url: "http://localhost:99999",
        project: "test",
      });
      
      // This should fail because it tries to make HTTP request
      await expect(db.query("MATCH (n) RETURN n")).rejects.toThrow();
      
      db.close();
    });

    it("should use remote client when NODE_ENV is undefined", async () => {
      vi.stubEnv("NODE_ENV", "");
      
      const db = await GraphDB({
        url: "http://localhost:99999",
        project: "test",
      });
      
      // Remote client should fail on query (no server)
      await expect(db.query("MATCH (n) RETURN n")).rejects.toThrow();
      
      db.close();
    });
  });

  describe("Options Handling", () => {
    beforeEach(() => {
      vi.stubEnv("NODE_ENV", "development");
    });

    afterEach(() => {
      vi.unstubAllEnvs();
    });

    it("should default env to production", async () => {
      const db = await GraphDB({
        url: "http://example.com",
        project: "test",
        dataPath: ":memory:",
      });
      
      // Default env is 'production', but in dev mode, local client is used
      expect(db).toBeDefined();
      db.close();
    });

    it("should accept test environment", async () => {
      const db = await GraphDB({
        url: "http://example.com",
        project: "test",
        env: "test",
        dataPath: ":memory:",
      });
      
      expect(db).toBeDefined();
      db.close();
    });
  });
});

describe("GraphDBError", () => {
  it("should have correct name", () => {
    const error = new GraphDBError("Test error");
    expect(error.name).toBe("GraphDBError");
  });

  it("should include position information", () => {
    const error = new GraphDBError("Syntax error", {
      position: 10,
      line: 1,
      column: 10,
    });
    
    expect(error.message).toBe("Syntax error");
    expect(error.position).toBe(10);
    expect(error.line).toBe(1);
    expect(error.column).toBe(10);
  });
});
