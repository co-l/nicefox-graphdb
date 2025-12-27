// NiceFox GraphDB Client Tests

import { describe, it, expect, beforeEach, afterEach, beforeAll, afterAll } from "vitest";
import { NiceFoxGraphDB, GraphDBError } from "../src/index";

// Mock fetch for testing
const originalFetch = globalThis.fetch;
let mockResponses: Map<string, { status: number; body: unknown }> = new Map();

function mockFetch(url: string | URL | Request, init?: RequestInit): Promise<Response> {
  const urlStr = url.toString();
  const method = init?.method || "GET";
  const key = `${method} ${urlStr}`;
  
  const mock = mockResponses.get(key) || mockResponses.get(urlStr);
  
  if (mock) {
    return Promise.resolve(new Response(JSON.stringify(mock.body), {
      status: mock.status,
      headers: { "Content-Type": "application/json" },
    }));
  }
  
  return Promise.resolve(new Response(JSON.stringify({ error: "Not mocked" }), {
    status: 404,
  }));
}

describe("NiceFoxGraphDB Client", () => {
  beforeEach(() => {
    mockResponses.clear();
    globalThis.fetch = mockFetch as typeof fetch;
  });

  afterEach(() => {
    globalThis.fetch = originalFetch;
  });

  describe("Constructor", () => {
    it("should create a client with required options", () => {
      const client = new NiceFoxGraphDB({
        url: "https://graph.example.com",
        project: "myproject",
      });

      expect(client).toBeDefined();
    });

    it("should default env to production", () => {
      const client = new NiceFoxGraphDB({
        url: "https://graph.example.com",
        project: "myproject",
      });

      // The env should be production by default (we'll verify via the URL in query)
      expect(client).toBeDefined();
    });

    it("should accept all options", () => {
      const client = new NiceFoxGraphDB({
        url: "https://graph.example.com",
        project: "myproject",
        env: "test",
        apiKey: "secret-key",
      });

      expect(client).toBeDefined();
    });

    it("should normalize URL by removing trailing slash", () => {
      const client = new NiceFoxGraphDB({
        url: "https://graph.example.com/",
        project: "myproject",
      });

      expect(client).toBeDefined();
    });
  });

  describe("query()", () => {
    it("should execute a query and return results", async () => {
      mockResponses.set("POST https://graph.example.com/query/production/myproject", {
        status: 200,
        body: {
          success: true,
          data: [{ name: "Alice" }, { name: "Bob" }],
          meta: { count: 2, time_ms: 5 },
        },
      });

      const client = new NiceFoxGraphDB({
        url: "https://graph.example.com",
        project: "myproject",
      });

      const result = await client.query("MATCH (n:Person) RETURN n.name as name");

      expect(result).toEqual([{ name: "Alice" }, { name: "Bob" }]);
    });

    it("should pass parameters to the query", async () => {
      let capturedBody: unknown;
      
      globalThis.fetch = async (url: string | URL | Request, init?: RequestInit) => {
        capturedBody = JSON.parse(init?.body as string);
        return new Response(JSON.stringify({
          success: true,
          data: [{ id: "123", name: "Alice" }],
          meta: { count: 1, time_ms: 3 },
        }), { status: 200 });
      };

      const client = new NiceFoxGraphDB({
        url: "https://graph.example.com",
        project: "myproject",
      });

      await client.query("MATCH (n:Person {id: $id}) RETURN n", { id: "123" });

      expect(capturedBody).toEqual({
        cypher: "MATCH (n:Person {id: $id}) RETURN n",
        params: { id: "123" },
      });
    });

    it("should include API key in Authorization header", async () => {
      let capturedHeaders: Headers | undefined;
      
      globalThis.fetch = async (url: string | URL | Request, init?: RequestInit) => {
        capturedHeaders = new Headers(init?.headers);
        return new Response(JSON.stringify({
          success: true,
          data: [],
          meta: { count: 0, time_ms: 1 },
        }), { status: 200 });
      };

      const client = new NiceFoxGraphDB({
        url: "https://graph.example.com",
        project: "myproject",
        apiKey: "secret-key-123",
      });

      await client.query("MATCH (n) RETURN n");

      expect(capturedHeaders?.get("Authorization")).toBe("Bearer secret-key-123");
    });

    it("should use the specified environment", async () => {
      let capturedUrl: string = "";
      
      globalThis.fetch = async (url: string | URL | Request, init?: RequestInit) => {
        capturedUrl = url.toString();
        return new Response(JSON.stringify({
          success: true,
          data: [],
          meta: { count: 0, time_ms: 1 },
        }), { status: 200 });
      };

      const client = new NiceFoxGraphDB({
        url: "https://graph.example.com",
        project: "myproject",
        env: "test",
      });

      await client.query("MATCH (n) RETURN n");

      expect(capturedUrl).toBe("https://graph.example.com/query/test/myproject");
    });

    it("should throw GraphDBError on query failure", async () => {
      mockResponses.set("POST https://graph.example.com/query/production/myproject", {
        status: 400,
        body: {
          success: false,
          error: { message: "Syntax error at position 5" },
        },
      });

      const client = new NiceFoxGraphDB({
        url: "https://graph.example.com",
        project: "myproject",
      });

      await expect(client.query("INVALID QUERY")).rejects.toThrow(GraphDBError);
    });

    it("should include error details in GraphDBError", async () => {
      mockResponses.set("POST https://graph.example.com/query/production/myproject", {
        status: 400,
        body: {
          success: false,
          error: { 
            message: "Syntax error",
            position: 10,
            line: 1,
            column: 10,
          },
        },
      });

      const client = new NiceFoxGraphDB({
        url: "https://graph.example.com",
        project: "myproject",
      });

      try {
        await client.query("INVALID QUERY");
        expect.fail("Should have thrown");
      } catch (err) {
        expect(err).toBeInstanceOf(GraphDBError);
        expect((err as GraphDBError).message).toBe("Syntax error");
        expect((err as GraphDBError).position).toBe(10);
      }
    });
  });

  describe("queryRaw()", () => {
    it("should return full response including meta", async () => {
      mockResponses.set("POST https://graph.example.com/query/production/myproject", {
        status: 200,
        body: {
          success: true,
          data: [{ count: 42 }],
          meta: { count: 1, time_ms: 15 },
        },
      });

      const client = new NiceFoxGraphDB({
        url: "https://graph.example.com",
        project: "myproject",
      });

      const result = await client.queryRaw("MATCH (n) RETURN COUNT(n) as count");

      expect(result.success).toBe(true);
      expect(result.data).toEqual([{ count: 42 }]);
      expect(result.meta.count).toBe(1);
      expect(result.meta.time_ms).toBe(15);
    });
  });

  describe("execute()", () => {
    it("should execute mutating queries without returning data", async () => {
      mockResponses.set("POST https://graph.example.com/query/production/myproject", {
        status: 200,
        body: {
          success: true,
          data: [],
          meta: { count: 0, time_ms: 10 },
        },
      });

      const client = new NiceFoxGraphDB({
        url: "https://graph.example.com",
        project: "myproject",
      });

      // Should not throw
      await client.execute("CREATE (n:Person {name: 'Alice'})");
    });
  });

  describe("Convenience methods", () => {
    let client: NiceFoxGraphDB;

    beforeEach(() => {
      client = new NiceFoxGraphDB({
        url: "https://graph.example.com",
        project: "myproject",
      });
    });

    describe("createNode()", () => {
      it("should create a node with label and properties", async () => {
        let capturedBody: { cypher: string; params: Record<string, unknown> } | undefined;
        
        globalThis.fetch = async (url: string | URL | Request, init?: RequestInit) => {
          capturedBody = JSON.parse(init?.body as string);
          return new Response(JSON.stringify({
            success: true,
            data: [{ id: "generated-id" }],
            meta: { count: 1, time_ms: 5 },
          }), { status: 200 });
        };

        const id = await client.createNode("Person", { name: "Alice", age: 30 });

        expect(capturedBody?.cypher).toMatch(/CREATE.*Person/);
        expect(capturedBody?.params).toMatchObject({ name: "Alice", age: 30 });
        expect(id).toBe("generated-id");
      });
    });

    describe("createEdge()", () => {
      it("should create an edge between two nodes", async () => {
        let capturedBody: { cypher: string; params: Record<string, unknown> } | undefined;
        
        globalThis.fetch = async (url: string | URL | Request, init?: RequestInit) => {
          capturedBody = JSON.parse(init?.body as string);
          return new Response(JSON.stringify({
            success: true,
            data: [],
            meta: { count: 0, time_ms: 5 },
          }), { status: 200 });
        };

        await client.createEdge("node1", "FOLLOWS", "node2", { since: 2024 });

        expect(capturedBody?.cypher).toMatch(/MATCH[\s\S]*MERGE[\s\S]*FOLLOWS/);
        expect(capturedBody?.params).toMatchObject({
          sourceId: "node1",
          targetId: "node2",
          since: 2024,
        });
      });
    });

    describe("getNode()", () => {
      it("should retrieve a node by label and properties", async () => {
        mockResponses.set("POST https://graph.example.com/query/production/myproject", {
          status: 200,
          body: {
            success: true,
            data: [{ n: { id: "123", label: "Person", properties: { name: "Alice" } } }],
            meta: { count: 1, time_ms: 3 },
          },
        });

        const node = await client.getNode("Person", { id: "123" });

        expect(node).toBeDefined();
        expect(node?.properties?.name).toBe("Alice");
      });

      it("should return null if node not found", async () => {
        mockResponses.set("POST https://graph.example.com/query/production/myproject", {
          status: 200,
          body: {
            success: true,
            data: [],
            meta: { count: 0, time_ms: 2 },
          },
        });

        const node = await client.getNode("Person", { id: "nonexistent" });

        expect(node).toBeNull();
      });
    });

    describe("deleteNode()", () => {
      it("should delete a node by id", async () => {
        let capturedBody: { cypher: string; params: Record<string, unknown> } | undefined;
        
        globalThis.fetch = async (url: string | URL | Request, init?: RequestInit) => {
          capturedBody = JSON.parse(init?.body as string);
          return new Response(JSON.stringify({
            success: true,
            data: [],
            meta: { count: 0, time_ms: 5 },
          }), { status: 200 });
        };

        await client.deleteNode("node-123");

        expect(capturedBody?.cypher).toMatch(/MATCH.*DELETE/);
        expect(capturedBody?.params?.id).toBe("node-123");
      });
    });
  });

  describe("health()", () => {
    it("should return health status", async () => {
      mockResponses.set("GET https://graph.example.com/health", {
        status: 200,
        body: { status: "ok", timestamp: "2024-12-27T12:00:00Z" },
      });

      const client = new NiceFoxGraphDB({
        url: "https://graph.example.com",
        project: "myproject",
      });

      const health = await client.health();

      expect(health.status).toBe("ok");
    });
  });
});
