import { describe, it, expect, beforeEach, afterEach } from "vitest";
import { GraphDatabase } from "../src/db";
import { Executor } from "../src/executor";
import { parse } from "../src/parser";
import { createApp } from "../src/routes";
import { DatabaseManager } from "../src/db";

/**
 * Security tests - SQL injection, Cypher injection, and other attack vectors
 */
describe("Security Tests", () => {
  let db: GraphDatabase;
  let executor: Executor;

  beforeEach(() => {
    db = new GraphDatabase(":memory:");
    db.initialize();
    executor = new Executor(db);

    // Seed some data
    executor.execute("CREATE (u:User {name: 'Alice', role: 'admin', password: 'secret123'})");
    executor.execute("CREATE (u:User {name: 'Bob', role: 'user', password: 'password456'})");
    executor.execute("CREATE (s:Secret {data: 'TOP_SECRET_DATA', classification: 'high'})");
  });

  afterEach(() => {
    db.close();
  });

  describe("SQL Injection via Property Values", () => {
    it("escapes single quotes in string values", () => {
      const result = executor.execute(
        "CREATE (n:Test {value: $val})",
        { val: "'; DROP TABLE nodes; --" }
      );
      expect(result.success).toBe(true);

      // Verify table still exists and query works
      const check = executor.execute("MATCH (n:Test) RETURN n.value");
      expect(check.success).toBe(true);
      if (check.success) {
        expect(check.data[0].n_value).toBe("'; DROP TABLE nodes; --");
      }

      // Verify nodes table still has data
      expect(db.countNodes()).toBeGreaterThan(0);
    });

    it("escapes double quotes in string values", () => {
      const result = executor.execute(
        "CREATE (n:Test {value: $val})",
        { val: '"; DROP TABLE nodes; --' }
      );
      expect(result.success).toBe(true);

      const check = executor.execute("MATCH (n:Test) RETURN n.value");
      expect(check.success).toBe(true);
    });

    it("handles nested SQL injection attempts in JSON", () => {
      const maliciousJson = {
        name: "test",
        nested: { attack: "'); DELETE FROM nodes; --" },
      };

      // This would be passed as a parameter
      const result = executor.execute(
        "CREATE (n:Test {data: $data})",
        { data: JSON.stringify(maliciousJson) }
      );
      expect(result.success).toBe(true);

      // Verify data integrity
      const nodeCount = db.countNodes();
      expect(nodeCount).toBeGreaterThanOrEqual(3); // Original 3 + new one
    });

    it("escapes backslashes and special characters", () => {
      const attacks = [
        "\\'; DROP TABLE nodes; --",
        "\0'; DROP TABLE nodes; --",
        "\\x00'; DROP TABLE nodes; --",
        "/**/; DROP TABLE nodes; --",
        "'; ATTACH DATABASE ':memory:' AS hack; --",
      ];

      for (const attack of attacks) {
        const result = executor.execute(
          "CREATE (n:Attack {payload: $p})",
          { p: attack }
        );
        expect(result.success).toBe(true);
      }

      // Verify database integrity
      expect(db.countNodes()).toBeGreaterThan(0);
    });

    it("prevents SQL injection via numeric parameters", () => {
      // Attempt to inject via what should be a number
      const result = executor.execute(
        "MATCH (u:User) WHERE u.role = $role RETURN u",
        { role: "1 OR 1=1; --" }
      );

      // Should either fail or return no results (not all users)
      if (result.success) {
        expect(result.data.length).toBeLessThanOrEqual(1);
      }
    });
  });

  describe("SQL Injection via Cypher Syntax", () => {
    it("does not allow breaking out of string literals in Cypher", () => {
      // Try to inject via the Cypher parser itself
      const result = executor.execute(
        "MATCH (u:User {name: 'Alice' OR '1'='1'}) RETURN u"
      );

      // Parser should reject this or it should return no results
      if (result.success) {
        // If it parsed, it should not have bypassed the filter
        expect(result.data.length).toBeLessThanOrEqual(1);
      }
    });

    it("rejects attempts to add additional clauses via string", () => {
      const result = executor.execute(
        "MATCH (u:User {name: 'Alice'}) RETURN u; DROP TABLE nodes; --"
      );

      // Should fail to parse due to semicolon
      expect(result.success).toBe(false);
    });

    it("prevents UNION-style attacks", () => {
      const result = executor.execute(
        "MATCH (u:User {name: 'x' }) RETURN u UNION SELECT * FROM secrets --'})"
      );

      // Parser should reject this
      expect(result.success).toBe(false);
    });

    it("handles label injection attempts", () => {
      // Try to inject via label
      const result = executor.execute(
        "MATCH (n:User}) RETURN n; DELETE FROM nodes WHERE (1=1"
      );

      expect(result.success).toBe(false);
    });
  });

  describe("SQL Injection via Property Names", () => {
    it("does not allow injection via property access", () => {
      // The property name should be safely handled
      const maliciousProperty = "name]; DROP TABLE nodes; --";

      // This should be caught at parse time or safely escaped
      const result = executor.execute(`MATCH (u:User) RETURN u.${maliciousProperty}`);

      // Should fail to parse
      expect(result.success).toBe(false);
    });

    it("safely handles special characters in property names via parameters", () => {
      // Even if someone tries to use weird property names
      const result = executor.execute(
        "CREATE (n:Test {normalProp: $val})",
        { val: "safe_value" }
      );
      expect(result.success).toBe(true);
    });
  });

  describe("Cypher Injection", () => {
    it("prevents breaking out of parameter context", () => {
      // Try to inject additional Cypher via parameter
      const result = executor.execute(
        "MATCH (u:User {name: $name}) RETURN u",
        { name: "Alice'}) RETURN u UNION MATCH (s:Secret) RETURN s; //" }
      );

      // Parameters are used as values, not interpolated into Cypher
      // So this should either find no user or just Alice
      if (result.success) {
        for (const row of result.data) {
          // Should not contain secret data
          expect(JSON.stringify(row)).not.toContain("TOP_SECRET");
        }
      }
    });

    it("correctly handles multiple relationship types in traversals", () => {
      // Setup: create edges
      const users = db.getNodesByLabel("User");
      const secrets = db.getNodesByLabel("Secret");
      if (users.length > 0 && secrets.length > 0) {
        db.insertEdge("access1", "CAN_ACCESS", users[0].id, secrets[0].id);
      }

      // Multiple relationship types are valid syntax
      // Security should be handled at the authorization layer, not by syntax limitations
      const result = executor.execute(
        "MATCH (u:User {name: 'Bob'})-[:KNOWS|CAN_ACCESS]->(s) RETURN s"
      );

      // Now parses successfully - query should execute
      expect(result.success).toBe(true);
      // Result depends on data setup - the query is valid
    });

    it("handles nested object injection in properties", () => {
      const result = executor.execute(
        "CREATE (n:Test {data: $d})",
        {
          d: {
            __proto__: { admin: true },
            constructor: { prototype: { admin: true } },
            normal: "value",
          },
        }
      );

      expect(result.success).toBe(true);

      // Verify the data was stored safely
      const check = executor.execute("MATCH (n:Test) RETURN n");
      if (check.success && check.data.length > 0) {
        const node = check.data[0].n as any;
        // Prototype pollution should not have occurred
        expect(({} as any).admin).toBeUndefined();
      }
    });
  });

  describe("HTTP Layer Security", () => {
    let dbManager: DatabaseManager;
    let app: ReturnType<typeof createApp>;

    beforeEach(() => {
      dbManager = new DatabaseManager(":memory:");
      app = createApp(dbManager);
    });

    afterEach(() => {
      dbManager.closeAll();
    });

    async function request(
      method: string,
      path: string,
      body?: unknown
    ): Promise<{ status: number; json: any }> {
      const req = new Request(`http://localhost${path}`, {
        method,
        headers: body ? { "Content-Type": "application/json" } : undefined,
        body: body ? JSON.stringify(body) : undefined,
      });

      const res = await app.fetch(req);
      const json = await res.json();
      return { status: res.status, json };
    }

    it("rejects path traversal in project names", async () => {
      const maliciousNames = [
        "../../../etc/passwd",
        "..\\..\\windows\\system32",
        "project/../../../secret",
        "project%2F..%2F..%2Fsecret",
        "project\x00.db",
      ];

      for (const name of maliciousNames) {
        const { status } = await request("POST", `/query/test/${encodeURIComponent(name)}`, {
          cypher: "MATCH (n) RETURN n",
        });

        // Should either work safely or reject
        // The key is it shouldn't access files outside the data directory
        expect(status).toBeLessThan(500); // No server errors
      }
    });

    it("rejects invalid environment values", async () => {
      const invalidEnvs = [
        "production; rm -rf /",
        "../production",
        "test\x00production",
        "PRODUCTION",
        "Production",
      ];

      for (const env of invalidEnvs) {
        const { status, json } = await request(
          "POST",
          `/query/${encodeURIComponent(env)}/myproject`,
          { cypher: "MATCH (n) RETURN n" }
        );

        expect(status).toBe(400);
        expect(json.success).toBe(false);
      }
    });

    it("handles malformed JSON gracefully", async () => {
      const req = new Request("http://localhost/query/test/myproject", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: '{"cypher": "MATCH (n) RETURN n"', // Missing closing brace
      });

      const res = await app.fetch(req);
      expect(res.status).toBe(400);

      const json = await res.json();
      expect(json.success).toBe(false);
    });

    it("handles extremely large payloads", async () => {
      const largeString = "x".repeat(1000000); // 1MB string

      const { status, json } = await request("POST", "/query/test/myproject", {
        cypher: `CREATE (n:Test {data: '${largeString}'})`,
      });

      // Should either work or fail gracefully
      expect(status).toBeLessThan(500);
    });

    it("handles null bytes in input", async () => {
      const { status, json } = await request("POST", "/query/test/myproject", {
        cypher: "MATCH (n) RETURN n\x00; DROP TABLE nodes;",
      });

      // Should handle gracefully
      expect(status).toBeLessThan(500);
    });

    it("rejects requests with excessively deep nesting", async () => {
      // Create deeply nested object
      let nested: any = { value: "deep" };
      for (let i = 0; i < 100; i++) {
        nested = { level: nested };
      }

      const { status } = await request("POST", "/query/test/myproject", {
        cypher: "CREATE (n:Test {data: $d})",
        params: { d: nested },
      });

      // Should handle without crashing
      expect(status).toBeLessThan(500);
    });
  });

  describe("Information Disclosure Prevention", () => {
    it("does not leak database schema in errors", () => {
      const result = executor.execute("INVALID SYNTAX HERE");

      expect(result.success).toBe(false);
      if (!result.success) {
        const errorMessage = result.error.message;
        // Check the error message content, not the field names (column/line/position are ok)
        expect(errorMessage.toLowerCase()).not.toContain("sqlite");
        expect(errorMessage).not.toContain("SQL");
        expect(errorMessage.toLowerCase()).not.toContain("table");
        // "column" in the field name is fine, just not in the message
        expect(errorMessage.toLowerCase()).not.toMatch(/\bcolumn\b.*\bname\b/);
      }
    });

    it("does not leak file paths in errors", () => {
      const result = executor.execute("INVALID SYNTAX");

      expect(result.success).toBe(false);
      if (!result.success) {
        const errorStr = JSON.stringify(result.error);
        expect(errorStr).not.toContain("/home");
        expect(errorStr).not.toContain("/var");
        expect(errorStr).not.toContain("C:\\");
        expect(errorStr).not.toContain(".ts");
        expect(errorStr).not.toContain(".js");
      }
    });

    it("provides useful but safe error messages", () => {
      const result = executor.execute("MTCH (n) RETURN n"); // Typo in MATCH

      expect(result.success).toBe(false);
      if (!result.success) {
        // Should indicate what went wrong without exposing internals
        expect(result.error.message).toBeDefined();
        expect(result.error.position).toBeDefined();
      }
    });
  });

  describe("Resource Exhaustion Prevention", () => {
    it("handles queries that would return large result sets", () => {
      // Create many nodes
      for (let i = 0; i < 100; i++) {
        executor.execute(`CREATE (n:Bulk {index: ${i}})`);
      }

      // Query without limit
      const result = executor.execute("MATCH (n:Bulk) RETURN n");

      expect(result.success).toBe(true);
      if (result.success) {
        // Should return results (current implementation doesn't have default limit)
        expect(result.data.length).toBe(100);
      }
    });

    it("respects LIMIT to prevent large result sets", () => {
      for (let i = 0; i < 100; i++) {
        executor.execute(`CREATE (n:Limited {index: ${i}})`);
      }

      const result = executor.execute("MATCH (n:Limited) RETURN n LIMIT 10");

      expect(result.success).toBe(true);
      if (result.success) {
        expect(result.data.length).toBe(10);
      }
    });

    it("handles regex-like patterns safely in CONTAINS", () => {
      executor.execute("CREATE (n:Test {data: 'aaaaaaaaaaaaaaaaaaaaaaaaaaaa'})");

      // Potential ReDoS pattern (though SQLite LIKE is not vulnerable to this)
      const result = executor.execute(
        "MATCH (n:Test) WHERE n.data CONTAINS 'a' RETURN n"
      );

      expect(result.success).toBe(true);
    });
  });

  describe("Parser Fuzzing", () => {
    const fuzzInputs = [
      "",
      " ",
      "\n\n\n",
      "\t\t\t",
      "(",
      ")",
      "()",
      "[]",
      "{}",
      "(((",
      ")))",
      "[[[",
      "]]]",
      "{{{",
      "}}}",
      "MATCH",
      "MATCH (",
      "MATCH ()",
      "MATCH (n",
      "MATCH (n)",
      "MATCH (n:)",
      "MATCH (n:Label",
      "MATCH (n:Label)",
      "MATCH (n:Label) RETURN",
      "MATCH (n:Label) RETURN n LIMIT",
      "MATCH (n:Label) RETURN n LIMIT -1",
      "MATCH (n:Label) RETURN n LIMIT 999999999999999999999",
      "CREATE (n:Label {",
      "CREATE (n:Label {key:",
      "CREATE (n:Label {key: })",
      "CREATE (n:Label {key: value})",
      "CREATE (n:Label {key: 'unclosed string})",
      "CREATE (n:Label {key: \"unclosed string})",
      `CREATE (n:Label {key: '${"a".repeat(10000)}'})`,
      "MATCH (a)-[:]->(b)",
      "MATCH (a)-[]->(b)",
      "MATCH (a)<-[]->(b)",
      "MATCH (a)-->(b)-->(c)-->(d)",
      "WHERE",
      "RETURN",
      "SET",
      "DELETE",
      "MERGE",
      "CREATE MATCH RETURN",
      "MATCH MATCH MATCH",
      "RETURN RETURN RETURN",
      "\u0000MATCH (n) RETURN n",
      "MATCH (n) RETURN n\u0000",
      "MATCH\u0000(n)\u0000RETURN\u0000n",
    ];

    it("handles fuzz inputs without crashing", () => {
      for (const input of fuzzInputs) {
        // Should not throw - may fail gracefully
        expect(() => {
          const result = executor.execute(input);
          // Just verify we get a response
          expect(result).toHaveProperty("success");
        }).not.toThrow();
      }
    });

    it("parser handles fuzz inputs without crashing", () => {
      for (const input of fuzzInputs) {
        expect(() => {
          const result = parse(input);
          expect(result).toHaveProperty("success");
        }).not.toThrow();
      }
    });
  });

  describe("Type Confusion Attacks", () => {
    it("handles type confusion in parameters", () => {
      const confusingParams = [
        { val: null },
        { val: undefined },
        { val: NaN },
        { val: Infinity },
        { val: -Infinity },
        { val: [] },
        { val: {} },
        { val: () => "function" },
        { val: Symbol("test") },
        { val: new Date() },
        { val: /regex/ },
        { val: new Map() },
        { val: new Set() },
        { val: BigInt(9007199254740991) },
      ];

      for (const params of confusingParams) {
        // Should handle without crashing
        expect(() => {
          const result = executor.execute(
            "CREATE (n:Test {value: $val})",
            params as any
          );
          expect(result).toHaveProperty("success");
        }).not.toThrow();
      }
    });

    it("handles unexpected types in Cypher values", () => {
      // These should all parse correctly or fail gracefully
      const queries = [
        "CREATE (n:Test {value: true})",
        "CREATE (n:Test {value: false})",
        "CREATE (n:Test {value: null})",
        "CREATE (n:Test {value: 0})",
        "CREATE (n:Test {value: -0})",
        "CREATE (n:Test {value: 1e10})",
        "CREATE (n:Test {value: 1.5e-10})",
        "CREATE (n:Test {value: []})",
        "CREATE (n:Test {value: [1, 2, 3]})",
        "CREATE (n:Test {value: ['a', 'b', 'c']})",
      ];

      for (const query of queries) {
        expect(() => {
          const result = executor.execute(query);
          expect(result).toHaveProperty("success");
        }).not.toThrow();
      }
    });
  });
});
