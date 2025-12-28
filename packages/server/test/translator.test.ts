import { describe, it, expect, beforeEach } from "vitest";
import { parse } from "../src/parser";
import { translate, Translator, TranslationResult } from "../src/translator";

// Helper to parse and translate in one step
function translateCypher(cypher: string, params: Record<string, unknown> = {}): TranslationResult {
  const parseResult = parse(cypher);
  if (!parseResult.success) {
    throw new Error(`Parse failed: ${parseResult.error.message}`);
  }
  return translate(parseResult.query, params);
}

describe("Translator", () => {
  describe("CREATE nodes", () => {
    it("generates INSERT for node with label", () => {
      const result = translateCypher("CREATE (n:Person)");

      expect(result.statements).toHaveLength(1);
      expect(result.statements[0].sql).toBe(
        "INSERT INTO nodes (id, label, properties) VALUES (?, ?, ?)"
      );
      expect(result.statements[0].params).toHaveLength(3);
      expect(result.statements[0].params[1]).toBe("Person");
      expect(result.statements[0].params[2]).toBe("{}");
    });

    it("generates INSERT with properties as JSON", () => {
      const result = translateCypher("CREATE (n:Person {name: 'Alice', age: 30})");

      expect(result.statements).toHaveLength(1);
      const props = JSON.parse(result.statements[0].params[2] as string);
      expect(props).toEqual({ name: "Alice", age: 30 });
    });

    it("generates UUID for node id", () => {
      const result = translateCypher("CREATE (n:Person)");

      const id = result.statements[0].params[0] as string;
      // UUID v4 format check
      expect(id).toMatch(
        /^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i
      );
    });

    it("handles parameter values in properties", () => {
      const result = translateCypher(
        "CREATE (n:Person {name: $name, age: $age})",
        { name: "Bob", age: 25 }
      );

      const props = JSON.parse(result.statements[0].params[2] as string);
      expect(props).toEqual({ name: "Bob", age: 25 });
    });

    it("handles boolean and null values", () => {
      const result = translateCypher(
        "CREATE (n:Person {active: true, score: null})"
      );

      const props = JSON.parse(result.statements[0].params[2] as string);
      expect(props).toEqual({ active: true, score: null });
    });

    it("handles array values", () => {
      const result = translateCypher(
        "CREATE (n:Person {tags: ['dev', 'admin']})"
      );

      const props = JSON.parse(result.statements[0].params[2] as string);
      expect(props).toEqual({ tags: ["dev", "admin"] });
    });
  });

  describe("CREATE relationships", () => {
    it("generates INSERT for edge between new nodes", () => {
      const result = translateCypher(
        "CREATE (a:Person {name: 'Alice'})-[:KNOWS]->(b:Person {name: 'Bob'})"
      );

      expect(result.statements).toHaveLength(3);

      // First: source node
      expect(result.statements[0].sql).toContain("INSERT INTO nodes");
      expect(result.statements[0].params[1]).toBe("Person");

      // Second: target node
      expect(result.statements[1].sql).toContain("INSERT INTO nodes");
      expect(result.statements[1].params[1]).toBe("Person");

      // Third: edge
      expect(result.statements[2].sql).toBe(
        "INSERT INTO edges (id, type, source_id, target_id, properties) VALUES (?, ?, ?, ?, ?)"
      );
      expect(result.statements[2].params[1]).toBe("KNOWS");
    });

    it("generates edge with properties", () => {
      const result = translateCypher(
        "CREATE (a:Person)-[:KNOWS {since: 2020}]->(b:Person)"
      );

      const edgeStmt = result.statements[2];
      const props = JSON.parse(edgeStmt.params[4] as string);
      expect(props).toEqual({ since: 2020 });
    });

    it("correctly links source and target IDs", () => {
      const result = translateCypher(
        "CREATE (a:Person)-[:KNOWS]->(b:Person)"
      );

      const sourceId = result.statements[0].params[0];
      const targetId = result.statements[1].params[0];
      const edgeSourceId = result.statements[2].params[2];
      const edgeTargetId = result.statements[2].params[3];

      expect(edgeSourceId).toBe(sourceId);
      expect(edgeTargetId).toBe(targetId);
    });

    it("swaps source/target for left-directed relationships", () => {
      const result = translateCypher(
        "CREATE (a:Person)<-[:KNOWS]-(b:Person)"
      );

      const sourceId = result.statements[0].params[0]; // a
      const targetId = result.statements[1].params[0]; // b
      const edgeSourceId = result.statements[2].params[2];
      const edgeTargetId = result.statements[2].params[3];

      // For left-directed, the edge goes from b to a
      expect(edgeSourceId).toBe(targetId); // b is source
      expect(edgeTargetId).toBe(sourceId); // a is target
    });
  });

  describe("MATCH + RETURN", () => {
    it("generates SELECT for simple node match", () => {
      const result = translateCypher("MATCH (n:Person) RETURN n");

      expect(result.statements).toHaveLength(1);
      expect(result.statements[0].sql).toContain("SELECT");
      expect(result.statements[0].sql).toContain("FROM nodes");
      expect(result.statements[0].sql).toContain("WHERE");
      expect(result.statements[0].sql).toContain("label = ?");
      expect(result.statements[0].params).toContain("Person");
    });

    it("generates SELECT with property filter", () => {
      const result = translateCypher(
        "MATCH (n:Person {name: 'Alice'}) RETURN n"
      );

      expect(result.statements[0].sql).toContain("json_extract");
      expect(result.statements[0].sql).toContain("$.name");
      expect(result.statements[0].params).toContain("Alice");
    });

    it("generates SELECT with parameter filter", () => {
      const result = translateCypher(
        "MATCH (n:Person {id: $id}) RETURN n",
        { id: "abc123" }
      );

      expect(result.statements[0].params).toContain("abc123");
    });

    it("returns property access", () => {
      const result = translateCypher("MATCH (n:Person) RETURN n.name");

      // Uses -> operator to preserve JSON types (booleans as true/false, not 1/0)
      expect(result.statements[0].sql).toContain("-> '$.name'");
      expect(result.returnColumns).toEqual(["n_name"]);
    });

    it("returns multiple properties", () => {
      const result = translateCypher("MATCH (n:Person) RETURN n.name, n.age");

      expect(result.returnColumns).toEqual(["n_name", "n_age"]);
    });

    it("respects LIMIT clause", () => {
      const result = translateCypher("MATCH (n:Person) RETURN n LIMIT 10");

      expect(result.statements[0].sql).toContain("LIMIT ?");
      expect(result.statements[0].params).toContain(10);
    });

    it("handles COUNT function", () => {
      const result = translateCypher("MATCH (n:Person) RETURN COUNT(n)");

      expect(result.statements[0].sql).toContain("COUNT(*)");
      expect(result.returnColumns).toEqual(["count"]);
    });

    it("generates ORDER BY clause with single property", () => {
      const result = translateCypher("MATCH (n:Person) RETURN n ORDER BY n.name");

      expect(result.statements[0].sql).toContain("ORDER BY");
      expect(result.statements[0].sql).toContain("json_extract");
      expect(result.statements[0].sql).toContain("$.name");
      expect(result.statements[0].sql).toContain("ASC");
    });

    it("generates ORDER BY with DESC", () => {
      const result = translateCypher("MATCH (n:Person) RETURN n ORDER BY n.age DESC");

      expect(result.statements[0].sql).toContain("ORDER BY");
      expect(result.statements[0].sql).toContain("DESC");
    });

    it("generates ORDER BY with multiple fields", () => {
      const result = translateCypher("MATCH (n:Person) RETURN n ORDER BY n.name ASC, n.age DESC");

      expect(result.statements[0].sql).toContain("ORDER BY");
      expect(result.statements[0].sql).toMatch(/ASC.*DESC/);
    });

    it("generates ORDER BY before LIMIT", () => {
      const result = translateCypher("MATCH (n:Person) RETURN n ORDER BY n.name LIMIT 10");

      const sql = result.statements[0].sql;
      const orderByIndex = sql.indexOf("ORDER BY");
      const limitIndex = sql.indexOf("LIMIT");
      expect(orderByIndex).toBeLessThan(limitIndex);
    });

    it("generates OFFSET for SKIP", () => {
      const result = translateCypher("MATCH (n:Person) RETURN n SKIP 5");

      expect(result.statements[0].sql).toContain("OFFSET");
      expect(result.statements[0].params).toContain(5);
    });

    it("generates LIMIT and OFFSET for SKIP with LIMIT", () => {
      const result = translateCypher("MATCH (n:Person) RETURN n SKIP 10 LIMIT 5");

      expect(result.statements[0].sql).toContain("LIMIT");
      expect(result.statements[0].sql).toContain("OFFSET");
      expect(result.statements[0].params).toContain(5);
      expect(result.statements[0].params).toContain(10);
    });

    it("generates ORDER BY with SKIP and LIMIT", () => {
      const result = translateCypher("MATCH (n:Person) RETURN n ORDER BY n.name SKIP 10 LIMIT 5");

      const sql = result.statements[0].sql;
      expect(sql).toContain("ORDER BY");
      expect(sql).toContain("LIMIT");
      expect(sql).toContain("OFFSET");
      
      // Verify order: ORDER BY before LIMIT before OFFSET
      const orderByIndex = sql.indexOf("ORDER BY");
      const limitIndex = sql.indexOf("LIMIT");
      const offsetIndex = sql.indexOf("OFFSET");
      expect(orderByIndex).toBeLessThan(limitIndex);
      expect(limitIndex).toBeLessThan(offsetIndex);
    });

    it("handles alias in RETURN", () => {
      const result = translateCypher(
        "MATCH (n:Person) RETURN n.name AS personName"
      );

      expect(result.statements[0].sql).toContain("AS personName");
      expect(result.returnColumns).toEqual(["personName"]);
    });

    it("handles id() function", () => {
      const result = translateCypher("MATCH (n:Person) RETURN id(n)");

      expect(result.statements[0].sql).toMatch(/n\d+\.id/);
    });
  });

  describe("MATCH relationships", () => {
    it("generates JOIN for relationship pattern", () => {
      const result = translateCypher(
        "MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN a, b"
      );

      expect(result.statements[0].sql).toContain("JOIN edges");
      expect(result.statements[0].sql).toContain("JOIN nodes");
      expect(result.statements[0].sql).toContain("source_id");
      expect(result.statements[0].sql).toContain("target_id");
    });

    it("filters by edge type", () => {
      const result = translateCypher(
        "MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN a"
      );

      expect(result.statements[0].sql).toContain("type = ?");
      expect(result.statements[0].params).toContain("KNOWS");
    });

    it("filters by node labels in relationship", () => {
      const result = translateCypher(
        "MATCH (a:Person)-[:KNOWS]->(b:Company) RETURN a"
      );

      expect(result.statements[0].params).toContain("Person");
      expect(result.statements[0].params).toContain("Company");
    });
  });

  describe("WHERE clause", () => {
    it("translates equals comparison", () => {
      const result = translateCypher(
        "MATCH (n:Person) WHERE n.age = 30 RETURN n"
      );

      expect(result.statements[0].sql).toContain("json_extract");
      expect(result.statements[0].sql).toContain("= ?");
      expect(result.statements[0].params).toContain(30);
    });

    it("translates not equals comparison", () => {
      const result = translateCypher(
        "MATCH (n:Person) WHERE n.name <> 'Bob' RETURN n"
      );

      expect(result.statements[0].sql).toContain("<> ?");
    });

    it("translates less than comparison", () => {
      const result = translateCypher(
        "MATCH (n:Person) WHERE n.age < 30 RETURN n"
      );

      expect(result.statements[0].sql).toContain("< ?");
    });

    it("translates greater than comparison", () => {
      const result = translateCypher(
        "MATCH (n:Person) WHERE n.age > 25 RETURN n"
      );

      expect(result.statements[0].sql).toContain("> ?");
    });

    it("translates AND conditions", () => {
      const result = translateCypher(
        "MATCH (n:Person) WHERE n.age > 25 AND n.age < 40 RETURN n"
      );

      expect(result.statements[0].sql).toContain("AND");
    });

    it("translates OR conditions", () => {
      const result = translateCypher(
        "MATCH (n:Person) WHERE n.name = 'Alice' OR n.name = 'Bob' RETURN n"
      );

      expect(result.statements[0].sql).toContain("OR");
    });

    it("translates NOT conditions", () => {
      const result = translateCypher(
        "MATCH (n:Person) WHERE NOT n.active = false RETURN n"
      );

      expect(result.statements[0].sql).toContain("NOT");
    });

    it("translates CONTAINS", () => {
      const result = translateCypher(
        "MATCH (n:Person) WHERE n.name CONTAINS 'ali' RETURN n"
      );

      expect(result.statements[0].sql).toContain("LIKE '%' ||");
      expect(result.statements[0].sql).toContain("|| '%'");
    });

    it("translates STARTS WITH", () => {
      const result = translateCypher(
        "MATCH (n:Person) WHERE n.name STARTS WITH 'A' RETURN n"
      );

      expect(result.statements[0].sql).toContain("LIKE");
      expect(result.statements[0].sql).toContain("|| '%'");
    });

    it("translates ENDS WITH", () => {
      const result = translateCypher(
        "MATCH (n:Person) WHERE n.name ENDS WITH 'e' RETURN n"
      );

      expect(result.statements[0].sql).toContain("LIKE '%' ||");
    });

    it("translates parameter in WHERE", () => {
      const result = translateCypher(
        "MATCH (n:Person) WHERE n.id = $id RETURN n",
        { id: "abc123" }
      );

      expect(result.statements[0].params).toContain("abc123");
    });
  });

  describe("SET", () => {
    it("generates UPDATE with json_set", () => {
      // Need to set up context first with a MATCH
      const parseResult = parse("MATCH (n:Person {id: 'abc123'}) SET n.name = 'Bob'");
      if (!parseResult.success) throw new Error("Parse failed");

      const translator = new Translator({ id: "abc123" });
      const result = translator.translate(parseResult.query);

      // The SET statement should use json_set
      const setStmt = result.statements.find((s) => s.sql.includes("UPDATE"));
      expect(setStmt).toBeDefined();
      expect(setStmt!.sql).toContain("json_set");
      expect(setStmt!.sql).toContain("$.name");
    });

    it("handles multiple SET assignments", () => {
      const parseResult = parse(
        "MATCH (n:Person {id: 'abc123'}) SET n.name = 'Bob', n.age = 31"
      );
      if (!parseResult.success) throw new Error("Parse failed");

      const translator = new Translator();
      const result = translator.translate(parseResult.query);

      const updateStmts = result.statements.filter((s) =>
        s.sql.includes("UPDATE")
      );
      expect(updateStmts).toHaveLength(2);
    });

    it("handles parameter in SET value", () => {
      const parseResult = parse(
        "MATCH (n:Person {id: 'abc123'}) SET n.name = $name"
      );
      if (!parseResult.success) throw new Error("Parse failed");

      const translator = new Translator({ name: "Charlie" });
      const result = translator.translate(parseResult.query);

      const setStmt = result.statements.find((s) => s.sql.includes("UPDATE"));
      expect(setStmt!.params).toContain(JSON.stringify("Charlie"));
    });
  });

  describe("DELETE", () => {
    it("generates DELETE for node", () => {
      const parseResult = parse("MATCH (n:Person {id: 'abc123'}) DELETE n");
      if (!parseResult.success) throw new Error("Parse failed");

      const translator = new Translator();
      const result = translator.translate(parseResult.query);

      const deleteStmt = result.statements.find((s) =>
        s.sql.includes("DELETE FROM nodes")
      );
      expect(deleteStmt).toBeDefined();
    });

    it("generates DELETE for edge", () => {
      const parseResult = parse("MATCH (a)-[r:KNOWS]->(b) DELETE r");
      if (!parseResult.success) throw new Error("Parse failed");

      const translator = new Translator();
      const result = translator.translate(parseResult.query);

      const deleteStmt = result.statements.find((s) =>
        s.sql.includes("DELETE FROM edges")
      );
      expect(deleteStmt).toBeDefined();
    });

    it("generates DETACH DELETE (deletes connected edges first)", () => {
      const parseResult = parse(
        "MATCH (n:Person {id: 'abc123'}) DETACH DELETE n"
      );
      if (!parseResult.success) throw new Error("Parse failed");

      const translator = new Translator();
      const result = translator.translate(parseResult.query);

      // Should have DELETE FROM edges before DELETE FROM nodes
      const deleteEdgesIdx = result.statements.findIndex((s) =>
        s.sql.includes("DELETE FROM edges")
      );
      const deleteNodesIdx = result.statements.findIndex((s) =>
        s.sql.includes("DELETE FROM nodes")
      );

      expect(deleteEdgesIdx).toBeLessThan(deleteNodesIdx);
    });
  });

  describe("MERGE", () => {
    it("generates INSERT OR IGNORE pattern", () => {
      const result = translateCypher("MERGE (n:Person {id: 'abc123'})");

      expect(result.statements).toHaveLength(1);
      expect(result.statements[0].sql).toContain("INSERT OR IGNORE");
      expect(result.statements[0].sql).toContain("NOT EXISTS");
    });

    it("includes match conditions in subquery", () => {
      const result = translateCypher(
        "MERGE (n:Person {id: 'abc123', name: 'Alice'})"
      );

      expect(result.statements[0].sql).toContain("json_extract");
      expect(result.statements[0].sql).toContain("$.id");
      expect(result.statements[0].sql).toContain("$.name");
    });
  });

  describe("Parameter binding", () => {
    it("replaces parameters with ? placeholders", () => {
      const result = translateCypher(
        "CREATE (n:Person {name: $name})",
        { name: "Alice" }
      );

      // The properties JSON should contain the resolved value
      const props = JSON.parse(result.statements[0].params[2] as string);
      expect(props.name).toBe("Alice");
    });

    it("maintains parameter order", () => {
      const result = translateCypher(
        "MATCH (n:Person {id: $id}) WHERE n.age > $minAge RETURN n",
        { id: "abc123", minAge: 25 }
      );

      // Parameters should be in order of appearance
      const params = result.statements[0].params;
      expect(params).toContain("Person");
      expect(params).toContain("abc123");
      expect(params).toContain(25);
    });
  });

  describe("Complex queries", () => {
    it("handles multi-clause query", () => {
      const parseResult = parse(`
        MATCH (a:Person {name: 'Alice'})
        MATCH (b:Person {name: 'Bob'})
        RETURN a, b
      `);
      if (!parseResult.success) throw new Error("Parse failed");

      const translator = new Translator();
      const result = translator.translate(parseResult.query);

      // Should produce a SELECT statement
      expect(result.statements.some((s) => s.sql.includes("SELECT"))).toBe(
        true
      );
    });

    it("handles multi-MATCH with shared variable in relationship", () => {
      const parseResult = parse(`
        MATCH (r:Report)-[:HAS_ITEM]->(bs:Item)
        MATCH (t:Transaction)-[:PART_OF]->(bs)
        RETURN t, bs.id
      `);
      if (!parseResult.success) throw new Error(`Parse failed: ${parseResult.error.message}`);

      const translator = new Translator({ reportId: "r1" });
      const result = translator.translate(parseResult.query);

      const sql = result.statements[0].sql;
      
      // Should have SELECT
      expect(sql).toContain("SELECT");
      
      // n0 = Report (r), n1 = Item (bs), n3 = Transaction (t)
      // The key is that bs should use n1 (not create a new alias)
      // and the second edge should connect to n1
      
      // Should have n3 for Transaction (t) - this is correct
      expect(sql).toContain("n3");
      
      // Should JOIN n1 for bs from the first pattern
      expect(sql).toContain("JOIN nodes n1");
      
      // The second edge (e4) should connect to n1 (the shared bs variable)
      // Either via JOIN or WHERE clause
      expect(sql).toContain("e4.target_id = n1.id");
      
      // Should NOT have n4 (which would mean bs was aliased twice)
      expect(sql).not.toContain("n4");
    });
  });

  describe("RETURN DISTINCT", () => {
    it("generates SELECT DISTINCT for property", () => {
      const result = translateCypher("MATCH (n:Person) RETURN DISTINCT n.city");

      expect(result.statements[0].sql).toContain("SELECT DISTINCT");
    });

    it("generates SELECT DISTINCT for multiple properties", () => {
      const result = translateCypher("MATCH (n:Person) RETURN DISTINCT n.city, n.country");

      expect(result.statements[0].sql).toContain("SELECT DISTINCT");
    });

    it("generates SELECT without DISTINCT when not specified", () => {
      const result = translateCypher("MATCH (n:Person) RETURN n.city");

      expect(result.statements[0].sql).not.toContain("DISTINCT");
    });

    it("generates SELECT DISTINCT with ORDER BY", () => {
      const result = translateCypher("MATCH (n:Person) RETURN DISTINCT n.city ORDER BY n.city");

      const sql = result.statements[0].sql;
      expect(sql).toContain("SELECT DISTINCT");
      expect(sql).toContain("ORDER BY");
    });

    it("generates SELECT DISTINCT with LIMIT", () => {
      const result = translateCypher("MATCH (n:Person) RETURN DISTINCT n.city LIMIT 10");

      const sql = result.statements[0].sql;
      expect(sql).toContain("SELECT DISTINCT");
      expect(sql).toContain("LIMIT");
    });
  });

  describe("OPTIONAL MATCH", () => {
    it("generates LEFT JOIN for OPTIONAL MATCH with relationship", () => {
      const result = translateCypher(
        "MATCH (n:Person) OPTIONAL MATCH (n)-[:KNOWS]->(m:Person) RETURN n, m"
      );

      expect(result.statements).toHaveLength(1);
      const sql = result.statements[0].sql;
      
      // Should use LEFT JOIN for the optional relationship
      expect(sql).toContain("LEFT JOIN edges");
      expect(sql).toContain("LEFT JOIN nodes");
    });

    it("generates LEFT JOIN for simple OPTIONAL MATCH node", () => {
      const result = translateCypher(
        "MATCH (n:Person {id: $id}) OPTIONAL MATCH (m:Company {parentId: $id}) RETURN n, m",
        { id: "123" }
      );

      expect(result.statements).toHaveLength(1);
      const sql = result.statements[0].sql;
      
      // Should use LEFT JOIN for optional node
      expect(sql).toContain("LEFT JOIN");
    });

    it("handles OPTIONAL MATCH with WHERE clause", () => {
      const result = translateCypher(
        "MATCH (n:Person) OPTIONAL MATCH (n)-[:KNOWS]->(m:Person) WHERE m.age > 25 RETURN n, m"
      );

      expect(result.statements).toHaveLength(1);
      const sql = result.statements[0].sql;
      
      // Should have LEFT JOIN
      expect(sql).toContain("LEFT JOIN");
      // WHERE condition should apply to the optional part
      expect(sql).toContain("json_extract");
    });

    it("handles multiple OPTIONAL MATCH clauses", () => {
      const result = translateCypher(`
        MATCH (n:Person)
        OPTIONAL MATCH (n)-[:KNOWS]->(friend:Person)
        OPTIONAL MATCH (n)-[:WORKS_AT]->(company:Company)
        RETURN n, friend, company
      `);

      expect(result.statements).toHaveLength(1);
      const sql = result.statements[0].sql;
      
      // Should have multiple LEFT JOINs
      const leftJoinCount = (sql.match(/LEFT JOIN/g) || []).length;
      expect(leftJoinCount).toBeGreaterThanOrEqual(4); // 2 edges + 2 nodes
    });

    it("combines regular MATCH with OPTIONAL MATCH correctly", () => {
      const result = translateCypher(
        "MATCH (n:Person)-[:WORKS_AT]->(c:Company) OPTIONAL MATCH (n)-[:KNOWS]->(m:Person) RETURN n, c, m"
      );

      expect(result.statements).toHaveLength(1);
      const sql = result.statements[0].sql;
      
      // Should have regular JOINs for required pattern
      expect(sql).toContain("JOIN edges");
      expect(sql).toContain("JOIN nodes");
      // And LEFT JOINs for optional pattern
      expect(sql).toContain("LEFT JOIN edges");
      expect(sql).toContain("LEFT JOIN nodes");
    });
  });

  describe("Standalone RETURN", () => {
    it("handles RETURN with literal number", () => {
      const result = translateCypher("RETURN 1");

      expect(result.statements).toHaveLength(1);
      expect(result.statements[0].sql).toContain("SELECT");
      expect(result.statements[0].params).toContain(1);
      expect(result.returnColumns).toEqual(["expr"]);
    });

    it("handles RETURN with literal string", () => {
      const result = translateCypher("RETURN 'hello'");

      expect(result.statements).toHaveLength(1);
      expect(result.statements[0].sql).toContain("SELECT");
      expect(result.statements[0].params).toContain("hello");
    });

    it("handles RETURN with alias", () => {
      const result = translateCypher("RETURN 1 AS one");

      expect(result.statements).toHaveLength(1);
      expect(result.returnColumns).toEqual(["one"]);
    });

    it("handles RETURN with multiple literals", () => {
      const result = translateCypher("RETURN 1, 'hello', true");

      expect(result.statements).toHaveLength(1);
      expect(result.statements[0].params).toContain(1);
      expect(result.statements[0].params).toContain("hello");
      // Boolean true is converted to 1 for SQLite compatibility
      expect(result.statements[0].params).toEqual([1, "hello", 1]);
    });
  });
});
