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
      expect(result.statements[0].params[1]).toBe('["Person"]');
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
      expect(result.statements[0].params[1]).toBe('["Person"]');

      // Second: target node
      expect(result.statements[1].sql).toContain("INSERT INTO nodes");
      expect(result.statements[1].params[1]).toBe('["Person"]');

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
      // Labels are stored as JSON arrays, so we use json_each for matching
      expect(result.statements[0].sql).toContain("json_each");
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

      // COUNT(n) counts nodes by their id
      expect(result.statements[0].sql).toContain("COUNT(");
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

      expect(result.statements[0].sql).toContain('AS "personName"');
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

    it("handles SET with single label", () => {
      const parseResult = parse("MATCH (n) SET n:Foo RETURN n");
      if (!parseResult.success) throw new Error("Parse failed");

      const translator = new Translator();
      const result = translator.translate(parseResult.query);

      const setStmt = result.statements.find((s) => s.sql.includes("UPDATE"));
      expect(setStmt).toBeDefined();
      expect(setStmt!.sql).toContain("UPDATE nodes SET label");
      expect(setStmt!.params[0]).toBe('["Foo"]');
    });

    it("handles SET with multiple labels", () => {
      const parseResult = parse("MATCH (n) SET n:Foo:Bar RETURN n");
      if (!parseResult.success) throw new Error("Parse failed");

      const translator = new Translator();
      const result = translator.translate(parseResult.query);

      const setStmt = result.statements.find((s) => s.sql.includes("UPDATE"));
      expect(setStmt).toBeDefined();
      expect(setStmt!.params[0]).toBe('["Foo","Bar"]');
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

    it("parses MERGE with relationship pattern", () => {
      // MERGE with relationship patterns are parsed but executed via executor, not translator
      const parseResult = parse(
        `MATCH (u:BF_User {id: $userId})
         MERGE (u)-[:BF_LEARNS]->(l:BF_Language {language: $language})
         ON CREATE SET l.proficiency = $proficiency,
                       l.created_at = $createdAt
         RETURN l.created_at as createdAt`
      );

      // Should parse successfully
      expect(parseResult.success).toBe(true);
      if (!parseResult.success) return;

      // Check MERGE clause was parsed correctly
      const mergeClause = parseResult.query.clauses.find(c => c.type === "MERGE") as any;
      expect(mergeClause).toBeDefined();
      expect(mergeClause.patterns).toHaveLength(1);
      expect(mergeClause.onCreateSet).toHaveLength(2);
      expect(mergeClause.onCreateSet[0].property).toBe("proficiency");
      expect(mergeClause.onCreateSet[1].property).toBe("created_at");
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
      // Integer literals are inlined in SQL (not params) to preserve integer division
      expect(result.statements[0].sql).toContain("1");
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
      const sql = result.statements[0].sql;
      // Integer literals are inlined directly into SQL for proper integer division
      expect(sql).toContain("1");
      expect(result.statements[0].params).toContain("hello");
      // Boolean true is converted to 1 for SQLite compatibility (also inlined)
      expect(sql).toMatch(/\b1\b/);
    });
  });

  describe("Label predicate expressions", () => {
    it("generates SQL for label predicate (n:Label)", () => {
      const result = translateCypher("MATCH (n) RETURN (n:Foo)");

      expect(result.statements).toHaveLength(1);
      // Uses json_each to check if label array contains the value
      expect(result.statements[0].sql).toContain("json_each");
      expect(result.statements[0].sql).toContain("value = 'Foo'");
      // Column name should be (n:Foo)
      expect(result.returnColumns).toContain("(n:Foo)");
    });

    it("generates SQL for label predicate with multiple labels", () => {
      const result = translateCypher("MATCH (n) RETURN (n:Foo:Bar)");

      expect(result.statements).toHaveLength(1);
      // Multiple labels use AND - all must be present
      expect(result.statements[0].sql).toContain("value = 'Foo'");
      expect(result.statements[0].sql).toContain("value = 'Bar'");
      // Column name should be (n:Foo:Bar)
      expect(result.returnColumns).toContain("(n:Foo:Bar)");
    });
  });

  describe("Aggregation functions", () => {
    it("generates SUM for property", () => {
      const result = translateCypher("MATCH (n:Order) RETURN SUM(n.amount)");

      expect(result.statements).toHaveLength(1);
      expect(result.statements[0].sql).toContain("SUM(");
      expect(result.statements[0].sql).toContain("json_extract");
      expect(result.statements[0].sql).toContain("$.amount");
      expect(result.returnColumns).toEqual(["sum"]);
    });

    it("generates AVG for property", () => {
      const result = translateCypher("MATCH (n:Order) RETURN AVG(n.amount)");

      expect(result.statements).toHaveLength(1);
      expect(result.statements[0].sql).toContain("AVG(");
      expect(result.statements[0].sql).toContain("json_extract");
      expect(result.returnColumns).toEqual(["avg"]);
    });

    it("generates MIN for property", () => {
      const result = translateCypher("MATCH (n:Order) RETURN MIN(n.amount)");

      expect(result.statements).toHaveLength(1);
      expect(result.statements[0].sql).toContain("MIN(");
      expect(result.statements[0].sql).toContain("json_extract");
      expect(result.returnColumns).toEqual(["min"]);
    });

    it("generates MAX for property", () => {
      const result = translateCypher("MATCH (n:Order) RETURN MAX(n.amount)");

      expect(result.statements).toHaveLength(1);
      expect(result.statements[0].sql).toContain("MAX(");
      expect(result.statements[0].sql).toContain("json_extract");
      expect(result.returnColumns).toEqual(["max"]);
    });

    it("generates COLLECT with json_group_array for property", () => {
      const result = translateCypher("MATCH (n:Person) RETURN COLLECT(n.name)");

      expect(result.statements).toHaveLength(1);
      // SQLite uses json_group_array for COLLECT
      expect(result.statements[0].sql).toContain("json_group_array(");
      expect(result.statements[0].sql).toContain("json_extract");
      expect(result.returnColumns).toEqual(["collect"]);
    });

    it("generates COLLECT with json_group_array for variable", () => {
      const result = translateCypher("MATCH (n:Person) RETURN COLLECT(n)");

      expect(result.statements).toHaveLength(1);
      expect(result.statements[0].sql).toContain("json_group_array(");
      expect(result.returnColumns).toEqual(["collect"]);
    });

    it("handles aggregation function with alias", () => {
      const result = translateCypher("MATCH (n:Order) RETURN SUM(n.amount) AS total");

      expect(result.statements[0].sql).toContain('AS "total"');
      expect(result.returnColumns).toEqual(["total"]);
    });

    it("handles multiple aggregation functions", () => {
      const result = translateCypher("MATCH (n:Order) RETURN SUM(n.amount) AS total, AVG(n.amount) AS average, COUNT(n) AS count");

      expect(result.statements[0].sql).toContain("SUM(");
      expect(result.statements[0].sql).toContain("AVG(");
      expect(result.statements[0].sql).toContain("COUNT(");
      expect(result.returnColumns).toEqual(["total", "average", "count"]);
    });

    it("handles aggregation with LIMIT", () => {
      const result = translateCypher("MATCH (n:Order) RETURN SUM(n.amount) AS total LIMIT 1");

      expect(result.statements[0].sql).toContain("SUM(");
      expect(result.statements[0].sql).toContain("LIMIT");
    });

    it("handles count(DISTINCT property)", () => {
      const result = translateCypher("MATCH (n:Person) RETURN count(DISTINCT n.city) AS uniqueCities");

      expect(result.statements[0].sql).toContain("COUNT(DISTINCT");
      expect(result.statements[0].sql).toContain("json_extract");
      expect(result.returnColumns).toEqual(["uniqueCities"]);
    });

    it("handles sum(DISTINCT property)", () => {
      const result = translateCypher("MATCH (n:Order) RETURN sum(DISTINCT n.amount) AS uniqueTotal");

      expect(result.statements[0].sql).toContain("SUM(DISTINCT");
      expect(result.statements[0].sql).toContain("json_extract");
      expect(result.returnColumns).toEqual(["uniqueTotal"]);
    });

    it("handles collect(DISTINCT property)", () => {
      const result = translateCypher("MATCH (n:Product) RETURN collect(DISTINCT n.category) AS categories");

      expect(result.statements[0].sql).toContain("DISTINCT");
      expect(result.statements[0].sql).toContain("GROUP_CONCAT");
      expect(result.returnColumns).toEqual(["categories"]);
    });

    it("generates COUNT(*) for counting all rows", () => {
      const result = translateCypher("MATCH (n:Person) RETURN COUNT(*) AS total");

      expect(result.statements).toHaveLength(1);
      expect(result.statements[0].sql).toContain("COUNT(*)");
      expect(result.returnColumns).toEqual(["total"]);
    });
  });

  describe("WITH clause", () => {
    it("translates simple WITH as subquery", () => {
      const result = translateCypher("MATCH (n:Person) WITH n RETURN n");

      expect(result.statements).toHaveLength(1);
      expect(result.statements[0].sql).toContain("SELECT");
      expect(result.statements[0].sql).toContain("FROM nodes");
    });

    it("translates WITH with property access and alias", () => {
      const result = translateCypher("MATCH (n:Person) WITH n.name AS name RETURN name");

      expect(result.statements).toHaveLength(1);
      const sql = result.statements[0].sql;
      expect(sql).toContain("SELECT");
      expect(result.returnColumns).toContain("name");
    });

    it("translates WITH DISTINCT", () => {
      const result = translateCypher("MATCH (n:Person) WITH DISTINCT n.city AS city RETURN city");

      const sql = result.statements[0].sql;
      expect(sql).toContain("SELECT DISTINCT");
    });

    it("translates WITH LIMIT", () => {
      const result = translateCypher("MATCH (n:Person) WITH n LIMIT 10 RETURN n");

      const sql = result.statements[0].sql;
      expect(sql).toContain("LIMIT");
    });

    it("translates WITH ORDER BY", () => {
      const result = translateCypher("MATCH (n:Person) WITH n ORDER BY n.name RETURN n");

      const sql = result.statements[0].sql;
      expect(sql).toContain("ORDER BY");
    });

    it("translates WITH followed by MATCH (query chaining)", () => {
      const result = translateCypher(`
        MATCH (n:Person)
        WITH n
        MATCH (n)-[:KNOWS]->(m:Person)
        RETURN n, m
      `);

      expect(result.statements).toHaveLength(1);
      const sql = result.statements[0].sql;
      expect(sql).toContain("SELECT");
      expect(sql).toContain("JOIN");
    });

    it("translates WITH aggregation", () => {
      const result = translateCypher("MATCH (n:Person) WITH COUNT(n) AS total RETURN total");

      const sql = result.statements[0].sql;
      expect(sql).toContain("COUNT(");
      expect(result.returnColumns).toContain("total");
    });

    it("translates WITH WHERE", () => {
      const result = translateCypher(`
        MATCH (n:Person)
        WITH n, n.age AS age
        WHERE age > 25
        RETURN n
      `);

      const sql = result.statements[0].sql;
      expect(sql).toContain("SELECT");
      expect(sql).toContain("WHERE");
    });

    it("translates multiple WITH clauses", () => {
      const result = translateCypher(`
        MATCH (n:Person)
        WITH n.name AS name
        WITH name
        RETURN name
      `);

      expect(result.statements).toHaveLength(1);
      expect(result.returnColumns).toContain("name");
    });
  });

  describe("CASE expressions", () => {
    it("generates CASE WHEN THEN ELSE END in SQL", () => {
      const result = translateCypher(
        "MATCH (n:Person) RETURN CASE WHEN n.age > 18 THEN 'adult' ELSE 'minor' END"
      );

      expect(result.statements).toHaveLength(1);
      const sql = result.statements[0].sql;
      expect(sql).toContain("CASE");
      expect(sql).toContain("WHEN");
      expect(sql).toContain("THEN");
      expect(sql).toContain("ELSE");
      expect(sql).toContain("END");
    });

    it("generates CASE with multiple WHEN clauses", () => {
      const result = translateCypher(`
        MATCH (n:Person) RETURN 
        CASE 
          WHEN n.age < 13 THEN 'child'
          WHEN n.age < 20 THEN 'teen'
          ELSE 'adult'
        END
      `);

      const sql = result.statements[0].sql;
      expect((sql.match(/WHEN/g) || []).length).toBe(2);
      expect((sql.match(/THEN/g) || []).length).toBe(2);
    });

    it("generates CASE without ELSE", () => {
      const result = translateCypher(
        "MATCH (n:Person) RETURN CASE WHEN n.active = true THEN 'active' END"
      );

      const sql = result.statements[0].sql;
      expect(sql).toContain("CASE");
      expect(sql).toContain("WHEN");
      expect(sql).not.toContain("ELSE");
      expect(sql).toContain("END");
    });

    it("handles CASE with alias", () => {
      const result = translateCypher(
        "MATCH (n:Person) RETURN CASE WHEN n.age > 18 THEN 'adult' ELSE 'minor' END AS category"
      );

      expect(result.returnColumns).toEqual(["category"]);
    });

    it("generates simple form CASE (with expression)", () => {
      const result = translateCypher(
        "MATCH (n:Person) RETURN CASE n.status WHEN 'A' THEN 'Active' WHEN 'I' THEN 'Inactive' ELSE 'Unknown' END"
      );

      const sql = result.statements[0].sql;
      expect(sql).toContain("CASE");
      expect(sql).toContain("WHEN");
      expect((sql.match(/WHEN/g) || []).length).toBe(2);
    });

    it("handles CASE with property access in THEN", () => {
      const result = translateCypher(
        "MATCH (n:Person) RETURN CASE WHEN n.active = true THEN n.name ELSE 'N/A' END"
      );

      const sql = result.statements[0].sql;
      expect(sql).toContain("CASE");
      // THEN should reference a property
      expect(sql).toContain("$.name");
    });

    it("handles CASE with numeric values", () => {
      const result = translateCypher(
        "MATCH (n:Person) RETURN CASE WHEN n.score > 90 THEN 1 WHEN n.score > 70 THEN 2 ELSE 3 END"
      );

      const sql = result.statements[0].sql;
      expect(sql).toContain("CASE");
      // Integer literals are inlined directly into SQL for proper integer division
      expect(sql).toContain("THEN 1");
      expect(sql).toContain("THEN 2");
      expect(sql).toContain("ELSE 3");
    });

    it("handles CASE with nested AND conditions", () => {
      const result = translateCypher(
        "MATCH (n:Person) RETURN CASE WHEN n.age > 18 AND n.active = true THEN 'active adult' ELSE 'other' END"
      );

      const sql = result.statements[0].sql;
      expect(sql).toContain("CASE");
      expect(sql).toContain("AND");
    });
  });

  describe("Variable-length paths", () => {
    it("generates recursive CTE for fixed length path", () => {
      const result = translateCypher(
        "MATCH (a:Person)-[*2]->(b:Person) RETURN a, b"
      );

      expect(result.statements).toHaveLength(1);
      // Should use recursive CTE for variable-length paths
      const sql = result.statements[0].sql;
      expect(sql).toContain("WITH RECURSIVE");
    });

    it("generates recursive CTE for range path", () => {
      const result = translateCypher(
        "MATCH (a:Person)-[*1..3]->(b:Person) RETURN a, b"
      );

      expect(result.statements).toHaveLength(1);
      const sql = result.statements[0].sql;
      expect(sql).toContain("WITH RECURSIVE");
    });

    it("generates recursive CTE with edge type filter", () => {
      const result = translateCypher(
        "MATCH (a:Person)-[:KNOWS*1..3]->(b:Person) RETURN a, b"
      );

      const sql = result.statements[0].sql;
      expect(sql).toContain("WITH RECURSIVE");
      expect(sql).toContain("type = ?");
      expect(result.statements[0].params).toContain("KNOWS");
    });

    it("generates recursive CTE for unbounded path", () => {
      const result = translateCypher(
        "MATCH (a:Person)-[*]->(b:Person) RETURN a, b"
      );

      const sql = result.statements[0].sql;
      expect(sql).toContain("WITH RECURSIVE");
    });

    it("generates path expression with variable-length pattern and dynamic length", () => {
      const result = translateCypher(
        "MATCH p = (a:Person)-[*1..2]->(b:Person) RETURN length(p) as len"
      );

      const sql = result.statements[0].sql;
      expect(sql).toContain("WITH RECURSIVE");
      // Path length should use the depth from the CTE, not a static value
      // The SELECT should reference the path CTE's depth column for length(p)
      expect(sql).toMatch(/path_\d+\.depth/);
    });
  });

  describe("UNION", () => {
    it("generates UNION for two queries", () => {
      const result = translateCypher(
        "MATCH (n:Person) RETURN n.name UNION MATCH (c:Company) RETURN c.name"
      );

      expect(result.statements).toHaveLength(1);
      expect(result.statements[0].sql).toContain("UNION");
      expect(result.statements[0].sql).not.toContain("UNION ALL");
    });

    it("generates UNION ALL", () => {
      const result = translateCypher(
        "MATCH (n:Person) RETURN n.name UNION ALL MATCH (c:Company) RETURN c.name"
      );

      expect(result.statements).toHaveLength(1);
      expect(result.statements[0].sql).toContain("UNION ALL");
    });

    it("generates UNION with multiple queries", () => {
      const result = translateCypher(
        "MATCH (n:Person) RETURN n.name UNION MATCH (c:Company) RETURN c.name UNION MATCH (p:Product) RETURN p.name"
      );

      expect(result.statements).toHaveLength(1);
      const sql = result.statements[0].sql;
      // Should have two UNIONs
      expect((sql.match(/UNION/g) || []).length).toBe(2);
    });

    it("generates UNION with proper column aliases", () => {
      const result = translateCypher(
        "MATCH (n:Person) RETURN n.name AS name UNION MATCH (c:Company) RETURN c.name AS name"
      );

      expect(result.returnColumns).toEqual(["name"]);
    });

    it("generates UNION with WHERE clauses", () => {
      const result = translateCypher(
        "MATCH (n:Person) WHERE n.age > 18 RETURN n.name UNION MATCH (c:Company) WHERE c.size > 10 RETURN c.name"
      );

      expect(result.statements).toHaveLength(1);
      const sql = result.statements[0].sql;
      expect(sql).toContain("UNION");
      expect(sql).toContain("WHERE");
    });
  });

  describe("EXISTS", () => {
    it("generates EXISTS subquery for relationship pattern", () => {
      const result = translateCypher(
        "MATCH (n:Person) WHERE EXISTS((n)-[:KNOWS]->(:Person)) RETURN n"
      );

      expect(result.statements).toHaveLength(1);
      expect(result.statements[0].sql).toContain("EXISTS");
      expect(result.statements[0].sql).toContain("SELECT 1");
    });

    it("generates NOT EXISTS subquery", () => {
      const result = translateCypher(
        "MATCH (n:Person) WHERE NOT EXISTS((n)-[:KNOWS]->(:Person)) RETURN n"
      );

      expect(result.statements).toHaveLength(1);
      expect(result.statements[0].sql).toContain("NOT");
      expect(result.statements[0].sql).toContain("EXISTS");
    });

    it("generates EXISTS with edge type filter", () => {
      const result = translateCypher(
        "MATCH (n:Person) WHERE EXISTS((n)-[:KNOWS]->()) RETURN n"
      );

      const sql = result.statements[0].sql;
      expect(sql).toContain("EXISTS");
      expect(sql).toContain("type = ?");
      expect(result.statements[0].params).toContain("KNOWS");
    });

    it("generates EXISTS with target label filter", () => {
      const result = translateCypher(
        "MATCH (n:Person) WHERE EXISTS((n)-[:KNOWS]->(m:Person)) RETURN n"
      );

      const sql = result.statements[0].sql;
      expect(sql).toContain("EXISTS");
      // Labels are stored as JSON arrays, so we use json_each for matching
      expect(sql).toContain("json_each");
    });

    it("combines EXISTS with AND condition", () => {
      const result = translateCypher(
        "MATCH (n:Person) WHERE n.age > 18 AND EXISTS((n)-[:KNOWS]->()) RETURN n"
      );

      const sql = result.statements[0].sql;
      expect(sql).toContain("AND");
      expect(sql).toContain("EXISTS");
    });

    it("combines EXISTS with OR condition", () => {
      const result = translateCypher(
        "MATCH (n:Person) WHERE EXISTS((n)-[:KNOWS]->()) OR n.age < 18 RETURN n"
      );

      const sql = result.statements[0].sql;
      expect(sql).toContain("OR");
      expect(sql).toContain("EXISTS");
    });
  });

  describe("UNWIND", () => {
    it("generates cross join with json_each for literal array", () => {
      const result = translateCypher("UNWIND [1, 2, 3] AS x RETURN x");

      expect(result.statements).toHaveLength(1);
      expect(result.statements[0].sql).toContain("json_each");
      expect(result.returnColumns).toEqual(["x"]);
    });

    it("generates cross join with json_each for parameter array", () => {
      const result = translateCypher("UNWIND $items AS item RETURN item", {
        items: ["a", "b", "c"],
      });

      expect(result.statements).toHaveLength(1);
      expect(result.statements[0].sql).toContain("json_each");
      expect(result.returnColumns).toEqual(["item"]);
    });

    it("handles UNWIND with MATCH", () => {
      const result = translateCypher(
        "UNWIND $ids AS id MATCH (n:Person) WHERE n.id = id RETURN n",
        { ids: ["1", "2", "3"] }
      );

      expect(result.statements).toHaveLength(1);
      // Should have json_each in the FROM/JOIN
      expect(result.statements[0].sql).toContain("json_each");
      expect(result.statements[0].sql).toContain("nodes");
    });

    it("handles UNWIND with empty array", () => {
      const result = translateCypher("UNWIND [] AS x RETURN x");

      expect(result.statements).toHaveLength(1);
      expect(result.statements[0].sql).toContain("json_each");
    });

    it("handles multiple UNWINDs (cartesian product)", () => {
      const result = translateCypher(
        "UNWIND [1, 2] AS x UNWIND [3, 4] AS y RETURN x, y"
      );

      expect(result.statements).toHaveLength(1);
      // Should have two json_each references
      const sql = result.statements[0].sql;
      const jsonEachCount = (sql.match(/json_each/g) || []).length;
      expect(jsonEachCount).toBe(2);
      expect(result.returnColumns).toEqual(["x", "y"]);
    });

    it("passes value from UNWIND to RETURN correctly", () => {
      const result = translateCypher("UNWIND [1, 2, 3] AS num RETURN num");

      expect(result.statements).toHaveLength(1);
      // The unwind alias should be accessible in the RETURN
      expect(result.statements[0].sql).toContain("value");
      expect(result.returnColumns).toEqual(["num"]);
    });

    it("handles UNWIND with COLLECT (roundtrip)", () => {
      const result = translateCypher(`
        MATCH (n:Person)
        WITH COLLECT(n.name) AS names
        UNWIND names AS name
        RETURN name
      `);

      expect(result.statements).toHaveLength(1);
      expect(result.statements[0].sql).toContain("json_each");
    });

    it("handles UNWIND with function call like range()", () => {
      const result = translateCypher("UNWIND range(1, 5) AS x RETURN x");

      expect(result.statements).toHaveLength(1);
      expect(result.statements[0].sql).toContain("json_each");
      expect(result.returnColumns).toEqual(["x"]);
    });
  });

  describe("String functions", () => {
    it("generates UPPER for toUpper()", () => {
      const result = translateCypher("MATCH (n:Person) RETURN toUpper(n.name)");

      expect(result.statements).toHaveLength(1);
      expect(result.statements[0].sql).toContain("UPPER(");
      expect(result.returnColumns).toEqual(["toupper"]);
    });

    it("generates LOWER for toLower()", () => {
      const result = translateCypher("MATCH (n:Person) RETURN toLower(n.name)");

      expect(result.statements).toHaveLength(1);
      expect(result.statements[0].sql).toContain("LOWER(");
      expect(result.returnColumns).toEqual(["tolower"]);
    });

    it("generates TRIM for trim()", () => {
      const result = translateCypher("MATCH (n:Person) RETURN trim(n.name)");

      expect(result.statements).toHaveLength(1);
      expect(result.statements[0].sql).toContain("TRIM(");
      expect(result.returnColumns).toEqual(["trim"]);
    });

    it("generates SUBSTR for substring()", () => {
      const result = translateCypher("MATCH (n:Person) RETURN substring(n.name, 0, 3)");

      expect(result.statements).toHaveLength(1);
      expect(result.statements[0].sql).toContain("SUBSTR(");
      expect(result.returnColumns).toEqual(["substring"]);
    });

    it("generates REPLACE for replace()", () => {
      const result = translateCypher("MATCH (n:Person) RETURN replace(n.name, 'a', 'b')");

      expect(result.statements).toHaveLength(1);
      expect(result.statements[0].sql).toContain("REPLACE(");
      expect(result.returnColumns).toEqual(["replace"]);
    });

    it("handles toUpper with alias", () => {
      const result = translateCypher("MATCH (n:Person) RETURN toUpper(n.name) AS upperName");

      expect(result.statements[0].sql).toContain('AS "upperName"');
      expect(result.returnColumns).toEqual(["upperName"]);
    });

    it("handles toString()", () => {
      const result = translateCypher("MATCH (n:Person) RETURN toString(n.age)");

      expect(result.statements).toHaveLength(1);
      expect(result.statements[0].sql).toContain("CAST(");
      expect(result.statements[0].sql).toContain("AS TEXT");
      expect(result.returnColumns).toEqual(["tostring"]);
    });
  });

  describe("Type conversion functions", () => {
    it("generates CAST AS INTEGER for toInteger()", () => {
      const result = translateCypher("MATCH (n:Person) RETURN toInteger(n.quantity)");

      expect(result.statements).toHaveLength(1);
      expect(result.statements[0].sql).toContain("CAST(");
      expect(result.statements[0].sql).toContain("AS INTEGER");
      expect(result.returnColumns).toEqual(["tointeger"]);
    });

    it("handles toInteger with string literal", () => {
      const result = translateCypher("RETURN toInteger('42')");

      expect(result.statements).toHaveLength(1);
      expect(result.statements[0].sql).toContain("CAST(");
      expect(result.statements[0].sql).toContain("AS INTEGER");
    });

    it("handles toInteger with alias", () => {
      const result = translateCypher("MATCH (n:Person) RETURN toInteger(n.age) AS ageInt");

      expect(result.statements[0].sql).toContain('AS "ageInt"');
      expect(result.returnColumns).toEqual(["ageInt"]);
    });

    it("generates CAST AS REAL for toFloat()", () => {
      const result = translateCypher("MATCH (n:Product) RETURN toFloat(n.price)");

      expect(result.statements).toHaveLength(1);
      expect(result.statements[0].sql).toContain("CAST(");
      expect(result.statements[0].sql).toContain("AS REAL");
      expect(result.returnColumns).toEqual(["tofloat"]);
    });

    it("handles toFloat with string literal", () => {
      const result = translateCypher("RETURN toFloat('3.14')");

      expect(result.statements).toHaveLength(1);
      expect(result.statements[0].sql).toContain("CAST(");
      expect(result.statements[0].sql).toContain("AS REAL");
    });

    it("handles toFloat with alias", () => {
      const result = translateCypher("MATCH (n:Product) RETURN toFloat(n.price) AS priceFloat");

      expect(result.statements[0].sql).toContain('AS "priceFloat"');
      expect(result.returnColumns).toEqual(["priceFloat"]);
    });

    it("generates CASE expression for toBoolean()", () => {
      const result = translateCypher("MATCH (n:Item) RETURN toBoolean(n.active)");

      expect(result.statements).toHaveLength(1);
      // toBoolean should use CASE to handle 'true'/'false' strings
      expect(result.statements[0].sql).toContain("CASE");
      expect(result.returnColumns).toEqual(["toboolean"]);
    });

    it("handles toBoolean with string literal", () => {
      const result = translateCypher("RETURN toBoolean('true')");

      expect(result.statements).toHaveLength(1);
      expect(result.statements[0].sql).toContain("CASE");
    });

    it("handles toBoolean with alias", () => {
      const result = translateCypher("MATCH (n:Item) RETURN toBoolean(n.active) AS isActive");

      expect(result.statements[0].sql).toContain('AS "isActive"');
      expect(result.returnColumns).toEqual(["isActive"]);
    });

    it("handles type conversion in arithmetic expressions", () => {
      const result = translateCypher("RETURN toInteger('10') + toInteger('5')");

      expect(result.statements).toHaveLength(1);
      // Should have two toInteger subqueries
      const sql = result.statements[0].sql;
      // Each toInteger generates a subquery with (SELECT CASE...)
      const subqueryCount = (sql.match(/\(SELECT CASE/g) || []).length;
      expect(subqueryCount).toBe(2);
    });
  });

  describe("Null/scalar functions", () => {
    it("generates COALESCE for coalesce()", () => {
      const result = translateCypher("MATCH (n:Person) RETURN coalesce(n.nickname, n.name)");

      expect(result.statements).toHaveLength(1);
      expect(result.statements[0].sql).toContain("COALESCE(");
      expect(result.returnColumns).toEqual(["coalesce"]);
    });

    it("handles coalesce with multiple arguments", () => {
      const result = translateCypher("MATCH (n:Person) RETURN coalesce(n.nickname, n.alias, n.name)");

      expect(result.statements[0].sql).toContain("COALESCE(");
      // Should have 3 arguments separated by commas
      const sql = result.statements[0].sql;
      const coalesceMatch = sql.match(/COALESCE\([^)]+\)/);
      expect(coalesceMatch).toBeTruthy();
    });

    it("handles coalesce with literal default", () => {
      const result = translateCypher("MATCH (n:Person) RETURN coalesce(n.nickname, 'Unknown')");

      expect(result.statements[0].sql).toContain("COALESCE(");
    });
  });

  describe("Math functions", () => {
    it("generates ABS for abs()", () => {
      const result = translateCypher("MATCH (n:Account) RETURN abs(n.balance)");

      expect(result.statements).toHaveLength(1);
      expect(result.statements[0].sql).toContain("ABS(");
      expect(result.returnColumns).toEqual(["abs"]);
    });

    it("generates ROUND for round()", () => {
      const result = translateCypher("MATCH (n:Product) RETURN round(n.price)");

      expect(result.statements).toHaveLength(1);
      expect(result.statements[0].sql).toContain("ROUND(");
      expect(result.returnColumns).toEqual(["round"]);
    });

    it("generates floor()", () => {
      const result = translateCypher("MATCH (n:Product) RETURN floor(n.price)");

      expect(result.statements).toHaveLength(1);
      // SQLite uses CAST with integer division for floor
      expect(result.statements[0].sql).toMatch(/CAST\(.*AS INTEGER\)|floor/i);
      expect(result.returnColumns).toEqual(["floor"]);
    });

    it("generates ceil()", () => {
      const result = translateCypher("MATCH (n:Product) RETURN ceil(n.price)");

      expect(result.statements).toHaveLength(1);
      // SQLite doesn't have native ceil, needs workaround
      expect(result.statements[0].sql).toMatch(/CASE|ceil/i);
      expect(result.returnColumns).toEqual(["ceil"]);
    });

    it("generates sqrt()", () => {
      const result = translateCypher("MATCH (n:Shape) RETURN sqrt(n.area)");

      expect(result.statements).toHaveLength(1);
      // SQLite uses pow(x, 0.5) or custom function
      expect(result.statements[0].sql).toMatch(/SQRT|pow/i);
      expect(result.returnColumns).toEqual(["sqrt"]);
    });

    it("generates rand()", () => {
      const result = translateCypher("RETURN rand()");

      expect(result.statements).toHaveLength(1);
      // SQLite uses (RANDOM() + 9223372036854775808) / 18446744073709551615.0 for 0-1 range
      expect(result.statements[0].sql).toMatch(/RANDOM|rand/i);
      expect(result.returnColumns).toEqual(["rand"]);
    });
  });

  describe("List functions", () => {
    it("generates json_array_length for size()", () => {
      const result = translateCypher("MATCH (n:Person) RETURN size(n.tags)");

      expect(result.statements).toHaveLength(1);
      expect(result.statements[0].sql).toContain("json_array_length(");
      expect(result.returnColumns).toEqual(["size"]);
    });

    it("generates head() for first element", () => {
      const result = translateCypher("MATCH (n:Person) RETURN head(n.tags)");

      expect(result.statements).toHaveLength(1);
      // SQLite uses json_extract with [0]
      expect(result.statements[0].sql).toMatch(/json_extract.*\[0\]/);
      expect(result.returnColumns).toEqual(["head"]);
    });

    it("generates last() for last element", () => {
      const result = translateCypher("MATCH (n:Person) RETURN last(n.tags)");

      expect(result.statements).toHaveLength(1);
      // SQLite uses json_extract with [-1] or json_array_length - 1
      expect(result.statements[0].sql).toMatch(/json_extract|json_array_length/);
      expect(result.returnColumns).toEqual(["last"]);
    });

    it("generates keys() for property keys", () => {
      const result = translateCypher("MATCH (n:Person) RETURN keys(n)");

      expect(result.statements).toHaveLength(1);
      // Uses SQLite's json_each to get keys
      expect(result.statements[0].sql).toMatch(/json_group_array|json_each/);
      expect(result.returnColumns).toEqual(["keys"]);
    });

    it("generates tail() for all but first element", () => {
      const result = translateCypher("MATCH (n:Person) RETURN tail(n.tags)");

      expect(result.statements).toHaveLength(1);
      // SQLite uses json_remove with $[0] to remove first element
      expect(result.statements[0].sql).toMatch(/json_remove|json/i);
      expect(result.returnColumns).toEqual(["tail"]);
    });

    it("generates range() for number list", () => {
      const result = translateCypher("RETURN range(1, 5)");

      expect(result.statements).toHaveLength(1);
      // range(1,5) should generate [1,2,3,4,5] using recursive CTE or json
      expect(result.statements[0].sql).toMatch(/WITH RECURSIVE|json/i);
      expect(result.returnColumns).toEqual(["range"]);
    });

    it("generates range() with step", () => {
      const result = translateCypher("RETURN range(0, 10, 2)");

      expect(result.statements).toHaveLength(1);
      expect(result.statements[0].sql).toMatch(/WITH RECURSIVE|json/i);
      expect(result.returnColumns).toEqual(["range"]);
    });

    it("generates split() for string to list", () => {
      const result = translateCypher("MATCH (n:Person) RETURN split(n.fullName, ' ')");

      expect(result.statements).toHaveLength(1);
      // SQLite doesn't have native split, we'll need a custom approach
      expect(result.statements[0].sql).toMatch(/json|split/i);
      expect(result.returnColumns).toEqual(["split"]);
    });
  });

  describe("Node/Relationship functions", () => {
    it("generates labels() for node labels", () => {
      const result = translateCypher("MATCH (n:Person) RETURN labels(n)");

      expect(result.statements).toHaveLength(1);
      // Returns array with the single label from the label column
      expect(result.statements[0].sql).toMatch(/json_array|label/i);
      expect(result.returnColumns).toEqual(["labels"]);
    });

    it("generates type() for relationship type", () => {
      const result = translateCypher("MATCH (a:Person)-[r:KNOWS]->(b:Person) RETURN type(r)");

      expect(result.statements).toHaveLength(1);
      // Returns the type column from edges table
      expect(result.statements[0].sql).toContain(".type");
      expect(result.returnColumns).toEqual(["type"]);
    });

    it("generates properties() for node properties", () => {
      const result = translateCypher("MATCH (n:Person) RETURN properties(n)");

      expect(result.statements).toHaveLength(1);
      // Returns the properties JSON column
      expect(result.statements[0].sql).toMatch(/properties|json/i);
      expect(result.returnColumns).toEqual(["properties"]);
    });

    it("generates properties() for relationship properties", () => {
      const result = translateCypher("MATCH (a:Person)-[r:KNOWS]->(b:Person) RETURN properties(r)");

      expect(result.statements).toHaveLength(1);
      // Returns the properties JSON column from edges
      expect(result.statements[0].sql).toMatch(/properties/);
      expect(result.returnColumns).toEqual(["properties"]);
    });
  });

  describe("IN operator", () => {
    it("generates IN for literal array", () => {
      const result = translateCypher(
        "MATCH (n:Person) WHERE n.name IN ['Alice', 'Bob'] RETURN n"
      );

      expect(result.statements).toHaveLength(1);
      const sql = result.statements[0].sql;
      expect(sql).toContain("IN");
      // Should have the names as parameters
      expect(result.statements[0].params).toContain("Alice");
      expect(result.statements[0].params).toContain("Bob");
    });

    it("generates IN for parameter array", () => {
      const result = translateCypher(
        "MATCH (n:Person) WHERE n.name IN $names RETURN n",
        { names: ["Alice", "Bob", "Charlie"] }
      );

      expect(result.statements).toHaveLength(1);
      const sql = result.statements[0].sql;
      expect(sql).toContain("IN");
    });

    it("generates IN with numeric array", () => {
      const result = translateCypher(
        "MATCH (n:Person) WHERE n.age IN [25, 30, 35] RETURN n"
      );

      expect(result.statements).toHaveLength(1);
      const sql = result.statements[0].sql;
      expect(sql).toContain("IN");
      expect(result.statements[0].params).toContain(25);
      expect(result.statements[0].params).toContain(30);
      expect(result.statements[0].params).toContain(35);
    });

    it("generates NOT IN", () => {
      const result = translateCypher(
        "MATCH (n:Person) WHERE NOT n.name IN ['Alice', 'Bob'] RETURN n"
      );

      expect(result.statements).toHaveLength(1);
      const sql = result.statements[0].sql;
      expect(sql).toContain("NOT");
      expect(sql).toContain("IN");
    });

    it("combines IN with AND condition", () => {
      const result = translateCypher(
        "MATCH (n:Person) WHERE n.name IN ['Alice', 'Bob'] AND n.age > 25 RETURN n"
      );

      expect(result.statements).toHaveLength(1);
      const sql = result.statements[0].sql;
      expect(sql).toContain("IN");
      expect(sql).toContain("AND");
    });

    it("combines IN with OR condition", () => {
      const result = translateCypher(
        "MATCH (n:Person) WHERE n.name IN ['Alice', 'Bob'] OR n.active = true RETURN n"
      );

      expect(result.statements).toHaveLength(1);
      const sql = result.statements[0].sql;
      expect(sql).toContain("IN");
      expect(sql).toContain("OR");
    });

    it("handles IN with empty array", () => {
      const result = translateCypher(
        "MATCH (n:Person) WHERE n.name IN [] RETURN n"
      );

      expect(result.statements).toHaveLength(1);
      // Empty IN should result in no matches (can be 1=0 or similar)
    });
  });

  describe("Arithmetic operators", () => {
    it("generates multiplication in RETURN", () => {
      const result = translateCypher(
        "MATCH (n:Order) RETURN n.price * n.quantity AS total"
      );

      expect(result.statements).toHaveLength(1);
      const sql = result.statements[0].sql;
      expect(sql).toContain("*");
      expect(result.returnColumns).toEqual(["total"]);
    });

    it("generates addition in RETURN", () => {
      const result = translateCypher(
        "MATCH (n:Product) RETURN n.price + 10 AS adjustedPrice"
      );

      expect(result.statements).toHaveLength(1);
      const sql = result.statements[0].sql;
      expect(sql).toContain("+");
      expect(result.returnColumns).toEqual(["adjustedPrice"]);
    });

    it("generates subtraction in RETURN", () => {
      const result = translateCypher(
        "MATCH (n:Account) RETURN n.balance - n.debt AS net"
      );

      expect(result.statements).toHaveLength(1);
      const sql = result.statements[0].sql;
      expect(sql).toContain("-");
      expect(result.returnColumns).toEqual(["net"]);
    });

    it("generates division in RETURN", () => {
      const result = translateCypher(
        "MATCH (n:Item) RETURN n.total / n.count AS average"
      );

      expect(result.statements).toHaveLength(1);
      const sql = result.statements[0].sql;
      expect(sql).toContain("/");
      expect(result.returnColumns).toEqual(["average"]);
    });

    it("generates modulo in RETURN", () => {
      const result = translateCypher(
        "MATCH (n:Number) RETURN n.value % 2 AS remainder"
      );

      expect(result.statements).toHaveLength(1);
      const sql = result.statements[0].sql;
      expect(sql).toContain("%");
      expect(result.returnColumns).toEqual(["remainder"]);
    });

    it("handles arithmetic with literals on both sides", () => {
      const result = translateCypher("RETURN 10 + 5 AS sum");

      expect(result.statements).toHaveLength(1);
      const sql = result.statements[0].sql;
      expect(sql).toContain("+");
      expect(result.returnColumns).toEqual(["sum"]);
    });

    it("handles mixed property and literal arithmetic", () => {
      const result = translateCypher(
        "MATCH (n:Product) RETURN n.price * 1.1 AS priceWithTax"
      );

      expect(result.statements).toHaveLength(1);
      const sql = result.statements[0].sql;
      expect(sql).toContain("*");
    });

    it("handles complex arithmetic expressions", () => {
      const result = translateCypher(
        "MATCH (n:Order) RETURN (n.price * n.quantity) + n.shipping AS totalWithShipping"
      );

      expect(result.statements).toHaveLength(1);
      const sql = result.statements[0].sql;
      expect(sql).toContain("*");
      expect(sql).toContain("+");
    });

    it("handles arithmetic in WHERE clause", () => {
      const result = translateCypher(
        "MATCH (n:Product) WHERE n.price * 2 > 100 RETURN n"
      );

      expect(result.statements).toHaveLength(1);
      const sql = result.statements[0].sql;
      expect(sql).toContain("*");
      expect(sql).toContain(">");
    });

    it("handles arithmetic with parameter", () => {
      const result = translateCypher(
        "MATCH (n:Product) RETURN n.price * $multiplier AS adjusted",
        { multiplier: 1.5 }
      );

      expect(result.statements).toHaveLength(1);
      const sql = result.statements[0].sql;
      expect(sql).toContain("*");
    });
  });

  describe("Date/Time functions", () => {
    it("generates date() for current date", () => {
      const result = translateCypher("RETURN date() AS today");

      expect(result.statements).toHaveLength(1);
      const sql = result.statements[0].sql;
      // SQLite uses DATE('now') for current date
      expect(sql).toMatch(/DATE\('now'\)|date\(/i);
      expect(result.returnColumns).toEqual(["today"]);
    });

    it("handles timestamp() in CREATE and SET", () => {
      // CREATE a node and then SET a timestamp property
      const parseResult = parse(`
        CREATE (n:Event {name: 'Test'})
        SET n.createdAt = timestamp()
      `);
      if (!parseResult.success) throw new Error(`Parse failed: ${parseResult.error.message}`);

      const translator = new Translator();
      const result = translator.translate(parseResult.query);

      // Should have INSERT for CREATE and UPDATE for SET
      expect(result.statements.length).toBeGreaterThanOrEqual(2);
      
      // The SET statement should contain timestamp function translation
      const setStmt = result.statements.find((s) => s.sql.includes("UPDATE"));
      expect(setStmt).toBeDefined();
      expect(setStmt!.sql).toContain("json_set");
      expect(setStmt!.sql).toContain("$.createdAt");
      // Should use strftime for timestamp
      expect(setStmt!.sql).toMatch(/strftime|UNIXEPOCH/i);
    });

    it("handles date() and datetime() in SET", () => {
      const parseResult = parse(`
        CREATE (n:Event {name: 'Test'})
        SET n.eventDate = date(), n.eventTime = datetime()
      `);
      if (!parseResult.success) throw new Error(`Parse failed: ${parseResult.error.message}`);

      const translator = new Translator();
      const result = translator.translate(parseResult.query);

      const setStmts = result.statements.filter((s) => s.sql.includes("UPDATE"));
      expect(setStmts).toHaveLength(2);
      
      // Check date() translation
      const dateStmt = setStmts.find((s) => s.sql.includes("$.eventDate"));
      expect(dateStmt).toBeDefined();
      expect(dateStmt!.sql).toMatch(/DATE\('now'\)/i);
      
      // Check datetime() translation
      const datetimeStmt = setStmts.find((s) => s.sql.includes("$.eventTime"));
      expect(datetimeStmt).toBeDefined();
      expect(datetimeStmt!.sql).toMatch(/DATETIME\('now'\)/i);
    });

    it("handles string functions in SET (toUpper, toLower, trim)", () => {
      const parseResult = parse(`
        CREATE (n:Person {name: ' Alice '})
        SET n.upperName = toUpper(n.name), n.lowerName = toLower(n.name), n.trimmedName = trim(n.name)
      `);
      if (!parseResult.success) throw new Error(`Parse failed: ${parseResult.error.message}`);

      const translator = new Translator();
      const result = translator.translate(parseResult.query);

      const setStmts = result.statements.filter((s) => s.sql.includes("UPDATE"));
      expect(setStmts).toHaveLength(3);
      
      expect(setStmts.find((s) => s.sql.includes("UPPER("))).toBeDefined();
      expect(setStmts.find((s) => s.sql.includes("LOWER("))).toBeDefined();
      expect(setStmts.find((s) => s.sql.includes("TRIM("))).toBeDefined();
    });

    it("handles math functions in SET (abs, round)", () => {
      const parseResult = parse(`
        CREATE (n:Account {balance: -50.7})
        SET n.absBalance = abs(n.balance), n.roundedBalance = round(n.balance)
      `);
      if (!parseResult.success) throw new Error(`Parse failed: ${parseResult.error.message}`);

      const translator = new Translator();
      const result = translator.translate(parseResult.query);

      const setStmts = result.statements.filter((s) => s.sql.includes("UPDATE"));
      expect(setStmts).toHaveLength(2);
      
      expect(setStmts.find((s) => s.sql.includes("ABS("))).toBeDefined();
      expect(setStmts.find((s) => s.sql.includes("ROUND("))).toBeDefined();
    });

    it("handles rand() in SET", () => {
      const parseResult = parse(`
        CREATE (n:Game {name: 'Test'})
        SET n.randomValue = rand()
      `);
      if (!parseResult.success) throw new Error(`Parse failed: ${parseResult.error.message}`);

      const translator = new Translator();
      const result = translator.translate(parseResult.query);

      const setStmt = result.statements.find((s) => s.sql.includes("UPDATE"));
      expect(setStmt).toBeDefined();
      expect(setStmt!.sql).toMatch(/RANDOM/i);
    });

    it("generates datetime() for current datetime", () => {
      const result = translateCypher("RETURN datetime() AS now");

      expect(result.statements).toHaveLength(1);
      const sql = result.statements[0].sql;
      // SQLite uses DATETIME('now') for current datetime
      expect(sql).toMatch(/DATETIME\('now'\)|datetime\(/i);
      expect(result.returnColumns).toEqual(["now"]);
    });

    it("generates timestamp() for unix timestamp", () => {
      const result = translateCypher("RETURN timestamp() AS ts");

      expect(result.statements).toHaveLength(1);
      const sql = result.statements[0].sql;
      // SQLite uses strftime('%s', 'now') * 1000 for millisecond timestamp
      expect(sql).toMatch(/strftime|UNIXEPOCH/i);
      expect(result.returnColumns).toEqual(["ts"]);
    });

    it("generates date() with string argument", () => {
      const result = translateCypher("RETURN date('2024-01-15') AS d");

      expect(result.statements).toHaveLength(1);
      const sql = result.statements[0].sql;
      expect(sql).toMatch(/DATE\(/i);
      expect(result.returnColumns).toEqual(["d"]);
    });

    it("generates datetime() with string argument", () => {
      const result = translateCypher("RETURN datetime('2024-01-15T12:30:00') AS dt");

      expect(result.statements).toHaveLength(1);
      const sql = result.statements[0].sql;
      expect(sql).toMatch(/DATETIME\(/i);
      expect(result.returnColumns).toEqual(["dt"]);
    });

    it("handles date comparison in WHERE clause", () => {
      const result = translateCypher(
        "MATCH (n:Event) WHERE n.date > date('2024-01-01') RETURN n"
      );

      expect(result.statements).toHaveLength(1);
      const sql = result.statements[0].sql;
      expect(sql).toContain(">");
      expect(sql).toMatch(/DATE\(/i);
    });

    it("handles date with MATCH", () => {
      const result = translateCypher(
        "MATCH (n:Event) RETURN n.name, date() AS today"
      );

      expect(result.statements).toHaveLength(1);
      expect(result.returnColumns).toContain("today");
    });
  });

  describe("List concatenation", () => {
    it("translates list literal in RETURN", () => {
      const result = translateCypher("RETURN [1, 2, 3] AS nums");

      expect(result.statements).toHaveLength(1);
      const sql = result.statements[0].sql;
      // Should generate JSON array
      expect(sql).toMatch(/json_array|json\(/i);
      expect(result.returnColumns).toEqual(["nums"]);
    });

    it("translates empty list in RETURN", () => {
      const result = translateCypher("RETURN [] AS empty");

      expect(result.statements).toHaveLength(1);
      const sql = result.statements[0].sql;
      expect(sql).toMatch(/json_array|json\(/i);
      expect(result.returnColumns).toEqual(["empty"]);
    });

    it("translates list concatenation with + operator", () => {
      const result = translateCypher("RETURN [1, 2] + [3, 4] AS combined");

      expect(result.statements).toHaveLength(1);
      const sql = result.statements[0].sql;
      // Should use json() to concatenate arrays
      expect(sql).toMatch(/json|json_array/i);
      expect(result.returnColumns).toEqual(["combined"]);
    });

    it("translates chained list concatenation", () => {
      const result = translateCypher("RETURN [1] + [2] + [3] AS chain");

      expect(result.statements).toHaveLength(1);
      const sql = result.statements[0].sql;
      expect(sql).toMatch(/json/i);
      expect(result.returnColumns).toEqual(["chain"]);
    });

    it("translates property + list concatenation", () => {
      const result = translateCypher(
        "MATCH (n:Item) RETURN n.tags + ['new'] AS allTags"
      );

      expect(result.statements).toHaveLength(1);
      const sql = result.statements[0].sql;
      expect(sql).toMatch(/json/i);
      expect(result.returnColumns).toEqual(["allTags"]);
    });
  });

  describe("List comprehensions", () => {
    it("translates list comprehension with WHERE filter", () => {
      const result = translateCypher("RETURN [x IN [1, 2, 3] WHERE x > 1] AS filtered");

      expect(result.statements).toHaveLength(1);
      const sql = result.statements[0].sql;
      // Should use json_each to iterate and filter
      expect(sql).toMatch(/json_each/i);
      expect(sql).toMatch(/WHERE/i);
      expect(result.returnColumns).toEqual(["filtered"]);
    });

    it("translates list comprehension with map projection", () => {
      const result = translateCypher("RETURN [x IN [1, 2, 3] | x * 2] AS doubled");

      expect(result.statements).toHaveLength(1);
      const sql = result.statements[0].sql;
      // Should use json_each and apply the transformation
      expect(sql).toMatch(/json_each/i);
      expect(result.returnColumns).toEqual(["doubled"]);
    });

    it("translates list comprehension with WHERE and map", () => {
      const result = translateCypher("RETURN [x IN [1, 2, 3, 4] WHERE x > 2 | x * 10] AS result");

      expect(result.statements).toHaveLength(1);
      const sql = result.statements[0].sql;
      // Should use json_each with WHERE filter and map
      expect(sql).toMatch(/json_each/i);
      expect(sql).toMatch(/WHERE/i);
      expect(result.returnColumns).toEqual(["result"]);
    });

    it("translates list comprehension with function as source", () => {
      const result = translateCypher("RETURN [x IN range(1, 5) WHERE x % 2 = 0] AS evens");

      expect(result.statements).toHaveLength(1);
      const sql = result.statements[0].sql;
      // Should handle range() function as source
      expect(sql).toMatch(/json_each/i);
      expect(result.returnColumns).toEqual(["evens"]);
    });

    it("translates list comprehension with property access as source", () => {
      const result = translateCypher("MATCH (n:Item) RETURN [x IN n.values WHERE x > 2] AS filtered");

      expect(result.statements).toHaveLength(1);
      const sql = result.statements[0].sql;
      expect(sql).toMatch(/json_each/i);
      expect(result.returnColumns).toEqual(["filtered"]);
    });

    it("translates list comprehension without filter or map (identity)", () => {
      const result = translateCypher("RETURN [x IN [1, 2, 3]] AS same");

      expect(result.statements).toHaveLength(1);
      const sql = result.statements[0].sql;
      expect(sql).toMatch(/json_each/i);
      expect(result.returnColumns).toEqual(["same"]);
    });
  });

  describe("Extended string functions", () => {
    it("translates left() function", () => {
      const result = translateCypher("RETURN left('hello', 3) AS result");

      expect(result.statements).toHaveLength(1);
      const sql = result.statements[0].sql;
      // SQLite uses SUBSTR(string, 1, length) for left()
      expect(sql).toMatch(/SUBSTR/i);
      expect(result.returnColumns).toEqual(["result"]);
    });

    it("translates left() with property access", () => {
      const result = translateCypher("MATCH (n:Item) RETURN left(n.name, 4) AS prefix");

      expect(result.statements).toHaveLength(1);
      const sql = result.statements[0].sql;
      expect(sql).toMatch(/SUBSTR/i);
      expect(sql).toMatch(/json_extract/i);
      expect(result.returnColumns).toEqual(["prefix"]);
    });

    it("translates right() function", () => {
      const result = translateCypher("RETURN right('hello', 3) AS result");

      expect(result.statements).toHaveLength(1);
      const sql = result.statements[0].sql;
      // SQLite uses SUBSTR with negative offset for right()
      expect(sql).toMatch(/SUBSTR/i);
      expect(result.returnColumns).toEqual(["result"]);
    });

    it("translates right() with property access", () => {
      const result = translateCypher("MATCH (n:Item) RETURN right(n.name, 3) AS suffix");

      expect(result.statements).toHaveLength(1);
      const sql = result.statements[0].sql;
      expect(sql).toMatch(/SUBSTR/i);
      expect(result.returnColumns).toEqual(["suffix"]);
    });

    it("translates ltrim() function", () => {
      const result = translateCypher("RETURN ltrim('   hello') AS result");

      expect(result.statements).toHaveLength(1);
      const sql = result.statements[0].sql;
      expect(sql).toMatch(/LTRIM/i);
      expect(result.returnColumns).toEqual(["result"]);
    });

    it("translates ltrim() with property access", () => {
      const result = translateCypher("MATCH (n:Item) RETURN ltrim(n.name) AS trimmed");

      expect(result.statements).toHaveLength(1);
      const sql = result.statements[0].sql;
      expect(sql).toMatch(/LTRIM/i);
      expect(result.returnColumns).toEqual(["trimmed"]);
    });

    it("translates rtrim() function", () => {
      const result = translateCypher("RETURN rtrim('hello   ') AS result");

      expect(result.statements).toHaveLength(1);
      const sql = result.statements[0].sql;
      expect(sql).toMatch(/RTRIM/i);
      expect(result.returnColumns).toEqual(["result"]);
    });

    it("translates rtrim() with property access", () => {
      const result = translateCypher("MATCH (n:Item) RETURN rtrim(n.name) AS trimmed");

      expect(result.statements).toHaveLength(1);
      const sql = result.statements[0].sql;
      expect(sql).toMatch(/RTRIM/i);
      expect(result.returnColumns).toEqual(["trimmed"]);
    });

    it("translates reverse() function", () => {
      const result = translateCypher("RETURN reverse('hello') AS result");

      expect(result.statements).toHaveLength(1);
      const sql = result.statements[0].sql;
      // SQLite doesn't have native REVERSE, uses recursive CTE
      expect(sql).toMatch(/RECURSIVE.*rev/i);
      expect(result.returnColumns).toEqual(["result"]);
    });

    it("translates reverse() with property access", () => {
      const result = translateCypher("MATCH (n:Item) RETURN reverse(n.name) AS reversed");

      expect(result.statements).toHaveLength(1);
      const sql = result.statements[0].sql;
      expect(sql).toMatch(/REVERSE/i);
      expect(result.returnColumns).toEqual(["reversed"]);
    });
  });

  describe("Percentile functions", () => {
    it("translates percentileDisc() with property access", () => {
      const result = translateCypher("MATCH (n:Score) RETURN percentileDisc(n.value, 0.5) AS median");

      expect(result.statements).toHaveLength(1);
      const sql = result.statements[0].sql;
      // Should generate SQL that computes discrete percentile
      expect(sql).toContain("json_extract");
      expect(sql).toContain("json_group_array");
      expect(result.returnColumns).toEqual(["median"]);
    });

    it("translates percentileCont() with property access", () => {
      const result = translateCypher("MATCH (n:Score) RETURN percentileCont(n.value, 0.5) AS median");

      expect(result.statements).toHaveLength(1);
      const sql = result.statements[0].sql;
      // Should generate SQL that computes continuous (interpolated) percentile
      expect(sql).toContain("json_extract");
      expect(result.returnColumns).toEqual(["median"]);
    });

    it("translates percentileDisc() with 0 percentile (minimum)", () => {
      const result = translateCypher("MATCH (n:Score) RETURN percentileDisc(n.value, 0) AS minVal");

      expect(result.statements).toHaveLength(1);
      expect(result.returnColumns).toEqual(["minVal"]);
    });

    it("translates percentileDisc() with 1 percentile (maximum)", () => {
      const result = translateCypher("MATCH (n:Score) RETURN percentileDisc(n.value, 1) AS maxVal");

      expect(result.statements).toHaveLength(1);
      expect(result.returnColumns).toEqual(["maxVal"]);
    });

    it("translates percentileCont() with parameter percentile", () => {
      const result = translateCypher(
        "MATCH (n:Score) RETURN percentileCont(n.value, $p) AS pval",
        { p: 0.9 }
      );

      expect(result.statements).toHaveLength(1);
      expect(result.returnColumns).toEqual(["pval"]);
    });

    it("includes percentile functions in aggregate function list", () => {
      // Test that percentile functions are recognized as aggregate functions
      // by checking the Translator's isAggregateExpression method
      // Note: GROUP BY generation for mixed aggregate/non-aggregate expressions
      // is not fully implemented in the translator yet
      const result = translateCypher(`
        MATCH (n:Score)
        RETURN percentileDisc(n.value, 0.5) AS median
      `);

      expect(result.statements).toHaveLength(1);
      const sql = result.statements[0].sql;
      // Should generate aggregate function SQL
      expect(sql).toContain("json_group_array");
    });
  });

  describe("List predicates (ALL, ANY, NONE, SINGLE)", () => {
    describe("ALL()", () => {
      it("translates ALL() with literal list", () => {
        const result = translateCypher("RETURN ALL(x IN [1, 2, 3] WHERE x > 0) AS result");

        expect(result.statements).toHaveLength(1);
        const sql = result.statements[0].sql;
        // ALL returns true when count of failing elements = 0
        // Or: NOT EXISTS(... WHERE NOT condition)
        expect(sql).toContain("json_each");
        expect(result.returnColumns).toEqual(["result"]);
      });

      it("translates ALL() with property access", () => {
        const result = translateCypher("MATCH (n:Item) RETURN ALL(x IN n.values WHERE x > 10) AS allAbove10");

        expect(result.statements).toHaveLength(1);
        const sql = result.statements[0].sql;
        expect(sql).toContain("json_each");
        expect(result.returnColumns).toEqual(["allAbove10"]);
      });

      it("translates ALL() in WHERE clause", () => {
        const result = translateCypher("MATCH (n:Item) WHERE ALL(x IN n.scores WHERE x >= 10) RETURN n");

        expect(result.statements).toHaveLength(1);
        const sql = result.statements[0].sql;
        expect(sql).toContain("WHERE");
        expect(sql).toContain("json_each");
      });
    });

    describe("ANY()", () => {
      it("translates ANY() with literal list", () => {
        const result = translateCypher("RETURN ANY(x IN [1, 2, 3] WHERE x > 2) AS result");

        expect(result.statements).toHaveLength(1);
        const sql = result.statements[0].sql;
        // ANY returns true when count of matching elements > 0
        // Or: EXISTS(... WHERE condition)
        expect(sql).toContain("json_each");
        expect(result.returnColumns).toEqual(["result"]);
      });

      it("translates ANY() with property access", () => {
        const result = translateCypher("MATCH (n:Task) RETURN ANY(t IN n.tags WHERE t = 'urgent') AS hasUrgent");

        expect(result.statements).toHaveLength(1);
        const sql = result.statements[0].sql;
        expect(sql).toContain("json_each");
        expect(result.returnColumns).toEqual(["hasUrgent"]);
      });

      it("translates ANY() in WHERE clause", () => {
        const result = translateCypher("MATCH (n:Task) WHERE ANY(t IN n.tags WHERE t = 'urgent') RETURN n");

        expect(result.statements).toHaveLength(1);
        const sql = result.statements[0].sql;
        expect(sql).toContain("WHERE");
        expect(sql).toContain("json_each");
      });
    });

    describe("NONE()", () => {
      it("translates NONE() with literal list", () => {
        const result = translateCypher("RETURN NONE(x IN [1, 2, 3] WHERE x > 10) AS result");

        expect(result.statements).toHaveLength(1);
        const sql = result.statements[0].sql;
        // NONE returns true when count of matching elements = 0
        // Or: NOT EXISTS(... WHERE condition)
        expect(sql).toContain("json_each");
        expect(result.returnColumns).toEqual(["result"]);
      });

      it("translates NONE() with property access", () => {
        const result = translateCypher("MATCH (n:Item) RETURN NONE(x IN n.values WHERE x < 0) AS noneNegative");

        expect(result.statements).toHaveLength(1);
        const sql = result.statements[0].sql;
        expect(sql).toContain("json_each");
        expect(result.returnColumns).toEqual(["noneNegative"]);
      });

      it("translates NONE() in WHERE clause", () => {
        const result = translateCypher("MATCH (n:Product) WHERE NONE(r IN n.reviews WHERE r < 3) RETURN n");

        expect(result.statements).toHaveLength(1);
        const sql = result.statements[0].sql;
        expect(sql).toContain("WHERE");
        expect(sql).toContain("json_each");
      });
    });

    describe("SINGLE()", () => {
      it("translates SINGLE() with literal list", () => {
        const result = translateCypher("RETURN SINGLE(x IN [1, 2, 10] WHERE x > 5) AS result");

        expect(result.statements).toHaveLength(1);
        const sql = result.statements[0].sql;
        // SINGLE returns true when count of matching elements = 1
        expect(sql).toContain("json_each");
        expect(result.returnColumns).toEqual(["result"]);
      });

      it("translates SINGLE() with property access", () => {
        const result = translateCypher("MATCH (n:Item) RETURN SINGLE(x IN n.values WHERE x > 50) AS singleLarge");

        expect(result.statements).toHaveLength(1);
        const sql = result.statements[0].sql;
        expect(sql).toContain("json_each");
        expect(result.returnColumns).toEqual(["singleLarge"]);
      });

      it("translates SINGLE() in WHERE clause", () => {
        const result = translateCypher("MATCH (n:Team) WHERE SINGLE(m IN n.members WHERE m = 'Alice') RETURN n");

        expect(result.statements).toHaveLength(1);
        const sql = result.statements[0].sql;
        expect(sql).toContain("WHERE");
        expect(sql).toContain("json_each");
      });
    });

    describe("Combined list predicates", () => {
      it("translates AND with two list predicates", () => {
        const result = translateCypher("RETURN ALL(x IN [1, 2] WHERE x > 0) AND ANY(x IN [1, 2] WHERE x > 1) AS result");

        expect(result.statements).toHaveLength(1);
        const sql = result.statements[0].sql;
        expect(sql).toContain("AND");
        expect(result.returnColumns).toEqual(["result"]);
      });

      it("translates NOT with list predicate", () => {
        const result = translateCypher("RETURN NOT ALL(x IN [1, -1] WHERE x > 0) AS result");

        expect(result.statements).toHaveLength(1);
        const sql = result.statements[0].sql;
        expect(sql).toContain("NOT");
        expect(result.returnColumns).toEqual(["result"]);
      });

      it("translates list predicate with range() source", () => {
        const result = translateCypher("RETURN ALL(x IN range(1, 5) WHERE x > 0) AS result");

        expect(result.statements).toHaveLength(1);
        const sql = result.statements[0].sql;
        expect(sql).toContain("json_each");
        expect(result.returnColumns).toEqual(["result"]);
      });
    });
  });
});
