import { describe, it, expect } from "vitest";
import {
  parse,
  Query,
  CreateClause,
  MatchClause,
  MergeClause,
  SetClause,
  DeleteClause,
  ReturnClause,
  NodePattern,
  RelationshipPattern,
} from "../src/parser";

// Helper to assert success and get query
function expectSuccess(input: string): Query {
  const result = parse(input);
  if (!result.success) {
    throw new Error(`Parse failed: ${result.error.message} at position ${result.error.position}`);
  }
  return result.query;
}

// Helper to assert failure and get error
function expectError(input: string) {
  const result = parse(input);
  if (result.success) {
    throw new Error(`Expected parse to fail, but got: ${JSON.stringify(result.query)}`);
  }
  return result.error;
}

describe("Parser", () => {
  describe("CREATE nodes", () => {
    it("parses node with label only", () => {
      const query = expectSuccess("CREATE (n:Person)");
      expect(query.clauses).toHaveLength(1);

      const clause = query.clauses[0] as CreateClause;
      expect(clause.type).toBe("CREATE");
      expect(clause.patterns).toHaveLength(1);

      const node = clause.patterns[0] as NodePattern;
      expect(node.variable).toBe("n");
      expect(node.label).toBe("Person");
      expect(node.properties).toBeUndefined();
    });

    it("parses node with label and properties", () => {
      const query = expectSuccess("CREATE (n:Person {name: 'Alice', age: 30})");
      const clause = query.clauses[0] as CreateClause;
      const node = clause.patterns[0] as NodePattern;

      expect(node.label).toBe("Person");
      expect(node.properties).toEqual({ name: "Alice", age: 30 });
    });

    it("parses node without variable", () => {
      const query = expectSuccess("CREATE (:Person {name: 'Alice'})");
      const clause = query.clauses[0] as CreateClause;
      const node = clause.patterns[0] as NodePattern;

      expect(node.variable).toBeUndefined();
      expect(node.label).toBe("Person");
      expect(node.properties).toEqual({ name: "Alice" });
    });

    it("parses node with nested object values", () => {
      const query = expectSuccess("CREATE (n:Person {name: 'Alice', active: true, score: null})");
      const clause = query.clauses[0] as CreateClause;
      const node = clause.patterns[0] as NodePattern;

      expect(node.properties).toEqual({
        name: "Alice",
        active: true,
        score: null,
      });
    });

    it("parses node with array property", () => {
      const query = expectSuccess("CREATE (n:Person {tags: ['dev', 'admin']})");
      const clause = query.clauses[0] as CreateClause;
      const node = clause.patterns[0] as NodePattern;

      expect(node.properties).toEqual({
        tags: ["dev", "admin"],
      });
    });

    it("parses multiple nodes in one CREATE", () => {
      const query = expectSuccess("CREATE (a:Person), (b:Company)");
      const clause = query.clauses[0] as CreateClause;

      expect(clause.patterns).toHaveLength(2);
      expect((clause.patterns[0] as NodePattern).label).toBe("Person");
      expect((clause.patterns[1] as NodePattern).label).toBe("Company");
    });
  });

  describe("CREATE relationships", () => {
    it("parses relationship between two nodes", () => {
      const query = expectSuccess("CREATE (a:Person)-[:KNOWS]->(b:Person)");
      const clause = query.clauses[0] as CreateClause;
      const rel = clause.patterns[0] as RelationshipPattern;

      expect(rel.source.label).toBe("Person");
      expect(rel.source.variable).toBe("a");
      expect(rel.edge.type).toBe("KNOWS");
      expect(rel.edge.direction).toBe("right");
      expect(rel.target.label).toBe("Person");
      expect(rel.target.variable).toBe("b");
    });

    it("parses relationship with properties", () => {
      const query = expectSuccess("CREATE (a:Person)-[:KNOWS {since: 2020}]->(b:Person)");
      const clause = query.clauses[0] as CreateClause;
      const rel = clause.patterns[0] as RelationshipPattern;

      expect(rel.edge.properties).toEqual({ since: 2020 });
    });

    it("parses relationship with variable", () => {
      const query = expectSuccess("CREATE (a:Person)-[r:KNOWS]->(b:Person)");
      const clause = query.clauses[0] as CreateClause;
      const rel = clause.patterns[0] as RelationshipPattern;

      expect(rel.edge.variable).toBe("r");
      expect(rel.edge.type).toBe("KNOWS");
    });

    it("parses left-directed relationship", () => {
      const query = expectSuccess("CREATE (a:Person)<-[:KNOWS]-(b:Person)");
      const clause = query.clauses[0] as CreateClause;
      const rel = clause.patterns[0] as RelationshipPattern;

      expect(rel.edge.direction).toBe("left");
    });

    it("parses undirected relationship", () => {
      const query = expectSuccess("CREATE (a:Person)-[:KNOWS]-(b:Person)");
      const clause = query.clauses[0] as CreateClause;
      const rel = clause.patterns[0] as RelationshipPattern;

      expect(rel.edge.direction).toBe("none");
    });
  });

  describe("MATCH", () => {
    it("parses simple node match", () => {
      const query = expectSuccess("MATCH (n:Person) RETURN n");
      const clause = query.clauses[0] as MatchClause;

      expect(clause.type).toBe("MATCH");
      expect(clause.patterns).toHaveLength(1);

      const node = clause.patterns[0] as NodePattern;
      expect(node.label).toBe("Person");
    });

    it("parses match with properties", () => {
      const query = expectSuccess("MATCH (n:Person {name: 'Alice'}) RETURN n");
      const clause = query.clauses[0] as MatchClause;
      const node = clause.patterns[0] as NodePattern;

      expect(node.properties).toEqual({ name: "Alice" });
    });

    it("parses match with relationship", () => {
      const query = expectSuccess("MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN a, b");
      const clause = query.clauses[0] as MatchClause;
      const rel = clause.patterns[0] as RelationshipPattern;

      expect(rel.source.label).toBe("Person");
      expect(rel.edge.type).toBe("KNOWS");
      expect(rel.target.label).toBe("Person");
    });

    it("parses match with relationship variable", () => {
      const query = expectSuccess("MATCH (a:Person)-[r:KNOWS]->(b:Person) RETURN r");
      const clause = query.clauses[0] as MatchClause;
      const rel = clause.patterns[0] as RelationshipPattern;

      expect(rel.edge.variable).toBe("r");
    });

    it("parses match without relationship type", () => {
      const query = expectSuccess("MATCH (a)-[r]->(b) RETURN r");
      const clause = query.clauses[0] as MatchClause;
      const rel = clause.patterns[0] as RelationshipPattern;

      expect(rel.edge.variable).toBe("r");
      expect(rel.edge.type).toBeUndefined();
    });

    it("parses multi-hop relationship pattern", () => {
      const query = expectSuccess(
        "MATCH (u:User)-[:HAS_INVOICE]->(i:Invoice)-[:BILLED_TO]->(c:Customer) RETURN i, c"
      );
      const clause = query.clauses[0] as MatchClause;

      // Should have 2 relationship patterns
      expect(clause.patterns).toHaveLength(2);

      const rel1 = clause.patterns[0] as RelationshipPattern;
      expect(rel1.source.variable).toBe("u");
      expect(rel1.source.label).toBe("User");
      expect(rel1.edge.type).toBe("HAS_INVOICE");
      expect(rel1.target.variable).toBe("i");
      expect(rel1.target.label).toBe("Invoice");

      const rel2 = clause.patterns[1] as RelationshipPattern;
      expect(rel2.source.variable).toBe("i");
      expect(rel2.source.label).toBeUndefined(); // References previous node
      expect(rel2.edge.type).toBe("BILLED_TO");
      expect(rel2.target.variable).toBe("c");
      expect(rel2.target.label).toBe("Customer");
    });

    it("parses multi-hop with properties on nodes", () => {
      const query = expectSuccess(
        "MATCH (u:User {id: $userId})-[:HAS_INVOICE]->(i:Invoice)-[:BILLED_TO]->(c:Customer) RETURN i"
      );
      const clause = query.clauses[0] as MatchClause;

      expect(clause.patterns).toHaveLength(2);
      const rel1 = clause.patterns[0] as RelationshipPattern;
      expect(rel1.source.properties).toEqual({ id: { type: "parameter", name: "userId" } });
    });

    it("parses three-hop relationship pattern", () => {
      const query = expectSuccess(
        "MATCH (a:A)-[:R1]->(b:B)-[:R2]->(c:C)-[:R3]->(d:D) RETURN a, d"
      );
      const clause = query.clauses[0] as MatchClause;

      expect(clause.patterns).toHaveLength(3);
    });
  });

  describe("WHERE", () => {
    it("parses WHERE with equals comparison", () => {
      const query = expectSuccess("MATCH (n:Person) WHERE n.name = 'Alice' RETURN n");
      const clause = query.clauses[0] as MatchClause;

      expect(clause.where).toBeDefined();
      expect(clause.where!.type).toBe("comparison");
      expect(clause.where!.operator).toBe("=");
    });

    it("parses WHERE with not equals", () => {
      const query = expectSuccess("MATCH (n:Person) WHERE n.name <> 'Bob' RETURN n");
      const clause = query.clauses[0] as MatchClause;

      expect(clause.where!.operator).toBe("<>");
    });

    it("parses WHERE with less than", () => {
      const query = expectSuccess("MATCH (n:Person) WHERE n.age < 30 RETURN n");
      const clause = query.clauses[0] as MatchClause;

      expect(clause.where!.operator).toBe("<");
    });

    it("parses WHERE with greater than", () => {
      const query = expectSuccess("MATCH (n:Person) WHERE n.age > 25 RETURN n");
      const clause = query.clauses[0] as MatchClause;

      expect(clause.where!.operator).toBe(">");
    });

    it("parses WHERE with less than or equal", () => {
      const query = expectSuccess("MATCH (n:Person) WHERE n.age <= 30 RETURN n");
      const clause = query.clauses[0] as MatchClause;

      expect(clause.where!.operator).toBe("<=");
    });

    it("parses WHERE with greater than or equal", () => {
      const query = expectSuccess("MATCH (n:Person) WHERE n.age >= 25 RETURN n");
      const clause = query.clauses[0] as MatchClause;

      expect(clause.where!.operator).toBe(">=");
    });

    it("parses WHERE with AND", () => {
      const query = expectSuccess("MATCH (n:Person) WHERE n.age > 25 AND n.age < 40 RETURN n");
      const clause = query.clauses[0] as MatchClause;

      expect(clause.where!.type).toBe("and");
      expect(clause.where!.conditions).toHaveLength(2);
    });

    it("parses WHERE with OR", () => {
      const query = expectSuccess("MATCH (n:Person) WHERE n.name = 'Alice' OR n.name = 'Bob' RETURN n");
      const clause = query.clauses[0] as MatchClause;

      expect(clause.where!.type).toBe("or");
      expect(clause.where!.conditions).toHaveLength(2);
    });

    it("parses WHERE with NOT", () => {
      const query = expectSuccess("MATCH (n:Person) WHERE NOT n.active = false RETURN n");
      const clause = query.clauses[0] as MatchClause;

      expect(clause.where!.type).toBe("not");
      expect(clause.where!.condition).toBeDefined();
    });

    it("parses WHERE with CONTAINS", () => {
      const query = expectSuccess("MATCH (n:Person) WHERE n.name CONTAINS 'ali' RETURN n");
      const clause = query.clauses[0] as MatchClause;

      expect(clause.where!.type).toBe("contains");
    });

    it("parses WHERE with STARTS WITH", () => {
      const query = expectSuccess("MATCH (n:Person) WHERE n.name STARTS WITH 'A' RETURN n");
      const clause = query.clauses[0] as MatchClause;

      expect(clause.where!.type).toBe("startsWith");
    });

    it("parses WHERE with ENDS WITH", () => {
      const query = expectSuccess("MATCH (n:Person) WHERE n.name ENDS WITH 'e' RETURN n");
      const clause = query.clauses[0] as MatchClause;

      expect(clause.where!.type).toBe("endsWith");
    });

    it("parses WHERE with parameter", () => {
      const query = expectSuccess("MATCH (n:Person) WHERE n.id = $id RETURN n");
      const clause = query.clauses[0] as MatchClause;

      expect(clause.where!.right!.type).toBe("parameter");
      expect(clause.where!.right!.name).toBe("id");
    });

    it("parses WHERE with IS NULL", () => {
      const query = expectSuccess("MATCH (n:Person) WHERE n.deleted IS NULL RETURN n");
      const clause = query.clauses[0] as MatchClause;

      expect(clause.where!.type).toBe("isNull");
      expect(clause.where!.left!.type).toBe("property");
      expect(clause.where!.left!.property).toBe("deleted");
    });

    it("parses WHERE with IS NOT NULL", () => {
      const query = expectSuccess("MATCH (n:Person) WHERE n.email IS NOT NULL RETURN n");
      const clause = query.clauses[0] as MatchClause;

      expect(clause.where!.type).toBe("isNotNull");
      expect(clause.where!.left!.type).toBe("property");
      expect(clause.where!.left!.property).toBe("email");
    });

    it("parses WHERE with parenthesized conditions", () => {
      const query = expectSuccess("MATCH (n:Person) WHERE n.age > 18 AND (n.role = 'admin' OR n.role = 'mod') RETURN n");
      const clause = query.clauses[0] as MatchClause;

      expect(clause.where!.type).toBe("and");
      expect(clause.where!.conditions).toHaveLength(2);
      // Second condition should be an OR
      expect(clause.where!.conditions![1].type).toBe("or");
    });

    it("parses WHERE with IS NULL in OR expression", () => {
      const query = expectSuccess("MATCH (n:Person) WHERE n.archived IS NULL OR n.archived = false RETURN n");
      const clause = query.clauses[0] as MatchClause;

      expect(clause.where!.type).toBe("or");
      expect(clause.where!.conditions).toHaveLength(2);
      expect(clause.where!.conditions![0].type).toBe("isNull");
      expect(clause.where!.conditions![1].type).toBe("comparison");
    });

    it("parses complex WHERE with parentheses and IS NULL", () => {
      const query = expectSuccess(
        "MATCH (u:User)-[:HAS]->(c:Customer) WHERE u.id = $userId AND (c.archived IS NULL OR c.archived = false) RETURN c"
      );
      const clause = query.clauses[0] as MatchClause;

      expect(clause.where!.type).toBe("and");
      expect(clause.where!.conditions).toHaveLength(2);
      // Second condition should be the parenthesized OR
      expect(clause.where!.conditions![1].type).toBe("or");
    });
  });

  describe("MERGE", () => {
    it("parses simple MERGE", () => {
      const query = expectSuccess("MERGE (n:Person {id: 'abc123'})");
      const clause = query.clauses[0] as MergeClause;

      expect(clause.type).toBe("MERGE");
      expect(clause.pattern.label).toBe("Person");
      expect(clause.pattern.properties).toEqual({ id: "abc123" });
    });

    it("parses MERGE with ON CREATE SET", () => {
      const query = expectSuccess("MERGE (n:Person {id: 'abc123'}) ON CREATE SET n.created = true");
      const clause = query.clauses[0] as MergeClause;

      expect(clause.onCreateSet).toHaveLength(1);
      expect(clause.onCreateSet![0].variable).toBe("n");
      expect(clause.onCreateSet![0].property).toBe("created");
    });

    it("parses MERGE with ON MATCH SET", () => {
      const query = expectSuccess("MERGE (n:Person {id: 'abc123'}) ON MATCH SET n.updated = true");
      const clause = query.clauses[0] as MergeClause;

      expect(clause.onMatchSet).toHaveLength(1);
      expect(clause.onMatchSet![0].variable).toBe("n");
      expect(clause.onMatchSet![0].property).toBe("updated");
    });

    it("parses MERGE with both ON CREATE and ON MATCH", () => {
      const query = expectSuccess(
        "MERGE (n:Person {id: 'abc123'}) ON CREATE SET n.created = true ON MATCH SET n.updated = true"
      );
      const clause = query.clauses[0] as MergeClause;

      expect(clause.onCreateSet).toHaveLength(1);
      expect(clause.onMatchSet).toHaveLength(1);
    });
  });

  describe("SET", () => {
    it("parses SET with single property", () => {
      const query = expectSuccess("MATCH (n:Person {id: 'abc123'}) SET n.name = 'Bob'");
      expect(query.clauses).toHaveLength(2);

      const setClause = query.clauses[1] as SetClause;
      expect(setClause.type).toBe("SET");
      expect(setClause.assignments).toHaveLength(1);
      expect(setClause.assignments[0].variable).toBe("n");
      expect(setClause.assignments[0].property).toBe("name");
    });

    it("parses SET with multiple properties", () => {
      const query = expectSuccess("MATCH (n:Person {id: 'abc123'}) SET n.name = 'Bob', n.age = 31");
      const setClause = query.clauses[1] as SetClause;

      expect(setClause.assignments).toHaveLength(2);
      expect(setClause.assignments[0].property).toBe("name");
      expect(setClause.assignments[1].property).toBe("age");
    });

    it("parses SET with parameter value", () => {
      const query = expectSuccess("MATCH (n:Person {id: 'abc123'}) SET n.name = $name");
      const setClause = query.clauses[1] as SetClause;

      expect(setClause.assignments[0].value.type).toBe("parameter");
      expect(setClause.assignments[0].value.name).toBe("name");
    });
  });

  describe("DELETE", () => {
    it("parses DELETE node", () => {
      const query = expectSuccess("MATCH (n:Person {id: 'abc123'}) DELETE n");
      const deleteClause = query.clauses[1] as DeleteClause;

      expect(deleteClause.type).toBe("DELETE");
      expect(deleteClause.variables).toEqual(["n"]);
      expect(deleteClause.detach).toBe(false);
    });

    it("parses DELETE multiple variables", () => {
      const query = expectSuccess("MATCH (a)-[r]->(b) DELETE r, a, b");
      const deleteClause = query.clauses[1] as DeleteClause;

      expect(deleteClause.variables).toEqual(["r", "a", "b"]);
    });

    it("parses DETACH DELETE", () => {
      const query = expectSuccess("MATCH (n:Person {id: 'abc123'}) DETACH DELETE n");
      const deleteClause = query.clauses[1] as DeleteClause;

      expect(deleteClause.detach).toBe(true);
    });
  });

  describe("RETURN", () => {
    it("parses RETURN with variable", () => {
      const query = expectSuccess("MATCH (n:Person) RETURN n");
      const returnClause = query.clauses[1] as ReturnClause;

      expect(returnClause.type).toBe("RETURN");
      expect(returnClause.items).toHaveLength(1);
      expect(returnClause.items[0].expression.type).toBe("variable");
      expect(returnClause.items[0].expression.variable).toBe("n");
    });

    it("parses RETURN with property", () => {
      const query = expectSuccess("MATCH (n:Person) RETURN n.name");
      const returnClause = query.clauses[1] as ReturnClause;

      expect(returnClause.items[0].expression.type).toBe("property");
      expect(returnClause.items[0].expression.variable).toBe("n");
      expect(returnClause.items[0].expression.property).toBe("name");
    });

    it("parses RETURN with multiple items", () => {
      const query = expectSuccess("MATCH (n:Person) RETURN n.name, n.age");
      const returnClause = query.clauses[1] as ReturnClause;

      expect(returnClause.items).toHaveLength(2);
    });

    it("parses RETURN with alias", () => {
      const query = expectSuccess("MATCH (n:Person) RETURN n.name AS personName");
      const returnClause = query.clauses[1] as ReturnClause;

      expect(returnClause.items[0].alias).toBe("personName");
    });

    it("parses RETURN with COUNT", () => {
      const query = expectSuccess("MATCH (n:Person) RETURN COUNT(n)");
      const returnClause = query.clauses[1] as ReturnClause;

      expect(returnClause.items[0].expression.type).toBe("function");
      expect(returnClause.items[0].expression.functionName).toBe("COUNT");
    });

    it("parses RETURN with LIMIT", () => {
      const query = expectSuccess("MATCH (n:Person) RETURN n LIMIT 10");
      const returnClause = query.clauses[1] as ReturnClause;

      expect(returnClause.limit).toBe(10);
    });

    it("parses RETURN COUNT(*) with alias and LIMIT", () => {
      const query = expectSuccess("MATCH (n:Person) RETURN COUNT(n) AS total LIMIT 1");
      const returnClause = query.clauses[1] as ReturnClause;

      expect(returnClause.items[0].expression.functionName).toBe("COUNT");
      expect(returnClause.items[0].alias).toBe("total");
      expect(returnClause.limit).toBe(1);
    });

    it("parses RETURN with ORDER BY single property", () => {
      const query = expectSuccess("MATCH (n:Person) RETURN n ORDER BY n.name");
      const returnClause = query.clauses[1] as ReturnClause;

      expect(returnClause.orderBy).toBeDefined();
      expect(returnClause.orderBy).toHaveLength(1);
      expect(returnClause.orderBy![0].expression.type).toBe("property");
      expect(returnClause.orderBy![0].expression.variable).toBe("n");
      expect(returnClause.orderBy![0].expression.property).toBe("name");
      expect(returnClause.orderBy![0].direction).toBe("ASC");
    });

    it("parses ORDER BY with explicit ASC", () => {
      const query = expectSuccess("MATCH (n:Person) RETURN n ORDER BY n.name ASC");
      const returnClause = query.clauses[1] as ReturnClause;

      expect(returnClause.orderBy![0].direction).toBe("ASC");
    });

    it("parses ORDER BY with DESC", () => {
      const query = expectSuccess("MATCH (n:Person) RETURN n ORDER BY n.age DESC");
      const returnClause = query.clauses[1] as ReturnClause;

      expect(returnClause.orderBy![0].direction).toBe("DESC");
    });

    it("parses ORDER BY with multiple fields", () => {
      const query = expectSuccess("MATCH (n:Person) RETURN n ORDER BY n.name ASC, n.age DESC");
      const returnClause = query.clauses[1] as ReturnClause;

      expect(returnClause.orderBy).toHaveLength(2);
      expect(returnClause.orderBy![0].expression.property).toBe("name");
      expect(returnClause.orderBy![0].direction).toBe("ASC");
      expect(returnClause.orderBy![1].expression.property).toBe("age");
      expect(returnClause.orderBy![1].direction).toBe("DESC");
    });

    it("parses ORDER BY with LIMIT", () => {
      const query = expectSuccess("MATCH (n:Person) RETURN n ORDER BY n.name LIMIT 10");
      const returnClause = query.clauses[1] as ReturnClause;

      expect(returnClause.orderBy).toHaveLength(1);
      expect(returnClause.limit).toBe(10);
    });

    it("parses RETURN with SKIP", () => {
      const query = expectSuccess("MATCH (n:Person) RETURN n SKIP 5");
      const returnClause = query.clauses[1] as ReturnClause;

      expect(returnClause.skip).toBe(5);
    });

    it("parses SKIP with LIMIT", () => {
      const query = expectSuccess("MATCH (n:Person) RETURN n SKIP 10 LIMIT 5");
      const returnClause = query.clauses[1] as ReturnClause;

      expect(returnClause.skip).toBe(10);
      expect(returnClause.limit).toBe(5);
    });

    it("parses ORDER BY with SKIP and LIMIT", () => {
      const query = expectSuccess("MATCH (n:Person) RETURN n ORDER BY n.name SKIP 10 LIMIT 5");
      const returnClause = query.clauses[1] as ReturnClause;

      expect(returnClause.orderBy).toHaveLength(1);
      expect(returnClause.skip).toBe(10);
      expect(returnClause.limit).toBe(5);
    });

    it("allows keywords as aliases", () => {
      // 'count' is a keyword but should work as an alias
      const query = expectSuccess("MATCH (n) RETURN COUNT(n) AS count");
      const returnClause = query.clauses[1] as ReturnClause;

      expect(returnClause.items[0].expression.functionName).toBe("COUNT");
      expect(returnClause.items[0].alias).toBe("count");
    });

    it("allows lowercase 'as' for aliases", () => {
      const query = expectSuccess("MATCH (n) RETURN n.name as name");
      const returnClause = query.clauses[1] as ReturnClause;

      expect(returnClause.items[0].alias).toBe("name");
    });

    it("allows various keywords as aliases", () => {
      // Test multiple keywords that might be used as aliases
      expectSuccess("MATCH (n) RETURN n.id AS id");
      expectSuccess("MATCH (n) RETURN n.type AS type");
      expectSuccess("MATCH (n) RETURN n.name AS name");
      expectSuccess("MATCH (n) RETURN COUNT(n) AS total");
      expectSuccess("MATCH (n) RETURN COUNT(n) AS count");
      expectSuccess("MATCH (n) RETURN n AS match"); // 'match' is a keyword
    });

    it("parses standalone RETURN with literal", () => {
      const query = expectSuccess("RETURN 1");
      expect(query.clauses).toHaveLength(1);

      const returnClause = query.clauses[0] as ReturnClause;
      expect(returnClause.type).toBe("RETURN");
      expect(returnClause.items).toHaveLength(1);
      expect(returnClause.items[0].expression.type).toBe("literal");
      expect(returnClause.items[0].expression.value).toBe(1);
    });

    it("parses RETURN DISTINCT with variable", () => {
      const query = expectSuccess("MATCH (n:Person) RETURN DISTINCT n");
      const returnClause = query.clauses[1] as ReturnClause;

      expect(returnClause.type).toBe("RETURN");
      expect(returnClause.distinct).toBe(true);
      expect(returnClause.items).toHaveLength(1);
      expect(returnClause.items[0].expression.type).toBe("variable");
    });

    it("parses RETURN DISTINCT with property", () => {
      const query = expectSuccess("MATCH (n:Person) RETURN DISTINCT n.city");
      const returnClause = query.clauses[1] as ReturnClause;

      expect(returnClause.distinct).toBe(true);
      expect(returnClause.items[0].expression.type).toBe("property");
      expect(returnClause.items[0].expression.property).toBe("city");
    });

    it("parses RETURN DISTINCT with multiple items", () => {
      const query = expectSuccess("MATCH (n:Person) RETURN DISTINCT n.city, n.country");
      const returnClause = query.clauses[1] as ReturnClause;

      expect(returnClause.distinct).toBe(true);
      expect(returnClause.items).toHaveLength(2);
    });

    it("parses RETURN DISTINCT with alias", () => {
      const query = expectSuccess("MATCH (n:Person) RETURN DISTINCT n.city AS location");
      const returnClause = query.clauses[1] as ReturnClause;

      expect(returnClause.distinct).toBe(true);
      expect(returnClause.items[0].alias).toBe("location");
    });

    it("parses RETURN DISTINCT with ORDER BY and LIMIT", () => {
      const query = expectSuccess("MATCH (n:Person) RETURN DISTINCT n.city ORDER BY n.city LIMIT 10");
      const returnClause = query.clauses[1] as ReturnClause;

      expect(returnClause.distinct).toBe(true);
      expect(returnClause.orderBy).toHaveLength(1);
      expect(returnClause.limit).toBe(10);
    });

    it("parses RETURN without DISTINCT (distinct is undefined)", () => {
      const query = expectSuccess("MATCH (n:Person) RETURN n");
      const returnClause = query.clauses[1] as ReturnClause;

      expect(returnClause.distinct).toBeUndefined();
    });

    it("parses lowercase return distinct", () => {
      const query = expectSuccess("match (n:Person) return distinct n.city");
      const returnClause = query.clauses[1] as ReturnClause;

      expect(returnClause.distinct).toBe(true);
    });
  });

  describe("Parameters", () => {
    it("parses parameter in node properties", () => {
      const query = expectSuccess("CREATE (n:Person {name: $name, age: $age})");
      const clause = query.clauses[0] as CreateClause;
      const node = clause.patterns[0] as NodePattern;

      expect(node.properties).toEqual({
        name: { type: "parameter", name: "name" },
        age: { type: "parameter", name: "age" },
      });
    });

    it("parses parameter in MATCH properties", () => {
      const query = expectSuccess("MATCH (n:Person {id: $id}) RETURN n");
      const clause = query.clauses[0] as MatchClause;
      const node = clause.patterns[0] as NodePattern;

      expect(node.properties).toEqual({
        id: { type: "parameter", name: "id" },
      });
    });
  });

  describe("Functions", () => {
    it("parses id() function", () => {
      const query = expectSuccess("MATCH (n:Person) RETURN id(n)");
      const returnClause = query.clauses[1] as ReturnClause;

      expect(returnClause.items[0].expression.type).toBe("function");
      expect(returnClause.items[0].expression.functionName).toBe("ID");
      expect(returnClause.items[0].expression.args).toHaveLength(1);
    });
  });

  describe("Error handling", () => {
    it("reports error for empty query", () => {
      const error = expectError("");
      expect(error.message).toBe("Empty query");
    });

    it("reports error for missing closing parenthesis", () => {
      const error = expectError("CREATE (n:Person");
      expect(error.message).toContain("Expected RPAREN");
    });

    it("reports error for invalid syntax", () => {
      const error = expectError("CREATE n:Person");
      expect(error.message).toContain("Expected");
    });

    it("reports error for unknown clause", () => {
      const error = expectError("INVALID (n:Person)");
      expect(error.message).toContain("Unexpected");
    });

    it("reports position in error", () => {
      const error = expectError("CREATE (n:Person");
      expect(error.position).toBeGreaterThan(0);
      expect(error.line).toBeGreaterThanOrEqual(1);
      expect(error.column).toBeGreaterThanOrEqual(1);
    });

    it("reports error for unterminated string", () => {
      const error = expectError("CREATE (n:Person {name: 'Alice})");
      expect(error.message).toContain("Unterminated string");
    });

    it("reports error for invalid relationship arrows", () => {
      const error = expectError("CREATE (a)<-[:KNOWS]->(b)");
      expect(error.message).toContain("arrows on both sides");
    });
  });

  describe("OPTIONAL MATCH", () => {
    it("parses simple OPTIONAL MATCH", () => {
      const query = expectSuccess("MATCH (n:Person) OPTIONAL MATCH (n)-[:KNOWS]->(m:Person) RETURN n, m");
      
      expect(query.clauses).toHaveLength(3);
      expect(query.clauses[0].type).toBe("MATCH");
      expect(query.clauses[1].type).toBe("OPTIONAL_MATCH");
      expect(query.clauses[2].type).toBe("RETURN");
      
      const optionalMatch = query.clauses[1] as MatchClause;
      expect(optionalMatch.patterns).toHaveLength(1);
    });

    it("parses OPTIONAL MATCH with node pattern only", () => {
      const query = expectSuccess("OPTIONAL MATCH (n:Person) RETURN n");
      
      expect(query.clauses).toHaveLength(2);
      expect(query.clauses[0].type).toBe("OPTIONAL_MATCH");
      
      const optionalMatch = query.clauses[0] as MatchClause;
      const node = optionalMatch.patterns[0] as NodePattern;
      expect(node.label).toBe("Person");
    });

    it("parses OPTIONAL MATCH with WHERE clause", () => {
      const query = expectSuccess(
        "MATCH (n:Person) OPTIONAL MATCH (n)-[:KNOWS]->(m:Person) WHERE m.age > 25 RETURN n, m"
      );
      
      expect(query.clauses).toHaveLength(3);
      const optionalMatch = query.clauses[1] as MatchClause;
      expect(optionalMatch.type).toBe("OPTIONAL_MATCH");
      expect(optionalMatch.where).toBeDefined();
      expect(optionalMatch.where!.type).toBe("comparison");
    });

    it("parses multiple OPTIONAL MATCH clauses", () => {
      const query = expectSuccess(`
        MATCH (n:Person)
        OPTIONAL MATCH (n)-[:KNOWS]->(friend:Person)
        OPTIONAL MATCH (n)-[:WORKS_AT]->(company:Company)
        RETURN n, friend, company
      `);
      
      expect(query.clauses).toHaveLength(4);
      expect(query.clauses[0].type).toBe("MATCH");
      expect(query.clauses[1].type).toBe("OPTIONAL_MATCH");
      expect(query.clauses[2].type).toBe("OPTIONAL_MATCH");
      expect(query.clauses[3].type).toBe("RETURN");
    });

    it("parses OPTIONAL MATCH with properties on relationship", () => {
      const query = expectSuccess(
        "MATCH (n:Person) OPTIONAL MATCH (n)-[r:KNOWS {since: 2020}]->(m) RETURN n, r, m"
      );
      
      const optionalMatch = query.clauses[1] as MatchClause;
      const rel = optionalMatch.patterns[0] as RelationshipPattern;
      expect(rel.edge.type).toBe("KNOWS");
      expect(rel.edge.properties).toEqual({ since: 2020 });
    });

    it("parses lowercase optional match", () => {
      const query = expectSuccess("match (n:Person) optional match (n)-[:KNOWS]->(m) return n, m");
      
      expect(query.clauses[1].type).toBe("OPTIONAL_MATCH");
    });
  });

  describe("Complex queries", () => {
    it("parses multi-clause query", () => {
      const query = expectSuccess(`
        MATCH (a:Person {name: 'Alice'})
        MATCH (b:Person {name: 'Bob'})
        CREATE (a)-[:KNOWS]->(b)
        RETURN a, b
      `);

      expect(query.clauses).toHaveLength(4);
      expect(query.clauses[0].type).toBe("MATCH");
      expect(query.clauses[1].type).toBe("MATCH");
      expect(query.clauses[2].type).toBe("CREATE");
      expect(query.clauses[3].type).toBe("RETURN");
    });

    it("parses MATCH-SET-RETURN", () => {
      const query = expectSuccess("MATCH (n:Person {id: $id}) SET n.name = $name RETURN n");

      expect(query.clauses).toHaveLength(3);
      expect(query.clauses[0].type).toBe("MATCH");
      expect(query.clauses[1].type).toBe("SET");
      expect(query.clauses[2].type).toBe("RETURN");
    });

    it("parses strings with escaped characters", () => {
      const query = expectSuccess(`CREATE (n:Person {bio: 'Line 1\\nLine 2'})`);
      const clause = query.clauses[0] as CreateClause;
      const node = clause.patterns[0] as NodePattern;

      expect(node.properties!.bio).toBe("Line 1\nLine 2");
    });

    it("parses strings with escaped quotes", () => {
      const query = expectSuccess(`CREATE (n:Person {quote: 'He said \\'hello\\''})`);
      const clause = query.clauses[0] as CreateClause;
      const node = clause.patterns[0] as NodePattern;

      expect(node.properties!.quote).toBe("He said 'hello'");
    });

    it("parses negative numbers", () => {
      const query = expectSuccess("CREATE (n:Account {balance: -100.50})");
      const clause = query.clauses[0] as CreateClause;
      const node = clause.patterns[0] as NodePattern;

      expect(node.properties!.balance).toBe(-100.5);
    });

    it("parses double-quoted strings", () => {
      const query = expectSuccess('CREATE (n:Person {name: "Alice"})');
      const clause = query.clauses[0] as CreateClause;
      const node = clause.patterns[0] as NodePattern;

      expect(node.properties!.name).toBe("Alice");
    });
  });
});
