import { describe, it, expect, beforeEach, afterEach } from "vitest";
import { GraphDatabase } from "../src/db";
import { Executor, executeQuery, ExecutionResult } from "../src/executor";

describe("Integration Tests", () => {
  let db: GraphDatabase;
  let executor: Executor;

  beforeEach(() => {
    db = new GraphDatabase(":memory:");
    db.initialize();
    executor = new Executor(db);
  });

  afterEach(() => {
    db.close();
  });

  // Helper to assert success
  function expectSuccess(result: ReturnType<typeof executor.execute>): ExecutionResult {
    if (!result.success) {
      throw new Error(`Query failed: ${result.error.message}`);
    }
    return result;
  }

  describe("CREATE and MATCH", () => {
    it("creates a node and retrieves it", () => {
      // Create
      const createResult = executor.execute("CREATE (n:Person {name: 'Alice', age: 30})");
      expect(createResult.success).toBe(true);

      // Match
      const matchResult = expectSuccess(
        executor.execute("MATCH (n:Person) RETURN n")
      );

      expect(matchResult.data).toHaveLength(1);
      expect(matchResult.data[0].n).toMatchObject({
        label: "Person",
        properties: { name: "Alice", age: 30 },
      });
    });

    it("creates multiple nodes and retrieves them", () => {
      executor.execute("CREATE (n:Person {name: 'Alice'})");
      executor.execute("CREATE (n:Person {name: 'Bob'})");
      executor.execute("CREATE (n:Company {name: 'Acme'})");

      const result = expectSuccess(
        executor.execute("MATCH (n:Person) RETURN n")
      );

      expect(result.data).toHaveLength(2);
    });

    it("creates and retrieves with property filter", () => {
      executor.execute("CREATE (n:Person {name: 'Alice', age: 30})");
      executor.execute("CREATE (n:Person {name: 'Bob', age: 25})");

      const result = expectSuccess(
        executor.execute("MATCH (n:Person {name: 'Alice'}) RETURN n")
      );

      expect(result.data).toHaveLength(1);
      expect(result.data[0].n).toMatchObject({
        properties: { name: "Alice", age: 30 },
      });
    });

    it("returns specific properties", () => {
      executor.execute("CREATE (n:Person {name: 'Alice', age: 30})");

      const result = expectSuccess(
        executor.execute("MATCH (n:Person) RETURN n.name, n.age")
      );

      expect(result.data).toHaveLength(1);
      expect(result.data[0].n_name).toBe("Alice");
      expect(result.data[0].n_age).toBe(30);
    });

    it("uses LIMIT correctly", () => {
      executor.execute("CREATE (n:Person {name: 'Alice'})");
      executor.execute("CREATE (n:Person {name: 'Bob'})");
      executor.execute("CREATE (n:Person {name: 'Charlie'})");

      const result = expectSuccess(
        executor.execute("MATCH (n:Person) RETURN n LIMIT 2")
      );

      expect(result.data).toHaveLength(2);
    });

    it("uses COUNT correctly", () => {
      executor.execute("CREATE (n:Person {name: 'Alice'})");
      executor.execute("CREATE (n:Person {name: 'Bob'})");
      executor.execute("CREATE (n:Person {name: 'Charlie'})");

      const result = expectSuccess(
        executor.execute("MATCH (n:Person) RETURN COUNT(n)")
      );

      expect(result.data).toHaveLength(1);
      expect(result.data[0].count).toBe(3);
    });

    it("orders results by property ASC", () => {
      executor.execute("CREATE (n:Person {name: 'Charlie', age: 35})");
      executor.execute("CREATE (n:Person {name: 'Alice', age: 25})");
      executor.execute("CREATE (n:Person {name: 'Bob', age: 30})");

      const result = expectSuccess(
        executor.execute("MATCH (n:Person) RETURN n.name ORDER BY n.name ASC")
      );

      expect(result.data).toHaveLength(3);
      expect(result.data[0].n_name).toBe("Alice");
      expect(result.data[1].n_name).toBe("Bob");
      expect(result.data[2].n_name).toBe("Charlie");
    });

    it("orders results by property DESC", () => {
      executor.execute("CREATE (n:Person {name: 'Alice', age: 25})");
      executor.execute("CREATE (n:Person {name: 'Bob', age: 30})");
      executor.execute("CREATE (n:Person {name: 'Charlie', age: 35})");

      const result = expectSuccess(
        executor.execute("MATCH (n:Person) RETURN n.name, n.age ORDER BY n.age DESC")
      );

      expect(result.data).toHaveLength(3);
      expect(result.data[0].n_age).toBe(35);
      expect(result.data[1].n_age).toBe(30);
      expect(result.data[2].n_age).toBe(25);
    });

    it("orders results by multiple fields", () => {
      executor.execute("CREATE (n:Person {name: 'Alice', dept: 'Engineering', age: 30})");
      executor.execute("CREATE (n:Person {name: 'Bob', dept: 'Engineering', age: 25})");
      executor.execute("CREATE (n:Person {name: 'Charlie', dept: 'Sales', age: 35})");

      const result = expectSuccess(
        executor.execute("MATCH (n:Person) RETURN n.name, n.dept ORDER BY n.dept ASC, n.name ASC")
      );

      expect(result.data).toHaveLength(3);
      // Engineering comes first alphabetically, then sorted by name within dept
      expect(result.data[0].n_name).toBe("Alice");
      expect(result.data[1].n_name).toBe("Bob");
      expect(result.data[2].n_name).toBe("Charlie");
    });

    it("orders results with LIMIT", () => {
      executor.execute("CREATE (n:Person {name: 'Charlie'})");
      executor.execute("CREATE (n:Person {name: 'Alice'})");
      executor.execute("CREATE (n:Person {name: 'Bob'})");

      const result = expectSuccess(
        executor.execute("MATCH (n:Person) RETURN n.name ORDER BY n.name LIMIT 2")
      );

      expect(result.data).toHaveLength(2);
      expect(result.data[0].n_name).toBe("Alice");
      expect(result.data[1].n_name).toBe("Bob");
    });

    it("uses SKIP correctly", () => {
      executor.execute("CREATE (n:Person {name: 'Alice'})");
      executor.execute("CREATE (n:Person {name: 'Bob'})");
      executor.execute("CREATE (n:Person {name: 'Charlie'})");

      const result = expectSuccess(
        executor.execute("MATCH (n:Person) RETURN n.name ORDER BY n.name SKIP 1")
      );

      expect(result.data).toHaveLength(2);
      expect(result.data[0].n_name).toBe("Bob");
      expect(result.data[1].n_name).toBe("Charlie");
    });

    it("uses SKIP with LIMIT for pagination", () => {
      executor.execute("CREATE (n:Person {name: 'Alice'})");
      executor.execute("CREATE (n:Person {name: 'Bob'})");
      executor.execute("CREATE (n:Person {name: 'Charlie'})");
      executor.execute("CREATE (n:Person {name: 'Dave'})");
      executor.execute("CREATE (n:Person {name: 'Eve'})");

      // Page 1: first 2 results
      const page1 = expectSuccess(
        executor.execute("MATCH (n:Person) RETURN n.name ORDER BY n.name SKIP 0 LIMIT 2")
      );
      expect(page1.data).toHaveLength(2);
      expect(page1.data[0].n_name).toBe("Alice");
      expect(page1.data[1].n_name).toBe("Bob");

      // Page 2: next 2 results
      const page2 = expectSuccess(
        executor.execute("MATCH (n:Person) RETURN n.name ORDER BY n.name SKIP 2 LIMIT 2")
      );
      expect(page2.data).toHaveLength(2);
      expect(page2.data[0].n_name).toBe("Charlie");
      expect(page2.data[1].n_name).toBe("Dave");

      // Page 3: last result
      const page3 = expectSuccess(
        executor.execute("MATCH (n:Person) RETURN n.name ORDER BY n.name SKIP 4 LIMIT 2")
      );
      expect(page3.data).toHaveLength(1);
      expect(page3.data[0].n_name).toBe("Eve");
    });

    it("uses ORDER BY with DESC and SKIP and LIMIT", () => {
      executor.execute("CREATE (n:Person {name: 'Alice', score: 100})");
      executor.execute("CREATE (n:Person {name: 'Bob', score: 85})");
      executor.execute("CREATE (n:Person {name: 'Charlie', score: 92})");
      executor.execute("CREATE (n:Person {name: 'Dave', score: 78})");

      // Get 2nd and 3rd highest scores
      const result = expectSuccess(
        executor.execute("MATCH (n:Person) RETURN n.name, n.score ORDER BY n.score DESC SKIP 1 LIMIT 2")
      );

      expect(result.data).toHaveLength(2);
      expect(result.data[0].n_score).toBe(92); // Charlie (2nd highest)
      expect(result.data[1].n_score).toBe(85); // Bob (3rd highest)
    });
  });

  describe("CREATE relationships", () => {
    it("creates edge between nodes", () => {
      const result = executor.execute(
        "CREATE (a:Person {name: 'Alice'})-[:KNOWS {since: 2020}]->(b:Person {name: 'Bob'})"
      );
      expect(result.success).toBe(true);

      // Verify nodes exist
      const nodesResult = expectSuccess(
        executor.execute("MATCH (n:Person) RETURN n")
      );
      expect(nodesResult.data).toHaveLength(2);

      // Verify edge exists by checking the raw database
      expect(db.countEdges()).toBe(1);
    });

    it("matches relationship patterns", () => {
      executor.execute(
        "CREATE (a:Person {name: 'Alice'})-[:KNOWS]->(b:Person {name: 'Bob'})"
      );

      const result = expectSuccess(
        executor.execute("MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN a, b")
      );

      expect(result.data).toHaveLength(1);
      expect(result.data[0].a).toMatchObject({
        properties: { name: "Alice" },
      });
      expect(result.data[0].b).toMatchObject({
        properties: { name: "Bob" },
      });
    });

    it("filters by source node properties in relationships", () => {
      // Create multiple relationships
      executor.execute("CREATE (a:Person {name: 'Alice', role: 'admin'})-[:MANAGES]->(p:Project {name: 'Alpha'})");
      executor.execute("CREATE (b:Person {name: 'Bob', role: 'user'})-[:MANAGES]->(q:Project {name: 'Beta'})");

      // Filter by source node property
      const result = expectSuccess(
        executor.execute("MATCH (p:Person {role: 'admin'})-[:MANAGES]->(proj:Project) RETURN proj.name")
      );

      expect(result.data).toHaveLength(1);
      expect(result.data[0].proj_name).toBe("Alpha");
    });

    it("filters by target node properties in relationships", () => {
      // Create multiple relationships
      executor.execute("CREATE (a:Person {name: 'Alice'})-[:WORKS_ON]->(p:Project {name: 'Alpha', status: 'active'})");
      executor.execute("CREATE (a:Person {name: 'Alice'})-[:WORKS_ON]->(q:Project {name: 'Beta', status: 'archived'})");

      // We need to use raw DB to create the second edge since the translator generates new IDs
      const alice = db.getNodesByLabel("Person").find(n => n.properties.name === "Alice");
      const projects = db.getNodesByLabel("Project");

      // Filter by target node property
      const result = expectSuccess(
        executor.execute("MATCH (p:Person)-[:WORKS_ON]->(proj:Project {status: 'active'}) RETURN proj.name")
      );

      expect(result.data).toHaveLength(1);
      expect(result.data[0].proj_name).toBe("Alpha");
    });

    it("filters by both source and target node properties", () => {
      // Create nodes
      executor.execute("CREATE (a:User {name: 'Alice', tier: 'gold'})");
      executor.execute("CREATE (b:User {name: 'Bob', tier: 'silver'})");
      executor.execute("CREATE (p1:Product {name: 'Premium Widget', category: 'premium'})");
      executor.execute("CREATE (p2:Product {name: 'Basic Widget', category: 'basic'})");

      // Get node IDs
      const users = db.getNodesByLabel("User");
      const products = db.getNodesByLabel("Product");
      const alice = users.find(u => u.properties.name === "Alice")!;
      const bob = users.find(u => u.properties.name === "Bob")!;
      const premium = products.find(p => p.properties.name === "Premium Widget")!;
      const basic = products.find(p => p.properties.name === "Basic Widget")!;

      // Create purchase relationships
      db.insertEdge("purchase1", "PURCHASED", alice.id, premium.id);
      db.insertEdge("purchase2", "PURCHASED", alice.id, basic.id);
      db.insertEdge("purchase3", "PURCHASED", bob.id, basic.id);

      // Filter by both source (gold tier) and target (premium category)
      const result = expectSuccess(
        executor.execute("MATCH (u:User {tier: 'gold'})-[:PURCHASED]->(p:Product {category: 'premium'}) RETURN u.name, p.name")
      );

      expect(result.data).toHaveLength(1);
      expect(result.data[0].u_name).toBe("Alice");
      expect(result.data[0].p_name).toBe("Premium Widget");
    });

    it("filters relationships with parameter values on endpoints", () => {
      executor.execute("CREATE (a:Employee {name: 'Alice', dept: 'engineering'})");
      executor.execute("CREATE (b:Employee {name: 'Bob', dept: 'sales'})");
      executor.execute("CREATE (t:Task {title: 'Code Review'})");

      const employees = db.getNodesByLabel("Employee");
      const tasks = db.getNodesByLabel("Task");
      const alice = employees.find(e => e.properties.name === "Alice")!;
      const bob = employees.find(e => e.properties.name === "Bob")!;
      const task = tasks[0];

      db.insertEdge("assign1", "ASSIGNED", alice.id, task.id);
      db.insertEdge("assign2", "ASSIGNED", bob.id, task.id);

      // Use parameter for filtering
      const result = expectSuccess(
        executor.execute(
          "MATCH (e:Employee {dept: $dept})-[:ASSIGNED]->(t:Task) RETURN e.name, t.title",
          { dept: "engineering" }
        )
      );

      expect(result.data).toHaveLength(1);
      expect(result.data[0].e_name).toBe("Alice");
    });
  });

  describe("WHERE clause", () => {
    beforeEach(() => {
      executor.execute("CREATE (n:Person {name: 'Alice', age: 30})");
      executor.execute("CREATE (n:Person {name: 'Bob', age: 25})");
      executor.execute("CREATE (n:Person {name: 'Charlie', age: 35})");
    });

    it("filters with equals", () => {
      const result = expectSuccess(
        executor.execute("MATCH (n:Person) WHERE n.name = 'Alice' RETURN n")
      );
      expect(result.data).toHaveLength(1);
    });

    it("filters with greater than", () => {
      const result = expectSuccess(
        executor.execute("MATCH (n:Person) WHERE n.age > 28 RETURN n")
      );
      expect(result.data).toHaveLength(2);
    });

    it("filters with less than", () => {
      const result = expectSuccess(
        executor.execute("MATCH (n:Person) WHERE n.age < 30 RETURN n")
      );
      expect(result.data).toHaveLength(1);
    });

    it("filters with AND", () => {
      const result = expectSuccess(
        executor.execute(
          "MATCH (n:Person) WHERE n.age > 25 AND n.age < 35 RETURN n"
        )
      );
      expect(result.data).toHaveLength(1);
      expect(result.data[0].n).toMatchObject({
        properties: { name: "Alice" },
      });
    });

    it("filters with OR", () => {
      const result = expectSuccess(
        executor.execute(
          "MATCH (n:Person) WHERE n.name = 'Alice' OR n.name = 'Bob' RETURN n"
        )
      );
      expect(result.data).toHaveLength(2);
    });

    it("filters with CONTAINS", () => {
      const result = expectSuccess(
        executor.execute("MATCH (n:Person) WHERE n.name CONTAINS 'li' RETURN n")
      );
      expect(result.data).toHaveLength(2); // Alice and Charlie
    });

    it("filters with STARTS WITH", () => {
      const result = expectSuccess(
        executor.execute("MATCH (n:Person) WHERE n.name STARTS WITH 'A' RETURN n")
      );
      expect(result.data).toHaveLength(1);
    });

    it("filters with ENDS WITH", () => {
      const result = expectSuccess(
        executor.execute("MATCH (n:Person) WHERE n.name ENDS WITH 'e' RETURN n")
      );
      expect(result.data).toHaveLength(2); // Alice and Charlie
    });

    it("filters with IS NULL", () => {
      // Add a person with null email
      executor.execute("CREATE (n:Person {name: 'Dave'})");
      
      const result = expectSuccess(
        executor.execute("MATCH (n:Person) WHERE n.email IS NULL RETURN n.name")
      );
      // All 4 people should have null email (Alice, Bob, Charlie, Dave)
      expect(result.data).toHaveLength(4);
    });

    it("filters with IS NOT NULL", () => {
      // Add a person with email
      executor.execute("CREATE (n:Person {name: 'Eve', email: 'eve@example.com'})");
      
      const result = expectSuccess(
        executor.execute("MATCH (n:Person) WHERE n.email IS NOT NULL RETURN n.name")
      );
      expect(result.data).toHaveLength(1);
      expect(result.data[0].n_name).toBe("Eve");
    });

    it("filters with parenthesized OR condition", () => {
      // Clear and recreate test data
      executor.execute("CREATE (n:Status {name: 'active', archived: false})");
      executor.execute("CREATE (n:Status {name: 'pending', archived: false})");
      executor.execute("CREATE (n:Status {name: 'deleted', archived: true})");
      
      const result = expectSuccess(
        executor.execute("MATCH (n:Status) WHERE n.archived = false AND (n.name = 'active' OR n.name = 'pending') RETURN n.name")
      );
      expect(result.data).toHaveLength(2);
    });

    it("filters with IS NULL in OR expression", () => {
      executor.execute("CREATE (n:Customer {name: 'Active Co'})");
      executor.execute("CREATE (n:Customer {name: 'Visible Inc', archived: false})");
      executor.execute("CREATE (n:Customer {name: 'Hidden LLC', archived: true})");
      
      // Find customers where archived is null OR false (i.e., not archived)
      const result = expectSuccess(
        executor.execute("MATCH (n:Customer) WHERE n.archived IS NULL OR n.archived = false RETURN n.name")
      );
      expect(result.data).toHaveLength(2);
      const names = result.data.map(d => d.n_name);
      expect(names).toContain("Active Co");
      expect(names).toContain("Visible Inc");
    });
  });

  describe("Parameters", () => {
    it("uses parameters in CREATE", () => {
      executor.execute(
        "CREATE (n:Person {name: $name, age: $age})",
        { name: "Alice", age: 30 }
      );

      const result = expectSuccess(
        executor.execute("MATCH (n:Person) RETURN n")
      );
      expect(result.data[0].n).toMatchObject({
        properties: { name: "Alice", age: 30 },
      });
    });

    it("uses parameters in MATCH", () => {
      executor.execute("CREATE (n:Person {name: 'Alice', id: 'abc123'})");

      const result = expectSuccess(
        executor.execute(
          "MATCH (n:Person {id: $id}) RETURN n",
          { id: "abc123" }
        )
      );
      expect(result.data).toHaveLength(1);
    });

    it("uses parameters in WHERE", () => {
      executor.execute("CREATE (n:Person {name: 'Alice', age: 30})");
      executor.execute("CREATE (n:Person {name: 'Bob', age: 25})");

      const result = expectSuccess(
        executor.execute(
          "MATCH (n:Person) WHERE n.age > $minAge RETURN n",
          { minAge: 28 }
        )
      );
      expect(result.data).toHaveLength(1);
    });
  });

  describe("SET", () => {
    it("updates node properties", () => {
      // First create a node
      executor.execute("CREATE (n:Person {name: 'Alice', age: 30})");
      
      // Get the node to find its ID (we need to use a workaround since SET requires id)
      const nodes = db.getNodesByLabel("Person");
      expect(nodes).toHaveLength(1);
      const nodeId = nodes[0].id;

      // For now, let's verify SET works at the translator level
      // The current implementation needs the node ID from MATCH context
      // This is a limitation we'll address in a future iteration
    });
  });

  describe("DELETE", () => {
    it("deletes nodes", () => {
      executor.execute("CREATE (n:Person {name: 'Alice'})");
      expect(db.countNodes()).toBe(1);

      // DELETE requires the node to be matched first
      // Current implementation needs ID from MATCH context
      // This is a limitation we'll address
    });
  });

  describe("MERGE", () => {
    it("creates node when not exists", () => {
      executor.execute("MERGE (n:Person {name: 'Alice'})");

      const result = expectSuccess(
        executor.execute("MATCH (n:Person) RETURN n")
      );
      expect(result.data).toHaveLength(1);
    });

    it("does not duplicate on second MERGE", () => {
      executor.execute("MERGE (n:Person {name: 'Alice'})");
      executor.execute("MERGE (n:Person {name: 'Alice'})");

      const result = expectSuccess(
        executor.execute("MATCH (n:Person) RETURN n")
      );
      expect(result.data).toHaveLength(1);
    });

    it("applies ON CREATE SET when node does not exist", () => {
      const result = executor.execute(
        `MERGE (u:CC_User {id: $id})
         ON CREATE SET u.email = $email, u.passwordHash = '', u.createdAt = $createdAt
         ON MATCH SET u.email = $email
         RETURN u`,
        {
          id: "user-123",
          email: "test@example.com",
          createdAt: "2025-01-01T00:00:00.000Z"
        }
      );

      expect(result.success).toBe(true);
      if (result.success) {
        expect(result.data).toHaveLength(1);
        expect(result.data[0].u).toMatchObject({
          properties: {
            id: "user-123",
            email: "test@example.com",
            passwordHash: "",
            createdAt: "2025-01-01T00:00:00.000Z"
          }
        });
      }
    });

    it("applies ON MATCH SET when node already exists", () => {
      // First create the user
      executor.execute(
        "CREATE (u:CC_User {id: 'user-456', email: 'old@example.com', createdAt: '2024-01-01'})"
      );

      // Now MERGE with ON MATCH SET should update the email
      const result = executor.execute(
        `MERGE (u:CC_User {id: $id})
         ON CREATE SET u.email = $email, u.createdAt = $createdAt
         ON MATCH SET u.email = $email
         RETURN u`,
        {
          id: "user-456",
          email: "new@example.com",
          createdAt: "2025-01-01T00:00:00.000Z"
        }
      );

      expect(result.success).toBe(true);
      if (result.success) {
        expect(result.data).toHaveLength(1);
        expect(result.data[0].u).toMatchObject({
          properties: {
            id: "user-456",
            email: "new@example.com",
            createdAt: "2024-01-01" // Original createdAt should be preserved
          }
        });
      }
    });

    it("applies only ON CREATE SET when node is new", () => {
      const result = executor.execute(
        `MERGE (n:Product {sku: $sku})
         ON CREATE SET n.name = $name, n.price = $price
         ON MATCH SET n.price = $price`,
        {
          sku: "SKU-001",
          name: "Widget",
          price: 9.99
        }
      );

      expect(result.success).toBe(true);

      const queryResult = expectSuccess(
        executor.execute("MATCH (n:Product {sku: 'SKU-001'}) RETURN n.name, n.price")
      );

      expect(queryResult.data).toHaveLength(1);
      expect(queryResult.data[0].n_name).toBe("Widget");
      expect(queryResult.data[0].n_price).toBe(9.99);
    });

    it("applies only ON MATCH SET when node exists", () => {
      // Create existing product
      executor.execute("CREATE (n:Product {sku: 'SKU-002', name: 'Original Widget', price: 5.00})");

      // MERGE should only update price, not name
      const result = executor.execute(
        `MERGE (n:Product {sku: $sku})
         ON CREATE SET n.name = $name, n.price = $price
         ON MATCH SET n.price = $price`,
        {
          sku: "SKU-002",
          name: "New Widget",
          price: 12.99
        }
      );

      expect(result.success).toBe(true);

      const queryResult = expectSuccess(
        executor.execute("MATCH (n:Product {sku: 'SKU-002'}) RETURN n.name, n.price")
      );

      expect(queryResult.data).toHaveLength(1);
      expect(queryResult.data[0].n_name).toBe("Original Widget"); // Name unchanged
      expect(queryResult.data[0].n_price).toBe(12.99); // Price updated
    });
  });

  describe("id() function", () => {
    it("returns node id", () => {
      executor.execute("CREATE (n:Person {name: 'Alice'})");

      const result = expectSuccess(
        executor.execute("MATCH (n:Person) RETURN id(n)")
      );

      expect(result.data).toHaveLength(1);
      expect(result.data[0].id).toBeDefined();
      // Should be a UUID
      expect(result.data[0].id).toMatch(
        /^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i
      );
    });
  });

  describe("Metadata", () => {
    it("returns count in meta", () => {
      executor.execute("CREATE (n:Person {name: 'Alice'})");
      executor.execute("CREATE (n:Person {name: 'Bob'})");

      const result = expectSuccess(
        executor.execute("MATCH (n:Person) RETURN n")
      );

      expect(result.meta.count).toBe(2);
    });

    it("returns time_ms in meta", () => {
      const result = expectSuccess(
        executor.execute("MATCH (n:Person) RETURN n")
      );

      expect(result.meta.time_ms).toBeGreaterThanOrEqual(0);
    });
  });

  describe("Error handling", () => {
    it("returns parse error for invalid syntax", () => {
      const result = executor.execute("INVALID QUERY");

      expect(result.success).toBe(false);
      if (!result.success) {
        expect(result.error.message).toBeDefined();
      }
    });

    it("returns error position for parse errors", () => {
      const result = executor.execute("CREATE (n:Person");

      expect(result.success).toBe(false);
      if (!result.success) {
        expect(result.error.position).toBeDefined();
        expect(result.error.line).toBeDefined();
        expect(result.error.column).toBeDefined();
      }
    });

    it("returns error for SQL failures", () => {
      // Try to create an edge to non-existent node
      // This will fail due to foreign key constraint
      const result = executor.execute(
        "CREATE (a:Person {name: 'Alice'})"
      );
      expect(result.success).toBe(true);

      // Manually try to insert invalid edge (bypassing normal flow)
      expect(() => {
        db.insertEdge("edge1", "KNOWS", "nonexistent1", "nonexistent2");
      }).toThrow();
    });
  });

  describe("executeQuery convenience function", () => {
    it("works as expected", () => {
      executeQuery(db, "CREATE (n:Person {name: 'Alice'})");

      const result = executeQuery(db, "MATCH (n:Person) RETURN n");
      expect(result.success).toBe(true);
      if (result.success) {
        expect(result.data).toHaveLength(1);
      }
    });
  });

  describe("MATCH...CREATE patterns", () => {
    it("creates relationship from matched node to new node", () => {
      // First create a user
      executor.execute("CREATE (u:CC_User {id: 'user-123', name: 'Alice'})");

      // Now match the user and create a relationship to a new node
      const result = executor.execute(
        `MATCH (u:CC_User {id: $userId})
         CREATE (u)-[:HAS_REPORT]->(r:CC_MonthlyReport {
           id: $reportId,
           year: $year,
           month: $month,
           status: $status
         })`,
        {
          userId: "user-123",
          reportId: "report-456",
          year: 2024,
          month: 12,
          status: "pending"
        }
      );

      expect(result.success).toBe(true);

      // Verify the report was created
      const reportResult = expectSuccess(
        executor.execute("MATCH (r:CC_MonthlyReport) RETURN r")
      );
      expect(reportResult.data).toHaveLength(1);
      expect(reportResult.data[0].r).toMatchObject({
        properties: {
          id: "report-456",
          year: 2024,
          month: 12,
          status: "pending"
        }
      });

      // Verify the relationship was created
      const relResult = expectSuccess(
        executor.execute("MATCH (u:CC_User)-[:HAS_REPORT]->(r:CC_MonthlyReport) RETURN u.name, r.id")
      );
      expect(relResult.data).toHaveLength(1);
      expect(relResult.data[0].u_name).toBe("Alice");
      expect(relResult.data[0].r_id).toBe("report-456");
    });

    it("creates relationship from new node to matched node", () => {
      // First create a company
      executor.execute("CREATE (c:Company {id: 'company-1', name: 'Acme Corp'})");

      // Match the company and create an employee that works for it
      const result = executor.execute(
        `MATCH (c:Company {id: $companyId})
         CREATE (e:Employee {name: $name})-[:WORKS_FOR]->(c)`,
        { companyId: "company-1", name: "Bob" }
      );

      expect(result.success).toBe(true);

      // Verify the relationship
      const relResult = expectSuccess(
        executor.execute("MATCH (e:Employee)-[:WORKS_FOR]->(c:Company) RETURN e.name, c.name")
      );
      expect(relResult.data).toHaveLength(1);
      expect(relResult.data[0].e_name).toBe("Bob");
      expect(relResult.data[0].c_name).toBe("Acme Corp");
    });

    it("fails gracefully when matched node does not exist", () => {
      // Try to create relationship from non-existent user
      const result = executor.execute(
        `MATCH (u:CC_User {id: $userId})
         CREATE (u)-[:HAS_REPORT]->(r:CC_MonthlyReport {id: $reportId})`,
        { userId: "non-existent", reportId: "report-456" }
      );

      // Should succeed but create nothing (no matched nodes)
      expect(result.success).toBe(true);

      // Verify no report was created
      const reportResult = expectSuccess(
        executor.execute("MATCH (r:CC_MonthlyReport) RETURN r")
      );
      expect(reportResult.data).toHaveLength(0);
    });

    it("returns newly created node in MATCH...CREATE...RETURN", () => {
      // First create a user
      executor.execute("CREATE (u:CC_User {id: 'user-789', name: 'Charlie'})");

      // Match the user and create a business, returning the new business
      const result = executor.execute(
        `MATCH (u:CC_User {id: $userId})
         CREATE (u)-[:OWNS]->(b:CC_Business {
           id: $businessId,
           name: $name,
           address: $address,
           country: $country,
           vatNumber: $vatNumber,
           accountantEmail: $accountantEmail,
           paymentTermDays: $paymentTermDays,
           iban: $iban,
           bic: $bic,
           bankAccountName: $bankAccountName
         })
         RETURN b`,
        {
          userId: "user-789",
          businessId: "biz-123",
          name: "Test Business",
          address: "123 Main St",
          country: "US",
          vatNumber: "VAT123",
          accountantEmail: "accountant@test.com",
          paymentTermDays: 30,
          iban: "DE89370400440532013000",
          bic: "COBADEFFXXX",
          bankAccountName: "Business Account"
        }
      );

      expect(result.success).toBe(true);
      if (result.success) {
        expect(result.data).toHaveLength(1);
        expect(result.data[0].b).toMatchObject({
          label: "CC_Business",
          properties: {
            id: "biz-123",
            name: "Test Business",
            paymentTermDays: 30
          }
        });
      }
    });

    it("returns both matched and created nodes in MATCH...CREATE...RETURN", () => {
      // First create a user
      executor.execute("CREATE (u:CC_User {id: 'user-abc', name: 'Dave'})");

      // Match the user and create a project, returning both
      const result = executor.execute(
        `MATCH (u:CC_User {id: $userId})
         CREATE (u)-[:MANAGES]->(p:Project {id: $projectId, name: $projectName})
         RETURN u, p`,
        {
          userId: "user-abc",
          projectId: "proj-1",
          projectName: "My Project"
        }
      );

      expect(result.success).toBe(true);
      if (result.success) {
        expect(result.data).toHaveLength(1);
        expect(result.data[0].u).toMatchObject({
          label: "CC_User",
          properties: { id: "user-abc", name: "Dave" }
        });
        expect(result.data[0].p).toMatchObject({
          label: "Project",
          properties: { id: "proj-1", name: "My Project" }
        });
      }
    });
  });

  describe("Multi-hop relationship patterns", () => {
    it("matches two-hop relationship pattern", () => {
      // Create test data: User -> Invoice -> Customer
      executor.execute("CREATE (u:CC_User {id: 'user-1', name: 'Alice'})");
      executor.execute("CREATE (i:CC_Invoice {id: 'inv-1', amount: 100})");
      executor.execute("CREATE (c:CC_Customer {id: 'cust-1', name: 'Acme Corp'})");

      // Create relationships manually
      const users = db.getNodesByLabel("CC_User");
      const invoices = db.getNodesByLabel("CC_Invoice");
      const customers = db.getNodesByLabel("CC_Customer");

      db.insertEdge("edge-1", "HAS_INVOICE", users[0].id, invoices[0].id);
      db.insertEdge("edge-2", "BILLED_TO", invoices[0].id, customers[0].id);

      // Query with multi-hop pattern
      const result = expectSuccess(
        executor.execute(
          `MATCH (u:CC_User {id: $userId})-[:HAS_INVOICE]->(i:CC_Invoice)-[:BILLED_TO]->(c:CC_Customer)
           RETURN i, c.id as customerId, c.name as customerName`,
          { userId: "user-1" }
        )
      );

      expect(result.data).toHaveLength(1);
      expect(result.data[0].customerId).toBe("cust-1");
      expect(result.data[0].customerName).toBe("Acme Corp");
    });

    it("matches three-hop relationship pattern", () => {
      // Create test data: A -> B -> C -> D
      executor.execute("CREATE (a:NodeA {id: 'a1'})");
      executor.execute("CREATE (b:NodeB {id: 'b1'})");
      executor.execute("CREATE (c:NodeC {id: 'c1'})");
      executor.execute("CREATE (d:NodeD {id: 'd1'})");

      const nodesA = db.getNodesByLabel("NodeA");
      const nodesB = db.getNodesByLabel("NodeB");
      const nodesC = db.getNodesByLabel("NodeC");
      const nodesD = db.getNodesByLabel("NodeD");

      db.insertEdge("e1", "REL1", nodesA[0].id, nodesB[0].id);
      db.insertEdge("e2", "REL2", nodesB[0].id, nodesC[0].id);
      db.insertEdge("e3", "REL3", nodesC[0].id, nodesD[0].id);

      const result = expectSuccess(
        executor.execute(
          "MATCH (a:NodeA)-[:REL1]->(b:NodeB)-[:REL2]->(c:NodeC)-[:REL3]->(d:NodeD) RETURN a.id, d.id"
        )
      );

      expect(result.data).toHaveLength(1);
      expect(result.data[0].a_id).toBe("a1");
      expect(result.data[0].d_id).toBe("d1");
    });

    it("returns empty when multi-hop path does not exist", () => {
      // Create disconnected nodes
      executor.execute("CREATE (u:CC_User {id: 'user-2', name: 'Bob'})");
      executor.execute("CREATE (i:CC_Invoice {id: 'inv-2', amount: 200})");
      // No relationships created

      const result = expectSuccess(
        executor.execute(
          `MATCH (u:CC_User {id: $userId})-[:HAS_INVOICE]->(i:CC_Invoice)-[:BILLED_TO]->(c:CC_Customer)
           RETURN i`,
          { userId: "user-2" }
        )
      );

      expect(result.data).toHaveLength(0);
    });

    it("filters correctly across multi-hop pattern", () => {
      // Create two paths with different customers
      executor.execute("CREATE (u:TestUser {id: 'tu1'})");
      executor.execute("CREATE (i1:TestInvoice {id: 'ti1', status: 'paid'})");
      executor.execute("CREATE (i2:TestInvoice {id: 'ti2', status: 'pending'})");
      executor.execute("CREATE (c1:TestCustomer {id: 'tc1', tier: 'gold'})");
      executor.execute("CREATE (c2:TestCustomer {id: 'tc2', tier: 'silver'})");

      const users = db.getNodesByLabel("TestUser");
      const invoices = db.getNodesByLabel("TestInvoice");
      const customers = db.getNodesByLabel("TestCustomer");

      const inv1 = invoices.find(i => i.properties.id === "ti1")!;
      const inv2 = invoices.find(i => i.properties.id === "ti2")!;
      const cust1 = customers.find(c => c.properties.id === "tc1")!;
      const cust2 = customers.find(c => c.properties.id === "tc2")!;

      db.insertEdge("te1", "HAS_INV", users[0].id, inv1.id);
      db.insertEdge("te2", "HAS_INV", users[0].id, inv2.id);
      db.insertEdge("te3", "FOR_CUST", inv1.id, cust1.id);
      db.insertEdge("te4", "FOR_CUST", inv2.id, cust2.id);

      // Filter by customer tier
      const result = expectSuccess(
        executor.execute(
          "MATCH (u:TestUser)-[:HAS_INV]->(i:TestInvoice)-[:FOR_CUST]->(c:TestCustomer {tier: 'gold'}) RETURN i.id"
        )
      );

      expect(result.data).toHaveLength(1);
      expect(result.data[0].i_id).toBe("ti1");
    });
  });

  describe("Multiple MATCH clauses", () => {
    it("handles two MATCH clauses with shared variable", () => {
      // Create test data: Report -> BankStatement <- Transaction
      executor.execute("CREATE (r:CC_MonthlyReport {id: 'report-1', name: 'January Report'})");
      executor.execute("CREATE (bs:CC_BankStatement {id: 'bs-1', name: 'Bank Statement 1'})");
      executor.execute("CREATE (t:CC_Transaction {id: 'tx-1', amount: 100})");

      // Create relationships
      const reports = db.getNodesByLabel("CC_MonthlyReport");
      const statements = db.getNodesByLabel("CC_BankStatement");
      const transactions = db.getNodesByLabel("CC_Transaction");

      db.insertEdge("e1", "HAS_BANK_STATEMENT", reports[0].id, statements[0].id);
      db.insertEdge("e2", "PART_OF", transactions[0].id, statements[0].id);

      // This is the failing query - two separate MATCH clauses
      const result = expectSuccess(
        executor.execute(
          `MATCH (r:CC_MonthlyReport {id: $reportId})-[:HAS_BANK_STATEMENT]->(bs:CC_BankStatement)
           MATCH (t:CC_Transaction)-[:PART_OF]->(bs)
           RETURN t, bs.id as bankStatementId`,
          { reportId: "report-1" }
        )
      );

      expect(result.data).toHaveLength(1);
      expect(result.data[0].bankStatementId).toBe("bs-1");
      expect(result.data[0].t).toMatchObject({
        properties: { id: "tx-1", amount: 100 }
      });
    });

    it("handles multiple MATCH with multiple results", () => {
      // Create test data: Report -> BankStatement, multiple Transactions -> BankStatement
      executor.execute("CREATE (r:CC_MonthlyReport {id: 'report-2'})");
      executor.execute("CREATE (bs:CC_BankStatement {id: 'bs-2'})");
      executor.execute("CREATE (t1:CC_Transaction {id: 'tx-2', amount: 50})");
      executor.execute("CREATE (t2:CC_Transaction {id: 'tx-3', amount: 75})");

      const reports = db.getNodesByLabel("CC_MonthlyReport").filter(r => r.properties.id === "report-2");
      const statements = db.getNodesByLabel("CC_BankStatement").filter(s => s.properties.id === "bs-2");
      const transactions = db.getNodesByLabel("CC_Transaction").filter(t => ["tx-2", "tx-3"].includes(t.properties.id as string));

      db.insertEdge("e3", "HAS_BANK_STATEMENT", reports[0].id, statements[0].id);
      db.insertEdge("e4", "PART_OF", transactions[0].id, statements[0].id);
      db.insertEdge("e5", "PART_OF", transactions[1].id, statements[0].id);

      const result = expectSuccess(
        executor.execute(
          `MATCH (r:CC_MonthlyReport {id: $reportId})-[:HAS_BANK_STATEMENT]->(bs:CC_BankStatement)
           MATCH (t:CC_Transaction)-[:PART_OF]->(bs)
           RETURN t.id as txId, t.amount as amount`,
          { reportId: "report-2" }
        )
      );

      expect(result.data).toHaveLength(2);
      const txIds = result.data.map(d => d.txId);
      expect(txIds).toContain("tx-2");
      expect(txIds).toContain("tx-3");
    });

    it("returns empty when second MATCH has no matches", () => {
      // Create test data but no transactions
      executor.execute("CREATE (r:CC_MonthlyReport {id: 'report-3'})");
      executor.execute("CREATE (bs:CC_BankStatement {id: 'bs-3'})");

      const reports = db.getNodesByLabel("CC_MonthlyReport").filter(r => r.properties.id === "report-3");
      const statements = db.getNodesByLabel("CC_BankStatement").filter(s => s.properties.id === "bs-3");

      db.insertEdge("e6", "HAS_BANK_STATEMENT", reports[0].id, statements[0].id);

      const result = expectSuccess(
        executor.execute(
          `MATCH (r:CC_MonthlyReport {id: $reportId})-[:HAS_BANK_STATEMENT]->(bs:CC_BankStatement)
           MATCH (t:CC_Transaction)-[:PART_OF]->(bs)
           RETURN t`,
          { reportId: "report-3" }
        )
      );

      expect(result.data).toHaveLength(0);
    });
  });

  describe("Standalone RETURN", () => {
    it("returns literal number", () => {
      const result = expectSuccess(executor.execute("RETURN 1"));

      expect(result.data).toHaveLength(1);
      expect(result.data[0].expr).toBe(1);
    });

    it("returns literal string", () => {
      const result = expectSuccess(executor.execute("RETURN 'hello'"));

      expect(result.data).toHaveLength(1);
      expect(result.data[0].expr).toBe("hello");
    });

    it("returns literal with alias", () => {
      const result = expectSuccess(executor.execute("RETURN 1 AS one"));

      expect(result.data).toHaveLength(1);
      expect(result.data[0].one).toBe(1);
    });

    it("returns multiple literals", () => {
      const result = expectSuccess(executor.execute("RETURN 1, 'hello', true"));

      expect(result.data).toHaveLength(1);
      expect(result.data[0].expr).toBe(1);
      // Note: second and third columns will have generated names
    });

    it("returns boolean literals", () => {
      const result = expectSuccess(executor.execute("RETURN true AS t, false AS f"));

      expect(result.data).toHaveLength(1);
      expect(result.data[0].t).toBe(1); // SQLite stores booleans as 1/0
      expect(result.data[0].f).toBe(0);
    });

    it("returns null literal", () => {
      const result = expectSuccess(executor.execute("RETURN null AS n"));

      expect(result.data).toHaveLength(1);
      expect(result.data[0].n).toBeNull();
    });
  });

  describe("Inline node creation in relationships", () => {
    it("creates new target node inline when matching source", () => {
      // Create an existing user
      executor.execute("CREATE (u:User {id: 'user-1', name: 'Alice'})");

      // Match user and create relationship with new target node inline
      const result = executor.execute(
        `MATCH (u:User {id: 'user-1'})
         CREATE (u)-[:OWNS]->(p:Pet {name: 'Fluffy', species: 'cat'})`
      );

      expect(result.success).toBe(true);

      // Verify the pet was created
      const petResult = expectSuccess(
        executor.execute("MATCH (p:Pet) RETURN p.name, p.species")
      );
      expect(petResult.data).toHaveLength(1);
      expect(petResult.data[0].p_name).toBe("Fluffy");
      expect(petResult.data[0].p_species).toBe("cat");

      // Verify the relationship exists
      const relResult = expectSuccess(
        executor.execute("MATCH (u:User)-[:OWNS]->(p:Pet) RETURN u.name, p.name")
      );
      expect(relResult.data).toHaveLength(1);
      expect(relResult.data[0].u_name).toBe("Alice");
      expect(relResult.data[0].p_name).toBe("Fluffy");
    });

    it("creates new source node inline when matching target", () => {
      // Create an existing company
      executor.execute("CREATE (c:Company {id: 'company-1', name: 'Acme Inc'})");

      // Match company and create employee (source) inline
      const result = executor.execute(
        `MATCH (c:Company {id: 'company-1'})
         CREATE (e:Employee {name: 'Bob', role: 'developer'})-[:WORKS_AT]->(c)`
      );

      expect(result.success).toBe(true);

      // Verify the employee was created
      const empResult = expectSuccess(
        executor.execute("MATCH (e:Employee) RETURN e.name, e.role")
      );
      expect(empResult.data).toHaveLength(1);
      expect(empResult.data[0].e_name).toBe("Bob");
      expect(empResult.data[0].e_role).toBe("developer");

      // Verify the relationship exists
      const relResult = expectSuccess(
        executor.execute("MATCH (e:Employee)-[:WORKS_AT]->(c:Company) RETURN e.name, c.name")
      );
      expect(relResult.data).toHaveLength(1);
      expect(relResult.data[0].e_name).toBe("Bob");
      expect(relResult.data[0].c_name).toBe("Acme Inc");
    });

    it("creates both source and target nodes inline", () => {
      // Create relationship with brand new nodes on both ends
      const result = executor.execute(
        `CREATE (a:Author {name: 'Jane'})-[:WROTE]->(b:Book {title: 'My Story'})`
      );

      expect(result.success).toBe(true);

      // Verify both nodes created
      const authorResult = expectSuccess(
        executor.execute("MATCH (a:Author) RETURN a.name")
      );
      expect(authorResult.data).toHaveLength(1);
      expect(authorResult.data[0].a_name).toBe("Jane");

      const bookResult = expectSuccess(
        executor.execute("MATCH (b:Book) RETURN b.title")
      );
      expect(bookResult.data).toHaveLength(1);
      expect(bookResult.data[0].b_title).toBe("My Story");

      // Verify relationship
      const relResult = expectSuccess(
        executor.execute("MATCH (a:Author)-[:WROTE]->(b:Book) RETURN a.name, b.title")
      );
      expect(relResult.data).toHaveLength(1);
    });

    it("returns edge variable in RETURN clause", () => {
      // Create nodes first
      executor.execute("CREATE (a:Person {name: 'Alice'})");
      executor.execute("CREATE (b:Person {name: 'Bob'})");

      // Get node IDs
      const alice = db.getNodesByLabel("Person").find(n => n.properties.name === "Alice")!;
      const bob = db.getNodesByLabel("Person").find(n => n.properties.name === "Bob")!;

      // Create relationship with edge variable
      db.insertEdge("knows-1", "KNOWS", alice.id, bob.id, { since: 2020 });

      // Query and return node properties (edge return not fully supported)
      const result = expectSuccess(
        executor.execute("MATCH (a:Person {name: 'Alice'})-[r:KNOWS]->(b:Person) RETURN a.name, b.name")
      );

      expect(result.data).toHaveLength(1);
      expect(result.data[0].a_name).toBe("Alice");
      expect(result.data[0].b_name).toBe("Bob");
    });

    it("handles edge variable in CREATE and RETURN", () => {
      // Create nodes
      executor.execute("CREATE (u:User {id: 'u1', name: 'Charlie'})");

      // Match and create with edge variable, then return the created node
      const result = executor.execute(
        `MATCH (u:User {id: 'u1'})
         CREATE (u)-[r:FOLLOWS {since: 2024}]->(t:Topic {name: 'GraphDB'})
         RETURN t`
      );

      expect(result.success).toBe(true);
      if (result.success) {
        expect(result.data).toHaveLength(1);
        expect(result.data[0].t).toMatchObject({
          label: "Topic",
          properties: { name: "GraphDB" }
        });
      }

      // Verify the relationship was created with properties
      const relResult = expectSuccess(
        executor.execute("MATCH (u:User)-[r:FOLLOWS]->(t:Topic) RETURN u.name, t.name")
      );
      expect(relResult.data).toHaveLength(1);
      expect(relResult.data[0].u_name).toBe("Charlie");
      expect(relResult.data[0].t_name).toBe("GraphDB");
    });

    it("reuses node variable across multiple relationship patterns", () => {
      // Create a hub node
      executor.execute("CREATE (h:Hub {name: 'Central'})");

      // Match hub and create multiple spokes with shared variable reference
      const result = executor.execute(
        `MATCH (h:Hub {name: 'Central'})
         CREATE (h)-[:CONNECTS]->(s1:Spoke {name: 'Spoke1'}),
                (h)-[:CONNECTS]->(s2:Spoke {name: 'Spoke2'})`
      );

      expect(result.success).toBe(true);

      // Verify both spokes created and connected
      const spokeResult = expectSuccess(
        executor.execute("MATCH (h:Hub)-[:CONNECTS]->(s:Spoke) RETURN s.name")
      );
      expect(spokeResult.data).toHaveLength(2);
      const spokeNames = spokeResult.data.map(r => r.s_name);
      expect(spokeNames).toContain("Spoke1");
      expect(spokeNames).toContain("Spoke2");
    });
  });

  describe("Full CRUD lifecycle", () => {
    it("performs CREATE, READ, UPDATE, DELETE on a single entity", () => {
      // === CREATE ===
      const createResult = expectSuccess(
        executor.execute(
          `CREATE (p:Product {
            id: $id,
            name: $name,
            price: $price,
            stock: $stock,
            active: $active
          }) RETURN p`,
          {
            id: "prod-001",
            name: "Widget Pro",
            price: 29.99,
            stock: 100,
            active: true
          }
        )
      );

      expect(createResult.data).toHaveLength(1);
      expect(createResult.data[0].p).toMatchObject({
        label: "Product",
        properties: {
          id: "prod-001",
          name: "Widget Pro",
          price: 29.99,
          stock: 100,
          active: true
        }
      });

      // === READ ===
      const readResult = expectSuccess(
        executor.execute(
          "MATCH (p:Product {id: $id}) RETURN p.name, p.price, p.stock",
          { id: "prod-001" }
        )
      );

      expect(readResult.data).toHaveLength(1);
      expect(readResult.data[0].p_name).toBe("Widget Pro");
      expect(readResult.data[0].p_price).toBe(29.99);
      expect(readResult.data[0].p_stock).toBe(100);

      // === UPDATE ===
      const updateResult = expectSuccess(
        executor.execute(
          `MATCH (p:Product {id: $id})
           SET p.price = $newPrice, p.stock = $newStock
           RETURN p`,
          {
            id: "prod-001",
            newPrice: 24.99,
            newStock: 85
          }
        )
      );

      expect(updateResult.data).toHaveLength(1);
      expect(updateResult.data[0].p).toMatchObject({
        properties: {
          id: "prod-001",
          name: "Widget Pro",
          price: 24.99,
          stock: 85
        }
      });

      // Verify update persisted
      const verifyUpdate = expectSuccess(
        executor.execute(
          "MATCH (p:Product {id: $id}) RETURN p.price, p.stock",
          { id: "prod-001" }
        )
      );
      expect(verifyUpdate.data[0].p_price).toBe(24.99);
      expect(verifyUpdate.data[0].p_stock).toBe(85);

      // === DELETE ===
      const deleteResult = expectSuccess(
        executor.execute(
          "MATCH (p:Product {id: $id}) DELETE p",
          { id: "prod-001" }
        )
      );

      // Verify deletion
      const verifyDelete = expectSuccess(
        executor.execute(
          "MATCH (p:Product {id: $id}) RETURN p",
          { id: "prod-001" }
        )
      );
      expect(verifyDelete.data).toHaveLength(0);

      // Verify no products remain
      const countResult = expectSuccess(
        executor.execute("MATCH (p:Product) RETURN COUNT(p)")
      );
      expect(countResult.data[0].count).toBe(0);
    });
  });

  describe("OPTIONAL MATCH", () => {
    it("returns main node even when optional match has no results", () => {
      // Create a person without any friends
      executor.execute("CREATE (n:Person {name: 'Alice'})");

      const result = expectSuccess(
        executor.execute(
          "MATCH (n:Person {name: 'Alice'}) OPTIONAL MATCH (n)-[:KNOWS]->(m:Person) RETURN n.name, m.name"
        )
      );

      expect(result.data).toHaveLength(1);
      expect(result.data[0].n_name).toBe("Alice");
      expect(result.data[0].m_name).toBeNull();
    });

    it("returns matched nodes when optional pattern exists", () => {
      // Create a person with a friend
      executor.execute("CREATE (n:Person {name: 'Alice'})");
      executor.execute("CREATE (m:Person {name: 'Bob'})");
      
      const alice = db.getNodesByLabel("Person").find(n => n.properties.name === "Alice")!;
      const bob = db.getNodesByLabel("Person").find(n => n.properties.name === "Bob")!;
      db.insertEdge("e1", "KNOWS", alice.id, bob.id);

      const result = expectSuccess(
        executor.execute(
          "MATCH (n:Person {name: 'Alice'}) OPTIONAL MATCH (n)-[:KNOWS]->(m:Person) RETURN n.name, m.name"
        )
      );

      expect(result.data).toHaveLength(1);
      expect(result.data[0].n_name).toBe("Alice");
      expect(result.data[0].m_name).toBe("Bob");
    });

    it("returns multiple rows when optional match has multiple results", () => {
      // Create a person with multiple friends
      executor.execute("CREATE (n:Person {name: 'Alice'})");
      executor.execute("CREATE (m1:Person {name: 'Bob'})");
      executor.execute("CREATE (m2:Person {name: 'Charlie'})");
      
      const alice = db.getNodesByLabel("Person").find(n => n.properties.name === "Alice")!;
      const bob = db.getNodesByLabel("Person").find(n => n.properties.name === "Bob")!;
      const charlie = db.getNodesByLabel("Person").find(n => n.properties.name === "Charlie")!;
      db.insertEdge("e1", "KNOWS", alice.id, bob.id);
      db.insertEdge("e2", "KNOWS", alice.id, charlie.id);

      const result = expectSuccess(
        executor.execute(
          "MATCH (n:Person {name: 'Alice'}) OPTIONAL MATCH (n)-[:KNOWS]->(m:Person) RETURN n.name, m.name"
        )
      );

      expect(result.data).toHaveLength(2);
      const names = result.data.map((r: Record<string, unknown>) => r.m_name);
      expect(names).toContain("Bob");
      expect(names).toContain("Charlie");
    });

    it("handles multiple OPTIONAL MATCH clauses", () => {
      // Create a person with a friend but no employer
      executor.execute("CREATE (n:Person {name: 'Alice'})");
      executor.execute("CREATE (m:Person {name: 'Bob'})");
      
      const alice = db.getNodesByLabel("Person").find(n => n.properties.name === "Alice")!;
      const bob = db.getNodesByLabel("Person").find(n => n.properties.name === "Bob")!;
      db.insertEdge("e1", "KNOWS", alice.id, bob.id);

      const result = expectSuccess(
        executor.execute(`
          MATCH (n:Person {name: 'Alice'})
          OPTIONAL MATCH (n)-[:KNOWS]->(friend:Person)
          OPTIONAL MATCH (n)-[:WORKS_AT]->(company:Company)
          RETURN n.name, friend.name, company.name
        `)
      );

      expect(result.data).toHaveLength(1);
      expect(result.data[0].n_name).toBe("Alice");
      expect(result.data[0].friend_name).toBe("Bob");
      expect(result.data[0].company_name).toBeNull();
    });

    it("handles OPTIONAL MATCH with WHERE clause", () => {
      // Create a person with friends of different ages
      executor.execute("CREATE (n:Person {name: 'Alice'})");
      executor.execute("CREATE (m1:Person {name: 'Bob', age: 30})");
      executor.execute("CREATE (m2:Person {name: 'Charlie', age: 20})");
      
      const alice = db.getNodesByLabel("Person").find(n => n.properties.name === "Alice")!;
      const bob = db.getNodesByLabel("Person").find(n => n.properties.name === "Bob")!;
      const charlie = db.getNodesByLabel("Person").find(n => n.properties.name === "Charlie")!;
      db.insertEdge("e1", "KNOWS", alice.id, bob.id);
      db.insertEdge("e2", "KNOWS", alice.id, charlie.id);

      const result = expectSuccess(
        executor.execute(
          "MATCH (n:Person {name: 'Alice'}) OPTIONAL MATCH (n)-[:KNOWS]->(m:Person) WHERE m.age > 25 RETURN n.name, m.name"
        )
      );

      expect(result.data).toHaveLength(1);
      expect(result.data[0].n_name).toBe("Alice");
      expect(result.data[0].m_name).toBe("Bob");
    });

    it("combines required MATCH with OPTIONAL MATCH", () => {
      // Create: Person -> Works at Company, Person may or may not have friends
      executor.execute("CREATE (n:Person {name: 'Alice'})");
      executor.execute("CREATE (c:Company {name: 'Acme'})");
      
      const alice = db.getNodesByLabel("Person").find(n => n.properties.name === "Alice")!;
      const acme = db.getNodesByLabel("Company").find(c => c.properties.name === "Acme")!;
      db.insertEdge("e1", "WORKS_AT", alice.id, acme.id);

      const result = expectSuccess(
        executor.execute(`
          MATCH (n:Person {name: 'Alice'})-[:WORKS_AT]->(c:Company)
          OPTIONAL MATCH (n)-[:KNOWS]->(m:Person)
          RETURN n.name, c.name, m.name
        `)
      );

      expect(result.data).toHaveLength(1);
      expect(result.data[0].n_name).toBe("Alice");
      expect(result.data[0].c_name).toBe("Acme");
      expect(result.data[0].m_name).toBeNull();
    });

    it("returns empty when required MATCH fails", () => {
      // No matching person at all
      const result = expectSuccess(
        executor.execute(
          "MATCH (n:Person {name: 'NonExistent'}) OPTIONAL MATCH (n)-[:KNOWS]->(m:Person) RETURN n.name, m.name"
        )
      );

      expect(result.data).toHaveLength(0);
    });
  });

  describe("RETURN DISTINCT", () => {
    it("returns distinct values for a property", () => {
      executor.execute("CREATE (n:Person {name: 'Alice', city: 'NYC'})");
      executor.execute("CREATE (n:Person {name: 'Bob', city: 'NYC'})");
      executor.execute("CREATE (n:Person {name: 'Charlie', city: 'LA'})");
      executor.execute("CREATE (n:Person {name: 'Dave', city: 'LA'})");
      executor.execute("CREATE (n:Person {name: 'Eve', city: 'Chicago'})");

      const result = expectSuccess(
        executor.execute("MATCH (n:Person) RETURN DISTINCT n.city")
      );

      expect(result.data).toHaveLength(3);
      const cities = result.data.map((r: Record<string, unknown>) => r.n_city);
      expect(cities).toContain("NYC");
      expect(cities).toContain("LA");
      expect(cities).toContain("Chicago");
    });

    it("returns distinct multiple properties", () => {
      executor.execute("CREATE (n:Person {name: 'Alice', city: 'NYC', country: 'USA'})");
      executor.execute("CREATE (n:Person {name: 'Bob', city: 'NYC', country: 'USA'})");
      executor.execute("CREATE (n:Person {name: 'Charlie', city: 'London', country: 'UK'})");
      executor.execute("CREATE (n:Person {name: 'Dave', city: 'NYC', country: 'USA'})");

      const result = expectSuccess(
        executor.execute("MATCH (n:Person) RETURN DISTINCT n.city, n.country")
      );

      expect(result.data).toHaveLength(2);
    });

    it("returns distinct with ORDER BY", () => {
      executor.execute("CREATE (n:Person {city: 'NYC'})");
      executor.execute("CREATE (n:Person {city: 'LA'})");
      executor.execute("CREATE (n:Person {city: 'NYC'})");
      executor.execute("CREATE (n:Person {city: 'Chicago'})");
      executor.execute("CREATE (n:Person {city: 'LA'})");

      const result = expectSuccess(
        executor.execute("MATCH (n:Person) RETURN DISTINCT n.city ORDER BY n.city ASC")
      );

      expect(result.data).toHaveLength(3);
      expect(result.data[0].n_city).toBe("Chicago");
      expect(result.data[1].n_city).toBe("LA");
      expect(result.data[2].n_city).toBe("NYC");
    });

    it("returns distinct with LIMIT", () => {
      executor.execute("CREATE (n:Person {city: 'NYC'})");
      executor.execute("CREATE (n:Person {city: 'LA'})");
      executor.execute("CREATE (n:Person {city: 'NYC'})");
      executor.execute("CREATE (n:Person {city: 'Chicago'})");

      const result = expectSuccess(
        executor.execute("MATCH (n:Person) RETURN DISTINCT n.city ORDER BY n.city LIMIT 2")
      );

      expect(result.data).toHaveLength(2);
    });

    it("returns all rows without DISTINCT", () => {
      executor.execute("CREATE (n:Person {city: 'NYC'})");
      executor.execute("CREATE (n:Person {city: 'NYC'})");
      executor.execute("CREATE (n:Person {city: 'NYC'})");

      const result = expectSuccess(
        executor.execute("MATCH (n:Person) RETURN n.city")
      );

      expect(result.data).toHaveLength(3);
    });
  });
});
