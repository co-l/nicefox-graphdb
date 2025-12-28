import { describe, it, expect, beforeEach, afterEach } from "vitest";
import { GraphDatabase, DatabaseManager } from "../src/db";

describe("GraphDatabase", () => {
  let db: GraphDatabase;

  beforeEach(() => {
    db = new GraphDatabase(":memory:");
    db.initialize();
  });

  afterEach(() => {
    db.close();
  });

  describe("Schema initialization", () => {
    it("creates nodes table", () => {
      const result = db.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='nodes'"
      );
      expect(result.rows).toHaveLength(1);
    });

    it("creates edges table", () => {
      const result = db.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='edges'"
      );
      expect(result.rows).toHaveLength(1);
    });

    it("creates indexes", () => {
      const result = db.execute(
        "SELECT name FROM sqlite_master WHERE type='index' AND name LIKE 'idx_%'"
      );
      expect(result.rows.length).toBeGreaterThanOrEqual(4);
    });

    it("can be called multiple times safely", () => {
      db.initialize();
      db.initialize();
      // Should not throw
      const result = db.execute("SELECT COUNT(*) as count FROM nodes");
      expect(result.rows[0]).toHaveProperty("count", 0);
    });
  });

  describe("Node operations", () => {
    it("inserts a node", () => {
      db.insertNode("node1", "Person", { name: "Alice" });
      expect(db.countNodes()).toBe(1);
    });

    it("retrieves a node by ID", () => {
      db.insertNode("node1", "Person", { name: "Alice", age: 30 });

      const node = db.getNode("node1");
      expect(node).not.toBeNull();
      expect(node!.id).toBe("node1");
      expect(node!.label).toEqual(["Person"]);
      expect(node!.properties).toEqual({ name: "Alice", age: 30 });
    });

    it("returns null for non-existent node", () => {
      const node = db.getNode("nonexistent");
      expect(node).toBeNull();
    });

    it("retrieves nodes by label", () => {
      db.insertNode("node1", "Person", { name: "Alice" });
      db.insertNode("node2", "Person", { name: "Bob" });
      db.insertNode("node3", "Company", { name: "Acme" });

      const persons = db.getNodesByLabel("Person");
      expect(persons).toHaveLength(2);
      expect(persons.map((n) => n.properties.name)).toContain("Alice");
      expect(persons.map((n) => n.properties.name)).toContain("Bob");
    });

    it("deletes a node", () => {
      db.insertNode("node1", "Person", { name: "Alice" });
      expect(db.countNodes()).toBe(1);

      const deleted = db.deleteNode("node1");
      expect(deleted).toBe(true);
      expect(db.countNodes()).toBe(0);
    });

    it("returns false when deleting non-existent node", () => {
      const deleted = db.deleteNode("nonexistent");
      expect(deleted).toBe(false);
    });

    it("updates node properties", () => {
      db.insertNode("node1", "Person", { name: "Alice" });

      db.updateNodeProperties("node1", { name: "Alicia", age: 31 });

      const node = db.getNode("node1");
      expect(node!.properties).toEqual({ name: "Alicia", age: 31 });
    });

    it("handles JSON properties correctly", () => {
      db.insertNode("node1", "Person", {
        name: "Alice",
        tags: ["dev", "admin"],
        metadata: { level: 5, active: true },
      });

      const node = db.getNode("node1");
      expect(node!.properties.tags).toEqual(["dev", "admin"]);
      expect(node!.properties.metadata).toEqual({ level: 5, active: true });
    });
  });

  describe("Edge operations", () => {
    beforeEach(() => {
      db.insertNode("node1", "Person", { name: "Alice" });
      db.insertNode("node2", "Person", { name: "Bob" });
    });

    it("inserts an edge", () => {
      db.insertEdge("edge1", "KNOWS", "node1", "node2", { since: 2020 });
      expect(db.countEdges()).toBe(1);
    });

    it("retrieves an edge by ID", () => {
      db.insertEdge("edge1", "KNOWS", "node1", "node2", { since: 2020 });

      const edge = db.getEdge("edge1");
      expect(edge).not.toBeNull();
      expect(edge!.id).toBe("edge1");
      expect(edge!.type).toBe("KNOWS");
      expect(edge!.source_id).toBe("node1");
      expect(edge!.target_id).toBe("node2");
      expect(edge!.properties).toEqual({ since: 2020 });
    });

    it("retrieves edges by type", () => {
      db.insertEdge("edge1", "KNOWS", "node1", "node2");
      db.insertEdge("edge2", "WORKS_WITH", "node1", "node2");

      const knowsEdges = db.getEdgesByType("KNOWS");
      expect(knowsEdges).toHaveLength(1);
      expect(knowsEdges[0].id).toBe("edge1");
    });

    it("deletes an edge", () => {
      db.insertEdge("edge1", "KNOWS", "node1", "node2");
      expect(db.countEdges()).toBe(1);

      const deleted = db.deleteEdge("edge1");
      expect(deleted).toBe(true);
      expect(db.countEdges()).toBe(0);
    });

    it("cascades delete from node to edges", () => {
      db.insertEdge("edge1", "KNOWS", "node1", "node2");
      expect(db.countEdges()).toBe(1);

      db.deleteNode("node1");
      expect(db.countEdges()).toBe(0);
    });

    it("enforces foreign key constraint", () => {
      expect(() => {
        db.insertEdge("edge1", "KNOWS", "node1", "nonexistent");
      }).toThrow();
    });
  });

  describe("execute", () => {
    it("runs SELECT queries and returns rows", () => {
      db.insertNode("node1", "Person", { name: "Alice" });
      db.insertNode("node2", "Person", { name: "Bob" });

      const result = db.execute("SELECT * FROM nodes WHERE label = ?", [
        "Person",
      ]);
      expect(result.rows).toHaveLength(2);
    });

    it("runs INSERT/UPDATE/DELETE and returns changes", () => {
      const result = db.execute(
        "INSERT INTO nodes (id, label, properties) VALUES (?, ?, ?)",
        ["node1", "Person", "{}"]
      );
      expect(result.changes).toBe(1);
    });

    it("supports json_extract in queries", () => {
      db.insertNode("node1", "Person", { name: "Alice", age: 30 });
      db.insertNode("node2", "Person", { name: "Bob", age: 25 });

      const result = db.execute(
        "SELECT * FROM nodes WHERE json_extract(properties, '$.age') > ?",
        [27]
      );
      expect(result.rows).toHaveLength(1);
      expect((result.rows[0] as any).id).toBe("node1");
    });

    it("supports json_set for updates", () => {
      db.insertNode("node1", "Person", { name: "Alice" });

      db.execute(
        "UPDATE nodes SET properties = json_set(properties, '$.age', ?) WHERE id = ?",
        [30, "node1"]
      );

      const node = db.getNode("node1");
      expect(node!.properties.age).toBe(30);
    });
  });

  describe("transaction", () => {
    it("commits successful transaction", () => {
      db.transaction(() => {
        db.insertNode("node1", "Person", { name: "Alice" });
        db.insertNode("node2", "Person", { name: "Bob" });
      });

      expect(db.countNodes()).toBe(2);
    });

    it("rolls back failed transaction", () => {
      try {
        db.transaction(() => {
          db.insertNode("node1", "Person", { name: "Alice" });
          throw new Error("Simulated failure");
        });
      } catch (e) {
        // Expected
      }

      expect(db.countNodes()).toBe(0);
    });

    it("returns value from transaction", () => {
      const result = db.transaction(() => {
        db.insertNode("node1", "Person", { name: "Alice" });
        return db.countNodes();
      });

      expect(result).toBe(1);
    });
  });
});

describe("DatabaseManager", () => {
  let manager: DatabaseManager;

  beforeEach(() => {
    manager = new DatabaseManager(":memory:");
  });

  afterEach(() => {
    manager.closeAll();
  });

  it("creates databases for different projects", () => {
    const db1 = manager.getDatabase("project-a", "production");
    const db2 = manager.getDatabase("project-b", "production");

    db1.insertNode("node1", "Person", { name: "Alice" });

    expect(db1.countNodes()).toBe(1);
    expect(db2.countNodes()).toBe(0);
  });

  it("creates databases for different environments", () => {
    const prod = manager.getDatabase("project-a", "production");
    const test = manager.getDatabase("project-a", "test");

    prod.insertNode("node1", "Person", { name: "Prod" });
    test.insertNode("node1", "Person", { name: "Test" });

    expect(prod.getNode("node1")!.properties.name).toBe("Prod");
    expect(test.getNode("node1")!.properties.name).toBe("Test");
  });

  it("reuses existing database connections", () => {
    const db1 = manager.getDatabase("project-a", "production");
    const db2 = manager.getDatabase("project-a", "production");

    expect(db1).toBe(db2);
  });

  it("lists open databases", () => {
    manager.getDatabase("project-a", "production");
    manager.getDatabase("project-b", "test");

    const list = manager.listDatabases();
    expect(list).toContain("production/project-a");
    expect(list).toContain("test/project-b");
  });

  it("closes all databases", () => {
    manager.getDatabase("project-a", "production");
    manager.getDatabase("project-b", "test");

    manager.closeAll();

    expect(manager.listDatabases()).toHaveLength(0);
  });
});
