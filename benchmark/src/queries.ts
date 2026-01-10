import type { QueryDefinition, ScaleConfig } from "./types.js";
import {
  randomUserId,
  randomItemId,
  randomCategory,
  uuid,
  fakeName,
  fakeEmail,
} from "./generator.js";

// Query definitions factory - needs config for random ID generation
export function createQueries(config: ScaleConfig): QueryDefinition[] {
  return [
    // ============ Category A: Point Lookups ============
    {
      name: "lookup_user_by_id",
      cypher: "MATCH (u:User {id: $id}) RETURN u",
      params: () => ({ id: randomUserId(config) }),
      category: "lookup",
    },
    {
      name: "lookup_item_by_id",
      cypher: "MATCH (i:Item) WHERE i.id = $id RETURN i",
      params: () => ({ id: randomItemId(config) }),
      category: "lookup",
    },
    {
      name: "lookup_user_by_email",
      cypher: "MATCH (u:User) WHERE u.email = $email RETURN u",
      params: () => ({ email: `user${randomUserId(config)}@example.com` }),
      category: "lookup",
    },

    // ============ Category B: Pattern Matching ============
    {
      name: "user_items",
      cypher: "MATCH (u:User {id: $id})-[:OWNS]->(i:Item) RETURN i",
      params: () => ({ id: randomUserId(config) }),
      category: "pattern",
    },
    {
      name: "items_by_category",
      cypher: `MATCH (u:User)-[:OWNS]->(i:Item {category: $cat}) 
               RETURN u.id, i.title LIMIT 100`,
      params: () => ({ cat: randomCategory() }),
      category: "pattern",
    },
    {
      name: "user_events",
      cypher: "MATCH (u:User {id: $id})-[:TRIGGERED]->(e:Event) RETURN e",
      params: () => ({ id: randomUserId(config) }),
      category: "pattern",
    },
    {
      name: "items_owned_by_multiple",
      cypher: `MATCH (u1:User)-[:OWNS]->(i:Item)<-[:OWNS]-(u2:User) 
               WHERE u1.id < u2.id 
               RETURN i.id, u1.id, u2.id LIMIT 50`,
      params: () => ({}),
      category: "pattern",
    },

    // ============ Category C: Aggregations ============
    {
      name: "user_item_counts",
      cypher: `MATCH (u:User)-[:OWNS]->(i:Item) 
               RETURN u.id, COUNT(i) AS cnt 
               ORDER BY cnt DESC LIMIT 10`,
      params: () => ({}),
      category: "aggregation",
    },
    {
      name: "category_stats",
      cypher: `MATCH (i:Item) 
               RETURN i.category, AVG(i.price) AS avg_price, COUNT(*) AS cnt`,
      params: () => ({}),
      category: "aggregation",
    },
    {
      name: "event_type_counts",
      cypher: `MATCH (e:Event) 
               RETURN e.type, COUNT(*) AS cnt 
               ORDER BY cnt DESC`,
      params: () => ({}),
      category: "aggregation",
    },
    {
      name: "user_event_summary",
      cypher: `MATCH (u:User {id: $id})-[:TRIGGERED]->(e:Event) 
               RETURN e.type, COUNT(*) AS cnt`,
      params: () => ({ id: randomUserId(config) }),
      category: "aggregation",
    },

    // ============ Category D: Traversals ============
    {
      name: "related_items_depth1",
      cypher: `MATCH (u:User {id: $id})-[:OWNS]->(i:Item)-[:RELATED_TO]->(r:Item) 
               RETURN DISTINCT r LIMIT 50`,
      params: () => ({ id: randomUserId(config) }),
      category: "traversal",
    },
    {
      name: "related_items_depth2",
      cypher: `MATCH (u:User {id: $id})-[:OWNS]->(i:Item)-[:RELATED_TO*1..2]->(r:Item) 
               RETURN DISTINCT r LIMIT 50`,
      params: () => ({ id: randomUserId(config) }),
      category: "traversal",
    },
    {
      name: "related_items_depth3",
      cypher: `MATCH (u:User {id: $id})-[:OWNS]->(i:Item)-[:RELATED_TO*1..3]->(r:Item) 
               RETURN DISTINCT r LIMIT 50`,
      params: () => ({ id: randomUserId(config) }),
      category: "traversal",
    },

    // ============ Category E: Write Operations ============
    {
      name: "create_user",
      cypher: `CREATE (u:User {id: $id, name: $name, email: $email, created_at: $ts})`,
      params: () => ({
        id: uuid(),
        name: fakeName(),
        email: fakeEmail(),
        ts: Date.now(),
      }),
      category: "write",
    },
    {
      name: "update_user_name",
      cypher: `MATCH (u:User {id: $id}) SET u.name = $name`,
      params: () => ({ id: randomUserId(config), name: fakeName() }),
      category: "write",
    },
    {
      name: "create_item",
      cypher: `CREATE (i:Item {id: $id, title: $title, category: $cat, price: $price})`,
      params: () => ({
        id: uuid(),
        title: `Benchmark Item ${Date.now()}`,
        cat: randomCategory(),
        price: Math.floor(Math.random() * 1000),
      }),
      category: "write",
    },
  ];
}

// Get queries by category
export function getQueriesByCategory(
  queries: QueryDefinition[],
  category: QueryDefinition["category"]
): QueryDefinition[] {
  return queries.filter((q) => q.category === category);
}

// Get read-only queries (for running before writes)
export function getReadQueries(queries: QueryDefinition[]): QueryDefinition[] {
  return queries.filter((q) => q.category !== "write");
}

// Get write queries
export function getWriteQueries(queries: QueryDefinition[]): QueryDefinition[] {
  return queries.filter((q) => q.category === "write");
}
