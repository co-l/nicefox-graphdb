# LeanGraph: SQLite-Based Graph Database Service

## Project Overview

Build a lightweight, self-hosted graph database service that provides Cypher-like query syntax over SQLite with JSON properties. Designed for developers who want schema-free graph storage with simple deployment, centralized backups, and multi-environment support.

## Core Requirements

### Must Have
- Schema-free nodes and edges (properties stored as JSON)
- Cypher-like query syntax for: CREATE, MERGE, MATCH, SET, DELETE, RETURN, COUNT, LIMIT, WHERE
- Multi-project support (isolated databases per project)
- Multi-environment support (production/test per project)
- HTTP API for queries
- CLI for administration
- TypeScript client library
- Hot backup capability (copy production DBs while service runs)
- Runs comfortably on 8GB RAM

### Nice to Have
- Query validation before execution
- Basic auth via API keys
- Request logging
- Metrics endpoint

## Architecture

```
/var/data/leangraph/
├── production/
│   ├── project-a.db
│   └── project-b.db
└── test/
    ├── project-a.db
    └── project-b.db
```

Single HTTP service manages all databases. Each request specifies environment + project.

## Database Schema (per SQLite file)

```sql
CREATE TABLE nodes (
    id TEXT PRIMARY KEY,
    label TEXT NOT NULL,
    properties JSON DEFAULT '{}'
);

CREATE TABLE edges (
    id TEXT PRIMARY KEY,
    type TEXT NOT NULL,
    source_id TEXT NOT NULL,
    target_id TEXT NOT NULL,
    properties JSON DEFAULT '{}',
    FOREIGN KEY (source_id) REFERENCES nodes(id) ON DELETE CASCADE,
    FOREIGN KEY (target_id) REFERENCES nodes(id) ON DELETE CASCADE
);

CREATE INDEX idx_nodes_label ON nodes(label);
CREATE INDEX idx_edges_type ON edges(type);
CREATE INDEX idx_edges_source ON edges(source_id);
CREATE INDEX idx_edges_target ON edges(target_id);
```

## Cypher Subset to Support

### CREATE
```cypher
CREATE (n:Person {name: 'Alice', age: 30})
CREATE (a:Person {name: 'Alice'})-[:KNOWS {since: 2020}]->(b:Person {name: 'Bob'})
```

### MERGE (create if not exists, match if exists)
```cypher
MERGE (n:Person {id: 'abc123'})
MERGE (n:Person {id: 'abc123'}) SET n.name = 'Alice'
```

### MATCH + RETURN
```cypher
MATCH (n:Person) RETURN n
MATCH (n:Person {name: 'Alice'}) RETURN n
MATCH (n:Person) WHERE n.age > 25 RETURN n.name, n.age
MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN a.name, b.name
MATCH (a:Person)-[r:KNOWS]->(b:Person) RETURN a, r, b
MATCH (a:Person)-[:KNOWS*1..3]->(b:Person) RETURN b  // variable length paths (stretch goal)
```

### SET
```cypher
MATCH (n:Person {id: 'abc123'}) SET n.name = 'Bob', n.age = 31
```

### DELETE
```cypher
MATCH (n:Person {id: 'abc123'}) DELETE n
MATCH (a)-[r:KNOWS]->(b) DELETE r
```

### COUNT + LIMIT
```cypher
MATCH (n:Person) RETURN COUNT(n)
MATCH (n:Person) RETURN n LIMIT 10
```

### Parameters
```cypher
CREATE (n:Person {name: $name, age: $age})
MATCH (n:Person {id: $id}) RETURN n
```

## HTTP API

### Query Endpoint
```
POST /query/:env/:project
Headers: Authorization: Bearer <api_key>
Body: {
  "cypher": "MATCH (n:Person) RETURN n LIMIT 10",
  "params": {}
}
Response: {
  "success": true,
  "data": [...],
  "meta": { "count": 10, "time_ms": 5 }
}
```

### Admin Endpoints
```
POST   /admin/projects/:env/:project    # Create new project DB
DELETE /admin/projects/:env/:project    # Delete project DB
POST   /admin/clone/:project            # Copy production → test
POST   /admin/wipe/:project             # Wipe test DB (refuses on production)
GET    /admin/backup                    # Trigger backup, return status
GET    /admin/list                      # List all projects and environments
GET    /health                          # Health check
```

## CLI

```bash
# Project management
leangraph create <project>              # Creates both prod and test DBs
leangraph delete <project>              # Deletes both (requires confirmation)
leangraph list                          # List all projects

# Environment management  
leangraph clone <project>               # Copy production → test
leangraph wipe <project>                # Wipe test DB only

# Backup
leangraph backup                        # Trigger backup now
leangraph backup --schedule "0 * * * *" # Show/set backup schedule

# Query (for debugging)
leangraph query <env> <project> "CYPHER QUERY HERE"

# Server
leangraph serve                         # Start the HTTP server
leangraph serve --port 3000 --data /var/data/leangraph
```

## TypeScript Client Library

```typescript
import { GraphDB } from 'leangraph';

// Initialize
const graph = await GraphDB({
  url: 'https://graph.yourdomain.com',
  project: 'myproject',
  env: process.env.NODE_ENV === 'production' ? 'production' : 'test',
  apiKey: process.env.GRAPHDB_API_KEY
});

// Query with Cypher
const users = await graph.query(`MATCH (u:User) RETURN u LIMIT 10`);

// Query with parameters
const user = await graph.query(
  `MATCH (u:User {id: $id}) RETURN u`,
  { id: 'abc123' }
);

// Convenience methods (optional, built on top of query)
await graph.createNode('User', { id: 'abc123', name: 'Alice' });
await graph.createEdge('abc123', 'FOLLOWS', 'def456', { since: 2024 });
const alice = await graph.getNode('User', { id: 'abc123' });
```

## Implementation Plan

### Phase 1: Core (get it working)
1. SQLite wrapper with schema initialization
2. Basic Cypher parser (start with regex, upgrade to proper parser if needed)
3. Query translator (Cypher → SQL)
4. HTTP server with query endpoint
5. Basic CLI (create, serve, query)

### Phase 2: Operations (make it usable)
6. Multi-environment support
7. Clone/wipe commands
8. Backup system
9. API key authentication
10. TypeScript client library

### Phase 3: Polish (make it nice)
11. Better error messages
12. Query validation
13. Logging
14. Metrics
15. Variable-length path queries (if needed)

## Tech Stack Suggestions

- **Runtime**: Bun (fast, good SQLite support, TypeScript native)
- **HTTP**: Hono or Elysia (lightweight, fast)
- **SQLite**: better-sqlite3 or Bun's native SQLite
- **CLI**: Commander.js or Bun's native arg parsing
- **Parser**: Start simple (regex + string manipulation), consider Chevrotain or Peggy if complexity grows

## File Structure

```
leangraph/
├── packages/
│   ├── server/           # HTTP service
│   │   ├── src/
│   │   │   ├── index.ts          # Entry point
│   │   │   ├── db.ts             # SQLite wrapper
│   │   │   ├── parser.ts         # Cypher parser
│   │   │   ├── translator.ts     # Cypher → SQL
│   │   │   ├── routes.ts         # HTTP routes
│   │   │   └── backup.ts         # Backup logic
│   │   └── package.json
│   ├── client/           # TypeScript client
│   │   ├── src/
│   │   │   └── index.ts
│   │   └── package.json
│   └── cli/              # CLI tool
│       ├── src/
│       │   └── index.ts
│       └── package.json
├── package.json          # Workspace root
└── README.md
```

## Key Design Decisions

1. **Why SQLite?** Battle-tested, zero-ops, file-based backup, runs anywhere, handles JSON well.

2. **Why not a real graph DB?** Kùzu abandoned, Neo4j's backup requires enterprise, Memgraph unstable, AGE's future uncertain. This is controllable.

3. **Why Cypher subset?** Familiar syntax, covers 90% of use cases, can extend later.

4. **Why HTTP service instead of direct file access?** Supports distributed projects, single backup point, environment isolation.

5. **Why separate production/test?** Clone prod to test for debugging, wipe test freely, never risk production data.

## Success Criteria

- [ ] Can create nodes and edges with arbitrary properties
- [ ] Can query with basic MATCH patterns
- [ ] Can run multiple projects in isolation
- [ ] Can clone production to test in one command
- [ ] Backup is one cron job
- [ ] New project setup takes < 30 seconds
- [ ] Memory usage stays under 500MB for typical workloads

## Notes for Claude in OpenCode

- Start with Phase 1, get a working prototype before adding features
- Test the Cypher parser thoroughly — it's the trickiest part
- Use parameterized SQL queries everywhere to prevent injection
- Keep the code simple and readable over clever
- When in doubt, ask — the human knows their use case better than the spec
