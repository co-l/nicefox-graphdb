# NiceFox GraphDB

[![npm version](https://img.shields.io/npm/v/nicefox-graphdb.svg)](https://www.npmjs.com/package/nicefox-graphdb)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

A lightweight graph database with Cypher query support, powered by SQLite.

## Installation

```bash
npm install nicefox-graphdb
```

## Quick Start

```typescript
import { GraphDB } from 'nicefox-graphdb';

const db = await GraphDB({
  url: 'https://my-graphdb.example.com',
  project: 'myapp',
  apiKey: process.env.GRAPHDB_API_KEY,
});

// Create nodes and relationships
await db.execute(`
  CREATE (alice:User {name: 'Alice'})-[:FOLLOWS]->(bob:User {name: 'Bob'})
`);

// Query the graph
const users = await db.query('MATCH (u:User) RETURN u.name AS name');
console.log(users); // [{ name: 'Alice' }, { name: 'Bob' }]

db.close();
```

## Development vs Production Mode

NiceFox GraphDB automatically adapts based on the `NODE_ENV` environment variable:

| Mode | `NODE_ENV` | Behavior |
|------|-----------|----------|
| **Development** | `development` | Uses local SQLite database. `url` and `apiKey` are ignored. |
| **Production** | `production` (or unset) | Connects to remote server via HTTP. `url` and `apiKey` are required. |

This means you can use the **exact same code** in both environments:

```typescript
// Works in both development and production!
const db = await GraphDB({
  url: 'https://my-graphdb.example.com',
  project: 'myapp',
  apiKey: process.env.GRAPHDB_API_KEY,
});
```

### Development Mode

When `NODE_ENV=development`:
- A local SQLite database is created automatically
- No server setup required
- `url` and `apiKey` parameters are ignored
- Data persists at `./data/{env}/{project}.db` by default

```bash
# Run your app in development mode
NODE_ENV=development node app.js
```

### Production Mode

When `NODE_ENV=production` (or unset):
- Connects to a remote GraphDB server via HTTP
- `url` and `apiKey` are required

```bash
# Run your app in production mode
NODE_ENV=production GRAPHDB_API_KEY=xxx node app.js
```

## Configuration Options

| Option | Type | Required | Default | Description |
|--------|------|----------|---------|-------------|
| `url` | `string` | Yes | - | Base URL of the GraphDB server (used in production) |
| `project` | `string` | Yes | - | Project name |
| `apiKey` | `string` | No | - | API key for authentication (used in production) |
| `env` | `'production' \| 'test'` | No | `'production'` | Environment (determines database isolation) |
| `dataPath` | `string` | No | `'./data'` | Path for local data storage (development only). Use `':memory:'` for in-memory database |

### Examples

```typescript
// Production: connect to remote server
const db = await GraphDB({
  url: 'https://graphdb.example.com',
  project: 'myapp',
  apiKey: 'your-api-key',
  env: 'production',
});

// Development: use local SQLite (url/apiKey ignored)
// NODE_ENV=development
const db = await GraphDB({
  url: 'https://graphdb.example.com', // ignored
  project: 'myapp',
  apiKey: 'your-api-key',             // ignored
  dataPath: './local-data',           // custom data directory
});

// Testing: use in-memory database
// NODE_ENV=development
const db = await GraphDB({
  url: 'https://graphdb.example.com',
  project: 'test-project',
  dataPath: ':memory:',               // resets on each run
});
```

## API Reference

### `GraphDB(options): Promise<GraphDBClient>`

Create a new GraphDB client. Returns a promise that resolves to a client instance.

### `db.query<T>(cypher, params?): Promise<T[]>`

Execute a Cypher query and return results as an array.

```typescript
const users = await db.query<{ name: string; age: number }>(
  'MATCH (u:User) WHERE u.age > $minAge RETURN u.name AS name, u.age AS age',
  { minAge: 21 }
);
// users = [{ name: 'Alice', age: 25 }, { name: 'Bob', age: 30 }]
```

### `db.execute(cypher, params?): Promise<void>`

Execute a mutating query (CREATE, SET, DELETE, MERGE) without expecting return data.

```typescript
await db.execute('CREATE (n:User {name: $name, email: $email})', {
  name: 'Alice',
  email: 'alice@example.com'
});
```

### `db.queryRaw<T>(cypher, params?): Promise<QueryResponse<T>>`

Execute a query and return the full response including metadata.

```typescript
const response = await db.queryRaw('MATCH (n) RETURN n LIMIT 10');
console.log(response.meta.count);   // Number of rows
console.log(response.meta.time_ms); // Query execution time in ms
console.log(response.data);         // Array of results
```

### `db.createNode(label, properties?): Promise<string>`

Create a node and return its ID.

```typescript
const userId = await db.createNode('User', { name: 'Alice', email: 'alice@example.com' });
```

### `db.getNode(label, filter): Promise<Record<string, unknown> | null>`

Find a node by label and properties.

```typescript
const user = await db.getNode('User', { email: 'alice@example.com' });
if (user) {
  console.log(user.name); // 'Alice'
}
```

### `db.updateNode(id, properties): Promise<void>`

Update properties on a node.

```typescript
await db.updateNode(userId, { name: 'Alice Smith', verified: true });
```

### `db.deleteNode(id): Promise<void>`

Delete a node and all its relationships (DETACH DELETE).

```typescript
await db.deleteNode(userId);
```

### `db.createEdge(sourceId, type, targetId, properties?): Promise<void>`

Create a relationship between two nodes.

```typescript
await db.createEdge(aliceId, 'FOLLOWS', bobId, { since: '2024-01-01' });
```

### `db.health(): Promise<{ status: string; timestamp: string }>`

Check server health. In development mode, always returns `{ status: 'ok', ... }`.

### `db.close(): void`

Close the client and release resources. **Always call this when done.**

```typescript
const db = await GraphDB({ ... });
try {
  // ... use db
} finally {
  db.close();
}
```

## Cypher Quick Reference

### Supported Clauses

| Clause | Example |
|--------|---------|
| `CREATE` | `CREATE (n:User {name: 'Alice'})` |
| `MATCH` | `MATCH (n:User) RETURN n` |
| `OPTIONAL MATCH` | `OPTIONAL MATCH (n)-[:KNOWS]->(m) RETURN m` |
| `MERGE` | `MERGE (n:User {email: $email})` |
| `WHERE` | `WHERE n.age > 21 AND n.active = true` |
| `SET` | `SET n.name = 'Bob', n.updated = true` |
| `DELETE` | `DELETE n` |
| `DETACH DELETE` | `DETACH DELETE n` |
| `RETURN` | `RETURN n.name AS name, count(*) AS total` |
| `WITH` | `WITH n, count(*) AS cnt WHERE cnt > 1` |
| `UNWIND` | `UNWIND $list AS item CREATE (n {value: item})` |
| `UNION / UNION ALL` | `MATCH (n:A) RETURN n UNION MATCH (m:B) RETURN m` |
| `ORDER BY` | `ORDER BY n.name DESC` |
| `SKIP / LIMIT` | `SKIP 10 LIMIT 5` |
| `DISTINCT` | `RETURN DISTINCT n.category` |
| `CASE/WHEN` | `RETURN CASE WHEN n.age > 18 THEN 'adult' ELSE 'minor' END` |
| `CALL` | `CALL db.labels() YIELD label RETURN label` |

### Operators

| Category | Operators |
|----------|-----------|
| Comparison | `=`, `<>`, `<`, `>`, `<=`, `>=` |
| Logical | `AND`, `OR`, `NOT` |
| String | `CONTAINS`, `STARTS WITH`, `ENDS WITH` |
| List | `IN` |
| Null | `IS NULL`, `IS NOT NULL` |
| Pattern | `EXISTS` |
| Arithmetic | `+`, `-`, `*`, `/`, `%` |

### Functions

**Aggregation:** `COUNT`, `SUM`, `AVG`, `MIN`, `MAX`, `COLLECT`

**Scalar:** `ID`, `coalesce`

**String:** `toUpper`, `toLower`, `trim`, `substring`, `replace`, `toString`, `split`

**List:** `size`, `head`, `last`, `tail`, `keys`, `range`

**Node/Relationship:** `labels`, `type`, `properties`

**Math:** `abs`, `ceil`, `floor`, `round`, `rand`, `sqrt`

**Date/Time:** `date`, `datetime`, `timestamp`

### Variable-Length Paths

```cypher
-- Find friends of friends (1 to 3 hops)
MATCH (a:User {name: 'Alice'})-[:KNOWS*1..3]->(b:User)
RETURN DISTINCT b.name
```

### Procedures

```cypher
-- List all labels
CALL db.labels() YIELD label RETURN label

-- List all relationship types
CALL db.relationshipTypes() YIELD type RETURN type

-- List all property keys
CALL db.propertyKeys() YIELD key RETURN key
```

## Running the Server (Production)

For production deployments, run a dedicated server:

```bash
# Start the server
npx nicefox-graphdb serve --port 3000 --data ./data

# Or with custom host binding
npx nicefox-graphdb serve --port 3000 --host 0.0.0.0 --data ./data
```

### Creating Projects

```bash
# Create a new project (generates API keys)
npx nicefox-graphdb create myapp --data ./data

# Output:
#   [created] production/myapp.db
#   [created] test/myapp.db
#   API Keys:
#     production: nfx_abc123...
#     test:       nfx_def456...
```

### CLI Reference

```bash
# Server
nicefox-graphdb serve [options]
  -p, --port <port>     Port to listen on (default: 3000)
  -d, --data <path>     Data directory (default: /var/data/nicefox-graphdb)
  -H, --host <host>     Host to bind to (default: localhost)
  -b, --backup <path>   Backup directory (enables backup endpoints)

# Project management
nicefox-graphdb create <project>   Create new project with API keys
nicefox-graphdb delete <project>   Delete project (use --force)
nicefox-graphdb list               List all projects

# Environment management
nicefox-graphdb clone <project>    Copy production to test
nicefox-graphdb wipe <project>     Clear test database

# Direct queries
nicefox-graphdb query <env> <project> "CYPHER"

# Backup
nicefox-graphdb backup [options]
  -o, --output <path>   Backup directory
  -p, --project <name>  Backup specific project
  --status              Show backup status

# API keys
nicefox-graphdb apikey add <project>
nicefox-graphdb apikey list
nicefox-graphdb apikey remove <prefix>
```

## Why NiceFox?

| Feature | NiceFox GraphDB | Neo4j |
|---------|-----------------|-------|
| **Deployment** | Single package, zero config | Complex setup, JVM required |
| **Development** | Local SQLite, no server needed | Server required |
| **Backup** | Just copy the SQLite file | Enterprise license required |
| **Resource usage** | ~50MB RAM | 1GB+ RAM minimum |
| **Cypher support** | Core subset | Full |
| **Cost** | Free, MIT license | Free tier limited |

NiceFox is ideal for:
- Applications needing graph queries without ops burden
- Projects that outgrow JSON but don't need a full graph database
- Self-hosted deployments where simplicity matters
- Development and testing with instant local databases

## Advanced Usage

### Direct Database Access

For advanced use cases, you can access the underlying components:

```typescript
import { GraphDatabase, Executor, parse, translate } from 'nicefox-graphdb';

// Direct database access
const db = new GraphDatabase('./my-database.db');
db.initialize();

const executor = new Executor(db);
const result = executor.execute('MATCH (n) RETURN n LIMIT 10');

db.close();

// Parse Cypher to AST
const parseResult = parse('MATCH (n:User) RETURN n');
if (parseResult.success) {
  console.log(parseResult.query);
}

// Translate AST to SQL
const translation = translate(parseResult.query, {});
console.log(translation.statements);
```

### Running a Custom Server

```typescript
import { createServer } from 'nicefox-graphdb';
import { serve } from '@hono/node-server';

const { app, dbManager } = createServer({
  dataPath: './data',
  apiKeys: {
    'my-api-key': { project: 'myapp', env: 'production' }
  }
});

serve({ fetch: app.fetch, port: 3000 });
```

## License

[MIT](https://github.com/co-l/nicefox-graphdb/blob/main/LICENSE) - Conrad Lelubre
