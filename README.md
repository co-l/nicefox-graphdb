# LeanGraph

[![npm version](https://img.shields.io/npm/v/leangraph.svg)](https://www.npmjs.com/package/leangraph)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![TCK](https://img.shields.io/badge/openCypher_TCK-100%25-brightgreen.svg)](https://opencypher.org/)

A lightweight, embeddable graph database with **full Cypher query support**, powered by SQLite.

> **100% openCypher TCK Compliance** — LeanGraph passes all 2,684 test scenarios from the openCypher Technology Compatibility Kit (Neo4j 3.5 baseline). Every Cypher feature that Neo4j 3.5 supports, LeanGraph supports.

## Why LeanGraph?

| Feature | LeanGraph | Neo4j |
|---------|-----------|-------|
| **Startup time** | Instant | 30+ seconds |
| **Memory** | ~50MB | 1GB+ minimum |
| **Deployment** | Single npm package | JVM + complex setup |
| **Docker required** | No | Typically yes |
| **Works offline** | Yes | Server required |
| **Backup** | Copy the SQLite file | Enterprise license |
| **Cypher support** | Full (Neo4j 3.5 parity) | Full |
| **Cost** | Free, MIT license | Free tier limited |

LeanGraph is ideal for:
- Production graph workloads with zero infrastructure
- Neo4j-level queries without Neo4j-level complexity
- Self-hosted apps where simplicity is a feature
- Instant local databases for development and testing

## Installation

```bash
npm install leangraph
npm install -D better-sqlite3
```

`better-sqlite3` is only needed for local and test modes. Production deployments using remote mode don't require it, keeping your `node_modules` lean and avoiding native rebuilds.

## Quick Start

```typescript
import { LeanGraph } from 'leangraph';

const db = await LeanGraph({ project: 'myapp' });

// Create nodes and relationships
await db.execute(`
  CREATE (alice:User {name: 'Alice'})-[:FOLLOWS]->(bob:User {name: 'Bob'})
`);

// Query the graph
const users = await db.query('MATCH (u:User) RETURN u.name AS name');
console.log(users); // [{ name: 'Alice' }, { name: 'Bob' }]

db.close();
```

## Modes

| Mode | `LEANGRAPH_MODE` | Behavior |
|------|------------------|----------|
| **Local** | unset or `local` | Embedded SQLite at `./data/{project}.db` |
| **Remote** | `remote` | HTTP connection to LeanGraph server |
| **Test** | `test` | In-memory SQLite (resets on restart) |

### Local Mode (default)

Uses an embedded SQLite database. No server required.

```typescript
const db = await LeanGraph({ project: 'myapp' });
// Data persists at ./data/myapp.db
```

### Remote Mode

Your code can stay identical for local development and production. Just configure environment variables:

**.env**
```bash
LEANGRAPH_MODE=remote
LEANGRAPH_API_KEY=lg_xxx
```

```typescript
// Same code works locally (dev) and remotely (production)
const db = await LeanGraph({ project: 'myapp' });
```

When `LEANGRAPH_MODE=remote` is set, LeanGraph automatically connects via HTTP instead of embedded LeanGraph.

> **Tip:** Remote mode doesn't use `better-sqlite3`, so installing it as a dev dependency speeds up production deploys by skipping native module compilation.

### Test Mode

Uses an in-memory SQLite database that resets when the process exits.

```typescript
const db = await LeanGraph({ mode: 'test', project: 'myapp' });
```

## Configuration

```typescript
interface LeanGraphOptions {
  mode?: "local" | "remote" | "test";
  project?: string;
  url?: string;
  apiKey?: string;
  dataPath?: string;
}
```

| Option | Environment Variable | Default | Description |
|--------|---------------------|---------|-------------|
| `mode` | `LEANGRAPH_MODE` | `"local"` | `local`, `remote`, or `test` |
| `project` | `LEANGRAPH_PROJECT` | — | Project name (required) |
| `url` | `LEANGRAPH_URL` | `"https://leangraph.io"` | Server URL (remote mode) |
| `apiKey` | `LEANGRAPH_API_KEY` | — | API key (remote mode) |
| `dataPath` | `LEANGRAPH_DATA_PATH` | `"./data"` | Data directory (local mode) |

Options passed to `LeanGraph()` take precedence over environment variables.

## API Reference

### `LeanGraph(options): Promise<LeanGraphClient>`

Create a new LeanGraph client. Returns a promise that resolves to a client instance.

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

### Convenience Methods

Thin wrappers around common Cypher operations:

```typescript
db.createNode(label, properties?): Promise<string>
db.getNode(label, filter): Promise<Record<string, unknown> | null>
db.updateNode(id, properties): Promise<void>
db.deleteNode(id): Promise<void>
db.createEdge(sourceId, type, targetId, properties?): Promise<void>
```

### `db.health(): Promise<{ status: string; timestamp: string }>`

Check server health. In development mode, always returns `{ status: 'ok', ... }`.

### `db.close(): void`

Close the client and release resources. **Always call this when done.**

```typescript
const db = await LeanGraph({ ... });
try {
  // ... use db
} finally {
  db.close();
}
```

## Common Patterns

### CRUD Operations

```typescript
// Create
await db.execute(
  'CREATE (u:User {name: $name, email: $email})',
  { name: 'Alice', email: 'alice@example.com' }
);

// Read
const [user] = await db.query<{ name: string; email: string }>(
  'MATCH (u:User {email: $email}) RETURN u.name AS name, u.email AS email',
  { email: 'alice@example.com' }
);

// Update
await db.execute(
  'MATCH (u:User {email: $email}) SET u.verified = true',
  { email: 'alice@example.com' }
);

// Delete
await db.execute(
  'MATCH (u:User {email: $email}) DETACH DELETE u',
  { email: 'alice@example.com' }
);
```

### Parameterized Queries

Always use parameters for user input:

```typescript
// Good - parameterized
const users = await db.query(
  'MATCH (u:User) WHERE u.email = $email RETURN u',
  { email: userInput }
);

// Bad - string interpolation (injection risk)
const users = await db.query(`MATCH (u:User) WHERE u.email = '${userInput}' RETURN u`);
```

### Typed Results

```typescript
interface User {
  name: string;
  email: string;
}

const users = await db.query<User>(
  'MATCH (u:User) RETURN u.name AS name, u.email AS email'
);

users[0].name;  // TypeScript knows this is string
```

### Relationships

```typescript
// Create a relationship
await db.execute(`
  MATCH (a:User {name: $from}), (b:User {name: $to})
  CREATE (a)-[:FOLLOWS {since: $since}]->(b)
`, { from: 'Alice', to: 'Bob', since: '2024-01-01' });

// Query relationships
const following = await db.query<{ name: string }>(`
  MATCH (:User {name: $name})-[:FOLLOWS]->(friend:User)
  RETURN friend.name AS name
`, { name: 'Alice' });

// Variable-length paths (1-3 hops)
const connections = await db.query<{ name: string }>(`
  MATCH (:User {name: $name})-[:FOLLOWS*1..3]->(connection:User)
  RETURN DISTINCT connection.name AS name
`, { name: 'Alice' });
```

### Upsert with MERGE

```typescript
await db.execute(`
  MERGE (u:User {email: $email})
  ON CREATE SET u.name = $name, u.createdAt = datetime()
  ON MATCH SET u.lastSeen = datetime()
`, { email: 'alice@example.com', name: 'Alice' });
```

### Batch Insert with UNWIND

```typescript
const users = [
  { name: 'Alice', email: 'alice@example.com' },
  { name: 'Bob', email: 'bob@example.com' },
];

await db.execute(`
  UNWIND $users AS data
  CREATE (u:User {name: data.name, email: data.email})
`, { users });
```

### Error Handling

```typescript
import { LeanGraph, LeanGraphError } from 'leangraph';

try {
  await db.query('MATCH (n:User RETURN n'); // syntax error
} catch (err) {
  if (err instanceof LeanGraphError) {
    console.error(`Query failed: ${err.message}`);
    console.error(`Position: line ${err.line}, column ${err.column}`);
  }
}
```

### Testing

Use test mode for fast, isolated tests:

```typescript
import { LeanGraph } from 'leangraph';

const db = await LeanGraph({ mode: 'test', project: 'test' });

// Tests run against in-memory database
await db.execute('CREATE (u:User {name: $name})', { name: 'Test' });
const [user] = await db.query('MATCH (u:User) RETURN u.name AS name');
assert(user.name === 'Test');

db.close(); // In-memory DB is discarded
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
npx leangraph serve --port 3000 --data ./data

# Or with custom host binding
npx leangraph serve --port 3000 --host 0.0.0.0 --data ./data
```

### Creating Projects

```bash
# Create a new project (generates API key)
npx leangraph create myapp --data ./data

# Output:
#   [created] production/myapp.db
#   API Key: lg_abc123...
```

### CLI Reference

```bash
# Server
leangraph serve [options]
  -p, --port <port>     Port to listen on (default: 3000)
  -d, --data <path>     Data directory (default: /var/data/leangraph)
  -H, --host <host>     Host to bind to (default: localhost)
  -b, --backup <path>   Backup directory (enables backup endpoints)

# Project management
leangraph create <project>   Create new project with API keys
leangraph delete <project>   Delete project (use --force)
leangraph list               List all projects

# Environment management
leangraph clone <project> --from <env> --to <env>   Copy between environments
leangraph wipe <project> --env <env>                Clear environment database

# Direct queries
leangraph query <env> <project> "CYPHER"

# Backup
leangraph backup [options]
  -o, --output <path>   Backup directory
  -p, --project <name>  Backup specific project
  --status              Show backup status

# API keys
leangraph apikey add <project>
leangraph apikey list
leangraph apikey remove <prefix>
```



## Advanced Usage

### Direct Database Access

For advanced use cases, you can access the underlying components:

```typescript
import { GraphDatabase, Executor, parse, translate } from 'leangraph';

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
import { createServer } from 'leangraph';
import { serve } from '@hono/node-server';

const { app, dbManager } = createServer({
  dataPath: './data',
  apiKeys: {
    'my-api-key': { project: 'myapp', env: 'production' }
  }
});

serve({ fetch: app.fetch, port: 3000 });
```

## Known Limitations

### Large Integer Precision

JavaScript cannot precisely represent integers larger than `Number.MAX_SAFE_INTEGER` (9,007,199,254,740,991). Integers beyond this range will lose precision, which can cause unexpected behavior when comparing values.

**Example of the problem:**
```javascript
// These two different numbers become equal in JavaScript!
const a = 4611686018427387905;
const b = 4611686018427387900;
console.log(a === b); // true (both round to 4611686018427388000)
```

**Workaround:** Use strings for large integer IDs:
```cypher
// Instead of:
CREATE (u:User {id: 4611686018427387905})

// Use strings:
CREATE (u:User {id: '4611686018427387905'})
MATCH (u:User {id: '4611686018427387905'}) RETURN u
```

This limitation affects all JavaScript-based systems, including Neo4j's JavaScript driver. For IDs that may exceed the safe integer range, string representation is the recommended approach.

## License

[MIT](https://github.com/co-l/leangraph/blob/main/LICENSE) - Conrad Lelubre
