# nicefox-graphdb-client

TypeScript client for NiceFox GraphDB.

## Installation

Install directly from GitHub:

```bash
npm install github:co-l/nicefox-graphdb#main
```

Then import the client (TypeScript):

```typescript
import { NiceFoxGraphDB } from 'nicefox-graphdb/packages/client/src/index.ts';
```

> **Note**: This imports TypeScript directly. Your project needs to support `.ts` imports (e.g., via `tsx`, `ts-node`, or a bundler like Vite/esbuild).

## Usage

```typescript
import { NiceFoxGraphDB } from 'nicefox-graphdb/packages/client/src/index.ts';

// Using environment variables (recommended)
// Set: GRAPHDB_PROJECT, GRAPHDB_API_KEY, and optionally GRAPHDB_URL, GRAPHDB_ENV
const graph = new NiceFoxGraphDB();

// Or with explicit options
const graph = new NiceFoxGraphDB({
  url: 'https://graphdb.nicefox.net',  // default
  project: 'myproject',
  env: 'production',                    // default
  apiKey: process.env.GRAPHDB_API_KEY
});

// Query
const users = await graph.query('MATCH (u:User) RETURN u LIMIT 10');

// Query with parameters
const user = await graph.query(
  'MATCH (u:User {id: $id}) RETURN u',
  { id: 'abc123' }
);

// Create nodes
await graph.execute(
  'CREATE (u:User {id: $id, name: $name, email: $email})',
  { id: 'user-1', name: 'Alice', email: 'alice@example.com' }
);

// Create relationships
await graph.execute(
  'MATCH (a:User {id: $from}), (b:User {id: $to}) CREATE (a)-[:FOLLOWS {since: $since}]->(b)',
  { from: 'user-1', to: 'user-2', since: 2024 }
);

// Count
const result = await graph.query('MATCH (u:User) RETURN COUNT(u) as count');
// result = [{ count: 42 }]
```

## API

### Constructor

```typescript
new NiceFoxGraphDB({
  url?: string;       // Server URL (default: GRAPHDB_URL or 'https://graphdb.nicefox.net')
  project?: string;   // Project name (default: GRAPHDB_PROJECT) - required
  env?: string;       // Environment name (default: GRAPHDB_ENV or 'production')
  apiKey?: string;    // API key (default: GRAPHDB_API_KEY)
  dataPath?: string;  // Data directory path (default: GRAPHDB_DATA_PATH)
})
```

### Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `GRAPHDB_URL` | Server URL | `https://graphdb.nicefox.net` |
| `GRAPHDB_PROJECT` | Project name | (required) |
| `NODE_ENV` | Environment name | `production` |
| `GRAPHDB_API_KEY` | API key for authentication | - |
| `GRAPHDB_DATA_PATH` | Data directory path | - |

### Methods

#### `query<T>(cypher: string, params?: object): Promise<T[]>`

Execute a Cypher query and return results.

#### `execute(cypher: string, params?: object): Promise<void>`

Execute a mutation (CREATE, SET, DELETE, MERGE) without returning data.

#### `queryRaw<T>(cypher: string, params?: object): Promise<QueryResponse<T>>`

Execute a query and return full response including metadata.

#### `health(): Promise<HealthResponse>`

Check server health.

## Supported Cypher

### Clauses & Keywords

| Keyword | Description |
|---------|-------------|
| `CREATE` | Create nodes and relationships |
| `MATCH` | Find patterns in the graph |
| `MERGE` | Match or create if not exists |
| `WHERE` | Filter results |
| `SET` | Update properties |
| `DELETE` | Delete nodes/relationships |
| `DETACH DELETE` | Delete nodes and their relationships |
| `RETURN` | Return results |
| `AS` | Aliases for returned values |
| `LIMIT` | Limit number of results |
| `SKIP` | Skip first N results |
| `ORDER BY` | Sort results (ASC/DESC) |
| `DISTINCT` | Remove duplicates |
| `AND` / `OR` / `NOT` | Boolean operators |
| `IS NULL` / `IS NOT NULL` | Null checks |
| `CONTAINS` / `STARTS WITH` / `ENDS WITH` | String matching |
| `IN` | List membership |
| `OPTIONAL MATCH` | Match or return null |
| `WITH` | Chain query parts |
| `UNION` / `UNION ALL` | Combine results |
| `UNWIND` | Expand list to rows |
| `CASE WHEN THEN ELSE END` | Conditional expressions |
| `EXISTS` | Check pattern existence |
| Variable-length paths (`*1..3`) | Path patterns |
| `CALL` (procedures) | Database introspection |

### Operators

| Operator | Description |
|----------|-------------|
| `=`, `<>`, `<`, `>`, `<=`, `>=` | Comparison |
| `+`, `-`, `*`, `/`, `%` | Arithmetic |

### Functions

**Aggregation**: `COUNT`, `SUM`, `AVG`, `MIN`, `MAX`, `COLLECT`

**Scalar**: `ID`, `coalesce`

**String**: `toUpper`, `toLower`, `trim`, `substring`, `replace`, `toString`, `split`

**List**: `size`, `head`, `last`, `tail`, `keys`, `range`

**Node/Relationship**: `labels`, `type`, `properties`

**Math**: `abs`, `ceil`, `floor`, `round`, `rand`, `sqrt`

**Date/Time**: `date`, `datetime`, `timestamp`

### Procedures

| Procedure | Description |
|-----------|-------------|
| `CALL db.labels() YIELD label` | List all node labels |
| `CALL db.relationshipTypes() YIELD type` | List all relationship types |
| `CALL db.propertyKeys() YIELD key` | List all property keys |

### Parameters

Use `$paramName` syntax for parameterized queries:

```typescript
await graph.query('MATCH (u:User {id: $id}) RETURN u', { id: 'abc123' });
```

## Examples

### Create a social graph

```typescript
// Create users
await graph.execute(`
  CREATE (alice:User {id: 'alice', name: 'Alice'}),
         (bob:User {id: 'bob', name: 'Bob'}),
         (charlie:User {id: 'charlie', name: 'Charlie'})
`);

// Create friendships
await graph.execute(`
  MATCH (a:User {id: 'alice'}), (b:User {id: 'bob'})
  CREATE (a)-[:FRIENDS {since: 2020}]->(b)
`);

// Find friends of friends
const fof = await graph.query(`
  MATCH (me:User {id: $userId})-[:FRIENDS]->(friend)-[:FRIENDS]->(fof)
  WHERE fof.id <> $userId
  RETURN fof
`, { userId: 'alice' });
```

### Task management

```typescript
// Create project with tasks
await graph.execute(`
  CREATE (p:Project {id: $projectId, name: $name})
`, { projectId: 'proj-1', name: 'My Project' });

await graph.execute(`
  MATCH (p:Project {id: $projectId})
  CREATE (t:Task {id: $taskId, title: $title, status: 'pending'})-[:BELONGS_TO]->(p)
`, { projectId: 'proj-1', taskId: 'task-1', title: 'First task' });

// Update task status
await graph.execute(`
  MATCH (t:Task {id: $taskId})
  SET t.status = $status
`, { taskId: 'task-1', status: 'completed' });

// Count pending tasks
const pending = await graph.query(`
  MATCH (t:Task)-[:BELONGS_TO]->(p:Project {id: $projectId})
  WHERE t.status = 'pending'
  RETURN COUNT(t) as count
`, { projectId: 'proj-1' });
```

## Important Conventions

### Node Return Structure

When returning a full node (e.g., `RETURN u`), the result contains the node's properties directly (Neo4j 3.5 format):

```typescript
const result = await graph.query('MATCH (u:User {id: $id}) RETURN u', { id: 'abc123' });
// result = [{
//   u: {
//     id: "abc123",               // Your application ID
//     name: "Alice",
//     email: "alice@example.com"
//   }
// }]

// Access properties directly
const user = result[0].u;
const userName = user.name;  // "Alice"
```

To access node metadata (internal ID, labels, relationship type), use the corresponding functions:

```typescript
// Get internal database ID
const result = await graph.query('MATCH (u:User {id: $id}) RETURN u, id(u) as nodeId', { id: 'abc123' });
// result = [{ u: { id: "abc123", name: "Alice" }, nodeId: "internal-uuid" }]

// Get node labels
const result = await graph.query('MATCH (u:User) RETURN labels(u) as labels');
// result = [{ labels: ["User"] }]

// Get relationship type
const result = await graph.query('MATCH ()-[r:FOLLOWS]->() RETURN type(r) as relType');
// result = [{ relType: "FOLLOWS" }]
```

### Automatic JSON Parsing

NiceFox GraphDB automatically parses JSON strings stored in properties. If you store a JSON string:

```typescript
await graph.execute(
  'CREATE (c:Chat {id: $id, messages: $messages})',
  { id: 'chat-1', messages: JSON.stringify([{ role: 'user', content: 'Hello' }]) }
);
```

When you retrieve it, the JSON string is automatically parsed back into an object/array:

```typescript
const result = await graph.query('MATCH (c:Chat {id: $id}) RETURN c', { id: 'chat-1' });
const chat = result[0].c;
// chat.messages is already an array, NOT a string!
// chat.messages = [{ role: 'user', content: 'Hello' }]

// DON'T do this - it will fail because messages is already parsed:
// const messages = JSON.parse(chat.messages);  // ERROR!

// DO this instead:
const messages = chat.messages;  // Already an array
```

This automatic parsing is recursive and applies to all JSON-serializable values in properties. If you need the raw string, store it with a wrapper or use a different serialization approach.
