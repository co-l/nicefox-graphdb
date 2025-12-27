# @nicefox/graphdb-client

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

const graph = new NiceFoxGraphDB({
  url: 'https://graphdb.nicefox.net',
  project: 'myproject',
  env: process.env.NODE_ENV === 'production' ? 'production' : 'test',
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
  url: string;        // GraphDB server URL
  project: string;    // Project name
  env?: string;       // 'production' (default) or 'test'
  apiKey?: string;    // API key for authentication
})
```

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

- `CREATE`, `MERGE`, `MATCH`, `SET`, `DELETE`, `DETACH DELETE`
- `RETURN` with `COUNT`, `LIMIT`, aliases (`AS`)
- `WHERE` with `=`, `<>`, `<`, `>`, `<=`, `>=`, `AND`, `OR`, `NOT`, `CONTAINS`, `STARTS WITH`, `ENDS WITH`
- Parameters: `$paramName` syntax

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
