# Quick Start Guide

Get LeanGraph running in 5 minutes.

## Prerequisites

- Node.js 18+
- npm or pnpm

## 1. Install the Package

```bash
npm install leangraph
```

## 2. Start the Server

Create a directory for your data and start the server:

```bash
# Create data directory
mkdir -p ./graphdb-data

# Start the server
npx leangraph serve --data ./graphdb-data --port 3000
```

You should see:

```
╔═══════════════════════════════════════════════════════════╗
║              LeanGraph Server v0.1.0                      ║
╠═══════════════════════════════════════════════════════════╣
║  Endpoint:  http://localhost:3000                         ║
║  Data:      /path/to/graphdb-data                         ║
║  Auth:      disabled                                      ║
╚═══════════════════════════════════════════════════════════╝
```

## 3. Create a Project

In a new terminal:

```bash
npx leangraph create myapp --data ./graphdb-data
```

This creates:
- `graphdb-data/production/myapp.db` - Production database
- `graphdb-data/test/myapp.db` - Test database
- Two API keys (one for each environment)

Save the API keys - you'll need them to authenticate.

## 4. Connect from Your Application

```typescript
import { LeanGraph } from 'leangraph';

const db = await LeanGraph({ project: 'myapp' });

// Create some data
await db.execute(`
  CREATE (alice:Person {id: 'alice', name: 'Alice', age: 30}),
         (bob:Person {id: 'bob', name: 'Bob', age: 25}),
         (charlie:Person {id: 'charlie', name: 'Charlie', age: 35})
`);

// Create relationships
await db.execute(`
  MATCH (a:Person {id: 'alice'}), (b:Person {id: 'bob'})
  CREATE (a)-[:KNOWS {since: 2020}]->(b)
`);

await db.execute(`
  MATCH (b:Person {id: 'bob'}), (c:Person {id: 'charlie'})
  CREATE (b)-[:KNOWS {since: 2021}]->(c)
`);

// Query the graph
const results = await db.query(`
  MATCH (p:Person)-[:KNOWS]->(friend:Person)
  RETURN p.name AS person, friend.name AS knows
`);

console.log(results);
// [
//   { person: 'Alice', knows: 'Bob' },
//   { person: 'Bob', knows: 'Charlie' }
// ]
```

## 5. Run Queries from CLI

You can also query directly from the command line:

```bash
# List all people
npx leangraph query test myapp "MATCH (p:Person) RETURN p.name, p.age" --data ./graphdb-data

# Count relationships
npx leangraph query test myapp "MATCH ()-[r:KNOWS]->() RETURN COUNT(r) as count" --data ./graphdb-data
```

## Testing Without a Server

For unit tests, use the in-memory test client:

```typescript
import { createTestClient } from 'leangraph';

describe('my graph tests', () => {
  let client;

  beforeEach(async () => {
    client = await createTestClient();
  });

  afterEach(() => {
    client.close();
  });

  it('should create and query nodes', async () => {
    await client.execute('CREATE (n:Test {value: 42})');
    
    const results = await client.query('MATCH (n:Test) RETURN n.value as value');
    
    expect(results).toEqual([{ value: 42 }]);
  });
});
```

## Next Steps

- **Enable authentication**: API keys are generated when you create a project. Pass them in the `apiKey` option.
- **Set up backups**: Use `npx leangraph backup --data ./graphdb-data --output ./backups`
- **Clone production to test**: `npx leangraph clone myapp --data ./graphdb-data --force`
- **Learn Cypher**: See the [Cypher Support](../README.md#cypher-support) section in the main README

## Common Patterns

### Using Parameters

Always use parameters for user input to prevent injection:

```typescript
// Good - parameterized
const user = await db.query(
  'MATCH (u:User {email: $email}) RETURN u',
  { email: userInput }
);

// Bad - string concatenation
const user = await db.query(
  `MATCH (u:User {email: "${userInput}"}) RETURN u`  // Don't do this!
);
```

### Convenience Methods

The client provides helper methods for common operations:

```typescript
// Create a node and get its ID
const id = await db.createNode('User', { name: 'Alice', email: 'alice@example.com' });

// Get a node by properties
const user = await db.getNode('User', { email: 'alice@example.com' });

// Update a node
await db.updateNode(id, { name: 'Alice Smith' });

// Create a relationship
await db.createEdge(aliceId, 'FOLLOWS', bobId, { since: 2024 });

// Delete a node (and its relationships)
await db.deleteNode(id);
```

### Aggregations

```typescript
// Count by label
const counts = await db.query(`
  MATCH (u:User)
  RETURN COUNT(u) as userCount
`);

// Group and count
const byStatus = await db.query(`
  MATCH (t:Task)
  RETURN t.status as status, COUNT(t) as count
`);

// Collect into lists
const friendLists = await db.query(`
  MATCH (p:Person)-[:KNOWS]->(friend:Person)
  RETURN p.name as person, COLLECT(friend.name) as friends
`);
```

## Troubleshooting

### "Database not found"

Make sure you've created the project first:

```bash
npx leangraph create myapp --data ./graphdb-data
```

### "Unauthorized"

If authentication is enabled, provide your API key:

```typescript
const db = await LeanGraph({
  project: 'myapp',
  apiKey: 'your-api-key',  // Required when auth is enabled
});
```

### Query Errors

Query errors include position information to help you debug:

```typescript
try {
  await db.query('MATCH (n:User) RETRN n');  // Typo: RETRN
} catch (error) {
  console.log(error.message);  // "Unexpected token: RETRN"
  console.log(error.line);     // 1
  console.log(error.column);   // 18
}
```
