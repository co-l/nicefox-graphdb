# LeanGraph - TCK Compliance Guide

### What This Means
We only run tests that Neo4j 3.5 actually passes. This gives us a realistic, achievable target rather than chasing features that even Neo4j doesn't support.

### Excluded Categories
- `clauses/call` - CALL procedures not yet supported

## TDD Workflow

This workflow is to be respected to the letter:

1. **Run `./scripts/comment-first-failing.sh` **
2. **Run tests** - `npm test | tail -12` - see it fail
3. **Fix the code** until green 
  - Use `npm run tck 'Delete4|1' -- -v --sql -f` to get the test results with the generated SQL query
  - Or use bash from the "Example for debugging" part of this document, to manually inspect queries and database state
4. **Run `./scripts/comment-fixed-tests.sh` ** - automatically updates failing_tests.ts with working tests
5. **Commit and push**


### Example

```typescript
// In failing-tests.ts, change:
  "clauses/delete > Delete4 - Delete clause interoperation with other clauses|1",

// To:
  // "clauses/delete > Delete4 - Delete clause interoperation with other clauses|1",
```

Then run `npm test`, fix code, commit, push.

## Quick Commands

```bash
# Run all tests (skipping known failing)
npm test

# List only expected-error tests (tests where we should reject the query)
npm run tck:failing -- --errors

# Test a specific TCK test with details
npm run tck 'Delete4|1' -- -v --sql -f

# See what error a test produces
npm run tck 'Delete4|1' -- -f
```

### Finding Fixed Tests

After implementing a fix, use `./scripts/comment-fixed-tests.sh` automatically updates failing_tests.ts with working tests

## Key Files

- `test/tck/failing-tests.ts` - List of skipped tests
- `src/parser.ts` - Cypher parsing
- `src/translator.ts` - AST → SQL
- `src/executor.ts` - Query execution

## Error Quick Reference

| Error | Fix Location |
|-------|--------------|
| `Unexpected token` | parser.ts |
| `no such column` | translator.ts |
| `near "X": syntax error` | translator.ts |
| `Too few parameter values` | translator.ts or executor.ts |
| `Unknown variable` | translator.ts |

## Example for debugging

```bash
cd /home/conrad/dev/leangraph && GRAPHDB_PROJECT=test-debug tsx -e "
(async () => {
  const { GraphDB } = require('./src/index.ts');
  const db = await GraphDB({ dataPath: ':memory:' });
  // Setup
  await db.execute('CREATE (a:A), (b:B)');
  await db.execute('MATCH (a:A), (b:B) CREATE (a)-[:T1]->(b), (b)-[:T2]->(a)');
  
  // First let's check what's in the database
  let result = await db.query('MATCH (n) RETURN n');
  console.log('Nodes:', result.length);
  
  result = await db.query('MATCH ()-[r]->() RETURN r');
  console.log('Edges:', result.length);
  
  // Now the actual query - step by step
  result = await db.query('MATCH (a) RETURN a');
  console.log('MATCH (a):', result.length, 'rows');
  
  result = await db.query('MATCH (a) MERGE (b) RETURN a, b');
  console.log('MATCH (a) MERGE (b):', result.length, 'rows');
  console.log('Result:', JSON.stringify(result, null, 2));
  
  db.close();
})();
"
```

```bash
cd /home/conrad/dev/leangraph && GRAPHDB_PROJECT=test-debug tsx -e "
const Database = require('better-sqlite3');
const db = new Database(':memory:');
// Test SQLite directly
console.log('NULL = 1:', db.prepare('SELECT NULL = 1').get());
console.log('json compare:', db.prepare(\"SELECT json(json_array(NULL)) = json(json_array(1))\").get());
console.log('extract null:', db.prepare(\"SELECT json_extract(json_array(NULL), '\\\$[0]')\").get());
console.log('json_type null:', db.prepare(\"SELECT json_type(json_array(NULL), '\\\$[0]')\").get());
"
```

```bash
cd /home/conrad/dev/leangraph && GRAPHDB_PROJECT=test-debug tsx -e "
const { Translator } = require('./src/translator.ts');
const { parse } = require('./src/parser.ts');
const query = \`
  WITH {exists: 42, notMissing: null} AS map
  RETURN 'exists' IN keys(map) AS a
\`;
const ast = parse(query);
console.log('Calling translate...');
const translator = new Translator({});
try {
  const result = translator.translate(ast.query);
  console.log('Result:', result);
} catch(e) {
  console.log('Error:', e.message);
  console.log('Stack:', e.stack);
}
"
```
