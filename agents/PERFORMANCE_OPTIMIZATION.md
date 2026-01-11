# LeanGraph - Performance Optimization Guide

The optimization plan is in `/OPTIMIZATION_PLAN.md`. This guide explains the workflow for implementing those optimizations.

**Phase 1 is complete.** All 11 original optimizations have been implemented. Phase 2 addresses gaps and improvements found in the Phase 1 implementations.

## Workflow

Follow this workflow exactly:

1. **Pick an optimization** from `OPTIMIZATION_PLAN.md` Phase 2 (start with P0)
2. **Run baseline benchmark** - `npm run benchmark -- -s micro -d leangraph`
3. **Implement the fix** - follow the code changes in the plan
4. **Run tests** - `npm test` - must pass!
5. **Re-benchmark** - `npm run benchmark -- -s micro -d leangraph`
6. **Compare results** - `npm run benchmark:compare <baseline> <target>`
   - Verify improvement or no regression
   - Check for unexpected side effects
   - Document performance changes
7. **Mark complete** - update checkbox in `OPTIMIZATION_PLAN.md`
8. **Commit and push`

### Example

```markdown
// In OPTIMIZATION_PLAN.md Phase 2, change:
| P0 | Expand property cache usage | 2-5x | Low | [ ] |

// To:
| P0 | Expand property cache usage | 2-5x | Low | [x] |
```

## Quick Commands

```bash
# Run micro benchmark (fast, ~1 min)
npm run benchmark -- -s micro -d leangraph

# Run quick benchmark (more accurate, ~5 min)
npm run benchmark -- -s quick -d leangraph

# Run all tests
npm test

# Show generated SQL for a query
npm run tck 'Match3|1' -- --sql

# Run specific TCK test with verbose output
npm run tck 'Return6|11' -- -v
```

## Debugging SQL Performance

### See Generated SQL

```bash
cd /home/conrad/dev/leangraph && tsx -e "
const { Translator } = require('./src/translator.ts');
const { parse } = require('./src/parser.ts');
const query = \`MATCH (u:User {id: 'u1'})-[:OWNS]->(i:Item) RETURN i\`;
const ast = parse(query);
const translator = new Translator({});
const result = translator.translate(ast.query);
console.log('SQL:', result.statements[0].sql);
console.log('Params:', result.statements[0].params);
"
```

### Run EXPLAIN QUERY PLAN

```bash
cd /home/conrad/dev/leangraph && tsx -e "
const Database = require('better-sqlite3');
const db = new Database(':memory:');
db.exec(\`
  CREATE TABLE nodes (id TEXT PRIMARY KEY, label JSON, properties JSON);
  CREATE TABLE edges (id TEXT PRIMARY KEY, type TEXT, source_id TEXT, target_id TEXT, properties JSON);
  CREATE INDEX idx_edges_type ON edges(type);
  CREATE INDEX idx_edges_source ON edges(source_id);
  CREATE INDEX idx_edges_target ON edges(target_id);
\`);
const sql = \`SELECT * FROM nodes n0
  JOIN edges e0 ON e0.source_id = n0.id
  JOIN nodes n1 ON n1.id = e0.target_id
  WHERE json_extract(n0.properties, '$.id') = ?\`;
console.log(db.prepare('EXPLAIN QUERY PLAN ' + sql).all('u1'));
"
```

### Time a Query

```bash
cd /home/conrad/dev/leangraph && tsx -e "
(async () => {
  const { LeanGraph } = require('./src/index.ts');
  const db = await LeanGraph({ dataPath: ':memory:' });
  
  // Setup
  for (let i = 0; i < 1000; i++) {
    await db.execute(\`CREATE (u:User {id: 'u\${i}', name: 'User \${i}'})\`);
  }
  
  // Time query
  const start = performance.now();
  const result = await db.query('MATCH (u:User {id: \$id}) RETURN u', { id: 'u500' });
  console.log('Time:', (performance.now() - start).toFixed(2), 'ms');
  console.log('Results:', result.length);
  
  db.close();
})();
"
```

## Key Files (Phase 2)

| File | What to Optimize |
|------|------------------|
| `src/executor.ts` | Property cache usage (27 locations), batch edge INSERTs, batch edge lookups |
| `src/db.ts` | Label index queries (getNodesByLabel), statement cache LRU |
| `src/translator.ts` | Label filter SQL, secondary CTE early termination |

## EXPLAIN Output Guide

```
SCAN TABLE nodes          -- Bad: full table scan
SEARCH nodes USING INDEX  -- Good: using index
USE TEMP B-TREE           -- OK for small sorts, bad for large
CORRELATED SUBQUERY       -- Often slow, try to flatten
```

## Common Patterns (Phase 2)

### Replacing Direct JSON.parse with Cache

```typescript
// Before (appears ~27 times in executor.ts):
const props = typeof row.properties === "string" 
  ? JSON.parse(row.properties) 
  : row.properties;

// After:
const props = this.getNodeProperties(row.id, row.properties);
```

### Batching INSERTs

```typescript
// Before (one at a time):
for (const edge of edges) {
  this.db.execute("INSERT INTO edges ... VALUES (?, ?, ?, ?, ?)", [...]);
}

// After (batched):
const BATCH_SIZE = 500;
for (let i = 0; i < edges.length; i += BATCH_SIZE) {
  const batch = edges.slice(i, i + BATCH_SIZE);
  const placeholders = batch.map(() => '(?, ?, ?, ?, ?)').join(',');
  const values = batch.flatMap(e => [e.id, e.type, e.sourceId, e.targetId, e.propsJson]);
  this.db.execute(`INSERT INTO edges ... VALUES ${placeholders}`, values);
}
```

### Using Label Index

```typescript
// Before (doesn't use index):
"SELECT * FROM nodes WHERE EXISTS (SELECT 1 FROM json_each(label) WHERE value = ?)"

// After (uses idx_nodes_primary_label):
"SELECT * FROM nodes WHERE json_extract(label, '$[0]') = ?"
```

## Verification Checklist

Before marking an optimization complete:

- [ ] `npm test` passes
- [ ] Benchmark shows improvement (or no regression)
- [ ] No new memory leaks (for caching changes)
- [ ] Code is readable and maintainable

## Benchmark Scales

| Scale | Nodes | Time | When to Use |
|-------|-------|------|-------------|
| `micro` | ~8K | ~1 min | Quick iteration |
| `quick` | ~170K | ~5 min | Verify improvement |
| `full` | ~17M | ~30 min | Final validation |

Start with `micro` for fast feedback, use `quick` to confirm gains.

## Benchmark Comparison

### How to Compare Benchmarks

```bash
# List available benchmarks
npm run benchmark:compare -- --list

# Compare two benchmarks
npm run benchmark:compare <baseline> <target>
```

### Interpreting Results

The comparison tool shows:
- Overall performance changes
- Query-specific improvements/regressions
- Memory and disk usage changes

### Benchmark Expectations

Not all optimizations show dramatic improvements in benchmarks:

- **Statement caching**: Benefits repeated identical SQL strings. Benchmarks use varying parameters, so gains appear in production workloads more than benchmarks.
- **Index changes**: Show up clearly in benchmarks since query patterns hit the indexes.
- **Batching**: Large improvements visible when benchmark includes bulk operations.

If the benchmark shows **no regression**, the optimization is still valid - it may benefit real-world usage patterns that benchmarks don't stress.

### Specialized Testing

For optimizations that don't show in standard benchmarks:

```bash
# Test UNWIND+CREATE with nodes (Phase 1)
cd /home/conrad/dev/leangraph && LEANGRAPH_PROJECT=test tsx -e "
(async () => {
  const { LeanGraph } = require('./src/index.ts');
  const db = await LeanGraph({ dataPath: ':memory:' });
  
  const start = performance.now();
  await db.query('UNWIND range(1, 1000) AS i CREATE (n:Item {num: i})');
  const end = performance.now();
  
  console.log('UNWIND+CREATE 1000 nodes:');
  console.log('  Time:', (end - start).toFixed(2), 'ms');
  
  db.close();
})();
"

# Test label index utilization (Phase 2)
cd /home/conrad/dev/leangraph && LEANGRAPH_PROJECT=test tsx -e "
(async () => {
  const { LeanGraph } = require('./src/index.ts');
  const db = await LeanGraph({ dataPath: ':memory:' });
  
  // Create many nodes
  await db.query('UNWIND range(1, 5000) AS i CREATE (n:User {id: i})');
  
  // Time label lookup
  const start = performance.now();
  for (let i = 0; i < 100; i++) {
    await db.query('MATCH (n:User) RETURN count(n)');
  }
  const end = performance.now();
  
  console.log('100 label lookups on 5K nodes:');
  console.log('  Avg:', ((end - start) / 100).toFixed(2), 'ms per query');
  
  db.close();
})();
"
