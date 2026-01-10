# Performance Optimization Workflow

Iterative TDD-style workflow for optimizing LeanGraph performance. Follow this workflow to systematically identify bottlenecks and improve query performance while keeping all tests green.

## Quick Start

```bash
# 1. Run baseline benchmark
npm run benchmark -- -N 30K -d leangraph --name baseline

# 2. Analyze results
npm run benchmark:analyze baseline

# 3. Investigate and optimize (see sections below)

# 4. Run tests - must pass!
npm test

# 5. Re-benchmark
npm run benchmark -- -N 30K -d leangraph --name iter-1

# 6. Compare
npm run benchmark:compare baseline iter-1
# or with analysis
npm run benchmark:analyze baseline iter-1

# 7. Log results
# Update benchmark/OPTIMIZATION_LOG.md

# 8. Repeat from step 2 until diminishing returns (<5% improvement)
```

## Workflow Diagram

```
BASELINE ─────────────────────────────────────────────────────────────
    │  npm run benchmark -- -N 30K -d leangraph --name baseline
    ▼
ANALYZE ──────────────────────────────────────────────────────────────
    │  npm run benchmark:analyze baseline
    │  → Identify top 3 slowest queries
    │  → Note investigation hints
    ▼
INVESTIGATE ──────────────────────────────────────────────────────────
    │  For the slowest query:
    │  1. Get the generated SQL
    │  2. Run EXPLAIN on it
    │  3. Identify bottleneck (scan, join, sort?)
    ▼
OPTIMIZE ─────────────────────────────────────────────────────────────
    │  Make targeted code changes
    │  Files: src/translator.ts, src/executor.ts, src/db.ts
    ▼
VERIFY ───────────────────────────────────────────────────────────────
    │  npm test  ← MUST PASS
    ▼
MEASURE ──────────────────────────────────────────────────────────────
    │  npm run benchmark -- -N 30K -d leangraph --name iter-N
    │  npm run benchmark:analyze baseline iter-N
    ▼
    │  Significant gains (>5%)?
    │     YES → Log to OPTIMIZATION_LOG.md → Loop back to ANALYZE
    │     NO  → DONE (diminishing returns)
```

## Commands Reference

### Benchmarking

```bash
# Run benchmark with custom node count
npm run benchmark -- -N 30K -d leangraph --name <name>

# List available benchmark results
npm run benchmark:analyze --list

# Analyze single run (shows slowest queries + hints)
npm run benchmark:analyze <name>

# Compare two runs (shows improvements/regressions)
npm run benchmark:analyze <baseline> <target>

# Detailed comparison with full table
npm run benchmark:compare <baseline> <target>
```

### Debugging Queries

```bash
# Show generated SQL for a benchmark query
# (Note: benchmark queries aren't in TCK, use debug script below)

# Debug a specific Cypher query
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

# Run EXPLAIN on generated SQL
cd /home/conrad/dev/leangraph && tsx -e "
const Database = require('better-sqlite3');
const db = new Database(':memory:');
// Create schema
db.exec(\`
  CREATE TABLE nodes (id TEXT PRIMARY KEY, label JSON, properties JSON);
  CREATE TABLE edges (id TEXT PRIMARY KEY, type TEXT, source_id TEXT, target_id TEXT, properties JSON);
  CREATE INDEX idx_edges_type ON edges(type);
  CREATE INDEX idx_edges_source ON edges(source_id);
  CREATE INDEX idx_edges_target ON edges(target_id);
\`);
// Your SQL here
const sql = \`SELECT * FROM nodes WHERE json_extract(properties, '$.id') = ?\`;
console.log('EXPLAIN:');
console.log(db.prepare('EXPLAIN QUERY PLAN ' + sql).all('u1'));
"
```

### Full Query Execution Debug

```bash
cd /home/conrad/dev/leangraph && tsx -e "
(async () => {
  const { LeanGraph } = require('./src/index.ts');
  const db = await LeanGraph({ dataPath: ':memory:' });
  
  // Setup test data
  await db.execute('CREATE (u:User {id: \"u1\", name: \"Alice\"})');
  await db.execute('CREATE (i:Item {id: \"i1\", category: \"books\"})');
  await db.execute('MATCH (u:User {id: \"u1\"}), (i:Item {id: \"i1\"}) CREATE (u)-[:OWNS]->(i)');
  
  // Time the query
  const start = performance.now();
  const result = await db.query('MATCH (u:User {id: \$id})-[:OWNS]->(i:Item) RETURN i', { id: 'u1' });
  console.log('Time:', (performance.now() - start).toFixed(2), 'ms');
  console.log('Result:', result);
  
  db.close();
})();
"
```

## Common Optimization Patterns

### 1. Property Filter Without Index

**Symptom:** Slow `MATCH (n:Label {prop: $val})` queries  
**Cause:** Full table scan with JSON extraction for every row  
**Detection:** EXPLAIN shows `SCAN TABLE nodes`

**Current Schema:**
```sql
CREATE TABLE nodes (id TEXT PRIMARY KEY, label JSON, properties JSON);
-- No index on properties!
```

**Fix Options:**

A. **Computed column + index** (for hot properties like `id`):
```sql
ALTER TABLE nodes ADD COLUMN prop_id TEXT 
  GENERATED ALWAYS AS (json_extract(properties, '$.id'));
CREATE INDEX idx_nodes_prop_id ON nodes(prop_id);
```

B. **Separate property table** (for general indexing):
```sql
CREATE TABLE node_properties (
  node_id TEXT, key TEXT, value TEXT,
  PRIMARY KEY (node_id, key)
);
CREATE INDEX idx_node_props_value ON node_properties(key, value);
```

C. **Translator optimization** - filter by label first:
```sql
-- Before: scans all nodes
SELECT * FROM nodes WHERE json_extract(properties, '$.id') = ?

-- After: filter by label first (if label indexed)
SELECT * FROM nodes 
WHERE json_extract(label, '$[0]') = 'User'
  AND json_extract(properties, '$.id') = ?
```

### 2. Variable-Length Path Explosion

**Symptom:** Slow `*1..N` traversals, especially depth 3+  
**Cause:** Exponential path expansion in recursive CTE  
**Detection:** Query time grows exponentially with depth

**Current Implementation:**
```sql
WITH RECURSIVE paths AS (
  SELECT ... FROM edges WHERE source_id = ?
  UNION ALL
  SELECT ... FROM edges JOIN paths ON ...
)
SELECT DISTINCT * FROM paths LIMIT 50;
```

**Fix Options:**

A. **LIMIT pushdown** - stop recursion early:
```sql
WITH RECURSIVE paths AS (
  SELECT ..., 1 as depth, 1 as row_num FROM edges WHERE source_id = ?
  UNION ALL
  SELECT ..., depth + 1, row_num + 1 
  FROM edges JOIN paths 
  WHERE depth < 3 AND row_num < 50  -- Early termination
)
SELECT DISTINCT * FROM paths LIMIT 50;
```

B. **Bidirectional search** - for point-to-point paths

C. **Path deduplication** - track visited nodes in CTE

### 3. Aggregation Over Large Sets

**Symptom:** Slow `COUNT`, `AVG`, `GROUP BY` queries  
**Cause:** Full table scan + in-memory aggregation  
**Detection:** EXPLAIN shows `SCAN` + `USE TEMP B-TREE`

**Fix Options:**

A. **Partial aggregation** in subquery
B. **Covering index** for aggregated columns
C. **Materialized view** for common aggregates (advanced)

### 4. Inefficient Join Order

**Symptom:** Slow multi-pattern matches  
**Cause:** Starting from large table instead of filtered one  
**Detection:** EXPLAIN shows large intermediate results

**Fix:**
Translator should emit joins in selectivity order:
1. Most selective filter first (e.g., `{id: $val}`)
2. Then edges from that node
3. Then connected nodes

## Key Source Files

| File | Purpose | Optimization Targets |
|------|---------|---------------------|
| `src/db.ts` | Schema, indexes | Add indexes, computed columns |
| `src/translator.ts` | Cypher → SQL | Query structure, join order |
| `src/executor.ts` | Query execution | Caching, batching |
| `src/parser.ts` | Cypher parsing | Usually not a bottleneck |

### Translator Hot Paths

- `translateMatch()` - Pattern matching → JOINs
- `translateVarLengthPath()` - `*1..N` → Recursive CTE
- `translateWhere()` - Conditions → WHERE clause
- `translateReturn()` - Projections → SELECT

### Executor Hot Paths

- `executeQuery()` - Main entry point
- `executePhase()` - Per-clause execution
- `formatResult()` - Result formatting

## Stop Conditions

Stop optimizing when any of these apply:

1. **Diminishing returns** - Last iteration showed <5% average improvement
2. **Target met** - All queries under acceptable threshold (e.g., p50 < 50ms)
3. **Schema changes required** - Would need breaking changes to data model
4. **Complexity tradeoff** - Further optimization adds significant code complexity

## Logging Results

After each optimization iteration, update `benchmark/OPTIMIZATION_LOG.md`:

```markdown
## Iteration N - [Date]

### Target
- Query: `related_items_depth3`
- Baseline p50: 142ms

### Changes
- Modified `translateVarLengthPath()` to add LIMIT pushdown
- File: src/translator.ts lines 234-267

### Results
- New p50: 89ms (-37%)
- All tests pass

### Notes
- Could go further with bidirectional search
- Decided to stop here, good enough for now
```
