# LeanGraph Performance Optimization Plan

This document outlines actionable performance optimizations for LeanGraph, prioritized by impact and effort.

## Quick Reference

| Priority | Optimization | Speedup | Effort | Status |
|----------|--------------|---------|--------|--------|
| P0 | SQLite performance pragmas | 2-3x | Low | [x] |
| P0 | Prepared statement cache | 2-5x | Low | [x] |
| P0 | Composite edge indexes | 5-20x | Low | [x] |
| P1 | Label index | 5-20x | Low | [x] |
| P1 | Variable-length path early termination | 10-100x | Medium | [x] |
| P1 | Batch INSERTs for UNWIND+CREATE | 10-100x | Medium | [x] |
| P2 | JSON property parse caching | 2-5x | Low | [x] |
| P2 | Reduce context cloning | 2-3x | Medium | [x] |
| P2 | Single-pass query classifier | 5-10x | Medium | [x] |
| P3 | Tokenizer string allocation | 30-50% | Low | [x] |
| P3 | Batch edge lookups in paths | 5-20x | Medium | [x] |

---

## P0: Critical (Do First)

### 1. SQLite Performance Pragmas

**File:** `src/db.ts:549-555`  
**Effort:** 5 lines of code  
**Impact:** 2-3x faster across all operations

**Current:**
```typescript
constructor(path: string = ":memory:") {
  this.db = new Database(path);
  this.db.pragma("journal_mode = WAL");
  this.db.pragma("foreign_keys = ON");
}
```

**Change to:**
```typescript
constructor(path: string = ":memory:") {
  this.db = new Database(path);
  this.db.pragma("journal_mode = WAL");
  this.db.pragma("foreign_keys = ON");
  this.db.pragma("synchronous = NORMAL");     // Safe with WAL, faster writes
  this.db.pragma("cache_size = -64000");      // 64MB cache (default is 2MB)
  this.db.pragma("temp_store = MEMORY");      // Temp tables in RAM
  this.db.pragma("mmap_size = 268435456");    // 256MB memory-mapped I/O
}
```

**Why:** Default SQLite settings are conservative. These are safe optimizations that dramatically improve I/O performance.

---

### 2. Prepared Statement Cache

**File:** `src/db.ts:570-592`  
**Effort:** ~30 lines  
**Impact:** 2-5x faster for repeated queries

**Problem:** Every `execute()` call runs `this.db.prepare(sql)` which parses and compiles the SQL statement.

**Current:**
```typescript
execute(sql: string, params: unknown[] = []): QueryResult {
  // ...
  const stmt = this.db.prepare(sql);  // Called every time!
  // ...
}
```

**Solution:** Add LRU statement cache:
```typescript
export class GraphDatabase {
  private db: Database.Database;
  private stmtCache: Map<string, Database.Statement> = new Map();
  private readonly STMT_CACHE_MAX = 100;

  private getCachedStatement(sql: string): Database.Statement {
    let stmt = this.stmtCache.get(sql);
    if (!stmt) {
      stmt = this.db.prepare(sql);
      if (this.stmtCache.size >= this.STMT_CACHE_MAX) {
        // Evict oldest entry (FIFO)
        const firstKey = this.stmtCache.keys().next().value;
        if (firstKey) this.stmtCache.delete(firstKey);
      }
      this.stmtCache.set(sql, stmt);
    }
    return stmt;
  }

  execute(sql: string, params: unknown[] = []): QueryResult {
    // ...
    const stmt = this.getCachedStatement(sql);
    // ...
  }
}
```

**Note:** Must clear cache on `close()` and handle dynamic SQL carefully.

---

### 3. Composite Edge Indexes

**File:** `src/db.ts:64-66`  
**Effort:** 2 lines of SQL  
**Impact:** 5-20x faster edge traversals

**Current indexes:**
```sql
CREATE INDEX IF NOT EXISTS idx_edges_type ON edges(type);
CREATE INDEX IF NOT EXISTS idx_edges_source ON edges(source_id);
CREATE INDEX IF NOT EXISTS idx_edges_target ON edges(target_id);
```

**Add these composite indexes:**
```sql
CREATE INDEX IF NOT EXISTS idx_edges_source_type ON edges(source_id, type);
CREATE INDEX IF NOT EXISTS idx_edges_target_type ON edges(target_id, type);
```

**Why:** Pattern `(a)-[:TYPE]->(b)` filters by both `source_id` AND `type`. Without composite index, SQLite uses one index then scans for the other condition.

---

## P1: High Priority

### 4. Label Index

**File:** `src/db.ts:47-67`  
**Effort:** Low  
**Impact:** 5-20x faster `MATCH (n:Label)` queries

**Problem:** Labels stored as JSON array `["User"]`. Every label filter does:
```sql
EXISTS(SELECT 1 FROM json_each(label) WHERE value = ?)
```
This requires full table scan with JSON parsing for every row.

**Solution A - Functional index (simple):**
```sql
CREATE INDEX IF NOT EXISTS idx_nodes_primary_label 
  ON nodes(json_extract(label, '$[0]'));
```
Only indexes first label. Good for single-label nodes (common case).

**Solution B - Separate label table (comprehensive):**
```sql
CREATE TABLE IF NOT EXISTS node_labels (
  node_id TEXT NOT NULL REFERENCES nodes(id) ON DELETE CASCADE,
  label TEXT NOT NULL,
  PRIMARY KEY (node_id, label)
);
CREATE INDEX IF NOT EXISTS idx_node_labels_label ON node_labels(label);
```
Requires updating `insertNode()`, `deleteNode()`, and translator to use this table.

**Recommendation:** Start with Solution A, migrate to B if multi-label queries are common.

---

### 5. Variable-Length Path Early Termination

**File:** `src/translator.ts` - `translateVariableLengthPath()`  
**Effort:** Medium  
**Impact:** 10-100x for path queries with LIMIT

**Problem:** Current recursive CTE expands ALL paths before applying LIMIT:
```sql
WITH RECURSIVE paths AS (
  SELECT ... FROM edges WHERE source_id = ?
  UNION ALL
  SELECT ... FROM edges JOIN paths ON ...
)
SELECT DISTINCT * FROM paths LIMIT 50;  -- LIMIT applied AFTER full expansion
```

For dense graphs, this generates millions of paths only to return 50.

**Solution:** Push LIMIT into recursion:
```sql
WITH RECURSIVE paths AS (
  SELECT ..., 1 as depth, ROW_NUMBER() OVER () as rn 
  FROM edges WHERE source_id = ?
  UNION ALL
  SELECT ..., depth + 1, rn + 1
  FROM edges JOIN paths 
  WHERE depth < @maxDepth
    AND rn < @limit * 10  -- Early termination heuristic
)
SELECT DISTINCT * FROM paths LIMIT 50;
```

**Implementation steps:**
1. Detect LIMIT clause following variable-length MATCH
2. Pass limit value to `translateVariableLengthPath()`
3. Add row counting and early termination to recursive CTE
4. Test with TCK and benchmark

---

### 6. Batch INSERTs for UNWIND+CREATE

**File:** `src/executor.ts:4173-4200`  
**Effort:** Medium  
**Impact:** 10-100x faster bulk inserts

**Problem:** `UNWIND range(1,10000) AS i CREATE (n {num: i})` executes 10,000 individual INSERT statements.

**Solution:** Collect inserts and batch them:
```typescript
// Instead of:
for (const combination of combinations) {
  this.db.execute("INSERT INTO nodes ...", [id, label, props]);
}

// Do:
const values: unknown[][] = [];
for (const combination of combinations) {
  values.push([id, labelJson, JSON.stringify(props)]);
}
if (values.length > 0) {
  const placeholders = values.map(() => '(?, ?, ?)').join(',');
  this.db.execute(
    `INSERT INTO nodes (id, label, properties) VALUES ${placeholders}`,
    values.flat()
  );
}
```

**Batch size:** Cap at 500-1000 rows per statement (SQLite limit is ~32766 params).

---

## P2: Medium Priority

### 7. JSON Property Parse Caching

**File:** `src/executor.ts`  
**Effort:** Low  
**Impact:** 2-5x for property-heavy queries

**Problem:** Node properties parsed from JSON string on every access:
```typescript
const nodeProps = typeof row.properties === "string" 
  ? JSON.parse(row.properties) 
  : row.properties;
```
Same node may be parsed 10+ times in a single query execution.

**Solution:** Cache parsed properties by node ID:
```typescript
export class CypherExecutor {
  private propertyCache = new Map<string, Record<string, unknown>>();
  
  private getNodeProperties(nodeId: string, propsJson: string): Record<string, unknown> {
    let props = this.propertyCache.get(nodeId);
    if (!props) {
      props = JSON.parse(propsJson);
      this.propertyCache.set(nodeId, props);
    }
    return props;
  }
  
  // Clear cache at start of each query execution
  execute(cypher: string, params?: Record<string, unknown>): QueryResult {
    this.propertyCache.clear();
    // ...
  }
}
```

---

### 8. Reduce Context Cloning

**File:** `src/executor.ts` - `cloneContext()`  
**Effort:** Medium  
**Impact:** 2-3x for row-heavy queries

**Problem:** `cloneContext()` called at start of nearly every clause:
```typescript
function cloneContext(ctx: PhaseContext): PhaseContext {
  return {
    nodeIds: new Map(ctx.nodeIds),
    edgeIds: new Map(ctx.edgeIds),
    values: new Map(ctx.values),
    rows: ctx.rows.map(row => new Map(row)),  // O(rows * cols)
  };
}
```

For 1000 rows x 10 columns x 5 clauses = 50,000 Map entries cloned.

**Solutions:**
1. **Copy-on-write:** Only clone when actually modifying
2. **Immutable.js:** Use persistent data structures
3. **Scope tracking:** Track which variables change per clause, clone only those

**Recommended approach:** Identify clauses that don't modify context (pure reads) and skip cloning for those.

---

### 9. Single-Pass Query Classifier

**File:** `src/executor.ts:185-311`  
**Effort:** Medium  
**Impact:** 5-10x faster query dispatch

**Problem:** 9+ `try*Execution()` methods called sequentially, each scanning all clauses:
```typescript
const phasedResult = this.tryPhasedExecution(query, params);
if (phasedResult !== null) return phasedResult;

const unwindCreateResult = this.tryUnwindCreateExecution(query, params);
if (unwindCreateResult !== null) return unwindCreateResult;
// ... 7 more checks
```

**Solution:** Single-pass classifier:
```typescript
private classifyQuery(query: Query): QueryPattern {
  const flags = {
    hasMatch: false, hasMerge: false, hasUnwind: false,
    hasCreate: false, hasSet: false, hasDelete: false,
    hasReturn: false, hasWith: false
  };
  
  for (const clause of query.clauses) {
    switch (clause.type) {
      case "MATCH": flags.hasMatch = true; break;
      case "MERGE": flags.hasMerge = true; break;
      // ...
    }
  }
  
  // Return pattern type based on flags
  if (flags.hasUnwind && flags.hasCreate) return "UNWIND_CREATE";
  if (flags.hasMatch && flags.hasMerge) return "MATCH_MERGE";
  // ...
}

execute(cypher: string, params?: Record<string, unknown>): QueryResult {
  const query = parse(cypher);
  const pattern = this.classifyQuery(query);
  
  switch (pattern) {
    case "UNWIND_CREATE": return this.executeUnwindCreate(query, params);
    case "MATCH_MERGE": return this.executeMatchMerge(query, params);
    // ...
  }
}
```

---

## P3: Lower Priority

### 10. Tokenizer String Allocation

**File:** `src/parser.ts:833-838`  
**Effort:** Low  
**Impact:** 30-50% faster tokenization

**Problem:** O(n^2) string concatenation:
```typescript
private readIdentifier(): string {
  let value = "";
  while (this.pos < this.input.length && this.isIdentifierChar(this.input[this.pos])) {
    value += this.input[this.pos];  // New string each iteration
    this.pos++;
  }
  return value;
}
```

**Solution:** Use slice():
```typescript
private readIdentifier(): string {
  const startPos = this.pos;
  while (this.pos < this.input.length && this.isIdentifierChar(this.input[this.pos])) {
    this.pos++;
    this.column++;
  }
  return this.input.slice(startPos, this.pos);
}
```

**Apply to:** `readIdentifier()`, `readString()`, `readNumber()`, `readBacktickIdentifier()`

---

### 11. Batch Edge Lookups in Paths

**File:** `src/executor.ts:2840-2881`  
**Effort:** Medium  
**Impact:** 5-20x for path queries

**Problem:** Individual edge lookup per relationship in path:
```typescript
for (const rel of relList) {
  const edgeResult = this.db.execute(
    "SELECT * FROM edges WHERE id = ?",
    [edgeId]
  );
}
```

**Solution:** Batch lookup:
```typescript
const edgeIds = relList.map(r => extractEdgeId(r)).filter(Boolean);
if (edgeIds.length > 0) {
  const placeholders = edgeIds.map(() => '?').join(',');
  const edgeResult = this.db.execute(
    `SELECT * FROM edges WHERE id IN (${placeholders})`,
    edgeIds
  );
  const edgeMap = new Map(edgeResult.rows.map(r => [r.id, r]));
  // Use edgeMap for lookups
}
```

---

## Benchmarking Workflow

After each optimization:

```bash
# 1. Run tests (must pass!)
npm test

# 2. Run micro benchmark
npm run benchmark -- -s micro -d leangraph

# 3. For significant changes, run quick benchmark
npm run benchmark -- -s quick -d leangraph

# 4. Compare results
npm run benchmark:report
```

---

## Implementation Order

1. **Week 1:** P0 items (pragmas, statement cache, composite indexes)
2. **Week 2:** P1 items (label index, batch inserts)
3. **Week 3:** P1 continued (variable-length path optimization)
4. **Week 4:** P2 items (property cache, context cloning, query classifier)
5. **Ongoing:** P3 items as time permits

---

## Notes

- Always run full test suite after changes: `npm test`
- Use TCK tool for regression testing: `npm run tck '<pattern>'`
- Document performance gains in commit messages
- Update this plan as items are completed
