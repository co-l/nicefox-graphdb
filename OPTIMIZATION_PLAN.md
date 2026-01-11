# LeanGraph Performance Optimization Plan

This document outlines actionable performance optimizations for LeanGraph, prioritized by impact and effort.

## Phase 2: Current Optimizations

These optimizations address gaps and improvements identified in Phase 1 implementations.

| Priority | Optimization | Speedup | Effort | Status |
|----------|--------------|---------|--------|--------|
| P0 | Expand property cache usage | 2-5x | Low | [x] |
| P0 | Fix label index utilization | 5-20x | Low | [x] |
| P1 | Batch edge INSERTs | 5-10x | Medium | [x] |
| P1 | Statement cache LRU eviction | ~20% | Low | [x] |
| P2 | Secondary CTE early termination | 10-100x | Medium | [ ] |
| P2 | Expand batch edge lookups | 2-5x | Medium | [ ] |

---

## P0: Critical (Do First)

### 1. Expand Property Cache Usage

**File:** `src/executor.ts`  
**Effort:** Low (search & replace pattern)  
**Impact:** 2-5x for property-heavy queries

**Problem:** The `getNodeProperties()` cache exists but is only used in 2 of 27+ locations. Most code still does direct `JSON.parse(row.properties)`.

**Current pattern (appears ~27 times):**
```typescript
const props = typeof row.properties === "string" 
  ? JSON.parse(row.properties) 
  : row.properties;
```

**Solution:**
1. Search for all `JSON.parse(row.properties)` and `JSON.parse(.*properties)` patterns
2. Replace with `this.getNodeProperties(row.id, row.properties)`
3. Add `getEdgeProperties(edgeId, propsJson)` method for edge property caching
4. Update edge property parsing to use the new cache

**Add this method:**
```typescript
private edgePropertyCache = new Map<string, Record<string, unknown>>();

private getEdgeProperties(edgeId: string, propsJson: string | object): Record<string, unknown> {
  let props = this.edgePropertyCache.get(edgeId);
  if (!props) {
    props = typeof propsJson === "string" ? JSON.parse(propsJson) : propsJson;
    if (props && typeof props === "object" && !Array.isArray(props)) {
      this.edgePropertyCache.set(edgeId, props);
    } else {
      props = {};
    }
  }
  return props || {};
}

// In execute(), also clear edge cache:
execute(cypher: string, params: Record<string, unknown> = {}): QueryResponse {
  this.propertyCache.clear();
  this.edgePropertyCache.clear();
  // ...
}
```

---

### 2. Fix Label Index Utilization

**File:** `src/db.ts`, `src/translator.ts`  
**Effort:** Low  
**Impact:** 5-20x faster `MATCH (n:Label)` queries

**Problem:** The `idx_nodes_primary_label` index exists on `json_extract(label, '$[0]')`, but queries don't use it.

**Current `getNodesByLabel()` (doesn't use index):**
```typescript
getNodesByLabel(label: string): Node[] {
  const result = this.execute(
    "SELECT * FROM nodes WHERE EXISTS (SELECT 1 FROM json_each(label) WHERE value = ?)",
    [label]
  );
  // ...
}
```

**Solution A - Optimize for primary label (common case):**
```typescript
getNodesByLabel(label: string): Node[] {
  // Use index for primary label, fallback for secondary labels
  const result = this.execute(
    `SELECT * FROM nodes WHERE json_extract(label, '$[0]') = ? 
     OR EXISTS (SELECT 1 FROM json_each(label) WHERE value = ? AND json_extract(label, '$[0]') != ?)`,
    [label, label, label]
  );
  // ...
}
```

**Solution B - Primary label only (simpler, covers 95% of cases):**
```typescript
getNodesByLabel(label: string): Node[] {
  const result = this.execute(
    "SELECT * FROM nodes WHERE json_extract(label, '$[0]') = ?",
    [label]
  );
  // ...
}
```

**Also update translator.ts:** Ensure label conditions in MATCH clauses emit `json_extract(label, '$[0]') = ?` instead of `EXISTS(SELECT 1 FROM json_each...)`.

---

## P1: High Priority

### 3. Batch Edge INSERTs

**File:** `src/executor.ts` - `tryUnwindCreateExecution()`  
**Effort:** Medium  
**Impact:** 5-10x faster bulk relationship creation

**Problem:** Node INSERTs are batched (500 per statement), but edge INSERTs are still individual.

**Current (edges inserted one at a time):**
```typescript
this.db.execute(
  "INSERT INTO edges (id, type, source_id, target_id, properties) VALUES (?, ?, ?, ?, ?)",
  [edgeId, type, sourceId, targetId, propsJson]
);
```

**Solution:** Collect edge inserts and batch them like nodes:
```typescript
// Collect edge inserts
const edgeInserts: Array<{id: string, type: string, sourceId: string, targetId: string, propsJson: string}> = [];

// ... in the loop:
edgeInserts.push({ id: edgeId, type, sourceId, targetId, propsJson });

// After collecting all edges:
const BATCH_SIZE = 500;
for (let i = 0; i < edgeInserts.length; i += BATCH_SIZE) {
  const batch = edgeInserts.slice(i, i + BATCH_SIZE);
  const placeholders = batch.map(() => '(?, ?, ?, ?, ?)').join(',');
  const values = batch.flatMap(e => [e.id, e.type, e.sourceId, e.targetId, e.propsJson]);
  
  this.db.execute(
    `INSERT INTO edges (id, type, source_id, target_id, properties) VALUES ${placeholders}`,
    values
  );
}
```

**Challenge:** Edges often depend on nodes created in the same batch. Solution: Collect all node inserts first, execute them, then collect and execute all edge inserts.

---

### 4. Statement Cache LRU Eviction

**File:** `src/db.ts` - `getCachedStatement()`  
**Effort:** Low  
**Impact:** ~20% better cache hit rate

**Problem:** Current FIFO eviction removes oldest-inserted statements, even if they're frequently used.

**Current (FIFO):**
```typescript
private getCachedStatement(sql: string): Database.Statement {
  let stmt = this.stmtCache.get(sql);
  if (!stmt) {
    stmt = this.db.prepare(sql);
    if (this.stmtCache.size >= this.STMT_CACHE_MAX) {
      const firstKey = this.stmtCache.keys().next().value;
      if (firstKey) this.stmtCache.delete(firstKey);
    }
    this.stmtCache.set(sql, stmt);
  }
  return stmt;
}
```

**Solution (LRU):** Move accessed entries to end of Map:
```typescript
private getCachedStatement(sql: string): Database.Statement {
  let stmt = this.stmtCache.get(sql);
  if (stmt) {
    // Move to end for LRU (delete and re-add)
    this.stmtCache.delete(sql);
    this.stmtCache.set(sql, stmt);
    return stmt;
  }
  
  // Not cached - prepare and add
  stmt = this.db.prepare(sql);
  if (this.stmtCache.size >= this.STMT_CACHE_MAX) {
    const firstKey = this.stmtCache.keys().next().value;
    if (firstKey) this.stmtCache.delete(firstKey);
  }
  this.stmtCache.set(sql, stmt);
  return stmt;
}
```

---

## P2: Medium Priority

### 5. Secondary CTE Early Termination

**File:** `src/translator.ts` - `translateVariableLengthPath()`  
**Effort:** Medium  
**Impact:** 10-100x for queries with multiple variable-length patterns

**Problem:** Only the primary CTE has early termination (`row_num` tracking). Secondary CTEs in the same query don't.

**Current:** Primary CTE includes `row_num < earlyTerminationLimit`, but secondary CTE (e.g., `path_1`) lacks this.

**Solution:** Apply the same early termination pattern to all variable-length CTEs:
1. Pass the `limitValue` to all CTE generation calls
2. Add `row_num` column and termination condition to secondary CTEs
3. Ensure depth tracking is consistent across all CTEs

**Implementation steps:**
1. Find where secondary CTEs are generated (around lines 3925-4090)
2. Add the `row_num` column to base case: `ROW_NUMBER() OVER () as row_num`
3. Add `p.row_num + 1` to recursive case
4. Add `AND p.row_num < ?` condition with `earlyTerminationLimit` parameter

---

### 6. Expand Batch Edge Lookups

**File:** `src/executor.ts`  
**Effort:** Medium  
**Impact:** 2-5x for edge-heavy queries

**Problem:** `batchGetEdgeInfo()` exists but is only used in 2 places. 21 other locations still do individual `SELECT * FROM edges WHERE id = ?` queries.

**Solution:** Identify hot paths with multiple sequential edge lookups and batch them:

1. **In expression evaluation:** When evaluating path expressions, collect all edge IDs first, then batch fetch
2. **In type checking:** Batch edge type lookups when checking multiple relationships
3. **In property extraction:** Use the edge property cache (from optimization #1) to avoid refetching

**Pattern to replace:**
```typescript
for (const edgeId of edgeIds) {
  const result = this.db.execute("SELECT * FROM edges WHERE id = ?", [edgeId]);
  // use result
}
```

**Replace with:**
```typescript
const edgeMap = this.batchGetEdgeInfo(edgeIds);
for (const edgeId of edgeIds) {
  const edge = edgeMap.get(edgeId);
  // use edge
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
npm run benchmark:compare <baseline> <target>
```

---

## Notes

- Always run full test suite after changes: `npm test`
- Use TCK tool for regression testing: `npm run tck '<pattern>'`
- Document performance gains in commit messages
- Mark items complete with `[x]` as they're finished

---

---

# Archive: Phase 1 (Completed)

All Phase 1 optimizations have been implemented. This section is preserved for reference.

## Phase 1 Summary

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

<details>
<summary>Phase 1 Implementation Details (click to expand)</summary>

### P0: SQLite Performance Pragmas
**File:** `src/db.ts:554-564`

All 6 pragmas implemented:
- `journal_mode = WAL`
- `foreign_keys = ON`
- `synchronous = NORMAL`
- `cache_size = -64000` (64MB)
- `temp_store = MEMORY`
- `mmap_size = 268435456` (256MB)

### P0: Prepared Statement Cache
**File:** `src/db.ts:551-591`

- Map-based cache with FIFO eviction
- Max 100 statements
- Cleared on `close()`

### P0: Composite Edge Indexes
**File:** `src/db.ts:67-68`

Added:
- `idx_edges_source_type ON edges(source_id, type)`
- `idx_edges_target_type ON edges(target_id, type)`

### P1: Label Index
**File:** `src/db.ts:69`

Functional index on primary label:
- `idx_nodes_primary_label ON nodes(json_extract(label, '$[0]'))`

### P1: Variable-Length Path Early Termination
**File:** `src/translator.ts:3354-3357`

- `row_num` column for tracking
- `earlyTerminationLimit = min(limit * 10, 10000)`
- Applied to primary CTE only

### P1: Batch INSERTs for UNWIND+CREATE
**File:** `src/executor.ts:4507-4518`

- Nodes batched at 500 per INSERT
- Multi-row VALUES syntax

### P2: JSON Property Parse Caching
**File:** `src/executor.ts:230-251`

- `propertyCache` Map
- `getNodeProperties()` method
- Cleared at query start

### P2: Reduce Context Cloning
**File:** `src/executor.ts:126-133`

- `cloneRows` parameter added
- `isReadOnlyClause()` skips cloning for MATCH, OPTIONAL_MATCH, RETURN

### P2: Single-Pass Query Classifier
**File:** `src/executor.ts:163-683`

- `QueryPattern` type with 10 patterns
- `classifyQuery()` single-pass flag collection
- Switch-based dispatch

### P3: Tokenizer String Allocation
**File:** `src/parser.ts`

All methods use `slice()`:
- `readIdentifier()` (lines 855-862)
- `readString()` (lines 654-717)
- `readNumber()` (lines 719-847)
- `readBacktickIdentifier()` (lines 864-902)

### P3: Batch Edge Lookups in Paths
**File:** `src/executor.ts:278-298`

- `batchGetEdgeInfo()` method
- Returns `Map<string, EdgeInfo>`
- Used in 2 path-processing locations

</details>
