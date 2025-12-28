# openCypher TCK Compliance Plan

## Current Status

- **Compliance**: ~65% (estimated, based on Phase 1 completion)
- **TCK Source**: https://github.com/opencypher/openCypher/tree/main/tck
- **Test Runner**: `packages/server/test/tck/tck.test.ts.skip`

## Recently Completed

### DISTINCT in Aggregations (December 2024)
- `count(DISTINCT n.property)` - Implemented
- `sum(DISTINCT n.property)` - Implemented  
- `collect(DISTINCT n.property)` - Implemented using GROUP_CONCAT
- Parser updated to handle DISTINCT keyword after function open paren
- Translator updated to generate SQL DISTINCT for COUNT/SUM/AVG/MIN/MAX

### Anonymous Nodes & Label Predicates (Already Supported)
- `MATCH ()-[r:KNOWS]->() RETURN r` - Works
- `MATCH (a)-[r]->() RETURN a, r` - Works
- `MATCH (:Person)-[r]->(:Company) RETURN r` - Works

## How to Run TCK Tests

```bash
# Clone TCK data (one-time setup)
cd packages/server/test/tck
git clone --depth 1 --filter=blob:none --sparse https://github.com/opencypher/openCypher.git
cd openCypher && git sparse-checkout set tck/features

# Enable and run tests
mv tck.test.ts.skip tck.test.ts
pnpm test -- -t "openCypher TCK"
```

---

## Tier 1: High Impact, Medium Effort (~100+ tests each)

### 1. Path Expressions `p = (a)-[r]->(b)`
**Failures**: ~39 tests

```cypher
MATCH p = (a)-->(b) RETURN p
MATCH p = (a)-[r:KNOWS]->(b) RETURN p, length(p)
```

**Implementation**:
- Add `PathExpression` AST node
- Parser: handle `variable = pattern` syntax
- Translator: collect nodes/edges into path object
- Add `length(path)`, `nodes(path)`, `relationships(path)` functions

### 2. Variable-Length Paths `[*]`, `[*1..3]`
**Failures**: ~37 tests

```cypher
MATCH (a)-[*]->(b) RETURN b
MATCH (a)-[*1..3]->(b) RETURN b
MATCH (a)-[:KNOWS*2..5]->(b) RETURN b
```

**Implementation**:
- Parser: handle `*`, `*n`, `*n..m` in relationship patterns
- Translator: generate recursive CTE in SQL
- Handle direction (outgoing, incoming, both)

### 3. Multiple Labels `:A:B:C`
**Failures**: ~19 tests

```cypher
CREATE (n:A:B:C {name: 'test'})
MATCH (n:A:B) RETURN n
```

**Implementation**:
- Change `label` column from string to JSON array
- Parser: collect multiple labels
- Translator: use `json_each` for label matching
- Migration for existing data

---

## Tier 2: Medium Impact, Low-Medium Effort (~10-20 tests each)

### 4. DISTINCT in Aggregations - COMPLETED
**Status**: Implemented

```cypher
RETURN count(DISTINCT n.name)    -- Uses COUNT(DISTINCT json_extract(...))
RETURN collect(DISTINCT n.category) -- Uses GROUP_CONCAT(DISTINCT ...) + json()
RETURN sum(DISTINCT n.value)     -- Uses SUM(DISTINCT json_extract(...))
```

**Implementation Notes**:
- Parser: handles `DISTINCT` keyword after function open parenthesis
- Added `distinct` boolean field to Expression interface
- Translator: adds DISTINCT to SQL for COUNT/SUM/AVG/MIN/MAX
- COLLECT uses `json('[' || GROUP_CONCAT(DISTINCT json_quote(...)) || ']')` 
  since SQLite's json_group_array doesn't support DISTINCT

### 5. MERGE with Relationships
**Failures**: ~18 tests

```cypher
MERGE (a:Person {name: 'Alice'})-[:KNOWS]->(b:Person {name: 'Bob'})
MERGE (a)-[r:KNOWS]->(b) ON CREATE SET r.since = date()
```

**Implementation**:
- Extend executor to handle relationship patterns in MERGE
- Check existence of full pattern before creating

### 6. Anonymous Nodes in Patterns - ALREADY SUPPORTED
**Status**: Working

```cypher
MATCH ()-[r:KNOWS]->() RETURN r    -- Anonymous source and target
MATCH (a)-[r]->() RETURN a, r      -- Anonymous target only
MATCH ()-[r]->(b) RETURN b, r      -- Anonymous source only
```

**Notes**: Parser and translator already handle empty `()` patterns correctly.

### 7. Label Predicates on Anonymous Nodes - ALREADY SUPPORTED
**Status**: Working

```cypher
MATCH (:Person)-[r]->(:Company) RETURN r
MATCH (a:Person)-[:WORKS_AT]->(:Company) RETURN a
```

**Notes**: Anonymous nodes with labels work correctly.

---

## Tier 3: Lower Impact, Various Effort

### 8. Pattern Comprehensions
**Failures**: ~6 tests

```cypher
RETURN [x IN range(1, 10) WHERE x % 2 = 0 | x * 2] AS evens
RETURN [(a)-->(b) | b.name] AS names
```

**Implementation**:
- Parser: handle `[variable IN expr WHERE cond | projection]`
- Complex translator logic for subqueries

### 9. Percentile Functions
**Failures**: ~5 tests

```cypher
RETURN percentileDisc(0.9, n.score)
RETURN percentileCont(0.5, n.value)
```

**Implementation**:
- Use SQLite window functions or custom aggregation
- `percentileDisc`: discrete percentile
- `percentileCont`: continuous (interpolated) percentile

### 10. List Concatenation
**Failures**: ~5 tests

```cypher
RETURN [1, 2] + [3, 4] AS combined
RETURN n.tags + ['new'] AS allTags
```

**Implementation**:
- Handle `+` operator for arrays in translator
- Use `json_array` concatenation in SQL

---

## Projected Compliance

| Phase | Focus | Tests Passing | Compliance |
|-------|-------|---------------|------------|
| Previous | - | 814 | 62.9% |
| Phase 1 | Quick Wins (4, 6, 7) | ~840 | ~65% |
| Phase 2 | Paths (1, 2, 3) | ~935 | ~72% |
| Phase 3 | Remaining (5, 8, 9, 10) | ~1000 | ~77% |

**Note**: Phase 1 complete. Items 6 and 7 were already working (anonymous nodes).

---

## Implementation Notes

### Adding a New Feature

1. **Write failing tests first** in `translator.test.ts` or `cypherqueries.test.ts`
2. **Update parser** if new syntax needed (`parser.ts`)
3. **Update translator** for SQL generation (`translator.ts`)
4. **Update executor** if runtime logic needed (`executor.ts`)
5. **Run TCK tests** to verify improvement

### Testing Against TCK

```bash
# Run specific TCK category
pnpm test -- -t "clauses/match"
pnpm test -- -t "expressions/aggregation"

# Check a specific feature
pnpm test -- -t "Variable length"
pnpm test -- -t "DISTINCT"
```

### Common Error Patterns

| Error | Cause | Fix Location |
|-------|-------|--------------|
| `Expected LPAREN, got IDENTIFIER 'p'` | Path expressions not parsed | parser.ts |
| `Expected expression, got STAR '*'` | Variable-length paths | parser.ts |
| `Expected DOT, got COLON ':'` | Multiple labels | parser.ts |
| ~~`Expected expression, got KEYWORD 'DISTINCT'`~~ | ~~DISTINCT in functions~~ | ~~Fixed~~ |
| `MERGE with relationship pattern must be executed` | MERGE relationships | executor.ts |

### Known Limitations

- **Implicit GROUP BY**: Queries mixing aggregates with non-aggregated columns
  (e.g., `RETURN n.department, count(DISTINCT n.skill)`) require relationship-based
  grouping rather than relying on implicit GROUP BY inference.

---

## References

- [openCypher Specification](https://opencypher.org/)
- [openCypher TCK](https://github.com/opencypher/openCypher/tree/main/tck)
- [Cypher Query Language Reference](https://neo4j.com/docs/cypher-manual/current/)
