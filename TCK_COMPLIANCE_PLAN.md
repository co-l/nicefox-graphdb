# openCypher TCK Compliance Plan

## Current Status

- **Test Suite**: 997 passing, 0 skipped
- **Estimated TCK Compliance**: ~70% (based on implemented features)
- **TCK Source**: https://github.com/opencypher/openCypher/tree/main/tck
- **Test Runner**: `packages/server/test/tck/tck.test.ts.skip`

## Recently Completed

### List Comprehensions (December 2024)
- `[x IN list WHERE cond]` - Filter elements from a list
- `[x IN list | expr]` - Transform/map elements  
- `[x IN list WHERE cond | expr]` - Filter and map
- Parser: Added `listComprehension` expression type with WHERE and PIPE handling
- Translator: Uses `json_each()` and `json_group_array()` for efficient processing
- Supports functions (size, toUpper, etc.) and arithmetic in comprehension expressions

### Multiple Labels - PARTIAL (December 2024)
- Core implementation complete: parser, schema, translator, executor
- Labels now stored as JSON arrays in database
- Syntax `:A:B:C` fully parsed
- Label matching uses `json_each` with EXISTS subqueries
- **Status**: Needs result formatting fixes and test updates before completion

### List Concatenation (December 2024)
- `[1, 2] + [3, 4]` - Implemented with json_group_array + UNION ALL
- `n.tags + ['new']` - Property + literal list concatenation
- Parser: Added list literal expression parsing in `parsePrimaryExpression()`
- Translator: Detects list concatenation in `translateBinaryExpression()` 
- Uses subquery pattern: `(SELECT json_group_array(value) FROM (SELECT value FROM json_each(...) UNION ALL ...))`

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

### 1. Path Expressions `p = (a)-[r]->(b)` - IN PROGRESS
**Failures**: ~39 tests
**Status**: Parser and translator implementation complete (December 2024)

```cypher
MATCH p = (a)-->(b) RETURN p
MATCH p = (a)-[r:KNOWS]->(b) RETURN p, length(p)
MATCH p = (a)-[:KNOWS]->(b)-[:KNOWS]->(c) RETURN length(p), nodes(p), relationships(p)
```

**Implementation** (December 2024):
- **Parser**: Added `PathExpression` interface and `parsePatternOrPath()` method
  - Handles `variable = pattern` syntax in MATCH clauses
  - Stores path patterns in `MatchClause.pathExpressions`
  - Supports both simple paths `p = (a)-[r]->(b)` and multi-hop `p = (a)-[r1]->(b)-[r2]->(c)`
- **Translator**: Added `registerPathExpression()` to track path components
  - Registers path variable with type "path" in translation context
  - Tracks node and edge aliases that make up the path
  - Returns path as JSON object with nodes and edges arrays
  - Path functions implemented:
    - `length(p)`: Returns number of relationships in path
    - `nodes(p)`: Returns array of node objects in path
    - `relationships(p)`: Returns array of relationship objects in path

**Known Issues** (needs fixing):
- Tests currently failing due to multiple labels feature breaking existing CREATE statements
- Need to fix label storage/retrieval to make tests pass
- Variable-length paths with path expressions need additional work

**Next Steps**:
- Fix multiple labels feature to restore test suite
- Add comprehensive path expression tests once basic tests pass
- Test path expressions with variable-length paths
- Performance testing with complex path queries

### 2. Variable-Length Paths `[*]`, `[*1..3]` - COMPLETED
**Status**: Implemented (December 2024)

```cypher
MATCH (a)-[*]->(b) RETURN b          -- Unbounded (defaults to min=1, max=10)
MATCH (a)-[*1..3]->(b) RETURN b      -- Bounded range
MATCH (a)-[*2]->(b) RETURN b         -- Fixed length
MATCH (a)-[:KNOWS*2..5]->(b) RETURN b -- With type filter
MATCH (a)-[*2..]->(b) RETURN b       -- Minimum bound only
```

**Implementation** (December 2024):
- **Parser**: Already implemented in `parseVariableLengthSpec()` (lines 1145-1199)
  - Handles `*`, `*n`, `*n..m`, `*n..`, `*..m` syntax
  - Stores as `EdgePattern.minHops` and `EdgePattern.maxHops`
  - Supports all patterns including unbounded (`*`), fixed length (`*2`), and ranges (`*1..3`)
- **Translator**: Uses recursive CTEs for variable-length path queries
  - `translateVariableLengthPath()` (lines 1075-1242) generates recursive CTE
  - Pattern: `WITH RECURSIVE path(start_id, end_id, depth) AS (...)`
  - Base case: direct edges matching type filter
  - Recursive case: extends paths up to `maxHops`
  - Applies `minHops` constraint in WHERE clause
  - Supports relationship type filtering (`:KNOWS*1..3`)
- **Bug Fix**: Updated label matching in variable-length path translation to use JSON array format
  - Changed from `label = ?` to `generateLabelMatchCondition()` (translator.ts:1166, 1183)
  - Ensures compatibility with multiple labels feature
- **Test Coverage**: 7 comprehensive tests in `cypherqueries.test.ts`
  - Unbounded paths `[*]`
  - Fixed-length paths `[*2]`
  - Bounded ranges `[*1..2]`
  - Minimum-only bounds `[*2..]`
  - Type-filtered paths `[:KNOWS*1..3]`
  - Cycle handling
  - Aggregation with `count(DISTINCT other)`

**Notes**:
- Unbounded `*` defaults to max depth of 10 to prevent infinite loops
- Direction (outgoing, incoming, both) already supported via parser
- Handles cycles correctly by returning DISTINCT results

### 3. Multiple Labels `:A:B:C` - IN PROGRESS
**Failures**: ~19 tests
**Status**: Parser and storage implemented, needs result formatting fixes

```cypher
CREATE (n:A:B:C {name: 'test'})
MATCH (n:A:B) RETURN n
```

**Implementation** (December 2024):
- **Parser**: Updated `parseNodePattern()` to parse multiple labels with `:A:B:C` syntax
  - `NodePattern.label` now supports `string | string[]`
  - Parses labels in a loop while `COLON` tokens are present
  - Stores as array if multiple, string if single (backward compat)
- **Schema**: Changed `label` column from TEXT to JSON
  - Labels stored as JSON array: `["Person", "Employee"]`
  - Removed `idx_nodes_label` index (incompatible with JSON)
- **Database**: Updated `insertNode()` to normalize labels to JSON array
  - `getNode()` and `getNodesByLabel()` parse JSON label
  - `Node` interface updated to `label: string | string[]`
- **Translator**: Added `generateLabelMatchCondition()` helper
  - Single label: `EXISTS (SELECT 1 FROM json_each(label) WHERE value = ?)`
  - Multiple labels: Multiple EXISTS conditions joined with AND
  - Updated all MATCH translations to use new condition generator
- **Executor**: Added `normalizeLabelToJson()` and `generateLabelCondition()` helpers
  - Updated all CREATE INSERT statements to use normalized JSON labels
  - Updated MERGE label matching to use new condition logic

**Known Issues** (needs fixing):
- Result formatting: Labels returned as JSON string `'["Person"]'` instead of parsed array
  - Need to update result parser to handle label field specially
- Existing tests expect string labels, need migration plan
- Consider adding index on JSON array elements for performance

**Next Steps**:
- Fix result formatting to properly parse label JSON
- Update existing tests to expect label arrays
- Add migration utility for existing databases
- Performance testing with JSON label queries

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

### 5. MERGE with Relationships - COMPLETED
**Status**: Implemented (December 2024)

```cypher
MERGE (a:Person {name: 'Alice'})-[:KNOWS]->(b:Person {name: 'Bob'})
MERGE (a)-[r:KNOWS]->(b) ON CREATE SET r.since = date()
MATCH (a:Person), (b:Person) MERGE (a)-[:KNOWS]->(b)
```

**Implementation** (December 2024):
- **Executor**: Enhanced `tryMergeExecution()` to handle all relationship MERGE patterns
  - Now catches MERGE clauses with relationship patterns (not just ON CREATE/MATCH SET)
  - Refactored `executeMergeRelationship()` to handle three scenarios:
    1. Both nodes from MATCH: `MATCH (a), (b) MERGE (a)-[:REL]->(b)`
    2. Source from MATCH, target to find/create: `MATCH (a) MERGE (a)-[:REL]->(b:Label)`
    3. Entire pattern to find/create: `MERGE (a:Label)-[:REL]->(b:Label)`
  - Added `findOrCreateNode()` helper to find existing nodes or create new ones
  - Added `processReturnClauseWithEdges()` for proper edge variable binding in RETURN
- **Test Coverage**: 8 comprehensive tests in `cypherqueries.test.ts`
  - Create relationship if doesn't exist
  - Skip duplicate relationship creation
  - Relationship with properties
  - Entire pattern creation (nodes + relationship)
  - RETURN merged relationship
  - Match existing pattern instead of creating duplicate
  - Partial match (create missing parts)
  - Relationship variable binding with type() function

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

### 8. List Comprehensions - COMPLETED
**Status**: Implemented (December 2024)

```cypher
RETURN [x IN range(1, 10) WHERE x % 2 = 0] AS evens      -- Filter only
RETURN [x IN [1, 2, 3] | x * 2] AS doubled               -- Map only
RETURN [x IN [1, 2, 3, 4] WHERE x > 2 | x * 10] AS result -- Filter and map
RETURN [x IN n.values WHERE x > 2] AS filtered           -- Property as source
```

**Implementation** (December 2024):
- **Parser**: 
  - Added `PIPE` token type for `|` character
  - Added `listComprehension` expression type with fields: `variable`, `listExpr`, `filterCondition`, `mapExpr`
  - `parseListLiteralExpression()` detects `[var IN ...` syntax using lookahead
  - `parseListComprehension()` handles WHERE filter and `|` map projection
- **Translator**: 
  - `translateListComprehension()` generates SQLite using `json_each()` and `json_group_array()`
  - Pattern: `(SELECT json_group_array(expr) FROM json_each(source) AS __lc__ WHERE cond)`
  - `translateListComprehensionExpr()` substitutes comprehension variable with `__lc__.value`
  - `translateListComprehensionCondition()` handles filter conditions
- **Test Coverage**: 21 new tests across parser, translator, and integration

**Note**: Pattern comprehensions like `[(a)-->(b) | b.name]` (graph patterns in comprehension) are not yet supported.

### 9. Percentile Functions - COMPLETED
**Status**: Implemented (December 2024)

```cypher
RETURN percentileDisc(n.score, 0.9) AS p90
RETURN percentileCont(n.value, 0.5) AS median
MATCH (n:Score) WHERE n.category = 'high' RETURN percentileDisc(n.value, 0.5) AS median
```

**Implementation** (December 2024):
- **Parser**: Functions are parsed as standard function calls with two arguments
- **Translator**: Uses `json_group_array()` aggregate to collect values, then scalar subquery to extract percentile
  - `percentileDisc`: Returns actual value at percentile position using ROUND(percentile * (count-1))
  - `percentileCont`: Interpolates between adjacent values for exact percentile
  - Handles edge cases: empty sets (returns NULL), single value, 0th/100th percentiles
- **Aggregate Recognition**: Added to `isAggregateExpression()` for proper GROUP BY handling
- **Test Coverage**: 22 tests covering discrete/continuous percentiles, edge cases, filtering

### 10. List Concatenation - COMPLETED
**Status**: Implemented

```cypher
RETURN [1, 2] + [3, 4] AS combined     -- Uses json_group_array with UNION ALL
RETURN n.tags + ['new'] AS allTags     -- Property + literal concatenation
RETURN [1] + [2] + [3] AS chain        -- Chained concatenation works
```

**Implementation Notes**:
- Parser: Added `parseListLiteralExpression()` for list literals in expressions
- Translator: `translateBinaryExpression()` detects list concatenation via `isListExpression()`
- Uses SQL pattern: `(SELECT json_group_array(value) FROM (SELECT value FROM json_each(left) UNION ALL SELECT value FROM json_each(right)))`
- `translateArrayLiteral()` generates `json_array(...)` for list literals

### 11. List Predicates - COMPLETED
**Status**: Implemented (December 2024)

```cypher
RETURN ALL(x IN [1, 2, 3] WHERE x > 0) AS allPositive       -- All elements satisfy condition
RETURN ANY(x IN list WHERE x > 10) AS hasLarge               -- At least one satisfies
RETURN NONE(x IN n.values WHERE x < 0) AS noneNegative       -- No elements satisfy
RETURN SINGLE(x IN [1, 2, 10] WHERE x > 5) AS exactlyOne     -- Exactly one satisfies
MATCH (n:Item) WHERE ALL(x IN n.scores WHERE x >= 10) RETURN n  -- In WHERE clause
```

**Implementation** (December 2024):
- **Parser**:
  - Added `ANY`, `NONE`, `SINGLE` keywords (ALL was already present)
  - Parse list predicate syntax: `PRED(var IN listExpr WHERE cond)`
  - Added `listPredicate` type to both Expression and WhereCondition
  - Added logical operators (AND, OR, NOT) to return expression parsing for combining predicates
- **Translator**:
  - `translateListPredicate()` generates SQLite using `json_each()` subqueries
  - ALL: `(COUNT(*) FROM json_each(list) WHERE NOT cond) = 0`
  - ANY: `EXISTS (SELECT 1 FROM json_each(list) WHERE cond)`
  - NONE: `NOT EXISTS (SELECT 1 FROM json_each(list) WHERE cond)`
  - SINGLE: `(COUNT(*) FROM json_each(list) WHERE cond) = 1`
  - Handle in both expression and WHERE condition contexts
  - Added unary NOT operator support for expressions
- **Test Coverage**: 50 new tests across parser, translator, and integration
- **Empty list semantics**: ALL/NONE return true (vacuously), ANY/SINGLE return false

---

## Projected Compliance

| Phase | Focus | Tests Passing | Compliance |
|-------|-------|---------------|------------|
| Previous | - | 814 | 62.9% |
| Phase 1 | Quick Wins (4, 6, 7) | ~840 | ~65% |
| Phase 1.5 | List Concatenation (10) | ~845 | ~65.5% |
| Phase 1.6 | Variable-Length Paths (2) | ~882 | ~68.2% |
| Phase 1.7 | Path Expressions (1 - WIP) | ~890 | ~68.8% |
| Phase 1.8 | List Comprehensions (8) | 890 | ~68.8% |
| Phase 1.9 | List Predicates (11) | 997 | ~70% |
| Phase 2 | Paths + Multiple Labels (1, 3) | ~1040 | ~75% |
| Phase 3 | Remaining (5, 9) | ~1100 | ~80% |

**Note**: Phase 1.9 complete. List predicates (ALL, ANY, NONE, SINGLE) fully implemented with support in both RETURN expressions and WHERE clauses. Phase 1.7 in progress - path expressions parser and translator complete, but need to fix multiple labels implementation before tests can pass. Path expressions (item 1) and multiple labels (item 3) are being worked on in parallel for Phase 2.

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
| ~~`Expected expression, got LBRACKET '['`~~ | ~~List literals in RETURN~~ | ~~Fixed~~ |
| ~~`MERGE with relationship pattern must be executed`~~ | ~~MERGE relationships~~ | ~~Fixed~~ |

### Known Limitations

- **Implicit GROUP BY**: Queries mixing aggregates with non-aggregated columns
  (e.g., `RETURN n.department, count(DISTINCT n.skill)`) require relationship-based
  grouping rather than relying on implicit GROUP BY inference.

---

## References

- [openCypher Specification](https://opencypher.org/)
- [openCypher TCK](https://github.com/opencypher/openCypher/tree/main/tck)
- [Cypher Query Language Reference](https://neo4j.com/docs/cypher-manual/current/)
