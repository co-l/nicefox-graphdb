# NiceFox GraphDB - TCK Compliance Guide

## Current Status
- **Passing**: 1310 tests (83.1%)
- **Failing**: 266 tests (skipped in test suite)
- **Outline scenarios**: 276 (skipped, require template expansion)

## How TCK Tests Work

TCK tests now run as part of the normal test suite. Failing tests are tracked in `packages/server/test/tck/failing-tests.ts` and automatically skipped.

```bash
# Run all tests (including TCK)
pnpm test -- --run

# Output shows:
# Tests  1125 passed | 445 skipped (1570)
```

### Quick TCK Test Tool

Use `pnpm tck` to quickly run individual tests without the full test suite:

```bash
pnpm tck 'Return6|11'           # Run specific test by ID
pnpm tck 'Counting matches'     # Run test by name
pnpm tck 'Match3' --list        # List all Match3 tests
pnpm tck 'Return6|11' -v        # Verbose output with setup/expected
pnpm tck 'Return6|11' --sql     # Show generated SQL
pnpm tck 'Return6|16' --force   # Run even if in failing list
```

This is the fastest way to debug a specific test - much quicker than running the full test suite.

## Workflow for Fixing Tests

### 1. Find a Test to Fix

Look at `packages/server/test/tck/failing-tests.ts` to see all failing tests:

```typescript
export const FAILING_TESTS = new Set([
  "clauses/match > Match2 - Match relationships|2",
  "clauses/match > Match2 - Match relationships|6",
  // ... 377 entries
]);
```

Or analyze by error type:
```bash
# Temporarily enable all tests to see errors
# Edit tck.test.ts: change `FAILING_TESTS.has(testKey)` to `false`
pnpm test -- --run 2>&1 | grep "Query failed:" | sed 's/.*Query failed: //' | sort | uniq -c | sort -rn | head -20
```

### 2. Remove Test from Skip List

Edit `packages/server/test/tck/failing-tests.ts` and remove the test key you want to fix:

```typescript
// Remove this line to enable the test:
"clauses/match > Match2 - Match relationships|2",
```

### 3. Run Tests to See the Failure

```bash
pnpm test -- --run
# The test will now run and fail, showing you the exact error
```

### 4. Write a Unit Test (TDD)

Add a focused test in the appropriate test file:
- `packages/server/test/parser.test.ts` - for parsing issues
- `packages/server/test/translator.test.ts` - for SQL generation issues
- `packages/server/test/cypherqueries.test.ts` - for end-to-end query issues

### 5. Implement the Fix

Key files:
- `packages/server/src/parser.ts` - Cypher tokenizer & AST
- `packages/server/src/translator.ts` - AST → SQL translation
- `packages/server/src/executor.ts` - Query execution

### 6. Verify All Tests Pass

```bash
pnpm test -- --run
# Should show increased pass count, same or fewer skipped
```

### 7. Commit

```bash
git add -A
git commit -m "fix(tck): [description of what you fixed]

- Detailed explanation
- TCK compliance: X/1570 tests passing (Y%)"
```

## Error Analysis Quick Reference

| Error Pattern | Location | Fix Type |
|---------------|----------|----------|
| `Expected X, got Y` | parser.ts | Token/syntax handling |
| `Unexpected token` | parser.ts | Missing syntax support |
| `no such column` | translator.ts | Variable scope/alias tracking |
| `near "X": syntax error` | translator.ts | SQL generation bug |
| `must be executed` | executor.ts | Needs runtime handling |
| `Unknown variable` | translator.ts | Variable not registered |
| `incomplete input` | translator.ts | SQL generation incomplete |

## Top Failing Categories (as of 2024-12-30)

Run this to get current breakdown:
```bash
# Edit tck.test.ts temporarily to run all tests, then:
pnpm test -- --run 2>&1 | grep "Query failed:" | sed 's/.*Query failed: //' | sort | uniq -c | sort -rn | head -15
```

Recent error counts:
- `no such column: e2.id` (14) - Variable-length path edge tracking
- `incomplete input` (14) - MERGE/SET SQL generation
- `MERGE with relationship pattern` (12) - Complex MERGE scenarios
- `Too few parameter values` (10) - Parameter binding
- `Unknown variable` (8) - Variable scope issues
- `FOREIGN KEY constraint failed` (8) - Node reference issues
- `Expected RBRACKET, got LBRACKET` (7) - Dynamic property access `r[key]`

## Recently Fixed

### 2024-12-31 (continued)
- MERGE + RETURN count(*) now returns proper count (processReturnClauseWithEdges fix)
- Comparison expressions inside map literals `{foo: a.name='Andres'}` (parser fix)
- TCK test runner now uses 'expr' column fallback for complex expression column names
- TCK test runner now matches string map patterns to actual map objects
- Match3|17,18,22: Cyclic patterns and returning bound nodes
- Return2|9,10,13: Map literals and expression projection
- Return4|9: Nested aggregation in map literals
- Return5|4: DISTINCT with nested maps
- Return6|2,6: Aggregation expressions and comparisons in maps
- Merge5|1,2,5-8,12: MERGE relationship + count(*)
- Merge6|2-6, Merge7|3, Merge8|1: MERGE relationship ON CREATE/MATCH + count(*)
- Enabled 32 more tests (1278 → 1310)

### 2024-12-31
- Keywords as variable names (e.g., `count`, `as` can now be used as variable names)
- Nested object literals in list expressions `[{a: 1}, {b: 2}]`
- Negative numbers in list expressions `[-1, -2, 3]`
- AND/OR operators inside parenthesized expressions `(a AND b)`
- Float literals starting with dot `.5` → `0.5`
- List indexing `expr[index]` with json_extract
- List slicing `expr[start..end]` with json_each subquery
- Enabled 19 more tests (1259 → 1278)

### 2024-12-30
- Label predicate expression `(n:Label)` in RETURN - checks if node has a label
- Fixed TCK parser to correctly handle "result should be empty" before side effects table
- Added relationship pattern matching in TCK test runner for `[:TYPE]` comparisons
- Added column name normalization for property expressions (`n.name` -> `n_name`)
- Enabled passing Create1-6, Delete1-3, Match2-3, Return3-6 tests
- Bidirectional relationship patterns `<-->` and `--`
- IS NULL / IS NOT NULL in RETURN expressions
- Multiple relationship types `[:TYPE1|TYPE2]`
- UNWIND with function calls and parenthesized expressions
- SET with parenthesized variable `(n).property`
- MERGE with WITH clause aliasing
- Anonymous node creation in CREATE
- SET with binary expressions `n.num = n.num + 1`

## Key Commands

```bash
# Run all tests
pnpm test -- --run

# Run specific test file
pnpm test -- --run packages/server/test/translator.test.ts

# Run tests matching pattern
pnpm test -- --run -t "UNWIND"

# Watch mode for development
pnpm test -- packages/server/test/translator.test.ts
```

## Architecture Reference

```
packages/server/src/
├── parser.ts      # Cypher tokenizer & parser → AST
├── translator.ts  # AST → SQL translation
├── executor.ts    # Query execution (handles multi-phase queries)
├── db.ts          # SQLite wrapper (nodes/edges tables)
├── routes.ts      # HTTP API endpoints
└── auth.ts        # API key authentication

packages/server/test/tck/
├── tck.test.ts        # TCK test runner (always runs)
├── failing-tests.ts   # Set of known failing test keys
├── tck-parser.ts      # Parses .feature files
└── openCypher/        # TCK feature files
```

## Tips

1. **Start small**: Pick tests with clear error messages like "Unexpected token X"

2. **Group related fixes**: Many tests fail for the same root cause

3. **Check the feature file**: Look at `packages/server/test/tck/openCypher/tck/features/` to understand what the test expects

4. **Use console.log in tests**: Add temporary logging in `runScenario()` to see queries and results

5. **SQLite quirks**: Remember SQLite uses `json_extract()` for property access and has different NULL handling than Neo4j
