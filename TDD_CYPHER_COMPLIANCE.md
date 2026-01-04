# NiceFox GraphDB - TCK Compliance Guide

## Current Status (vs Neo4j 3.5 Baseline)
- **Target**: 2703 tests (what Neo4j 3.5 passes)
- **Passing**: 1327 tests (49.0% of target)
- **Failing**: 1357 tests (to be fixed)
- **Not in baseline**: 19 tests (parser edge cases)

### What This Means
We only run tests that Neo4j 3.5 actually passes. This gives us a realistic, achievable target rather than chasing features that even Neo4j doesn't support.

### Excluded Categories
- `clauses/call` - CALL procedures not yet supported

## Recent Fixes
- **Match7-24**: Fixed self-loop matching in OPTIONAL MATCH - undirected patterns `(a)-[r]-(a)` now correctly return self-loops once instead of twice. The fix adds a NOT condition to prevent duplicate matches when `source_id = target_id`.
- **Match7-28**: Fixed OPTIONAL MATCH with inline label predicate - when the target node with a specific label doesn't exist, the query now correctly returns 1 row with NULL instead of multiple rows. The fix adds `DISTINCT` to the SELECT when there's an OPTIONAL MATCH with a label predicate on a new edge's target node, preventing row multiplication from multiple edges that don't match the label.
- **Match7-8**: Fixed multi-hop OPTIONAL MATCH - for a chain like `(a)-->(b)-->(c)`, if the intermediate node b is found but there's no edge from b to c, the query now correctly returns NULL for b instead of the node. The fix adds clause boundary tracking and checks if all edges in a connected optional chain exist before returning intermediate nodes. Separate OPTIONAL MATCH clauses are correctly treated as independent patterns.

## TDD Workflow

The workflow is simple:

1. **Unskip the first test** in `test/tck/failing-tests.ts` (comment out the first uncommented line)
2. **Run tests** - `npm test` - see it fail
3. **Fix the code** until green
4. **Find other fixed tests** - Run `TCK_TEST_ALL=1 npm test` to check if your fix also fixed other tests
5. **Update failing-tests.ts** - Comment out any tests that now pass (shown in the output)
6. **Update this document**
7. **Commit and push**


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

# Run ALL tests including known failing ones
# Shows which tests from failing-tests.ts now pass
TCK_TEST_ALL=1 npm test

# Find tests that now pass (extracts just the relevant output)
npm run tck:check-fixed

# Test a specific TCK test with details
npm run tck 'Delete4|1' -- -v --sql -f

# See what error a test produces
npm run tck 'Delete4|1' -- -f
```

### Finding Fixed Tests

After implementing a fix, use `tck:check-fixed` to quickly see which tests now pass:

```bash
npm run tck:check-fixed
```

This runs all tests and extracts just the newly passing tests:

```
🎉 Tests from FAILING_TESTS that now PASS (3):
   These can be removed from failing-tests.ts:

   // "clauses/create > Create1 - Creating nodes|16",
   // "clauses/create > Create1 - Creating nodes|17",
   // "clauses/create > Create1 - Creating nodes|18",
```

Copy these commented lines to `failing-tests.ts` to mark them as passing.

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
