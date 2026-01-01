# NiceFox GraphDB - TCK Compliance Guide

## Current Status (vs Neo4j 3.5 Baseline)
- **Target**: 2703 tests (what Neo4j 3.5 passes)
- **Passing**: 829 tests (30.7% of target)
- **Failing**: 1874 tests (to be fixed)
- **Not in baseline**: 19 tests (parser edge cases)

### What This Means
We only run tests that Neo4j 3.5 actually passes. This gives us a realistic, achievable target rather than chasing features that even Neo4j doesn't support.

### Excluded Categories
- `clauses/call` - CALL procedures not yet supported

## TDD Workflow

The workflow is simple:

1. **Unskip the first test** in `packages/server/test/tck/failing-tests.ts` (comment out the first uncommented line)
2. **Run tests** - `pnpm test -- --run` - see it fail
3. **Fix the code** until green
4. **Uncomment all failing tests to see by chance if you didn't fix another one**
5. **Comment actual failing tests**
6. **Update this document**
7. **Commit and push**


### Tests to Skip

Some tests require significant architectural changes. Skip these during normal TDD cycles:

| Test | Reason |
|------|--------|
| `Match4\|4` | Requires sequential multi-phase execution. The setup query has chained UNWIND clauses where the second UNWIND (`range(0, size(nodeList)-2, 1)`) depends on `nodeList` from a previous WITH clause with `collect()`. This needs the executor to run phases sequentially and pass results between them. |

### Example

```typescript
// In failing-tests.ts, change:
  "clauses/delete > Delete4 - Delete clause interoperation with other clauses|1",

// To:
  // "clauses/delete > Delete4 - Delete clause interoperation with other clauses|1",
```

Then run `pnpm test -- --run`, fix code, commit, push.

## Quick Commands

```bash
# Run all tests
pnpm test -- --run

# Test a specific TCK test with details
pnpm tck 'Delete4|1' -v --sql -f

# See what error a test produces
pnpm tck 'Delete4|1' -f
```

## Key Files

- `packages/server/test/tck/failing-tests.ts` - List of skipped tests
- `packages/server/src/parser.ts` - Cypher parsing
- `packages/server/src/translator.ts` - AST → SQL
- `packages/server/src/executor.ts` - Query execution

## Error Quick Reference

| Error | Fix Location |
|-------|--------------|
| `Unexpected token` | parser.ts |
| `no such column` | translator.ts |
| `near "X": syntax error` | translator.ts |
| `Too few parameter values` | translator.ts or executor.ts |
| `Unknown variable` | translator.ts |
