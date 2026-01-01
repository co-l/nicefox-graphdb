# NiceFox GraphDB - TCK Compliance Guide

## Current Status (vs Neo4j 3.5 Baseline)
- **Target**: 2703 tests (what Neo4j 3.5 passes)
- **Passing**: 1101 tests (40.7% of target)
- **Failing**: 1583 tests (to be fixed)
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
4. **Find other fixed tests** - Run `TCK_TEST_ALL=1 pnpm test -- --run` to check if your fix also fixed other tests
5. **Update failing-tests.ts** - Comment out any tests that now pass (shown in the output)
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
# Run all tests (skipping known failing)
pnpm test -- --run

# Run ALL tests including known failing ones
# Shows which tests from failing-tests.ts now pass
TCK_TEST_ALL=1 pnpm test -- --run

# Test a specific TCK test with details
pnpm tck 'Delete4|1' -v --sql -f

# See what error a test produces
pnpm tck 'Delete4|1' -f
```

### Finding Fixed Tests

After implementing a fix, run with `TCK_TEST_ALL=1` to discover other tests that might have been fixed:

```bash
TCK_TEST_ALL=1 pnpm test -- --run
```

At the end of the test run, you'll see output like:

```
🎉 Tests from FAILING_TESTS that now PASS (3):
   These can be removed from failing-tests.ts:

   // "clauses/create > Create1 - Creating nodes|16",
   // "clauses/create > Create1 - Creating nodes|17",
   // "clauses/create > Create1 - Creating nodes|18",
```

Copy these commented lines to `failing-tests.ts` to mark them as passing.

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
