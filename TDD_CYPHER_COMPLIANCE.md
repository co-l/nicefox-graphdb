# NiceFox GraphDB - TCK Compliance Guide

## Current Status
- **Passing**: 2010 tests (41.2%)
- **Failing**: 2870 tests (skipped in test suite)
- **Total**: 4880 tests

### Test Coverage
- Scenario Outlines are now expanded (2558 tests from outlines)
- Error expectation tests are now included (695 tests)
- All TCK categories are covered except `clauses/call`

### Excluded Categories
- `clauses/call` - CALL procedures not yet supported

## TDD Workflow

The workflow is simple:

1. **Unskip the first test** in `packages/server/test/tck/failing-tests.ts` (comment out the first uncommented line)
2. **Run tests** - `pnpm test -- --run` - see it fail
3. **Fix the code** until green
4. **Commit and push**
5. **Repeat**

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
