# NiceFox GraphDB - TCK Compliance Guide

## Current Status
- **Passing**: 1383 tests (87.7%)
- **Failing**: 193 tests (skipped in test suite)
- **Total**: 1576 tests

## TDD Workflow

The workflow is simple:

1. **Unskip the first test** in `packages/server/test/tck/failing-tests.ts` (comment out the first uncommented line)
2. **Run tests** - `pnpm test -- --run` - see it fail
3. **Fix the code** until green
4. **Commit and push**
5. **Repeat**

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
