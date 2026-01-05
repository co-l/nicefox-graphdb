# NiceFox GraphDB - TCK Compliance Guide

## Current Status (vs Neo4j 3.5 Baseline)
- **Target**: 2703 tests (what Neo4j 3.5 passes)
- **Passing**: 1407 tests (52.1% of target)
- **Failing**: 1277 tests (to be fixed)
- **Not in baseline**: 19 tests (parser edge cases)

### What This Means
We only run tests that Neo4j 3.5 actually passes. This gives us a realistic, achievable target rather than chasing features that even Neo4j doesn't support.

### Excluded Categories
- `clauses/call` - CALL procedures not yet supported

## Test Types

The TCK includes two main types of tests:

### 1. Expected Result Tests (majority)
Tests where a query should succeed and return specific data:
```gherkin
When executing query:
  """
  MATCH (n:Person) RETURN n.name
  """
Then the result should be:
  | n.name  |
  | 'Alice' |
```

### 2. Expected Error Tests (127 in failing list)
Tests where a query should be **rejected** with an error:
```gherkin
When executing query:
  """
  RETURN 1 AS a, 2 AS a
  """
Then a SyntaxError should be raised at compile time
```

These tests verify that our implementation correctly rejects invalid Cypher.


## TDD Workflow

This workflow is to be respected to the letter:

1. **Run `./scripts/comment-first-failing.sh` **
2. **Run tests** - `npm test` - see it fail
3. **Fix the code** until green
4. **Run `./scripts/comment-fixed-tests.sh` ** - automatically updates failing_tests.ts with working tests
5. **Commit and push**


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

# Find tests that now pass (extracts just the relevant output)
npm run tck:check-fixed

# List only expected-error tests (tests where we should reject the query)
npm run tck:failing -- --errors

# Show detailed list with queries
npm run tck:failing -- --errors --full


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

