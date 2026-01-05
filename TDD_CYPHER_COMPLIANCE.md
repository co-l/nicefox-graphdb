# NiceFox GraphDB - TCK Compliance Guide

## Current Status (vs Neo4j 3.5 Baseline)
- **Target**: 2703 tests (what Neo4j 3.5 passes)
- **Passing**: 1387 tests (51.3% of target)
- **Failing**: 1297 tests (to be fixed)
- **Not in baseline**: 19 tests (parser edge cases)

### What This Means
We only run tests that Neo4j 3.5 actually passes. This gives us a realistic, achievable target rather than chasing features that even Neo4j doesn't support.

### Excluded Categories
- `clauses/call` - CALL procedures not yet supported

## Recent Fixes
- **Boolean Type Strictness (48 tests)**: AND/OR/NOT operators now reject non-boolean operands at compile time. `RETURN 123 AND true` now correctly throws `SyntaxError: InvalidArgumentType`.
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

# List all failing tests by category
npm run tck:failing

# List only expected-error tests (tests where we should reject the query)
npm run tck:failing -- --errors

# Show detailed list with queries
npm run tck:failing -- --errors --full

# Filter by category
npm run tck:failing -- --category merge

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

## Expected Error Tests Breakdown

Of the 1297 failing tests, **69 are expected-error tests** where we should reject the query but currently don't.

To list them: `npm run tck:failing -- --errors`

### By Error Type

| Error Type | Count | Description | Fix Location |
|------------|-------|-------------|--------------|
| SyntaxError | ~46 | Parser should reject | parser.ts |
| TypeError | 22 | Type mismatch at runtime | executor.ts |
| ArgumentError | 6 | Invalid function arguments | translator.ts/executor.ts |
| EntityNotFound | 3 | Access deleted node/rel | executor.ts |
| SemanticError | 2 | Semantic validation | translator.ts |

### Priority Categories

#### 1. ~~Boolean Type Strictness~~ ✅ FIXED (48 tests fixed)
~~Queries like `RETURN 123 AND true` or `RETURN NOT 0` should fail.~~

Now validates:
- AND/OR operands must be boolean → `SyntaxError: InvalidArgumentType`
- NOT operand must be boolean → `SyntaxError: InvalidArgumentType`

Static type checking at translation time rejects literals, lists, maps, etc.

#### 2. ~~SKIP/LIMIT Validation~~ ✅ FIXED (7 tests fixed)
~~Negative or float values should be rejected.~~

Now validates:
- Negative integers in SKIP/LIMIT → `NegativeIntegerArgument` error
- Floating point values in SKIP/LIMIT → `InvalidArgumentType` error

Parameters are rejected at parse time (not supported yet).

#### 3. Duplicate Column Names (4 tests)
Duplicate aliases in RETURN/WITH should be rejected.

```cypher
-- Should fail (SyntaxError at compile)
RETURN 1 AS a, 2 AS a
WITH 1 AS a, 2 AS a RETURN a
```

**Fix**: Check for duplicate aliases in translator.

#### 4. UNION Column Mismatch (4 tests)
UNION requires matching column names.

```cypher
-- Should fail (SyntaxError at compile)
RETURN 1 AS a UNION RETURN 2 AS b
```

**Fix**: Validate UNION column names match in translator.

#### 5. List Indexing Type Errors (22 tests)
Using non-integer indexes or indexing non-lists.

```cypher
-- Should fail (TypeError at runtime)
WITH [1,2,3] AS list RETURN list[1.5]
WITH [1,2,3] AS list RETURN list['a']
WITH 'string' AS x RETURN x[0]
```

**Fix**: Add runtime type checking in executor.

#### 6. Accessing Deleted Entities (3 tests)
Accessing properties of deleted nodes/relationships.

```cypher
-- Should fail (EntityNotFound at runtime)
MATCH (n) DELETE n RETURN n.name
```

**Fix**: Track deleted entities in executor, error on access.

#### 7. Integer Overflow (2 tests)
Integers beyond int64 range should fail.

```cypher
-- Should fail (SyntaxError at compile)
RETURN 9223372036854775808
```

**Fix**: Validate integer literals in parser.

#### 8. Invalid Unicode Escapes (1 test)
Invalid escape sequences in strings.

```cypher
-- Should fail (SyntaxError at compile)
RETURN '\uH'
```

**Fix**: Validate unicode escapes in tokenizer.

### Recommended Fix Order

1. ~~**SKIP/LIMIT validation**~~ ✅ DONE - 7 tests fixed
2. ~~**Duplicate column names**~~ ✅ DONE - 2 tests fixed (Return4|10, With4|4)
3. ~~**Boolean strictness**~~ ✅ DONE - 48 tests fixed (AND/OR/NOT type checking)
4. **Integer overflow** - 2 tests, parser only
5. **Invalid unicode** - 1 test, tokenizer only
6. **UNION column mismatch** - 4 tests, translator only
7. **List indexing types** - 22 tests, runtime type checking
8. **Deleted entity access** - 3 tests, executor state tracking
