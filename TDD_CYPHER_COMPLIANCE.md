# NiceFox GraphDB - TCK Compliance Session

## Context
SQLite-based graph database with Cypher query support. Currently at 74.5% TCK compliance (1097/1478 tests passing).

## Goal
Improve openCypher TCK compliance by fixing parser/translator/executor issues.

## Workflow
1. Enable TCK tests: `mv packages/server/test/tck/tck.test.ts.skip tck.test.ts`
2. Run tests: `pnpm test -- --run`
3. Analyze failures by category (check error patterns)
4. Use TDD: write unit test → implement fix → verify with TCK
5. Before committing: rename back to `.skip`, ensure unit tests pass
6. Update this file's "Current Priority Fixes" section with new stats

## Recently Fixed (2024-12-30)
- SET n:Label syntax - Added support for adding labels to nodes with SET clause
- WITH * syntax - Pass through all bound variables
- RETURN * syntax - Return all matched variables
- UNWIND variable handling in COLLECT - Fixed collecting scalar values from UNWIND
- SET n = {props} syntax - Replace all node properties with a map
- SET n += {props} syntax - Merge properties from a map (with null removal)

## Current Priority Fixes (by impact)
1. Multi-MATCH variable scope (18 tests) - `no such column: n0.id` in chained patterns
2. MERGE relationship patterns (18 tests) - Need executor handling
3. Source node resolution (18 tests) - CREATE edge cases
4. Map/list access in DELETE (14 tests) - `nodes.key` and `list[$index]` syntax

**Update this section after each session** with:
- New pass rate and test counts
- Completed fixes (move to "Recently Fixed" in TCK_COMPLIANCE_PLAN.md)
- New priority items based on error analysis

## Key Files
- `packages/server/src/parser.ts` - Cypher tokenizer & AST
- `packages/server/src/translator.ts` - AST → SQL
- `packages/server/src/executor.ts` - Query execution
- `packages/server/test/tck/tck.test.ts.skip` - TCK runner
- `TCK_COMPLIANCE_PLAN.md` - Detailed progress tracking

## Commands
```bash
# Run all tests
pnpm test -- --run

# Run TCK and grep for specific errors
pnpm test -- --run 2>&1 | grep "Expected DOT"

# Count errors by type
pnpm test -- --run 2>&1 | grep "Query failed:" | sed 's/Query:.*//' | sort | uniq -c | sort -rn

# Count failures by category
pnpm test -- --run 2>&1 | grep -E "^( FAIL)" | sed 's/.*openCypher TCK > //' | cut -d'>' -f1 | sort | uniq -c | sort -rn

# Run specific test file
pnpm test -- --run packages/server/test/translator.test.ts
```

## TDD Pattern
1. Find failing TCK test category
2. Write minimal unit test in `translator.test.ts` or `parser.test.ts`
3. Implement fix
4. Verify unit test passes
5. Re-run TCK to confirm improvement
6. Update `TCK_COMPLIANCE_PLAN.md` with new stats
7. Update "Current Priority Fixes" above

## Error Analysis Quick Reference

| Error Pattern | Likely Location | Fix Type |
|---------------|-----------------|----------|
| `Expected X, got Y` | parser.ts | Token/syntax handling |
| `no such column` | translator.ts | Variable scope/alias |
| `near "X": syntax error` | translator.ts | SQL generation |
| `must be executed` | executor.ts | Runtime handling |

## Architecture Reference
```
packages/server/src/
├── parser.ts      # Cypher tokenizer & parser → AST
├── translator.ts  # AST → SQL translation
├── executor.ts    # Query execution (handles multi-phase queries)
├── db.ts          # SQLite wrapper (nodes/edges tables)
├── routes.ts      # HTTP API endpoints
└── auth.ts        # API key authentication
```
