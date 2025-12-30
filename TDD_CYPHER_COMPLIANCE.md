# NiceFox GraphDB - TCK Compliance Session

## Context
SQLite-based graph database with Cypher query support. Currently at 74.9% TCK compliance (1110/1492 tests passing, 68 skipped).

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
- MERGE with WITH clause aliasing - `MATCH (n) WITH n AS a MERGE (a)-[:T]->(b)` now works
- Anonymous node creation in CREATE - `CREATE ()-[:R]->()` creates unlabeled nodes
- SET with binary expressions - `SET n.num = n.num + 1` works with arithmetic
- Property access in node maps - `{name: person.bornIn}` parsed and executed
- Property access in ON CREATE/MATCH SET - Referenced properties resolved at runtime

## Current Priority Fixes (by impact)
1. Variable-length path edge tracking (14 tests) - `no such column: e2.id` errors
2. MERGE relationship patterns (12 tests) - Complex multi-MERGE scenarios
3. Incomplete input in MERGE SET (14 tests) - Multi-line ON CREATE/MATCH SET parsing
4. Invalid relationship pattern (8 tests) - Bidirectional arrow patterns
5. FOREIGN KEY constraint failed (8 tests) - WITH aliased nodes in CREATE

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
