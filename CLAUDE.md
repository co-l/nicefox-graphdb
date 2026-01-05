# NiceFox GraphDB

SQLite-based graph database with Cypher query support.

## Architecture

```
src/
├── index.ts       # Main entry, factory function (GraphDB)
├── local.ts       # Local embedded SQLite client
├── remote.ts      # Remote HTTP client
├── types.ts       # Shared types
├── parser.ts      # Cypher tokenizer & parser → AST
├── translator.ts  # AST → SQL translation
├── executor.ts    # Query execution (handles multi-phase queries)
├── db.ts          # SQLite wrapper (nodes/edges tables)
├── routes.ts      # HTTP API endpoints
├── server.ts      # Server startup
├── auth.ts        # API key authentication
├── backup.ts      # Backup utilities
├── cli.ts         # CLI entry point
└── cli-helpers.ts # CLI utilities
```

## Development

```bash
npm test               # Run all tests
npm run build          # Build to dist/
npm run dev            # Run server in dev mode
npm run tck 'Return6|11'  # Run single TCK test
npm run tck 'Match3' -- -l   # List matching TCK tests
npm run tck 'Return6|11' -- --sql  # Show generated SQL
```

Use TDD: write failing tests first, then implement.

### TCK Test Tool

Quick way to run individual openCypher TCK tests:

```bash
npm run tck <pattern> [-- options]

Options:
  -v, --verbose    Show detailed test info
  -l, --list       List matching tests
  --sql            Show generated SQL
  -f, --force      Run even if in failing list
```

Examples:
```bash
npm run tck 'Return6|11'           # Exact test by number
npm run tck 'Counting matches'     # Match by name
npm run tck 'Match3' -- --list     # List all Match3 tests
```

### List Failing Tests

See all currently failing TCK tests for TDD workflow:

```bash
npm run tck:failing               # Summary by category
npm run tck:failing -- --full     # Detailed list with queries
npm run tck:failing -- --errors   # Only tests expecting errors
npm run tck:failing -- --category match  # Filter by category
```

## Next Implementation Priorities

See `README.md` for the full support table. Priority candidates for implementation:

1. **`CALL` procedures** - Database introspection
   ```cypher
   CALL db.labels() YIELD label RETURN label
   CALL db.relationshipTypes() YIELD type RETURN type
   ```

2. **Path expressions** - Named paths and path functions
   ```cypher
   MATCH p = (a:Person)-[:KNOWS*]->(b:Person) RETURN p
   RETURN length(p) AS pathLength
   ```

3. **List comprehensions** - Filter and transform lists
   ```cypher
   RETURN [x IN range(1, 10) WHERE x % 2 = 0] AS evens
   ```

## Key Patterns

- Parser produces AST (see interfaces in `parser.ts`: `Query`, `Clause`, `Expression`, etc.)
- Translator maintains context (`ctx`) to track variables, aliases, patterns
- Executor uses multi-phase execution for MATCH+CREATE/SET/DELETE queries
- Tests mirror source structure: `test/parser.test.ts`, `test/translator.test.ts`, etc.

## Implementation Notes

### Adding a new WHERE operator (e.g., `IN`)
1. Add keyword to `KEYWORDS` set in `parser.ts` if needed
2. Add condition type to `WhereCondition` interface (e.g., `"in"`)
3. Implement parsing in `parseComparisonCondition()` - check for keyword after expression
4. Add case handling in `translateWhere()` in `translator.ts`
5. Write tests first in `translator.test.ts`

Example: `IN` uses `WhereCondition.list` for the array expression.

### Adding a new function
1. Add function handling in `translateExpression()` in `translator.ts`
2. Use `translateFunctionArg()` for argument translation
3. Map to appropriate SQLite function or expression
4. For use in WHERE, also add case `"function"` in `translateWhereExpression()`
5. Write tests first in `translator.test.ts`

### Adding binary operators (arithmetic)
1. Add token types (`PLUS`, `SLASH`, `PERCENT`) to tokenizer's `singleCharTokens`
2. Extend `Expression` type to include `"binary"` with `operator`, `left`, `right`
3. Use precedence parsing: `parseExpression()` → `parseAdditiveExpression()` → `parseMultiplicativeExpression()` → `parsePrimaryExpression()`
4. Add `translateBinaryExpression()` in translator
5. Handle in `translateWhereExpression()` for WHERE clause arithmetic

## Specs

See `graph-db-spec.md` for full project specification.
