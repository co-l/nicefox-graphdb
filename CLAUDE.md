# NiceFox GraphDB

SQLite-based graph database with Cypher query support.

## Architecture

```
packages/server/src/
├── parser.ts      # Cypher tokenizer & parser → AST
├── translator.ts  # AST → SQL translation
├── executor.ts    # Query execution (handles multi-phase queries)
├── db.ts          # SQLite wrapper (nodes/edges tables)
├── routes.ts      # HTTP API endpoints
└── auth.ts        # API key authentication
```

## Development

```bash
pnpm test              # Run all tests
pnpm test -- --run     # Run once (no watch)
```

Use TDD: write failing tests first, then implement.

## Cypher Support

See `README.md` for current keyword support table. Priority candidates for implementation:
- `WITH` - Query chaining
- `UNWIND` - List expansion
- `CASE` - Conditional expressions
- `SUM` / `AVG` / `MIN` / `MAX` / `COLLECT` - Aggregation functions
- Variable-length paths (`*1..3`)

## Key Patterns

- Parser produces AST (see interfaces in `parser.ts`: `Query`, `Clause`, `Expression`, etc.)
- Translator maintains context (`ctx`) to track variables, aliases, patterns
- Executor uses multi-phase execution for MATCH+CREATE/SET/DELETE queries
- Tests mirror source structure: `test/parser.test.ts`, `test/translator.test.ts`, etc.

## Specs

See `graph-db-spec.md` for full project specification.
