# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.1.0] - 2025-01-XX

### Added

#### Cypher Query Support

**Clauses & Keywords**
- `CREATE` - Create nodes and relationships
- `MATCH` - Find patterns in the graph
- `MERGE` - Match or create if not exists
- `WHERE` - Filter results with conditions
- `SET` - Update properties
- `DELETE` / `DETACH DELETE` - Remove nodes and relationships
- `RETURN` - Return query results
- `AS` - Aliases for returned values
- `LIMIT` / `SKIP` - Pagination
- `ORDER BY` - Sort results (ASC/DESC)
- `DISTINCT` - Remove duplicates
- `OPTIONAL MATCH` - Match or return null
- `WITH` - Chain query parts
- `UNION` / `UNION ALL` - Combine results
- `UNWIND` - Expand list to rows
- `CASE WHEN THEN ELSE END` - Conditional expressions
- `EXISTS` - Check pattern existence
- `CALL` - Database introspection procedures
- Variable-length paths (`*1..3`)

**Operators**
- Comparison: `=`, `<>`, `<`, `>`, `<=`, `>=`
- Arithmetic: `+`, `-`, `*`, `/`, `%`
- Boolean: `AND`, `OR`, `NOT`
- String: `CONTAINS`, `STARTS WITH`, `ENDS WITH`
- Null checks: `IS NULL`, `IS NOT NULL`
- List membership: `IN`

**Functions**
- Aggregation: `COUNT`, `SUM`, `AVG`, `MIN`, `MAX`, `COLLECT`
- Scalar: `ID`, `coalesce`
- String: `toUpper`, `toLower`, `trim`, `substring`, `replace`, `toString`, `split`
- List: `size`, `head`, `last`, `tail`, `keys`, `range`
- Node/Relationship: `labels`, `type`, `properties`
- Math: `abs`, `ceil`, `floor`, `round`, `rand`, `sqrt`
- Date/Time: `date`, `datetime`, `timestamp`

**Procedures**
- `CALL db.labels() YIELD label` - List all node labels
- `CALL db.relationshipTypes() YIELD type` - List all relationship types
- `CALL db.propertyKeys() YIELD key` - List all property keys

#### Architecture
- Multi-project support with isolated databases
- Multi-environment support (production/test)
- HTTP API for queries
- API key authentication
- Hot backup capability

#### Packages
- `leangraph` - Unified npm package (client + server + CLI)
- TypeScript client library with `GraphDB` factory function
- `createTestClient()` for in-memory testing
- CLI for server and project administration

#### CLI Commands
- `leangraph serve` - Start HTTP server
- `leangraph create <project>` - Create new project
- `leangraph delete <project>` - Delete project
- `leangraph list` - List all projects
- `leangraph query <env> <project> <cypher>` - Execute query
- `leangraph clone <project>` - Clone production to test
- `leangraph wipe <project>` - Wipe test database
- `leangraph backup` - Backup databases
- `leangraph apikey add/list/remove` - Manage API keys
