# Contributing to LeanGraph

Thank you for your interest in contributing to LeanGraph! This document provides guidelines and instructions for contributing.

## Prerequisites

- Node.js 18+
- pnpm (install via `npm install -g pnpm`)

## Getting Started

1. Fork the repository on GitHub
2. Clone your fork:
   ```bash
   git clone https://github.com/YOUR_USERNAME/leangraph.git
   cd leangraph
   ```
3. Install dependencies:
   ```bash
   pnpm install
   ```
4. Run tests to verify everything works:
   ```bash
   pnpm test
   ```

## Project Structure

```
leangraph/
├── packages/
│   ├── leangraph/  # Unified npm package (re-exports client + server)
│   ├── server/           # Core server: parser, translator, executor, HTTP API
│   ├── client/           # TypeScript client library
│   └── cli/              # Command-line interface
├── docs/                 # Documentation
└── deploy/               # Deployment scripts
```

### Key Server Components

| File | Purpose |
|------|---------|
| `packages/server/src/parser.ts` | Cypher tokenizer & parser (produces AST) |
| `packages/server/src/translator.ts` | AST to SQL translation |
| `packages/server/src/executor.ts` | Query execution (handles multi-phase queries) |
| `packages/server/src/db.ts` | SQLite wrapper (nodes/edges tables) |
| `packages/server/src/routes.ts` | HTTP API endpoints |
| `packages/server/src/auth.ts` | API key authentication |

## Development Workflow

### Running Tests

```bash
# Run all tests
pnpm test

# Run tests in watch mode
pnpm test:watch

# Run tests with coverage
pnpm test:coverage
```

### Building

```bash
# Build all packages
pnpm build
```

### Running the Server Locally

```bash
# Start development server
pnpm dev
```

## Test-Driven Development

This project follows TDD. When adding new features:

1. **Write failing tests first** in the appropriate test file
2. **Implement the feature** to make tests pass
3. **Refactor** if needed while keeping tests green

Test files mirror the source structure:
- `packages/server/src/parser.ts` → `packages/server/test/parser.test.ts`
- `packages/server/src/translator.ts` → `packages/server/test/translator.test.ts`

## Making Changes

### Adding a New Cypher Feature

1. Start with parser tests in `packages/server/test/parser.test.ts`
2. Implement parsing in `packages/server/src/parser.ts`
3. Add translator tests in `packages/server/test/translator.test.ts`
4. Implement SQL translation in `packages/server/src/translator.ts`
5. Add integration tests in `packages/server/test/integration.test.ts`

### Code Style

- TypeScript for all code
- Follow existing patterns in the codebase
- Keep code simple and readable over clever
- Use parameterized SQL queries everywhere (prevent injection)

## Submitting Changes

1. Create a new branch for your feature:
   ```bash
   git checkout -b feature/my-new-feature
   ```

2. Make your changes and commit with clear messages:
   ```bash
   git commit -m "Add support for XYZ in Cypher parser"
   ```

3. Ensure all tests pass:
   ```bash
   pnpm test
   ```

4. Push to your fork:
   ```bash
   git push origin feature/my-new-feature
   ```

5. Open a Pull Request on GitHub

### Pull Request Guidelines

- Provide a clear description of what your PR does
- Reference any related issues
- Ensure tests pass
- Keep PRs focused - one feature or fix per PR

## Reporting Issues

When reporting bugs, please include:

- Steps to reproduce
- Expected behavior
- Actual behavior
- Node.js version
- Operating system

## Questions?

Feel free to open an issue for questions or discussion.
