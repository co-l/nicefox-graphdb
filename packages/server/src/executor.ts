// Query Executor - Full pipeline: Cypher → Parse → Translate → Execute → Format

import { parse, ParseResult, Query } from "./parser.js";
import { translate, TranslationResult, Translator } from "./translator.js";
import { GraphDatabase } from "./db.js";

// ============================================================================
// Types
// ============================================================================

export interface ExecutionResult {
  success: true;
  data: Record<string, unknown>[];
  meta: {
    count: number;
    time_ms: number;
  };
}

export interface ExecutionError {
  success: false;
  error: {
    message: string;
    position?: number;
    line?: number;
    column?: number;
  };
}

export type QueryResponse = ExecutionResult | ExecutionError;

// ============================================================================
// Executor
// ============================================================================

export class Executor {
  private db: GraphDatabase;

  constructor(db: GraphDatabase) {
    this.db = db;
  }

  /**
   * Execute a Cypher query and return formatted results
   */
  execute(cypher: string, params: Record<string, unknown> = {}): QueryResponse {
    const startTime = performance.now();

    try {
      // 1. Parse the Cypher query
      const parseResult = parse(cypher);
      if (!parseResult.success) {
        return {
          success: false,
          error: {
            message: parseResult.error.message,
            position: parseResult.error.position,
            line: parseResult.error.line,
            column: parseResult.error.column,
          },
        };
      }

      // 2. Translate to SQL
      const translator = new Translator(params);
      const translation = translator.translate(parseResult.query);

      // 3. Execute SQL statements
      let rows: Record<string, unknown>[] = [];
      const returnColumns = translation.returnColumns;

      this.db.transaction(() => {
        for (const stmt of translation.statements) {
          const result = this.db.execute(stmt.sql, stmt.params);

          // If this is a SELECT (RETURN clause), capture the results
          if (result.rows.length > 0 || stmt.sql.trim().toUpperCase().startsWith("SELECT")) {
            rows = result.rows;
          }
        }
      });

      // 4. Format results
      const formattedRows = this.formatResults(rows, returnColumns);

      const endTime = performance.now();

      return {
        success: true,
        data: formattedRows,
        meta: {
          count: formattedRows.length,
          time_ms: Math.round((endTime - startTime) * 100) / 100,
        },
      };
    } catch (error) {
      return {
        success: false,
        error: {
          message: error instanceof Error ? error.message : String(error),
        },
      };
    }
  }

  /**
   * Format raw database results into a more usable structure
   */
  private formatResults(
    rows: Record<string, unknown>[],
    returnColumns?: string[]
  ): Record<string, unknown>[] {
    return rows.map((row) => {
      const formatted: Record<string, unknown> = {};

      for (const [key, value] of Object.entries(row)) {
        formatted[key] = this.deepParseJson(value);
      }

      return formatted;
    });
  }

  /**
   * Recursively parse JSON strings in a value
   */
  private deepParseJson(value: unknown): unknown {
    if (typeof value === "string") {
      try {
        const parsed = JSON.parse(value);
        // Recursively process if it's an object or array
        if (typeof parsed === "object" && parsed !== null) {
          return this.deepParseJson(parsed);
        }
        return parsed;
      } catch {
        // Not valid JSON, return as-is
        return value;
      }
    }

    if (Array.isArray(value)) {
      return value.map((item) => this.deepParseJson(item));
    }

    if (typeof value === "object" && value !== null) {
      const result: Record<string, unknown> = {};
      for (const [k, v] of Object.entries(value)) {
        result[k] = this.deepParseJson(v);
      }
      return result;
    }

    return value;
  }
}

// ============================================================================
// Convenience function
// ============================================================================

export function executeQuery(
  db: GraphDatabase,
  cypher: string,
  params: Record<string, unknown> = {}
): QueryResponse {
  return new Executor(db).execute(cypher, params);
}
