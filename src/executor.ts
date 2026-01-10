// Query Executor - Full pipeline: Cypher → Parse → Translate → Execute → Format

import {
  parse,
  ParseResult,
  Query,
  Clause,
  MatchClause,
  CreateClause,
  MergeClause,
  SetClause,
  DeleteClause,
  ReturnClause,
  WithClause,
  UnwindClause,
  NodePattern,
  RelationshipPattern,
  PathExpression,
  SetAssignment,
  Expression,
  PropertyValue,
  ParameterRef,
  VariableRef,
  WhereCondition,
  ReturnItem,
} from "./parser.js";
import { translate, TranslationResult, Translator } from "./translator.js";
import { GraphDatabase } from "./db.js";

// ============================================================================
// Timezone Helpers
// ============================================================================

/**
 * Check if a string is an IANA timezone name (contains '/')
 */
function isIANATimezone(tz: string): boolean {
  // IANA timezones like 'Europe/Stockholm', 'America/New_York'
  // Offsets are like '+01:00', '-08:00', 'Z'
  return tz.includes('/') && !tz.startsWith('+') && !tz.startsWith('-');
}

/**
 * Convert IANA timezone to UTC offset for a given date.
 * Returns offset string like '+01:00' or '-05:00'.
 * If conversion fails, returns '+00:00'.
 */
function getTimezoneOffset(timezone: string, year: number, month: number, day: number, hour: number, minute: number = 0): string {
  if (!isIANATimezone(timezone)) {
    return timezone;
  }
  
  try {
    // Create a UTC date for the given local time in the timezone
    // We need to find what UTC offset the timezone has at this local time
    const date = new Date(Date.UTC(year, month - 1, day, hour, minute, 0));
    
    // Use Intl to get the offset
    const formatter = new Intl.DateTimeFormat('en-US', {
      timeZone: timezone,
      timeZoneName: 'longOffset'
    });
    
    const parts = formatter.formatToParts(date);
    const tzPart = parts.find(p => p.type === 'timeZoneName');
    
    if (tzPart && tzPart.value) {
      // Extract offset from "GMT+01:00" or "GMT-05:00" or "GMT"
      const match = tzPart.value.match(/GMT([+-]\d{2}:\d{2})?/);
      if (match) {
        return match[1] || '+00:00'; // 'GMT' alone means +00:00
      }
    }
    
    return '+00:00'; // Fallback
  } catch {
    return '+00:00'; // If timezone is invalid, return UTC
  }
}

// ============================================================================
// Types
// ============================================================================

/**
 * Phase execution context - carries variable values between phases
 * 
 * Cypher is inherently sequential. Each clause operates on variables from
 * previous clauses. This context tracks:
 * - nodeIds: Map variable names to node IDs (for nodes we've created/matched)
 * - edgeIds: Map variable names to edge IDs (for edges we've created/matched)
 * - values: Map variable names to arbitrary values (scalars, lists, aggregates)
 * - rows: The current "row set" - multiple rows from MATCH or UNWIND
 */
export interface PhaseContext {
  // Maps variable names to node IDs
  nodeIds: Map<string, string>;
  // Maps variable names to edge IDs  
  edgeIds: Map<string, string>;
  // Maps variable names to any value (scalars, lists, objects, etc.)
  values: Map<string, unknown>;
  // Current row set - each row is a map of variable -> value
  // This is critical for handling UNWIND, MATCH, etc. that produce multiple rows
  rows: Array<Map<string, unknown>>;
}

/**
 * Create an empty phase context
 */
function createEmptyContext(): PhaseContext {
  return {
    nodeIds: new Map(),
    edgeIds: new Map(),
    values: new Map(),
    rows: [new Map()], // Start with single empty row
  };
}

/**
 * Clone a phase context
 */
function cloneContext(ctx: PhaseContext): PhaseContext {
  return {
    nodeIds: new Map(ctx.nodeIds),
    edgeIds: new Map(ctx.edgeIds),
    values: new Map(ctx.values),
    rows: ctx.rows.map(row => new Map(row)),
  };
}

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
  private propertyCache = new Map<string, Record<string, unknown>>();

  constructor(db: GraphDatabase) {
    this.db = db;
  }

  /**
   * Get node properties from cache or parse from JSON string and cache them
   */
  private getNodeProperties(nodeId: string, propsJson: string | object): Record<string, unknown> {
    let props = this.propertyCache.get(nodeId);
    if (!props) {
      props = typeof propsJson === "string" ? JSON.parse(propsJson) : propsJson;
      if (props && typeof props === "object" && !Array.isArray(props)) {
        this.propertyCache.set(nodeId, props);
      } else {
        // Fallback for invalid data
        props = {};
      }
    }
    return props || {};
  }

  /**
   * Execute a Cypher query and return formatted results
   */
  execute(cypher: string, params: Record<string, unknown> = {}): QueryResponse {
    const startTime = performance.now();
    // Clear property cache at start of each query execution
    this.propertyCache.clear();

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

      // 2. Try phase-based execution for complex multi-phase queries
      const phasedResult = this.tryPhasedExecution(parseResult.query, params);
      if (phasedResult !== null) {
        const endTime = performance.now();
        return {
          success: true,
          data: phasedResult,
          meta: {
            count: phasedResult.length,
            time_ms: Math.round((endTime - startTime) * 100) / 100,
          },
        };
      }

      // 2.1. Check for UNWIND with CREATE pattern (needs special handling)
      const unwindCreateResult = this.tryUnwindCreateExecution(parseResult.query, params);
      if (unwindCreateResult !== null) {
        const endTime = performance.now();
        return {
          success: true,
          data: unwindCreateResult,
          meta: {
            count: unwindCreateResult.length,
            time_ms: Math.round((endTime - startTime) * 100) / 100,
          },
        };
      }

      // 2.2. Check for UNWIND with MERGE pattern (needs special handling)
      const unwindMergeResult = this.tryUnwindMergeExecution(parseResult.query, params);
      if (unwindMergeResult !== null) {
        const endTime = performance.now();
        return {
          success: true,
          data: unwindMergeResult,
          meta: {
            count: unwindMergeResult.length,
            time_ms: Math.round((endTime - startTime) * 100) / 100,
          },
        };
      }

      // 2.3. Check for MATCH+WITH(COLLECT)+UNWIND+RETURN pattern (needs subquery for aggregates)
      const collectUnwindResult = this.tryCollectUnwindExecution(parseResult.query, params);
      if (collectUnwindResult !== null) {
        const endTime = performance.now();
        return {
          success: true,
          data: collectUnwindResult,
          meta: {
            count: collectUnwindResult.length,
            time_ms: Math.round((endTime - startTime) * 100) / 100,
          },
        };
      }

      // 2.4. Check for MATCH+WITH(COLLECT)+DELETE[expr] pattern
      const collectDeleteResult = this.tryCollectDeleteExecution(parseResult.query, params);
      if (collectDeleteResult !== null) {
        const endTime = performance.now();
        return {
          success: true,
          data: collectDeleteResult,
          meta: {
            count: collectDeleteResult.length,
            time_ms: Math.round((endTime - startTime) * 100) / 100,
          },
        };
      }

      // 2.5. Check for CREATE...RETURN pattern (needs special handling)
      const createReturnResult = this.tryCreateReturnExecution(parseResult.query, params);
      if (createReturnResult !== null) {
        const endTime = performance.now();
        return {
          success: true,
          data: createReturnResult,
          meta: {
            count: createReturnResult.length,
            time_ms: Math.round((endTime - startTime) * 100) / 100,
          },
        };
      }

      // 2.5. Check for MERGE with ON CREATE SET / ON MATCH SET (needs special handling)
      const mergeResult = this.tryMergeExecution(parseResult.query, params);
      if (mergeResult !== null) {
        const endTime = performance.now();
        return {
          success: true,
          data: mergeResult,
          meta: {
            count: mergeResult.length,
            time_ms: Math.round((endTime - startTime) * 100) / 100,
          },
        };
      }

      // 2.6. Check for MATCH...WITH...MATCH pattern with bound relationship list
      // e.g., MATCH ()-[r1]->()-[r2]->() WITH [r1, r2] AS rs MATCH (a)-[rs*]->(b) RETURN a, b
      const boundRelListResult = this.tryBoundRelationshipListExecution(parseResult.query, params);
      if (boundRelListResult !== null) {
        const endTime = performance.now();
        return {
          success: true,
          data: boundRelListResult,
          meta: {
            count: boundRelListResult.length,
            time_ms: Math.round((endTime - startTime) * 100) / 100,
          },
        };
      }

      // 3. Check if this is a pattern that needs multi-phase execution
      // (MATCH...CREATE, MATCH...SET, MATCH...DELETE with relationship patterns)
      const multiPhaseResult = this.tryMultiPhaseExecution(parseResult.query, params);
      if (multiPhaseResult !== null) {
        const endTime = performance.now();
        return {
          success: true,
          data: multiPhaseResult,
          meta: {
            count: multiPhaseResult.length,
            time_ms: Math.round((endTime - startTime) * 100) / 100,
          },
        };
      }

      // 3. Standard single-phase execution: Translate to SQL
      const translator = new Translator(params);
      const translation = translator.translate(parseResult.query);

      // 4. Execute SQL statements
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

      // 5. Format results
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

  // ============================================================================
  // Phase-Based Execution
  // ============================================================================

  /**
   * Check if a query needs phase-based execution and execute it if so.
   * 
   * Phase boundaries are detected when:
   * 1. A WITH clause contains an aggregate function (collect, count, sum, etc.)
   * 2. An UNWIND clause references a variable that doesn't exist yet (from previous phase)
   * 3. A clause references a variable computed from aggregation
   * 
   * Returns null if the query can be handled by standard SQL translation.
   */
  private tryPhasedExecution(
    query: Query,
    params: Record<string, unknown>
  ): Record<string, unknown>[] | null {
    const phases = this.detectPhases(query);
    
    // Semantic validation: Check if MERGE tries to use a variable already bound by MATCH
    this.validateMergeVariables(query);

    // Semantic validation: SET expressions cannot reference undefined variables
    this.validateSetClauseValueVariables(query, params);

    // Semantic validation: ORDER BY expressions cannot reference undefined or out-of-scope variables
    this.validateOrderByVariables(query, params);
    
    // Check if we need phased execution for MATCH + MERGE combinations
    // These need special handling for proper Cartesian product semantics
    // BUT: If MERGE has ON CREATE SET or ON MATCH SET, let tryMergeExecution handle it
    const hasMatch = query.clauses.some(c => c.type === "MATCH");
    const hasCreate = query.clauses.some(c => c.type === "CREATE");
    const hasMerge = query.clauses.some(c => c.type === "MERGE");
    const hasWithClause = query.clauses.some(c => c.type === "WITH");
    const mergeClause = query.clauses.find(c => c.type === "MERGE") as MergeClause | undefined;
    const mergeHasSetClauses = mergeClause?.onCreateSet || mergeClause?.onMatchSet;
    const needsMergePhasedExecution = (hasMatch || hasCreate) && hasMerge && !mergeHasSetClauses;
    
    // CREATE + WITH needs phased execution because SQL translation can't properly
    // reference the created node's data (UUID is generated at runtime)
    const needsCreateWithPhasedExecution = hasCreate && hasWithClause && !hasMatch;
    
    // If only one phase and no special combo, standard execution can handle it
    if (phases.length <= 1 && !needsMergePhasedExecution && !needsCreateWithPhasedExecution) {
      return null;
    }
    
    // For MATCH/CREATE+MERGE, execute all clauses in sequence using phased execution
    const clausesToExecute = needsMergePhasedExecution && phases.length <= 1 
      ? [query.clauses]  // All clauses as one phase, but processed by phased executor
      : phases;
    
    // Check if query has a RETURN clause
    const hasReturn = query.clauses.some(c => c.type === "RETURN");
    
    // Execute phases sequentially
    let context = createEmptyContext();
    
    this.db.transaction(() => {
      for (let i = 0; i < clausesToExecute.length; i++) {
        const phase = clausesToExecute[i];
        const isLastPhase = i === clausesToExecute.length - 1;
        context = this.executePhase(phase, context, params, isLastPhase);
      }
    });
    
    // If no RETURN clause, return empty array (write-only query)
    if (!hasReturn) {
      return [];
    }
    
    // Convert context rows to result format
    return this.contextToResults(context);
  }

  /**
   * Validate MERGE variables - cannot MERGE a variable that's already bound by MATCH
   * 
   * The rule is: you cannot MERGE a node pattern that is just a variable already
   * bound by MATCH. MATCH guarantees the node exists, so MERGE would be meaningless.
   * 
   * However, you CAN use already-bound variables in:
   * - Relationship MERGE endpoints: MATCH (a), (b) MERGE (a)-[:REL]->(b) is valid
   * - Subsequent MERGE clauses: MERGE (c) MERGE (c) is valid (second one just matches)
   * 
   * Additionally: you cannot impose new predicates (labels/properties) on a variable
   * that is already bound. E.g., CREATE (a:Foo) MERGE (a)-[:R]->(a:Bar) is invalid
   * because it tries to impose label :Bar on variable 'a' which is already bound.
   */
  private validateMergeVariables(query: Query): void {
    // Track variables bound by MATCH clauses (not CREATE/MERGE)
    const matchBoundVariables = new Set<string>();
    // Track ALL bound variables (MATCH, CREATE, MERGE) for label/property conflict detection
    const allBoundVariables = new Set<string>();
    
    for (const clause of query.clauses) {
      if (clause.type === "MATCH" || clause.type === "OPTIONAL_MATCH") {
        // Collect all variables bound by MATCH
        for (const pattern of clause.patterns) {
          this.collectPatternVariables(pattern, matchBoundVariables);
          this.collectPatternVariables(pattern, allBoundVariables);
        }
      } else if (clause.type === "CREATE") {
        // Collect variables bound by CREATE (for label/property conflict detection)
        for (const pattern of clause.patterns) {
          this.collectPatternVariables(pattern, allBoundVariables);
        }
      } else if (clause.type === "MERGE") {
        // Check if MERGE tries to use a MATCH-bound variable as a standalone node pattern
        for (const pattern of clause.patterns) {
          this.checkMergePatternForBoundVariables(pattern, matchBoundVariables);
          // Also check for imposing new predicates on any bound variable
          this.checkMergePatternForLabelConflicts(pattern, allBoundVariables);
        }
        // Add MERGE variables to allBoundVariables for subsequent clauses
        for (const pattern of clause.patterns) {
          this.collectPatternVariables(pattern, allBoundVariables);
        }
        // Note: We don't add MERGE variables to matchBoundVariables
        // because MERGE (c) followed by MERGE (c) is valid
      }
    }
  }

  /**
   * Validate that SET assignment values do not reference undefined variables.
   *
   * Example (invalid):
   *   MATCH (a) SET a.name = missing RETURN a
   */
  private validateSetClauseValueVariables(query: Query, params: Record<string, unknown>): void {
    this.validateSetClauseValueVariablesInQuery(query, params);
  }

  /**
   * Validate that ORDER BY expressions do not reference undefined or out-of-scope variables.
   *
   * Examples (invalid):
   *   MATCH (a), (b) WITH a ORDER BY b RETURN a
   *   MATCH (a) RETURN a.name AS name ORDER BY missing
   */
  private validateOrderByVariables(query: Query, params: Record<string, unknown>): void {
    this.validateOrderByVariablesInQuery(query, params);
  }

  private validateOrderByVariablesInQuery(query: Query, params: Record<string, unknown>): void {
    let scope = new Set<string>();

    for (const clause of query.clauses) {
      if (clause.type === "UNION") {
        this.validateOrderByVariablesInQuery(clause.left, params);
        this.validateOrderByVariablesInQuery(clause.right, params);
        return;
      }

      if (clause.type === "MATCH" || clause.type === "OPTIONAL_MATCH") {
        for (const pattern of clause.patterns) this.collectPatternVariables(pattern, scope);
        if (clause.pathExpressions) {
          for (const pathExpr of clause.pathExpressions) {
            scope.add(pathExpr.variable);
            for (const pattern of pathExpr.patterns) this.collectPatternVariables(pattern, scope);
          }
        }
        continue;
      }

      if (clause.type === "CREATE") {
        for (const pattern of clause.patterns) this.collectPatternVariables(pattern, scope);
        continue;
      }

      if (clause.type === "MERGE") {
        for (const pattern of clause.patterns) this.collectPatternVariables(pattern, scope);
        if (clause.pathExpressions) {
          for (const pathExpr of clause.pathExpressions) {
            scope.add(pathExpr.variable);
            for (const pattern of pathExpr.patterns) this.collectPatternVariables(pattern, scope);
          }
        }
        continue;
      }

      if (clause.type === "UNWIND") {
        scope.add(clause.alias);
        continue;
      }

      if (clause.type === "CALL") {
        for (const y of clause.yields || []) scope.add(y);
        continue;
      }

      if (clause.type === "WITH") {
        // Reset scope to WITH projections; `WITH *` preserves incoming scope.
        const hasStar = clause.items.some(
          (item) => item.expression.type === "variable" && item.expression.variable === "*" && !item.alias
        );

        const nextScope = hasStar ? new Set(scope) : new Set<string>();
        for (const item of clause.items) {
          if (item.alias) {
            nextScope.add(item.alias);
          } else if (item.expression.type === "variable" && item.expression.variable && item.expression.variable !== "*") {
            nextScope.add(item.expression.variable);
          }
        }

        // ORDER BY in WITH may reference both incoming variables and projected aliases
        if (clause.orderBy) {
          const orderScope = new Set(scope);
          for (const v of nextScope) orderScope.add(v);
          for (const orderItem of clause.orderBy) {
            this.validateExpressionVariablesInScope(orderItem.expression, orderScope, params);
          }
        }

        scope = nextScope;
        continue;
      }

      if (clause.type === "RETURN") {
        if (clause.orderBy) {
          // ORDER BY in RETURN can reference RETURN aliases
          const orderScope = new Set(scope);
          for (const item of clause.items) {
            if (item.alias) orderScope.add(item.alias);
          }
          for (const orderItem of clause.orderBy) {
            this.validateExpressionVariablesInScope(orderItem.expression, orderScope, params);
          }
        }
        continue;
      }
    }
  }

  private validateSetClauseValueVariablesInQuery(query: Query, params: Record<string, unknown>): void {
    let scope = new Set<string>();

    for (const clause of query.clauses) {
      if (clause.type === "UNION") {
        this.validateSetClauseValueVariablesInQuery(clause.left, params);
        this.validateSetClauseValueVariablesInQuery(clause.right, params);
        return;
      }

      if (clause.type === "MATCH" || clause.type === "OPTIONAL_MATCH") {
        for (const pattern of clause.patterns) this.collectPatternVariables(pattern, scope);
        if (clause.pathExpressions) {
          for (const pathExpr of clause.pathExpressions) {
            scope.add(pathExpr.variable);
            for (const pattern of pathExpr.patterns) this.collectPatternVariables(pattern, scope);
          }
        }
        continue;
      }

      if (clause.type === "CREATE") {
        for (const pattern of clause.patterns) this.collectPatternVariables(pattern, scope);
        continue;
      }

      if (clause.type === "MERGE") {
        for (const pattern of clause.patterns) this.collectPatternVariables(pattern, scope);
        if (clause.pathExpressions) {
          for (const pathExpr of clause.pathExpressions) {
            scope.add(pathExpr.variable);
            for (const pattern of pathExpr.patterns) this.collectPatternVariables(pattern, scope);
          }
        }
        continue;
      }

      if (clause.type === "UNWIND") {
        scope.add(clause.alias);
        continue;
      }

      if (clause.type === "WITH") {
        // Reset scope to WITH projections; `WITH *` preserves incoming scope.
        const hasStar = clause.items.some(
          (item) => item.expression.type === "variable" && item.expression.variable === "*" && !item.alias
        );

        const nextScope = hasStar ? new Set(scope) : new Set<string>();
        for (const item of clause.items) {
          if (item.alias) {
            nextScope.add(item.alias);
          } else if (item.expression.type === "variable" && item.expression.variable && item.expression.variable !== "*") {
            nextScope.add(item.expression.variable);
          }
        }

        scope = nextScope;
        continue;
      }

      if (clause.type === "SET") {
        for (const assignment of clause.assignments) {
          if (!scope.has(assignment.variable)) {
            throw new Error(`SyntaxError: Variable \`${assignment.variable}\` not defined`);
          }
          if (assignment.value) {
            this.validateExpressionVariablesInScope(assignment.value, scope, params);
          }
        }
      }
    }
  }

  private validateExpressionVariablesInScope(
    expression: Expression,
    scope: Set<string>,
    params: Record<string, unknown>
  ): void {
    switch (expression.type) {
      case "literal":
      case "parameter":
        return;

      case "variable": {
        const name = expression.variable ?? expression.name;
        if (name && !scope.has(name)) {
          throw new Error(`SyntaxError: Variable \`${name}\` not defined`);
        }
        return;
      }

      case "property": {
        if (expression.variable && !scope.has(expression.variable)) {
          throw new Error(`SyntaxError: Variable \`${expression.variable}\` not defined`);
        }
        return;
      }

      case "propertyAccess": {
        if (expression.object) this.validateExpressionVariablesInScope(expression.object, scope, params);
        return;
      }

      case "indexAccess": {
        if (expression.array) this.validateExpressionVariablesInScope(expression.array, scope, params);
        if (expression.index) this.validateExpressionVariablesInScope(expression.index, scope, params);
        return;
      }

      case "unary": {
        if (expression.operand) this.validateExpressionVariablesInScope(expression.operand, scope, params);
        return;
      }

      case "binary":
      case "comparison": {
        if (expression.left) this.validateExpressionVariablesInScope(expression.left, scope, params);
        if (expression.right) this.validateExpressionVariablesInScope(expression.right, scope, params);
        return;
      }

      case "function": {
        for (const arg of expression.args || []) {
          this.validateExpressionVariablesInScope(arg, scope, params);
        }
        return;
      }

      case "object": {
        for (const prop of expression.properties || []) {
          this.validateExpressionVariablesInScope(prop.value, scope, params);
        }
        return;
      }

      case "case": {
        if (expression.expression) {
          this.validateExpressionVariablesInScope(expression.expression, scope, params);
        }
        for (const when of expression.whens || []) {
          this.validateWhereConditionVariablesInScope(when.condition, scope, params);
          this.validateExpressionVariablesInScope(when.result, scope, params);
        }
        if (expression.elseExpr) {
          this.validateExpressionVariablesInScope(expression.elseExpr, scope, params);
        }
        return;
      }

      case "listComprehension":
      case "listPredicate": {
        if (expression.listExpr) this.validateExpressionVariablesInScope(expression.listExpr, scope, params);
        const nextScope = new Set(scope);
        if (expression.variable) nextScope.add(expression.variable);
        if (expression.filterCondition) {
          this.validateWhereConditionVariablesInScope(expression.filterCondition, nextScope, params);
        }
        if (expression.type === "listComprehension" && expression.mapExpr) {
          this.validateExpressionVariablesInScope(expression.mapExpr, nextScope, params);
        }
        return;
      }

      case "labelPredicate": {
        if (expression.variable && !scope.has(expression.variable)) {
          throw new Error(`SyntaxError: Variable \`${expression.variable}\` not defined`);
        }
        return;
      }
    }
  }

  private validateWhereConditionVariablesInScope(
    condition: WhereCondition,
    scope: Set<string>,
    params: Record<string, unknown>
  ): void {
    if (condition.type === "comparison") {
      if (condition.left) this.validateExpressionVariablesInScope(condition.left, scope, params);
      if (condition.right) this.validateExpressionVariablesInScope(condition.right, scope, params);
      return;
    }

    if (condition.type === "and" || condition.type === "or") {
      for (const c of condition.conditions || []) {
        this.validateWhereConditionVariablesInScope(c, scope, params);
      }
      return;
    }

    if (condition.type === "not") {
      if (condition.condition) this.validateWhereConditionVariablesInScope(condition.condition, scope, params);
    }
  }
  
  /**
   * Collect all node and relationship variables from a pattern
   */
  private collectPatternVariables(pattern: NodePattern | RelationshipPattern, variables: Set<string>): void {
    if (this.isRelationshipPattern(pattern)) {
      // Relationship pattern - collect source, target, and edge variables
      if (pattern.source?.variable) {
        variables.add(pattern.source.variable);
      }
      if (pattern.target?.variable) {
        variables.add(pattern.target.variable);
      }
      if (pattern.edge?.variable) {
        variables.add(pattern.edge.variable);
      }
    } else {
      // Node pattern
      const nodePattern = pattern as NodePattern;
      if (nodePattern.variable) {
        variables.add(nodePattern.variable);
      }
    }
  }
  
  /**
   * Check if a MERGE pattern tries to use an already-bound variable
   */
  private checkMergePatternForBoundVariables(pattern: NodePattern | RelationshipPattern, boundVariables: Set<string>): void {
    if (this.isRelationshipPattern(pattern)) {
      // For relationship patterns - source and target can be bound (that's the point)
      // but the edge variable itself cannot be already bound
      if (pattern.edge?.variable && boundVariables.has(pattern.edge.variable)) {
        throw new Error(`Cannot merge relationship using variable '${pattern.edge.variable}' that is already bound`);
      }
    } else {
      // For simple node patterns - the node variable cannot be already bound
      const nodePattern = pattern as NodePattern;
      if (nodePattern.variable && boundVariables.has(nodePattern.variable)) {
        throw new Error(`Cannot merge node using variable '${nodePattern.variable}' that is already bound`);
      }
    }
  }
  
  /**
   * Check if a MERGE pattern tries to impose new labels/properties on an already-bound variable.
   * E.g., CREATE (a:Foo) MERGE (a)-[:R]->(a:Bar) is invalid because :Bar conflicts with :Foo
   */
  private checkMergePatternForLabelConflicts(pattern: NodePattern | RelationshipPattern, boundVariables: Set<string>): void {
    if (this.isRelationshipPattern(pattern)) {
      // Check source node - if variable is bound and has label/properties, that's a conflict
      if (pattern.source?.variable && boundVariables.has(pattern.source.variable)) {
        if (pattern.source.label || (pattern.source.properties && Object.keys(pattern.source.properties).length > 0)) {
          throw new Error(`Variable \`${pattern.source.variable}\` already declared`);
        }
      }
      // Check target node - if variable is bound and has label/properties, that's a conflict
      if (pattern.target?.variable && boundVariables.has(pattern.target.variable)) {
        if (pattern.target.label || (pattern.target.properties && Object.keys(pattern.target.properties).length > 0)) {
          throw new Error(`Variable \`${pattern.target.variable}\` already declared`);
        }
      }
    }
    // For simple node patterns, the other check already handles it
  }

  /**
   * Validate that MERGE properties in the AST don't contain explicit null values.
   * MERGE cannot use null property values because null = null is undefined in Cypher.
   * This validates the original AST properties, not the resolved values,
   * to distinguish between explicit null (invalid) and variable references that may resolve to null.
   */
  private validateMergeAstPropertiesNotNull(props: Record<string, unknown>, context: string, params: Record<string, unknown>): void {
    for (const [key, value] of Object.entries(props)) {
      // Check for explicit null in AST
      if (value === null) {
        throw new Error(`Cannot merge the following ${context} because of null property key '${key}': null property values are not allowed in MERGE`);
      }
      // Check for parameter that resolves to null
      if (typeof value === "object" && value !== null) {
        const typed = value as { type?: string; name?: string };
        if (typed.type === "parameter" && typed.name && params[typed.name] === null) {
          throw new Error(`Cannot merge the following ${context} because of null property key '${key}': null property values are not allowed in MERGE`);
        }
      }
    }
  }

  /**
   * Detect phase boundaries in a query.
   * 
   * A phase boundary occurs when:
   * - A WITH clause contains aggregate functions (collect, count, sum, avg, min, max)
   * - An UNWIND clause references a variable from a previous WITH aggregate
   */
  private detectPhases(query: Query): Clause[][] {
    const clauses = query.clauses;
    const phases: Clause[][] = [];
    let currentPhase: Clause[] = [];
    
    // Track variables that are computed from aggregates
    const aggregateVariables = new Set<string>();
    // Track variables that are computed from non-deterministic expressions (like rand())
    const nonDeterministicVariables = new Set<string>();
    // Track all known variables in current phase
    const knownVariables = new Set<string>();
    // Track if we've seen OPTIONAL_MATCH (and no regular MATCH) before this WITH
    // This is for the case: OPTIONAL MATCH ... WITH ... OPTIONAL MATCH
    // where the first OPTIONAL MATCH might return null rows
    let hasOnlyOptionalMatchBeforeWith = false;
    let hasRegularMatchBeforeWith = false;
    
    for (let i = 0; i < clauses.length; i++) {
      const clause = clauses[i];
      
      // Check if this clause needs to start a new phase
      let needsNewPhase = false;
      
      if (clause.type === "UNWIND") {
        // Check if UNWIND expression references an aggregate variable
        const referencedVars = this.getExpressionVariables(clause.expression);
        for (const v of referencedVars) {
          if (aggregateVariables.has(v)) {
            needsNewPhase = true;
            break;
          }
        }
      }
      
      // Check if MATCH after a WITH with aggregates - needs phase boundary
      // This handles patterns like:
      //   MATCH ... WITH me, count(...) AS H1, you MATCH ... RETURN sum(... H1 ...)
      // The second MATCH needs to be in a new phase to properly use the aggregated H1 value
      if (clause.type === "MATCH" && i > 0 && aggregateVariables.size > 0) {
        // Check if previous clause is WITH
        const prevClause = clauses[i - 1];
        if (prevClause.type === "WITH") {
          needsNewPhase = true;
        }
      }
      
      // Check if MATCH after a WITH with SKIP or LIMIT - needs phase boundary
      // This handles patterns like:
      //   MATCH (a) WITH a.name AS property ORDER BY property SKIP 1 MATCH (b) WHERE b.id = idToUse
      // The second MATCH needs to be in a new phase because SKIP affects the row set before joining
      if (clause.type === "MATCH" && i > 0) {
        const prevClause = clauses[i - 1];
        if (prevClause.type === "WITH" && (prevClause.skip !== undefined || prevClause.limit !== undefined)) {
          needsNewPhase = true;
        }
      }
      
      // Check if OPTIONAL_MATCH after a WITH that followed only OPTIONAL_MATCH (no regular MATCH)
      // This pattern needs phased execution for proper OPTIONAL MATCH null semantics
      // Example: OPTIONAL MATCH (a) WITH a OPTIONAL MATCH (a)-->(b) RETURN b
      // The first OPTIONAL MATCH with no data should produce 1 row with a=null
      if (clause.type === "OPTIONAL_MATCH" && i > 0) {
        const prevClause = clauses[i - 1];
        if (prevClause.type === "WITH" && hasOnlyOptionalMatchBeforeWith && !hasRegularMatchBeforeWith) {
          needsNewPhase = true;
        }
      }
      
      // Check if WITH after a WITH with aggregation AND WHERE - needs phase boundary
      // This handles patterns like:
      //   MATCH ... WITH x, count(*) AS c WHERE c > 1 WITH x WHERE x.name <> 'foo' RETURN count(*)
      // The first WITH with aggregation creates a grouping context, and the WHERE on aggregates
      // (HAVING) needs to be applied before the second WITH is processed.
      // 
      // Note: We only add the phase boundary when the previous WITH has a WHERE clause,
      // because without WHERE, the aggregation can be handled in SQL (no HAVING needed).
      if (clause.type === "WITH" && i > 0) {
        const prevClause = clauses[i - 1];
        if (prevClause.type === "WITH") {
          const prevWithClause = prevClause as WithClause;
          const prevHasAggregate = prevWithClause.items.some(item => 
            this.expressionHasAggregate(item.expression)
          );
          // Only force phase boundary if the aggregating WITH has a WHERE (HAVING)
          if (prevHasAggregate && prevWithClause.where) {
            needsNewPhase = true;
          }
        }
      }
      
      // Check if WITH/UNWIND/RETURN after a WITH with non-deterministic expressions - needs phase boundary
      // This handles patterns like:
      //   WITH [y IN list WHERE rand() > 0.5 | y] AS filtered WITH filtered + x AS result
      // The rand() in the list comprehension must be evaluated once and the result used in subsequent clauses.
      // Without phased execution, the translator would re-expand the expression, calling rand() multiple times.
      // 
      // HOWEVER: If the referencing clause has aggregation, we should NOT create a phase boundary,
      // because the aggregation needs to operate on all the rows from the previous clause.
      // In that case, the translator can still use SQL aggregation semantics.
      if ((clause.type === "WITH" || clause.type === "UNWIND" || clause.type === "RETURN") && i > 0 && nonDeterministicVariables.size > 0) {
        // Don't trigger for clauses with aggregation - let them be handled by the translator
        const hasAggregation = clause.type === "WITH" && 
          clause.items.some(item => this.expressionHasAggregate(item.expression));
        
        if (!hasAggregation) {
          // Check if this clause references any non-deterministic variables
          const referencedVars = this.getClauseReferencedVariables(clause);
          for (const v of referencedVars) {
            if (nonDeterministicVariables.has(v)) {
              needsNewPhase = true;
              break;
            }
          }
        }
      }
      
      if (needsNewPhase && currentPhase.length > 0) {
        phases.push(currentPhase);
        currentPhase = [];
        hasOnlyOptionalMatchBeforeWith = false; // Reset for new phase
        hasRegularMatchBeforeWith = false;
      }
      
      currentPhase.push(clause);
      
      // Track MATCH types in this phase
      if (clause.type === "OPTIONAL_MATCH") {
        hasOnlyOptionalMatchBeforeWith = true;
      } else if (clause.type === "MATCH") {
        hasRegularMatchBeforeWith = true;
      }
      
      // Track variables defined by this clause
      this.trackClauseVariables(clause, knownVariables, aggregateVariables);
      
      // Check if WITH with aggregate - marks end of a phase if next clause references aggregates
      if (clause.type === "WITH") {
        const hasAggregate = clause.items.some(item => 
          this.expressionHasAggregate(item.expression)
        );
        
        if (hasAggregate) {
          // Mark all aliased items as aggregate variables
          for (const item of clause.items) {
            if (item.alias) {
              aggregateVariables.add(item.alias);
            }
          }
        }
        
        // Also check for non-deterministic expressions
        for (const item of clause.items) {
          if (item.alias && this.expressionHasNonDeterministic(item.expression)) {
            nonDeterministicVariables.add(item.alias);
          }
        }
      }
    }
    
    // Add final phase
    if (currentPhase.length > 0) {
      phases.push(currentPhase);
    }
    
    return phases;
  }

  /**
   * Get all variable names referenced in an expression
   */
  private getExpressionVariables(expr: Expression): Set<string> {
    const vars = new Set<string>();
    
    if (expr.type === "variable" && expr.variable && expr.variable !== "*") {
      vars.add(expr.variable);
    } else if (expr.type === "property" && expr.variable) {
      vars.add(expr.variable);
    } else if (expr.type === "function" && expr.args) {
      for (const arg of expr.args) {
        for (const v of this.getExpressionVariables(arg)) {
          vars.add(v);
        }
      }
    } else if (expr.type === "binary" && expr.left && expr.right) {
      for (const v of this.getExpressionVariables(expr.left)) {
        vars.add(v);
      }
      for (const v of this.getExpressionVariables(expr.right)) {
        vars.add(v);
      }
    } else if (expr.type === "comparison" && expr.left && expr.right) {
      // Handle comparison expressions like: a = b, x > y, etc.
      for (const v of this.getExpressionVariables(expr.left)) {
        vars.add(v);
      }
      for (const v of this.getExpressionVariables(expr.right)) {
        vars.add(v);
      }
    } else if (expr.type === "listPredicate") {
      // Handle ALL/ANY/NONE/SINGLE(x IN list WHERE condition)
      // Note: expr.variable is the loop variable (x), not a referenced variable
      if (expr.listExpr) {
        for (const v of this.getExpressionVariables(expr.listExpr)) {
          vars.add(v);
        }
      }
      // filterCondition may reference external variables (not the loop variable)
      if (expr.filterCondition) {
        const filterVars = this.getWhereVariables(expr.filterCondition as WhereCondition);
        for (const v of filterVars) {
          // Don't include the loop variable itself
          if (v !== expr.variable) {
            vars.add(v);
          }
        }
      }
    } else if (expr.type === "listComprehension") {
      // Handle [x IN list WHERE cond | mapExpr]
      if (expr.listExpr) {
        for (const v of this.getExpressionVariables(expr.listExpr)) {
          vars.add(v);
        }
      }
      if (expr.filterCondition) {
        const filterVars = this.getWhereVariables(expr.filterCondition as WhereCondition);
        for (const v of filterVars) {
          if (v !== expr.variable) {
            vars.add(v);
          }
        }
      }
      if (expr.mapExpr) {
        for (const v of this.getExpressionVariables(expr.mapExpr)) {
          if (v !== expr.variable) {
            vars.add(v);
          }
        }
      }
    } else if (expr.type === "case") {
      // Handle CASE WHEN ... THEN ... ELSE ... END
      if (expr.expression) {
        for (const v of this.getExpressionVariables(expr.expression)) {
          vars.add(v);
        }
      }
      if (expr.whens) {
        for (const when of expr.whens) {
          // CaseWhen has condition (WhereCondition) and result (Expression)
          if (when.condition) {
            const condVars = this.getWhereVariables(when.condition);
            for (const v of condVars) {
              vars.add(v);
            }
          }
          if (when.result) {
            for (const v of this.getExpressionVariables(when.result)) {
              vars.add(v);
            }
          }
        }
      }
      if (expr.elseExpr) {
        for (const v of this.getExpressionVariables(expr.elseExpr)) {
          vars.add(v);
        }
      }
    } else if (expr.type === "unary" && expr.operand) {
      // Handle NOT expr, -expr, etc.
      for (const v of this.getExpressionVariables(expr.operand)) {
        vars.add(v);
      }
    } else if (expr.type === "propertyAccess" && expr.object) {
      // Handle expr.property
      for (const v of this.getExpressionVariables(expr.object)) {
        vars.add(v);
      }
    } else if (expr.type === "indexAccess") {
      // Handle list[index]
      if (expr.array) {
        for (const v of this.getExpressionVariables(expr.array)) {
          vars.add(v);
        }
      }
      if (expr.index) {
        for (const v of this.getExpressionVariables(expr.index)) {
          vars.add(v);
        }
      }
    } else if (expr.type === "object" && expr.properties) {
      // Handle object literals {a: expr1, b: expr2}
      for (const prop of expr.properties) {
        if (prop.value) {
          for (const v of this.getExpressionVariables(prop.value)) {
            vars.add(v);
          }
        }
      }
    } else if (expr.type === "in") {
      // Handle value IN list
      if (expr.left) {
        for (const v of this.getExpressionVariables(expr.left)) {
          vars.add(v);
        }
      }
      if (expr.list) {
        for (const v of this.getExpressionVariables(expr.list)) {
          vars.add(v);
        }
      }
    }
    
    return vars;
  }

  /**
   * Get all variable names referenced in a WHERE condition
   */
  private getWhereVariables(cond: WhereCondition): Set<string> {
    const vars = new Set<string>();
    
    if (cond.left) {
      for (const v of this.getExpressionVariables(cond.left)) {
        vars.add(v);
      }
    }
    if (cond.right) {
      for (const v of this.getExpressionVariables(cond.right)) {
        vars.add(v);
      }
    }
    if (cond.list) {
      for (const v of this.getExpressionVariables(cond.list)) {
        vars.add(v);
      }
    }
    if (cond.listExpr) {
      for (const v of this.getExpressionVariables(cond.listExpr)) {
        vars.add(v);
      }
    }
    if (cond.conditions) {
      for (const subCond of cond.conditions) {
        for (const v of this.getWhereVariables(subCond)) {
          vars.add(v);
        }
      }
    }
    if (cond.condition) {
      for (const v of this.getWhereVariables(cond.condition)) {
        vars.add(v);
      }
    }
    if (cond.filterCondition) {
      for (const v of this.getWhereVariables(cond.filterCondition)) {
        vars.add(v);
      }
    }
    
    return vars;
  }

  /**
   * Get all variables referenced by a clause (not defined, just referenced)
   */
  private getClauseReferencedVariables(clause: Clause): Set<string> {
    const vars = new Set<string>();
    
    if (clause.type === "WITH") {
      for (const item of clause.items) {
        for (const v of this.getExpressionVariables(item.expression)) {
          vars.add(v);
        }
      }
      // Also check WHERE clause for variable references
      if (clause.where) {
        for (const v of this.getWhereVariables(clause.where)) {
          vars.add(v);
        }
      }
    } else if (clause.type === "UNWIND") {
      for (const v of this.getExpressionVariables(clause.expression)) {
        vars.add(v);
      }
    } else if (clause.type === "RETURN") {
      for (const item of clause.items) {
        for (const v of this.getExpressionVariables(item.expression)) {
          vars.add(v);
        }
      }
    }
    
    return vars;
  }

  /**
   * Track variables defined by a clause
   */
  private trackClauseVariables(
    clause: Clause,
    knownVariables: Set<string>,
    aggregateVariables: Set<string>
  ): void {
    switch (clause.type) {
      case "CREATE":
      case "MERGE":
        for (const pattern of clause.patterns) {
          if (this.isRelationshipPattern(pattern)) {
            if (pattern.source.variable) knownVariables.add(pattern.source.variable);
            if (pattern.target.variable) knownVariables.add(pattern.target.variable);
            if (pattern.edge.variable) knownVariables.add(pattern.edge.variable);
          } else if (pattern.variable) {
            knownVariables.add(pattern.variable);
          }
        }
        break;
        
      case "MATCH":
      case "OPTIONAL_MATCH":
        for (const pattern of clause.patterns) {
          if (this.isRelationshipPattern(pattern)) {
            if (pattern.source.variable) knownVariables.add(pattern.source.variable);
            if (pattern.target.variable) knownVariables.add(pattern.target.variable);
            if (pattern.edge.variable) knownVariables.add(pattern.edge.variable);
          } else if (pattern.variable) {
            knownVariables.add(pattern.variable);
          }
        }
        break;
        
      case "UNWIND":
        knownVariables.add(clause.alias);
        break;
        
      case "WITH":
        // WITH resets scope - only aliased items are visible after
        for (const item of clause.items) {
          if (item.alias) {
            knownVariables.add(item.alias);
          } else if (item.expression.type === "variable" && item.expression.variable) {
            knownVariables.add(item.expression.variable);
          }
        }
        break;
    }
  }

  /**
   * Check if an expression contains aggregate functions
   */
  private expressionHasAggregate(expr: Expression): boolean {
    if (expr.type === "function") {
      const funcName = expr.functionName?.toUpperCase();
      if (["COLLECT", "COUNT", "SUM", "AVG", "MIN", "MAX"].includes(funcName || "")) {
        return true;
      }
      // Check args recursively
      if (expr.args) {
        return expr.args.some(arg => this.expressionHasAggregate(arg));
      }
    } else if (expr.type === "binary") {
      const leftHas = expr.left ? this.expressionHasAggregate(expr.left) : false;
      const rightHas = expr.right ? this.expressionHasAggregate(expr.right) : false;
      return leftHas || rightHas;
    }
    return false;
  }

  /**
   * Check if a WhereCondition contains non-deterministic functions
   */
  private whereConditionHasNonDeterministic(cond: WhereCondition): boolean {
    if (cond.left && this.expressionHasNonDeterministic(cond.left)) return true;
    if (cond.right && this.expressionHasNonDeterministic(cond.right)) return true;
    if (cond.conditions) {
      for (const c of cond.conditions) {
        if (this.whereConditionHasNonDeterministic(c)) return true;
      }
    }
    if (cond.condition && this.whereConditionHasNonDeterministic(cond.condition)) return true;
    if (cond.list && this.expressionHasNonDeterministic(cond.list)) return true;
    if (cond.listExpr && this.expressionHasNonDeterministic(cond.listExpr)) return true;
    if (cond.filterCondition && this.whereConditionHasNonDeterministic(cond.filterCondition)) return true;
    return false;
  }

  /**
   * Check if an expression contains non-deterministic functions (like rand())
   * These expressions cannot be safely inlined by the translator and require
   * phased execution to materialize intermediate results.
   */
  private expressionHasNonDeterministic(expr: Expression): boolean {
    if (expr.type === "function") {
      const funcName = expr.functionName?.toUpperCase();
      // rand() and randomUUID() are non-deterministic
      if (["RAND", "RANDOMUUID"].includes(funcName || "")) {
        return true;
      }
      // Check args recursively
      if (expr.args) {
        return expr.args.some(arg => this.expressionHasNonDeterministic(arg));
      }
    } else if (expr.type === "binary") {
      const leftHas = expr.left ? this.expressionHasNonDeterministic(expr.left) : false;
      const rightHas = expr.right ? this.expressionHasNonDeterministic(expr.right) : false;
      return leftHas || rightHas;
    } else if (expr.type === "case") {
      // Check expression and else branches
      if (expr.expression && this.expressionHasNonDeterministic(expr.expression)) return true;
      if (expr.elseExpr && this.expressionHasNonDeterministic(expr.elseExpr)) return true;
      if (expr.whens) {
        for (const when of expr.whens) {
          // CaseWhen.condition is a WhereCondition, not Expression
          if (this.whereConditionHasNonDeterministic(when.condition)) return true;
          if (this.expressionHasNonDeterministic(when.result)) return true;
        }
      }
      return false;
    } else if (expr.type === "listComprehension" || expr.type === "listPredicate") {
      // Check the list expression and filter condition
      if (expr.listExpr && this.expressionHasNonDeterministic(expr.listExpr)) return true;
      if (expr.filterCondition && this.whereConditionHasNonDeterministic(expr.filterCondition)) return true;
      if (expr.mapExpr && this.expressionHasNonDeterministic(expr.mapExpr)) return true;
      return false;
    } else if (expr.type === "comparison") {
      if (expr.left && this.expressionHasNonDeterministic(expr.left)) return true;
      if (expr.right && this.expressionHasNonDeterministic(expr.right)) return true;
      return false;
    }
    return false;
  }

  /**
   * Execute a single phase with the given context
   */
  private executePhase(
    clauses: Clause[],
    inputContext: PhaseContext,
    params: Record<string, unknown>,
    isLastPhase: boolean
  ): PhaseContext {
    let context = cloneContext(inputContext);
    
    for (const clause of clauses) {
      context = this.executeClause(clause, context, params);
    }
    
    return context;
  }

  /**
   * Execute a single clause and update context
   */
  private executeClause(
    clause: Clause,
    context: PhaseContext,
    params: Record<string, unknown>
  ): PhaseContext {
    switch (clause.type) {
      case "CREATE":
        return this.executeCreateClause(clause, context, params);
      case "MERGE":
        return this.executeMergeClause(clause, context, params);
      case "WITH":
        return this.executeWithClause(clause, context, params);
      case "UNWIND":
        return this.executeUnwindClause(clause, context, params);
      case "MATCH":
      case "OPTIONAL_MATCH":
        return this.executeMatchClause(clause, context, params);
      case "RETURN":
        return this.executeReturnClause(clause, context, params);
      case "SET":
        return this.executeSetClause(clause, context, params);
      case "DELETE":
        return this.executeDeleteClause(clause, context, params);
      default:
        // For unsupported clause types, return context unchanged
        return context;
    }
  }

  /**
   * Execute CREATE clause
   */
  private executeCreateClause(
    clause: CreateClause,
    context: PhaseContext,
    params: Record<string, unknown>
  ): PhaseContext {
    const newContext = cloneContext(context);
    const newRows: Array<Map<string, unknown>> = [];
    
    // For each row in the current context, execute the CREATE
    for (const row of context.rows) {
      const rowContext = new Map(row);
      
      for (const pattern of clause.patterns) {
        if (this.isRelationshipPattern(pattern)) {
          this.executeCreateRelationshipInContext(pattern, rowContext, newContext, params);
        } else {
          this.executeCreateNodeInContext(pattern as NodePattern, rowContext, newContext, params);
        }
      }
      
      newRows.push(rowContext);
    }
    
    newContext.rows = newRows;
    return newContext;
  }

  /**
   * Execute MERGE clause
   * 
   * MERGE semantics:
   * - For each input row, find nodes/edges matching the MERGE pattern
   * - If match found, bind to existing nodes (creating Cartesian product with all matches)
   * - If no match, create new nodes/edges
   * 
   * For MERGE (b) with no label/properties after MATCH (a):
   * - If ANY nodes exist, match ALL of them → Cartesian product with input rows
   * - If NO nodes exist, create one node per input row
   */
  private executeMergeClause(
    clause: MergeClause,
    context: PhaseContext,
    params: Record<string, unknown>
  ): PhaseContext {
    const newContext = cloneContext(context);
    const newRows: Array<Map<string, unknown>> = [];
    
    for (const inputRow of context.rows) {
      for (const pattern of clause.patterns) {
        if (this.isRelationshipPattern(pattern)) {
          // Relationship MERGE - more complex, delegate to helper
          const mergeRows = this.executeMergeRelationshipInContext(
            pattern, inputRow, newContext, params
          );
          newRows.push(...mergeRows);
        } else {
          // Node MERGE
          const mergeRows = this.executeMergeNodeInContext(
            pattern as NodePattern, inputRow, newContext, params
          );
          newRows.push(...mergeRows);
        }
      }
    }
    
    // Handle path expressions - build path objects from the merged patterns
    if (clause.pathExpressions && clause.pathExpressions.length > 0) {
      for (const row of newRows) {
        for (const pathExpr of clause.pathExpressions) {
          const path = this.buildPathFromPatterns(pathExpr.patterns, row);
          if (path !== null) {
            row.set(pathExpr.variable, path);
          }
        }
      }
    }
    
    newContext.rows = newRows;
    return newContext;
  }
  
  /**
   * Build a path object from relationship patterns.
   * Returns an array alternating [nodeProps, edgeProps, nodeProps, ...]
   * or null if the path cannot be constructed.
   */
  private buildPathFromPatterns(
    patterns: (NodePattern | RelationshipPattern)[],
    row: Map<string, unknown>
  ): unknown[] | null {
    if (patterns.length === 0) return null;
    
    const pathElements: unknown[] = [];
    
    for (const pattern of patterns) {
      if (this.isRelationshipPattern(pattern)) {
        const relPattern = pattern as RelationshipPattern;
        
        // Get source node
        const sourceVar = relPattern.source.variable;
        const sourceNode = sourceVar ? row.get(sourceVar) : null;
        if (!sourceNode || typeof sourceNode !== 'object') continue;
        
        // Add source node props (excluding internal _nf_id)
        const sourceProps = { ...(sourceNode as Record<string, unknown>) };
        delete sourceProps._nf_id;
        if (pathElements.length === 0) {
          pathElements.push(sourceProps);
        }
        
        // Get edge
        const edgeVar = relPattern.edge.variable;
        const edge = edgeVar ? row.get(edgeVar) : null;
        const edgeProps = edge && typeof edge === 'object' 
          ? { ...(edge as Record<string, unknown>) }
          : {};
        delete (edgeProps as Record<string, unknown>)._nf_id;
        pathElements.push(edgeProps);
        
        // Get target node
        const targetVar = relPattern.target.variable;
        const targetNode = targetVar ? row.get(targetVar) : null;
        if (!targetNode || typeof targetNode !== 'object') continue;
        
        const targetProps = { ...(targetNode as Record<string, unknown>) };
        delete targetProps._nf_id;
        pathElements.push(targetProps);
      }
    }
    
    return pathElements.length > 0 ? pathElements : null;
  }

  /**
   * Execute MERGE for a node pattern within a row context
   * Returns multiple rows if multiple nodes match (Cartesian product)
   */
  private executeMergeNodeInContext(
    pattern: NodePattern,
    inputRow: Map<string, unknown>,
    globalContext: PhaseContext,
    params: Record<string, unknown>
  ): Array<Map<string, unknown>> {
    // If variable is already bound, just use the bound value (no-op match)
    // This is valid for cases like: MERGE (c) MERGE (c) - second MERGE just matches
    if (pattern.variable && inputRow.has(pattern.variable)) {
      return [new Map(inputRow)];
    }
    
    const props = this.resolvePropertiesInContext(pattern.properties || {}, inputRow, params);
    
    // Build query to find existing matching nodes
    const conditions: string[] = [];
    const conditionParams: unknown[] = [];
    
    // Label condition
    if (pattern.label) {
      const labels = Array.isArray(pattern.label) ? pattern.label : [pattern.label];
      for (const label of labels) {
        conditions.push(`EXISTS (SELECT 1 FROM json_each(label) WHERE value = ?)`);
        conditionParams.push(label);
      }
    }
    
    // Property conditions
    for (const [key, value] of Object.entries(props)) {
      conditions.push(`json_extract(properties, '$.${key}') = ?`);
      conditionParams.push(value);
    }
    
    // Find existing nodes
    const findSql = conditions.length > 0
      ? `SELECT id, label, properties FROM nodes WHERE ${conditions.join(" AND ")}`
      : `SELECT id, label, properties FROM nodes`;
    
    const findResult = this.db.execute(findSql, conditionParams);
    
    if (findResult.rows.length > 0) {
      // Nodes found - create Cartesian product (one output row per found node)
      const outputRows: Array<Map<string, unknown>> = [];
      
      for (const row of findResult.rows) {
        const outputRow = new Map(inputRow);
        
        if (pattern.variable) {
           const nodeProps = this.getNodeProperties(
             typeof row.id === "string" ? row.id : "", 
             typeof row.properties === "string" || (typeof row.properties === "object" && row.properties !== null) ? row.properties : "{}"
           );
          const nodeObj = { ...nodeProps, _nf_id: row.id };
          outputRow.set(pattern.variable, nodeObj);
        }
        
        outputRows.push(outputRow);
      }
      
      return outputRows;
    } else {
      // No nodes found - create one
      const id = crypto.randomUUID();
      const labelJson = this.normalizeLabelToJson(pattern.label);
      
      this.db.execute(
        "INSERT INTO nodes (id, label, properties) VALUES (?, ?, ?)",
        [id, labelJson, JSON.stringify(props)]
      );
      
      const outputRow = new Map(inputRow);
      if (pattern.variable) {
        const nodeObj = { ...props, _nf_id: id };
        outputRow.set(pattern.variable, nodeObj);
        globalContext.nodeIds.set(pattern.variable, id);
      }
      
      return [outputRow];
    }
  }

  /**
   * Execute MERGE for a relationship pattern within a row context
   * Returns multiple rows if multiple matches found
   */
  private executeMergeRelationshipInContext(
    pattern: RelationshipPattern,
    inputRow: Map<string, unknown>,
    globalContext: PhaseContext,
    params: Record<string, unknown>
  ): Array<Map<string, unknown>> {
    // MERGE requires a relationship type
    if (!pattern.edge.type) {
      throw new Error("MERGE requires a relationship type");
    }
    
    // For relationship MERGE, we need to:
    // 1. Resolve source and target nodes (from context or create)
    // 2. Find or create the relationship
    
    // Get source node
    let sourceId: string | null = null;
    if (pattern.source.variable && inputRow.has(pattern.source.variable)) {
      sourceId = this.extractNodeId(inputRow.get(pattern.source.variable));
    }
    
    // Get target node
    let targetId: string | null = null;
    if (pattern.target.variable && inputRow.has(pattern.target.variable)) {
      targetId = this.extractNodeId(inputRow.get(pattern.target.variable));
    }
    
    // If source or target not in context, we need to find/create them
    // This is a simplified implementation - full MERGE relationship semantics is complex
    
    if (!sourceId || !targetId) {
      // Can't do relationship MERGE without both endpoints
      // Just return the input row unchanged
      return [inputRow];
    }
    
    const edgeType = pattern.edge.type || "";
    
    // Validate no explicit null properties in MERGE (check AST before resolution)
    this.validateMergeAstPropertiesNotNull(pattern.edge.properties || {}, "relationship", params);
    
    const edgeProps = this.resolvePropertiesInContext(pattern.edge.properties || {}, inputRow, params);
    
    // Adjust for direction
    const [actualSource, actualTarget] = 
      pattern.edge.direction === "left" ? [targetId, sourceId] : [sourceId, targetId];
    
    // Build property conditions for the WHERE clause
    let propConditions = "";
    const propParams: unknown[] = [];
    for (const [key, value] of Object.entries(edgeProps)) {
      if (Array.isArray(value) || (typeof value === "object" && value !== null)) {
        // For arrays and objects, compare JSON representations
        propConditions += ` AND json_extract(properties, '$.${key}') = json(?)`;
        propParams.push(JSON.stringify(value));
      } else {
        propConditions += ` AND json_extract(properties, '$.${key}') = ?`;
        propParams.push(value);
      }
    }
    
    // Find existing edge - for undirected patterns, check both directions
    let findSql: string;
    let findParams: unknown[];
    
    if (pattern.edge.direction === "none") {
      // Undirected: check both source→target and target→source
      findSql = `SELECT id, properties, source_id, target_id FROM edges WHERE type = ? AND (
        (source_id = ? AND target_id = ?)${propConditions} OR 
        (source_id = ? AND target_id = ?)${propConditions}
      )`;
      findParams = [edgeType, actualSource, actualTarget, ...propParams, actualTarget, actualSource, ...propParams];
    } else {
      // Directed: only check specified direction
      findSql = `SELECT id, properties, source_id, target_id FROM edges WHERE source_id = ? AND target_id = ? AND type = ?${propConditions}`;
      findParams = [actualSource, actualTarget, edgeType, ...propParams];
    }
    
    const findResult = this.db.execute(findSql, findParams);
    
    if (findResult.rows.length > 0) {
      // Edges found - create Cartesian product (one output row per found edge)
      const outputRows: Array<Map<string, unknown>> = [];
      
      for (const edgeRow of findResult.rows) {
        const outputRow = new Map(inputRow);
        
        if (pattern.edge.variable) {
          const props = typeof edgeRow.properties === "string"
            ? JSON.parse(edgeRow.properties)
            : edgeRow.properties;
          // Include _nf_start and _nf_end for startNode() and endNode() functions
          // Use the actual source/target from the found edge (may be reversed for undirected)
          outputRow.set(pattern.edge.variable, { 
            ...props, 
            _nf_id: edgeRow.id,
            _nf_start: edgeRow.source_id,
            _nf_end: edgeRow.target_id
          });
        }
        
        outputRows.push(outputRow);
      }
      
      return outputRows;
    } else {
      // No edges found - create one
      const outputRow = new Map(inputRow);
      const edgeId = crypto.randomUUID();
      this.db.execute(
        "INSERT INTO edges (id, type, source_id, target_id, properties) VALUES (?, ?, ?, ?, ?)",
        [edgeId, edgeType, actualSource, actualTarget, JSON.stringify(edgeProps)]
      );
      
      if (pattern.edge.variable) {
        // Include _nf_start and _nf_end for startNode() and endNode() functions
        outputRow.set(pattern.edge.variable, { 
          ...edgeProps, 
          _nf_id: edgeId,
          _nf_start: actualSource,
          _nf_end: actualTarget
        });
        globalContext.edgeIds.set(pattern.edge.variable, edgeId);
      }
      
      return [outputRow];
    }
  }

  /**
   * Execute CREATE for a single node pattern within a row context
   */
  private executeCreateNodeInContext(
    pattern: NodePattern,
    rowContext: Map<string, unknown>,
    globalContext: PhaseContext,
    params: Record<string, unknown>
  ): void {
    const id = crypto.randomUUID();
    const labelJson = this.normalizeLabelToJson(pattern.label);
    const props = this.resolvePropertiesInContext(pattern.properties || {}, rowContext, params);
    
    this.db.execute(
      "INSERT INTO nodes (id, label, properties) VALUES (?, ?, ?)",
      [id, labelJson, JSON.stringify(props)]
    );
    
    if (pattern.variable) {
      // Store as a node object with _nf_id for consistency with MATCH output
      const nodeObj = { ...props, _nf_id: id };
      rowContext.set(pattern.variable, nodeObj);
      globalContext.nodeIds.set(pattern.variable, id);
    }
  }

  /**
   * Extract a node ID from a value that could be:
   * - A raw UUID string
   * - A JSON string like '{"name":"Alice","_nf_id":"uuid"}'
   * - An object with _nf_id property
   */
  private extractNodeId(value: unknown): string | null {
    if (typeof value === "string") {
      // Try parsing as JSON to extract _nf_id
      try {
        const parsed = JSON.parse(value);
        if (typeof parsed === "object" && parsed !== null && "_nf_id" in parsed) {
          return parsed._nf_id as string;
        }
      } catch {
        // Not JSON, assume it's a raw ID
        return value;
      }
      return value;
    }
    
    if (typeof value === "object" && value !== null && "_nf_id" in value) {
      return (value as Record<string, unknown>)._nf_id as string;
    }
    
    return null;
  }

  /**
   * Execute CREATE for a relationship pattern within a row context
   */
  private executeCreateRelationshipInContext(
    pattern: RelationshipPattern,
    rowContext: Map<string, unknown>,
    globalContext: PhaseContext,
    params: Record<string, unknown>
  ): void {
    let sourceId: string;
    let targetId: string;
    
    // Resolve source node
    if (pattern.source.variable && rowContext.has(pattern.source.variable)) {
      const nodeValue = rowContext.get(pattern.source.variable);
      const extractedId = this.extractNodeId(nodeValue);
      if (extractedId) {
        sourceId = extractedId;
      } else {
        throw new Error(`Cannot resolve source node ID from variable ${pattern.source.variable}`);
      }
    } else {
      // Create new source node
      sourceId = crypto.randomUUID();
      const props = this.resolvePropertiesInContext(pattern.source.properties || {}, rowContext, params);
      this.db.execute(
        "INSERT INTO nodes (id, label, properties) VALUES (?, ?, ?)",
        [sourceId, this.normalizeLabelToJson(pattern.source.label), JSON.stringify(props)]
      );
      if (pattern.source.variable) {
        rowContext.set(pattern.source.variable, sourceId);
        globalContext.nodeIds.set(pattern.source.variable, sourceId);
      }
    }
    
    // Resolve target node
    if (pattern.target.variable && rowContext.has(pattern.target.variable)) {
      const nodeValue = rowContext.get(pattern.target.variable);
      const extractedId = this.extractNodeId(nodeValue);
      if (extractedId) {
        targetId = extractedId;
      } else {
        throw new Error(`Cannot resolve target node ID from variable ${pattern.target.variable}`);
      }
    } else {
      // Create new target node
      targetId = crypto.randomUUID();
      const props = this.resolvePropertiesInContext(pattern.target.properties || {}, rowContext, params);
      this.db.execute(
        "INSERT INTO nodes (id, label, properties) VALUES (?, ?, ?)",
        [targetId, this.normalizeLabelToJson(pattern.target.label), JSON.stringify(props)]
      );
      if (pattern.target.variable) {
        rowContext.set(pattern.target.variable, targetId);
        globalContext.nodeIds.set(pattern.target.variable, targetId);
      }
    }
    
    // Handle direction
    const [actualSource, actualTarget] = 
      pattern.edge.direction === "left" ? [targetId, sourceId] : [sourceId, targetId];
    
    // Create edge
    const edgeId = crypto.randomUUID();
    const edgeType = pattern.edge.type || "";
    const edgeProps = this.resolvePropertiesInContext(pattern.edge.properties || {}, rowContext, params);
    
    this.db.execute(
      "INSERT INTO edges (id, type, source_id, target_id, properties) VALUES (?, ?, ?, ?, ?)",
      [edgeId, edgeType, actualSource, actualTarget, JSON.stringify(edgeProps)]
    );
    
    if (pattern.edge.variable) {
      rowContext.set(pattern.edge.variable, edgeId);
      globalContext.edgeIds.set(pattern.edge.variable, edgeId);
    }
  }

  /**
   * Execute WITH clause
   */
  private executeWithClause(
    clause: WithClause,
    context: PhaseContext,
    params: Record<string, unknown>
  ): PhaseContext {
    const newContext = cloneContext(context);
    
    // Check for aggregates - they collapse rows by grouping keys
    const hasAggregate = clause.items.some(item => 
      this.expressionHasAggregate(item.expression)
    );
    
    if (hasAggregate) {
      // Aggregate mode with grouping: group by non-aggregate expressions
      // For each unique combination of grouping keys, compute aggregates
      
      // Identify grouping keys (non-aggregate expressions) and aggregate expressions
      const groupingItems: { alias: string; expression: Expression }[] = [];
      const aggregateItems: { alias: string; expression: Expression }[] = [];
      
      for (const item of clause.items) {
        const alias = item.alias || this.getExpressionName(item.expression);
        if (this.expressionHasAggregate(item.expression)) {
          aggregateItems.push({ alias, expression: item.expression });
        } else {
          groupingItems.push({ alias, expression: item.expression });
        }
      }
      
      // Group rows by grouping key values
      const groups = new Map<string, Array<Map<string, unknown>>>();
      
      for (const row of context.rows) {
        // Compute the grouping key (JSON string of grouping values)
        const keyValues: Record<string, unknown> = {};
        for (const { alias, expression } of groupingItems) {
          keyValues[alias] = this.evaluateExpressionInRow(expression, row, params);
        }
        const groupKey = JSON.stringify(keyValues);
        
        if (!groups.has(groupKey)) {
          groups.set(groupKey, []);
        }
        groups.get(groupKey)!.push(row);
      }
      
      // For each group, compute aggregates and produce output row
      const outputRows: Array<Map<string, unknown>> = [];
      
      for (const [groupKey, groupRows] of groups) {
        const outputRow = new Map<string, unknown>();
        
        // Add grouping values
        const groupKeyValues = JSON.parse(groupKey);
        for (const alias of Object.keys(groupKeyValues)) {
          outputRow.set(alias, groupKeyValues[alias]);
        }
        
        // Compute aggregates for this group
        for (const { alias, expression } of aggregateItems) {
          const value = this.evaluateAggregateExpression(expression, groupRows, params);
          outputRow.set(alias, value);
        }
        
        // Apply WHERE filter (HAVING semantics) after aggregation
        if (clause.where) {
          const passes = this.evaluateWhereInRow(clause.where, outputRow, params);
          if (!passes) continue;
        }
        
        outputRows.push(outputRow);
      }
      
      newContext.rows = outputRows;
    } else {
      // Non-aggregate mode: transform each row
      let newRows: Array<Map<string, unknown>> = [];
      
      for (const row of context.rows) {
        // Apply WHERE filter if present
        if (clause.where) {
          const passes = this.evaluateWhereInRow(clause.where, row, params);
          if (!passes) continue;
        }
        
        const outputRow = new Map<string, unknown>();
        
        // Handle WITH * - pass through all variables
        const hasWildcard = clause.items.some(item => 
          item.expression.type === "variable" && item.expression.variable === "*"
        );
        
        if (hasWildcard) {
          // Copy all variables from input row
          for (const [key, value] of row) {
            outputRow.set(key, value);
          }
        }
        
        // Process each WITH item
        for (const item of clause.items) {
          if (item.expression.type === "variable" && item.expression.variable === "*") {
            continue; // Already handled
          }
          
          const alias = item.alias || this.getExpressionName(item.expression);
          const value = this.evaluateExpressionInRow(item.expression, row, params);
          outputRow.set(alias, value);
        }
        
        newRows.push(outputRow);
      }
      
      // Apply ORDER BY if present
      if (clause.orderBy && clause.orderBy.length > 0) {
        newRows = this.sortRowsByOrderBy(newRows, clause.orderBy, params);
      }
      
      // Apply SKIP if present
      if (clause.skip !== undefined) {
        const skipValue = this.evaluateLiteralExpression(clause.skip, params);
        if (typeof skipValue === "number" && skipValue > 0) {
          newRows = newRows.slice(skipValue);
        }
      }
      
      // Apply LIMIT if present
      if (clause.limit !== undefined) {
        const limitValue = this.evaluateLiteralExpression(clause.limit, params);
        if (typeof limitValue === "number" && limitValue >= 0) {
          newRows = newRows.slice(0, limitValue);
        }
      }
      
      newContext.rows = newRows;
    }
    
    return newContext;
  }

  /**
   * Sort rows by ORDER BY expressions
   */
  private sortRowsByOrderBy(
    rows: Array<Map<string, unknown>>,
    orderBy: { expression: Expression; direction: "ASC" | "DESC" }[],
    params: Record<string, unknown>
  ): Array<Map<string, unknown>> {
    return [...rows].sort((a, b) => {
      for (const { expression, direction } of orderBy) {
        const aVal = this.evaluateExpressionInRow(expression, a, params);
        const bVal = this.evaluateExpressionInRow(expression, b, params);
        
        const cmp = this.compareValues(aVal, bVal);
        if (cmp !== 0) {
          return direction === "DESC" ? -cmp : cmp;
        }
      }
      return 0;
    });
  }

  /**
   * Compare two values for sorting (Cypher ordering semantics)
   */
  private compareValues(a: unknown, b: unknown): number {
    // Handle nulls - null comes first in Cypher
    if (a === null && b === null) return 0;
    if (a === null) return -1;
    if (b === null) return 1;
    
    // Same type comparison
    if (typeof a === "number" && typeof b === "number") {
      return a - b;
    }
    if (typeof a === "string" && typeof b === "string") {
      return a.localeCompare(b);
    }
    if (typeof a === "boolean" && typeof b === "boolean") {
      return (a ? 1 : 0) - (b ? 1 : 0);
    }
    
    // Different types - convert to string
    return String(a).localeCompare(String(b));
  }

  /**
   * Evaluate a literal expression (for SKIP/LIMIT)
   */
  private evaluateLiteralExpression(expr: Expression, params: Record<string, unknown>): unknown {
    if (expr.type === "literal") {
      return expr.value;
    }
    if (expr.type === "parameter") {
      return params[expr.name || ""];
    }
    return null;
  }

  /**
   * Execute UNWIND clause
   */
  private executeUnwindClause(
    clause: UnwindClause,
    context: PhaseContext,
    params: Record<string, unknown>
  ): PhaseContext {
    const newContext = cloneContext(context);
    const newRows: Array<Map<string, unknown>> = [];
    
    for (const row of context.rows) {
      // Evaluate the list expression in this row's context
      const listValue = this.evaluateExpressionInRow(clause.expression, row, params);
      
      if (!Array.isArray(listValue)) {
        // If not an array, treat as single-element
        const newRow = new Map(row);
        newRow.set(clause.alias, listValue);
        newRows.push(newRow);
      } else {
        // Expand the list into multiple rows
        for (const element of listValue) {
          const newRow = new Map(row);
          newRow.set(clause.alias, element);
          newRows.push(newRow);
        }
      }
    }
    
    newContext.rows = newRows;
    return newContext;
  }

  /**
   * Execute MATCH clause
   */
  private executeMatchClause(
    clause: MatchClause,
    context: PhaseContext,
    params: Record<string, unknown>
  ): PhaseContext {
    // Check for special case: variable-length pattern with pre-bound relationship list
    // e.g., MATCH (first)-[rs*]->(second) where rs is a list of relationships from WITH
    const boundRelListPattern = this.findBoundRelationshipListPattern(clause, context);
    if (boundRelListPattern) {
      return this.executeMatchWithBoundRelList(clause, context, boundRelListPattern, params);
    }
    
    // Get all variables referenced in the pattern (bound variables that must exist)
    // and variables introduced by the pattern (new variables)
    const boundVars = new Set<string>();
    const introducedVars = new Set<string>();
    
    for (const pattern of clause.patterns) {
      if (this.isRelationshipPattern(pattern)) {
        // Check each variable - if it's in context, it's bound; otherwise it's introduced
        if (pattern.source.variable) {
          if (context.rows.length > 0 && context.rows[0].has(pattern.source.variable)) {
            boundVars.add(pattern.source.variable);
          } else {
            introducedVars.add(pattern.source.variable);
          }
        }
        if (pattern.target.variable) {
          if (context.rows.length > 0 && context.rows[0].has(pattern.target.variable)) {
            boundVars.add(pattern.target.variable);
          } else {
            introducedVars.add(pattern.target.variable);
          }
        }
        if (pattern.edge.variable) {
          if (context.rows.length > 0 && context.rows[0].has(pattern.edge.variable)) {
            boundVars.add(pattern.edge.variable);
          } else {
            introducedVars.add(pattern.edge.variable);
          }
        }
      } else if (pattern.variable) {
        if (context.rows.length > 0 && context.rows[0].has(pattern.variable)) {
          boundVars.add(pattern.variable);
        } else {
          introducedVars.add(pattern.variable);
        }
      }
    }
    
    // For OPTIONAL_MATCH: if any bound variable is null in the context row,
    // the match cannot succeed - return null for introduced variables
    // This handles cases like: WITH a, b (where both are null) OPTIONAL MATCH (b)-[r]->(a)
    if (clause.type === "OPTIONAL_MATCH" && boundVars.size > 0) {
      const newContext = cloneContext(context);
      const newRows: Array<Map<string, unknown>> = [];
      
      for (const inputRow of context.rows) {
        // Check if any bound variable is null
        let anyBoundIsNull = false;
        for (const varName of boundVars) {
          const value = inputRow.get(varName);
          if (value === null || value === undefined) {
            anyBoundIsNull = true;
            break;
          }
        }
        
        if (anyBoundIsNull) {
          // Bound variable is null, so OPTIONAL MATCH returns null for new variables
          const newRow = new Map(inputRow);
          for (const varName of introducedVars) {
            newRow.set(varName, null);
          }
          newRows.push(newRow);
        } else {
          // Bound variables are not null - need to execute the match for this row
          // For now, add this row to be processed by SQL
          // Mark this row as needing SQL execution
          (inputRow as any).__needsSqlExecution = true;
          newRows.push(inputRow);
        }
      }
      
      // Check if all rows were handled without SQL
      const rowsNeedingSql = newRows.filter(r => (r as any).__needsSqlExecution);
      if (rowsNeedingSql.length === 0) {
        // All rows handled - return directly
        newContext.rows = newRows;
        return newContext;
      }
      
      // Some rows need SQL execution - continue to the SQL path below
      // But only for rows that need it
      // For simplicity, if any row needs SQL, run SQL for all and filter later
      // Clean up the marker
      for (const row of newRows) {
        delete (row as any).__needsSqlExecution;
      }
    }
    
    // For queries with bound variables, we need to execute the match for each input row
    // using that row's variable values as constraints
    const newContext = cloneContext(context);
    const newRows: Array<Map<string, unknown>> = [];
    
    // Check if WHERE references context variables (not pattern variables)
    const contextVarNames = new Set<string>();
    for (const inputRow of context.rows) {
      for (const [key, _] of inputRow) {
        contextVarNames.add(key);
      }
      break; // All rows have same keys
    }
    
    // Get variables referenced in WHERE that are context variables
    const whereReferencesContext = clause.where ? 
      this.whereReferencesContextVars(clause.where, contextVarNames, introducedVars) : false;
    
    if (boundVars.size > 0 || whereReferencesContext) {
      // For complex patterns (multi-hop, anonymous nodes), use SQL translation with 
      // constraints for bound variables. This handles patterns like:
      // WITH me, you MATCH (me)-[r1:ATE]->()<-[r2:ATE]-(you) 
      // where me and you are bound but the middle node is new.
      // Also handles: WITH x MATCH (n) WHERE n.id = x (WHERE refs context)
      
      // Execute match for each input row using SQL with bound variable constraints
      for (const inputRow of context.rows) {
        const matchResults = this.executeMatchWithSqlForRow(clause, inputRow, boundVars, introducedVars, params);
        
        if (matchResults.length > 0) {
          for (const matchResult of matchResults) {
            const outputRow = new Map(inputRow);
            for (const [key, value] of matchResult) {
              outputRow.set(key, value);
            }
            newRows.push(outputRow);
          }
        } else if (clause.type === "OPTIONAL_MATCH") {
          // No match found - add null for introduced variables
          const outputRow = new Map(inputRow);
          for (const varName of introducedVars) {
            outputRow.set(varName, null);
          }
          newRows.push(outputRow);
        }
        // For regular MATCH with no results, row is excluded
      }
      
      newContext.rows = newRows;
      return newContext;
    }
    
    // No bound variables and no context references - delegate to SQL translation for MATCH
    // This handles standalone MATCH clauses well
    
    const matchQuery: Query = {
      clauses: [
        clause,
        {
          type: "RETURN" as const,
          items: this.buildReturnItemsForMatch(clause),
        },
      ],
    };
    
    const translator = new Translator(params);
    const translation = translator.translate(matchQuery);
    
    for (const stmt of translation.statements) {
      const result = this.db.execute(stmt.sql, stmt.params);
      
      for (const sqlRow of result.rows) {
        // For each input row, create output rows with matched data
        for (const inputRow of context.rows) {
          const outputRow = new Map(inputRow);
          
          // Add matched variables to row
          for (const [key, value] of Object.entries(sqlRow)) {
            if (key.startsWith("_")) continue; // Skip internal columns
            outputRow.set(key, value);
          }
          
          newRows.push(outputRow);
        }
      }
    }
    
    if (newRows.length > 0) {
      newContext.rows = newRows;
    } else if (clause.type === "OPTIONAL_MATCH") {
      // Optional match with no results - keep input rows but add null for new variables
      // Add null values for introduced variables to each input row
      newContext.rows = context.rows.map(row => {
        const newRow = new Map(row);
        for (const varName of introducedVars) {
          if (!newRow.has(varName)) {
            newRow.set(varName, null);
          }
        }
        return newRow;
      });
    }
    
    return newContext;
  }

  /**
   * Execute a MATCH clause for a single input row, using bound variable values as constraints
   */
  private executeMatchForRow(
    clause: MatchClause,
    inputRow: Map<string, unknown>,
    boundVars: Set<string>,
    introducedVars: Set<string>,
    params: Record<string, unknown>
  ): Array<Map<string, unknown>> {
    const results: Array<Map<string, unknown>> = [];
    
    for (const pattern of clause.patterns) {
      if (this.isRelationshipPattern(pattern)) {
        // Relationship pattern with bound endpoints
        const relPattern = pattern as RelationshipPattern;
        
        // Get bound node IDs
        const sourceVar = relPattern.source.variable;
        const targetVar = relPattern.target.variable;
        const edgeVar = relPattern.edge.variable;
        
        let sourceId: string | null = null;
        let targetId: string | null = null;
        
        if (sourceVar && boundVars.has(sourceVar)) {
          sourceId = this.extractNodeId(inputRow.get(sourceVar));
        }
        if (targetVar && boundVars.has(targetVar)) {
          targetId = this.extractNodeId(inputRow.get(targetVar));
        }
        
        // If we have both bound nodes, find edges between them
        if (sourceId && targetId) {
          // Build edge query - for undirected pattern (a)--(b), match both directions
          let edgeSql: string;
          let edgeParams: unknown[];
          
          const edgeType = relPattern.edge.type;
          
          if (relPattern.edge.direction === "none") {
            // Undirected: match edges in either direction
            edgeSql = `SELECT id, type, source_id, target_id, properties FROM edges 
                       WHERE ((source_id = ? AND target_id = ?) OR (source_id = ? AND target_id = ?))`;
            edgeParams = [sourceId, targetId, targetId, sourceId];
            if (edgeType) {
              edgeSql += ` AND type = ?`;
              edgeParams.push(edgeType);
            }
          } else if (relPattern.edge.direction === "left") {
            // Left: (a)<--(b) means edge goes from b to a
            edgeSql = `SELECT id, type, source_id, target_id, properties FROM edges 
                       WHERE source_id = ? AND target_id = ?`;
            edgeParams = [targetId, sourceId];
            if (edgeType) {
              edgeSql += ` AND type = ?`;
              edgeParams.push(edgeType);
            }
          } else {
            // Right: (a)-->(b) means edge goes from a to b
            edgeSql = `SELECT id, type, source_id, target_id, properties FROM edges 
                       WHERE source_id = ? AND target_id = ?`;
            edgeParams = [sourceId, targetId];
            if (edgeType) {
              edgeSql += ` AND type = ?`;
              edgeParams.push(edgeType);
            }
          }
          
          const edgeResult = this.db.execute(edgeSql, edgeParams);
          
          for (const row of edgeResult.rows) {
            const matchRow = new Map<string, unknown>();
            
            if (edgeVar) {
              const edgeProps = typeof row.properties === "string"
                ? JSON.parse(row.properties)
                : row.properties;
              matchRow.set(edgeVar, { ...edgeProps, _nf_id: row.id });
            }
            
            results.push(matchRow);
          }
        }
      } else {
        // Node pattern - not typically used with bound vars in OPTIONAL MATCH after WITH
        // but handle for completeness
        const nodePattern = pattern as NodePattern;
        if (nodePattern.variable && boundVars.has(nodePattern.variable)) {
          // Node is already bound - just pass through
          results.push(new Map());
        }
      }
    }
    
    return results;
  }

  /**
   * Execute a MATCH clause for a single input row using SQL translation.
   * This handles complex patterns (multi-hop, anonymous nodes) by:
   * 1. Translating the MATCH to SQL
   * 2. Adding WHERE constraints for bound variables (matching by node ID)
   * 3. Executing and returning results
   */
  private executeMatchWithSqlForRow(
    clause: MatchClause,
    inputRow: Map<string, unknown>,
    boundVars: Set<string>,
    introducedVars: Set<string>,
    params: Record<string, unknown>
  ): Array<Map<string, unknown>> {
    // Create a map of context variable names for quick lookup
    const contextVarNames = new Set<string>();
    for (const [key, _] of inputRow) {
      contextVarNames.add(key);
    }
    
    // Transform the clause to substitute context variable references with parameters
    // This handles cases like: WITH x AS foo MATCH (n) WHERE n.id = foo
    const transformedClause = this.transformClauseForContext(clause, contextVarNames);
    
    // Build a MATCH + RETURN query for all pattern variables
    const matchQuery: Query = {
      clauses: [
        transformedClause,
        {
          type: "RETURN" as const,
          items: this.buildReturnItemsForMatch(transformedClause),
        },
      ],
    };
    
    // Create params with context values prefixed with _ctx_
    const mergedParams: Record<string, unknown> = { ...params };
    for (const [key, value] of inputRow) {
      mergedParams[`_ctx_${key}`] = value;
    }
    
    // Translate to SQL with merged parameters
    const translator = new Translator(mergedParams);
    const translation = translator.translate(matchQuery);
    
    if (translation.statements.length === 0) {
      return [];
    }
    
    // For each bound variable, we need to add a WHERE constraint
    // to match the specific node ID from the input row
    const stmt = translation.statements[0];
    let sql = stmt.sql;
    const sqlParams = [...stmt.params];
    
    // Build WHERE constraints for bound variables
    // We need to constrain node variables by their _nf_id
    for (const varName of boundVars) {
      const value = inputRow.get(varName);
      const nodeId = this.extractNodeId(value);
      
      if (!nodeId) continue;
      
      // The translator uses table aliases like n0, n1, etc.
      // We need to find the alias for this variable
      // The translator generates: SELECT ... FROM nodes n0 ... WHERE ...
      // Variable mappings are internal to translator, so we use a different approach:
      // Add a JSON condition that matches the _nf_id in the output
      
      // Instead of modifying the SQL directly (which is fragile), 
      // we'll filter the results after execution
    }
    
    // Execute the SQL query
    const result = this.db.execute(sql, sqlParams);
    
    // Filter results to only include rows where bound variables match
    const matchedResults: Array<Map<string, unknown>> = [];
    
    for (const sqlRow of result.rows) {
      // Check if all bound variables match their expected values
      let matches = true;
      
      for (const varName of boundVars) {
        const expectedValue = inputRow.get(varName);
        const expectedId = this.extractNodeId(expectedValue);
        
        if (!expectedId) continue;
        
        // Get the actual value from SQL result
        const actualValue = sqlRow[varName];
        const actualId = this.extractNodeId(actualValue);
        
        if (actualId !== expectedId) {
          matches = false;
          break;
        }
      }
      
      if (matches) {
        const matchRow = new Map<string, unknown>();
        
        // Add all introduced variables to the result
        for (const varName of introducedVars) {
          if (varName in sqlRow) {
            matchRow.set(varName, sqlRow[varName]);
          }
        }
        
        matchedResults.push(matchRow);
      }
    }
    
    return matchedResults;
  }

  /**
   * Transform a MATCH clause to substitute context variable references with parameter references.
   * This converts WHERE conditions like `n.id = foo` (where foo is from context)
   * to `n.id = $_ctx_foo` so the translator can handle it.
   */
  private transformClauseForContext(
    clause: MatchClause,
    contextVars: Set<string>
  ): MatchClause {
    if (!clause.where) {
      return clause;
    }
    
    // Deep clone the clause
    const transformed: MatchClause = JSON.parse(JSON.stringify(clause));
    
    // Transform WHERE condition to use parameter references for context variables
    if (transformed.where) {
      transformed.where = this.transformWhereForContext(transformed.where, contextVars);
    }
    
    return transformed;
  }

  /**
   * Transform a WHERE condition to substitute context variable references with parameter references
   */
  private transformWhereForContext(
    condition: WhereCondition,
    contextVars: Set<string>
  ): WhereCondition {
    // Deep clone
    const result: WhereCondition = JSON.parse(JSON.stringify(condition));
    
    // Transform based on condition type
    if (result.type === "comparison") {
      if (result.left) {
        result.left = this.transformExpressionForContext(result.left, contextVars);
      }
      if (result.right) {
        result.right = this.transformExpressionForContext(result.right, contextVars);
      }
    } else if (result.type === "and" || result.type === "or") {
      if (result.conditions) {
        result.conditions = result.conditions.map(c => 
          this.transformWhereForContext(c, contextVars)
        );
      }
    } else if (result.type === "not" && result.condition) {
      result.condition = this.transformWhereForContext(result.condition, contextVars);
    }
    
    return result;
  }

  /**
   * Transform an expression to substitute context variable references with parameter references
   */
  private transformExpressionForContext(
    expr: Expression,
    contextVars: Set<string>
  ): Expression {
    // If this is a variable reference to a context variable, convert to parameter
    if (expr.type === "variable" && expr.variable && contextVars.has(expr.variable)) {
      return {
        type: "parameter",
        name: `_ctx_${expr.variable}`,
      };
    }
    
    // If this is a property access on a context variable, convert to parameter
    if (expr.type === "property" && expr.variable && contextVars.has(expr.variable)) {
      // Context variable with property access - this is trickier
      // For now, return as-is and let it fail (shouldn't happen in typical usage)
      return expr;
    }
    
    // Deep clone and recursively transform
    const result: Expression = JSON.parse(JSON.stringify(expr));
    
    if (result.type === "binary") {
      if (result.left) {
        result.left = this.transformExpressionForContext(result.left, contextVars);
      }
      if (result.right) {
        result.right = this.transformExpressionForContext(result.right, contextVars);
      }
    } else if (result.type === "function" && result.args) {
      result.args = result.args.map(arg => 
        this.transformExpressionForContext(arg, contextVars)
      );
    }
    
    return result;
  }

  /**
   * Check if a WHERE condition references context variables
   */
  private whereReferencesContextVars(
    condition: WhereCondition,
    contextVars: Set<string>,
    patternVars: Set<string>
  ): boolean {
    if (condition.type === "comparison") {
      if (condition.left && this.expressionReferencesContextVar(condition.left, contextVars, patternVars)) {
        return true;
      }
      if (condition.right && this.expressionReferencesContextVar(condition.right, contextVars, patternVars)) {
        return true;
      }
    } else if (condition.type === "and" || condition.type === "or") {
      if (condition.conditions) {
        for (const c of condition.conditions) {
          if (this.whereReferencesContextVars(c, contextVars, patternVars)) {
            return true;
          }
        }
      }
    } else if (condition.type === "not" && condition.condition) {
      return this.whereReferencesContextVars(condition.condition, contextVars, patternVars);
    }
    return false;
  }

  /**
   * Check if an expression references a context variable (not a pattern variable)
   */
  private expressionReferencesContextVar(
    expr: Expression,
    contextVars: Set<string>,
    patternVars: Set<string>
  ): boolean {
    if (expr.type === "variable" && expr.variable) {
      // It's a context variable if it's in contextVars but not in patternVars
      return contextVars.has(expr.variable) && !patternVars.has(expr.variable);
    }
    if (expr.type === "binary") {
      if (expr.left && this.expressionReferencesContextVar(expr.left, contextVars, patternVars)) {
        return true;
      }
      if (expr.right && this.expressionReferencesContextVar(expr.right, contextVars, patternVars)) {
        return true;
      }
    }
    if (expr.type === "function" && expr.args) {
      for (const arg of expr.args) {
        if (this.expressionReferencesContextVar(arg, contextVars, patternVars)) {
          return true;
        }
      }
    }
    return false;
  }

  /**
   * Find a pattern with a variable-length edge where the edge variable is bound to a list
   * Returns the pattern info if found, null otherwise
   */
  private findBoundRelationshipListPattern(
    clause: MatchClause,
    context: PhaseContext
  ): { pattern: RelationshipPattern; edgeVar: string; sourceVar: string; targetVar: string } | null {
    for (const pattern of clause.patterns) {
      if (!this.isRelationshipPattern(pattern)) continue;
      
      const relPattern = pattern as RelationshipPattern;
      const edgeVar = relPattern.edge.variable;
      
      // Check if this is a variable-length pattern with a bound variable
      if (!edgeVar) continue;
      const isVarLength = relPattern.edge.minHops !== undefined || relPattern.edge.maxHops !== undefined;
      if (!isVarLength) continue;
      
      // Check if the edge variable is already bound in the context rows
      for (const row of context.rows) {
        const boundValue = row.get(edgeVar);
        if (boundValue !== undefined && Array.isArray(boundValue)) {
          // Found a bound relationship list
          return {
            pattern: relPattern,
            edgeVar,
            sourceVar: relPattern.source.variable || "_first",
            targetVar: relPattern.target.variable || "_second"
          };
        }
      }
    }
    return null;
  }

  /**
   * Execute MATCH with a pre-bound relationship list
   * e.g., MATCH (first)-[rs*]->(second) where rs = [r1, r2, ...]
   * This finds the path by following the exact sequence of relationships in rs
   */
  private executeMatchWithBoundRelList(
    clause: MatchClause,
    context: PhaseContext,
    boundInfo: { pattern: RelationshipPattern; edgeVar: string; sourceVar: string; targetVar: string },
    params: Record<string, unknown>
  ): PhaseContext {
    const newContext = cloneContext(context);
    const newRows: Array<Map<string, unknown>> = [];
    
    for (const inputRow of context.rows) {
      const relList = inputRow.get(boundInfo.edgeVar);
      if (!Array.isArray(relList) || relList.length === 0) {
        // No relationships, skip this row
        continue;
      }
      
      // Follow the sequence of relationships to find the path endpoints
      // Each relationship in the list is either an edge object or edge ID
      
      let currentNodeId: string | null = null;
      let firstNodeId: string | null = null;
      let lastNodeId: string | null = null;
      let valid = true;
      
      for (let i = 0; i < relList.length; i++) {
        const rel = relList[i];
        
        // Extract edge info - could be object with id, source_id, target_id or string ID
        let edgeInfo: { id: string; source_id: string; target_id: string } | null = null;
        
        if (typeof rel === "object" && rel !== null) {
          // Edge object from MATCH - may have _nf_id instead of id
          const relObj = rel as Record<string, unknown>;
          const edgeId = (relObj._nf_id || relObj.id) as string;
          if (edgeId) {
            // Look up the edge to get source/target
            const edgeResult = this.db.execute(
              "SELECT id, source_id, target_id FROM edges WHERE id = ?",
              [edgeId]
            );
            if (edgeResult.rows.length > 0) {
              const row = edgeResult.rows[0];
              edgeInfo = {
                id: row.id as string,
                source_id: row.source_id as string,
                target_id: row.target_id as string
              };
            }
          }
        } else if (typeof rel === "string") {
          // Could be a JSON string like '{"_nf_id":"uuid"}' or a raw UUID string
          let edgeId: string | null = null;
          
          try {
            const parsed = JSON.parse(rel);
            if (typeof parsed === "object" && parsed !== null) {
              edgeId = (parsed._nf_id || parsed.id) as string;
            }
          } catch {
            // Not JSON, assume it's a raw ID
            edgeId = rel;
          }
          
          if (edgeId) {
            const edgeResult = this.db.execute(
              "SELECT id, source_id, target_id FROM edges WHERE id = ?",
              [edgeId]
            );
            if (edgeResult.rows.length > 0) {
              const row = edgeResult.rows[0];
              edgeInfo = {
                id: row.id as string,
                source_id: row.source_id as string,
                target_id: row.target_id as string
              };
            }
          }
        }
        
        if (!edgeInfo) {
          valid = false;
          break;
        }
        
        if (i === 0) {
          // First edge: determine direction from pattern
          // For pattern (first)-[rs*]->(second), we follow edges left-to-right
          if (boundInfo.pattern.edge.direction === "left") {
            // Left-directed: (first)<-[rs*]-(second), so first is at target_id of first edge
            currentNodeId = edgeInfo.target_id;
            firstNodeId = currentNodeId;
            currentNodeId = edgeInfo.source_id;
          } else {
            // Right-directed or undirected: first is at source_id of first edge
            currentNodeId = edgeInfo.source_id;
            firstNodeId = currentNodeId;
            currentNodeId = edgeInfo.target_id;
          }
        } else {
          // Subsequent edges: follow the chain
          // The current node should be connected to this edge
          if (boundInfo.pattern.edge.direction === "left") {
            if (edgeInfo.target_id !== currentNodeId && edgeInfo.source_id !== currentNodeId) {
              valid = false;
              break;
            }
            currentNodeId = edgeInfo.target_id === currentNodeId ? edgeInfo.source_id : edgeInfo.target_id;
          } else {
            if (edgeInfo.source_id !== currentNodeId && edgeInfo.target_id !== currentNodeId) {
              valid = false;
              break;
            }
            currentNodeId = edgeInfo.source_id === currentNodeId ? edgeInfo.target_id : edgeInfo.source_id;
          }
        }
        
        if (i === relList.length - 1) {
          lastNodeId = currentNodeId;
        }
      }
      
      if (!valid || !firstNodeId || !lastNodeId) {
        continue;
      }
      
      // Check if source/target variables are already bound - if so, verify they match
      const boundSourceValue = inputRow.get(boundInfo.sourceVar);
      const boundTargetValue = inputRow.get(boundInfo.targetVar);
      
      let skipDueToBoundMismatch = false;
      
      if (boundSourceValue !== undefined) {
        const boundSourceId = this.extractNodeId(boundSourceValue);
        if (boundSourceId && boundSourceId !== firstNodeId) {
          // Source is bound but doesn't match path start - skip this row
          skipDueToBoundMismatch = true;
        }
      }
      
      if (boundTargetValue !== undefined) {
        const boundTargetId = this.extractNodeId(boundTargetValue);
        if (boundTargetId && boundTargetId !== lastNodeId) {
          // Target is bound but doesn't match path end - skip this row
          skipDueToBoundMismatch = true;
        }
      }
      
      if (skipDueToBoundMismatch) {
        continue;
      }
      
      // Look up the first and last nodes
      const firstNodeResult = this.db.execute(
        "SELECT id, properties FROM nodes WHERE id = ?",
        [firstNodeId]
      );
      const lastNodeResult = this.db.execute(
        "SELECT id, properties FROM nodes WHERE id = ?",
        [lastNodeId]
      );
      
      if (firstNodeResult.rows.length === 0 || lastNodeResult.rows.length === 0) {
        continue;
      }
      
      // Build output row with first and second nodes
      const outputRow = new Map(inputRow);
      
      const firstNode = firstNodeResult.rows[0];
      const lastNode = lastNodeResult.rows[0];
      
      // Format nodes like the translator does (with _nf_id embedded)
      const firstProps = typeof firstNode.properties === "string" 
        ? JSON.parse(firstNode.properties) 
        : firstNode.properties;
      const lastProps = typeof lastNode.properties === "string"
        ? JSON.parse(lastNode.properties)
        : lastNode.properties;
      
      outputRow.set(boundInfo.sourceVar, { ...firstProps, _nf_id: firstNode.id });
      outputRow.set(boundInfo.targetVar, { ...lastProps, _nf_id: lastNode.id });
      
      newRows.push(outputRow);
    }
    
    if (newRows.length > 0) {
      newContext.rows = newRows;
    } else if (clause.type === "OPTIONAL_MATCH") {
      // OPTIONAL MATCH with no results keeps the input rows
      newContext.rows = context.rows;
    } else {
      // Regular MATCH with no results returns empty (excludes the row)
      newContext.rows = [];
    }
    
    return newContext;
  }

  /**
   * Build RETURN items for all variables in a MATCH pattern
   */
  private buildReturnItemsForMatch(clause: MatchClause): ReturnItem[] {
    const items: ReturnItem[] = [];
    const seen = new Set<string>();
    
    for (const pattern of clause.patterns) {
      if (this.isRelationshipPattern(pattern)) {
        if (pattern.source.variable && !seen.has(pattern.source.variable)) {
          seen.add(pattern.source.variable);
          items.push({
            expression: { type: "variable", variable: pattern.source.variable },
          });
        }
        if (pattern.target.variable && !seen.has(pattern.target.variable)) {
          seen.add(pattern.target.variable);
          items.push({
            expression: { type: "variable", variable: pattern.target.variable },
          });
        }
        if (pattern.edge.variable && !seen.has(pattern.edge.variable)) {
          seen.add(pattern.edge.variable);
          items.push({
            expression: { type: "variable", variable: pattern.edge.variable },
          });
        }
      } else if (pattern.variable && !seen.has(pattern.variable)) {
        seen.add(pattern.variable);
        items.push({
          expression: { type: "variable", variable: pattern.variable },
        });
      }
    }
    
    return items;
  }

  /**
   * Execute RETURN clause
   */
  private executeReturnClause(
    clause: ReturnClause,
    context: PhaseContext,
    params: Record<string, unknown>
  ): PhaseContext {
    const newContext = cloneContext(context);
    
    // Check if any return item contains an aggregate function
    const hasAggregate = clause.items.some(item => 
      this.expressionHasAggregate(item.expression)
    );
    
    if (hasAggregate) {
      // Aggregate mode with grouping: group by non-aggregate expressions
      // For each unique combination of grouping keys, compute aggregates
      
      // Identify grouping keys (non-aggregate expressions) and aggregate expressions
      const groupingItems: { alias: string; expression: Expression }[] = [];
      const aggregateItems: { alias: string; expression: Expression }[] = [];
      
      for (const item of clause.items) {
        const alias = item.alias || this.getExpressionName(item.expression);
        if (this.expressionHasAggregate(item.expression)) {
          aggregateItems.push({ alias, expression: item.expression });
        } else {
          groupingItems.push({ alias, expression: item.expression });
        }
      }
      
      // Group rows by grouping key values
      const groups = new Map<string, Array<Map<string, unknown>>>();
      
      for (const row of context.rows) {
        // Compute the grouping key (JSON string of grouping values)
        const keyValues: Record<string, unknown> = {};
        for (const { alias, expression } of groupingItems) {
          keyValues[alias] = this.evaluateExpressionInRow(expression, row, params);
        }
        const groupKey = JSON.stringify(keyValues);
        
        if (!groups.has(groupKey)) {
          groups.set(groupKey, []);
        }
        groups.get(groupKey)!.push(row);
      }
      
      // For each group, compute aggregates and produce output row
      const outputRows: Array<Map<string, unknown>> = [];
      
      for (const [groupKey, groupRows] of groups) {
        const outputRow = new Map<string, unknown>();
        
        // Add grouping values
        const groupKeyValues = JSON.parse(groupKey);
        for (const alias of Object.keys(groupKeyValues)) {
          outputRow.set(alias, groupKeyValues[alias]);
        }
        
        // Compute aggregates for this group
        for (const { alias, expression } of aggregateItems) {
          const value = this.evaluateAggregateExpression(expression, groupRows, params);
          outputRow.set(alias, value);
        }
        
        outputRows.push(outputRow);
      }
      
      newContext.rows = outputRows;
    } else {
      // Non-aggregate mode: transform each row
      const newRows: Array<Map<string, unknown>> = [];
      
      // Check if RETURN * - pass through all variables
      const hasWildcard = clause.items.some(item => 
        item.expression.type === "variable" && item.expression.variable === "*"
      );
      
      for (const row of context.rows) {
        const outputRow = new Map<string, unknown>();
        
        if (hasWildcard) {
          // Copy all variables from input row
          for (const [key, value] of row) {
            outputRow.set(key, value);
          }
        }
        
        for (const item of clause.items) {
          // Skip wildcard - already handled
          if (item.expression.type === "variable" && item.expression.variable === "*") {
            continue;
          }
          
          const alias = item.alias || this.getExpressionName(item.expression);
          const value = this.evaluateExpressionInRow(item.expression, row, params);
          outputRow.set(alias, value);
        }
        
        newRows.push(outputRow);
      }
      
      // Apply DISTINCT if requested
      if (clause.distinct) {
        const seen = new Set<string>();
        const distinctRows: Array<Map<string, unknown>> = [];
        
        for (const row of newRows) {
          // Create a key from row values
          const keyObj: Record<string, unknown> = {};
          for (const [k, v] of row) {
            // For node/edge objects, use _nf_id as the key part
            if (typeof v === "object" && v !== null && "_nf_id" in (v as object)) {
              keyObj[k] = (v as Record<string, unknown>)._nf_id;
            } else {
              keyObj[k] = v;
            }
          }
          const key = JSON.stringify(keyObj);
          
          if (!seen.has(key)) {
            seen.add(key);
            distinctRows.push(row);
          }
        }
        
        newContext.rows = distinctRows;
      } else {
        newContext.rows = newRows;
      }
    }
    
    return newContext;
  }

  /**
   * Execute SET clause
   */
  private executeSetClause(
    clause: SetClause,
    context: PhaseContext,
    params: Record<string, unknown>
  ): PhaseContext {
    for (const row of context.rows) {
      for (const assignment of clause.assignments) {
        const nodeId = row.get(assignment.variable) as string;
        if (!nodeId) continue;
        
        if (assignment.property && assignment.value) {
          const value = this.evaluateExpressionInRow(assignment.value, row, params);
          this.db.execute(
            `UPDATE nodes SET properties = json_set(properties, '$.${assignment.property}', json(?)) WHERE id = ?`,
            [JSON.stringify(value), nodeId]
          );
        }
      }
    }
    
    return context;
  }

  /**
   * Execute DELETE clause
   */
  private executeDeleteClause(
    clause: DeleteClause,
    context: PhaseContext,
    params: Record<string, unknown>
  ): PhaseContext {
    for (const row of context.rows) {
      for (const variable of clause.variables) {
        const id = row.get(variable) as string;
        if (!id) continue;
        
        if (clause.detach) {
          this.db.execute("DELETE FROM edges WHERE source_id = ? OR target_id = ?", [id, id]);
        }
        
        // Try nodes first, then edges
        const result = this.db.execute("DELETE FROM nodes WHERE id = ?", [id]);
        if (result.changes === 0) {
          this.db.execute("DELETE FROM edges WHERE id = ?", [id]);
        }
      }
    }
    
    return context;
  }

  /**
   * Evaluate an expression in the context of a single row
   */
  private evaluateExpressionInRow(
    expr: Expression,
    row: Map<string, unknown>,
    params: Record<string, unknown>
  ): unknown {
    switch (expr.type) {
      case "literal":
        return expr.value;
        
      case "parameter":
        return params[expr.name!];
        
      case "variable":
        return row.get(expr.variable!);
        
      case "property": {
        const varValue = row.get(expr.variable!);
        if (varValue === null || varValue === undefined) return null;
        
        // Case 1: varValue is a JSON string (from MATCH translator output)
        // It contains properties with _nf_id embedded
        if (typeof varValue === "string") {
          try {
            const parsed = JSON.parse(varValue);
            if (typeof parsed === "object" && parsed !== null) {
              // Extract property from the embedded properties
              return parsed[expr.property!] ?? null;
            }
          } catch {
            // Not valid JSON, treat as node ID
          }
          
          // Try as node ID
          let result = this.db.execute(
            `SELECT json_extract(properties, '$.${expr.property}') as value FROM nodes WHERE id = ?`,
            [varValue]
          );
          if (result.rows.length > 0) {
            return this.deepParseJson(result.rows[0].value);
          }
          
          // Try edges
          result = this.db.execute(
            `SELECT json_extract(properties, '$.${expr.property}') as value FROM edges WHERE id = ?`,
            [varValue]
          );
          if (result.rows.length > 0) {
            return this.deepParseJson(result.rows[0].value);
          }
        }
        
        // Case 2: varValue is already a parsed object
        if (typeof varValue === "object" && varValue !== null) {
          return (varValue as Record<string, unknown>)[expr.property!] ?? null;
        }
        
        return null;
      }
        
      case "function":
        return this.evaluateFunctionInRow(expr, row, params);
        
      case "binary":
        return this.evaluateBinaryInRow(expr, row, params);
        
      case "propertyAccess": {
        // Handle expressions like startNode(r).id - property access on function result
        const objValue = this.evaluateExpressionInRow(expr.object!, row, params);
        if (objValue === null || objValue === undefined) return null;
        
        // Get the property from the object
        if (typeof objValue === "object" && objValue !== null) {
          return (objValue as Record<string, unknown>)[expr.property!] ?? null;
        }
        
        // If it's a JSON string, try to parse and extract
        if (typeof objValue === "string") {
          try {
            const parsed = JSON.parse(objValue);
            if (typeof parsed === "object" && parsed !== null) {
              return parsed[expr.property!] ?? null;
            }
          } catch {
            // Not valid JSON
          }
        }
        
        return null;
      }
      
      case "object": {
        // Evaluate object literal expression like {first: m.id, second: 42}
        const result: Record<string, unknown> = {};
        for (const prop of expr.properties || []) {
          result[prop.key] = this.evaluateExpressionInRow(prop.value, row, params);
        }
        return result;
      }
        
      case "listComprehension": {
        // Evaluate list comprehension: [x IN list WHERE cond | mapExpr]
        const listValue = this.evaluateExpressionInRow(expr.listExpr!, row, params);
        if (!Array.isArray(listValue)) return [];
        
        const results: unknown[] = [];
        for (const item of listValue) {
          // Create a new row with the loop variable
          const itemRow = new Map(row);
          if (expr.variable) {
            itemRow.set(expr.variable, item);
          }
          
          // Check filter condition if present
          let passesFilter = true;
          if (expr.filterCondition) {
            // Convert Map to Record for evaluateWhereConditionOnRow
            const rowRecord = Object.fromEntries(itemRow);
            passesFilter = this.evaluateWhereConditionOnRow(expr.filterCondition, rowRecord, params);
          }
          
          if (passesFilter) {
            // Apply map expression if present, otherwise use the item
            const resultValue = expr.mapExpr 
              ? this.evaluateExpressionInRow(expr.mapExpr, itemRow, params)
              : item;
            results.push(resultValue);
          }
        }
        return results;
      }
      
      case "case": {
        // Evaluate CASE WHEN ... THEN ... ELSE ... END
        // Simple form: CASE expr WHEN val THEN result ELSE result END
        // Searched form: CASE WHEN condition THEN result ELSE result END
        if (expr.whens) {
          const rowRecord = Object.fromEntries(row);
          for (const when of expr.whens) {
            // Evaluate the condition (WhereCondition)
            if (this.evaluateWhereConditionOnRow(when.condition, rowRecord, params)) {
              return this.evaluateExpressionInRow(when.result, row, params);
            }
          }
        }
        // Return else expression or null
        return expr.elseExpr ? this.evaluateExpressionInRow(expr.elseExpr, row, params) : null;
      }
      
      case "listPredicate": {
        // Evaluate ALL/ANY/NONE/SINGLE(x IN list WHERE condition)
        const listValue = this.evaluateExpressionInRow(expr.listExpr!, row, params);
        if (!Array.isArray(listValue)) return null;
        
        let matchCount = 0;
        for (const item of listValue) {
          // Create a new row with the loop variable
          const itemRow = new Map(row);
          if (expr.variable) {
            itemRow.set(expr.variable, item);
          }
          
          // Check filter condition
          let passesFilter = true;
          if (expr.filterCondition) {
            const rowRecord = Object.fromEntries(itemRow);
            passesFilter = this.evaluateWhereConditionOnRow(expr.filterCondition, rowRecord, params);
          }
          
          if (passesFilter) {
            matchCount++;
          }
        }
        
        // Return result based on predicate type
        switch (expr.predicateType) {
          case "ALL":
            return matchCount === listValue.length;
          case "ANY":
            return matchCount > 0;
          case "NONE":
            return matchCount === 0;
          case "SINGLE":
            return matchCount === 1;
          default:
            return null;
        }
      }
      
      case "comparison": {
        // Evaluate comparison expression: left op right
        const left = this.evaluateExpressionInRow(expr.left!, row, params);
        const right = this.evaluateExpressionInRow(expr.right!, row, params);
        return this.evaluateComparison(left, right, expr.comparisonOperator || "=");
      }
      
      case "unary": {
        // Evaluate unary expressions: NOT, -, +
        const operandValue = this.evaluateExpressionInRow(expr.operand!, row, params);
        switch (expr.operator) {
          case "NOT":
            // Handle boolean negation
            if (operandValue === null || operandValue === undefined) return null;
            return !operandValue;
          case "-":
            // Handle numeric negation
            if (typeof operandValue === "number") return -operandValue;
            return null;
          case "+":
            // Unary plus (identity for numbers)
            if (typeof operandValue === "number") return operandValue;
            return null;
          default:
            return null;
        }
      }
        
      default:
        return null;
    }
  }

  /**
   * Evaluate a function expression in a row context
   */
  private evaluateFunctionInRow(
    expr: Expression,
    row: Map<string, unknown>,
    params: Record<string, unknown>
  ): unknown {
    const funcName = expr.functionName?.toUpperCase();
    const args = expr.args || [];
    
    switch (funcName) {
      case "RANGE": {
        if (args.length < 2) return [];
        const start = this.evaluateExpressionInRow(args[0], row, params) as number;
        const end = this.evaluateExpressionInRow(args[1], row, params) as number;
        const step = args.length > 2 ? this.evaluateExpressionInRow(args[2], row, params) as number : 1;
        
        const result: number[] = [];
        if (step > 0) {
          for (let i = start; i <= end; i += step) {
            result.push(i);
          }
        } else if (step < 0) {
          for (let i = start; i >= end; i += step) {
            result.push(i);
          }
        }
        return result;
      }
      
      case "SIZE": {
        if (args.length === 0) return 0;
        const value = this.evaluateExpressionInRow(args[0], row, params);
        if (Array.isArray(value)) return value.length;
        if (typeof value === "string") return value.length;
        return 0;
      }
      
      case "LIST": {
        // List literal: [a, b, c]
        return args.map(arg => this.evaluateExpressionInRow(arg, row, params));
      }
      
      case "INDEX": {
        // List indexing: list[index]
        if (args.length < 2) return null;
        const list = this.evaluateExpressionInRow(args[0], row, params) as unknown[];
        const index = this.evaluateExpressionInRow(args[1], row, params) as number;
        if (!Array.isArray(list)) return null;
        const normalizedIndex = index < 0 ? list.length + index : index;
        return list[normalizedIndex] ?? null;
      }
      
      case "ID": {
        if (args.length === 0) return null;
        const nodeVal = this.evaluateExpressionInRow(args[0], row, params);
        // Extract _nf_id from node/edge object
        if (typeof nodeVal === "object" && nodeVal !== null && "_nf_id" in nodeVal) {
          return (nodeVal as Record<string, unknown>)._nf_id;
        }
        return nodeVal;
      }
      
      case "TYPE": {
        if (args.length === 0) return null;
        const edgeVal = this.evaluateExpressionInRow(args[0], row, params);
        
        // Extract edge ID
        let edgeId: string | null = null;
        if (typeof edgeVal === "object" && edgeVal !== null && "_nf_id" in edgeVal) {
          edgeId = (edgeVal as Record<string, unknown>)._nf_id as string;
        } else if (typeof edgeVal === "string") {
          edgeId = edgeVal;
        }
        
        if (!edgeId) return null;
        
        // Look up edge type from database
        const result = this.db.execute(
          "SELECT type FROM edges WHERE id = ?",
          [edgeId]
        );
        
        if (result.rows.length > 0) {
          return result.rows[0].type;
        }
        return null;
      }
      
      case "STARTNODE": {
        if (args.length === 0) return null;
        const edgeVal = this.evaluateExpressionInRow(args[0], row, params);
        
        // Extract start node ID from edge object
        let startNodeId: string | null = null;
        if (typeof edgeVal === "object" && edgeVal !== null) {
          const edgeObj = edgeVal as Record<string, unknown>;
          if ("_nf_start" in edgeObj) {
            startNodeId = edgeObj._nf_start as string;
          } else if ("_nf_id" in edgeObj) {
            // Look up edge from database to get source_id
            const edgeResult = this.db.execute(
              "SELECT source_id FROM edges WHERE id = ?",
              [edgeObj._nf_id]
            );
            if (edgeResult.rows.length > 0) {
              startNodeId = edgeResult.rows[0].source_id as string;
            }
          }
        }
        
        if (!startNodeId) return null;
        
        // Look up node from database
        const nodeResult = this.db.execute(
          "SELECT id, properties FROM nodes WHERE id = ?",
          [startNodeId]
        );
        
        if (nodeResult.rows.length > 0) {
          const nodeProps = typeof nodeResult.rows[0].properties === "string"
            ? JSON.parse(nodeResult.rows[0].properties)
            : nodeResult.rows[0].properties;
          return { ...nodeProps, _nf_id: nodeResult.rows[0].id };
        }
        return null;
      }
      
      case "ENDNODE": {
        if (args.length === 0) return null;
        const edgeVal = this.evaluateExpressionInRow(args[0], row, params);
        
        // Extract end node ID from edge object
        let endNodeId: string | null = null;
        if (typeof edgeVal === "object" && edgeVal !== null) {
          const edgeObj = edgeVal as Record<string, unknown>;
          if ("_nf_end" in edgeObj) {
            endNodeId = edgeObj._nf_end as string;
          } else if ("_nf_id" in edgeObj) {
            // Look up edge from database to get target_id
            const edgeResult = this.db.execute(
              "SELECT target_id FROM edges WHERE id = ?",
              [edgeObj._nf_id]
            );
            if (edgeResult.rows.length > 0) {
              endNodeId = edgeResult.rows[0].target_id as string;
            }
          }
        }
        
        if (!endNodeId) return null;
        
        // Look up node from database
        const nodeResult = this.db.execute(
          "SELECT id, properties FROM nodes WHERE id = ?",
          [endNodeId]
        );
        
        if (nodeResult.rows.length > 0) {
          const nodeProps = typeof nodeResult.rows[0].properties === "string"
            ? JSON.parse(nodeResult.rows[0].properties)
            : nodeResult.rows[0].properties;
          return { ...nodeProps, _nf_id: nodeResult.rows[0].id };
        }
        return null;
      }
      
      // Math functions
      case "ABS": {
        if (args.length === 0) return null;
        const value = this.evaluateExpressionInRow(args[0], row, params);
        if (typeof value !== "number") return null;
        return Math.abs(value);
      }
      
      case "CEIL":
      case "CEILING": {
        if (args.length === 0) return null;
        const value = this.evaluateExpressionInRow(args[0], row, params);
        if (typeof value !== "number") return null;
        return Math.ceil(value);
      }
      
      case "FLOOR": {
        if (args.length === 0) return null;
        const value = this.evaluateExpressionInRow(args[0], row, params);
        if (typeof value !== "number") return null;
        return Math.floor(value);
      }
      
      case "ROUND": {
        if (args.length === 0) return null;
        const value = this.evaluateExpressionInRow(args[0], row, params);
        if (typeof value !== "number") return null;
        return Math.round(value);
      }
      
      case "SIGN": {
        if (args.length === 0) return null;
        const value = this.evaluateExpressionInRow(args[0], row, params);
        if (typeof value !== "number") return null;
        return Math.sign(value);
      }
      
      case "SQRT": {
        if (args.length === 0) return null;
        const value = this.evaluateExpressionInRow(args[0], row, params);
        if (typeof value !== "number") return null;
        return Math.sqrt(value);
      }
      
      case "RAND": {
        // rand() returns a random float between 0 (inclusive) and 1 (exclusive)
        return Math.random();
      }
      
      case "RANDOMUUID": {
        // randomUUID() returns a UUID v4
        return crypto.randomUUID();
      }
      
      case "COALESCE": {
        // coalesce(a, b, ...) returns the first non-null value
        for (const arg of args) {
          const value = this.evaluateExpressionInRow(arg, row, params);
          if (value !== null && value !== undefined) {
            return value;
          }
        }
        return null;
      }
      
      case "REVERSE": {
        // reverse(list) or reverse(string)
        if (args.length === 0) return null;
        const value = this.evaluateExpressionInRow(args[0], row, params);
        if (Array.isArray(value)) {
          return [...value].reverse();
        }
        if (typeof value === "string") {
          return value.split("").reverse().join("");
        }
        return null;
      }
      
      case "DURATION": {
        // duration(string) parses an ISO 8601 duration string
        // duration(map) builds duration from components
        // Format: P[nY][nM][nW][nD][T[nH][nM][n.nS]]
        if (args.length > 0) {
          const arg = this.evaluateExpressionInRow(args[0], row, params);
          if (arg === null) return null;
          if (arg && typeof arg === "object" && !Array.isArray(arg)) {
            const map = arg as Record<string, unknown>;
            
            // Handle fractional values by spilling over to smaller units
            // Neo4j behavior: 0.5 years = 6 months, 0.5 months = ~15.2 days (Gregorian average), etc.
            let years = Number(map.years ?? 0);
            let months = Number(map.months ?? 0);
            let weeks = Number(map.weeks ?? 0);
            let days = Number(map.days ?? 0);
            let hours = Number(map.hours ?? 0);
            let minutes = Number(map.minutes ?? 0);
            let seconds = Number(map.seconds ?? 0);
            let nanoseconds = Number(map.nanoseconds ?? 0);
            
            // Spill fractional parts to smaller units
            const yearFrac = years - Math.trunc(years);
            years = Math.trunc(years);
            months += yearFrac * 12;
            
            const monthFrac = months - Math.trunc(months);
            months = Math.trunc(months);
            days += monthFrac * (365.2425 / 12);  // Neo4j uses Gregorian average (30.436875 days/month)
            
            const weekFrac = weeks - Math.trunc(weeks);
            weeks = Math.trunc(weeks);
            days += weekFrac * 7;
            
            const dayFrac = days - Math.trunc(days);
            days = Math.trunc(days);
            hours += dayFrac * 24;
            
            const hourFrac = hours - Math.trunc(hours);
            hours = Math.trunc(hours);
            minutes += hourFrac * 60;
            
            const minFrac = minutes - Math.trunc(minutes);
            minutes = Math.trunc(minutes);
            seconds += minFrac * 60;
            
            const secFrac = seconds - Math.trunc(seconds);
            seconds = Math.trunc(seconds);
            nanoseconds += secFrac * 1e9;
            nanoseconds = Math.round(nanoseconds);
            
            // Normalize: carry overflow from smaller to larger units
            if (nanoseconds >= 1e9) {
              seconds += Math.floor(nanoseconds / 1e9);
              nanoseconds = nanoseconds % 1e9;
            }
            if (seconds >= 60) {
              minutes += Math.floor(seconds / 60);
              seconds = seconds % 60;
            }
            if (minutes >= 60) {
              hours += Math.floor(minutes / 60);
              minutes = minutes % 60;
            }
            // Note: We don't normalize hours to days or months to years
            // because duration semantics treat these differently in Neo4j
            
            // Build ISO 8601 duration string
            let datePart = "";
            if (years !== 0) datePart += `${years}Y`;
            if (months !== 0) datePart += `${months}M`;
            if (weeks !== 0) datePart += `${weeks}W`;
            if (days !== 0) datePart += `${days}D`;
            
            let timePart = "";
            if (hours !== 0) timePart += `${hours}H`;
            if (minutes !== 0) timePart += `${minutes}M`;
            if (seconds !== 0 || nanoseconds !== 0) {
              if (nanoseconds !== 0) {
                timePart += `${seconds}.${String(Math.trunc(nanoseconds)).padStart(9, "0")}S`;
              } else {
                timePart += `${seconds}S`;
              }
            }
            
            if (timePart !== "") {
              return `P${datePart}T${timePart}`;
            }
            return `P${datePart || "T0S"}`;
          }
          // If string, return as-is
          return String(arg);
        }
        throw new Error("duration() requires an argument");
      }
      
      default:
        return null;
    }
  }

  /**
   * Evaluate a binary expression in a row context
   */
  private evaluateBinaryInRow(
    expr: Expression,
    row: Map<string, unknown>,
    params: Record<string, unknown>
  ): unknown {
    if (!expr.left || !expr.right || !expr.operator) return null;
    
    const left = this.evaluateExpressionInRow(expr.left, row, params);
    const right = this.evaluateExpressionInRow(expr.right, row, params);
    
    // Handle list concatenation
    if (expr.operator === "+" && (Array.isArray(left) || Array.isArray(right))) {
      const leftArr = Array.isArray(left) ? left : [left];
      const rightArr = Array.isArray(right) ? right : [right];
      return [...leftArr, ...rightArr];
    }
    
    // Numeric operations
    const leftNum = left as number;
    const rightNum = right as number;
    
    switch (expr.operator) {
      case "+": return leftNum + rightNum;
      case "-": return leftNum - rightNum;
      case "*": return leftNum * rightNum;
      case "/": return leftNum / rightNum;
      case "%": return leftNum % rightNum;
      case "^": return Math.pow(leftNum, rightNum);
      default: return null;
    }
  }

  /**
   * Evaluate an aggregate expression across all rows
   */
  private evaluateAggregateExpression(
    expr: Expression,
    rows: Array<Map<string, unknown>>,
    params: Record<string, unknown>
  ): unknown {
    // Handle binary expressions that may contain aggregates (e.g., [a] + collect(n) + [b])
    if (expr.type === "binary" && expr.left && expr.right && expr.operator) {
      const leftHasAgg = this.expressionHasAggregate(expr.left);
      const rightHasAgg = this.expressionHasAggregate(expr.right);
      
      // Evaluate left and right, recursing for aggregates
      const left = leftHasAgg 
        ? this.evaluateAggregateExpression(expr.left, rows, params)
        : (rows.length > 0 ? this.evaluateExpressionInRow(expr.left, rows[0], params) : null);
      const right = rightHasAgg
        ? this.evaluateAggregateExpression(expr.right, rows, params)
        : (rows.length > 0 ? this.evaluateExpressionInRow(expr.right, rows[0], params) : null);
      
      // Handle list concatenation
      if (expr.operator === "+" && (Array.isArray(left) || Array.isArray(right))) {
        const leftArr = Array.isArray(left) ? left : [left];
        const rightArr = Array.isArray(right) ? right : [right];
        return [...leftArr, ...rightArr];
      }
      
      // Numeric operations
      const leftNum = left as number;
      const rightNum = right as number;
      
      switch (expr.operator) {
        case "+": return leftNum + rightNum;
        case "-": return leftNum - rightNum;
        case "*": return leftNum * rightNum;
        case "/": return leftNum / rightNum;
        case "%": return leftNum % rightNum;
        case "^": return Math.pow(leftNum, rightNum);
        default: return null;
      }
    }
    
    if (expr.type !== "function") return null;
    
    const funcName = expr.functionName?.toUpperCase();
    const args = expr.args || [];
    
    switch (funcName) {
      case "COLLECT": {
        if (args.length === 0) return [];
        return rows.map(row => this.evaluateExpressionInRow(args[0], row, params));
      }
      
      case "COUNT": {
        if (args.length === 0) return rows.length;
        return rows.filter(row => {
          const value = this.evaluateExpressionInRow(args[0], row, params);
          return value !== null && value !== undefined;
        }).length;
      }
      
      case "SUM": {
        if (args.length === 0) return 0;
        return rows.reduce((sum, row) => {
          const value = this.evaluateExpressionInRow(args[0], row, params);
          return sum + (typeof value === "number" ? value : 0);
        }, 0);
      }
      
      case "AVG": {
        if (args.length === 0 || rows.length === 0) return null;
        const values = rows.map(row => this.evaluateExpressionInRow(args[0], row, params))
          .filter(v => typeof v === "number") as number[];
        if (values.length === 0) return null;
        return values.reduce((a, b) => a + b, 0) / values.length;
      }
      
      case "MIN": {
        if (args.length === 0 || rows.length === 0) return null;
        const values = rows.map(row => this.evaluateExpressionInRow(args[0], row, params))
          .filter(v => v !== null && v !== undefined);
        if (values.length === 0) return null;
        return Math.min(...values.map(v => v as number));
      }
      
      case "MAX": {
        if (args.length === 0 || rows.length === 0) return null;
        const values = rows.map(row => this.evaluateExpressionInRow(args[0], row, params))
          .filter(v => v !== null && v !== undefined);
        if (values.length === 0) return null;
        return Math.max(...values.map(v => v as number));
      }
      
      default:
        return null;
    }
  }

  /**
   * Evaluate a WHERE condition in a row context
   */
  private evaluateWhereInRow(
    condition: WhereCondition,
    row: Map<string, unknown>,
    params: Record<string, unknown>
  ): boolean {
    switch (condition.type) {
      case "comparison": {
        const left = this.evaluateExpressionInRow(condition.left!, row, params);
        const right = this.evaluateExpressionInRow(condition.right!, row, params);
        
        switch (condition.operator) {
          case "=": return left === right;
          case "<>": return left !== right;
          case "<": return (left as number) < (right as number);
          case ">": return (left as number) > (right as number);
          case "<=": return (left as number) <= (right as number);
          case ">=": return (left as number) >= (right as number);
          default: return true;
        }
      }
      
      case "and":
        return condition.conditions!.every(c => this.evaluateWhereInRow(c, row, params));
        
      case "or":
        return condition.conditions!.some(c => this.evaluateWhereInRow(c, row, params));
        
      case "not":
        return !this.evaluateWhereInRow(condition.condition!, row, params);
      
      case "listPredicate": {
        // Handle ALL/ANY/NONE/SINGLE(x IN list WHERE condition) in WHERE clause
        const condExpr = condition as unknown as Expression;
        const listValue = this.evaluateExpressionInRow(condExpr.listExpr!, row, params);
        if (!Array.isArray(listValue)) return false;
        
        let matchCount = 0;
        for (const item of listValue) {
          // Create a new row with the loop variable
          const itemRow = new Map(row);
          if (condExpr.variable) {
            itemRow.set(condExpr.variable, item);
          }
          
          // Check filter condition
          let passesFilter = true;
          if (condExpr.filterCondition) {
            const rowRecord = Object.fromEntries(itemRow);
            passesFilter = this.evaluateWhereConditionOnRow(condExpr.filterCondition, rowRecord, params);
          }
          
          if (passesFilter) {
            matchCount++;
          }
        }
        
        // Return result based on predicate type
        switch (condExpr.predicateType) {
          case "ALL":
            return matchCount === listValue.length;
          case "ANY":
            return matchCount > 0;
          case "NONE":
            return matchCount === 0;
          case "SINGLE":
            return matchCount === 1;
          default:
            return false;
        }
      }
        
      default:
        return true;
    }
  }

  /**
   * Resolve properties with row context
   */
  private resolvePropertiesInContext(
    props: Record<string, unknown>,
    row: Map<string, unknown>,
    params: Record<string, unknown>
  ): Record<string, unknown> {
    const resolved: Record<string, unknown> = {};
    
    for (const [key, value] of Object.entries(props)) {
      resolved[key] = this.resolvePropertyValueInContext(value, row, params);
    }
    
    return resolved;
  }

  /**
   * Resolve a property value with row context
   */
  private resolvePropertyValueInContext(
    value: unknown,
    row: Map<string, unknown>,
    params: Record<string, unknown>
  ): unknown {
    if (typeof value !== "object" || value === null) {
      return value;
    }
    
    const typed = value as { type?: string; name?: string; variable?: string };
    
    if (typed.type === "parameter" && typed.name) {
      return params[typed.name];
    }
    
    if (typed.type === "variable" && typed.name) {
      return row.get(typed.name);
    }
    
    // Handle Expression type
    if ("type" in typed) {
      return this.evaluateExpressionInRow(typed as Expression, row, params);
    }
    
    return value;
  }

  /**
   * Convert context to result format
   */
  private contextToResults(context: PhaseContext): Record<string, unknown>[] {
    return context.rows.map(row => {
      const result: Record<string, unknown> = {};
      for (const [key, value] of row) {
        // Parse JSON strings to objects (nodes/edges are often stored as JSON strings)
        result[key] = this.deepParseJson(value);
      }
      return result;
    });
  }

  /**
   * Handle UNWIND with CREATE pattern
   * UNWIND expands an array and executes CREATE for each element
   */
  private tryUnwindCreateExecution(
    query: Query,
    params: Record<string, unknown>
  ): Record<string, unknown>[] | null {
    const clauses = query.clauses;
    
    // Find UNWIND, CREATE, WITH, and RETURN clauses
    const unwindClauses: UnwindClause[] = [];
    const createClauses: CreateClause[] = [];
    const withClauses: WithClause[] = [];
    let returnClause: ReturnClause | null = null;
    
    for (const clause of clauses) {
      if (clause.type === "UNWIND") {
        unwindClauses.push(clause);
      } else if (clause.type === "CREATE") {
        createClauses.push(clause);
      } else if (clause.type === "RETURN") {
        returnClause = clause;
      } else if (clause.type === "WITH") {
        withClauses.push(clause);
      } else if (clause.type === "MATCH") {
        // If there's a MATCH, don't handle here
        return null;
      }
    }
    
    // Only handle if we have both UNWIND and CREATE
    if (unwindClauses.length === 0 || createClauses.length === 0) {
      return null;
    }
    
    // For each UNWIND, expand the array and execute CREATE
    const results: Record<string, unknown>[] = [];
    
    // Get the values from the UNWIND expression
    const unwindValues = this.evaluateUnwindExpressions(unwindClauses, params);
    
    // Generate all combinations (cartesian product) of UNWIND values
    const combinations = this.generateCartesianProduct(unwindValues);
    
    // Check if RETURN has aggregate functions
    const hasAggregates = returnClause?.items.some(item => 
      item.expression.type === "function" && 
      ["sum", "count", "avg", "min", "max", "collect"].includes(item.expression.functionName?.toLowerCase() || "")
    );
    
    // Check if any WITH clause has aggregate functions
    const hasWithAggregates = withClauses.some(clause =>
      clause.items.some(item =>
        item.expression.type === "function" &&
        ["sum", "count", "avg", "min", "max", "collect"].includes(item.expression.functionName?.toLowerCase() || "")
      )
    );
    
    // Extract WITH aggregate info
    const withAggregateMap: Map<string, { functionName: string; argVariable: string; argProperty?: string }> = new Map();
    for (const withClause of withClauses) {
      for (const item of withClause.items) {
        if (item.alias && item.expression.type === "function") {
          const funcName = item.expression.functionName?.toLowerCase();
          if (funcName && ["sum", "count", "avg", "min", "max", "collect"].includes(funcName)) {
            const args = item.expression.args || [];
            if (args.length > 0) {
              const arg = args[0];
              if (arg.type === "variable") {
                withAggregateMap.set(item.alias, {
                  functionName: funcName,
                  argVariable: arg.variable!,
                });
              } else if (arg.type === "property") {
                withAggregateMap.set(item.alias, {
                  functionName: funcName,
                  argVariable: arg.variable!,
                  argProperty: arg.property,
                });
              }
            }
          }
        }
      }
    }
    
    // Check if RETURN references WITH aggregate aliases
    const returnsWithAggregateAliases = returnClause?.items.some(item =>
      item.expression.type === "variable" && withAggregateMap.has(item.expression.variable!)
    ) || false;
    
    // For aggregates, collect intermediate values
    const aggregateValues: Map<string, number[]> = new Map();
    // Also collect values for WITH aggregates (can be numbers or objects for collect)
    const withAggregateValues: Map<string, unknown[]> = new Map();
    
    this.db.transaction(() => {
      // Collect all node inserts for batching
      const nodeInserts: {id: string, labelJson: string, propsJson: string, variable?: string, combinationIndex: number}[] = [];
      
      for (let comboIndex = 0; comboIndex < combinations.length; comboIndex++) {
        const combination = combinations[comboIndex];
        // Build a map of unwind variable -> current value
        const unwindContext: Record<string, unknown> = {};
        for (let i = 0; i < unwindClauses.length; i++) {
          unwindContext[unwindClauses[i].alias] = combination[i];
        }
        
        for (const createClause of createClauses) {
          for (const pattern of createClause.patterns) {
            if (this.isRelationshipPattern(pattern)) {
              // Relationships are handled individually (they may reference nodes created in the same combination)
              const createdIds: Map<string, string> = new Map();
              this.executeCreateRelationshipPatternWithUnwind(pattern, createdIds, params, unwindContext);
            } else {
              const id = crypto.randomUUID();
              const labelJson = this.normalizeLabelToJson(pattern.label);
              const props = this.resolvePropertiesWithUnwind(pattern.properties || {}, params, unwindContext);
              
              nodeInserts.push({
                id,
                labelJson,
                propsJson: JSON.stringify(props),
                variable: pattern.variable,
                combinationIndex: comboIndex
              });
            }
          }
        }
      }
      
      // Batch insert nodes (cap at 500 rows per statement)
      const BATCH_SIZE = 500;
      for (let i = 0; i < nodeInserts.length; i += BATCH_SIZE) {
        const batch = nodeInserts.slice(i, i + BATCH_SIZE);
        const placeholders = batch.map(() => '(?, ?, ?)').join(',');
        const values = batch.flatMap(insert => [insert.id, insert.labelJson, insert.propsJson]);
        
        this.db.execute(
          `INSERT INTO nodes (id, label, properties) VALUES ${placeholders}`,
          values
        );
      }
      
      // Process each combination with batched inserts
      for (let comboIndex = 0; comboIndex < combinations.length; comboIndex++) {
        const combination = combinations[comboIndex];
        // Build a map of unwind variable -> current value
        const unwindContext: Record<string, unknown> = {};
        for (let i = 0; i < unwindClauses.length; i++) {
          unwindContext[unwindClauses[i].alias] = combination[i];
        }
        
        // Execute CREATE with the unwind context
        const createdIds: Map<string, string> = new Map();
        
        // Add nodes from batch insert to createdIds
        for (const insert of nodeInserts) {
          if (insert.combinationIndex === comboIndex && insert.variable) {
            createdIds.set(insert.variable, insert.id);
          }
        }
        
        // Handle relationships (they may reference nodes created in this combination)
        for (const createClause of createClauses) {
          for (const pattern of createClause.patterns) {
            if (this.isRelationshipPattern(pattern)) {
              this.executeCreateRelationshipPatternWithUnwind(pattern, createdIds, params, unwindContext);
            }
          }
        }
        
        // Handle RETURN if present
        if (returnClause) {
          // Check WITH filter first
          let passesWithFilter = true;
          for (const withClause of withClauses) {
            if (withClause.where) {
              passesWithFilter = this.evaluateWithWhereCondition(withClause.where, createdIds, params);
              if (!passesWithFilter) break;
            }
          }
          
          if (!passesWithFilter) continue;
          
          if (hasAggregates || hasWithAggregates) {
            // Collect values for WITH aggregates
            if (hasWithAggregates) {
              for (const [alias, aggInfo] of withAggregateMap) {
                let value: unknown;
                
                const id = createdIds.get(aggInfo.argVariable);
                if (id) {
                  let result = this.db.execute(
                    "SELECT properties FROM nodes WHERE id = ?",
                    [id]
                  );
                  
                  if (result.rows.length === 0) {
                    result = this.db.execute(
                      "SELECT properties FROM edges WHERE id = ?",
                      [id]
                    );
                  }
                  
                  if (result.rows.length > 0) {
                    const props = typeof result.rows[0].properties === "string"
                      ? JSON.parse(result.rows[0].properties)
                      : result.rows[0].properties;
                    
                    if (aggInfo.argProperty) {
                      // Collect property value
                      value = props[aggInfo.argProperty];
                    } else {
                      // Collect the whole node object (for collect(n))
                      value = props;
                    }
                  }
                }
                
                if (value !== undefined) {
                  if (!withAggregateValues.has(alias)) {
                    withAggregateValues.set(alias, []);
                  }
                  withAggregateValues.get(alias)!.push(value);
                }
              }
            }
            
            // Collect values for RETURN aggregates
            if (hasAggregates) {
              for (const item of returnClause!.items) {
                const alias = item.alias || this.getExpressionName(item.expression);
                
                if (item.expression.type === "function") {
                  const funcName = item.expression.functionName?.toLowerCase();
                  const args = item.expression.args || [];
                  
                  if (args.length > 0) {
                    const arg = args[0];
                    let value: number | undefined;
                    
                    if (arg.type === "property") {
                      const variable = arg.variable!;
                      const property = arg.property!;
                      const id = createdIds.get(variable);
                      
                      if (id) {
                        // Try nodes first, then edges
                        let result = this.db.execute(
                          "SELECT properties FROM nodes WHERE id = ?",
                          [id]
                        );
                        
                        if (result.rows.length === 0) {
                          // Try edges table
                          result = this.db.execute(
                            "SELECT properties FROM edges WHERE id = ?",
                            [id]
                          );
                        }
                        
                        if (result.rows.length > 0) {
                          const props = typeof result.rows[0].properties === "string"
                            ? JSON.parse(result.rows[0].properties)
                            : result.rows[0].properties;
                          value = props[property];
                        }
                      }
                    }
                    
                    if (value !== undefined) {
                      if (!aggregateValues.has(alias)) {
                        aggregateValues.set(alias, []);
                      }
                      aggregateValues.get(alias)!.push(value);
                    }
                  } else if (funcName === "count") {
                    // count(*) - just count iterations
                    if (!aggregateValues.has(alias)) {
                      aggregateValues.set(alias, []);
                    }
                    aggregateValues.get(alias)!.push(1);
                  }
                }
              }
            }
          } else {
            // Non-aggregate case - build result row as before
            const resultRow: Record<string, unknown> = {};
            
            for (const item of returnClause.items) {
              const alias = item.alias || this.getExpressionName(item.expression);
              
              if (item.expression.type === "variable") {
                const variable = item.expression.variable!;
                const id = createdIds.get(variable);
                
                if (id) {
                  const nodeResult = this.db.execute(
                    "SELECT id, label, properties FROM nodes WHERE id = ?",
                    [id]
                  );
                  
                  if (nodeResult.rows.length > 0) {
                    const row = nodeResult.rows[0];
                    // Neo4j 3.5 format: return properties directly
                    resultRow[alias] = typeof row.properties === "string"
                      ? JSON.parse(row.properties)
                      : row.properties;
                  }
                }
              } else if (item.expression.type === "property") {
                // Handle property access like n.num or r.prop
                const variable = item.expression.variable!;
                const property = item.expression.property!;
                const id = createdIds.get(variable);
                
                if (id) {
                  // Try nodes first, then edges
                  let nodeResult = this.db.execute(
                    "SELECT properties FROM nodes WHERE id = ?",
                    [id]
                  );
                  
                  if (nodeResult.rows.length === 0) {
                    // Try edges table
                    nodeResult = this.db.execute(
                      "SELECT properties FROM edges WHERE id = ?",
                      [id]
                    );
                  }
                  
                  if (nodeResult.rows.length > 0) {
                    const props = typeof nodeResult.rows[0].properties === "string"
                      ? JSON.parse(nodeResult.rows[0].properties)
                      : nodeResult.rows[0].properties;
                    resultRow[alias] = props[property];
                  }
                }
              }
            }
            
            if (Object.keys(resultRow).length > 0) {
              results.push(resultRow);
            }
          }
        }
      }
    });
    
    // Compute WITH aggregate results if RETURN references them
    if (returnsWithAggregateAliases && returnClause) {
      const withAggregateResult: Record<string, unknown> = {};
      
      for (const item of returnClause.items) {
        if (item.expression.type === "variable") {
          const alias = item.expression.variable!;
          if (withAggregateMap.has(alias)) {
            const aggInfo = withAggregateMap.get(alias)!;
            const values = withAggregateValues.get(alias) || [];
            
            switch (aggInfo.functionName) {
              case "sum": {
                const nums = values as number[];
                withAggregateResult[alias] = nums.reduce((a, b) => a + b, 0);
                break;
              }
              case "count":
                withAggregateResult[alias] = values.length;
                break;
              case "avg": {
                const nums = values as number[];
                withAggregateResult[alias] = nums.length > 0 
                  ? nums.reduce((a, b) => a + b, 0) / nums.length 
                  : null;
                break;
              }
              case "min": {
                const nums = values as number[];
                withAggregateResult[alias] = nums.length > 0 ? Math.min(...nums) : null;
                break;
              }
              case "max": {
                const nums = values as number[];
                withAggregateResult[alias] = nums.length > 0 ? Math.max(...nums) : null;
                break;
              }
              case "collect":
                withAggregateResult[alias] = values;
                break;
            }
          }
        }
      }
      
      if (Object.keys(withAggregateResult).length > 0) {
        results.push(withAggregateResult);
      }
    }
    
    // Compute aggregate results if needed
    if (hasAggregates && returnClause) {
      const aggregateResult: Record<string, unknown> = {};
      
      for (const item of returnClause.items) {
        const alias = item.alias || this.getExpressionName(item.expression);
        
        if (item.expression.type === "function") {
          const funcName = item.expression.functionName?.toLowerCase();
          const values = aggregateValues.get(alias) || [];
          
          switch (funcName) {
            case "sum":
              aggregateResult[alias] = values.reduce((a, b) => a + b, 0);
              break;
            case "count":
              aggregateResult[alias] = values.length;
              break;
            case "avg":
              aggregateResult[alias] = values.length > 0 
                ? values.reduce((a, b) => a + b, 0) / values.length 
                : null;
              break;
            case "min":
              aggregateResult[alias] = values.length > 0 ? Math.min(...values) : null;
              break;
            case "max":
              aggregateResult[alias] = values.length > 0 ? Math.max(...values) : null;
              break;
            case "collect":
              aggregateResult[alias] = values;
              break;
          }
        }
      }
      
      if (Object.keys(aggregateResult).length > 0) {
        results.push(aggregateResult);
      }
    }
    
    // Apply SKIP and LIMIT if present in RETURN clause
    let finalResults = results;
    if (returnClause) {
      if (returnClause.skip !== undefined && returnClause.skip !== null) {
        const skipValue = this.evaluateSkipLimitExpression(returnClause.skip, params);
        finalResults = finalResults.slice(skipValue);
      }
      
      if (returnClause.limit !== undefined && returnClause.limit !== null) {
        const limitValue = this.evaluateSkipLimitExpression(returnClause.limit, params);
        finalResults = finalResults.slice(0, limitValue);
      }
    }
    
    return finalResults;
  }

  /**
   * Evaluate a WITH clause WHERE condition against created nodes
   */
  private evaluateWithWhereCondition(
    condition: WhereCondition,
    createdIds: Map<string, string>,
    params: Record<string, unknown>
  ): boolean {
    if (condition.type === "comparison") {
      // Handle comparison: n.prop % 2 = 0
      const left = condition.left!;
      const right = condition.right!;
      const operator = condition.operator!;
      
      const leftValue = this.evaluateExpressionForFilter(left, createdIds, params);
      const rightValue = this.evaluateExpressionForFilter(right, createdIds, params);
      
      switch (operator) {
        case "=": return leftValue === rightValue;
        case "<>": return leftValue !== rightValue;
        case "<": return (leftValue as number) < (rightValue as number);
        case ">": return (leftValue as number) > (rightValue as number);
        case "<=": return (leftValue as number) <= (rightValue as number);
        case ">=": return (leftValue as number) >= (rightValue as number);
        default: return true;
      }
    } else if (condition.type === "and") {
      return condition.conditions!.every((c: WhereCondition) => this.evaluateWithWhereCondition(c, createdIds, params));
    } else if (condition.type === "or") {
      return condition.conditions!.some((c: WhereCondition) => this.evaluateWithWhereCondition(c, createdIds, params));
    }
    
    // For other condition types, pass through
    return true;
  }
  
  /**
   * Evaluate a WITH clause WHERE condition using captured property values
   * This is used for patterns like: WITH n.num AS num ... DELETE n ... WITH num WHERE num % 2 = 0
   */
  private evaluateWithWhereConditionWithPropertyAliases(
    condition: WhereCondition,
    resolvedIds: Record<string, string>,
    capturedPropertyValues: Record<string, unknown>,
    propertyAliasMap: Map<string, { variable: string; property: string }>,
    params: Record<string, unknown>
  ): boolean {
    if (condition.type === "comparison") {
      const left = condition.left!;
      const right = condition.right!;
      const operator = condition.operator!;
      
      const leftValue = this.evaluateExpressionWithPropertyAliases(left, resolvedIds, capturedPropertyValues, propertyAliasMap, params);
      const rightValue = this.evaluateExpressionWithPropertyAliases(right, resolvedIds, capturedPropertyValues, propertyAliasMap, params);
      
      switch (operator) {
        case "=": return leftValue === rightValue;
        case "<>": return leftValue !== rightValue;
        case "<": return (leftValue as number) < (rightValue as number);
        case ">": return (leftValue as number) > (rightValue as number);
        case "<=": return (leftValue as number) <= (rightValue as number);
        case ">=": return (leftValue as number) >= (rightValue as number);
        default: return true;
      }
    } else if (condition.type === "and") {
      return condition.conditions!.every((c: WhereCondition) => 
        this.evaluateWithWhereConditionWithPropertyAliases(c, resolvedIds, capturedPropertyValues, propertyAliasMap, params)
      );
    } else if (condition.type === "or") {
      return condition.conditions!.some((c: WhereCondition) => 
        this.evaluateWithWhereConditionWithPropertyAliases(c, resolvedIds, capturedPropertyValues, propertyAliasMap, params)
      );
    }
    
    return true;
  }
  
  /**
   * Evaluate an expression using captured property values (for property alias references)
   */
  private evaluateExpressionWithPropertyAliases(
    expr: Expression,
    resolvedIds: Record<string, string>,
    capturedPropertyValues: Record<string, unknown>,
    propertyAliasMap: Map<string, { variable: string; property: string }>,
    params: Record<string, unknown>
  ): unknown {
    if (expr.type === "literal") {
      return expr.value;
    } else if (expr.type === "variable") {
      const varName = expr.variable!;
      // Check if this is a property alias
      if (propertyAliasMap.has(varName)) {
        return capturedPropertyValues[varName];
      }
      // Otherwise it might be a node ID
      return resolvedIds[varName];
    } else if (expr.type === "property") {
      const variable = expr.variable!;
      const property = expr.property!;
      const id = resolvedIds[variable];
      
      if (id) {
        // Try nodes first, then edges
        let result = this.db.execute(
          "SELECT properties FROM nodes WHERE id = ?",
          [id]
        );
        
        if (result.rows.length === 0) {
          result = this.db.execute(
            "SELECT properties FROM edges WHERE id = ?",
            [id]
          );
        }
        
        if (result.rows.length > 0) {
          const props = typeof result.rows[0].properties === "string"
            ? JSON.parse(result.rows[0].properties)
            : result.rows[0].properties;
          return props[property];
        }
      }
      return null;
    } else if (expr.type === "binary") {
      const left = this.evaluateExpressionWithPropertyAliases(expr.left!, resolvedIds, capturedPropertyValues, propertyAliasMap, params);
      const right = this.evaluateExpressionWithPropertyAliases(expr.right!, resolvedIds, capturedPropertyValues, propertyAliasMap, params);
      
      switch (expr.operator) {
        case "+": return (left as number) + (right as number);
        case "-": return (left as number) - (right as number);
        case "*": return (left as number) * (right as number);
        case "/": return (left as number) / (right as number);
        case "%": return (left as number) % (right as number);
        default: return null;
      }
    } else if (expr.type === "parameter") {
      return params[expr.name!];
    }
    
    return null;
  }
  
  /**
   * Evaluate an expression for filtering in UNWIND+CREATE+WITH context
   */
  private evaluateExpressionForFilter(
    expr: Expression,
    createdIds: Map<string, string>,
    params: Record<string, unknown>
  ): unknown {
    if (expr.type === "literal") {
      return expr.value;
    } else if (expr.type === "property") {
      const variable = expr.variable!;
      const property = expr.property!;
      const id = createdIds.get(variable);
      
      if (id) {
        // Try nodes first, then edges
        let result = this.db.execute(
          "SELECT properties FROM nodes WHERE id = ?",
          [id]
        );
        
        if (result.rows.length === 0) {
          // Try edges table
          result = this.db.execute(
            "SELECT properties FROM edges WHERE id = ?",
            [id]
          );
        }
        
        if (result.rows.length > 0) {
          const props = typeof result.rows[0].properties === "string"
            ? JSON.parse(result.rows[0].properties)
            : result.rows[0].properties;
          return props[property];
        }
      }
      return null;
    } else if (expr.type === "binary") {
      const left = this.evaluateExpressionForFilter(expr.left!, createdIds, params);
      const right = this.evaluateExpressionForFilter(expr.right!, createdIds, params);
      
      switch (expr.operator) {
        case "+": return (left as number) + (right as number);
        case "-": return (left as number) - (right as number);
        case "*": return (left as number) * (right as number);
        case "/": return (left as number) / (right as number);
        case "%": return (left as number) % (right as number);
        default: return null;
      }
    } else if (expr.type === "parameter") {
      return params[expr.name!];
    }
    
    return null;
  }

  /**
   * Handle UNWIND + MERGE pattern (optionally with WITH clauses between)
   * This requires special handling to resolve UNWIND variables in MERGE patterns
   */
  private tryUnwindMergeExecution(
    query: Query,
    params: Record<string, unknown>
  ): Record<string, unknown>[] | null {
    const clauses = query.clauses;
    
    // Find UNWIND, WITH, MERGE, and RETURN clauses in order
    const unwindClauses: UnwindClause[] = [];
    const withClauses: WithClause[] = [];
    let mergeClause: MergeClause | null = null;
    let returnClause: ReturnClause | null = null;
    
    for (const clause of clauses) {
      if (clause.type === "UNWIND") {
        unwindClauses.push(clause);
      } else if (clause.type === "WITH") {
        withClauses.push(clause);
      } else if (clause.type === "MERGE") {
        mergeClause = clause;
      } else if (clause.type === "RETURN") {
        returnClause = clause;
      } else if (clause.type === "MATCH" || clause.type === "CREATE") {
        // If there's a MATCH or CREATE, don't handle here
        return null;
      }
    }
    
    // Only handle if we have both UNWIND and MERGE
    if (unwindClauses.length === 0 || !mergeClause) {
      return null;
    }
    
    // Get the values from the UNWIND expression
    const unwindValues = this.evaluateUnwindExpressions(unwindClauses, params);
    
    // Generate all combinations (cartesian product) of UNWIND values
    const combinations = this.generateCartesianProduct(unwindValues);
    
    // Build initial rows from UNWIND combinations
    let rows: Record<string, unknown>[] = [];
    for (const combination of combinations) {
      const row: Record<string, unknown> = {};
      for (let i = 0; i < unwindClauses.length; i++) {
        row[unwindClauses[i].alias] = combination[i];
      }
      rows.push(row);
    }
    
    // Apply WITH clauses to transform rows
    for (const withClause of withClauses) {
      rows = this.applyWithClauseToRows(rows, withClause, params);
    }
    
    // Track merged nodes for RETURN
    const mergedNodes: Array<{ variable: string; id: string; label: string; properties: Record<string, unknown> }> = [];
    
    this.db.transaction(() => {
      for (const row of rows) {
        // Execute MERGE for each pattern
        for (const pattern of mergeClause!.patterns) {
          if (!this.isRelationshipPattern(pattern)) {
            // Node pattern MERGE
            const nodePattern = pattern as NodePattern;
            const props = this.resolvePropertiesFromRow(nodePattern.properties || {}, row, params);
            const labelJson = this.normalizeLabelToJson(nodePattern.label);
            
            // Check if node exists
            let whereConditions: string[] = [];
            let whereParams: unknown[] = [];
            
            if (nodePattern.label) {
              whereConditions.push("EXISTS (SELECT 1 FROM json_each(label) WHERE value = ?)");
              whereParams.push(nodePattern.label);
            }
            
            for (const [key, value] of Object.entries(props)) {
              whereConditions.push(`json_extract(properties, '$.${key}') = ?`);
              whereParams.push(value);
            }
            
            const existsQuery = whereConditions.length > 0
              ? `SELECT id, properties FROM nodes WHERE ${whereConditions.join(" AND ")}`
              : "SELECT id, properties FROM nodes LIMIT 1";
            
            const existsResult = this.db.execute(existsQuery, whereParams);
            
            let nodeId: string;
            let nodeProps: Record<string, unknown>;
            
            if (existsResult.rows.length === 0) {
              // Node doesn't exist, create it
              nodeId = crypto.randomUUID();
              nodeProps = props;
              this.db.execute(
                "INSERT INTO nodes (id, label, properties) VALUES (?, ?, ?)",
                [nodeId, labelJson, JSON.stringify(props)]
              );
            } else {
              nodeId = existsResult.rows[0].id as string;
              const propsStr = existsResult.rows[0].properties as string;
              nodeProps = propsStr ? JSON.parse(propsStr) : {};
            }
            
            // Track merged node for RETURN
            if (nodePattern.variable) {
              // Normalize label to string
              const labelStr = Array.isArray(nodePattern.label) 
                ? nodePattern.label[0] || ""
                : nodePattern.label || "";
              mergedNodes.push({
                variable: nodePattern.variable,
                id: nodeId,
                label: labelStr,
                properties: nodeProps,
              });
            }
          }
        }
      }
    });
    
    // Handle RETURN clause
    if (returnClause) {
      const results: Record<string, unknown>[] = [];
      
      for (const mergedNode of mergedNodes) {
        const resultRow: Record<string, unknown> = {};
        
        for (const item of returnClause.items) {
          const alias = item.alias || this.expressionToString(item.expression);
          
          if (item.expression.type === "function" && 
              item.expression.functionName?.toLowerCase() === "count" &&
              (!item.expression.args || item.expression.args.length === 0 ||
               (item.expression.args.length === 1 && 
                item.expression.args[0].type === "literal" &&
                item.expression.args[0].value === "*"))) {
            return [{ [alias]: mergedNodes.length }];
          } else if (item.expression.type === "property") {
            // Property access like a.num
            if (item.expression.variable === mergedNode.variable) {
              resultRow[alias] = mergedNode.properties[item.expression.property!];
            }
          } else if (item.expression.type === "variable") {
            // Return whole node
            if (item.expression.variable === mergedNode.variable) {
              resultRow[alias] = {
                _id: mergedNode.id,
                _labels: mergedNode.label ? [mergedNode.label] : [],
                ...mergedNode.properties,
              };
            }
          }
        }
        
        if (Object.keys(resultRow).length > 0) {
          results.push(resultRow);
        }
      }
      
      return results;
    }
    
    return [];
  }
  
  /**
   * Apply a WITH clause to transform rows
   */
  private applyWithClauseToRows(
    rows: Record<string, unknown>[],
    withClause: WithClause,
    params: Record<string, unknown>
  ): Record<string, unknown>[] {
    // Apply WHERE filter if present
    if (withClause.where) {
      rows = rows.filter(row => this.evaluateWhereConditionOnRow(withClause.where!, row, params));
    }
    
    // Transform rows based on WITH items (projection/renaming)
    let newRows = rows.map(row => {
      const newRow: Record<string, unknown> = {};
      for (const item of withClause.items) {
        const alias = item.alias || this.expressionToString(item.expression);
        newRow[alias] = this.evaluateExpressionOnRow(item.expression, row, params);
      }
      return newRow;
    });
    
    // Apply DISTINCT if present
    if (withClause.distinct) {
      const seen = new Set<string>();
      newRows = newRows.filter(row => {
        const key = JSON.stringify(row);
        if (seen.has(key)) return false;
        seen.add(key);
        return true;
      });
    }
    
    return newRows;
  }
  
  /**
   * Evaluate a WHERE condition on a row context
   */
  private evaluateWhereConditionOnRow(
    condition: WhereCondition,
    row: Record<string, unknown>,
    params: Record<string, unknown>
  ): boolean {
    if (condition.type === "and") {
      return condition.conditions!.every(c => this.evaluateWhereConditionOnRow(c, row, params));
    }
    if (condition.type === "or") {
      return condition.conditions!.some(c => this.evaluateWhereConditionOnRow(c, row, params));
    }
    if (condition.type === "not") {
      return !this.evaluateWhereConditionOnRow(condition.condition!, row, params);
    }
    if (condition.type === "comparison") {
      const left = this.evaluateExpressionOnRow(condition.left!, row, params);
      const right = this.evaluateExpressionOnRow(condition.right!, row, params);
      return this.evaluateComparison(left, right, condition.operator!);
    }
    if (condition.type === "expression") {
      // WHERE <expression> - evaluate the expression and check if it's truthy
      // The expression is stored in condition.left
      const value = this.evaluateExpressionOnRow(condition.left!, row, params);
      // In Cypher, only true is truthy; false and null are falsy
      return value === true || value === 1;
    }
    // Default true for unsupported conditions
    return true;
  }
  
  /**
   * Evaluate an expression in the context of a row
   */
  private evaluateExpressionOnRow(
    expr: Expression,
    row: Record<string, unknown>,
    params: Record<string, unknown>
  ): unknown {
    if (expr.type === "literal") {
      return expr.value;
    }
    if (expr.type === "variable") {
      return row[expr.variable!];
    }
    if (expr.type === "parameter") {
      return params[expr.name!];
    }
    if (expr.type === "property") {
      const obj = row[expr.variable!];
      if (obj && typeof obj === "object" && expr.property) {
        return (obj as Record<string, unknown>)[expr.property];
      }
      return null;
    }
    if (expr.type === "function") {
      // Handle functions - convert row to Map for evaluateFunctionInRow
      const rowMap = new Map(Object.entries(row));
      return this.evaluateFunctionInRow(expr, rowMap, params);
    }
    if (expr.type === "binary") {
      // Handle binary expressions
      const rowMap = new Map(Object.entries(row));
      return this.evaluateBinaryInRow(expr, rowMap, params);
    }
    // Default null for unsupported expressions
    return null;
  }
  
  /**
   * Check if a value is a parameter reference
   */
  private isParamRef(value: unknown): value is { type: "parameter"; name: string } {
    return typeof value === "object" && value !== null && 
           (value as Record<string, unknown>).type === "parameter" &&
           typeof (value as Record<string, unknown>).name === "string";
  }
  
  /**
   * Check if a value is a variable reference
   */
  private isVarRef(value: unknown): value is { type: "variable"; name: string } {
    return typeof value === "object" && value !== null && 
           (value as Record<string, unknown>).type === "variable" &&
           typeof (value as Record<string, unknown>).name === "string";
  }
  
  /**
   * Resolve MERGE pattern properties from a row context
   */
  private resolvePropertiesFromRow(
    properties: Record<string, unknown>,
    row: Record<string, unknown>,
    params: Record<string, unknown>
  ): Record<string, unknown> {
    const resolved: Record<string, unknown> = {};
    for (const [key, value] of Object.entries(properties)) {
      if (this.isParamRef(value)) {
        resolved[key] = params[value.name];
      } else if (this.isVarRef(value)) {
        resolved[key] = row[value.name];
      } else if (value !== null && typeof value === "object" && "type" in value) {
        const expr = value as Expression;
        resolved[key] = this.evaluateExpressionOnRow(expr, row, params);
      } else {
        resolved[key] = value;
      }
    }
    return resolved;
  }
  
  /**
   * Convert expression to string for alias/key purposes
   */
  private expressionToString(expr: Expression): string {
    if (expr.type === "variable") return expr.variable || "";
    if (expr.type === "property") return `${expr.variable}.${expr.property}`;
    if (expr.type === "literal") return String(expr.value);
    return "expr";
  }
  
  /**
   * Compare two values with operator
   */
  private evaluateComparison(left: unknown, right: unknown, operator: string): boolean {
    if (left === null || right === null) return false;
    switch (operator) {
      case "=": return left === right;
      case "<>": case "!=": return left !== right;
      case ">": return (left as number) > (right as number);
      case "<": return (left as number) < (right as number);
      case ">=": return (left as number) >= (right as number);
      case "<=": return (left as number) <= (right as number);
      default: return false;
    }
  }

  /**
   * Handle MATCH+WITH(COLLECT)+UNWIND+RETURN pattern
   * This requires a subquery for the aggregate function because SQLite doesn't
   * allow aggregate functions directly inside json_each()
   */
  private tryCollectUnwindExecution(
    query: Query,
    params: Record<string, unknown>
  ): Record<string, unknown>[] | null {
    const clauses = query.clauses;
    
    // Find the pattern: MATCH + WITH (containing COLLECT) + UNWIND + RETURN
    let matchClauses: MatchClause[] = [];
    let withClause: WithClause | null = null;
    let unwindClause: UnwindClause | null = null;
    let returnClause: ReturnClause | null = null;
    
    for (const clause of clauses) {
      if (clause.type === "MATCH" || clause.type === "OPTIONAL_MATCH") {
        matchClauses.push(clause);
      } else if (clause.type === "WITH") {
        withClause = clause;
      } else if (clause.type === "UNWIND") {
        unwindClause = clause;
      } else if (clause.type === "RETURN") {
        returnClause = clause;
      } else {
        // Unsupported clause in this pattern
        return null;
      }
    }
    
    // Must have MATCH, WITH, UNWIND, and RETURN
    if (matchClauses.length === 0 || !withClause || !unwindClause || !returnClause) {
      return null;
    }
    
    // WITH must have exactly one item that's a COLLECT function
    if (withClause.items.length !== 1) {
      return null;
    }
    
    const withItem = withClause.items[0];
    if (withItem.expression.type !== "function" || withItem.expression.functionName !== "COLLECT") {
      return null;
    }
    
    const collectAlias = withItem.alias;
    if (!collectAlias) {
      return null;
    }
    
    // UNWIND must reference the COLLECT alias
    if (unwindClause.expression.type !== "variable" || unwindClause.expression.variable !== collectAlias) {
      return null;
    }
    
    // Execute in two phases:
    // Phase 1: Run MATCH with COLLECT to get the aggregated array
    // Phase 2: Expand the array and return results
    
    // Build a query to get the collected values
    const collectArg = withItem.expression.args?.[0];
    if (!collectArg) {
      return null;
    }
    
    // Build a MATCH...RETURN query that collects the values
    const collectQuery: Query = {
      clauses: [
        ...matchClauses,
        {
          type: "RETURN" as const,
          items: [{
            expression: withItem.expression,
            alias: collectAlias,
          }],
        },
      ],
    };
    
    // Translate and execute the collect query
    const translator = new Translator(params);
    const collectTranslation = translator.translate(collectQuery);
    
    let collectedValues: unknown[] = [];
    for (const stmt of collectTranslation.statements) {
      const result = this.db.execute(stmt.sql, stmt.params);
      if (result.rows.length > 0) {
        // The result should have a single row with the collected array
        const row = result.rows[0];
        const collected = row[collectAlias];
        if (typeof collected === "string") {
          try {
            collectedValues = JSON.parse(collected);
          } catch {
            collectedValues = [collected];
          }
        } else if (Array.isArray(collected)) {
          collectedValues = collected;
        }
      }
    }
    
    // Build results by expanding the collected values
    const results: Record<string, unknown>[] = [];
    const unwindAlias = unwindClause.alias;
    
    for (const value of collectedValues) {
      const resultRow: Record<string, unknown> = {};
      
      for (const item of returnClause.items) {
        const alias = item.alias || this.getExpressionName(item.expression);
        
        if (item.expression.type === "variable" && item.expression.variable === unwindAlias) {
          resultRow[alias] = value;
        }
      }
      
      if (Object.keys(resultRow).length > 0) {
        results.push(resultRow);
      }
    }
    
    return results;
  }

  /**
   * Handle MATCH+WITH(COLLECT)+DELETE[expr] pattern
   * This handles queries like:
   *   MATCH (:User)-[:FRIEND]->(n)
   *   WITH collect(n) AS friends
   *   DETACH DELETE friends[$friendIndex]
   */
  private tryCollectDeleteExecution(
    query: Query,
    params: Record<string, unknown>
  ): Record<string, unknown>[] | null {
    const clauses = query.clauses;
    
    // Find the pattern: MATCH + WITH (containing COLLECT) + DELETE
    let matchClauses: MatchClause[] = [];
    let withClause: WithClause | null = null;
    let deleteClause: DeleteClause | null = null;
    
    for (const clause of clauses) {
      if (clause.type === "MATCH" || clause.type === "OPTIONAL_MATCH") {
        matchClauses.push(clause);
      } else if (clause.type === "WITH") {
        withClause = clause;
      } else if (clause.type === "DELETE") {
        deleteClause = clause;
      } else {
        // Unsupported clause in this pattern
        return null;
      }
    }
    
    // Must have MATCH, WITH, and DELETE with expressions
    if (matchClauses.length === 0 || !withClause || !deleteClause) {
      return null;
    }
    
    // DELETE must have expressions (not just simple variables)
    if (!deleteClause.expressions || deleteClause.expressions.length === 0) {
      return null;
    }
    
    // WITH must have exactly one item that's a COLLECT function
    if (withClause.items.length !== 1) {
      return null;
    }
    
    const withItem = withClause.items[0];
    if (withItem.expression.type !== "function" || 
        withItem.expression.functionName?.toUpperCase() !== "COLLECT") {
      return null;
    }
    
    const collectAlias = withItem.alias;
    if (!collectAlias) {
      return null;
    }
    
    // Execute in phases:
    // Phase 1: Run MATCH to get individual node IDs, then collect them manually
    const collectArg = withItem.expression.args?.[0];
    if (!collectArg) {
      return null;
    }
    
    // The collect arg should be a variable like 'n'
    if (collectArg.type !== "variable") {
      return null;
    }
    const collectVarName = collectArg.variable!;
    
    // Build a MATCH...RETURN query that returns the node IDs individually
    const matchQuery: Query = {
      clauses: [
        ...matchClauses,
        {
          type: "RETURN" as const,
          items: [{
            expression: {
              type: "function" as const,
              functionName: "ID",
              args: [collectArg],
            },
            alias: "_nodeId",
          }],
        },
      ],
    };
    
    // Translate and execute the match query
    const translator = new Translator(params);
    const matchTranslation = translator.translate(matchQuery);
    
    let collectedIds: string[] = [];
    for (const stmt of matchTranslation.statements) {
      const result = this.db.execute(stmt.sql, stmt.params);
      for (const row of result.rows) {
        const nodeId = row["_nodeId"];
        if (typeof nodeId === "string") {
          collectedIds.push(nodeId);
        }
      }
    }
    
    // Phase 2: Evaluate each delete expression and delete the nodes
    const context: Record<string, unknown> = {
      [collectAlias]: collectedIds,
    };
    
    this.db.transaction(() => {
      for (const expr of deleteClause!.expressions!) {
        // Evaluate the expression to get the node ID
        const nodeId = this.evaluateDeleteExpression(expr, params, context);
        
        if (nodeId) {
          if (deleteClause!.detach) {
            // DETACH DELETE: First delete all edges connected to this node
            this.db.execute("DELETE FROM edges WHERE source_id = ? OR target_id = ?", [nodeId, nodeId]);
          }
          
          // Try deleting from nodes first
          const nodeResult = this.db.execute("DELETE FROM nodes WHERE id = ?", [nodeId]);
          
          if (nodeResult.changes === 0) {
            // Try deleting from edges
            this.db.execute("DELETE FROM edges WHERE id = ?", [nodeId]);
          }
        }
      }
    });
    
    // Return empty result (DELETE doesn't return rows)
    return [];
  }

  /**
   * Evaluate a DELETE expression (like friends[$index]) with collected context
   */
  private evaluateDeleteExpression(
    expr: Expression,
    params: Record<string, unknown>,
    context: Record<string, unknown>
  ): string | null {
    if (expr.type === "variable") {
      const value = context[expr.variable!];
      if (typeof value === "string") {
        return value;
      }
      return null;
    }
    
    if (expr.type === "function" && expr.functionName === "INDEX") {
      // List access: list[index]
      const listExpr = expr.args![0];
      const indexExpr = expr.args![1];
      
      // Get the list
      let list: unknown[];
      if (listExpr.type === "variable") {
        list = context[listExpr.variable!] as unknown[];
      } else {
        // Unsupported list expression
        return null;
      }
      
      // Get the index
      let index: number;
      if (indexExpr.type === "literal") {
        index = indexExpr.value as number;
      } else if (indexExpr.type === "parameter") {
        index = params[indexExpr.name!] as number;
      } else {
        // Unsupported index expression
        return null;
      }
      
      // Handle negative indices
      if (index < 0) {
        index = list.length + index;
      }
      
      if (index >= 0 && index < list.length) {
        const value = list[index];
        return typeof value === "string" ? value : null;
      }
      return null;
    }
    
    return null;
  }

  /**
   * Evaluate UNWIND expressions to get the arrays to iterate over
   */
  private evaluateUnwindExpressions(
    unwindClauses: UnwindClause[],
    params: Record<string, unknown>
  ): unknown[][] {
    return unwindClauses.map((clause) => {
      return this.evaluateListExpression(clause.expression, params);
    });
  }

  /**
   * Evaluate a SKIP or LIMIT expression to a number
   */
  private evaluateSkipLimitExpression(expr: Expression, params: Record<string, unknown>): number {
    if (expr.type === "literal") {
      return expr.value as number;
    } else if (expr.type === "parameter") {
      return params[expr.name!] as number ?? 0;
    } else if (expr.type === "function") {
      // Evaluate functions like toInteger(rand()*9)
      const row = new Map<string, unknown>();
      const result = this.evaluateExpressionInRow(expr, row, params);
      return typeof result === "number" ? Math.floor(result) : 0;
    } else if (expr.type === "binary") {
      // Evaluate binary expressions like rand() * 9
      const row = new Map<string, unknown>();
      const result = this.evaluateExpressionInRow(expr, row, params);
      return typeof result === "number" ? Math.floor(result) : 0;
    }
    return 0;
  }

  /**
   * Evaluate an expression that should return a list
   */
  private evaluateListExpression(expr: Expression, params: Record<string, unknown>): unknown[] {
    if (expr.type === "literal") {
      return expr.value as unknown[];
    } else if (expr.type === "parameter") {
      return params[expr.name!] as unknown[];
    } else if (expr.type === "function") {
      const funcName = expr.functionName?.toUpperCase();
      
      // range(start, end[, step])
      if (funcName === "RANGE") {
        const args = expr.args || [];
        if (args.length < 2) {
          throw new Error("range() requires at least 2 arguments");
        }
        
        const startVal = this.evaluateSimpleExpression(args[0], params);
        const endVal = this.evaluateSimpleExpression(args[1], params);
        const stepVal = args.length > 2 ? this.evaluateSimpleExpression(args[2], params) : 1;
        
        if (typeof startVal !== "number" || typeof endVal !== "number" || typeof stepVal !== "number") {
          throw new Error("range() arguments must be numbers");
        }
        
        const result: number[] = [];
        if (stepVal > 0) {
          for (let i = startVal; i <= endVal; i += stepVal) {
            result.push(i);
          }
        } else if (stepVal < 0) {
          for (let i = startVal; i >= endVal; i += stepVal) {
            result.push(i);
          }
        }
        return result;
      }
      
      throw new Error(`Unsupported function in UNWIND: ${funcName}`);
    }
    
    throw new Error(`Unsupported UNWIND expression type: ${expr.type}`);
  }

  /**
   * Evaluate a simple expression (literals, parameters, basic arithmetic)
   */
  private evaluateSimpleExpression(expr: Expression, params: Record<string, unknown>): unknown {
    if (expr.type === "literal") {
      return expr.value;
    } else if (expr.type === "parameter") {
      return params[expr.name!];
    } else if (expr.type === "binary") {
      const left = this.evaluateSimpleExpression(expr.left!, params) as number;
      const right = this.evaluateSimpleExpression(expr.right!, params) as number;
      
      switch (expr.operator) {
        case "+": return left + right;
        case "-": return left - right;
        case "*": return left * right;
        case "/": return left / right;
        case "%": return left % right;
        case "^": return Math.pow(left, right);
        default: throw new Error(`Unsupported operator: ${expr.operator}`);
      }
    }
    
    throw new Error(`Cannot evaluate expression type: ${expr.type}`);
  }

  /**
   * Generate cartesian product of arrays
   */
  private generateCartesianProduct(arrays: unknown[][]): unknown[][] {
    if (arrays.length === 0) return [[]];
    
    return arrays.reduce<unknown[][]>((acc, curr) => {
      const result: unknown[][] = [];
      for (const a of acc) {
        for (const c of curr) {
          result.push([...a, c]);
        }
      }
      return result;
    }, [[]]);
  }

  /**
   * Resolve properties, including unwind variable references and binary expressions
   */
  private resolvePropertiesWithUnwind(
    props: Record<string, unknown>,
    params: Record<string, unknown>,
    unwindContext: Record<string, unknown>
  ): Record<string, unknown> {
    const resolved: Record<string, unknown> = {};
    for (const [key, value] of Object.entries(props)) {
      resolved[key] = this.resolvePropertyValueWithUnwind(value, params, unwindContext);
    }
    return resolved;
  }

  /**
   * Resolve a single property value, handling binary expressions recursively
   */
  private resolvePropertyValueWithUnwind(
    value: unknown,
    params: Record<string, unknown>,
    unwindContext: Record<string, unknown>
  ): unknown {
    if (
      typeof value === "object" &&
      value !== null &&
      "type" in value
    ) {
      const typedValue = value as { type: string; name?: string; variable?: string; property?: string; operator?: string; left?: unknown; right?: unknown };
      
      if (typedValue.type === "parameter" && typedValue.name) {
        return params[typedValue.name];
      } else if (typedValue.type === "variable" && typedValue.name) {
        // This is an unwind variable reference
        if (!(typedValue.name in unwindContext)) {
          throw new Error(`Variable \`${typedValue.name}\` not defined`);
        }
        return unwindContext[typedValue.name];
      } else if (typedValue.type === "property" && typedValue.variable && typedValue.property) {
        // Property access: x.prop - look up variable value and get property
        const varValue = unwindContext[typedValue.variable];
        if (varValue && typeof varValue === "object" && typedValue.property in (varValue as Record<string, unknown>)) {
          return (varValue as Record<string, unknown>)[typedValue.property];
        }
        return null;
      } else if (typedValue.type === "binary" && typedValue.operator && typedValue.left !== undefined && typedValue.right !== undefined) {
        // Binary expression: evaluate left and right, then apply operator
        const leftVal = this.resolvePropertyValueWithUnwind(typedValue.left, params, unwindContext);
        const rightVal = this.resolvePropertyValueWithUnwind(typedValue.right, params, unwindContext);
        
        // Both must be numbers for arithmetic operations
        const leftNum = typeof leftVal === "number" ? leftVal : Number(leftVal);
        const rightNum = typeof rightVal === "number" ? rightVal : Number(rightVal);
        
        switch (typedValue.operator) {
          case "+": return leftNum + rightNum;
          case "-": return leftNum - rightNum;
          case "*": return leftNum * rightNum;
          case "/": return leftNum / rightNum;
          case "%": return leftNum % rightNum;
          case "^": return Math.pow(leftNum, rightNum);
          default: return null;
        }
      } else if (typedValue.type === "function") {
        // Function call in property value (e.g., datetime())
        const funcValue = typedValue as { type: "function"; name: string; args?: unknown[] };
        return this.evaluateFunctionInProperty(funcValue.name, funcValue.args || [], params, unwindContext);
      } else if (typedValue.type === "map") {
        const mapValue = typedValue as { type: "map"; properties?: Record<string, unknown> };
        const out: Record<string, unknown> = {};
        for (const [k, v] of Object.entries(mapValue.properties || {})) {
          out[k] = this.resolvePropertyValueWithUnwind(v, params, unwindContext);
        }
        return out;
      }
      return value;
    }
    return value;
  }

  /**
   * Evaluate a function call within a property value context
   */
  private evaluateFunctionInProperty(
    funcName: string,
    args: unknown[],
    params: Record<string, unknown>,
    unwindContext: Record<string, unknown>
  ): unknown {
    const upperName = funcName.toUpperCase();

    const pad2 = (n: number): string => String(n).padStart(2, "0");
    const pad4 = (n: number): string => String(n).padStart(4, "0");
    
    switch (upperName) {
      case "DATETIME": {
        // datetime() returns current ISO datetime string
        // datetime(string) parses the string
        // datetime(map) formats from components (optionally with timezone)
        if (args.length > 0) {
          const arg = this.resolvePropertyValueWithUnwind(args[0], params, unwindContext);
          if (arg && typeof arg === "object" && !Array.isArray(arg)) {
            const map = arg as Record<string, unknown>;
            const year = Number(map.year);
            const month = Number(map.month ?? 1);
            const day = Number(map.day ?? 1);
            const hour = Number(map.hour ?? 0);
            const minute = Number(map.minute ?? 0);
            const secondVal = map.second;
            const nanosVal = map.nanosecond;
            const hasSecond = secondVal !== undefined || nanosVal !== undefined;
            const second = Number(secondVal ?? 0);
            const nanos = Number(nanosVal ?? 0);
            const tz = map.timezone !== undefined ? String(map.timezone) : "";
            let time = hasSecond
              ? `${pad2(Math.trunc(hour))}:${pad2(Math.trunc(minute))}:${pad2(Math.trunc(second))}`
              : `${pad2(Math.trunc(hour))}:${pad2(Math.trunc(minute))}`;
            if (nanosVal !== undefined) time += `.${String(Math.trunc(nanos)).padStart(9, "0")}`;
            // Format timezone: IANA names get offset + [name], offsets stay as-is
            let tzPart = tz;
            if (isIANATimezone(tz)) {
              const offset = getTimezoneOffset(tz, Math.trunc(year), Math.trunc(month), Math.trunc(day), Math.trunc(hour), Math.trunc(minute));
              tzPart = `${offset}[${tz}]`;
            }
            return `${pad4(Math.trunc(year))}-${pad2(Math.trunc(month))}-${pad2(Math.trunc(day))}T${time}${tzPart}`;
          }
          return String(arg);
        }
        return new Date().toISOString();
      }
      case "DATE": {
        // date() returns current date string (YYYY-MM-DD)
        // date(string) parses the string
        // date(map) formats from components
        if (args.length > 0) {
          const arg = this.resolvePropertyValueWithUnwind(args[0], params, unwindContext);
          if (arg && typeof arg === "object" && !Array.isArray(arg)) {
            const map = arg as Record<string, unknown>;
            const year = Number(map.year);
            const month = Number(map.month ?? 1);
            const day = Number(map.day ?? 1);
            if (!Number.isFinite(year) || !Number.isFinite(month) || !Number.isFinite(day)) {
              return String(arg).split("T")[0];
            }
            return `${pad4(Math.trunc(year))}-${pad2(Math.trunc(month))}-${pad2(Math.trunc(day))}`;
          }
          return String(arg).split("T")[0];
        }
        return new Date().toISOString().split("T")[0];
      }
      case "TIME": {
        // time() returns current time string (HH:MM:SS)
        // time(map) formats from components (optionally with timezone)
        if (args.length > 0) {
          const arg = this.resolvePropertyValueWithUnwind(args[0], params, unwindContext);
          if (arg && typeof arg === "object" && !Array.isArray(arg)) {
            const map = arg as Record<string, unknown>;
            const hour = Number(map.hour ?? 0);
            const minute = Number(map.minute ?? 0);
            const secondVal = map.second;
            const nanosVal = map.nanosecond;
            const hasSecond = secondVal !== undefined || nanosVal !== undefined;
            const second = Number(secondVal ?? 0);
            const nanos = Number(nanosVal ?? 0);
            const tz = map.timezone !== undefined ? String(map.timezone) : "";
            let out = hasSecond
              ? `${pad2(Math.trunc(hour))}:${pad2(Math.trunc(minute))}:${pad2(Math.trunc(second))}`
              : `${pad2(Math.trunc(hour))}:${pad2(Math.trunc(minute))}`;
            if (nanosVal !== undefined) out += `.${String(Math.trunc(nanos)).padStart(9, "0")}`;
            // For TIME, if IANA timezone convert to offset + [name]
            if (isIANATimezone(tz)) {
              // Time doesn't have full date context, use current date for offset calculation
              const now = new Date();
              const offset = getTimezoneOffset(tz, now.getFullYear(), now.getMonth() + 1, now.getDate(), Math.trunc(hour), Math.trunc(minute));
              return out + `${offset}[${tz}]`;
            }
            return out + tz;
          }
          const str = String(arg);
          const match = str.match(/(\d{2}:\d{2}:\d{2})/);
          return match ? match[1] : str;
        }
        return new Date().toISOString().split("T")[1].split(".")[0];
      }
      case "LOCALDATETIME": {
        if (args.length > 0) {
          const arg = this.resolvePropertyValueWithUnwind(args[0], params, unwindContext);
          if (arg && typeof arg === "object" && !Array.isArray(arg)) {
            const map = arg as Record<string, unknown>;
            const year = Number(map.year);
            const month = Number(map.month ?? 1);
            const day = Number(map.day ?? 1);
            const hour = Number(map.hour ?? 0);
            const minute = Number(map.minute ?? 0);
            const secondVal = map.second;
            const nanosVal = map.nanosecond;
            const hasSecond = secondVal !== undefined || nanosVal !== undefined;
            const second = Number(secondVal ?? 0);
            const nanos = Number(nanosVal ?? 0);
            let time = hasSecond
              ? `${pad2(Math.trunc(hour))}:${pad2(Math.trunc(minute))}:${pad2(Math.trunc(second))}`
              : `${pad2(Math.trunc(hour))}:${pad2(Math.trunc(minute))}`;
            if (nanosVal !== undefined) time += `.${String(Math.trunc(nanos)).padStart(9, "0")}`;
            return `${pad4(Math.trunc(year))}-${pad2(Math.trunc(month))}-${pad2(Math.trunc(day))}T${time}`;
          }
          return String(arg);
        }
        return new Date().toISOString().replace(/Z$/, "");
      }
      case "LOCALTIME": {
        if (args.length > 0) {
          const arg = this.resolvePropertyValueWithUnwind(args[0], params, unwindContext);
          if (arg && typeof arg === "object" && !Array.isArray(arg)) {
            const map = arg as Record<string, unknown>;
            const hour = Number(map.hour ?? 0);
            const minute = Number(map.minute ?? 0);
            const secondVal = map.second;
            const nanosVal = map.nanosecond;
            const hasSecond = secondVal !== undefined || nanosVal !== undefined;
            const second = Number(secondVal ?? 0);
            const nanos = Number(nanosVal ?? 0);
            let out = hasSecond
              ? `${pad2(Math.trunc(hour))}:${pad2(Math.trunc(minute))}:${pad2(Math.trunc(second))}`
              : `${pad2(Math.trunc(hour))}:${pad2(Math.trunc(minute))}`;
            if (nanosVal !== undefined) out += `.${String(Math.trunc(nanos)).padStart(9, "0")}`;
            return out;
          }
          return String(arg);
        }
        return new Date().toISOString().split("T")[1].split(".")[0];
      }
      case "DURATION": {
        // duration(string) parses an ISO 8601 duration string
        // duration(map) builds duration from components
        // Format: P[nY][nM][nW][nD][T[nH][nM][n.nS]]
        if (args.length > 0) {
          const arg = this.resolvePropertyValueWithUnwind(args[0], params, unwindContext);
          if (arg === null) return null;
          if (arg && typeof arg === "object" && !Array.isArray(arg)) {
            const map = arg as Record<string, unknown>;
            
            // Handle fractional values by spilling over to smaller units
            // Neo4j behavior: 0.5 years = 6 months, 0.5 months = ~15.2 days (Gregorian average), etc.
            let years = Number(map.years ?? 0);
            let months = Number(map.months ?? 0);
            let weeks = Number(map.weeks ?? 0);
            let days = Number(map.days ?? 0);
            let hours = Number(map.hours ?? 0);
            let minutes = Number(map.minutes ?? 0);
            let seconds = Number(map.seconds ?? 0);
            let nanoseconds = Number(map.nanoseconds ?? 0);
            
            // Spill fractional parts to smaller units
            const yearFrac = years - Math.trunc(years);
            years = Math.trunc(years);
            months += yearFrac * 12;
            
            const monthFrac = months - Math.trunc(months);
            months = Math.trunc(months);
            days += monthFrac * (365.2425 / 12);  // Neo4j uses Gregorian average (30.436875 days/month)
            
            const weekFrac = weeks - Math.trunc(weeks);
            weeks = Math.trunc(weeks);
            days += weekFrac * 7;
            
            const dayFrac = days - Math.trunc(days);
            days = Math.trunc(days);
            hours += dayFrac * 24;
            
            const hourFrac = hours - Math.trunc(hours);
            hours = Math.trunc(hours);
            minutes += hourFrac * 60;
            
            const minFrac = minutes - Math.trunc(minutes);
            minutes = Math.trunc(minutes);
            seconds += minFrac * 60;
            
            const secFrac = seconds - Math.trunc(seconds);
            seconds = Math.trunc(seconds);
            nanoseconds += secFrac * 1e9;
            nanoseconds = Math.round(nanoseconds);
            
            // Normalize: carry overflow from smaller to larger units
            if (nanoseconds >= 1e9) {
              seconds += Math.floor(nanoseconds / 1e9);
              nanoseconds = nanoseconds % 1e9;
            }
            if (seconds >= 60) {
              minutes += Math.floor(seconds / 60);
              seconds = seconds % 60;
            }
            if (minutes >= 60) {
              hours += Math.floor(minutes / 60);
              minutes = minutes % 60;
            }
            // Note: We don't normalize hours to days or months to years
            // because duration semantics treat these differently in Neo4j
            
            // Build ISO 8601 duration string
            let datePart = "";
            if (years !== 0) datePart += `${years}Y`;
            if (months !== 0) datePart += `${months}M`;
            if (weeks !== 0) datePart += `${weeks}W`;
            if (days !== 0) datePart += `${days}D`;
            
            let timePart = "";
            if (hours !== 0) timePart += `${hours}H`;
            if (minutes !== 0) timePart += `${minutes}M`;
            if (seconds !== 0 || nanoseconds !== 0) {
              if (nanoseconds !== 0) {
                timePart += `${seconds}.${String(Math.trunc(nanoseconds)).padStart(9, "0")}S`;
              } else {
                timePart += `${seconds}S`;
              }
            }
            
            if (timePart !== "") {
              return `P${datePart}T${timePart}`;
            }
            return `P${datePart || "T0S"}`;
          }
          // If string, return as-is
          return String(arg);
        }
        throw new Error("duration() requires an argument");
      }
      case "TIMESTAMP": {
        // timestamp() returns milliseconds since epoch
        return Date.now();
      }
      case "RANDOMUUID": {
        // randomUUID() returns a UUID v4
        return crypto.randomUUID();
      }
      default:
        throw new Error(`Unknown function in property value: ${funcName}`);
    }
  }

  /**
   * Execute CREATE relationship pattern with unwind context
   */
  private executeCreateRelationshipPatternWithUnwind(
    rel: RelationshipPattern,
    createdIds: Map<string, string>,
    params: Record<string, unknown>,
    unwindContext: Record<string, unknown>
  ): void {
    let sourceId: string;
    let targetId: string;
    
    // Determine source node ID
    if (rel.source.variable && createdIds.has(rel.source.variable)) {
      sourceId = createdIds.get(rel.source.variable)!;
    } else if (rel.source.variable && !createdIds.has(rel.source.variable) && !rel.source.label) {
      // Variable referenced but not found and no label - error
      throw new Error(`Cannot resolve source node: ${rel.source.variable}`);
    } else {
      // Create new source node (with or without label - anonymous nodes are valid)
      sourceId = crypto.randomUUID();
      const props = this.resolvePropertiesWithUnwind(rel.source.properties || {}, params, unwindContext);
      this.db.execute(
        "INSERT INTO nodes (id, label, properties) VALUES (?, ?, ?)",
        [sourceId, this.normalizeLabelToJson(rel.source.label), JSON.stringify(props)]
      );
      if (rel.source.variable) {
        createdIds.set(rel.source.variable, sourceId);
      }
    }
    
    // Determine target node ID
    if (rel.target.variable && createdIds.has(rel.target.variable)) {
      targetId = createdIds.get(rel.target.variable)!;
    } else if (rel.target.variable && !createdIds.has(rel.target.variable) && !rel.target.label) {
      // Variable referenced but not found and no label - error
      throw new Error(`Cannot resolve target node: ${rel.target.variable}`);
    } else {
      // Create new target node (with or without label - anonymous nodes are valid)
      targetId = crypto.randomUUID();
      const props = this.resolvePropertiesWithUnwind(rel.target.properties || {}, params, unwindContext);
      this.db.execute(
        "INSERT INTO nodes (id, label, properties) VALUES (?, ?, ?)",
        [targetId, this.normalizeLabelToJson(rel.target.label), JSON.stringify(props)]
      );
      if (rel.target.variable) {
        createdIds.set(rel.target.variable, targetId);
      }
    }
    
    // Swap source/target for left-directed relationships
    const [actualSource, actualTarget] =
      rel.edge.direction === "left" ? [targetId, sourceId] : [sourceId, targetId];
    
    // Create edge
    const edgeId = crypto.randomUUID();
    const edgeType = rel.edge.type || "";
    const edgeProps = this.resolvePropertiesWithUnwind(rel.edge.properties || {}, params, unwindContext);
    
    this.db.execute(
      "INSERT INTO edges (id, type, source_id, target_id, properties) VALUES (?, ?, ?, ?, ?)",
      [edgeId, edgeType, actualSource, actualTarget, JSON.stringify(edgeProps)]
    );
    
    if (rel.edge.variable) {
      createdIds.set(rel.edge.variable, edgeId);
    }
  }

  /**
   * Handle CREATE...RETURN pattern by creating nodes/edges and then querying them back
   */
  private tryCreateReturnExecution(
    query: Query,
    params: Record<string, unknown>
  ): Record<string, unknown>[] | null {
    // Check if this is a CREATE...RETURN pattern (no MATCH)
    const clauses = query.clauses;
    
    // Must have at least CREATE and RETURN
    if (clauses.length < 2) return null;
    
    // Find CREATE and RETURN clauses
    const createClauses: CreateClause[] = [];
    const setClauses: SetClause[] = [];
    let returnClause: ReturnClause | null = null;
    
    for (const clause of clauses) {
      if (clause.type === "CREATE") {
        createClauses.push(clause);
      } else if (clause.type === "SET") {
        setClauses.push(clause);
      } else if (clause.type === "RETURN") {
        returnClause = clause;
      } else if (clause.type === "MATCH") {
        // If there's a MATCH, this is not a pure CREATE...RETURN pattern
        return null;
      } else if (clause.type === "MERGE") {
        // If there's a MERGE, let tryMergeExecution handle it
        return null;
      }
    }
    
    if (createClauses.length === 0 || !returnClause) return null;
    
    // Execute CREATE and track created node IDs
    const createdIds: Map<string, string> = new Map();
    
    for (const createClause of createClauses) {
      for (const pattern of createClause.patterns) {
        if (this.isRelationshipPattern(pattern)) {
          // Handle relationship pattern
          this.executeCreateRelationshipPattern(pattern, createdIds, params);
        } else {
          // Handle node pattern
          const id = crypto.randomUUID();
          const labelJson = this.normalizeLabelToJson(pattern.label);
          const props = this.resolveProperties(pattern.properties || {}, params);
          
          this.db.execute(
            "INSERT INTO nodes (id, label, properties) VALUES (?, ?, ?)",
            [id, labelJson, JSON.stringify(props)]
          );
          
          if (pattern.variable) {
            createdIds.set(pattern.variable, id);
          }
        }
      }
    }
    
    // Execute SET clauses if present
    if (setClauses.length > 0) {
      // Convert createdIds Map to Record for executeSetWithResolvedIds
      const resolvedIds: Record<string, string> = {};
      for (const [variable, id] of createdIds) {
        resolvedIds[variable] = id;
      }
      
      for (const setClause of setClauses) {
        this.executeSetWithResolvedIds(setClause, resolvedIds, params);
      }
    }
    
    // Now query the created nodes/edges based on RETURN items
    const results: Record<string, unknown>[] = [];
    const resultRow: Record<string, unknown> = {};
    
    for (const item of returnClause.items) {
      const alias = item.alias || this.getExpressionName(item.expression);
      
      if (item.expression.type === "variable") {
        const variable = item.expression.variable!;
        const id = createdIds.get(variable);
        
        if (id) {
          // Query the node
          const nodeResult = this.db.execute(
            "SELECT id, label, properties FROM nodes WHERE id = ?",
            [id]
          );
          
          if (nodeResult.rows.length > 0) {
            const row = nodeResult.rows[0];
            // Neo4j 3.5 format: return properties directly
            resultRow[alias] = typeof row.properties === "string" 
              ? JSON.parse(row.properties) 
              : row.properties;
          }
        }
      } else if (item.expression.type === "property") {
        const variable = item.expression.variable!;
        const property = item.expression.property!;
        const id = createdIds.get(variable);
        
        if (id) {
          // Try nodes first
          const nodeResult = this.db.execute(
            `SELECT json_extract(properties, '$.${property}') as value FROM nodes WHERE id = ?`,
            [id]
          );
          
          if (nodeResult.rows.length > 0) {
            resultRow[alias] = this.deepParseJson(nodeResult.rows[0].value);
          } else {
            // Try edges if not found in nodes
            const edgeResult = this.db.execute(
              `SELECT json_extract(properties, '$.${property}') as value FROM edges WHERE id = ?`,
              [id]
            );
            
            if (edgeResult.rows.length > 0) {
              resultRow[alias] = this.deepParseJson(edgeResult.rows[0].value);
            }
          }
        }
      } else if (item.expression.type === "function" && item.expression.functionName === "ID") {
        // Handle id(n) function
        const args = item.expression.args;
        if (args && args.length > 0 && args[0].type === "variable") {
          const variable = args[0].variable!;
          const id = createdIds.get(variable);
          if (id) {
            resultRow[alias] = id;
          }
        }
      } else if (item.expression.type === "function" && item.expression.functionName?.toUpperCase() === "LABELS") {
        // Handle labels(n) function
        const args = item.expression.args;
        if (args && args.length > 0 && args[0].type === "variable") {
          const variable = args[0].variable!;
          const id = createdIds.get(variable);
          if (id) {
            const nodeResult = this.db.execute(
              "SELECT label FROM nodes WHERE id = ?",
              [id]
            );
            if (nodeResult.rows.length > 0) {
              const label = nodeResult.rows[0].label;
              const parsed = typeof label === "string" ? JSON.parse(label) : label;
              resultRow[alias] = Array.isArray(parsed) ? parsed : [parsed];
            }
          }
        }
      } else if (item.expression.type === "function" && item.expression.functionName === "INDEX") {
        // Handle dynamic property access: n['propertyName'] or n['nam' + 'e']
        const args = item.expression.args;
        if (args && args.length === 2 && args[0].type === "variable") {
          const variable = args[0].variable!;
          const id = createdIds.get(variable);
          
          if (id) {
            // Evaluate the property key expression (handles literals, binary expressions like 'nam' + 'e', parameters)
            const propertyKey = this.evaluateExpressionForFilter(args[1], createdIds, params);
            
            if (typeof propertyKey === "string") {
              // Try nodes first
              const nodeResult = this.db.execute(
                `SELECT json_extract(properties, '$.' || ?) as value FROM nodes WHERE id = ?`,
                [propertyKey, id]
              );
              
              if (nodeResult.rows.length > 0) {
                resultRow[alias] = this.deepParseJson(nodeResult.rows[0].value);
              } else {
                // Try edges if not found in nodes
                const edgeResult = this.db.execute(
                  `SELECT json_extract(properties, '$.' || ?) as value FROM edges WHERE id = ?`,
                  [propertyKey, id]
                );
                
                if (edgeResult.rows.length > 0) {
                  resultRow[alias] = this.deepParseJson(edgeResult.rows[0].value);
                }
              }
            }
          }
        }
      }
    }
    
    if (Object.keys(resultRow).length > 0) {
      results.push(resultRow);
    }
    
    // Apply SKIP and LIMIT from returnClause (mutations already happened, now filter results)
    let finalResults = results;
    
    if (returnClause.skip !== undefined && returnClause.skip !== null) {
      const skipValue = this.evaluateSkipLimitExpression(returnClause.skip, params);
      finalResults = finalResults.slice(skipValue);
    }
    
    if (returnClause.limit !== undefined && returnClause.limit !== null) {
      const limitValue = this.evaluateSkipLimitExpression(returnClause.limit, params);
      finalResults = finalResults.slice(0, limitValue);
    }
    
    return finalResults;
  }

  /**
   * Handle MATCH...WITH...MATCH patterns where the second MATCH has a variable-length 
   * pattern with a bound relationship list from the WITH clause.
   * 
   * Example: MATCH ()-[r1]->()-[r2]->() WITH [r1, r2] AS rs MATCH (a)-[rs*]->(b) RETURN a, b
   * 
   * The second MATCH should follow the exact sequence of relationships in rs.
   * Returns null if this pattern is not detected.
   */
  private tryBoundRelationshipListExecution(
    query: Query,
    params: Record<string, unknown>
  ): Record<string, unknown>[] | null {
    const clauses = query.clauses;
    
    // Look for: MATCH...WITH...MATCH...RETURN pattern
    let firstMatch: MatchClause | null = null;
    let withClause: WithClause | null = null;
    let secondMatch: MatchClause | null = null;
    let returnClause: ReturnClause | null = null;
    
    let foundWith = false;
    for (const clause of clauses) {
      if (clause.type === "MATCH" || clause.type === "OPTIONAL_MATCH") {
        if (!foundWith) {
          firstMatch = clause;
        } else {
          secondMatch = clause;
        }
      } else if (clause.type === "WITH") {
        withClause = clause;
        foundWith = true;
      } else if (clause.type === "RETURN") {
        returnClause = clause;
      }
    }
    
    if (!firstMatch || !withClause || !secondMatch || !returnClause) {
      return null;
    }
    
    // Check if WITH creates a list that's used in second MATCH as a var-length pattern
    // Look for pattern like: WITH [r1, r2] AS rs
    // List literals with variables are stored as: { type: "function", functionName: "LIST", args: [...] }
    let boundListVar: string | null = null;
    let listExprVars: string[] = [];
    
    for (const item of withClause.items) {
      if (item.alias && item.expression.type === "function" && 
          item.expression.functionName === "LIST" && item.expression.args) {
        // This is a list literal [r1, r2, ...]
        boundListVar = item.alias;
        listExprVars = [];
        for (const elem of item.expression.args) {
          if (elem.type === "variable" && elem.variable) {
            listExprVars.push(elem.variable);
          }
        }
        break;
      }
    }
    
    if (!boundListVar || listExprVars.length === 0) {
      return null;
    }
    
    // Check if second MATCH has a var-length pattern using the bound list
    let hasVarLengthWithBoundList = false;
    let sourceVar: string | null = null;
    let targetVar: string | null = null;
    
    for (const pattern of secondMatch.patterns) {
      if (this.isRelationshipPattern(pattern)) {
        const relPattern = pattern as RelationshipPattern;
        if (relPattern.edge.variable === boundListVar &&
            (relPattern.edge.minHops !== undefined || relPattern.edge.maxHops !== undefined)) {
          hasVarLengthWithBoundList = true;
          sourceVar = relPattern.source.variable || null;
          targetVar = relPattern.target.variable || null;
          break;
        }
      }
    }
    
    if (!hasVarLengthWithBoundList || !sourceVar || !targetVar) {
      return null;
    }
    
    // Check if sourceVar or targetVar are bound in the WITH clause (not the relationship list)
    // This handles cases like: WITH [r1, r2] AS rs, a AS second, b AS first
    // where first and second are bound from the first MATCH
    let boundSourceFromVar: string | null = null;  // e.g., "b" if "b AS first"
    let boundTargetFromVar: string | null = null;  // e.g., "a" if "a AS second"
    
    for (const item of withClause.items) {
      if (item.alias === sourceVar && item.expression.type === "variable" && item.expression.variable) {
        boundSourceFromVar = item.expression.variable;
      }
      if (item.alias === targetVar && item.expression.type === "variable" && item.expression.variable) {
        boundTargetFromVar = item.expression.variable;
      }
    }
    
    // Build the list of variables to fetch in phase 1
    // Include relationship vars + any bound node vars
    const varsToFetch = [...listExprVars];
    if (boundSourceFromVar && !varsToFetch.includes(boundSourceFromVar)) {
      varsToFetch.push(boundSourceFromVar);
    }
    if (boundTargetFromVar && !varsToFetch.includes(boundTargetFromVar)) {
      varsToFetch.push(boundTargetFromVar);
    }
    
    // Execute the pattern in phases
    // Phase 1: Execute first MATCH to get relationships (and bound nodes if any)
    const phase1Query: Query = {
      clauses: [
        firstMatch,
        {
          type: "RETURN" as const,
          items: varsToFetch.map(v => ({
            expression: { type: "variable" as const, variable: v }
          })),
        },
      ],
    };
    
    const translator1 = new Translator(params);
    const translation1 = translator1.translate(phase1Query);
    
    const phase1Results: Array<Map<string, unknown>> = [];
    for (const stmt of translation1.statements) {
      const result = this.db.execute(stmt.sql, stmt.params);
      for (const row of result.rows) {
        const rowMap = new Map<string, unknown>();
        for (const [key, value] of Object.entries(row)) {
          rowMap.set(key, value);
        }
        phase1Results.push(rowMap);
      }
    }
    
    if (phase1Results.length === 0) {
      return [];
    }
    
    // Apply WITH LIMIT if present
    let limitedResults = phase1Results;
    if (withClause.limit !== undefined) {
      const limitVal = this.evaluateSkipLimitExpression(withClause.limit, params);
      limitedResults = phase1Results.slice(0, limitVal);
    }
    
    // Phase 2: For each row, build the list and follow the relationships
    const finalResults: Record<string, unknown>[] = [];
    
    for (const row of limitedResults) {
      // Build the relationship list from the row
      const relList: unknown[] = [];
      for (const v of listExprVars) {
        const relValue = row.get(v);
        if (relValue !== undefined) {
          relList.push(relValue);
        }
      }
      
      if (relList.length === 0) continue;
      
      // Follow the sequence of relationships to find endpoints
      let currentNodeId: string | null = null;
      let firstNodeId: string | null = null;
      let lastNodeId: string | null = null;
      let valid = true;
      
      for (let i = 0; i < relList.length; i++) {
        const rel = relList[i];
        
        // Extract edge info
        let edgeInfo: { id: string; source_id: string; target_id: string } | null = null;
        
        if (typeof rel === "object" && rel !== null) {
          const relObj = rel as Record<string, unknown>;
          const edgeId = (relObj._nf_id || relObj.id) as string;
          if (edgeId) {
            const edgeResult = this.db.execute(
              "SELECT id, source_id, target_id FROM edges WHERE id = ?",
              [edgeId]
            );
            if (edgeResult.rows.length > 0) {
              const r = edgeResult.rows[0];
              edgeInfo = {
                id: r.id as string,
                source_id: r.source_id as string,
                target_id: r.target_id as string
              };
            }
          }
        } else if (typeof rel === "string") {
          // Could be a JSON string like '{"_nf_id":"uuid"}' or a raw edge ID
          let edgeId = rel;
          try {
            const parsed = JSON.parse(rel);
            if (typeof parsed === "object" && parsed !== null && (parsed._nf_id || parsed.id)) {
              edgeId = (parsed._nf_id || parsed.id) as string;
            }
          } catch {
            // Not JSON, use as-is
          }
          
          const edgeResult = this.db.execute(
            "SELECT id, source_id, target_id FROM edges WHERE id = ?",
            [edgeId]
          );
          if (edgeResult.rows.length > 0) {
            const r = edgeResult.rows[0];
            edgeInfo = {
              id: r.id as string,
              source_id: r.source_id as string,
              target_id: r.target_id as string
            };
          }
        }
        
        if (!edgeInfo) {
          valid = false;
          break;
        }
        
        if (i === 0) {
          // First edge: start from source, go to target
          currentNodeId = edgeInfo.source_id;
          firstNodeId = currentNodeId;
          currentNodeId = edgeInfo.target_id;
        } else {
          // Subsequent edges: verify chain continuity
          if (edgeInfo.source_id !== currentNodeId && edgeInfo.target_id !== currentNodeId) {
            valid = false;
            break;
          }
          currentNodeId = edgeInfo.source_id === currentNodeId ? edgeInfo.target_id : edgeInfo.source_id;
        }
        
        if (i === relList.length - 1) {
          lastNodeId = currentNodeId;
        }
      }
      
      if (!valid || !firstNodeId || !lastNodeId) {
        continue;
      }
      
      // If source/target are bound from WITH, verify path endpoints match
      if (boundSourceFromVar) {
        const boundSourceVal = row.get(boundSourceFromVar);
        const boundSourceId = this.extractNodeId(boundSourceVal);
        if (boundSourceId && boundSourceId !== firstNodeId) {
          // Path start doesn't match bound source node - skip this row
          continue;
        }
      }
      
      if (boundTargetFromVar) {
        const boundTargetVal = row.get(boundTargetFromVar);
        const boundTargetId = this.extractNodeId(boundTargetVal);
        if (boundTargetId && boundTargetId !== lastNodeId) {
          // Path end doesn't match bound target node - skip this row
          continue;
        }
      }
      
      // Look up the nodes
      const firstNodeResult = this.db.execute(
        "SELECT id, label, properties FROM nodes WHERE id = ?",
        [firstNodeId]
      );
      const lastNodeResult = this.db.execute(
        "SELECT id, label, properties FROM nodes WHERE id = ?",
        [lastNodeId]
      );
      
      if (firstNodeResult.rows.length === 0 || lastNodeResult.rows.length === 0) {
        continue;
      }
      
      const firstNode = firstNodeResult.rows[0];
      const lastNode = lastNodeResult.rows[0];
      
      const firstProps = typeof firstNode.properties === "string"
        ? JSON.parse(firstNode.properties)
        : firstNode.properties;
      const lastProps = typeof lastNode.properties === "string"
        ? JSON.parse(lastNode.properties)
        : lastNode.properties;
      
      // Build result based on RETURN items
      const resultRow: Record<string, unknown> = {};
      for (const item of returnClause.items) {
        const alias = item.alias || this.getExpressionName(item.expression);
        
        if (item.expression.type === "variable") {
          if (item.expression.variable === sourceVar) {
            resultRow[alias] = { ...firstProps, _nf_id: firstNode.id };
          } else if (item.expression.variable === targetVar) {
            resultRow[alias] = { ...lastProps, _nf_id: lastNode.id };
          }
        }
      }
      
      if (Object.keys(resultRow).length > 0) {
        finalResults.push(resultRow);
      }
    }
    
    return finalResults;
  }

  /**
   * Handle MERGE clauses that need special execution (relationship patterns or ON CREATE/MATCH SET)
   * Returns null if this is not a MERGE pattern that needs special handling
   */
  private tryMergeExecution(
    query: Query,
    params: Record<string, unknown>
  ): Record<string, unknown>[] | null {
    const clauses = query.clauses;
    
    let matchClauses: MatchClause[] = [];
    let createClauses: CreateClause[] = [];
    let withClauses: WithClause[] = [];
    let mergeClauses: MergeClause[] = [];
    let returnClause: ReturnClause | null = null;
    
    for (const clause of clauses) {
      if (clause.type === "MERGE") {
        mergeClauses.push(clause);
      } else if (clause.type === "RETURN") {
        returnClause = clause;
      } else if (clause.type === "MATCH") {
        matchClauses.push(clause);
      } else if (clause.type === "CREATE") {
        createClauses.push(clause);
      } else if (clause.type === "WITH") {
        withClauses.push(clause);
      } else {
        // Other clause types present - don't handle
        return null;
      }
    }
    
    if (mergeClauses.length === 0) {
      return null;
    }
    
    // Check if any MERGE needs special handling:
    // 1. Has relationship patterns
    // 2. Has ON CREATE SET or ON MATCH SET
    // 3. Has RETURN clause (translator can't handle MERGE + RETURN properly for new nodes)
    // 4. Multiple MERGE clauses (need phased handling)
    // 5. Has path expressions
    const hasRelationshipPattern = mergeClauses.some(m => m.patterns.some(p => this.isRelationshipPattern(p)));
    const hasSetClauses = mergeClauses.some(m => m.onCreateSet || m.onMatchSet);
    const hasPathExpressions = mergeClauses.some(m => m.pathExpressions && m.pathExpressions.length > 0);
    
    if (!hasRelationshipPattern && !hasSetClauses && !hasPathExpressions && !returnClause && mergeClauses.length === 1) {
      // Simple single node MERGE without SET clauses, path expressions, and no RETURN - let translator handle it
      return null;
    }
    
    // For multiple MERGEs with path expressions (like MERGE (a) MERGE (b) MERGE p = (a)-[:R]->(b)),
    // use the new multi-merge handler
    if (mergeClauses.length > 1 && hasPathExpressions) {
      return this.executeMultipleMergeClauses(matchClauses, createClauses, withClauses, mergeClauses, returnClause, params);
    }
    
    // For single MERGE with SET clauses or relationship patterns, use the original handler
    // This preserves the existing behavior for ON CREATE SET / ON MATCH SET
    if (mergeClauses.length === 1) {
      return this.executeMergeWithSetClauses(matchClauses, createClauses, withClauses, mergeClauses[0], returnClause, params);
    }
    
    // For multiple MERGEs without path expressions, use the multi-merge handler
    return this.executeMultipleMergeClauses(matchClauses, createClauses, withClauses, mergeClauses, returnClause, params);
  }

  /**
   * Execute multiple MERGE clauses in sequence, handling path expressions
   */
  private executeMultipleMergeClauses(
    matchClauses: MatchClause[],
    createClauses: CreateClause[],
    withClauses: WithClause[],
    mergeClauses: MergeClause[],
    returnClause: ReturnClause | null,
    params: Record<string, unknown>
  ): Record<string, unknown>[] {
    // Track all matched/created nodes and edges
    const matchedNodes = new Map<string, { id: string; label: string; properties: Record<string, unknown> }>();
    const matchedEdges = new Map<string, { id: string; type: string; source_id: string; target_id: string; properties: Record<string, unknown> }>();
    // Track paths for path expressions
    const paths = new Map<string, unknown[]>();
    
    // Execute MATCH clauses first (if any)
    for (const matchClause of matchClauses) {
      for (const pattern of matchClause.patterns) {
        if (!this.isRelationshipPattern(pattern)) {
          const nodePattern = pattern as NodePattern;
          const matchProps = this.resolveProperties(nodePattern.properties || {}, params);
          
          const conditions: string[] = [];
          const conditionParams: unknown[] = [];
          
          if (nodePattern.label) {
            const labelCondition = this.generateLabelCondition(nodePattern.label);
            conditions.push(labelCondition.sql);
            conditionParams.push(...labelCondition.params);
          }
          
          for (const [key, value] of Object.entries(matchProps)) {
            conditions.push(`json_extract(properties, '$.${key}') = ?`);
            conditionParams.push(value);
          }
          
          const sql = conditions.length > 0
            ? `SELECT id, label, properties FROM nodes WHERE ${conditions.join(" AND ")}`
            : "SELECT id, label, properties FROM nodes";
          
          const result = this.db.execute(sql, conditionParams);
          if (result.rows.length > 0 && nodePattern.variable) {
            const row = result.rows[0];
            matchedNodes.set(nodePattern.variable, {
              id: row.id as string,
              label: row.label as string,
              properties: this.getNodeProperties(
                typeof row.id === "string" ? row.id : "",
                typeof row.properties === "string" || (typeof row.properties === "object" && row.properties !== null) ? row.properties : "{}"
              ),
            });
          }
        }
      }
    }
    
    // Execute CREATE clauses (if any)
    for (const createClause of createClauses) {
      for (const pattern of createClause.patterns) {
        if (!this.isRelationshipPattern(pattern)) {
          const nodePattern = pattern as NodePattern;
          const props = this.resolveProperties(nodePattern.properties || {}, params);
          const id = crypto.randomUUID();
          const labelJson = nodePattern.label ? JSON.stringify([nodePattern.label]) : "[]";
          
          this.db.execute(
            "INSERT INTO nodes (id, label, properties) VALUES (?, ?, ?)",
            [id, labelJson, JSON.stringify(props)]
          );
          
          if (nodePattern.variable) {
            matchedNodes.set(nodePattern.variable, {
              id,
              label: nodePattern.label as string || "",
              properties: props,
            });
          }
        }
      }
    }
    
    // Execute MERGE clauses in sequence
    for (const mergeClause of mergeClauses) {
      for (const pattern of mergeClause.patterns) {
        if (this.isRelationshipPattern(pattern)) {
          // Relationship MERGE
          const relPattern = pattern as RelationshipPattern;
          this.executeMergeRelationshipWithTracking(relPattern, mergeClause, matchedNodes, matchedEdges, params);
        } else {
          // Node MERGE
          const nodePattern = pattern as NodePattern;
          this.executeMergeNodeWithTracking(nodePattern, mergeClause, matchedNodes, params);
        }
      }
      
      // Handle path expressions for this MERGE clause
      if (mergeClause.pathExpressions && mergeClause.pathExpressions.length > 0) {
        for (const pathExpr of mergeClause.pathExpressions) {
          const path = this.buildPathFromPatternsWithTracking(
            pathExpr.patterns,
            matchedNodes,
            matchedEdges
          );
          if (path !== null) {
            paths.set(pathExpr.variable, path);
          }
        }
      }
    }
    
    // Process RETURN clause if present
    if (returnClause) {
      return this.processReturnClauseWithPaths(returnClause, matchedNodes, matchedEdges, paths, params);
    }
    
    return [];
  }

  /**
   * Execute a MERGE for a node pattern, tracking the result
   */
  private executeMergeNodeWithTracking(
    pattern: NodePattern,
    mergeClause: MergeClause,
    matchedNodes: Map<string, { id: string; label: string; properties: Record<string, unknown> }>,
    params: Record<string, unknown>
  ): void {
    // If variable is already bound, use the existing node
    if (pattern.variable && matchedNodes.has(pattern.variable)) {
      return;
    }
    
    const matchProps = this.resolvePropertiesWithMatchedNodes(pattern.properties || {}, params, matchedNodes);
    
    // Build WHERE conditions to find existing node
    const conditions: string[] = [];
    const conditionParams: unknown[] = [];
    
    if (pattern.label) {
      const labelCondition = this.generateLabelCondition(pattern.label);
      conditions.push(labelCondition.sql);
      conditionParams.push(...labelCondition.params);
    }
    
    for (const [key, value] of Object.entries(matchProps)) {
      conditions.push(`json_extract(properties, '$.${key}') = ?`);
      conditionParams.push(value);
    }
    
    // Try to find existing node
    let findResult: { rows: Record<string, unknown>[] };
    if (conditions.length > 0) {
      const findSql = `SELECT id, label, properties FROM nodes WHERE ${conditions.join(" AND ")}`;
      findResult = this.db.execute(findSql, conditionParams);
    } else {
      findResult = this.db.execute("SELECT id, label, properties FROM nodes LIMIT 1");
    }
    
    let nodeId: string;
    let wasCreated = false;
    
    if (findResult.rows.length === 0) {
      // Node doesn't exist - create it
      nodeId = crypto.randomUUID();
      wasCreated = true;
      
      const nodeProps = { ...matchProps };
      
      // Apply ON CREATE SET properties
      if (mergeClause.onCreateSet) {
        for (const assignment of mergeClause.onCreateSet) {
          if (!assignment.value || !assignment.property) continue;
          const value = this.evaluateExpressionWithMatchedNodes(assignment.value, params, matchedNodes);
          nodeProps[assignment.property] = value;
        }
      }
      
      const labelJson = pattern.label 
        ? (Array.isArray(pattern.label) ? JSON.stringify(pattern.label) : JSON.stringify([pattern.label]))
        : "[]";
      
      this.db.execute(
        "INSERT INTO nodes (id, label, properties) VALUES (?, ?, ?)",
        [nodeId, labelJson, JSON.stringify(nodeProps)]
      );
    } else {
      // Node exists - apply ON MATCH SET
      nodeId = findResult.rows[0].id as string;
      
      if (mergeClause.onMatchSet) {
        for (const assignment of mergeClause.onMatchSet) {
          if (!assignment.value || !assignment.property) continue;
          const value = this.evaluateExpressionWithMatchedNodes(assignment.value, params, matchedNodes);
          this.db.execute(
            `UPDATE nodes SET properties = json_set(properties, '$.${assignment.property}', json(?)) WHERE id = ?`,
            [JSON.stringify(value), nodeId]
          );
        }
      }
    }
    
    // Store the node in matchedNodes
    if (pattern.variable) {
      const nodeResult = this.db.execute("SELECT id, label, properties FROM nodes WHERE id = ?", [nodeId]);
      if (nodeResult.rows.length > 0) {
        const row = nodeResult.rows[0];
        matchedNodes.set(pattern.variable, {
          id: row.id as string,
          label: row.label as string,
          properties: typeof row.properties === "string" ? JSON.parse(row.properties) : row.properties,
        });
      }
    }
  }

  /**
   * Execute a MERGE for a relationship pattern, tracking the result
   */
  private executeMergeRelationshipWithTracking(
    pattern: RelationshipPattern,
    mergeClause: MergeClause,
    matchedNodes: Map<string, { id: string; label: string; properties: Record<string, unknown> }>,
    matchedEdges: Map<string, { id: string; type: string; source_id: string; target_id: string; properties: Record<string, unknown> }>,
    params: Record<string, unknown>
  ): void {
    // Get source and target node IDs
    const sourceVar = pattern.source.variable;
    const targetVar = pattern.target.variable;
    
    const sourceNode = sourceVar ? matchedNodes.get(sourceVar) : null;
    const targetNode = targetVar ? matchedNodes.get(targetVar) : null;
    
    if (!sourceNode || !targetNode) {
      // Can't create relationship without both endpoints
      return;
    }
    
    const edgeType = pattern.edge.type || "";
    
    // Validate no explicit null properties in MERGE (check AST before resolution)
    this.validateMergeAstPropertiesNotNull(pattern.edge.properties || {}, "relationship", params);
    
    const edgeProps = this.resolvePropertiesWithMatchedNodes(pattern.edge.properties || {}, params, matchedNodes);
    
    // Adjust for direction
    const [actualSource, actualTarget] = 
      pattern.edge.direction === "left" ? [targetNode.id, sourceNode.id] : [sourceNode.id, targetNode.id];
    
    // Find existing edge
    const findSql = `SELECT id, type, source_id, target_id, properties FROM edges WHERE source_id = ? AND target_id = ? AND type = ?`;
    const findResult = this.db.execute(findSql, [actualSource, actualTarget, edgeType]);
    
    let edgeId: string;
    
    if (findResult.rows.length > 0) {
      // Edge exists
      edgeId = findResult.rows[0].id as string;
      
      // Apply ON MATCH SET
      if (mergeClause.onMatchSet) {
        for (const assignment of mergeClause.onMatchSet) {
          if (!assignment.value || !assignment.property) continue;
          const value = this.evaluateExpressionWithMatchedNodes(assignment.value, params, matchedNodes);
          this.db.execute(
            `UPDATE edges SET properties = json_set(properties, '$.${assignment.property}', json(?)) WHERE id = ?`,
            [JSON.stringify(value), edgeId]
          );
        }
      }
    } else {
      // Create edge
      edgeId = crypto.randomUUID();
      
      // Apply ON CREATE SET to props
      if (mergeClause.onCreateSet) {
        for (const assignment of mergeClause.onCreateSet) {
          if (!assignment.value || !assignment.property) continue;
          const value = this.evaluateExpressionWithMatchedNodes(assignment.value, params, matchedNodes);
          edgeProps[assignment.property] = value;
        }
      }
      
      this.db.execute(
        "INSERT INTO edges (id, type, source_id, target_id, properties) VALUES (?, ?, ?, ?, ?)",
        [edgeId, edgeType, actualSource, actualTarget, JSON.stringify(edgeProps)]
      );
    }
    
    // Store the edge in matchedEdges
    if (pattern.edge.variable) {
      const edgeResult = this.db.execute("SELECT id, type, source_id, target_id, properties FROM edges WHERE id = ?", [edgeId]);
      if (edgeResult.rows.length > 0) {
        const row = edgeResult.rows[0];
        matchedEdges.set(pattern.edge.variable, {
          id: row.id as string,
          type: row.type as string,
          source_id: row.source_id as string,
          target_id: row.target_id as string,
          properties: typeof row.properties === "string" ? JSON.parse(row.properties) : row.properties,
        });
      }
    }
  }

  /**
   * Build a path from patterns using tracked nodes and edges
   */
  private buildPathFromPatternsWithTracking(
    patterns: (NodePattern | RelationshipPattern)[],
    matchedNodes: Map<string, { id: string; label: string; properties: Record<string, unknown> }>,
    matchedEdges: Map<string, { id: string; type: string; source_id: string; target_id: string; properties: Record<string, unknown> }>
  ): unknown[] | null {
    if (patterns.length === 0) return null;
    
    const pathElements: unknown[] = [];
    
    for (const pattern of patterns) {
      if (this.isRelationshipPattern(pattern)) {
        const relPattern = pattern as RelationshipPattern;
        
        // Get source node
        const sourceVar = relPattern.source.variable;
        const sourceNode = sourceVar ? matchedNodes.get(sourceVar) : null;
        if (!sourceNode) continue;
        
        // Add source node props (excluding internal _nf_id)
        if (pathElements.length === 0) {
          pathElements.push({ ...sourceNode.properties });
        }
        
        // Get edge
        const edgeVar = relPattern.edge.variable;
        const edge = edgeVar ? matchedEdges.get(edgeVar) : null;
        const edgeProps = edge ? { ...edge.properties } : {};
        pathElements.push(edgeProps);
        
        // Get target node
        const targetVar = relPattern.target.variable;
        const targetNode = targetVar ? matchedNodes.get(targetVar) : null;
        if (!targetNode) continue;
        
        pathElements.push({ ...targetNode.properties });
      }
    }
    
    return pathElements.length > 0 ? pathElements : null;
  }

  /**
   * Process RETURN clause with paths support
   */
  private processReturnClauseWithPaths(
    returnClause: ReturnClause,
    matchedNodes: Map<string, { id: string; label: string; properties: Record<string, unknown> }>,
    matchedEdges: Map<string, { id: string; type: string; source_id: string; target_id: string; properties: Record<string, unknown> }>,
    paths: Map<string, unknown[]>,
    params: Record<string, unknown>
  ): Record<string, unknown>[] {
    const results: Record<string, unknown>[] = [];
    const resultRow: Record<string, unknown> = {};
    
    for (const item of returnClause.items) {
      const alias = item.alias || this.getExpressionName(item.expression);
      
      if (item.expression.type === "variable") {
        const varName = item.expression.variable!;
        
        // Check if it's a path
        const path = paths.get(varName);
        if (path) {
          resultRow[alias] = path;
          continue;
        }
        
        // Check if it's a node
        const node = matchedNodes.get(varName);
        if (node) {
          resultRow[alias] = node.properties;
          continue;
        }
        
        // Check if it's an edge
        const edge = matchedEdges.get(varName);
        if (edge) {
          resultRow[alias] = edge.properties;
          continue;
        }
      } else if (item.expression.type === "property") {
        const varName = item.expression.variable!;
        const propName = item.expression.property!;
        
        const node = matchedNodes.get(varName);
        if (node) {
          resultRow[alias] = node.properties[propName];
          continue;
        }
        
        const edge = matchedEdges.get(varName);
        if (edge) {
          resultRow[alias] = edge.properties[propName];
          continue;
        }
      } else if (item.expression.type === "function") {
        const funcName = item.expression.functionName?.toUpperCase();
        
        if (funcName === "TYPE" && item.expression.args?.length === 1) {
          const arg = item.expression.args[0];
          if (arg.type === "variable") {
            const edge = matchedEdges.get(arg.variable!);
            if (edge) {
              resultRow[alias] = edge.type;
              continue;
            }
          }
        } else if (funcName === "ID" && item.expression.args?.length === 1) {
          const arg = item.expression.args[0];
          if (arg.type === "variable") {
            const node = matchedNodes.get(arg.variable!);
            if (node) {
              resultRow[alias] = node.id;
              continue;
            }
            const edge = matchedEdges.get(arg.variable!);
            if (edge) {
              resultRow[alias] = edge.id;
              continue;
            }
          }
        }
      }
    }
    
    results.push(resultRow);
    return results;
  }

  /**
   * Execute a MERGE clause with ON CREATE SET and/or ON MATCH SET
   * 
   * This needs to handle the case where MATCH returns multiple rows.
   * For each MATCH row, we execute the MERGE (with its ON CREATE SET / ON MATCH SET)
   * and collect results for RETURN.
   */
  private executeMergeWithSetClauses(
    matchClauses: MatchClause[],
    createClauses: CreateClause[],
    withClauses: WithClause[],
    mergeClause: MergeClause,
    returnClause: ReturnClause | null,
    params: Record<string, unknown>
  ): Record<string, unknown>[] {
    // Validate that variables used in ON CREATE SET / ON MATCH SET are defined
    const validVariables = new Set<string>();
    
    // Collect variables from MATCH clauses
    for (const matchClause of matchClauses) {
      for (const pattern of matchClause.patterns) {
        if (this.isRelationshipPattern(pattern)) {
          const relPattern = pattern as RelationshipPattern;
          if (relPattern.source?.variable) validVariables.add(relPattern.source.variable);
          if (relPattern.target?.variable) validVariables.add(relPattern.target.variable);
          if (relPattern.edge?.variable) validVariables.add(relPattern.edge.variable);
        } else {
          const nodePattern = pattern as NodePattern;
          if (nodePattern.variable) validVariables.add(nodePattern.variable);
        }
      }
    }
    
    // Collect variables from CREATE clauses
    for (const createClause of createClauses) {
      for (const pattern of createClause.patterns) {
        if (this.isRelationshipPattern(pattern)) {
          const relPattern = pattern as RelationshipPattern;
          if (relPattern.source?.variable) validVariables.add(relPattern.source.variable);
          if (relPattern.target?.variable) validVariables.add(relPattern.target.variable);
          if (relPattern.edge?.variable) validVariables.add(relPattern.edge.variable);
        } else {
          const nodePattern = pattern as NodePattern;
          if (nodePattern.variable) validVariables.add(nodePattern.variable);
        }
      }
    }
    
    // Collect variables from MERGE pattern
    for (const pattern of mergeClause.patterns) {
      if (this.isRelationshipPattern(pattern)) {
        const relPattern = pattern as RelationshipPattern;
        if (relPattern.source?.variable) validVariables.add(relPattern.source.variable);
        if (relPattern.target?.variable) validVariables.add(relPattern.target.variable);
        if (relPattern.edge?.variable) validVariables.add(relPattern.edge.variable);
      } else {
        const nodePattern = pattern as NodePattern;
        if (nodePattern.variable) validVariables.add(nodePattern.variable);
      }
    }
    
    // Collect variables from WITH clauses
    for (const withClause of withClauses) {
      for (const item of withClause.items) {
        if (item.alias) validVariables.add(item.alias);
      }
    }
    
    // Validate ON CREATE SET variables
    if (mergeClause.onCreateSet) {
      for (const assignment of mergeClause.onCreateSet) {
        if (!validVariables.has(assignment.variable)) {
          throw new Error(`SyntaxError: Variable \`${assignment.variable}\` not defined`);
        }
      }
    }
    
    // Validate ON MATCH SET variables
    if (mergeClause.onMatchSet) {
      for (const assignment of mergeClause.onMatchSet) {
        if (!validVariables.has(assignment.variable)) {
          throw new Error(`SyntaxError: Variable \`${assignment.variable}\` not defined`);
        }
      }
    }
    
    // First, execute CREATE clauses to create any prerequisite nodes
    const createdNodes = new Map<string, { id: string; label: string; properties: Record<string, unknown> }>();
    
    for (const createClause of createClauses) {
      for (const pattern of createClause.patterns) {
        if (this.isRelationshipPattern(pattern)) {
          continue;
        }
        
        const nodePattern = pattern as NodePattern;
        const props = this.resolveProperties(nodePattern.properties || {}, params);
        const id = crypto.randomUUID();
        const labelJson = nodePattern.label ? JSON.stringify([nodePattern.label]) : "[]";
        
        this.db.execute(
          "INSERT INTO nodes (id, label, properties) VALUES (?, ?, ?)",
          [id, labelJson, JSON.stringify(props)]
        );
        
        if (nodePattern.variable) {
          const labelStr = Array.isArray(nodePattern.label) 
            ? nodePattern.label.join(":") 
            : (nodePattern.label || "");
          createdNodes.set(nodePattern.variable, {
            id,
            label: labelStr,
            properties: props,
          });
        }
      }
    }
    
    // Execute MATCH clauses to get ALL rows (not just one)
    // For queries like: MATCH (person:Person) MERGE (city:City) ON CREATE SET city.name = person.bornIn
    // We need to iterate over all Person nodes
    
    // Build a list of all MATCH rows
    type MatchRow = Map<string, { id: string; label: string; properties: Record<string, unknown> }>;
    let matchRows: MatchRow[] = [new Map(createdNodes)]; // Start with created nodes in each row
    
    // Extract id() conditions from WHERE clauses
    const idConditions = new Map<string, string>();
    for (const matchClause of matchClauses) {
      if (matchClause.where) {
        this.extractIdConditions(matchClause.where, idConditions, params);
      }
    }
    
    for (const matchClause of matchClauses) {
      // For each pattern within a MATCH clause, we build a Cartesian product
      // MATCH (a:A), (b:B) means: for each A node, for each B node, create a row
      for (const pattern of matchClause.patterns) {
        if (this.isRelationshipPattern(pattern)) {
          throw new Error("Relationship patterns in MATCH before MERGE not yet supported");
        }
        
        const nodePattern = pattern as NodePattern;
        const matchProps = this.resolveProperties(nodePattern.properties || {}, params);
        
        // Build WHERE conditions
        const conditions: string[] = [];
        const conditionParams: unknown[] = [];
        
        if (nodePattern.variable && idConditions.has(nodePattern.variable)) {
          conditions.push("id = ?");
          conditionParams.push(idConditions.get(nodePattern.variable));
        }
        
        if (nodePattern.label) {
          const labelCondition = this.generateLabelCondition(nodePattern.label);
          conditions.push(labelCondition.sql);
          conditionParams.push(...labelCondition.params);
        }
        
        for (const [key, value] of Object.entries(matchProps)) {
          conditions.push(`json_extract(properties, '$.${key}') = ?`);
          conditionParams.push(value);
        }
        
        const findSql = conditions.length > 0
          ? `SELECT id, label, properties FROM nodes WHERE ${conditions.join(" AND ")}`
          : `SELECT id, label, properties FROM nodes`;
        const findResult = this.db.execute(findSql, conditionParams);
        
        // For each existing match row, create new rows for each found node (Cartesian product)
        const newMatchRows: MatchRow[] = [];
        for (const existingRow of matchRows) {
          for (const sqlRow of findResult.rows) {
            const newRow = new Map(existingRow);
            if (nodePattern.variable) {
              const labelValue = typeof sqlRow.label === "string" ? JSON.parse(sqlRow.label) : sqlRow.label;
              newRow.set(nodePattern.variable, {
                id: sqlRow.id as string,
                label: labelValue,
                properties: typeof sqlRow.properties === "string" ? JSON.parse(sqlRow.properties) : sqlRow.properties,
              });
            }
            newMatchRows.push(newRow);
          }
        }
        
        // Update matchRows after processing each pattern
        if (newMatchRows.length > 0) {
          matchRows = newMatchRows;
        }
      }
    }
    
    // Process WITH clauses to handle aliasing
    for (const withClause of withClauses) {
      for (const item of withClause.items) {
        const alias = item.alias;
        const expr = item.expression;
        
        if (alias && expr.type === "variable" && expr.variable) {
          const sourceVar = expr.variable;
          for (const row of matchRows) {
            const sourceNode = row.get(sourceVar);
            if (sourceNode) {
              row.set(alias, sourceNode);
            }
          }
        }
      }
    }
    
    // Now execute MERGE for each match row and collect results
    const allResults: Record<string, unknown>[] = [];
    const patterns = mergeClause.patterns;
    
    // Check if RETURN has aggregate functions - need special handling
    const hasAggregate = returnClause && returnClause.items.some(item => 
      this.expressionHasAggregate(item.expression)
    );
    
    // Track all matched nodes and edges for aggregation
    const allMatchedNodeRows: Map<string, { id: string; label: string; properties: Record<string, unknown> }>[] = [];
    const allMatchedEdgeRows: Map<string, { id: string; type: string; source_id: string; target_id: string; properties: Record<string, unknown> }>[] = [];
    
    for (const matchRow of matchRows) {
      // Convert matchRow to the format expected by executeMergeNodeForRow
      const matchedNodes = new Map(matchRow);
      const matchedEdges = new Map<string, { id: string; type: string; source_id: string; target_id: string; properties: Record<string, unknown> }>();
      
      if (patterns.length === 1 && !this.isRelationshipPattern(patterns[0])) {
        if (hasAggregate) {
          // Execute MERGE without RETURN processing - we'll aggregate later
          this.executeMergeNodeForRow(patterns[0] as NodePattern, mergeClause, null, params, matchedNodes);
          allMatchedNodeRows.push(new Map(matchedNodes));
        } else {
          const rowResults = this.executeMergeNodeForRow(patterns[0] as NodePattern, mergeClause, returnClause, params, matchedNodes);
          allResults.push(...rowResults);
        }
      } else if (patterns.length === 1 && this.isRelationshipPattern(patterns[0])) {
        if (hasAggregate) {
          // Execute MERGE without RETURN processing - we'll aggregate later
          // We need a version that captures the edge info but doesn't process RETURN
          this.executeMergeRelationshipForRowWithTracking(
            patterns[0] as RelationshipPattern, 
            mergeClause, 
            params, 
            matchedNodes, 
            matchedEdges
          );
          allMatchedNodeRows.push(new Map(matchedNodes));
          allMatchedEdgeRows.push(new Map(matchedEdges));
        } else {
          const rowResults = this.executeMergeRelationshipForRow(patterns[0] as RelationshipPattern, mergeClause, returnClause, params, matchedNodes);
          allResults.push(...rowResults);
        }
      } else {
        throw new Error("Complex MERGE patterns not yet supported");
      }
    }
    
    // If we have aggregates, process them now
    if (hasAggregate && returnClause) {
      return this.processAggregateReturn(returnClause, allMatchedNodeRows, allMatchedEdgeRows, params);
    }
    
    return allResults;
  }
  
  /**
   * Execute a simple node MERGE for a single input row
   * Used when there's a MATCH before MERGE - this handles one row at a time
   */
  private executeMergeNodeForRow(
    pattern: NodePattern,
    mergeClause: MergeClause,
    returnClause: ReturnClause | null,
    params: Record<string, unknown>,
    matchedNodes: Map<string, { id: string; label: string; properties: Record<string, unknown> }>
  ): Record<string, unknown>[] {
    const matchProps = this.resolvePropertiesWithMatchedNodes(pattern.properties || {}, params, matchedNodes);
    
    // Build WHERE conditions to find existing node
    const conditions: string[] = [];
    const conditionParams: unknown[] = [];
    
    if (pattern.label) {
      const labelCondition = this.generateLabelCondition(pattern.label);
      conditions.push(labelCondition.sql);
      conditionParams.push(...labelCondition.params);
    }
    
    for (const [key, value] of Object.entries(matchProps)) {
      conditions.push(`json_extract(properties, '$.${key}') = ?`);
      conditionParams.push(value);
    }
    
    // Try to find existing node
    let findResult: { rows: Record<string, unknown>[] };
    if (conditions.length > 0) {
      const findSql = `SELECT id, label, properties FROM nodes WHERE ${conditions.join(" AND ")}`;
      findResult = this.db.execute(findSql, conditionParams);
    } else {
      findResult = this.db.execute("SELECT id, label, properties FROM nodes LIMIT 1");
    }
    
    let nodeId: string;
    let wasCreated = false;
    
    if (findResult.rows.length === 0) {
      // Node doesn't exist - create it
      nodeId = crypto.randomUUID();
      wasCreated = true;
      
      const nodeProps = { ...matchProps };
      const additionalLabels: string[] = [];
      
      // Apply ON CREATE SET properties
      if (mergeClause.onCreateSet) {
        for (const assignment of mergeClause.onCreateSet) {
          if (assignment.labels && assignment.labels.length > 0) {
            additionalLabels.push(...assignment.labels);
            continue;
          }
          if (!assignment.value || !assignment.property) continue;
          const value = this.evaluateExpressionWithMatchedNodes(assignment.value, params, matchedNodes);
          nodeProps[assignment.property] = value;
        }
      }
      
      const allLabels = pattern.label 
        ? (Array.isArray(pattern.label) ? [...pattern.label] : [pattern.label])
        : [];
      allLabels.push(...additionalLabels);
      const labelJson = JSON.stringify(allLabels);
      
      this.db.execute(
        "INSERT INTO nodes (id, label, properties) VALUES (?, ?, ?)",
        [nodeId, labelJson, JSON.stringify(nodeProps)]
      );
    } else {
      // Node exists - apply ON MATCH SET
      nodeId = findResult.rows[0].id as string;
      
      if (mergeClause.onMatchSet) {
        for (const assignment of mergeClause.onMatchSet) {
          if (assignment.labels && assignment.labels.length > 0) {
            const newLabelsJson = JSON.stringify(assignment.labels);
            this.db.execute(
              `UPDATE nodes SET label = (SELECT json_group_array(value) FROM (
                SELECT DISTINCT value FROM (
                  SELECT value FROM json_each(nodes.label)
                  UNION ALL
                  SELECT value FROM json_each(?)
                ) ORDER BY value
              )) WHERE id = ?`,
              [newLabelsJson, nodeId]
            );
            continue;
          }
          if (!assignment.value || !assignment.property) continue;
          const value = this.evaluateExpressionWithMatchedNodes(assignment.value, params, matchedNodes);
          this.db.execute(
            `UPDATE nodes SET properties = json_set(properties, '$.${assignment.property}', json(?)) WHERE id = ?`,
            [JSON.stringify(value), nodeId]
          );
        }
      }
    }
    
    // Store the node in matchedNodes for RETURN processing
    if (pattern.variable) {
      const nodeResult = this.db.execute("SELECT id, label, properties FROM nodes WHERE id = ?", [nodeId]);
      if (nodeResult.rows.length > 0) {
        const row = nodeResult.rows[0];
        matchedNodes.set(pattern.variable, {
          id: row.id as string,
          label: row.label as string,
          properties: typeof row.properties === "string" ? JSON.parse(row.properties) : row.properties,
        });
      }
    }
    
    // If there's a RETURN clause, process it
    if (returnClause) {
      return this.processReturnClause(returnClause, matchedNodes, params);
    }
    
    return [];
  }
  
  /**
   * Execute a relationship MERGE for a single input row
   */
  private executeMergeRelationshipForRow(
    pattern: RelationshipPattern,
    mergeClause: MergeClause,
    returnClause: ReturnClause | null,
    params: Record<string, unknown>,
    matchedNodes: Map<string, { id: string; label: string; properties: Record<string, unknown> }>
  ): Record<string, unknown>[] {
    // Delegate to the existing executeMergeRelationship for now
    return this.executeMergeRelationship(pattern, mergeClause, returnClause, params, matchedNodes);
  }
  
  /**
   * Execute a relationship MERGE for a single input row, tracking edges for aggregation
   * This version doesn't process RETURN - it just executes the MERGE and captures the edge info
   */
  private executeMergeRelationshipForRowWithTracking(
    pattern: RelationshipPattern,
    mergeClause: MergeClause,
    params: Record<string, unknown>,
    matchedNodes: Map<string, { id: string; label: string; properties: Record<string, unknown> }>,
    matchedEdges: Map<string, { id: string; type: string; source_id: string; target_id: string; properties: Record<string, unknown> }>
  ): void {
    const sourceVar = pattern.source.variable;
    const targetVar = pattern.target.variable;
    const edgeType = pattern.edge.type || "";
    
    if (!edgeType) {
      throw new Error("MERGE requires a relationship type");
    }
    
    this.validateMergeAstPropertiesNotNull(pattern.edge.properties || {}, "relationship", params);
    this.validateMergeAstPropertiesNotNull(pattern.source.properties || {}, "node", params);
    this.validateMergeAstPropertiesNotNull(pattern.target.properties || {}, "node", params);
    
    const edgeProps = this.resolveProperties(pattern.edge.properties || {}, params);
    const sourceProps = this.resolveProperties(pattern.source.properties || {}, params);
    const targetProps = this.resolveProperties(pattern.target.properties || {}, params);
    
    // Resolve or create source node
    let sourceNodeId: string;
    if (sourceVar && matchedNodes.has(sourceVar)) {
      sourceNodeId = matchedNodes.get(sourceVar)!.id;
    } else {
      const sourceResult = this.findOrCreateNode(pattern.source, sourceProps, params);
      sourceNodeId = sourceResult.id;
      if (sourceVar) {
        matchedNodes.set(sourceVar, sourceResult);
      }
    }
    
    // Resolve or create target node
    let targetNodeId: string;
    if (targetVar && matchedNodes.has(targetVar)) {
      targetNodeId = matchedNodes.get(targetVar)!.id;
    } else {
      const targetResult = this.findOrCreateNode(pattern.target, targetProps, params);
      targetNodeId = targetResult.id;
      if (targetVar) {
        matchedNodes.set(targetVar, targetResult);
      }
    }
    
    // Check if the relationship already exists
    const findEdgeConditions: string[] = [
      "source_id = ?",
      "target_id = ?",
    ];
    const findEdgeParams: unknown[] = [sourceNodeId, targetNodeId];
    
    if (edgeType) {
      findEdgeConditions.push("type = ?");
      findEdgeParams.push(edgeType);
    }
    
    for (const [key, value] of Object.entries(edgeProps)) {
      findEdgeConditions.push(`json_extract(properties, '$.${key}') = ?`);
      findEdgeParams.push(value);
    }
    
    const findEdgeSql = `SELECT id, type, source_id, target_id, properties FROM edges WHERE ${findEdgeConditions.join(" AND ")}`;
    const findEdgeResult = this.db.execute(findEdgeSql, findEdgeParams);
    
    let edgeId: string;
    
    if (findEdgeResult.rows.length === 0) {
      // Create new edge
      edgeId = crypto.randomUUID();
      const finalEdgeProps = { ...edgeProps };
      
      // Apply ON CREATE SET to edge
      if (mergeClause.onCreateSet) {
        for (const assignment of mergeClause.onCreateSet) {
          if (assignment.labels) continue;
          if (!assignment.value || !assignment.property) continue;
          // Check if assignment is for the edge variable
          if (pattern.edge.variable && assignment.variable === pattern.edge.variable) {
            const value = this.evaluateExpression(assignment.value, params);
            finalEdgeProps[assignment.property] = value;
          }
        }
      }
      
      this.db.execute(
        "INSERT INTO edges (id, type, source_id, target_id, properties) VALUES (?, ?, ?, ?, ?)",
        [edgeId, edgeType, sourceNodeId, targetNodeId, JSON.stringify(finalEdgeProps)]
      );
    } else {
      // Edge exists
      edgeId = findEdgeResult.rows[0].id as string;
      
      // Apply ON MATCH SET to edge
      if (mergeClause.onMatchSet) {
        for (const assignment of mergeClause.onMatchSet) {
          if (assignment.labels) continue;
          if (!assignment.value || !assignment.property) continue;
          // Check if assignment is for the edge variable
          if (pattern.edge.variable && assignment.variable === pattern.edge.variable) {
            const value = this.evaluateExpression(assignment.value, params);
            this.db.execute(
              `UPDATE edges SET properties = json_set(properties, '$.${assignment.property}', json(?)) WHERE id = ?`,
              [JSON.stringify(value), edgeId]
            );
          }
        }
      }
    }
    
    // Store edge in matchedEdges
    if (pattern.edge.variable) {
      const edgeResult = this.db.execute(
        "SELECT id, type, source_id, target_id, properties FROM edges WHERE id = ?",
        [edgeId]
      );
      if (edgeResult.rows.length > 0) {
        const row = edgeResult.rows[0];
        matchedEdges.set(pattern.edge.variable, {
          id: row.id as string,
          type: row.type as string,
          source_id: row.source_id as string,
          target_id: row.target_id as string,
          properties: typeof row.properties === "string" ? JSON.parse(row.properties) : row.properties,
        });
      }
    }
  }
  
  /**
   * Process RETURN clause with aggregate functions over multiple rows of matched nodes/edges
   */
  private processAggregateReturn(
    returnClause: ReturnClause,
    allMatchedNodeRows: Map<string, { id: string; label: string; properties: Record<string, unknown> }>[],
    allMatchedEdgeRows: Map<string, { id: string; type: string; source_id: string; target_id: string; properties: Record<string, unknown> }>[],
    params: Record<string, unknown>
  ): Record<string, unknown>[] {
    const resultRow: Record<string, unknown> = {};
    
    for (const item of returnClause.items) {
      const alias = item.alias || this.getExpressionName(item.expression);
      
      if (item.expression.type === "function") {
        const funcName = item.expression.functionName?.toUpperCase();
        
        if (funcName === "COUNT") {
          // count(r) or count(*) - count the number of rows
          resultRow[alias] = allMatchedNodeRows.length > 0 ? allMatchedNodeRows.length : allMatchedEdgeRows.length;
        } else if (funcName === "COLLECT") {
          // collect(r) - collect values from all rows
          const args = item.expression.args;
          if (args && args.length > 0 && args[0].type === "variable") {
            const varName = args[0].variable!;
            const collected: unknown[] = [];
            
            // Try collecting from edges first
            for (const edgeRow of allMatchedEdgeRows) {
              const edge = edgeRow.get(varName);
              if (edge) {
                collected.push(edge.properties);
              }
            }
            
            // If no edges, try nodes
            if (collected.length === 0) {
              for (const nodeRow of allMatchedNodeRows) {
                const node = nodeRow.get(varName);
                if (node) {
                  collected.push(node.properties);
                }
              }
            }
            
            resultRow[alias] = collected;
          }
        }
      }
    }
    
    return [resultRow];
  }
  
  /**
   * Resolve properties with access to matched nodes for property references
   */
  private resolvePropertiesWithMatchedNodes(
    props: Record<string, unknown>,
    params: Record<string, unknown>,
    matchedNodes: Map<string, { id: string; label: string; properties: Record<string, unknown> }>
  ): Record<string, unknown> {
    const resolved: Record<string, unknown> = {};
    for (const [key, value] of Object.entries(props)) {
      resolved[key] = this.resolvePropertyValueWithMatchedNodes(value, params, matchedNodes);
    }
    return resolved;
  }
  
  /**
   * Resolve a single property value with matched nodes context
   */
  private resolvePropertyValueWithMatchedNodes(
    value: unknown,
    params: Record<string, unknown>,
    matchedNodes: Map<string, { id: string; label: string; properties: Record<string, unknown> }>
  ): unknown {
    if (typeof value !== "object" || value === null) {
      return value;
    }
    
    const typed = value as Expression;
    
    if (typed.type === "parameter" && typed.name) {
      return params[typed.name];
    }
    
    if (typed.type === "property" && typed.variable && typed.property) {
      const nodeInfo = matchedNodes.get(typed.variable);
      if (nodeInfo) {
        return nodeInfo.properties[typed.property];
      }
      return null;
    }
    
    return value;
  }
  
  /**
   * Evaluate an expression with matched nodes context
   */
  private evaluateExpressionWithMatchedNodes(
    expr: Expression,
    params: Record<string, unknown>,
    matchedNodes: Map<string, { id: string; label: string; properties: Record<string, unknown> }>
  ): unknown {
    if (expr.type === "literal") {
      return expr.value;
    }
    
    if (expr.type === "parameter" && expr.name) {
      return params[expr.name];
    }
    
    if (expr.type === "property" && expr.variable && expr.property) {
      const nodeInfo = matchedNodes.get(expr.variable);
      if (nodeInfo) {
        return nodeInfo.properties[expr.property];
      }
      return null;
    }
    
    if (expr.type === "binary" && expr.left && expr.right && expr.operator) {
      const left = this.evaluateExpressionWithMatchedNodes(expr.left, params, matchedNodes);
      const right = this.evaluateExpressionWithMatchedNodes(expr.right, params, matchedNodes);
      
      const leftNum = left as number;
      const rightNum = right as number;
      
      switch (expr.operator) {
        case "+": return leftNum + rightNum;
        case "-": return leftNum - rightNum;
        case "*": return leftNum * rightNum;
        case "/": return leftNum / rightNum;
        case "%": return leftNum % rightNum;
        default: return null;
      }
    }
    
    return null;
  }

  /**
   * Execute a simple node MERGE
   */
  private executeMergeNode(
    pattern: NodePattern,
    mergeClause: MergeClause,
    returnClause: ReturnClause | null,
    params: Record<string, unknown>,
    matchedNodes: Map<string, { id: string; label: string; properties: Record<string, unknown> }>
  ): Record<string, unknown>[] {
    const matchProps = this.resolveProperties(pattern.properties || {}, params);
    
    // Build WHERE conditions to find existing node
    const conditions: string[] = [];
    const conditionParams: unknown[] = [];
    
    if (pattern.label) {
      const labelCondition = this.generateLabelCondition(pattern.label);
      conditions.push(labelCondition.sql);
      conditionParams.push(...labelCondition.params);
    }
    
    for (const [key, value] of Object.entries(matchProps)) {
      conditions.push(`json_extract(properties, '$.${key}') = ?`);
      conditionParams.push(value);
    }
    
    // Try to find existing node
    let findResult: { rows: Record<string, unknown>[] };
    if (conditions.length > 0) {
      const findSql = `SELECT id, label, properties FROM nodes WHERE ${conditions.join(" AND ")}`;
      findResult = this.db.execute(findSql, conditionParams);
    } else {
      // MERGE with no label and no properties - match any node
      findResult = this.db.execute("SELECT id, label, properties FROM nodes LIMIT 1");
    }
    
    let nodeId: string;
    let wasCreated = false;
    
    if (findResult.rows.length === 0) {
      // Node doesn't exist - create it
      nodeId = crypto.randomUUID();
      wasCreated = true;
      
      // Start with match properties
      const nodeProps = { ...matchProps };
      
      // Collect additional labels from ON CREATE SET
      const additionalLabels: string[] = [];
      
      // Apply ON CREATE SET properties
      if (mergeClause.onCreateSet) {
        // Convert matchedNodes to resolvedIds format for expression evaluation
        const resolvedIds: Record<string, string> = {};
        for (const [varName, nodeInfo] of matchedNodes) {
          resolvedIds[varName] = nodeInfo.id;
        }
        
        for (const assignment of mergeClause.onCreateSet) {
          // Handle label assignments: ON CREATE SET a:Label
          if (assignment.labels && assignment.labels.length > 0) {
            additionalLabels.push(...assignment.labels);
            continue;
          }
          if (!assignment.value || !assignment.property) continue;
          const value = assignment.value.type === "property" || assignment.value.type === "binary"
            ? this.evaluateExpressionWithContext(assignment.value, params, resolvedIds)
            : this.evaluateExpression(assignment.value, params);
          nodeProps[assignment.property] = value;
        }
      }
      
      // Combine pattern label with additional labels
      const allLabels = pattern.label 
        ? (Array.isArray(pattern.label) ? [...pattern.label] : [pattern.label])
        : [];
      allLabels.push(...additionalLabels);
      const labelJson = JSON.stringify(allLabels);
      
      this.db.execute(
        "INSERT INTO nodes (id, label, properties) VALUES (?, ?, ?)",
        [nodeId, labelJson, JSON.stringify(nodeProps)]
      );
    } else {
      // Node exists - apply ON MATCH SET
      nodeId = findResult.rows[0].id as string;
      
      if (mergeClause.onMatchSet) {
        // Convert matchedNodes to resolvedIds format for expression evaluation
        const resolvedIds: Record<string, string> = {};
        for (const [varName, nodeInfo] of matchedNodes) {
          resolvedIds[varName] = nodeInfo.id;
        }
        
        for (const assignment of mergeClause.onMatchSet) {
          // Handle label assignments: ON MATCH SET a:Label
          if (assignment.labels && assignment.labels.length > 0) {
            const newLabelsJson = JSON.stringify(assignment.labels);
            this.db.execute(
              `UPDATE nodes SET label = (SELECT json_group_array(value) FROM (
                SELECT DISTINCT value FROM (
                  SELECT value FROM json_each(nodes.label)
                  UNION ALL
                  SELECT value FROM json_each(?)
                ) ORDER BY value
              )) WHERE id = ?`,
              [newLabelsJson, nodeId]
            );
            continue;
          }
          if (!assignment.value || !assignment.property) continue;
          const value = assignment.value.type === "property" || assignment.value.type === "binary"
            ? this.evaluateExpressionWithContext(assignment.value, params, resolvedIds)
            : this.evaluateExpression(assignment.value, params);
          this.db.execute(
            `UPDATE nodes SET properties = json_set(properties, '$.${assignment.property}', json(?)) WHERE id = ?`,
            [JSON.stringify(value), nodeId]
          );
        }
      }
    }
    
    // Store the node in matchedNodes for RETURN processing
    if (pattern.variable) {
      const nodeResult = this.db.execute("SELECT id, label, properties FROM nodes WHERE id = ?", [nodeId]);
      if (nodeResult.rows.length > 0) {
        const row = nodeResult.rows[0];
        matchedNodes.set(pattern.variable, {
          id: row.id as string,
          label: row.label as string,
          properties: typeof row.properties === "string" ? JSON.parse(row.properties) : row.properties,
        });
      }
    }
    
    // If there's a RETURN clause, process it
    if (returnClause) {
      return this.processReturnClause(returnClause, matchedNodes, params);
    }
    
    return [];
  }

  /**
   * Execute a relationship MERGE: MERGE (a)-[:TYPE]->(b)
   * Handles multiple scenarios:
   * 1. MATCH (a), (b) MERGE (a)-[:REL]->(b) - both nodes already matched
   * 2. MATCH (a) MERGE (a)-[:REL]->(b:Label {props}) - source matched, target to find/create
   * 3. MERGE (a:Label)-[:REL]->(b:Label) - entire pattern to find/create
   */
  private executeMergeRelationship(
    pattern: RelationshipPattern,
    mergeClause: MergeClause,
    returnClause: ReturnClause | null,
    params: Record<string, unknown>,
    matchedNodes: Map<string, { id: string; label: string; properties: Record<string, unknown> }>
  ): Record<string, unknown>[] {
    const sourceVar = pattern.source.variable;
    const targetVar = pattern.target.variable;
    const edgeType = pattern.edge.type || "";
    
    // MERGE requires a relationship type
    if (!edgeType) {
      throw new Error("MERGE requires a relationship type");
    }
    // Validate no explicit null properties in MERGE (check AST before resolution)
    this.validateMergeAstPropertiesNotNull(pattern.edge.properties || {}, "relationship", params);
    this.validateMergeAstPropertiesNotNull(pattern.source.properties || {}, "node", params);
    this.validateMergeAstPropertiesNotNull(pattern.target.properties || {}, "node", params);
    
    const edgeProps = this.resolveProperties(pattern.edge.properties || {}, params);
    const sourceProps = this.resolveProperties(pattern.source.properties || {}, params);
    const targetProps = this.resolveProperties(pattern.target.properties || {}, params);
    
    // Track edges for RETURN
    const matchedEdges = new Map<string, { id: string; type: string; source_id: string; target_id: string; properties: Record<string, unknown> }>();
    
    // Resolve or create source node
    let sourceNodeId: string;
    if (sourceVar && matchedNodes.has(sourceVar)) {
      // Source already matched from MATCH clause
      sourceNodeId = matchedNodes.get(sourceVar)!.id;
    } else {
      // Need to find or create source node
      const sourceResult = this.findOrCreateNode(pattern.source, sourceProps, params);
      sourceNodeId = sourceResult.id;
      if (sourceVar) {
        matchedNodes.set(sourceVar, sourceResult);
      }
    }
    
    // Resolve or create target node
    let targetNodeId: string;
    if (targetVar && matchedNodes.has(targetVar)) {
      // Target already matched from MATCH clause
      targetNodeId = matchedNodes.get(targetVar)!.id;
    } else {
      // Need to find or create target node
      const targetResult = this.findOrCreateNode(pattern.target, targetProps, params);
      targetNodeId = targetResult.id;
      if (targetVar) {
        matchedNodes.set(targetVar, targetResult);
      }
    }
    
    // Check if the relationship already exists between these two nodes
    const findEdgeConditions: string[] = [
      "source_id = ?",
      "target_id = ?",
    ];
    const findEdgeParams: unknown[] = [sourceNodeId, targetNodeId];
    
    if (edgeType) {
      findEdgeConditions.push("type = ?");
      findEdgeParams.push(edgeType);
    }
    
    // Add edge property conditions if any
    for (const [key, value] of Object.entries(edgeProps)) {
      findEdgeConditions.push(`json_extract(properties, '$.${key}') = ?`);
      findEdgeParams.push(value);
    }
    
    const findEdgeSql = `SELECT id, type, source_id, target_id, properties FROM edges WHERE ${findEdgeConditions.join(" AND ")}`;
    const findEdgeResult = this.db.execute(findEdgeSql, findEdgeParams);
    
    let edgeId: string;
    let wasCreated = false;
    
    if (findEdgeResult.rows.length === 0) {
      // Relationship doesn't exist - create it
      edgeId = crypto.randomUUID();
      wasCreated = true;
      
      // Start with the pattern properties
      const finalEdgeProps = { ...edgeProps };
      
      // Apply ON CREATE SET properties (these apply to the target node, not the edge in this pattern)
      if (mergeClause.onCreateSet) {
        for (const assignment of mergeClause.onCreateSet) {
          // Skip label assignments (handled separately)
          if (assignment.labels) continue;
          if (!assignment.value || !assignment.property) continue;
          const value = this.evaluateExpression(assignment.value, params);
          // Update target node with ON CREATE SET
          this.db.execute(
            `UPDATE nodes SET properties = json_set(properties, '$.${assignment.property}', json(?)) WHERE id = ?`,
            [JSON.stringify(value), targetNodeId]
          );
        }
      }
      
      this.db.execute(
        "INSERT INTO edges (id, type, source_id, target_id, properties) VALUES (?, ?, ?, ?, ?)",
        [edgeId, edgeType, sourceNodeId, targetNodeId, JSON.stringify(finalEdgeProps)]
      );
    } else {
      // Relationship exists - apply ON MATCH SET
      edgeId = findEdgeResult.rows[0].id as string;
      
      if (mergeClause.onMatchSet) {
        for (const assignment of mergeClause.onMatchSet) {
          // Skip label assignments (handled separately)
          if (assignment.labels) continue;
          if (!assignment.value || !assignment.property) continue;
          const value = this.evaluateExpression(assignment.value, params);
          // Update target node with ON MATCH SET
          this.db.execute(
            `UPDATE nodes SET properties = json_set(properties, '$.${assignment.property}', json(?)) WHERE id = ?`,
            [JSON.stringify(value), targetNodeId]
          );
        }
      }
    }
    
    // Store the edge in matchedEdges for RETURN processing
    if (pattern.edge.variable) {
      const edgeResult = this.db.execute(
        "SELECT id, type, source_id, target_id, properties FROM edges WHERE id = ?",
        [edgeId]
      );
      if (edgeResult.rows.length > 0) {
        const row = edgeResult.rows[0];
        matchedEdges.set(pattern.edge.variable, {
          id: row.id as string,
          type: row.type as string,
          source_id: row.source_id as string,
          target_id: row.target_id as string,
          properties: typeof row.properties === "string" ? JSON.parse(row.properties) : row.properties,
        });
      }
    }
    
    // Refresh target node data in matchedNodes (may have been updated by ON CREATE/MATCH SET)
    if (targetVar) {
      const nodeResult = this.db.execute("SELECT id, label, properties FROM nodes WHERE id = ?", [targetNodeId]);
      if (nodeResult.rows.length > 0) {
        const row = nodeResult.rows[0];
        matchedNodes.set(targetVar, {
          id: row.id as string,
          label: row.label as string,
          properties: typeof row.properties === "string" ? JSON.parse(row.properties) : row.properties,
        });
      }
    }
    
    // If there's a RETURN clause, process it
    if (returnClause) {
      return this.processReturnClauseWithEdges(returnClause, matchedNodes, matchedEdges, params);
    }
    
    return [];
  }
  
  /**
   * Find an existing node matching the pattern, or create a new one
   */
  private findOrCreateNode(
    pattern: NodePattern,
    props: Record<string, unknown>,
    params: Record<string, unknown>
  ): { id: string; label: string; properties: Record<string, unknown> } {
    // Build conditions to find existing node
    const conditions: string[] = [];
    const conditionParams: unknown[] = [];
    
    if (pattern.label) {
      const labelCondition = this.generateLabelCondition(pattern.label);
      conditions.push(labelCondition.sql);
      conditionParams.push(...labelCondition.params);
    }
    
    for (const [key, value] of Object.entries(props)) {
      conditions.push(`json_extract(properties, '$.${key}') = ?`);
      conditionParams.push(value);
    }
    
    // If we have conditions, try to find existing node
    if (conditions.length > 0) {
      const findSql = `SELECT id, label, properties FROM nodes WHERE ${conditions.join(" AND ")}`;
      const findResult = this.db.execute(findSql, conditionParams);
      
      if (findResult.rows.length > 0) {
        const row = findResult.rows[0];
        return {
          id: row.id as string,
          label: typeof row.label === "string" ? JSON.parse(row.label) : row.label,
          properties: typeof row.properties === "string" ? JSON.parse(row.properties) : row.properties,
        };
      }
    }
    
    // Node doesn't exist - create it
    const nodeId = crypto.randomUUID();
    const labelJson = this.normalizeLabelToJson(pattern.label);
    
    this.db.execute(
      "INSERT INTO nodes (id, label, properties) VALUES (?, ?, ?)",
      [nodeId, labelJson, JSON.stringify(props)]
    );
    
    return {
      id: nodeId,
      label: labelJson,
      properties: props,
    };
  }
  
  /**
   * Process a RETURN clause using matched nodes and edges
   */
  private processReturnClauseWithEdges(
    returnClause: ReturnClause,
    matchedNodes: Map<string, { id: string; label: string; properties: Record<string, unknown> }>,
    matchedEdges: Map<string, { id: string; type: string; source_id: string; target_id: string; properties: Record<string, unknown> }>,
    params: Record<string, unknown>
  ): Record<string, unknown>[] {
    const results: Record<string, unknown>[] = [];
    const resultRow: Record<string, unknown> = {};
    
    for (const item of returnClause.items) {
      const alias = item.alias || this.getExpressionName(item.expression);
      
      if (item.expression.type === "variable") {
        const varName = item.expression.variable!;
        
        // Check if it's a node
        const node = matchedNodes.get(varName);
        if (node) {
          // Neo4j 3.5 format: return properties directly
          resultRow[alias] = node.properties;
          continue;
        }
        
        // Check if it's an edge
        const edge = matchedEdges.get(varName);
        if (edge) {
          // Neo4j 3.5 format: return properties directly
          resultRow[alias] = edge.properties;
          continue;
        }
      } else if (item.expression.type === "property") {
        const varName = item.expression.variable!;
        const propName = item.expression.property!;
        
        const node = matchedNodes.get(varName);
        if (node) {
          resultRow[alias] = node.properties[propName];
          continue;
        }
        
        const edge = matchedEdges.get(varName);
        if (edge) {
          resultRow[alias] = edge.properties[propName];
          continue;
        }
      } else if (item.expression.type === "function") {
        const funcName = item.expression.functionName?.toUpperCase();
        
        if (funcName === "TYPE" && item.expression.args?.length === 1) {
          const arg = item.expression.args[0];
          if (arg.type === "variable") {
            const edge = matchedEdges.get(arg.variable!);
            if (edge) {
              resultRow[alias] = edge.type;
              continue;
            }
          }
        } else if (funcName === "COUNT") {
          // count(*) or count(r) - return 1 for MERGE results
          resultRow[alias] = 1;
          continue;
        }
      } else if (item.expression.type === "comparison") {
        // Handle comparison expressions like: l.created_at = $createdAt
        const left = this.evaluateReturnExpression(item.expression.left!, matchedNodes, params);
        const right = this.evaluateReturnExpression(item.expression.right!, matchedNodes, params);
        const op = item.expression.comparisonOperator;
        
        let result: boolean;
        switch (op) {
          case "=":
            result = left === right;
            break;
          case "<>":
            result = left !== right;
            break;
          case "<":
            result = (left as number) < (right as number);
            break;
          case ">":
            result = (left as number) > (right as number);
            break;
          case "<=":
            result = (left as number) <= (right as number);
            break;
          case ">=":
            result = (left as number) >= (right as number);
            break;
          default:
            result = false;
        }
        resultRow[alias] = result;
      }
    }
    
    results.push(resultRow);
    return results;
  }

  /**
   * Process a RETURN clause using matched nodes
   */
  private processReturnClause(
    returnClause: ReturnClause,
    matchedNodes: Map<string, { id: string; label: string; properties: Record<string, unknown> }>,
    params: Record<string, unknown>
  ): Record<string, unknown>[] {
    const results: Record<string, unknown>[] = [];
    const resultRow: Record<string, unknown> = {};
    
    for (const item of returnClause.items) {
      const alias = item.alias || this.getExpressionName(item.expression);
      
      if (item.expression.type === "variable") {
        const node = matchedNodes.get(item.expression.variable!);
        if (node) {
          // Neo4j 3.5 format: return properties directly
          resultRow[alias] = node.properties;
        }
      } else if (item.expression.type === "property") {
        const node = matchedNodes.get(item.expression.variable!);
        if (node) {
          resultRow[alias] = node.properties[item.expression.property!];
        }
      } else if (item.expression.type === "function") {
        // Handle function expressions like count(*), labels(n)
        const funcName = item.expression.functionName?.toUpperCase();
        
        if (funcName === "COUNT") {
          // count(*) on MERGE results - count the matched/created nodes
          resultRow[alias] = 1; // MERGE always results in exactly one node
        } else if (funcName === "LABELS") {
          // labels(n) function
          const args = item.expression.args;
          if (args && args.length > 0 && args[0].type === "variable") {
            const node = matchedNodes.get(args[0].variable!);
            if (node) {
              const label = this.normalizeLabelForOutput(node.label);
              resultRow[alias] = Array.isArray(label) ? label : (label ? [label] : []);
            }
          }
        }
      } else if (item.expression.type === "comparison") {
        // Handle comparison expressions like: l.created_at = $createdAt
        const left = this.evaluateReturnExpression(item.expression.left!, matchedNodes, params);
        const right = this.evaluateReturnExpression(item.expression.right!, matchedNodes, params);
        const op = item.expression.comparisonOperator;
        
        let result: boolean;
        switch (op) {
          case "=":
            result = left === right;
            break;
          case "<>":
            result = left !== right;
            break;
          case "<":
            result = (left as number) < (right as number);
            break;
          case ">":
            result = (left as number) > (right as number);
            break;
          case "<=":
            result = (left as number) <= (right as number);
            break;
          case ">=":
            result = (left as number) >= (right as number);
            break;
          default:
            result = false;
        }
        resultRow[alias] = result;
      }
    }
    
    results.push(resultRow);
    return results;
  }

  /**
   * Evaluate an expression for RETURN clause
   */
  private evaluateReturnExpression(
    expr: Expression,
    matchedNodes: Map<string, { id: string; label: string; properties: Record<string, unknown> }>,
    params: Record<string, unknown>
  ): unknown {
    if (expr.type === "property") {
      const node = matchedNodes.get(expr.variable!);
      if (node) {
        return node.properties[expr.property!];
      }
      return null;
    } else if (expr.type === "parameter") {
      return params[expr.name!];
    } else if (expr.type === "literal") {
      return expr.value;
    }
    return null;
  }

  /**
   * Execute a CREATE relationship pattern, tracking created IDs
   */
  private executeCreateRelationshipPattern(
    rel: RelationshipPattern,
    createdIds: Map<string, string>,
    params: Record<string, unknown>
  ): void {
    let sourceId: string;
    let targetId: string;
    
    // Determine source node ID
    if (rel.source.variable && createdIds.has(rel.source.variable)) {
      sourceId = createdIds.get(rel.source.variable)!;
    } else if (rel.source.variable && !createdIds.has(rel.source.variable) && !rel.source.label) {
      // Variable referenced but not found and no label - error
      throw new Error(`Cannot resolve source node: ${rel.source.variable}`);
    } else {
      // Create new source node (with or without label - anonymous nodes are valid)
      sourceId = crypto.randomUUID();
      const props = this.resolveProperties(rel.source.properties || {}, params);
      const labelJson = this.normalizeLabelToJson(rel.source.label);
      this.db.execute(
        "INSERT INTO nodes (id, label, properties) VALUES (?, ?, ?)",
        [sourceId, labelJson, JSON.stringify(props)]
      );
      if (rel.source.variable) {
        createdIds.set(rel.source.variable, sourceId);
      }
    }
    
    // Determine target node ID
    if (rel.target.variable && createdIds.has(rel.target.variable)) {
      targetId = createdIds.get(rel.target.variable)!;
    } else if (rel.target.variable && !createdIds.has(rel.target.variable) && !rel.target.label) {
      // Variable referenced but not found and no label - error
      throw new Error(`Cannot resolve target node: ${rel.target.variable}`);
    } else {
      // Create new target node (with or without label - anonymous nodes are valid)
      targetId = crypto.randomUUID();
      const props = this.resolveProperties(rel.target.properties || {}, params);
      const labelJson = this.normalizeLabelToJson(rel.target.label);
      this.db.execute(
        "INSERT INTO nodes (id, label, properties) VALUES (?, ?, ?)",
        [targetId, labelJson, JSON.stringify(props)]
      );
      if (rel.target.variable) {
        createdIds.set(rel.target.variable, targetId);
      }
    }
    
    // Swap source/target for left-directed relationships
    const [actualSource, actualTarget] =
      rel.edge.direction === "left" ? [targetId, sourceId] : [sourceId, targetId];
    
    // Create edge
    const edgeId = crypto.randomUUID();
    const edgeType = rel.edge.type || "";
    const edgeProps = this.resolveProperties(rel.edge.properties || {}, params);
    
    this.db.execute(
      "INSERT INTO edges (id, type, source_id, target_id, properties) VALUES (?, ?, ?, ?, ?)",
      [edgeId, edgeType, actualSource, actualTarget, JSON.stringify(edgeProps)]
    );
    
    if (rel.edge.variable) {
      createdIds.set(rel.edge.variable, edgeId);
    }
  }

  /**
   * Get a name for an expression (for default aliases)
   */
  private getExpressionName(expr: Expression): string {
    switch (expr.type) {
      case "variable":
        return expr.variable!;
      case "property":
        return `${expr.variable}_${expr.property}`;
      case "function": {
        // Build function expression like count(a) or count(*)
        const funcName = expr.functionName!.toLowerCase();
        if (expr.args && expr.args.length > 0) {
          // Special case: INDEX function should be rendered as list[index] notation
          if (funcName === "index" && expr.args.length === 2) {
            const listName = this.getExpressionName(expr.args[0]);
            const indexName = this.getExpressionName(expr.args[1]);
            return `${listName}[${indexName}]`;
          }
          const argNames = expr.args.map(arg => {
            if (arg.type === "variable") return arg.variable!;
            if (arg.type === "property") return `${arg.variable}.${arg.property}`;
            return "?";
          });
          return `${funcName}(${argNames.join(", ")})`;
        }
        // Empty args for count(*) or similar
        return `${funcName}(*)`;
      }
      case "literal": {
        // For literals, use the string representation of the value
        if (expr.value === null) return "NULL";
        if (typeof expr.value === "string") return `'${expr.value}'`;
        // For arrays/objects, use JSON.stringify to preserve nested structure
        if (Array.isArray(expr.value) || typeof expr.value === "object") {
          return JSON.stringify(expr.value);
        }
        return String(expr.value);
      }
      default:
        return "expr";
    }
  }

  /**
   * Detect and handle patterns that need multi-phase execution:
   * - MATCH...CREATE that references matched variables
   * - MATCH...SET that updates matched nodes/edges via relationships
   * - MATCH...DELETE that deletes matched nodes/edges via relationships
   * Returns null if this is not a multi-phase pattern, otherwise returns the result data.
   */
  private tryMultiPhaseExecution(
    query: Query,
    params: Record<string, unknown>
  ): Record<string, unknown>[] | null {
    // Categorize clauses
    const matchClauses: MatchClause[] = [];
    const withClauses: WithClause[] = [];
    const createClauses: CreateClause[] = [];
    const setClauses: SetClause[] = [];
    const deleteClauses: DeleteClause[] = [];
    let returnClause: ReturnClause | null = null;

    for (const clause of query.clauses) {
      switch (clause.type) {
        case "MATCH":
          matchClauses.push(clause);
          break;
        case "WITH":
          withClauses.push(clause);
          break;
        case "CREATE":
          createClauses.push(clause);
          break;
        case "SET":
          setClauses.push(clause);
          break;
        case "DELETE":
          deleteClauses.push(clause);
          break;
        case "RETURN":
          returnClause = clause;
          break;
        default:
          // MERGE and other clauses - use standard execution
          return null;
      }
    }

    // Need at least one MATCH clause for multi-phase
    if (matchClauses.length === 0) {
      return null;
    }

    // Check if any MATCH has relationship patterns (multi-hop)
    const hasRelationshipPattern = matchClauses.some((m) =>
      m.patterns.some((p) => this.isRelationshipPattern(p))
    );

    // Use multi-phase for:
    // - Relationship patterns (multi-hop) - except simple MATCH...RETURN without mutations
    // - MATCH...CREATE referencing matched vars
    // - MATCH...SET (always needs ID resolution)
    // - MATCH...DELETE (always needs ID resolution)
    const hasMutations = createClauses.length > 0 || setClauses.length > 0 || deleteClauses.length > 0;
    const needsMultiPhase = 
      (hasRelationshipPattern && hasMutations) ||
      createClauses.length > 0 ||
      setClauses.length > 0 ||
      deleteClauses.length > 0;

    if (!needsMultiPhase) {
      return null;
    }

    // Collect all variables defined in MATCH clauses
    const matchedVariables = new Set<string>();
    for (const matchClause of matchClauses) {
      for (const pattern of matchClause.patterns) {
        this.collectVariablesFromPattern(pattern, matchedVariables);
      }
    }

    // Build alias map from WITH clauses: alias -> original variable
    // e.g., WITH n AS a creates aliasMap["a"] = "n"
    // Supports chaining: WITH n AS a, then WITH a AS x -> x points to n
    const aliasMap = new Map<string, string>();
    // Also track property expression aliases: alias -> { variable, property }
    // e.g., WITH n.num AS num creates propertyAliasMap["num"] = { variable: "n", property: "num" }
    const propertyAliasMap = new Map<string, { variable: string; property: string }>();
    // Track WITH aggregate aliases: alias -> { functionName, argVariable }
    // e.g., WITH sum(num) AS sum creates withAggregateMap["sum"] = { functionName: "SUM", argVariable: "num" }
    const withAggregateMap = new Map<string, { functionName: string; argVariable: string }>();
    for (const withClause of withClauses) {
      for (const item of withClause.items) {
        if (item.alias && item.expression.type === "variable" && item.expression.variable) {
          const original = item.expression.variable;
          // Track aliases that refer to matched variables OR to existing aliases
          if (matchedVariables.has(original)) {
            aliasMap.set(item.alias, original);
          } else if (aliasMap.has(original)) {
            // Chained alias: x -> a -> n, resolve the chain
            aliasMap.set(item.alias, aliasMap.get(original)!);
          }
        } else if (item.alias && item.expression.type === "property" && item.expression.variable && item.expression.property) {
          // Track property expression aliases
          propertyAliasMap.set(item.alias, { 
            variable: item.expression.variable, 
            property: item.expression.property 
          });
        } else if (item.alias && item.expression.type === "function") {
          // Track WITH aggregate aliases like: WITH sum(num) AS sum
          const funcName = item.expression.functionName?.toUpperCase();
          const aggregateFunctions = ["SUM", "COUNT", "AVG", "MIN", "MAX", "COLLECT"];
          if (funcName && aggregateFunctions.includes(funcName) && item.expression.args?.length === 1) {
            const arg = item.expression.args[0];
            if (arg.type === "variable" && arg.variable) {
              withAggregateMap.set(item.alias, {
                functionName: funcName,
                argVariable: arg.variable,
              });
            }
          }
        }
      }
    }

    // Helper to resolve an alias to its original variable (follows chains)
    const resolveAlias = (varName: string): string | null => {
      if (matchedVariables.has(varName)) return varName;
      if (aliasMap.has(varName)) {
        // The alias map already stores the fully resolved original (no chains)
        return aliasMap.get(varName)!;
      }
      return null;
    };

    // Validate that all variable references in CREATE properties are defined
    // (either matched variables, aliases, or parameters)
    this.validateCreatePropertyVariables(createClauses, matchedVariables, aliasMap, params);

    // Determine which variables need to be resolved for CREATE
    const referencedInCreate = new Set<string>();
    for (const createClause of createClauses) {
      for (const pattern of createClause.patterns) {
        this.findReferencedVariablesWithAliases(pattern, matchedVariables, aliasMap, referencedInCreate);
      }
    }

    // Determine which variables need to be resolved for SET
    const referencedInSet = new Set<string>();
    for (const setClause of setClauses) {
      for (const assignment of setClause.assignments) {
        const resolved = resolveAlias(assignment.variable);
        if (resolved) {
          referencedInSet.add(resolved);
        }
      }
    }

    // Determine which variables need to be resolved for DELETE
    const referencedInDelete = new Set<string>();
    for (const deleteClause of deleteClauses) {
      for (const variable of deleteClause.variables) {
        const resolved = resolveAlias(variable);
        if (resolved) {
          referencedInDelete.add(resolved);
        }
      }
    }

    // Combine all referenced variables
    const allReferencedVars = new Set([
      ...referencedInCreate,
      ...referencedInSet,
      ...referencedInDelete,
    ]);

    // Check if any CREATE/SET/DELETE pattern uses an aliased variable
    // This is the specific case we need multi-phase for: WITH introduces an alias
    // that is then used in a mutation clause
    const aliasesUsedInMutation = new Set<string>();
    
    // Check CREATE patterns for aliased variables
    for (const createClause of createClauses) {
      for (const pattern of createClause.patterns) {
        if (this.isRelationshipPattern(pattern)) {
          if (pattern.source.variable && aliasMap.has(pattern.source.variable)) {
            aliasesUsedInMutation.add(pattern.source.variable);
          }
          if (pattern.target.variable && aliasMap.has(pattern.target.variable)) {
            aliasesUsedInMutation.add(pattern.target.variable);
          }
        }
      }
    }
    
    // Check SET assignments for aliased variables
    for (const setClause of setClauses) {
      for (const assignment of setClause.assignments) {
        if (aliasMap.has(assignment.variable)) {
          aliasesUsedInMutation.add(assignment.variable);
        }
      }
    }
    
    // Check DELETE for aliased variables
    for (const deleteClause of deleteClauses) {
      for (const variable of deleteClause.variables) {
        if (aliasMap.has(variable)) {
          aliasesUsedInMutation.add(variable);
        }
      }
    }

    // Only use multi-phase WITH handling if aliases are actually used in mutations
    const needsWithAliasHandling = aliasesUsedInMutation.size > 0;
    
    // Check if RETURN references any property aliases (like `num` from `WITH n.num AS num`)
    // This requires multi-phase execution because the translator can't handle property aliases
    let returnUsesPropertyAliases = false;
    if (returnClause && propertyAliasMap.size > 0) {
      const returnVars = this.collectReturnVariables(returnClause);
      returnUsesPropertyAliases = returnVars.some(v => propertyAliasMap.has(v));
    }
    
    // Check if RETURN references any WITH aggregate aliases (like `sum` from `WITH sum(num) AS sum`)
    let returnUsesWithAggregates = false;
    if (returnClause && withAggregateMap.size > 0) {
      const returnVars = this.collectReturnVariables(returnClause);
      returnUsesWithAggregates = returnVars.some(v => withAggregateMap.has(v));
    }

    // DEBUG (disabled)
    // console.log("tryMultiPhaseExecution debug:", {
    //   withClauses: withClauses.length,
    //   aliasMap: [...aliasMap.entries()],
    //   propertyAliasMap: [...propertyAliasMap.entries()],
    //   withAggregateMap: [...withAggregateMap.entries()],
    //   aliasesUsedInMutation: [...aliasesUsedInMutation],
    //   needsWithAliasHandling,
    //   returnUsesPropertyAliases,
    //   returnUsesWithAggregates,
    //   createClauses: createClauses.length,
    // });

    // If WITH clauses are present but no aliases are used in mutations AND no property aliases in RETURN AND no WITH aggregates in RETURN, use standard execution
    if (withClauses.length > 0 && !needsWithAliasHandling && !returnUsesPropertyAliases && !returnUsesWithAggregates) {
      return null;
    }

    // If no relationship patterns and nothing references matched vars, use standard execution
    if (!hasRelationshipPattern && allReferencedVars.size === 0 && !needsWithAliasHandling && !returnUsesPropertyAliases && !returnUsesWithAggregates) {
      return null;
    }

    // If WITH aliases are used in mutations, add the original variables to resolution set
    if (needsWithAliasHandling) {
      for (const alias of aliasesUsedInMutation) {
        const original = aliasMap.get(alias);
        if (original) {
          allReferencedVars.add(original);
        }
      }
    }

    // For relationship patterns with SET/DELETE, we need to resolve all matched variables
    if (hasRelationshipPattern && (setClauses.length > 0 || deleteClauses.length > 0)) {
      // Add all matched variables to the resolution set
      for (const v of matchedVariables) {
        allReferencedVars.add(v);
      }
    }

    // Multi-phase execution needed
    return this.executeMultiPhaseGeneral(
      matchClauses,
      withClauses,
      createClauses,
      setClauses,
      deleteClauses,
      returnClause,
      allReferencedVars,
      matchedVariables,
      aliasMap,
      propertyAliasMap,
      withAggregateMap,
      params
    );
  }

  /**
   * Collect variable names from a pattern
   */
  private collectVariablesFromPattern(
    pattern: NodePattern | RelationshipPattern,
    variables: Set<string>
  ): void {
    if (this.isRelationshipPattern(pattern)) {
      if (pattern.source.variable) variables.add(pattern.source.variable);
      if (pattern.target.variable) variables.add(pattern.target.variable);
      if (pattern.edge.variable) variables.add(pattern.edge.variable);
    } else {
      if (pattern.variable) variables.add(pattern.variable);
    }
  }

  /**
   * Find variables in CREATE that reference MATCH variables (with alias support)
   */
  private findReferencedVariablesWithAliases(
    pattern: NodePattern | RelationshipPattern,
    matchedVars: Set<string>,
    aliasMap: Map<string, string>,
    referenced: Set<string>
  ): void {
    const resolveToOriginal = (varName: string | undefined): string | null => {
      if (!varName) return null;
      if (matchedVars.has(varName)) return varName;
      if (aliasMap.has(varName)) return aliasMap.get(varName)!;
      return null;
    };

    if (this.isRelationshipPattern(pattern)) {
      // Source node references a matched variable if it has no label
      if (pattern.source.variable && !pattern.source.label) {
        const original = resolveToOriginal(pattern.source.variable);
        if (original) referenced.add(original);
      }
      // Target node references a matched variable if it has no label
      if (pattern.target.variable && !pattern.target.label) {
        const original = resolveToOriginal(pattern.target.variable);
        if (original) referenced.add(original);
      }
    }
  }

  /**
   * Validate that all variable references in CREATE clause properties are defined.
   * Throws an error if an undefined variable is referenced.
   */
  private validateCreatePropertyVariables(
    createClauses: CreateClause[],
    matchedVariables: Set<string>,
    aliasMap: Map<string, string>,
    params: Record<string, unknown>
  ): void {
    for (const createClause of createClauses) {
      for (const pattern of createClause.patterns) {
        // Track variables that will be defined in this pattern (for relationship patterns)
        const willDefine = new Set<string>();
        
        if (this.isRelationshipPattern(pattern)) {
          // For relationship patterns, check source, edge, and target properties
          // Source node - its variable will be available for subsequent parts
          if (pattern.source.variable) {
            willDefine.add(pattern.source.variable);
          }
          this.validatePropertiesForUndefinedVariables(
            pattern.source.properties || {},
            matchedVariables,
            aliasMap,
            willDefine,
            params
          );
          
          // Edge properties
          if (pattern.edge.variable) {
            willDefine.add(pattern.edge.variable);
          }
          this.validatePropertiesForUndefinedVariables(
            pattern.edge.properties || {},
            matchedVariables,
            aliasMap,
            willDefine,
            params
          );
          
          // Target node properties
          if (pattern.target.variable) {
            willDefine.add(pattern.target.variable);
          }
          this.validatePropertiesForUndefinedVariables(
            pattern.target.properties || {},
            matchedVariables,
            aliasMap,
            willDefine,
            params
          );
        } else {
          // Simple node pattern
          if (pattern.variable) {
            willDefine.add(pattern.variable);
          }
          this.validatePropertiesForUndefinedVariables(
            pattern.properties || {},
            matchedVariables,
            aliasMap,
            willDefine,
            params
          );
        }
      }
    }
  }

  /**
   * Check a properties object for undefined variable references.
   * Throws an error if found.
   */
  private validatePropertiesForUndefinedVariables(
    props: Record<string, unknown>,
    matchedVariables: Set<string>,
    aliasMap: Map<string, string>,
    willDefine: Set<string>,
    params: Record<string, unknown>
  ): void {
    for (const [_key, value] of Object.entries(props)) {
      this.validateValueForUndefinedVariables(value, matchedVariables, aliasMap, willDefine, params);
    }
  }

  /**
   * Recursively check a value for undefined variable references.
   */
  private validateValueForUndefinedVariables(
    value: unknown,
    matchedVariables: Set<string>,
    aliasMap: Map<string, string>,
    willDefine: Set<string>,
    params: Record<string, unknown>
  ): void {
    if (typeof value !== "object" || value === null) return;
    
    const typedValue = value as { type?: string; name?: string; variable?: string; left?: unknown; right?: unknown };
    
    if (typedValue.type === "variable" && typedValue.name) {
      const varName = typedValue.name;
      // Check if it's a matched variable, an alias, or a parameter
      const isValidVar = 
        matchedVariables.has(varName) ||
        aliasMap.has(varName) ||
        willDefine.has(varName) ||
        params.hasOwnProperty(varName);
      
      if (!isValidVar) {
        throw new Error(`Variable \`${varName}\` not defined`);
      }
    } else if (typedValue.type === "property" && typedValue.variable) {
      const varName = typedValue.variable;
      // Check if it's a matched variable, an alias, or will be defined
      const isValidVar = 
        matchedVariables.has(varName) ||
        aliasMap.has(varName) ||
        willDefine.has(varName);
      
      if (!isValidVar) {
        throw new Error(`Variable \`${varName}\` not defined`);
      }
    } else if (typedValue.type === "binary") {
      // Recursively check left and right operands
      if (typedValue.left) {
        this.validateValueForUndefinedVariables(typedValue.left, matchedVariables, aliasMap, willDefine, params);
      }
      if (typedValue.right) {
        this.validateValueForUndefinedVariables(typedValue.right, matchedVariables, aliasMap, willDefine, params);
      }
    }
    // Parameters and literals are always valid, no check needed
  }

  /**
   * Execute a complex pattern with MATCH...CREATE/SET/DELETE in multiple phases
   */
  private executeMultiPhaseGeneral(
    matchClauses: MatchClause[],
    withClauses: WithClause[],
    createClauses: CreateClause[],
    setClauses: SetClause[],
    deleteClauses: DeleteClause[],
    returnClause: ReturnClause | null,
    referencedVars: Set<string>,
    allMatchedVars: Set<string>,
    aliasMap: Map<string, string>,
    propertyAliasMap: Map<string, { variable: string; property: string }>,
    withAggregateMap: Map<string, { functionName: string; argVariable: string }>,
    params: Record<string, unknown>
  ): Record<string, unknown>[] {
    // Phase 1: Execute MATCH to get actual node/edge IDs
    const varsToResolve = referencedVars.size > 0 ? referencedVars : allMatchedVars;
    
    // Also need to include variables referenced by property aliases
    for (const [_, propInfo] of propertyAliasMap) {
      if (allMatchedVars.has(propInfo.variable)) {
        varsToResolve.add(propInfo.variable);
      }
    }
    
    // Collect deleted variables for edge type capture
    const deletedVars = new Set<string>();
    for (const deleteClause of deleteClauses) {
      for (const variable of deleteClause.variables) {
        deletedVars.add(variable);
        // Also add alias resolution
        if (aliasMap.has(variable)) {
          deletedVars.add(aliasMap.get(variable)!);
        }
      }
    }
    
    // Detect type(r) in RETURN where r is a deleted variable - need to capture edge type before deletion
    const edgeTypesToCapture = new Set<string>();
    if (returnClause && deletedVars.size > 0) {
      for (const item of returnClause.items) {
        if (item.expression.type === "function" && 
            item.expression.functionName?.toUpperCase() === "TYPE" &&
            item.expression.args?.length === 1 &&
            item.expression.args[0].type === "variable") {
          const varName = item.expression.args[0].variable!;
          if (deletedVars.has(varName)) {
            edgeTypesToCapture.add(varName);
          }
        }
      }
    }
    
    // Build RETURN items: IDs for all variables + property values for property aliases
    const returnItems: { expression: Expression; alias: string }[] = [];
    
    // Add ID lookups for variables
    for (const v of varsToResolve) {
      returnItems.push({
        expression: { type: "function" as const, functionName: "ID", args: [{ type: "variable" as const, variable: v }] },
        alias: `_id_${v}`,
      });
    }
    
    // Add property value lookups for property aliases
    for (const [alias, propInfo] of propertyAliasMap) {
      returnItems.push({
        expression: { type: "property" as const, variable: propInfo.variable, property: propInfo.property },
        alias: `_prop_${alias}`,
      });
    }
    
    // Add edge type lookups for deleted edges used in type() function
    for (const v of edgeTypesToCapture) {
      returnItems.push({
        expression: { type: "function" as const, functionName: "TYPE", args: [{ type: "variable" as const, variable: v }] },
        alias: `_type_${v}`,
      });
    }
    
    const matchQuery: Query = {
      clauses: [
        ...matchClauses,
        {
          type: "RETURN" as const,
          items: returnItems,
        },
      ],
    };

    const translator = new Translator(params);
    const matchTranslation = translator.translate(matchQuery);

    let matchedRows: Record<string, unknown>[] = [];
    for (const stmt of matchTranslation.statements) {
      const result = this.db.execute(stmt.sql, stmt.params);
      if (result.rows.length > 0) {
        matchedRows = result.rows;
      }
    }

    // If no nodes matched, return empty
    if (matchedRows.length === 0) {
      return [];
    }

    // Phase 2: Execute CREATE/SET/DELETE for each matched row
    // Keep track of all resolved IDs (including newly created nodes) for RETURN
    const allResolvedIds: Record<string, string>[] = [];
    // Also keep track of captured property values (before nodes are deleted)
    const allCapturedPropertyValues: Record<string, unknown>[] = [];
    // Keep track of captured edge types (before edges are deleted)
    const allCapturedEdgeTypes: Record<string, string>[] = [];
    // Keep track of deleted variables
    const allDeletedVariables: Set<string>[] = [];
    
    this.db.transaction(() => {
      for (const row of matchedRows) {
        // Build a map of variable -> actual node/edge ID
        const resolvedIds: Record<string, string> = {};
        for (const v of varsToResolve) {
          resolvedIds[v] = row[`_id_${v}`] as string;
        }

        // Add aliased variable names pointing to the same IDs
        // e.g., if WITH n AS a, then resolvedIds["a"] = resolvedIds["n"]
        for (const [alias, original] of aliasMap.entries()) {
          if (resolvedIds[original]) {
            resolvedIds[alias] = resolvedIds[original];
          }
        }
        
        // Capture property alias values BEFORE any mutations (especially DELETE)
        // e.g., WITH n.num AS num -> capturedPropertyValues["num"] = value
        const capturedPropertyValues: Record<string, unknown> = {};
        for (const [alias, _] of propertyAliasMap) {
          const rawValue = row[`_prop_${alias}`];
          // Parse JSON values if they're strings (SQLite returns JSON as strings)
          capturedPropertyValues[alias] = this.deepParseJson(rawValue);
        }
        
        // Capture edge types BEFORE DELETE for type() function on deleted edges
        const capturedEdgeTypes: Record<string, string> = {};
        for (const v of edgeTypesToCapture) {
          const edgeType = row[`_type_${v}`];
          if (typeof edgeType === "string") {
            capturedEdgeTypes[v] = edgeType;
          }
        }

        // Execute CREATE with resolved IDs (this mutates resolvedIds to include new node IDs)
        for (const createClause of createClauses) {
          this.executeCreateWithResolvedIds(createClause, resolvedIds, params);
        }

        // Execute SET with resolved IDs
        for (const setClause of setClauses) {
          this.executeSetWithResolvedIds(setClause, resolvedIds, params);
        }

        // Execute DELETE with resolved IDs and track deleted variables
        const deletedVariables = new Set<string>();
        for (const deleteClause of deleteClauses) {
          const deleted = this.executeDeleteWithResolvedIds(deleteClause, resolvedIds);
          deleted.forEach(v => deletedVariables.add(v));
        }
        
        // Save the resolved IDs for this row (including newly created nodes)
        allResolvedIds.push({ ...resolvedIds });
        allCapturedPropertyValues.push(capturedPropertyValues);
        allCapturedEdgeTypes.push(capturedEdgeTypes);
        allDeletedVariables.push(deletedVariables);
      }
    });

    // Phase 3: Execute RETURN if present
    if (returnClause) {
      // Check if RETURN references any newly created variables (not in matched vars or aliases)
      const returnVars = this.collectReturnVariables(returnClause);
      const referencesCreatedVars = returnVars.some(v => 
        !allMatchedVars.has(v) && !aliasMap.has(v) && !propertyAliasMap.has(v)
      );
      
      // Check if RETURN references any property aliases (like `num` from `WITH n.num AS num`)
      const referencesPropertyAliases = returnVars.some(v => propertyAliasMap.has(v));
      
      // If SET or DELETE was executed or WITH clauses are present, we need to use buildReturnResults
      // because aliased variables need to be resolved from our ID map
      // For DELETE, nodes are gone so we can't query them - we need to return based on original match count
      if (referencesCreatedVars || referencesPropertyAliases || setClauses.length > 0 || deleteClauses.length > 0 || withClauses.length > 0) {
        // Apply WITH clause WHERE filters to captured property values
        // This handles patterns like: WITH n.num AS num ... DELETE n ... WITH num WHERE num % 2 = 0 ... RETURN num
        let filteredResolvedIds = allResolvedIds;
        let filteredPropertyValues = allCapturedPropertyValues;
        let filteredEdgeTypes = allCapturedEdgeTypes;
        
        for (const withClause of withClauses) {
          if (withClause.where) {
            // Filter the captured values based on the WITH WHERE condition
            const filteredPairs: { resolvedIds: Record<string, string>; propertyValues: Record<string, unknown>; edgeTypes: Record<string, string> }[] = [];
            
            for (let i = 0; i < filteredResolvedIds.length; i++) {
              const resolvedIds = filteredResolvedIds[i];
              const propertyValues = filteredPropertyValues[i] || {};
              const edgeTypes = filteredEdgeTypes[i] || {};
              
              // Check if this row passes the WITH WHERE filter
              const passes = this.evaluateWithWhereConditionWithPropertyAliases(
                withClause.where,
                resolvedIds,
                propertyValues,
                propertyAliasMap,
                params
              );
              
              if (passes) {
                filteredPairs.push({ resolvedIds, propertyValues, edgeTypes });
              }
            }
            
            filteredResolvedIds = filteredPairs.map(p => p.resolvedIds);
            filteredPropertyValues = filteredPairs.map(p => p.propertyValues);
            filteredEdgeTypes = filteredPairs.map(p => p.edgeTypes);
          }
        }
        
        // RETURN references created nodes, aliased vars, property aliases, or data was modified - use buildReturnResults with resolved IDs
        return this.buildReturnResults(returnClause, filteredResolvedIds, filteredPropertyValues, propertyAliasMap, withAggregateMap, filteredEdgeTypes, allDeletedVariables, params);
      } else {
        // RETURN only references matched nodes and no mutations - use translator-based approach
        const fullQuery: Query = {
          clauses: [...matchClauses, returnClause],
        };
        const returnTranslator = new Translator(params);
        const returnTranslation = returnTranslator.translate(fullQuery);

        let rows: Record<string, unknown>[] = [];
        for (const stmt of returnTranslation.statements) {
          const result = this.db.execute(stmt.sql, stmt.params);
          if (result.rows.length > 0 || stmt.sql.trim().toUpperCase().startsWith("SELECT")) {
            rows = result.rows;
          }
        }
        return this.formatResults(rows, returnTranslation.returnColumns);
      }
    }

    return [];
  }
  
  /**
   * Collect variable names referenced in a RETURN clause
   */
  private collectReturnVariables(returnClause: ReturnClause): string[] {
    const vars: string[] = [];
    
    for (const item of returnClause.items) {
      this.collectExpressionVariables(item.expression, vars);
    }
    
    return vars;
  }
  
  /**
   * Collect variable names from an expression
   */
  private collectExpressionVariables(expr: Expression, vars: string[]): void {
    if (expr.type === "variable" && expr.variable) {
      vars.push(expr.variable);
    } else if (expr.type === "property" && expr.variable) {
      vars.push(expr.variable);
    } else if (expr.type === "function" && expr.args) {
      for (const arg of expr.args) {
        this.collectExpressionVariables(arg, vars);
      }
    }
  }
  
  /**
   * Build RETURN results from resolved node/edge IDs
   */
  private buildReturnResults(
    returnClause: ReturnClause,
    allResolvedIds: Record<string, string>[],
    allCapturedPropertyValues: Record<string, unknown>[] = [],
    propertyAliasMap: Map<string, { variable: string; property: string }> = new Map(),
    withAggregateMap: Map<string, { functionName: string; argVariable: string }> = new Map(),
    allCapturedEdgeTypes: Record<string, string>[] = [],
    allDeletedVariables: Set<string>[] = [],
    params: Record<string, unknown> = {}
  ): Record<string, unknown>[] {
    // Check if all return items are aggregates (like count(*))
    // If so, we should return a single aggregated row instead of per-row results
    const allAggregates = returnClause.items.every(item => 
      item.expression.type === "function" && 
      ["COUNT", "SUM", "AVG", "MIN", "MAX", "COLLECT"].includes(item.expression.functionName?.toUpperCase() || "")
    );
    
    // Check if RETURN references a WITH aggregate alias (like `sum` from `WITH sum(num) AS sum`)
    // If so, we need to compute the aggregate and return it
    const returnReferencesWithAggregates = returnClause.items.some(item => 
      item.expression.type === "variable" && 
      item.expression.variable &&
      withAggregateMap.has(item.expression.variable)
    );
    
    // Handle RETURN that references WITH aggregate aliases (like `RETURN sum` where `sum` is from `WITH sum(num) AS sum`)
    if (returnReferencesWithAggregates) {
      const resultRow: Record<string, unknown> = {};
      
      for (const item of returnClause.items) {
        const alias = item.alias || this.getExpressionName(item.expression);
        
        if (item.expression.type === "variable" && item.expression.variable) {
          const varName = item.expression.variable;
          
          // Check if this is a WITH aggregate alias
          if (withAggregateMap.has(varName)) {
            const aggInfo = withAggregateMap.get(varName)!;
            const { functionName, argVariable } = aggInfo;
            
            // Compute the aggregate from captured property values
            if (propertyAliasMap.has(argVariable)) {
              // The argument is a property alias - compute aggregate from captured values
              switch (functionName) {
                case "SUM": {
                  let sum = 0;
                  for (const capturedValues of allCapturedPropertyValues) {
                    const value = capturedValues[argVariable];
                    if (typeof value === "number") {
                      sum += value;
                    } else if (typeof value === "string") {
                      const parsed = parseFloat(value);
                      if (!isNaN(parsed)) sum += parsed;
                    }
                  }
                  resultRow[alias] = sum;
                  break;
                }
                case "COUNT": {
                  resultRow[alias] = allCapturedPropertyValues.length;
                  break;
                }
                case "AVG": {
                  let sum = 0;
                  let count = 0;
                  for (const capturedValues of allCapturedPropertyValues) {
                    const value = capturedValues[argVariable];
                    if (typeof value === "number") {
                      sum += value;
                      count++;
                    } else if (typeof value === "string") {
                      const parsed = parseFloat(value);
                      if (!isNaN(parsed)) {
                        sum += parsed;
                        count++;
                      }
                    }
                  }
                  resultRow[alias] = count > 0 ? sum / count : null;
                  break;
                }
                case "MIN": {
                  let min: number | null = null;
                  for (const capturedValues of allCapturedPropertyValues) {
                    const value = capturedValues[argVariable];
                    const numValue = typeof value === "number" ? value : parseFloat(value as string);
                    if (!isNaN(numValue)) {
                      min = min === null ? numValue : Math.min(min, numValue);
                    }
                  }
                  resultRow[alias] = min;
                  break;
                }
                case "MAX": {
                  let max: number | null = null;
                  for (const capturedValues of allCapturedPropertyValues) {
                    const value = capturedValues[argVariable];
                    const numValue = typeof value === "number" ? value : parseFloat(value as string);
                    if (!isNaN(numValue)) {
                      max = max === null ? numValue : Math.max(max, numValue);
                    }
                  }
                  resultRow[alias] = max;
                  break;
                }
                case "COLLECT": {
                  const values: unknown[] = [];
                  for (const capturedValues of allCapturedPropertyValues) {
                    values.push(capturedValues[argVariable]);
                  }
                  resultRow[alias] = values;
                  break;
                }
              }
            }
          }
        }
      }
      
      return [resultRow];
    }
    
    if (allAggregates && returnClause.items.length > 0) {
      // Return a single row with aggregated values
      const resultRow: Record<string, unknown> = {};
      for (const item of returnClause.items) {
        const alias = item.alias || this.getExpressionName(item.expression);
        const funcName = item.expression.functionName?.toUpperCase();
        
        if (funcName === "COUNT") {
          resultRow[alias] = allResolvedIds.length;
        } else if (funcName === "SUM") {
          // Sum values - may reference property aliases
          const arg = item.expression.args?.[0];
          if (arg?.type === "variable" && arg.variable) {
            const varName = arg.variable;
            let sum = 0;
            
            // Check if this variable is a property alias
            if (propertyAliasMap.has(varName)) {
              // Sum the captured property values
              for (const capturedValues of allCapturedPropertyValues) {
                const value = capturedValues[varName];
                if (typeof value === "number") {
                  sum += value;
                } else if (typeof value === "string") {
                  const parsed = parseFloat(value);
                  if (!isNaN(parsed)) sum += parsed;
                }
              }
            }
            resultRow[alias] = sum;
          }
        } else if (funcName === "COLLECT") {
          // Collect all values for the variable
          const arg = item.expression.args?.[0];
          if (arg?.type === "variable" && arg.variable) {
            const values: unknown[] = [];
            const varName = arg.variable;
            
            // Check if this variable is a property alias
            if (propertyAliasMap.has(varName)) {
              // Collect the captured property values
              for (const capturedValues of allCapturedPropertyValues) {
                values.push(capturedValues[varName]);
              }
            } else {
              // It's a node/edge variable - query from database
              // Neo4j 3.5 format: collect just properties
              for (const resolvedIds of allResolvedIds) {
                const nodeId = resolvedIds[varName];
                if (nodeId) {
                  const nodeResult = this.db.execute(
                    "SELECT properties FROM nodes WHERE id = ?",
                    [nodeId]
                  );
                  if (nodeResult.rows.length > 0) {
                    const row = nodeResult.rows[0];
                    values.push(typeof row.properties === "string" ? JSON.parse(row.properties) : row.properties);
                  }
                }
              }
            }
            resultRow[alias] = values;
          }
        } else if (funcName === "AVG") {
          // Average values - may reference property aliases
          const arg = item.expression.args?.[0];
          if (arg?.type === "variable" && arg.variable) {
            const varName = arg.variable;
            let sum = 0;
            let count = 0;
            
            // Check if this variable is a property alias
            if (propertyAliasMap.has(varName)) {
              // Average the captured property values
              for (const capturedValues of allCapturedPropertyValues) {
                const value = capturedValues[varName];
                if (typeof value === "number") {
                  sum += value;
                  count++;
                } else if (typeof value === "string") {
                  const parsed = parseFloat(value);
                  if (!isNaN(parsed)) {
                    sum += parsed;
                    count++;
                  }
                }
              }
            }
            resultRow[alias] = count > 0 ? sum / count : null;
          }
        } else if (funcName === "MIN") {
          // Min value - may reference property aliases
          const arg = item.expression.args?.[0];
          if (arg?.type === "variable" && arg.variable) {
            const varName = arg.variable;
            let min: number | null = null;
            
            // Check if this variable is a property alias
            if (propertyAliasMap.has(varName)) {
              for (const capturedValues of allCapturedPropertyValues) {
                const value = capturedValues[varName];
                const numValue = typeof value === "number" ? value : parseFloat(value as string);
                if (!isNaN(numValue)) {
                  min = min === null ? numValue : Math.min(min, numValue);
                }
              }
            }
            resultRow[alias] = min;
          }
        } else if (funcName === "MAX") {
          // Max value - may reference property aliases
          const arg = item.expression.args?.[0];
          if (arg?.type === "variable" && arg.variable) {
            const varName = arg.variable;
            let max: number | null = null;
            
            // Check if this variable is a property alias
            if (propertyAliasMap.has(varName)) {
              for (const capturedValues of allCapturedPropertyValues) {
                const value = capturedValues[varName];
                const numValue = typeof value === "number" ? value : parseFloat(value as string);
                if (!isNaN(numValue)) {
                  max = max === null ? numValue : Math.max(max, numValue);
                }
              }
            }
            resultRow[alias] = max;
          }
        }
        // Add other aggregate handlers as needed
      }
      return [resultRow];
    }
    
    const results: Record<string, unknown>[] = [];
    
    for (let i = 0; i < allResolvedIds.length; i++) {
      const resolvedIds = allResolvedIds[i];
      const capturedValues = allCapturedPropertyValues[i] || {};
      const capturedEdgeTypes = allCapturedEdgeTypes[i] || {};
      const resultRow: Record<string, unknown> = {};
      
      for (const item of returnClause.items) {
        const alias = item.alias || this.getExpressionName(item.expression);
        
        if (item.expression.type === "variable") {
          const variable = item.expression.variable!;
          
          // Check if this variable is a property alias
          if (propertyAliasMap.has(variable)) {
            // Use the captured property value
            resultRow[alias] = capturedValues[variable];
          } else {
            // Check if this variable was deleted
            const deletedVariables = allDeletedVariables[i] || new Set();
            if (deletedVariables.has(variable)) {
              throw new Error(`EntityNotFound: The deleted entity ${variable} no longer exists in the graph.`);
            }
            
            const nodeId = resolvedIds[variable];
            
            if (nodeId) {
              // Query the node/edge by ID
              const nodeResult = this.db.execute(
                "SELECT id, label, properties FROM nodes WHERE id = ?",
                [nodeId]
              );
              
              if (nodeResult.rows.length > 0) {
                const row = nodeResult.rows[0];
                // Neo4j 3.5 format: return properties directly
                resultRow[alias] = typeof row.properties === "string"
                  ? JSON.parse(row.properties)
                  : row.properties;
              } else {
                // Try edges
                const edgeResult = this.db.execute(
                  "SELECT id, type, source_id, target_id, properties FROM edges WHERE id = ?",
                  [nodeId]
                );
                if (edgeResult.rows.length > 0) {
                  const row = edgeResult.rows[0];
                  // Neo4j 3.5 format: return properties directly
                  resultRow[alias] = typeof row.properties === "string"
                    ? JSON.parse(row.properties)
                    : row.properties;
                }
              }
            }
          }
        } else if (item.expression.type === "property") {
          const variable = item.expression.variable!;
          const property = item.expression.property!;
          
          // Check if this variable was deleted
          const deletedVariables = allDeletedVariables[i] || new Set();
          if (deletedVariables.has(variable)) {
            throw new Error(`EntityNotFound: The deleted entity ${variable} no longer exists in the graph.`);
          }
          
          const nodeId = resolvedIds[variable];
          
          if (nodeId) {
            // Try nodes first
            const nodeResult = this.db.execute(
              `SELECT json_extract(properties, '$.${property}') as value FROM nodes WHERE id = ?`,
              [nodeId]
            );
            
            if (nodeResult.rows.length > 0) {
              resultRow[alias] = this.deepParseJson(nodeResult.rows[0].value);
            } else {
              // Try edges
              const edgeResult = this.db.execute(
                `SELECT json_extract(properties, '$.${property}') as value FROM edges WHERE id = ?`,
                [nodeId]
              );
              if (edgeResult.rows.length > 0) {
                resultRow[alias] = this.deepParseJson(edgeResult.rows[0].value);
              }
            }
          }
        } else if (item.expression.type === "function" && item.expression.functionName === "ID") {
          // Handle id(n) function
          const args = item.expression.args;
          if (args && args.length > 0 && args[0].type === "variable") {
            const variable = args[0].variable!;
            
            // Check if this variable was deleted
            const deletedVariables = allDeletedVariables[i] || new Set();
            if (deletedVariables.has(variable)) {
              throw new Error(`EntityNotFound: The deleted entity ${variable} no longer exists in the graph.`);
            }
            
            const nodeId = resolvedIds[variable];
            if (nodeId) {
              resultRow[alias] = nodeId;
            }
          }
        } else if (item.expression.type === "function" && item.expression.functionName?.toUpperCase() === "LABELS") {
          // Handle labels(n) function
          const args = item.expression.args;
          if (args && args.length > 0 && args[0].type === "variable") {
            const variable = args[0].variable!;
            
            // Check if this variable was deleted
            const deletedVariables = allDeletedVariables[i] || new Set();
            if (deletedVariables.has(variable)) {
              throw new Error(`EntityNotFound: The deleted entity ${variable} no longer exists in the graph.`);
            }
            
            const nodeId = resolvedIds[variable];
            if (nodeId) {
              const nodeResult = this.db.execute(
                "SELECT label FROM nodes WHERE id = ?",
                [nodeId]
              );
              if (nodeResult.rows.length > 0) {
                const labelValue = nodeResult.rows[0].label;
                // Parse label - could be a JSON array or a string
                if (typeof labelValue === "string") {
                  try {
                    const parsed = JSON.parse(labelValue);
                    resultRow[alias] = Array.isArray(parsed) ? parsed : [parsed];
                  } catch {
                    resultRow[alias] = labelValue ? [labelValue] : [];
                  }
                } else if (Array.isArray(labelValue)) {
                  resultRow[alias] = labelValue;
                } else {
                  resultRow[alias] = [];
                }
              }
            }
          }
        } else if (item.expression.type === "function" && item.expression.functionName?.toUpperCase() === "TYPE") {
          // Handle type(r) function
          const args = item.expression.args;
          if (args && args.length > 0 && args[0].type === "variable") {
            const variable = args[0].variable!;
            
            // Check if this variable was deleted (and not captured)
            const deletedVariables = allDeletedVariables[i] || new Set();
            if (deletedVariables.has(variable) && !capturedEdgeTypes[variable]) {
              throw new Error(`EntityNotFound: The deleted entity ${variable} no longer exists in the graph.`);
            }
            
            // First check if we have a captured edge type (for deleted edges)
            if (capturedEdgeTypes[variable]) {
              resultRow[alias] = capturedEdgeTypes[variable];
            } else {
              // Fall back to querying the database
              const edgeId = resolvedIds[variable];
              if (edgeId) {
                const edgeResult = this.db.execute(
                  "SELECT type FROM edges WHERE id = ?",
                  [edgeId]
                );
                if (edgeResult.rows.length > 0) {
                  resultRow[alias] = edgeResult.rows[0].type;
                }
              }
            }
          }
        } else if (item.expression.type === "function" && item.expression.functionName?.toUpperCase() === "COUNT") {
          // Handle count(*) or count(n) - for MATCH+SET+RETURN patterns
          // If we're in buildReturnResults, return the number of rows we processed
          resultRow[alias] = allResolvedIds.length;
        } else if (item.expression.type === "function" && item.expression.functionName?.toUpperCase() === "SIZE") {
          // Handle size(n.property) - get property from database and compute size
          const args = item.expression.args;
          if (args && args.length > 0) {
            const arg = args[0];
            if (arg.type === "property" && arg.variable && arg.property) {
              const nodeId = resolvedIds[arg.variable];
              if (nodeId) {
                // Try nodes first
                const nodeResult = this.db.execute(
                  `SELECT json_extract(properties, '$.${arg.property}') as value FROM nodes WHERE id = ?`,
                  [nodeId]
                );
                if (nodeResult.rows.length > 0) {
                  const value = this.deepParseJson(nodeResult.rows[0].value);
                  if (Array.isArray(value)) {
                    resultRow[alias] = value.length;
                  } else if (typeof value === "string") {
                    resultRow[alias] = value.length;
                  } else {
                    resultRow[alias] = 0;
                  }
                } else {
                  // Try edges
                  const edgeResult = this.db.execute(
                    `SELECT json_extract(properties, '$.${arg.property}') as value FROM edges WHERE id = ?`,
                    [nodeId]
                  );
                  if (edgeResult.rows.length > 0) {
                    const value = this.deepParseJson(edgeResult.rows[0].value);
                    if (Array.isArray(value)) {
                      resultRow[alias] = value.length;
                    } else if (typeof value === "string") {
                      resultRow[alias] = value.length;
                    } else {
                      resultRow[alias] = 0;
                    }
                  }
                }
              }
            }
          }
        } else if (item.expression.type === "literal") {
          // Handle literal values like RETURN 42 AS num
          resultRow[alias] = item.expression.value;
        }
      }
      
      if (Object.keys(resultRow).length > 0) {
        results.push(resultRow);
      }
    }
    
    // Apply SKIP and LIMIT to the results
    let finalResults = results;
    
    if (returnClause.skip !== undefined) {
      const skipValue = this.evaluateSkipLimitExpression(returnClause.skip, params);
      if (skipValue > 0) {
        finalResults = finalResults.slice(skipValue);
      }
    }
    
    if (returnClause.limit !== undefined) {
      const limitValue = this.evaluateSkipLimitExpression(returnClause.limit, params);
      finalResults = finalResults.slice(0, limitValue);
    }
    
    return finalResults;
  }

  /**
   * Execute SET clause with pre-resolved node IDs
   */
  private executeSetWithResolvedIds(
    setClause: SetClause,
    resolvedIds: Record<string, string>,
    params: Record<string, unknown>
  ): void {
    for (const assignment of setClause.assignments) {
      const nodeId = resolvedIds[assignment.variable];
      if (!nodeId) {
        throw new Error(`Cannot resolve variable for SET: ${assignment.variable}`);
      }

      // Handle label assignments
      if (assignment.labels && assignment.labels.length > 0) {
        const newLabelsJson = JSON.stringify(assignment.labels);
        this.db.execute(
          `UPDATE nodes SET label = (SELECT json_group_array(value) FROM (
            SELECT DISTINCT value FROM (
              SELECT value FROM json_each(nodes.label)
              UNION ALL
              SELECT value FROM json_each(?)
            ) ORDER BY value
          )) WHERE id = ?`,
          [newLabelsJson, nodeId]
        );
        continue;
      }

      // Handle SET n = {props} - replace all properties
      if (assignment.replaceProps && assignment.value) {
        const newProps = this.evaluateObjectExpression(assignment.value, params);
        // Filter out null values (they should be removed)
        const filteredProps: Record<string, unknown> = {};
        for (const [key, val] of Object.entries(newProps)) {
          if (val !== null) {
            filteredProps[key] = val;
          }
        }
        // Try nodes first, then edges
        const nodeResult = this.db.execute(
          `UPDATE nodes SET properties = ? WHERE id = ?`,
          [JSON.stringify(filteredProps), nodeId]
        );
        if (nodeResult.changes === 0) {
          this.db.execute(
            `UPDATE edges SET properties = ? WHERE id = ?`,
            [JSON.stringify(filteredProps), nodeId]
          );
        }
        continue;
      }

      // Handle SET n += {props} - merge properties
      if (assignment.mergeProps && assignment.value) {
        const newProps = this.evaluateObjectExpression(assignment.value, params);
        
        const nullKeys = Object.entries(newProps)
          .filter(([_, val]) => val === null)
          .map(([key, _]) => key);
        const nonNullProps: Record<string, unknown> = {};
        for (const [key, val] of Object.entries(newProps)) {
          if (val !== null) {
            nonNullProps[key] = val;
          }
        }

        if (Object.keys(nonNullProps).length === 0 && nullKeys.length === 0) {
          // Empty map - no-op
          continue;
        }

        if (nullKeys.length > 0) {
          // Need to merge non-null props and remove null keys
          const removePaths = nullKeys.map(k => `'$.${k}'`).join(', ');
          const nodeResult = this.db.execute(
            `UPDATE nodes SET properties = json_remove(json_patch(properties, ?), ${removePaths}) WHERE id = ?`,
            [JSON.stringify(nonNullProps), nodeId]
          );
          if (nodeResult.changes === 0) {
            this.db.execute(
              `UPDATE edges SET properties = json_remove(json_patch(properties, ?), ${removePaths}) WHERE id = ?`,
              [JSON.stringify(nonNullProps), nodeId]
            );
          }
        } else {
          // Just merge
          const nodeResult = this.db.execute(
            `UPDATE nodes SET properties = json_patch(properties, ?) WHERE id = ?`,
            [JSON.stringify(nonNullProps), nodeId]
          );
          if (nodeResult.changes === 0) {
            this.db.execute(
              `UPDATE edges SET properties = json_patch(properties, ?) WHERE id = ?`,
              [JSON.stringify(nonNullProps), nodeId]
            );
          }
        }
        continue;
      }

      // Handle property assignments
      if (!assignment.value || !assignment.property) {
        throw new Error(`Invalid SET assignment for variable: ${assignment.variable}`);
      }

      // Use context-aware evaluation for expressions that may reference properties
      const value = assignment.value.type === "binary" || assignment.value.type === "property"
        ? this.evaluateExpressionWithContext(assignment.value, params, resolvedIds)
        : this.evaluateExpression(assignment.value, params);

      // If value is null, remove the property instead of setting it to null
      if (value === null) {
        const nodeResult = this.db.execute(
          `UPDATE nodes SET properties = json_remove(properties, '$.${assignment.property}') WHERE id = ?`,
          [nodeId]
        );
        if (nodeResult.changes === 0) {
          this.db.execute(
            `UPDATE edges SET properties = json_remove(properties, '$.${assignment.property}') WHERE id = ?`,
            [nodeId]
          );
        }
      } else {
        // Update the property using json_set
        // We need to determine if it's a node or edge - for now assume node
        // Try nodes first, then edges
        const nodeResult = this.db.execute(
          `UPDATE nodes SET properties = json_set(properties, '$.${assignment.property}', json(?)) WHERE id = ?`,
          [JSON.stringify(value), nodeId]
        );

        if (nodeResult.changes === 0) {
          // Try edges
          this.db.execute(
            `UPDATE edges SET properties = json_set(properties, '$.${assignment.property}', json(?)) WHERE id = ?`,
            [JSON.stringify(value), nodeId]
          );
        }
      }
    }
  }

  /**
   * Evaluate an object expression to get its key-value pairs
   */
  private evaluateObjectExpression(
    expr: Expression,
    params: Record<string, unknown>
  ): Record<string, unknown> {
    if (expr.type === "object" && expr.properties) {
      const result: Record<string, unknown> = {};
      for (const prop of expr.properties) {
        result[prop.key] = this.evaluateExpression(prop.value, params);
      }
      return result;
    }
    if (expr.type === "parameter") {
      const paramValue = params[expr.name!];
      if (typeof paramValue === "object" && paramValue !== null) {
        return paramValue as Record<string, unknown>;
      }
      throw new Error(`Parameter ${expr.name} is not an object`);
    }
    throw new Error(`Expected object expression, got ${expr.type}`);
  }

  /**
   * Execute DELETE clause with pre-resolved node/edge IDs
   * Returns the set of variables that were deleted
   */
  private executeDeleteWithResolvedIds(
    deleteClause: DeleteClause,
    resolvedIds: Record<string, string>
  ): Set<string> {
    const deletedVariables = new Set<string>();
    for (const variable of deleteClause.variables) {
      const id = resolvedIds[variable];
      if (!id) {
        throw new Error(`Cannot resolve variable for DELETE: ${variable}`);
      }

      if (deleteClause.detach) {
        // DETACH DELETE: First delete all edges connected to this node
        this.db.execute("DELETE FROM edges WHERE source_id = ? OR target_id = ?", [id, id]);
      } else {
        // Check if this is a node with connected edges
        const edgeCheck = this.db.execute(
          "SELECT 1 FROM edges WHERE source_id = ? OR target_id = ? LIMIT 1",
          [id, id]
        );
        if (edgeCheck.rows.length > 0) {
          throw new Error("Cannot delete node because it still has relationships. To delete this node, you must first delete its relationships, or use DETACH DELETE.");
        }
      }

      // Try deleting from nodes first
      const nodeResult = this.db.execute("DELETE FROM nodes WHERE id = ?", [id]);

      if (nodeResult.changes === 0) {
        // Try deleting from edges
        this.db.execute("DELETE FROM edges WHERE id = ?", [id]);
      }
      
      // Mark variable as deleted
      deletedVariables.add(variable);
    }
    
    return deletedVariables;
  }

  /**
   * Evaluate an expression to get its value
   * Note: For property and binary expressions that reference nodes, use evaluateExpressionWithContext
   */
  private evaluateExpression(expr: Expression, params: Record<string, unknown>): unknown {
    switch (expr.type) {
      case "literal":
        return expr.value;
      case "parameter":
        return params[expr.name!];
      case "function": {
        // Evaluate function calls (e.g., datetime(), timestamp())
        const funcName = expr.functionName!.toUpperCase();
        const args = expr.args || [];
        return this.evaluateFunctionInProperty(funcName, args, params, {});
      }
      default:
        throw new Error(`Cannot evaluate expression of type ${expr.type}`);
    }
  }

  /**
   * Evaluate an expression with access to node/edge context for property lookups
   */
  private evaluateExpressionWithContext(
    expr: Expression, 
    params: Record<string, unknown>,
    resolvedIds: Record<string, string>
  ): unknown {
    switch (expr.type) {
      case "literal":
        return expr.value;
      case "parameter":
        return params[expr.name!];
      case "property": {
        // Look up property from node/edge
        const varName = expr.variable!;
        const propName = expr.property!;
        const entityId = resolvedIds[varName];
        if (!entityId) {
          throw new Error(`Unknown variable: ${varName}`);
        }
        // Try nodes first
        const nodeResult = this.db.execute(
          `SELECT json_extract(properties, '$.${propName}') AS value FROM nodes WHERE id = ?`,
          [entityId]
        );
        if (nodeResult.rows.length > 0) {
          const value = nodeResult.rows[0].value;
          // json_extract returns JSON-encoded strings for arrays/objects
          // Parse if it looks like JSON
          if (typeof value === "string" && (value.startsWith("[") || value.startsWith("{"))) {
            try {
              return JSON.parse(value);
            } catch {
              return value;
            }
          }
          return value;
        }
        // Try edges
        const edgeResult = this.db.execute(
          `SELECT json_extract(properties, '$.${propName}') AS value FROM edges WHERE id = ?`,
          [entityId]
        );
        if (edgeResult.rows.length > 0) {
          const value = edgeResult.rows[0].value;
          if (typeof value === "string" && (value.startsWith("[") || value.startsWith("{"))) {
            try {
              return JSON.parse(value);
            } catch {
              return value;
            }
          }
          return value;
        }
        return null;
      }
      case "binary": {
        // Evaluate arithmetic expressions
        const left = this.evaluateExpressionWithContext(expr.left!, params, resolvedIds);
        const right = this.evaluateExpressionWithContext(expr.right!, params, resolvedIds);
        
        // Handle null values
        if (left === null || right === null) {
          return null;
        }
        
        switch (expr.operator) {
          case "+":
            // Handle list concatenation
            if (Array.isArray(left) && Array.isArray(right)) {
              return [...left, ...right];
            }
            if (Array.isArray(left)) {
              return [...left, right];
            }
            if (Array.isArray(right)) {
              return [left, ...right];
            }
            return (left as number) + (right as number);
          case "-":
            return (left as number) - (right as number);
          case "*":
            return (left as number) * (right as number);
          case "/":
            return (left as number) / (right as number);
          case "%":
            return (left as number) % (right as number);
          case "^":
            return Math.pow(left as number, right as number);
          default:
            throw new Error(`Unknown binary operator: ${expr.operator}`);
        }
      }
      case "function": {
        // Evaluate function calls (e.g., datetime(), timestamp())
        const funcName = expr.functionName!.toUpperCase();
        const args = expr.args || [];
        return this.evaluateFunctionInProperty(funcName, args, params, {});
      }
      default:
        throw new Error(`Cannot evaluate expression of type ${expr.type}`);
    }
  }

  /**
   * Execute a CREATE clause with pre-resolved node IDs for referenced variables
   * The resolvedIds map is mutated to include newly created node IDs
   */
  private executeCreateWithResolvedIds(
    createClause: CreateClause,
    resolvedIds: Record<string, string>,
    params: Record<string, unknown>
  ): void {
    for (const pattern of createClause.patterns) {
      if (this.isRelationshipPattern(pattern)) {
        this.createRelationshipWithResolvedIds(pattern, resolvedIds, params);
      } else {
        // Simple node creation - create directly and track the ID
        const nodePattern = pattern as NodePattern;
        const nodeId = crypto.randomUUID();
        const labelJson = this.normalizeLabelToJson(nodePattern.label);
        const props = this.resolveProperties(nodePattern.properties || {}, params);
        
        this.db.execute(
          "INSERT INTO nodes (id, label, properties) VALUES (?, ?, ?)",
          [nodeId, labelJson, JSON.stringify(props)]
        );
        
        // Track the created node ID so subsequent patterns can reference it
        if (nodePattern.variable) {
          resolvedIds[nodePattern.variable] = nodeId;
        }
      }
    }
  }

  /**
   * Create a relationship where some endpoints reference pre-existing nodes.
   * The resolvedIds map is mutated to include newly created node IDs.
   */
  private createRelationshipWithResolvedIds(
    rel: RelationshipPattern,
    resolvedIds: Record<string, string>,
    params: Record<string, unknown>
  ): void {
    let sourceId: string;
    let targetId: string;

    // Determine source node ID
    if (rel.source.variable && resolvedIds[rel.source.variable]) {
      sourceId = resolvedIds[rel.source.variable];
    } else if (rel.source.variable && !resolvedIds[rel.source.variable] && !rel.source.label) {
      // Variable referenced but not found and no label - error
      throw new Error(`Cannot resolve source node: ${rel.source.variable}`);
    } else {
      // Create new source node (with or without label - anonymous nodes are valid)
      sourceId = crypto.randomUUID();
      const props = this.resolveProperties(rel.source.properties || {}, params);
      const labelJson = this.normalizeLabelToJson(rel.source.label);
      this.db.execute(
        "INSERT INTO nodes (id, label, properties) VALUES (?, ?, ?)",
        [sourceId, labelJson, JSON.stringify(props)]
      );
      // Add to resolvedIds so subsequent patterns can reference it
      if (rel.source.variable) {
        resolvedIds[rel.source.variable] = sourceId;
      }
    }

    // Determine target node ID
    if (rel.target.variable && resolvedIds[rel.target.variable]) {
      targetId = resolvedIds[rel.target.variable];
    } else if (rel.target.variable && !resolvedIds[rel.target.variable] && !rel.target.label) {
      // Variable referenced but not found and no label - error
      throw new Error(`Cannot resolve target node: ${rel.target.variable}`);
    } else {
      // Create new target node (with or without label - anonymous nodes are valid)
      targetId = crypto.randomUUID();
      const props = this.resolveProperties(rel.target.properties || {}, params);
      const labelJson = this.normalizeLabelToJson(rel.target.label);
      this.db.execute(
        "INSERT INTO nodes (id, label, properties) VALUES (?, ?, ?)",
        [targetId, labelJson, JSON.stringify(props)]
      );
      // Add to resolvedIds so subsequent patterns can reference it
      if (rel.target.variable) {
        resolvedIds[rel.target.variable] = targetId;
      }
    }

    // Swap source/target for left-directed relationships
    const [actualSource, actualTarget] =
      rel.edge.direction === "left" ? [targetId, sourceId] : [sourceId, targetId];

    // Create edge
    const edgeId = crypto.randomUUID();
    const edgeType = rel.edge.type || "";
    const edgeProps = this.resolveProperties(rel.edge.properties || {}, params);

    this.db.execute(
      "INSERT INTO edges (id, type, source_id, target_id, properties) VALUES (?, ?, ?, ?, ?)",
      [edgeId, edgeType, actualSource, actualTarget, JSON.stringify(edgeProps)]
    );

    // Add edge to resolvedIds if it has a variable
    if (rel.edge.variable) {
      resolvedIds[rel.edge.variable] = edgeId;
    }
  }

  /**
   * Resolve parameter references and binary expressions in properties
   */
  private resolveProperties(
    props: Record<string, unknown>,
    params: Record<string, unknown>
  ): Record<string, unknown> {
    // Use the unwind version with empty context for non-unwind cases
    return this.resolvePropertiesWithUnwind(props, params, {});
  }

  /**
   * Type guard for relationship patterns
   */
  private isRelationshipPattern(pattern: NodePattern | RelationshipPattern): pattern is RelationshipPattern {
    return "source" in pattern && "edge" in pattern && "target" in pattern;
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
        // If returnColumns is specified, only include those columns
        if (!returnColumns || returnColumns.includes(key)) {
          formatted[key] = this.deepParseJson(value);
        }
      }

      return formatted;
    });
  }

  /**
   * Recursively parse JSON strings in a value
   * Also normalizes labels (single-element arrays become strings)
   */
  private deepParseJson(value: unknown, key?: string): unknown {
    if (typeof value === "string") {
      try {
        const parsed = JSON.parse(value);
        // Recursively process if it's an object or array
        if (typeof parsed === "object" && parsed !== null) {
          return this.deepParseJson(parsed, key);
        }
        return parsed;
      } catch {
        // Not valid JSON, return as-is
        return value;
      }
    }

    if (Array.isArray(value)) {
      // If this is a label field, normalize it (single element -> string)
      if (key === "label") {
        return value.length === 1 ? value[0] : value;
      }
      return value.map((item) => this.deepParseJson(item));
    }

    if (typeof value === "object" && value !== null) {
      const result: Record<string, unknown> = {};
      for (const [k, v] of Object.entries(value)) {
        result[k] = this.deepParseJson(v, k);
      }
      return result;
    }

    return value;
  }

  /**
   * Normalize label to JSON string for storage
   * Handles both single labels and multiple labels
   */
  private normalizeLabelToJson(label: string | string[] | undefined): string {
    if (!label) {
      return JSON.stringify([]);
    }
    const labelArray = Array.isArray(label) ? label : [label];
    return JSON.stringify(labelArray);
  }

  /**
   * Normalize label for output (from database JSON to user-friendly format)
   * Single label: return string, multiple labels: return array
   */
  private normalizeLabelForOutput(label: unknown): string | string[] {
    if (label === null || label === undefined) {
      return [];
    }
    
    // If it's already an array, normalize it
    if (Array.isArray(label)) {
      return label.length === 1 ? label[0] : label;
    }
    
    // If it's a JSON string, parse it
    if (typeof label === "string") {
      try {
        const parsed = JSON.parse(label);
        if (Array.isArray(parsed)) {
          return parsed.length === 1 ? parsed[0] : parsed;
        }
        return parsed;
      } catch {
        // Not valid JSON, return as-is
        return label;
      }
    }
    
    return String(label);
  }

  /**
   * Generate SQL condition for label matching
   * Supports both single and multiple labels
   */
  private generateLabelCondition(label: string | string[]): { sql: string; params: unknown[] } {
    const labels = Array.isArray(label) ? label : [label];
    
    if (labels.length === 1) {
      return {
        sql: `EXISTS (SELECT 1 FROM json_each(label) WHERE value = ?)`,
        params: [labels[0]]
      };
    } else {
      // Multiple labels: all must exist
      const conditions = labels.map(() => `EXISTS (SELECT 1 FROM json_each(label) WHERE value = ?)`);
      return {
        sql: conditions.join(" AND "),
        params: labels
      };
    }
  }

  /**
   * Extract id() function conditions from a WHERE clause
   * For conditions like: WHERE id(n) = $id
   * Populates the idConditions map with variable -> id value mappings
   */
  private extractIdConditions(
    where: WhereCondition,
    idConditions: Map<string, string>,
    params: Record<string, unknown>
  ): void {
    if (where.type === "and" && where.conditions) {
      // Recursively process AND conditions
      for (const condition of where.conditions) {
        this.extractIdConditions(condition, idConditions, params);
      }
    } else if (where.type === "comparison" && where.operator === "=") {
      // Check if left side is id() function call
      if (where.left?.type === "function" && where.left.functionName === "ID") {
        const arg = where.left.args?.[0];
        if (arg?.type === "variable" && arg.variable) {
          // Get the id value from the right side
          let idValue: string | undefined;
          if (where.right?.type === "parameter" && where.right.name) {
            idValue = params[where.right.name] as string;
          } else if (where.right?.type === "literal") {
            idValue = where.right.value as string;
          }
          if (idValue !== undefined) {
            idConditions.set(arg.variable, idValue);
          }
        }
      }
    }
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
