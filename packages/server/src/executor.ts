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
  SetAssignment,
  Expression,
  PropertyValue,
  ParameterRef,
  VariableRef,
  WhereCondition,
} from "./parser.js";
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

      // 2. Check for UNWIND with CREATE pattern (needs special handling)
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
    // Also collect values for WITH aggregates
    const withAggregateValues: Map<string, number[]> = new Map();
    
    this.db.transaction(() => {
      for (const combination of combinations) {
        // Build a map of unwind variable -> current value
        const unwindContext: Record<string, unknown> = {};
        for (let i = 0; i < unwindClauses.length; i++) {
          unwindContext[unwindClauses[i].alias] = combination[i];
        }
        
        // Execute CREATE with the unwind context
        const createdIds: Map<string, string> = new Map();
        for (const createClause of createClauses) {
          for (const pattern of createClause.patterns) {
            if (this.isRelationshipPattern(pattern)) {
              this.executeCreateRelationshipPatternWithUnwind(pattern, createdIds, params, unwindContext);
            } else {
              const id = crypto.randomUUID();
              const labelJson = this.normalizeLabelToJson(pattern.label);
              const props = this.resolvePropertiesWithUnwind(pattern.properties || {}, params, unwindContext);
              
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
                let value: number | undefined;
                
                if (aggInfo.argProperty) {
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
                      value = props[aggInfo.argProperty];
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
              case "sum":
                withAggregateResult[alias] = values.reduce((a, b) => a + b, 0);
                break;
              case "count":
                withAggregateResult[alias] = values.length;
                break;
              case "avg":
                withAggregateResult[alias] = values.length > 0 
                  ? values.reduce((a, b) => a + b, 0) / values.length 
                  : null;
                break;
              case "min":
                withAggregateResult[alias] = values.length > 0 ? Math.min(...values) : null;
                break;
              case "max":
                withAggregateResult[alias] = values.length > 0 ? Math.max(...values) : null;
                break;
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
        const skipValue = typeof returnClause.skip === "number" 
          ? returnClause.skip 
          : (params[returnClause.skip] as number) || 0;
        finalResults = finalResults.slice(skipValue);
      }
      
      if (returnClause.limit !== undefined && returnClause.limit !== null) {
        const limitValue = typeof returnClause.limit === "number"
          ? returnClause.limit
          : (params[returnClause.limit] as number) || 0;
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
   * Handle UNWIND + MERGE pattern
   * This requires special handling to resolve UNWIND variables in MERGE patterns
   */
  private tryUnwindMergeExecution(
    query: Query,
    params: Record<string, unknown>
  ): Record<string, unknown>[] | null {
    const clauses = query.clauses;
    
    // Find UNWIND and MERGE clauses
    const unwindClauses: UnwindClause[] = [];
    let mergeClause: MergeClause | null = null;
    let returnClause: ReturnClause | null = null;
    
    for (const clause of clauses) {
      if (clause.type === "UNWIND") {
        unwindClauses.push(clause);
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
    
    // Track created/merged node count
    let mergedCount = 0;
    
    this.db.transaction(() => {
      for (const combination of combinations) {
        // Build a map of unwind variable -> current value
        const unwindContext: Record<string, unknown> = {};
        for (let i = 0; i < unwindClauses.length; i++) {
          unwindContext[unwindClauses[i].alias] = combination[i];
        }
        
        // Execute MERGE for each pattern
        for (const pattern of mergeClause!.patterns) {
          if (!this.isRelationshipPattern(pattern)) {
            // Node pattern MERGE
            const nodePattern = pattern as NodePattern;
            const props = this.resolvePropertiesWithUnwind(nodePattern.properties || {}, params, unwindContext);
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
              ? `SELECT id FROM nodes WHERE ${whereConditions.join(" AND ")}`
              : "SELECT id FROM nodes LIMIT 1";
            
            const existsResult = this.db.execute(existsQuery, whereParams);
            
            if (existsResult.rows.length === 0) {
              // Node doesn't exist, create it
              const id = crypto.randomUUID();
              this.db.execute(
                "INSERT INTO nodes (id, label, properties) VALUES (?, ?, ?)",
                [id, labelJson, JSON.stringify(props)]
              );
            }
            
            mergedCount++;
          }
        }
      }
    });
    
    // Handle RETURN clause
    if (returnClause) {
      // For count(*) return the number of UNWIND iterations
      for (const item of returnClause.items) {
        if (item.expression.type === "function" && 
            item.expression.functionName?.toLowerCase() === "count" &&
            (!item.expression.args || item.expression.args.length === 0 ||
             (item.expression.args.length === 1 && 
              item.expression.args[0].type === "literal" &&
              item.expression.args[0].value === "*"))) {
          const alias = item.alias || "count(*)";
          return [{ [alias]: mergedCount }];
        }
      }
    }
    
    return [];
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
    
    switch (upperName) {
      case "DATETIME": {
        // datetime() returns current ISO datetime string
        // datetime(string) parses the string
        if (args.length > 0) {
          const arg = this.resolvePropertyValueWithUnwind(args[0], params, unwindContext);
          return String(arg);
        }
        return new Date().toISOString();
      }
      case "DATE": {
        // date() returns current date string (YYYY-MM-DD)
        // date(string) parses the string
        if (args.length > 0) {
          const arg = this.resolvePropertyValueWithUnwind(args[0], params, unwindContext);
          return String(arg).split("T")[0];
        }
        return new Date().toISOString().split("T")[0];
      }
      case "TIME": {
        // time() returns current time string (HH:MM:SS)
        if (args.length > 0) {
          const arg = this.resolvePropertyValueWithUnwind(args[0], params, unwindContext);
          const str = String(arg);
          const match = str.match(/(\d{2}:\d{2}:\d{2})/);
          return match ? match[1] : str;
        }
        return new Date().toISOString().split("T")[1].split(".")[0];
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
      }
    }
    
    if (Object.keys(resultRow).length > 0) {
      results.push(resultRow);
    }
    
    // Apply SKIP and LIMIT from returnClause (mutations already happened, now filter results)
    let finalResults = results;
    
    if (returnClause.skip !== undefined && returnClause.skip !== null) {
      const skipValue = typeof returnClause.skip === "number" 
        ? returnClause.skip 
        : (params[returnClause.skip] as number) || 0;
      finalResults = finalResults.slice(skipValue);
    }
    
    if (returnClause.limit !== undefined && returnClause.limit !== null) {
      const limitValue = typeof returnClause.limit === "number"
        ? returnClause.limit
        : (params[returnClause.limit] as number) || 0;
      finalResults = finalResults.slice(0, limitValue);
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
    let mergeClause: MergeClause | null = null;
    let returnClause: ReturnClause | null = null;
    
    for (const clause of clauses) {
      if (clause.type === "MERGE") {
        mergeClause = clause;
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
    
    if (!mergeClause) {
      return null;
    }
    
    // Check if this MERGE needs special handling:
    // 1. Has relationship patterns
    // 2. Has ON CREATE SET or ON MATCH SET
    // 3. Has RETURN clause (translator can't handle MERGE + RETURN properly for new nodes)
    const hasRelationshipPattern = mergeClause.patterns.some(p => this.isRelationshipPattern(p));
    const hasSetClauses = mergeClause.onCreateSet || mergeClause.onMatchSet;
    
    if (!hasRelationshipPattern && !hasSetClauses && !returnClause) {
      // Simple node MERGE without SET clauses and no RETURN - let translator handle it
      return null;
    }
    
    // Execute MERGE with special handling
    return this.executeMergeWithSetClauses(matchClauses, createClauses, withClauses, mergeClause, returnClause, params);
  }

  /**
   * Execute a MERGE clause with ON CREATE SET and/or ON MATCH SET
   */
  private executeMergeWithSetClauses(
    matchClauses: MatchClause[],
    createClauses: CreateClause[],
    withClauses: WithClause[],
    mergeClause: MergeClause,
    returnClause: ReturnClause | null,
    params: Record<string, unknown>
  ): Record<string, unknown>[] {
    // Track matched/created nodes 
    const matchedNodes = new Map<string, { id: string; label: string; properties: Record<string, unknown> }>();
    
    // Execute CREATE clauses first to create nodes
    for (const createClause of createClauses) {
      for (const pattern of createClause.patterns) {
        if (this.isRelationshipPattern(pattern)) {
          // Skip relationship patterns for now - just focus on node creation
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
          matchedNodes.set(nodePattern.variable, {
            id,
            label: labelStr,
            properties: props,
          });
        }
      }
    }
    
    // Execute MATCH clauses to get referenced nodes
    // First, extract id() conditions from WHERE clauses
    const idConditions = new Map<string, string>(); // variable -> id value
    for (const matchClause of matchClauses) {
      if (matchClause.where) {
        this.extractIdConditions(matchClause.where, idConditions, params);
      }
    }
    
    for (const matchClause of matchClauses) {
      for (const pattern of matchClause.patterns) {
        if (this.isRelationshipPattern(pattern)) {
          // For now, only handle simple node patterns in MATCH before MERGE
          throw new Error("Relationship patterns in MATCH before MERGE not yet supported");
        }
        
        const nodePattern = pattern as NodePattern;
        const matchProps = this.resolveProperties(nodePattern.properties || {}, params);
        
        // Build WHERE conditions
        const conditions: string[] = [];
        const conditionParams: unknown[] = [];
        
        // Check if we have an id() condition for this variable
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
        
        // Build the query - if no conditions, select all nodes (for cases like MATCH (a), (b))
        const findSql = conditions.length > 0
          ? `SELECT id, label, properties FROM nodes WHERE ${conditions.join(" AND ")}`
          : `SELECT id, label, properties FROM nodes LIMIT 1`;
        const findResult = this.db.execute(findSql, conditionParams);
        
        if (findResult.rows.length > 0 && nodePattern.variable) {
          const row = findResult.rows[0];
          const labelValue = typeof row.label === "string" ? JSON.parse(row.label) : row.label;
          matchedNodes.set(nodePattern.variable, {
            id: row.id as string,
            label: labelValue,
            properties: typeof row.properties === "string" ? JSON.parse(row.properties) : row.properties,
          });
        }
      }
    }
    
    // Process WITH clauses to handle aliasing
    // e.g., WITH n AS a, m AS b - creates aliases that map a->n, b->m in matched nodes
    for (const withClause of withClauses) {
      for (const item of withClause.items) {
        const alias = item.alias;
        const expr = item.expression;
        
        // Handle WITH n AS a - aliasing matched variable
        if (alias && expr.type === "variable" && expr.variable) {
          const sourceVar = expr.variable;
          const sourceNode = matchedNodes.get(sourceVar);
          if (sourceNode) {
            // Create alias pointing to the same node
            matchedNodes.set(alias, sourceNode);
          }
        }
      }
    }
    
    // Now handle the MERGE pattern
    const patterns = mergeClause.patterns;
    
    // Check if this is a relationship pattern or a simple node pattern
    if (patterns.length === 1 && !this.isRelationshipPattern(patterns[0])) {
      // Simple node MERGE
      return this.executeMergeNode(patterns[0] as NodePattern, mergeClause, returnClause, params, matchedNodes);
    } else if (patterns.length === 1 && this.isRelationshipPattern(patterns[0])) {
      // Relationship MERGE
      return this.executeMergeRelationship(patterns[0] as RelationshipPattern, mergeClause, returnClause, params, matchedNodes);
    } else {
      throw new Error("Complex MERGE patterns not yet supported");
    }
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
          const argNames = expr.args.map(arg => {
            if (arg.type === "variable") return arg.variable!;
            return "?";
          });
          return `${funcName}(${argNames.join(", ")})`;
        }
        // Empty args for count(*) or similar
        return `${funcName}(*)`;
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
   * Find variables in CREATE that reference MATCH variables
   */
  private findReferencedVariables(
    pattern: NodePattern | RelationshipPattern,
    matchedVars: Set<string>,
    referenced: Set<string>
  ): void {
    if (this.isRelationshipPattern(pattern)) {
      // Source node references a matched variable if it has no label
      if (pattern.source.variable && !pattern.source.label && matchedVars.has(pattern.source.variable)) {
        referenced.add(pattern.source.variable);
      }
      // Target node references a matched variable if it has no label
      if (pattern.target.variable && !pattern.target.label && matchedVars.has(pattern.target.variable)) {
        referenced.add(pattern.target.variable);
      }
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

        // Execute DELETE with resolved IDs
        for (const deleteClause of deleteClauses) {
          this.executeDeleteWithResolvedIds(deleteClause, resolvedIds);
        }
        
        // Save the resolved IDs for this row (including newly created nodes)
        allResolvedIds.push({ ...resolvedIds });
        // Save the captured property values
        allCapturedPropertyValues.push(capturedPropertyValues);
        // Save the captured edge types
        allCapturedEdgeTypes.push(capturedEdgeTypes);
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
        return this.buildReturnResults(returnClause, filteredResolvedIds, filteredPropertyValues, propertyAliasMap, withAggregateMap, filteredEdgeTypes);
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
    allCapturedEdgeTypes: Record<string, string>[] = []
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
    
    const skip = returnClause.skip ?? 0;
    const limit = returnClause.limit;
    
    if (skip > 0) {
      finalResults = finalResults.slice(skip);
    }
    
    if (limit !== undefined) {
      finalResults = finalResults.slice(0, limit);
    }
    
    return finalResults;
  }

  /**
   * Execute a MATCH...CREATE pattern in multiple phases (legacy, for backwards compatibility)
   */
  private executeMultiPhase(
    matchClauses: MatchClause[],
    createClauses: CreateClause[],
    referencedVars: Set<string>,
    params: Record<string, unknown>
  ): Record<string, unknown>[] {
    return this.executeMultiPhaseGeneral(
      matchClauses,
      [], // no WITH clauses
      createClauses,
      [],
      [],
      null,
      referencedVars,
      referencedVars,
      new Map(), // no alias map
      new Map(), // no property alias map
      new Map(), // no WITH aggregate map
      params
    );
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
   */
  private executeDeleteWithResolvedIds(
    deleteClause: DeleteClause,
    resolvedIds: Record<string, string>
  ): void {
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
    }
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
        formatted[key] = this.deepParseJson(value);
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
