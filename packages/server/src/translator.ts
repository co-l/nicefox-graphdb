// Cypher → SQL Translator

import {
  Query,
  Clause,
  CreateClause,
  MatchClause,
  MergeClause,
  SetClause,
  DeleteClause,
  ReturnClause,
  NodePattern,
  RelationshipPattern,
  EdgePattern,
  WhereCondition,
  Expression,
  PropertyValue,
  ParameterRef,
  SetAssignment,
  ReturnItem,
} from "./parser.js";

// ============================================================================
// Types
// ============================================================================

export interface SqlStatement {
  sql: string;
  params: unknown[];
}

export interface TranslationResult {
  statements: SqlStatement[];
  returnColumns?: string[];
}

export interface TranslationContext {
  // Maps Cypher variable names to SQL table aliases
  variables: Map<string, { type: "node" | "edge"; alias: string }>;
  // Parameter values provided by the user
  paramValues: Record<string, unknown>;
  // Counter for generating unique aliases
  aliasCounter: number;
}

// ============================================================================
// Translator
// ============================================================================

export class Translator {
  private ctx: TranslationContext;

  constructor(paramValues: Record<string, unknown> = {}) {
    this.ctx = {
      variables: new Map(),
      paramValues,
      aliasCounter: 0,
    };
  }

  translate(query: Query): TranslationResult {
    const statements: SqlStatement[] = [];
    let returnColumns: string[] | undefined;

    for (const clause of query.clauses) {
      const result = this.translateClause(clause);
      if (result.statements) {
        statements.push(...result.statements);
      }
      if (result.returnColumns) {
        returnColumns = result.returnColumns;
      }
    }

    return { statements, returnColumns };
  }

  private translateClause(clause: Clause): { statements?: SqlStatement[]; returnColumns?: string[] } {
    switch (clause.type) {
      case "CREATE":
        return { statements: this.translateCreate(clause) };
      case "MATCH":
        return { statements: this.translateMatch(clause, false) };
      case "OPTIONAL_MATCH":
        return { statements: this.translateMatch(clause, true) };
      case "MERGE":
        return { statements: this.translateMerge(clause) };
      case "SET":
        return { statements: this.translateSet(clause) };
      case "DELETE":
        return { statements: this.translateDelete(clause) };
      case "RETURN":
        return this.translateReturn(clause);
      default:
        throw new Error(`Unknown clause type: ${(clause as Clause).type}`);
    }
  }

  // ============================================================================
  // CREATE
  // ============================================================================

  private translateCreate(clause: CreateClause): SqlStatement[] {
    const statements: SqlStatement[] = [];

    for (const pattern of clause.patterns) {
      if (this.isRelationshipPattern(pattern)) {
        statements.push(...this.translateCreateRelationship(pattern));
      } else {
        statements.push(this.translateCreateNode(pattern));
      }
    }

    return statements;
  }

  private translateCreateNode(node: NodePattern): SqlStatement {
    const id = this.generateId();
    const label = node.label || "";
    const properties = this.serializeProperties(node.properties || {});

    if (node.variable) {
      this.ctx.variables.set(node.variable, { type: "node", alias: id });
    }

    return {
      sql: "INSERT INTO nodes (id, label, properties) VALUES (?, ?, ?)",
      params: [id, label, properties.json],
    };
  }

  private translateCreateRelationship(rel: RelationshipPattern): SqlStatement[] {
    const statements: SqlStatement[] = [];

    // Create source node if it has a label (new node)
    let sourceId: string;
    if (rel.source.label) {
      const sourceStmt = this.translateCreateNode(rel.source);
      statements.push(sourceStmt);
      sourceId = sourceStmt.params[0] as string;
    } else if (rel.source.variable) {
      const existing = this.ctx.variables.get(rel.source.variable);
      if (!existing) {
        throw new Error(`Unknown variable: ${rel.source.variable}`);
      }
      sourceId = existing.alias;
    } else {
      throw new Error("Source node must have a label or reference an existing variable");
    }

    // Create target node if it has a label (new node)
    let targetId: string;
    if (rel.target.label) {
      const targetStmt = this.translateCreateNode(rel.target);
      statements.push(targetStmt);
      targetId = targetStmt.params[0] as string;
    } else if (rel.target.variable) {
      const existing = this.ctx.variables.get(rel.target.variable);
      if (!existing) {
        throw new Error(`Unknown variable: ${rel.target.variable}`);
      }
      targetId = existing.alias;
    } else {
      throw new Error("Target node must have a label or reference an existing variable");
    }

    // Create edge
    const edgeId = this.generateId();
    const edgeType = rel.edge.type || "";
    const edgeProperties = this.serializeProperties(rel.edge.properties || {});

    if (rel.edge.variable) {
      this.ctx.variables.set(rel.edge.variable, { type: "edge", alias: edgeId });
    }

    // Swap source/target for left-directed relationships
    const [actualSource, actualTarget] =
      rel.edge.direction === "left" ? [targetId, sourceId] : [sourceId, targetId];

    statements.push({
      sql: "INSERT INTO edges (id, type, source_id, target_id, properties) VALUES (?, ?, ?, ?, ?)",
      params: [edgeId, edgeType, actualSource, actualTarget, edgeProperties.json],
    });

    return statements;
  }

  // ============================================================================
  // MATCH
  // ============================================================================

  private translateMatch(clause: MatchClause, optional: boolean = false): SqlStatement[] {
    // MATCH doesn't produce standalone statements - it sets up context for RETURN/SET/DELETE
    // The actual SELECT is generated when we encounter RETURN

    for (const pattern of clause.patterns) {
      if (this.isRelationshipPattern(pattern)) {
        this.registerRelationshipPattern(pattern, optional);
      } else {
        this.registerNodePattern(pattern, optional);
      }
    }

    // Store the where clause in context for later use
    // For OPTIONAL MATCH, we need to associate the where with the optional patterns
    if (clause.where) {
      if (optional) {
        // Store optional where clauses separately to apply them correctly
        if (!(this.ctx as any).optionalWhereClauses) {
          (this.ctx as any).optionalWhereClauses = [];
        }
        (this.ctx as any).optionalWhereClauses.push(clause.where);
      } else {
        (this.ctx as any).whereClause = clause.where;
      }
    }

    return [];
  }

  private registerNodePattern(node: NodePattern, optional: boolean = false): string {
    const alias = `n${this.ctx.aliasCounter++}`;
    if (node.variable) {
      this.ctx.variables.set(node.variable, { type: "node", alias });
    }
    // Store pattern info for later
    (this.ctx as any)[`pattern_${alias}`] = node;
    // Track if this node pattern is optional
    (this.ctx as any)[`optional_${alias}`] = optional;
    return alias;
  }

  private registerRelationshipPattern(rel: RelationshipPattern, optional: boolean = false): void {
    // Check if source node is already registered (for chained patterns or multi-MATCH)
    let sourceAlias: string;
    let sourceIsNew = false;
    if (rel.source.variable && this.ctx.variables.has(rel.source.variable)) {
      sourceAlias = this.ctx.variables.get(rel.source.variable)!.alias;
    } else {
      sourceAlias = this.registerNodePattern(rel.source, optional);
      sourceIsNew = true;
    }

    // Check if target node is already registered (for multi-MATCH shared variables)
    let targetAlias: string;
    let targetIsNew = false;
    if (rel.target.variable && this.ctx.variables.has(rel.target.variable)) {
      targetAlias = this.ctx.variables.get(rel.target.variable)!.alias;
    } else {
      targetAlias = this.registerNodePattern(rel.target, optional);
      targetIsNew = true;
    }

    const edgeAlias = `e${this.ctx.aliasCounter++}`;

    if (rel.edge.variable) {
      this.ctx.variables.set(rel.edge.variable, { type: "edge", alias: edgeAlias });
    }

    (this.ctx as any)[`pattern_${edgeAlias}`] = rel.edge;
    (this.ctx as any)[`optional_${edgeAlias}`] = optional;

    // Store relationship patterns as an array to support multi-hop
    if (!(this.ctx as any).relationshipPatterns) {
      (this.ctx as any).relationshipPatterns = [];
    }
    (this.ctx as any).relationshipPatterns.push({ 
      sourceAlias, 
      targetAlias, 
      edgeAlias, 
      edge: rel.edge, 
      optional,
      sourceIsNew,
      targetIsNew
    });

    // Keep backwards compatibility with single pattern
    (this.ctx as any).relationshipPattern = { sourceAlias, targetAlias, edgeAlias, edge: rel.edge, optional };
  }

  // ============================================================================
  // MERGE
  // ============================================================================

  private translateMerge(clause: MergeClause): SqlStatement[] {
    // MERGE: Create if not exists, match if exists
    // This requires multiple statements or an UPSERT pattern

    const node = clause.pattern;
    const label = node.label || "";
    const props = node.properties || {};
    const serialized = this.serializeProperties(props);

    // Build condition to find existing node
    const conditions: string[] = ["label = ?"];
    const params: unknown[] = [label];

    for (const [key, value] of Object.entries(props)) {
      if (this.isParameterRef(value)) {
        conditions.push(`json_extract(properties, '$.${key}') = ?`);
        params.push(this.ctx.paramValues[value.name]);
      } else {
        conditions.push(`json_extract(properties, '$.${key}') = ?`);
        params.push(value);
      }
    }

    const id = this.generateId();
    if (node.variable) {
      this.ctx.variables.set(node.variable, { type: "node", alias: id });
    }

    // SQLite INSERT OR IGNORE + SELECT approach
    // First, try to insert
    const insertSql = `INSERT OR IGNORE INTO nodes (id, label, properties) 
      SELECT ?, ?, ? 
      WHERE NOT EXISTS (SELECT 1 FROM nodes WHERE ${conditions.join(" AND ")})`;

    return [
      {
        sql: insertSql,
        params: [id, label, serialized.json, ...params],
      },
    ];
  }

  // ============================================================================
  // SET
  // ============================================================================

  private translateSet(clause: SetClause): SqlStatement[] {
    const statements: SqlStatement[] = [];

    for (const assignment of clause.assignments) {
      const varInfo = this.ctx.variables.get(assignment.variable);
      if (!varInfo) {
        throw new Error(`Unknown variable: ${assignment.variable}`);
      }

      const table = varInfo.type === "node" ? "nodes" : "edges";
      const value = this.evaluateExpression(assignment.value);

      // Use json_set to update the property
      statements.push({
        sql: `UPDATE ${table} SET properties = json_set(properties, '$.${assignment.property}', json(?)) WHERE id = ?`,
        params: [JSON.stringify(value), varInfo.alias],
      });
    }

    return statements;
  }

  // ============================================================================
  // DELETE
  // ============================================================================

  private translateDelete(clause: DeleteClause): SqlStatement[] {
    const statements: SqlStatement[] = [];

    for (const variable of clause.variables) {
      const varInfo = this.ctx.variables.get(variable);
      if (!varInfo) {
        throw new Error(`Unknown variable: ${variable}`);
      }

      const table = varInfo.type === "node" ? "nodes" : "edges";

      if (clause.detach && varInfo.type === "node") {
        // DETACH DELETE: First delete all edges connected to this node
        statements.push({
          sql: "DELETE FROM edges WHERE source_id = ? OR target_id = ?",
          params: [varInfo.alias, varInfo.alias],
        });
      }

      statements.push({
        sql: `DELETE FROM ${table} WHERE id = ?`,
        params: [varInfo.alias],
      });
    }

    return statements;
  }

  // ============================================================================
  // RETURN
  // ============================================================================

  private translateReturn(clause: ReturnClause): { statements: SqlStatement[]; returnColumns: string[] } {
    const selectParts: string[] = [];
    const returnColumns: string[] = [];
    const fromParts: string[] = [];
    const joinParts: string[] = [];
    const joinParams: unknown[] = []; // Parameters for JOIN ON clauses
    const whereParts: string[] = [];
    const whereParams: unknown[] = []; // Parameters for WHERE clause

    // Track which tables we need
    const neededTables = new Set<string>();

    // Process return items
    const exprParams: unknown[] = [];
    for (const item of clause.items) {
      const { sql: exprSql, tables, params: itemParams } = this.translateExpression(item.expression);
      tables.forEach((t) => neededTables.add(t));
      exprParams.push(...itemParams);

      const alias = item.alias || this.getExpressionName(item.expression);
      selectParts.push(`${exprSql} AS ${alias}`);
      returnColumns.push(alias);
    }

    // Build FROM clause based on registered patterns
    const relPatterns = (this.ctx as any).relationshipPatterns as Array<{
      sourceAlias: string;
      targetAlias: string;
      edgeAlias: string;
      edge: { type?: string; properties?: Record<string, PropertyValue> };
      optional?: boolean;
      sourceIsNew?: boolean;
      targetIsNew?: boolean;
    }> | undefined;

    if (relPatterns && relPatterns.length > 0) {
      // Track which node aliases we've already added to FROM/JOIN
      const addedNodeAliases = new Set<string>();
      // Track which node aliases have had their filters added (to avoid duplicates)
      const filteredNodeAliases = new Set<string>();

      // Relationship query - handle multi-hop patterns
      for (let i = 0; i < relPatterns.length; i++) {
        const relPattern = relPatterns[i];
        const isOptional = relPattern.optional === true;
        const joinType = isOptional ? "LEFT JOIN" : "JOIN";

        if (i === 0 && !isOptional) {
          // First non-optional relationship: add source node to FROM
          fromParts.push(`nodes ${relPattern.sourceAlias}`);
          addedNodeAliases.add(relPattern.sourceAlias);
        } else if (!addedNodeAliases.has(relPattern.sourceAlias)) {
          // For subsequent patterns, if source is not already added, we need to JOIN it
          // For optional patterns, use LEFT JOIN
          if (isOptional && relPattern.sourceIsNew) {
            // This shouldn't happen often - optional patterns usually reference existing nodes
            joinParts.push(`${joinType} nodes ${relPattern.sourceAlias} ON 1=1`);
          } else if (i === 0) {
            // First pattern is optional but source is new - add to FROM
            fromParts.push(`nodes ${relPattern.sourceAlias}`);
          } else {
            joinParts.push(`JOIN nodes ${relPattern.sourceAlias} ON 1=1`);
          }
          addedNodeAliases.add(relPattern.sourceAlias);
        }

        // Build ON conditions for the edge join
        let edgeOnConditions: string[] = [];
        let edgeOnParams: unknown[] = [];
        
        // Add edge join - need to determine direction based on whether source/target already exist
        if (addedNodeAliases.has(relPattern.targetAlias) && !addedNodeAliases.has(relPattern.sourceAlias)) {
          edgeOnConditions.push(`${relPattern.edgeAlias}.target_id = ${relPattern.targetAlias}.id`);
        } else {
          edgeOnConditions.push(`${relPattern.edgeAlias}.source_id = ${relPattern.sourceAlias}.id`);
        }

        // For optional patterns, add type filter to ON clause instead of WHERE
        if (relPattern.edge.type) {
          if (isOptional) {
            edgeOnConditions.push(`${relPattern.edgeAlias}.type = ?`);
            edgeOnParams.push(relPattern.edge.type);
          } else {
            whereParts.push(`${relPattern.edgeAlias}.type = ?`);
            whereParams.push(relPattern.edge.type);
          }
        }

        joinParts.push(`${joinType} edges ${relPattern.edgeAlias} ON ${edgeOnConditions.join(" AND ")}`);
        joinParams.push(...edgeOnParams);

        // Build ON conditions for the target node join
        let targetOnConditions: string[] = [];
        let targetOnParams: unknown[] = [];

        // Add target node join if not already added
        if (!addedNodeAliases.has(relPattern.targetAlias)) {
          targetOnConditions.push(`${relPattern.edgeAlias}.target_id = ${relPattern.targetAlias}.id`);
          
          // For optional patterns, add label and property filters to ON clause
          const targetPattern = (this.ctx as any)[`pattern_${relPattern.targetAlias}`];
          if (isOptional && targetPattern?.label) {
            targetOnConditions.push(`${relPattern.targetAlias}.label = ?`);
            targetOnParams.push(targetPattern.label);
            filteredNodeAliases.add(relPattern.targetAlias);
          }
          if (isOptional && targetPattern?.properties) {
            for (const [key, value] of Object.entries(targetPattern.properties)) {
              if (this.isParameterRef(value as PropertyValue)) {
                targetOnConditions.push(`json_extract(${relPattern.targetAlias}.properties, '$.${key}') = ?`);
                targetOnParams.push(this.ctx.paramValues[(value as ParameterRef).name]);
              } else {
                targetOnConditions.push(`json_extract(${relPattern.targetAlias}.properties, '$.${key}') = ?`);
                targetOnParams.push(value);
              }
            }
          }

          joinParts.push(`${joinType} nodes ${relPattern.targetAlias} ON ${targetOnConditions.join(" AND ")}`);
          joinParams.push(...targetOnParams);
          addedNodeAliases.add(relPattern.targetAlias);
        } else {
          // Target was already added, but we need to ensure edge connects to it
          // Add WHERE condition to connect edge's target to the existing node
          if (isOptional) {
            // For optional, we need to handle this in ON clause of edge
            // This is already handled above by adding to edgeOnConditions
            whereParts.push(`(${relPattern.edgeAlias}.id IS NULL OR ${relPattern.edgeAlias}.target_id = ${relPattern.targetAlias}.id)`);
          } else {
            whereParts.push(`${relPattern.edgeAlias}.target_id = ${relPattern.targetAlias}.id`);
          }
        }

        // Add source node filters (label and properties) if not already done and not optional
        if (!filteredNodeAliases.has(relPattern.sourceAlias)) {
          const sourcePattern = (this.ctx as any)[`pattern_${relPattern.sourceAlias}`];
          const sourceIsOptional = (this.ctx as any)[`optional_${relPattern.sourceAlias}`] === true;
          
          if (sourcePattern?.label) {
            if (sourceIsOptional) {
              // For optional source nodes, this shouldn't happen often
              // as optional patterns usually reference required nodes
            } else {
              whereParts.push(`${relPattern.sourceAlias}.label = ?`);
              whereParams.push(sourcePattern.label);
            }
          }
          if (sourcePattern?.properties && !sourceIsOptional) {
            for (const [key, value] of Object.entries(sourcePattern.properties)) {
              if (this.isParameterRef(value as PropertyValue)) {
                whereParts.push(`json_extract(${relPattern.sourceAlias}.properties, '$.${key}') = ?`);
                whereParams.push(this.ctx.paramValues[(value as ParameterRef).name]);
              } else {
                whereParts.push(`json_extract(${relPattern.sourceAlias}.properties, '$.${key}') = ?`);
                whereParams.push(value);
              }
            }
          }
          filteredNodeAliases.add(relPattern.sourceAlias);
        }

        // Add target node filters (label and properties) if not already done and not optional
        if (!filteredNodeAliases.has(relPattern.targetAlias)) {
          const targetPattern = (this.ctx as any)[`pattern_${relPattern.targetAlias}`];
          if (!isOptional) {
            if (targetPattern?.label) {
              whereParts.push(`${relPattern.targetAlias}.label = ?`);
              whereParams.push(targetPattern.label);
            }
            if (targetPattern?.properties) {
              for (const [key, value] of Object.entries(targetPattern.properties)) {
                if (this.isParameterRef(value as PropertyValue)) {
                  whereParts.push(`json_extract(${relPattern.targetAlias}.properties, '$.${key}') = ?`);
                  whereParams.push(this.ctx.paramValues[(value as ParameterRef).name]);
                } else {
                  whereParts.push(`json_extract(${relPattern.targetAlias}.properties, '$.${key}') = ?`);
                  whereParams.push(value);
                }
              }
            }
          }
          filteredNodeAliases.add(relPattern.targetAlias);
        }
      }
    } else {
      // Simple node query (no relationships)
      let hasFromClause = false;
      
      for (const [variable, info] of this.ctx.variables) {
        const pattern = (this.ctx as any)[`pattern_${info.alias}`];
        const isOptional = (this.ctx as any)[`optional_${info.alias}`] === true;
        
        if (pattern && info.type === "node") {
          if (!hasFromClause && !isOptional) {
            // First non-optional node goes in FROM
            fromParts.push(`nodes ${info.alias}`);
            hasFromClause = true;

            if (pattern.label) {
              whereParts.push(`${info.alias}.label = ?`);
              whereParams.push(pattern.label);
            }

            if (pattern.properties) {
              for (const [key, value] of Object.entries(pattern.properties)) {
                if (this.isParameterRef(value as PropertyValue)) {
                  whereParts.push(`json_extract(${info.alias}.properties, '$.${key}') = ?`);
                  whereParams.push(this.ctx.paramValues[(value as ParameterRef).name]);
                } else {
                  whereParts.push(`json_extract(${info.alias}.properties, '$.${key}') = ?`);
                  whereParams.push(value);
                }
              }
            }
          } else if (isOptional) {
            // Optional node - use LEFT JOIN
            const onConditions: string[] = ["1=1"];
            const onParams: unknown[] = [];
            
            if (pattern.label) {
              onConditions.push(`${info.alias}.label = ?`);
              onParams.push(pattern.label);
            }
            
            if (pattern.properties) {
              for (const [key, value] of Object.entries(pattern.properties)) {
                if (this.isParameterRef(value as PropertyValue)) {
                  onConditions.push(`json_extract(${info.alias}.properties, '$.${key}') = ?`);
                  onParams.push(this.ctx.paramValues[(value as ParameterRef).name]);
                } else {
                  onConditions.push(`json_extract(${info.alias}.properties, '$.${key}') = ?`);
                  onParams.push(value);
                }
              }
            }
            
            joinParts.push(`LEFT JOIN nodes ${info.alias} ON ${onConditions.join(" AND ")}`);
            joinParams.push(...onParams);
          } else {
            // Non-optional node that's not the first - use regular JOIN
            fromParts.push(`nodes ${info.alias}`);

            if (pattern.label) {
              whereParts.push(`${info.alias}.label = ?`);
              whereParams.push(pattern.label);
            }

            if (pattern.properties) {
              for (const [key, value] of Object.entries(pattern.properties)) {
                if (this.isParameterRef(value as PropertyValue)) {
                  whereParts.push(`json_extract(${info.alias}.properties, '$.${key}') = ?`);
                  whereParams.push(this.ctx.paramValues[(value as ParameterRef).name]);
                } else {
                  whereParts.push(`json_extract(${info.alias}.properties, '$.${key}') = ?`);
                  whereParams.push(value);
                }
              }
            }
          }
        }
      }
    }

    // Add WHERE conditions from MATCH
    const matchWhereClause = (this.ctx as any).whereClause;
    if (matchWhereClause) {
      const { sql: whereSql, params: conditionParams } = this.translateWhere(matchWhereClause);
      whereParts.push(whereSql);
      whereParams.push(...conditionParams);
    }

    // Add WHERE conditions from OPTIONAL MATCH
    // These should be applied as: (optional_var IS NULL OR condition)
    // This ensures the main row is still returned even if the optional match fails the WHERE
    const optionalWhereClauses = (this.ctx as any).optionalWhereClauses as WhereCondition[] | undefined;
    if (optionalWhereClauses && optionalWhereClauses.length > 0) {
      for (const optionalWhere of optionalWhereClauses) {
        const { sql: whereSql, params: conditionParams } = this.translateWhere(optionalWhere);
        // Find the main variable in the condition to check for NULL
        const optionalVars = this.findVariablesInCondition(optionalWhere);
        if (optionalVars.length > 0) {
          // Get the first optional variable's alias to check for NULL
          const firstVar = optionalVars[0];
          const varInfo = this.ctx.variables.get(firstVar);
          if (varInfo) {
            whereParts.push(`(${varInfo.alias}.id IS NULL OR ${whereSql})`);
            whereParams.push(...conditionParams);
          }
        } else {
          // No variables found, just add the condition
          whereParts.push(whereSql);
          whereParams.push(...conditionParams);
        }
      }
    }

    // Build final SQL
    const distinctKeyword = clause.distinct ? "DISTINCT " : "";
    let sql = `SELECT ${distinctKeyword}${selectParts.join(", ")}`;

    if (fromParts.length > 0) {
      sql += ` FROM ${fromParts.join(", ")}`;
    }

    if (joinParts.length > 0) {
      sql += ` ${joinParts.join(" ")}`;
    }

    if (whereParts.length > 0) {
      sql += ` WHERE ${whereParts.join(" AND ")}`;
    }

    // Add ORDER BY clause
    if (clause.orderBy && clause.orderBy.length > 0) {
      const orderParts = clause.orderBy.map(({ expression, direction }) => {
        const { sql: exprSql } = this.translateOrderByExpression(expression);
        return `${exprSql} ${direction}`;
      });
      sql += ` ORDER BY ${orderParts.join(", ")}`;
    }

    // Add LIMIT and OFFSET (SKIP)
    // SQLite requires LIMIT before OFFSET
    if (clause.limit !== undefined || clause.skip !== undefined) {
      if (clause.limit !== undefined) {
        sql += ` LIMIT ?`;
        whereParams.push(clause.limit);
      } else if (clause.skip !== undefined) {
        // SKIP without LIMIT - need a large limit for SQLite
        sql += ` LIMIT -1`;
      }

      if (clause.skip !== undefined) {
        sql += ` OFFSET ?`;
        whereParams.push(clause.skip);
      }
    }

    // Combine params in the order they appear in SQL: SELECT -> JOINs -> WHERE
    const allParams = [...exprParams, ...joinParams, ...whereParams];

    return {
      statements: [{ sql, params: allParams }],
      returnColumns,
    };
  }

  private translateExpression(expr: Expression): { sql: string; tables: string[]; params: unknown[] } {
    const tables: string[] = [];
    const params: unknown[] = [];

    switch (expr.type) {
      case "variable": {
        const varInfo = this.ctx.variables.get(expr.variable!);
        if (!varInfo) {
          throw new Error(`Unknown variable: ${expr.variable}`);
        }
        tables.push(varInfo.alias);
        // Return the whole row as JSON for variables
        return {
          sql: `json_object('id', ${varInfo.alias}.id, 'label', ${varInfo.alias}.label, 'properties', ${varInfo.alias}.properties)`,
          tables,
          params,
        };
      }

      case "property": {
        const varInfo = this.ctx.variables.get(expr.variable!);
        if (!varInfo) {
          throw new Error(`Unknown variable: ${expr.variable}`);
        }
        tables.push(varInfo.alias);
        // Use -> operator to preserve JSON types (returns 'true'/'false' not 1/0)
        return {
          sql: `${varInfo.alias}.properties -> '$.${expr.property}'`,
          tables,
          params,
        };
      }

      case "function": {
        if (expr.functionName === "COUNT") {
          if (expr.args && expr.args.length > 0) {
            const argExpr = this.translateExpression(expr.args[0]);
            tables.push(...argExpr.tables);
            params.push(...argExpr.params);
            return { sql: `COUNT(*)`, tables, params };
          }
          return { sql: "COUNT(*)", tables, params };
        }
        if (expr.functionName === "ID") {
          if (expr.args && expr.args.length > 0 && expr.args[0].type === "variable") {
            const varInfo = this.ctx.variables.get(expr.args[0].variable!);
            if (!varInfo) {
              throw new Error(`Unknown variable: ${expr.args[0].variable}`);
            }
            tables.push(varInfo.alias);
            return { sql: `${varInfo.alias}.id`, tables, params };
          }
        }
        throw new Error(`Unknown function: ${expr.functionName}`);
      }

      case "literal": {
        // Convert booleans to 1/0 for SQLite
        const value = expr.value === true ? 1 : expr.value === false ? 0 : expr.value;
        params.push(value);
        return { sql: "?", tables, params };
      }

      case "parameter": {
        params.push(this.ctx.paramValues[expr.name!]);
        return { sql: "?", tables, params };
      }

      default:
        throw new Error(`Unknown expression type: ${expr.type}`);
    }
  }

  private translateWhere(condition: WhereCondition): { sql: string; params: unknown[] } {
    switch (condition.type) {
      case "comparison": {
        const left = this.translateWhereExpression(condition.left!);
        const right = this.translateWhereExpression(condition.right!);
        return {
          sql: `${left.sql} ${condition.operator} ${right.sql}`,
          params: [...left.params, ...right.params],
        };
      }

      case "and": {
        const parts = condition.conditions!.map((c) => this.translateWhere(c));
        return {
          sql: `(${parts.map((p) => p.sql).join(" AND ")})`,
          params: parts.flatMap((p) => p.params),
        };
      }

      case "or": {
        const parts = condition.conditions!.map((c) => this.translateWhere(c));
        return {
          sql: `(${parts.map((p) => p.sql).join(" OR ")})`,
          params: parts.flatMap((p) => p.params),
        };
      }

      case "not": {
        const inner = this.translateWhere(condition.condition!);
        return {
          sql: `NOT (${inner.sql})`,
          params: inner.params,
        };
      }

      case "contains": {
        const left = this.translateWhereExpression(condition.left!);
        const right = this.translateWhereExpression(condition.right!);
        return {
          sql: `${left.sql} LIKE '%' || ${right.sql} || '%'`,
          params: [...left.params, ...right.params],
        };
      }

      case "startsWith": {
        const left = this.translateWhereExpression(condition.left!);
        const right = this.translateWhereExpression(condition.right!);
        return {
          sql: `${left.sql} LIKE ${right.sql} || '%'`,
          params: [...left.params, ...right.params],
        };
      }

      case "endsWith": {
        const left = this.translateWhereExpression(condition.left!);
        const right = this.translateWhereExpression(condition.right!);
        return {
          sql: `${left.sql} LIKE '%' || ${right.sql}`,
          params: [...left.params, ...right.params],
        };
      }

      case "isNull": {
        const left = this.translateWhereExpression(condition.left!);
        return {
          sql: `${left.sql} IS NULL`,
          params: left.params,
        };
      }

      case "isNotNull": {
        const left = this.translateWhereExpression(condition.left!);
        return {
          sql: `${left.sql} IS NOT NULL`,
          params: left.params,
        };
      }

      default:
        throw new Error(`Unknown condition type: ${condition.type}`);
    }
  }

  private translateOrderByExpression(expr: Expression): { sql: string } {
    switch (expr.type) {
      case "property": {
        const varInfo = this.ctx.variables.get(expr.variable!);
        if (!varInfo) {
          throw new Error(`Unknown variable: ${expr.variable}`);
        }
        return {
          sql: `json_extract(${varInfo.alias}.properties, '$.${expr.property}')`,
        };
      }

      case "variable": {
        const varInfo = this.ctx.variables.get(expr.variable!);
        if (!varInfo) {
          throw new Error(`Unknown variable: ${expr.variable}`);
        }
        return { sql: `${varInfo.alias}.id` };
      }

      case "function": {
        if (expr.functionName === "ID") {
          if (expr.args && expr.args.length > 0 && expr.args[0].type === "variable") {
            const varInfo = this.ctx.variables.get(expr.args[0].variable!);
            if (!varInfo) {
              throw new Error(`Unknown variable: ${expr.args[0].variable}`);
            }
            return { sql: `${varInfo.alias}.id` };
          }
        }
        throw new Error(`Cannot order by function: ${expr.functionName}`);
      }

      default:
        throw new Error(`Cannot order by expression of type ${expr.type}`);
    }
  }

  private translateWhereExpression(expr: Expression): { sql: string; params: unknown[] } {
    switch (expr.type) {
      case "property": {
        const varInfo = this.ctx.variables.get(expr.variable!);
        if (!varInfo) {
          throw new Error(`Unknown variable: ${expr.variable}`);
        }
        return {
          sql: `json_extract(${varInfo.alias}.properties, '$.${expr.property}')`,
          params: [],
        };
      }

      case "literal": {
        // Convert booleans to 1/0 for SQLite
        const value = expr.value === true ? 1 : expr.value === false ? 0 : expr.value;
        return { sql: "?", params: [value] };
      }

      case "parameter": {
        let value = this.ctx.paramValues[expr.name!];
        // Convert booleans to 1/0 for SQLite
        if (value === true) value = 1;
        else if (value === false) value = 0;
        return { sql: "?", params: [value] };
      }

      case "variable": {
        const varInfo = this.ctx.variables.get(expr.variable!);
        if (!varInfo) {
          throw new Error(`Unknown variable: ${expr.variable}`);
        }
        return { sql: `${varInfo.alias}.id`, params: [] };
      }

      default:
        throw new Error(`Unknown expression type in WHERE: ${expr.type}`);
    }
  }

  // ============================================================================
  // Helpers
  // ============================================================================

  private isRelationshipPattern(pattern: NodePattern | RelationshipPattern): pattern is RelationshipPattern {
    return "source" in pattern && "edge" in pattern && "target" in pattern;
  }

  private findVariablesInCondition(condition: WhereCondition): string[] {
    const vars: string[] = [];
    
    const collectFromExpression = (expr: Expression | undefined) => {
      if (!expr) return;
      if (expr.type === "property" && expr.variable) {
        vars.push(expr.variable);
      } else if (expr.type === "variable" && expr.variable) {
        vars.push(expr.variable);
      }
    };
    
    const collectFromCondition = (cond: WhereCondition) => {
      collectFromExpression(cond.left);
      collectFromExpression(cond.right);
      if (cond.conditions) {
        for (const c of cond.conditions) {
          collectFromCondition(c);
        }
      }
      if (cond.condition) {
        collectFromCondition(cond.condition);
      }
    };
    
    collectFromCondition(condition);
    return [...new Set(vars)];
  }

  private isParameterRef(value: PropertyValue): value is ParameterRef {
    return typeof value === "object" && value !== null && "type" in value && value.type === "parameter";
  }

  private serializeProperties(props: Record<string, PropertyValue>): { json: string; params: unknown[] } {
    const resolved: Record<string, unknown> = {};
    const params: unknown[] = [];

    for (const [key, value] of Object.entries(props)) {
      if (this.isParameterRef(value)) {
        resolved[key] = this.ctx.paramValues[value.name];
      } else {
        resolved[key] = value;
      }
    }

    return { json: JSON.stringify(resolved), params };
  }

  private evaluateExpression(expr: Expression): unknown {
    switch (expr.type) {
      case "literal":
        return expr.value;
      case "parameter":
        return this.ctx.paramValues[expr.name!];
      default:
        throw new Error(`Cannot evaluate expression of type ${expr.type}`);
    }
  }

  private getExpressionName(expr: Expression): string {
    switch (expr.type) {
      case "variable":
        return expr.variable!;
      case "property":
        return `${expr.variable}_${expr.property}`;
      case "function":
        return expr.functionName!.toLowerCase();
      default:
        return "expr";
    }
  }

  private generateId(): string {
    return crypto.randomUUID();
  }
}

// Convenience function
export function translate(query: Query, params: Record<string, unknown> = {}): TranslationResult {
  return new Translator(params).translate(query);
}
