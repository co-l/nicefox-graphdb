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
        return { statements: this.translateMatch(clause) };
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

  private translateMatch(clause: MatchClause): SqlStatement[] {
    // MATCH doesn't produce standalone statements - it sets up context for RETURN/SET/DELETE
    // The actual SELECT is generated when we encounter RETURN

    for (const pattern of clause.patterns) {
      if (this.isRelationshipPattern(pattern)) {
        this.registerRelationshipPattern(pattern);
      } else {
        this.registerNodePattern(pattern);
      }
    }

    // Store the where clause in context for later use
    if (clause.where) {
      (this.ctx as any).whereClause = clause.where;
    }

    return [];
  }

  private registerNodePattern(node: NodePattern): string {
    const alias = `n${this.ctx.aliasCounter++}`;
    if (node.variable) {
      this.ctx.variables.set(node.variable, { type: "node", alias });
    }
    // Store pattern info for later
    (this.ctx as any)[`pattern_${alias}`] = node;
    return alias;
  }

  private registerRelationshipPattern(rel: RelationshipPattern): void {
    const sourceAlias = this.registerNodePattern(rel.source);
    const targetAlias = this.registerNodePattern(rel.target);
    const edgeAlias = `e${this.ctx.aliasCounter++}`;

    if (rel.edge.variable) {
      this.ctx.variables.set(rel.edge.variable, { type: "edge", alias: edgeAlias });
    }

    (this.ctx as any)[`pattern_${edgeAlias}`] = rel.edge;
    (this.ctx as any).relationshipPattern = { sourceAlias, targetAlias, edgeAlias, edge: rel.edge };
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
    const whereParts: string[] = [];
    const params: unknown[] = [];

    // Track which tables we need
    const neededTables = new Set<string>();

    // Process return items
    for (const item of clause.items) {
      const { sql: exprSql, tables } = this.translateExpression(item.expression);
      tables.forEach((t) => neededTables.add(t));

      const alias = item.alias || this.getExpressionName(item.expression);
      selectParts.push(`${exprSql} AS ${alias}`);
      returnColumns.push(alias);
    }

    // Build FROM clause based on registered patterns
    const relPattern = (this.ctx as any).relationshipPattern;
    if (relPattern) {
      // Relationship query
      fromParts.push(`nodes ${relPattern.sourceAlias}`);
      joinParts.push(`JOIN edges ${relPattern.edgeAlias} ON ${relPattern.edgeAlias}.source_id = ${relPattern.sourceAlias}.id`);
      joinParts.push(`JOIN nodes ${relPattern.targetAlias} ON ${relPattern.edgeAlias}.target_id = ${relPattern.targetAlias}.id`);

      // Add edge type filter if specified
      if (relPattern.edge.type) {
        whereParts.push(`${relPattern.edgeAlias}.type = ?`);
        params.push(relPattern.edge.type);
      }

      // Add source node filters (label and properties)
      const sourcePattern = (this.ctx as any)[`pattern_${relPattern.sourceAlias}`];
      if (sourcePattern?.label) {
        whereParts.push(`${relPattern.sourceAlias}.label = ?`);
        params.push(sourcePattern.label);
      }
      if (sourcePattern?.properties) {
        for (const [key, value] of Object.entries(sourcePattern.properties)) {
          if (this.isParameterRef(value as PropertyValue)) {
            whereParts.push(`json_extract(${relPattern.sourceAlias}.properties, '$.${key}') = ?`);
            params.push(this.ctx.paramValues[(value as ParameterRef).name]);
          } else {
            whereParts.push(`json_extract(${relPattern.sourceAlias}.properties, '$.${key}') = ?`);
            params.push(value);
          }
        }
      }

      // Add target node filters (label and properties)
      const targetPattern = (this.ctx as any)[`pattern_${relPattern.targetAlias}`];
      if (targetPattern?.label) {
        whereParts.push(`${relPattern.targetAlias}.label = ?`);
        params.push(targetPattern.label);
      }
      if (targetPattern?.properties) {
        for (const [key, value] of Object.entries(targetPattern.properties)) {
          if (this.isParameterRef(value as PropertyValue)) {
            whereParts.push(`json_extract(${relPattern.targetAlias}.properties, '$.${key}') = ?`);
            params.push(this.ctx.paramValues[(value as ParameterRef).name]);
          } else {
            whereParts.push(`json_extract(${relPattern.targetAlias}.properties, '$.${key}') = ?`);
            params.push(value);
          }
        }
      }
    } else {
      // Simple node query
      for (const [variable, info] of this.ctx.variables) {
        const pattern = (this.ctx as any)[`pattern_${info.alias}`];
        if (pattern && info.type === "node") {
          fromParts.push(`nodes ${info.alias}`);

          if (pattern.label) {
            whereParts.push(`${info.alias}.label = ?`);
            params.push(pattern.label);
          }

          if (pattern.properties) {
            for (const [key, value] of Object.entries(pattern.properties)) {
              if (this.isParameterRef(value as PropertyValue)) {
                whereParts.push(`json_extract(${info.alias}.properties, '$.${key}') = ?`);
                params.push(this.ctx.paramValues[(value as ParameterRef).name]);
              } else {
                whereParts.push(`json_extract(${info.alias}.properties, '$.${key}') = ?`);
                params.push(value);
              }
            }
          }
        }
      }
    }

    // Add WHERE conditions from MATCH
    const whereClause = (this.ctx as any).whereClause;
    if (whereClause) {
      const { sql: whereSql, params: whereParams } = this.translateWhere(whereClause);
      whereParts.push(whereSql);
      params.push(...whereParams);
    }

    // Build final SQL
    let sql = `SELECT ${selectParts.join(", ")}`;

    if (fromParts.length > 0) {
      sql += ` FROM ${fromParts.join(", ")}`;
    }

    if (joinParts.length > 0) {
      sql += ` ${joinParts.join(" ")}`;
    }

    if (whereParts.length > 0) {
      sql += ` WHERE ${whereParts.join(" AND ")}`;
    }

    if (clause.limit !== undefined) {
      sql += ` LIMIT ?`;
      params.push(clause.limit);
    }

    return {
      statements: [{ sql, params }],
      returnColumns,
    };
  }

  private translateExpression(expr: Expression): { sql: string; tables: string[] } {
    const tables: string[] = [];

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
        };
      }

      case "function": {
        if (expr.functionName === "COUNT") {
          if (expr.args && expr.args.length > 0) {
            const argExpr = this.translateExpression(expr.args[0]);
            tables.push(...argExpr.tables);
            return { sql: `COUNT(*)`, tables };
          }
          return { sql: "COUNT(*)", tables };
        }
        if (expr.functionName === "ID") {
          if (expr.args && expr.args.length > 0 && expr.args[0].type === "variable") {
            const varInfo = this.ctx.variables.get(expr.args[0].variable!);
            if (!varInfo) {
              throw new Error(`Unknown variable: ${expr.args[0].variable}`);
            }
            tables.push(varInfo.alias);
            return { sql: `${varInfo.alias}.id`, tables };
          }
        }
        throw new Error(`Unknown function: ${expr.functionName}`);
      }

      case "literal": {
        return { sql: "?", tables };
      }

      case "parameter": {
        return { sql: "?", tables };
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

      default:
        throw new Error(`Unknown condition type: ${condition.type}`);
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
        return { sql: "?", params: [expr.value] };
      }

      case "parameter": {
        const value = this.ctx.paramValues[expr.name!];
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
