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
  WithClause,
  UnwindClause,
  UnionClause,
  CallClause,
  NodePattern,
  RelationshipPattern,
  EdgePattern,
  WhereCondition,
  Expression,
  ObjectProperty,
  PropertyValue,
  ParameterRef,
  VariableRef,
  SetAssignment,
  ReturnItem,
  CaseWhen,
  parse,
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
  variables: Map<string, { type: "node" | "edge" | "path"; alias: string }>;
  // Parameter values provided by the user
  paramValues: Record<string, unknown>;
  // Counter for generating unique aliases
  aliasCounter: number;
  // WITH clause info for query chaining
  withClauses?: WithClause[];
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

    // Handle standalone CALL (no RETURN clause following)
    const callClause = (this.ctx as any).callClause as {
      procedure: string;
      yields: string[];
      returnColumn: string;
      tableName: string;
      columnName: string;
      where?: WhereCondition;
    } | undefined;

    if (callClause && statements.length === 0) {
      // Generate SQL for standalone CALL
      const params: unknown[] = [];
      let sql = `SELECT DISTINCT ${callClause.columnName} AS "${callClause.returnColumn}" FROM ${callClause.tableName}`;
      sql += ` WHERE ${callClause.columnName} IS NOT NULL AND ${callClause.columnName} <> ''`;
      
      // Add WHERE from CALL...YIELD...WHERE
      if (callClause.where) {
        // Use columnName (actual SQL column) for WHERE clause translation
        const whereResult = this.translateCallWhere(callClause.where, callClause.columnName);
        sql += ` AND (${whereResult.sql})`;
        params.push(...whereResult.params);
      }

      statements.push({ sql, params });
      returnColumns = [callClause.returnColumn];
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
      case "WITH":
        return { statements: this.translateWith(clause) };
      case "UNWIND":
        return { statements: this.translateUnwind(clause) };
      case "UNION":
        return this.translateUnion(clause as UnionClause);
      case "CALL":
        return this.translateCall(clause as CallClause);
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
    // Normalize label to JSON array format
    const labelArray = node.label 
      ? (Array.isArray(node.label) ? node.label : [node.label])
      : [];
    const labelJson = JSON.stringify(labelArray);
    const properties = this.serializeProperties(node.properties || {});

    if (node.variable) {
      this.ctx.variables.set(node.variable, { type: "node", alias: id });
    }

    return {
      sql: "INSERT INTO nodes (id, label, properties) VALUES (?, ?, ?)",
      params: [id, labelJson, properties.json],
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

    // Handle path expressions (e.g., p = (a)-[r]->(b))
    if (clause.pathExpressions) {
      for (const pathExpr of clause.pathExpressions) {
        this.registerPathExpression(pathExpr, optional);
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

  private registerPathExpression(pathExpr: any, optional: boolean = false): void {
    // Register the path variable so it can be returned
    const pathAlias = `path${this.ctx.aliasCounter++}`;
    this.ctx.variables.set(pathExpr.variable, { type: "path", alias: pathAlias });

    // Store path information
    if (!(this.ctx as any).pathExpressions) {
      (this.ctx as any).pathExpressions = [];
    }

    const nodeAliases: string[] = [];
    const edgeAliases: string[] = [];
    let isVariableLength = false;

    // Register all patterns within the path
    for (const pattern of pathExpr.patterns) {
      if (this.isRelationshipPattern(pattern)) {
        const relPattern = pattern as any;
        
        // Check if source is first in chain - if so, record it before registering relationship
        const isFirstInChain = nodeAliases.length === 0;
        
        // Register the relationship pattern - this will handle node and edge registration
        this.registerRelationshipPattern(pattern, optional);
        
        // Now extract the aliases that were created
        const relPatternInfo = (this.ctx as any).relationshipPatterns[(this.ctx as any).relationshipPatterns.length - 1];
        
        // Track if this path contains a variable-length pattern
        if (relPatternInfo.isVariableLength) {
          isVariableLength = true;
        }
        
        // Add source node alias (only for first pattern in chain)
        if (isFirstInChain) {
          nodeAliases.push(relPatternInfo.sourceAlias);
        }
        
        // Add edge alias
        edgeAliases.push(relPatternInfo.edgeAlias);
        
        // Add target node alias
        nodeAliases.push(relPatternInfo.targetAlias);
      } else {
        // Single node pattern in path
        const nodeAlias = this.registerNodePattern(pattern, optional);
        nodeAliases.push(nodeAlias);
      }
    }

    // For variable-length paths, pre-allocate a CTE name so that length(p) can reference it
    let pathCteName: string | undefined;
    if (isVariableLength) {
      pathCteName = `path_${this.ctx.aliasCounter++}`;
    }

    (this.ctx as any).pathExpressions.push({
      variable: pathExpr.variable,
      alias: pathAlias,
      nodeAliases: Array.from(new Set(nodeAliases)), // Remove duplicates
      edgeAliases,
      patterns: pathExpr.patterns,
      optional,
      isVariableLength,
      pathCteName  // CTE name for variable-length path, used by length(p)
    });
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
    
    // Check if this is a variable-length pattern
    const isVariableLength = rel.edge.minHops !== undefined || rel.edge.maxHops !== undefined;
    
    (this.ctx as any).relationshipPatterns.push({ 
      sourceAlias, 
      targetAlias, 
      edgeAlias, 
      edge: rel.edge, 
      optional,
      sourceIsNew,
      targetIsNew,
      isVariableLength,
      minHops: rel.edge.minHops,
      maxHops: rel.edge.maxHops,
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
    // Note: Complex MERGE with ON CREATE SET / ON MATCH SET is handled by executor

    // For now, only handle simple node patterns
    if (clause.patterns.length !== 1) {
      throw new Error("MERGE with multiple patterns not supported in translator");
    }

    const pattern = clause.patterns[0];
    if (this.isRelationshipPattern(pattern)) {
      // Relationship MERGE is handled by executor
      throw new Error("MERGE with relationship pattern must be executed, not translated");
    }

    const node = pattern as NodePattern;
    const label = node.label || "";
    const props = node.properties || {};
    const serialized = this.serializeProperties(props);

    // Build condition to find existing node
    const labelCondition = this.generateLabelMatchCondition("", label);
    const conditions: string[] = [labelCondition.sql.replace(/^[^.]+\./, "")]; // Remove alias prefix
    const params: unknown[] = [...labelCondition.params];

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

    // Normalize label to JSON array for storage
    const labelJson = this.normalizeLabelToJson(label);

    // SQLite INSERT OR IGNORE + SELECT approach
    // First, try to insert
    const insertSql = `INSERT OR IGNORE INTO nodes (id, label, properties) 
      SELECT ?, ?, ? 
      WHERE NOT EXISTS (SELECT 1 FROM nodes WHERE ${conditions.join(" AND ")})`;

    return [
      {
        sql: insertSql,
        params: [id, labelJson, serialized.json, ...params],
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
      
      // Handle label assignment: SET n:Label1:Label2
      if (assignment.labels && assignment.labels.length > 0) {
        if (varInfo.type !== "node") {
          throw new Error(`Cannot set labels on a relationship: ${assignment.variable}`);
        }
        // Add labels to the existing label array using JSON functions
        // We need to merge existing labels with new ones and deduplicate
        // Use: json_array_length to check existing, json_each to iterate
        // For simplicity: UPDATE nodes SET label = (SELECT json_group_array(DISTINCT value) FROM (
        //   SELECT value FROM json_each(nodes.label)
        //   UNION
        //   SELECT value FROM json_each(?)))
        // WHERE id = ?
        const newLabelsJson = JSON.stringify(assignment.labels);
        statements.push({
          sql: `UPDATE nodes SET label = (SELECT json_group_array(value) FROM (
            SELECT DISTINCT value FROM (
              SELECT value FROM json_each(nodes.label)
              UNION ALL
              SELECT value FROM json_each(?)
            ) ORDER BY value
          )) WHERE id = ?`,
          params: [newLabelsJson, varInfo.alias],
        });
        continue;
      }
      
      // Handle SET n = {props} - replace all properties
      if (assignment.replaceProps && assignment.value) {
        const newProps = this.evaluateObjectExpression(assignment.value);
        // Filter out null values (they should be removed)
        const filteredProps: Record<string, unknown> = {};
        for (const [key, val] of Object.entries(newProps)) {
          if (val !== null) {
            filteredProps[key] = val;
          }
        }
        statements.push({
          sql: `UPDATE ${table} SET properties = ? WHERE id = ?`,
          params: [JSON.stringify(filteredProps), varInfo.alias],
        });
        continue;
      }
      
      // Handle SET n += {props} - merge properties
      if (assignment.mergeProps && assignment.value) {
        const newProps = this.evaluateObjectExpression(assignment.value);
        // Use json_patch to merge. For null values, we need to remove those properties.
        // json_patch doesn't handle null removal, so we build a compound expression.
        // First merge, then remove null values.
        // Actually, SQLite's json_patch DOES overwrite values, but null means "remove" in JSON Merge Patch (RFC 7396)
        // However, SQLite's json_patch doesn't follow RFC 7396 for null handling.
        // We'll use a combination: json_patch to add new properties, then json_remove for nulls.
        
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
          statements.push({
            sql: `UPDATE ${table} SET properties = json_remove(json_patch(properties, ?), ${removePaths}) WHERE id = ?`,
            params: [JSON.stringify(nonNullProps), varInfo.alias],
          });
        } else {
          // Just merge
          statements.push({
            sql: `UPDATE ${table} SET properties = json_patch(properties, ?) WHERE id = ?`,
            params: [JSON.stringify(nonNullProps), varInfo.alias],
          });
        }
        continue;
      }
      
      // Handle property assignment: SET n.prop = value
      if (!assignment.property || !assignment.value) {
        throw new Error(`Invalid SET assignment for variable: ${assignment.variable}`);
      }
      
      // Check if the value is a dynamic expression (function, binary op, etc.) that needs SQL translation
      if (assignment.value.type === "function" || assignment.value.type === "binary") {
        const { sql: exprSql, params: exprParams } = this.translateExpression(assignment.value);
        // Use json_set with the SQL expression directly
        statements.push({
          sql: `UPDATE ${table} SET properties = json_set(properties, '$.${assignment.property}', ${exprSql}) WHERE id = ?`,
          params: [...exprParams, varInfo.alias],
        });
      } else {
        const value = this.evaluateExpression(assignment.value);
        // Use json_set to update the property
        statements.push({
          sql: `UPDATE ${table} SET properties = json_set(properties, '$.${assignment.property}', json(?)) WHERE id = ?`,
          params: [JSON.stringify(value), varInfo.alias],
        });
      }
    }

    return statements;
  }
  
  /**
   * Evaluate an object expression to get its key-value pairs.
   */
  private evaluateObjectExpression(expr: Expression): Record<string, unknown> {
    if (expr.type === "object" && expr.properties) {
      const result: Record<string, unknown> = {};
      for (const prop of expr.properties) {
        result[prop.key] = this.evaluateExpression(prop.value);
      }
      return result;
    }
    if (expr.type === "parameter") {
      const paramValue = this.ctx.paramValues[expr.name!];
      if (typeof paramValue === "object" && paramValue !== null) {
        return paramValue as Record<string, unknown>;
      }
      throw new Error(`Parameter ${expr.name} is not an object`);
    }
    throw new Error(`Expected object expression, got ${expr.type}`);
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
    // Check if this is a RETURN after CALL
    const callClause = (this.ctx as any).callClause as {
      procedure: string;
      yields: string[];
      returnColumn: string;
      tableName: string;
      columnName: string;
      where?: WhereCondition;
    } | undefined;

    if (callClause) {
      return this.translateReturnFromCall(clause, callClause);
    }

    const selectParts: string[] = [];
    const returnColumns: string[] = [];
    const fromParts: string[] = [];
    const joinParts: string[] = [];
    const joinParams: unknown[] = []; // Parameters for JOIN ON clauses
    const whereParts: string[] = [];
    const whereParams: unknown[] = []; // Parameters for WHERE clause
    
    // Apply WITH modifiers if present
    const withDistinct = (this.ctx as any).withDistinct as boolean | undefined;
    const withOrderBy = (this.ctx as any).withOrderBy as { expression: Expression; direction: "ASC" | "DESC" }[] | undefined;
    const withSkip = (this.ctx as any).withSkip as number | undefined;
    const withLimit = (this.ctx as any).withLimit as number | undefined;
    const withWhere = (this.ctx as any).withWhere as WhereCondition | undefined;

    // Track which tables we need
    const neededTables = new Set<string>();

    // Process return items
    const exprParams: unknown[] = [];
    
    // Check for RETURN * (return all bound variables)
    let returnItems = clause.items;
    if (clause.items.length > 0 && 
        clause.items[0].expression.type === "variable" && 
        clause.items[0].expression.variable === "*") {
      // Expand * to all bound variables
      const expandedItems: ReturnItem[] = [];
      for (const [varName, varInfo] of this.ctx.variables) {
        expandedItems.push({ expression: { type: "variable", variable: varName } });
      }
      // Add any other items after the * (e.g., RETURN *, count(*) AS cnt)
      for (let i = 1; i < clause.items.length; i++) {
        expandedItems.push(clause.items[i]);
      }
      returnItems = expandedItems;
    }
    
    for (const item of returnItems) {
      const { sql: exprSql, tables, params: itemParams } = this.translateExpression(item.expression);
      tables.forEach((t) => neededTables.add(t));
      exprParams.push(...itemParams);

      const alias = item.alias || this.getExpressionName(item.expression);
      selectParts.push(`${exprSql} AS ${this.quoteAlias(alias)}`);
      returnColumns.push(alias);
    }

    // Build FROM clause based on registered patterns
    const relPatterns = (this.ctx as any).relationshipPatterns as Array<{
      sourceAlias: string;
      targetAlias: string;
      edgeAlias: string;
      edge: { type?: string; properties?: Record<string, PropertyValue>; minHops?: number; maxHops?: number };
      optional?: boolean;
      sourceIsNew?: boolean;
      targetIsNew?: boolean;
      isVariableLength?: boolean;
      minHops?: number;
      maxHops?: number;
    }> | undefined;

    // Check if any pattern is variable-length
    const hasVariableLengthPattern = relPatterns?.some(p => p.isVariableLength);
    
    if (hasVariableLengthPattern && relPatterns) {
      // Use recursive CTE for variable-length paths
      return this.translateVariableLengthPath(clause, relPatterns, selectParts, returnColumns, exprParams, whereParams);
    }

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

        // Add edge property filters
        if (relPattern.edge.properties) {
          for (const [key, value] of Object.entries(relPattern.edge.properties)) {
            if (this.isParameterRef(value as PropertyValue)) {
              if (isOptional) {
                edgeOnConditions.push(`json_extract(${relPattern.edgeAlias}.properties, '$.${key}') = ?`);
                edgeOnParams.push(this.ctx.paramValues[(value as ParameterRef).name]);
              } else {
                whereParts.push(`json_extract(${relPattern.edgeAlias}.properties, '$.${key}') = ?`);
                whereParams.push(this.ctx.paramValues[(value as ParameterRef).name]);
              }
            } else {
              if (isOptional) {
                edgeOnConditions.push(`json_extract(${relPattern.edgeAlias}.properties, '$.${key}') = ?`);
                edgeOnParams.push(value);
              } else {
                whereParts.push(`json_extract(${relPattern.edgeAlias}.properties, '$.${key}') = ?`);
                whereParams.push(value);
              }
            }
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
            const labelMatch = this.generateLabelMatchCondition(relPattern.targetAlias, targetPattern.label);
            targetOnConditions.push(labelMatch.sql);
            targetOnParams.push(...labelMatch.params);
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
              const labelMatch = this.generateLabelMatchCondition(relPattern.sourceAlias, sourcePattern.label);
              whereParts.push(labelMatch.sql);
              whereParams.push(...labelMatch.params);
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
              const labelMatch = this.generateLabelMatchCondition(relPattern.targetAlias, targetPattern.label);
              whereParts.push(labelMatch.sql);
              whereParams.push(...labelMatch.params);
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
              const labelMatch = this.generateLabelMatchCondition(info.alias, pattern.label);
              whereParts.push(labelMatch.sql);
              whereParams.push(...labelMatch.params);
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
              const labelMatch = this.generateLabelMatchCondition(info.alias, pattern.label);
              onConditions.push(labelMatch.sql);
              onParams.push(...labelMatch.params);
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
              const labelMatch = this.generateLabelMatchCondition(info.alias, pattern.label);
              whereParts.push(labelMatch.sql);
              whereParams.push(...labelMatch.params);
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

    // Add UNWIND tables using json_each
    const unwindClauses = (this.ctx as any).unwindClauses as Array<{
      alias: string;
      variable: string;
      jsonExpr: string;
      params: unknown[];
    }> | undefined;
    
    if (unwindClauses && unwindClauses.length > 0) {
      for (const unwindClause of unwindClauses) {
        // Use CROSS JOIN with json_each to expand the array
        if (fromParts.length === 0 && joinParts.length === 0) {
          // No FROM yet, use json_each directly
          fromParts.push(`json_each(${unwindClause.jsonExpr}) ${unwindClause.alias}`);
        } else {
          // Add as a cross join
          joinParts.push(`CROSS JOIN json_each(${unwindClause.jsonExpr}) ${unwindClause.alias}`);
        }
        exprParams.push(...unwindClause.params);
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
    
    // Add WHERE conditions from WITH clause
    if (withWhere) {
      const { sql: whereSql, params: conditionParams } = this.translateWhere(withWhere);
      whereParts.push(whereSql);
      whereParams.push(...conditionParams);
    }

    // Build final SQL
    // Apply DISTINCT from either the RETURN clause or preceding WITH
    const distinctKeyword = (clause.distinct || withDistinct) ? "DISTINCT " : "";
    let sql = `SELECT ${distinctKeyword}${selectParts.join(", ")}`;

    if (fromParts.length > 0) {
      sql += ` FROM ${fromParts.join(", ")}`;
    } else if (joinParts.length > 0) {
      // If we have JOINs but no FROM, we need a dummy FROM clause
      // This happens with OPTIONAL MATCH without a prior MATCH
      sql += ` FROM (SELECT 1) __dummy__`;
    }

    if (joinParts.length > 0) {
      sql += ` ${joinParts.join(" ")}`;
    }

    if (whereParts.length > 0) {
      sql += ` WHERE ${whereParts.join(" AND ")}`;
    }

    // Add ORDER BY clause - use WITH orderBy if RETURN doesn't have one
    const effectiveOrderBy = clause.orderBy && clause.orderBy.length > 0 ? clause.orderBy : withOrderBy;
    if (effectiveOrderBy && effectiveOrderBy.length > 0) {
      const orderParts = effectiveOrderBy.map(({ expression, direction }) => {
        const { sql: exprSql } = this.translateOrderByExpression(expression, returnColumns);
        return `${exprSql} ${direction}`;
      });
      sql += ` ORDER BY ${orderParts.join(", ")}`;
    }

    // Add LIMIT and OFFSET (SKIP) - combine WITH and RETURN values
    const effectiveLimit = clause.limit !== undefined ? clause.limit : withLimit;
    const effectiveSkip = clause.skip !== undefined ? clause.skip : withSkip;
    
    if (effectiveLimit !== undefined || effectiveSkip !== undefined) {
      if (effectiveLimit !== undefined) {
        sql += ` LIMIT ?`;
        whereParams.push(effectiveLimit);
      } else if (effectiveSkip !== undefined) {
        // SKIP without LIMIT - need a large limit for SQLite
        sql += ` LIMIT -1`;
      }

      if (effectiveSkip !== undefined) {
        sql += ` OFFSET ?`;
        whereParams.push(effectiveSkip);
      }
    }

    // Combine params in the order they appear in SQL: SELECT -> JOINs -> WHERE
    const allParams = [...exprParams, ...joinParams, ...whereParams];

    return {
      statements: [{ sql, params: allParams }],
      returnColumns,
    };
  }

  // ============================================================================
  // WITH
  // ============================================================================

  private translateWith(clause: WithClause): SqlStatement[] {
    // WITH clause stores its info in context for subsequent clauses
    // It creates a new "scope" by updating variable mappings
    
    if (!this.ctx.withClauses) {
      this.ctx.withClauses = [];
    }
    this.ctx.withClauses.push(clause);
    
    // Store where clause for later use
    if (clause.where) {
      (this.ctx as any).withWhere = clause.where;
    }
    
    // Store ORDER BY, SKIP, LIMIT for later use  
    if (clause.orderBy) {
      (this.ctx as any).withOrderBy = clause.orderBy;
    }
    if (clause.skip !== undefined) {
      (this.ctx as any).withSkip = clause.skip;
    }
    if (clause.limit !== undefined) {
      (this.ctx as any).withLimit = clause.limit;
    }
    if (clause.distinct) {
      (this.ctx as any).withDistinct = true;
    }
    
    // Update variable mappings for WITH items
    // Variables without aliases keep their current mappings
    // Variables with aliases create new mappings based on expression type
    for (const item of clause.items) {
      const alias = item.alias;
      
      if (item.expression.type === "variable") {
        // Check for WITH * (pass through all variables)
        if (item.expression.variable === "*") {
          // All existing variables remain in scope - nothing to do
          continue;
        }
        // Variable passthrough - keep or create mapping
        const originalVar = item.expression.variable!;
        const originalInfo = this.ctx.variables.get(originalVar);
        if (originalInfo && alias) {
          this.ctx.variables.set(alias, originalInfo);
        }
      } else if (item.expression.type === "property" && alias) {
        // Property access with alias - this creates a "virtual" variable
        // We'll track this separately for the return phase
        if (!(this.ctx as any).withAliases) {
          (this.ctx as any).withAliases = new Map();
        }
        (this.ctx as any).withAliases.set(alias, item.expression);
      } else if (item.expression.type === "function" && alias) {
        // Function with alias - track for return phase
        if (!(this.ctx as any).withAliases) {
          (this.ctx as any).withAliases = new Map();
        }
        (this.ctx as any).withAliases.set(alias, item.expression);
      }
    }
    
    // WITH doesn't generate SQL statements directly - 
    // the SQL is generated by the final RETURN clause
    return [];
  }

  // ============================================================================
  // UNWIND
  // ============================================================================

  // ============================================================================
  // CALL procedure RETURN handling
  // ============================================================================

  private translateReturnFromCall(
    clause: ReturnClause,
    callClause: {
      procedure: string;
      yields: string[];
      returnColumn: string;
      tableName: string;
      columnName: string;
      where?: WhereCondition;
    }
  ): { statements: SqlStatement[]; returnColumns: string[] } {
    const params: unknown[] = [];
    const returnColumns: string[] = [];

    // Build SELECT parts
    const selectParts: string[] = [];
    for (const item of clause.items) {
      // For CALL, variables reference the yield column
      let exprSql: string;
      if (item.expression.type === "variable") {
        // Check if this variable is a yield variable
        const yieldRef = (this.ctx as any)[`call_yield_${item.expression.variable}`];
        if (yieldRef) {
          exprSql = yieldRef;
        } else {
          throw new Error(`Unknown variable: ${item.expression.variable}`);
        }
      } else {
        const translated = this.translateExpression(item.expression);
        exprSql = translated.sql;
        params.push(...translated.params);
      }
      const alias = item.alias || this.getExpressionName(item.expression);
      selectParts.push(`${exprSql} AS ${this.quoteAlias(alias)}`);
      returnColumns.push(alias);
    }

    // Build base query
    let sql = `SELECT DISTINCT ${selectParts.join(", ")} FROM ${callClause.tableName}`;
    
    // Add WHERE conditions
    const whereParts: string[] = [];
    
    // Base condition: exclude null/empty values
    whereParts.push(`${callClause.columnName} IS NOT NULL`);
    whereParts.push(`${callClause.columnName} <> ''`);
    
    // Add WHERE from CALL...YIELD...WHERE
    if (callClause.where) {
      // Use columnName (actual SQL column) for WHERE clause translation
      const whereResult = this.translateCallWhere(callClause.where, callClause.columnName);
      whereParts.push(whereResult.sql);
      params.push(...whereResult.params);
    }
    
    sql += ` WHERE ${whereParts.join(" AND ")}`;

    // Handle ORDER BY
    if (clause.orderBy && clause.orderBy.length > 0) {
      const orderParts: string[] = [];
      for (const order of clause.orderBy) {
        let orderSql: string;
        if (order.expression.type === "variable") {
          const yieldRef = (this.ctx as any)[`call_yield_${order.expression.variable}`];
          if (yieldRef) {
            orderSql = yieldRef;
          } else {
            throw new Error(`Unknown variable: ${order.expression.variable}`);
          }
        } else {
          const translated = this.translateExpression(order.expression);
          orderSql = translated.sql;
          params.push(...translated.params);
        }
        orderParts.push(`${orderSql} ${order.direction}`);
      }
      sql += ` ORDER BY ${orderParts.join(", ")}`;
    }

    // Handle SKIP
    if (clause.skip !== undefined) {
      sql += ` OFFSET ${clause.skip}`;
    }

    // Handle LIMIT
    if (clause.limit !== undefined) {
      sql += ` LIMIT ${clause.limit}`;
    }

    return {
      statements: [{ sql, params }],
      returnColumns,
    };
  }

  // ============================================================================
  // Variable-length paths
  // ============================================================================

  private translateVariableLengthPath(
    clause: ReturnClause,
    relPatterns: Array<{
      sourceAlias: string;
      targetAlias: string;
      edgeAlias: string;
      edge: { type?: string; properties?: Record<string, PropertyValue>; minHops?: number; maxHops?: number };
      optional?: boolean;
      sourceIsNew?: boolean;
      targetIsNew?: boolean;
      isVariableLength?: boolean;
      minHops?: number;
      maxHops?: number;
    }>,
    selectParts: string[],
    returnColumns: string[],
    exprParams: unknown[],
    whereParams: unknown[]
  ): { statements: SqlStatement[]; returnColumns: string[] } {
    // For variable-length paths, we use SQLite's recursive CTEs
    // Pattern: WITH RECURSIVE path(start_id, end_id, depth) AS (
    //   SELECT source_id, target_id, 1 FROM edges WHERE ...
    //   UNION ALL
    //   SELECT p.start_id, e.target_id, p.depth + 1
    //   FROM path p JOIN edges e ON p.end_id = e.source_id
    //   WHERE p.depth < max_depth
    // )
    // SELECT ... FROM nodes n0, path, nodes n1 WHERE n0.id = path.start_id AND n1.id = path.end_id ...

    const varLengthPattern = relPatterns.find(p => p.isVariableLength);
    if (!varLengthPattern) {
      throw new Error("No variable-length pattern found");
    }

    const minHops = varLengthPattern.minHops ?? 1;
    // For unbounded paths (*), use a reasonable default max
    // For fixed length (*2), maxHops equals minHops
    const maxHops = varLengthPattern.maxHops ?? 10;
    const edgeType = varLengthPattern.edge.type;
    const sourceAlias = varLengthPattern.sourceAlias;
    const targetAlias = varLengthPattern.targetAlias;

    const allParams: unknown[] = [...exprParams];

    // Check if a path expression already allocated a CTE name for this variable-length pattern
    // This allows length(p) to reference the correct CTE
    let pathCteName: string | undefined;
    const pathExpressions = (this.ctx as any).pathExpressions as Array<{
      isVariableLength?: boolean;
      pathCteName?: string;
    }> | undefined;
    if (pathExpressions) {
      const pathExpr = pathExpressions.find(p => p.isVariableLength && p.pathCteName);
      if (pathExpr) {
        pathCteName = pathExpr.pathCteName;
      }
    }
    // If no pre-allocated name, generate a new one
    if (!pathCteName) {
      pathCteName = `path_${this.ctx.aliasCounter++}`;
    }
    
    // Base condition for edges
    let edgeCondition = "1=1";
    if (edgeType) {
      edgeCondition = "type = ?";
      allParams.push(edgeType);
    }

    // Build the CTE
    // The depth represents the number of edges traversed
    // For *2, we want exactly 2 edges, so depth should stop at maxHops
    // The condition is p.depth < maxHops to allow one more recursion step
    const cte = `WITH RECURSIVE ${pathCteName}(start_id, end_id, depth) AS (
  SELECT source_id, target_id, 1 FROM edges WHERE ${edgeCondition}
  UNION ALL
  SELECT p.start_id, e.target_id, p.depth + 1
  FROM ${pathCteName} p
  JOIN edges e ON p.end_id = e.source_id
  WHERE p.depth < ?${edgeType ? " AND e.type = ?" : ""}
)`;

    // For maxHops=2, we need depth to reach 2, so recursion limit should be maxHops
    allParams.push(maxHops);
    if (edgeType) {
      allParams.push(edgeType);
    }

    // Build WHERE conditions
    const whereParts: string[] = [];
    
    // Connect source node to path start
    whereParts.push(`${sourceAlias}.id = ${pathCteName}.start_id`);
    // Connect target node to path end
    whereParts.push(`${targetAlias}.id = ${pathCteName}.end_id`);
    // Apply min depth constraint
    if (minHops > 1) {
      whereParts.push(`${pathCteName}.depth >= ?`);
      allParams.push(minHops);
    }

    // Add source/target label filters
    const sourcePattern = (this.ctx as any)[`pattern_${sourceAlias}`];
    if (sourcePattern?.label) {
      const labelMatch = this.generateLabelMatchCondition(sourceAlias, sourcePattern.label);
      whereParts.push(labelMatch.sql);
      allParams.push(...labelMatch.params);
    }
    if (sourcePattern?.properties) {
      for (const [key, value] of Object.entries(sourcePattern.properties)) {
        if (this.isParameterRef(value as PropertyValue)) {
          whereParts.push(`json_extract(${sourceAlias}.properties, '$.${key}') = ?`);
          allParams.push(this.ctx.paramValues[(value as ParameterRef).name]);
        } else {
          whereParts.push(`json_extract(${sourceAlias}.properties, '$.${key}') = ?`);
          allParams.push(value);
        }
      }
    }

    const targetPattern = (this.ctx as any)[`pattern_${targetAlias}`];
    if (targetPattern?.label) {
      const labelMatch = this.generateLabelMatchCondition(targetAlias, targetPattern.label);
      whereParts.push(labelMatch.sql);
      allParams.push(...labelMatch.params);
    }
    if (targetPattern?.properties) {
      for (const [key, value] of Object.entries(targetPattern.properties)) {
        if (this.isParameterRef(value as PropertyValue)) {
          whereParts.push(`json_extract(${targetAlias}.properties, '$.${key}') = ?`);
          allParams.push(this.ctx.paramValues[(value as ParameterRef).name]);
        } else {
          whereParts.push(`json_extract(${targetAlias}.properties, '$.${key}') = ?`);
          allParams.push(value);
        }
      }
    }

    // Add WHERE clause from MATCH if present
    const matchWhereClause = (this.ctx as any).whereClause;
    if (matchWhereClause) {
      const { sql: whereSql, params: conditionParams } = this.translateWhere(matchWhereClause);
      whereParts.push(whereSql);
      allParams.push(...conditionParams);
    }

    // Build final SQL
    const distinctKeyword = clause.distinct ? "DISTINCT " : "";
    let sql = `${cte}\nSELECT ${distinctKeyword}${selectParts.join(", ")}`;
    sql += ` FROM nodes ${sourceAlias}, ${pathCteName}, nodes ${targetAlias}`;
    
    if (whereParts.length > 0) {
      sql += ` WHERE ${whereParts.join(" AND ")}`;
    }

    // Check if we need GROUP BY (when mixing aggregates with non-aggregates)
    const hasAggregates = clause.items.some(item => this.isAggregateExpression(item.expression));
    const nonAggregateItems = clause.items.filter(item => !this.isAggregateExpression(item.expression));
    
    if (hasAggregates && nonAggregateItems.length > 0) {
      // Build GROUP BY using the translated expressions for non-aggregates
      const groupByParts: string[] = [];
      for (const item of nonAggregateItems) {
        const { sql: exprSql } = this.translateExpression(item.expression);
        groupByParts.push(exprSql);
      }
      sql += ` GROUP BY ${groupByParts.join(", ")}`;
    }

    // Add ORDER BY if present
    if (clause.orderBy && clause.orderBy.length > 0) {
      const orderParts = clause.orderBy.map(({ expression, direction }) => {
        const { sql: exprSql } = this.translateOrderByExpression(expression, returnColumns);
        return `${exprSql} ${direction}`;
      });
      sql += ` ORDER BY ${orderParts.join(", ")}`;
    }

    // Add LIMIT and SKIP
    if (clause.limit !== undefined || clause.skip !== undefined) {
      if (clause.limit !== undefined) {
        sql += ` LIMIT ?`;
        allParams.push(clause.limit);
      } else if (clause.skip !== undefined) {
        sql += ` LIMIT -1`;
      }

      if (clause.skip !== undefined) {
        sql += ` OFFSET ?`;
        allParams.push(clause.skip);
      }
    }

    return {
      statements: [{ sql, params: allParams }],
      returnColumns,
    };
  }

  // ============================================================================
  // UNION
  // ============================================================================

  private translateUnion(clause: UnionClause): { statements: SqlStatement[]; returnColumns: string[] } {
    // Translate left query (create a fresh translator to avoid context contamination)
    const leftTranslator = new Translator(this.ctx.paramValues);
    const leftResult = leftTranslator.translate(clause.left);
    
    // Translate right query
    const rightTranslator = new Translator(this.ctx.paramValues);
    const rightResult = rightTranslator.translate(clause.right);
    
    // Get the SQL from both sides (should be SELECT statements)
    const leftSql = leftResult.statements.map(s => s.sql).join("; ");
    const rightSql = rightResult.statements.map(s => s.sql).join("; ");
    
    // Combine params from both sides
    const allParams = [
      ...leftResult.statements.flatMap(s => s.params),
      ...rightResult.statements.flatMap(s => s.params),
    ];
    
    // Build UNION SQL
    const unionKeyword = clause.all ? "UNION ALL" : "UNION";
    const sql = `${leftSql} ${unionKeyword} ${rightSql}`;
    
    // Return columns come from the left query
    const returnColumns = leftResult.returnColumns || [];
    
    return {
      statements: [{ sql, params: allParams }],
      returnColumns,
    };
  }

  private translateUnwind(clause: UnwindClause): SqlStatement[] {
    // UNWIND expands an array into rows using SQLite's json_each()
    // We store the unwind info in context for use in RETURN
    
    const alias = `unwind${this.ctx.aliasCounter++}`;
    
    // Store the unwind information for later use
    if (!(this.ctx as any).unwindClauses) {
      (this.ctx as any).unwindClauses = [];
    }
    
    // Determine the expression for json_each
    let jsonExpr: string;
    let params: unknown[] = [];
    
    if (clause.expression.type === "literal") {
      // Literal array - serialize to JSON
      jsonExpr = "?";
      params.push(JSON.stringify(clause.expression.value));
    } else if (clause.expression.type === "parameter") {
      // Parameter - will be resolved at runtime
      jsonExpr = "?";
      const paramValue = this.ctx.paramValues[clause.expression.name!];
      params.push(JSON.stringify(paramValue));
    } else if (clause.expression.type === "variable") {
      // Variable reference - could be from WITH/COLLECT
      const varName = clause.expression.variable!;
      const withAliases = (this.ctx as any).withAliases as Map<string, Expression> | undefined;
      
      if (withAliases && withAliases.has(varName)) {
        // It's a WITH alias - need to inline the expression
        const originalExpr = withAliases.get(varName)!;
        const translated = this.translateExpression(originalExpr);
        jsonExpr = translated.sql;
        params.push(...translated.params);
      } else {
        // It's a regular variable
        const varInfo = this.ctx.variables.get(varName);
        if (varInfo) {
          jsonExpr = `${varInfo.alias}.properties`;
        } else {
          throw new Error(`Unknown variable in UNWIND: ${varName}`);
        }
      }
    } else if (clause.expression.type === "property") {
      // Property access on a variable
      const varInfo = this.ctx.variables.get(clause.expression.variable!);
      if (!varInfo) {
        throw new Error(`Unknown variable: ${clause.expression.variable}`);
      }
      jsonExpr = `json_extract(${varInfo.alias}.properties, '$.${clause.expression.property}')`;
    } else {
      throw new Error(`Unsupported expression type in UNWIND: ${clause.expression.type}`);
    }
    
    (this.ctx as any).unwindClauses.push({
      alias,
      variable: clause.alias,
      jsonExpr,
      params,
    });
    
    // Register the unwind alias as a variable for subsequent use
    // We use 'unwind' as the type to distinguish it from nodes/edges
    this.ctx.variables.set(clause.alias, { type: "node", alias }); // Using 'node' as a placeholder type
    
    // Store special marker that this is an unwind variable
    (this.ctx as any)[`unwind_${alias}`] = true;
    
    // UNWIND doesn't generate SQL directly - it sets up context for RETURN
    return [];
  }

  private translateCall(clause: CallClause): { statements?: SqlStatement[]; returnColumns?: string[] } {
    // CALL procedures for database introspection
    // Supported procedures:
    // - db.labels() - returns all distinct node labels
    // - db.relationshipTypes() - returns all distinct relationship types

    const procedure = clause.procedure.toLowerCase();
    
    let tableName: string;
    let columnName: string;
    let returnColumn: string;
    const params: unknown[] = [];

    switch (procedure) {
      case "db.labels":
        // Get distinct labels from nodes table (labels are stored as JSON arrays)
        // We need to extract individual labels from json_each
        tableName = "nodes, json_each(nodes.label)";
        columnName = "json_each.value";
        returnColumn = "label";
        break;
      case "db.relationshiptypes":
        // Get distinct types from edges table
        tableName = "edges";
        columnName = "type";
        returnColumn = "type";
        break;
      default:
        throw new Error(`Unknown procedure: ${clause.procedure}`);
    }

    // Store call info in context for use in subsequent RETURN
    (this.ctx as any).callClause = {
      procedure: clause.procedure,
      yields: clause.yields || [returnColumn],
      returnColumn,
      tableName,
      columnName,
      where: clause.where,
    };

    // Register yield variables for subsequent clauses
    // Use columnName (actual SQL column) for variable resolution
    if (clause.yields) {
      for (const yieldVar of clause.yields) {
        // Use a special marker to track CALL yield variables
        (this.ctx as any)[`call_yield_${yieldVar}`] = columnName;
      }
    } else {
      // Default yield variable matches the return column
      (this.ctx as any)[`call_yield_${returnColumn}`] = columnName;
    }

    // Don't generate SQL here - let translateReturn handle it if there's a RETURN clause
    // Only generate standalone SQL if there's no RETURN clause following
    return {
      statements: [],
      returnColumns: [returnColumn],
    };
  }

  private translateCallWhere(condition: WhereCondition, yieldColumn: string): { sql: string; params: unknown[] } {
    const params: unknown[] = [];

    switch (condition.type) {
      case "comparison": {
        // Handle comparisons like "label <> 'SystemNode'"
        let leftSql: string;
        
        if (condition.left?.type === "variable" && condition.left.variable === yieldColumn) {
          leftSql = yieldColumn;
        } else if (condition.left) {
          const leftResult = this.translateExpressionForCall(condition.left, yieldColumn);
          leftSql = leftResult.sql;
          params.push(...leftResult.params);
        } else {
          throw new Error("Missing left side of comparison");
        }

        let rightSql: string;
        if (condition.right) {
          const rightResult = this.translateExpressionForCall(condition.right, yieldColumn);
          rightSql = rightResult.sql;
          params.push(...rightResult.params);
        } else {
          throw new Error("Missing right side of comparison");
        }

        return { sql: `${leftSql} ${condition.operator} ${rightSql}`, params };
      }

      case "and": {
        const parts = condition.conditions!.map(c => this.translateCallWhere(c, yieldColumn));
        const sql = parts.map(p => `(${p.sql})`).join(" AND ");
        for (const p of parts) params.push(...p.params);
        return { sql, params };
      }

      case "or": {
        const parts = condition.conditions!.map(c => this.translateCallWhere(c, yieldColumn));
        const sql = parts.map(p => `(${p.sql})`).join(" OR ");
        for (const p of parts) params.push(...p.params);
        return { sql, params };
      }

      case "not": {
        const inner = this.translateCallWhere(condition.condition!, yieldColumn);
        return { sql: `NOT (${inner.sql})`, params: inner.params };
      }

      default:
        throw new Error(`Unsupported condition type in CALL WHERE: ${condition.type}`);
    }
  }

  private translateExpressionForCall(expr: Expression, yieldColumn: string): { sql: string; params: unknown[] } {
    const params: unknown[] = [];

    switch (expr.type) {
      case "variable":
        // If the variable matches the yield column, use the column name directly
        if (expr.variable === yieldColumn) {
          return { sql: yieldColumn, params };
        }
        // Check if it's a yield variable
        const yieldRef = (this.ctx as any)[`call_yield_${expr.variable}`];
        if (yieldRef) {
          return { sql: yieldRef, params };
        }
        throw new Error(`Unknown variable in CALL WHERE: ${expr.variable}`);

      case "literal":
        if (typeof expr.value === "string") {
          return { sql: "?", params: [expr.value] };
        }
        if (typeof expr.value === "number") {
          return { sql: "?", params: [expr.value] };
        }
        if (typeof expr.value === "boolean") {
          return { sql: expr.value ? "1" : "0", params };
        }
        if (expr.value === null) {
          return { sql: "NULL", params };
        }
        throw new Error(`Unsupported literal type in CALL WHERE: ${typeof expr.value}`);

      case "parameter":
        const paramValue = this.ctx.paramValues[expr.name!];
        return { sql: "?", params: [paramValue] };

      default:
        throw new Error(`Unsupported expression type in CALL WHERE: ${expr.type}`);
    }
  }

  private translateExpression(expr: Expression): { sql: string; tables: string[]; params: unknown[] } {
    const tables: string[] = [];
    const params: unknown[] = [];

    switch (expr.type) {
      case "variable": {
        // First check if this is a WITH alias
        const withAliases = (this.ctx as any).withAliases as Map<string, Expression> | undefined;
        if (withAliases && withAliases.has(expr.variable!)) {
          // This variable is actually an alias from WITH - translate the underlying expression
          const originalExpr = withAliases.get(expr.variable!)!;
          return this.translateExpression(originalExpr);
        }
        
        // Check if this is a CALL yield variable
        const callYieldRef = (this.ctx as any)[`call_yield_${expr.variable}`];
        if (callYieldRef) {
          // This variable comes from a CALL...YIELD clause
          return { sql: callYieldRef, tables, params };
        }
        
        // Check if this is an UNWIND variable
        const unwindClauses = (this.ctx as any).unwindClauses as Array<{
          alias: string;
          variable: string;
          jsonExpr: string;
          params: unknown[];
        }> | undefined;
        
        if (unwindClauses) {
          const unwindClause = unwindClauses.find(u => u.variable === expr.variable);
          if (unwindClause) {
            tables.push(unwindClause.alias);
            // UNWIND variables access the 'value' column from json_each
            return {
              sql: `${unwindClause.alias}.value`,
              tables,
              params,
            };
          }
        }
        
        const varInfo = this.ctx.variables.get(expr.variable!);
        if (!varInfo) {
          throw new Error(`Unknown variable: ${expr.variable}`);
        }
        
        // Handle path variables
        if (varInfo.type === "path") {
          const pathExpressions = (this.ctx as any).pathExpressions as Array<{
            variable: string;
            alias: string;
            nodeAliases: string[];
            edgeAliases: string[];
            isVariableLength?: boolean;
            pathCteName?: string;
          }> | undefined;
          
          if (pathExpressions) {
            const pathInfo = pathExpressions.find(p => p.variable === expr.variable);
            if (pathInfo) {
              // For variable-length paths, we only have start/end nodes in the CTE
              // Intermediate nodes/edges are not directly accessible
              if (pathInfo.isVariableLength) {
                // Only add the node tables (start and end)
                tables.push(...pathInfo.nodeAliases);
                
                // Construct a simplified path object for variable-length paths
                const nodesJson = pathInfo.nodeAliases.map(alias => 
                  `json_object('id', ${alias}.id, 'label', ${alias}.label, 'properties', ${alias}.properties)`
                ).join(', ');
                
                // For variable-length paths, edges array shows the path length
                // Full edge tracking would require extending the CTE
                return {
                  sql: `json_object('nodes', json_array(${nodesJson}), 'length', ${pathInfo.pathCteName}.depth)`,
                  tables,
                  params,
                };
              }
              
              // For fixed-length paths, include all nodes and edges
              tables.push(...pathInfo.nodeAliases, ...pathInfo.edgeAliases);
              
              // Construct a path object with nodes and edges arrays
              const nodesJson = pathInfo.nodeAliases.map(alias => 
                `json_object('id', ${alias}.id, 'label', ${alias}.label, 'properties', ${alias}.properties)`
              ).join(', ');
              
              const edgesJson = pathInfo.edgeAliases.map(alias =>
                `json_object('id', ${alias}.id, 'type', ${alias}.type, 'source_id', ${alias}.source_id, 'target_id', ${alias}.target_id, 'properties', ${alias}.properties)`
              ).join(', ');
              
              return {
                sql: `json_object('nodes', json_array(${nodesJson}), 'edges', json_array(${edgesJson}))`,
                tables,
                params,
              };
            }
          }
          throw new Error(`Path information not found for variable: ${expr.variable}`);
        }
        
        tables.push(varInfo.alias);
        // Return the whole row as JSON for variables
        // Nodes have: id, label, properties
        // Edges have: id, type, source_id, target_id, properties
        if (varInfo.type === "edge") {
          return {
            sql: `json_object('id', ${varInfo.alias}.id, 'type', ${varInfo.alias}.type, 'source_id', ${varInfo.alias}.source_id, 'target_id', ${varInfo.alias}.target_id, 'properties', ${varInfo.alias}.properties)`,
            tables,
            params,
          };
        }
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
            const arg = expr.args[0];
            const distinctKeyword = expr.distinct ? "DISTINCT " : "";
            
            // For count(DISTINCT n.property), we need to count distinct property values
            if (arg.type === "property") {
              const varInfo = this.ctx.variables.get(arg.variable!);
              if (!varInfo) {
                throw new Error(`Unknown variable: ${arg.variable}`);
              }
              tables.push(varInfo.alias);
              return {
                sql: `COUNT(${distinctKeyword}json_extract(${varInfo.alias}.properties, '$.${arg.property}'))`,
                tables,
                params,
              };
            } else if (arg.type === "variable") {
              const varInfo = this.ctx.variables.get(arg.variable!);
              if (!varInfo) {
                throw new Error(`Unknown variable: ${arg.variable}`);
              }
              tables.push(varInfo.alias);
              // For count(n) or count(DISTINCT n), count nodes by id
              return {
                sql: `COUNT(${distinctKeyword}${varInfo.alias}.id)`,
                tables,
                params,
              };
            }
            // For other expressions, fall back to COUNT(*)
            const argExpr = this.translateExpression(arg);
            tables.push(...argExpr.tables);
            params.push(...argExpr.params);
            return { sql: `COUNT(${distinctKeyword}${argExpr.sql})`, tables, params };
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
        // Aggregation functions: SUM, AVG, MIN, MAX
        if (expr.functionName === "SUM" || expr.functionName === "AVG" || 
            expr.functionName === "MIN" || expr.functionName === "MAX") {
          if (expr.args && expr.args.length > 0) {
            const arg = expr.args[0];
            const distinctKeyword = expr.distinct ? "DISTINCT " : "";
            
            if (arg.type === "property") {
              const varInfo = this.ctx.variables.get(arg.variable!);
              if (!varInfo) {
                throw new Error(`Unknown variable: ${arg.variable}`);
              }
              tables.push(varInfo.alias);
              // Use json_extract for numeric properties in aggregations
              return {
                sql: `${expr.functionName}(${distinctKeyword}json_extract(${varInfo.alias}.properties, '$.${arg.property}'))`,
                tables,
                params,
              };
            } else if (arg.type === "variable") {
              const varInfo = this.ctx.variables.get(arg.variable!);
              if (!varInfo) {
                throw new Error(`Unknown variable: ${arg.variable}`);
              }
              tables.push(varInfo.alias);
              // For variable, aggregate the id
              return {
                sql: `${expr.functionName}(${distinctKeyword}${varInfo.alias}.id)`,
                tables,
                params,
              };
            }
          }
          throw new Error(`${expr.functionName} requires a property or variable argument`);
        }
        
        // Percentile functions: PERCENTILEDISC, PERCENTILECONT
        // percentileDisc(value, percentile) - Returns discrete value at percentile position
        // percentileCont(value, percentile) - Returns interpolated value at percentile position
        // 
        // These are aggregate functions that:
        // 1. Uses json_group_array() to collect all values into a sorted array (aggregate)
        // 2. Extracts the value at the appropriate percentile position
        if (expr.functionName === "PERCENTILEDISC" || expr.functionName === "PERCENTILECONT") {
          if (expr.args && expr.args.length >= 2) {
            const valueArg = expr.args[0];
            const percentileArg = expr.args[1];
            
            // Get the value expression (property access)
            let valueExpr: string;
            if (valueArg.type === "property") {
              const varInfo = this.ctx.variables.get(valueArg.variable!);
              if (!varInfo) {
                throw new Error(`Unknown variable: ${valueArg.variable}`);
              }
              tables.push(varInfo.alias);
              valueExpr = `json_extract(${varInfo.alias}.properties, '$.${valueArg.property}')`;
            } else {
              const argResult = this.translateFunctionArg(valueArg);
              tables.push(...argResult.tables);
              params.push(...argResult.params);
              valueExpr = argResult.sql;
            }
            
            // Get the percentile value (0-1 range)
            const percentileResult = this.translateFunctionArg(percentileArg);
            const percentileVal = percentileResult.sql;
            
            // We need to push the percentile parameters multiple times since the value
            // is used in multiple places in the SQL (for bounds checking and calculation)
            
            if (expr.functionName === "PERCENTILEDISC") {
              // Discrete percentile: returns actual value from sorted list at percentile position
              // Formula: index = ROUND(percentile * (count - 1))
              // Use a correlated subquery pattern with json_group_array
              // Push params 3 times for the 3 uses of percentileVal
              params.push(...percentileResult.params); // for <= 0 check
              params.push(...percentileResult.params); // for >= 1 check
              params.push(...percentileResult.params); // for index calculation
              return {
                sql: `(SELECT CASE
  WHEN json_array_length(sv) = 0 OR sv IS NULL THEN NULL
  WHEN ${percentileVal} <= 0 THEN json_extract(sv, '$[0]')
  WHEN ${percentileVal} >= 1 THEN json_extract(sv, '$[' || (json_array_length(sv) - 1) || ']')
  ELSE json_extract(sv, '$[' || CAST(ROUND(${percentileVal} * (json_array_length(sv) - 1)) AS INTEGER) || ']')
END FROM (SELECT json_group_array(${valueExpr}) as sv))`,
                tables,
                params,
              };
            } else {
              // Continuous percentile: interpolates between values
              // Formula: position = percentile * (count - 1)
              //          lower_idx = FLOOR(position), upper_idx = CEIL(position)
              //          fraction = position - lower_idx
              //          result = lower_val + fraction * (upper_val - lower_val)
              // Push params 5 times for the 5 uses of percentileVal
              params.push(...percentileResult.params); // for <= 0 check
              params.push(...percentileResult.params); // for >= 1 check
              params.push(...percentileResult.params); // for pos calculation
              params.push(...percentileResult.params); // for li calculation
              params.push(...percentileResult.params); // for ui calculation
              return {
                sql: `(SELECT CASE
  WHEN json_array_length(sv) = 0 OR sv IS NULL THEN NULL
  WHEN json_array_length(sv) = 1 THEN json_extract(sv, '$[0]')
  WHEN ${percentileVal} <= 0 THEN json_extract(sv, '$[0]')
  WHEN ${percentileVal} >= 1 THEN json_extract(sv, '$[' || (json_array_length(sv) - 1) || ']')
  ELSE (
    SELECT json_extract(sv, '$[' || li || ']') + 
           (p - li) * (json_extract(sv, '$[' || ui || ']') - json_extract(sv, '$[' || li || ']'))
    FROM (
      SELECT 
        ${percentileVal} * (json_array_length(sv) - 1) as p,
        CAST(${percentileVal} * (json_array_length(sv) - 1) AS INTEGER) as li,
        MIN(CAST(${percentileVal} * (json_array_length(sv) - 1) AS INTEGER) + 1, json_array_length(sv) - 1) as ui
    )
  )
END FROM (SELECT json_group_array(${valueExpr}) as sv))`,
                tables,
                params,
              };
            }
          }
          throw new Error(`${expr.functionName} requires two arguments: value and percentile`);
        }
        
        // COLLECT: gather values into an array using SQLite's json_group_array
        if (expr.functionName === "COLLECT") {
          if (expr.args && expr.args.length > 0) {
            const arg = expr.args[0];
            
            // For DISTINCT, SQLite doesn't support json_group_array(DISTINCT ...)
            // We use json() to parse a JSON array string built from GROUP_CONCAT(DISTINCT ...)
            const useDistinct = expr.distinct === true;
            
            if (arg.type === "property") {
              const varInfo = this.ctx.variables.get(arg.variable!);
              if (!varInfo) {
                throw new Error(`Unknown variable: ${arg.variable}`);
              }
              tables.push(varInfo.alias);
              
              if (useDistinct) {
                // Build JSON array from GROUP_CONCAT(DISTINCT ...)
                // The trick: '[' || GROUP_CONCAT(DISTINCT json_quote(value)) || ']'
                // json_quote properly escapes strings for JSON
                const extractExpr = `json_extract(${varInfo.alias}.properties, '$.${arg.property}')`;
                return {
                  sql: `json('[' || GROUP_CONCAT(DISTINCT json_quote(${extractExpr})) || ']')`,
                  tables,
                  params,
                };
              }
              
              return {
                sql: `json_group_array(json_extract(${varInfo.alias}.properties, '$.${arg.property}'))`,
                tables,
                params,
              };
            } else if (arg.type === "variable") {
              // Check if this is an UNWIND variable (scalar values from json_each)
              const unwindClauses = (this.ctx as any).unwindClauses as Array<{
                alias: string;
                variable: string;
                jsonExpr: string;
                params: unknown[];
              }> | undefined;
              
              if (unwindClauses) {
                const unwindClause = unwindClauses.find(u => u.variable === arg.variable);
                if (unwindClause) {
                  tables.push(unwindClause.alias);
                  // For UNWIND variables, collect the raw values from json_each
                  if (useDistinct) {
                    return {
                      sql: `json('[' || GROUP_CONCAT(DISTINCT json_quote(${unwindClause.alias}.value)) || ']')`,
                      tables,
                      params,
                    };
                  }
                  return {
                    sql: `json_group_array(${unwindClause.alias}.value)`,
                    tables,
                    params,
                  };
                }
              }
              
              const varInfo = this.ctx.variables.get(arg.variable!);
              if (!varInfo) {
                throw new Error(`Unknown variable: ${arg.variable}`);
              }
              tables.push(varInfo.alias);
              // For full variable, collect as JSON objects (DISTINCT on objects is complex, skip for now)
              return {
                sql: `json_group_array(json_object('id', ${varInfo.alias}.id, 'label', ${varInfo.alias}.label, 'properties', ${varInfo.alias}.properties))`,
                tables,
                params,
              };
            } else if (arg.type === "object") {
              // COLLECT with object literal: collect({key: expr, ...})
              const objResult = this.translateObjectLiteral(arg);
              tables.push(...objResult.tables);
              params.push(...objResult.params);
              return {
                sql: `json_group_array(${objResult.sql})`,
                tables,
                params,
              };
            }
          }
          throw new Error(`COLLECT requires a property, variable, or object argument`);
        }

        // ============================================================================
        // Path functions
        // ============================================================================

        // LENGTH: return the number of relationships in a path
        if (expr.functionName === "LENGTH") {
          if (expr.args && expr.args.length > 0) {
            const arg = expr.args[0];
            if (arg.type === "variable") {
              const varInfo = this.ctx.variables.get(arg.variable!);
              if (!varInfo) {
                throw new Error(`Unknown variable: ${arg.variable}`);
              }
              
              if (varInfo.type === "path") {
                const pathExpressions = (this.ctx as any).pathExpressions as Array<{
                  variable: string;
                  edgeAliases: string[];
                  isVariableLength?: boolean;
                  pathCteName?: string;
                }> | undefined;
                
                if (pathExpressions) {
                  const pathInfo = pathExpressions.find(p => p.variable === arg.variable);
                  if (pathInfo) {
                    // For variable-length paths, use the CTE's depth column
                    if (pathInfo.isVariableLength && pathInfo.pathCteName) {
                      return { sql: `${pathInfo.pathCteName}.depth`, tables, params };
                    }
                    // For fixed-length paths, return static length
                    return { sql: `${pathInfo.edgeAliases.length}`, tables, params };
                  }
                }
              }
            }
          }
          throw new Error("LENGTH requires a path variable argument");
        }

        // NODES: return array of nodes in a path
        if (expr.functionName === "NODES") {
          if (expr.args && expr.args.length > 0) {
            const arg = expr.args[0];
            if (arg.type === "variable") {
              const varInfo = this.ctx.variables.get(arg.variable!);
              if (!varInfo) {
                throw new Error(`Unknown variable: ${arg.variable}`);
              }
              
              if (varInfo.type === "path") {
                const pathExpressions = (this.ctx as any).pathExpressions as Array<{
                  variable: string;
                  nodeAliases: string[];
                }> | undefined;
                
                if (pathExpressions) {
                  const pathInfo = pathExpressions.find(p => p.variable === arg.variable);
                  if (pathInfo) {
                    tables.push(...pathInfo.nodeAliases);
                    
                    // Return array of node objects
                    const nodesJson = pathInfo.nodeAliases.map(alias => 
                      `json_object('id', ${alias}.id, 'label', ${alias}.label, 'properties', ${alias}.properties)`
                    ).join(', ');
                    
                    return { sql: `json_array(${nodesJson})`, tables, params };
                  }
                }
              }
            }
          }
          throw new Error("NODES requires a path variable argument");
        }

        // RELATIONSHIPS: return array of relationships in a path
        if (expr.functionName === "RELATIONSHIPS") {
          if (expr.args && expr.args.length > 0) {
            const arg = expr.args[0];
            if (arg.type === "variable") {
              const varInfo = this.ctx.variables.get(arg.variable!);
              if (!varInfo) {
                throw new Error(`Unknown variable: ${arg.variable}`);
              }
              
              if (varInfo.type === "path") {
                const pathExpressions = (this.ctx as any).pathExpressions as Array<{
                  variable: string;
                  edgeAliases: string[];
                }> | undefined;
                
                if (pathExpressions) {
                  const pathInfo = pathExpressions.find(p => p.variable === arg.variable);
                  if (pathInfo) {
                    tables.push(...pathInfo.edgeAliases);
                    
                    // Return array of relationship objects
                    const edgesJson = pathInfo.edgeAliases.map(alias =>
                      `json_object('id', ${alias}.id, 'type', ${alias}.type, 'source_id', ${alias}.source_id, 'target_id', ${alias}.target_id, 'properties', ${alias}.properties)`
                    ).join(', ');
                    
                    return { sql: `json_array(${edgesJson})`, tables, params };
                  }
                }
              }
            }
          }
          throw new Error("RELATIONSHIPS requires a path variable argument");
        }

        // ============================================================================
        // String functions
        // ============================================================================

        // TOUPPER: convert string to uppercase
        if (expr.functionName === "TOUPPER") {
          if (expr.args && expr.args.length > 0) {
            const argResult = this.translateFunctionArg(expr.args[0]);
            tables.push(...argResult.tables);
            params.push(...argResult.params);
            return { sql: `UPPER(${argResult.sql})`, tables, params };
          }
          throw new Error("toUpper requires an argument");
        }

        // TOLOWER: convert string to lowercase
        if (expr.functionName === "TOLOWER") {
          if (expr.args && expr.args.length > 0) {
            const argResult = this.translateFunctionArg(expr.args[0]);
            tables.push(...argResult.tables);
            params.push(...argResult.params);
            return { sql: `LOWER(${argResult.sql})`, tables, params };
          }
          throw new Error("toLower requires an argument");
        }

        // TRIM: remove leading/trailing whitespace
        if (expr.functionName === "TRIM") {
          if (expr.args && expr.args.length > 0) {
            const argResult = this.translateFunctionArg(expr.args[0]);
            tables.push(...argResult.tables);
            params.push(...argResult.params);
            return { sql: `TRIM(${argResult.sql})`, tables, params };
          }
          throw new Error("trim requires an argument");
        }

        // SUBSTRING: extract substring (Cypher uses 0-based indexing, SQLite uses 1-based)
        if (expr.functionName === "SUBSTRING") {
          if (expr.args && expr.args.length >= 2) {
            const strResult = this.translateFunctionArg(expr.args[0]);
            const startResult = this.translateFunctionArg(expr.args[1]);
            tables.push(...strResult.tables, ...startResult.tables);
            params.push(...strResult.params, ...startResult.params);
            
            if (expr.args.length >= 3) {
              const lenResult = this.translateFunctionArg(expr.args[2]);
              tables.push(...lenResult.tables);
              params.push(...lenResult.params);
              // Cypher uses 0-based, SQLite uses 1-based indexing
              return { sql: `SUBSTR(${strResult.sql}, ${startResult.sql} + 1, ${lenResult.sql})`, tables, params };
            }
            return { sql: `SUBSTR(${strResult.sql}, ${startResult.sql} + 1)`, tables, params };
          }
          throw new Error("substring requires at least 2 arguments");
        }

        // REPLACE: replace occurrences of a string
        if (expr.functionName === "REPLACE") {
          if (expr.args && expr.args.length === 3) {
            const strResult = this.translateFunctionArg(expr.args[0]);
            const fromResult = this.translateFunctionArg(expr.args[1]);
            const toResult = this.translateFunctionArg(expr.args[2]);
            tables.push(...strResult.tables, ...fromResult.tables, ...toResult.tables);
            params.push(...strResult.params, ...fromResult.params, ...toResult.params);
            return { sql: `REPLACE(${strResult.sql}, ${fromResult.sql}, ${toResult.sql})`, tables, params };
          }
          throw new Error("replace requires 3 arguments");
        }

        // TOSTRING: convert value to string
        if (expr.functionName === "TOSTRING") {
          if (expr.args && expr.args.length > 0) {
            const arg = expr.args[0];
            // Special case: if the argument is a boolean literal, return 'true' or 'false' directly
            if (arg.type === "literal" && typeof arg.value === "boolean") {
              return { 
                sql: arg.value ? "'true'" : "'false'", 
                tables, 
                params 
              };
            }
            const argResult = this.translateFunctionArg(arg);
            tables.push(...argResult.tables);
            params.push(...argResult.params);
            // Simple CAST to TEXT - let SQLite handle the conversion
            return { 
              sql: `CAST(${argResult.sql} AS TEXT)`, 
              tables, 
              params 
            };
          }
          throw new Error("toString requires an argument");
        }

        // ============================================================================
        // Type conversion functions
        // ============================================================================

        // TOINTEGER: convert value to integer
        if (expr.functionName === "TOINTEGER") {
          if (expr.args && expr.args.length > 0) {
            const argResult = this.translateFunctionArg(expr.args[0]);
            tables.push(...argResult.tables);
            params.push(...argResult.params);
            // Use SQLite's NULLIF + CAST pattern to return NULL for invalid conversions
            // SQLite returns 0 for invalid string casts, so we use a subquery to handle this
            // For simple cases, CAST works; for complex cases we use IIF (SQLite 3.32+)
            return { 
              sql: `(SELECT CASE WHEN v IS NULL THEN NULL WHEN typeof(v) IN ('integer', 'real') THEN CAST(v AS INTEGER) WHEN typeof(v) = 'text' AND v GLOB '[+-][0-9]*' THEN CAST(CAST(v AS REAL) AS INTEGER) WHEN typeof(v) = 'text' AND v GLOB '[0-9]*' THEN CAST(CAST(v AS REAL) AS INTEGER) ELSE NULL END FROM (SELECT ${argResult.sql} AS v))`, 
              tables, 
              params 
            };
          }
          throw new Error("toInteger requires an argument");
        }

        // TOFLOAT: convert value to float
        if (expr.functionName === "TOFLOAT") {
          if (expr.args && expr.args.length > 0) {
            const argResult = this.translateFunctionArg(expr.args[0]);
            tables.push(...argResult.tables);
            params.push(...argResult.params);
            // Use subquery to evaluate argument once
            return { 
              sql: `(SELECT CASE WHEN v IS NULL THEN NULL WHEN typeof(v) IN ('integer', 'real') THEN CAST(v AS REAL) WHEN typeof(v) = 'text' AND v GLOB '[+-][0-9.]*' THEN CAST(v AS REAL) WHEN typeof(v) = 'text' AND v GLOB '[0-9.]*' THEN CAST(v AS REAL) ELSE NULL END FROM (SELECT ${argResult.sql} AS v))`, 
              tables, 
              params 
            };
          }
          throw new Error("toFloat requires an argument");
        }

        // TOBOOLEAN: convert value to boolean
        if (expr.functionName === "TOBOOLEAN") {
          if (expr.args && expr.args.length > 0) {
            const argResult = this.translateFunctionArg(expr.args[0]);
            tables.push(...argResult.tables);
            params.push(...argResult.params);
            // Convert 'true'/'false' strings (case insensitive) to 1/0
            // Return NULL for invalid values
            // Use subquery to evaluate argument once
            return { 
              sql: `(SELECT CASE WHEN v IS NULL THEN NULL WHEN v = 1 OR LOWER(v) = 'true' THEN 1 WHEN v = 0 OR LOWER(v) = 'false' THEN 0 ELSE NULL END FROM (SELECT ${argResult.sql} AS v))`, 
              tables, 
              params 
            };
          }
          throw new Error("toBoolean requires an argument");
        }

        // ============================================================================
        // Null/scalar functions
        // ============================================================================

        // COALESCE: return first non-null value
        if (expr.functionName === "COALESCE") {
          if (expr.args && expr.args.length > 0) {
            const argResults = expr.args.map(arg => this.translateFunctionArg(arg));
            for (const r of argResults) {
              tables.push(...r.tables);
              params.push(...r.params);
            }
            const argsSql = argResults.map(r => r.sql).join(", ");
            return { sql: `COALESCE(${argsSql})`, tables, params };
          }
          throw new Error("coalesce requires at least one argument");
        }

        // ============================================================================
        // Math functions
        // ============================================================================

        // ABS: absolute value
        if (expr.functionName === "ABS") {
          if (expr.args && expr.args.length > 0) {
            const argResult = this.translateFunctionArg(expr.args[0]);
            tables.push(...argResult.tables);
            params.push(...argResult.params);
            return { sql: `ABS(${argResult.sql})`, tables, params };
          }
          throw new Error("abs requires an argument");
        }

        // ROUND: round to nearest integer
        if (expr.functionName === "ROUND") {
          if (expr.args && expr.args.length > 0) {
            const argResult = this.translateFunctionArg(expr.args[0]);
            tables.push(...argResult.tables);
            params.push(...argResult.params);
            return { sql: `ROUND(${argResult.sql})`, tables, params };
          }
          throw new Error("round requires an argument");
        }

        // FLOOR: round down to integer
        if (expr.functionName === "FLOOR") {
          if (expr.args && expr.args.length > 0) {
            const argResult = this.translateFunctionArg(expr.args[0]);
            tables.push(...argResult.tables);
            params.push(...argResult.params);
            // SQLite doesn't have FLOOR, use CAST for positive numbers or CASE for proper floor
            return { sql: `CAST(${argResult.sql} AS INTEGER)`, tables, params };
          }
          throw new Error("floor requires an argument");
        }

        // CEIL: round up to integer
        if (expr.functionName === "CEIL") {
          if (expr.args && expr.args.length > 0) {
            const argResult = this.translateFunctionArg(expr.args[0]);
            tables.push(...argResult.tables);
            params.push(...argResult.params);
            // SQLite doesn't have CEIL, simulate with CASE
            return { 
              sql: `CASE WHEN ${argResult.sql} = CAST(${argResult.sql} AS INTEGER) THEN CAST(${argResult.sql} AS INTEGER) ELSE CAST(${argResult.sql} AS INTEGER) + 1 END`, 
              tables, 
              params 
            };
          }
          throw new Error("ceil requires an argument");
        }

        // SQRT: square root
        if (expr.functionName === "SQRT") {
          if (expr.args && expr.args.length > 0) {
            const argResult = this.translateFunctionArg(expr.args[0]);
            tables.push(...argResult.tables);
            params.push(...argResult.params);
            // SQLite doesn't have native SQRT, use pow(x, 0.5) via math extension or custom
            // In standard SQLite we can use: exp(0.5 * ln(x)) but that also requires extension
            // Fall back to placeholder that works if math functions are loaded
            return { sql: `SQRT(${argResult.sql})`, tables, params };
          }
          throw new Error("sqrt requires an argument");
        }

        // RAND: random float between 0 and 1
        if (expr.functionName === "RAND") {
          // SQLite's RANDOM() returns integer between -9223372036854775808 and 9223372036854775807
          // Convert to 0-1 range: (RANDOM() + 9223372036854775808) / 18446744073709551615.0
          return { 
            sql: `((RANDOM() + 9223372036854775808) / 18446744073709551615.0)`, 
            tables, 
            params 
          };
        }

        // ============================================================================
        // List functions
        // ============================================================================

        // SIZE: get length of array
        if (expr.functionName === "SIZE") {
          if (expr.args && expr.args.length > 0) {
            const argResult = this.translateFunctionArg(expr.args[0]);
            tables.push(...argResult.tables);
            params.push(...argResult.params);
            return { sql: `json_array_length(${argResult.sql})`, tables, params };
          }
          throw new Error("size requires an argument");
        }

        // HEAD: get first element of array
        if (expr.functionName === "HEAD") {
          if (expr.args && expr.args.length > 0) {
            const argResult = this.translateFunctionArg(expr.args[0]);
            tables.push(...argResult.tables);
            params.push(...argResult.params);
            return { sql: `json_extract(${argResult.sql}, '$[0]')`, tables, params };
          }
          throw new Error("head requires an argument");
        }

        // LAST: get last element of array
        if (expr.functionName === "LAST") {
          if (expr.args && expr.args.length > 0) {
            const argResult = this.translateFunctionArg(expr.args[0]);
            tables.push(...argResult.tables);
            params.push(...argResult.params);
            // SQLite: json_extract with [json_array_length - 1] or $[#-1] syntax
            return { sql: `json_extract(${argResult.sql}, '$[#-1]')`, tables, params };
          }
          throw new Error("last requires an argument");
        }

        // KEYS: get property keys of a node
        if (expr.functionName === "KEYS") {
          if (expr.args && expr.args.length > 0) {
            const arg = expr.args[0];
            if (arg.type === "variable") {
              const varInfo = this.ctx.variables.get(arg.variable!);
              if (!varInfo) {
                throw new Error(`Unknown variable: ${arg.variable}`);
              }
              tables.push(varInfo.alias);
              // Use json_each to get keys, then aggregate them
              return { 
                sql: `(SELECT json_group_array(key) FROM json_each(${varInfo.alias}.properties))`, 
                tables, 
                params 
              };
            }
          }
          throw new Error("keys requires a variable argument");
        }

        // TAIL: get all but first element of array
        if (expr.functionName === "TAIL") {
          if (expr.args && expr.args.length > 0) {
            const argResult = this.translateFunctionArg(expr.args[0]);
            tables.push(...argResult.tables);
            params.push(...argResult.params);
            // Use json_remove to remove first element ($[0])
            return { sql: `json_remove(${argResult.sql}, '$[0]')`, tables, params };
          }
          throw new Error("tail requires an argument");
        }

        // RANGE: generate a list of numbers
        if (expr.functionName === "RANGE") {
          if (expr.args && expr.args.length >= 2) {
            const startResult = this.translateFunctionArg(expr.args[0]);
            const endResult = this.translateFunctionArg(expr.args[1]);
            params.push(...startResult.params, ...endResult.params);
            
            // Check for optional step parameter
            let stepValue = "1";
            if (expr.args.length >= 3) {
              const stepResult = this.translateFunctionArg(expr.args[2]);
              params.push(...stepResult.params);
              stepValue = stepResult.sql;
            }
            
            // Use recursive CTE to generate range
            // This is a subquery that generates the array
            return { 
              sql: `(WITH RECURSIVE r(n) AS (
  VALUES(${startResult.sql})
  UNION ALL
  SELECT n + ${stepValue} FROM r WHERE n + ${stepValue} <= ${endResult.sql}
) SELECT json_group_array(n) FROM r)`, 
              tables, 
              params 
            };
          }
          throw new Error("range requires at least 2 arguments");
        }

        // SPLIT: split string by delimiter into array
        if (expr.functionName === "SPLIT") {
          if (expr.args && expr.args.length === 2) {
            const strResult = this.translateFunctionArg(expr.args[0]);
            const delimResult = this.translateFunctionArg(expr.args[1]);
            tables.push(...strResult.tables, ...delimResult.tables);
            params.push(...strResult.params, ...delimResult.params);
            // SQLite doesn't have native split, use recursive CTE with instr
            // This creates a JSON array from splitting the string
            return { 
              sql: `(WITH RECURSIVE split(str, rest, pos) AS (
  VALUES('', ${strResult.sql} || ${delimResult.sql}, 0)
  UNION ALL
  SELECT
    CASE WHEN instr(rest, ${delimResult.sql}) > 0 
         THEN substr(rest, 1, instr(rest, ${delimResult.sql}) - 1)
         ELSE rest 
    END,
    CASE WHEN instr(rest, ${delimResult.sql}) > 0 
         THEN substr(rest, instr(rest, ${delimResult.sql}) + length(${delimResult.sql}))
         ELSE '' 
    END,
    pos + 1
  FROM split WHERE rest != ''
) SELECT json_group_array(str) FROM split WHERE pos > 0)`, 
              tables, 
              params 
            };
          }
          throw new Error("split requires 2 arguments");
        }

        // ============================================================================
        // Node/Relationship functions
        // ============================================================================

        // LABELS: get node labels (returns array with single label)
        if (expr.functionName === "LABELS") {
          if (expr.args && expr.args.length > 0) {
            const arg = expr.args[0];
            if (arg.type === "variable") {
              const varInfo = this.ctx.variables.get(arg.variable!);
              if (!varInfo) {
                throw new Error(`Unknown variable: ${arg.variable}`);
              }
              if (varInfo.type !== "node") {
                throw new Error("labels() requires a node variable");
              }
              tables.push(varInfo.alias);
              // Return a JSON array containing the single label
              return { 
                sql: `json_array(${varInfo.alias}.label)`, 
                tables, 
                params 
              };
            }
          }
          throw new Error("labels requires a node variable argument");
        }

        // TYPE: get relationship type
        if (expr.functionName === "TYPE") {
          if (expr.args && expr.args.length > 0) {
            const arg = expr.args[0];
            if (arg.type === "variable") {
              const varInfo = this.ctx.variables.get(arg.variable!);
              if (!varInfo) {
                throw new Error(`Unknown variable: ${arg.variable}`);
              }
              if (varInfo.type !== "edge") {
                throw new Error("type() requires a relationship variable");
              }
              tables.push(varInfo.alias);
              // Return the type column from edges table
              return { 
                sql: `${varInfo.alias}.type`, 
                tables, 
                params 
              };
            }
          }
          throw new Error("type requires a relationship variable argument");
        }

        // PROPERTIES: get all properties as a map
        if (expr.functionName === "PROPERTIES") {
          if (expr.args && expr.args.length > 0) {
            const arg = expr.args[0];
            if (arg.type === "variable") {
              const varInfo = this.ctx.variables.get(arg.variable!);
              if (!varInfo) {
                throw new Error(`Unknown variable: ${arg.variable}`);
              }
              tables.push(varInfo.alias);
              // Return the properties JSON column directly
              return { 
                sql: `${varInfo.alias}.properties`, 
                tables, 
                params 
              };
            }
          }
          throw new Error("properties requires a variable argument");
        }

        // ============================================================================
        // Date/Time functions
        // ============================================================================

        // DATE: get current date or parse date string
        if (expr.functionName === "DATE") {
          if (expr.args && expr.args.length > 0) {
            // date('2024-01-15') - parse date string
            const argResult = this.translateFunctionArg(expr.args[0]);
            tables.push(...argResult.tables);
            params.push(...argResult.params);
            return { sql: `DATE(${argResult.sql})`, tables, params };
          }
          // date() - current date
          return { sql: `DATE('now')`, tables, params };
        }

        // DATETIME: get current datetime or parse datetime string
        if (expr.functionName === "DATETIME") {
          if (expr.args && expr.args.length > 0) {
            // datetime('2024-01-15T12:30:00') - parse datetime string
            const argResult = this.translateFunctionArg(expr.args[0]);
            tables.push(...argResult.tables);
            params.push(...argResult.params);
            return { sql: `DATETIME(${argResult.sql})`, tables, params };
          }
          // datetime() - current datetime
          return { sql: `DATETIME('now')`, tables, params };
        }

        // TIMESTAMP: get unix timestamp in milliseconds
        if (expr.functionName === "TIMESTAMP") {
          // Cypher returns milliseconds since epoch
          // SQLite: strftime('%s', 'now') returns seconds, multiply by 1000
          return { sql: `(CAST(strftime('%s', 'now') AS INTEGER) * 1000)`, tables, params };
        }

        // ============================================================================
        // Extended string functions
        // ============================================================================

        // LEFT: get leftmost N characters
        if (expr.functionName === "LEFT") {
          if (expr.args && expr.args.length === 2) {
            const strResult = this.translateFunctionArg(expr.args[0]);
            const lenResult = this.translateFunctionArg(expr.args[1]);
            tables.push(...strResult.tables, ...lenResult.tables);
            // SQLite: SUBSTR(string, 1, length) returns leftmost N chars
            // Handle null: CASE WHEN str IS NULL THEN NULL ELSE SUBSTR(...) END
            // Need to duplicate params since strResult.sql appears twice
            params.push(...strResult.params, ...strResult.params, ...lenResult.params);
            return { 
              sql: `CASE WHEN ${strResult.sql} IS NULL THEN NULL ELSE SUBSTR(${strResult.sql}, 1, ${lenResult.sql}) END`, 
              tables, 
              params 
            };
          }
          throw new Error("left requires 2 arguments");
        }

        // RIGHT: get rightmost N characters
        if (expr.functionName === "RIGHT") {
          if (expr.args && expr.args.length === 2) {
            const strResult = this.translateFunctionArg(expr.args[0]);
            const lenResult = this.translateFunctionArg(expr.args[1]);
            tables.push(...strResult.tables, ...lenResult.tables);
            // SQLite: SUBSTR(string, -length) returns rightmost N chars
            // But this doesn't handle edge cases:
            // - length = 0 should return ''
            // - length > string length should return whole string
            // strResult.sql appears 4 times, lenResult.sql appears 4 times
            params.push(
              ...strResult.params,  // for IS NULL check
              ...lenResult.params,  // for = 0 check
              ...lenResult.params,  // for >= LENGTH check
              ...strResult.params,  // for LENGTH()
              ...strResult.params,  // for THEN branch
              ...strResult.params,  // for ELSE SUBSTR
              ...lenResult.params   // for -length
            );
            return { 
              sql: `CASE WHEN ${strResult.sql} IS NULL THEN NULL WHEN ${lenResult.sql} = 0 THEN '' WHEN ${lenResult.sql} >= LENGTH(${strResult.sql}) THEN ${strResult.sql} ELSE SUBSTR(${strResult.sql}, -${lenResult.sql}) END`, 
              tables, 
              params 
            };
          }
          throw new Error("right requires 2 arguments");
        }

        // LTRIM: remove leading whitespace
        if (expr.functionName === "LTRIM") {
          if (expr.args && expr.args.length > 0) {
            const argResult = this.translateFunctionArg(expr.args[0]);
            tables.push(...argResult.tables);
            params.push(...argResult.params);
            return { sql: `LTRIM(${argResult.sql})`, tables, params };
          }
          throw new Error("ltrim requires an argument");
        }

        // RTRIM: remove trailing whitespace
        if (expr.functionName === "RTRIM") {
          if (expr.args && expr.args.length > 0) {
            const argResult = this.translateFunctionArg(expr.args[0]);
            tables.push(...argResult.tables);
            params.push(...argResult.params);
            return { sql: `RTRIM(${argResult.sql})`, tables, params };
          }
          throw new Error("rtrim requires an argument");
        }

        // REVERSE: reverse a string
        if (expr.functionName === "REVERSE") {
          if (expr.args && expr.args.length > 0) {
            const argResult = this.translateFunctionArg(expr.args[0]);
            tables.push(...argResult.tables);
            // argResult.sql appears 2 times
            params.push(...argResult.params, ...argResult.params);
            // SQLite doesn't have native REVERSE, use recursive CTE
            // Pattern: WITH RECURSIVE r AS (SELECT '', str UNION ALL SELECT SUBSTR(rest,1,1)||acc, SUBSTR(rest,2) FROM r WHERE rest<>'') SELECT acc
            return { 
              sql: `(SELECT CASE WHEN ${argResult.sql} IS NULL THEN NULL ELSE (WITH RECURSIVE rev(acc, rest) AS (VALUES('', ${argResult.sql}) UNION ALL SELECT SUBSTR(rest, 1, 1) || acc, SUBSTR(rest, 2) FROM rev WHERE rest <> '') SELECT acc FROM rev WHERE rest = '') END)`, 
              tables, 
              params 
            };
          }
          throw new Error("reverse requires an argument");
        }

        throw new Error(`Unknown function: ${expr.functionName}`);
      }

      case "literal": {
        // Handle array literals specially - use json_array()
        if (Array.isArray(expr.value)) {
          return this.translateArrayLiteral(expr.value);
        }
        // Convert booleans to 1/0 for SQLite
        const value = expr.value === true ? 1 : expr.value === false ? 0 : expr.value;
        params.push(value);
        return { sql: "?", tables, params };
      }

      case "parameter": {
        params.push(this.ctx.paramValues[expr.name!]);
        return { sql: "?", tables, params };
      }

      case "case": {
        return this.translateCaseExpression(expr);
      }

      case "binary": {
        return this.translateBinaryExpression(expr);
      }

      case "object": {
        return this.translateObjectLiteral(expr);
      }

      case "comparison": {
        return this.translateComparisonExpression(expr);
      }

      case "listComprehension": {
        return this.translateListComprehension(expr);
      }

      case "listPredicate": {
        return this.translateListPredicate(expr);
      }

      case "unary": {
        return this.translateUnaryExpression(expr);
      }

      default:
        throw new Error(`Unknown expression type: ${expr.type}`);
    }
  }

  private translateCaseExpression(expr: Expression): { sql: string; tables: string[]; params: unknown[] } {
    const tables: string[] = [];
    const params: unknown[] = [];
    
    let sql = "CASE";
    
    // Process each WHEN clause
    for (const when of expr.whens || []) {
      // Translate the condition
      const { sql: condSql, params: condParams } = this.translateWhere(when.condition);
      params.push(...condParams);
      
      // Translate the result expression
      const { sql: resultSql, tables: resultTables, params: resultParams } = this.translateExpression(when.result);
      tables.push(...resultTables);
      params.push(...resultParams);
      
      sql += ` WHEN ${condSql} THEN ${resultSql}`;
    }
    
    // Add ELSE clause if present
    if (expr.elseExpr) {
      const { sql: elseSql, tables: elseTables, params: elseParams } = this.translateExpression(expr.elseExpr);
      tables.push(...elseTables);
      params.push(...elseParams);
      sql += ` ELSE ${elseSql}`;
    }
    
    sql += " END";
    
    return { sql, tables, params };
  }

  private translateBinaryExpression(expr: Expression): { sql: string; tables: string[]; params: unknown[] } {
    const tables: string[] = [];
    const params: unknown[] = [];

    const leftResult = this.translateExpression(expr.left!);
    const rightResult = this.translateExpression(expr.right!);

    tables.push(...leftResult.tables, ...rightResult.tables);
    params.push(...leftResult.params, ...rightResult.params);

    // Check if this is list concatenation (+ operator with arrays)
    if (expr.operator === "+" && this.isListExpression(expr.left!) && this.isListExpression(expr.right!)) {
      // Use JSON function to concatenate arrays
      // Pattern: (SELECT json_group_array(value) FROM (SELECT value FROM json_each(left) UNION ALL SELECT value FROM json_each(right)))
      const leftArraySql = this.wrapForArray(expr.left!, leftResult.sql);
      const rightArraySql = this.wrapForArray(expr.right!, rightResult.sql);
      
      return {
        sql: `(SELECT json_group_array(value) FROM (SELECT value FROM json_each(${leftArraySql}) UNION ALL SELECT value FROM json_each(${rightArraySql})))`,
        tables,
        params,
      };
    }

    // Handle logical operators (AND, OR)
    if (expr.operator === "AND" || expr.operator === "OR") {
      return {
        sql: `(${leftResult.sql} ${expr.operator} ${rightResult.sql})`,
        tables,
        params,
      };
    }

    // For property access in arithmetic, we need to use json_extract to get the numeric value
    const leftSql = this.wrapForArithmetic(expr.left!, leftResult.sql);
    const rightSql = this.wrapForArithmetic(expr.right!, rightResult.sql);

    return {
      sql: `(${leftSql} ${expr.operator} ${rightSql})`,
      tables,
      params,
    };
  }

  private isListExpression(expr: Expression): boolean {
    // Check if expression is likely a list/array type
    if (expr.type === "literal" && Array.isArray(expr.value)) {
      return true;
    }
    if (expr.type === "property") {
      // Properties that access array fields - we assume it could be a list
      // In a full implementation, you'd track types, but for now assume + with property could be list concat
      return true;
    }
    if (expr.type === "binary" && expr.operator === "+") {
      // Nested binary + could be chained list concatenation
      return this.isListExpression(expr.left!) || this.isListExpression(expr.right!);
    }
    if (expr.type === "function") {
      // List-returning functions like collect(), range(), etc.
      const listFunctions = ["COLLECT", "RANGE", "KEYS", "LABELS", "SPLIT", "TAIL"];
      return listFunctions.includes(expr.functionName || "");
    }
    return false;
  }

  private wrapForArray(expr: Expression, sql: string): string {
    // For property access, use json_extract to get the JSON array
    if (expr.type === "property") {
      const varInfo = this.ctx.variables.get(expr.variable!);
      if (varInfo) {
        return `json_extract(${varInfo.alias}.properties, '$.${expr.property}')`;
      }
    }
    return sql;
  }

  private wrapForArithmetic(expr: Expression, sql: string): string {
    // For property access, the -> operator returns JSON, we need to extract as a number
    if (expr.type === "property") {
      // Replace -> with json_extract for numeric operations
      const varInfo = this.ctx.variables.get(expr.variable!);
      if (varInfo) {
        return `json_extract(${varInfo.alias}.properties, '$.${expr.property}')`;
      }
    }
    return sql;
  }

  private translateComparisonExpression(expr: Expression): { sql: string; tables: string[]; params: unknown[] } {
    const tables: string[] = [];
    const params: unknown[] = [];

    const leftResult = this.translateExpression(expr.left!);
    const rightResult = this.translateExpression(expr.right!);

    tables.push(...leftResult.tables, ...rightResult.tables);
    params.push(...leftResult.params, ...rightResult.params);

    // For property access in comparisons, use json_extract for proper comparison
    const leftSql = this.wrapForComparison(expr.left!, leftResult.sql);
    const rightSql = this.wrapForComparison(expr.right!, rightResult.sql);

    return {
      sql: `(${leftSql} ${expr.comparisonOperator} ${rightSql})`,
      tables,
      params,
    };
  }

  private wrapForComparison(expr: Expression, sql: string): string {
    // For property access, use json_extract to get proper value for comparison
    if (expr.type === "property") {
      const varInfo = this.ctx.variables.get(expr.variable!);
      if (varInfo) {
        return `json_extract(${varInfo.alias}.properties, '$.${expr.property}')`;
      }
    }
    return sql;
  }

  private translateObjectLiteral(expr: Expression): { sql: string; tables: string[]; params: unknown[] } {
    const tables: string[] = [];
    const params: unknown[] = [];
    
    // Build json_object() call with key-value pairs
    const keyValuePairs: string[] = [];
    
    if (expr.properties) {
      for (const prop of expr.properties) {
        // Add key as a string literal
        params.push(prop.key);
        
        // Translate the value expression
        const valueResult = this.translateExpression(prop.value);
        tables.push(...valueResult.tables);
        params.push(...valueResult.params);
        
        keyValuePairs.push(`?, ${valueResult.sql}`);
      }
    }
    
    return {
      sql: `json_object(${keyValuePairs.join(", ")})`,
      tables,
      params,
    };
  }

  private translateArrayLiteral(values: PropertyValue[]): { sql: string; tables: string[]; params: unknown[] } {
    const tables: string[] = [];
    const params: unknown[] = [];
    
    if (values.length === 0) {
      return { sql: "json_array()", tables, params };
    }
    
    const valueParts: string[] = [];
    for (const value of values) {
      if (typeof value === "string" || typeof value === "number") {
        params.push(value);
        valueParts.push("?");
      } else if (typeof value === "boolean") {
        params.push(value ? 1 : 0);
        valueParts.push("?");
      } else if (value === null) {
        valueParts.push("NULL");
      } else if (Array.isArray(value)) {
        // Nested array
        const nested = this.translateArrayLiteral(value);
        tables.push(...nested.tables);
        params.push(...nested.params);
        valueParts.push(nested.sql);
      } else if (this.isParameterRef(value)) {
        params.push(this.ctx.paramValues[value.name]);
        valueParts.push("?");
      } else {
        // For other objects, serialize as JSON
        params.push(JSON.stringify(value));
        valueParts.push("?");
      }
    }
    
    return {
      sql: `json_array(${valueParts.join(", ")})`,
      tables,
      params,
    };
  }

  /**
   * Translate a list comprehension expression.
   * Syntax: [variable IN listExpr WHERE filterCondition | mapExpr]
   * 
   * Translates to SQLite using json_each and json_group_array:
   * (SELECT json_group_array(value_or_mapped) FROM json_each(listExpr) WHERE filter)
   */
  private translateListComprehension(expr: Expression): { sql: string; tables: string[]; params: unknown[] } {
    const tables: string[] = [];
    const params: unknown[] = [];
    
    const variable = expr.variable!;
    const listExpr = expr.listExpr!;
    const filterCondition = expr.filterCondition;
    const mapExpr = expr.mapExpr;
    
    // Translate the source list expression
    const listResult = this.translateExpression(listExpr);
    tables.push(...listResult.tables);
    
    // Wrap the source expression for json_each
    let sourceExpr = listResult.sql;
    if (listExpr.type === "property") {
      // For property access, use json_extract 
      const varInfo = this.ctx.variables.get(listExpr.variable!);
      if (varInfo) {
        sourceExpr = `json_extract(${varInfo.alias}.properties, '$.${listExpr.property}')`;
      }
    }
    
    // Determine what to select: the mapped expression or just the value
    let selectExpr = `__lc__.value`;
    let mapParams: unknown[] = [];
    if (mapExpr) {
      const mapResult = this.translateListComprehensionExpr(mapExpr, variable, "__lc__");
      mapParams = mapResult.params;
      selectExpr = mapResult.sql;
    }
    
    // Build the WHERE clause if filter is present
    let whereClause = "";
    let filterParams: unknown[] = [];
    if (filterCondition) {
      const filterResult = this.translateListComprehensionCondition(filterCondition, variable, "__lc__");
      filterParams = filterResult.params;
      whereClause = ` WHERE ${filterResult.sql}`;
    }
    
    // Build the final SQL using json_group_array
    const sql = `(SELECT json_group_array(${selectExpr}) FROM json_each(${sourceExpr}) AS __lc__${whereClause})`;
    
    // Params must match SQL order: selectExpr params, then source params, then filter params
    params.push(...mapParams, ...listResult.params, ...filterParams);
    
    return { sql, tables, params };
  }

  /**
   * Translate an expression within a list comprehension, replacing
   * references to the comprehension variable with the json_each value column.
   */
  private translateListComprehensionExpr(
    expr: Expression,
    compVar: string,
    tableAlias: string
  ): { sql: string; params: unknown[] } {
    const params: unknown[] = [];
    
    switch (expr.type) {
      case "variable":
        if (expr.variable === compVar) {
          return { sql: `${tableAlias}.value`, params };
        }
        // Fall through to regular translation
        const varResult = this.translateExpression(expr);
        return { sql: varResult.sql, params: varResult.params };
        
      case "binary": {
        const left = this.translateListComprehensionExpr(expr.left!, compVar, tableAlias);
        const right = this.translateListComprehensionExpr(expr.right!, compVar, tableAlias);
        params.push(...left.params, ...right.params);
        return { sql: `(${left.sql} ${expr.operator} ${right.sql})`, params };
      }
      
      case "literal":
        if (expr.value === null) {
          return { sql: "NULL", params };
        }
        params.push(expr.value);
        return { sql: "?", params };
        
      case "parameter":
        params.push(this.ctx.paramValues[expr.name!]);
        return { sql: "?", params };
        
      case "function": {
        // Handle functions like size(x)
        const funcArgs: string[] = [];
        for (const arg of expr.args || []) {
          const argResult = this.translateListComprehensionExpr(arg, compVar, tableAlias);
          params.push(...argResult.params);
          funcArgs.push(argResult.sql);
        }
        
        // Map Cypher functions to SQLite equivalents
        const funcName = expr.functionName!;
        if (funcName === "SIZE" || funcName === "LENGTH") {
          return { sql: `LENGTH(${funcArgs[0]})`, params };
        }
        if (funcName === "TOUPPER" || funcName === "UPPER") {
          return { sql: `UPPER(${funcArgs[0]})`, params };
        }
        if (funcName === "TOLOWER" || funcName === "LOWER") {
          return { sql: `LOWER(${funcArgs[0]})`, params };
        }
        if (funcName === "ABS") {
          return { sql: `ABS(${funcArgs[0]})`, params };
        }
        
        return { sql: `${funcName}(${funcArgs.join(", ")})`, params };
      }
      
      default:
        // Fall back to regular translation
        const result = this.translateExpression(expr);
        return { sql: result.sql, params: result.params };
    }
  }

  /**
   * Translate a WHERE condition within a list comprehension.
   */
  private translateListComprehensionCondition(
    condition: WhereCondition,
    compVar: string,
    tableAlias: string
  ): { sql: string; params: unknown[] } {
    const params: unknown[] = [];
    
    switch (condition.type) {
      case "comparison": {
        const left = this.translateListComprehensionExpr(condition.left!, compVar, tableAlias);
        const right = this.translateListComprehensionExpr(condition.right!, compVar, tableAlias);
        params.push(...left.params, ...right.params);
        return {
          sql: `${left.sql} ${condition.operator} ${right.sql}`,
          params,
        };
      }
      
      case "and": {
        const parts = condition.conditions!.map(c => 
          this.translateListComprehensionCondition(c, compVar, tableAlias)
        );
        return {
          sql: `(${parts.map(p => p.sql).join(" AND ")})`,
          params: parts.flatMap(p => p.params),
        };
      }
      
      case "or": {
        const parts = condition.conditions!.map(c => 
          this.translateListComprehensionCondition(c, compVar, tableAlias)
        );
        return {
          sql: `(${parts.map(p => p.sql).join(" OR ")})`,
          params: parts.flatMap(p => p.params),
        };
      }
      
      case "not": {
        const inner = this.translateListComprehensionCondition(condition.condition!, compVar, tableAlias);
        return {
          sql: `NOT (${inner.sql})`,
          params: inner.params,
        };
      }
      
      default:
        throw new Error(`Unsupported condition type in list comprehension: ${condition.type}`);
    }
  }

  /**
   * Translate a list predicate expression: ALL/ANY/NONE/SINGLE(var IN list WHERE cond)
   * 
   * Implementation uses a CTE to evaluate the list once and avoid parameter duplication issues:
   * - ALL: true when count of elements NOT satisfying condition = 0 (empty list = true)
   * - ANY: true when count of elements satisfying condition > 0 (empty list = false)  
   * - NONE: true when count of elements satisfying condition = 0 (empty list = true)
   * - SINGLE: true when count of elements satisfying condition = 1 (empty list = false)
   */
  private translateListPredicate(expr: Expression): { sql: string; tables: string[]; params: unknown[] } {
    const tables: string[] = [];
    const params: unknown[] = [];
    
    const predicateType = expr.predicateType!;
    const variable = expr.variable!;
    const listExpr = expr.listExpr!;
    const filterCondition = expr.filterCondition!;
    
    // Translate the source list expression
    const listResult = this.translateExpression(listExpr);
    tables.push(...listResult.tables);
    params.push(...listResult.params);
    
    // Get the list SQL - wrap for array if needed
    const listSql = this.wrapForListPredicate(listExpr, listResult.sql);
    
    // Translate the filter condition, substituting the list predicate variable with __lp__.value
    const condResult = this.translateListComprehensionCondition(filterCondition, variable, "__lp__");
    params.push(...condResult.params);
    
    let sql: string;
    
    switch (predicateType) {
      case "ALL":
        // ALL: true when no elements violate the condition
        // For empty list, ALL is vacuously true
        // Use a single subquery that counts elements not satisfying condition
        // If list is empty, count is 0, which equals 0, so result is true
        sql = `((SELECT COUNT(*) FROM json_each(${listSql}) AS __lp__ WHERE NOT (${condResult.sql})) = 0)`;
        break;
        
      case "ANY":
        // ANY: true when at least one element satisfies the condition
        // For empty list, ANY is false
        sql = `(EXISTS (SELECT 1 FROM json_each(${listSql}) AS __lp__ WHERE ${condResult.sql}))`;
        break;
        
      case "NONE":
        // NONE: true when no elements satisfy the condition
        // For empty list, NONE is true
        sql = `(NOT EXISTS (SELECT 1 FROM json_each(${listSql}) AS __lp__ WHERE ${condResult.sql}))`;
        break;
        
      case "SINGLE":
        // SINGLE: true when exactly one element satisfies the condition
        // For empty list, SINGLE is false
        sql = `((SELECT COUNT(*) FROM json_each(${listSql}) AS __lp__ WHERE ${condResult.sql}) = 1)`;
        break;
        
      default:
        throw new Error(`Unknown list predicate type: ${predicateType}`);
    }
    
    return { sql, tables, params };
  }

  /**
   * Wrap an expression for use with json_each in list predicates
   */
  private wrapForListPredicate(expr: Expression, sql: string): string {
    // For property access, use json_extract to get the JSON array
    if (expr.type === "property") {
      const varInfo = this.ctx.variables.get(expr.variable!);
      if (varInfo) {
        return `json_extract(${varInfo.alias}.properties, '$.${expr.property}')`;
      }
    }
    // For literal arrays, the sql is already a json_array() call
    // For function calls like range(), the sql is already the function call
    return sql;
  }

  /**
   * Translate a unary expression: NOT expr
   */
  private translateUnaryExpression(expr: Expression): { sql: string; tables: string[]; params: unknown[] } {
    const tables: string[] = [];
    const params: unknown[] = [];
    
    if (expr.operator === "NOT") {
      const operandResult = this.translateExpression(expr.operand!);
      tables.push(...operandResult.tables);
      params.push(...operandResult.params);
      
      return {
        sql: `NOT (${operandResult.sql})`,
        tables,
        params,
      };
    }
    
    throw new Error(`Unknown unary operator: ${expr.operator}`);
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

      case "exists": {
        return this.translateExistsCondition(condition);
      }

      case "in": {
        return this.translateInCondition(condition);
      }

      case "listPredicate": {
        // List predicate in WHERE clause - convert WhereCondition to Expression format
        // and reuse the expression translator
        const expr: Expression = {
          type: "listPredicate",
          predicateType: condition.predicateType,
          variable: condition.variable,
          listExpr: condition.listExpr,
          filterCondition: condition.filterCondition,
        };
        const result = this.translateListPredicate(expr);
        return {
          sql: result.sql,
          params: result.params,
        };
      }

      default:
        throw new Error(`Unknown condition type: ${condition.type}`);
    }
  }

  private translateExistsCondition(condition: WhereCondition): { sql: string; params: unknown[] } {
    const pattern = condition.pattern;
    if (!pattern) {
      throw new Error("EXISTS condition must have a pattern");
    }

    const params: unknown[] = [];
    let sql: string;

    if (this.isRelationshipPattern(pattern)) {
      // EXISTS with relationship pattern: EXISTS((n)-[:TYPE]->(m))
      // Generate: EXISTS (SELECT 1 FROM edges e WHERE e.source_id = n.id AND e.type = ? AND ...)
      const rel = pattern as RelationshipPattern;
      
      // Get the source variable's alias from context
      const sourceVar = rel.source.variable;
      const sourceInfo = sourceVar ? this.ctx.variables.get(sourceVar) : null;
      
      if (!sourceInfo) {
        throw new Error(`EXISTS pattern references unknown variable: ${sourceVar}`);
      }
      
      const edgeAlias = `exists_e${this.ctx.aliasCounter++}`;
      const targetAlias = `exists_n${this.ctx.aliasCounter++}`;
      
      const conditions: string[] = [];
      
      // Connect edge to source node
      if (rel.edge.direction === "left") {
        conditions.push(`${edgeAlias}.target_id = ${sourceInfo.alias}.id`);
      } else {
        conditions.push(`${edgeAlias}.source_id = ${sourceInfo.alias}.id`);
      }
      
      // Filter by edge type if specified
      if (rel.edge.type) {
        conditions.push(`${edgeAlias}.type = ?`);
        params.push(rel.edge.type);
      }
      
      // Check if target has a label - need to join to nodes table
      let fromClause = `edges ${edgeAlias}`;
      if (rel.target.label) {
        if (rel.edge.direction === "left") {
          fromClause += ` JOIN nodes ${targetAlias} ON ${edgeAlias}.source_id = ${targetAlias}.id`;
        } else {
          fromClause += ` JOIN nodes ${targetAlias} ON ${edgeAlias}.target_id = ${targetAlias}.id`;
        }
        const labelMatch = this.generateLabelMatchCondition(targetAlias, rel.target.label);
        conditions.push(labelMatch.sql);
        params.push(...labelMatch.params);
      }
      
      sql = `EXISTS (SELECT 1 FROM ${fromClause} WHERE ${conditions.join(" AND ")})`;
    } else {
      // EXISTS with node-only pattern: EXISTS((n))
      // This is less common but valid - check if the node variable exists
      const node = pattern as NodePattern;
      const nodeVar = node.variable;
      const nodeInfo = nodeVar ? this.ctx.variables.get(nodeVar) : null;
      
      if (!nodeInfo) {
        throw new Error(`EXISTS pattern references unknown variable: ${nodeVar}`);
      }
      
      // Node exists if it has an id (always true for matched nodes)
      sql = `${nodeInfo.alias}.id IS NOT NULL`;
    }

    return { sql, params };
  }

  private translateInCondition(condition: WhereCondition): { sql: string; params: unknown[] } {
    const left = this.translateWhereExpression(condition.left!);
    const params = [...left.params];
    
    const listExpr = condition.list;
    if (!listExpr) {
      throw new Error("IN condition must have a list expression");
    }

    if (listExpr.type === "literal" && Array.isArray(listExpr.value)) {
      const values = listExpr.value as unknown[];
      
      // Handle empty array - no matches possible
      if (values.length === 0) {
        return { sql: "1 = 0", params: [] };
      }
      
      // Generate placeholders for each value
      const placeholders = values.map(() => "?").join(", ");
      params.push(...values);
      
      return {
        sql: `${left.sql} IN (${placeholders})`,
        params,
      };
    }
    
    if (listExpr.type === "parameter") {
      // For parameter arrays, we use json_each to check membership
      const paramValue = this.ctx.paramValues[listExpr.name!];
      if (Array.isArray(paramValue)) {
        if (paramValue.length === 0) {
          return { sql: "1 = 0", params: [] };
        }
        const placeholders = paramValue.map(() => "?").join(", ");
        params.push(...paramValue);
        return {
          sql: `${left.sql} IN (${placeholders})`,
          params,
        };
      }
      throw new Error(`Parameter ${listExpr.name} must be an array for IN clause`);
    }

    if (listExpr.type === "variable" || listExpr.type === "property") {
      // For variable/property references, use json_each subquery
      const listResult = this.translateWhereExpression(listExpr);
      params.push(...listResult.params);
      return {
        sql: `${left.sql} IN (SELECT value FROM json_each(${listResult.sql}))`,
        params,
      };
    }

    throw new Error(`Unsupported list expression type in IN clause: ${listExpr.type}`);
  }

  private translateOrderByExpression(expr: Expression, returnAliases: string[] = []): { sql: string } {
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
        // First check if this is a return column alias (e.g., ORDER BY total)
        // SQL allows ORDER BY to reference SELECT column aliases directly
        if (returnAliases.includes(expr.variable!)) {
          return { sql: expr.variable! };
        }
        
        // Check if this is an UNWIND variable
        const unwindClauses = (this.ctx as any).unwindClauses as Array<{
          alias: string;
          variable: string;
          jsonExpr: string;
          params: unknown[];
        }> | undefined;
        
        if (unwindClauses) {
          const unwindClause = unwindClauses.find(u => u.variable === expr.variable);
          if (unwindClause) {
            return { sql: `${unwindClause.alias}.value` };
          }
        }
        
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
        // First check if this is a WITH alias
        const withAliases = (this.ctx as any).withAliases as Map<string, Expression> | undefined;
        if (withAliases && withAliases.has(expr.variable!)) {
          // This variable is actually an alias from WITH - translate the underlying expression
          const originalExpr = withAliases.get(expr.variable!)!;
          return this.translateWhereExpression(originalExpr);
        }
        
        // Check if this is an UNWIND variable
        const unwindClauses = (this.ctx as any).unwindClauses as Array<{
          alias: string;
          variable: string;
          jsonExpr: string;
          params: unknown[];
        }> | undefined;
        
        if (unwindClauses) {
          const unwindClause = unwindClauses.find(u => u.variable === expr.variable);
          if (unwindClause) {
            // UNWIND variables access the 'value' column from json_each
            return { sql: `${unwindClause.alias}.value`, params: [] };
          }
        }
        
        const varInfo = this.ctx.variables.get(expr.variable!);
        if (!varInfo) {
          throw new Error(`Unknown variable: ${expr.variable}`);
        }
        return { sql: `${varInfo.alias}.id`, params: [] };
      }

      case "binary": {
        const leftResult = this.translateWhereExpression(expr.left!);
        const rightResult = this.translateWhereExpression(expr.right!);
        return {
          sql: `(${leftResult.sql} ${expr.operator} ${rightResult.sql})`,
          params: [...leftResult.params, ...rightResult.params],
        };
      }

      case "function": {
        // Delegate to translateExpression for functions
        const result = this.translateExpression(expr);
        return { sql: result.sql, params: result.params };
      }

      default:
        throw new Error(`Unknown expression type in WHERE: ${expr.type}`);
    }
  }

  // ============================================================================
  // Helpers
  // ============================================================================

  /**
   * Translate a function argument expression to SQL.
   * Handles property access, literals, parameters, and variables.
   */
  private translateFunctionArg(expr: Expression): { sql: string; tables: string[]; params: unknown[] } {
    const tables: string[] = [];
    const params: unknown[] = [];

    switch (expr.type) {
      case "property": {
        const varInfo = this.ctx.variables.get(expr.variable!);
        if (!varInfo) {
          throw new Error(`Unknown variable: ${expr.variable}`);
        }
        tables.push(varInfo.alias);
        return {
          sql: `json_extract(${varInfo.alias}.properties, '$.${expr.property}')`,
          tables,
          params,
        };
      }
      case "literal": {
        const value = expr.value === true ? 1 : expr.value === false ? 0 : expr.value;
        params.push(value);
        return { sql: "?", tables, params };
      }
      case "parameter": {
        params.push(this.ctx.paramValues[expr.name!]);
        return { sql: "?", tables, params };
      }
      case "variable": {
        const varInfo = this.ctx.variables.get(expr.variable!);
        if (!varInfo) {
          throw new Error(`Unknown variable: ${expr.variable}`);
        }
        tables.push(varInfo.alias);
        return { sql: `${varInfo.alias}.id`, tables, params };
      }
      default:
        // For nested function calls, use translateExpression
        return this.translateExpression(expr);
    }
  }

  private isRelationshipPattern(pattern: NodePattern | RelationshipPattern): pattern is RelationshipPattern {
    return "source" in pattern && "edge" in pattern && "target" in pattern;
  }

  /**
   * Generate SQL condition to match labels stored as JSON array.
   * For a single label "Person", checks if label array contains "Person"
   * For multiple labels ["A", "B"], checks if label array contains all of them
   */
  private generateLabelMatchCondition(alias: string, label: string | string[]): { sql: string; params: unknown[] } {
    const labels = Array.isArray(label) ? label : [label];
    const prefix = alias ? `${alias}.` : "";
    
    if (labels.length === 1) {
      // Single label: check if it exists in the JSON array
      return {
        sql: `EXISTS (SELECT 1 FROM json_each(${prefix}label) WHERE value = ?)`,
        params: [labels[0]]
      };
    } else {
      // Multiple labels: check if all exist in the JSON array
      const conditions = labels.map(() => 
        `EXISTS (SELECT 1 FROM json_each(${prefix}label) WHERE value = ?)`
      );
      return {
        sql: conditions.join(" AND "),
        params: labels
      };
    }
  }

  /**
   * Normalize label to JSON array string for storage
   */
  private normalizeLabelToJson(label: string | string[] | undefined): string {
    if (!label) {
      return JSON.stringify([]);
    }
    const labelArray = Array.isArray(label) ? label : [label];
    return JSON.stringify(labelArray);
  }

  /**
   * Quote an identifier for use as SQL alias (handles reserved words like FROM, TO)
   */
  private quoteAlias(alias: string): string {
    // SQLite uses double quotes for identifiers
    return `"${alias}"`;
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

  /**
   * Check if an expression is an aggregate function (COUNT, SUM, AVG, MIN, MAX, COLLECT, PERCENTILEDISC, PERCENTILECONT)
   */
  private isAggregateExpression(expr: Expression): boolean {
    if (expr.type === "function" && expr.functionName) {
      const aggregateFunctions = ["COUNT", "SUM", "AVG", "MIN", "MAX", "COLLECT", "PERCENTILEDISC", "PERCENTILECONT"];
      return aggregateFunctions.includes(expr.functionName.toUpperCase());
    }
    return false;
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
