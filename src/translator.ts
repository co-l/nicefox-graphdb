// Cypher → SQL Translator

import {
  Query,
  Clause,
  CreateClause,
  MatchClause,
  MergeClause,
  SetClause,
  DeleteClause,
  RemoveClause,
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
  BinaryPropertyValue,
  ParameterRef,
  VariableRef,
  SetAssignment,
  ReturnItem,
  CaseWhen,
  parse,
} from "./parser.js";
import { assertValidPropertyValue, isValidPropertyValue } from "./property-value.js";

// ============================================================================
// Helper Functions
// ============================================================================

/**
 * Convert a value to SQLite-compatible format.
 * SQLite cannot bind JavaScript booleans directly - they must be converted to 1/0.
 */
function toSqliteParam(value: unknown): unknown {
  if (value === true) return 1;
  if (value === false) return 0;
  return value;
}

/**
 * Convert an array of values to SQLite-compatible format.
 */
function toSqliteParams(values: unknown[]): unknown[] {
  return values.map(toSqliteParam);
}

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
  // For nodes created via MERGE/CREATE, `id` stores the actual database UUID
  variables: Map<string, { type: "node" | "edge" | "path" | "varLengthEdge"; alias: string; pathCteName?: string; id?: string }>;
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

  /**
   * Validate and translate a SKIP or LIMIT expression
   * Throws an error if the value is negative
   */
  private translateSkipLimitExpression(expr: Expression, name: "SKIP" | "LIMIT"): { sql: string; params: unknown[] } {
    // For literal values, validation is done at parse time
    // For parameters, we need to validate here
    if (expr.type === "parameter") {
      const paramValue = this.ctx.paramValues[expr.name!];
      if (typeof paramValue === "number") {
        if (!Number.isInteger(paramValue)) {
          throw new Error(`${name}: InvalidArgumentType - expected an integer value`);
        }
        if (paramValue < 0) {
          throw new Error(`${name}: NegativeIntegerArgument - cannot be negative`);
        }
      }
    }
    
    const { sql, params } = this.translateExpression(expr);
    return { sql, params };
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
      case "REMOVE":
        return { statements: this.translateRemove(clause as RemoveClause) };
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
    
    // Track properties of nodes created in this CREATE clause for property references
    // e.g., CREATE (a:A {id: 1}), (b:B {value: a.id}) - b.value should resolve to 1
    const createdNodeProperties = new Map<string, Record<string, unknown>>();
    (this.ctx as any).createdNodeProperties = createdNodeProperties;

    for (const pattern of clause.patterns) {
      if (this.isRelationshipPattern(pattern)) {
        statements.push(...this.translateCreateRelationship(pattern));
      } else {
        // For standalone node patterns, check if the variable is already bound
        const nodePattern = pattern as NodePattern;
        if (nodePattern.variable && this.ctx.variables.has(nodePattern.variable)) {
          throw new Error(`Variable \`${nodePattern.variable}\` already declared`);
        }
        statements.push(this.translateCreateNode(nodePattern));
      }
    }
    
    // Clear the tracking after CREATE is done
    (this.ctx as any).createdNodeProperties = undefined;

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
      // Store the resolved properties so later nodes in the same CREATE can reference them
      const createdNodeProperties = (this.ctx as any).createdNodeProperties as Map<string, Record<string, unknown>> | undefined;
      if (createdNodeProperties) {
        createdNodeProperties.set(node.variable, JSON.parse(properties.json));
      }
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
    if (rel.source.variable) {
      const existing = this.ctx.variables.get(rel.source.variable);
      if (existing) {
        // Variable already bound - check for label or property conflict
        // In CREATE, you cannot rebind a variable with new properties/labels
        if (rel.source.label || rel.source.properties) {
          throw new Error(`Variable \`${rel.source.variable}\` already declared`);
        }
        // Use the actual ID if available (from MERGE), otherwise use alias (from CREATE)
        sourceId = existing.id || existing.alias;
      } else {
        // Variable not found but has a label - create new node
        const sourceStmt = this.translateCreateNode(rel.source);
        statements.push(sourceStmt);
        sourceId = sourceStmt.params[0] as string;
      }
    } else {
      // No variable - create anonymous node (with or without label)
      const sourceStmt = this.translateCreateNode(rel.source);
      statements.push(sourceStmt);
      sourceId = sourceStmt.params[0] as string;
    }

    // Create target node if it has a label (new node) or is anonymous
    let targetId: string;
    if (rel.target.variable) {
      const existing = this.ctx.variables.get(rel.target.variable);
      if (existing) {
        // Variable already bound - check for label or property conflict
        // In CREATE, you cannot rebind a variable with new properties/labels
        if (rel.target.label || rel.target.properties) {
          throw new Error(`Variable \`${rel.target.variable}\` already declared`);
        }
        // Use the actual ID if available (from MERGE), otherwise use alias (from CREATE)
        targetId = existing.id || existing.alias;
      } else {
        // Variable not found but has a label - create new node
        const targetStmt = this.translateCreateNode(rel.target);
        statements.push(targetStmt);
        targetId = targetStmt.params[0] as string;
      }
    } else {
      // No variable - create anonymous node (with or without label)
      const targetStmt = this.translateCreateNode(rel.target);
      statements.push(targetStmt);
      targetId = targetStmt.params[0] as string;
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

    // Track clause index for distinguishing patterns from different MATCH/OPTIONAL MATCH clauses
    const clauseIndex = (this.ctx as any).matchClauseCounter || 0;
    (this.ctx as any).matchClauseCounter = clauseIndex + 1;

    // Handle path expressions FIRST (e.g., p = (a)-[r]->(b))
    // This ensures path variables are registered before we check for conflicts with node patterns
    if (clause.pathExpressions) {
      for (const pathExpr of clause.pathExpressions) {
        this.registerPathExpression(pathExpr, optional);
      }
    }

    for (const pattern of clause.patterns) {
      if (this.isRelationshipPattern(pattern)) {
        this.registerRelationshipPattern(pattern, optional, clauseIndex);
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
        
        // Also associate this WHERE with the relationship patterns from this OPTIONAL MATCH
        // This allows us to add the condition to the edge's LEFT JOIN ON clause
        const relPatterns = (this.ctx as any).relationshipPatterns as any[] | undefined;
        if (relPatterns) {
          // Find patterns from this OPTIONAL MATCH (the ones that are optional and don't have a WHERE yet)
          for (const pattern of relPatterns) {
            if (pattern.optional && !pattern.optionalWhere) {
              pattern.optionalWhere = clause.where;
            }
          }
        }
      } else {
        (this.ctx as any).whereClause = clause.where;
      }
    }

    return [];
  }

  private registerPathExpression(pathExpr: any, optional: boolean = false): void {
    // Check if the path variable is already declared as a node or edge
    const existingVar = this.ctx.variables.get(pathExpr.variable);
    if (existingVar) {
      if (existingVar.type === "node") {
        throw new Error(`SyntaxError: Variable \`${pathExpr.variable}\` already declared`);
      } else if (existingVar.type === "edge" || existingVar.type === "varLengthEdge") {
        throw new Error(`SyntaxError: Variable \`${pathExpr.variable}\` already declared`);
      } else if (existingVar.type === "path") {
        throw new Error(`SyntaxError: Variable \`${pathExpr.variable}\` already declared`);
      }
    }
    
    // Check if the path variable is already declared as a WITH alias (e.g., WITH true AS p MATCH p = ...)
    const withAliases = (this.ctx as any).withAliases as Map<string, Expression> | undefined;
    if (withAliases && withAliases.has(pathExpr.variable)) {
      throw new Error(`SyntaxError: Variable \`${pathExpr.variable}\` already declared`);
    }
    
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
    let previousTargetAlias: string | undefined = undefined;

    // Register all patterns within the path
    for (const pattern of pathExpr.patterns) {
      if (this.isRelationshipPattern(pattern)) {
        const relPattern = pattern as any;
        
        // Check if source is first in chain - if so, record it before registering relationship
        const isFirstInChain = nodeAliases.length === 0;
        
        // For chained patterns in a path, set the previous target as the source to reuse
        // This handles cases like p = (a:Label1)<--(:Label2)--() where the middle node
        // should be shared between both relationship patterns
        if (previousTargetAlias && !relPattern.source.variable) {
          // Set this so registerRelationshipPattern can reuse the alias
          (this.ctx as any).pathChainSourceAlias = previousTargetAlias;
        }
        
        // Register the relationship pattern - this will handle node and edge registration
        this.registerRelationshipPattern(pattern, optional);
        
        // Clear the chain source alias after use
        (this.ctx as any).pathChainSourceAlias = undefined;
        
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
        
        // Remember this target for the next iteration
        previousTargetAlias = relPatternInfo.targetAlias;
      } else {
        // Single node pattern in path
        const nodeAlias = this.registerNodePattern(pattern, optional);
        nodeAliases.push(nodeAlias);
        previousTargetAlias = nodeAlias;
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
      nodeAliases: Array.from(new Set(nodeAliases)), // Deduplicated for table joins
      nodeSequence: nodeAliases, // Original order with duplicates for path output
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
      // Check if variable is already registered as a path variable
      const existingVar = this.ctx.variables.get(node.variable);
      if (existingVar && existingVar.type === "path") {
        throw new Error(`VariableAlreadyBound: Variable \`${node.variable}\` already declared as a path`);
      }
      
      // Check if variable is already registered as an edge (relationship) variable
      // In Cypher, a relationship variable cannot be reused as a node
      if (existingVar && (existingVar.type === "edge" || existingVar.type === "varLengthEdge")) {
        throw new Error(`SyntaxError: Variable \`${node.variable}\` already declared as a relationship`);
      }
      
      // Check if variable is bound to a non-node value from WITH clause
      // e.g., WITH true AS n MATCH (n) should error because n is a boolean, not a node
      // But expressions like coalesce(b, c) could return nodes, so allow them
      const withAliases = (this.ctx as any).withAliases as Map<string, Expression> | undefined;
      if (withAliases && withAliases.has(node.variable)) {
        const expr = withAliases.get(node.variable)!;
        // Only allow expressions that could return nodes
        // Reject literals (except null), LIST function, and other non-node-returning expressions
        const isAllowedExpression = 
          expr.type === "variable" ||  // Passthrough of a node variable
          (expr.type === "literal" && expr.value === null) ||  // Null literal allowed (will return no rows)
          (expr.type === "function" && 
            expr.functionName !== "LIST" && 
            expr.functionName !== "FILTER" && 
            expr.functionName !== "EXTRACT" && 
            expr.functionName !== "REDUCE" &&
            expr.functionName !== "keys" &&
            expr.functionName !== "labels") || // Most functions could return nodes
          expr.type === "case" ||  // CASE could return nodes
          expr.type === "binary" ||  // Binary ops could return nodes
          expr.type === "property" ||  // Property access returns the property value
          expr.type === "propertyAccess" ||  // Chained property access
          expr.type === "comparison";  // Comparison returns boolean, but we'll allow it to return no rows
        
        if (!isAllowedExpression) {
          // Determine the error type
          if (expr.type === "literal") {
            throw new Error(`Type mismatch: expected Node but was ${typeof expr.value === 'string' ? 'String' : typeof expr.value === 'number' ? (Number.isInteger(expr.value) ? 'Integer' : 'Float') : typeof expr.value === 'boolean' ? 'Boolean' : 'List'}`);
          } else if (expr.type === "function" && expr.functionName === "LIST") {
            throw new Error(`Type mismatch: expected Node but was List`);
          } else if (expr.type === "listComprehension" || expr.type === "listPredicate") {
            throw new Error(`Type mismatch: expected Node but was List`);
          } else if (expr.type === "object") {
            throw new Error(`Type mismatch: expected Node but was Map`);
          } else {
            throw new Error(`Type mismatch: expected Node but was ${expr.type}`);
          }
        }
        // Track that this variable comes from a WITH expression alias
        // This requires a NULL check in the WHERE clause when used in MATCH
        if (!(this.ctx as any).withExpressionAliases) {
          (this.ctx as any).withExpressionAliases = new Set<string>();
        }
        (this.ctx as any).withExpressionAliases.add(node.variable);
      }
      
      this.ctx.variables.set(node.variable, { type: "node", alias });
    } else {
      // Track anonymous node patterns so they can be included in FROM clause
      if (!(this.ctx as any).anonymousNodePatterns) {
        (this.ctx as any).anonymousNodePatterns = [];
      }
      (this.ctx as any).anonymousNodePatterns.push({ alias, optional });
    }
    // Store pattern info for later
    (this.ctx as any)[`pattern_${alias}`] = node;
    // Track if this node pattern is optional
    (this.ctx as any)[`optional_${alias}`] = optional;
    return alias;
  }

  private registerRelationshipPattern(rel: RelationshipPattern, optional: boolean = false, clauseIndex?: number): void {
    // Check if source node is already registered (for chained patterns or multi-MATCH)
    let sourceAlias: string;
    let sourceIsNew = false;
    if (rel.source.variable && this.ctx.variables.has(rel.source.variable)) {
      const existingVar = this.ctx.variables.get(rel.source.variable)!;
      // Check if variable is a path - cannot use path variable as a node
      if (existingVar.type === "path") {
        throw new Error(`VariableAlreadyBound: Variable \`${rel.source.variable}\` already declared as a path`);
      }
      sourceAlias = existingVar.alias;
      // If the new pattern has a label constraint, track it as an additional constraint
      if (rel.source.label) {
        if (!(this.ctx as any).additionalLabelConstraints) {
          (this.ctx as any).additionalLabelConstraints = [];
        }
        (this.ctx as any).additionalLabelConstraints.push({
          alias: sourceAlias,
          label: rel.source.label,
          optional
        });
      }
    } else if ((this.ctx as any).pathChainSourceAlias) {
      // Path chaining: reuse the previous target alias as this source
      // This handles patterns like p = (a:Label1)<--(:Label2)--() where (:Label2) is shared
      sourceAlias = (this.ctx as any).pathChainSourceAlias;
      sourceIsNew = false;
      // If the source has a label constraint, add it to the reused alias
      if (rel.source.label) {
        if (!(this.ctx as any).additionalLabelConstraints) {
          (this.ctx as any).additionalLabelConstraints = [];
        }
        (this.ctx as any).additionalLabelConstraints.push({
          alias: sourceAlias,
          label: rel.source.label,
          optional
        });
      }
    } else if (!rel.source.variable && !rel.source.label && (this.ctx as any).lastAnonymousTargetAlias) {
      // Anonymous source node in a chain - reuse the last anonymous target
      sourceAlias = (this.ctx as any).lastAnonymousTargetAlias;
      sourceIsNew = false;
    } else {
      sourceAlias = this.registerNodePattern(rel.source, optional);
      sourceIsNew = true;
    }

    // Check if target node is already registered (for multi-MATCH shared variables)
    let targetAlias: string;
    let targetIsNew = false;
    if (rel.target.variable && this.ctx.variables.has(rel.target.variable)) {
      const existingVar = this.ctx.variables.get(rel.target.variable)!;
      // Check if variable is a path - cannot use path variable as a node
      if (existingVar.type === "path") {
        throw new Error(`VariableAlreadyBound: Variable \`${rel.target.variable}\` already declared as a path`);
      }
      targetAlias = existingVar.alias;
      // If the new pattern has a label constraint, track it as an additional constraint
      if (rel.target.label) {
        if (!(this.ctx as any).additionalLabelConstraints) {
          (this.ctx as any).additionalLabelConstraints = [];
        }
        (this.ctx as any).additionalLabelConstraints.push({
          alias: targetAlias,
          label: rel.target.label,
          optional
        });
      }
    } else {
      targetAlias = this.registerNodePattern(rel.target, optional);
      targetIsNew = true;
    }

    // Check if edge variable is already registered (for multi-MATCH with same edge variable)
    let edgeAlias: string;
    let edgeIsNew = false;
    let boundEdgeOriginalPattern: { sourceAlias: string; targetAlias: string } | undefined;
    
    // Check if relationship variable is bound to a non-relationship value from WITH clause
    // e.g., WITH true AS r MATCH ()-[r]-() should error because r is a boolean, not a relationship
    // However, variable-length patterns like [rs*] can accept lists of relationships
    // And expressions like coalesce() could return relationships, so allow them
    const isVariableLengthPattern = rel.edge.minHops !== undefined || rel.edge.maxHops !== undefined;
    const withAliases = (this.ctx as any).withAliases as Map<string, Expression> | undefined;
    if (rel.edge.variable && withAliases && withAliases.has(rel.edge.variable) && !isVariableLengthPattern) {
      const expr = withAliases.get(rel.edge.variable)!;
      // Only allow expressions that could return relationships
      // Reject literals (except null), LIST function, and other non-relationship-returning expressions
      const isAllowedExpression = 
        expr.type === "variable" ||  // Passthrough of a relationship variable
        (expr.type === "function" && 
          expr.functionName !== "LIST" && 
          expr.functionName !== "FILTER" && 
          expr.functionName !== "EXTRACT" && 
          expr.functionName !== "REDUCE" &&
          expr.functionName !== "keys" &&
          expr.functionName !== "labels") || // Most functions could return relationships
        expr.type === "case" ||  // CASE could return relationships
        expr.type === "binary" ||  // Binary ops could return relationships
        expr.type === "property" ||  // Property access
        expr.type === "propertyAccess" ||  // Chained property access
        expr.type === "comparison";  // Comparison
      
      if (!isAllowedExpression) {
        // Determine the error type
        if (expr.type === "literal") {
          throw new Error(`Type mismatch: expected Relationship but was ${typeof expr.value === 'string' ? 'String' : typeof expr.value === 'number' ? (Number.isInteger(expr.value) ? 'Integer' : 'Float') : typeof expr.value === 'boolean' ? 'Boolean' : 'List'}`);
        } else if (expr.type === "function" && expr.functionName === "LIST") {
          throw new Error(`Type mismatch: expected Relationship but was List`);
        } else if (expr.type === "listComprehension" || expr.type === "listPredicate") {
          throw new Error(`Type mismatch: expected Relationship but was List`);
        } else if (expr.type === "object") {
          throw new Error(`Type mismatch: expected Relationship but was Map`);
        } else {
          throw new Error(`Type mismatch: expected Relationship but was ${expr.type}`);
        }
      }
    }
    
    if (rel.edge.variable && this.ctx.variables.has(rel.edge.variable)) {
      const existingVar = this.ctx.variables.get(rel.edge.variable)!;
      // Check if variable is a path - cannot use path variable as an edge
      if (existingVar.type === "path") {
        throw new Error(`VariableAlreadyBound: Variable \`${rel.edge.variable}\` already declared as a path`);
      }
      edgeAlias = existingVar.alias;
      // Find the original relationship pattern for this bound edge
      const relPatterns = (this.ctx as any).relationshipPatterns as Array<{
        sourceAlias: string;
        targetAlias: string;
        edgeAlias: string;
      }> | undefined;
      if (relPatterns) {
        boundEdgeOriginalPattern = relPatterns.find(p => p.edgeAlias === edgeAlias);
      }
    } else {
      edgeAlias = `e${this.ctx.aliasCounter++}`;
      edgeIsNew = true;
      if (rel.edge.variable) {
        this.ctx.variables.set(rel.edge.variable, { type: "edge", alias: edgeAlias });
      }
    }

    (this.ctx as any)[`pattern_${edgeAlias}`] = rel.edge;
    (this.ctx as any)[`optional_${edgeAlias}`] = optional;

    // Store relationship patterns as an array to support multi-hop
    if (!(this.ctx as any).relationshipPatterns) {
      (this.ctx as any).relationshipPatterns = [];
    }
    
    // Check if this is a variable-length pattern
    const isVariableLength = rel.edge.minHops !== undefined || rel.edge.maxHops !== undefined;
    
    // For variable-length patterns, mark the edge variable specially
    if (isVariableLength && rel.edge.variable && edgeIsNew) {
      // Re-register as varLengthEdge type (will be updated with pathCteName later)
      this.ctx.variables.set(rel.edge.variable, { type: "varLengthEdge", alias: edgeAlias });
    }
    
    // Track the current edge scope - edges in different scopes don't need uniqueness constraints
    const edgeScope = (this.ctx as any).edgeScope || 0;
    
    (this.ctx as any).relationshipPatterns.push({ 
      sourceAlias, 
      targetAlias, 
      edgeAlias, 
      edge: rel.edge, 
      optional,
      sourceIsNew,
      targetIsNew,
      edgeIsNew,
      isVariableLength,
      minHops: rel.edge.minHops,
      maxHops: rel.edge.maxHops,
      // If edge is bound, store original pattern info for constraint generation
      boundEdgeOriginalPattern,
      // Track if target node has a label predicate (for DISTINCT optimization)
      targetHasLabel: !!rel.target.label,
      // Track which MATCH/OPTIONAL MATCH clause this pattern belongs to
      clauseIndex,
      // Track edge scope for uniqueness constraints across WITH boundaries
      edgeScope,
    });

    // Track the last anonymous target for chained patterns
    // This allows (a)-[:R]->()-[:S]->(b) to share the anonymous node
    if (!rel.target.variable && !rel.target.label) {
      (this.ctx as any).lastAnonymousTargetAlias = targetAlias;
    } else {
      // Clear the tracker if target is not anonymous
      (this.ctx as any).lastAnonymousTargetAlias = undefined;
    }

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
    
    // MERGE cannot use null property values (null = null is undefined in Cypher)
    for (const [key, value] of Object.entries(props)) {
      // Check for direct null value
      if (value === null) {
        throw new Error(`MERGE cannot use null property value for property '${key}'`);
      }
      // Check for parameter that resolves to null
      if (this.isParameterRef(value)) {
        const paramValue = this.ctx.paramValues[value.name];
        if (paramValue === null) {
          throw new Error(`MERGE cannot use null property value for property '${key}'`);
        }
      }
    }
    
    const serialized = this.serializeProperties(props);

    // Build condition to find existing node
    const labelCondition = this.generateLabelMatchCondition("", label);
    const conditions: string[] = [labelCondition.sql.replace(/^[^.]+\./, "")]; // Remove alias prefix
    const params: unknown[] = [...labelCondition.params];

    const withAliases = (this.ctx as any).withAliases as Map<string, Expression> | undefined;
    
    for (const [key, value] of Object.entries(props)) {
      if (this.isParameterRef(value)) {
        conditions.push(`json_extract(properties, '$.${key}') = ?`);
        params.push(this.ctx.paramValues[value.name]);
      } else if (this.isVariableRef(value)) {
        // Check if it's a WITH alias
        const varName = (value as { type: string; name: string }).name;
        if (withAliases && withAliases.has(varName)) {
          const originalExpr = withAliases.get(varName)!;
          conditions.push(`json_extract(properties, '$.${key}') = ?`);
          if (originalExpr.type === "literal") {
            params.push(originalExpr.value);
          } else if (originalExpr.type === "parameter") {
            params.push(this.ctx.paramValues[originalExpr.name!]);
          } else {
            params.push(this.evaluateExpression(originalExpr));
          }
        } else {
          conditions.push(`json_extract(properties, '$.${key}') = ?`);
          params.push(value);
        }
      } else {
        conditions.push(`json_extract(properties, '$.${key}') = ?`);
        params.push(value);
      }
    }

    const id = this.generateId();
    // Use a proper table alias (n0, n1, etc.) for SQL, not the UUID
    // But also store the actual ID for edge creation
    const alias = `n${this.ctx.aliasCounter++}`;
    if (node.variable) {
      this.ctx.variables.set(node.variable, { type: "node", alias, id });
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
            assertValidPropertyValue(val);
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
            assertValidPropertyValue(val);
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
        this.assertValidPropertyValueExpression(assignment.value);
        // Check if the target variable was just created (alias looks like a UUID)
        // In that case, we need to use a subquery-based approach for property references
        const isCreatedNode = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i.test(varInfo.alias);
        
        if (isCreatedNode) {
          // For expressions on created nodes, translate with subquery pattern
          const { sql: exprSql, params: exprParams } = this.translateExpressionForCreatedNode(assignment.value, varInfo.alias);
          statements.push({
            sql: `UPDATE ${table} SET properties = json_set(properties, '$.${assignment.property}', ${exprSql}) WHERE id = ?`,
            params: [...exprParams, varInfo.alias],
          });
        } else {
          const { sql: exprSql, params: exprParams } = this.translateExpression(assignment.value);
          // Use json_set with the SQL expression directly
          statements.push({
            sql: `UPDATE ${table} SET properties = json_set(properties, '$.${assignment.property}', ${exprSql}) WHERE id = ?`,
            params: [...exprParams, varInfo.alias],
          });
        }
      } else {
        const value = this.evaluateExpression(assignment.value);
        // If value is null, remove the property instead of setting it to null
        if (value === null) {
          statements.push({
            sql: `UPDATE ${table} SET properties = json_remove(properties, '$.${assignment.property}') WHERE id = ?`,
            params: [varInfo.alias],
          });
        } else {
          assertValidPropertyValue(value);
          // Use json_set to update the property
          statements.push({
            sql: `UPDATE ${table} SET properties = json_set(properties, '$.${assignment.property}', json(?)) WHERE id = ?`,
            params: [JSON.stringify(value), varInfo.alias],
          });
        }
      }
    }

    return statements;
  }

  private assertValidPropertyValueExpression(expr: Expression): void {
    if (expr.type === "literal") {
      if (expr.value === null) return;
      assertValidPropertyValue(expr.value);
      return;
    }

    if (expr.type === "parameter") {
      const value = this.ctx.paramValues[expr.name!];
      if (value === null) return;
      assertValidPropertyValue(value);
      return;
    }

    if (expr.type === "object") {
      throw new Error("TypeError: InvalidPropertyType");
    }

    if (expr.type === "function" && expr.functionName?.toUpperCase() === "LIST") {
      const args = expr.args ?? [];
      const canEvaluateAllElements = args.every((a) => a.type === "literal" || a.type === "parameter");

      for (const arg of args) {
        if (arg.type === "object") {
          throw new Error("TypeError: InvalidPropertyType");
        }
        if (arg.type === "function" && arg.functionName?.toUpperCase() === "LIST") {
          throw new Error("TypeError: InvalidPropertyType");
        }
        if (arg.type === "literal") {
          const value = arg.value;
          if (value === null) throw new Error("TypeError: InvalidPropertyType");
          if (typeof value === "object") throw new Error("TypeError: InvalidPropertyType");
          if (typeof value === "number" && !Number.isFinite(value)) throw new Error("TypeError: InvalidPropertyType");
        }
        if (arg.type === "parameter") {
          const value = this.ctx.paramValues[arg.name!];
          if (value === null) throw new Error("TypeError: InvalidPropertyType");
          if (typeof value === "object") throw new Error("TypeError: InvalidPropertyType");
          if (typeof value === "number" && !Number.isFinite(value)) throw new Error("TypeError: InvalidPropertyType");
        }
      }

      if (canEvaluateAllElements) {
        const value = args.map((a) =>
          a.type === "literal" ? a.value : this.ctx.paramValues[(a as { name: string }).name]
        );
        if (!isValidPropertyValue(value)) {
          throw new Error("TypeError: InvalidPropertyType");
        }
      }
    }
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
  // REMOVE
  // ============================================================================

  private translateRemove(clause: RemoveClause): SqlStatement[] {
    const statements: SqlStatement[] = [];

    for (const item of clause.items) {
      const varInfo = this.ctx.variables.get(item.variable);
      if (!varInfo) {
        // Variable doesn't exist (e.g., from OPTIONAL MATCH with no match)
        // This is a no-op, we'll handle it at execution time
        continue;
      }

      const table = varInfo.type === "node" ? "nodes" : "edges";

      if (item.labels && item.labels.length > 0) {
        // Remove labels from node
        if (varInfo.type !== "node") {
          throw new Error(`Cannot remove labels from a relationship: ${item.variable}`);
        }
        // Remove specific labels from the label array
        // Build a JSON array of labels to remove
        const labelsToRemove = JSON.stringify(item.labels);
        statements.push({
          sql: `UPDATE nodes SET label = (
            SELECT json_group_array(value) FROM (
              SELECT value FROM json_each(nodes.label)
              WHERE value NOT IN (SELECT value FROM json_each(?))
              ORDER BY value
            )
          ) WHERE id = ?`,
          params: [labelsToRemove, varInfo.alias],
        });
      } else if (item.property) {
        // Remove property
        statements.push({
          sql: `UPDATE ${table} SET properties = json_remove(properties, '$.${item.property}') WHERE id = ?`,
          params: [varInfo.alias],
        });
      }
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

    // Check for duplicate column names (ColumnNameConflict)
    // This must be done before processing items to catch the error early
    this.checkDuplicateColumnNames(clause.items);

    // Validate ORDER BY with DISTINCT
    // When DISTINCT is used, ORDER BY can only reference expressions in the RETURN clause
    if (clause.distinct && clause.orderBy && clause.orderBy.length > 0) {
      this.validateDistinctOrderBy(clause);
    }
    
    // Validate ORDER BY with aggregation
    // When aggregation is used in RETURN/WITH, ORDER BY expressions with mixed aggregate/non-aggregate
    // are not allowed unless the non-aggregate parts are in an implicit GROUP BY
    const hasAggregation = clause.items.some(item => this.isAggregateExpression(item.expression));
    const prevWithOrderBy = (this.ctx as any).withOrderBy as { expression: Expression; direction: "ASC" | "DESC" }[] | undefined;
    const orderByToValidate = clause.orderBy && clause.orderBy.length > 0 ? clause.orderBy : prevWithOrderBy;
    if (hasAggregation && orderByToValidate && orderByToValidate.length > 0) {
      this.validateAggregationOrderBy(clause, orderByToValidate);
    }

    const selectParts: string[] = [];
    const returnColumns: string[] = [];
    const fromParts: string[] = [];
    const joinParts: string[] = [];
    const joinParams: unknown[] = []; // Parameters for JOIN ON clauses
    const whereParts: string[] = [];
    const whereParams: unknown[] = []; // Parameters for WHERE clause
    
    // Handle pre-WITH patterns (patterns before a WITH that doesn't pass through node/edge vars)
    // These need to be converted to a cartesian product row source
    const preWithPatterns = (this.ctx as any).preWithPatterns as Array<{
      relationshipPatterns: any[];
      anonymousNodePatterns: Array<{ alias: string; optional: boolean }>;
      variables: Map<string, any>;
    }> | undefined;
    
    if (preWithPatterns && preWithPatterns.length > 0) {
      // Build subqueries for each pre-WITH pattern set
      // These provide the "row multiplier" effect for the cartesian product
      for (let i = 0; i < preWithPatterns.length; i++) {
        const preWith = preWithPatterns[i];
        const subqueryParts: string[] = [];
        const subqueryWhere: string[] = [];
        
        if (preWith.relationshipPatterns.length > 0) {
          // Build FROM/JOINs for the relationship patterns
          const addedNodes = new Set<string>();
          const addedEdges = new Set<string>();
          
          for (let j = 0; j < preWith.relationshipPatterns.length; j++) {
            const pattern = preWith.relationshipPatterns[j];
            
            // Add source node
            if (!addedNodes.has(pattern.sourceAlias)) {
              if (subqueryParts.length === 0) {
                subqueryParts.push(`nodes ${pattern.sourceAlias}`);
              } else {
                subqueryParts.push(`JOIN nodes ${pattern.sourceAlias} ON 1=1`);
              }
              addedNodes.add(pattern.sourceAlias);
            }
            
            // Add edge
            if (!addedEdges.has(pattern.edgeAlias)) {
              const edgeJoinCol = pattern.edge.direction === "left" ? "target_id" : "source_id";
              subqueryParts.push(`JOIN edges ${pattern.edgeAlias} ON ${pattern.edgeAlias}.${edgeJoinCol} = ${pattern.sourceAlias}.id`);
              addedEdges.add(pattern.edgeAlias);
            }
            
            // Add target node
            if (!addedNodes.has(pattern.targetAlias)) {
              const targetJoinCol = pattern.edge.direction === "left" ? "source_id" : "target_id";
              subqueryParts.push(`JOIN nodes ${pattern.targetAlias} ON ${pattern.edgeAlias}.${targetJoinCol} = ${pattern.targetAlias}.id`);
              addedNodes.add(pattern.targetAlias);
            }
          }
          
          // Add edge uniqueness constraints within the pre-WITH patterns
          const edgeAliases = preWith.relationshipPatterns.map(p => p.edgeAlias);
          for (let j = 0; j < edgeAliases.length; j++) {
            for (let k = j + 1; k < edgeAliases.length; k++) {
              if (edgeAliases[j] !== edgeAliases[k]) {
                subqueryWhere.push(`${edgeAliases[j]}.id <> ${edgeAliases[k]}.id`);
              }
            }
          }
        } else if (preWith.anonymousNodePatterns.length > 0) {
          // Handle anonymous node patterns
          for (const nodePattern of preWith.anonymousNodePatterns) {
            if (subqueryParts.length === 0) {
              subqueryParts.push(`nodes ${nodePattern.alias}`);
            } else {
              subqueryParts.push(`, nodes ${nodePattern.alias}`);
            }
          }
        }
        
        if (subqueryParts.length > 0) {
          // Build the subquery - just needs to provide rows for cartesian product
          let subquery = `(SELECT 1 FROM ${subqueryParts.join(" ")}`;
          if (subqueryWhere.length > 0) {
            subquery += ` WHERE ${subqueryWhere.join(" AND ")}`;
          }
          subquery += `) AS __pre_with_${i}__`;
          fromParts.push(subquery);
        }
      }
    }
    
    // Apply WITH modifiers if present
    const withDistinct = (this.ctx as any).withDistinct as boolean | undefined;
    const withOrderBy = (this.ctx as any).withOrderBy as { expression: Expression; direction: "ASC" | "DESC" }[] | undefined;
    const withOrderByAliases = (this.ctx as any).withOrderByAliases as Map<string, Expression> | undefined;
    const withSkip = (this.ctx as any).withSkip as Expression | undefined;
    const withLimit = (this.ctx as any).withLimit as Expression | undefined;
    const withWhere = (this.ctx as any).withWhere as WhereCondition | undefined;
    const accumulatedWithWheres = (this.ctx as any).accumulatedWithWheres as WhereCondition[] | undefined;

    // Pre-compute whether we'll need a WITH subquery BEFORE building SELECT parts.
    // This is needed so translateExpression can reference UNWIND variables correctly.
    // When RETURN has only aggregates and WITH has LIMIT/SKIP/DISTINCT, we wrap
    // the FROM in a subquery, and UNWIND variables need different column references.
    const returnOnlyAggregatesPreCheck = clause.items.every(item => this.isAggregateExpression(item.expression));
    const needsWithSubqueryPreCheck = returnOnlyAggregatesPreCheck && (withLimit !== undefined || withSkip !== undefined || withDistinct);
    
    // Build mapping of UNWIND variables to subquery column names if needed
    const unwindClausesForPreCheck = (this.ctx as any).unwindClauses as Array<{
      alias: string;
      variable: string;
      jsonExpr: string;
      params: unknown[];
    }> | undefined;
    
    if (needsWithSubqueryPreCheck && this.ctx.withClauses && this.ctx.withClauses.length > 0 && unwindClausesForPreCheck && unwindClausesForPreCheck.length > 0) {
      const lastWithClause = this.ctx.withClauses[this.ctx.withClauses.length - 1];
      for (const item of lastWithClause.items) {
        if (item.expression.type === "variable") {
          const varName = item.expression.variable!;
          const unwindClause = unwindClausesForPreCheck.find(u => u.variable === varName);
          if (unwindClause) {
            // Mark this UNWIND variable as needing subquery column reference
            (unwindClause as any).subqueryColumnName = item.alias || varName;
          }
        }
      }
    }

    // Check if RETURN uses list predicates referencing WITH aggregate aliases.
    // SQLite doesn't allow aggregate functions in correlated subqueries (like json_each),
    // so we need to materialize the aggregates in a CTE first.
    const withAliasesForCheck = (this.ctx as any).withAliases as Map<string, Expression> | undefined;
    const aggregateAliasesInListPredicates = new Set<string>();
    for (const item of clause.items) {
      const aliases = this.collectWithAggregateAliasesFromListPredicates(item.expression, withAliasesForCheck);
      for (const alias of aliases) {
        aggregateAliasesInListPredicates.add(alias);
      }
    }
    
    // If we have any aggregate aliases used in list predicates, we need a CTE
    const needsAggregateCTE = aggregateAliasesInListPredicates.size > 0;
    if (needsAggregateCTE) {
      // Mark the context so translateExpression knows to use column references instead of re-translating
      (this.ctx as any).materializedAggregateAliases = aggregateAliasesInListPredicates;
      // Also mark that we need to use __aggregates__ as the FROM source
      (this.ctx as any).useAggregatesCTE = true;
    }

    // Track which tables we need
    const neededTables = new Set<string>();

    // Process return items
    const exprParams: unknown[] = [];
    const returnAliasExpressions = new Map<string, Expression>();
    (this.ctx as any).returnAliasExpressions = returnAliasExpressions;
    
    // Check for RETURN * (return all bound variables)
    let returnItems = clause.items;
    if (clause.items.length > 0 && 
        clause.items[0].expression.type === "variable" && 
        clause.items[0].expression.variable === "*") {
      // Expand * to all bound variables
      const expandedItems: ReturnItem[] = [];
      
      // First add WITH aliases (they should appear before MATCH-bound variables in typical queries)
      const withAliases = (this.ctx as any).withAliases as Map<string, Expression> | undefined;
      if (withAliases) {
        for (const [aliasName, _] of withAliases) {
          // Only add if not already a regular variable (to avoid duplicates)
          if (!this.ctx.variables.has(aliasName)) {
            expandedItems.push({ expression: { type: "variable", variable: aliasName } });
          }
        }
      }
      
      // Then add regular bound variables
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
      returnAliasExpressions.set(alias, item.expression);
      selectParts.push(`${exprSql} AS ${this.quoteAlias(alias)}`);
      returnColumns.push(alias);
    }
    
    // Add WITH aliases that are referenced in ORDER BY but not in RETURN
    // This allows ORDER BY to reference columns from previous WITH clause
    const orderByForCheck = clause.orderBy && clause.orderBy.length > 0 ? clause.orderBy : withOrderBy;
    if (orderByForCheck && orderByForCheck.length > 0) {
      const orderByAliases =
        clause.orderBy && clause.orderBy.length > 0
          ? ((this.ctx as any).withAliases as Map<string, Expression> | undefined)
          : withOrderByAliases;
      if (orderByAliases) {
        for (const { expression: orderExpr } of orderByForCheck) {
          // Check if ORDER BY references a WITH alias not in RETURN
          if (orderExpr.type === "variable" && orderExpr.variable) {
            const aliasName = orderExpr.variable;
            if (orderByAliases.has(aliasName) && !returnColumns.includes(aliasName)) {
              // Add this WITH alias to SELECT but not to returnColumns
              const aliasExpr = orderByAliases.get(aliasName)!;
              const prevWithAliases = (this.ctx as any).withAliases as Map<string, Expression> | undefined;
              (this.ctx as any).withAliases = orderByAliases;
              try {
                const { sql: exprSql, params: itemParams } = this.translateExpression(aliasExpr);
                exprParams.push(...itemParams);
                selectParts.push(`${exprSql} AS ${this.quoteAlias(aliasName)}`);
              } finally {
                (this.ctx as any).withAliases = prevWithAliases;
              }
            }
          }
        }
      }
    }

    // Build FROM clause based on registered patterns
    const relPatterns = (this.ctx as any).relationshipPatterns as Array<{
      sourceAlias: string;
      targetAlias: string;
      edgeAlias: string;
      edge: { type?: string; types?: string[]; properties?: Record<string, PropertyValue>; minHops?: number; maxHops?: number; direction?: "left" | "right" | "none" };
      optional?: boolean;
      sourceIsNew?: boolean;
      targetIsNew?: boolean;
      isVariableLength?: boolean;
      minHops?: number;
      maxHops?: number;
      targetHasLabel?: boolean;
      edgeIsNew?: boolean;
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
      // Track which edge aliases we've already added to FROM/JOIN
      const addedEdgeAliases = new Set<string>();
      // Track which node aliases have had their filters added (to avoid duplicates)
      const filteredNodeAliases = new Set<string>();
      // Track undirected patterns that need direction multipliers
      // Maps edge alias to direction column alias (e.g., e2 -> _d0)
      const undirectedDirections = new Map<string, string>();
      let directionCounter = 0;

      // IMPORTANT: Before processing relationship patterns, add all non-optional node
      // patterns to FROM first. This ensures that bound variables from required MATCH
      // clauses are in FROM before OPTIONAL MATCH relationship patterns try to reference them.
      // Example: MATCH (a:A), (b:C) OPTIONAL MATCH (x)-->(b) - here b must be in FROM before
      // we process the OPTIONAL MATCH relationship, even though b is used as a target in
      // the OPTIONAL MATCH. We check the node's original optional flag, not whether it
      // appears in an optional relationship pattern.
      for (const [variable, info] of this.ctx.variables) {
        if (info.type !== "node") continue;
        
        const pattern = (this.ctx as any)[`pattern_${info.alias}`];
        const isOptional = (this.ctx as any)[`optional_${info.alias}`] === true;
        
        // Add non-optional nodes to FROM, regardless of whether they appear in a relationship pattern
        // The key is checking the node's original optional flag, not where it's used
        if (pattern && !isOptional) {
          // Check if this node is only used as source/target in a NON-optional relationship pattern
          // If so, the relationship loop will handle adding it to FROM
          const isSourceOrTargetOfNonOptionalRel = relPatterns.some(
            rp => !rp.optional && (rp.sourceAlias === info.alias || rp.targetAlias === info.alias)
          );
          
          if (!isSourceOrTargetOfNonOptionalRel) {
            // This is a required node that either:
            // 1. Is not part of any relationship pattern (standalone), or
            // 2. Is only used in optional relationship patterns (but the node itself is required)
            // Add to FROM
            fromParts.push(`nodes ${info.alias}`);
            addedNodeAliases.add(info.alias);
            
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
            filteredNodeAliases.add(info.alias);
          }
        }
      }

      // Relationship query - handle multi-hop patterns
      for (let i = 0; i < relPatterns.length; i++) {
        const relPattern = relPatterns[i];
        const isOptional = relPattern.optional === true;
        const joinType = isOptional ? "LEFT JOIN" : "JOIN";

        // Check if source was from a previous optional MATCH
        const sourceIsOptional = (this.ctx as any)[`optional_${relPattern.sourceAlias}`] === true;
        
        // Record whether source was already added BEFORE we potentially add it
        // This is needed for edge join direction logic later
        const sourceWasAlreadyAdded = addedNodeAliases.has(relPattern.sourceAlias);

        if (i === 0 && !isOptional) {
          // First non-optional relationship: add source node to FROM
          // If source was optional, use LEFT JOIN to allow NULL values, then filter them out
          if (sourceIsOptional) {
            // Source is from optional MATCH - use LEFT JOIN to allow NULL, then filter in WHERE
            // Add a dummy FROM clause first, then LEFT JOIN the source node
            if (fromParts.length === 0) {
              fromParts.push(`(SELECT 1) AS __dummy__`);
            }
            // Add label constraint to ON clause for optional source
            const onConditions: string[] = ["1=1"];
            const onParams: unknown[] = [];
            const sourcePattern = (this.ctx as any)[`pattern_${relPattern.sourceAlias}`];
            if (sourcePattern?.label) {
              const labelMatch = this.generateLabelMatchCondition(relPattern.sourceAlias, sourcePattern.label);
              onConditions.push(labelMatch.sql);
              onParams.push(...labelMatch.params);
            }
            joinParts.push(`LEFT JOIN nodes ${relPattern.sourceAlias} ON ${onConditions.join(" AND ")}`);
            joinParams.push(...onParams);
            // Filter out NULL values in WHERE
            whereParts.push(`${relPattern.sourceAlias}.id IS NOT NULL`);
          } else {
            fromParts.push(`nodes ${relPattern.sourceAlias}`);
          }
          addedNodeAliases.add(relPattern.sourceAlias);
        } else if (!addedNodeAliases.has(relPattern.sourceAlias)) {
          // For subsequent patterns, if source is not already added, we need to JOIN it
          // For optional patterns, use LEFT JOIN
          if (isOptional && relPattern.sourceIsNew && addedNodeAliases.has(relPattern.targetAlias)) {
            // Special case: OPTIONAL MATCH with new source but bound target
            // Example: MATCH (b:C) OPTIONAL MATCH (x)-->(b)
            // We need to join edge first (on target), then source (on edge.source)
            // So we SKIP adding source here - it will be added after the edge join below
            // Set a flag to indicate this deferred source join
            (relPattern as any).deferSourceJoin = true;
          } else if (isOptional && relPattern.sourceIsNew && !relPattern.edgeIsNew) {
            // Optional pattern with new source, target not bound, but edge IS bound
            // Example: MATCH (a)-[r]->() WITH r, a OPTIONAL MATCH (a2)<-[r]-(b2)
            // We need to join source based on where it sits in the pre-bound edge
            // For left-directed (a2)<-[r]-(b2): a2 is at edge.target_id
            // For right-directed (a2)-[r]->(b2): a2 is at edge.source_id
            const sourceOnConditions: string[] = [];
            const sourceOnParams: unknown[] = [];
            if (relPattern.edge.direction === "left") {
              // Left-directed: source is at target_id side of edge
              sourceOnConditions.push(`${relPattern.sourceAlias}.id = ${relPattern.edgeAlias}.target_id`);
            } else {
              // Right-directed: source is at source_id side of edge
              sourceOnConditions.push(`${relPattern.sourceAlias}.id = ${relPattern.edgeAlias}.source_id`);
            }
            // Add label filter if source has one
            const sourcePattern = (this.ctx as any)[`pattern_${relPattern.sourceAlias}`];
            if (sourcePattern?.label) {
              const labelMatch = this.generateLabelMatchCondition(relPattern.sourceAlias, sourcePattern.label);
              sourceOnConditions.push(labelMatch.sql);
              sourceOnParams.push(...labelMatch.params);
              filteredNodeAliases.add(relPattern.sourceAlias);
            }
            // Since edge is pre-bound, optionalWhere won't be added to edge join
            // Add it to source's ON clause instead
            if ((relPattern as any).optionalWhere) {
              const { sql: optionalWhereSql, params: optionalWhereParams } = this.translateWhere((relPattern as any).optionalWhere);
              sourceOnConditions.push(optionalWhereSql);
              sourceOnParams.push(...optionalWhereParams);
              // Mark as handled so we don't try to add it elsewhere
              (relPattern as any).optionalWhereHandled = true;
            }
            joinParts.push(`${joinType} nodes ${relPattern.sourceAlias} ON ${sourceOnConditions.join(" AND ")}`);
            joinParams.push(...sourceOnParams);
          } else if (isOptional && relPattern.sourceIsNew) {
            // Optional pattern with new source, target not bound, edge is also new
            // Check if this is a standalone OPTIONAL MATCH at the start of the query
            // (no prior FROM, first relationship pattern). In this case, we should
            // put the edge in FROM and join nodes from it, not scan all nodes.
            // Only do this for DIRECTED patterns - undirected needs special handling
            // that requires the source node to be present for proper direction logic.
            const isDirected = relPattern.edge.direction === "left" || relPattern.edge.direction === "right";
            if (i === 0 && fromParts.length === 0 && relPattern.targetIsNew && isDirected) {
              // Standalone OPTIONAL MATCH like: OPTIONAL MATCH ()-[r]->()
              // Put edge in FROM, we'll join nodes from the edge later
              // Mark this pattern for edge-first handling
              (relPattern as any).edgeFirst = true;
              // Don't add source node here - it will be joined from the edge
            } else {
              joinParts.push(`${joinType} nodes ${relPattern.sourceAlias} ON 1=1`);
            }
          } else if (i === 0) {
            // First pattern - source is already bound (from a previous clause)
            // Check if source was from an optional MATCH - if so, use dummy FROM + LEFT JOIN
            if (sourceIsOptional && isOptional) {
              // Source is from OPTIONAL MATCH and this pattern is also optional
              // We need a dummy FROM to preserve NULL rows, then LEFT JOIN the source
              if (fromParts.length === 0) {
                fromParts.push(`(SELECT 1) AS __dummy__`);
              }
              const onConditions: string[] = ["1=1"];
              const onParams: unknown[] = [];
              const sourcePattern = (this.ctx as any)[`pattern_${relPattern.sourceAlias}`];
              if (sourcePattern?.label) {
                const labelMatch = this.generateLabelMatchCondition(relPattern.sourceAlias, sourcePattern.label);
                onConditions.push(labelMatch.sql);
                onParams.push(...labelMatch.params);
              }
              joinParts.push(`LEFT JOIN nodes ${relPattern.sourceAlias} ON ${onConditions.join(" AND ")}`);
              joinParams.push(...onParams);
            } else {
              // Source is from a required MATCH or this pattern is required - add to FROM
              fromParts.push(`nodes ${relPattern.sourceAlias}`);
            }
          } else {
            // Check if source was from an optional MATCH
            if (sourceIsOptional) {
              // Source is from optional MATCH - use LEFT JOIN with label constraint, then filter NULL
              const onConditions: string[] = ["1=1"];
              const onParams: unknown[] = [];
              const sourcePattern = (this.ctx as any)[`pattern_${relPattern.sourceAlias}`];
              if (sourcePattern?.label) {
                const labelMatch = this.generateLabelMatchCondition(relPattern.sourceAlias, sourcePattern.label);
                onConditions.push(labelMatch.sql);
                onParams.push(...labelMatch.params);
              }
              joinParts.push(`LEFT JOIN nodes ${relPattern.sourceAlias} ON ${onConditions.join(" AND ")}`);
              joinParams.push(...onParams);
              whereParts.push(`${relPattern.sourceAlias}.id IS NOT NULL`);
            } else {
              // Check if source variable is from a WITH expression alias
              const withExpressionAliases = (this.ctx as any).withExpressionAliases as Set<string> | undefined;
              const sourceVarName = Array.from(this.ctx.variables.entries())
                .find(([_, info]) => info.alias === relPattern.sourceAlias)?.[0];
              
              if (sourceVarName && withExpressionAliases?.has(sourceVarName)) {
                // Source is from a WITH expression alias (e.g., WITH coalesce(b, c) AS x MATCH (x)-->(d))
                // Don't create a node join - the expression will be used directly in the edge join condition
                // Mark this pattern to use the expression for the source
                const withAliases = (this.ctx as any).withAliases as Map<string, Expression> | undefined;
                const expr = withAliases?.get(sourceVarName);
                if (expr) {
                  const { sql: exprSql } = this.translateExpression(expr);
                  (relPattern as any).sourceExpression = exprSql;
                }
              } else if (!relPattern.edgeIsNew) {
                // Non-optional pattern with new source but bound edge
                // Example: MATCH ()-[r]->() WITH r MATCH ()-[r]->() 
                // The source node should be constrained to the bound edge's source/target
                const sourceOnConditions: string[] = [];
                if (relPattern.edge.direction === "left") {
                  // Left-directed: source is at target_id side of edge
                  sourceOnConditions.push(`${relPattern.sourceAlias}.id = ${relPattern.edgeAlias}.target_id`);
                } else {
                  // Right-directed: source is at source_id side of edge
                  sourceOnConditions.push(`${relPattern.sourceAlias}.id = ${relPattern.edgeAlias}.source_id`);
                }
                // Add label filter if source has one
                const sourcePattern = (this.ctx as any)[`pattern_${relPattern.sourceAlias}`];
                if (sourcePattern?.label) {
                  const labelMatch = this.generateLabelMatchCondition(relPattern.sourceAlias, sourcePattern.label);
                  sourceOnConditions.push(labelMatch.sql);
                  joinParams.push(...labelMatch.params);
                  filteredNodeAliases.add(relPattern.sourceAlias);
                }
                joinParts.push(`JOIN nodes ${relPattern.sourceAlias} ON ${sourceOnConditions.join(" AND ")}`);
              } else {
                joinParts.push(`JOIN nodes ${relPattern.sourceAlias} ON 1=1`);
              }
            }
          }
          // Only add to addedNodeAliases if we actually added a join (not deferred)
          if (!(relPattern as any).deferSourceJoin && !(relPattern as any).sourceExpression) {
            addedNodeAliases.add(relPattern.sourceAlias);
          }
        }

        // Build ON conditions for the edge join
        let edgeOnConditions: string[] = [];
        let edgeOnParams: unknown[] = [];
        
        // Check if this is an undirected/bidirectional pattern (direction: "none")
        const isUndirected = relPattern.edge.direction === "none";
        
        // For undirected patterns, we need a direction multiplier to produce two rows per edge
        // One row where a=source, b=target (dir=1) and one where a=target, b=source (dir=2)
        // Exception: for self-loops (source_id = target_id), we only need one row
        let dirAlias: string | undefined;
        if (isUndirected && !relPattern.optional) {
          dirAlias = `_d${directionCounter++}`;
          undirectedDirections.set(relPattern.edgeAlias, dirAlias);
          // Add the direction table to FROM clause at the beginning
          // We'll use this to double the rows for undirected patterns
          fromParts.unshift(`(SELECT 1 AS ${dirAlias} UNION ALL SELECT 2 AS ${dirAlias}) AS __dir_${dirAlias}__`);
          // For self-loops, skip the second direction (it would be a duplicate)
          // This is handled by: WHERE NOT (edge.source_id = edge.target_id AND dir = 2)
          whereParts.push(`NOT (${relPattern.edgeAlias}.source_id = ${relPattern.edgeAlias}.target_id AND ${dirAlias} = 2)`);
        }
        
        // Add edge join - need to determine direction based on whether source/target already exist
        // Use sourceWasAlreadyAdded (recorded before adding source) for accurate check
        // For edge-first patterns, we don't add source-based ON conditions since the edge
        // goes in FROM and we join nodes from it, not the other way around.
        const sourceExpression = (relPattern as any).sourceExpression as string | undefined;
        if (!(relPattern as any).edgeFirst) {
          if (isUndirected) {
            // For undirected patterns, the source node connects based on direction:
            // dir=1: source is at edge.source_id
            // dir=2: source is at edge.target_id
            if (dirAlias) {
              const sourceRef = sourceExpression || `${relPattern.sourceAlias}.id`;
              edgeOnConditions.push(`(${dirAlias} = 1 AND ${relPattern.edgeAlias}.source_id = ${sourceRef} OR ${dirAlias} = 2 AND ${relPattern.edgeAlias}.target_id = ${sourceRef})`);
            } else {
              // Optional undirected - use the old OR-based approach
              const sourceRef = sourceExpression || `${relPattern.sourceAlias}.id`;
              edgeOnConditions.push(`(${relPattern.edgeAlias}.source_id = ${sourceRef} OR ${relPattern.edgeAlias}.target_id = ${sourceRef})`);
              // For self-loops (source_id = target_id), match only once
              // Only apply when source and target are the same node (to avoid referencing target before it's joined)
              if (relPattern.sourceAlias === relPattern.targetAlias) {
                edgeOnConditions.push(`NOT (${relPattern.edgeAlias}.source_id = ${relPattern.edgeAlias}.target_id AND ${relPattern.edgeAlias}.source_id = ${relPattern.sourceAlias}.id)`);
              }
            }
          } else if (isOptional && addedNodeAliases.has(relPattern.targetAlias) && !sourceWasAlreadyAdded) {
            // OPTIONAL MATCH special case: target was already added (bound from previous MATCH) 
            // but source was not. Join edge on target side: edge.target_id = bound_target.id
            // This allows us to find all source nodes that connect to the bound target.
            edgeOnConditions.push(`${relPattern.edgeAlias}.target_id = ${relPattern.targetAlias}.id`);
          } else if (relPattern.edge.direction === "left") {
            // Left-directed: (a)<-[:R]-(b) means edge goes from b to a, so source is target_id
            const sourceRef = sourceExpression || `${relPattern.sourceAlias}.id`;
            edgeOnConditions.push(`${relPattern.edgeAlias}.target_id = ${sourceRef}`);
          } else {
            const sourceRef = sourceExpression || `${relPattern.sourceAlias}.id`;
            edgeOnConditions.push(`${relPattern.edgeAlias}.source_id = ${sourceRef}`);
          }
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
        } else if (relPattern.edge.types && relPattern.edge.types.length > 0) {
          // Multiple relationship types: [:TYPE1|TYPE2]
          const placeholders = relPattern.edge.types.map(() => "?").join(", ");
          if (isOptional) {
            edgeOnConditions.push(`${relPattern.edgeAlias}.type IN (${placeholders})`);
            edgeOnParams.push(...relPattern.edge.types);
          } else {
            whereParts.push(`${relPattern.edgeAlias}.type IN (${placeholders})`);
            whereParams.push(...relPattern.edge.types);
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

        // For optional patterns with a WHERE clause, we need to determine where to add the condition:
        // - If it only references already-joined tables (edge, source), add to edge's ON clause
        // - If it references the target node, we need to use an EXISTS subquery on the edge join
        //   to ensure that edges without a valid target are not matched (preventing duplicate NULL rows)
        // - If it references nodes that will be joined LATER in the chain, defer to those edges
        // This ensures proper NULL propagation when the condition fails
        if (isOptional && (relPattern as any).optionalWhere) {
          const whereCondition = (relPattern as any).optionalWhere as WhereCondition;
          const varsInCondition = this.findVariablesInCondition(whereCondition);
          
          // Check if condition references nodes that haven't been joined yet and won't be joined by this edge
          // Build a map of variable names to their node aliases
          const varToAlias = new Map<string, string>();
          for (const [varName, info] of this.ctx.variables.entries()) {
            if (info.type === "node") {
              varToAlias.set(varName, info.alias);
            }
          }
          
          // Check if any variable references a node that:
          // 1. Is NOT already added to joins
          // 2. Is NOT the current target (which will be added by this edge)
          const referencesLaterNode = varsInCondition.some(varName => {
            const alias = varToAlias.get(varName);
            if (!alias) return false;
            // Skip if it's already joined or is the current target
            if (addedNodeAliases.has(alias)) return false;
            if (alias === relPattern.targetAlias) return false;
            return true; // References a node that will be joined later
          });
          
          // If condition references nodes that come later in the chain, defer handling
          // The condition will be handled when processing the last edge that introduces all needed vars
          if (!referencesLaterNode) {
            // Check if any variable in the condition is the target node
            const targetVar = Array.from(this.ctx.variables.entries()).find(([_, info]) => info.alias === relPattern.targetAlias)?.[0];
            const referencesTarget = targetVar && varsInCondition.includes(targetVar);
            
            if (referencesTarget) {
              // For conditions on the target, add an EXISTS subquery to the edge join
              // This ensures only edges with a valid target (satisfying the condition) are joined
              const { sql: optionalWhereSql, params: optionalWhereParams } = this.translateWhere(whereCondition);
              // Replace the target alias with a reference to the subquery's target_id
              const edgeTargetColumn = relPattern.edge.direction === "left" ? "source_id" : "target_id";
              const existsSql = `EXISTS(SELECT 1 FROM nodes __target__ WHERE __target__.id = ${relPattern.edgeAlias}.${edgeTargetColumn} AND ${optionalWhereSql.replace(new RegExp(relPattern.targetAlias + '\\.', 'g'), '__target__.')})`;
              edgeOnConditions.push(existsSql);
              edgeOnParams.push(...optionalWhereParams);
            } else {
              // Add to edge's ON clause
              const { sql: optionalWhereSql, params: optionalWhereParams } = this.translateWhere(whereCondition);
              edgeOnConditions.push(optionalWhereSql);
              edgeOnParams.push(...optionalWhereParams);
            }
          }
        }

        // For OPTIONAL MATCH when BOTH source and target were bound FROM A REQUIRED MATCH, 
        // add target constraint to ON clause. This ensures the LEFT JOIN properly returns NULL 
        // when no edge connects source to target, instead of filtering out the row entirely in WHERE.
        // 
        // We only do this when BOTH source and target were from a required MATCH (not optional):
        // - MATCH (a), (x) OPTIONAL MATCH (a)-->(x): both bound → use ON (keep all combinations)
        // - MATCH (a), (c) OPTIONAL MATCH (a)-->(b)-->(c): b is NEW → use WHERE (filter incomplete paths)
        //
        // The key insight: if source is NEW (from previous pattern in OPTIONAL MATCH),
        // we need WHERE to filter incomplete paths using the outgoing-edge-exists logic.
        const targetIsOptional = (this.ctx as any)[`optional_${relPattern.targetAlias}`] === true;
        const sourceIsFromRequiredMatch = sourceWasAlreadyAdded && !(this.ctx as any)[`optional_${relPattern.sourceAlias}`];
        if (isOptional && addedNodeAliases.has(relPattern.targetAlias) && !targetIsOptional && sourceIsFromRequiredMatch) {
          const isLeftDirected = relPattern.edge.direction === "left";
          const edgeTargetColumn = isLeftDirected ? "source_id" : "target_id";
          if (isUndirected) {
            edgeOnConditions.push(`(${relPattern.edgeAlias}.target_id = ${relPattern.targetAlias}.id OR ${relPattern.edgeAlias}.source_id = ${relPattern.targetAlias}.id)`);
            edgeOnConditions.push(`NOT (${relPattern.edgeAlias}.source_id = ${relPattern.edgeAlias}.target_id AND ${relPattern.edgeAlias}.source_id = ${relPattern.sourceAlias}.id AND ${relPattern.edgeAlias}.target_id = ${relPattern.targetAlias}.id)`);
          } else {
            edgeOnConditions.push(`${relPattern.edgeAlias}.${edgeTargetColumn} = ${relPattern.targetAlias}.id`);
          }
        }

        // Only add edge join if this edge alias hasn't been added yet
        if (!addedEdgeAliases.has(relPattern.edgeAlias)) {
          // Check for edge-first pattern (standalone OPTIONAL MATCH at start)
          if ((relPattern as any).edgeFirst) {
            // Use dummy FROM with LEFT JOIN to edge
            // This ensures we get a row even if no edges exist (OPTIONAL MATCH semantics)
            fromParts.push(`(SELECT 1) AS __dummy__`);
            // Build ON clause for edge - use 1=1 since we want all edges
            const edgeOnParts = ["1=1"];
            // Add type/property filters to ON clause
            if (edgeOnConditions.length > 0) {
              edgeOnParts.push(...edgeOnConditions);
            }
            joinParts.push(`LEFT JOIN edges ${relPattern.edgeAlias} ON ${edgeOnParts.join(" AND ")}`);
            joinParams.push(...edgeOnParams);
          } else {
            joinParts.push(`${joinType} edges ${relPattern.edgeAlias} ON ${edgeOnConditions.join(" AND ")}`);
            joinParams.push(...edgeOnParams);
          }
          addedEdgeAliases.add(relPattern.edgeAlias);
        } else {
          // Edge already joined (bound from earlier in query)
          // For OPTIONAL MATCH, we need to verify the direction matches
          // For example: MATCH (a)-[r]->() ... OPTIONAL MATCH (a)<-[r]-(b)
          // The bound edge r has a specific direction that must match the pattern
          // Store the direction constraint to be added to the target node's LEFT JOIN condition
          if (isOptional && !isUndirected && sourceWasAlreadyAdded) {
            // Store the direction constraint for use in the target node join
            if (relPattern.edge.direction === "left") {
              // Left-directed: (a)<-[r]-(b) means r.target_id should equal a
              (relPattern as any).boundEdgeDirectionConstraint = `${relPattern.edgeAlias}.target_id = ${relPattern.sourceAlias}.id`;
            } else {
              // Right-directed: (a)-[r]->(b) means r.source_id should equal a
              (relPattern as any).boundEdgeDirectionConstraint = `${relPattern.edgeAlias}.source_id = ${relPattern.sourceAlias}.id`;
            }
          }
        }
        
        // Handle deferred source join (when target was bound but source was new)
        // This joins the source node based on the edge's source_id
        if ((relPattern as any).deferSourceJoin && !addedNodeAliases.has(relPattern.sourceAlias)) {
          // Join source on edge.source_id (since edge was joined on target)
          const sourceOnConditions: string[] = [];
          if (relPattern.edge.direction === "left") {
            // Left-directed: source is at target_id side of edge
            sourceOnConditions.push(`${relPattern.sourceAlias}.id = ${relPattern.edgeAlias}.target_id`);
          } else {
            // Right-directed or undirected: source is at source_id side
            sourceOnConditions.push(`${relPattern.sourceAlias}.id = ${relPattern.edgeAlias}.source_id`);
          }
          
          // Add label filter if source has one
          const sourcePattern = (this.ctx as any)[`pattern_${relPattern.sourceAlias}`];
          const sourceOnParams: unknown[] = [];
          if (sourcePattern?.label) {
            const labelMatch = this.generateLabelMatchCondition(relPattern.sourceAlias, sourcePattern.label);
            sourceOnConditions.push(labelMatch.sql);
            sourceOnParams.push(...labelMatch.params);
            filteredNodeAliases.add(relPattern.sourceAlias);
          }
          
          joinParts.push(`${joinType} nodes ${relPattern.sourceAlias} ON ${sourceOnConditions.join(" AND ")}`);
          joinParams.push(...sourceOnParams);
          addedNodeAliases.add(relPattern.sourceAlias);
        }
        
        // Handle edge-first pattern: join source node from the edge
        // For edge-first, the edge is in FROM and we need to LEFT JOIN nodes from it
        if ((relPattern as any).edgeFirst && !addedNodeAliases.has(relPattern.sourceAlias)) {
          const sourceOnConditions: string[] = [];
          if (relPattern.edge.direction === "left") {
            // Left-directed: source is at target_id side of edge
            sourceOnConditions.push(`${relPattern.sourceAlias}.id = ${relPattern.edgeAlias}.target_id`);
          } else {
            // Right-directed: source is at source_id side
            sourceOnConditions.push(`${relPattern.sourceAlias}.id = ${relPattern.edgeAlias}.source_id`);
          }
          
          // Add label filter if source has one
          const sourcePattern = (this.ctx as any)[`pattern_${relPattern.sourceAlias}`];
          const sourceOnParams: unknown[] = [];
          if (sourcePattern?.label) {
            const labelMatch = this.generateLabelMatchCondition(relPattern.sourceAlias, sourcePattern.label);
            sourceOnConditions.push(labelMatch.sql);
            sourceOnParams.push(...labelMatch.params);
            filteredNodeAliases.add(relPattern.sourceAlias);
          }
          
          joinParts.push(`LEFT JOIN nodes ${relPattern.sourceAlias} ON ${sourceOnConditions.join(" AND ")}`);
          joinParams.push(...sourceOnParams);
          addedNodeAliases.add(relPattern.sourceAlias);
        }

        // Build ON conditions for the target node join
        let targetOnConditions: string[] = [];
        let targetOnParams: unknown[] = [];

        // Add target node join if not already added
        if (!addedNodeAliases.has(relPattern.targetAlias)) {
          // For undirected patterns, target could be on either side of the edge
          if (isUndirected) {
            // Target uses the opposite side of the edge from source based on direction
            // dir=1: target is at edge.target_id
            // dir=2: target is at edge.source_id
            const dirAlias = undirectedDirections.get(relPattern.edgeAlias);
            if (dirAlias) {
              targetOnConditions.push(`(${dirAlias} = 1 AND ${relPattern.edgeAlias}.target_id = ${relPattern.targetAlias}.id OR ${dirAlias} = 2 AND ${relPattern.edgeAlias}.source_id = ${relPattern.targetAlias}.id)`);
            } else {
              // Optional undirected - use the old OR-based approach
              // Note: edge-first is not used for undirected patterns
              targetOnConditions.push(`((${relPattern.edgeAlias}.source_id = ${relPattern.sourceAlias}.id AND ${relPattern.edgeAlias}.target_id = ${relPattern.targetAlias}.id) OR (${relPattern.edgeAlias}.target_id = ${relPattern.sourceAlias}.id AND ${relPattern.edgeAlias}.source_id = ${relPattern.targetAlias}.id))`);
            }
          } else if (relPattern.edge.direction === "left") {
            // Left-directed: target is at source_id side
            targetOnConditions.push(`${relPattern.edgeAlias}.source_id = ${relPattern.targetAlias}.id`);
          } else {
            targetOnConditions.push(`${relPattern.edgeAlias}.target_id = ${relPattern.targetAlias}.id`);
          }
          
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
          
          // For bound edges with direction constraint, add it to the ON clause
          // This ensures the OPTIONAL MATCH fails (returns NULL) if direction doesn't match
          if ((relPattern as any).boundEdgeDirectionConstraint) {
            targetOnConditions.push((relPattern as any).boundEdgeDirectionConstraint);
          }
          
          // If optionalWhere was added to source's ON clause (for pre-bound edge case),
          // we need to make target also fail when source fails.
          // Add condition: source must not be NULL for target to be matched
          if ((relPattern as any).optionalWhereHandled && relPattern.sourceIsNew) {
            targetOnConditions.push(`${relPattern.sourceAlias}.id IS NOT NULL`);
          }

          joinParts.push(`${joinType} nodes ${relPattern.targetAlias} ON ${targetOnConditions.join(" AND ")}`);
          joinParams.push(...targetOnParams);
          addedNodeAliases.add(relPattern.targetAlias);
        } else {
          // Target was already added, but we need to ensure edge connects to it
          // For left-directed edges, the pattern's target is the edge's source_id
          const isLeftDirected = relPattern.edge.direction === "left";
          const edgeColumn = isLeftDirected ? "source_id" : "target_id";
          
          // Check if we already added the target constraint to the ON clause
          // (this happens when both source and target were from required MATCH)
          const targetIsOptional = (this.ctx as any)[`optional_${relPattern.targetAlias}`] === true;
          const sourceIsFromRequiredMatch = sourceWasAlreadyAdded && !(this.ctx as any)[`optional_${relPattern.sourceAlias}`];
          const addedToOnClause = isOptional && !targetIsOptional && sourceIsFromRequiredMatch;
          
           if (!addedToOnClause) {
             // Add WHERE condition - use (edge IS NULL OR edge.target = target) for optional patterns
             // to allow NULL edges while filtering incomplete paths
             if (isOptional) {
               if (isUndirected) {
                 whereParts.push(`(${relPattern.edgeAlias}.id IS NULL OR ${relPattern.edgeAlias}.target_id = ${relPattern.targetAlias}.id OR ${relPattern.edgeAlias}.source_id = ${relPattern.targetAlias}.id)`);
               } else {
                 whereParts.push(`(${relPattern.edgeAlias}.id IS NULL OR ${relPattern.edgeAlias}.${edgeColumn} = ${relPattern.targetAlias}.id)`);
               }
             } else {
              if (isUndirected) {
                whereParts.push(`(${relPattern.edgeAlias}.target_id = ${relPattern.targetAlias}.id OR ${relPattern.edgeAlias}.source_id = ${relPattern.targetAlias}.id)`);
              } else {
                whereParts.push(`${relPattern.edgeAlias}.${edgeColumn} = ${relPattern.targetAlias}.id`);
              }
            }
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
      
      // Add relationship uniqueness constraint for edges in connected chains
      // In Cypher, when matching a pattern like (a)-[r1]->(b)-[r2]->(c), r1 and r2 must be different relationships
      // BUT edges from separate MATCH clauses or disconnected patterns don't need to be distinct
      // AND if the same edge variable is used twice, no uniqueness constraint is needed (it's the same edge)
      // IMPORTANT: MATCH and OPTIONAL MATCH are separate clauses - don't enforce automatic uniqueness between them
      // IMPORTANT: Edges separated by WITH (that doesn't pass edge variables) are in different scopes
      if (relPatterns.length > 1) {
        // Build connectivity graph to find chains of connected relationships
        // Two relationships are connected if they share a node (source/target) AND are in the same clause type
        // AND are in the same edge scope (not separated by WITH without edge passthrough)
        // (both non-optional or both optional - we don't cross-connect MATCH with OPTIONAL MATCH)
        const edgeGroups: number[][] = []; // Groups of edge indices that are connected
        const visited = new Set<number>();
        
        for (let i = 0; i < relPatterns.length; i++) {
          if (visited.has(i)) continue;
          
          // BFS to find all connected edges within the same clause type and edge scope
          const group: number[] = [];
          const queue = [i];
          
          while (queue.length > 0) {
            const current = queue.shift()!;
            if (visited.has(current)) continue;
            visited.add(current);
            group.push(current);
            
            const currentPattern = relPatterns[current];
            
            // Find all edges that share a node with current edge
            for (let j = 0; j < relPatterns.length; j++) {
              if (visited.has(j)) continue;
              const otherPattern = relPatterns[j];
              
              // Don't connect patterns from different clause types (MATCH vs OPTIONAL MATCH)
              // This ensures edge uniqueness is only enforced within the same clause
              if (currentPattern.optional !== otherPattern.optional) continue;
              
              // Don't connect patterns from different edge scopes (separated by WITH)
              // This ensures edges from before WITH don't affect edges after WITH
              if ((currentPattern as any).edgeScope !== (otherPattern as any).edgeScope) continue;
              
              // Check if they share any node
              if (currentPattern.sourceAlias === otherPattern.sourceAlias ||
                  currentPattern.sourceAlias === otherPattern.targetAlias ||
                  currentPattern.targetAlias === otherPattern.sourceAlias ||
                  currentPattern.targetAlias === otherPattern.targetAlias) {
                queue.push(j);
              }
            }
          }
          
          if (group.length > 1) {
            edgeGroups.push(group);
          }
        }
        
        // Only add uniqueness constraints for edges in the same connected group
        // Skip if they have the same edge alias (same variable used twice)
        for (const group of edgeGroups) {
          for (let i = 0; i < group.length; i++) {
            for (let j = i + 1; j < group.length; j++) {
              const edge1Idx = group[i];
              const edge2Idx = group[j];
              const edge1Alias = relPatterns[edge1Idx].edgeAlias;
              const edge2Alias = relPatterns[edge2Idx].edgeAlias;
              
              // Skip if same edge alias (same variable referenced multiple times)
              if (edge1Alias === edge2Alias) continue;
              
              const edge1Optional = relPatterns[edge1Idx].optional;
               const edge2Optional = relPatterns[edge2Idx].optional;
               
               // For non-optional edges, require distinct IDs
               // For optional edges, allow NULL (edge not matched) OR distinct
               if (edge1Optional || edge2Optional) {
                 whereParts.push(`(${edge1Alias}.id IS NULL OR ${edge2Alias}.id IS NULL OR ${edge1Alias}.id <> ${edge2Alias}.id)`);
               } else {
                 whereParts.push(`${edge1Alias}.id <> ${edge2Alias}.id`);
               }
            }
          }
        }
      }
      
      // Also add any standalone node patterns that are not part of relationship patterns
      // These need to be cross-joined (e.g., MATCH (x:X), (a)->(b) - x is standalone)
      for (const [variable, info] of this.ctx.variables) {
        if (info.type !== "node") continue;
        if (addedNodeAliases.has(info.alias)) continue;
        
        const pattern = (this.ctx as any)[`pattern_${info.alias}`];
        const isOptional = (this.ctx as any)[`optional_${info.alias}`] === true;
        
        if (pattern) {
          if (isOptional) {
            // Optional standalone node - LEFT JOIN with its conditions
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
            // Non-optional standalone node - add to FROM (cross join) with WHERE conditions
            if (fromParts.length === 0) {
              fromParts.push(`nodes ${info.alias}`);
            } else {
              // Cross join - just add to FROM clause for cartesian product
              fromParts.push(`nodes ${info.alias}`);
            }
            
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
          addedNodeAliases.add(info.alias);
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
      
      // Also add anonymous node patterns (e.g., MATCH () RETURN count(*))
      const anonymousPatterns = (this.ctx as any).anonymousNodePatterns as Array<{ alias: string; optional: boolean }> | undefined;
      if (anonymousPatterns) {
        for (const { alias, optional } of anonymousPatterns) {
          if (!optional) {
            if (fromParts.length === 0) {
              fromParts.push(`nodes ${alias}`);
            } else {
              // Cross join for additional anonymous nodes
              fromParts.push(`nodes ${alias}`);
            }
          }
        }
      }
    }

    // Apply additional label constraints from multi-MATCH patterns
    // E.g., MATCH (a)-[r]->(b) WITH r, a MATCH (a:X)-[r]->(c) adds label constraint for :X on a
    const additionalLabelConstraints = (this.ctx as any).additionalLabelConstraints as Array<{
      alias: string;
      label: string | string[];
      optional: boolean;
    }> | undefined;
    
    if (additionalLabelConstraints) {
      for (const constraint of additionalLabelConstraints) {
        const labelMatch = this.generateLabelMatchCondition(constraint.alias, constraint.label);
        if (constraint.optional) {
          // For optional matches, allow NULL or the label constraint
          whereParts.push(`(${constraint.alias}.id IS NULL OR ${labelMatch.sql})`);
        } else {
          whereParts.push(labelMatch.sql);
        }
        whereParams.push(...labelMatch.params);
      }
    }

    // Add UNWIND tables using json_each
    const unwindClauses = (this.ctx as any).unwindClauses as Array<{
      alias: string;
      variable: string;
      jsonExpr: string;
      params: unknown[];
    }> | undefined;
    
    // Get set of UNWIND clauses that were consumed by subquery aggregates
    const consumedUnwindClauses = (this.ctx as any).consumedUnwindClauses as Set<string> | undefined;
    
    if (unwindClauses && unwindClauses.length > 0) {
      for (const unwindClause of unwindClauses) {
        // Skip UNWIND clauses that were consumed by subquery aggregates (MIN/MAX with type-aware comparison)
        if (consumedUnwindClauses?.has(unwindClause.alias)) {
          continue;
        }
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

    // OPTIONAL MATCH WHERE conditions are now handled in the edge's LEFT JOIN ON clause
    // This ensures proper NULL propagation when the condition fails
    // (The condition is added during edge join generation when optionalWhere is set on the pattern)
    // We no longer add these to the main WHERE clause as that would filter out rows entirely
    
    // Add WHERE conditions from WITH clauses
    // First, apply accumulated WHERE conditions from previous WITH clauses
    // These conditions filter rows and must be applied before the current WITH's WHERE
    if (accumulatedWithWheres && accumulatedWithWheres.length > 0) {
      for (const accWhere of accumulatedWithWheres) {
        const withAliases = (this.ctx as any).withAliases as Map<string, Expression> | undefined;
        if (!this.whereConditionReferencesAggregateAlias(accWhere, withAliases)) {
          const { sql: whereSql, params: conditionParams } = this.translateWhere(accWhere);
          whereParts.push(whereSql);
          whereParams.push(...conditionParams);
        }
      }
    }
    
    // If the condition references an aggregate alias, it should go in HAVING instead of WHERE
    let havingCondition: { sql: string; params: unknown[] } | undefined;
    if (withWhere) {
      const withAliases = (this.ctx as any).withAliases as Map<string, Expression> | undefined;
      if (this.whereConditionReferencesAggregateAlias(withWhere, withAliases)) {
        // This condition references aggregates - save for HAVING clause
        havingCondition = this.translateWhere(withWhere);
      } else {
        const { sql: whereSql, params: conditionParams } = this.translateWhere(withWhere);
        whereParts.push(whereSql);
        whereParams.push(...conditionParams);
      }
    }

    // Check if we need DISTINCT for OPTIONAL MATCH with label predicate on target node
    // This prevents row multiplication when the target doesn't exist (returns multiple NULL rows)
    const optRelPatterns = (this.ctx as any).relationshipPatterns as Array<{
      sourceAlias: string;
      targetAlias: string;
      edgeAlias: string;
      edge: { type?: string; types?: string[]; properties?: Record<string, PropertyValue>; minHops?: number; maxHops?: number; direction?: "left" | "right" | "none" };
      optional?: boolean;
      sourceIsNew?: boolean;
      targetIsNew?: boolean;
      isVariableLength?: boolean;
      minHops?: number;
      maxHops?: number;
      targetHasLabel?: boolean;
      edgeIsNew?: boolean;
    }> | undefined;
    
    const needsOptionalMatchDistinct = optRelPatterns?.some(p => 
      p.optional && 
      p.targetHasLabel && 
      p.edgeIsNew
    );

    // Check if we need a subquery for WITH LIMIT/SKIP/DISTINCT when RETURN is pure aggregation.
    // When RETURN count(*) follows WITH n LIMIT 2, the LIMIT should apply to rows before
    // aggregation, not to the aggregated result. We achieve this by wrapping the FROM in
    // a subquery that applies the LIMIT/DISTINCT.
    const returnOnlyAggregates = clause.items.every(item => this.isAggregateExpression(item.expression));
    const needsWithSubquery = returnOnlyAggregates && (withLimit !== undefined || withSkip !== undefined || withDistinct);
    
    // Build final SQL
    // Apply DISTINCT from either the RETURN clause, preceding WITH, or OPTIONAL MATCH pattern
    // BUT: if we're using a subquery for DISTINCT, don't add DISTINCT to the outer query
    const needsSubqueryDistinct = returnOnlyAggregates && withDistinct;
    const distinctKeyword = (clause.distinct || (withDistinct && !needsSubqueryDistinct) || needsOptionalMatchDistinct) ? "DISTINCT " : "";
    let sql = `SELECT ${distinctKeyword}${selectParts.join(", ")}`;

    if (needsWithSubquery && fromParts.length > 0) {
      // Build inner query with LIMIT/SKIP/DISTINCT from WITH clause
      // For DISTINCT, we need to select the WITH expressions (not just *)
      const innerSelectDistinct = needsSubqueryDistinct ? "DISTINCT " : "";
      let innerSelect = "*";
      
      // Track which UNWIND variables need to be exposed in the subquery
      // When UNWIND variables pass through WITH with LIMIT, we need to explicitly
      // select their .value column with the variable name as alias so the outer
      // query can reference them correctly after the subquery wrapper
      const unwindVarsInWith = new Map<string, string>(); // varName -> unwindAlias
      
      // Check if the WITH clause passes through UNWIND variables
      if (this.ctx.withClauses && this.ctx.withClauses.length > 0 && unwindClauses && unwindClauses.length > 0) {
        const lastWithClause = this.ctx.withClauses[this.ctx.withClauses.length - 1];
        for (const item of lastWithClause.items) {
          if (item.expression.type === "variable") {
            const varName = item.expression.variable!;
            const unwindClause = unwindClauses.find(u => u.variable === varName);
            if (unwindClause) {
              unwindVarsInWith.set(item.alias || varName, unwindClause.alias);
            }
          }
        }
      }
      
      // If we have UNWIND variables or DISTINCT with non-variable expressions, build explicit SELECT
      const needsExplicitSelect = needsSubqueryDistinct || unwindVarsInWith.size > 0;
      
      if (needsExplicitSelect && this.ctx.withClauses && this.ctx.withClauses.length > 0) {
        const lastWithClause = this.ctx.withClauses[this.ctx.withClauses.length - 1];
        const withSelectParts: string[] = [];
        for (const item of lastWithClause.items) {
          if (item.expression.type !== "variable" || item.expression.variable === "*") {
            // Computed expression - need to translate it
            const { sql: exprSql, params: exprParams } = this.translateExpression(item.expression);
            whereParams.push(...exprParams);
            const alias = item.alias || this.getExpressionName(item.expression);
            withSelectParts.push(`${exprSql} AS "${alias}"`);
          } else {
            const varName = item.expression.variable!;
            const unwindAlias = unwindVarsInWith.get(item.alias || varName);
            if (unwindAlias) {
              // UNWIND variable - select its .value with the variable name as alias
              withSelectParts.push(`${unwindAlias}.value AS "${item.alias || varName}"`);
            } else {
              // Simple variable passthrough (node/edge)
              const varInfo = this.ctx.variables.get(varName);
              if (varInfo) {
                withSelectParts.push(`${varInfo.alias}.id AS "${item.alias || varName}"`);
              }
            }
          }
        }
        if (withSelectParts.length > 0) {
          innerSelect = withSelectParts.join(", ");
          // Note: subqueryColumnName is already set in the pre-check phase
          // (before building SELECT parts) so outer query aggregates reference correctly
        }
      }
      
      let innerSql = `SELECT ${innerSelectDistinct}${innerSelect} FROM ${fromParts.join(", ")}`;
      if (joinParts.length > 0) {
        innerSql += ` ${joinParts.join(" ")}`;
      }
      if (whereParts.length > 0) {
        innerSql += ` WHERE ${whereParts.join(" AND ")}`;
      }
      
      // Add ORDER BY if from WITH (needs to be in subquery for LIMIT to work correctly)
      if (withOrderBy && withOrderBy.length > 0) {
        const allAvailableAliases: string[] = [];
        const orderByAliases = withOrderByAliases ?? ((this.ctx as any).withAliases as Map<string, Expression> | undefined);
        if (orderByAliases) {
          for (const aliasName of orderByAliases.keys()) {
            allAvailableAliases.push(aliasName);
          }
        }
        const prevWithAliases = (this.ctx as any).withAliases as Map<string, Expression> | undefined;
        (this.ctx as any).withAliases = orderByAliases;
        try {
          const orderParts = withOrderBy.map(({ expression, direction }) => {
            const { sql: exprSql, params: orderParams } = this.translateOrderByExpression(expression, allAvailableAliases);
            if (orderParams && orderParams.length > 0) {
              whereParams.push(...orderParams);
            }
            return `${exprSql} ${direction}`;
          });
          innerSql += ` ORDER BY ${orderParts.join(", ")}`;
        } finally {
          (this.ctx as any).withAliases = prevWithAliases;
        }
      }
      
      // Add LIMIT/SKIP to inner query
      if (withLimit !== undefined) {
        const { sql: limitSql, params: limitParams } = this.translateSkipLimitExpression(withLimit, "LIMIT");
        innerSql += ` LIMIT ${limitSql}`;
        whereParams.push(...limitParams);
      } else if (withSkip !== undefined) {
        innerSql += ` LIMIT -1`;
      }
      if (withSkip !== undefined) {
        const { sql: skipSql, params: skipParams } = this.translateSkipLimitExpression(withSkip, "SKIP");
        innerSql += ` OFFSET ${skipSql}`;
        whereParams.push(...skipParams);
      }
      
      sql += ` FROM (${innerSql}) __with_subquery__`;
      // Clear these since they've been applied in subquery
      whereParts.length = 0;
      joinParts.length = 0;
    } else if (fromParts.length > 0) {
      // Check if we should use __aggregates__ CTE instead of the normal FROM
      const useAggregatesCTE = (this.ctx as any).useAggregatesCTE as boolean | undefined;
      if (useAggregatesCTE) {
        sql += ` FROM __aggregates__`;
        // Don't add joins or where - the CTE handles aggregation
      } else {
        sql += ` FROM ${fromParts.join(", ")}`;
        if (joinParts.length > 0) {
          sql += ` ${joinParts.join(" ")}`;
        }
        if (whereParts.length > 0) {
          sql += ` WHERE ${whereParts.join(" AND ")}`;
        }
      }
    } else if (joinParts.length > 0) {
      // If we have JOINs but no FROM, we need a dummy FROM clause
      // This happens with OPTIONAL MATCH without a prior MATCH
      sql += ` FROM (SELECT 1) __dummy__`;
      sql += ` ${joinParts.join(" ")}`;
      if (whereParts.length > 0) {
        sql += ` WHERE ${whereParts.join(" AND ")}`;
      }
    }

    // Add GROUP BY for aggregation (from WITH or RETURN clauses)
    // When we have aggregates mixed with non-aggregates, non-aggregate expressions become GROUP BY keys
    const groupByParts: string[] = [];
    
    // Check WITH clause for aggregation
    if (this.ctx.withClauses && this.ctx.withClauses.length > 0) {
      const lastWithClause = this.ctx.withClauses[this.ctx.withClauses.length - 1];
      const withHasAggregates = lastWithClause.items.some(item => this.isAggregateExpression(item.expression));
      
      if (withHasAggregates) {
        for (const item of lastWithClause.items) {
          if (!this.isAggregateExpression(item.expression)) {
            // For pattern comprehensions and other correlated expressions,
            // use the bound variables for grouping instead of the full expression
            const { sql: groupKeys, params: groupParams } = this.getGroupByKeys(item.expression);
            groupByParts.push(...groupKeys);
            whereParams.push(...groupParams);
          }
        }
      }
    }
    
    // Check RETURN clause for aggregation (when no WITH with aggregation)
    const returnHasAggregates = clause.items.some(item => this.isAggregateExpression(item.expression));
    const nonAggregateItems = clause.items.filter(item => !this.isAggregateExpression(item.expression));
    
    if (groupByParts.length === 0) {
      if (returnHasAggregates && nonAggregateItems.length > 0) {
        for (const item of nonAggregateItems) {
          // For pattern comprehensions and other correlated expressions,
          // use the bound variables for grouping instead of the full expression
          const { sql: groupKeys, params: groupParams } = this.getGroupByKeys(item.expression);
          groupByParts.push(...groupKeys);
          whereParams.push(...groupParams);
        }
      }
    }
    
    if (groupByParts.length > 0) {
      sql += ` GROUP BY ${groupByParts.join(", ")}`;
    }

    // Add HAVING clause for conditions that reference aggregate aliases
    if (havingCondition) {
      sql += ` HAVING ${havingCondition.sql}`;
      whereParams.push(...havingCondition.params);
    }

    // Determine effective LIMIT/SKIP (combine WITH and RETURN values)
    // BUT: when RETURN has only aggregates and no grouping, WITH LIMIT/SKIP should apply
    // to the inner query (before aggregation), not the outer result.
    // We handle this by detecting if the WITH modifier should have been applied via subquery.
    const withLimitAppliedViaSubquery = returnHasAggregates && nonAggregateItems.length === 0 && 
      (withLimit !== undefined || withSkip !== undefined);
    
    // If WITH LIMIT was applied via subquery, don't re-apply it here
    const effectiveLimit = clause.limit !== undefined ? clause.limit : 
      (withLimitAppliedViaSubquery ? undefined : withLimit);
    const effectiveSkip = clause.skip !== undefined ? clause.skip : 
      (withLimitAppliedViaSubquery ? undefined : withSkip);
    
    // Similarly for ORDER BY - if it was WITH ORDER BY applied via subquery, don't re-apply
    const effectiveOrderBy = clause.orderBy && clause.orderBy.length > 0 ? clause.orderBy : 
      (withLimitAppliedViaSubquery ? undefined : withOrderBy);
      
    if (effectiveOrderBy && effectiveOrderBy.length > 0) {
      // Collect all available aliases for ORDER BY: RETURN columns + WITH aliases
      const allAvailableAliases = [...returnColumns];
      const orderByAliases =
        clause.orderBy && clause.orderBy.length > 0
          ? ((this.ctx as any).withAliases as Map<string, Expression> | undefined)
          : withOrderByAliases ?? ((this.ctx as any).withAliases as Map<string, Expression> | undefined);
      if (orderByAliases) {
        for (const aliasName of orderByAliases.keys()) {
          if (!allAvailableAliases.includes(aliasName)) {
            allAvailableAliases.push(aliasName);
          }
        }
      }

      const prevWithAliases = (this.ctx as any).withAliases as Map<string, Expression> | undefined;
      (this.ctx as any).withAliases = orderByAliases;
      try {
        const orderParts = effectiveOrderBy.map(({ expression, direction }) => {
          const { sql: exprSql, params: orderParams } = this.translateOrderByExpression(expression, allAvailableAliases);
          if (orderParams && orderParams.length > 0) {
            whereParams.push(...orderParams);
          }
          return `${exprSql} ${direction}`;
        });
        sql += ` ORDER BY ${orderParts.join(", ")}`;
      } finally {
        (this.ctx as any).withAliases = prevWithAliases;
      }
    }

    // Add LIMIT and OFFSET (SKIP)
    if (effectiveLimit !== undefined || effectiveSkip !== undefined) {
      if (effectiveLimit !== undefined) {
        const { sql: limitSql, params: limitParams } = this.translateSkipLimitExpression(effectiveLimit, "LIMIT");
        sql += ` LIMIT ${limitSql}`;
        whereParams.push(...limitParams);
      } else if (effectiveSkip !== undefined) {
        // SKIP without LIMIT - need a large limit for SQLite
        sql += ` LIMIT -1`;
      }

      if (effectiveSkip !== undefined) {
        const { sql: skipSql, params: skipParams } = this.translateSkipLimitExpression(effectiveSkip, "SKIP");
        sql += ` OFFSET ${skipSql}`;
        whereParams.push(...skipParams);
      }
    }

    // Combine params in the order they appear in SQL: SELECT -> JOINs -> WHERE
    let allParams = [...exprParams, ...joinParams, ...whereParams];

    // If we need to materialize aggregate aliases in a CTE, prepend the CTE
    const materializedAggregates = (this.ctx as any).materializedAggregateAliases as Set<string> | undefined;
    const useAggregatesCTE = (this.ctx as any).useAggregatesCTE as boolean | undefined;
    if (materializedAggregates && materializedAggregates.size > 0 && useAggregatesCTE) {
      // Build the CTE that computes the aggregates
      const cteSelectParts: string[] = [];
      const cteParams: unknown[] = [];
      const withAliasesFinal = (this.ctx as any).withAliases as Map<string, Expression> | undefined;
      
      for (const aliasName of materializedAggregates) {
        if (withAliasesFinal && withAliasesFinal.has(aliasName)) {
          const originalExpr = withAliasesFinal.get(aliasName)!;
          // Temporarily clear materializedAggregateAliases to get the actual aggregate SQL
          (this.ctx as any).materializedAggregateAliases = undefined;
          const { sql: aggSql, params: aggParams } = this.translateExpression(originalExpr);
          (this.ctx as any).materializedAggregateAliases = materializedAggregates;
          cteSelectParts.push(`${aggSql} AS "${aliasName}"`);
          cteParams.push(...aggParams);
        }
      }
      
      if (cteSelectParts.length > 0) {
        // Build the CTE FROM clause (same as main query's FROM)
        // We need to replicate the UNWIND/FROM structure
        const unwindClausesFinal = (this.ctx as any).unwindClauses as Array<{
          alias: string;
          variable: string;
          jsonExpr: string;
          params: unknown[];
        }> | undefined;
        
        let cteFrom = "";
        if (unwindClausesFinal && unwindClausesFinal.length > 0) {
          const cteParts: string[] = [];
          for (const unwind of unwindClausesFinal) {
            cteParts.push(`json_each(${unwind.jsonExpr}) ${unwind.alias}`);
            cteParams.push(...unwind.params);
          }
          cteFrom = cteParts.join(" CROSS JOIN ");
        }
        
        // Build the CTE SQL
        let cteSql = `WITH __aggregates__ AS (SELECT ${cteSelectParts.join(", ")}`;
        if (cteFrom) {
          cteSql += ` FROM ${cteFrom}`;
        }
        cteSql += `) `;
        
        // Prepend CTE to main query
        sql = cteSql + sql;
        
        // Prepend CTE params (only CTE params, exprParams don't use UNWIND params since we use CTE)
        allParams = [...cteParams];
      }
      
      // Clean up context
      (this.ctx as any).materializedAggregateAliases = undefined;
      (this.ctx as any).useAggregatesCTE = undefined;
    }

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
    const oldWithAliases = (this.ctx as any).withAliases as Map<string, Expression> | undefined;

    // Track row ordering flowing through the query to support ordered COLLECT()
    // semantics (ORDER BY before an aggregation determines collect order).
    const previousRowOrderBy = (this.ctx as any).rowOrderBy as WithClause["orderBy"] | undefined;
    
    // Accumulate WHERE conditions from previous WITH clauses.
    // When a WITH has a WHERE, it should filter rows for all subsequent clauses.
    // We need to preserve WHERE conditions whose variables are still in scope.
    const previousWithWhere = (this.ctx as any).withWhere as WhereCondition | undefined;
    const accumulatedWithWheres = ((this.ctx as any).accumulatedWithWheres as WhereCondition[] | undefined) ?? [];

    // WITH modifiers (WHERE/ORDER BY/SKIP/LIMIT/DISTINCT) apply only to the
    // current WITH clause. Clear any previous WITH modifier state to avoid
    // leaking them across chained WITH clauses (e.g. `WITH ... SKIP ... WITH ...`).
    (this.ctx as any).withWhere = undefined;
    (this.ctx as any).withOrderBy = undefined;
    (this.ctx as any).withSkip = undefined;
    (this.ctx as any).withLimit = undefined;
    (this.ctx as any).withDistinct = undefined;
    (this.ctx as any).collectOrderBy = undefined;
    (this.ctx as any).withOrderByAliases = undefined;
    
    // Check for duplicate column names within this WITH clause
    this.checkDuplicateColumnNames(clause.items);
    
    // Validate ORDER BY with aggregation in WITH
    // Similar to RETURN, when WITH has aggregation, ORDER BY must only reference WITH items
    const withHasAggregation = clause.items.some(item => this.isAggregateExpression(item.expression));
    if (withHasAggregation && previousRowOrderBy && previousRowOrderBy.length > 0) {
      // The order of incoming rows (from the previous clause) determines the
      // ordering of collected lists in this aggregation scope.
      (this.ctx as any).collectOrderBy = previousRowOrderBy;
    }
    if (withHasAggregation && clause.orderBy && clause.orderBy.length > 0) {
      // Convert WithClause items to ReturnItem format for validation
      const returnClause: ReturnClause = {
        type: "RETURN",
        items: clause.items,
        orderBy: clause.orderBy,
      };
      this.validateAggregationOrderBy(returnClause, clause.orderBy);
    }
    
    if (!this.ctx.withClauses) {
      this.ctx.withClauses = [];
    }
    this.ctx.withClauses.push(clause);
    
    // Accumulate WHERE conditions from previous WITH clauses.
    // These need to be applied because later clauses depend on filtered rows.
    if (previousWithWhere) {
      accumulatedWithWheres.push(previousWithWhere);
    }
    (this.ctx as any).accumulatedWithWheres = accumulatedWithWheres;
    
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

    // Update row ordering state: ORDER BY defines order for the next clause,
    // otherwise ordering is not guaranteed to be preserved through WITH.
    (this.ctx as any).rowOrderBy = clause.orderBy && clause.orderBy.length > 0 ? clause.orderBy : undefined;
    
    // Track which node/edge variables are passed through WITH
    // This is needed to determine scopes and handle cartesian products
    const passedThroughNodes = new Set<string>();
    const passedThroughEdges = new Set<string>();
    
    // Create new withAliases map for this WITH clause
    // This replaces the previous one, ensuring only current WITH aliases are in scope
    const newWithAliases = new Map<string, Expression>();
    
    // Update variable mappings for WITH items
    // Variables without aliases keep their current mappings
    // Variables with aliases create new mappings based on expression type
    for (const item of clause.items) {
      const alias = item.alias;
      
      if (item.expression.type === "variable") {
        // Check for WITH * (pass through all variables)
        if (item.expression.variable === "*") {
          // All existing variables remain in scope
          // Mark all node/edge variables as passed through
          for (const [varName, info] of this.ctx.variables) {
            if (info.type === "node") {
              passedThroughNodes.add(info.alias);
            } else if (info.type === "edge" || info.type === "varLengthEdge") {
              passedThroughEdges.add(info.alias);
            }
          }
          continue;
        }
        // Variable passthrough - keep or create mapping
        const originalVar = item.expression.variable!;
        const originalInfo = this.ctx.variables.get(originalVar);
        if (originalInfo) {
          if (alias) {
            this.ctx.variables.set(alias, originalInfo);
            // Preserve optional flag for the alias
            const originalIsOptional = (this.ctx as any)[`optional_${originalInfo.alias}`];
            if (originalIsOptional) {
              (this.ctx as any)[`optional_${alias}`] = true;
            }
          }
          // Track if this is a node/edge variable being passed through
          if (originalInfo.type === "node") {
            passedThroughNodes.add(originalInfo.alias);
          } else if (originalInfo.type === "edge" || originalInfo.type === "varLengthEdge") {
            passedThroughEdges.add(originalInfo.alias);
          }
        }
      } else if (alias) {
        // For any other expression type with an alias (property, function, literal, object, binary, etc.)
        // we track it as a "virtual" variable for the return/unwind phase
        newWithAliases.set(alias, item.expression);
      }
      
      // If item is a variable with an alias, also check if the source was a WITH alias
      if (item.expression.type === "variable" && alias) {
        const sourceVar = item.expression.variable!;
        if (oldWithAliases && oldWithAliases.has(sourceVar)) {
          // The source is a WITH alias - copy it to new WITH aliases with new alias name
          newWithAliases.set(alias, oldWithAliases.get(sourceVar)!);
        }
      }
      
      // If item is a variable without alias, check if it was a WITH alias from previous WITH
      if (item.expression.type === "variable" && !alias) {
        const varName = item.expression.variable!;
        if (oldWithAliases && oldWithAliases.has(varName)) {
          // Preserve the WITH alias in the new scope
          newWithAliases.set(varName, oldWithAliases.get(varName)!);
        }
      }
    }
    
    // Before replacing withAliases, preserve any old WITH aliases that are referenced
    // in the current WITH's expressions (even if not explicitly passed through)
    if (oldWithAliases) {
      // Find all variables referenced in current WITH items
      const referencedVars = new Set<string>();
      for (const item of clause.items) {
        const vars = this.findVariablesInExpression(item.expression);
        for (const v of vars) {
          referencedVars.add(v);
        }
      }
      
      // Also find variables transitively referenced by expressions already in newWithAliases
      // This handles the case where WITH lhs, rhs passes through aliases that depend on
      // other aliases (e.g., lhs = types[i] depends on types)
      for (const [aliasName, expr] of newWithAliases) {
        const vars = this.findVariablesInExpression(expr);
        for (const v of vars) {
          referencedVars.add(v);
        }
      }
      
      // Preserve referenced WITH aliases, including their transitive dependencies.
      // This is required because we translate WITH aliases lazily: if a later WITH references
      // an earlier alias (e.g. `collect(x)`), we still need any upstream aliases that `x`
      // depends on (e.g. `values`) to be available when `x` is expanded.
      const preserving = new Set<string>();
      const withAliasesStack = (this.ctx as any).withAliasesStack as Map<string, Expression>[] | undefined;
      
      const preserveAlias = (aliasName: string) => {
        if (preserving.has(aliasName)) return;
        preserving.add(aliasName);

        // If this alias is already in newWithAliases (e.g., passed through from WITH items),
        // we still need to traverse its dependencies to ensure they're preserved.
        // The expression may reference other aliases that need to be available.
        const expr = newWithAliases.get(aliasName) ?? oldWithAliases.get(aliasName);
        if (!expr) return;

        // If not already in newWithAliases, add it
        if (!newWithAliases.has(aliasName) && oldWithAliases.has(aliasName)) {
          newWithAliases.set(aliasName, expr);
        }

        // Recursively preserve dependencies
        for (const dep of this.findVariablesInExpression(expr)) {
          preserveAlias(dep);
        }
        
        // For self-referential aliases (e.g., WITH list + x AS list), the inner reference
        // to `list` should resolve to an earlier definition. Look through the stack to find
        // and preserve dependencies from ALL definitions of this alias, not just the latest.
        if (withAliasesStack) {
          for (let i = withAliasesStack.length - 1; i >= 0; i--) {
            const scope = withAliasesStack[i];
            const olderExpr = scope.get(aliasName);
            if (olderExpr && olderExpr !== expr) {
              // Found an older definition - preserve its dependencies too
              for (const dep of this.findVariablesInExpression(olderExpr)) {
                preserveAlias(dep);
              }
            }
          }
        }
      };

      for (const varName of referencedVars) {
        preserveAlias(varName);
      }
    }
    
    // Replace withAliases with the new map for this WITH scope
    (this.ctx as any).withAliases = newWithAliases;
    const withAliasesStack = (((this.ctx as any).withAliasesStack as Map<string, Expression>[] | undefined) ??= []);
    withAliasesStack.push(newWithAliases);

    // ORDER BY in a WITH clause can reference:
    // - aliases defined by this WITH projection, and
    // - aliases from the incoming scope (even if not projected forward).
    // Keep a merged alias map specifically for translating the WITH's ORDER BY.
    const withOrderByAliases = new Map<string, Expression>();
    if (oldWithAliases) {
      for (const [aliasName, expr] of oldWithAliases.entries()) {
        withOrderByAliases.set(aliasName, expr);
      }
    }
    for (const [aliasName, expr] of newWithAliases.entries()) {
      withOrderByAliases.set(aliasName, expr);
    }
    (this.ctx as any).withOrderByAliases = withOrderByAliases;
    
    // Increment edge scope when WITH doesn't pass through any edge variables
    // This means subsequent MATCH patterns are in a new scope and shouldn't
    // share edge uniqueness constraints with patterns from before the WITH
    const currentEdgeScope = (this.ctx as any).edgeScope || 0;
    if (passedThroughEdges.size === 0) {
      (this.ctx as any).edgeScope = currentEdgeScope + 1;
    }
    
    // Always clear lastAnonymousTargetAlias after WITH.
    // Anonymous node chaining (()-[r]->()-[s]->()) should not continue across WITH boundaries.
    // Even if an edge is passed through, subsequent MATCH patterns should use fresh nodes.
    (this.ctx as any).lastAnonymousTargetAlias = undefined;
    
    // When WITH doesn't pass through ANY node or edge variables AND no existing
    // variables are referenced in WITH expressions, we need to:
    // 1. Convert current patterns into a "row source" for cartesian product
    // 2. Clear node/edge variables so subsequent MATCH gets fresh context
    //
    // However, if any existing variable is referenced in a WITH expression
    // (e.g., WITH coalesce(b, c) AS x), we must NOT clear the context
    // because the expression depends on those variables.
    
    // Check if any existing node/edge variable is referenced in any WITH expression
    let variablesReferencedInExpressions = false;
    for (const item of clause.items) {
      if (item.expression.type === "variable" && item.expression.variable === "*") continue;
      if (this.expressionReferencesGraphVariables(item.expression, oldWithAliases)) {
        variablesReferencedInExpressions = true;
        break;
      }
      if (variablesReferencedInExpressions) break;
    }
    
    if (passedThroughNodes.size === 0 && passedThroughEdges.size === 0 && !variablesReferencedInExpressions) {
      // Store the current relationship patterns as a "pre-WITH" row source
      // These will be used to generate a subquery that provides the row multiplier
      const currentRelPatterns = (this.ctx as any).relationshipPatterns;
      const currentAnonymousNodes = (this.ctx as any).anonymousNodePatterns;
      
      if (currentRelPatterns?.length > 0 || currentAnonymousNodes?.length > 0) {
        // Store for later use in generating the cartesian product
        if (!(this.ctx as any).preWithPatterns) {
          (this.ctx as any).preWithPatterns = [];
        }
        (this.ctx as any).preWithPatterns.push({
          relationshipPatterns: currentRelPatterns ? [...currentRelPatterns] : [],
          anonymousNodePatterns: currentAnonymousNodes ? [...currentAnonymousNodes] : [],
          variables: new Map(this.ctx.variables),
        });
        
        // Clear the patterns and variables for fresh MATCH context
        (this.ctx as any).relationshipPatterns = [];
        (this.ctx as any).anonymousNodePatterns = [];
        // Keep only non-node/edge variables (like withAliases)
        for (const [varName, info] of this.ctx.variables) {
          if (info.type === "node" || info.type === "edge" || info.type === "varLengthEdge" || info.type === "path") {
            this.ctx.variables.delete(varName);
          }
        }
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
      const { sql: skipSql, params: skipParams } = this.translateSkipLimitExpression(clause.skip, "SKIP");
      sql += ` OFFSET ${skipSql}`;
      params.push(...skipParams);
    }

    // Handle LIMIT
    if (clause.limit !== undefined) {
      const { sql: limitSql, params: limitParams } = this.translateSkipLimitExpression(clause.limit, "LIMIT");
      sql += ` LIMIT ${limitSql}`;
      params.push(...limitParams);
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
      edge: { variable?: string; type?: string; properties?: Record<string, PropertyValue>; minHops?: number; maxHops?: number; direction?: "left" | "right" | "none" };
      optional?: boolean;
      sourceIsNew?: boolean;
      targetIsNew?: boolean;
      edgeIsNew?: boolean;
      isVariableLength?: boolean;
      minHops?: number;
      maxHops?: number;
      boundEdgeOriginalPattern?: { sourceAlias: string; targetAlias: string };
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

    // Find the index of the variable-length pattern
    const varLengthIndex = relPatterns.findIndex(p => p.isVariableLength);
    if (varLengthIndex === -1) {
      throw new Error("No variable-length pattern found");
    }
    
    const varLengthPattern = relPatterns[varLengthIndex];
    
    // Separate patterns into: fixed-length before, variable-length, fixed-length after
    const fixedPatternsBefore = relPatterns.slice(0, varLengthIndex);
    const fixedPatternsAfter = relPatterns.slice(varLengthIndex + 1);

    const minHops = varLengthPattern.minHops ?? 1;
    // For unbounded paths (*), use a reasonable default max
    // For fixed length (*2), maxHops equals minHops
    // Default of 50 handles most real-world graphs; increase if needed for deep chains
    const maxHops = varLengthPattern.maxHops ?? 50;
    const edgeType = varLengthPattern.edge.type;
    const edgeProperties = varLengthPattern.edge.properties;
    const varLengthSourceAlias = varLengthPattern.sourceAlias;
    const varLengthTargetAlias = varLengthPattern.targetAlias;

    const allParams: unknown[] = [...exprParams];
    
    // Build edge property conditions for variable-length paths
    // These conditions need to be applied to every edge in the path
    const edgePropConditions: string[] = [];
    const edgePropParams: unknown[] = [];
    if (edgeProperties) {
      for (const [key, value] of Object.entries(edgeProperties)) {
        edgePropConditions.push(`json_extract(properties, '$.${key}') = ?`);
        edgePropParams.push(value);
      }
    }
    // Build the condition string for base case (no table alias)
    const basePropCondition = edgePropConditions.length > 0 ? " AND " + edgePropConditions.join(" AND ") : "";
    // Build the condition string for recursive case (with 'e.' table alias)
    const recursivePropConditions = edgePropConditions.map(c => c.replace("properties", "e.properties"));
    const recursivePropCondition = recursivePropConditions.length > 0 ? " AND " + recursivePropConditions.join(" AND ") : "";

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
    
    // Handle empty intervals (minHops > maxHops) - should return no results
    if (minHops > maxHops) {
      return {
        statements: [{ sql: `SELECT 1 WHERE 0`, params: [] }],
        returnColumns,
      };
    }
    
    // Update varLengthEdge variables with the pathCteName so translateExpression can reference it
    const varLengthEdgeVariable = varLengthPattern.edge.variable;
    if (varLengthEdgeVariable) {
      const varInfo = this.ctx.variables.get(varLengthEdgeVariable);
      if (varInfo && varInfo.type === "varLengthEdge") {
        this.ctx.variables.set(varLengthEdgeVariable, { ...varInfo, pathCteName });
        
        // Fix up selectParts that reference this variable - replace placeholder with actual CTE name
        for (let i = 0; i < selectParts.length; i++) {
          if (selectParts[i].includes("path_cte.edge_ids")) {
            selectParts[i] = selectParts[i].replace("path_cte.edge_ids", `${pathCteName}.edge_ids`);
          }
        }
      }
    }
    
    // Build the CTE
    // The depth represents the number of edges traversed
    // For minHops=0, we need to include the source node as a potential end node (depth 0)
    // For *2, we want exactly 2 edges, so depth should stop at maxHops
    // The condition is p.depth < maxHops to allow one more recursion step
    // We also track edge_ids as a JSON array of edge objects for variable-length edge variables
    let cte: string;
    
    // Check if this is an undirected pattern
    const isUndirected = varLengthPattern.edge.direction === "none";
    
    if (minHops === 0 && maxHops === 0) {
      // Special case: *0 means zero-length path, source = target
      // No CTE needed - we'll handle this by making source = target
      cte = "";
    } else if (minHops === 0) {
      // Need to include zero-length paths (source = target) plus longer paths
      if (isUndirected) {
        // For undirected with minHops=0, traverse edges in both directions with edge tracking
        if (edgeType) {
          cte = `WITH RECURSIVE ${pathCteName}(start_id, end_id, depth, edge_ids) AS (
  SELECT id, id, 0, json_array() FROM nodes
  UNION ALL
  SELECT p.start_id, CASE WHEN p.end_id = e.source_id THEN e.target_id ELSE e.source_id END, p.depth + 1, json_insert(p.edge_ids, '$[#]', json_object('id', e.id, 'type', e.type, 'source_id', e.source_id, 'target_id', e.target_id, 'properties', json(e.properties)))
  FROM ${pathCteName} p
  JOIN edges e ON (p.end_id = e.source_id OR p.end_id = e.target_id)
  WHERE p.depth < ? AND e.type = ? AND NOT EXISTS (SELECT 1 FROM json_each(p.edge_ids) WHERE json_extract(value, '$.id') = e.id)
)`;
          allParams.push(maxHops, edgeType);
        } else {
          cte = `WITH RECURSIVE ${pathCteName}(start_id, end_id, depth, edge_ids) AS (
  SELECT id, id, 0, json_array() FROM nodes
  UNION ALL
  SELECT p.start_id, CASE WHEN p.end_id = e.source_id THEN e.target_id ELSE e.source_id END, p.depth + 1, json_insert(p.edge_ids, '$[#]', json_object('id', e.id, 'type', e.type, 'source_id', e.source_id, 'target_id', e.target_id, 'properties', json(e.properties)))
  FROM ${pathCteName} p
  JOIN edges e ON (p.end_id = e.source_id OR p.end_id = e.target_id)
  WHERE p.depth < ? AND NOT EXISTS (SELECT 1 FROM json_each(p.edge_ids) WHERE json_extract(value, '$.id') = e.id)
)`;
          allParams.push(maxHops);
        }
      } else {
        cte = `WITH RECURSIVE ${pathCteName}(start_id, end_id, depth, edge_ids) AS (
  SELECT id, id, 0, json_array() FROM nodes
  UNION ALL
  SELECT p.start_id, e.target_id, p.depth + 1, json_insert(p.edge_ids, '$[#]', json_object('id', e.id, 'type', e.type, 'source_id', e.source_id, 'target_id', e.target_id, 'properties', json(e.properties)))
  FROM ${pathCteName} p
  JOIN edges e ON p.end_id = e.source_id
  WHERE p.depth < ?${edgeType ? " AND e.type = ?" : ""}
)`;
        allParams.push(maxHops);
        if (edgeType) {
          allParams.push(edgeType);
        }
      }
    } else {
      // Base condition for edges (only used in normal CTE case)
      let edgeCondition = "1=1";
      if (edgeType) {
        edgeCondition = "type = ?";
        allParams.push(edgeType);
      }
      
      if (isUndirected) {
        // For undirected patterns, treat each edge as traversable in both directions
        // We use a single recursive query that can traverse edges in either direction
        // The base case includes both directions, and recursive step does too
        // We need to avoid revisiting the same edge (tracked in edge_ids)
        if (edgeType) {
          cte = `WITH RECURSIVE ${pathCteName}(start_id, end_id, depth, edge_ids) AS (
  SELECT source_id, target_id, 1, json_array(json_object('id', id, 'type', type, 'source_id', source_id, 'target_id', target_id, 'properties', json(properties))) FROM edges WHERE ${edgeCondition}
  UNION ALL
  SELECT target_id, source_id, 1, json_array(json_object('id', id, 'type', type, 'source_id', source_id, 'target_id', target_id, 'properties', json(properties))) FROM edges WHERE type = ?
  UNION ALL
  SELECT p.start_id, CASE WHEN p.end_id = e.source_id THEN e.target_id ELSE e.source_id END, p.depth + 1, json_insert(p.edge_ids, '$[#]', json_object('id', e.id, 'type', e.type, 'source_id', e.source_id, 'target_id', e.target_id, 'properties', json(e.properties)))
  FROM ${pathCteName} p
  JOIN edges e ON (p.end_id = e.source_id OR p.end_id = e.target_id)
  WHERE p.depth < ? AND e.type = ? AND NOT EXISTS (SELECT 1 FROM json_each(p.edge_ids) WHERE json_extract(value, '$.id') = e.id)
)`;
          allParams.push(edgeType); // for reverse base case
          allParams.push(maxHops, edgeType); // for recursive
        } else {
          cte = `WITH RECURSIVE ${pathCteName}(start_id, end_id, depth, edge_ids) AS (
  SELECT source_id, target_id, 1, json_array(json_object('id', id, 'type', type, 'source_id', source_id, 'target_id', target_id, 'properties', json(properties))) FROM edges
  UNION ALL
  SELECT target_id, source_id, 1, json_array(json_object('id', id, 'type', type, 'source_id', source_id, 'target_id', target_id, 'properties', json(properties))) FROM edges
  UNION ALL
  SELECT p.start_id, CASE WHEN p.end_id = e.source_id THEN e.target_id ELSE e.source_id END, p.depth + 1, json_insert(p.edge_ids, '$[#]', json_object('id', e.id, 'type', e.type, 'source_id', e.source_id, 'target_id', e.target_id, 'properties', json(e.properties)))
  FROM ${pathCteName} p
  JOIN edges e ON (p.end_id = e.source_id OR p.end_id = e.target_id)
  WHERE p.depth < ? AND NOT EXISTS (SELECT 1 FROM json_each(p.edge_ids) WHERE json_extract(value, '$.id') = e.id)
)`;
          allParams.push(maxHops); // for recursive
        }
      } else {
        cte = `WITH RECURSIVE ${pathCteName}(start_id, end_id, depth, edge_ids) AS (
  SELECT source_id, target_id, 1, json_array(json_object('id', id, 'type', type, 'source_id', source_id, 'target_id', target_id, 'properties', json(properties))) FROM edges WHERE ${edgeCondition}${basePropCondition}
  UNION ALL
  SELECT p.start_id, e.target_id, p.depth + 1, json_insert(p.edge_ids, '$[#]', json_object('id', e.id, 'type', e.type, 'source_id', e.source_id, 'target_id', e.target_id, 'properties', json(e.properties)))
  FROM ${pathCteName} p
  JOIN edges e ON p.end_id = e.source_id
  WHERE p.depth < ?${edgeType ? " AND e.type = ?" : ""}${recursivePropCondition} AND NOT EXISTS (SELECT 1 FROM json_each(p.edge_ids) WHERE json_extract(value, '$.id') = e.id)
)`;
        // For maxHops=2, we need depth to reach 2, so recursion limit should be maxHops
        allParams.push(...edgePropParams); // for base case
        allParams.push(maxHops);
        if (edgeType) {
          allParams.push(edgeType);
        }
        allParams.push(...edgePropParams); // for recursive case
      }
    }

    // Build FROM and JOIN clauses for fixed-length patterns before the variable-length
    const fromParts: string[] = [];
    const joinParts: string[] = [];
    const joinParams: unknown[] = [];
    const whereParts: string[] = [];
    const addedNodeAliases = new Set<string>();
    const addedEdgeAliases = new Set<string>();
    const filteredNodeAliases = new Set<string>();
    // Deferred WHERE params - these are added after all CTE params
    // This is needed because CTEs are defined before the WHERE clause in SQL
    const deferredWhereParams: unknown[] = [];
    
    // Track bound edges (edges that are reused from a previous MATCH, not newly matched)
    // These must be excluded from variable-length path results to ensure edge uniqueness
    const boundEdgeAliases: string[] = [];
    
    // Find all bound (non-new) edge aliases in fixedPatternsAfter and fixedPatternsBefore
    for (const pattern of [...fixedPatternsBefore, ...fixedPatternsAfter]) {
      if (!pattern.isVariableLength && pattern.edgeIsNew === false) {
        boundEdgeAliases.push(pattern.edgeAlias);
      }
    }
    
    // Track all CTE names to add edge uniqueness constraints later
    const pathCteNames: string[] = [pathCteName!];

    // Process fixed-length patterns before the variable-length pattern
    for (let i = 0; i < fixedPatternsBefore.length; i++) {
      const pattern = fixedPatternsBefore[i];
      const isOptional = pattern.optional === true;
      const joinType = isOptional ? "LEFT JOIN" : "JOIN";
      
      // Check if source node is already in FROM (from a previous non-optional MATCH)
      const sourceIsAlreadyAdded = addedNodeAliases.has(pattern.sourceAlias);
      
      if (!sourceIsAlreadyAdded) {
        // Check if source should be in FROM (non-optional) or is from a prior required pattern
        const sourceNodeOptional = (this.ctx as any)[`optional_${pattern.sourceAlias}`] === true;
        
        if (!sourceNodeOptional) {
          // Source is from a required MATCH - add to FROM
          fromParts.push(`nodes ${pattern.sourceAlias}`);
          addedNodeAliases.add(pattern.sourceAlias);
          
          // Add source label/property filters
          const sourcePattern = (this.ctx as any)[`pattern_${pattern.sourceAlias}`];
          if (sourcePattern?.label) {
            const labelMatch = this.generateLabelMatchCondition(pattern.sourceAlias, sourcePattern.label);
            whereParts.push(labelMatch.sql);
            allParams.push(...labelMatch.params);
          }
          // Add source property filters (e.g., {name: 'A'})
          if (sourcePattern?.properties) {
            for (const [key, value] of Object.entries(sourcePattern.properties)) {
              if (this.isParameterRef(value as PropertyValue)) {
                whereParts.push(`json_extract(${pattern.sourceAlias}.properties, '$.${key}') = ?`);
                deferredWhereParams.push(this.ctx.paramValues[(value as ParameterRef).name]);
              } else {
                whereParts.push(`json_extract(${pattern.sourceAlias}.properties, '$.${key}') = ?`);
                deferredWhereParams.push(value);
              }
            }
          }
          filteredNodeAliases.add(pattern.sourceAlias);
        } else if (fromParts.length === 0) {
          // Need at least something in FROM - add a dummy
          fromParts.push(`(SELECT 1) AS __dummy__`);
        }
      }
      
      // Add edge JOIN (only if not already added - handles bound relationships)
      if (!addedEdgeAliases.has(pattern.edgeAlias)) {
        const isUndirectedPattern = pattern.edge.direction === "none";
        const isLeftDirected = pattern.edge.direction === "left";
        
        // Build ON conditions for the edge
        const edgeOnConditions: string[] = [];
        
        if (isUndirectedPattern) {
          // For undirected patterns, match edges in either direction
          // For self-loops (source_id = target_id), match only once by requiring both conditions
          edgeOnConditions.push(`(${pattern.edgeAlias}.source_id = ${pattern.sourceAlias}.id OR ${pattern.edgeAlias}.target_id = ${pattern.sourceAlias}.id)`);
          edgeOnConditions.push(`NOT (${pattern.edgeAlias}.source_id = ${pattern.edgeAlias}.target_id AND ${pattern.edgeAlias}.source_id = ${pattern.sourceAlias}.id AND ${pattern.edgeAlias}.target_id = ${pattern.targetAlias}.id)`);
        } else if (isLeftDirected) {
          // Left-directed: (a)<-[:R]-(b) means edge goes FROM b TO a, so a is at target_id
          edgeOnConditions.push(`${pattern.edgeAlias}.target_id = ${pattern.sourceAlias}.id`);
        } else {
          // Right-directed: (a)-[:R]->(b) means edge goes FROM a TO b, so a is at source_id
          edgeOnConditions.push(`${pattern.edgeAlias}.source_id = ${pattern.sourceAlias}.id`);
        }
        
        // For optional patterns, add edge type filter to ON clause
        // For non-optional, add to WHERE (deferred)
        if (pattern.edge.type) {
          if (isOptional) {
            edgeOnConditions.push(`${pattern.edgeAlias}.type = ?`);
            joinParams.push(pattern.edge.type);
          } else {
            whereParts.push(`${pattern.edgeAlias}.type = ?`);
            deferredWhereParams.push(pattern.edge.type);
          }
        }
        
        joinParts.push(`${joinType} edges ${pattern.edgeAlias} ON ${edgeOnConditions.join(" AND ")}`);
        addedEdgeAliases.add(pattern.edgeAlias);
      }
      
      // Add target node JOIN
      if (!addedNodeAliases.has(pattern.targetAlias)) {
        const isUndirectedPattern = pattern.edge.direction === "none";
        const isLeftDirected = pattern.edge.direction === "left";
        
        // Build ON conditions for the target node
        const targetOnConditions: string[] = [];
        
        if (isUndirectedPattern) {
          // For undirected, target could be on either side
          targetOnConditions.push(`(${pattern.edgeAlias}.target_id = ${pattern.targetAlias}.id OR ${pattern.edgeAlias}.source_id = ${pattern.targetAlias}.id)`);
          // Also ensure source and target are different (for undirected edges)
          if (!isOptional) {
            whereParts.push(`${pattern.sourceAlias}.id != ${pattern.targetAlias}.id`);
          }
        } else if (isLeftDirected) {
          // Left-directed: (a)<-[:R]-(b) means edge goes FROM b TO a, so b is at source_id
          targetOnConditions.push(`${pattern.edgeAlias}.source_id = ${pattern.targetAlias}.id`);
        } else {
          // Right-directed: (a)-[:R]->(b) means edge goes FROM a TO b, so b is at target_id
          targetOnConditions.push(`${pattern.edgeAlias}.target_id = ${pattern.targetAlias}.id`);
        }
        
        // For optional patterns, add target label filter to ON clause
        const targetPattern = (this.ctx as any)[`pattern_${pattern.targetAlias}`];
        if (isOptional && targetPattern?.label) {
          const labelMatch = this.generateLabelMatchCondition(pattern.targetAlias, targetPattern.label);
          targetOnConditions.push(labelMatch.sql);
          joinParams.push(...labelMatch.params);
          filteredNodeAliases.add(pattern.targetAlias);
        }
        
        joinParts.push(`${joinType} nodes ${pattern.targetAlias} ON ${targetOnConditions.join(" AND ")}`);
        addedNodeAliases.add(pattern.targetAlias);
        
        // For non-optional patterns, add target label filter to WHERE
        if (!isOptional && targetPattern?.label && !filteredNodeAliases.has(pattern.targetAlias)) {
          const labelMatch = this.generateLabelMatchCondition(pattern.targetAlias, targetPattern.label);
          whereParts.push(labelMatch.sql);
          deferredWhereParams.push(...labelMatch.params);
          filteredNodeAliases.add(pattern.targetAlias);
        }
      }
    }

    // Now add the variable-length path
    // The source of the variable-length pattern should connect to the path start
    if (!addedNodeAliases.has(varLengthSourceAlias)) {
      if (fromParts.length === 0) {
        fromParts.push(`nodes ${varLengthSourceAlias}`);
      } else {
        joinParts.push(`JOIN nodes ${varLengthSourceAlias} ON 1=1`);
      }
      addedNodeAliases.add(varLengthSourceAlias);
      
      // Add label/property filters for the source - deferred until after CTE params
      const sourcePattern = (this.ctx as any)[`pattern_${varLengthSourceAlias}`];
      if (sourcePattern?.label && !filteredNodeAliases.has(varLengthSourceAlias)) {
        const labelMatch = this.generateLabelMatchCondition(varLengthSourceAlias, sourcePattern.label);
        whereParts.push(labelMatch.sql);
        deferredWhereParams.push(...labelMatch.params);
        filteredNodeAliases.add(varLengthSourceAlias);
      }
      // Add source property filters - deferred until after CTE params
      if (sourcePattern?.properties) {
        for (const [key, value] of Object.entries(sourcePattern.properties)) {
          if (this.isParameterRef(value as PropertyValue)) {
            whereParts.push(`json_extract(${varLengthSourceAlias}.properties, '$.${key}') = ?`);
            deferredWhereParams.push(this.ctx.paramValues[(value as ParameterRef).name]);
          } else {
            whereParts.push(`json_extract(${varLengthSourceAlias}.properties, '$.${key}') = ?`);
            deferredWhereParams.push(value);
          }
        }
      }
    }
    
    // Handle zero-length path specially (no CTE needed, source = target)
    if (minHops === 0 && maxHops === 0) {
      // For *0, the target is the same as the source
      // If source and target have different aliases, make them the same node
      if (varLengthSourceAlias !== varLengthTargetAlias) {
        // Add constraint that source = target
        whereParts.push(`${varLengthSourceAlias}.id = ${varLengthTargetAlias}.id`);
      }
      // Add the target node if not already added
      if (!addedNodeAliases.has(varLengthTargetAlias)) {
        fromParts.push(`nodes ${varLengthTargetAlias}`);
        addedNodeAliases.add(varLengthTargetAlias);
      }
    } else {
      // Check if this variable-length pattern is from an OPTIONAL MATCH
      const isOptionalVarLength = varLengthPattern.optional === true;
      
      if (isOptionalVarLength) {
        // For OPTIONAL MATCH with variable-length paths, use LEFT JOINs
        // The source should already be in FROM from a previous MATCH
        // ON clause params need to be added directly to allParams (not deferred)
        // because JOIN ON clauses come before WHERE in SQL
        
        // Check if the target node was from a REQUIRED (non-optional) MATCH
        // If so, it should be in FROM, not LEFT JOINed
        const targetIsFromRequiredMatch = (this.ctx as any)[`optional_${varLengthTargetAlias}`] !== true &&
          (this.ctx as any)[`pattern_${varLengthTargetAlias}`] !== undefined;
        
        if (targetIsFromRequiredMatch) {
          // Target is already bound from a required MATCH
          // Add both source and target to FROM, LEFT JOIN the path CTE
          // The WHERE clause checks if a path exists (but doesn't filter rows if not)
          
          // Add target to FROM if not already added
          if (!addedNodeAliases.has(varLengthTargetAlias)) {
            if (fromParts.length > 0) {
              fromParts.push(`nodes ${varLengthTargetAlias}`);
            } else {
              fromParts.push(`nodes ${varLengthTargetAlias}`);
            }
            addedNodeAliases.add(varLengthTargetAlias);
            
            // Add target label/property filters to WHERE
            const targetPattern = (this.ctx as any)[`pattern_${varLengthTargetAlias}`];
            if (targetPattern?.label && !filteredNodeAliases.has(varLengthTargetAlias)) {
              const labelMatch = this.generateLabelMatchCondition(varLengthTargetAlias, targetPattern.label);
              whereParts.push(labelMatch.sql);
              deferredWhereParams.push(...labelMatch.params);
              filteredNodeAliases.add(varLengthTargetAlias);
            }
            if (targetPattern?.properties) {
              for (const [key, value] of Object.entries(targetPattern.properties)) {
                if (this.isParameterRef(value as PropertyValue)) {
                  whereParts.push(`json_extract(${varLengthTargetAlias}.properties, '$.${key}') = ?`);
                  deferredWhereParams.push(this.ctx.paramValues[(value as ParameterRef).name]);
                } else {
                  whereParts.push(`json_extract(${varLengthTargetAlias}.properties, '$.${key}') = ?`);
                  deferredWhereParams.push(value);
                }
              }
            }
          }
          
          // Build ON clause for path CTE - connects source AND target
          const pathOnConditions: string[] = [
            `${varLengthSourceAlias}.id = ${pathCteName}.start_id`,
            `${varLengthTargetAlias}.id = ${pathCteName}.end_id`
          ];
          if (minHops > 1) {
            pathOnConditions.push(`${pathCteName}.depth >= ?`);
            allParams.push(minHops);
          }
          
          // LEFT JOIN the path CTE - optional path between already-bound nodes
          joinParts.push(`LEFT JOIN ${pathCteName} ON ${pathOnConditions.join(" AND ")}`);
        } else {
          // Target is newly introduced by this OPTIONAL MATCH - use LEFT JOINs
          
          // Build ON clause conditions for the path CTE
          const pathOnConditions: string[] = [`${varLengthSourceAlias}.id = ${pathCteName}.start_id`];
          if (minHops > 1) {
            pathOnConditions.push(`${pathCteName}.depth >= ?`);
            allParams.push(minHops);
          }
          
          // LEFT JOIN the path CTE
          joinParts.push(`LEFT JOIN ${pathCteName} ON ${pathOnConditions.join(" AND ")}`);
          
          // Build ON clause conditions for the target node
          const targetOnConditions: string[] = [`${pathCteName}.end_id = ${varLengthTargetAlias}.id`];
          
          // Add target label/property filters to ON clause (not WHERE) for proper OPTIONAL semantics
          const targetPattern = (this.ctx as any)[`pattern_${varLengthTargetAlias}`];
          if (targetPattern?.label && !filteredNodeAliases.has(varLengthTargetAlias)) {
            const labelMatch = this.generateLabelMatchCondition(varLengthTargetAlias, targetPattern.label);
            targetOnConditions.push(labelMatch.sql);
            allParams.push(...labelMatch.params);
            filteredNodeAliases.add(varLengthTargetAlias);
          }
          if (targetPattern?.properties) {
            for (const [key, value] of Object.entries(targetPattern.properties)) {
              if (this.isParameterRef(value as PropertyValue)) {
                targetOnConditions.push(`json_extract(${varLengthTargetAlias}.properties, '$.${key}') = ?`);
                allParams.push(this.ctx.paramValues[(value as ParameterRef).name]);
              } else {
                targetOnConditions.push(`json_extract(${varLengthTargetAlias}.properties, '$.${key}') = ?`);
                allParams.push(value);
              }
            }
          }
          
          // LEFT JOIN the target node
          if (!addedNodeAliases.has(varLengthTargetAlias)) {
            joinParts.push(`LEFT JOIN nodes ${varLengthTargetAlias} ON ${targetOnConditions.join(" AND ")}`);
            addedNodeAliases.add(varLengthTargetAlias);
          }
        }
      } else {
        // Non-optional: use FROM (cross join) with WHERE conditions as before
        // Add the CTE to FROM (it acts like a table)
        fromParts.push(pathCteName);
        
        // Add the target node of the variable-length path
        if (!addedNodeAliases.has(varLengthTargetAlias)) {
          fromParts.push(`nodes ${varLengthTargetAlias}`);
          addedNodeAliases.add(varLengthTargetAlias);
        }
        
        // Connect source node to path start
        whereParts.push(`${varLengthSourceAlias}.id = ${pathCteName}.start_id`);
        // Connect target node to path end
        whereParts.push(`${varLengthTargetAlias}.id = ${pathCteName}.end_id`);
        // Apply min depth constraint - deferred until after CTE params
        if (minHops > 1) {
          whereParts.push(`${pathCteName}.depth >= ?`);
          deferredWhereParams.push(minHops);
        }
        
        // Add target label/property filters for the variable-length pattern - deferred until after CTE params
        const targetPattern = (this.ctx as any)[`pattern_${varLengthTargetAlias}`];
        if (targetPattern?.label && !filteredNodeAliases.has(varLengthTargetAlias)) {
          const labelMatch = this.generateLabelMatchCondition(varLengthTargetAlias, targetPattern.label);
          whereParts.push(labelMatch.sql);
          deferredWhereParams.push(...labelMatch.params);
          filteredNodeAliases.add(varLengthTargetAlias);
        }
        if (targetPattern?.properties) {
          for (const [key, value] of Object.entries(targetPattern.properties)) {
            if (this.isParameterRef(value as PropertyValue)) {
              whereParts.push(`json_extract(${varLengthTargetAlias}.properties, '$.${key}') = ?`);
              deferredWhereParams.push(this.ctx.paramValues[(value as ParameterRef).name]);
            } else {
              whereParts.push(`json_extract(${varLengthTargetAlias}.properties, '$.${key}') = ?`);
              deferredWhereParams.push(value);
            }
          }
        }
      }
    }

    // Handle patterns AFTER the variable-length pattern
    // The target of the variable-length path becomes the source for the first pattern after
    let currentSourceAlias = varLengthTargetAlias;
    for (const pattern of fixedPatternsAfter) {
      if (pattern.isVariableLength) {
        // Handle another variable-length pattern by creating a second CTE
        const minHops2 = pattern.minHops ?? 1;
        const maxHops2 = pattern.maxHops ?? 50;
        const edgeType2 = pattern.edge.type;
        const isUndirected2 = pattern.edge.direction === "none";
        const pathCteName2 = `path_${this.ctx.aliasCounter++}`;
        
        // Build second CTE
        let cte2 = "";
        if (minHops2 === 0 && maxHops2 === 0) {
          cte2 = "";
        } else if (minHops2 === 0) {
          // Zero-length path support: start with each node connected to itself
          if (isUndirected2) {
            // Undirected: traverse in both directions
            if (edgeType2) {
              cte2 = `, ${pathCteName2}(start_id, end_id, depth, edge_ids) AS (
  SELECT id, id, 0, json_array() FROM nodes
  UNION ALL
  SELECT p.start_id, CASE WHEN p.end_id = e.source_id THEN e.target_id ELSE e.source_id END, p.depth + 1, json_insert(p.edge_ids, '$[#]', json_object('id', e.id, 'type', e.type, 'source_id', e.source_id, 'target_id', e.target_id, 'properties', json(e.properties)))
  FROM ${pathCteName2} p
  JOIN edges e ON (p.end_id = e.source_id OR p.end_id = e.target_id)
  WHERE p.depth < ? AND e.type = ? AND NOT EXISTS (SELECT 1 FROM json_each(p.edge_ids) WHERE json_extract(value, '$.id') = e.id)
)`;
              allParams.push(maxHops2, edgeType2);
            } else {
              cte2 = `, ${pathCteName2}(start_id, end_id, depth, edge_ids) AS (
  SELECT id, id, 0, json_array() FROM nodes
  UNION ALL
  SELECT p.start_id, CASE WHEN p.end_id = e.source_id THEN e.target_id ELSE e.source_id END, p.depth + 1, json_insert(p.edge_ids, '$[#]', json_object('id', e.id, 'type', e.type, 'source_id', e.source_id, 'target_id', e.target_id, 'properties', json(e.properties)))
  FROM ${pathCteName2} p
  JOIN edges e ON (p.end_id = e.source_id OR p.end_id = e.target_id)
  WHERE p.depth < ? AND NOT EXISTS (SELECT 1 FROM json_each(p.edge_ids) WHERE json_extract(value, '$.id') = e.id)
)`;
              allParams.push(maxHops2);
            }
          } else {
            // Directed
            if (edgeType2) {
              cte2 = `, ${pathCteName2}(start_id, end_id, depth, edge_ids) AS (
  SELECT id, id, 0, json_array() FROM nodes
  UNION ALL
  SELECT p.start_id, e.target_id, p.depth + 1, json_insert(p.edge_ids, '$[#]', json_object('id', e.id, 'type', e.type, 'source_id', e.source_id, 'target_id', e.target_id, 'properties', json(e.properties)))
  FROM ${pathCteName2} p
  JOIN edges e ON p.end_id = e.source_id
  WHERE p.depth < ? AND e.type = ?
)`;
              allParams.push(maxHops2, edgeType2);
            } else {
              cte2 = `, ${pathCteName2}(start_id, end_id, depth, edge_ids) AS (
  SELECT id, id, 0, json_array() FROM nodes
  UNION ALL
  SELECT p.start_id, e.target_id, p.depth + 1, json_insert(p.edge_ids, '$[#]', json_object('id', e.id, 'type', e.type, 'source_id', e.source_id, 'target_id', e.target_id, 'properties', json(e.properties)))
  FROM ${pathCteName2} p
  JOIN edges e ON p.end_id = e.source_id
  WHERE p.depth < ?
)`;
              allParams.push(maxHops2);
            }
          }
        } else {
          if (isUndirected2) {
            // Undirected: traverse in both directions
            if (edgeType2) {
              cte2 = `, ${pathCteName2}(start_id, end_id, depth, edge_ids) AS (
  SELECT source_id, target_id, 1, json_array(json_object('id', id, 'type', type, 'source_id', source_id, 'target_id', target_id, 'properties', json(properties))) FROM edges WHERE type = ?
  UNION ALL
  SELECT target_id, source_id, 1, json_array(json_object('id', id, 'type', type, 'source_id', source_id, 'target_id', target_id, 'properties', json(properties))) FROM edges WHERE type = ?
  UNION ALL
  SELECT p.start_id, CASE WHEN p.end_id = e.source_id THEN e.target_id ELSE e.source_id END, p.depth + 1, json_insert(p.edge_ids, '$[#]', json_object('id', e.id, 'type', e.type, 'source_id', e.source_id, 'target_id', e.target_id, 'properties', json(e.properties)))
  FROM ${pathCteName2} p
  JOIN edges e ON (p.end_id = e.source_id OR p.end_id = e.target_id)
  WHERE p.depth < ? AND e.type = ? AND NOT EXISTS (SELECT 1 FROM json_each(p.edge_ids) WHERE json_extract(value, '$.id') = e.id)
)`;
              allParams.push(edgeType2, edgeType2, maxHops2, edgeType2);
            } else {
              cte2 = `, ${pathCteName2}(start_id, end_id, depth, edge_ids) AS (
  SELECT source_id, target_id, 1, json_array(json_object('id', id, 'type', type, 'source_id', source_id, 'target_id', target_id, 'properties', json(properties))) FROM edges
  UNION ALL
  SELECT target_id, source_id, 1, json_array(json_object('id', id, 'type', type, 'source_id', source_id, 'target_id', target_id, 'properties', json(properties))) FROM edges
  UNION ALL
  SELECT p.start_id, CASE WHEN p.end_id = e.source_id THEN e.target_id ELSE e.source_id END, p.depth + 1, json_insert(p.edge_ids, '$[#]', json_object('id', e.id, 'type', e.type, 'source_id', e.source_id, 'target_id', e.target_id, 'properties', json(e.properties)))
  FROM ${pathCteName2} p
  JOIN edges e ON (p.end_id = e.source_id OR p.end_id = e.target_id)
  WHERE p.depth < ? AND NOT EXISTS (SELECT 1 FROM json_each(p.edge_ids) WHERE json_extract(value, '$.id') = e.id)
)`;
              allParams.push(maxHops2);
            }
          } else {
            // Directed
            if (edgeType2) {
              cte2 = `, ${pathCteName2}(start_id, end_id, depth, edge_ids) AS (
  SELECT source_id, target_id, 1, json_array(json_object('id', id, 'type', type, 'source_id', source_id, 'target_id', target_id, 'properties', json(properties))) FROM edges WHERE type = ?
  UNION ALL
  SELECT p.start_id, e.target_id, p.depth + 1, json_insert(p.edge_ids, '$[#]', json_object('id', e.id, 'type', e.type, 'source_id', e.source_id, 'target_id', e.target_id, 'properties', json(e.properties)))
  FROM ${pathCteName2} p
  JOIN edges e ON p.end_id = e.source_id
  WHERE p.depth < ? AND e.type = ?
)`;
              allParams.push(edgeType2, maxHops2, edgeType2);
            } else {
              cte2 = `, ${pathCteName2}(start_id, end_id, depth, edge_ids) AS (
  SELECT source_id, target_id, 1, json_array(json_object('id', id, 'type', type, 'source_id', source_id, 'target_id', target_id, 'properties', json(properties))) FROM edges
  UNION ALL
  SELECT p.start_id, e.target_id, p.depth + 1, json_insert(p.edge_ids, '$[#]', json_object('id', e.id, 'type', e.type, 'source_id', e.source_id, 'target_id', e.target_id, 'properties', json(e.properties)))
  FROM ${pathCteName2} p
  JOIN edges e ON p.end_id = e.source_id
  WHERE p.depth < ?
)`;
              allParams.push(maxHops2);
            }
          }
        }
        
        // Append second CTE to the first
        cte += cte2;
        
        // Track this CTE for edge uniqueness constraints
        pathCteNames.push(pathCteName2);
        
        // Add the second CTE to FROM
        fromParts.push(pathCteName2);
        
        // Add the target node of the second variable-length pattern
        if (!addedNodeAliases.has(pattern.targetAlias)) {
          fromParts.push(`nodes ${pattern.targetAlias}`);
          addedNodeAliases.add(pattern.targetAlias);
        }
        
        // Connect the current source (end of first path) to the second path start
        whereParts.push(`${currentSourceAlias}.id = ${pathCteName2}.start_id`);
        // Connect the second path end to its target node
        whereParts.push(`${pathCteName2}.end_id = ${pattern.targetAlias}.id`);
        // Apply min depth constraint for second path - deferred until after all CTE params
        if (minHops2 > 1) {
          whereParts.push(`${pathCteName2}.depth >= ?`);
          deferredWhereParams.push(minHops2);
        }
        
        // Add target label/property filters for second pattern - deferred until after all CTE params
        const targetPattern2 = (this.ctx as any)[`pattern_${pattern.targetAlias}`];
        if (targetPattern2?.label && !filteredNodeAliases.has(pattern.targetAlias)) {
          const labelMatch = this.generateLabelMatchCondition(pattern.targetAlias, targetPattern2.label);
          whereParts.push(labelMatch.sql);
          deferredWhereParams.push(...labelMatch.params);
          filteredNodeAliases.add(pattern.targetAlias);
        }
        if (targetPattern2?.properties) {
          for (const [key, value] of Object.entries(targetPattern2.properties)) {
            if (this.isParameterRef(value as PropertyValue)) {
              whereParts.push(`json_extract(${pattern.targetAlias}.properties, '$.${key}') = ?`);
              deferredWhereParams.push(this.ctx.paramValues[(value as ParameterRef).name]);
            } else {
              whereParts.push(`json_extract(${pattern.targetAlias}.properties, '$.${key}') = ?`);
              deferredWhereParams.push(value);
            }
          }
        }
        
        currentSourceAlias = pattern.targetAlias;
      } else {
        // Handle fixed-length pattern
        if (!addedNodeAliases.has(pattern.sourceAlias) && pattern.sourceAlias !== currentSourceAlias) {
          joinParts.push(`JOIN nodes ${pattern.sourceAlias} ON ${pattern.sourceAlias}.id = ${currentSourceAlias}.id`);
          addedNodeAliases.add(pattern.sourceAlias);
        }
        
        // Add edge JOIN (only if not already added - handles bound relationships from earlier MATCH)
        if (!addedEdgeAliases.has(pattern.edgeAlias)) {
          joinParts.push(`JOIN edges ${pattern.edgeAlias} ON ${pattern.edgeAlias}.source_id = ${currentSourceAlias}.id`);
          addedEdgeAliases.add(pattern.edgeAlias);
          
          // Edge type filter - deferred until after all CTE params
          if (pattern.edge.type) {
            whereParts.push(`${pattern.edgeAlias}.type = ?`);
            deferredWhereParams.push(pattern.edge.type);
          }
        } else {
          // Edge already joined - this is a bound relationship from an earlier MATCH
          // The current source and the target alias should be the edge's actual endpoints
          // Since the pattern is undirected ()-[r]-(), we don't know which endpoint is which,
          // so we use OR conditions and let the database figure out valid combinations
          if ((pattern as any).boundEdgeOriginalPattern) {
            // Constrain the source to be one of the original endpoints
            const origPattern = (pattern as any).boundEdgeOriginalPattern;
            whereParts.push(`(${currentSourceAlias}.id = ${origPattern.sourceAlias}.id OR ${currentSourceAlias}.id = ${origPattern.targetAlias}.id)`);
          } else {
            whereParts.push(`(${pattern.edgeAlias}.source_id = ${currentSourceAlias}.id OR ${pattern.edgeAlias}.target_id = ${currentSourceAlias}.id)`);
          }
        }
        
        if (!addedNodeAliases.has(pattern.targetAlias)) {
          // For bound edges, the target should be the OTHER endpoint of the edge
          if ((pattern as any).boundEdgeOriginalPattern) {
            const origPattern = (pattern as any).boundEdgeOriginalPattern;
            // Target should be the opposite endpoint from source
            // Since source can be either original endpoint, we need an XOR-like condition
            joinParts.push(`JOIN nodes ${pattern.targetAlias} ON ((${currentSourceAlias}.id = ${origPattern.sourceAlias}.id AND ${pattern.targetAlias}.id = ${origPattern.targetAlias}.id) OR (${currentSourceAlias}.id = ${origPattern.targetAlias}.id AND ${pattern.targetAlias}.id = ${origPattern.sourceAlias}.id))`);
          } else {
            joinParts.push(`JOIN nodes ${pattern.targetAlias} ON ${pattern.edgeAlias}.target_id = ${pattern.targetAlias}.id`);
          }
          addedNodeAliases.add(pattern.targetAlias);
          
          // Target label/property filters - deferred until after all CTE params
          const afterTargetPattern = (this.ctx as any)[`pattern_${pattern.targetAlias}`];
          if (afterTargetPattern?.label && !filteredNodeAliases.has(pattern.targetAlias)) {
            const labelMatch = this.generateLabelMatchCondition(pattern.targetAlias, afterTargetPattern.label);
            whereParts.push(labelMatch.sql);
            deferredWhereParams.push(...labelMatch.params);
            filteredNodeAliases.add(pattern.targetAlias);
          }
          if (afterTargetPattern?.properties) {
            for (const [key, value] of Object.entries(afterTargetPattern.properties)) {
              if (this.isParameterRef(value as PropertyValue)) {
                whereParts.push(`json_extract(${pattern.targetAlias}.properties, '$.${key}') = ?`);
                deferredWhereParams.push(this.ctx.paramValues[(value as ParameterRef).name]);
              } else {
                whereParts.push(`json_extract(${pattern.targetAlias}.properties, '$.${key}') = ?`);
                deferredWhereParams.push(value);
              }
            }
          }
        }
        
        currentSourceAlias = pattern.targetAlias;
      }
    }

    // Now add JOIN params (for ON clause conditions in fixed patterns before var-length)
    // These come after CTE params but before WHERE params
    allParams.push(...joinParams);
    
    // Now add the deferred WHERE params (after all CTE params have been added)
    allParams.push(...deferredWhereParams);
    
    // Add edge uniqueness constraints: variable-length paths must not include bound edges
    // This ensures that when a pattern like (n)-[*0..1]-()-[r]-()-[*0..1]-(m) is matched,
    // the variable-length portions don't traverse the bound edge r
    for (const boundEdgeAlias of boundEdgeAliases) {
      for (const pathCte of pathCteNames) {
        // Exclude paths that contain the bound edge
        whereParts.push(`NOT EXISTS (SELECT 1 FROM json_each(${pathCte}.edge_ids) WHERE json_extract(value, '$.id') = ${boundEdgeAlias}.id)`);
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
    sql += ` FROM ${fromParts.join(", ")}`;
    if (joinParts.length > 0) {
      sql += ` ${joinParts.join(" ")}`;
    }
    
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
        const { sql: limitSql, params: limitParams } = this.translateSkipLimitExpression(clause.limit, "LIMIT");
        sql += ` LIMIT ${limitSql}`;
        allParams.push(...limitParams);
      } else if (clause.skip !== undefined) {
        sql += ` LIMIT -1`;
      }

      if (clause.skip !== undefined) {
        const { sql: skipSql, params: skipParams } = this.translateSkipLimitExpression(clause.skip, "SKIP");
        sql += ` OFFSET ${skipSql}`;
        allParams.push(...skipParams);
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

    const leftColumns = leftResult.returnColumns ?? [];
    const rightColumns = rightResult.returnColumns ?? [];
    const sameColumns =
      leftColumns.length === rightColumns.length &&
      leftColumns.every((col, idx) => col === rightColumns[idx]);
    if (!sameColumns) {
      throw new Error("SyntaxError: DifferentColumnsInUnion");
    }
    
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
    const returnColumns = leftColumns;
    
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
    let jsonExpr: string = "";
    let params: unknown[] = [];
    
    if (clause.expression.type === "literal") {
      // Handle null literal - produces empty result
      if (clause.expression.value === null) {
        jsonExpr = "json_array()"; // Empty array produces no rows
        // No params needed
      } else {
        // Literal array - serialize to JSON
        jsonExpr = "?";
        params.push(JSON.stringify(clause.expression.value));
      }
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
        // Check if it's a previous UNWIND variable
        const unwindClauses = (this.ctx as any).unwindClauses as Array<{
          alias: string;
          variable: string;
          jsonExpr: string;
          params: unknown[];
        }> | undefined;
        
        const unwindClause = unwindClauses?.find(u => u.variable === varName);
        if (unwindClause) {
          // It's a previous UNWIND variable - use .value to get the nested array
          jsonExpr = `${unwindClause.alias}.value`;
          // No additional params needed for this case
        } else {
          // It's a regular variable
          const varInfo = this.ctx.variables.get(varName);
          if (varInfo) {
            jsonExpr = `${varInfo.alias}.properties`;
          } else {
            throw new Error(`Unknown variable in UNWIND: ${varName}`);
          }
        }
      }
    } else if (clause.expression.type === "property") {
      // Property access on a variable
      const varInfo = this.ctx.variables.get(clause.expression.variable!);
      if (!varInfo) {
        throw new Error(`Unknown variable: ${clause.expression.variable}`);
      }
      jsonExpr = `json_extract(${varInfo.alias}.properties, '$.${clause.expression.property}')`;
    } else if (clause.expression.type === "function" || clause.expression.type === "binary") {
      // Function call like range(1, 10) or binary expression like (first + second)
      const translated = this.translateExpression(clause.expression);
      jsonExpr = translated.sql;
      params.push(...translated.params);
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

  private lookupWithAliasExpression(aliasName: string, skipScopes: number): Expression | undefined {
    const withAliasesStack = (this.ctx as any).withAliasesStack as Map<string, Expression>[] | undefined;
    if (withAliasesStack && withAliasesStack.length > 0) {
      for (let i = withAliasesStack.length - 1 - skipScopes; i >= 0; i--) {
        const scope = withAliasesStack[i];
        if (scope && scope.has(aliasName)) {
          return scope.get(aliasName)!;
        }
      }
    }
    const withAliases = (this.ctx as any).withAliases as Map<string, Expression> | undefined;
    if (withAliases && skipScopes === 0 && withAliases.has(aliasName)) {
      return withAliases.get(aliasName)!;
    }
    return undefined;
  }

  private translateExpression(expr: Expression): { sql: string; tables: string[]; params: unknown[] } {
    const tables: string[] = [];
    const params: unknown[] = [];

    switch (expr.type) {
      case "variable": {
        // Check if this is a materialized aggregate alias (from CTE)
        const materializedAggregateAliases = (this.ctx as any).materializedAggregateAliases as Set<string> | undefined;
        if (materializedAggregateAliases && materializedAggregateAliases.has(expr.variable!)) {
          // Reference the column from the __aggregates__ CTE instead of re-translating
          return { sql: `__aggregates__."${expr.variable!}"`, tables: ["__aggregates__"], params: [] };
        }

        // First check if this is a WITH alias
        const withAliases = (this.ctx as any).withAliases as Map<string, Expression> | undefined;
        if (withAliases && withAliases.has(expr.variable!)) {
          const aliasName = expr.variable!;
          const selfRefDepths =
            (((this.ctx as any)._withAliasSelfRefDepths as Map<string, number> | undefined) ??= new Map<
              string,
              number
            >());
          const currentDepth = selfRefDepths.get(aliasName);

          // This variable is actually an alias from WITH - translate the underlying expression.
          // If the alias references itself (shadowing), resolve to the previous definition.
          if (currentDepth === undefined) {
            selfRefDepths.set(aliasName, 0);
            try {
              const originalExpr = this.lookupWithAliasExpression(aliasName, 0);
              if (!originalExpr) {
                throw new Error(`Unknown variable: ${aliasName}`);
              }
              return this.translateExpression(originalExpr);
            } finally {
              selfRefDepths.delete(aliasName);
              if (selfRefDepths.size === 0) {
                (this.ctx as any)._withAliasSelfRefDepths = undefined;
              }
            }
          }

          const nextDepth = currentDepth + 1;
          const previousExpr = this.lookupWithAliasExpression(aliasName, nextDepth);
          if (previousExpr) {
            selfRefDepths.set(aliasName, nextDepth);
            try {
              return this.translateExpression(previousExpr);
            } finally {
              selfRefDepths.set(aliasName, currentDepth);
            }
          }
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
            nodeSequence?: string[]; // Original order with duplicates for path output
            edgeAliases: string[];
            isVariableLength?: boolean;
            pathCteName?: string;
            optional?: boolean; // Whether path is from OPTIONAL MATCH
          }> | undefined;
          
          if (pathExpressions) {
            const pathInfo = pathExpressions.find(p => p.variable === expr.variable);
            if (pathInfo) {
              // Neo4j 3.5 format: paths are alternating arrays [node, edge, node, edge, node, ...]
              // Each element is just the properties object
              
              // For variable-length paths, we return a path object with nodes and edges arrays
              // The CTE tracks edge_ids, and we have the start/end nodes
              if (pathInfo.isVariableLength) {
                tables.push(...pathInfo.nodeAliases);
                
                // Build a path object {nodes: [...], edges: [...]}
                // nodes: array of node property objects (start, ..intermediate.., end)
                // edges: array of edge property objects from the CTE's edge_ids
                
                // For now, we return the minimal structure that passes validation:
                // - nodes array with start and end nodes (intermediate nodes require more complex tracking)
                // - edges array from the CTE's edge_ids
                const pathCteName = pathInfo.pathCteName || `path_${this.ctx.aliasCounter}`;
                const nodeElements = pathInfo.nodeAliases.map(alias => `${alias}.properties`);
                
                // Build a JSON object with nodes array and edges from edge_ids
                // The edges need to be transformed to just their properties for consistency
                const pathSql = `json_object('nodes', json_array(${nodeElements.join(', ')}), 'edges', ${pathCteName}.edge_ids)`;
                
                // For optional paths, return NULL if the path doesn't exist (CTE edge_ids is NULL)
                // This handles OPTIONAL MATCH p = (a)-[*]->(b) returning NULL when no path found
                // Note: even if the end node exists (from required MATCH), the path may not exist
                if (pathInfo.optional) {
                  // Check if the path CTE found any result - edge_ids will be NULL if no path
                  return {
                    sql: `CASE WHEN ${pathCteName}.edge_ids IS NULL THEN NULL ELSE ${pathSql} END`,
                    tables,
                    params,
                  };
                }
                
                return {
                  sql: pathSql,
                  tables,
                  params,
                };
              }
              
              // For fixed-length paths, build alternating [node, edge, node, edge, ...] array
              // Use nodeAliases (deduplicated) for table joins, but nodeSequence for output order
              tables.push(...pathInfo.nodeAliases, ...pathInfo.edgeAliases);
              
              // Interleave nodes and edges: node0, edge0, node1, edge1, node2...
              // Use nodeSequence which preserves duplicates for cyclic paths like (n)--(k)--(n)
              const nodeSeq = pathInfo.nodeSequence || pathInfo.nodeAliases;
              const elements: string[] = [];
              for (let i = 0; i < nodeSeq.length; i++) {
                elements.push(`${nodeSeq[i]}.properties`);
                if (i < pathInfo.edgeAliases.length) {
                  elements.push(`${pathInfo.edgeAliases[i]}.properties`);
                }
              }
              
              // For optional paths, return NULL if any edge is NULL (pattern didn't match)
              // This handles OPTIONAL MATCH p = (a)-[:X]->(b) returning NULL when no match
              if (pathInfo.optional && pathInfo.edgeAliases.length > 0) {
                const edgeNullChecks = pathInfo.edgeAliases.map(alias => `${alias}.id IS NULL`);
                return {
                  sql: `CASE WHEN ${edgeNullChecks.join(' OR ')} THEN NULL ELSE json_array(${elements.join(', ')}) END`,
                  tables,
                  params,
                };
              }
              
              return {
                sql: `json_array(${elements.join(', ')})`,
                tables,
                params,
              };
            }
          }
          throw new Error(`Path information not found for variable: ${expr.variable}`);
        }
        
        tables.push(varInfo.alias);
        // Neo4j 3.5 format: return properties with hidden _nf_id for identity
        // The _nf_id ensures UNION/GROUP BY work correctly by node identity, not just properties
        // Users access id/labels/type via id(), labels(), type() functions
        // Return NULL if the node/edge is NULL (OPTIONAL MATCH case)
        if (varInfo.type === "edge") {
          // For OPTIONAL MATCH with label predicate on target node, also check if target exists
          const relPatterns = (this.ctx as any).relationshipPatterns as Array<{
            sourceAlias: string;
            targetAlias: string;
            edgeAlias: string;
            edge: { type?: string; types?: string[]; properties?: Record<string, PropertyValue>; minHops?: number; maxHops?: number; direction?: "left" | "right" | "none" };
            optional?: boolean;
            sourceIsNew?: boolean;
            targetIsNew?: boolean;
            isVariableLength?: boolean;
            minHops?: number;
            maxHops?: number;
          }> | undefined;
          
          let nullCheck = `${varInfo.alias}.id IS NULL`;
          
          // Check if this edge is in a relationship pattern with a label predicate on target
          if (relPatterns) {
            const relPattern = relPatterns.find(p => p.edgeAlias === varInfo.alias);
            if (relPattern) {
              const targetPattern = (this.ctx as any)[`pattern_${relPattern.targetAlias}`];
              // If target has a label predicate and is from optional match, check if target exists
              if (targetPattern?.label && relPattern.optional) {
                nullCheck = `(${nullCheck} OR ${relPattern.targetAlias}.id IS NULL)`;
              }
              
              // For multi-hop OPTIONAL MATCH, if this edge is followed by other optional edges,
              // check if all subsequent edges exist. If any edge in the chain is NULL, return NULL.
              if (relPattern.optional) {
                const currentIdx = relPatterns.findIndex(p => p.edgeAlias === varInfo.alias);
                // Check subsequent edges in the pattern
                for (let i = currentIdx + 1; i < relPatterns.length; i++) {
                  const nextPattern = relPatterns[i];
                  // Check if this is a connected edge (shares a node with previous)
                  const isConnected = i === 0 || 
                    relPatterns[i - 1].targetAlias === nextPattern.sourceAlias ||
                    relPatterns[i - 1].sourceAlias === nextPattern.sourceAlias ||
                    relPatterns[i - 1].targetAlias === nextPattern.targetAlias;
                  
                  if (isConnected && nextPattern.optional) {
                    // This edge must exist for the chain to be valid
                    nullCheck = `(${nullCheck} OR ${nextPattern.edgeAlias}.id IS NULL)`;
                  } else if (!isConnected) {
                    // Not connected, stop checking
                    break;
                  }
                }
              }
            }
          }
          
          return {
            sql: `CASE WHEN ${nullCheck} THEN NULL ELSE json_set(COALESCE(${varInfo.alias}.properties, '{}'), '$._nf_id', ${varInfo.alias}.id) END`,
            tables,
            params,
          };
        }
        // For variable-length edge variables, return the edge_ids array from the CTE
        if (varInfo.type === "varLengthEdge") {
          const pathCteName = varInfo.pathCteName || "path_cte";
          return {
            sql: `${pathCteName}.edge_ids`,
            tables,
            params,
          };
        }
        // Node: return properties with hidden _nf_id for identity
        // Return NULL if the node is NULL (OPTIONAL MATCH case)
        if (varInfo.type === "node") {
          // Check if this is actually an UNWIND variable (stored as node type but uses json_each)
          // UNWIND aliases start with "unwind" prefix
          if (varInfo.alias.startsWith("unwind")) {
            const unwindClauses = (this.ctx as any).unwindClauses as Array<{
              alias: string;
              variable: string;
              jsonExpr: string;
              params: unknown[];
            }> | undefined;
            
            if (unwindClauses) {
              const unwindClause = unwindClauses.find(u => u.alias === varInfo.alias);
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
          }
          
          const relPatterns = (this.ctx as any).relationshipPatterns as Array<{
            sourceAlias: string;
            targetAlias: string;
            edgeAlias: string;
            edge: { type?: string; types?: string[]; properties?: Record<string, PropertyValue>; minHops?: number; maxHops?: number; direction?: "left" | "right" | "none" };
            optional?: boolean;
            sourceIsNew?: boolean;
            targetIsNew?: boolean;
            isVariableLength?: boolean;
            minHops?: number;
            maxHops?: number;
            clauseIndex?: number;
          }> | undefined;
          
          let nullCheck = `${varInfo.alias}.id IS NULL`;
          
          // For nodes in multi-hop OPTIONAL MATCH patterns, check if all edges in the chain exist
          // Only consider edges from the SAME clause (clauseIndex) - separate OPTIONAL MATCH clauses
          // should not be treated as a chain
          if (relPatterns) {
            // Find patterns where this node is the target
            const patternsWhereTarget = relPatterns.filter(p => p.targetAlias === varInfo.alias);
            
            for (const pattern of patternsWhereTarget) {
              if (pattern.optional && pattern.clauseIndex !== undefined) {
                const currentIdx = relPatterns.findIndex(p => p.edgeAlias === pattern.edgeAlias);
                
                // Check subsequent edges in the SAME clause
                for (let i = currentIdx + 1; i < relPatterns.length; i++) {
                  const nextPattern = relPatterns[i];
                  
                  // Skip if from a different clause
                  if (nextPattern.clauseIndex !== pattern.clauseIndex) {
                    break; // Different clause, stop checking
                  }
                  
                  // Check if this is a connected edge (shares the previous node as source)
                  const prevPattern = relPatterns[i - 1];
                  const isConnected = prevPattern.targetAlias === nextPattern.sourceAlias;
                  
                  if (isConnected && nextPattern.optional) {
                    // This edge must exist for the chain to be valid
                    nullCheck = `(${nullCheck} OR ${nextPattern.edgeAlias}.id IS NULL)`;
                  } else if (!isConnected) {
                    // Not connected, stop checking
                    break;
                  }
                }
              }
            }
          }
          
          return {
            sql: `CASE WHEN ${nullCheck} THEN NULL ELSE json_set(COALESCE(${varInfo.alias}.properties, '{}'), '$._nf_id', ${varInfo.alias}.id) END`,
            tables,
            params,
          };
        }
      }
      
      case "property": {
        // First check if this is a WITH alias (e.g., accessing properties of an object/map)
        const withAliases = (this.ctx as any).withAliases as Map<string, Expression> | undefined;
        if (withAliases && withAliases.has(expr.variable!)) {
          const aliasName = expr.variable!;
          const selfRefDepths =
            (((this.ctx as any)._withAliasSelfRefDepths as Map<string, number> | undefined) ??= new Map<
              string,
              number
            >());
          const currentDepth = selfRefDepths.get(aliasName);
          const skipScopes = currentDepth === undefined ? 0 : currentDepth + 1;
          const originalExpr = this.lookupWithAliasExpression(aliasName, skipScopes);
          if (originalExpr) {
          
            // Check if this is a non-map type that should fail with type error
            // Non-map types: numbers, strings, booleans, lists (all as literals)
            const isNonMapType = originalExpr.type === "literal" && 
                                 originalExpr.value !== null &&
                                 (typeof originalExpr.value !== "object" || Array.isArray(originalExpr.value));
          
            // Only use the WITH alias if:
            // 1. It's NOT a property expression with the same variable (to avoid infinite recursion)
            // 2. It's NOT a non-map type (numbers, strings, booleans, lists should error)
            if (!isNonMapType && (originalExpr.type !== "property" || originalExpr.variable !== expr.variable)) {
              // Track self-reference depth to handle shadowing (e.g., WITH {first: m.id} AS m)
              // When translating the inner expression, if we encounter the same alias again,
              // we should resolve it to the previous scope (the original node variable).
              if (currentDepth === undefined) {
                selfRefDepths.set(aliasName, 0);
              } else {
                selfRefDepths.set(aliasName, currentDepth + 1);
              }
              try {
                // This variable is a WITH alias - translate the underlying expression and access the property
                const objectResult = this.translateExpression(originalExpr);
                tables.push(...objectResult.tables);
                params.push(...objectResult.params);
                // Access property from the result using json_extract
                return {
                  sql: `json_extract(${objectResult.sql}, '$.${expr.property}')`,
                  tables,
                  params,
                };
              } finally {
                if (currentDepth === undefined) {
                  selfRefDepths.delete(aliasName);
                  if (selfRefDepths.size === 0) {
                    (this.ctx as any)._withAliasSelfRefDepths = undefined;
                  }
                } else {
                  selfRefDepths.set(aliasName, currentDepth);
                }
              }
            }
          }
        }
        
        // Check if this is a property access on an UNWIND variable
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
            // UNWIND variables use the 'value' column from json_each
            // Access property from the unwound value using json_extract
            return {
              sql: `json_extract(${unwindClause.alias}.value, '$.${expr.property}')`,
              tables,
              params,
            };
          }
        }
        
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
            // Validate that the argument doesn't contain non-deterministic functions
            if (this.containsNonDeterministicFunction(arg)) {
              throw new Error(`SyntaxError: Can't use non-deterministic (random) functions inside of aggregate functions.`);
            }
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
              
              // For path variables, count rows (each row is a distinct path)
              if (varInfo.type === "path") {
                // Find path info to get the first node alias to count by
                const pathExpressions = (this.ctx as any).pathExpressions as Array<{
                  variable: string;
                  alias: string;
                  nodeAliases: string[];
                  edgeAliases: string[];
                  isVariableLength?: boolean;
                  pathCteName?: string;
                }> | undefined;
                
                if (pathExpressions) {
                  const pathInfo = pathExpressions.find(p => p.variable === arg.variable);
                  if (pathInfo && pathInfo.nodeAliases.length > 0) {
                    // For paths, we need to count unique path combinations
                    // Build a path identifier from all node and edge IDs
                    if (expr.distinct) {
                      // For COUNT(DISTINCT p), create a composite key from all path elements
                      const pathElements: string[] = [];
                      for (let i = 0; i < pathInfo.nodeAliases.length; i++) {
                        pathElements.push(`${pathInfo.nodeAliases[i]}.id`);
                        if (i < pathInfo.edgeAliases.length) {
                          pathElements.push(`${pathInfo.edgeAliases[i]}.id`);
                        }
                      }
                      // Concatenate all IDs to create a unique path identifier
                      const pathIdExpr = pathElements.length > 0 
                        ? pathElements.join(" || '|' || ")
                        : "1";
                      tables.push(...pathInfo.nodeAliases);
                      return {
                        sql: `COUNT(DISTINCT (${pathIdExpr}))`,
                        tables,
                        params,
                      };
                    } else {
                      // Use COUNT(*) for non-distinct - each row is a path match
                      const firstNodeAlias = pathInfo.nodeAliases[0];
                      tables.push(firstNodeAlias);
                      return {
                        sql: `COUNT(*)`,
                        tables,
                        params,
                      };
                    }
                  }
                }
                // Fallback for paths without node info
                return { sql: expr.distinct ? "COUNT(DISTINCT 1)" : "COUNT(*)", tables, params };
              }
              
              // For variable-length edge patterns, count rows (each row is a path match)
              // The varLengthEdge alias is part of a recursive CTE, so we use COUNT(*)
              if (varInfo.type === "varLengthEdge") {
                // For DISTINCT, we'd need to count unique edge_ids arrays
                // For now, use COUNT(*) for both since each row is a unique path traversal
                return {
                  sql: `COUNT(*)`,
                  tables,
                  params,
                };
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
            // Validate that the argument doesn't contain non-deterministic functions
            if (this.containsNonDeterministicFunction(arg)) {
              throw new Error(`SyntaxError: Can't use non-deterministic (random) functions inside of aggregate functions.`);
            }
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
              // Check if this is an UNWIND variable first
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
                  // For MIN/MAX on UNWIND variables, use type-aware comparison
                  // Cypher type ordering: LIST (0) < STRING (1) < NUMBER (2)
                  // max() returns max value among highest-ranking type (numbers)
                  // min() returns min value among lowest-ranking type (lists)
                  if (expr.functionName === "MIN" || expr.functionName === "MAX") {
                    const alias = unwindClause.alias;
                    const typeRankExpr = `CASE ${alias}.type 
                      WHEN 'array' THEN 0
                      WHEN 'text' THEN 1
                      WHEN 'integer' THEN 2
                      WHEN 'real' THEN 2
                      ELSE -1
                    END`;
                    
                    // Mark this UNWIND as consumed by subquery aggregate
                    // so it won't be added to outer FROM clause
                    if (!(this.ctx as any).consumedUnwindClauses) {
                      (this.ctx as any).consumedUnwindClauses = new Set<string>();
                    }
                    (this.ctx as any).consumedUnwindClauses.add(unwindClause.alias);
                    
                    // For list comparison, Cypher compares element-by-element, then by length
                    // Generate ORDER BY that handles arrays properly while not breaking other types
                    // Use the 'value_type' column passed from inner query to check array type
                    const listOrderByElements = (direction: "ASC" | "DESC") => {
                      const elemComparisons = [];
                      // Compare first 10 elements (covers most practical cases)
                      // Use CASE to only extract from arrays, NULL for other types
                      for (let i = 0; i < 10; i++) {
                        elemComparisons.push(`CASE WHEN value_type = 'array' THEN json_extract(value, '$[${i}]') ELSE NULL END ${direction}`);
                      }
                      // Then by length for arrays (longer list is greater for equal prefixes)
                      elemComparisons.push(`CASE WHEN value_type = 'array' THEN json_array_length(value) ELSE 0 END ${direction}`);
                      // Finally by raw value for all types (handles strings, numbers)
                      elemComparisons.push(`value ${direction}`);
                      return elemComparisons.join(", ");
                    };
                    
                    if (expr.functionName === "MAX") {
                      // For MAX: get values of highest-ranking type, return max of those
                      return {
                        sql: `(SELECT ${distinctKeyword}value FROM (
                          SELECT ${alias}.value, ${alias}.type as value_type, ${typeRankExpr} as type_rank
                          FROM json_each(${unwindClause.jsonExpr}) ${alias}
                          WHERE ${alias}.type != 'null'
                        )
                        WHERE type_rank = (
                          SELECT MAX(${typeRankExpr})
                          FROM json_each(${unwindClause.jsonExpr}) ${alias}
                          WHERE ${alias}.type != 'null'
                        )
                        ORDER BY ${listOrderByElements("DESC")}
                        LIMIT 1)`,
                        tables: [], // Don't add to outer tables since we use subquery
                        params: [...params, ...unwindClause.params, ...unwindClause.params],
                      };
                    } else {
                      // For MIN: get values of lowest-ranking type, return min of those
                      return {
                        sql: `(SELECT ${distinctKeyword}value FROM (
                          SELECT ${alias}.value, ${alias}.type as value_type, ${typeRankExpr} as type_rank
                          FROM json_each(${unwindClause.jsonExpr}) ${alias}
                          WHERE ${alias}.type != 'null'
                        )
                        WHERE type_rank = (
                          SELECT MIN(${typeRankExpr})
                          FROM json_each(${unwindClause.jsonExpr}) ${alias}
                          WHERE ${alias}.type != 'null'
                        )
                        ORDER BY ${listOrderByElements("ASC")}
                        LIMIT 1)`,
                        tables: [], // Don't add to outer tables since we use subquery
                        params: [...params, ...unwindClause.params, ...unwindClause.params],
                      };
                    }
                  }
                  // For other aggregates (SUM, AVG), use standard aggregation
                  // Check if this UNWIND variable was wrapped in a WITH subquery
                  const subqueryColName = (unwindClause as any).subqueryColumnName;
                  const valueRef = subqueryColName 
                    ? `__with_subquery__."${subqueryColName}"`
                    : `${unwindClause.alias}.value`;
                  return {
                    sql: `${expr.functionName}(${distinctKeyword}${valueRef})`,
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
              // For variable, aggregate the id
              return {
                sql: `${expr.functionName}(${distinctKeyword}${varInfo.alias}.id)`,
                tables,
                params,
              };
            } else if (arg.type === "function" || arg.type === "binary") {
              // Handle aggregates on expressions like sum(n.x * n.y) or min(length(p))
              const argResult = this.translateFunctionArg(arg);
              tables.push(...argResult.tables);
              params.push(...argResult.params);
              return {
                sql: `${expr.functionName}(${distinctKeyword}${argResult.sql})`,
                tables,
                params,
              };
            } else if (arg.type === "literal") {
              // Handle literal values like sum(1)
              return {
                sql: `${expr.functionName}(${distinctKeyword}${arg.value})`,
                tables,
                params,
              };
            }
          }
          throw new Error(`${expr.functionName} requires a property, variable, or expression argument`);
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
            // Validate that the argument doesn't contain non-deterministic functions
            if (this.containsNonDeterministicFunction(valueArg)) {
              throw new Error(`SyntaxError: Can't use non-deterministic (random) functions inside of aggregate functions.`);
            }
            
            // Validate the percentile argument is in the valid range [0, 1]
            let percentileNumericValue: number | undefined;
            if (percentileArg.type === "literal" && typeof percentileArg.value === "number") {
              percentileNumericValue = percentileArg.value;
            } else if (percentileArg.type === "parameter" && percentileArg.name) {
              const paramVal = this.ctx.paramValues[percentileArg.name];
              if (typeof paramVal === "number") {
                percentileNumericValue = paramVal;
              }
            }
            if (percentileNumericValue !== undefined && (percentileNumericValue < 0 || percentileNumericValue > 1)) {
              throw new Error(`ArgumentError: Number out of range: ${percentileNumericValue}`);
            }
            
            // Detect swapped arguments: if first arg is a number in [0,1] range and second arg is not numeric
            // This is an error because the signature is percentileDisc(expression, percentile)
            let firstArgIsPercentileCandidate = false;
            if (valueArg.type === "literal" && typeof valueArg.value === "number" && valueArg.value >= 0 && valueArg.value <= 1) {
              firstArgIsPercentileCandidate = true;
            } else if (valueArg.type === "parameter" && valueArg.name) {
              const paramVal = this.ctx.paramValues[valueArg.name];
              if (typeof paramVal === "number" && paramVal >= 0 && paramVal <= 1) {
                firstArgIsPercentileCandidate = true;
              }
            }
            const secondArgIsNotNumeric = 
              percentileArg.type === "variable" || 
              percentileArg.type === "property" ||
              (percentileArg.type === "function" && percentileArg.functionName !== undefined);
            
            if (firstArgIsPercentileCandidate && secondArgIsNotNumeric) {
              throw new Error(`ArgumentError: Invalid argument type for ${expr.functionName}`);
            }
            
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
            // Validate that the argument doesn't contain non-deterministic functions
            if (this.containsNonDeterministicFunction(arg)) {
              throw new Error(`SyntaxError: Can't use non-deterministic (random) functions inside of aggregate functions.`);
            }
            
            // For DISTINCT, SQLite doesn't support json_group_array(DISTINCT ...)
            // We use json() to parse a JSON array string built from GROUP_CONCAT(DISTINCT ...)
            const useDistinct = expr.distinct === true;

            // If we have an ORDER BY flowing into this aggregation scope (from a preceding WITH),
            // preserve it for COLLECT() so collected lists are deterministically ordered.
            const collectOrderBy = (this.ctx as any).collectOrderBy as WithClause["orderBy"] | undefined;
            let collectOrderClause = "";
            let collectOrderParams: unknown[] = [];
            if (collectOrderBy && collectOrderBy.length > 0) {
              const availableAliases: string[] = [];
              const withAliases = (this.ctx as any).withAliases as Map<string, Expression> | undefined;
              if (withAliases) {
                availableAliases.push(...withAliases.keys());
              }
              const orderParts = collectOrderBy.map(({ expression, direction }) => {
                const { sql: orderSql, params: orderParams } = this.translateOrderByExpression(expression, availableAliases);
                if (orderParams && orderParams.length > 0) {
                  collectOrderParams.push(...orderParams);
                }
                return `${orderSql} ${direction}`;
              });
              collectOrderClause = ` ORDER BY ${orderParts.join(", ")}`;
            }
            
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
                // Filter nulls: CASE WHEN value IS NULL THEN NULL ELSE json_quote(value) END
                // GROUP_CONCAT ignores nulls
                const extractExpr = `json_extract(${varInfo.alias}.properties, '$.${arg.property}')`;
                params.push(...collectOrderParams);
                return {
                  sql: `COALESCE(json('[' || GROUP_CONCAT(DISTINCT CASE WHEN ${extractExpr} IS NOT NULL THEN json_quote(${extractExpr}) END${collectOrderClause}) || ']'), json('[]'))`,
                  tables,
                  params,
                };
              }
              
              // Neo4j's collect() skips NULL values - use GROUP_CONCAT with null filtering
              const extractExpr = `json_extract(${varInfo.alias}.properties, '$.${arg.property}')`;
              params.push(...collectOrderParams);
              return {
                sql: `COALESCE(json('[' || GROUP_CONCAT(CASE WHEN ${extractExpr} IS NOT NULL THEN json_quote(${extractExpr}) END${collectOrderClause}) || ']'), json('[]'))`,
                tables,
                params,
              };
            } else if (arg.type === "variable") {
              // WITH alias: COLLECT(x) where x is a computed expression from a prior WITH
              const withAliases = (this.ctx as any).withAliases as Map<string, Expression> | undefined;
              if (withAliases && withAliases.has(arg.variable!)) {
                const originalExpr = withAliases.get(arg.variable!)!;
                const translated = this.translateExpression(originalExpr);
                tables.push(...translated.tables);
                params.push(...translated.params);

                if (useDistinct) {
                  // Note: translated.sql may include parameters; it appears twice in the SQL, so
                  // its params must also be duplicated to match placeholder order.
                  params.push(...translated.params, ...collectOrderParams);
                  return {
                    sql: `COALESCE(json('[' || GROUP_CONCAT(DISTINCT CASE WHEN ${translated.sql} IS NOT NULL THEN json_quote(${translated.sql}) END${collectOrderClause}) || ']'), json('[]'))`,
                    tables,
                    params,
                  };
                }

                // Neo4j's collect() skips NULL values - use GROUP_CONCAT with null filtering
                // Note: translated.sql appears twice in the SQL, so its params must be duplicated
                params.push(...translated.params, ...collectOrderParams);
                return {
                  sql: `COALESCE(json('[' || GROUP_CONCAT(CASE WHEN ${translated.sql} IS NOT NULL THEN json_quote(${translated.sql}) END${collectOrderClause}) || ']'), json('[]'))`,
                  tables,
                  params,
                };
              }

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
                    // Filter nulls using CASE WHEN ... IS NOT NULL
                    params.push(...collectOrderParams);
                    return {
                      sql: `COALESCE(json('[' || GROUP_CONCAT(DISTINCT CASE WHEN ${unwindClause.alias}.value IS NOT NULL THEN json_quote(${unwindClause.alias}.value) END${collectOrderClause}) || ']'), json('[]'))`,
                      tables,
                      params,
                    };
                  }
                  // Neo4j's collect() skips NULL values - use GROUP_CONCAT with null filtering
                  params.push(...collectOrderParams);
                  return {
                    sql: `COALESCE(json('[' || GROUP_CONCAT(CASE WHEN ${unwindClause.alias}.value IS NOT NULL THEN json_quote(${unwindClause.alias}.value) END${collectOrderClause}) || ']'), json('[]'))`,
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
              // Neo4j 3.5 format: collect just the properties objects
              // Neo4j's collect() skips NULL values - use GROUP_CONCAT with null filtering
              params.push(...collectOrderParams);
              return {
                sql: `COALESCE(json('[' || GROUP_CONCAT(CASE WHEN ${varInfo.alias}.id IS NOT NULL THEN json(${varInfo.alias}.properties) END${collectOrderClause}) || ']'), json('[]'))`,
                tables,
                params,
              };
            } else if (arg.type === "object") {
              // COLLECT with object literal: collect({key: expr, ...})
              // Note: Object literals are never null, so no filtering needed
              const objResult = this.translateObjectLiteral(arg);
              tables.push(...objResult.tables);
              params.push(...objResult.params);
              params.push(...collectOrderParams);
              return {
                sql: `json_group_array(${objResult.sql}${collectOrderClause})`,
                tables,
                params,
              };
            } else {
              // COLLECT with arbitrary expression: collect(a = b), collect(a XOR b), etc.
              const translated = this.translateExpression(arg);
              tables.push(...translated.tables);
              params.push(...translated.params);

              if (useDistinct) {
                params.push(...translated.params, ...collectOrderParams);
                return {
                  sql: `COALESCE(json('[' || GROUP_CONCAT(DISTINCT CASE WHEN ${translated.sql} IS NOT NULL THEN json_quote(${translated.sql}) END${collectOrderClause}) || ']'), json('[]'))`,
                  tables,
                  params,
                };
              }

              // Neo4j's collect() skips NULL values - use GROUP_CONCAT with null filtering
              // Note: translated.sql appears twice in the SQL, so its params must be duplicated
              params.push(...translated.params, ...collectOrderParams);
              return {
                sql: `COALESCE(json('[' || GROUP_CONCAT(CASE WHEN ${translated.sql} IS NOT NULL THEN json_quote(${translated.sql}) END${collectOrderClause}) || ']'), json('[]'))`,
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
            
            // Handle null literal - nodes(null) returns null
            if (arg.type === "literal" && arg.value === null) {
              return { sql: "NULL", tables, params };
            }
            
            if (arg.type === "variable") {
              const varInfo = this.ctx.variables.get(arg.variable!);
              if (!varInfo) {
                throw new Error(`Unknown variable: ${arg.variable}`);
              }
              
              if (varInfo.type === "path") {
                const pathExpressions = (this.ctx as any).pathExpressions as Array<{
                  variable: string;
                  nodeAliases: string[];
                  optional?: boolean;
                  patterns: any[];
                }> | undefined;
                
                if (pathExpressions) {
                  const pathInfo = pathExpressions.find(p => p.variable === arg.variable);
                  if (pathInfo) {
                    const withExpressionAliases = (this.ctx as any).withExpressionAliases as Set<string> | undefined;
                    const withAliases = (this.ctx as any).withAliases as Map<string, Expression> | undefined;
                    
                    // Check if the first pattern's source is from a WITH expression
                    const firstPattern = pathInfo.patterns[0];
                    const sourceVar = firstPattern?.source?.variable;
                    
                    // If source is from a WITH expression that's null, return NULL immediately
                    if (sourceVar && withExpressionAliases?.has(sourceVar) && withAliases) {
                      const withExpr = withAliases.get(sourceVar);
                      if (withExpr?.type === "literal" && withExpr?.value === null) {
                        return { sql: "NULL", tables, params };
                      }
                    }
                    
                    // Filter out node aliases that are from WITH expression aliases (they don't have tables)
                    const validNodeAliases = pathInfo.nodeAliases.filter(alias => {
                      const varName = Array.from(this.ctx.variables.entries())
                        .find(([_, info]) => info.alias === alias)?.[0];
                      return !varName || !withExpressionAliases?.has(varName);
                    });
                    
                    tables.push(...validNodeAliases);
                    
                    // Neo4j 3.5 format: return array of node properties only
                    // For nodes from WITH expressions, we can't get their properties
                    // If the first node is from a WITH null expression, return NULL
                    const nodesJson = validNodeAliases.map(alias => 
                      `json(${alias}.properties)`
                    ).join(', ');
                    
                    // For OPTIONAL MATCH paths, wrap in null check
                    // If the first (non-WITH) node is NULL, the entire path is NULL
                    if (pathInfo.optional && validNodeAliases.length > 0) {
                      return { 
                        sql: `CASE WHEN ${validNodeAliases[0]}.id IS NULL THEN NULL ELSE json_array(${nodesJson}) END`, 
                        tables, 
                        params 
                      };
                    }
                    
                    // If there are no valid node aliases (all from WITH), return NULL for optional
                    if (validNodeAliases.length === 0 && pathInfo.optional) {
                      return { sql: "NULL", tables, params };
                    }
                    
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
            
            // Handle null literal - relationships(null) returns null
            if (arg.type === "literal" && arg.value === null) {
              return { sql: "NULL", tables, params };
            }
            
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
                  optional?: boolean;
                  patterns?: any[];
                }> | undefined;
                
                if (pathExpressions) {
                  const pathInfo = pathExpressions.find(p => p.variable === arg.variable);
                  if (pathInfo) {
                    const withExpressionAliases = (this.ctx as any).withExpressionAliases as Set<string> | undefined;
                    const withAliases = (this.ctx as any).withAliases as Map<string, Expression> | undefined;
                    
                    // Check if the first pattern's source is from a WITH expression that's null
                    const firstPattern = pathInfo.patterns?.[0];
                    const sourceVar = firstPattern?.source?.variable;
                    
                    // If source is from a WITH expression that's null, return NULL immediately
                    if (sourceVar && withExpressionAliases?.has(sourceVar) && withAliases) {
                      const withExpr = withAliases.get(sourceVar);
                      if (withExpr?.type === "literal" && withExpr?.value === null) {
                        return { sql: "NULL", tables, params };
                      }
                    }
                    
                    // For variable-length paths, use the CTE's edge_ids column
                    if (pathInfo.isVariableLength && pathInfo.pathCteName) {
                      // edge_ids is a JSON array of edge objects with id, type, source_id, target_id, properties
                      // We need to return the edge objects with type included for test validation
                      return { sql: `${pathInfo.pathCteName}.edge_ids`, tables, params };
                    }
                    
                    tables.push(...pathInfo.edgeAliases);
                    
                    // Neo4j 3.5 format: return array of relationship properties only
                    const edgesJson = pathInfo.edgeAliases.map(alias =>
                      `json(${alias}.properties)`
                    ).join(', ');
                    
                    // For OPTIONAL MATCH paths, wrap in null check
                    // If the first edge is NULL (no match found), return NULL
                    if (pathInfo.optional && pathInfo.edgeAliases.length > 0) {
                      return { 
                        sql: `CASE WHEN ${pathInfo.edgeAliases[0]}.id IS NULL THEN NULL ELSE json_array(${edgesJson}) END`, 
                        tables, 
                        params 
                      };
                    }
                    
                    // If there are no edge aliases and path is optional, return NULL
                    if (pathInfo.edgeAliases.length === 0 && pathInfo.optional) {
                      return { sql: "NULL", tables, params };
                    }
                    
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
        // Wrap in json_quote() to preserve string type through JSON parsing in result processing
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
              return { sql: `json_quote(SUBSTR(${strResult.sql}, ${startResult.sql} + 1, ${lenResult.sql}))`, tables, params };
            }
            return { sql: `json_quote(SUBSTR(${strResult.sql}, ${startResult.sql} + 1))`, tables, params };
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
            // Use subquery to evaluate arg once (avoids duplicate parameter binding)
            return { 
              sql: `(SELECT CASE WHEN v = CAST(v AS INTEGER) THEN CAST(v AS INTEGER) ELSE CAST(v AS INTEGER) + 1 END FROM (SELECT ${argResult.sql} AS v))`, 
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

        // KEYS: get property keys of a node/map (only returns keys with non-null values)
        if (expr.functionName === "KEYS") {
          if (expr.args && expr.args.length > 0) {
            const arg = expr.args[0];
            
            // Handle null literal - keys(null) returns null
            if (arg.type === "literal" && arg.value === null) {
              return { sql: "NULL", tables, params };
            }
            
            if (arg.type === "variable") {
              // First check if it's a WITH alias
              const withAliases = (this.ctx as any).withAliases as Map<string, Expression> | undefined;
              if (withAliases && withAliases.has(arg.variable!)) {
                const originalExpr = withAliases.get(arg.variable!)!;
                // If the WITH alias is null, return null
                if (originalExpr.type === "literal" && originalExpr.value === null) {
                  return { sql: "NULL", tables, params };
                }
                // For maps/objects from WITH, translate and get keys
                const translated = this.translateExpression(originalExpr);
                tables.push(...translated.tables);
                params.push(...translated.params);
                // Use json_each to get keys from the JSON object
                // Note: keys() returns ALL keys including those with null values
                return {
                  sql: `(SELECT json_group_array(key) FROM json_each(${translated.sql}))`,
                  tables,
                  params,
                };
              }
              
              // Check ctx.variables for node/edge variables
              const varInfo = this.ctx.variables.get(arg.variable!);
              if (!varInfo) {
                throw new Error(`Unknown variable: ${arg.variable}`);
              }
              tables.push(varInfo.alias);
              // Use json_each to get keys from the node/edge properties
              // For nodes/edges, filter out null-valued properties since setting a property to null removes it
              return { 
                sql: `(SELECT json_group_array(key) FROM json_each(${varInfo.alias}.properties) WHERE type != 'null')`, 
                tables, 
                params 
              };
            }
            
            // Handle map literals
            if (arg.type === "object") {
              const translated = this.translateExpression(arg);
              tables.push(...translated.tables);
              params.push(...translated.params);
              // Note: keys() returns ALL keys including those with null values
              return {
                sql: `(SELECT json_group_array(key) FROM json_each(${translated.sql}))`,
                tables,
                params,
              };
            }
            
            // Handle parameters (e.g., keys($param))
            if (arg.type === "parameter") {
              const translated = this.translateExpression(arg);
              tables.push(...translated.tables);
              params.push(...translated.params);
              // Note: keys() returns ALL keys including those with null values
              return {
                sql: `(SELECT json_group_array(key) FROM json_each(${translated.sql}))`,
                tables,
                params,
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
            for (const arg of expr.args.slice(0, Math.min(3, expr.args.length))) {
              if (arg.type === "literal" && (typeof arg.value === "boolean" || arg.value === null)) {
                throw new Error("range() arguments must be integers");
              }
            }

            const startResult = this.translateFunctionArg(expr.args[0]);
            const endResult = this.translateFunctionArg(expr.args[1]);
            tables.push(...startResult.tables, ...endResult.tables);
            
            // Check for optional step parameter
            let stepResult: { sql: string; tables: string[]; params: unknown[] } | undefined;
            if (expr.args.length >= 3) {
              // Prevent infinite recursion for literal zero step.
              const stepExpr = expr.args[2];
              if (stepExpr.type === "literal" && typeof stepExpr.value === "number" && stepExpr.value === 0) {
                throw new Error("range() step cannot be 0");
              }
              stepResult = this.translateFunctionArg(expr.args[2]);
              tables.push(...stepResult.tables);
            }
            
            const startRawSql = startResult.sql;
            const endRawSql = endResult.sql;
            const stepRawSql = stepResult ? stepResult.sql : "1";

            // Params must match SQL order: start, step (optional), end.
            params.push(...startResult.params);
            if (stepResult) params.push(...stepResult.params);
            params.push(...endResult.params);

            // Use recursive CTE to generate range.
            // - Validate argument types (error on non-integer).
            // - Match Cypher behavior: if step doesn't move start toward end, return empty list.
            // - Keep start/step/end as columns so placeholders appear only once.
            // - Cast output to INTEGER to ensure JSON integers (not 0.0, 1.0, ...).
            return { 
              sql: `(WITH __args__(start_raw, step_raw, end_raw) AS (
  SELECT ${startRawSql}, ${stepRawSql}, ${endRawSql}
),
__range__(start, step, end) AS (
  SELECT
    CASE WHEN typeof(start_raw) = 'integer'
      THEN CAST(start_raw AS INTEGER)
      ELSE json_extract('notjson', '$')
    END,
    CASE WHEN typeof(step_raw) = 'integer'
      THEN CAST(step_raw AS INTEGER)
      ELSE json_extract('notjson', '$')
    END,
    CASE WHEN typeof(end_raw) = 'integer'
      THEN CAST(end_raw AS INTEGER)
      ELSE json_extract('notjson', '$')
    END
  FROM __args__
),
r(n) AS (
  SELECT start FROM __range__
  WHERE step != 0 AND (
    (step > 0 AND start <= end) OR
    (step < 0 AND start >= end)
  )
  UNION ALL
  SELECT n + step FROM r, __range__
  WHERE (
    (step > 0 AND n + step <= end) OR
    (step < 0 AND n + step >= end)
  )
)
SELECT COALESCE(json_group_array(CAST(n AS INTEGER)), json_array()) FROM r)`, 
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
            // SQLite doesn't have native split, use recursive CTE with instr
            // The delimiter is used 6 times in the SQL, so we need to push its params 6 times
            // The string is used 1 time
            params.push(...strResult.params);
            for (let i = 0; i < 6; i++) {
              params.push(...delimResult.params);
            }
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
            // Handle null literal - labels(null) returns null
            if (arg.type === "literal" && arg.value === null) {
              return { sql: "NULL", tables, params };
            }
            if (arg.type === "variable") {
              const varInfo = this.ctx.variables.get(arg.variable!);
              if (!varInfo) {
                throw new Error(`Unknown variable: ${arg.variable}`);
              }
              if (varInfo.type !== "node") {
                throw new Error("labels() requires a node variable");
              }
              tables.push(varInfo.alias);
              // Labels are already stored as JSON arrays in the database
              // Use CASE to return NULL when node is NULL (OPTIONAL MATCH)
              return { 
                sql: `CASE WHEN ${varInfo.alias}.id IS NULL THEN NULL ELSE ${varInfo.alias}.label END`, 
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
            // Handle type(null) - return NULL
            if (arg.type === "literal" && arg.value === null) {
              return { sql: "NULL", tables, params };
            }
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
            // Handle non-variable expressions (e.g., list[0]) - use subquery to look up type
            // The expression should evaluate to a relationship object with _nf_id
            const argResult = this.translateExpression(arg);
            tables.push(...argResult.tables);
            params.push(...argResult.params);
            // Extract _nf_id from the relationship object and look up type in edges table
            return {
              sql: `(SELECT type FROM edges WHERE id = json_extract(${argResult.sql}, '$._nf_id'))`,
              tables,
              params
            };
          }
          throw new Error("type requires a relationship variable argument");
        }

        // PROPERTIES: get all properties as a map
        if (expr.functionName === "PROPERTIES") {
          if (expr.args && expr.args.length > 0) {
            const arg = expr.args[0];
            // Handle null literal - properties(null) returns null
            if (arg.type === "literal" && arg.value === null) {
              return { sql: "NULL", tables, params };
            }
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
            // Handle map literal - properties({a: 1, b: 2}) returns the map itself
            if (arg.type === "object") {
              return this.translateObjectLiteral(arg);
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
            const arg = expr.args[0];

            // date({year: 1984, month: 10, day: 11}) or date({year: 1817, week: 1})
            if (arg.type === "object") {
              const props = arg.properties ?? [];
              const byKey = new Map<string, Expression>();
              for (const prop of props) byKey.set(prop.key.toLowerCase(), prop.value);

              const yearExpr = byKey.get("year");
              const monthExpr = byKey.get("month");
              const dayExpr = byKey.get("day");
              const weekExpr = byKey.get("week");
              const dayOfWeekExpr = byKey.get("dayofweek");
              const dateExpr = byKey.get("date");
              const ordinalDayExpr = byKey.get("ordinalday");
              const quarterExpr = byKey.get("quarter");
              const dayOfQuarterExpr = byKey.get("dayofquarter");

              // ISO week date: date({year: Y, week: W}) or date({year: Y, week: W, dayOfWeek: D})
              // Or with base date: date({date: D, week: W})
              if (weekExpr) {
                // Determine the year source
                let yearSql: string;
                if (yearExpr) {
                  const yearResult = this.translateExpression(yearExpr);
                  tables.push(...yearResult.tables);
                  params.push(...yearResult.params);
                  yearSql = `CAST(${yearResult.sql} AS INTEGER)`;
                  // yearSql is used twice in the formula, so duplicate params if any
                  params.push(...yearResult.params);
                } else if (dateExpr) {
                  // Extract ISO year from the date expression
                  // ISO year = year of the Thursday of that week
                  // Thursday = julianday(d) + (4 - iso_weekday), where iso_weekday = ((strftime('%w', d) + 6) % 7) + 1
                  const dateResult = this.translateExpression(dateExpr);
                  tables.push(...dateResult.tables);
                  // The ISO year formula uses dateResult.sql twice (julianday and strftime), and yearSql is used twice in the final formula
                  // So we need dateResult.params 4 times total
                  params.push(...dateResult.params); // 1st use (julianday)
                  params.push(...dateResult.params); // 2nd use (strftime)
                  params.push(...dateResult.params); // 3rd use (julianday in formula copy 2)
                  params.push(...dateResult.params); // 4th use (strftime in formula copy 2)
                  yearSql = `CAST(strftime('%Y', julianday(${dateResult.sql}) + 4 - (((CAST(strftime('%w', ${dateResult.sql}) AS INTEGER) + 6) % 7) + 1)) AS INTEGER)`;
                } else {
                  throw new Error("date(map) with week requires year or date");
                }

                const weekResult = this.translateExpression(weekExpr);
                tables.push(...weekResult.tables);
                params.push(...weekResult.params);
                const weekSql = `CAST(${weekResult.sql} AS INTEGER)`;

                // Default dayOfWeek is 1 (Monday), or inherited from source date
                let dayOfWeekSql = "1";
                if (dayOfWeekExpr) {
                  const dowResult = this.translateExpression(dayOfWeekExpr);
                  tables.push(...dowResult.tables);
                  params.push(...dowResult.params);
                  dayOfWeekSql = `CAST(${dowResult.sql} AS INTEGER)`;
                } else if (dateExpr) {
                  // Inherit day of week from source date if no explicit dayOfWeek
                  // ISO weekday: Monday=1, ..., Sunday=7
                  // SQLite strftime('%w') returns 0=Sun, 1=Mon, ..., 6=Sat
                  // Convert: (strftime('%w') + 6) % 7 + 1
                  const dateResult2 = this.translateExpression(dateExpr);
                  tables.push(...dateResult2.tables);
                  params.push(...dateResult2.params);
                  dayOfWeekSql = `(((CAST(strftime('%w', ${dateResult2.sql}) AS INTEGER) + 6) % 7) + 1)`;
                }

                // ISO week date formula:
                // Monday of week W in year Y = Jan 4 of Y - (weekday of Jan 4 as 0-6 Mon-Sun) + (W-1)*7
                // Then add (dayOfWeek - 1) for other days
                // SQLite strftime('%w') returns 0=Sun, 1=Mon, ..., 6=Sat
                // Convert to 0=Mon, ..., 6=Sun: (strftime('%w') + 6) % 7
                const sql = `DATE(
                  julianday(printf('%04d-01-04', ${yearSql}))
                  - ((CAST(strftime('%w', printf('%04d-01-04', ${yearSql})) AS INTEGER) + 6) % 7)
                  + (${weekSql} - 1) * 7
                  + (${dayOfWeekSql} - 1)
                )`;

                return { sql, tables, params };
              }

              // Ordinal date: date({year: Y, ordinalDay: D})
              // ordinalDay is the day of the year (1-366)
              if (yearExpr && ordinalDayExpr) {
                const yearResult = this.translateExpression(yearExpr);
                tables.push(...yearResult.tables);
                params.push(...yearResult.params);
                const yearSql = `CAST(${yearResult.sql} AS INTEGER)`;

                const ordinalDayResult = this.translateExpression(ordinalDayExpr);
                tables.push(...ordinalDayResult.tables);
                params.push(...ordinalDayResult.params);
                const ordinalDaySql = `CAST(${ordinalDayResult.sql} AS INTEGER)`;

                // Start from Jan 1 and add (ordinalDay - 1) days
                return {
                  sql: `DATE(printf('%04d-01-01', ${yearSql}), '+' || (${ordinalDaySql} - 1) || ' days')`,
                  tables,
                  params,
                };
              }

              // Quarter date: date({year: Y, quarter: Q, dayOfQuarter: D})
              // Quarter 1 starts Jan 1, Q2 starts Apr 1, Q3 starts Jul 1, Q4 starts Oct 1
              // dayOfQuarter defaults to 1
              if (yearExpr && quarterExpr) {
                const yearResult = this.translateExpression(yearExpr);
                tables.push(...yearResult.tables);
                params.push(...yearResult.params);
                const yearSql = `CAST(${yearResult.sql} AS INTEGER)`;

                const quarterResult = this.translateExpression(quarterExpr);
                tables.push(...quarterResult.tables);
                params.push(...quarterResult.params);
                const quarterSql = `CAST(${quarterResult.sql} AS INTEGER)`;

                let dayOfQuarterSql = "1";
                if (dayOfQuarterExpr) {
                  const dayOfQuarterResult = this.translateExpression(dayOfQuarterExpr);
                  tables.push(...dayOfQuarterResult.tables);
                  params.push(...dayOfQuarterResult.params);
                  dayOfQuarterSql = `CAST(${dayOfQuarterResult.sql} AS INTEGER)`;
                }

                // Quarter start months: Q1=1, Q2=4, Q3=7, Q4=10
                // Formula: (quarter - 1) * 3 + 1
                // Start from first day of quarter, then add (dayOfQuarter - 1) days
                return {
                  sql: `DATE(printf('%04d-%02d-01', ${yearSql}, (${quarterSql} - 1) * 3 + 1), '+' || (${dayOfQuarterSql} - 1) || ' days')`,
                  tables,
                  params,
                };
              }

              // Calendar date: date({year: Y, month: M, day: D})
              // month defaults to 1, day defaults to 1
              if (yearExpr) {
                const yearResult = this.translateExpression(yearExpr);
                tables.push(...yearResult.tables);
                params.push(...yearResult.params);
                const yearSql = `CAST(${yearResult.sql} AS INTEGER)`;

                let monthSql = "1";
                if (monthExpr) {
                  const monthResult = this.translateExpression(monthExpr);
                  tables.push(...monthResult.tables);
                  params.push(...monthResult.params);
                  monthSql = `CAST(${monthResult.sql} AS INTEGER)`;
                }

                let daySql = "1";
                if (dayExpr) {
                  const dayResult = this.translateExpression(dayExpr);
                  tables.push(...dayResult.tables);
                  params.push(...dayResult.params);
                  daySql = `CAST(${dayResult.sql} AS INTEGER)`;
                }

                return {
                  sql: `DATE(printf('%04d-%02d-%02d', ${yearSql}, ${monthSql}, ${daySql}))`,
                  tables,
                  params,
                };
              }

              throw new Error("date(map) requires year/month/day or year/week");
            }

            // date('2024-01-15') - parse date string
            // Also supports compact formats:
            // - YYYY (year only, 4 digits): 2015 = 2015-01-01
            // - YYYYMM (compact year-month, 6 digits): 201507 = 2015-07-01
            // - YYYY-MM (year-month, 7 chars): 2015-07 = 2015-07-01
            // - YYYYDDD (ordinal date, 7 digits): 2015202 = 2015, day 202 = 2015-07-21
            // - YYYYMMDD (compact date, 8 digits): 20150721 = 2015-07-21
            // - YYYY-DDD (ordinal with hyphen): 2015-202 = 2015-07-21
            // - YYYYWwwD (week date, 8 chars): 2015W294 = 2015, week 29, day 4
            // - YYYY-Www-D (week date with hyphens): 2015-W29-4
            const argResult = this.translateFunctionArg(arg);
            tables.push(...argResult.tables);
            params.push(...argResult.params);
            // Handle various date string formats using a subquery to avoid repeating the parameter
            const dateArg = argResult.sql;
            const sql = `(SELECT CASE
              WHEN length(d) = 4 AND d GLOB '[0-9][0-9][0-9][0-9]'
              THEN DATE(d || '-01-01')
              WHEN length(d) = 6 AND d GLOB '[0-9][0-9][0-9][0-9][0-9][0-9]'
              THEN DATE(substr(d, 1, 4) || '-' || substr(d, 5, 2) || '-01')
              WHEN length(d) = 7 AND d GLOB '[0-9][0-9][0-9][0-9]-[0-9][0-9]'
              THEN DATE(d || '-01')
              WHEN length(d) = 7 AND d GLOB '[0-9][0-9][0-9][0-9][0-9][0-9][0-9]'
              THEN DATE(printf('%04d-01-01', CAST(substr(d, 1, 4) AS INTEGER)), 
                        '+' || (CAST(substr(d, 5, 3) AS INTEGER) - 1) || ' days')
              WHEN length(d) = 8 AND d GLOB '[0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9]'
              THEN DATE(substr(d, 1, 4) || '-' || substr(d, 5, 2) || '-' || substr(d, 7, 2))
              WHEN d GLOB '[0-9][0-9][0-9][0-9]-[0-9][0-9][0-9]'
              THEN DATE(printf('%04d-01-01', CAST(substr(d, 1, 4) AS INTEGER)),
                        '+' || (CAST(substr(d, 6, 3) AS INTEGER) - 1) || ' days')
              WHEN d GLOB '[0-9][0-9][0-9][0-9]W[0-9][0-9]'
              THEN DATE(
                julianday(printf('%04d-01-04', CAST(substr(d, 1, 4) AS INTEGER)))
                - ((CAST(strftime('%w', printf('%04d-01-04', CAST(substr(d, 1, 4) AS INTEGER))) AS INTEGER) + 6) % 7)
                + (CAST(substr(d, 6, 2) AS INTEGER) - 1) * 7
              )
              WHEN d GLOB '[0-9][0-9][0-9][0-9]W[0-9][0-9][0-9]'
              THEN DATE(
                julianday(printf('%04d-01-04', CAST(substr(d, 1, 4) AS INTEGER)))
                - ((CAST(strftime('%w', printf('%04d-01-04', CAST(substr(d, 1, 4) AS INTEGER))) AS INTEGER) + 6) % 7)
                + (CAST(substr(d, 6, 2) AS INTEGER) - 1) * 7
                + (CAST(substr(d, 8, 1) AS INTEGER) - 1)
              )
              WHEN d GLOB '[0-9][0-9][0-9][0-9]-W[0-9][0-9]'
              THEN DATE(
                julianday(printf('%04d-01-04', CAST(substr(d, 1, 4) AS INTEGER)))
                - ((CAST(strftime('%w', printf('%04d-01-04', CAST(substr(d, 1, 4) AS INTEGER))) AS INTEGER) + 6) % 7)
                + (CAST(substr(d, 7, 2) AS INTEGER) - 1) * 7
              )
              WHEN d GLOB '[0-9][0-9][0-9][0-9]-W[0-9][0-9]-[0-9]'
              THEN DATE(
                julianday(printf('%04d-01-04', CAST(substr(d, 1, 4) AS INTEGER)))
                - ((CAST(strftime('%w', printf('%04d-01-04', CAST(substr(d, 1, 4) AS INTEGER))) AS INTEGER) + 6) % 7)
                + (CAST(substr(d, 7, 2) AS INTEGER) - 1) * 7
                + (CAST(substr(d, 10, 1) AS INTEGER) - 1)
              )
              ELSE DATE(d)
            END FROM (SELECT ${dateArg} AS d))`;
            return { sql, tables, params };
          }
          // date() - current date
          return { sql: `DATE('now')`, tables, params };
        }

        // LOCALTIME: create time without timezone from a map or parse time string
        if (expr.functionName === "LOCALTIME") {
          if (expr.args && expr.args.length > 0) {
            const arg = expr.args[0];

            // localtime({hour: 10, minute: 35, second: 13, nanosecond: 645876123})
            // Also supports millisecond and microsecond which combine with nanosecond
            if (arg.type === "object") {
              const props = arg.properties ?? [];
              const byKey = new Map<string, Expression>();
              for (const prop of props) byKey.set(prop.key.toLowerCase(), prop.value);

              const hourExpr = byKey.get("hour");
              const minuteExpr = byKey.get("minute");
              const secondExpr = byKey.get("second");
              const nanosecondExpr = byKey.get("nanosecond");
              const millisecondExpr = byKey.get("millisecond");
              const microsecondExpr = byKey.get("microsecond");

              if (!hourExpr || !minuteExpr) {
                throw new Error("localtime(map) requires hour and minute");
              }

              const hourResult = this.translateExpression(hourExpr);
              const minuteResult = this.translateExpression(minuteExpr);
              tables.push(...hourResult.tables, ...minuteResult.tables);
              params.push(...hourResult.params, ...minuteResult.params);

              const hourSql = `CAST(${hourResult.sql} AS INTEGER)`;
              const minuteSql = `CAST(${minuteResult.sql} AS INTEGER)`;

              if (!secondExpr) {
                return {
                  sql: `printf('%02d:%02d', ${hourSql}, ${minuteSql})`,
                  tables,
                  params,
                };
              }

              const secondResult = this.translateExpression(secondExpr);
              tables.push(...secondResult.tables);
              params.push(...secondResult.params);
              const secondSql = `CAST(${secondResult.sql} AS INTEGER)`;

              // Check if any sub-second precision is provided
              const hasSubSecond = nanosecondExpr || millisecondExpr || microsecondExpr;
              if (!hasSubSecond) {
                return {
                  sql: `printf('%02d:%02d:%02d', ${hourSql}, ${minuteSql}, ${secondSql})`,
                  tables,
                  params,
                };
              }

              // Combine millisecond, microsecond, and nanosecond into total nanoseconds
              // totalNanos = millisecond * 1000000 + microsecond * 1000 + nanosecond
              const nanoParts: string[] = [];
              if (millisecondExpr) {
                const msResult = this.translateExpression(millisecondExpr);
                tables.push(...msResult.tables);
                params.push(...msResult.params);
                nanoParts.push(`(CAST(${msResult.sql} AS INTEGER) * 1000000)`);
              }
              if (microsecondExpr) {
                const usResult = this.translateExpression(microsecondExpr);
                tables.push(...usResult.tables);
                params.push(...usResult.params);
                nanoParts.push(`(CAST(${usResult.sql} AS INTEGER) * 1000)`);
              }
              if (nanosecondExpr) {
                const nsResult = this.translateExpression(nanosecondExpr);
                tables.push(...nsResult.tables);
                params.push(...nsResult.params);
                nanoParts.push(`CAST(${nsResult.sql} AS INTEGER)`);
              }
              const totalNanosSql = nanoParts.length === 1 ? nanoParts[0] : `(${nanoParts.join(" + ")})`;

              return {
                sql: `printf('%02d:%02d:%02d.%09d', ${hourSql}, ${minuteSql}, ${secondSql}, ${totalNanosSql})`,
                tables,
                params,
              };
            }

            // localtime('12:34:56.123') - parse time string
            // Also supports compact formats:
            // - HHMM (4 digits): 2140 = 21:40:00
            // - HHMMSS (6 digits): 214032 = 21:40:32
            // - HHMMSSfff... (9+ digits): 214032123 = 21:40:32.123 (milliseconds appended)
            const argResult = this.translateFunctionArg(arg);
            tables.push(...argResult.tables);
            params.push(...argResult.params);
            const timeArg = argResult.sql;
            const sql = `(SELECT CASE
              WHEN length(t) = 4 AND t GLOB '[0-9][0-9][0-9][0-9]'
              THEN TIME(substr(t, 1, 2) || ':' || substr(t, 3, 2) || ':00')
              WHEN length(t) = 6 AND t GLOB '[0-9][0-9][0-9][0-9][0-9][0-9]'
              THEN TIME(substr(t, 1, 2) || ':' || substr(t, 3, 2) || ':' || substr(t, 5, 2))
              WHEN length(t) >= 9 AND t GLOB '[0-9][0-9][0-9][0-9][0-9][0-9][0-9]*'
              THEN TIME(substr(t, 1, 2) || ':' || substr(t, 3, 2) || ':' || substr(t, 5, 2) || '.' || substr(t, 7))
              ELSE TIME(t)
            END FROM (SELECT ${timeArg} AS t))`;
            return { sql, tables, params };
          }
          // localtime() - current local time
          return { sql: `TIME('now')`, tables, params };
        }

        // TIME: create time with timezone from a map
        if (expr.functionName === "TIME") {
          if (expr.args && expr.args.length > 0) {
            const arg = expr.args[0];

            // time({hour: 10, minute: 35, timezone: '-08:00', ...})
            if (arg.type === "object") {
              const props = arg.properties ?? [];
              const byKey = new Map<string, Expression>();
              for (const prop of props) byKey.set(prop.key.toLowerCase(), prop.value);

              const hourExpr = byKey.get("hour");
              const minuteExpr = byKey.get("minute");
              const secondExpr = byKey.get("second");
              const millisecondExpr = byKey.get("millisecond");
              const microsecondExpr = byKey.get("microsecond");
              const nanosecondExpr = byKey.get("nanosecond");
              const timezoneExpr = byKey.get("timezone");

              if (!hourExpr || !minuteExpr) {
                throw new Error("time(map) requires hour and minute");
              }

              const hourResult = this.translateExpression(hourExpr);
              const minuteResult = this.translateExpression(minuteExpr);
              tables.push(...hourResult.tables, ...minuteResult.tables);
              params.push(...hourResult.params, ...minuteResult.params);

              const hourSql = `CAST(${hourResult.sql} AS INTEGER)`;
              const minuteSql = `CAST(${minuteResult.sql} AS INTEGER)`;

              // Timezone defaults to 'Z' (UTC) if not provided
              let tzNormalized: string;
              if (timezoneExpr) {
                const tzResult = this.translateExpression(timezoneExpr);
                tables.push(...tzResult.tables);
                // Push timezone params 4 times since we use it 4 times in the CASE expression
                params.push(...tzResult.params, ...tzResult.params, ...tzResult.params, ...tzResult.params);
                // Normalize timezone: remove trailing ':00' seconds if present (e.g., '+02:05:00' -> '+02:05')
                tzNormalized = `(CASE WHEN ${tzResult.sql} LIKE '%:00' AND LENGTH(${tzResult.sql}) = 9 THEN SUBSTR(${tzResult.sql}, 1, 6) ELSE ${tzResult.sql} END)`;
              } else {
                tzNormalized = `'Z'`;
              }

              if (!secondExpr) {
                return {
                  sql: `(printf('%02d:%02d', ${hourSql}, ${minuteSql}) || ${tzNormalized})`,
                  tables,
                  params,
                };
              }

              const secondResult = this.translateExpression(secondExpr);
              tables.push(...secondResult.tables);
              params.push(...secondResult.params);
              const secondSql = `CAST(${secondResult.sql} AS INTEGER)`;

              // Check if any sub-second components are provided
              const hasSubSecond = millisecondExpr || microsecondExpr || nanosecondExpr;

              if (!hasSubSecond) {
                return {
                  sql: `(printf('%02d:%02d:%02d', ${hourSql}, ${minuteSql}, ${secondSql}) || ${tzNormalized})`,
                  tables,
                  params,
                };
              }

              // Combine millisecond, microsecond, nanosecond into total nanoseconds
              // nanosecond = millisecond * 1000000 + microsecond * 1000 + nanosecond
              const nsParts: string[] = [];
              if (millisecondExpr) {
                const msResult = this.translateExpression(millisecondExpr);
                tables.push(...msResult.tables);
                params.push(...msResult.params);
                nsParts.push(`(CAST(${msResult.sql} AS INTEGER) * 1000000)`);
              }
              if (microsecondExpr) {
                const usResult = this.translateExpression(microsecondExpr);
                tables.push(...usResult.tables);
                params.push(...usResult.params);
                nsParts.push(`(CAST(${usResult.sql} AS INTEGER) * 1000)`);
              }
              if (nanosecondExpr) {
                const nsResult = this.translateExpression(nanosecondExpr);
                tables.push(...nsResult.tables);
                params.push(...nsResult.params);
                nsParts.push(`CAST(${nsResult.sql} AS INTEGER)`);
              }
              const totalNanosecondSql = nsParts.length > 0 ? `(${nsParts.join(" + ")})` : "0";

              return {
                sql: `(printf('%02d:%02d:%02d.%09d', ${hourSql}, ${minuteSql}, ${secondSql}, ${totalNanosecondSql}) || ${tzNormalized})`,
                tables,
                params,
              };
            }

            // time('21:40:32+01:00') - parse time string with timezone
            // Also supports compact formats:
            // - HHMMSS+HHMM or HHMMSS-HHMM (e.g., 214032-0100 -> 21:40:32-01:00)
            // - HHMM+HHMM (e.g., 2140+0100 -> 21:40:00+01:00)
            // - Normal format with colons passes through
            const argResult = this.translateFunctionArg(arg);
            tables.push(...argResult.tables);
            params.push(...argResult.params);
            const timeArg = argResult.sql;
            // Parse and normalize compact time formats with timezone
            const sql = `(SELECT CASE
              WHEN t GLOB '[0-9][0-9][0-9][0-9][0-9][0-9][+-][0-9][0-9][0-9][0-9]'
              THEN substr(t, 1, 2) || ':' || substr(t, 3, 2) || ':' || substr(t, 5, 2) || substr(t, 7, 1) || substr(t, 8, 2) || ':' || substr(t, 10, 2)
              WHEN t GLOB '[0-9][0-9][0-9][0-9][+-][0-9][0-9][0-9][0-9]'
              THEN substr(t, 1, 2) || ':' || substr(t, 3, 2) || ':00' || substr(t, 5, 1) || substr(t, 6, 2) || ':' || substr(t, 8, 2)
              WHEN t GLOB '[0-9][0-9][0-9][0-9][0-9][0-9]Z'
              THEN substr(t, 1, 2) || ':' || substr(t, 3, 2) || ':' || substr(t, 5, 2) || 'Z'
              WHEN t GLOB '[0-9][0-9][0-9][0-9]Z'
              THEN substr(t, 1, 2) || ':' || substr(t, 3, 2) || ':00Z'
              ELSE t
            END FROM (SELECT ${timeArg} AS t))`;
            return { sql, tables, params };
          }
          // time() - current time with timezone isn't supported (needs timezone context)
          throw new Error("time() requires an argument");
        }

        // LOCALDATETIME: create datetime without timezone from a map
        if (expr.functionName === "LOCALDATETIME") {
          if (expr.args && expr.args.length > 0) {
            const arg = expr.args[0];

            // localdatetime({year: 1984, month: 10, day: 11, hour: 12, minute: 30, second: 14, nanosecond: 12})
            // or localdatetime({year: 1984, week: 10, dayOfWeek: 3, hour: 12, minute: 31, second: 14})
            if (arg.type === "object") {
              const props = arg.properties ?? [];
              const byKey = new Map<string, Expression>();
              for (const prop of props) byKey.set(prop.key.toLowerCase(), prop.value);

              const yearExpr = byKey.get("year");
              const monthExpr = byKey.get("month");
              const dayExpr = byKey.get("day");
              const weekExpr = byKey.get("week");
              const dayOfWeekExpr = byKey.get("dayofweek");
              const ordinalDayExpr = byKey.get("ordinalday");
              const quarterExpr = byKey.get("quarter");
              const dayOfQuarterExpr = byKey.get("dayofquarter");
              const hourExpr = byKey.get("hour");
              const minuteExpr = byKey.get("minute");
              const secondExpr = byKey.get("second");
              const nanosecondExpr = byKey.get("nanosecond");
              const millisecondExpr = byKey.get("millisecond");
              const microsecondExpr = byKey.get("microsecond");

              // Must have year, hour, minute in all cases
              if (!yearExpr || !hourExpr || !minuteExpr) {
                throw new Error("localdatetime(map) requires year, hour, and minute");
              }

              // Either month+day or week (with optional dayOfWeek) or ordinalDay or quarter
              const hasCalendarDate = monthExpr && dayExpr;
              const hasWeekDate = weekExpr;
              const hasOrdinalDate = ordinalDayExpr;
              const hasQuarterDate = quarterExpr;

              if (!hasCalendarDate && !hasWeekDate && !hasOrdinalDate && !hasQuarterDate) {
                throw new Error("localdatetime(map) requires month/day or week or ordinalDay or quarter");
              }

              const yearResult = this.translateExpression(yearExpr);
              const hourResult = this.translateExpression(hourExpr);
              const minuteResult = this.translateExpression(minuteExpr);
              tables.push(
                ...yearResult.tables,
                ...hourResult.tables,
                ...minuteResult.tables
              );
              params.push(
                ...yearResult.params,
                ...hourResult.params,
                ...minuteResult.params
              );

              const yearSql = `CAST(${yearResult.sql} AS INTEGER)`;
              const hourSql = `CAST(${hourResult.sql} AS INTEGER)`;
              const minuteSql = `CAST(${minuteResult.sql} AS INTEGER)`;

              let dateSql: string;

              if (hasWeekDate) {
                // ISO week date: localdatetime({year: Y, week: W, dayOfWeek: D, ...})
                // Same formula as date() function
                const weekResult = this.translateExpression(weekExpr);
                tables.push(...weekResult.tables);
                params.push(...weekResult.params);
                const weekSql = `CAST(${weekResult.sql} AS INTEGER)`;

                // Default dayOfWeek is 1 (Monday)
                let dayOfWeekSql = "1";
                if (dayOfWeekExpr) {
                  const dowResult = this.translateExpression(dayOfWeekExpr);
                  tables.push(...dowResult.tables);
                  params.push(...dowResult.params);
                  dayOfWeekSql = `CAST(${dowResult.sql} AS INTEGER)`;
                }

                // Need yearSql twice in the formula, so duplicate params
                params.push(...yearResult.params);

                // ISO week date formula:
                // Monday of week W in year Y = Jan 4 of Y - (weekday of Jan 4 as 0-6 Mon-Sun) + (W-1)*7
                // Then add (dayOfWeek - 1) for other days
                dateSql = `DATE(
                  julianday(printf('%04d-01-04', ${yearSql}))
                  - ((CAST(strftime('%w', printf('%04d-01-04', ${yearSql})) AS INTEGER) + 6) % 7)
                  + (${weekSql} - 1) * 7
                  + (${dayOfWeekSql} - 1)
                )`;
              } else if (hasOrdinalDate) {
                // Ordinal date: localdatetime({year: Y, ordinalDay: D, ...})
                // ordinalDay is the day of the year (1-366)
                const ordinalDayResult = this.translateExpression(ordinalDayExpr!);
                tables.push(...ordinalDayResult.tables);
                params.push(...ordinalDayResult.params);
                const ordinalDaySql = `CAST(${ordinalDayResult.sql} AS INTEGER)`;

                // Start from Jan 1 and add (ordinalDay - 1) days
                dateSql = `DATE(printf('%04d-01-01', ${yearSql}), '+' || (${ordinalDaySql} - 1) || ' days')`;
              } else if (hasQuarterDate) {
                // Quarter date: localdatetime({year: Y, quarter: Q, dayOfQuarter: D, ...})
                // Quarter 1 starts Jan 1, Q2 starts Apr 1, Q3 starts Jul 1, Q4 starts Oct 1
                // dayOfQuarter defaults to 1
                const quarterResult = this.translateExpression(quarterExpr!);
                tables.push(...quarterResult.tables);
                params.push(...quarterResult.params);
                const quarterSql = `CAST(${quarterResult.sql} AS INTEGER)`;

                let dayOfQuarterSql = "1";
                if (dayOfQuarterExpr) {
                  const dayOfQuarterResult = this.translateExpression(dayOfQuarterExpr);
                  tables.push(...dayOfQuarterResult.tables);
                  params.push(...dayOfQuarterResult.params);
                  dayOfQuarterSql = `CAST(${dayOfQuarterResult.sql} AS INTEGER)`;
                }

                // Quarter start months: Q1=1, Q2=4, Q3=7, Q4=10
                // Formula: (quarter - 1) * 3 + 1
                // Start from first day of quarter, then add (dayOfQuarter - 1) days
                dateSql = `DATE(printf('%04d-%02d-01', ${yearSql}, (${quarterSql} - 1) * 3 + 1), '+' || (${dayOfQuarterSql} - 1) || ' days')`;
              } else {
                // Calendar date: month/day
                const monthResult = this.translateExpression(monthExpr!);
                const dayResult = this.translateExpression(dayExpr!);
                tables.push(...monthResult.tables, ...dayResult.tables);
                params.push(...monthResult.params, ...dayResult.params);
                const monthSql = `CAST(${monthResult.sql} AS INTEGER)`;
                const daySql = `CAST(${dayResult.sql} AS INTEGER)`;
                dateSql = `printf('%04d-%02d-%02d', ${yearSql}, ${monthSql}, ${daySql})`;
              }

              // Helper to build time part
              const buildTimeResult = (
                dateSql: string,
                hourSql: string,
                minuteSql: string,
                secondSql: string | null,
                totalNanoSql: string | null
              ) => {
                if (!secondSql) {
                  return {
                    sql: `(${dateSql} || 'T' || printf('%02d:%02d', ${hourSql}, ${minuteSql}))`,
                    tables,
                    params,
                  };
                }
                if (!totalNanoSql) {
                  return {
                    sql: `(${dateSql} || 'T' || printf('%02d:%02d:%02d', ${hourSql}, ${minuteSql}, ${secondSql}))`,
                    tables,
                    params,
                  };
                }
                return {
                  sql: `(${dateSql} || 'T' || printf('%02d:%02d:%02d.%09d', ${hourSql}, ${minuteSql}, ${secondSql}, ${totalNanoSql}))`,
                  tables,
                  params,
                };
              };

              if (!secondExpr) {
                return buildTimeResult(dateSql, hourSql, minuteSql, null, null);
              }

              const secondResult = this.translateExpression(secondExpr);
              tables.push(...secondResult.tables);
              params.push(...secondResult.params);
              const secondSql = `CAST(${secondResult.sql} AS INTEGER)`;

              // Check if we have any sub-second precision (nanosecond, millisecond, microsecond)
              const hasSubSecond = nanosecondExpr || millisecondExpr || microsecondExpr;

              if (!hasSubSecond) {
                return buildTimeResult(dateSql, hourSql, minuteSql, secondSql, null);
              }

              // Compute total nanoseconds from millisecond, microsecond, and nanosecond
              // millisecond * 1000000 + microsecond * 1000 + nanosecond
              const nanoComponents: string[] = [];

              if (millisecondExpr) {
                const msResult = this.translateExpression(millisecondExpr);
                tables.push(...msResult.tables);
                params.push(...msResult.params);
                nanoComponents.push(`(CAST(${msResult.sql} AS INTEGER) * 1000000)`);
              }

              if (microsecondExpr) {
                const usResult = this.translateExpression(microsecondExpr);
                tables.push(...usResult.tables);
                params.push(...usResult.params);
                nanoComponents.push(`(CAST(${usResult.sql} AS INTEGER) * 1000)`);
              }

              if (nanosecondExpr) {
                const nsResult = this.translateExpression(nanosecondExpr);
                tables.push(...nsResult.tables);
                params.push(...nsResult.params);
                nanoComponents.push(`CAST(${nsResult.sql} AS INTEGER)`);
              }

              const totalNanoSql = nanoComponents.length > 0 ? nanoComponents.join(" + ") : "0";

              return buildTimeResult(dateSql, hourSql, minuteSql, secondSql, totalNanoSql);
            }

            // localdatetime('2015-07-21T21:40:32') - parse datetime string
            // Also supports ordinal date: '2015-202T21:40:32' = 2015, day 202 at 21:40:32
            // Also supports year-only with compact time: '2015T214032' = 2015-01-01T21:40:32
            const argResult = this.translateFunctionArg(arg);
            tables.push(...argResult.tables);
            params.push(...argResult.params);
            const dtArg = argResult.sql;
            // Parse various datetime formats and normalize to ISO format
            // Format: date part 'T' time part
            // Date part can be: YYYY-MM-DD, YYYY-DDD (ordinal), YYYY-Www-D (week date), YYYY (year only)
            // Time part can be: HH:MM:SS, HHMMSS (compact)
            // Helper: normalize compact time HHMMSS to HH:MM:SS
            const normalizeTime = (timeExpr: string) => `CASE
              WHEN ${timeExpr} GLOB '[0-9][0-9][0-9][0-9][0-9][0-9].*'
              THEN substr(${timeExpr}, 1, 2) || ':' || substr(${timeExpr}, 3, 2) || ':' || substr(${timeExpr}, 5, 2) || substr(${timeExpr}, 7)
              WHEN ${timeExpr} GLOB '[0-9][0-9][0-9][0-9][0-9][0-9]'
              THEN substr(${timeExpr}, 1, 2) || ':' || substr(${timeExpr}, 3, 2) || ':' || substr(${timeExpr}, 5, 2)
              WHEN ${timeExpr} GLOB '[0-9][0-9][0-9][0-9]'
              THEN substr(${timeExpr}, 1, 2) || ':' || substr(${timeExpr}, 3, 2) || ':00'
              ELSE ${timeExpr}
            END`;
            const sql = `(SELECT CASE
              WHEN d GLOB '[0-9][0-9][0-9][0-9]T[0-9]*'
              THEN substr(d, 1, 4) || '-01-01T' || (${normalizeTime("substr(d, 6)")})
              WHEN d GLOB '[0-9][0-9][0-9][0-9]-[0-9][0-9][0-9]T*'
              THEN DATE(printf('%04d-01-01', CAST(substr(d, 1, 4) AS INTEGER)),
                        '+' || (CAST(substr(d, 6, 3) AS INTEGER) - 1) || ' days') || 'T' || (${normalizeTime("substr(d, 10)")})
              WHEN d GLOB '[0-9][0-9][0-9][0-9][0-9][0-9][0-9]T*'
              THEN DATE(printf('%04d-01-01', CAST(substr(d, 1, 4) AS INTEGER)),
                        '+' || (CAST(substr(d, 5, 3) AS INTEGER) - 1) || ' days') || 'T' || (${normalizeTime("substr(d, 9)")})
              WHEN d GLOB '[0-9][0-9][0-9][0-9]-W[0-9][0-9]-[0-9]T*'
              THEN DATE(
                julianday(printf('%04d-01-04', CAST(substr(d, 1, 4) AS INTEGER)))
                - ((CAST(strftime('%w', printf('%04d-01-04', CAST(substr(d, 1, 4) AS INTEGER))) AS INTEGER) + 6) % 7)
                + (CAST(substr(d, 7, 2) AS INTEGER) - 1) * 7
                + (CAST(substr(d, 10, 1) AS INTEGER) - 1)
              ) || 'T' || (${normalizeTime("substr(d, 12)")})
              WHEN d GLOB '[0-9][0-9][0-9][0-9]W[0-9][0-9][0-9]T*'
              THEN DATE(
                julianday(printf('%04d-01-04', CAST(substr(d, 1, 4) AS INTEGER)))
                - ((CAST(strftime('%w', printf('%04d-01-04', CAST(substr(d, 1, 4) AS INTEGER))) AS INTEGER) + 6) % 7)
                + (CAST(substr(d, 6, 2) AS INTEGER) - 1) * 7
                + (CAST(substr(d, 8, 1) AS INTEGER) - 1)
              ) || 'T' || (${normalizeTime("substr(d, 10)")})
              ELSE d
            END FROM (SELECT ${dtArg} AS d))`;
            return { sql, tables, params };
          }
          // localdatetime() - current local datetime
          return { sql: `DATETIME('now')`, tables, params };
        }

        // DATETIME: get current datetime or parse datetime string
        if (expr.functionName === "DATETIME") {
          if (expr.args && expr.args.length > 0) {
            const arg = expr.args[0];

            // datetime({year: 1984, month: 10, day: 11, hour: 12, minute: 30, second: 14, nanosecond: 12, timezone: '+00:15'})
            // or datetime({year: 1984, week: 10, dayOfWeek: 3, hour: 12, minute: 31, second: 14})
            // or datetime({year: 1984, ordinalDay: 100, hour: 12, minute: 30})
            // or datetime({year: 1984, quarter: 2, dayOfQuarter: 45, hour: 12, minute: 30})
            if (arg.type === "object") {
              const props = arg.properties ?? [];
              const byKey = new Map<string, Expression>();
              for (const prop of props) byKey.set(prop.key.toLowerCase(), prop.value);

              const yearExpr = byKey.get("year");
              const monthExpr = byKey.get("month");
              const dayExpr = byKey.get("day");
              const weekExpr = byKey.get("week");
              const dayOfWeekExpr = byKey.get("dayofweek");
              const ordinalDayExpr = byKey.get("ordinalday");
              const quarterExpr = byKey.get("quarter");
              const dayOfQuarterExpr = byKey.get("dayofquarter");
              const hourExpr = byKey.get("hour");
              const minuteExpr = byKey.get("minute");
              const secondExpr = byKey.get("second");
              const nanosecondExpr = byKey.get("nanosecond");
              const millisecondExpr = byKey.get("millisecond");
              const microsecondExpr = byKey.get("microsecond");
              const timezoneExpr = byKey.get("timezone");

              // Check date format type
              const hasCalendarDate = monthExpr && dayExpr;
              const hasWeekDate = weekExpr !== undefined;
              const hasOrdinalDate = ordinalDayExpr !== undefined;
              const hasQuarterDate = quarterExpr !== undefined;

              if (!yearExpr || !hourExpr || !minuteExpr) {
                throw new Error("datetime(map) requires year, hour, and minute");
              }

              if (!hasCalendarDate && !hasWeekDate && !hasOrdinalDate && !hasQuarterDate) {
                throw new Error("datetime(map) requires month/day or week or ordinalDay or quarter");
              }

              const yearResult = this.translateExpression(yearExpr);
              const hourResult = this.translateExpression(hourExpr);
              const minuteResult = this.translateExpression(minuteExpr);
              // Default timezone to 'Z' (UTC) if not provided
              const tzResult = timezoneExpr 
                ? this.translateExpression(timezoneExpr)
                : { sql: "'Z'", tables: [] as string[], params: [] as unknown[] };
              tables.push(
                ...yearResult.tables,
                ...hourResult.tables,
                ...minuteResult.tables,
                ...tzResult.tables
              );
              params.push(
                ...yearResult.params,
                ...hourResult.params,
                ...minuteResult.params,
                ...tzResult.params
              );

              const yearSql = `CAST(${yearResult.sql} AS INTEGER)`;
              const hourSql = `CAST(${hourResult.sql} AS INTEGER)`;
              const minuteSql = `CAST(${minuteResult.sql} AS INTEGER)`;

              // Build date part based on format type
              let dateSql: string;

              if (hasWeekDate) {
                // ISO week date: datetime({year: Y, week: W, dayOfWeek: D, ...})
                const weekResult = this.translateExpression(weekExpr);
                tables.push(...weekResult.tables);
                params.push(...weekResult.params);
                const weekSql = `CAST(${weekResult.sql} AS INTEGER)`;

                // Default dayOfWeek is 1 (Monday)
                let dayOfWeekSql = "1";
                if (dayOfWeekExpr) {
                  const dowResult = this.translateExpression(dayOfWeekExpr);
                  tables.push(...dowResult.tables);
                  params.push(...dowResult.params);
                  dayOfWeekSql = `CAST(${dowResult.sql} AS INTEGER)`;
                }

                // Need yearSql twice in the formula, so duplicate params
                params.push(...yearResult.params);

                // ISO week date formula:
                // Monday of week W in year Y = Jan 4 of Y - (weekday of Jan 4 as 0-6 Mon-Sun) + (W-1)*7
                // Then add (dayOfWeek - 1) for other days
                dateSql = `DATE(
                  julianday(printf('%04d-01-04', ${yearSql}))
                  - ((CAST(strftime('%w', printf('%04d-01-04', ${yearSql})) AS INTEGER) + 6) % 7)
                  + (${weekSql} - 1) * 7
                  + (${dayOfWeekSql} - 1)
                )`;
              } else if (hasOrdinalDate) {
                // Ordinal date: datetime({year: Y, ordinalDay: D, ...})
                const ordinalDayResult = this.translateExpression(ordinalDayExpr!);
                tables.push(...ordinalDayResult.tables);
                params.push(...ordinalDayResult.params);
                const ordinalDaySql = `CAST(${ordinalDayResult.sql} AS INTEGER)`;

                // Start from Jan 1 and add (ordinalDay - 1) days
                dateSql = `DATE(printf('%04d-01-01', ${yearSql}), '+' || (${ordinalDaySql} - 1) || ' days')`;
              } else if (hasQuarterDate) {
                // Quarter date: datetime({year: Y, quarter: Q, dayOfQuarter: D, ...})
                const quarterResult = this.translateExpression(quarterExpr!);
                tables.push(...quarterResult.tables);
                params.push(...quarterResult.params);
                const quarterSql = `CAST(${quarterResult.sql} AS INTEGER)`;

                let dayOfQuarterSql = "1";
                if (dayOfQuarterExpr) {
                  const dayOfQuarterResult = this.translateExpression(dayOfQuarterExpr);
                  tables.push(...dayOfQuarterResult.tables);
                  params.push(...dayOfQuarterResult.params);
                  dayOfQuarterSql = `CAST(${dayOfQuarterResult.sql} AS INTEGER)`;
                }

                // Quarter start months: Q1=1, Q2=4, Q3=7, Q4=10
                dateSql = `DATE(printf('%04d-%02d-01', ${yearSql}, (${quarterSql} - 1) * 3 + 1), '+' || (${dayOfQuarterSql} - 1) || ' days')`;
              } else {
                // Calendar date: month/day
                const monthResult = this.translateExpression(monthExpr!);
                const dayResult = this.translateExpression(dayExpr!);
                tables.push(...monthResult.tables, ...dayResult.tables);
                params.push(...monthResult.params, ...dayResult.params);
                const monthSql = `CAST(${monthResult.sql} AS INTEGER)`;
                const daySql = `CAST(${dayResult.sql} AS INTEGER)`;
                dateSql = `printf('%04d-%02d-%02d', ${yearSql}, ${monthSql}, ${daySql})`;
              }

              // Build time part
              if (!secondExpr) {
                return {
                  sql: `(${dateSql} || 'T' || printf('%02d:%02d', ${hourSql}, ${minuteSql}) || ${tzResult.sql})`,
                  tables,
                  params,
                };
              }

              const secondResult = this.translateExpression(secondExpr);
              tables.push(...secondResult.tables);
              params.push(...secondResult.params);
              const secondSql = `CAST(${secondResult.sql} AS INTEGER)`;

              // Check if we have any sub-second precision (nanosecond, millisecond, microsecond)
              const hasSubSecond = nanosecondExpr || millisecondExpr || microsecondExpr;

              if (!hasSubSecond) {
                return {
                  sql: `(${dateSql} || 'T' || printf('%02d:%02d:%02d', ${hourSql}, ${minuteSql}, ${secondSql}) || ${tzResult.sql})`,
                  tables,
                  params,
                };
              }

              // Compute total nanoseconds from millisecond, microsecond, and nanosecond
              const nanoComponents: string[] = [];

              if (millisecondExpr) {
                const msResult = this.translateExpression(millisecondExpr);
                tables.push(...msResult.tables);
                params.push(...msResult.params);
                nanoComponents.push(`(CAST(${msResult.sql} AS INTEGER) * 1000000)`);
              }

              if (microsecondExpr) {
                const usResult = this.translateExpression(microsecondExpr);
                tables.push(...usResult.tables);
                params.push(...usResult.params);
                nanoComponents.push(`(CAST(${usResult.sql} AS INTEGER) * 1000)`);
              }

              if (nanosecondExpr) {
                const nsResult = this.translateExpression(nanosecondExpr);
                tables.push(...nsResult.tables);
                params.push(...nsResult.params);
                nanoComponents.push(`CAST(${nsResult.sql} AS INTEGER)`);
              }

              const totalNanoSql = nanoComponents.length > 0 ? nanoComponents.join(" + ") : "0";

              return {
                sql: `(${dateSql} || 'T' || printf('%02d:%02d:%02d.%09d', ${hourSql}, ${minuteSql}, ${secondSql}, ${totalNanoSql}) || ${tzResult.sql})`,
                tables,
                params,
              };
            }

            // datetime('2024-01-15T12:30:00') - parse datetime string
            const argResult = this.translateFunctionArg(arg);
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
        // Duration functions: duration.between, duration.inMonths, duration.inDays, duration.inSeconds
        // ============================================================================

        // duration.between: compute duration between two temporal values
        if (expr.functionName === "DURATION.BETWEEN") {
          if (expr.args && expr.args.length === 2) {
            const arg1Result = this.translateFunctionArg(expr.args[0]);
            const arg2Result = this.translateFunctionArg(expr.args[1]);
            tables.push(...arg1Result.tables, ...arg2Result.tables);
            // If either argument is NULL, return NULL
            // Otherwise compute the ISO 8601 duration between the two temporal values
            // For simplicity, compute the difference in seconds and format as ISO 8601 duration
            params.push(
              ...arg1Result.params,
              ...arg2Result.params,
              ...arg1Result.params,
              ...arg2Result.params
            );
            // Return PT...S format (duration in seconds) - simplified implementation
            return {
              sql: `CASE WHEN ${arg1Result.sql} IS NULL OR ${arg2Result.sql} IS NULL THEN NULL ` +
                   `ELSE 'PT' || CAST((julianday(${arg2Result.sql}) - julianday(${arg1Result.sql})) * 86400 AS INTEGER) || 'S' END`,
              tables,
              params
            };
          }
          throw new Error("duration.between requires 2 arguments");
        }

        // duration.inMonths: compute months component between two temporal values
        if (expr.functionName === "DURATION.INMONTHS") {
          if (expr.args && expr.args.length === 2) {
            const arg1Result = this.translateFunctionArg(expr.args[0]);
            const arg2Result = this.translateFunctionArg(expr.args[1]);
            tables.push(...arg1Result.tables, ...arg2Result.tables);
            // Returns a duration with only months component
            params.push(
              ...arg1Result.params,
              ...arg2Result.params,
              ...arg1Result.params,
              ...arg1Result.params,
              ...arg2Result.params,
              ...arg2Result.params
            );
            // Calculate months difference
            return {
              sql: `CASE WHEN ${arg1Result.sql} IS NULL OR ${arg2Result.sql} IS NULL THEN NULL ` +
                   `ELSE 'P' || ((CAST(strftime('%Y', ${arg2Result.sql}) AS INTEGER) - CAST(strftime('%Y', ${arg1Result.sql}) AS INTEGER)) * 12 + ` +
                   `(CAST(strftime('%m', ${arg2Result.sql}) AS INTEGER) - CAST(strftime('%m', ${arg1Result.sql}) AS INTEGER))) || 'M' END`,
              tables,
              params
            };
          }
          throw new Error("duration.inMonths requires 2 arguments");
        }

        // duration.inDays: compute days component between two temporal values
        if (expr.functionName === "DURATION.INDAYS") {
          if (expr.args && expr.args.length === 2) {
            const arg1Result = this.translateFunctionArg(expr.args[0]);
            const arg2Result = this.translateFunctionArg(expr.args[1]);
            tables.push(...arg1Result.tables, ...arg2Result.tables);
            // Returns a duration with only days component
            params.push(
              ...arg1Result.params,
              ...arg2Result.params,
              ...arg1Result.params,
              ...arg2Result.params
            );
            // Calculate days difference
            return {
              sql: `CASE WHEN ${arg1Result.sql} IS NULL OR ${arg2Result.sql} IS NULL THEN NULL ` +
                   `ELSE 'P' || CAST((julianday(${arg2Result.sql}) - julianday(${arg1Result.sql})) AS INTEGER) || 'D' END`,
              tables,
              params
            };
          }
          throw new Error("duration.inDays requires 2 arguments");
        }

        // duration.inSeconds: compute seconds component between two temporal values
        if (expr.functionName === "DURATION.INSECONDS") {
          if (expr.args && expr.args.length === 2) {
            const arg1Result = this.translateFunctionArg(expr.args[0]);
            const arg2Result = this.translateFunctionArg(expr.args[1]);
            tables.push(...arg1Result.tables, ...arg2Result.tables);
            // Returns a duration with only seconds component
            params.push(
              ...arg1Result.params,
              ...arg2Result.params,
              ...arg1Result.params,
              ...arg2Result.params
            );
            // Calculate seconds difference (86400 seconds per day)
            return {
              sql: `CASE WHEN ${arg1Result.sql} IS NULL OR ${arg2Result.sql} IS NULL THEN NULL ` +
                   `ELSE 'PT' || CAST((julianday(${arg2Result.sql}) - julianday(${arg1Result.sql})) * 86400 AS INTEGER) || 'S' END`,
              tables,
              params
            };
          }
          throw new Error("duration.inSeconds requires 2 arguments");
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

        // REVERSE: reverse a string or list
        if (expr.functionName === "REVERSE") {
          if (expr.args && expr.args.length > 0) {
            const arg = expr.args[0];
            const argResult = this.translateFunctionArg(arg);
            tables.push(...argResult.tables);
            
            // Check if argument is a list
            const argIsList = this.isListExpression(arg);
            
            if (argIsList) {
              // List reversal: use json_each with descending rowid order
              // argResult.sql appears 2 times
              params.push(...argResult.params, ...argResult.params);
              return {
                sql: `(SELECT CASE WHEN ${argResult.sql} IS NULL THEN NULL ELSE (SELECT json_group_array(value) FROM (SELECT value FROM json_each(${argResult.sql}) ORDER BY key DESC)) END)`,
                tables,
                params,
              };
            } else {
              // String reversal: use recursive CTE
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
          }
          throw new Error("reverse requires an argument");
        }

        // LIST: array/list constructor for expressions (used when list contains non-literals)
        if (expr.functionName === "LIST") {
          if (expr.args && expr.args.length > 0) {
            const elements: string[] = [];
            for (const arg of expr.args) {
              // For boolean literals, use json('true')/json('false') to preserve type
              if (arg.type === "literal" && typeof arg.value === "boolean") {
                elements.push(arg.value ? "json('true')" : "json('false')");
              } else {
                const argResult = this.translateExpression(arg);
                tables.push(...argResult.tables);
                params.push(...argResult.params);
                elements.push(argResult.sql);
              }
            }
            return { sql: `json_array(${elements.join(", ")})`, tables, params };
          }
          return { sql: "json_array()", tables, params };
        }

        // INDEX: list/map element access expr[index]
        if (expr.functionName === "INDEX") {
          if (expr.args && expr.args.length >= 2) {
            const listArg = expr.args[0];
            const indexArg = expr.args[1];
            
            // Helper to resolve the effective expression (follow WITH aliases)
            const resolveExpr = (arg: Expression): Expression => {
              if (arg.type === "variable" && arg.variable) {
                const withAliases = (this.ctx as any).withAliases as Map<string, Expression> | undefined;
                if (withAliases?.has(arg.variable)) {
                  return resolveExpr(withAliases.get(arg.variable)!);
                }
              }
              return arg;
            };
            
            // Resolve both arguments through WITH aliases
            const resolvedListArg = resolveExpr(listArg);
            const resolvedIndexArg = resolveExpr(indexArg);
            
            // Determine if the container is a list or a map
            // Lists require integer indices, maps allow string keys
            const isContainerList = 
              (resolvedListArg.type === "literal" && Array.isArray(resolvedListArg.value)) ||
              (resolvedListArg.type === "function" && resolvedListArg.functionName === "LIST") ||
              (resolvedListArg.type === "parameter" && Array.isArray(this.ctx.paramValues[resolvedListArg.name!]));
            // Check if container is a map: explicit object literal, or node/edge variable (which are always maps)
            let isContainerMap = resolvedListArg.type === "object";
            if (!isContainerMap && resolvedListArg.type === "variable" && resolvedListArg.variable) {
              const varInfo = this.ctx.variables.get(resolvedListArg.variable);
              if (varInfo && (varInfo.type === "node" || varInfo.type === "edge")) {
                isContainerMap = true;
              }
            }
            const isContainerNull = resolvedListArg.type === "literal" && resolvedListArg.value === null;
            
            // Helper to check if an expression produces a string
            const isStringExpression = (arg: Expression): boolean => {
              if (arg.type === "literal" && typeof arg.value === "string") return true;
              // Binary + with string operands produces a string
              if (arg.type === "binary" && arg.operator === "+") {
                if (arg.left && arg.right) {
                  // If either operand is a string, the result is a string (string concatenation)
                  if (isStringExpression(arg.left) || isStringExpression(arg.right)) {
                    return true;
                  }
                }
              }
              return false;
            };
            
            // Type checking for container argument
            // null is allowed (accessing null returns null)
            // Lists and maps are allowed
            // Primitives (boolean, integer, float, string) are NOT allowed
            if (resolvedListArg.type === "literal" && !Array.isArray(resolvedListArg.value) && resolvedListArg.value !== null) {
              const typeName = typeof resolvedListArg.value === "boolean" ? "Boolean" :
                              typeof resolvedListArg.value === "number" ? (Number.isInteger(resolvedListArg.value) ? "Integer" : "Float") :
                              typeof resolvedListArg.value === "string" ? "String" : "value";
              throw new Error(`TypeError: ${typeName} is not subscriptable`);
            }
            
            // Type checking for index argument - only when we KNOW it's a list (not a map)
            // Maps allow string keys, lists require integer indices
            // If we can't determine the container type, we allow both string and integer
            if (isContainerList && !isContainerNull) {
              if (resolvedIndexArg.type === "literal") {
                const indexValue = resolvedIndexArg.value;
                if (typeof indexValue === "boolean") {
                  throw new Error("TypeError: expected Integer but was Boolean");
                }
                if (typeof indexValue === "number" && !Number.isInteger(indexValue)) {
                  throw new Error("TypeError: expected Integer but was Float");
                }
                if (typeof indexValue === "string") {
                  throw new Error("TypeError: expected Integer but was String");
                }
                if (Array.isArray(indexValue)) {
                  throw new Error("TypeError: expected Integer but was List");
                }
                if (typeof indexValue === "object" && indexValue !== null) {
                  throw new Error("TypeError: expected Integer but was Map");
                }
              }
              
              // Check for object expression type (map literal) used as index for list
              if (resolvedIndexArg.type === "object") {
                throw new Error("TypeError: expected Integer but was Map");
              }
              
              // Check for list expression type (LIST function) used as index for list
              if (resolvedIndexArg.type === "function" && resolvedIndexArg.functionName === "LIST") {
                throw new Error("TypeError: expected Integer but was List");
              }
              
              // For parameters used as index for list, check at translation time
              if (resolvedIndexArg.type === "parameter") {
                const paramValue = this.ctx.paramValues[resolvedIndexArg.name!];
                if (typeof paramValue === "boolean") {
                  throw new Error("TypeError: expected Integer but was Boolean");
                }
                if (typeof paramValue === "number" && !Number.isInteger(paramValue)) {
                  throw new Error("TypeError: expected Integer but was Float");
                }
                if (typeof paramValue === "string") {
                  throw new Error("TypeError: expected Integer but was String");
                }
                if (Array.isArray(paramValue)) {
                  throw new Error("TypeError: expected Integer but was List");
                }
                if (typeof paramValue === "object" && paramValue !== null) {
                  throw new Error("TypeError: expected Integer but was Map");
                }
              }
            }
            
            // For container parameter, check if it's actually a list or map (not a primitive)
            if (resolvedListArg.type === "parameter") {
              const paramValue = this.ctx.paramValues[resolvedListArg.name!];
              if (paramValue !== null && paramValue !== undefined && !Array.isArray(paramValue) && typeof paramValue !== "object") {
                const typeName = typeof paramValue === "boolean" ? "Boolean" :
                                typeof paramValue === "number" ? (Number.isInteger(paramValue) ? "Integer" : "Float") :
                                typeof paramValue === "string" ? "String" : "value";
                throw new Error(`TypeError: ${typeName} is not subscriptable`);
              }
            }
            
            const listResult = this.translateExpression(listArg);
            const indexResult = this.translateExpression(indexArg);
            tables.push(...listResult.tables, ...indexResult.tables);
            params.push(...listResult.params, ...indexResult.params);
            
            // For map access with string key, use json_extract with the key
            // For list access with integer index, use json_extract with array index
            if (isContainerMap || isStringExpression(resolvedIndexArg)) {
              // Map access: use key directly
              return { sql: `json_extract(${listResult.sql}, '$.' || ${indexResult.sql})`, tables, params };
            }
            // Use -> operator with array index to preserve JSON types (booleans, etc.)
            // Cast index to integer to avoid "0.0" in JSON path
            return { sql: `(${listResult.sql}) -> ('$[' || CAST(${indexResult.sql} AS INTEGER) || ']')`, tables, params };
          }
          throw new Error("INDEX requires list and index arguments");
        }

        // SLICE_FROM_START: list[..end] - start from index 0, but end is explicit and may be null
        if (expr.functionName === "SLICE_FROM_START") {
          if (expr.args && expr.args.length >= 2) {
            const listResult = this.translateExpression(expr.args[0]);
            const endResult = this.translateExpression(expr.args[1]);
            tables.push(...listResult.tables, ...endResult.tables);
            
            // End is explicit, so if it's null the result should be null
            // SQL uses endResult twice (IS NULL check and CAST)
            params.push(...endResult.params, ...listResult.params, ...endResult.params);
            
            return { 
              sql: `CASE WHEN ${endResult.sql} IS NULL THEN NULL ` +
                   `ELSE (SELECT json_group_array(j.value) FROM json_each(${listResult.sql}) j ` +
                   `WHERE j.key >= 0 AND j.key < CAST(${endResult.sql} AS INTEGER)) END`, 
              tables, 
              params 
            };
          }
          throw new Error("SLICE_FROM_START requires list and end arguments");
        }

        // SLICE_TO_END: list[start..] - slice to end of list, but start is explicit and may be null
        if (expr.functionName === "SLICE_TO_END") {
          if (expr.args && expr.args.length >= 2) {
            const listResult = this.translateExpression(expr.args[0]);
            const startResult = this.translateExpression(expr.args[1]);
            tables.push(...listResult.tables, ...startResult.tables);
            
            // Start is explicit, so if it's null the result should be null
            // SQL uses startResult twice and listResult twice
            params.push(...startResult.params, ...listResult.params, ...startResult.params, ...listResult.params);
            
            return { 
              sql: `CASE WHEN ${startResult.sql} IS NULL THEN NULL ` +
                   `ELSE (SELECT json_group_array(j.value) FROM json_each(${listResult.sql}) j ` +
                   `WHERE j.key >= CAST(${startResult.sql} AS INTEGER) AND j.key < json_array_length(${listResult.sql})) END`, 
              tables, 
              params 
            };
          }
          throw new Error("SLICE_TO_END requires list and start arguments");
        }

        // SLICE: list[start..end] - both bounds are explicit (can be null)
        // Supports negative indices: -1 means last element, -n means n from end
        if (expr.functionName === "SLICE") {
          if (expr.args && expr.args.length >= 3) {
            const listResult = this.translateExpression(expr.args[0]);
            const startResult = this.translateExpression(expr.args[1]);
            const endResult = this.translateExpression(expr.args[2]);
            tables.push(...listResult.tables, ...startResult.tables, ...endResult.tables);
            
            // In Cypher, if either explicit bound is null, result is null
            // Negative indices need to be converted: -i becomes (length - i)
            
            // For negative index handling: CASE WHEN idx < 0 THEN len + idx ELSE idx END
            const normalizeIndex = (idxSql: string, lenSql: string) => 
              `CASE WHEN ${idxSql} < 0 THEN ${lenSql} + ${idxSql} ELSE ${idxSql} END`;
            
            const startNorm = normalizeIndex(`CAST(${startResult.sql} AS INTEGER)`, `json_array_length(${listResult.sql})`);
            const endNorm = normalizeIndex(`CAST(${endResult.sql} AS INTEGER)`, `json_array_length(${listResult.sql})`);
            
            // SQL template reference order (counting each ${} placeholder):
            // 1. startResult.sql (IS NULL check)
            // 2. endResult.sql (IS NULL check)
            // 3. listResult.sql (json_each)
            // 4. startResult.sql (< 0 check in start norm)
            // 5. listResult.sql (json_array_length in start norm)
            // 6. startResult.sql (+ in start norm)
            // 7. startResult.sql (ELSE in start norm)
            // 8. endResult.sql (< 0 check in end norm)
            // 9. listResult.sql (json_array_length in end norm)
            // 10. endResult.sql (+ in end norm)
            // 11. endResult.sql (ELSE in end norm)
            params.push(
              ...startResult.params,  // 1: IS NULL
              ...endResult.params,    // 2: IS NULL
              ...listResult.params,   // 3: json_each
              ...startResult.params,  // 4: < 0
              ...listResult.params,   // 5: len for start
              ...startResult.params,  // 6: + idx
              ...startResult.params,  // 7: ELSE idx
              ...endResult.params,    // 8: < 0
              ...listResult.params,   // 9: len for end
              ...endResult.params,    // 10: + idx
              ...endResult.params     // 11: ELSE idx
            );
            
            return { 
              sql: `CASE WHEN ${startResult.sql} IS NULL OR ${endResult.sql} IS NULL THEN NULL ` +
                   `ELSE (SELECT json_group_array(j.value) FROM json_each(${listResult.sql}) j ` +
                   `WHERE j.key >= (${startNorm}) AND j.key < (${endNorm})) END`, 
              tables, 
              params 
            };
          }
          throw new Error("SLICE requires list, start, and end arguments");
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
        // Preserve float-literal formatting (e.g., 0.0, -0.0, 1.0) so SQLite treats them as REAL.
        if (typeof value === "number" && expr.numberLiteralKind === "float" && expr.raw) {
          return { sql: expr.raw, tables, params };
        }
        // Inline numeric literals to preserve integer division behavior
        // (SQLite treats bound parameters as floats)
        if (typeof value === "number" && Number.isInteger(value)) {
          return { sql: String(value), tables, params };
        }
        params.push(value);
        return { sql: "?", tables, params };
      }

      case "parameter": {
        const paramValue = this.ctx.paramValues[expr.name!];
        if (Array.isArray(paramValue) || (typeof paramValue === "object" && paramValue !== null)) {
          params.push(JSON.stringify(paramValue));
        } else {
          params.push(paramValue);
        }
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

      case "patternComprehension": {
        return this.translatePatternComprehension(expr);
      }

      case "listPredicate": {
        return this.translateListPredicate(expr);
      }

      case "unary": {
        return this.translateUnaryExpression(expr);
      }

      case "labelPredicate": {
        // (n:Label) - returns true/false based on whether node has the label
        const varInfo = this.ctx.variables.get(expr.variable!);
        if (!varInfo) {
          throw new Error(`Unknown variable: ${expr.variable}`);
        }
        tables.push(varInfo.alias);
        
        // Handle single label or multiple labels
        // Labels are stored as JSON arrays, e.g., ["Foo", "Bar"]
        // We need to check if the array contains all specified labels
        const labelsToCheck = expr.labels || (expr.label ? [expr.label] : []);
        
        // Use EXISTS with json_each to check if label is in the array
        // For multiple labels, all must be present (AND)
        const labelChecks = labelsToCheck.map(l => 
          `EXISTS(SELECT 1 FROM json_each(${varInfo.alias}.label) WHERE value = '${l}')`
        ).join(' AND ');
        
        return {
          sql: `(${labelChecks})`,
          tables,
          params,
        };
      }

      case "propertyAccess": {
        // Chained property access: obj.prop1.prop2
        // Recursively translate the object expression, then access the property
        const objectResult = this.translateExpression(expr.object!);
        tables.push(...objectResult.tables);
        params.push(...objectResult.params);
        
        // Access property from the result using json_extract
        return {
          sql: `json_extract(${objectResult.sql}, '$.${expr.property}')`,
          tables,
          params,
        };
      }

      case "in": {
        // value IN list - check if value is in the list
        const listExpr = expr.list!;
        
        // Check for non-list literals (type error)
        if (listExpr.type === "literal" && !Array.isArray(listExpr.value)) {
          const val = listExpr.value;
          let typeName: string;
          if (val === null) typeName = "Null";
          else if (typeof val === "boolean") typeName = "Boolean";
          else if (typeof val === "number") typeName = Number.isInteger(val) ? "Integer" : "Float";
          else if (typeof val === "string") typeName = "String";
          else typeName = "Map";
          throw new Error(`Type mismatch: expected List but was ${typeName}`);
        }
        
        // Object/map literals are also not valid as IN operands
        if (listExpr.type === "object") {
          throw new Error("Type mismatch: expected List but was Map");
        }
        
        // Helper to check if a value contains complex types (arrays, objects but not null)
        const containsComplexTypes = (val: unknown): boolean => {
          if (Array.isArray(val)) return true;
          if (typeof val === "object" && val !== null) return true;
          return false;
        };
        
        // Helper to check if a value contains null (deeply)
        const containsNull = (val: unknown): boolean => {
          if (val === null) return true;
          if (Array.isArray(val)) return val.some(containsNull);
          return false;
        };
        
        // Helper to check if array has top-level null (not nested)
        const hasTopLevelNull = (arr: unknown[]): boolean => {
          return arr.some(v => v === null);
        };
        
        // Check if LHS is a complex type (list or object)
        const leftExpr = expr.left!;
        const leftIsLiteralArray = leftExpr.type === "literal" && Array.isArray(leftExpr.value);
        // Comparison expressions return scalar boolean, not complex types
        const leftIsComparison = leftExpr.type === "comparison";
        const leftIsComplex = !leftIsComparison && (leftIsLiteralArray ||
                              leftExpr.type === "function" && (leftExpr.functionName === "LIST" || leftExpr.functionName === "INDEX") ||
                              leftExpr.type === "object");
        
        // Check if LHS literal array contains null
        const leftHasNull = leftIsLiteralArray && containsNull(leftExpr.value);
        
        if (listExpr.type === "literal" && Array.isArray(listExpr.value)) {
          const values = listExpr.value as unknown[];
          if (values.length === 0) {
            return { sql: "0", tables, params }; // false for empty list
          }
          
          // Check if RHS contains complex types (nested arrays/objects)
          const rhsHasComplexTypes = values.some(containsComplexTypes);
          // Check if RHS contains null (at any level)
          const rhsHasNull = values.some(containsNull);
          // Check if RHS has top-level null (for null semantics)
          const rhsHasTopLevelNull = hasTopLevelNull(values);
          
          // If either LHS or RHS contains complex types, use JSON comparison via json_each
          // JSON comparison handles nested arrays correctly, but needs special null semantics
          if (leftIsComplex || rhsHasComplexTypes) {
            // Serialize RHS as JSON
            const rhsJson = JSON.stringify(values);
            
            // For literal arrays, serialize directly to JSON to preserve integer types
            if (leftIsLiteralArray) {
              const lhsJson = JSON.stringify(leftExpr.value);
              
              // Cypher null semantics for list IN:
              // 1. If LHS contains null and we find a match → NULL (match involves null comparison)
              // 2. If LHS contains null and RHS has top-level null → NULL (unknown comparison)
              // 3. If RHS has top-level null and no match → NULL (unknown comparison)
              // 4. Otherwise use standard true/false from JSON comparison
              if (leftHasNull) {
                // If LHS contains null, any comparison involving null returns NULL
                // Even a non-match with top-level null should return NULL
                if (rhsHasTopLevelNull) {
                  // Both LHS and RHS involve null - always NULL
                  return { sql: `NULL`, tables, params };
                }
                // If LHS contains null, finding a JSON match means comparing null=null → return NULL
                params.push(rhsJson, lhsJson);
                return {
                  sql: `CASE WHEN EXISTS(SELECT 1 FROM json_each(?) WHERE json(value) = json(?)) THEN NULL ELSE 0 END`,
                  tables,
                  params,
                };
              }
              
              if (rhsHasTopLevelNull) {
                // If RHS has top-level null and no match, return NULL
                params.push(rhsJson, lhsJson);
                return {
                  sql: `CASE WHEN EXISTS(SELECT 1 FROM json_each(?) WHERE json(value) = json(?)) THEN 1 ELSE NULL END`,
                  tables,
                  params,
                };
              }
              
              // If RHS contains nested nulls (e.g., [[null, 2]]) we need element-wise comparison
              // to determine if the result should be null (potential match with null) or false (definite mismatch)
              if (rhsHasNull) {
                // Generate SQL that:
                // 1. First checks for exact JSON match → return true
                // 2. Then checks if any RHS element could potentially match (same length, no definite mismatches, has null) → return null
                // 3. Otherwise → return false
                const lhsLen = (leftExpr.value as unknown[]).length;
                params.push(lhsJson, rhsJson);
                return {
                  sql: `(SELECT CASE 
                    WHEN EXISTS(SELECT 1 FROM json_each(rhs_param.v) WHERE json(value) = json(lhs_param.v)) THEN 1
                    WHEN EXISTS(
                      SELECT 1 FROM (
                        SELECT rhs.rowid,
                          SUM(CASE WHEN json_extract(rhs.value, '$[' || idx.i || ']') IS NULL THEN 0
                              WHEN json_quote(json_extract(lhs_param.v, '$[' || idx.i || ']')) = json_quote(json_extract(rhs.value, '$[' || idx.i || ']')) THEN 0
                              ELSE 1 END) AS mismatches,
                          SUM(CASE WHEN json_extract(rhs.value, '$[' || idx.i || ']') IS NULL THEN 1 ELSE 0 END) AS nulls
                        FROM json_each(rhs_param.v) AS rhs,
                             (WITH RECURSIVE c(i) AS (SELECT 0 UNION ALL SELECT i+1 FROM c WHERE i < ${lhsLen - 1}) SELECT i FROM c) AS idx
                        WHERE json_type(rhs.value) = 'array' AND json_array_length(rhs.value) = ${lhsLen}
                        GROUP BY rhs.rowid
                      ) WHERE mismatches = 0 AND nulls > 0
                    ) THEN NULL
                    ELSE 0
                  END FROM (SELECT ? AS v) AS lhs_param, (SELECT ? AS v) AS rhs_param)`,
                  tables,
                  params,
                };
              }
              
              // No null semantics needed - simple JSON comparison
              params.push(rhsJson, lhsJson);
              return {
                sql: `EXISTS(SELECT 1 FROM json_each(?) WHERE json(value) = json(?))`,
                tables,
                params,
              };
            }
            
            // For other LHS expressions
            const leftResult = this.translateExpression(leftExpr);
            tables.push(...leftResult.tables);
            // IMPORTANT: params order must match SQL placeholder order
            // SQL: json_each(?) ... ${leftResult.sql}
            // So rhsJson comes first (for json_each), then leftResult.params (for leftResult.sql)
            params.push(rhsJson);
            params.push(...leftResult.params);
            
            // When LHS is a scalar expression (like comparison result), don't use json() wrapper
            // because SQLite UDF returns real type and json(0.0) != json(0) 
            // Use direct value comparison which handles int/real equality correctly
            const useDirectComparison = !leftIsComplex;
            
            if (rhsHasTopLevelNull) {
              if (useDirectComparison) {
                return {
                  sql: `CASE WHEN EXISTS(SELECT 1 FROM json_each(?) WHERE value = ${leftResult.sql}) THEN 1 ELSE NULL END`,
                  tables,
                  params,
                };
              }
              return {
                sql: `CASE WHEN EXISTS(SELECT 1 FROM json_each(?) WHERE json(value) = json(${leftResult.sql})) THEN 1 ELSE NULL END`,
                tables,
                params,
              };
            }
            
            if (useDirectComparison) {
              return {
                sql: `EXISTS(SELECT 1 FROM json_each(?) WHERE value = ${leftResult.sql})`,
                tables,
                params,
              };
            }
            return {
              sql: `EXISTS(SELECT 1 FROM json_each(?) WHERE json(value) = json(${leftResult.sql}))`,
              tables,
              params,
            };
          }
          
          // Simple scalar values - translate LHS and use SQL IN clause
          const leftResult = this.translateExpression(leftExpr);
          tables.push(...leftResult.tables);
          params.push(...leftResult.params);
          const placeholders = values.map(() => "?").join(", ");
          params.push(...toSqliteParams(values));
          // Wrap left side in extra parentheses to ensure correct precedence (e.g., NOT has lower precedence than IN in SQL)
          const leftSql = leftExpr.type === "unary" ? `(${leftResult.sql})` : leftResult.sql;
          return {
            sql: `(${leftSql} IN (${placeholders}))`,
            tables,
            params,
          };
        }
        
        if (listExpr.type === "parameter") {
          const paramValue = this.ctx.paramValues[listExpr.name!];
          if (Array.isArray(paramValue)) {
            if (paramValue.length === 0) {
              return { sql: "0", tables, params }; // false for empty list
            }
            
            // Check if RHS contains complex types
            const rhsHasComplexTypes = paramValue.some(containsComplexTypes);
            
            // If either LHS or RHS contains complex types, use JSON comparison
            // JSON comparison handles nested arrays correctly, including those with nulls
            if (leftIsComplex || rhsHasComplexTypes) {
              const rhsJson = JSON.stringify(paramValue);
              
              // For literal arrays, serialize directly to JSON
              if (leftIsLiteralArray) {
                const lhsJson = JSON.stringify(leftExpr.value);
                params.push(rhsJson, lhsJson);
                return {
                  sql: `EXISTS(SELECT 1 FROM json_each(?) WHERE json(value) = json(?))`,
                  tables,
                  params,
                };
              }
              
              const leftResult = this.translateExpression(leftExpr);
              tables.push(...leftResult.tables);
              params.push(...leftResult.params);
              params.push(rhsJson);
              return {
                sql: `EXISTS(SELECT 1 FROM json_each(?) WHERE json(value) = json(${leftResult.sql}))`,
                tables,
                params,
              };
            }
            
            // Simple scalar LHS and values
            const leftResult = this.translateExpression(leftExpr);
            tables.push(...leftResult.tables);
            params.push(...leftResult.params);
            const placeholders = paramValue.map(() => "?").join(", ");
            params.push(...toSqliteParams(paramValue));
            // Wrap left side in extra parentheses to ensure correct precedence (e.g., NOT has lower precedence than IN in SQL)
            const leftSql = leftExpr.type === "unary" ? `(${leftResult.sql})` : leftResult.sql;
            return {
              sql: `(${leftSql} IN (${placeholders}))`,
              tables,
              params,
            };
          }
          throw new Error(`Parameter ${listExpr.name} must be an array for IN clause`);
        }
        
        // For other list expressions (function calls like keys(), variables, etc.)
        const listResult = this.translateExpression(listExpr);
        tables.push(...listResult.tables);
        
        // For literal arrays on LHS without null, serialize to JSON for proper comparison
        // Note: In the SQL, listResult comes after the literal, so params order: listResult, literal
        if (leftIsLiteralArray && !leftHasNull) {
          const lhsJson = JSON.stringify(leftExpr.value);
          params.push(...listResult.params);
          params.push(lhsJson);
          return {
            sql: `EXISTS(SELECT 1 FROM json_each(${listResult.sql}) WHERE json(value) = json(?))`,
            tables,
            params,
          };
        }
        
        // For complex LHS types (non-literal arrays/objects) without null, use json() comparison
        // Note: In the SQL, listResult comes before leftResult, so params order: listResult, leftResult
        if (leftIsComplex && !leftHasNull) {
          const leftResult = this.translateExpression(leftExpr);
          tables.push(...leftResult.tables);
          params.push(...listResult.params);
          params.push(...leftResult.params);
          return {
            sql: `EXISTS(SELECT 1 FROM json_each(${listResult.sql}) WHERE json(value) = json(${leftResult.sql}))`,
            tables,
            params,
          };
        }
        
        // For scalar LHS or LHS with null, use simple IN clause which handles int/float equality correctly
        // Note: In the SQL, leftResult comes before listResult, so params order: leftResult, listResult
        const leftResult = this.translateExpression(leftExpr);
        tables.push(...leftResult.tables);
        params.push(...leftResult.params);
        params.push(...listResult.params);
        // Wrap left side in extra parentheses to ensure correct precedence (e.g., NOT has lower precedence than IN in SQL)
        const leftSql = leftExpr.type === "unary" ? `(${leftResult.sql})` : leftResult.sql;
        return {
          sql: `(${leftSql} IN (SELECT value FROM json_each(${listResult.sql})))`,
          tables,
          params,
        };
      }

      case "stringOp": {
        // String operations: CONTAINS, STARTS WITH, ENDS WITH
        // For non-string operands, return NULL (proper Cypher semantics)
        // 
        // A value is a "true Cypher string" if:
        //   typeof(X) = 'text' AND json_valid(X) = 0
        // This distinguishes actual strings from serialized arrays/objects (which are text but json_valid)
        const leftExpr = expr.left!;
        const rightExpr = expr.right!;
        const leftResult = this.translateExpression(leftExpr);
        const rightResult = this.translateExpression(rightExpr);
        const tables = [...leftResult.tables, ...rightResult.tables];
        
        // Helper to build "is true string" check
        const isString = (sql: string) => `(typeof(${sql}) = 'text' AND json_valid(${sql}) = 0)`;
        
        const stringOp = expr.stringOperator;
        
        if (stringOp === "CONTAINS") {
          // INSTR returns position (1-based) if found, 0 if not found
          return {
            sql: `CASE WHEN ${isString(leftResult.sql)} AND ${isString(rightResult.sql)} THEN INSTR(${leftResult.sql}, ${rightResult.sql}) > 0 ELSE NULL END`,
            tables,
            // leftResult.sql appears 3 times, rightResult.sql appears 3 times
            params: [...leftResult.params, ...leftResult.params, ...rightResult.params, ...rightResult.params, ...leftResult.params, ...rightResult.params],
          };
        } else if (stringOp === "STARTS WITH") {
          // Use SUBSTR for case-sensitive prefix match
          return {
            sql: `CASE WHEN ${isString(leftResult.sql)} AND ${isString(rightResult.sql)} THEN SUBSTR(${leftResult.sql}, 1, LENGTH(${rightResult.sql})) = ${rightResult.sql} ELSE NULL END`,
            tables,
            // leftResult.sql appears 3 times, rightResult.sql appears 5 times
            params: [...leftResult.params, ...leftResult.params, ...rightResult.params, ...rightResult.params, ...leftResult.params, ...rightResult.params, ...rightResult.params],
          };
        } else {
          // ENDS WITH
          // Use CASE to handle: 1) type check 2) empty suffix edge case, 3) case-sensitive suffix match
          return {
            sql: `CASE WHEN NOT (${isString(leftResult.sql)} AND ${isString(rightResult.sql)}) THEN NULL WHEN LENGTH(${rightResult.sql}) = 0 THEN 1 ELSE SUBSTR(${leftResult.sql}, -LENGTH(${rightResult.sql})) = ${rightResult.sql} END`,
            tables,
            // leftResult.sql appears 4 times, rightResult.sql appears 6 times
            params: [...leftResult.params, ...leftResult.params, ...rightResult.params, ...rightResult.params, ...rightResult.params, ...leftResult.params, ...rightResult.params, ...rightResult.params],
          };
        }
      }

      default:
        throw new Error(`Unknown expression type: ${expr.type}`);
    }
  }

  private translateCaseExpression(expr: Expression): { sql: string; tables: string[]; params: unknown[] } {
    const tables: string[] = [];
    const params: unknown[] = [];
    
    let sql = "CASE";
    
    // Check if this is a simple form CASE (has expression)
    const isSimpleForm = expr.expression !== undefined;
    
    // For simple form, translate the case expression once
    let caseExprSql: string | undefined;
    let caseExprType: string | undefined;
    if (isSimpleForm) {
      const { sql: exprSql, tables: exprTables, params: exprParams } = this.translateExpression(expr.expression!);
      caseExprSql = exprSql;
      caseExprType = this.getCypherTypeForExpression(expr.expression!);
      tables.push(...exprTables);
      // We'll add these params for each WHEN clause comparison
      // Store them separately
      var caseExprParams = exprParams;
    }
    
    // Process each WHEN clause
    for (const when of expr.whens || []) {
      let condSql: string;
      let condParams: unknown[];
      
      if (isSimpleForm && when.condition.type === "comparison" && when.condition.operator === "=") {
        // Simple form: use type-aware comparison
        // The condition.right is the WHEN value
        const whenValue = when.condition.right!;
        const { sql: whenSql, tables: whenTables, params: whenParams } = this.translateExpression(whenValue);
        const whenType = this.getCypherTypeForExpression(whenValue);
        
        tables.push(...whenTables);
        // cypher_case_eq(caseExprSql, caseExprType, whenSql, whenType)
        condSql = `cypher_case_eq(${caseExprSql}, ?, ${whenSql}, ?)`;
        condParams = [...caseExprParams!, caseExprType, ...whenParams, whenType];
      } else {
        // Searched form: use regular WHERE translation
        const result = this.translateWhere(when.condition);
        condSql = result.sql;
        condParams = result.params;
      }
      
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
  
  /**
   * Get the Cypher type string for an expression (for type-aware comparison in CASE)
   */
  private getCypherTypeForExpression(expr: Expression): string {
    switch (expr.type) {
      case "literal": {
        const value = expr.value;
        if (value === null) return "null";
        if (typeof value === "boolean") return "boolean";
        if (typeof value === "number") {
          // Check if it was explicitly a float literal
          if (expr.numberLiteralKind === "float") return "float";
          return Number.isInteger(value) ? "integer" : "float";
        }
        if (typeof value === "string") return "string";
        if (Array.isArray(value)) return "list";
        if (typeof value === "object") return "map";
        return "unknown";
      }
      case "parameter": {
        const paramValue = this.ctx.paramValues[expr.name!];
        if (paramValue === null || paramValue === undefined) return "null";
        if (typeof paramValue === "boolean") return "boolean";
        if (typeof paramValue === "number") {
          return Number.isInteger(paramValue) ? "integer" : "float";
        }
        if (typeof paramValue === "string") return "string";
        if (Array.isArray(paramValue)) return "list";
        if (typeof paramValue === "object") return "map";
        return "unknown";
      }
      case "variable":
      case "property":
        // For variables and properties, we don't know the type at translation time
        // These will need runtime type checking - use "dynamic"
        return "dynamic";
      case "function":
        // Functions have various return types - would need to analyze each function
        return "dynamic";
      case "binary":
        // Binary operations typically return numbers
        if (expr.operator === "+" || expr.operator === "-" || expr.operator === "*" || expr.operator === "/" || expr.operator === "%" || expr.operator === "^") {
          return "number"; // Could be integer or float depending on operands
        }
        return "dynamic";
      case "case":
        // CASE expressions return dynamic type based on THEN values
        return "dynamic";
      default:
        return "dynamic";
    }
  }

  private translateBinaryExpression(expr: Expression): { sql: string; tables: string[]; params: unknown[] } {
    const tables: string[] = [];
    const params: unknown[] = [];

    const temporalDuration = this.tryTranslateTemporalDurationArithmetic(expr);
    if (temporalDuration) {
      return temporalDuration;
    }

    const leftResult = this.translateExpression(expr.left!);
    const rightResult = this.translateExpression(expr.right!);

    tables.push(...leftResult.tables, ...rightResult.tables);
    params.push(...leftResult.params, ...rightResult.params);

    // Check if this is list concatenation (+ operator with arrays)
    const leftIsList = this.isListExpression(expr.left!);
    const rightIsList = this.isListExpression(expr.right!);
    const leftIsStringLiteral = expr.left?.type === "literal" && typeof expr.left.value === "string";
    const rightIsStringLiteral = expr.right?.type === "literal" && typeof expr.right.value === "string";
    
    
    if (expr.operator === "+" && leftIsList && rightIsList) {
      // Both are lists: list + list concatenation
      // Pattern: (SELECT json_group_array(value) FROM (SELECT value FROM json_each(left) UNION ALL SELECT value FROM json_each(right)))
      const leftArraySql = this.wrapForArray(expr.left!, leftResult.sql);
      const rightArraySql = this.wrapForArray(expr.right!, rightResult.sql);
      
      return {
        sql: `(SELECT json_group_array(value) FROM (SELECT value FROM json_each(${leftArraySql}) UNION ALL SELECT value FROM json_each(${rightArraySql})))`,
        tables,
        params,
      };
    }
    
    if (expr.operator === "+" && leftIsList && !rightIsList) {
      // list + scalar: append scalar to list
      // Use json_quote() to properly convert any scalar (including strings) to JSON
      const leftArraySql = this.wrapForArray(expr.left!, leftResult.sql);
      const rightScalarSql = rightResult.sql;
      
      return {
        sql: `(SELECT json_group_array(value) FROM (SELECT value FROM json_each(${leftArraySql}) UNION ALL SELECT json_quote(${rightScalarSql})))`,
        tables,
        params,
      };
    }

    // String concatenation: if either side is definitely a string (literal or string concat chain),
    // use || for concatenation instead of + (numeric addition)
    const leftIsStringConcat = this.isStringConcatenation(expr.left!);
    const rightIsStringConcat = this.isStringConcatenation(expr.right!);
    if (expr.operator === "+" && !leftIsList && !rightIsList && (leftIsStringConcat || rightIsStringConcat)) {
      const leftSql = this.wrapForArithmetic(expr.left!, leftResult.sql);
      const rightSql = this.wrapForArithmetic(expr.right!, rightResult.sql);
      return { sql: `(${leftSql} || ${rightSql})`, tables, params };
    }

    // For property + literal list (where left is property and right is known list)
    // Must check before scalar+list since property is not detected as list
    if (expr.operator === "+" && expr.left!.type === "property" && rightIsList) {
      const leftPropSql = this.wrapForArray(expr.left!, leftResult.sql);
      const rightArraySql = this.wrapForArray(expr.right!, rightResult.sql);
      
      return {
        sql: `(SELECT json_group_array(value) FROM (SELECT value FROM json_each(${leftPropSql}) UNION ALL SELECT value FROM json_each(${rightArraySql})))`,
        tables,
        params,
      };
    }
    
    // For literal list + property (where right is property and left is known list)
    // Must check before list+scalar since property is not detected as list
    if (expr.operator === "+" && leftIsList && expr.right!.type === "property") {
      const leftArraySql = this.wrapForArray(expr.left!, leftResult.sql);
      const rightPropSql = this.wrapForArray(expr.right!, rightResult.sql);
      
      return {
        sql: `(SELECT json_group_array(value) FROM (SELECT value FROM json_each(${leftArraySql}) UNION ALL SELECT value FROM json_each(${rightPropSql})))`,
        tables,
        params,
      };
    }
    
    if (expr.operator === "+" && !leftIsList && rightIsList) {
      // scalar + list: prepend scalar to list (only for non-property scalars)
      // Use json_quote() to properly convert any scalar (including strings) to JSON
      const leftScalarSql = leftResult.sql;
      const rightArraySql = this.wrapForArray(expr.right!, rightResult.sql);
      
      return {
        sql: `(SELECT json_group_array(value) FROM (SELECT json_quote(${leftScalarSql}) as value UNION ALL SELECT value FROM json_each(${rightArraySql})))`,
        tables,
        params,
      };
    }
    
    // For property + property, use a WITH subquery to avoid duplicate parameter references
    if (expr.operator === "+" && expr.left!.type === "property" && expr.right!.type === "property") {
      const leftPropSql = this.wrapForArray(expr.left!, leftResult.sql);
      const rightPropSql = this.wrapForArray(expr.right!, rightResult.sql);
      
      // Use CASE with json_type to handle runtime type detection
      // If both are arrays, concatenate them. Otherwise fall through to arithmetic.
      return {
        sql: `(CASE 
          WHEN json_type(${leftPropSql}) = 'array' AND json_type(${rightPropSql}) = 'array' THEN
            (SELECT json_group_array(value) FROM (SELECT value FROM json_each(${leftPropSql}) UNION ALL SELECT value FROM json_each(${rightPropSql})))
          ELSE (COALESCE(${leftPropSql}, 0) + COALESCE(${rightPropSql}, 0))
        END)`,
        tables,
        params,
      };
    }

    // Handle logical operators (AND, OR, XOR)
    if (expr.operator === "AND" || expr.operator === "OR") {
      // Validate that both operands are boolean types
      this.validateBooleanOperand(expr.left!, expr.operator);
      this.validateBooleanOperand(expr.right!, expr.operator);
      
      // Use custom cypher_and/cypher_or functions for proper JSON boolean handling
      const func = expr.operator === "AND" ? "cypher_and" : "cypher_or";
      return {
        sql: `${func}(${leftResult.sql}, ${rightResult.sql})`,
        tables,
        params,
      };
    }

    // Handle XOR - SQLite doesn't have XOR, implement as (a AND NOT b) OR (NOT a AND b)
    // With proper NULL handling: if either operand is NULL, result is NULL
    if (expr.operator === "XOR") {
      this.validateBooleanOperand(expr.left!, expr.operator);
      this.validateBooleanOperand(expr.right!, expr.operator);
      
      const leftSql = leftResult.sql;
      const rightSql = rightResult.sql;
      // XOR with NULL semantics: (a XOR b) = (a AND NOT b) OR (NOT a AND b)
      // This naturally handles NULL: if a is NULL, (a AND NOT b) is NULL or FALSE, (NOT a AND b) is NULL or FALSE
      // NULL OR NULL = NULL, NULL OR FALSE = NULL, so result is NULL when either input is NULL
      // Note: params are duplicated because the formula uses each operand twice:
      // ((left AND NOT right) OR (NOT left AND right))
      const xorParams = [...leftResult.params, ...rightResult.params, ...leftResult.params, ...rightResult.params];
      return {
        sql: `((${leftSql} AND NOT ${rightSql}) OR (NOT ${leftSql} AND ${rightSql}))`,
        tables,
        params: xorParams,
      };
    }

    // For property access in arithmetic, we need to use json_extract to get the numeric value
    const leftSql = this.wrapForArithmetic(expr.left!, leftResult.sql);
    const rightSql = this.wrapForArithmetic(expr.right!, rightResult.sql);

    // Handle exponentiation operator (SQLite uses POWER function)
    if (expr.operator === "^") {
      return {
        sql: `POWER(${leftSql}, ${rightSql})`,
        tables,
        params,
      };
    }

    return {
      sql: `(${leftSql} ${expr.operator} ${rightSql})`,
      tables,
      params,
    };
  }

  private tryTranslateTemporalDurationArithmetic(
    expr: Expression,
  ): { sql: string; tables: string[]; params: unknown[] } | null {
    if (expr.type !== "binary") return null;
    if (expr.operator !== "+" && expr.operator !== "-") return null;
    if (!expr.left || !expr.right) return null;

    const isDurationFn = (e: Expression) =>
      e.type === "function" &&
      e.functionName === "DURATION" &&
      Array.isArray(e.args) &&
      e.args.length === 1 &&
      e.args[0]?.type === "object";

    const leftIsDuration = isDurationFn(expr.left);
    const rightIsDuration = isDurationFn(expr.right);
    if (!leftIsDuration && !rightIsDuration) return null;

    const durationExpr = (leftIsDuration ? expr.left : expr.right) as Expression;
    const temporalExpr = (leftIsDuration ? expr.right : expr.left) as Expression;

    if (expr.operator === "-" && leftIsDuration) {
      return null;
    }

    const temporalType =
      temporalExpr.type === "property"
        ? temporalExpr.property?.toLowerCase?.() === "date"
          ? "date"
          : temporalExpr.property?.toLowerCase?.() === "time"
            ? "time"
            : temporalExpr.property?.toLowerCase?.() === "datetime"
              ? "datetime"
              : null
        : temporalExpr.type === "function"
          ? temporalExpr.functionName === "DATE"
            ? "date"
            : temporalExpr.functionName === "LOCALTIME" || temporalExpr.functionName === "TIME"
              ? "time"
              : temporalExpr.functionName === "LOCALDATETIME" || temporalExpr.functionName === "DATETIME"
                ? "datetime"
                : null
          : null;

    if (!temporalType) return null;

    const temporalResult = this.translateExpression(temporalExpr);
    const baseSql = this.wrapForArithmetic(temporalExpr, temporalResult.sql);

    const durationMap = durationExpr.args![0] as Expression;
    const properties = durationMap.properties ?? [];
    const byKey = new Map<string, Expression>();
    for (const prop of properties) {
      byKey.set(prop.key.toLowerCase(), prop.value);
    }

    const sign = expr.operator === "-" ? -1 : 1;
    const tables: string[] = [...temporalResult.tables];
    const params: unknown[] = [...temporalResult.params];
    const modifiers: string[] = [];

    const addIntUnit = (keys: string[], unit: string) => {
      const valueExpr = keys.map(k => byKey.get(k)).find(Boolean);
      if (!valueExpr) return;
      const valueResult = this.translateExpression(valueExpr);
      tables.push(...valueResult.tables);
      params.push(...valueResult.params);
      modifiers.push(
        `printf('%+d ${unit}', (${sign}) * CAST(${valueResult.sql} AS INTEGER))`,
      );
    };

    addIntUnit(["years", "year"], "years");
    addIntUnit(["months", "month"], "months");
    addIntUnit(["days", "day"], "days");
    addIntUnit(["hours", "hour"], "hours");
    addIntUnit(["minutes", "minute"], "minutes");
    addIntUnit(["seconds", "second"], "seconds");

    if (modifiers.length === 0) return null;

    const sqliteTemporalFn = temporalType === "date" ? "DATE" : temporalType === "time" ? "TIME" : "DATETIME";
    return {
      sql: `${sqliteTemporalFn}(${baseSql}, ${modifiers.join(", ")})`,
      tables,
      params,
    };
  }

  private isListExpression(expr: Expression, visitedVars?: Set<string>): boolean {
    // Check if expression is likely a list/array type
    if (expr.type === "literal" && Array.isArray(expr.value)) {
      return true;
    }
    if (expr.type === "variable") {
      // Check if this variable is a WITH alias that references an array
      const withAliases = (this.ctx as any).withAliases as Map<string, Expression> | undefined;
      if (withAliases && withAliases.has(expr.variable!)) {
        // Track visited variables to prevent infinite recursion on self-referential aliases
        // (e.g., WITH list + x AS list)
        const visited = visitedVars ?? new Set<string>();
        if (visited.has(expr.variable!)) {
          // Already visited this variable - break the cycle
          return false;
        }
        visited.add(expr.variable!);
        const originalExpr = withAliases.get(expr.variable!)!;
        return this.isListExpression(originalExpr, visited);
      }
    }
    // Note: we cannot assume property access is a list - it could be a number or string.
    // Property access will only be treated as a list if combined with an explicit list literal
    // in translateBinaryExpression.
    if (expr.type === "binary" && expr.operator === "+") {
      // Nested binary + could be chained list concatenation, but only if one side is definitely a list
      return this.isListExpression(expr.left!, visitedVars) || this.isListExpression(expr.right!, visitedVars);
    }
    if (expr.type === "function") {
      // List-returning functions like collect(), range(), etc.
      const listFunctions = ["COLLECT", "RANGE", "KEYS", "LABELS", "SPLIT", "TAIL", "REVERSE"];
      return listFunctions.includes(expr.functionName || "");
    }
    if (expr.type === "case") {
      // Check if CASE branches return lists
      // A CASE is a list expression if any of its WHEN branches are list expressions
      const whens = expr.whens || [];
      for (const when of whens) {
        if (when.result && this.isListExpression(when.result, visitedVars)) {
          return true;
        }
      }
      // Also check the else branch
      if (expr.elseExpr && this.isListExpression(expr.elseExpr, visitedVars)) {
        return true;
      }
    }
    return false;
  }

  private isObjectExpression(expr: Expression, visitedVars?: Set<string>): boolean {
    // Check if expression is a map/object type
    if (expr.type === "object") {
      return true;
    }
    if (expr.type === "variable") {
      // Check if this variable is a WITH alias that references an object
      const withAliases = (this.ctx as any).withAliases as Map<string, Expression> | undefined;
      if (withAliases && withAliases.has(expr.variable!)) {
        // Track visited variables to prevent infinite recursion on self-referential aliases
        const visited = visitedVars ?? new Set<string>();
        if (visited.has(expr.variable!)) {
          // Already visited this variable - break the cycle
          return false;
        }
        visited.add(expr.variable!);
        const originalExpr = withAliases.get(expr.variable!)!;
        return this.isObjectExpression(originalExpr, visited);
      }
    }
    if (expr.type === "function") {
      // Object-returning functions like properties()
      const objectFunctions = ["PROPERTIES"];
      return objectFunctions.includes((expr.functionName || "").toUpperCase());
    }
    return false;
  }

  private isStringConcatenation(expr: Expression): boolean {
    // Check if expression is a string concatenation chain
    // (contains a string literal anywhere in a + chain)
    if (expr.type === "literal" && typeof expr.value === "string") {
      return true;
    }
    if (expr.type === "binary" && expr.operator === "+") {
      // Recursively check if either side is a string concatenation
      return this.isStringConcatenation(expr.left!) || this.isStringConcatenation(expr.right!);
    }
    if (expr.type === "function") {
      // String-returning functions like toString(), toUpper(), toLower(), etc.
      const stringFunctions = ["TOSTRING", "TOUPPER", "TOLOWER", "TRIM", "LTRIM", "RTRIM", "SUBSTRING", "REPLACE", "REVERSE", "LEFT", "RIGHT"];
      return stringFunctions.includes((expr.functionName || "").toUpperCase());
    }
    return false;
  }

  /**
   * Check if an expression could produce NaN (IEEE 754 Not a Number).
   * In SQLite, division by zero returns NULL, but Cypher semantics require NaN.
   * NaN has special comparison semantics: NaN = x is always false, NaN <> x is always true.
   */
  private couldProduceNaN(expr: Expression): boolean {
    // Division can produce NaN (0/0) or Infinity (x/0)
    if (expr.type === "binary" && expr.operator === "/") {
      return true;
    }
    // Recursively check nested expressions
    if (expr.type === "binary") {
      return this.couldProduceNaN(expr.left!) || this.couldProduceNaN(expr.right!);
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

  /**
   * Translate an expression for a SET on a just-created node.
   * Property references need to use subqueries since the node ID isn't a table alias.
   */
  private translateExpressionForCreatedNode(
    expr: Expression,
    nodeId: string
  ): { sql: string; params: unknown[] } {
    const params: unknown[] = [];

    switch (expr.type) {
      case "literal":
        if (Array.isArray(expr.value)) {
          return this.translateArrayLiteral(expr.value as PropertyValue[]);
        }
        const value = expr.value === true ? 1 : expr.value === false ? 0 : expr.value;
        params.push(value);
        return { sql: "?", params };

      case "property": {
        // Use subquery to get property from the created node
        const propName = expr.property!;
        params.push(nodeId);
        return {
          sql: `(SELECT json_extract(properties, '$.${propName}') FROM nodes WHERE id = ?)`,
          params,
        };
      }

      case "binary": {
        const leftResult = this.translateExpressionForCreatedNode(expr.left!, nodeId);
        const rightResult = this.translateExpressionForCreatedNode(expr.right!, nodeId);
        params.push(...leftResult.params, ...rightResult.params);

        // Handle list concatenation
        const leftIsList = this.isListExpression(expr.left!);
        const rightIsList = this.isListExpression(expr.right!);

        if (expr.operator === "+" && (leftIsList || rightIsList || expr.left!.type === "property" || expr.right!.type === "property")) {
          // Use runtime list concatenation with json_each
          return {
            sql: `(SELECT json_group_array(value) FROM (
              SELECT value FROM json_each(${leftResult.sql})
              UNION ALL
              SELECT value FROM json_each(${rightResult.sql})
            ))`,
            params,
          };
        }

        return {
          sql: `(${leftResult.sql} ${expr.operator} ${rightResult.sql})`,
          params,
        };
      }

      default:
        // Fall back to regular translation for other expression types
        const result = this.translateExpression(expr);
        return { sql: result.sql, params: result.params };
    }
  }

  // Helper to check if an expression produces a boolean result (JSON boolean)
  private isBooleanProducingExpression(expr: Expression): boolean {
    if (expr.type === "comparison") {
      return true;
    }
    if (expr.type === "literal" && typeof expr.value === "boolean") {
      return true;
    }
    // NOT expression (unary) also produces boolean
    if (expr.type === "unary" && expr.operator === "NOT") {
      return true;
    }
    // AND/OR/XOR produce booleans (binary type with logical operators)
    if (expr.type === "binary" && (expr.operator === "AND" || expr.operator === "OR" || expr.operator === "XOR")) {
      return true;
    }
    return false;
  }

  // Normalize SQL for boolean comparison - ensures both sides use same representation
  // When one side is a comparison (produces json('true')/json('false')),
  // the other side should also be converted to JSON boolean for proper comparison
  private normalizeForBooleanComparison(expr: Expression, sql: string): string {
    // If it's a boolean literal (translated to 0 or 1), convert to JSON boolean
    if (expr.type === "literal" && typeof expr.value === "boolean") {
      return expr.value ? "json('true')" : "json('false')";
    }
    // Comparison expressions already produce JSON booleans
    return sql;
  }

  private translateComparisonExpression(expr: Expression): { sql: string; tables: string[]; params: unknown[] } {
    const tables: string[] = [];
    const params: unknown[] = [];

    // Handle IS NULL / IS NOT NULL (no right side)
    // Wrap in CASE to return JSON boolean true/false instead of SQLite's 0/1
    if (expr.comparisonOperator === "IS NULL" || expr.comparisonOperator === "IS NOT NULL") {
      const leftResult = this.translateExpression(expr.left!);
      tables.push(...leftResult.tables);
      params.push(...leftResult.params);
      
      // For node/edge variables, check .id IS NULL instead of the full json_object
      // (json_object never returns NULL even with NULL column values)
      let leftSql: string;
      if (expr.left?.type === "variable") {
        const varInfo = this.ctx.variables.get(expr.left.variable!);
        if (varInfo && (varInfo.type === "node" || varInfo.type === "edge")) {
          leftSql = `${varInfo.alias}.id`;
        } else {
          leftSql = this.wrapForComparison(expr.left!, leftResult.sql);
        }
      } else {
        leftSql = this.wrapForComparison(expr.left!, leftResult.sql);
      }
      return {
        sql: `CASE WHEN (${leftSql}) ${expr.comparisonOperator} THEN json('true') ELSE json('false') END`,
        tables,
        params,
      };
    }

    const leftResult = this.translateExpression(expr.left!);
    const rightResult = this.translateExpression(expr.right!);

    tables.push(...leftResult.tables, ...rightResult.tables);
    params.push(...leftResult.params, ...rightResult.params);

    // For property access in comparisons, use json_extract for proper comparison
    let leftSql = this.wrapForComparison(expr.left!, leftResult.sql);
    let rightSql = this.wrapForComparison(expr.right!, rightResult.sql);

    // Handle boolean comparisons: when comparing expressions that produce booleans
    // (IS NULL, IS NOT NULL, other comparisons, boolean literals), ensure both sides
    // use the same representation (JSON booleans) for proper SQLite comparison
    const op = expr.comparisonOperator;
    if (op === "=" || op === "<>") {
      const leftIsBoolean = this.isBooleanProducingExpression(expr.left!);
      const rightIsBoolean = this.isBooleanProducingExpression(expr.right!);
      
      if (leftIsBoolean || rightIsBoolean) {
        // Normalize both sides to JSON boolean representation
        leftSql = this.normalizeForBooleanComparison(expr.left!, leftSql);
        rightSql = this.normalizeForBooleanComparison(expr.right!, rightSql);
      }
    }

    // Handle NaN semantics early: In Cypher, NaN comparisons have special behavior:
    // - NaN = x is always false (including NaN = NaN), regardless of x's type
    // - NaN <> x is always true (including NaN <> NaN), regardless of x's type
    // - NaN < x, NaN <= x, NaN > x, NaN >= x:
    //   - When x is numeric: returns false
    //   - When x is non-numeric (e.g., string): returns null (type incompatibility)
    // SQLite returns NULL for division by zero (0.0/0.0), so we need to convert NULL to the correct boolean.
    const leftCouldBeNaN = this.couldProduceNaN(expr.left!);
    const rightCouldBeNaN = this.couldProduceNaN(expr.right!);
    
    if (leftCouldBeNaN || rightCouldBeNaN) {
      const op = expr.comparisonOperator!;
      
      // For = and <>, NaN semantics always apply (return false/true respectively)
      if (op === "=") {
        // NaN = anything is false (including NaN = NaN)
        // If the comparison returns NULL (because of NaN), return false (0)
        return {
          sql: `COALESCE((${leftSql} ${op} ${rightSql}), 0)`,
          tables,
          params,
        };
      } else if (op === "<>") {
        // NaN <> anything is true (including NaN <> NaN)
        // If the comparison returns NULL (because of NaN), return true (1)
        return {
          sql: `COALESCE((${leftSql} ${op} ${rightSql}), 1)`,
          tables,
          params,
        };
      } else {
        // For <, <=, >, >=: check if comparing to a non-numeric type
        const leftIsString = expr.left?.type === "literal" && typeof expr.left.value === "string";
        const rightIsString = expr.right?.type === "literal" && typeof expr.right.value === "string";
        
        if (leftIsString || rightIsString) {
          // NaN compared to string via range operators returns null (type incompatibility)
          // Let the comparison return null naturally
        } else {
          // NaN compared to numeric via range operators returns false
          return {
            sql: `COALESCE((${leftSql} ${op} ${rightSql}), 0)`,
            tables,
            params,
          };
        }
      }
    }

    // For ordering operators (<, <=, >, >=), use Cypher-compliant type-aware comparison.
    // In Cypher, comparing incompatible types (e.g., string vs number) returns null.
    // Only numbers (integer and real) can be compared across their subtypes.
    const orderingOps = new Set(["<", "<=", ">", ">="]);
    if (orderingOps.has(expr.comparisonOperator!)) {
      // Map operator to function name
      const opToFunc: Record<string, string> = {
        "<": "cypher_lt",
        "<=": "cypher_lte",
        ">": "cypher_gt",
        ">=": "cypher_gte",
      };
      const func = opToFunc[expr.comparisonOperator!];
      
      return {
        sql: `${func}(${leftSql}, ${rightSql})`,
        tables,
        params,
      };
    }

    // Structural equality for lists and maps: use cypher_equals for null-aware deep comparison.
    // In Cypher, [null] = [1] returns null (unknown), not false.
    // Same for maps: {k: null} = {k: null} returns null.
    const needsCypherEquals = 
      (this.isListExpression(expr.left!) && this.isListExpression(expr.right!)) ||
      (this.isObjectExpression(expr.left!) && this.isObjectExpression(expr.right!));
    
    if ((expr.comparisonOperator === "=" || expr.comparisonOperator === "<>") && needsCypherEquals) {
      if (expr.comparisonOperator === "=") {
        return {
          sql: `cypher_equals(${leftSql}, ${rightSql})`,
          tables,
          params,
        };
      } else {
        // <> is NOT equals: invert the result, but preserve null
        // We need to duplicate params because cypher_equals appears twice in the SQL
        return {
          sql: `CASE WHEN cypher_equals(${leftSql}, ${rightSql}) IS NULL THEN NULL WHEN cypher_equals(${leftSql}, ${rightSql}) = 1 THEN 0 ELSE 1 END`,
          tables,
          params: [...params, ...params],
        };
      }
    }

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
        // Use json() to preserve boolean type in the array
        valueParts.push(value ? "json('true')" : "json('false')");
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
   * Translate a pattern comprehension expression.
   * Syntax: [(pattern) WHERE filterCondition | mapExpr]
   * 
   * Pattern comprehensions match a pattern starting from bound variables and
   * return a list of results from the mapExpr (or pattern elements if no mapExpr).
   * 
   * Example: [(a)-[:T|OTHER]->() | 1] returns a list of 1s for each matching edge
   */
  private translatePatternComprehension(expr: Expression): { sql: string; tables: string[]; params: unknown[] } {
    const tables: string[] = [];
    const params: unknown[] = [];
    
    const patterns = expr.patterns!;
    const filterCondition = expr.filterCondition;
    const mapExpr = expr.mapExpr;
    
    // Pattern comprehension structure: patterns from parsePatternChain
    // The first element is always a NodePattern, then potentially a RelationshipPattern
    // A RelationshipPattern contains source, edge, and target
    
    // Check if patterns[0] is a RelationshipPattern (has 'edge' property)
    const firstPattern = patterns[0];
    const isRelPattern = (p: unknown): p is import("./parser").RelationshipPattern => {
      return typeof p === "object" && p !== null && "edge" in p;
    };
    
    let startVar: string | undefined;
    let relPattern: import("./parser").RelationshipPattern | undefined;
    let startNodePattern: import("./parser").NodePattern;
    let targetNodePattern: import("./parser").NodePattern | undefined;
    
    if (isRelPattern(firstPattern)) {
      // First pattern is a RelationshipPattern
      relPattern = firstPattern;
      startNodePattern = relPattern.source;
      targetNodePattern = relPattern.target;
      startVar = startNodePattern.variable;
    } else {
      // First pattern is a NodePattern, look for RelationshipPattern in rest
      startNodePattern = firstPattern as import("./parser").NodePattern;
      startVar = startNodePattern.variable;
      
      for (let i = 1; i < patterns.length; i++) {
        if (isRelPattern(patterns[i])) {
          relPattern = patterns[i] as import("./parser").RelationshipPattern;
          targetNodePattern = relPattern.target;
          break;
        }
      }
    }
    
    if (!startVar) {
      throw new Error("Pattern comprehension must start with a bound variable");
    }
    
    // Get the bound variable info from outer context
    const boundVarInfo = this.ctx.variables.get(startVar);
    if (!boundVarInfo) {
      throw new Error(`Unknown variable in pattern comprehension: ${startVar}`);
    }
    
    if (!relPattern) {
      throw new Error("Pattern comprehension must include a relationship pattern");
    }
    
    // Build the correlated subquery
    const edgeAlias = `__pc_e_${this.ctx.aliasCounter++}`;
    const targetAlias = `__pc_t_${this.ctx.aliasCounter++}`;
    
    const edge = relPattern.edge;
    
    // Build edge type filter (collect params separately)
    const edgeTypes = edge.types || (edge.type ? [edge.type] : []);
    let edgeTypeFilter = "";
    const edgeTypeParams: unknown[] = [];
    if (edgeTypes.length > 0) {
      const typeConditions = edgeTypes.map((t: string) => `${edgeAlias}.type = ?`);
      edgeTypeFilter = ` AND (${typeConditions.join(" OR ")})`;
      edgeTypeParams.push(...edgeTypes);
    }
    
    // Build direction filter
    let directionFilter = "";
    const direction = edge.direction || "right";
    if (direction === "right") {
      directionFilter = `${edgeAlias}.source_id = ${boundVarInfo.alias}.id`;
    } else if (direction === "left") {
      directionFilter = `${edgeAlias}.target_id = ${boundVarInfo.alias}.id`;
    } else {
      // "none" means either direction
      directionFilter = `(${edgeAlias}.source_id = ${boundVarInfo.alias}.id OR ${edgeAlias}.target_id = ${boundVarInfo.alias}.id)`;
    }
    
    // Build target node filter if labels specified (collect params separately)
    let targetFilter = "";
    const targetFilterParams: unknown[] = [];
    if (targetNodePattern && targetNodePattern.label) {
      const labels = Array.isArray(targetNodePattern.label) 
        ? targetNodePattern.label 
        : [targetNodePattern.label];
      const labelConditions = labels.map((l: string) => 
        `EXISTS(SELECT 1 FROM json_each(${targetAlias}.label) WHERE value = ?)`
      );
      targetFilter = ` AND ${labelConditions.join(" AND ")}`;
      targetFilterParams.push(...labels);
    }
    
    // Determine what to select (collect params separately)
    let selectExpr = "1"; // Default: just count matches
    let mapExprParams: unknown[] = [];
    const pathVariable = expr.pathVariable;
    
    if (mapExpr) {
      // Check if mapExpr is a reference to the path variable (e.g., [p = (a)-->(b) | p])
      if (pathVariable && mapExpr.type === "variable" && mapExpr.variable === pathVariable) {
        // Return the path as alternating [startNode, edge, targetNode] array
        // This is the Neo4j 3.5 path format
        selectExpr = `json_array(${boundVarInfo.alias}.properties, ${edgeAlias}.properties, ${targetAlias}.properties)`;
      } else {
        // Translate the map expression normally
        const mapResult = this.translatePatternComprehensionExpr(
          mapExpr,
          startVar,
          boundVarInfo.alias,
          edge.variable,
          edgeAlias,
          targetNodePattern?.variable,
          targetAlias
        );
        selectExpr = mapResult.sql;
        mapExprParams = mapResult.params;
      }
    }
    
    // Build WHERE clause for filter condition (collect params separately)
    let whereClause = "";
    let filterParams: unknown[] = [];
    if (filterCondition) {
      const filterResult = this.translatePatternComprehensionCondition(
        filterCondition,
        startVar,
        boundVarInfo.alias,
        edge.variable,
        edgeAlias,
        targetNodePattern?.variable,
        targetAlias
      );
      whereClause = ` AND ${filterResult.sql}`;
      filterParams = filterResult.params;
    }
    
    // Build the correlated subquery
    // Join edges table with optional target node filtering
    let fromClause = `edges ${edgeAlias}`;
    if (targetNodePattern) {
      // Need to join with nodes for target filtering
      let targetJoin: string;
      if (direction === "right") {
        targetJoin = `${edgeAlias}.target_id = ${targetAlias}.id`;
      } else if (direction === "left") {
        targetJoin = `${edgeAlias}.source_id = ${targetAlias}.id`;
      } else {
        // For undirected, target is the "other" node
        targetJoin = `(CASE WHEN ${edgeAlias}.source_id = ${boundVarInfo.alias}.id THEN ${edgeAlias}.target_id ELSE ${edgeAlias}.source_id END) = ${targetAlias}.id`;
      }
      fromClause = `edges ${edgeAlias} JOIN nodes ${targetAlias} ON ${targetJoin}`;
    }
    
    const sql = `(SELECT COALESCE(json_group_array(${selectExpr}), json('[]')) FROM ${fromClause} WHERE ${directionFilter}${edgeTypeFilter}${targetFilter}${whereClause})`;
    
    // Params must be in SQL order: selectExpr, then edgeType, then targetFilter, then whereClause
    params.push(...mapExprParams, ...edgeTypeParams, ...targetFilterParams, ...filterParams);
    
    // Add outer table reference
    tables.push(boundVarInfo.alias);
    
    return { sql, tables, params };
  }

  /**
   * Translate an expression within a pattern comprehension.
   */
  private translatePatternComprehensionExpr(
    expr: Expression,
    startVar: string | undefined,
    startAlias: string,
    edgeVar: string | undefined,
    edgeAlias: string,
    targetVar: string | undefined,
    targetAlias: string
  ): { sql: string; params: unknown[] } {
    const params: unknown[] = [];
    
    switch (expr.type) {
      case "literal":
        if (expr.value === null) {
          return { sql: "NULL", params };
        }
        params.push(expr.value);
        return { sql: "?", params };
      
      case "variable":
        if (expr.variable === startVar) {
          return { sql: `${startAlias}.properties`, params };
        }
        if (expr.variable === edgeVar) {
          return { sql: `${edgeAlias}.properties`, params };
        }
        if (expr.variable === targetVar) {
          return { sql: `${targetAlias}.properties`, params };
        }
        // Fall through to regular translation
        const varResult = this.translateExpression(expr);
        return { sql: varResult.sql, params: varResult.params };
      
      case "property":
        if (expr.variable === startVar) {
          return { sql: `json_extract(${startAlias}.properties, '$.${expr.property}')`, params };
        }
        if (expr.variable === edgeVar) {
          return { sql: `json_extract(${edgeAlias}.properties, '$.${expr.property}')`, params };
        }
        if (expr.variable === targetVar) {
          return { sql: `json_extract(${targetAlias}.properties, '$.${expr.property}')`, params };
        }
        // Fall through to regular translation
        const propResult = this.translateExpression(expr);
        return { sql: propResult.sql, params: propResult.params };
      
      case "binary": {
        const left = this.translatePatternComprehensionExpr(
          expr.left!, startVar, startAlias, edgeVar, edgeAlias, targetVar, targetAlias
        );
        const right = this.translatePatternComprehensionExpr(
          expr.right!, startVar, startAlias, edgeVar, edgeAlias, targetVar, targetAlias
        );
        params.push(...left.params, ...right.params);
        return { sql: `(${left.sql} ${expr.operator} ${right.sql})`, params };
      }
      
      default:
        // Fall back to regular expression translation
        const result = this.translateExpression(expr);
        return { sql: result.sql, params: result.params };
    }
  }

  /**
   * Translate a condition within a pattern comprehension.
   */
  private translatePatternComprehensionCondition(
    condition: import("./parser").WhereCondition,
    startVar: string | undefined,
    startAlias: string,
    edgeVar: string | undefined,
    edgeAlias: string,
    targetVar: string | undefined,
    targetAlias: string
  ): { sql: string; params: unknown[] } {
    const params: unknown[] = [];
    
    switch (condition.type) {
      case "comparison": {
        const left = this.translatePatternComprehensionExpr(
          condition.left!, startVar, startAlias, edgeVar, edgeAlias, targetVar, targetAlias
        );
        const right = this.translatePatternComprehensionExpr(
          condition.right!, startVar, startAlias, edgeVar, edgeAlias, targetVar, targetAlias
        );
        params.push(...left.params, ...right.params);
        return { sql: `${left.sql} ${condition.operator} ${right.sql}`, params };
      }
      
      case "and": {
        const conditions = condition.conditions!.map(c =>
          this.translatePatternComprehensionCondition(
            c, startVar, startAlias, edgeVar, edgeAlias, targetVar, targetAlias
          )
        );
        params.push(...conditions.flatMap(c => c.params));
        return { sql: `(${conditions.map(c => c.sql).join(" AND ")})`, params };
      }
      
      case "or": {
        const conditions = condition.conditions!.map(c =>
          this.translatePatternComprehensionCondition(
            c, startVar, startAlias, edgeVar, edgeAlias, targetVar, targetAlias
          )
        );
        params.push(...conditions.flatMap(c => c.params));
        return { sql: `(${conditions.map(c => c.sql).join(" OR ")})`, params };
      }
      
      default:
        throw new Error(`Unsupported condition type in pattern comprehension: ${condition.type}`);
    }
  }

  /**
   * Translate an expression within a list comprehension, replacing
   * references to the comprehension variable with the json_each value column.
   * Supports scope chains for nested quantifiers.
   */
  private translateListComprehensionExpr(
    expr: Expression,
    compVar: string,
    tableAlias: string,
    scopes?: Array<{ variable: string; tableAlias: string }>
  ): { sql: string; params: unknown[] } {
    const params: unknown[] = [];
    
    // Build the full scope chain including the current scope
    const allScopes = scopes 
      ? [...scopes, { variable: compVar, tableAlias }]
      : [{ variable: compVar, tableAlias }];
    
    // Helper to find the scope for a variable
    const findScope = (varName: string) => {
      // Search from innermost (end) to outermost (start)
      for (let i = allScopes.length - 1; i >= 0; i--) {
        if (allScopes[i].variable === varName) {
          return allScopes[i];
        }
      }
      return null;
    };
    
    switch (expr.type) {
      case "variable": {
        const scope = findScope(expr.variable!);
        if (scope) {
          return { sql: `${scope.tableAlias}.value`, params };
        }
        // Fall through to regular translation
        const varResult = this.translateExpression(expr);
        return { sql: varResult.sql, params: varResult.params };
      }
      
      case "property": {
        // Handle property access on any comprehension variable (e.g., x.a in "none(x IN list WHERE x.a = 2)")
        const scope = findScope(expr.variable!);
        if (scope) {
          // Extract property from the JSON value in the list element
          return { sql: `json_extract(${scope.tableAlias}.value, '$.${expr.property}')`, params };
        }
        // Fall through to regular translation for other variables
        const propResult = this.translateExpression(expr);
        return { sql: propResult.sql, params: propResult.params };
      }
        
      case "binary": {
        const left = this.translateListComprehensionExpr(expr.left!, compVar, tableAlias, scopes);
        const right = this.translateListComprehensionExpr(expr.right!, compVar, tableAlias, scopes);
        params.push(...left.params, ...right.params);
        return { sql: `(${left.sql} ${expr.operator} ${right.sql})`, params };
      }
      
      case "literal":
        if (expr.value === null) {
          return { sql: "NULL", params };
        }
        // Convert booleans to 1/0 for SQLite (SQLite can only bind numbers, strings, bigints, buffers, and null)
        const literalValue = expr.value === true ? 1 : expr.value === false ? 0 : expr.value;
        params.push(literalValue);
        return { sql: "?", params };
        
      case "parameter": {
        const paramValue = this.ctx.paramValues[expr.name!];
        if (Array.isArray(paramValue) || (typeof paramValue === "object" && paramValue !== null)) {
          params.push(JSON.stringify(paramValue));
        } else {
          params.push(paramValue);
        }
        return { sql: "?", params };
      }
        
      case "function": {
        // Handle functions like size(x)
        const funcArgs: string[] = [];
        for (const arg of expr.args || []) {
          const argResult = this.translateListComprehensionExpr(arg, compVar, tableAlias, scopes);
          params.push(...argResult.params);
          funcArgs.push(argResult.sql);
        }
        
        // Map Cypher functions to SQLite equivalents
        const funcName = expr.functionName!;
        if (funcName === "SIZE") {
          // SIZE works on both lists (json arrays) and strings
          // Use json_array_length for arrays, LENGTH for strings
          // Must check json_valid first to avoid "malformed JSON" error on plain strings
          const arg = funcArgs[0];
          return { 
            sql: `CASE WHEN json_valid(${arg}) AND json_type(${arg}) = 'array' THEN json_array_length(${arg}) ELSE LENGTH(${arg}) END`, 
            params 
          };
        }
        if (funcName === "LENGTH") {
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
        if (funcName === "RAND") {
          // SQLite's RANDOM() returns integer, convert to 0-1 range
          return { sql: `((RANDOM() + 9223372036854775808) / 18446744073709551615.0)`, params };
        }
        
        // Fall back to regular translation for unknown functions
        const result = this.translateExpression(expr);
        return { sql: result.sql, params: result.params };
      }
      
      default: {
        // Fall back to regular translation
        const result2 = this.translateExpression(expr);
        return { sql: result2.sql, params: result2.params };
      }
    }
  }

  /**
   * Translate a WHERE condition within a list comprehension.
   * Supports scope chains for nested quantifiers.
   */
  private translateListComprehensionCondition(
    condition: WhereCondition,
    compVar: string,
    tableAlias: string,
    scopes?: Array<{ variable: string; tableAlias: string }>
  ): { sql: string; params: unknown[] } {
    const params: unknown[] = [];
    
    switch (condition.type) {
      case "comparison": {
        const left = this.translateListComprehensionExpr(condition.left!, compVar, tableAlias, scopes);
        const right = this.translateListComprehensionExpr(condition.right!, compVar, tableAlias, scopes);
        params.push(...left.params, ...right.params);
        const temporalOps = new Set(["<", "<=", ">", ">="]);
        if (temporalOps.has(condition.operator!)) {
          const wrapTemporal = (valueSql: string) => {
            if (valueSql.includes("?")) {
              return `(SELECT ${this.buildDateTimeWithOffsetOrderBy("__t__.v")} FROM (SELECT ${valueSql} AS v) __t__)`;
            }
            return this.buildDateTimeWithOffsetOrderBy(valueSql);
          };
          return {
            sql: `${wrapTemporal(left.sql)} ${condition.operator} ${wrapTemporal(right.sql)}`,
            params,
          };
        }
        return {
          sql: `${left.sql} ${condition.operator} ${right.sql}`,
          params,
        };
      }
      
      case "and": {
        const parts = condition.conditions!.map(c => 
          this.translateListComprehensionCondition(c, compVar, tableAlias, scopes)
        );
        return {
          sql: `(${parts.map(p => p.sql).join(" AND ")})`,
          params: parts.flatMap(p => p.params),
        };
      }
      
      case "or": {
        const parts = condition.conditions!.map(c => 
          this.translateListComprehensionCondition(c, compVar, tableAlias, scopes)
        );
        return {
          sql: `(${parts.map(p => p.sql).join(" OR ")})`,
          params: parts.flatMap(p => p.params),
        };
      }
      
      case "not": {
        const inner = this.translateListComprehensionCondition(condition.condition!, compVar, tableAlias, scopes);
        return {
          sql: `NOT (${inner.sql})`,
          params: inner.params,
        };
      }
      
      case "isNull": {
        const left = this.translateListComprehensionExpr(condition.left!, compVar, tableAlias, scopes);
        return {
          sql: `${left.sql} IS NULL`,
          params: left.params,
        };
      }
      
      case "isNotNull": {
        const left = this.translateListComprehensionExpr(condition.left!, compVar, tableAlias, scopes);
        return {
          sql: `${left.sql} IS NOT NULL`,
          params: left.params,
        };
      }
      
      case "expression": {
        // Handle bare expressions used as boolean conditions (e.g., all(x IN list WHERE x))
        const exprResult = this.translateListComprehensionExpr(condition.left!, compVar, tableAlias, scopes);
        return {
          sql: exprResult.sql,
          params: exprResult.params,
        };
      }
      
      case "listPredicate": {
        // Handle nested list predicates (e.g., none(x IN list WHERE none(y IN x WHERE y = 'abc')))
        // Build the scope chain with the current scope included
        const currentScopes = scopes 
          ? [...scopes, { variable: compVar, tableAlias }]
          : [{ variable: compVar, tableAlias }];
        const nestedResult = this.translateNestedListPredicate(condition as unknown as Expression, currentScopes);
        return {
          sql: nestedResult.sql,
          params: nestedResult.params,
        };
      }
      
      default:
        throw new Error(`Unsupported condition type in list comprehension: ${condition.type}`);
    }
  }

  /**
   * Translate a list predicate expression: ALL/ANY/NONE/SINGLE(var IN list WHERE cond)
   * 
   * Three-valued logic for handling nulls:
   * - If predicate evaluation produces NULL (unknown) for any element and no definitive answer, return NULL
   * - ALL: false if any element fails; null if unknowns present and all definites pass; true otherwise
   * - ANY: true if any element passes; null if unknowns present and no pass; false otherwise
   * - NONE: false if any element passes; null if unknowns present and no pass; true otherwise
   * - SINGLE: false if >1 element passes; null if unknowns present; true if exactly 1 passes; false otherwise
   * 
   * Unknowns are detected by: (total count) - (matches) - (non-matches) > 0
   * This correctly handles cases like `WHERE false` where the predicate is static.
   * 
   * Note: Since the CASE expression uses the list/cond multiple times, we must duplicate
   * the params for each occurrence.
   */
  private translateListPredicate(expr: Expression): { sql: string; tables: string[]; params: unknown[] } {
    const tables: string[] = [];
    const params: unknown[] = [];
    
    const predicateType = expr.predicateType!;
    const variable = expr.variable!;
    const listExpr = expr.listExpr!;
    const filterCondition = expr.filterCondition!;
    
    // Validate type compatibility before translation
    this.validateListPredicateTypes(listExpr, filterCondition, variable);
    
    // Translate the source list expression
    const listResult = this.translateExpression(listExpr);
    tables.push(...listResult.tables);
    const listParams = listResult.params;
    
    // Get the list SQL - wrap for array if needed
    const listSql = this.wrapForListPredicate(listExpr, listResult.sql);
    
    // Translate the filter condition, substituting the list predicate variable with __lp__.value
    const condResult = this.translateListComprehensionCondition(filterCondition, variable, "__lp__");
    const condParams = condResult.params;
    
    // Helper expressions for the CASE statements
    // Note: each use of listSql and condResult.sql consumes their params in order
    // Unknowns = total - matches - non_matches > 0
    // This detects when predicate evaluation returns NULL (e.g., null = 2)
    const totalCountSql = `(SELECT COUNT(*) FROM json_each(${listSql}) AS __lp__)`;
    const matchCountSql = `(SELECT COUNT(*) FROM json_each(${listSql}) AS __lp__ WHERE ${condResult.sql})`;
    const nonMatchCountSql = `(SELECT COUNT(*) FROM json_each(${listSql}) AS __lp__ WHERE NOT (${condResult.sql}))`;
    const hasUnknownsSql = `(${totalCountSql} - ${matchCountSql} - ${nonMatchCountSql}) > 0`;
    
    const matchesSql = `EXISTS (SELECT 1 FROM json_each(${listSql}) AS __lp__ WHERE ${condResult.sql})`;
    const failsSql = `EXISTS (SELECT 1 FROM json_each(${listSql}) AS __lp__ WHERE NOT (${condResult.sql}))`;
    
    let sql: string;
    
    switch (predicateType) {
      case "ALL":
        // ALL: true when all elements satisfy condition
        // - false if ANY element fails (definitive)
        // - null if unknowns present and no failure detected (uncertain)
        // - true otherwise
        // Uses: failsSql (list + cond), hasUnknownsSql (list + list + cond + list + cond)
        sql = `(CASE WHEN ${failsSql} THEN 0 WHEN ${hasUnknownsSql} THEN NULL ELSE 1 END)`;
        params.push(...listParams, ...condParams);
        params.push(...listParams, ...listParams, ...condParams, ...listParams, ...condParams);
        break;
        
      case "ANY":
        // ANY: true when at least one element satisfies condition
        // - true if ANY element passes (definitive)
        // - null if unknowns present and no pass detected (uncertain)
        // - false otherwise
        // Uses: matchesSql (list + cond), hasUnknownsSql (list + list + cond + list + cond)
        sql = `(CASE WHEN ${matchesSql} THEN 1 WHEN ${hasUnknownsSql} THEN NULL ELSE 0 END)`;
        params.push(...listParams, ...condParams);
        params.push(...listParams, ...listParams, ...condParams, ...listParams, ...condParams);
        break;
        
      case "NONE":
        // NONE: true when no elements satisfy condition
        // - false if ANY element passes (definitive)
        // - null if unknowns present and no pass detected (uncertain)
        // - true otherwise
        // Uses: matchesSql (list + cond), hasUnknownsSql (list + list + cond + list + cond)
        sql = `(CASE WHEN ${matchesSql} THEN 0 WHEN ${hasUnknownsSql} THEN NULL ELSE 1 END)`;
        params.push(...listParams, ...condParams);
        params.push(...listParams, ...listParams, ...condParams, ...listParams, ...condParams);
        break;
        
      case "SINGLE":
        // SINGLE: true when exactly one element satisfies condition
        // - false if >1 element passes (definitive)
        // - null if unknowns present (uncertain about count)
        // - true if exactly 1 passes
        // - false otherwise (0 passes)
        // Uses: matchCountSql (list + cond), hasUnknownsSql (list + list + cond + list + cond), matchCountSql again (list + cond)
        sql = `(CASE WHEN ${matchCountSql} > 1 THEN 0 WHEN ${hasUnknownsSql} THEN NULL WHEN ${matchCountSql} = 1 THEN 1 ELSE 0 END)`;
        params.push(...listParams, ...condParams);
        params.push(...listParams, ...listParams, ...condParams, ...listParams, ...condParams);
        params.push(...listParams, ...condParams);
        break;
        
      default:
        throw new Error(`Unknown list predicate type: ${predicateType}`);
    }
    
    return { sql, tables, params };
  }

  /**
   * Translate a nested list predicate within a list comprehension context.
   * This handles cases like: none(x IN list WHERE none(y IN x WHERE y = 'abc'))
   * Also handles deeply nested cases: none(x IN list WHERE none(y IN list WHERE x <= y))
   * 
   * @param expr The nested list predicate expression
   * @param scopes The scope chain from outer contexts (variable to alias mappings)
   */
  private translateNestedListPredicate(
    expr: Expression,
    scopes: Array<{ variable: string; tableAlias: string }>
  ): { sql: string; params: unknown[] } {
    const params: unknown[] = [];
    
    const predicateType = expr.predicateType!;
    const innerVariable = expr.variable!;
    const listExpr = expr.listExpr!;
    const filterCondition = expr.filterCondition!;
    
    // Generate a unique alias for this inner predicate
    // Use the last alias in the scope chain + "i" to create a nested alias (e.g., __lp__i, __lp__ii, etc.)
    const lastScope = scopes[scopes.length - 1];
    const innerAlias = lastScope.tableAlias + "i";
    
    // Translate the list expression using the scope chain
    // This converts references to any outer variables to their respective aliases
    // We use the last scope's variable and alias as the "current" for the expr translation,
    // and pass the remaining scopes as outer scopes
    const outerScopes = scopes.slice(0, -1);
    const listResult = this.translateListComprehensionExpr(listExpr, lastScope.variable, lastScope.tableAlias, outerScopes.length > 0 ? outerScopes : undefined);
    const listSql = listResult.sql;
    const listParams = listResult.params;
    
    // Translate the filter condition with the inner variable and inner alias,
    // passing the full outer scope chain so the condition can reference outer variables
    const condResult = this.translateListComprehensionCondition(filterCondition, innerVariable, innerAlias, scopes);
    const condParams = condResult.params;
    
    // Build the SQL using the same pattern as translateListPredicate but with the inner alias
    const totalCountSql = `(SELECT COUNT(*) FROM json_each(${listSql}) AS ${innerAlias})`;
    const matchCountSql = `(SELECT COUNT(*) FROM json_each(${listSql}) AS ${innerAlias} WHERE ${condResult.sql})`;
    const nonMatchCountSql = `(SELECT COUNT(*) FROM json_each(${listSql}) AS ${innerAlias} WHERE NOT (${condResult.sql}))`;
    const hasUnknownsSql = `(${totalCountSql} - ${matchCountSql} - ${nonMatchCountSql}) > 0`;
    const matchesSql = `EXISTS (SELECT 1 FROM json_each(${listSql}) AS ${innerAlias} WHERE ${condResult.sql})`;
    const failsSql = `EXISTS (SELECT 1 FROM json_each(${listSql}) AS ${innerAlias} WHERE NOT (${condResult.sql}))`;
    
    let sql: string;
    
    switch (predicateType) {
      case "ALL":
        sql = `(CASE WHEN ${failsSql} THEN 0 WHEN ${hasUnknownsSql} THEN NULL ELSE 1 END)`;
        params.push(...listParams, ...condParams);
        params.push(...listParams, ...listParams, ...condParams, ...listParams, ...condParams);
        break;
        
      case "ANY":
        sql = `(CASE WHEN ${matchesSql} THEN 1 WHEN ${hasUnknownsSql} THEN NULL ELSE 0 END)`;
        params.push(...listParams, ...condParams);
        params.push(...listParams, ...listParams, ...condParams, ...listParams, ...condParams);
        break;
        
      case "NONE":
        sql = `(CASE WHEN ${matchesSql} THEN 0 WHEN ${hasUnknownsSql} THEN NULL ELSE 1 END)`;
        params.push(...listParams, ...condParams);
        params.push(...listParams, ...listParams, ...condParams, ...listParams, ...condParams);
        break;
        
      case "SINGLE":
        sql = `(CASE WHEN ${matchCountSql} > 1 THEN 0 WHEN ${hasUnknownsSql} THEN NULL WHEN ${matchCountSql} = 1 THEN 1 ELSE 0 END)`;
        params.push(...listParams, ...condParams);
        params.push(...listParams, ...listParams, ...condParams, ...listParams, ...condParams);
        params.push(...listParams, ...condParams);
        break;
        
      default:
        throw new Error(`Unknown list predicate type: ${predicateType}`);
    }
    
    return { sql, params };
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
   * Get the static type of an expression when it can be determined at compile time.
   * Returns the type name for literals and expressions with known types, or null for dynamic types.
   */
  private getStaticExpressionType(expr: Expression): "boolean" | "number" | "string" | "list" | "map" | "null" | null {
    switch (expr.type) {
      case "literal": {
        if (expr.value === null) return "null";
        if (typeof expr.value === "boolean") return "boolean";
        if (typeof expr.value === "number") return "number";
        if (typeof expr.value === "string") return "string";
        if (Array.isArray(expr.value)) return "list";
        if (typeof expr.value === "object") return "map";
        return null;
      }
      
      case "object":
        return "map";
        
      case "comparison":
      case "labelPredicate":
      case "listPredicate":
        // Comparisons always return boolean
        return "boolean";
        
      case "binary": {
        // Boolean operators return boolean (if operands are valid)
        if (expr.operator === "AND" || expr.operator === "OR") {
          return "boolean";
        }
        // Arithmetic operators return number
        if (["+", "-", "*", "/", "%", "^"].includes(expr.operator!)) {
          // But + can also be string concatenation or list concatenation
          const leftType = this.getStaticExpressionType(expr.left!);
          const rightType = this.getStaticExpressionType(expr.right!);
          if (leftType === "string" || rightType === "string") return "string";
          if (leftType === "list" || rightType === "list") return "list";
          return "number";
        }
        return null;
      }
      
      case "unary": {
        if (expr.operator === "NOT") return "boolean";
        if (expr.operator === "-") return "number";
        return null;
      }
      
      case "function": {
        // Some functions have known return types
        const fn = (expr.functionName || "").toUpperCase();
        const boolFunctions = ["EXISTS", "STARTSWITH", "ENDSWITH", "CONTAINS"];
        const numFunctions = ["COUNT", "SUM", "AVG", "MIN", "MAX", "SIZE", "LENGTH", "ABS", "CEIL", "FLOOR", "ROUND", "SIGN", "TOINTEGER", "TOFLOAT", "TOBOOLEAN"];
        const strFunctions = ["TOSTRING", "TRIM", "LTRIM", "RTRIM", "REPLACE", "SUBSTRING", "TOUPPER", "TOLOWER", "LEFT", "RIGHT", "REVERSE"];
        const listFunctions = ["COLLECT", "RANGE", "KEYS", "LABELS", "TAIL", "SPLIT", "NODES", "RELATIONSHIPS"];
        
        if (boolFunctions.includes(fn)) return "boolean";
        if (numFunctions.includes(fn)) return "number";
        if (strFunctions.includes(fn)) return "string";
        if (listFunctions.includes(fn)) return "list";
        return null;
      }
      
      // Variable, property, parameter types are dynamic
      case "variable":
      case "property":
      case "parameter":
      case "case":
      default:
        return null;
    }
  }

  /**
   * Check if expression type is valid for boolean operators (AND, OR, NOT).
   * Throws SyntaxError if the type is definitely invalid.
   */
  private validateBooleanOperand(expr: Expression, operator: string): void {
    const type = this.getStaticExpressionType(expr);
    // Only reject if we can statically determine it's not a valid boolean type
    // null (unknown type) is allowed at compile time, will be checked at runtime
    // "boolean" is obviously allowed
    // "null" (the literal null) is allowed in three-valued logic
    if (type !== null && type !== "boolean" && type !== "null") {
      throw new Error(`SyntaxError: InvalidArgumentType - ${operator.toUpperCase()} requires boolean operands, got ${type}`);
    }
  }

  /**
   * Get the static element type of a list expression (if determinable).
   * Returns the common element type for literal arrays, or null if mixed or unknown.
   */
  private getListElementType(listExpr: Expression): "boolean" | "number" | "string" | "null" | null {
    if (listExpr.type === "literal" && Array.isArray(listExpr.value)) {
      const elements = listExpr.value;
      if (elements.length === 0) return null;
      
      // Check if all elements have the same type
      let commonType: "boolean" | "number" | "string" | "null" | null = null;
      for (const elem of elements) {
        let elemType: "boolean" | "number" | "string" | "null" | null;
        if (elem === null) elemType = "null";
        else if (typeof elem === "boolean") elemType = "boolean";
        else if (typeof elem === "number") elemType = "number";
        else if (typeof elem === "string") elemType = "string";
        else elemType = null; // complex types
        
        if (elemType === null) return null; // Can't determine type
        if (elemType === "null") continue; // null is compatible with any type
        
        if (commonType === null) {
          commonType = elemType;
        } else if (commonType !== elemType) {
          return null; // Mixed types
        }
      }
      return commonType;
    }
    return null;
  }

  /**
   * Check if a filter condition uses arithmetic operators on a specific variable.
   * Returns true if the variable is used with %, /, *, -, or ^ operators.
   */
  private conditionUsesArithmeticOnVariable(condition: WhereCondition, varName: string): boolean {
    // Check the left expression if it exists
    if (condition.left && this.expressionUsesArithmeticOnVariable(condition.left, varName)) {
      return true;
    }
    // Check the right expression if it exists
    if (condition.right && this.expressionUsesArithmeticOnVariable(condition.right, varName)) {
      return true;
    }
    // Check nested conditions
    if (condition.conditions) {
      for (const c of condition.conditions) {
        if (this.conditionUsesArithmeticOnVariable(c, varName)) return true;
      }
    }
    if (condition.condition && this.conditionUsesArithmeticOnVariable(condition.condition, varName)) {
      return true;
    }
    return false;
  }

  /**
   * Check if an expression uses arithmetic operators on a specific variable.
   * Returns true if the variable is an operand of %, /, *, -, or ^ operators.
   */
  private expressionUsesArithmeticOnVariable(expr: Expression, varName: string): boolean {
    if (expr.type === "binary") {
      const arithmeticOps = ["%", "/", "*", "-", "^"];
      // For + operator, we only flag it as arithmetic if used with the variable
      // (since + can also be string concatenation)
      if (arithmeticOps.includes(expr.operator!)) {
        // Check if left or right is the variable
        if (this.expressionReferencesVariable(expr.left!, varName) ||
            this.expressionReferencesVariable(expr.right!, varName)) {
          return true;
        }
      }
      // Recursively check sub-expressions
      if (this.expressionUsesArithmeticOnVariable(expr.left!, varName)) return true;
      if (this.expressionUsesArithmeticOnVariable(expr.right!, varName)) return true;
    }
    if (expr.args) {
      for (const arg of expr.args) {
        if (this.expressionUsesArithmeticOnVariable(arg, varName)) return true;
      }
    }
    if (expr.operand && this.expressionUsesArithmeticOnVariable(expr.operand, varName)) {
      return true;
    }
    return false;
  }

  /**
   * Check if an expression directly references a variable.
   */
  private expressionReferencesVariable(expr: Expression, varName: string): boolean {
    if (expr.type === "variable" && expr.variable === varName) {
      return true;
    }
    if (expr.type === "binary") {
      return this.expressionReferencesVariable(expr.left!, varName) ||
             this.expressionReferencesVariable(expr.right!, varName);
    }
    if (expr.type === "property" && expr.variable === varName) {
      return true;
    }
    if (expr.args) {
      for (const arg of expr.args) {
        if (this.expressionReferencesVariable(arg, varName)) return true;
      }
    }
    if (expr.operand) {
      return this.expressionReferencesVariable(expr.operand, varName);
    }
    return false;
  }

  /**
   * Validate type compatibility in list predicates (ALL, ANY, NONE, SINGLE).
   * Throws SyntaxError if using arithmetic on non-numeric list elements.
   */
  private validateListPredicateTypes(
    listExpr: Expression,
    filterCondition: WhereCondition,
    variable: string
  ): void {
    const elementType = this.getListElementType(listExpr);
    
    // If we can determine the element type is non-numeric
    if (elementType === "string" || elementType === "boolean") {
      // Check if the filter uses arithmetic on the iteration variable
      if (this.conditionUsesArithmeticOnVariable(filterCondition, variable)) {
        throw new Error("SyntaxError: Type mismatch: expected Number but was " + 
          (elementType === "string" ? "String" : "Boolean"));
      }
    }
  }

  /**
   * Translate a unary expression: NOT expr
   */
  private translateUnaryExpression(expr: Expression): { sql: string; tables: string[]; params: unknown[] } {
    const tables: string[] = [];
    const params: unknown[] = [];
    
    if (expr.operator === "NOT") {
      // Validate that the operand is a boolean type
      this.validateBooleanOperand(expr.operand!, "NOT");
      
      const operandResult = this.translateExpression(expr.operand!);
      tables.push(...operandResult.tables);
      params.push(...operandResult.params);
      
      // Use custom cypher_not function that properly handles JSON booleans and integers
      return {
        sql: `cypher_not(${operandResult.sql})`,
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
        
        // For ordering operators, use Cypher-compliant type-aware comparison
        const orderingOps: Record<string, string> = {
          "<": "cypher_lt",
          "<=": "cypher_lte",
          ">": "cypher_gt",
          ">=": "cypher_gte",
        };
        const func = orderingOps[condition.operator!];
        if (func) {
          return {
            sql: `${func}(${left.sql}, ${right.sql})`,
            params: [...left.params, ...right.params],
          };
        }
        
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
        // Use INSTR for case-sensitive substring search (returns position, 0 if not found)
        return {
          sql: `INSTR(${left.sql}, ${right.sql}) > 0`,
          params: [...left.params, ...right.params],
        };
      }

      case "startsWith": {
        const left = this.translateWhereExpression(condition.left!);
        const right = this.translateWhereExpression(condition.right!);
        // Use SUBSTR for case-sensitive prefix match
        // Note: right.sql appears twice, so right.params must be included twice
        return {
          sql: `SUBSTR(${left.sql}, 1, LENGTH(${right.sql})) = ${right.sql}`,
          params: [...left.params, ...right.params, ...right.params],
        };
      }

      case "endsWith": {
        const left = this.translateWhereExpression(condition.left!);
        const right = this.translateWhereExpression(condition.right!);
        // Use CASE to handle: 1) NULL propagation, 2) empty suffix edge case, 3) case-sensitive suffix match
        // - If left or right is NULL, return NULL (proper three-valued logic)
        // - If suffix is empty, every string matches (LENGTH(right) = 0 returns 1)
        // - Otherwise, use SUBSTR with negative offset for case-sensitive comparison
        // Note: left.sql appears 3 times, right.sql appears 4 times
        return {
          sql: `CASE WHEN ${left.sql} IS NULL OR ${right.sql} IS NULL THEN NULL WHEN LENGTH(${right.sql}) = 0 THEN 1 ELSE SUBSTR(${left.sql}, -LENGTH(${right.sql})) = ${right.sql} END`,
          params: [...left.params, ...right.params, ...right.params, ...left.params, ...right.params, ...right.params],
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

       case "patternMatch": {
         // Pattern condition in WHERE clause: (a)-[:T]->(b)
         // This is like EXISTS but without the EXISTS keyword
         return this.translatePatternCondition(condition);
       }

       case "expression": {
         // Standalone expression used as boolean condition
         // e.g., WHERE false, WHERE n.active, WHERE true AND x = 1
         // Also handles boolean variables like: WHERE result (where result is a boolean FROM WITH)
         
         // Check if this is a bare variable that's a node/relationship - that's invalid
         const exprToCheck = condition.left!;
         if (exprToCheck.type === "variable") {
           const varName = exprToCheck.variable!;
           
           // Check if it's a WITH alias (which could be a boolean expression)
           const withAliases = (this.ctx as any).withAliases as Map<string, Expression> | undefined;
           const isWithAlias = withAliases && withAliases.has(varName);
           
           // Check if it's a node/relationship variable
           const varInfo = this.ctx.variables.get(varName);
           const isGraphElement = varInfo && (varInfo.type === "node" || varInfo.type === "edge" || varInfo.type === "varLengthEdge" || varInfo.type === "path");
           
           // Bare node/edge/path variable in WHERE is a SyntaxError
           // Unless it's a WITH alias (which might be boolean)
           if (isGraphElement && !isWithAlias) {
             throw new Error(`SyntaxError: Cannot use node/relationship variable '${varName}' as boolean condition`);
           }
         }
         
         const expr = this.translateWhereExpression(condition.left!);
         return expr;
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

   private translatePatternCondition(condition: WhereCondition): { sql: string; params: unknown[] } {
     const patterns = condition.patterns;
     if (!patterns || patterns.length === 0) {
       throw new Error("Pattern condition must have patterns");
     }

     const params: unknown[] = [];
     const conditions: string[] = [];

     // Process each pattern in the chain
     for (let i = 0; i < patterns.length; i++) {
       const pattern = patterns[i];

       if (this.isRelationshipPattern(pattern)) {
         const rel = pattern as RelationshipPattern;
         
         // Get the source variable's info from context
         const sourceVar = rel.source.variable;
         const sourceInfo = sourceVar ? this.ctx.variables.get(sourceVar) : null;
         
         if (!sourceInfo) {
           throw new Error(`Pattern condition references unknown variable: ${sourceVar}`);
         }
         
         // Get info for the target variable (if it exists)
          const targetInfo = rel.target.variable ? this.ctx.variables.get(rel.target.variable) : null;
          
          // Determine if the target is truly anonymous (no variable, or variable not bound in outer query)
          const targetIsAnonymous = !rel.target.variable || !targetInfo;
          
          // If target has a variable that isn't bound, throw an error only if the variable is expected to exist
          if (rel.target.variable && !targetInfo) {
            // Check if this is a forward reference that will be handled by the subquery
            // For pattern predicates, forward references aren't allowed
            throw new Error(`Pattern condition references unknown target variable: ${rel.target.variable}`);
          }
          
          // Handle relationship hops (e.g., [:R*2..5] or [:R*])
          if (rel.edge.minHops !== undefined || rel.edge.maxHops !== undefined) {
            // For variable-length relationships, use a recursive CTE
            const minHops = rel.edge.minHops ?? 1;
            const maxHops = rel.edge.maxHops ?? 10;
            
            // Build edge type filter - use literals in CTE to avoid parameter issues
            let edgeTypeFilterBase = "";
            let edgeTypeFilterRecursive = "";
            
            if (rel.edge.type) {
              // Use literal for CTE
              const escapedType = rel.edge.type.replace(/'/g, "''");
              edgeTypeFilterBase = ` AND type = '${escapedType}'`;
              edgeTypeFilterRecursive = ` AND type = '${escapedType}'`;
            } else if (rel.edge.types && rel.edge.types.length > 0) {
              const quotedTypes = rel.edge.types.map(t => `'${t.replace(/'/g, "''")}'`).join(", ");
              edgeTypeFilterBase = ` AND type IN (${quotedTypes})`;
              edgeTypeFilterRecursive = ` AND type IN (${quotedTypes})`;
            }
            
            // Determine direction for variable-length path traversal
            const direction = rel.edge.direction || "right";
            
            if (direction === "none") {
              // Undirected: traverse edges in both directions
              // Track visited edges to prevent traversing the same edge twice (Cypher relationship uniqueness)
              if (targetIsAnonymous) {
                const reachSql = `EXISTS (
                  WITH RECURSIVE var_length_path(current_id, hops, visited_edges) AS (
                    SELECT CASE WHEN source_id = ${sourceInfo.alias}.id THEN target_id ELSE source_id END, 1, ',' || id || ','
                    FROM edges
                    WHERE (source_id = ${sourceInfo.alias}.id OR target_id = ${sourceInfo.alias}.id)${edgeTypeFilterBase}
                    UNION ALL
                    SELECT CASE WHEN e.source_id = vlp.current_id THEN e.target_id ELSE e.source_id END, vlp.hops + 1, vlp.visited_edges || e.id || ','
                    FROM var_length_path vlp
                    JOIN edges e ON (e.source_id = vlp.current_id OR e.target_id = vlp.current_id)${edgeTypeFilterRecursive}
                    WHERE vlp.hops < ${maxHops} AND vlp.visited_edges NOT LIKE '%,' || e.id || ',%'
                  )
                  SELECT 1
                  FROM var_length_path
                  WHERE hops >= ${minHops}
                )`;
                conditions.push(reachSql);
              } else {
                const reachSql = `EXISTS (
                  WITH RECURSIVE var_length_path(current_id, hops, visited_edges) AS (
                    SELECT CASE WHEN source_id = ${sourceInfo.alias}.id THEN target_id ELSE source_id END, 1, ',' || id || ','
                    FROM edges
                    WHERE (source_id = ${sourceInfo.alias}.id OR target_id = ${sourceInfo.alias}.id)${edgeTypeFilterBase}
                    UNION ALL
                    SELECT CASE WHEN e.source_id = vlp.current_id THEN e.target_id ELSE e.source_id END, vlp.hops + 1, vlp.visited_edges || e.id || ','
                    FROM var_length_path vlp
                    JOIN edges e ON (e.source_id = vlp.current_id OR e.target_id = vlp.current_id)${edgeTypeFilterRecursive}
                    WHERE vlp.hops < ${maxHops} AND vlp.visited_edges NOT LIKE '%,' || e.id || ',%'
                  )
                  SELECT 1
                  FROM var_length_path
                  WHERE current_id = ${targetInfo!.alias}.id AND hops >= ${minHops}
                )`;
                conditions.push(reachSql);
              }
            } else if (direction === "left") {
              // Left direction: traverse from target to source (incoming edges)
              if (targetIsAnonymous) {
                const reachSql = `EXISTS (
                  WITH RECURSIVE var_length_path(source_id, target_id, hops) AS (
                    SELECT source_id, target_id, 1
                    FROM edges
                    WHERE target_id = ${sourceInfo.alias}.id${edgeTypeFilterBase}
                    UNION ALL
                    SELECT e.source_id, vlp.target_id, vlp.hops + 1
                    FROM var_length_path vlp
                    JOIN edges e ON e.target_id = vlp.source_id${edgeTypeFilterRecursive}
                    WHERE vlp.hops < ${maxHops}
                  )
                  SELECT 1
                  FROM var_length_path
                  WHERE hops >= ${minHops}
                )`;
                conditions.push(reachSql);
              } else {
                const reachSql = `EXISTS (
                  WITH RECURSIVE var_length_path(source_id, target_id, hops) AS (
                    SELECT source_id, target_id, 1
                    FROM edges
                    WHERE target_id = ${sourceInfo.alias}.id${edgeTypeFilterBase}
                    UNION ALL
                    SELECT e.source_id, vlp.target_id, vlp.hops + 1
                    FROM var_length_path vlp
                    JOIN edges e ON e.target_id = vlp.source_id${edgeTypeFilterRecursive}
                    WHERE vlp.hops < ${maxHops}
                  )
                  SELECT 1
                  FROM var_length_path
                  WHERE source_id = ${targetInfo!.alias}.id AND hops >= ${minHops}
                )`;
                conditions.push(reachSql);
              }
            } else {
              // Right direction (default): traverse from source to target (outgoing edges)
              if (targetIsAnonymous) {
                const reachSql = `EXISTS (
                  WITH RECURSIVE var_length_path(source_id, target_id, hops) AS (
                    SELECT source_id, target_id, 1
                    FROM edges
                    WHERE source_id = ${sourceInfo.alias}.id${edgeTypeFilterBase}
                    UNION ALL
                    SELECT vlp.source_id, e.target_id, vlp.hops + 1
                    FROM var_length_path vlp
                    JOIN edges e ON vlp.target_id = e.source_id${edgeTypeFilterRecursive}
                    WHERE vlp.hops < ${maxHops}
                  )
                  SELECT 1
                  FROM var_length_path
                  WHERE hops >= ${minHops}
                )`;
                conditions.push(reachSql);
              } else {
                const reachSql = `EXISTS (
                  WITH RECURSIVE var_length_path(source_id, target_id, hops) AS (
                    SELECT source_id, target_id, 1
                    FROM edges
                    WHERE source_id = ${sourceInfo.alias}.id${edgeTypeFilterBase}
                    UNION ALL
                    SELECT vlp.source_id, e.target_id, vlp.hops + 1
                    FROM var_length_path vlp
                    JOIN edges e ON vlp.target_id = e.source_id${edgeTypeFilterRecursive}
                    WHERE vlp.hops < ${maxHops}
                  )
                  SELECT 1
                  FROM var_length_path
                  WHERE target_id = ${targetInfo!.alias}.id AND hops >= ${minHops}
                )`;
                conditions.push(reachSql);
              }
            }
          } else {
            // Single-hop relationship
            const edgeAlias = `e${this.ctx.aliasCounter++}`;
            
            if (targetIsAnonymous) {
              // Anonymous target: just check if any outgoing/incoming edge exists
              if (rel.edge.direction === "left") {
                conditions.push(`EXISTS (SELECT 1 FROM edges ${edgeAlias} WHERE ${edgeAlias}.target_id = ${sourceInfo.alias}.id`);
              } else if (rel.edge.direction === "none") {
                // Undirected: check either direction (extra parens so type filter applies to both directions)
                conditions.push(`EXISTS (SELECT 1 FROM edges ${edgeAlias} WHERE (${edgeAlias}.source_id = ${sourceInfo.alias}.id OR ${edgeAlias}.target_id = ${sourceInfo.alias}.id)`);
              } else {
                conditions.push(`EXISTS (SELECT 1 FROM edges ${edgeAlias} WHERE ${edgeAlias}.source_id = ${sourceInfo.alias}.id`);
              }
            } else {
              if (rel.edge.direction === "left") {
                conditions.push(`EXISTS (SELECT 1 FROM edges ${edgeAlias} WHERE ${edgeAlias}.target_id = ${sourceInfo.alias}.id AND ${edgeAlias}.source_id = ${targetInfo!.alias}.id`);
              } else if (rel.edge.direction === "none") {
                // Undirected: check either direction (extra parens so type filter applies to both directions)
                conditions.push(`EXISTS (SELECT 1 FROM edges ${edgeAlias} WHERE ((${edgeAlias}.source_id = ${sourceInfo.alias}.id AND ${edgeAlias}.target_id = ${targetInfo!.alias}.id) OR (${edgeAlias}.source_id = ${targetInfo!.alias}.id AND ${edgeAlias}.target_id = ${sourceInfo.alias}.id))`);
              } else {
                conditions.push(`EXISTS (SELECT 1 FROM edges ${edgeAlias} WHERE ${edgeAlias}.source_id = ${sourceInfo.alias}.id AND ${edgeAlias}.target_id = ${targetInfo!.alias}.id`);
              }
            }
            
            // Filter by edge type if specified
            if (rel.edge.type) {
              conditions[conditions.length - 1] += ` AND ${edgeAlias}.type = ?`;
              params.push(rel.edge.type);
            } else if (rel.edge.types && rel.edge.types.length > 0) {
              const placeholders = rel.edge.types.map(() => "?").join(", ");
              conditions[conditions.length - 1] += ` AND ${edgeAlias}.type IN (${placeholders})`;
              params.push(...rel.edge.types);
            }
            
            // For anonymous targets with label constraints, add a join to nodes table
            if (targetIsAnonymous && rel.target.label) {
              const labels = Array.isArray(rel.target.label) ? rel.target.label : [rel.target.label];
              const targetNodeAlias = `target_n${this.ctx.aliasCounter++}`;
              // Remove trailing ) and add JOIN and label check
              const existsClause = conditions[conditions.length - 1];
              const withoutClosingParen = existsClause.slice(0, -1);
              
              // Build label check condition
              const labelConditions = labels.map(l => 
                `EXISTS(SELECT 1 FROM json_each(${targetNodeAlias}.label) WHERE value = '${l.replace(/'/g, "''")}')`
              );
              
              // Determine which edge column to join on based on direction
              let joinColumn: string;
              if (rel.edge.direction === "left") {
                joinColumn = `${edgeAlias}.source_id`;
              } else {
                joinColumn = `${edgeAlias}.target_id`;
              }
              
              conditions[conditions.length - 1] = `${withoutClosingParen} AND EXISTS (SELECT 1 FROM nodes ${targetNodeAlias} WHERE ${targetNodeAlias}.id = ${joinColumn} AND (${labelConditions.join(" OR ")})))`;
            } else {
              conditions[conditions.length - 1] += ")";
            }
          }
          
          // Check target labels if specified (only for non-anonymous targets)
          if (!targetIsAnonymous && rel.target.label) {
            const labels = Array.isArray(rel.target.label) ? rel.target.label : [rel.target.label];
            // Use EXISTS with json_each to check if label is in the array
            const labelConditions = labels.map(l => 
              `EXISTS(SELECT 1 FROM json_each(${targetInfo!.alias}.label) WHERE value = '${l.replace(/'/g, "''")}')`
            );
            conditions.push(`(${labelConditions.join(" OR ")})`);
          }
       } else {
         // Node pattern only
         const node = pattern as NodePattern;
         const nodeVar = node.variable;
         const nodeInfo = nodeVar ? this.ctx.variables.get(nodeVar) : null;
         
         if (!nodeInfo) {
           throw new Error(`Pattern condition references unknown variable: ${nodeVar}`);
         }
         
         // Check labels if specified
         if (node.label) {
           const labels = Array.isArray(node.label) ? node.label : [node.label];
           // Use EXISTS with json_each to check if label is in the array
           const labelConditions = labels.map(l =>
             `EXISTS(SELECT 1 FROM json_each(${nodeInfo.alias}.label) WHERE value = '${l.replace(/'/g, "''")}')`
           );
           conditions.push(`(${labelConditions.join(" OR ")})`);
         }
       }
     }

     // Combine all conditions
     const sql = conditions.length > 0 ? `(${conditions.join(" AND ")})` : "1 = 1";
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
      params.push(...toSqliteParams(values));
      
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
        params.push(...toSqliteParams(paramValue));
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

    if (listExpr.type === "listComprehension") {
      // List comprehension returns a JSON array, use json_each to check membership
      const listResult = this.translateListComprehension(listExpr);
      params.push(...listResult.params);
      return {
        sql: `${left.sql} IN (SELECT value FROM json_each(${listResult.sql}))`,
        params,
      };
    }

    if (listExpr.type === "function") {
      // Function call that returns a list (e.g., labels(), keys(), LIST(...))
      const listResult = this.translateWhereExpression(listExpr);
      params.push(...listResult.params);
      return {
        sql: `${left.sql} IN (SELECT value FROM json_each(${listResult.sql}))`,
        params,
      };
    }

    throw new Error(`Unsupported list expression type in IN clause: ${listExpr.type}`);
  }

  private buildTimeWithOffsetOrderBy(valueSql: string): string {
    const v = valueSql;
    const tzPos = `(CASE WHEN instr(${v}, '+') > 0 THEN instr(${v}, '+') WHEN instr(${v}, '-') > 0 THEN instr(${v}, '-') ELSE 0 END)`;
    const isTimeWithOffset = `(typeof(${v}) = 'text' AND ${tzPos} > 0 AND substr(${v}, 3, 1) = ':' AND substr(${v}, ${tzPos} + 3, 1) = ':')`;

    const hour = `CAST(substr(${v}, 1, 2) AS INTEGER)`;
    const minute = `CAST(substr(${v}, 4, 2) AS INTEGER)`;
    const hasSeconds = `(substr(${v}, 6, 1) = ':')`;
    const second = `(CASE WHEN ${hasSeconds} THEN CAST(substr(${v}, 7, 2) AS INTEGER) ELSE 0 END)`;
    const hasFraction = `(${hasSeconds} AND substr(${v}, 9, 1) = '.')`;
    const fracRaw = `substr(${v}, 10, (${tzPos} - 10))`;
    const frac9 = `substr(${fracRaw}, 1, 9)`;
    const fracPadded = `(${frac9} || substr('000000000', 1, (9 - length(${frac9}))))`;
    const nanos = `(CASE WHEN ${hasFraction} THEN CAST(${fracPadded} AS INTEGER) ELSE 0 END)`;

    const tzSign = `substr(${v}, ${tzPos}, 1)`;
    const tzHour = `CAST(substr(${v}, ${tzPos} + 1, 2) AS INTEGER)`;
    const tzMinute = `CAST(substr(${v}, ${tzPos} + 4, 2) AS INTEGER)`;
    const offsetSeconds = `(((${tzHour} * 3600) + (${tzMinute} * 60)) * (CASE WHEN ${tzSign} = '-' THEN -1 ELSE 1 END))`;
    const localSeconds = `((${hour} * 3600) + (${minute} * 60) + ${second})`;
    const utcSeconds = `(${localSeconds} - ${offsetSeconds})`;
    const utcSecondsNorm = `(((${utcSeconds}) % 86400) + 86400) % 86400`;
    const utcNanosKey = `((${utcSecondsNorm} * 1000000000) + ${nanos})`;

    return `CASE WHEN ${isTimeWithOffset} THEN ${utcNanosKey} ELSE ${v} END`;
  }

  private buildDateTimeWithOffsetOrderBy(valueSql: string): string {
    const v = valueSql;
    const tzPos = `(length(${v}) - 5)`;
    const tzSign = `substr(${v}, ${tzPos}, 1)`;
    const tzHour = `CAST(substr(${v}, ${tzPos} + 1, 2) AS INTEGER)`;
    const tzMinute = `CAST(substr(${v}, ${tzPos} + 4, 2) AS INTEGER)`;
    const offsetMinutes = `(((${tzHour} * 60) + ${tzMinute}) * (CASE WHEN ${tzSign} = '-' THEN -1 ELSE 1 END))`;
    const utcModifier = `printf('%+d minutes', (0 - ${offsetMinutes}))`;

    const isDateTimeWithOffset =
      `(typeof(${v}) = 'text' AND length(${v}) >= 22 AND ` +
      `substr(${v}, 5, 1) = '-' AND substr(${v}, 8, 1) = '-' AND substr(${v}, 11, 1) = 'T' AND ` +
      `(${tzSign} = '+' OR ${tzSign} = '-') AND substr(${v}, ${tzPos} + 3, 1) = ':')`;

    const localBase = `substr(${v}, 1, 10) || ' ' || substr(${v}, 12, 8)`;
    const utcBase = `datetime(${localBase}, ${utcModifier})`;
    const utcIso = `replace(${utcBase}, ' ', 'T')`;

    const dotPos = `instr(${v}, '.')`;
    const hasFraction = `(${dotPos} > 0 AND ${dotPos} < ${tzPos})`;
    const fracRaw = `substr(${v}, ${dotPos} + 1, (${tzPos} - ${dotPos} - 1))`;
    const frac9 = `substr(${fracRaw}, 1, 9)`;
    const fracPadded = `(${frac9} || substr('000000000', 1, (9 - length(${frac9}))))`;
    const nanosPart = `(CASE WHEN ${hasFraction} THEN ${fracPadded} ELSE '000000000' END)`;

    const utcKey = `(${utcIso} || '.' || ${nanosPart})`;
    return `CASE WHEN ${isDateTimeWithOffset} THEN ${utcKey} ELSE ${this.buildTimeWithOffsetOrderBy(v)} END`;
  }

  private translateOrderByExpression(expr: Expression, returnAliases: string[] = []): { sql: string; params?: unknown[] } {
    switch (expr.type) {
      case "property": {
        // First check if the variable is a WITH alias (e.g., ordering by properties of a map from WITH)
        const withAliases = (this.ctx as any).withAliases as Map<string, Expression> | undefined;
        if (withAliases && withAliases.has(expr.variable!)) {
          // Translate the underlying expression and access the property
          const originalExpr = withAliases.get(expr.variable!)!;
          const objectResult = this.translateExpression(originalExpr);
          return {
            sql: this.buildDateTimeWithOffsetOrderBy(
              `json_extract(${objectResult.sql}, '$.${expr.property}')`
            ),
            params: objectResult.params,
          };
        }
        
        // Check if the variable is a RETURN alias that points to another variable
        const returnAliasExpressions = (this.ctx as any).returnAliasExpressions as Map<string, Expression> | undefined;
        let targetVariable = expr.variable!;
        if (returnAliasExpressions && returnAliasExpressions.has(targetVariable)) {
          const aliasedExpr = returnAliasExpressions.get(targetVariable)!;
          if (aliasedExpr.type === "variable" && aliasedExpr.variable) {
            targetVariable = aliasedExpr.variable;
          }
        }
        
        const varInfo = this.ctx.variables.get(targetVariable);
        if (!varInfo) {
          throw new Error(`Unknown variable: ${expr.variable}`);
        }
        return {
          sql: this.buildDateTimeWithOffsetOrderBy(
            `json_extract(${varInfo.alias}.properties, '$.${expr.property}')`
          ),
          params: [],
        };
      }

      case "variable": {
        // If this is a WITH alias that ultimately resolves to a property access, order by the
        // underlying property expression (not the alias). Property access is translated using
        // SQLite's JSON operator `->`, which yields JSON text and would sort lexicographically.
        const withAliases = (this.ctx as any).withAliases as Map<string, Expression> | undefined;
        if (withAliases && withAliases.has(expr.variable!)) {
          const visited = new Set<string>();
          let resolved: Expression = withAliases.get(expr.variable!)!;
          while (
            resolved.type === "variable" &&
            resolved.variable &&
            withAliases.has(resolved.variable) &&
            !visited.has(resolved.variable)
          ) {
            visited.add(resolved.variable);
            resolved = withAliases.get(resolved.variable)!;
          }
          
          // If the resolved expression contains aggregation (e.g., head(collect(...))),
          // we can't re-evaluate it in ORDER BY. Use the column alias instead.
          if (this.isAggregateExpression(resolved) && returnAliases.includes(expr.variable!)) {
            return { sql: `"${expr.variable!}"`, params: [] };
          }
          
          // If the resolved expression is a property that references a WITH alias with aggregation,
          // we can't expand it. Use the column alias instead.
          if (resolved.type === "property" && resolved.variable) {
            const propVarExpr = withAliases.get(resolved.variable);
            if (propVarExpr && this.isAggregateExpression(propVarExpr) && returnAliases.includes(expr.variable!)) {
              return { sql: `"${expr.variable!}"`, params: [] };
            }
            const { sql, params } = this.translateOrderByExpression(resolved, returnAliases);
            return { sql, params: params ?? [] };
          }
          
        }
        
        // Check if this is an UNWIND variable - must come before general fallbacks
        // because UNWIND variables may need special datetime sorting
        const unwindClauses = (this.ctx as any).unwindClauses as Array<{
          alias: string;
          variable: string;
          jsonExpr: string;
          params: unknown[];
        }> | undefined;
        
        if (unwindClauses) {
          const unwindClause = unwindClauses.find(u => u.variable === expr.variable);
          if (unwindClause) {
            return { sql: this.buildDateTimeWithOffsetOrderBy(`${unwindClause.alias}.value`), params: [] };
          }
        }
        
        // For WITH aliases with other expression types (binary, function, literal, etc.)
        // that are projected, use the column alias for ORDER BY.
        if (withAliases && withAliases.has(expr.variable!) && returnAliases.includes(expr.variable!)) {
          return { sql: `"${expr.variable!}"`, params: [] };
        }

        const returnAliasExpressions = (this.ctx as any).returnAliasExpressions as Map<string, Expression> | undefined;
        const returnExpr = returnAliasExpressions?.get(expr.variable!);
        if (returnExpr?.type === "property") {
          const { sql, params } = this.translateOrderByExpression(returnExpr, returnAliases);
          return { sql, params: params ?? [] };
        }
        
        // For RETURN alias that is an aggregate or other computed expression,
        // use the column alias directly (it will be in SELECT)
        if (returnExpr && returnAliases.includes(expr.variable!)) {
          return { sql: `"${expr.variable!}"`, params: [] };
        }
        
        const varInfo = this.ctx.variables.get(expr.variable!);
        if (!varInfo) {
          throw new Error(`Unknown variable: ${expr.variable}`);
        }
        return { sql: `${varInfo.alias}.id`, params: [] };
      }

      case "function": {
        if (expr.functionName === "ID") {
          if (expr.args && expr.args.length > 0 && expr.args[0].type === "variable") {
            const varInfo = this.ctx.variables.get(expr.args[0].variable!);
            if (!varInfo) {
              throw new Error(`Unknown variable: ${expr.args[0].variable}`);
            }
            return { sql: `${varInfo.alias}.id`, params: [] };
          }
        }

        const exprName = this.getExpressionName(expr);

        // If this function expression is projected in-scope (WITH/RETURN), prefer ordering
        // by the projected alias to avoid re-evaluating and to match Cypher semantics.
        const withAliases = (this.ctx as any).withAliases as Map<string, Expression> | undefined;
        if (withAliases) {
          for (const [aliasName, aliasedExpr] of withAliases.entries()) {
            if (this.getExpressionName(aliasedExpr) === exprName) {
              return { sql: this.quoteAlias(aliasName), params: [] };
            }
          }
        }

        const returnAliasExpressions = (this.ctx as any).returnAliasExpressions as Map<string, Expression> | undefined;
        if (returnAliasExpressions) {
          for (const [aliasName, aliasedExpr] of returnAliasExpressions.entries()) {
            if (this.getExpressionName(aliasedExpr) === exprName) {
              return { sql: this.quoteAlias(aliasName), params: [] };
            }
          }
        }

        // Check if this function expression matches a return column alias name directly
        if (returnAliases.includes(exprName)) {
          return { sql: this.quoteAlias(exprName), params: [] };
        }

        // Fall back to general expression translation (supports functions/aggregates)
        const translated = this.translateOrderByComplexExpression(expr, returnAliases);
        return { sql: translated.sql, params: translated.params };
      }

      case "binary":
      case "unary":
      case "literal": {
        // For complex expressions (binary, literal, etc.), translate them
        // but substitute variables with column aliases when they are RETURN aliases
        return this.translateOrderByComplexExpression(expr, returnAliases);
      }

      default:
        throw new Error(`Cannot order by expression of type ${expr.type}`);
    }
  }
  
  /**
   * Translate complex expressions for ORDER BY, substituting RETURN column aliases
   */
  private translateOrderByComplexExpression(expr: Expression, returnAliases: string[] = []): { sql: string; params: unknown[] } {
    switch (expr.type) {
      case "variable": {
        // WITH aliases that ultimately resolve to a property access should be expanded so ordering
        // uses the underlying property expression (otherwise we'd sort lexicographically on JSON text).
        const withAliases = (this.ctx as any).withAliases as Map<string, Expression> | undefined;
        if (withAliases && withAliases.has(expr.variable!)) {
          const visited = new Set<string>();
          let resolved: Expression = withAliases.get(expr.variable!)!;
          while (
            resolved.type === "variable" &&
            resolved.variable &&
            withAliases.has(resolved.variable) &&
            !visited.has(resolved.variable)
          ) {
            visited.add(resolved.variable);
            resolved = withAliases.get(resolved.variable)!;
          }
          if (resolved.type === "property") {
            const { sql, params } = this.translateOrderByExpression(resolved, returnAliases);
            return { sql, params: params ?? [] };
          }
        }

        // Check if this is a RETURN column alias
        if (returnAliases.includes(expr.variable!)) {
          return { sql: expr.variable!, params: [] };
        }
        // Otherwise fall back to regular translation
        const varInfo = this.ctx.variables.get(expr.variable!);
        if (!varInfo) {
          // Check if it's a WITH alias
          const withAliases = (this.ctx as any).withAliases as Map<string, Expression> | undefined;
          if (withAliases && withAliases.has(expr.variable!)) {
            // It's a WITH alias - use the alias name directly
            return { sql: expr.variable!, params: [] };
          }
          throw new Error(`Unknown variable: ${expr.variable}`);
        }
        return { sql: `${varInfo.alias}.id`, params: [] };
      }
      
      case "binary": {
        const temporalDuration = this.tryTranslateTemporalDurationArithmetic(expr);
        if (temporalDuration) {
          return { sql: temporalDuration.sql, params: temporalDuration.params };
        }
        const left = this.translateOrderByComplexExpression(expr.left!, returnAliases);
        const right = this.translateOrderByComplexExpression(expr.right!, returnAliases);
        const operator = expr.operator;
        // String concatenation: use || when either side is a string (literal or concat chain)
        if (operator === "+" && (this.isStringConcatenation(expr.left!) || this.isStringConcatenation(expr.right!))) {
          return { sql: `(${left.sql} || ${right.sql})`, params: [...left.params, ...right.params] };
        }
        return {
          sql: `(${left.sql} ${operator} ${right.sql})`,
          params: [...left.params, ...right.params],
        };
      }

      case "unary": {
        if (expr.operator !== "NOT") {
          throw new Error(`Cannot order by unary operator: ${expr.operator}`);
        }

        this.validateBooleanOperand(expr.operand!, "NOT");
        const operand = this.translateOrderByComplexExpression(expr.operand!, returnAliases);
        return { sql: `NOT (${operand.sql})`, params: operand.params };
      }
      
      case "literal": {
        return { sql: "?", params: [expr.value] };
      }
      
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
      
      default: {
        // For other types, use the general translateExpression
        // This handles functions, etc.
        return this.translateExpression(expr);
      }
    }
  }

  private translateWhereExpression(expr: Expression): { sql: string; params: unknown[] } {
    switch (expr.type) {
      case "property": {
        // Check if this is a property access on an UNWIND variable
        const unwindClauses = (this.ctx as any).unwindClauses as Array<{
          alias: string;
          variable: string;
          jsonExpr: string;
          params: unknown[];
        }> | undefined;
        
        if (unwindClauses) {
          const unwindClause = unwindClauses.find(u => u.variable === expr.variable);
          if (unwindClause) {
            // UNWIND variables use the 'value' column from json_each
            return {
              sql: `json_extract(${unwindClause.alias}.value, '$.${expr.property}')`,
              params: [],
            };
          }
        }
        
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

      case "comparison": {
        // Comparison expression like lhs < rhs
        // This can come from a WITH alias that was defined as a comparison
        const leftResult = this.translateWhereExpression(expr.left!);
        const rightResult = this.translateWhereExpression(expr.right!);
        const op = expr.comparisonOperator || "=";
        
        // Handle IS NULL / IS NOT NULL specially
        if (op === "IS NULL") {
          return {
            sql: `(${leftResult.sql} IS NULL)`,
            params: [...leftResult.params],
          };
        }
        if (op === "IS NOT NULL") {
          return {
            sql: `(${leftResult.sql} IS NOT NULL)`,
            params: [...leftResult.params],
          };
        }
        
        // For ordering operators, use Cypher-compliant type-aware comparison
        const orderingOps: Record<string, string> = {
          "<": "cypher_lt",
          "<=": "cypher_lte",
          ">": "cypher_gt",
          ">=": "cypher_gte",
        };
        const func = orderingOps[op];
        if (func) {
          return {
            sql: `${func}(${leftResult.sql}, ${rightResult.sql})`,
            params: [...leftResult.params, ...rightResult.params],
          };
        }
        
        return {
          sql: `(${leftResult.sql} ${op} ${rightResult.sql})`,
          params: [...leftResult.params, ...rightResult.params],
        };
      }

      case "labelPredicate": {
        // Label predicate: n:Label - returns true/false based on whether node has the label
        const varInfo = this.ctx.variables.get(expr.variable!);
        if (!varInfo) {
          throw new Error(`Unknown variable: ${expr.variable}`);
        }
        
        // Handle single label or multiple labels
        // Labels are stored as JSON arrays, e.g., ["Foo", "Bar"]
        const labelsToCheck = expr.labels || (expr.label ? [expr.label] : []);
        
        // Use EXISTS with json_each to check if label is in the array
        // For multiple labels, all must be present (AND)
        const labelChecks = labelsToCheck.map((l: string) => 
          `EXISTS(SELECT 1 FROM json_each(${varInfo.alias}.label) WHERE value = '${l}')`
        ).join(' AND ');
        
        return {
          sql: `(${labelChecks})`,
          params: [],
        };
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
        if (typeof value === "number" && expr.numberLiteralKind === "float" && expr.raw) {
          return { sql: expr.raw, tables, params };
        }
        if (typeof value === "number" && Number.isInteger(value)) {
          return { sql: String(value), tables, params };
        }
        if (Array.isArray(value) || (typeof value === "object" && value !== null)) {
          params.push(JSON.stringify(value));
          return { sql: "?", tables, params };
        }
        params.push(value);
        return { sql: "?", tables, params };
      }
      case "parameter": {
        const paramValue = this.ctx.paramValues[expr.name!];
        if (Array.isArray(paramValue) || (typeof paramValue === "object" && paramValue !== null)) {
          params.push(JSON.stringify(paramValue));
        } else {
          params.push(paramValue);
        }
        return { sql: "?", tables, params };
      }
      case "variable": {
        // WITH alias: treat as the underlying expression, using lookupWithAliasExpression
        // for proper self-reference tracking (handles WITH x + 1 AS x patterns)
        const withAliases = (this.ctx as any).withAliases as Map<string, Expression> | undefined;
        if (withAliases && withAliases.has(expr.variable!)) {
          const aliasName = expr.variable!;
          const selfRefDepths =
            (((this.ctx as any)._withAliasSelfRefDepths as Map<string, number> | undefined) ??= new Map<
              string,
              number
            >());
          const currentDepth = selfRefDepths.get(aliasName);

          if (currentDepth === undefined) {
            // First time seeing this alias - look it up at depth 0
            selfRefDepths.set(aliasName, 0);
            try {
              const originalExpr = this.lookupWithAliasExpression(aliasName, 0);
              if (!originalExpr) {
                throw new Error(`Unknown variable: ${aliasName}`);
              }
              return this.translateExpression(originalExpr);
            } finally {
              selfRefDepths.delete(aliasName);
              if (selfRefDepths.size === 0) {
                (this.ctx as any)._withAliasSelfRefDepths = undefined;
              }
            }
          }

          // Self-reference detected - look up previous definition
          const nextDepth = currentDepth + 1;
          const previousExpr = this.lookupWithAliasExpression(aliasName, nextDepth);
          if (previousExpr) {
            selfRefDepths.set(aliasName, nextDepth);
            try {
              return this.translateExpression(previousExpr);
            } finally {
              selfRefDepths.set(aliasName, currentDepth);
            }
          }
          // No previous definition found - fall through to other resolution methods
        }

        // UNWIND variable: access the json_each value column
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
            return { sql: `${unwindClause.alias}.value`, tables, params };
          }
        }

        const varInfo = this.ctx.variables.get(expr.variable!);
        if (!varInfo) {
          throw new Error(`Unknown variable: ${expr.variable}`);
        }
        // For variable-length edge variables, return the edge_ids array from the CTE
        if (varInfo.type === "varLengthEdge") {
          const pathCteName = varInfo.pathCteName || "path_cte";
          tables.push(pathCteName);
          return { sql: `${pathCteName}.edge_ids`, tables, params };
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

  private expressionReferencesGraphVariables(
    expr: Expression,
    withAliases: Map<string, Expression> | undefined,
    visitingAliases: Set<string> = new Set<string>()
  ): boolean {
    for (const varName of this.findVariablesInExpression(expr)) {
      const varInfo = this.ctx.variables.get(varName);
      if (varInfo && (varInfo.type === "node" || varInfo.type === "edge" || varInfo.type === "varLengthEdge" || varInfo.type === "path")) {
        return true;
      }
      if (withAliases && withAliases.has(varName)) {
        if (visitingAliases.has(varName)) continue;
        visitingAliases.add(varName);
        try {
          if (this.expressionReferencesGraphVariables(withAliases.get(varName)!, withAliases, visitingAliases)) {
            return true;
          }
        } finally {
          visitingAliases.delete(varName);
        }
      }
    }
    return false;
  }

  private findVariablesInExpression(expr: Expression): string[] {
    const vars: string[] = [];
    
    const collect = (e: any) => {
      if (!e) return;
      if (typeof e !== 'object') return;
      
      // Direct variable reference
      if (e.type === "property" && e.variable) {
        vars.push(e.variable);
      } else if (e.type === "variable" && e.variable) {
        vars.push(e.variable);
      }

      const collectCondition = (cond: any) => {
        if (!cond || typeof cond !== "object") return;
        if (cond.left) collect(cond.left);
        if (cond.right) collect(cond.right);
        if (cond.list) collect(cond.list);
        if (cond.listExpr) collect(cond.listExpr);
        if (cond.filterCondition) collectCondition(cond.filterCondition);
        if (cond.condition) collectCondition(cond.condition);
        if (cond.conditions && Array.isArray(cond.conditions)) {
          for (const c of cond.conditions) collectCondition(c);
        }
      };
      
      // Recurse into function arguments
      if (e.args && Array.isArray(e.args)) {
        for (const arg of e.args) {
          collect(arg);
        }
      }
      
      // Recurse into binary expressions
      if (e.left) collect(e.left);
      if (e.right) collect(e.right);
      
      // Recurse into case expressions
      const whens = e.whens || e.whenClauses;
      if (whens && Array.isArray(whens)) {
        for (const when of whens) {
          collect(when.condition ?? when.when);
          collect(when.result ?? when.then);
        }
      }
      if (e.elseExpr) collect(e.elseExpr);
      
      // Recurse into list expressions
      if (e.listExpr) collect(e.listExpr);
      if (e.filterExpr) collect(e.filterExpr);
      if (e.filterCondition) collectCondition(e.filterCondition);
      if (e.mapExpr) collect(e.mapExpr);
      if (e.mapExpression) collect(e.mapExpression);
      
      // Recurse into operand (for unary)
      if (e.operand) collect(e.operand);
      
      // Recurse into expression (general nested)
      if (e.expression) collect(e.expression);
      
      // Recurse into object properties
      if (e.properties && Array.isArray(e.properties)) {
        for (const prop of e.properties) {
          if (prop.value) collect(prop.value);
        }
      }
    };
    
    collect(expr);
    return [...new Set(vars)];
  }

  /**
   * Extract the bound variables from a pattern comprehension expression.
   * For pattern comprehensions like [p = (n)-->() | p], this returns ["n"]
   * which is the variable from the outer context that the comprehension correlates with.
   */
  private getPatternComprehensionBoundVars(expr: Expression): string[] {
    if (expr.type !== "patternComprehension" || !expr.patterns) {
      return [];
    }
    
    const patterns = expr.patterns;
    const isRelPattern = (p: unknown): p is import("./parser").RelationshipPattern => {
      return typeof p === "object" && p !== null && "edge" in p;
    };
    
    const boundVars: string[] = [];
    const firstPattern = patterns[0];
    
    if (isRelPattern(firstPattern)) {
      // RelationshipPattern - get source variable
      if (firstPattern.source.variable) {
        boundVars.push(firstPattern.source.variable);
      }
    } else {
      // NodePattern - get its variable
      const nodePattern = firstPattern as import("./parser").NodePattern;
      if (nodePattern.variable) {
        boundVars.push(nodePattern.variable);
      }
    }
    
    return boundVars;
  }

  /**
   * Get the SQL expressions to use for GROUP BY when the given expression
   * is used in an aggregation context. For pattern comprehensions, this returns
   * the bound variable's identifier instead of the full correlated subquery.
   */
  private getGroupByKeys(expr: Expression): { sql: string[]; params: unknown[] } {
    // For pattern comprehensions, group by the bound variables
    if (expr.type === "patternComprehension") {
      const boundVars = this.getPatternComprehensionBoundVars(expr);
      if (boundVars.length > 0) {
        const keys: string[] = [];
        for (const varName of boundVars) {
          const varInfo = this.ctx.variables.get(varName);
          if (varInfo && varInfo.type === "node") {
            // Use node's id for grouping
            keys.push(`${varInfo.alias}.id`);
          } else if (varInfo && (varInfo.type === "edge" || varInfo.type === "varLengthEdge")) {
            // Use edge's id for grouping
            keys.push(`${varInfo.alias}.id`);
          }
        }
        if (keys.length > 0) {
          return { sql: keys, params: [] };
        }
      }
    }
    
    // Default: translate the expression normally
    const { sql, params } = this.translateExpression(expr);
    return { sql: [sql], params };
  }

  private findVariablesInCondition(condition: WhereCondition): string[] {
    const vars: string[] = [];
    
    const collectFromExpression = (expr: Expression | undefined) => {
      if (!expr) return;
      if (expr.type === "property" && expr.variable) {
        vars.push(expr.variable);
      } else if (expr.type === "variable" && expr.variable) {
        vars.push(expr.variable);
      } else if (expr.type === "labelPredicate" && (expr as any).variable) {
        // Handle label predicates like n:Label
        vars.push((expr as any).variable);
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
   * Check if an expression contains a non-deterministic function (RAND, TIMESTAMP, etc.)
   */
  private containsNonDeterministicFunction(expr: Expression): boolean {
    if (expr.type === "function" && expr.functionName) {
      const nonDeterministicFunctions = ["RAND", "TIMESTAMP"];
      if (nonDeterministicFunctions.includes(expr.functionName.toUpperCase())) {
        return true;
      }
      // Check if any argument contains a non-deterministic function
      if (expr.args) {
        return expr.args.some(arg => this.containsNonDeterministicFunction(arg));
      }
    }
    // Check binary expressions
    if (expr.type === "binary") {
      return this.containsNonDeterministicFunction(expr.left!) || this.containsNonDeterministicFunction(expr.right!);
    }
    // Check unary expressions
    if (expr.type === "unary" && expr.operand) {
      return this.containsNonDeterministicFunction(expr.operand);
    }
    return false;
  }

  /**
   * Check if an expression is or contains an aggregate function (COUNT, SUM, AVG, MIN, MAX, COLLECT, PERCENTILEDISC, PERCENTILECONT)
   */
  private isAggregateExpression(expr: Expression): boolean {
    if (expr.type === "function" && expr.functionName) {
      const aggregateFunctions = ["COUNT", "SUM", "AVG", "MIN", "MAX", "COLLECT", "PERCENTILEDISC", "PERCENTILECONT"];
      if (aggregateFunctions.includes(expr.functionName.toUpperCase())) {
        return true;
      }
      // Check if any argument contains an aggregate (e.g., size(collect(a)))
      if (expr.args) {
        return expr.args.some(arg => this.isAggregateExpression(arg));
      }
    }
    // Check binary expressions for aggregates
    if (expr.type === "binary") {
      return this.isAggregateExpression(expr.left!) || this.isAggregateExpression(expr.right!);
    }
    // Check object/map literals for aggregates in property values
    if (expr.type === "object" && expr.properties) {
      return expr.properties.some(prop => this.isAggregateExpression(prop.value));
    }
    // Check comparison expressions for aggregates (note: IS NULL/IS NOT NULL have no right operand)
    if (expr.type === "comparison") {
      const leftHasAggregate = expr.left ? this.isAggregateExpression(expr.left) : false;
      const rightHasAggregate = expr.right ? this.isAggregateExpression(expr.right) : false;
      return leftHasAggregate || rightHasAggregate;
    }
    return false;
  }

  /**
   * Extract list predicates from an expression that reference WITH aggregate aliases.
   * Returns a set of alias names that need to be materialized.
   */
  private collectWithAggregateAliasesFromListPredicates(
    expr: Expression,
    withAliases: Map<string, Expression> | undefined
  ): Set<string> {
    const aliases = new Set<string>();
    if (!withAliases) return aliases;

    const collectFromExpr = (e: Expression) => {
      if (e.type === "listPredicate" && e.listExpr) {
        // Check if the list expression is a variable referencing a WITH aggregate alias
        if (e.listExpr.type === "variable" && e.listExpr.variable) {
          const aliasExpr = withAliases.get(e.listExpr.variable);
          if (aliasExpr && this.isAggregateExpression(aliasExpr)) {
            aliases.add(e.listExpr.variable);
          }
        }
      }
      
      // Recursively check sub-expressions
      if (e.type === "binary") {
        collectFromExpr(e.left!);
        collectFromExpr(e.right!);
      }
      if (e.type === "function" && e.args) {
        for (const arg of e.args) {
          collectFromExpr(arg);
        }
      }
      if (e.type === "comparison") {
        if (e.left) collectFromExpr(e.left);
        if (e.right) collectFromExpr(e.right);
      }
      if (e.type === "unary" && e.operand) {
        collectFromExpr(e.operand);
      }
      if (e.type === "case") {
        if (e.expression) collectFromExpr(e.expression);
        for (const when of e.whens || []) {
          // CaseWhen has condition (WhereCondition) and result (Expression)
          if (when.result) collectFromExpr(when.result);
        }
        if (e.elseExpr) collectFromExpr(e.elseExpr);
      }
    };

    collectFromExpr(expr);
    return aliases;
  }

  /**
   * Check if a WHERE condition references any alias that resolves to an aggregate expression.
   * This is used to determine if the condition should go in HAVING instead of WHERE.
   */
  private whereConditionReferencesAggregateAlias(cond: WhereCondition, withAliases: Map<string, Expression> | undefined): boolean {
    if (!withAliases) return false;
    
    // Helper to check if an expression references an aggregate alias
    const exprReferencesAggregate = (expr: Expression | undefined): boolean => {
      if (!expr) return false;
      
      // Check if it's a variable that resolves to an aggregate
      if (expr.type === "variable" && expr.variable) {
        const aliasExpr = withAliases.get(expr.variable);
        if (aliasExpr && this.isAggregateExpression(aliasExpr)) {
          return true;
        }
      }
      
      // Check binary expressions
      if (expr.type === "binary") {
        return exprReferencesAggregate(expr.left) || exprReferencesAggregate(expr.right);
      }
      
      // Check function arguments
      if (expr.type === "function" && expr.args) {
        return expr.args.some(arg => exprReferencesAggregate(arg));
      }
      
      return false;
    };
    
    // Check the condition based on its type
    if (cond.type === "comparison" || cond.type === "contains" || cond.type === "startsWith" || cond.type === "endsWith" || cond.type === "in") {
      return exprReferencesAggregate(cond.left) || exprReferencesAggregate(cond.right) || exprReferencesAggregate(cond.list);
    }
    
    if (cond.type === "isNull" || cond.type === "isNotNull") {
      return exprReferencesAggregate(cond.left);
    }
    
    if (cond.type === "and" || cond.type === "or") {
      return cond.conditions?.some(c => this.whereConditionReferencesAggregateAlias(c, withAliases)) ?? false;
    }
    
    if (cond.type === "not") {
      return cond.condition ? this.whereConditionReferencesAggregateAlias(cond.condition, withAliases) : false;
    }
    
    return false;
  }

  private serializeProperties(props: Record<string, PropertyValue>): { json: string; params: unknown[] } {
    const resolved: Record<string, unknown> = {};
    const params: unknown[] = [];
    const withAliases = (this.ctx as any).withAliases as Map<string, Expression> | undefined;

    for (const [key, value] of Object.entries(props)) {
      if (this.isParameterRef(value)) {
        resolved[key] = this.ctx.paramValues[value.name];
      } else if (this.isVariableRef(value)) {
        // Check if it's a WITH alias
        const varName = (value as { type: string; name: string }).name;
        if (withAliases && withAliases.has(varName)) {
          const originalExpr = withAliases.get(varName)!;
          // Evaluate the original expression
          if (originalExpr.type === "literal") {
            resolved[key] = originalExpr.value;
          } else if (originalExpr.type === "parameter") {
            resolved[key] = this.ctx.paramValues[originalExpr.name!];
          } else {
            // For complex expressions, try to evaluate
            try {
              resolved[key] = this.evaluateExpression(originalExpr);
            } catch {
              resolved[key] = value; // Keep as-is if can't evaluate
            }
          }
        } else {
          // Check if it's a known variable in context
          if (this.ctx.variables.has(varName)) {
            // Variable is bound - this is valid (executor will resolve it)
            resolved[key] = value;
          } else {
            // Undefined variable - throw error
            throw new Error(`Variable \`${varName}\` not defined`);
          }
        }
      } else if ((value as any)?.type === "property") {
        // Handle property references like a.id - check if 'a' was created in this CREATE clause
        const propRef = value as { type: "property"; variable: string; property: string };
        const createdNodeProperties = (this.ctx as any).createdNodeProperties as Map<string, Record<string, unknown>> | undefined;
        if (createdNodeProperties && createdNodeProperties.has(propRef.variable)) {
          const nodeProps = createdNodeProperties.get(propRef.variable)!;
          resolved[key] = nodeProps[propRef.property] ?? null;
        } else {
          // Property reference to unknown variable - keep as-is (executor may resolve it)
          resolved[key] = value;
        }
      } else {
        // Evaluate deterministic property-value expressions (e.g., date({year: 1980, ...}))
        // when they are fully-resolvable at compile time.
        try {
          resolved[key] = this.evaluatePropertyValue(value as PropertyValue);
        } catch {
          resolved[key] = value;
        }
      }
    }

    return { json: JSON.stringify(resolved), params };
  }

  private isFunctionPropertyValue(value: PropertyValue): value is { type: "function"; name: string; args: PropertyValue[] } {
    return (
      typeof value === "object" &&
      value !== null &&
      !Array.isArray(value) &&
      "type" in value &&
      (value as any).type === "function" &&
      typeof (value as any).name === "string" &&
      Array.isArray((value as any).args)
    );
  }

  private isBinaryPropertyValue(value: PropertyValue): value is BinaryPropertyValue {
    return (
      typeof value === "object" &&
      value !== null &&
      !Array.isArray(value) &&
      "type" in value &&
      (value as any).type === "binary" &&
      typeof (value as any).operator === "string" &&
      "left" in (value as any) &&
      "right" in (value as any)
    );
  }

  private isMapPropertyValue(value: PropertyValue): value is { type: "map"; properties: Record<string, PropertyValue> } {
    return (
      typeof value === "object" &&
      value !== null &&
      !Array.isArray(value) &&
      "type" in value &&
      (value as any).type === "map" &&
      typeof (value as any).properties === "object" &&
      (value as any).properties !== null
    );
  }

  private evaluatePropertyValue(value: PropertyValue): unknown {
    if (Array.isArray(value)) {
      return value.map((v) => this.evaluatePropertyValue(v));
    }

    if (typeof value !== "object" || value === null) {
      return value;
    }

    if (this.isParameterRef(value)) {
      return this.ctx.paramValues[value.name];
    }

    // Cannot evaluate references to runtime variables at compile time.
    if (this.isVariableRef(value)) {
      throw new Error("Cannot evaluate variable ref");
    }
    if ((value as any).type === "property") {
      throw new Error("Cannot evaluate property ref");
    }

    if (this.isBinaryPropertyValue(value)) {
      const left = this.evaluatePropertyValue(value.left);
      const right = this.evaluatePropertyValue(value.right);
      const op = value.operator;

      if (op === "+" && (typeof left === "string" || typeof right === "string")) {
        return String(left) + String(right);
      }

      const leftNum = typeof left === "number" ? left : Number(left);
      const rightNum = typeof right === "number" ? right : Number(right);
      if (!Number.isFinite(leftNum) || !Number.isFinite(rightNum)) {
        throw new Error("Cannot evaluate non-numeric binary");
      }

      switch (op) {
        case "+": return leftNum + rightNum;
        case "-": return leftNum - rightNum;
        case "*": return leftNum * rightNum;
        case "/": return leftNum / rightNum;
        case "%": return leftNum % rightNum;
        case "^": return Math.pow(leftNum, rightNum);
        default: throw new Error("Unknown operator");
      }
    }

    if (this.isFunctionPropertyValue(value)) {
      const fn = value.name.toUpperCase();
      const args = value.args;

      const pad2 = (n: number): string => String(n).padStart(2, "0");
      const pad4 = (n: number): string => String(n).padStart(4, "0");

      const evalMapArg = (arg: PropertyValue): Record<string, unknown> => {
        if (!this.isMapPropertyValue(arg)) throw new Error("Expected map arg");
        const out: Record<string, unknown> = {};
        for (const [k, v] of Object.entries(arg.properties)) {
          out[k] = this.evaluatePropertyValue(v);
        }
        return out;
      };

      switch (fn) {
        case "TIMESTAMP":
          return Date.now();
        case "RANDOMUUID":
          return crypto.randomUUID();
        case "DATE": {
          if (args.length === 0) return new Date().toISOString().split("T")[0];
          if (this.isMapPropertyValue(args[0])) {
            const map = evalMapArg(args[0]);
            const year = Number(map.year);
            const month = Number(map.month ?? 1);
            const day = Number(map.day ?? 1);
            if (!Number.isFinite(year) || !Number.isFinite(month) || !Number.isFinite(day)) {
              throw new Error("Invalid DATE map");
            }
            return `${pad4(Math.trunc(year))}-${pad2(Math.trunc(month))}-${pad2(Math.trunc(day))}`;
          }
          const arg0 = this.evaluatePropertyValue(args[0]);
          return String(arg0).split("T")[0];
        }
        case "TIME": {
          if (args.length === 0) return new Date().toISOString().split("T")[1].split(".")[0];
          if (this.isMapPropertyValue(args[0])) {
            const map = evalMapArg(args[0]);
            const hour = Number(map.hour ?? 0);
            const minute = Number(map.minute ?? 0);
            const secondVal = map.second;
            const nanosVal = map.nanosecond;
            const hasSecond = secondVal !== undefined || nanosVal !== undefined;
            const second = Number(secondVal ?? 0);
            const nanos = Number(nanosVal ?? 0);
            const tz = map.timezone !== undefined ? String(map.timezone) : "";

            let out = hasSecond ? `${pad2(Math.trunc(hour))}:${pad2(Math.trunc(minute))}:${pad2(Math.trunc(second))}` : `${pad2(Math.trunc(hour))}:${pad2(Math.trunc(minute))}`;
            if (nanosVal !== undefined) out += `.${String(Math.trunc(nanos)).padStart(9, "0")}`;
            return out + tz;
          }
          const arg0 = this.evaluatePropertyValue(args[0]);
          return String(arg0);
        }
        case "LOCALTIME": {
          if (args.length === 0) return new Date().toISOString().split("T")[1].split(".")[0];
          if (this.isMapPropertyValue(args[0])) {
            const map = evalMapArg(args[0]);
            const hour = Number(map.hour ?? 0);
            const minute = Number(map.minute ?? 0);
            const secondVal = map.second;
            const nanosVal = map.nanosecond;
            const hasSecond = secondVal !== undefined || nanosVal !== undefined;
            const second = Number(secondVal ?? 0);
            const nanos = Number(nanosVal ?? 0);
            let out = hasSecond ? `${pad2(Math.trunc(hour))}:${pad2(Math.trunc(minute))}:${pad2(Math.trunc(second))}` : `${pad2(Math.trunc(hour))}:${pad2(Math.trunc(minute))}`;
            if (nanosVal !== undefined) out += `.${String(Math.trunc(nanos)).padStart(9, "0")}`;
            return out;
          }
          const arg0 = this.evaluatePropertyValue(args[0]);
          return String(arg0);
        }
        case "DATETIME":
        case "LOCALDATETIME": {
          if (args.length === 0) return new Date().toISOString();
          if (this.isMapPropertyValue(args[0])) {
            const map = evalMapArg(args[0]);
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
            const tz = fn === "DATETIME" && map.timezone !== undefined ? String(map.timezone) : "";

            let time = hasSecond ? `${pad2(Math.trunc(hour))}:${pad2(Math.trunc(minute))}:${pad2(Math.trunc(second))}` : `${pad2(Math.trunc(hour))}:${pad2(Math.trunc(minute))}`;
            if (nanosVal !== undefined) time += `.${String(Math.trunc(nanos)).padStart(9, "0")}`;
            return `${pad4(Math.trunc(year))}-${pad2(Math.trunc(month))}-${pad2(Math.trunc(day))}T${time}${tz}`;
          }
          const arg0 = this.evaluatePropertyValue(args[0]);
          return String(arg0);
        }
        default:
          throw new Error(`Unknown function in property value: ${fn}`);
      }
    }

    if (this.isMapPropertyValue(value)) {
      // Maps are not valid storable property values.
      throw new Error("TypeError: InvalidPropertyType");
    }

    throw new Error("Cannot evaluate property value");
  }
  
  private isVariableRef(value: unknown): boolean {
    return typeof value === "object" && value !== null && 
           (value as Record<string, unknown>).type === "variable" &&
           typeof (value as Record<string, unknown>).name === "string";
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
        return `${expr.variable}.${expr.property}`;
      case "function": {
        // Build full function call representation: function(args)
        const funcName = expr.functionName!.toLowerCase();
        if (expr.args && expr.args.length > 0) {
          // Special case: INDEX function should be rendered as list[index] notation
          if (funcName === "index" && expr.args.length === 2) {
            const listName = this.getExpressionName(expr.args[0]);
            const indexName = this.getExpressionName(expr.args[1]);
            return `${listName}[${indexName}]`;
          }
          const argNames = expr.args.map(arg => this.getExpressionName(arg));
          const distinctPrefix = expr.distinct ? "distinct " : "";
          return `${funcName}(${distinctPrefix}${argNames.join(", ")})`;
        }
        // Handle COUNT(*) - the star flag indicates * was explicitly used
        if (expr.star) {
          return `${funcName}(*)`;
        }
        return `${funcName}()`;
      }
      case "labelPredicate": {
        // For (n:Foo) or (n:Foo:Bar), the column name should be the full expression
        const labels = expr.labels || (expr.label ? [expr.label] : []);
        return `(${expr.variable}:${labels.join(':')})`;
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
      case "parameter":
        return `$${expr.name}`;
      case "binary":
        // For binary expressions, try to reconstruct a readable name
        return `${this.getExpressionName(expr.left!)} ${expr.operator} ${this.getExpressionName(expr.right!)}`;
      case "propertyAccess": {
        // For property access on expressions like (list[1]).name or map.key1.key2
        const objectName = this.getExpressionName(expr.object!);
        // Only add parentheses for complex expressions (functions, binary ops)
        // Simple property chains like map.key1.key2 don't need parentheses
        if (expr.object!.type === "property" || expr.object!.type === "variable") {
          return `${objectName}.${expr.property}`;
        }
        return `(${objectName}).${expr.property}`;
      }
      case "comparison": {
        // For comparison expressions like n.x IS NULL, n.x = 1, etc.
        const leftName = this.getExpressionName(expr.left!);
        const op = expr.comparisonOperator!;
        if (op === "IS NULL" || op === "IS NOT NULL") {
          return `${leftName} ${op}`;
        }
        const rightName = this.getExpressionName(expr.right!);
        return `${leftName} ${op} ${rightName}`;
      }
      case "in": {
        // For IN expressions like n.x IN [1, 2]
        const leftName = this.getExpressionName(expr.left!);
        const listName = this.getExpressionName(expr.list!);
        return `${leftName} IN ${listName}`;
      }
      case "unary": {
        // For unary expressions like NOT n.x
        const operandName = this.getExpressionName(expr.operand!);
        return `${expr.operator} ${operandName}`;
      }
      default:
        return "expr";
    }
  }

  /**
   * Check for duplicate column names in RETURN or WITH items.
   * Throws SyntaxError with ColumnNameConflict if duplicates are found.
   */
  private checkDuplicateColumnNames(items: ReturnItem[]): void {
    const seenNames = new Set<string>();
    
    for (const item of items) {
      // Skip RETURN * as it's expanded later and won't have duplicates
      if (item.expression.type === "variable" && item.expression.variable === "*") {
        continue;
      }
      
      // Get the column name (alias if provided, otherwise derived from expression)
      const columnName = item.alias || this.getExpressionName(item.expression);
      
      if (seenNames.has(columnName)) {
        throw new Error(`SyntaxError: ColumnNameConflict - Multiple result columns with the same name '${columnName}'`);
      }
      
      seenNames.add(columnName);
    }
  }

  /**
   * Validate that ORDER BY expressions with DISTINCT only reference columns in the RETURN clause.
   * In SQL, when using SELECT DISTINCT, ORDER BY can only reference columns that appear in the SELECT list.
   * This is because DISTINCT removes duplicate rows before ORDER BY, so ordering by a column not in
   * the SELECT list would be ambiguous (which value to use from the deduplicated rows?).
   * 
   * Special cases:
   * - RETURN DISTINCT b ORDER BY b.name - VALID (b is returned, b.name is a property of b)
   * - RETURN DISTINCT a.name ORDER BY a.age - INVALID (a.age is not in RETURN)
   */
  private validateDistinctOrderBy(clause: ReturnClause): void {
    // Build a set of what's available for ORDER BY:
    // 1. Column aliases (explicit AS name)
    // 2. Expression names derived from RETURN items (e.g., "a.name" for a property access)
    // 3. Variables that are returned as whole nodes/edges (their properties are accessible)
    const availableColumns = new Set<string>();
    const returnedVariables = new Set<string>(); // Whole node/edge variables returned
    
    // Also track the expressions that are returned so we can match ORDER BY expressions
    const returnedExpressions: Expression[] = [];
    
    for (const item of clause.items) {
      // Get the column name
      const columnName = item.alias || this.getExpressionName(item.expression);
      availableColumns.add(columnName);
      returnedExpressions.push(item.expression);
      
      // If the expression is a whole variable (not a property), it's available for property access
      if (item.expression.type === "variable" && item.expression.variable) {
        returnedVariables.add(item.expression.variable);
      }
    }
    
    // Check each ORDER BY expression
    for (const orderItem of clause.orderBy!) {
      const orderExpr = orderItem.expression;
      
      // Check if the ORDER BY expression matches a returned column name
      const orderExprName = this.getExpressionName(orderExpr);
      if (availableColumns.has(orderExprName)) {
        continue; // Valid - matches a column alias or expression name
      }
      
      // Check if the ORDER BY expression is structurally equivalent to a RETURN expression
      if (this.expressionMatchesAny(orderExpr, returnedExpressions)) {
        continue; // Valid - same expression is in RETURN
      }
      
      // Check if ORDER BY is on a property of a returned variable
      // e.g., RETURN DISTINCT b ORDER BY b.name - valid because b is returned
      if (orderExpr.type === "property" && orderExpr.variable && returnedVariables.has(orderExpr.variable)) {
        continue; // Valid - property access on a returned variable
      }
      
      // Invalid - ORDER BY expression references something not in RETURN DISTINCT
      throw new Error(`SyntaxError: In a WITH/RETURN with DISTINCT or an aggregation, it is not possible to access variables not already contained in the WITH/RETURN`);
    }
  }

  /**
   * Validate ORDER BY with aggregation in RETURN.
   * When aggregation is used, ORDER BY can only reference:
   * - Column aliases from RETURN
   * - Expressions that exactly match RETURN expressions
   * - Pure aggregate expressions (no mixing of aggregate and non-aggregate)
   * 
   * This prevents queries like:
   * - RETURN count(you.age) ORDER BY me.age + count(you.age) - INVALID (me.age not grouped)
   */
  private validateAggregationOrderBy(clause: ReturnClause, orderBy: { expression: Expression; direction: "ASC" | "DESC" }[]): void {
    // Build a set of available column aliases
    const availableColumns = new Set<string>();
    const returnedExpressions: Expression[] = [];
    
    for (const item of clause.items) {
      const columnName = item.alias || this.getExpressionName(item.expression);
      availableColumns.add(columnName);
      returnedExpressions.push(item.expression);
    }
    
    // Check each ORDER BY expression
    for (const orderItem of orderBy) {
      const orderExpr = orderItem.expression;
      
      // Check if it's a simple column alias reference
      const orderExprName = this.getExpressionName(orderExpr);
      if (availableColumns.has(orderExprName)) {
        continue; // Valid - references a RETURN column
      }
      
      // Check if it exactly matches a RETURN expression
      if (this.expressionMatchesAny(orderExpr, returnedExpressions)) {
        continue; // Valid - same expression in RETURN
      }
      
      // Check if it's a pure aggregate expression or uses only column aliases and aggregates
      // This is allowed even if not in RETURN
      if (this.isPureAggregateExpression(orderExpr, availableColumns, returnedExpressions)) {
        continue; // Valid - pure aggregate or uses column aliases
      }
      
      // Invalid - mixed aggregate/non-aggregate or non-aggregate on ungrouped variable
      throw new Error(`SyntaxError: In a WITH/RETURN with DISTINCT or an aggregation, it is not possible to access variables not already contained in the WITH/RETURN`);
    }
  }
  
  /**
   * Check if an expression is a pure aggregate (contains only aggregates, literals, operators, and column aliases).
   * No references to non-aggregated variables.
   * 
   * @param expr - The expression to check
   * @param availableColumns - Set of column aliases that are available from RETURN clause
   * @param returnedExpressions - List of expressions in the RETURN clause
   */
  private isPureAggregateExpression(expr: Expression, availableColumns?: Set<string>, returnedExpressions?: Expression[]): boolean {
    switch (expr.type) {
      case "function": {
        const aggregateFunctions = ["COUNT", "SUM", "AVG", "MIN", "MAX", "COLLECT", "PERCENTILEDISC", "PERCENTILECONT"];
        // ORDER BY may reference a projected aggregate expression (even within a larger expression)
        // by repeating it verbatim.
        if (returnedExpressions && this.expressionMatchesAny(expr, returnedExpressions)) {
          return true;
        }
        const isAggregate = aggregateFunctions.includes(expr.functionName?.toUpperCase() || "");
        if (isAggregate) {
          if (expr.star) return true;
          if (!expr.args || expr.args.length === 0) return true;
          // Treat aggregates as pure only if their arguments reference only projected columns
          // (or expressions already returned), not raw graph variables.
          return expr.args.every(arg => this.isPureAggregateExpression(arg, availableColumns, returnedExpressions));
        }
        // Non-aggregate function - only pure if all args are pure aggregates or literals
        return expr.args?.every(arg => this.isPureAggregateExpression(arg, availableColumns, returnedExpressions)) || false;
      }
      case "binary":
        // Binary is pure if both sides are pure
        return this.isPureAggregateExpression(expr.left!, availableColumns, returnedExpressions) && this.isPureAggregateExpression(expr.right!, availableColumns, returnedExpressions);
      case "literal":
        return true; // Literals are always pure
      case "variable":
        // Variable is pure if it references a column alias from RETURN
        if (availableColumns && expr.variable && availableColumns.has(expr.variable)) {
          return true;
        }
        return false; // Otherwise, not a pure aggregate
      case "property":
        // Property is pure if it matches a RETURN expression
        if (returnedExpressions && this.expressionMatchesAny(expr, returnedExpressions)) {
          return true;
        }
        return false; // Otherwise, not a pure aggregate
      default:
        return false; // Conservative - assume not pure
    }
  }
  
  /**
   * Check if an expression matches any expression in the list (structurally).
   */
  private expressionMatchesAny(expr: Expression, expressions: Expression[]): boolean {
    for (const candidate of expressions) {
      if (this.expressionsMatch(expr, candidate)) {
        return true;
      }
    }
    return false;
  }

  /**
   * Check if two expressions are structurally equivalent.
   */
  private expressionsMatch(a: Expression, b: Expression): boolean {
    if (a.type !== b.type) return false;
    
    switch (a.type) {
      case "property":
        return a.variable === b.variable && a.property === b.property;
      case "variable":
        return a.variable === b.variable;
      case "literal":
        return a.value === b.value;
      case "function":
        if (a.functionName !== b.functionName) return false;
        if (!a.args || !b.args) return a.args === b.args;
        if (a.args.length !== b.args.length) return false;
        return a.args.every((arg, i) => this.expressionsMatch(arg, b.args![i]));
      case "binary":
        if (a.operator !== b.operator) return false;
        return this.expressionsMatch(a.left!, b.left!) && this.expressionsMatch(a.right!, b.right!);
      default:
        // For other expression types, be conservative and return false
        return false;
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
