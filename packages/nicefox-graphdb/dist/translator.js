// Cypher → SQL Translator
// ============================================================================
// Translator
// ============================================================================
export class Translator {
    ctx;
    constructor(paramValues = {}) {
        this.ctx = {
            variables: new Map(),
            paramValues,
            aliasCounter: 0,
        };
    }
    translate(query) {
        const statements = [];
        let returnColumns;
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
        const callClause = this.ctx.callClause;
        if (callClause && statements.length === 0) {
            // Generate SQL for standalone CALL
            const params = [];
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
    translateClause(clause) {
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
                return this.translateUnion(clause);
            case "CALL":
                return this.translateCall(clause);
            default:
                throw new Error(`Unknown clause type: ${clause.type}`);
        }
    }
    // ============================================================================
    // CREATE
    // ============================================================================
    translateCreate(clause) {
        const statements = [];
        for (const pattern of clause.patterns) {
            if (this.isRelationshipPattern(pattern)) {
                statements.push(...this.translateCreateRelationship(pattern));
            }
            else {
                // For standalone node patterns, check if the variable is already bound
                const nodePattern = pattern;
                if (nodePattern.variable && this.ctx.variables.has(nodePattern.variable)) {
                    throw new Error(`Variable \`${nodePattern.variable}\` already declared`);
                }
                statements.push(this.translateCreateNode(nodePattern));
            }
        }
        return statements;
    }
    translateCreateNode(node) {
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
    translateCreateRelationship(rel) {
        const statements = [];
        // Create source node if it has a label (new node)
        let sourceId;
        if (rel.source.variable) {
            const existing = this.ctx.variables.get(rel.source.variable);
            if (existing) {
                // Variable already bound - check for label or property conflict
                // In CREATE, you cannot rebind a variable with new properties/labels
                if (rel.source.label || rel.source.properties) {
                    throw new Error(`Variable \`${rel.source.variable}\` already declared`);
                }
                sourceId = existing.alias;
            }
            else {
                // Variable not found but has a label - create new node
                const sourceStmt = this.translateCreateNode(rel.source);
                statements.push(sourceStmt);
                sourceId = sourceStmt.params[0];
            }
        }
        else {
            // No variable - create anonymous node (with or without label)
            const sourceStmt = this.translateCreateNode(rel.source);
            statements.push(sourceStmt);
            sourceId = sourceStmt.params[0];
        }
        // Create target node if it has a label (new node) or is anonymous
        let targetId;
        if (rel.target.variable) {
            const existing = this.ctx.variables.get(rel.target.variable);
            if (existing) {
                // Variable already bound - check for label or property conflict
                // In CREATE, you cannot rebind a variable with new properties/labels
                if (rel.target.label || rel.target.properties) {
                    throw new Error(`Variable \`${rel.target.variable}\` already declared`);
                }
                targetId = existing.alias;
            }
            else {
                // Variable not found but has a label - create new node
                const targetStmt = this.translateCreateNode(rel.target);
                statements.push(targetStmt);
                targetId = targetStmt.params[0];
            }
        }
        else {
            // No variable - create anonymous node (with or without label)
            const targetStmt = this.translateCreateNode(rel.target);
            statements.push(targetStmt);
            targetId = targetStmt.params[0];
        }
        // Create edge
        const edgeId = this.generateId();
        const edgeType = rel.edge.type || "";
        const edgeProperties = this.serializeProperties(rel.edge.properties || {});
        if (rel.edge.variable) {
            this.ctx.variables.set(rel.edge.variable, { type: "edge", alias: edgeId });
        }
        // Swap source/target for left-directed relationships
        const [actualSource, actualTarget] = rel.edge.direction === "left" ? [targetId, sourceId] : [sourceId, targetId];
        statements.push({
            sql: "INSERT INTO edges (id, type, source_id, target_id, properties) VALUES (?, ?, ?, ?, ?)",
            params: [edgeId, edgeType, actualSource, actualTarget, edgeProperties.json],
        });
        return statements;
    }
    // ============================================================================
    // MATCH
    // ============================================================================
    translateMatch(clause, optional = false) {
        // MATCH doesn't produce standalone statements - it sets up context for RETURN/SET/DELETE
        // The actual SELECT is generated when we encounter RETURN
        // Handle path expressions FIRST (e.g., p = (a)-[r]->(b))
        // This ensures path variables are registered before we check for conflicts with node patterns
        if (clause.pathExpressions) {
            for (const pathExpr of clause.pathExpressions) {
                this.registerPathExpression(pathExpr, optional);
            }
        }
        for (const pattern of clause.patterns) {
            if (this.isRelationshipPattern(pattern)) {
                this.registerRelationshipPattern(pattern, optional);
            }
            else {
                this.registerNodePattern(pattern, optional);
            }
        }
        // Store the where clause in context for later use
        // For OPTIONAL MATCH, we need to associate the where with the optional patterns
        if (clause.where) {
            if (optional) {
                // Store optional where clauses separately to apply them correctly
                if (!this.ctx.optionalWhereClauses) {
                    this.ctx.optionalWhereClauses = [];
                }
                this.ctx.optionalWhereClauses.push(clause.where);
            }
            else {
                this.ctx.whereClause = clause.where;
            }
        }
        return [];
    }
    registerPathExpression(pathExpr, optional = false) {
        // Register the path variable so it can be returned
        const pathAlias = `path${this.ctx.aliasCounter++}`;
        this.ctx.variables.set(pathExpr.variable, { type: "path", alias: pathAlias });
        // Store path information
        if (!this.ctx.pathExpressions) {
            this.ctx.pathExpressions = [];
        }
        const nodeAliases = [];
        const edgeAliases = [];
        let isVariableLength = false;
        let previousTargetAlias = undefined;
        // Register all patterns within the path
        for (const pattern of pathExpr.patterns) {
            if (this.isRelationshipPattern(pattern)) {
                const relPattern = pattern;
                // Check if source is first in chain - if so, record it before registering relationship
                const isFirstInChain = nodeAliases.length === 0;
                // For chained patterns in a path, set the previous target as the source to reuse
                // This handles cases like p = (a:Label1)<--(:Label2)--() where the middle node
                // should be shared between both relationship patterns
                if (previousTargetAlias && !relPattern.source.variable) {
                    // Set this so registerRelationshipPattern can reuse the alias
                    this.ctx.pathChainSourceAlias = previousTargetAlias;
                }
                // Register the relationship pattern - this will handle node and edge registration
                this.registerRelationshipPattern(pattern, optional);
                // Clear the chain source alias after use
                this.ctx.pathChainSourceAlias = undefined;
                // Now extract the aliases that were created
                const relPatternInfo = this.ctx.relationshipPatterns[this.ctx.relationshipPatterns.length - 1];
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
            }
            else {
                // Single node pattern in path
                const nodeAlias = this.registerNodePattern(pattern, optional);
                nodeAliases.push(nodeAlias);
                previousTargetAlias = nodeAlias;
            }
        }
        // For variable-length paths, pre-allocate a CTE name so that length(p) can reference it
        let pathCteName;
        if (isVariableLength) {
            pathCteName = `path_${this.ctx.aliasCounter++}`;
        }
        this.ctx.pathExpressions.push({
            variable: pathExpr.variable,
            alias: pathAlias,
            nodeAliases: Array.from(new Set(nodeAliases)), // Deduplicated for table joins
            nodeSequence: nodeAliases, // Original order with duplicates for path output
            edgeAliases,
            patterns: pathExpr.patterns,
            optional,
            isVariableLength,
            pathCteName // CTE name for variable-length path, used by length(p)
        });
    }
    registerNodePattern(node, optional = false) {
        const alias = `n${this.ctx.aliasCounter++}`;
        if (node.variable) {
            // Check if variable is already registered as a path variable
            const existingVar = this.ctx.variables.get(node.variable);
            if (existingVar && existingVar.type === "path") {
                throw new Error(`VariableAlreadyBound: Variable \`${node.variable}\` already declared as a path`);
            }
            this.ctx.variables.set(node.variable, { type: "node", alias });
        }
        else {
            // Track anonymous node patterns so they can be included in FROM clause
            if (!this.ctx.anonymousNodePatterns) {
                this.ctx.anonymousNodePatterns = [];
            }
            this.ctx.anonymousNodePatterns.push({ alias, optional });
        }
        // Store pattern info for later
        this.ctx[`pattern_${alias}`] = node;
        // Track if this node pattern is optional
        this.ctx[`optional_${alias}`] = optional;
        return alias;
    }
    registerRelationshipPattern(rel, optional = false) {
        // Check if source node is already registered (for chained patterns or multi-MATCH)
        let sourceAlias;
        let sourceIsNew = false;
        if (rel.source.variable && this.ctx.variables.has(rel.source.variable)) {
            const existingVar = this.ctx.variables.get(rel.source.variable);
            // Check if variable is a path - cannot use path variable as a node
            if (existingVar.type === "path") {
                throw new Error(`VariableAlreadyBound: Variable \`${rel.source.variable}\` already declared as a path`);
            }
            sourceAlias = existingVar.alias;
            // If the new pattern has a label constraint, track it as an additional constraint
            if (rel.source.label) {
                if (!this.ctx.additionalLabelConstraints) {
                    this.ctx.additionalLabelConstraints = [];
                }
                this.ctx.additionalLabelConstraints.push({
                    alias: sourceAlias,
                    label: rel.source.label,
                    optional
                });
            }
        }
        else if (this.ctx.pathChainSourceAlias) {
            // Path chaining: reuse the previous target alias as this source
            // This handles patterns like p = (a:Label1)<--(:Label2)--() where (:Label2) is shared
            sourceAlias = this.ctx.pathChainSourceAlias;
            sourceIsNew = false;
            // If the source has a label constraint, add it to the reused alias
            if (rel.source.label) {
                if (!this.ctx.additionalLabelConstraints) {
                    this.ctx.additionalLabelConstraints = [];
                }
                this.ctx.additionalLabelConstraints.push({
                    alias: sourceAlias,
                    label: rel.source.label,
                    optional
                });
            }
        }
        else if (!rel.source.variable && !rel.source.label && this.ctx.lastAnonymousTargetAlias) {
            // Anonymous source node in a chain - reuse the last anonymous target
            sourceAlias = this.ctx.lastAnonymousTargetAlias;
            sourceIsNew = false;
        }
        else {
            sourceAlias = this.registerNodePattern(rel.source, optional);
            sourceIsNew = true;
        }
        // Check if target node is already registered (for multi-MATCH shared variables)
        let targetAlias;
        let targetIsNew = false;
        if (rel.target.variable && this.ctx.variables.has(rel.target.variable)) {
            const existingVar = this.ctx.variables.get(rel.target.variable);
            // Check if variable is a path - cannot use path variable as a node
            if (existingVar.type === "path") {
                throw new Error(`VariableAlreadyBound: Variable \`${rel.target.variable}\` already declared as a path`);
            }
            targetAlias = existingVar.alias;
            // If the new pattern has a label constraint, track it as an additional constraint
            if (rel.target.label) {
                if (!this.ctx.additionalLabelConstraints) {
                    this.ctx.additionalLabelConstraints = [];
                }
                this.ctx.additionalLabelConstraints.push({
                    alias: targetAlias,
                    label: rel.target.label,
                    optional
                });
            }
        }
        else {
            targetAlias = this.registerNodePattern(rel.target, optional);
            targetIsNew = true;
        }
        // Check if edge variable is already registered (for multi-MATCH with same edge variable)
        let edgeAlias;
        let edgeIsNew = false;
        let boundEdgeOriginalPattern;
        if (rel.edge.variable && this.ctx.variables.has(rel.edge.variable)) {
            const existingVar = this.ctx.variables.get(rel.edge.variable);
            // Check if variable is a path - cannot use path variable as an edge
            if (existingVar.type === "path") {
                throw new Error(`VariableAlreadyBound: Variable \`${rel.edge.variable}\` already declared as a path`);
            }
            edgeAlias = existingVar.alias;
            // Find the original relationship pattern for this bound edge
            const relPatterns = this.ctx.relationshipPatterns;
            if (relPatterns) {
                boundEdgeOriginalPattern = relPatterns.find(p => p.edgeAlias === edgeAlias);
            }
        }
        else {
            edgeAlias = `e${this.ctx.aliasCounter++}`;
            edgeIsNew = true;
            if (rel.edge.variable) {
                this.ctx.variables.set(rel.edge.variable, { type: "edge", alias: edgeAlias });
            }
        }
        this.ctx[`pattern_${edgeAlias}`] = rel.edge;
        this.ctx[`optional_${edgeAlias}`] = optional;
        // Store relationship patterns as an array to support multi-hop
        if (!this.ctx.relationshipPatterns) {
            this.ctx.relationshipPatterns = [];
        }
        // Check if this is a variable-length pattern
        const isVariableLength = rel.edge.minHops !== undefined || rel.edge.maxHops !== undefined;
        // For variable-length patterns, mark the edge variable specially
        if (isVariableLength && rel.edge.variable && edgeIsNew) {
            // Re-register as varLengthEdge type (will be updated with pathCteName later)
            this.ctx.variables.set(rel.edge.variable, { type: "varLengthEdge", alias: edgeAlias });
        }
        this.ctx.relationshipPatterns.push({
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
        });
        // Track the last anonymous target for chained patterns
        // This allows (a)-[:R]->()-[:S]->(b) to share the anonymous node
        if (!rel.target.variable && !rel.target.label) {
            this.ctx.lastAnonymousTargetAlias = targetAlias;
        }
        else {
            // Clear the tracker if target is not anonymous
            this.ctx.lastAnonymousTargetAlias = undefined;
        }
        // Keep backwards compatibility with single pattern
        this.ctx.relationshipPattern = { sourceAlias, targetAlias, edgeAlias, edge: rel.edge, optional };
    }
    // ============================================================================
    // MERGE
    // ============================================================================
    translateMerge(clause) {
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
        const node = pattern;
        const label = node.label || "";
        const props = node.properties || {};
        const serialized = this.serializeProperties(props);
        // Build condition to find existing node
        const labelCondition = this.generateLabelMatchCondition("", label);
        const conditions = [labelCondition.sql.replace(/^[^.]+\./, "")]; // Remove alias prefix
        const params = [...labelCondition.params];
        const withAliases = this.ctx.withAliases;
        for (const [key, value] of Object.entries(props)) {
            if (this.isParameterRef(value)) {
                conditions.push(`json_extract(properties, '$.${key}') = ?`);
                params.push(this.ctx.paramValues[value.name]);
            }
            else if (this.isVariableRef(value)) {
                // Check if it's a WITH alias
                const varName = value.name;
                if (withAliases && withAliases.has(varName)) {
                    const originalExpr = withAliases.get(varName);
                    conditions.push(`json_extract(properties, '$.${key}') = ?`);
                    if (originalExpr.type === "literal") {
                        params.push(originalExpr.value);
                    }
                    else if (originalExpr.type === "parameter") {
                        params.push(this.ctx.paramValues[originalExpr.name]);
                    }
                    else {
                        params.push(this.evaluateExpression(originalExpr));
                    }
                }
                else {
                    conditions.push(`json_extract(properties, '$.${key}') = ?`);
                    params.push(value);
                }
            }
            else {
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
    translateSet(clause) {
        const statements = [];
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
                const filteredProps = {};
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
                const nonNullProps = {};
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
                }
                else {
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
                }
                else {
                    const { sql: exprSql, params: exprParams } = this.translateExpression(assignment.value);
                    // Use json_set with the SQL expression directly
                    statements.push({
                        sql: `UPDATE ${table} SET properties = json_set(properties, '$.${assignment.property}', ${exprSql}) WHERE id = ?`,
                        params: [...exprParams, varInfo.alias],
                    });
                }
            }
            else {
                const value = this.evaluateExpression(assignment.value);
                // If value is null, remove the property instead of setting it to null
                if (value === null) {
                    statements.push({
                        sql: `UPDATE ${table} SET properties = json_remove(properties, '$.${assignment.property}') WHERE id = ?`,
                        params: [varInfo.alias],
                    });
                }
                else {
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
    /**
     * Evaluate an object expression to get its key-value pairs.
     */
    evaluateObjectExpression(expr) {
        if (expr.type === "object" && expr.properties) {
            const result = {};
            for (const prop of expr.properties) {
                result[prop.key] = this.evaluateExpression(prop.value);
            }
            return result;
        }
        if (expr.type === "parameter") {
            const paramValue = this.ctx.paramValues[expr.name];
            if (typeof paramValue === "object" && paramValue !== null) {
                return paramValue;
            }
            throw new Error(`Parameter ${expr.name} is not an object`);
        }
        throw new Error(`Expected object expression, got ${expr.type}`);
    }
    // ============================================================================
    // DELETE
    // ============================================================================
    translateDelete(clause) {
        const statements = [];
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
    translateReturn(clause) {
        // Check if this is a RETURN after CALL
        const callClause = this.ctx.callClause;
        if (callClause) {
            return this.translateReturnFromCall(clause, callClause);
        }
        const selectParts = [];
        const returnColumns = [];
        const fromParts = [];
        const joinParts = [];
        const joinParams = []; // Parameters for JOIN ON clauses
        const whereParts = [];
        const whereParams = []; // Parameters for WHERE clause
        // Apply WITH modifiers if present
        const withDistinct = this.ctx.withDistinct;
        const withOrderBy = this.ctx.withOrderBy;
        const withSkip = this.ctx.withSkip;
        const withLimit = this.ctx.withLimit;
        const withWhere = this.ctx.withWhere;
        // Track which tables we need
        const neededTables = new Set();
        // Process return items
        const exprParams = [];
        // Check for RETURN * (return all bound variables)
        let returnItems = clause.items;
        if (clause.items.length > 0 &&
            clause.items[0].expression.type === "variable" &&
            clause.items[0].expression.variable === "*") {
            // Expand * to all bound variables
            const expandedItems = [];
            // First add WITH aliases (they should appear before MATCH-bound variables in typical queries)
            const withAliases = this.ctx.withAliases;
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
            selectParts.push(`${exprSql} AS ${this.quoteAlias(alias)}`);
            returnColumns.push(alias);
        }
        // Build FROM clause based on registered patterns
        const relPatterns = this.ctx.relationshipPatterns;
        // Check if any pattern is variable-length
        const hasVariableLengthPattern = relPatterns?.some(p => p.isVariableLength);
        if (hasVariableLengthPattern && relPatterns) {
            // Use recursive CTE for variable-length paths
            return this.translateVariableLengthPath(clause, relPatterns, selectParts, returnColumns, exprParams, whereParams);
        }
        if (relPatterns && relPatterns.length > 0) {
            // Track which node aliases we've already added to FROM/JOIN
            const addedNodeAliases = new Set();
            // Track which edge aliases we've already added to FROM/JOIN
            const addedEdgeAliases = new Set();
            // Track which node aliases have had their filters added (to avoid duplicates)
            const filteredNodeAliases = new Set();
            // IMPORTANT: Before processing relationship patterns, add all non-optional node
            // patterns to FROM first. This ensures that bound variables from required MATCH
            // clauses are in FROM before OPTIONAL MATCH relationship patterns try to reference them.
            // Example: MATCH (a:A), (b:C) OPTIONAL MATCH (x)-->(b) - here b must be in FROM before
            // we process the OPTIONAL MATCH relationship, even though b is used as a target in
            // the OPTIONAL MATCH. We check the node's original optional flag, not whether it
            // appears in an optional relationship pattern.
            for (const [variable, info] of this.ctx.variables) {
                if (info.type !== "node")
                    continue;
                const pattern = this.ctx[`pattern_${info.alias}`];
                const isOptional = this.ctx[`optional_${info.alias}`] === true;
                // Add non-optional nodes to FROM, regardless of whether they appear in a relationship pattern
                // The key is checking the node's original optional flag, not where it's used
                if (pattern && !isOptional) {
                    // Check if this node is only used as source/target in a NON-optional relationship pattern
                    // If so, the relationship loop will handle adding it to FROM
                    const isSourceOrTargetOfNonOptionalRel = relPatterns.some(rp => !rp.optional && (rp.sourceAlias === info.alias || rp.targetAlias === info.alias));
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
                                if (this.isParameterRef(value)) {
                                    whereParts.push(`json_extract(${info.alias}.properties, '$.${key}') = ?`);
                                    whereParams.push(this.ctx.paramValues[value.name]);
                                }
                                else {
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
                const sourceIsOptional = this.ctx[`optional_${relPattern.sourceAlias}`] === true;
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
                        const onConditions = ["1=1"];
                        const onParams = [];
                        const sourcePattern = this.ctx[`pattern_${relPattern.sourceAlias}`];
                        if (sourcePattern?.label) {
                            const labelMatch = this.generateLabelMatchCondition(relPattern.sourceAlias, sourcePattern.label);
                            onConditions.push(labelMatch.sql);
                            onParams.push(...labelMatch.params);
                        }
                        joinParts.push(`LEFT JOIN nodes ${relPattern.sourceAlias} ON ${onConditions.join(" AND ")}`);
                        joinParams.push(...onParams);
                        // Filter out NULL values in WHERE
                        whereParts.push(`${relPattern.sourceAlias}.id IS NOT NULL`);
                    }
                    else {
                        fromParts.push(`nodes ${relPattern.sourceAlias}`);
                    }
                    addedNodeAliases.add(relPattern.sourceAlias);
                }
                else if (!addedNodeAliases.has(relPattern.sourceAlias)) {
                    // For subsequent patterns, if source is not already added, we need to JOIN it
                    // For optional patterns, use LEFT JOIN
                    if (isOptional && relPattern.sourceIsNew && addedNodeAliases.has(relPattern.targetAlias)) {
                        // Special case: OPTIONAL MATCH with new source but bound target
                        // Example: MATCH (b:C) OPTIONAL MATCH (x)-->(b)
                        // We need to join edge first (on target), then source (on edge.source)
                        // So we SKIP adding source here - it will be added after the edge join below
                        // Set a flag to indicate this deferred source join
                        relPattern.deferSourceJoin = true;
                    }
                    else if (isOptional && relPattern.sourceIsNew) {
                        // Optional pattern with new source but target not bound
                        // This shouldn't happen often - optional patterns usually reference existing nodes
                        joinParts.push(`${joinType} nodes ${relPattern.sourceAlias} ON 1=1`);
                    }
                    else if (i === 0) {
                        // First pattern is optional but source is new - add to FROM
                        fromParts.push(`nodes ${relPattern.sourceAlias}`);
                    }
                    else {
                        // Check if source was from an optional MATCH
                        if (sourceIsOptional) {
                            // Source is from optional MATCH - use LEFT JOIN with label constraint, then filter NULL
                            const onConditions = ["1=1"];
                            const onParams = [];
                            const sourcePattern = this.ctx[`pattern_${relPattern.sourceAlias}`];
                            if (sourcePattern?.label) {
                                const labelMatch = this.generateLabelMatchCondition(relPattern.sourceAlias, sourcePattern.label);
                                onConditions.push(labelMatch.sql);
                                onParams.push(...labelMatch.params);
                            }
                            joinParts.push(`LEFT JOIN nodes ${relPattern.sourceAlias} ON ${onConditions.join(" AND ")}`);
                            joinParams.push(...onParams);
                            whereParts.push(`${relPattern.sourceAlias}.id IS NOT NULL`);
                        }
                        else {
                            joinParts.push(`JOIN nodes ${relPattern.sourceAlias} ON 1=1`);
                        }
                    }
                    // Only add to addedNodeAliases if we actually added a join (not deferred)
                    if (!relPattern.deferSourceJoin) {
                        addedNodeAliases.add(relPattern.sourceAlias);
                    }
                }
                // Build ON conditions for the edge join
                let edgeOnConditions = [];
                let edgeOnParams = [];
                // Check if this is an undirected/bidirectional pattern (direction: "none")
                const isUndirected = relPattern.edge.direction === "none";
                // Add edge join - need to determine direction based on whether source/target already exist
                // Use sourceWasAlreadyAdded (recorded before adding source) for accurate check
                if (isUndirected) {
                    // For undirected patterns, match edges in either direction
                    edgeOnConditions.push(`(${relPattern.edgeAlias}.source_id = ${relPattern.sourceAlias}.id OR ${relPattern.edgeAlias}.target_id = ${relPattern.sourceAlias}.id)`);
                }
                else if (isOptional && addedNodeAliases.has(relPattern.targetAlias) && !sourceWasAlreadyAdded) {
                    // OPTIONAL MATCH special case: target was already added (bound from previous MATCH) 
                    // but source was not. Join edge on target side: edge.target_id = bound_target.id
                    // This allows us to find all source nodes that connect to the bound target.
                    edgeOnConditions.push(`${relPattern.edgeAlias}.target_id = ${relPattern.targetAlias}.id`);
                }
                else if (relPattern.edge.direction === "left") {
                    // Left-directed: (a)<-[:R]-(b) means edge goes from b to a, so source is target_id
                    edgeOnConditions.push(`${relPattern.edgeAlias}.target_id = ${relPattern.sourceAlias}.id`);
                }
                else {
                    edgeOnConditions.push(`${relPattern.edgeAlias}.source_id = ${relPattern.sourceAlias}.id`);
                }
                // For optional patterns, add type filter to ON clause instead of WHERE
                if (relPattern.edge.type) {
                    if (isOptional) {
                        edgeOnConditions.push(`${relPattern.edgeAlias}.type = ?`);
                        edgeOnParams.push(relPattern.edge.type);
                    }
                    else {
                        whereParts.push(`${relPattern.edgeAlias}.type = ?`);
                        whereParams.push(relPattern.edge.type);
                    }
                }
                else if (relPattern.edge.types && relPattern.edge.types.length > 0) {
                    // Multiple relationship types: [:TYPE1|TYPE2]
                    const placeholders = relPattern.edge.types.map(() => "?").join(", ");
                    if (isOptional) {
                        edgeOnConditions.push(`${relPattern.edgeAlias}.type IN (${placeholders})`);
                        edgeOnParams.push(...relPattern.edge.types);
                    }
                    else {
                        whereParts.push(`${relPattern.edgeAlias}.type IN (${placeholders})`);
                        whereParams.push(...relPattern.edge.types);
                    }
                }
                // Add edge property filters
                if (relPattern.edge.properties) {
                    for (const [key, value] of Object.entries(relPattern.edge.properties)) {
                        if (this.isParameterRef(value)) {
                            if (isOptional) {
                                edgeOnConditions.push(`json_extract(${relPattern.edgeAlias}.properties, '$.${key}') = ?`);
                                edgeOnParams.push(this.ctx.paramValues[value.name]);
                            }
                            else {
                                whereParts.push(`json_extract(${relPattern.edgeAlias}.properties, '$.${key}') = ?`);
                                whereParams.push(this.ctx.paramValues[value.name]);
                            }
                        }
                        else {
                            if (isOptional) {
                                edgeOnConditions.push(`json_extract(${relPattern.edgeAlias}.properties, '$.${key}') = ?`);
                                edgeOnParams.push(value);
                            }
                            else {
                                whereParts.push(`json_extract(${relPattern.edgeAlias}.properties, '$.${key}') = ?`);
                                whereParams.push(value);
                            }
                        }
                    }
                }
                // Only add edge join if this edge alias hasn't been added yet
                if (!addedEdgeAliases.has(relPattern.edgeAlias)) {
                    joinParts.push(`${joinType} edges ${relPattern.edgeAlias} ON ${edgeOnConditions.join(" AND ")}`);
                    joinParams.push(...edgeOnParams);
                    addedEdgeAliases.add(relPattern.edgeAlias);
                }
                else {
                    // Edge already joined - just add any additional type/property filters to WHERE
                    // (The ON conditions would be redundant but filters might differ)
                    // Note: edgeOnParams were already added when the edge was first joined
                }
                // Handle deferred source join (when target was bound but source was new)
                // This joins the source node based on the edge's source_id
                if (relPattern.deferSourceJoin && !addedNodeAliases.has(relPattern.sourceAlias)) {
                    // Join source on edge.source_id (since edge was joined on target)
                    const sourceOnConditions = [];
                    if (relPattern.edge.direction === "left") {
                        // Left-directed: source is at target_id side of edge
                        sourceOnConditions.push(`${relPattern.sourceAlias}.id = ${relPattern.edgeAlias}.target_id`);
                    }
                    else {
                        // Right-directed or undirected: source is at source_id side
                        sourceOnConditions.push(`${relPattern.sourceAlias}.id = ${relPattern.edgeAlias}.source_id`);
                    }
                    // Add label filter if source has one
                    const sourcePattern = this.ctx[`pattern_${relPattern.sourceAlias}`];
                    const sourceOnParams = [];
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
                // Build ON conditions for the target node join
                let targetOnConditions = [];
                let targetOnParams = [];
                // Add target node join if not already added
                if (!addedNodeAliases.has(relPattern.targetAlias)) {
                    // For undirected patterns, target could be on either side of the edge
                    if (isUndirected) {
                        // Target is whichever end of the edge is not the source
                        targetOnConditions.push(`((${relPattern.edgeAlias}.source_id = ${relPattern.sourceAlias}.id AND ${relPattern.edgeAlias}.target_id = ${relPattern.targetAlias}.id) OR (${relPattern.edgeAlias}.target_id = ${relPattern.sourceAlias}.id AND ${relPattern.edgeAlias}.source_id = ${relPattern.targetAlias}.id))`);
                    }
                    else if (relPattern.edge.direction === "left") {
                        // Left-directed: target is at source_id side
                        targetOnConditions.push(`${relPattern.edgeAlias}.source_id = ${relPattern.targetAlias}.id`);
                    }
                    else {
                        targetOnConditions.push(`${relPattern.edgeAlias}.target_id = ${relPattern.targetAlias}.id`);
                    }
                    // For optional patterns, add label and property filters to ON clause
                    const targetPattern = this.ctx[`pattern_${relPattern.targetAlias}`];
                    if (isOptional && targetPattern?.label) {
                        const labelMatch = this.generateLabelMatchCondition(relPattern.targetAlias, targetPattern.label);
                        targetOnConditions.push(labelMatch.sql);
                        targetOnParams.push(...labelMatch.params);
                        filteredNodeAliases.add(relPattern.targetAlias);
                    }
                    if (isOptional && targetPattern?.properties) {
                        for (const [key, value] of Object.entries(targetPattern.properties)) {
                            if (this.isParameterRef(value)) {
                                targetOnConditions.push(`json_extract(${relPattern.targetAlias}.properties, '$.${key}') = ?`);
                                targetOnParams.push(this.ctx.paramValues[value.name]);
                            }
                            else {
                                targetOnConditions.push(`json_extract(${relPattern.targetAlias}.properties, '$.${key}') = ?`);
                                targetOnParams.push(value);
                            }
                        }
                    }
                    joinParts.push(`${joinType} nodes ${relPattern.targetAlias} ON ${targetOnConditions.join(" AND ")}`);
                    joinParams.push(...targetOnParams);
                    addedNodeAliases.add(relPattern.targetAlias);
                }
                else {
                    // Target was already added, but we need to ensure edge connects to it
                    // Add WHERE condition to connect edge to the existing node
                    // For left-directed edges, the pattern's target is the edge's source_id
                    const isLeftDirected = relPattern.edge.direction === "left";
                    const edgeColumn = isLeftDirected ? "source_id" : "target_id";
                    if (isOptional) {
                        // For optional, we need to handle this in ON clause of edge
                        // This is already handled above by adding to edgeOnConditions
                        if (isUndirected) {
                            whereParts.push(`(${relPattern.edgeAlias}.id IS NULL OR ${relPattern.edgeAlias}.target_id = ${relPattern.targetAlias}.id OR ${relPattern.edgeAlias}.source_id = ${relPattern.targetAlias}.id)`);
                        }
                        else {
                            whereParts.push(`(${relPattern.edgeAlias}.id IS NULL OR ${relPattern.edgeAlias}.${edgeColumn} = ${relPattern.targetAlias}.id)`);
                        }
                    }
                    else {
                        if (isUndirected) {
                            whereParts.push(`(${relPattern.edgeAlias}.target_id = ${relPattern.targetAlias}.id OR ${relPattern.edgeAlias}.source_id = ${relPattern.targetAlias}.id)`);
                        }
                        else {
                            whereParts.push(`${relPattern.edgeAlias}.${edgeColumn} = ${relPattern.targetAlias}.id`);
                        }
                    }
                }
                // Add source node filters (label and properties) if not already done and not optional
                if (!filteredNodeAliases.has(relPattern.sourceAlias)) {
                    const sourcePattern = this.ctx[`pattern_${relPattern.sourceAlias}`];
                    const sourceIsOptional = this.ctx[`optional_${relPattern.sourceAlias}`] === true;
                    if (sourcePattern?.label) {
                        if (sourceIsOptional) {
                            // For optional source nodes, this shouldn't happen often
                            // as optional patterns usually reference required nodes
                        }
                        else {
                            const labelMatch = this.generateLabelMatchCondition(relPattern.sourceAlias, sourcePattern.label);
                            whereParts.push(labelMatch.sql);
                            whereParams.push(...labelMatch.params);
                        }
                    }
                    if (sourcePattern?.properties && !sourceIsOptional) {
                        for (const [key, value] of Object.entries(sourcePattern.properties)) {
                            if (this.isParameterRef(value)) {
                                whereParts.push(`json_extract(${relPattern.sourceAlias}.properties, '$.${key}') = ?`);
                                whereParams.push(this.ctx.paramValues[value.name]);
                            }
                            else {
                                whereParts.push(`json_extract(${relPattern.sourceAlias}.properties, '$.${key}') = ?`);
                                whereParams.push(value);
                            }
                        }
                    }
                    filteredNodeAliases.add(relPattern.sourceAlias);
                }
                // Add target node filters (label and properties) if not already done and not optional
                if (!filteredNodeAliases.has(relPattern.targetAlias)) {
                    const targetPattern = this.ctx[`pattern_${relPattern.targetAlias}`];
                    if (!isOptional) {
                        if (targetPattern?.label) {
                            const labelMatch = this.generateLabelMatchCondition(relPattern.targetAlias, targetPattern.label);
                            whereParts.push(labelMatch.sql);
                            whereParams.push(...labelMatch.params);
                        }
                        if (targetPattern?.properties) {
                            for (const [key, value] of Object.entries(targetPattern.properties)) {
                                if (this.isParameterRef(value)) {
                                    whereParts.push(`json_extract(${relPattern.targetAlias}.properties, '$.${key}') = ?`);
                                    whereParams.push(this.ctx.paramValues[value.name]);
                                }
                                else {
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
            if (relPatterns.length > 1) {
                // Build connectivity graph to find chains of connected relationships
                // Two relationships are connected if they share a node (source/target)
                const edgeGroups = []; // Groups of edge indices that are connected
                const visited = new Set();
                for (let i = 0; i < relPatterns.length; i++) {
                    if (visited.has(i))
                        continue;
                    // BFS to find all connected edges
                    const group = [];
                    const queue = [i];
                    while (queue.length > 0) {
                        const current = queue.shift();
                        if (visited.has(current))
                            continue;
                        visited.add(current);
                        group.push(current);
                        const currentPattern = relPatterns[current];
                        // Find all edges that share a node with current edge
                        for (let j = 0; j < relPatterns.length; j++) {
                            if (visited.has(j))
                                continue;
                            const otherPattern = relPatterns[j];
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
                            if (edge1Alias === edge2Alias)
                                continue;
                            const edge1Optional = relPatterns[edge1Idx].optional;
                            const edge2Optional = relPatterns[edge2Idx].optional;
                            // For non-optional edges, require distinct IDs
                            // For optional edges, allow NULL (edge not matched) OR distinct
                            if (edge1Optional || edge2Optional) {
                                whereParts.push(`(${edge1Alias}.id IS NULL OR ${edge2Alias}.id IS NULL OR ${edge1Alias}.id <> ${edge2Alias}.id)`);
                            }
                            else {
                                whereParts.push(`${edge1Alias}.id <> ${edge2Alias}.id`);
                            }
                        }
                    }
                }
            }
            // Also add any standalone node patterns that are not part of relationship patterns
            // These need to be cross-joined (e.g., MATCH (x:X), (a)->(b) - x is standalone)
            for (const [variable, info] of this.ctx.variables) {
                if (info.type !== "node")
                    continue;
                if (addedNodeAliases.has(info.alias))
                    continue;
                const pattern = this.ctx[`pattern_${info.alias}`];
                const isOptional = this.ctx[`optional_${info.alias}`] === true;
                if (pattern) {
                    if (isOptional) {
                        // Optional standalone node - LEFT JOIN with its conditions
                        const onConditions = ["1=1"];
                        const onParams = [];
                        if (pattern.label) {
                            const labelMatch = this.generateLabelMatchCondition(info.alias, pattern.label);
                            onConditions.push(labelMatch.sql);
                            onParams.push(...labelMatch.params);
                        }
                        if (pattern.properties) {
                            for (const [key, value] of Object.entries(pattern.properties)) {
                                if (this.isParameterRef(value)) {
                                    onConditions.push(`json_extract(${info.alias}.properties, '$.${key}') = ?`);
                                    onParams.push(this.ctx.paramValues[value.name]);
                                }
                                else {
                                    onConditions.push(`json_extract(${info.alias}.properties, '$.${key}') = ?`);
                                    onParams.push(value);
                                }
                            }
                        }
                        joinParts.push(`LEFT JOIN nodes ${info.alias} ON ${onConditions.join(" AND ")}`);
                        joinParams.push(...onParams);
                    }
                    else {
                        // Non-optional standalone node - add to FROM (cross join) with WHERE conditions
                        if (fromParts.length === 0) {
                            fromParts.push(`nodes ${info.alias}`);
                        }
                        else {
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
                                if (this.isParameterRef(value)) {
                                    whereParts.push(`json_extract(${info.alias}.properties, '$.${key}') = ?`);
                                    whereParams.push(this.ctx.paramValues[value.name]);
                                }
                                else {
                                    whereParts.push(`json_extract(${info.alias}.properties, '$.${key}') = ?`);
                                    whereParams.push(value);
                                }
                            }
                        }
                    }
                    addedNodeAliases.add(info.alias);
                }
            }
        }
        else {
            // Simple node query (no relationships)
            let hasFromClause = false;
            for (const [variable, info] of this.ctx.variables) {
                const pattern = this.ctx[`pattern_${info.alias}`];
                const isOptional = this.ctx[`optional_${info.alias}`] === true;
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
                                if (this.isParameterRef(value)) {
                                    whereParts.push(`json_extract(${info.alias}.properties, '$.${key}') = ?`);
                                    whereParams.push(this.ctx.paramValues[value.name]);
                                }
                                else {
                                    whereParts.push(`json_extract(${info.alias}.properties, '$.${key}') = ?`);
                                    whereParams.push(value);
                                }
                            }
                        }
                    }
                    else if (isOptional) {
                        // Optional node - use LEFT JOIN
                        const onConditions = ["1=1"];
                        const onParams = [];
                        if (pattern.label) {
                            const labelMatch = this.generateLabelMatchCondition(info.alias, pattern.label);
                            onConditions.push(labelMatch.sql);
                            onParams.push(...labelMatch.params);
                        }
                        if (pattern.properties) {
                            for (const [key, value] of Object.entries(pattern.properties)) {
                                if (this.isParameterRef(value)) {
                                    onConditions.push(`json_extract(${info.alias}.properties, '$.${key}') = ?`);
                                    onParams.push(this.ctx.paramValues[value.name]);
                                }
                                else {
                                    onConditions.push(`json_extract(${info.alias}.properties, '$.${key}') = ?`);
                                    onParams.push(value);
                                }
                            }
                        }
                        joinParts.push(`LEFT JOIN nodes ${info.alias} ON ${onConditions.join(" AND ")}`);
                        joinParams.push(...onParams);
                    }
                    else {
                        // Non-optional node that's not the first - use regular JOIN
                        fromParts.push(`nodes ${info.alias}`);
                        if (pattern.label) {
                            const labelMatch = this.generateLabelMatchCondition(info.alias, pattern.label);
                            whereParts.push(labelMatch.sql);
                            whereParams.push(...labelMatch.params);
                        }
                        if (pattern.properties) {
                            for (const [key, value] of Object.entries(pattern.properties)) {
                                if (this.isParameterRef(value)) {
                                    whereParts.push(`json_extract(${info.alias}.properties, '$.${key}') = ?`);
                                    whereParams.push(this.ctx.paramValues[value.name]);
                                }
                                else {
                                    whereParts.push(`json_extract(${info.alias}.properties, '$.${key}') = ?`);
                                    whereParams.push(value);
                                }
                            }
                        }
                    }
                }
            }
            // Also add anonymous node patterns (e.g., MATCH () RETURN count(*))
            const anonymousPatterns = this.ctx.anonymousNodePatterns;
            if (anonymousPatterns) {
                for (const { alias, optional } of anonymousPatterns) {
                    if (!optional) {
                        if (fromParts.length === 0) {
                            fromParts.push(`nodes ${alias}`);
                        }
                        else {
                            // Cross join for additional anonymous nodes
                            fromParts.push(`nodes ${alias}`);
                        }
                    }
                }
            }
        }
        // Apply additional label constraints from multi-MATCH patterns
        // E.g., MATCH (a)-[r]->(b) WITH r, a MATCH (a:X)-[r]->(c) adds label constraint for :X on a
        const additionalLabelConstraints = this.ctx.additionalLabelConstraints;
        if (additionalLabelConstraints) {
            for (const constraint of additionalLabelConstraints) {
                const labelMatch = this.generateLabelMatchCondition(constraint.alias, constraint.label);
                if (constraint.optional) {
                    // For optional matches, allow NULL or the label constraint
                    whereParts.push(`(${constraint.alias}.id IS NULL OR ${labelMatch.sql})`);
                }
                else {
                    whereParts.push(labelMatch.sql);
                }
                whereParams.push(...labelMatch.params);
            }
        }
        // Add UNWIND tables using json_each
        const unwindClauses = this.ctx.unwindClauses;
        if (unwindClauses && unwindClauses.length > 0) {
            for (const unwindClause of unwindClauses) {
                // Use CROSS JOIN with json_each to expand the array
                if (fromParts.length === 0 && joinParts.length === 0) {
                    // No FROM yet, use json_each directly
                    fromParts.push(`json_each(${unwindClause.jsonExpr}) ${unwindClause.alias}`);
                }
                else {
                    // Add as a cross join
                    joinParts.push(`CROSS JOIN json_each(${unwindClause.jsonExpr}) ${unwindClause.alias}`);
                }
                exprParams.push(...unwindClause.params);
            }
        }
        // Add WHERE conditions from MATCH
        const matchWhereClause = this.ctx.whereClause;
        if (matchWhereClause) {
            const { sql: whereSql, params: conditionParams } = this.translateWhere(matchWhereClause);
            whereParts.push(whereSql);
            whereParams.push(...conditionParams);
        }
        // Add WHERE conditions from OPTIONAL MATCH
        // These should be applied as: (optional_var IS NULL OR condition)
        // This ensures the main row is still returned even if the optional match fails the WHERE
        const optionalWhereClauses = this.ctx.optionalWhereClauses;
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
                }
                else {
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
        }
        else if (joinParts.length > 0) {
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
        // Add GROUP BY for aggregation (from WITH or RETURN clauses)
        // When we have aggregates mixed with non-aggregates, non-aggregate expressions become GROUP BY keys
        const groupByParts = [];
        // Check WITH clause for aggregation
        if (this.ctx.withClauses && this.ctx.withClauses.length > 0) {
            const lastWithClause = this.ctx.withClauses[this.ctx.withClauses.length - 1];
            const withHasAggregates = lastWithClause.items.some(item => this.isAggregateExpression(item.expression));
            if (withHasAggregates) {
                for (const item of lastWithClause.items) {
                    if (!this.isAggregateExpression(item.expression)) {
                        // Translate the non-aggregate expression and add to GROUP BY
                        const { sql: exprSql } = this.translateExpression(item.expression);
                        groupByParts.push(exprSql);
                    }
                }
            }
        }
        // Check RETURN clause for aggregation (when no WITH with aggregation)
        if (groupByParts.length === 0) {
            const returnHasAggregates = clause.items.some(item => this.isAggregateExpression(item.expression));
            const nonAggregateItems = clause.items.filter(item => !this.isAggregateExpression(item.expression));
            if (returnHasAggregates && nonAggregateItems.length > 0) {
                for (const item of nonAggregateItems) {
                    const { sql: exprSql } = this.translateExpression(item.expression);
                    groupByParts.push(exprSql);
                }
            }
        }
        if (groupByParts.length > 0) {
            sql += ` GROUP BY ${groupByParts.join(", ")}`;
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
            }
            else if (effectiveSkip !== undefined) {
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
    translateWith(clause) {
        // WITH clause stores its info in context for subsequent clauses
        // It creates a new "scope" by updating variable mappings
        if (!this.ctx.withClauses) {
            this.ctx.withClauses = [];
        }
        this.ctx.withClauses.push(clause);
        // Store where clause for later use
        if (clause.where) {
            this.ctx.withWhere = clause.where;
        }
        // Store ORDER BY, SKIP, LIMIT for later use  
        if (clause.orderBy) {
            this.ctx.withOrderBy = clause.orderBy;
        }
        if (clause.skip !== undefined) {
            this.ctx.withSkip = clause.skip;
        }
        if (clause.limit !== undefined) {
            this.ctx.withLimit = clause.limit;
        }
        if (clause.distinct) {
            this.ctx.withDistinct = true;
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
                const originalVar = item.expression.variable;
                const originalInfo = this.ctx.variables.get(originalVar);
                if (originalInfo && alias) {
                    this.ctx.variables.set(alias, originalInfo);
                    // Preserve optional flag for the alias
                    const originalIsOptional = this.ctx[`optional_${originalInfo.alias}`];
                    if (originalIsOptional) {
                        this.ctx[`optional_${alias}`] = true;
                    }
                }
            }
            else if (alias) {
                // For any other expression type with an alias (property, function, literal, object, binary, etc.)
                // we track it as a "virtual" variable for the return/unwind phase
                if (!this.ctx.withAliases) {
                    this.ctx.withAliases = new Map();
                }
                this.ctx.withAliases.set(alias, item.expression);
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
    translateReturnFromCall(clause, callClause) {
        const params = [];
        const returnColumns = [];
        // Build SELECT parts
        const selectParts = [];
        for (const item of clause.items) {
            // For CALL, variables reference the yield column
            let exprSql;
            if (item.expression.type === "variable") {
                // Check if this variable is a yield variable
                const yieldRef = this.ctx[`call_yield_${item.expression.variable}`];
                if (yieldRef) {
                    exprSql = yieldRef;
                }
                else {
                    throw new Error(`Unknown variable: ${item.expression.variable}`);
                }
            }
            else {
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
        const whereParts = [];
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
            const orderParts = [];
            for (const order of clause.orderBy) {
                let orderSql;
                if (order.expression.type === "variable") {
                    const yieldRef = this.ctx[`call_yield_${order.expression.variable}`];
                    if (yieldRef) {
                        orderSql = yieldRef;
                    }
                    else {
                        throw new Error(`Unknown variable: ${order.expression.variable}`);
                    }
                }
                else {
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
    translateVariableLengthPath(clause, relPatterns, selectParts, returnColumns, exprParams, whereParams) {
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
        const maxHops = varLengthPattern.maxHops ?? 10;
        const edgeType = varLengthPattern.edge.type;
        const edgeProperties = varLengthPattern.edge.properties;
        const varLengthSourceAlias = varLengthPattern.sourceAlias;
        const varLengthTargetAlias = varLengthPattern.targetAlias;
        const allParams = [...exprParams];
        // Build edge property conditions for variable-length paths
        // These conditions need to be applied to every edge in the path
        const edgePropConditions = [];
        const edgePropParams = [];
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
        let pathCteName;
        const pathExpressions = this.ctx.pathExpressions;
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
        let cte;
        // Check if this is an undirected pattern
        const isUndirected = varLengthPattern.edge.direction === "none";
        if (minHops === 0 && maxHops === 0) {
            // Special case: *0 means zero-length path, source = target
            // No CTE needed - we'll handle this by making source = target
            cte = "";
        }
        else if (minHops === 0) {
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
                }
                else {
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
            }
            else {
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
        }
        else {
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
                }
                else {
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
            }
            else {
                cte = `WITH RECURSIVE ${pathCteName}(start_id, end_id, depth, edge_ids) AS (
  SELECT source_id, target_id, 1, json_array(json_object('id', id, 'type', type, 'source_id', source_id, 'target_id', target_id, 'properties', json(properties))) FROM edges WHERE ${edgeCondition}${basePropCondition}
  UNION ALL
  SELECT p.start_id, e.target_id, p.depth + 1, json_insert(p.edge_ids, '$[#]', json_object('id', e.id, 'type', e.type, 'source_id', e.source_id, 'target_id', e.target_id, 'properties', json(e.properties)))
  FROM ${pathCteName} p
  JOIN edges e ON p.end_id = e.source_id
  WHERE p.depth < ?${edgeType ? " AND e.type = ?" : ""}${recursivePropCondition}
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
        const fromParts = [];
        const joinParts = [];
        const joinParams = [];
        const whereParts = [];
        const addedNodeAliases = new Set();
        const addedEdgeAliases = new Set();
        const filteredNodeAliases = new Set();
        // Deferred WHERE params - these are added after all CTE params
        // This is needed because CTEs are defined before the WHERE clause in SQL
        const deferredWhereParams = [];
        // Process fixed-length patterns before the variable-length pattern
        for (let i = 0; i < fixedPatternsBefore.length; i++) {
            const pattern = fixedPatternsBefore[i];
            if (i === 0) {
                // First pattern: add source to FROM
                fromParts.push(`nodes ${pattern.sourceAlias}`);
                addedNodeAliases.add(pattern.sourceAlias);
                // Add source label/property filters
                const sourcePattern = this.ctx[`pattern_${pattern.sourceAlias}`];
                if (sourcePattern?.label) {
                    const labelMatch = this.generateLabelMatchCondition(pattern.sourceAlias, sourcePattern.label);
                    whereParts.push(labelMatch.sql);
                    allParams.push(...labelMatch.params);
                }
                filteredNodeAliases.add(pattern.sourceAlias);
            }
            // Add edge JOIN (only if not already added - handles bound relationships)
            if (!addedEdgeAliases.has(pattern.edgeAlias)) {
                const isUndirectedPattern = pattern.edge.direction === "none";
                if (isUndirectedPattern) {
                    // For undirected patterns, match edges in either direction
                    joinParts.push(`JOIN edges ${pattern.edgeAlias} ON (${pattern.edgeAlias}.source_id = ${pattern.sourceAlias}.id OR ${pattern.edgeAlias}.target_id = ${pattern.sourceAlias}.id)`);
                }
                else {
                    joinParts.push(`JOIN edges ${pattern.edgeAlias} ON ${pattern.edgeAlias}.source_id = ${pattern.sourceAlias}.id`);
                }
                addedEdgeAliases.add(pattern.edgeAlias);
                // Add edge type filter
                if (pattern.edge.type) {
                    whereParts.push(`${pattern.edgeAlias}.type = ?`);
                    allParams.push(pattern.edge.type);
                }
            }
            // Add target node JOIN
            if (!addedNodeAliases.has(pattern.targetAlias)) {
                const isUndirectedPattern = pattern.edge.direction === "none";
                if (isUndirectedPattern) {
                    // For undirected, target could be on either side
                    joinParts.push(`JOIN nodes ${pattern.targetAlias} ON (${pattern.edgeAlias}.target_id = ${pattern.targetAlias}.id OR ${pattern.edgeAlias}.source_id = ${pattern.targetAlias}.id)`);
                    // Also ensure source and target are different (for undirected edges)
                    whereParts.push(`${pattern.sourceAlias}.id != ${pattern.targetAlias}.id`);
                }
                else {
                    joinParts.push(`JOIN nodes ${pattern.targetAlias} ON ${pattern.edgeAlias}.target_id = ${pattern.targetAlias}.id`);
                }
                addedNodeAliases.add(pattern.targetAlias);
            }
        }
        // Now add the variable-length path
        // The source of the variable-length pattern should connect to the path start
        if (!addedNodeAliases.has(varLengthSourceAlias)) {
            if (fromParts.length === 0) {
                fromParts.push(`nodes ${varLengthSourceAlias}`);
            }
            else {
                joinParts.push(`JOIN nodes ${varLengthSourceAlias} ON 1=1`);
            }
            addedNodeAliases.add(varLengthSourceAlias);
            // Add label/property filters for the source - deferred until after CTE params
            const sourcePattern = this.ctx[`pattern_${varLengthSourceAlias}`];
            if (sourcePattern?.label && !filteredNodeAliases.has(varLengthSourceAlias)) {
                const labelMatch = this.generateLabelMatchCondition(varLengthSourceAlias, sourcePattern.label);
                whereParts.push(labelMatch.sql);
                deferredWhereParams.push(...labelMatch.params);
                filteredNodeAliases.add(varLengthSourceAlias);
            }
            // Add source property filters - deferred until after CTE params
            if (sourcePattern?.properties) {
                for (const [key, value] of Object.entries(sourcePattern.properties)) {
                    if (this.isParameterRef(value)) {
                        whereParts.push(`json_extract(${varLengthSourceAlias}.properties, '$.${key}') = ?`);
                        deferredWhereParams.push(this.ctx.paramValues[value.name]);
                    }
                    else {
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
        }
        else {
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
        }
        // Add target label/property filters for the variable-length pattern - deferred until after CTE params
        const targetPattern = this.ctx[`pattern_${varLengthTargetAlias}`];
        if (targetPattern?.label && !filteredNodeAliases.has(varLengthTargetAlias)) {
            const labelMatch = this.generateLabelMatchCondition(varLengthTargetAlias, targetPattern.label);
            whereParts.push(labelMatch.sql);
            deferredWhereParams.push(...labelMatch.params);
            filteredNodeAliases.add(varLengthTargetAlias);
        }
        if (targetPattern?.properties) {
            for (const [key, value] of Object.entries(targetPattern.properties)) {
                if (this.isParameterRef(value)) {
                    whereParts.push(`json_extract(${varLengthTargetAlias}.properties, '$.${key}') = ?`);
                    deferredWhereParams.push(this.ctx.paramValues[value.name]);
                }
                else {
                    whereParts.push(`json_extract(${varLengthTargetAlias}.properties, '$.${key}') = ?`);
                    deferredWhereParams.push(value);
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
                const maxHops2 = pattern.maxHops ?? 10;
                const edgeType2 = pattern.edge.type;
                const isUndirected2 = pattern.edge.direction === "none";
                const pathCteName2 = `path_${this.ctx.aliasCounter++}`;
                // Build second CTE
                let cte2 = "";
                if (minHops2 === 0 && maxHops2 === 0) {
                    cte2 = "";
                }
                else if (minHops2 === 0) {
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
                        }
                        else {
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
                    }
                    else {
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
                        }
                        else {
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
                }
                else {
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
                        }
                        else {
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
                    }
                    else {
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
                        }
                        else {
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
                const targetPattern2 = this.ctx[`pattern_${pattern.targetAlias}`];
                if (targetPattern2?.label && !filteredNodeAliases.has(pattern.targetAlias)) {
                    const labelMatch = this.generateLabelMatchCondition(pattern.targetAlias, targetPattern2.label);
                    whereParts.push(labelMatch.sql);
                    deferredWhereParams.push(...labelMatch.params);
                    filteredNodeAliases.add(pattern.targetAlias);
                }
                if (targetPattern2?.properties) {
                    for (const [key, value] of Object.entries(targetPattern2.properties)) {
                        if (this.isParameterRef(value)) {
                            whereParts.push(`json_extract(${pattern.targetAlias}.properties, '$.${key}') = ?`);
                            deferredWhereParams.push(this.ctx.paramValues[value.name]);
                        }
                        else {
                            whereParts.push(`json_extract(${pattern.targetAlias}.properties, '$.${key}') = ?`);
                            deferredWhereParams.push(value);
                        }
                    }
                }
                currentSourceAlias = pattern.targetAlias;
            }
            else {
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
                }
                else {
                    // Edge already joined - this is a bound relationship from an earlier MATCH
                    // The current source and the target alias should be the edge's actual endpoints
                    // Since the pattern is undirected ()-[r]-(), we don't know which endpoint is which,
                    // so we use OR conditions and let the database figure out valid combinations
                    if (pattern.boundEdgeOriginalPattern) {
                        // Constrain the source to be one of the original endpoints
                        const origPattern = pattern.boundEdgeOriginalPattern;
                        whereParts.push(`(${currentSourceAlias}.id = ${origPattern.sourceAlias}.id OR ${currentSourceAlias}.id = ${origPattern.targetAlias}.id)`);
                    }
                    else {
                        whereParts.push(`(${pattern.edgeAlias}.source_id = ${currentSourceAlias}.id OR ${pattern.edgeAlias}.target_id = ${currentSourceAlias}.id)`);
                    }
                }
                if (!addedNodeAliases.has(pattern.targetAlias)) {
                    // For bound edges, the target should be the OTHER endpoint of the edge
                    if (pattern.boundEdgeOriginalPattern) {
                        const origPattern = pattern.boundEdgeOriginalPattern;
                        // Target should be the opposite endpoint from source
                        // Since source can be either original endpoint, we need an XOR-like condition
                        joinParts.push(`JOIN nodes ${pattern.targetAlias} ON ((${currentSourceAlias}.id = ${origPattern.sourceAlias}.id AND ${pattern.targetAlias}.id = ${origPattern.targetAlias}.id) OR (${currentSourceAlias}.id = ${origPattern.targetAlias}.id AND ${pattern.targetAlias}.id = ${origPattern.sourceAlias}.id))`);
                    }
                    else {
                        joinParts.push(`JOIN nodes ${pattern.targetAlias} ON ${pattern.edgeAlias}.target_id = ${pattern.targetAlias}.id`);
                    }
                    addedNodeAliases.add(pattern.targetAlias);
                    // Target label/property filters - deferred until after all CTE params
                    const afterTargetPattern = this.ctx[`pattern_${pattern.targetAlias}`];
                    if (afterTargetPattern?.label && !filteredNodeAliases.has(pattern.targetAlias)) {
                        const labelMatch = this.generateLabelMatchCondition(pattern.targetAlias, afterTargetPattern.label);
                        whereParts.push(labelMatch.sql);
                        deferredWhereParams.push(...labelMatch.params);
                        filteredNodeAliases.add(pattern.targetAlias);
                    }
                    if (afterTargetPattern?.properties) {
                        for (const [key, value] of Object.entries(afterTargetPattern.properties)) {
                            if (this.isParameterRef(value)) {
                                whereParts.push(`json_extract(${pattern.targetAlias}.properties, '$.${key}') = ?`);
                                deferredWhereParams.push(this.ctx.paramValues[value.name]);
                            }
                            else {
                                whereParts.push(`json_extract(${pattern.targetAlias}.properties, '$.${key}') = ?`);
                                deferredWhereParams.push(value);
                            }
                        }
                    }
                }
                currentSourceAlias = pattern.targetAlias;
            }
        }
        // Now add the deferred WHERE params (after all CTE params have been added)
        allParams.push(...deferredWhereParams);
        // Add WHERE clause from MATCH if present
        const matchWhereClause = this.ctx.whereClause;
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
            const groupByParts = [];
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
            }
            else if (clause.skip !== undefined) {
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
    translateUnion(clause) {
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
    translateUnwind(clause) {
        // UNWIND expands an array into rows using SQLite's json_each()
        // We store the unwind info in context for use in RETURN
        const alias = `unwind${this.ctx.aliasCounter++}`;
        // Store the unwind information for later use
        if (!this.ctx.unwindClauses) {
            this.ctx.unwindClauses = [];
        }
        // Determine the expression for json_each
        let jsonExpr = "";
        let params = [];
        if (clause.expression.type === "literal") {
            // Handle null literal - produces empty result
            if (clause.expression.value === null) {
                jsonExpr = "json_array()"; // Empty array produces no rows
                // No params needed
            }
            else {
                // Literal array - serialize to JSON
                jsonExpr = "?";
                params.push(JSON.stringify(clause.expression.value));
            }
        }
        else if (clause.expression.type === "parameter") {
            // Parameter - will be resolved at runtime
            jsonExpr = "?";
            const paramValue = this.ctx.paramValues[clause.expression.name];
            params.push(JSON.stringify(paramValue));
        }
        else if (clause.expression.type === "variable") {
            // Variable reference - could be from WITH/COLLECT
            const varName = clause.expression.variable;
            const withAliases = this.ctx.withAliases;
            if (withAliases && withAliases.has(varName)) {
                // It's a WITH alias - need to inline the expression
                const originalExpr = withAliases.get(varName);
                const translated = this.translateExpression(originalExpr);
                jsonExpr = translated.sql;
                params.push(...translated.params);
            }
            else {
                // Check if it's a previous UNWIND variable
                const unwindClauses = this.ctx.unwindClauses;
                const unwindClause = unwindClauses?.find(u => u.variable === varName);
                if (unwindClause) {
                    // It's a previous UNWIND variable - use .value to get the nested array
                    jsonExpr = `${unwindClause.alias}.value`;
                    // No additional params needed for this case
                }
                else {
                    // It's a regular variable
                    const varInfo = this.ctx.variables.get(varName);
                    if (varInfo) {
                        jsonExpr = `${varInfo.alias}.properties`;
                    }
                    else {
                        throw new Error(`Unknown variable in UNWIND: ${varName}`);
                    }
                }
            }
        }
        else if (clause.expression.type === "property") {
            // Property access on a variable
            const varInfo = this.ctx.variables.get(clause.expression.variable);
            if (!varInfo) {
                throw new Error(`Unknown variable: ${clause.expression.variable}`);
            }
            jsonExpr = `json_extract(${varInfo.alias}.properties, '$.${clause.expression.property}')`;
        }
        else if (clause.expression.type === "function" || clause.expression.type === "binary") {
            // Function call like range(1, 10) or binary expression like (first + second)
            const translated = this.translateExpression(clause.expression);
            jsonExpr = translated.sql;
            params.push(...translated.params);
        }
        else {
            throw new Error(`Unsupported expression type in UNWIND: ${clause.expression.type}`);
        }
        this.ctx.unwindClauses.push({
            alias,
            variable: clause.alias,
            jsonExpr,
            params,
        });
        // Register the unwind alias as a variable for subsequent use
        // We use 'unwind' as the type to distinguish it from nodes/edges
        this.ctx.variables.set(clause.alias, { type: "node", alias }); // Using 'node' as a placeholder type
        // Store special marker that this is an unwind variable
        this.ctx[`unwind_${alias}`] = true;
        // UNWIND doesn't generate SQL directly - it sets up context for RETURN
        return [];
    }
    translateCall(clause) {
        // CALL procedures for database introspection
        // Supported procedures:
        // - db.labels() - returns all distinct node labels
        // - db.relationshipTypes() - returns all distinct relationship types
        const procedure = clause.procedure.toLowerCase();
        let tableName;
        let columnName;
        let returnColumn;
        const params = [];
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
        this.ctx.callClause = {
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
                this.ctx[`call_yield_${yieldVar}`] = columnName;
            }
        }
        else {
            // Default yield variable matches the return column
            this.ctx[`call_yield_${returnColumn}`] = columnName;
        }
        // Don't generate SQL here - let translateReturn handle it if there's a RETURN clause
        // Only generate standalone SQL if there's no RETURN clause following
        return {
            statements: [],
            returnColumns: [returnColumn],
        };
    }
    translateCallWhere(condition, yieldColumn) {
        const params = [];
        switch (condition.type) {
            case "comparison": {
                // Handle comparisons like "label <> 'SystemNode'"
                let leftSql;
                if (condition.left?.type === "variable" && condition.left.variable === yieldColumn) {
                    leftSql = yieldColumn;
                }
                else if (condition.left) {
                    const leftResult = this.translateExpressionForCall(condition.left, yieldColumn);
                    leftSql = leftResult.sql;
                    params.push(...leftResult.params);
                }
                else {
                    throw new Error("Missing left side of comparison");
                }
                let rightSql;
                if (condition.right) {
                    const rightResult = this.translateExpressionForCall(condition.right, yieldColumn);
                    rightSql = rightResult.sql;
                    params.push(...rightResult.params);
                }
                else {
                    throw new Error("Missing right side of comparison");
                }
                return { sql: `${leftSql} ${condition.operator} ${rightSql}`, params };
            }
            case "and": {
                const parts = condition.conditions.map(c => this.translateCallWhere(c, yieldColumn));
                const sql = parts.map(p => `(${p.sql})`).join(" AND ");
                for (const p of parts)
                    params.push(...p.params);
                return { sql, params };
            }
            case "or": {
                const parts = condition.conditions.map(c => this.translateCallWhere(c, yieldColumn));
                const sql = parts.map(p => `(${p.sql})`).join(" OR ");
                for (const p of parts)
                    params.push(...p.params);
                return { sql, params };
            }
            case "not": {
                const inner = this.translateCallWhere(condition.condition, yieldColumn);
                return { sql: `NOT (${inner.sql})`, params: inner.params };
            }
            default:
                throw new Error(`Unsupported condition type in CALL WHERE: ${condition.type}`);
        }
    }
    translateExpressionForCall(expr, yieldColumn) {
        const params = [];
        switch (expr.type) {
            case "variable":
                // If the variable matches the yield column, use the column name directly
                if (expr.variable === yieldColumn) {
                    return { sql: yieldColumn, params };
                }
                // Check if it's a yield variable
                const yieldRef = this.ctx[`call_yield_${expr.variable}`];
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
                const paramValue = this.ctx.paramValues[expr.name];
                return { sql: "?", params: [paramValue] };
            default:
                throw new Error(`Unsupported expression type in CALL WHERE: ${expr.type}`);
        }
    }
    translateExpression(expr) {
        const tables = [];
        const params = [];
        switch (expr.type) {
            case "variable": {
                // First check if this is a WITH alias
                const withAliases = this.ctx.withAliases;
                if (withAliases && withAliases.has(expr.variable)) {
                    // This variable is actually an alias from WITH - translate the underlying expression
                    const originalExpr = withAliases.get(expr.variable);
                    return this.translateExpression(originalExpr);
                }
                // Check if this is a CALL yield variable
                const callYieldRef = this.ctx[`call_yield_${expr.variable}`];
                if (callYieldRef) {
                    // This variable comes from a CALL...YIELD clause
                    return { sql: callYieldRef, tables, params };
                }
                // Check if this is an UNWIND variable
                const unwindClauses = this.ctx.unwindClauses;
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
                const varInfo = this.ctx.variables.get(expr.variable);
                if (!varInfo) {
                    throw new Error(`Unknown variable: ${expr.variable}`);
                }
                // Handle path variables
                if (varInfo.type === "path") {
                    const pathExpressions = this.ctx.pathExpressions;
                    if (pathExpressions) {
                        const pathInfo = pathExpressions.find(p => p.variable === expr.variable);
                        if (pathInfo) {
                            // Neo4j 3.5 format: paths are alternating arrays [node, edge, node, edge, node, ...]
                            // Each element is just the properties object
                            // For variable-length paths, we only have start/end nodes in the CTE
                            if (pathInfo.isVariableLength) {
                                tables.push(...pathInfo.nodeAliases);
                                // Build alternating array with just properties
                                // For variable-length, we only have start/end nodes, no intermediate edges
                                const elements = pathInfo.nodeAliases.map(alias => `${alias}.properties`);
                                return {
                                    sql: `json_array(${elements.join(', ')})`,
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
                            const elements = [];
                            for (let i = 0; i < nodeSeq.length; i++) {
                                elements.push(`${nodeSeq[i]}.properties`);
                                if (i < pathInfo.edgeAliases.length) {
                                    elements.push(`${pathInfo.edgeAliases[i]}.properties`);
                                }
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
                    return {
                        sql: `CASE WHEN ${varInfo.alias}.id IS NULL THEN NULL ELSE json_set(COALESCE(${varInfo.alias}.properties, '{}'), '$._nf_id', ${varInfo.alias}.id) END`,
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
                return {
                    sql: `CASE WHEN ${varInfo.alias}.id IS NULL THEN NULL ELSE json_set(COALESCE(${varInfo.alias}.properties, '{}'), '$._nf_id', ${varInfo.alias}.id) END`,
                    tables,
                    params,
                };
            }
            case "property": {
                const varInfo = this.ctx.variables.get(expr.variable);
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
                            const varInfo = this.ctx.variables.get(arg.variable);
                            if (!varInfo) {
                                throw new Error(`Unknown variable: ${arg.variable}`);
                            }
                            tables.push(varInfo.alias);
                            return {
                                sql: `COUNT(${distinctKeyword}json_extract(${varInfo.alias}.properties, '$.${arg.property}'))`,
                                tables,
                                params,
                            };
                        }
                        else if (arg.type === "variable") {
                            const varInfo = this.ctx.variables.get(arg.variable);
                            if (!varInfo) {
                                throw new Error(`Unknown variable: ${arg.variable}`);
                            }
                            // For path variables, count rows (each row is a distinct path)
                            if (varInfo.type === "path") {
                                // Find path info to get the first node alias to count by
                                const pathExpressions = this.ctx.pathExpressions;
                                if (pathExpressions) {
                                    const pathInfo = pathExpressions.find(p => p.variable === arg.variable);
                                    if (pathInfo && pathInfo.nodeAliases.length > 0) {
                                        // Count by the first node's id to represent path count
                                        const firstNodeAlias = pathInfo.nodeAliases[0];
                                        tables.push(firstNodeAlias);
                                        // Use COUNT(*) for paths - each row is a distinct path match
                                        return {
                                            sql: `COUNT(${distinctKeyword}*)`,
                                            tables,
                                            params,
                                        };
                                    }
                                }
                                // Fallback for paths without node info
                                return { sql: `COUNT(${distinctKeyword}*)`, tables, params };
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
                        const varInfo = this.ctx.variables.get(expr.args[0].variable);
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
                            const varInfo = this.ctx.variables.get(arg.variable);
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
                        }
                        else if (arg.type === "variable") {
                            // Check if this is an UNWIND variable first
                            const unwindClauses = this.ctx.unwindClauses;
                            if (unwindClauses) {
                                const unwindClause = unwindClauses.find(u => u.variable === arg.variable);
                                if (unwindClause) {
                                    tables.push(unwindClause.alias);
                                    // For UNWIND variables, aggregate the value from json_each
                                    return {
                                        sql: `${expr.functionName}(${distinctKeyword}${unwindClause.alias}.value)`,
                                        tables,
                                        params,
                                    };
                                }
                            }
                            const varInfo = this.ctx.variables.get(arg.variable);
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
                        else if (arg.type === "function" || arg.type === "binary") {
                            // Handle aggregates on expressions like sum(n.x * n.y) or min(length(p))
                            const argResult = this.translateFunctionArg(arg);
                            tables.push(...argResult.tables);
                            params.push(...argResult.params);
                            return {
                                sql: `${expr.functionName}(${distinctKeyword}${argResult.sql})`,
                                tables,
                                params,
                            };
                        }
                        else if (arg.type === "literal") {
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
                        // Get the value expression (property access)
                        let valueExpr;
                        if (valueArg.type === "property") {
                            const varInfo = this.ctx.variables.get(valueArg.variable);
                            if (!varInfo) {
                                throw new Error(`Unknown variable: ${valueArg.variable}`);
                            }
                            tables.push(varInfo.alias);
                            valueExpr = `json_extract(${varInfo.alias}.properties, '$.${valueArg.property}')`;
                        }
                        else {
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
                        }
                        else {
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
                            const varInfo = this.ctx.variables.get(arg.variable);
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
                                return {
                                    sql: `COALESCE(json('[' || GROUP_CONCAT(DISTINCT CASE WHEN ${extractExpr} IS NOT NULL THEN json_quote(${extractExpr}) END) || ']'), json('[]'))`,
                                    tables,
                                    params,
                                };
                            }
                            return {
                                sql: `json_group_array(json_extract(${varInfo.alias}.properties, '$.${arg.property}'))`,
                                tables,
                                params,
                            };
                        }
                        else if (arg.type === "variable") {
                            // Check if this is an UNWIND variable (scalar values from json_each)
                            const unwindClauses = this.ctx.unwindClauses;
                            if (unwindClauses) {
                                const unwindClause = unwindClauses.find(u => u.variable === arg.variable);
                                if (unwindClause) {
                                    tables.push(unwindClause.alias);
                                    // For UNWIND variables, collect the raw values from json_each
                                    if (useDistinct) {
                                        // Filter nulls using CASE WHEN ... IS NOT NULL
                                        return {
                                            sql: `COALESCE(json('[' || GROUP_CONCAT(DISTINCT CASE WHEN ${unwindClause.alias}.value IS NOT NULL THEN json_quote(${unwindClause.alias}.value) END) || ']'), json('[]'))`,
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
                            const varInfo = this.ctx.variables.get(arg.variable);
                            if (!varInfo) {
                                throw new Error(`Unknown variable: ${arg.variable}`);
                            }
                            tables.push(varInfo.alias);
                            // Neo4j 3.5 format: collect just the properties objects
                            // Use CASE WHEN to filter nulls
                            return {
                                sql: `json_group_array(CASE WHEN ${varInfo.alias}.id IS NOT NULL THEN json(${varInfo.alias}.properties) END)`,
                                tables,
                                params,
                            };
                        }
                        else if (arg.type === "object") {
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
                            const varInfo = this.ctx.variables.get(arg.variable);
                            if (!varInfo) {
                                throw new Error(`Unknown variable: ${arg.variable}`);
                            }
                            if (varInfo.type === "path") {
                                const pathExpressions = this.ctx.pathExpressions;
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
                            const varInfo = this.ctx.variables.get(arg.variable);
                            if (!varInfo) {
                                throw new Error(`Unknown variable: ${arg.variable}`);
                            }
                            if (varInfo.type === "path") {
                                const pathExpressions = this.ctx.pathExpressions;
                                if (pathExpressions) {
                                    const pathInfo = pathExpressions.find(p => p.variable === arg.variable);
                                    if (pathInfo) {
                                        tables.push(...pathInfo.nodeAliases);
                                        // Neo4j 3.5 format: return array of node properties only
                                        const nodesJson = pathInfo.nodeAliases.map(alias => `json(${alias}.properties)`).join(', ');
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
                            const varInfo = this.ctx.variables.get(arg.variable);
                            if (!varInfo) {
                                throw new Error(`Unknown variable: ${arg.variable}`);
                            }
                            if (varInfo.type === "path") {
                                const pathExpressions = this.ctx.pathExpressions;
                                if (pathExpressions) {
                                    const pathInfo = pathExpressions.find(p => p.variable === arg.variable);
                                    if (pathInfo) {
                                        tables.push(...pathInfo.edgeAliases);
                                        // Neo4j 3.5 format: return array of relationship properties only
                                        const edgesJson = pathInfo.edgeAliases.map(alias => `json(${alias}.properties)`).join(', ');
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
                            const varInfo = this.ctx.variables.get(arg.variable);
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
                            const varInfo = this.ctx.variables.get(arg.variable);
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
                            const varInfo = this.ctx.variables.get(arg.variable);
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
                            const varInfo = this.ctx.variables.get(arg.variable);
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
                        params.push(...strResult.params, // for IS NULL check
                        ...lenResult.params, // for = 0 check
                        ...lenResult.params, // for >= LENGTH check
                        ...strResult.params, // for LENGTH()
                        ...strResult.params, // for THEN branch
                        ...strResult.params, // for ELSE SUBSTR
                        ...lenResult.params // for -length
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
                // LIST: array/list constructor for expressions (used when list contains non-literals)
                if (expr.functionName === "LIST") {
                    if (expr.args && expr.args.length > 0) {
                        const elements = [];
                        for (const arg of expr.args) {
                            const argResult = this.translateExpression(arg);
                            tables.push(...argResult.tables);
                            params.push(...argResult.params);
                            elements.push(argResult.sql);
                        }
                        return { sql: `json_array(${elements.join(", ")})`, tables, params };
                    }
                    return { sql: "json_array()", tables, params };
                }
                // INDEX: list/map element access expr[index]
                if (expr.functionName === "INDEX") {
                    if (expr.args && expr.args.length >= 2) {
                        const listResult = this.translateExpression(expr.args[0]);
                        const indexResult = this.translateExpression(expr.args[1]);
                        tables.push(...listResult.tables, ...indexResult.tables);
                        params.push(...listResult.params, ...indexResult.params);
                        // Use json_extract with array index - note: Cypher uses 0-based, SQLite json uses 0-based too
                        // Cast index to integer to avoid "0.0" in JSON path
                        return { sql: `json_extract(${listResult.sql}, '$[' || CAST(${indexResult.sql} AS INTEGER) || ']')`, tables, params };
                    }
                    throw new Error("INDEX requires list and index arguments");
                }
                // SLICE: list slice expr[start..end]
                if (expr.functionName === "SLICE") {
                    if (expr.args && expr.args.length >= 3) {
                        const listResult = this.translateExpression(expr.args[0]);
                        const startResult = this.translateExpression(expr.args[1]);
                        const endResult = this.translateExpression(expr.args[2]);
                        tables.push(...listResult.tables, ...startResult.tables, ...endResult.tables);
                        params.push(...listResult.params, ...startResult.params, ...endResult.params);
                        // For slice, we use a subquery to generate the slice
                        // Cypher slice is [start..end] where end is exclusive
                        // null start means 0, null end means array length
                        const startSql = startResult.sql === "?" && expr.args[1].value === null ? "0" : `CAST(${startResult.sql} AS INTEGER)`;
                        const endSql = endResult.sql === "?" && expr.args[2].value === null
                            ? `json_array_length(${listResult.sql})`
                            : `CAST(${endResult.sql} AS INTEGER)`;
                        // Use subquery with json_each to build sliced array
                        return {
                            sql: `(SELECT json_group_array(j.value) FROM json_each(${listResult.sql}) j WHERE j.key >= ${startSql} AND j.key < ${endSql})`,
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
                // Inline numeric literals to preserve integer division behavior
                // (SQLite treats bound parameters as floats)
                if (typeof value === "number" && Number.isInteger(value)) {
                    return { sql: String(value), tables, params };
                }
                params.push(value);
                return { sql: "?", tables, params };
            }
            case "parameter": {
                params.push(this.ctx.paramValues[expr.name]);
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
            case "labelPredicate": {
                // (n:Label) - returns true/false based on whether node has the label
                const varInfo = this.ctx.variables.get(expr.variable);
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
                const labelChecks = labelsToCheck.map(l => `EXISTS(SELECT 1 FROM json_each(${varInfo.alias}.label) WHERE value = '${l}')`).join(' AND ');
                return {
                    sql: `(${labelChecks})`,
                    tables,
                    params,
                };
            }
            case "propertyAccess": {
                // Chained property access: obj.prop1.prop2
                // Recursively translate the object expression, then access the property
                const objectResult = this.translateExpression(expr.object);
                tables.push(...objectResult.tables);
                params.push(...objectResult.params);
                // Access property from the result using json_extract
                return {
                    sql: `json_extract(${objectResult.sql}, '$.${expr.property}')`,
                    tables,
                    params,
                };
            }
            default:
                throw new Error(`Unknown expression type: ${expr.type}`);
        }
    }
    translateCaseExpression(expr) {
        const tables = [];
        const params = [];
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
    translateBinaryExpression(expr) {
        const tables = [];
        const params = [];
        const leftResult = this.translateExpression(expr.left);
        const rightResult = this.translateExpression(expr.right);
        tables.push(...leftResult.tables, ...rightResult.tables);
        params.push(...leftResult.params, ...rightResult.params);
        // Check if this is list concatenation (+ operator with arrays)
        const leftIsList = this.isListExpression(expr.left);
        const rightIsList = this.isListExpression(expr.right);
        if (expr.operator === "+" && leftIsList && rightIsList) {
            // Both are lists: list + list concatenation
            // Pattern: (SELECT json_group_array(value) FROM (SELECT value FROM json_each(left) UNION ALL SELECT value FROM json_each(right)))
            const leftArraySql = this.wrapForArray(expr.left, leftResult.sql);
            const rightArraySql = this.wrapForArray(expr.right, rightResult.sql);
            return {
                sql: `(SELECT json_group_array(value) FROM (SELECT value FROM json_each(${leftArraySql}) UNION ALL SELECT value FROM json_each(${rightArraySql})))`,
                tables,
                params,
            };
        }
        if (expr.operator === "+" && leftIsList && !rightIsList) {
            // list + scalar: append scalar to list
            const leftArraySql = this.wrapForArray(expr.left, leftResult.sql);
            const rightScalarSql = rightResult.sql;
            return {
                sql: `(SELECT json_group_array(value) FROM (SELECT value FROM json_each(${leftArraySql}) UNION ALL SELECT json(${rightScalarSql})))`,
                tables,
                params,
            };
        }
        // For property + literal list (where left is property and right is known list)
        // Must check before scalar+list since property is not detected as list
        if (expr.operator === "+" && expr.left.type === "property" && rightIsList) {
            const leftPropSql = this.wrapForArray(expr.left, leftResult.sql);
            const rightArraySql = this.wrapForArray(expr.right, rightResult.sql);
            return {
                sql: `(SELECT json_group_array(value) FROM (SELECT value FROM json_each(${leftPropSql}) UNION ALL SELECT value FROM json_each(${rightArraySql})))`,
                tables,
                params,
            };
        }
        // For literal list + property (where right is property and left is known list)
        // Must check before list+scalar since property is not detected as list
        if (expr.operator === "+" && leftIsList && expr.right.type === "property") {
            const leftArraySql = this.wrapForArray(expr.left, leftResult.sql);
            const rightPropSql = this.wrapForArray(expr.right, rightResult.sql);
            return {
                sql: `(SELECT json_group_array(value) FROM (SELECT value FROM json_each(${leftArraySql}) UNION ALL SELECT value FROM json_each(${rightPropSql})))`,
                tables,
                params,
            };
        }
        if (expr.operator === "+" && !leftIsList && rightIsList) {
            // scalar + list: prepend scalar to list (only for non-property scalars)
            const leftScalarSql = leftResult.sql;
            const rightArraySql = this.wrapForArray(expr.right, rightResult.sql);
            return {
                sql: `(SELECT json_group_array(value) FROM (SELECT json(${leftScalarSql}) as value UNION ALL SELECT value FROM json_each(${rightArraySql})))`,
                tables,
                params,
            };
        }
        // For property + property, use a WITH subquery to avoid duplicate parameter references
        if (expr.operator === "+" && expr.left.type === "property" && expr.right.type === "property") {
            const leftPropSql = this.wrapForArray(expr.left, leftResult.sql);
            const rightPropSql = this.wrapForArray(expr.right, rightResult.sql);
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
        // Handle logical operators (AND, OR)
        if (expr.operator === "AND" || expr.operator === "OR") {
            return {
                sql: `(${leftResult.sql} ${expr.operator} ${rightResult.sql})`,
                tables,
                params,
            };
        }
        // For property access in arithmetic, we need to use json_extract to get the numeric value
        const leftSql = this.wrapForArithmetic(expr.left, leftResult.sql);
        const rightSql = this.wrapForArithmetic(expr.right, rightResult.sql);
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
    isListExpression(expr) {
        // Check if expression is likely a list/array type
        if (expr.type === "literal" && Array.isArray(expr.value)) {
            return true;
        }
        if (expr.type === "variable") {
            // Check if this variable is a WITH alias that references an array
            const withAliases = this.ctx.withAliases;
            if (withAliases && withAliases.has(expr.variable)) {
                const originalExpr = withAliases.get(expr.variable);
                return this.isListExpression(originalExpr);
            }
        }
        // Note: we cannot assume property access is a list - it could be a number or string.
        // Property access will only be treated as a list if combined with an explicit list literal
        // in translateBinaryExpression.
        if (expr.type === "binary" && expr.operator === "+") {
            // Nested binary + could be chained list concatenation, but only if one side is definitely a list
            return this.isListExpression(expr.left) || this.isListExpression(expr.right);
        }
        if (expr.type === "function") {
            // List-returning functions like collect(), range(), etc.
            const listFunctions = ["COLLECT", "RANGE", "KEYS", "LABELS", "SPLIT", "TAIL"];
            return listFunctions.includes(expr.functionName || "");
        }
        return false;
    }
    wrapForArray(expr, sql) {
        // For property access, use json_extract to get the JSON array
        if (expr.type === "property") {
            const varInfo = this.ctx.variables.get(expr.variable);
            if (varInfo) {
                return `json_extract(${varInfo.alias}.properties, '$.${expr.property}')`;
            }
        }
        return sql;
    }
    wrapForArithmetic(expr, sql) {
        // For property access, the -> operator returns JSON, we need to extract as a number
        if (expr.type === "property") {
            // Replace -> with json_extract for numeric operations
            const varInfo = this.ctx.variables.get(expr.variable);
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
    translateExpressionForCreatedNode(expr, nodeId) {
        const params = [];
        switch (expr.type) {
            case "literal":
                if (Array.isArray(expr.value)) {
                    return this.translateArrayLiteral(expr.value);
                }
                const value = expr.value === true ? 1 : expr.value === false ? 0 : expr.value;
                params.push(value);
                return { sql: "?", params };
            case "property": {
                // Use subquery to get property from the created node
                const propName = expr.property;
                params.push(nodeId);
                return {
                    sql: `(SELECT json_extract(properties, '$.${propName}') FROM nodes WHERE id = ?)`,
                    params,
                };
            }
            case "binary": {
                const leftResult = this.translateExpressionForCreatedNode(expr.left, nodeId);
                const rightResult = this.translateExpressionForCreatedNode(expr.right, nodeId);
                params.push(...leftResult.params, ...rightResult.params);
                // Handle list concatenation
                const leftIsList = this.isListExpression(expr.left);
                const rightIsList = this.isListExpression(expr.right);
                if (expr.operator === "+" && (leftIsList || rightIsList || expr.left.type === "property" || expr.right.type === "property")) {
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
    translateComparisonExpression(expr) {
        const tables = [];
        const params = [];
        // Handle IS NULL / IS NOT NULL (no right side)
        // Wrap in CASE to return JSON boolean true/false instead of SQLite's 0/1
        if (expr.comparisonOperator === "IS NULL" || expr.comparisonOperator === "IS NOT NULL") {
            const leftResult = this.translateExpression(expr.left);
            tables.push(...leftResult.tables);
            params.push(...leftResult.params);
            // For node/edge variables, check .id IS NULL instead of the full json_object
            // (json_object never returns NULL even with NULL column values)
            let leftSql;
            if (expr.left?.type === "variable") {
                const varInfo = this.ctx.variables.get(expr.left.variable);
                if (varInfo && (varInfo.type === "node" || varInfo.type === "edge")) {
                    leftSql = `${varInfo.alias}.id`;
                }
                else {
                    leftSql = this.wrapForComparison(expr.left, leftResult.sql);
                }
            }
            else {
                leftSql = this.wrapForComparison(expr.left, leftResult.sql);
            }
            return {
                sql: `CASE WHEN ${leftSql} ${expr.comparisonOperator} THEN json('true') ELSE json('false') END`,
                tables,
                params,
            };
        }
        const leftResult = this.translateExpression(expr.left);
        const rightResult = this.translateExpression(expr.right);
        tables.push(...leftResult.tables, ...rightResult.tables);
        params.push(...leftResult.params, ...rightResult.params);
        // For property access in comparisons, use json_extract for proper comparison
        const leftSql = this.wrapForComparison(expr.left, leftResult.sql);
        const rightSql = this.wrapForComparison(expr.right, rightResult.sql);
        return {
            sql: `(${leftSql} ${expr.comparisonOperator} ${rightSql})`,
            tables,
            params,
        };
    }
    wrapForComparison(expr, sql) {
        // For property access, use json_extract to get proper value for comparison
        if (expr.type === "property") {
            const varInfo = this.ctx.variables.get(expr.variable);
            if (varInfo) {
                return `json_extract(${varInfo.alias}.properties, '$.${expr.property}')`;
            }
        }
        return sql;
    }
    translateObjectLiteral(expr) {
        const tables = [];
        const params = [];
        // Build json_object() call with key-value pairs
        const keyValuePairs = [];
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
    translateArrayLiteral(values) {
        const tables = [];
        const params = [];
        if (values.length === 0) {
            return { sql: "json_array()", tables, params };
        }
        const valueParts = [];
        for (const value of values) {
            if (typeof value === "string" || typeof value === "number") {
                params.push(value);
                valueParts.push("?");
            }
            else if (typeof value === "boolean") {
                params.push(value ? 1 : 0);
                valueParts.push("?");
            }
            else if (value === null) {
                valueParts.push("NULL");
            }
            else if (Array.isArray(value)) {
                // Nested array
                const nested = this.translateArrayLiteral(value);
                tables.push(...nested.tables);
                params.push(...nested.params);
                valueParts.push(nested.sql);
            }
            else if (this.isParameterRef(value)) {
                params.push(this.ctx.paramValues[value.name]);
                valueParts.push("?");
            }
            else {
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
    translateListComprehension(expr) {
        const tables = [];
        const params = [];
        const variable = expr.variable;
        const listExpr = expr.listExpr;
        const filterCondition = expr.filterCondition;
        const mapExpr = expr.mapExpr;
        // Translate the source list expression
        const listResult = this.translateExpression(listExpr);
        tables.push(...listResult.tables);
        // Wrap the source expression for json_each
        let sourceExpr = listResult.sql;
        if (listExpr.type === "property") {
            // For property access, use json_extract 
            const varInfo = this.ctx.variables.get(listExpr.variable);
            if (varInfo) {
                sourceExpr = `json_extract(${varInfo.alias}.properties, '$.${listExpr.property}')`;
            }
        }
        // Determine what to select: the mapped expression or just the value
        let selectExpr = `__lc__.value`;
        let mapParams = [];
        if (mapExpr) {
            const mapResult = this.translateListComprehensionExpr(mapExpr, variable, "__lc__");
            mapParams = mapResult.params;
            selectExpr = mapResult.sql;
        }
        // Build the WHERE clause if filter is present
        let whereClause = "";
        let filterParams = [];
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
    translateListComprehensionExpr(expr, compVar, tableAlias) {
        const params = [];
        switch (expr.type) {
            case "variable":
                if (expr.variable === compVar) {
                    return { sql: `${tableAlias}.value`, params };
                }
                // Fall through to regular translation
                const varResult = this.translateExpression(expr);
                return { sql: varResult.sql, params: varResult.params };
            case "binary": {
                const left = this.translateListComprehensionExpr(expr.left, compVar, tableAlias);
                const right = this.translateListComprehensionExpr(expr.right, compVar, tableAlias);
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
                params.push(this.ctx.paramValues[expr.name]);
                return { sql: "?", params };
            case "function": {
                // Handle functions like size(x)
                const funcArgs = [];
                for (const arg of expr.args || []) {
                    const argResult = this.translateListComprehensionExpr(arg, compVar, tableAlias);
                    params.push(...argResult.params);
                    funcArgs.push(argResult.sql);
                }
                // Map Cypher functions to SQLite equivalents
                const funcName = expr.functionName;
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
    translateListComprehensionCondition(condition, compVar, tableAlias) {
        const params = [];
        switch (condition.type) {
            case "comparison": {
                const left = this.translateListComprehensionExpr(condition.left, compVar, tableAlias);
                const right = this.translateListComprehensionExpr(condition.right, compVar, tableAlias);
                params.push(...left.params, ...right.params);
                return {
                    sql: `${left.sql} ${condition.operator} ${right.sql}`,
                    params,
                };
            }
            case "and": {
                const parts = condition.conditions.map(c => this.translateListComprehensionCondition(c, compVar, tableAlias));
                return {
                    sql: `(${parts.map(p => p.sql).join(" AND ")})`,
                    params: parts.flatMap(p => p.params),
                };
            }
            case "or": {
                const parts = condition.conditions.map(c => this.translateListComprehensionCondition(c, compVar, tableAlias));
                return {
                    sql: `(${parts.map(p => p.sql).join(" OR ")})`,
                    params: parts.flatMap(p => p.params),
                };
            }
            case "not": {
                const inner = this.translateListComprehensionCondition(condition.condition, compVar, tableAlias);
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
    translateListPredicate(expr) {
        const tables = [];
        const params = [];
        const predicateType = expr.predicateType;
        const variable = expr.variable;
        const listExpr = expr.listExpr;
        const filterCondition = expr.filterCondition;
        // Translate the source list expression
        const listResult = this.translateExpression(listExpr);
        tables.push(...listResult.tables);
        params.push(...listResult.params);
        // Get the list SQL - wrap for array if needed
        const listSql = this.wrapForListPredicate(listExpr, listResult.sql);
        // Translate the filter condition, substituting the list predicate variable with __lp__.value
        const condResult = this.translateListComprehensionCondition(filterCondition, variable, "__lp__");
        params.push(...condResult.params);
        let sql;
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
    wrapForListPredicate(expr, sql) {
        // For property access, use json_extract to get the JSON array
        if (expr.type === "property") {
            const varInfo = this.ctx.variables.get(expr.variable);
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
    translateUnaryExpression(expr) {
        const tables = [];
        const params = [];
        if (expr.operator === "NOT") {
            const operandResult = this.translateExpression(expr.operand);
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
    translateWhere(condition) {
        switch (condition.type) {
            case "comparison": {
                const left = this.translateWhereExpression(condition.left);
                const right = this.translateWhereExpression(condition.right);
                return {
                    sql: `${left.sql} ${condition.operator} ${right.sql}`,
                    params: [...left.params, ...right.params],
                };
            }
            case "and": {
                const parts = condition.conditions.map((c) => this.translateWhere(c));
                return {
                    sql: `(${parts.map((p) => p.sql).join(" AND ")})`,
                    params: parts.flatMap((p) => p.params),
                };
            }
            case "or": {
                const parts = condition.conditions.map((c) => this.translateWhere(c));
                return {
                    sql: `(${parts.map((p) => p.sql).join(" OR ")})`,
                    params: parts.flatMap((p) => p.params),
                };
            }
            case "not": {
                const inner = this.translateWhere(condition.condition);
                return {
                    sql: `NOT (${inner.sql})`,
                    params: inner.params,
                };
            }
            case "contains": {
                const left = this.translateWhereExpression(condition.left);
                const right = this.translateWhereExpression(condition.right);
                return {
                    sql: `${left.sql} LIKE '%' || ${right.sql} || '%'`,
                    params: [...left.params, ...right.params],
                };
            }
            case "startsWith": {
                const left = this.translateWhereExpression(condition.left);
                const right = this.translateWhereExpression(condition.right);
                return {
                    sql: `${left.sql} LIKE ${right.sql} || '%'`,
                    params: [...left.params, ...right.params],
                };
            }
            case "endsWith": {
                const left = this.translateWhereExpression(condition.left);
                const right = this.translateWhereExpression(condition.right);
                return {
                    sql: `${left.sql} LIKE '%' || ${right.sql}`,
                    params: [...left.params, ...right.params],
                };
            }
            case "isNull": {
                const left = this.translateWhereExpression(condition.left);
                return {
                    sql: `${left.sql} IS NULL`,
                    params: left.params,
                };
            }
            case "isNotNull": {
                const left = this.translateWhereExpression(condition.left);
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
                const expr = {
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
    translateExistsCondition(condition) {
        const pattern = condition.pattern;
        if (!pattern) {
            throw new Error("EXISTS condition must have a pattern");
        }
        const params = [];
        let sql;
        if (this.isRelationshipPattern(pattern)) {
            // EXISTS with relationship pattern: EXISTS((n)-[:TYPE]->(m))
            // Generate: EXISTS (SELECT 1 FROM edges e WHERE e.source_id = n.id AND e.type = ? AND ...)
            const rel = pattern;
            // Get the source variable's alias from context
            const sourceVar = rel.source.variable;
            const sourceInfo = sourceVar ? this.ctx.variables.get(sourceVar) : null;
            if (!sourceInfo) {
                throw new Error(`EXISTS pattern references unknown variable: ${sourceVar}`);
            }
            const edgeAlias = `exists_e${this.ctx.aliasCounter++}`;
            const targetAlias = `exists_n${this.ctx.aliasCounter++}`;
            const conditions = [];
            // Connect edge to source node
            if (rel.edge.direction === "left") {
                conditions.push(`${edgeAlias}.target_id = ${sourceInfo.alias}.id`);
            }
            else {
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
                }
                else {
                    fromClause += ` JOIN nodes ${targetAlias} ON ${edgeAlias}.target_id = ${targetAlias}.id`;
                }
                const labelMatch = this.generateLabelMatchCondition(targetAlias, rel.target.label);
                conditions.push(labelMatch.sql);
                params.push(...labelMatch.params);
            }
            sql = `EXISTS (SELECT 1 FROM ${fromClause} WHERE ${conditions.join(" AND ")})`;
        }
        else {
            // EXISTS with node-only pattern: EXISTS((n))
            // This is less common but valid - check if the node variable exists
            const node = pattern;
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
    translateInCondition(condition) {
        const left = this.translateWhereExpression(condition.left);
        const params = [...left.params];
        const listExpr = condition.list;
        if (!listExpr) {
            throw new Error("IN condition must have a list expression");
        }
        if (listExpr.type === "literal" && Array.isArray(listExpr.value)) {
            const values = listExpr.value;
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
            const paramValue = this.ctx.paramValues[listExpr.name];
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
    translateOrderByExpression(expr, returnAliases = []) {
        switch (expr.type) {
            case "property": {
                const varInfo = this.ctx.variables.get(expr.variable);
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
                if (returnAliases.includes(expr.variable)) {
                    return { sql: expr.variable };
                }
                // Check if this is an UNWIND variable
                const unwindClauses = this.ctx.unwindClauses;
                if (unwindClauses) {
                    const unwindClause = unwindClauses.find(u => u.variable === expr.variable);
                    if (unwindClause) {
                        return { sql: `${unwindClause.alias}.value` };
                    }
                }
                const varInfo = this.ctx.variables.get(expr.variable);
                if (!varInfo) {
                    throw new Error(`Unknown variable: ${expr.variable}`);
                }
                return { sql: `${varInfo.alias}.id` };
            }
            case "function": {
                if (expr.functionName === "ID") {
                    if (expr.args && expr.args.length > 0 && expr.args[0].type === "variable") {
                        const varInfo = this.ctx.variables.get(expr.args[0].variable);
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
    translateWhereExpression(expr) {
        switch (expr.type) {
            case "property": {
                const varInfo = this.ctx.variables.get(expr.variable);
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
                let value = this.ctx.paramValues[expr.name];
                // Convert booleans to 1/0 for SQLite
                if (value === true)
                    value = 1;
                else if (value === false)
                    value = 0;
                return { sql: "?", params: [value] };
            }
            case "variable": {
                // First check if this is a WITH alias
                const withAliases = this.ctx.withAliases;
                if (withAliases && withAliases.has(expr.variable)) {
                    // This variable is actually an alias from WITH - translate the underlying expression
                    const originalExpr = withAliases.get(expr.variable);
                    return this.translateWhereExpression(originalExpr);
                }
                // Check if this is an UNWIND variable
                const unwindClauses = this.ctx.unwindClauses;
                if (unwindClauses) {
                    const unwindClause = unwindClauses.find(u => u.variable === expr.variable);
                    if (unwindClause) {
                        // UNWIND variables access the 'value' column from json_each
                        return { sql: `${unwindClause.alias}.value`, params: [] };
                    }
                }
                const varInfo = this.ctx.variables.get(expr.variable);
                if (!varInfo) {
                    throw new Error(`Unknown variable: ${expr.variable}`);
                }
                return { sql: `${varInfo.alias}.id`, params: [] };
            }
            case "binary": {
                const leftResult = this.translateWhereExpression(expr.left);
                const rightResult = this.translateWhereExpression(expr.right);
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
    translateFunctionArg(expr) {
        const tables = [];
        const params = [];
        switch (expr.type) {
            case "property": {
                const varInfo = this.ctx.variables.get(expr.variable);
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
                params.push(this.ctx.paramValues[expr.name]);
                return { sql: "?", tables, params };
            }
            case "variable": {
                const varInfo = this.ctx.variables.get(expr.variable);
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
    isRelationshipPattern(pattern) {
        return "source" in pattern && "edge" in pattern && "target" in pattern;
    }
    /**
     * Generate SQL condition to match labels stored as JSON array.
     * For a single label "Person", checks if label array contains "Person"
     * For multiple labels ["A", "B"], checks if label array contains all of them
     */
    generateLabelMatchCondition(alias, label) {
        const labels = Array.isArray(label) ? label : [label];
        const prefix = alias ? `${alias}.` : "";
        if (labels.length === 1) {
            // Single label: check if it exists in the JSON array
            return {
                sql: `EXISTS (SELECT 1 FROM json_each(${prefix}label) WHERE value = ?)`,
                params: [labels[0]]
            };
        }
        else {
            // Multiple labels: check if all exist in the JSON array
            const conditions = labels.map(() => `EXISTS (SELECT 1 FROM json_each(${prefix}label) WHERE value = ?)`);
            return {
                sql: conditions.join(" AND "),
                params: labels
            };
        }
    }
    /**
     * Normalize label to JSON array string for storage
     */
    normalizeLabelToJson(label) {
        if (!label) {
            return JSON.stringify([]);
        }
        const labelArray = Array.isArray(label) ? label : [label];
        return JSON.stringify(labelArray);
    }
    /**
     * Quote an identifier for use as SQL alias (handles reserved words like FROM, TO)
     */
    quoteAlias(alias) {
        // SQLite uses double quotes for identifiers
        return `"${alias}"`;
    }
    findVariablesInCondition(condition) {
        const vars = [];
        const collectFromExpression = (expr) => {
            if (!expr)
                return;
            if (expr.type === "property" && expr.variable) {
                vars.push(expr.variable);
            }
            else if (expr.type === "variable" && expr.variable) {
                vars.push(expr.variable);
            }
        };
        const collectFromCondition = (cond) => {
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
    isParameterRef(value) {
        return typeof value === "object" && value !== null && "type" in value && value.type === "parameter";
    }
    /**
     * Check if an expression is or contains an aggregate function (COUNT, SUM, AVG, MIN, MAX, COLLECT, PERCENTILEDISC, PERCENTILECONT)
     */
    isAggregateExpression(expr) {
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
            return this.isAggregateExpression(expr.left) || this.isAggregateExpression(expr.right);
        }
        return false;
    }
    serializeProperties(props) {
        const resolved = {};
        const params = [];
        const withAliases = this.ctx.withAliases;
        for (const [key, value] of Object.entries(props)) {
            if (this.isParameterRef(value)) {
                resolved[key] = this.ctx.paramValues[value.name];
            }
            else if (this.isVariableRef(value)) {
                // Check if it's a WITH alias
                const varName = value.name;
                if (withAliases && withAliases.has(varName)) {
                    const originalExpr = withAliases.get(varName);
                    // Evaluate the original expression
                    if (originalExpr.type === "literal") {
                        resolved[key] = originalExpr.value;
                    }
                    else if (originalExpr.type === "parameter") {
                        resolved[key] = this.ctx.paramValues[originalExpr.name];
                    }
                    else {
                        // For complex expressions, try to evaluate
                        try {
                            resolved[key] = this.evaluateExpression(originalExpr);
                        }
                        catch {
                            resolved[key] = value; // Keep as-is if can't evaluate
                        }
                    }
                }
                else {
                    // Check if it's a known variable in context
                    if (this.ctx.variables.has(varName)) {
                        // Variable is bound - this is valid (executor will resolve it)
                        resolved[key] = value;
                    }
                    else {
                        // Undefined variable - throw error
                        throw new Error(`Variable \`${varName}\` not defined`);
                    }
                }
            }
            else {
                resolved[key] = value;
            }
        }
        return { json: JSON.stringify(resolved), params };
    }
    isVariableRef(value) {
        return typeof value === "object" && value !== null &&
            value.type === "variable" &&
            typeof value.name === "string";
    }
    evaluateExpression(expr) {
        switch (expr.type) {
            case "literal":
                return expr.value;
            case "parameter":
                return this.ctx.paramValues[expr.name];
            default:
                throw new Error(`Cannot evaluate expression of type ${expr.type}`);
        }
    }
    getExpressionName(expr) {
        switch (expr.type) {
            case "variable":
                return expr.variable;
            case "property":
                return `${expr.variable}_${expr.property}`;
            case "function":
                return expr.functionName.toLowerCase();
            case "labelPredicate": {
                // For (n:Foo) or (n:Foo:Bar), the column name should be the full expression
                const labels = expr.labels || (expr.label ? [expr.label] : []);
                return `(${expr.variable}:${labels.join(':')})`;
            }
            default:
                return "expr";
        }
    }
    generateId() {
        return crypto.randomUUID();
    }
}
// Convenience function
export function translate(query, params = {}) {
    return new Translator(params).translate(query);
}
//# sourceMappingURL=translator.js.map