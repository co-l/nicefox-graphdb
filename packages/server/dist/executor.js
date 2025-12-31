// Query Executor - Full pipeline: Cypher → Parse → Translate → Execute → Format
import { parse, } from "./parser.js";
import { Translator } from "./translator.js";
// ============================================================================
// Executor
// ============================================================================
export class Executor {
    db;
    constructor(db) {
        this.db = db;
    }
    /**
     * Execute a Cypher query and return formatted results
     */
    execute(cypher, params = {}) {
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
            let rows = [];
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
        }
        catch (error) {
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
    tryUnwindCreateExecution(query, params) {
        const clauses = query.clauses;
        // Find UNWIND and CREATE clauses
        const unwindClauses = [];
        const createClauses = [];
        let returnClause = null;
        for (const clause of clauses) {
            if (clause.type === "UNWIND") {
                unwindClauses.push(clause);
            }
            else if (clause.type === "CREATE") {
                createClauses.push(clause);
            }
            else if (clause.type === "RETURN") {
                returnClause = clause;
            }
            else if (clause.type === "MATCH") {
                // If there's a MATCH, don't handle here
                return null;
            }
        }
        // Only handle if we have both UNWIND and CREATE
        if (unwindClauses.length === 0 || createClauses.length === 0) {
            return null;
        }
        // For each UNWIND, expand the array and execute CREATE
        const results = [];
        // Get the values from the UNWIND expression
        const unwindValues = this.evaluateUnwindExpressions(unwindClauses, params);
        // Generate all combinations (cartesian product) of UNWIND values
        const combinations = this.generateCartesianProduct(unwindValues);
        this.db.transaction(() => {
            for (const combination of combinations) {
                // Build a map of unwind variable -> current value
                const unwindContext = {};
                for (let i = 0; i < unwindClauses.length; i++) {
                    unwindContext[unwindClauses[i].alias] = combination[i];
                }
                // Execute CREATE with the unwind context
                const createdIds = new Map();
                for (const createClause of createClauses) {
                    for (const pattern of createClause.patterns) {
                        if (this.isRelationshipPattern(pattern)) {
                            this.executeCreateRelationshipPatternWithUnwind(pattern, createdIds, params, unwindContext);
                        }
                        else {
                            const id = crypto.randomUUID();
                            const labelJson = this.normalizeLabelToJson(pattern.label);
                            const props = this.resolvePropertiesWithUnwind(pattern.properties || {}, params, unwindContext);
                            this.db.execute("INSERT INTO nodes (id, label, properties) VALUES (?, ?, ?)", [id, labelJson, JSON.stringify(props)]);
                            if (pattern.variable) {
                                createdIds.set(pattern.variable, id);
                            }
                        }
                    }
                }
                // Handle RETURN if present
                if (returnClause) {
                    const resultRow = {};
                    for (const item of returnClause.items) {
                        const alias = item.alias || this.getExpressionName(item.expression);
                        if (item.expression.type === "variable") {
                            const variable = item.expression.variable;
                            const id = createdIds.get(variable);
                            if (id) {
                                const nodeResult = this.db.execute("SELECT id, label, properties FROM nodes WHERE id = ?", [id]);
                                if (nodeResult.rows.length > 0) {
                                    const row = nodeResult.rows[0];
                                    resultRow[alias] = {
                                        id: row.id,
                                        label: this.normalizeLabelForOutput(row.label),
                                        properties: typeof row.properties === "string"
                                            ? JSON.parse(row.properties)
                                            : row.properties,
                                    };
                                }
                            }
                        }
                    }
                    if (Object.keys(resultRow).length > 0) {
                        results.push(resultRow);
                    }
                }
            }
        });
        return results;
    }
    /**
     * Handle MATCH+WITH(COLLECT)+UNWIND+RETURN pattern
     * This requires a subquery for the aggregate function because SQLite doesn't
     * allow aggregate functions directly inside json_each()
     */
    tryCollectUnwindExecution(query, params) {
        const clauses = query.clauses;
        // Find the pattern: MATCH + WITH (containing COLLECT) + UNWIND + RETURN
        let matchClauses = [];
        let withClause = null;
        let unwindClause = null;
        let returnClause = null;
        for (const clause of clauses) {
            if (clause.type === "MATCH" || clause.type === "OPTIONAL_MATCH") {
                matchClauses.push(clause);
            }
            else if (clause.type === "WITH") {
                withClause = clause;
            }
            else if (clause.type === "UNWIND") {
                unwindClause = clause;
            }
            else if (clause.type === "RETURN") {
                returnClause = clause;
            }
            else {
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
        const collectQuery = {
            clauses: [
                ...matchClauses,
                {
                    type: "RETURN",
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
        let collectedValues = [];
        for (const stmt of collectTranslation.statements) {
            const result = this.db.execute(stmt.sql, stmt.params);
            if (result.rows.length > 0) {
                // The result should have a single row with the collected array
                const row = result.rows[0];
                const collected = row[collectAlias];
                if (typeof collected === "string") {
                    try {
                        collectedValues = JSON.parse(collected);
                    }
                    catch {
                        collectedValues = [collected];
                    }
                }
                else if (Array.isArray(collected)) {
                    collectedValues = collected;
                }
            }
        }
        // Build results by expanding the collected values
        const results = [];
        const unwindAlias = unwindClause.alias;
        for (const value of collectedValues) {
            const resultRow = {};
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
     * Evaluate UNWIND expressions to get the arrays to iterate over
     */
    evaluateUnwindExpressions(unwindClauses, params) {
        return unwindClauses.map((clause) => {
            const expr = clause.expression;
            if (expr.type === "literal") {
                return expr.value;
            }
            else if (expr.type === "parameter") {
                return params[expr.name];
            }
            throw new Error(`Unsupported UNWIND expression type: ${expr.type}`);
        });
    }
    /**
     * Generate cartesian product of arrays
     */
    generateCartesianProduct(arrays) {
        if (arrays.length === 0)
            return [[]];
        return arrays.reduce((acc, curr) => {
            const result = [];
            for (const a of acc) {
                for (const c of curr) {
                    result.push([...a, c]);
                }
            }
            return result;
        }, [[]]);
    }
    /**
     * Resolve properties, including unwind variable references
     */
    resolvePropertiesWithUnwind(props, params, unwindContext) {
        const resolved = {};
        for (const [key, value] of Object.entries(props)) {
            if (typeof value === "object" &&
                value !== null &&
                "type" in value) {
                if (value.type === "parameter" && "name" in value) {
                    resolved[key] = params[value.name];
                }
                else if (value.type === "variable" && "name" in value) {
                    // This is an unwind variable reference
                    resolved[key] = unwindContext[value.name];
                }
                else {
                    resolved[key] = value;
                }
            }
            else {
                resolved[key] = value;
            }
        }
        return resolved;
    }
    /**
     * Execute CREATE relationship pattern with unwind context
     */
    executeCreateRelationshipPatternWithUnwind(rel, createdIds, params, unwindContext) {
        let sourceId;
        let targetId;
        // Determine source node ID
        if (rel.source.variable && createdIds.has(rel.source.variable)) {
            sourceId = createdIds.get(rel.source.variable);
        }
        else if (rel.source.variable && !createdIds.has(rel.source.variable) && !rel.source.label) {
            // Variable referenced but not found and no label - error
            throw new Error(`Cannot resolve source node: ${rel.source.variable}`);
        }
        else {
            // Create new source node (with or without label - anonymous nodes are valid)
            sourceId = crypto.randomUUID();
            const props = this.resolvePropertiesWithUnwind(rel.source.properties || {}, params, unwindContext);
            this.db.execute("INSERT INTO nodes (id, label, properties) VALUES (?, ?, ?)", [sourceId, this.normalizeLabelToJson(rel.source.label), JSON.stringify(props)]);
            if (rel.source.variable) {
                createdIds.set(rel.source.variable, sourceId);
            }
        }
        // Determine target node ID
        if (rel.target.variable && createdIds.has(rel.target.variable)) {
            targetId = createdIds.get(rel.target.variable);
        }
        else if (rel.target.variable && !createdIds.has(rel.target.variable) && !rel.target.label) {
            // Variable referenced but not found and no label - error
            throw new Error(`Cannot resolve target node: ${rel.target.variable}`);
        }
        else {
            // Create new target node (with or without label - anonymous nodes are valid)
            targetId = crypto.randomUUID();
            const props = this.resolvePropertiesWithUnwind(rel.target.properties || {}, params, unwindContext);
            this.db.execute("INSERT INTO nodes (id, label, properties) VALUES (?, ?, ?)", [targetId, this.normalizeLabelToJson(rel.target.label), JSON.stringify(props)]);
            if (rel.target.variable) {
                createdIds.set(rel.target.variable, targetId);
            }
        }
        // Swap source/target for left-directed relationships
        const [actualSource, actualTarget] = rel.edge.direction === "left" ? [targetId, sourceId] : [sourceId, targetId];
        // Create edge
        const edgeId = crypto.randomUUID();
        const edgeType = rel.edge.type || "";
        const edgeProps = this.resolvePropertiesWithUnwind(rel.edge.properties || {}, params, unwindContext);
        this.db.execute("INSERT INTO edges (id, type, source_id, target_id, properties) VALUES (?, ?, ?, ?, ?)", [edgeId, edgeType, actualSource, actualTarget, JSON.stringify(edgeProps)]);
        if (rel.edge.variable) {
            createdIds.set(rel.edge.variable, edgeId);
        }
    }
    /**
     * Handle CREATE...RETURN pattern by creating nodes/edges and then querying them back
     */
    tryCreateReturnExecution(query, params) {
        // Check if this is a CREATE...RETURN pattern (no MATCH)
        const clauses = query.clauses;
        // Must have at least CREATE and RETURN
        if (clauses.length < 2)
            return null;
        // Find CREATE and RETURN clauses
        const createClauses = [];
        let returnClause = null;
        for (const clause of clauses) {
            if (clause.type === "CREATE") {
                createClauses.push(clause);
            }
            else if (clause.type === "RETURN") {
                returnClause = clause;
            }
            else if (clause.type === "MATCH") {
                // If there's a MATCH, this is not a pure CREATE...RETURN pattern
                return null;
            }
        }
        if (createClauses.length === 0 || !returnClause)
            return null;
        // Execute CREATE and track created node IDs
        const createdIds = new Map();
        for (const createClause of createClauses) {
            for (const pattern of createClause.patterns) {
                if (this.isRelationshipPattern(pattern)) {
                    // Handle relationship pattern
                    this.executeCreateRelationshipPattern(pattern, createdIds, params);
                }
                else {
                    // Handle node pattern
                    const id = crypto.randomUUID();
                    const labelJson = this.normalizeLabelToJson(pattern.label);
                    const props = this.resolveProperties(pattern.properties || {}, params);
                    this.db.execute("INSERT INTO nodes (id, label, properties) VALUES (?, ?, ?)", [id, labelJson, JSON.stringify(props)]);
                    if (pattern.variable) {
                        createdIds.set(pattern.variable, id);
                    }
                }
            }
        }
        // Now query the created nodes/edges based on RETURN items
        const results = [];
        const resultRow = {};
        for (const item of returnClause.items) {
            const alias = item.alias || this.getExpressionName(item.expression);
            if (item.expression.type === "variable") {
                const variable = item.expression.variable;
                const id = createdIds.get(variable);
                if (id) {
                    // Query the node
                    const nodeResult = this.db.execute("SELECT id, label, properties FROM nodes WHERE id = ?", [id]);
                    if (nodeResult.rows.length > 0) {
                        const row = nodeResult.rows[0];
                        resultRow[alias] = {
                            id: row.id,
                            label: this.normalizeLabelForOutput(row.label),
                            properties: typeof row.properties === "string"
                                ? JSON.parse(row.properties)
                                : row.properties,
                        };
                    }
                }
            }
            else if (item.expression.type === "property") {
                const variable = item.expression.variable;
                const property = item.expression.property;
                const id = createdIds.get(variable);
                if (id) {
                    // Try nodes first
                    const nodeResult = this.db.execute(`SELECT json_extract(properties, '$.${property}') as value FROM nodes WHERE id = ?`, [id]);
                    if (nodeResult.rows.length > 0) {
                        resultRow[alias] = nodeResult.rows[0].value;
                    }
                    else {
                        // Try edges if not found in nodes
                        const edgeResult = this.db.execute(`SELECT json_extract(properties, '$.${property}') as value FROM edges WHERE id = ?`, [id]);
                        if (edgeResult.rows.length > 0) {
                            resultRow[alias] = edgeResult.rows[0].value;
                        }
                    }
                }
            }
            else if (item.expression.type === "function" && item.expression.functionName === "ID") {
                // Handle id(n) function
                const args = item.expression.args;
                if (args && args.length > 0 && args[0].type === "variable") {
                    const variable = args[0].variable;
                    const id = createdIds.get(variable);
                    if (id) {
                        resultRow[alias] = id;
                    }
                }
            }
        }
        if (Object.keys(resultRow).length > 0) {
            results.push(resultRow);
        }
        return results;
    }
    /**
     * Handle MERGE clauses that need special execution (relationship patterns or ON CREATE/MATCH SET)
     * Returns null if this is not a MERGE pattern that needs special handling
     */
    tryMergeExecution(query, params) {
        const clauses = query.clauses;
        let matchClauses = [];
        let withClauses = [];
        let mergeClause = null;
        let returnClause = null;
        for (const clause of clauses) {
            if (clause.type === "MERGE") {
                mergeClause = clause;
            }
            else if (clause.type === "RETURN") {
                returnClause = clause;
            }
            else if (clause.type === "MATCH") {
                matchClauses.push(clause);
            }
            else if (clause.type === "WITH") {
                withClauses.push(clause);
            }
            else {
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
        return this.executeMergeWithSetClauses(matchClauses, withClauses, mergeClause, returnClause, params);
    }
    /**
     * Execute a MERGE clause with ON CREATE SET and/or ON MATCH SET
     */
    executeMergeWithSetClauses(matchClauses, withClauses, mergeClause, returnClause, params) {
        // Track matched nodes from MATCH clauses
        const matchedNodes = new Map();
        // Execute MATCH clauses first to get referenced nodes
        for (const matchClause of matchClauses) {
            for (const pattern of matchClause.patterns) {
                if (this.isRelationshipPattern(pattern)) {
                    // For now, only handle simple node patterns in MATCH before MERGE
                    throw new Error("Relationship patterns in MATCH before MERGE not yet supported");
                }
                const nodePattern = pattern;
                const matchProps = this.resolveProperties(nodePattern.properties || {}, params);
                // Build WHERE conditions
                const conditions = [];
                const conditionParams = [];
                if (nodePattern.label) {
                    const labelCondition = this.generateLabelCondition(nodePattern.label);
                    conditions.push(labelCondition.sql);
                    conditionParams.push(...labelCondition.params);
                }
                for (const [key, value] of Object.entries(matchProps)) {
                    conditions.push(`json_extract(properties, '$.${key}') = ?`);
                    conditionParams.push(value);
                }
                const findSql = `SELECT id, label, properties FROM nodes WHERE ${conditions.join(" AND ")}`;
                const findResult = this.db.execute(findSql, conditionParams);
                if (findResult.rows.length > 0 && nodePattern.variable) {
                    const row = findResult.rows[0];
                    const labelValue = typeof row.label === "string" ? JSON.parse(row.label) : row.label;
                    matchedNodes.set(nodePattern.variable, {
                        id: row.id,
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
            return this.executeMergeNode(patterns[0], mergeClause, returnClause, params, matchedNodes);
        }
        else if (patterns.length === 1 && this.isRelationshipPattern(patterns[0])) {
            // Relationship MERGE
            return this.executeMergeRelationship(patterns[0], mergeClause, returnClause, params, matchedNodes);
        }
        else {
            throw new Error("Complex MERGE patterns not yet supported");
        }
    }
    /**
     * Execute a simple node MERGE
     */
    executeMergeNode(pattern, mergeClause, returnClause, params, matchedNodes) {
        const matchProps = this.resolveProperties(pattern.properties || {}, params);
        // Build WHERE conditions to find existing node
        const conditions = [];
        const conditionParams = [];
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
        let findResult;
        if (conditions.length > 0) {
            const findSql = `SELECT id, label, properties FROM nodes WHERE ${conditions.join(" AND ")}`;
            findResult = this.db.execute(findSql, conditionParams);
        }
        else {
            // MERGE with no label and no properties - match any node
            findResult = this.db.execute("SELECT id, label, properties FROM nodes LIMIT 1");
        }
        let nodeId;
        let wasCreated = false;
        if (findResult.rows.length === 0) {
            // Node doesn't exist - create it
            nodeId = crypto.randomUUID();
            wasCreated = true;
            // Start with match properties
            const nodeProps = { ...matchProps };
            // Collect additional labels from ON CREATE SET
            const additionalLabels = [];
            // Apply ON CREATE SET properties
            if (mergeClause.onCreateSet) {
                // Convert matchedNodes to resolvedIds format for expression evaluation
                const resolvedIds = {};
                for (const [varName, nodeInfo] of matchedNodes) {
                    resolvedIds[varName] = nodeInfo.id;
                }
                for (const assignment of mergeClause.onCreateSet) {
                    // Handle label assignments: ON CREATE SET a:Label
                    if (assignment.labels && assignment.labels.length > 0) {
                        additionalLabels.push(...assignment.labels);
                        continue;
                    }
                    if (!assignment.value || !assignment.property)
                        continue;
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
            this.db.execute("INSERT INTO nodes (id, label, properties) VALUES (?, ?, ?)", [nodeId, labelJson, JSON.stringify(nodeProps)]);
        }
        else {
            // Node exists - apply ON MATCH SET
            nodeId = findResult.rows[0].id;
            if (mergeClause.onMatchSet) {
                // Convert matchedNodes to resolvedIds format for expression evaluation
                const resolvedIds = {};
                for (const [varName, nodeInfo] of matchedNodes) {
                    resolvedIds[varName] = nodeInfo.id;
                }
                for (const assignment of mergeClause.onMatchSet) {
                    // Handle label assignments: ON MATCH SET a:Label
                    if (assignment.labels && assignment.labels.length > 0) {
                        const newLabelsJson = JSON.stringify(assignment.labels);
                        this.db.execute(`UPDATE nodes SET label = (SELECT json_group_array(value) FROM (
                SELECT DISTINCT value FROM (
                  SELECT value FROM json_each(nodes.label)
                  UNION ALL
                  SELECT value FROM json_each(?)
                ) ORDER BY value
              )) WHERE id = ?`, [newLabelsJson, nodeId]);
                        continue;
                    }
                    if (!assignment.value || !assignment.property)
                        continue;
                    const value = assignment.value.type === "property" || assignment.value.type === "binary"
                        ? this.evaluateExpressionWithContext(assignment.value, params, resolvedIds)
                        : this.evaluateExpression(assignment.value, params);
                    this.db.execute(`UPDATE nodes SET properties = json_set(properties, '$.${assignment.property}', json(?)) WHERE id = ?`, [JSON.stringify(value), nodeId]);
                }
            }
        }
        // Store the node in matchedNodes for RETURN processing
        if (pattern.variable) {
            const nodeResult = this.db.execute("SELECT id, label, properties FROM nodes WHERE id = ?", [nodeId]);
            if (nodeResult.rows.length > 0) {
                const row = nodeResult.rows[0];
                matchedNodes.set(pattern.variable, {
                    id: row.id,
                    label: row.label,
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
    executeMergeRelationship(pattern, mergeClause, returnClause, params, matchedNodes) {
        const sourceVar = pattern.source.variable;
        const targetVar = pattern.target.variable;
        const edgeType = pattern.edge.type || "";
        const edgeProps = this.resolveProperties(pattern.edge.properties || {}, params);
        const sourceProps = this.resolveProperties(pattern.source.properties || {}, params);
        const targetProps = this.resolveProperties(pattern.target.properties || {}, params);
        // Track edges for RETURN
        const matchedEdges = new Map();
        // Resolve or create source node
        let sourceNodeId;
        if (sourceVar && matchedNodes.has(sourceVar)) {
            // Source already matched from MATCH clause
            sourceNodeId = matchedNodes.get(sourceVar).id;
        }
        else {
            // Need to find or create source node
            const sourceResult = this.findOrCreateNode(pattern.source, sourceProps, params);
            sourceNodeId = sourceResult.id;
            if (sourceVar) {
                matchedNodes.set(sourceVar, sourceResult);
            }
        }
        // Resolve or create target node
        let targetNodeId;
        if (targetVar && matchedNodes.has(targetVar)) {
            // Target already matched from MATCH clause
            targetNodeId = matchedNodes.get(targetVar).id;
        }
        else {
            // Need to find or create target node
            const targetResult = this.findOrCreateNode(pattern.target, targetProps, params);
            targetNodeId = targetResult.id;
            if (targetVar) {
                matchedNodes.set(targetVar, targetResult);
            }
        }
        // Check if the relationship already exists between these two nodes
        const findEdgeConditions = [
            "source_id = ?",
            "target_id = ?",
        ];
        const findEdgeParams = [sourceNodeId, targetNodeId];
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
        let edgeId;
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
                    if (assignment.labels)
                        continue;
                    if (!assignment.value || !assignment.property)
                        continue;
                    const value = this.evaluateExpression(assignment.value, params);
                    // Update target node with ON CREATE SET
                    this.db.execute(`UPDATE nodes SET properties = json_set(properties, '$.${assignment.property}', json(?)) WHERE id = ?`, [JSON.stringify(value), targetNodeId]);
                }
            }
            this.db.execute("INSERT INTO edges (id, type, source_id, target_id, properties) VALUES (?, ?, ?, ?, ?)", [edgeId, edgeType, sourceNodeId, targetNodeId, JSON.stringify(finalEdgeProps)]);
        }
        else {
            // Relationship exists - apply ON MATCH SET
            edgeId = findEdgeResult.rows[0].id;
            if (mergeClause.onMatchSet) {
                for (const assignment of mergeClause.onMatchSet) {
                    // Skip label assignments (handled separately)
                    if (assignment.labels)
                        continue;
                    if (!assignment.value || !assignment.property)
                        continue;
                    const value = this.evaluateExpression(assignment.value, params);
                    // Update target node with ON MATCH SET
                    this.db.execute(`UPDATE nodes SET properties = json_set(properties, '$.${assignment.property}', json(?)) WHERE id = ?`, [JSON.stringify(value), targetNodeId]);
                }
            }
        }
        // Store the edge in matchedEdges for RETURN processing
        if (pattern.edge.variable) {
            const edgeResult = this.db.execute("SELECT id, type, source_id, target_id, properties FROM edges WHERE id = ?", [edgeId]);
            if (edgeResult.rows.length > 0) {
                const row = edgeResult.rows[0];
                matchedEdges.set(pattern.edge.variable, {
                    id: row.id,
                    type: row.type,
                    source_id: row.source_id,
                    target_id: row.target_id,
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
                    id: row.id,
                    label: row.label,
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
    findOrCreateNode(pattern, props, params) {
        // Build conditions to find existing node
        const conditions = [];
        const conditionParams = [];
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
                    id: row.id,
                    label: typeof row.label === "string" ? JSON.parse(row.label) : row.label,
                    properties: typeof row.properties === "string" ? JSON.parse(row.properties) : row.properties,
                };
            }
        }
        // Node doesn't exist - create it
        const nodeId = crypto.randomUUID();
        const labelJson = this.normalizeLabelToJson(pattern.label);
        this.db.execute("INSERT INTO nodes (id, label, properties) VALUES (?, ?, ?)", [nodeId, labelJson, JSON.stringify(props)]);
        return {
            id: nodeId,
            label: labelJson,
            properties: props,
        };
    }
    /**
     * Process a RETURN clause using matched nodes and edges
     */
    processReturnClauseWithEdges(returnClause, matchedNodes, matchedEdges, params) {
        const results = [];
        const resultRow = {};
        for (const item of returnClause.items) {
            const alias = item.alias || this.getExpressionName(item.expression);
            if (item.expression.type === "variable") {
                const varName = item.expression.variable;
                // Check if it's a node
                const node = matchedNodes.get(varName);
                if (node) {
                    resultRow[alias] = {
                        id: node.id,
                        label: this.normalizeLabelForOutput(node.label),
                        properties: node.properties,
                    };
                    continue;
                }
                // Check if it's an edge
                const edge = matchedEdges.get(varName);
                if (edge) {
                    resultRow[alias] = {
                        id: edge.id,
                        type: edge.type,
                        source_id: edge.source_id,
                        target_id: edge.target_id,
                        properties: edge.properties,
                    };
                    continue;
                }
            }
            else if (item.expression.type === "property") {
                const varName = item.expression.variable;
                const propName = item.expression.property;
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
            }
            else if (item.expression.type === "function") {
                if (item.expression.functionName === "TYPE" && item.expression.args?.length === 1) {
                    const arg = item.expression.args[0];
                    if (arg.type === "variable") {
                        const edge = matchedEdges.get(arg.variable);
                        if (edge) {
                            resultRow[alias] = edge.type;
                            continue;
                        }
                    }
                }
            }
            else if (item.expression.type === "comparison") {
                // Handle comparison expressions like: l.created_at = $createdAt
                const left = this.evaluateReturnExpression(item.expression.left, matchedNodes, params);
                const right = this.evaluateReturnExpression(item.expression.right, matchedNodes, params);
                const op = item.expression.comparisonOperator;
                let result;
                switch (op) {
                    case "=":
                        result = left === right;
                        break;
                    case "<>":
                        result = left !== right;
                        break;
                    case "<":
                        result = left < right;
                        break;
                    case ">":
                        result = left > right;
                        break;
                    case "<=":
                        result = left <= right;
                        break;
                    case ">=":
                        result = left >= right;
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
    processReturnClause(returnClause, matchedNodes, params) {
        const results = [];
        const resultRow = {};
        for (const item of returnClause.items) {
            const alias = item.alias || this.getExpressionName(item.expression);
            if (item.expression.type === "variable") {
                const node = matchedNodes.get(item.expression.variable);
                if (node) {
                    resultRow[alias] = {
                        id: node.id,
                        label: this.normalizeLabelForOutput(node.label),
                        properties: node.properties,
                    };
                }
            }
            else if (item.expression.type === "property") {
                const node = matchedNodes.get(item.expression.variable);
                if (node) {
                    resultRow[alias] = node.properties[item.expression.property];
                }
            }
            else if (item.expression.type === "function") {
                // Handle function expressions like count(*), labels(n)
                const funcName = item.expression.functionName?.toUpperCase();
                if (funcName === "COUNT") {
                    // count(*) on MERGE results - count the matched/created nodes
                    resultRow[alias] = 1; // MERGE always results in exactly one node
                }
                else if (funcName === "LABELS") {
                    // labels(n) function
                    const args = item.expression.args;
                    if (args && args.length > 0 && args[0].type === "variable") {
                        const node = matchedNodes.get(args[0].variable);
                        if (node) {
                            const label = this.normalizeLabelForOutput(node.label);
                            resultRow[alias] = Array.isArray(label) ? label : (label ? [label] : []);
                        }
                    }
                }
            }
            else if (item.expression.type === "comparison") {
                // Handle comparison expressions like: l.created_at = $createdAt
                const left = this.evaluateReturnExpression(item.expression.left, matchedNodes, params);
                const right = this.evaluateReturnExpression(item.expression.right, matchedNodes, params);
                const op = item.expression.comparisonOperator;
                let result;
                switch (op) {
                    case "=":
                        result = left === right;
                        break;
                    case "<>":
                        result = left !== right;
                        break;
                    case "<":
                        result = left < right;
                        break;
                    case ">":
                        result = left > right;
                        break;
                    case "<=":
                        result = left <= right;
                        break;
                    case ">=":
                        result = left >= right;
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
    evaluateReturnExpression(expr, matchedNodes, params) {
        if (expr.type === "property") {
            const node = matchedNodes.get(expr.variable);
            if (node) {
                return node.properties[expr.property];
            }
            return null;
        }
        else if (expr.type === "parameter") {
            return params[expr.name];
        }
        else if (expr.type === "literal") {
            return expr.value;
        }
        return null;
    }
    /**
     * Execute a CREATE relationship pattern, tracking created IDs
     */
    executeCreateRelationshipPattern(rel, createdIds, params) {
        let sourceId;
        let targetId;
        // Determine source node ID
        if (rel.source.variable && createdIds.has(rel.source.variable)) {
            sourceId = createdIds.get(rel.source.variable);
        }
        else if (rel.source.variable && !createdIds.has(rel.source.variable) && !rel.source.label) {
            // Variable referenced but not found and no label - error
            throw new Error(`Cannot resolve source node: ${rel.source.variable}`);
        }
        else {
            // Create new source node (with or without label - anonymous nodes are valid)
            sourceId = crypto.randomUUID();
            const props = this.resolveProperties(rel.source.properties || {}, params);
            const labelJson = this.normalizeLabelToJson(rel.source.label);
            this.db.execute("INSERT INTO nodes (id, label, properties) VALUES (?, ?, ?)", [sourceId, labelJson, JSON.stringify(props)]);
            if (rel.source.variable) {
                createdIds.set(rel.source.variable, sourceId);
            }
        }
        // Determine target node ID
        if (rel.target.variable && createdIds.has(rel.target.variable)) {
            targetId = createdIds.get(rel.target.variable);
        }
        else if (rel.target.variable && !createdIds.has(rel.target.variable) && !rel.target.label) {
            // Variable referenced but not found and no label - error
            throw new Error(`Cannot resolve target node: ${rel.target.variable}`);
        }
        else {
            // Create new target node (with or without label - anonymous nodes are valid)
            targetId = crypto.randomUUID();
            const props = this.resolveProperties(rel.target.properties || {}, params);
            const labelJson = this.normalizeLabelToJson(rel.target.label);
            this.db.execute("INSERT INTO nodes (id, label, properties) VALUES (?, ?, ?)", [targetId, labelJson, JSON.stringify(props)]);
            if (rel.target.variable) {
                createdIds.set(rel.target.variable, targetId);
            }
        }
        // Swap source/target for left-directed relationships
        const [actualSource, actualTarget] = rel.edge.direction === "left" ? [targetId, sourceId] : [sourceId, targetId];
        // Create edge
        const edgeId = crypto.randomUUID();
        const edgeType = rel.edge.type || "";
        const edgeProps = this.resolveProperties(rel.edge.properties || {}, params);
        this.db.execute("INSERT INTO edges (id, type, source_id, target_id, properties) VALUES (?, ?, ?, ?, ?)", [edgeId, edgeType, actualSource, actualTarget, JSON.stringify(edgeProps)]);
        if (rel.edge.variable) {
            createdIds.set(rel.edge.variable, edgeId);
        }
    }
    /**
     * Get a name for an expression (for default aliases)
     */
    getExpressionName(expr) {
        switch (expr.type) {
            case "variable":
                return expr.variable;
            case "property":
                return `${expr.variable}_${expr.property}`;
            case "function":
                return expr.functionName.toLowerCase();
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
    tryMultiPhaseExecution(query, params) {
        // Categorize clauses
        const matchClauses = [];
        const createClauses = [];
        const setClauses = [];
        const deleteClauses = [];
        let returnClause = null;
        for (const clause of query.clauses) {
            switch (clause.type) {
                case "MATCH":
                    matchClauses.push(clause);
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
        const hasRelationshipPattern = matchClauses.some((m) => m.patterns.some((p) => this.isRelationshipPattern(p)));
        // Use multi-phase for:
        // - Relationship patterns (multi-hop)
        // - MATCH...CREATE referencing matched vars
        // - MATCH...SET (always needs ID resolution)
        // - MATCH...DELETE (always needs ID resolution)
        const needsMultiPhase = hasRelationshipPattern ||
            createClauses.length > 0 ||
            setClauses.length > 0 ||
            deleteClauses.length > 0;
        if (!needsMultiPhase) {
            return null;
        }
        // Collect all variables defined in MATCH clauses
        const matchedVariables = new Set();
        for (const matchClause of matchClauses) {
            for (const pattern of matchClause.patterns) {
                this.collectVariablesFromPattern(pattern, matchedVariables);
            }
        }
        // Determine which variables need to be resolved for CREATE
        const referencedInCreate = new Set();
        for (const createClause of createClauses) {
            for (const pattern of createClause.patterns) {
                this.findReferencedVariables(pattern, matchedVariables, referencedInCreate);
            }
        }
        // Determine which variables need to be resolved for SET
        const referencedInSet = new Set();
        for (const setClause of setClauses) {
            for (const assignment of setClause.assignments) {
                if (matchedVariables.has(assignment.variable)) {
                    referencedInSet.add(assignment.variable);
                }
            }
        }
        // Determine which variables need to be resolved for DELETE
        const referencedInDelete = new Set();
        for (const deleteClause of deleteClauses) {
            for (const variable of deleteClause.variables) {
                if (matchedVariables.has(variable)) {
                    referencedInDelete.add(variable);
                }
            }
        }
        // Combine all referenced variables
        const allReferencedVars = new Set([
            ...referencedInCreate,
            ...referencedInSet,
            ...referencedInDelete,
        ]);
        // If no relationship patterns and nothing references matched vars, use standard execution
        if (!hasRelationshipPattern && allReferencedVars.size === 0) {
            return null;
        }
        // For relationship patterns with SET/DELETE, we need to resolve all matched variables
        if (hasRelationshipPattern && (setClauses.length > 0 || deleteClauses.length > 0)) {
            // Add all matched variables to the resolution set
            for (const v of matchedVariables) {
                allReferencedVars.add(v);
            }
        }
        // Multi-phase execution needed
        return this.executeMultiPhaseGeneral(matchClauses, createClauses, setClauses, deleteClauses, returnClause, allReferencedVars, matchedVariables, params);
    }
    /**
     * Collect variable names from a pattern
     */
    collectVariablesFromPattern(pattern, variables) {
        if (this.isRelationshipPattern(pattern)) {
            if (pattern.source.variable)
                variables.add(pattern.source.variable);
            if (pattern.target.variable)
                variables.add(pattern.target.variable);
            if (pattern.edge.variable)
                variables.add(pattern.edge.variable);
        }
        else {
            if (pattern.variable)
                variables.add(pattern.variable);
        }
    }
    /**
     * Find variables in CREATE that reference MATCH variables
     */
    findReferencedVariables(pattern, matchedVars, referenced) {
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
     * Execute a complex pattern with MATCH...CREATE/SET/DELETE in multiple phases
     */
    executeMultiPhaseGeneral(matchClauses, createClauses, setClauses, deleteClauses, returnClause, referencedVars, allMatchedVars, params) {
        // Phase 1: Execute MATCH to get actual node/edge IDs
        const varsToResolve = referencedVars.size > 0 ? referencedVars : allMatchedVars;
        const matchQuery = {
            clauses: [
                ...matchClauses,
                {
                    type: "RETURN",
                    items: Array.from(varsToResolve).map((v) => ({
                        expression: { type: "function", functionName: "ID", args: [{ type: "variable", variable: v }] },
                        alias: `_id_${v}`,
                    })),
                },
            ],
        };
        const translator = new Translator(params);
        const matchTranslation = translator.translate(matchQuery);
        let matchedRows = [];
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
        const allResolvedIds = [];
        this.db.transaction(() => {
            for (const row of matchedRows) {
                // Build a map of variable -> actual node/edge ID
                const resolvedIds = {};
                for (const v of varsToResolve) {
                    resolvedIds[v] = row[`_id_${v}`];
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
            }
        });
        // Phase 3: Execute RETURN if present
        if (returnClause) {
            // Check if RETURN references any newly created variables (not in matched vars)
            const returnVars = this.collectReturnVariables(returnClause);
            const referencesCreatedVars = returnVars.some(v => !allMatchedVars.has(v));
            // If SET was executed, we need to use buildReturnResults to get updated values
            // because re-running MATCH with original WHERE conditions may not find the updated nodes
            if (referencesCreatedVars || setClauses.length > 0) {
                // RETURN references created nodes or data was modified - use buildReturnResults with resolved IDs
                return this.buildReturnResults(returnClause, allResolvedIds);
            }
            else {
                // RETURN only references matched nodes and no mutations - use translator-based approach
                const fullQuery = {
                    clauses: [...matchClauses, returnClause],
                };
                const returnTranslator = new Translator(params);
                const returnTranslation = returnTranslator.translate(fullQuery);
                let rows = [];
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
    collectReturnVariables(returnClause) {
        const vars = [];
        for (const item of returnClause.items) {
            this.collectExpressionVariables(item.expression, vars);
        }
        return vars;
    }
    /**
     * Collect variable names from an expression
     */
    collectExpressionVariables(expr, vars) {
        if (expr.type === "variable" && expr.variable) {
            vars.push(expr.variable);
        }
        else if (expr.type === "property" && expr.variable) {
            vars.push(expr.variable);
        }
        else if (expr.type === "function" && expr.args) {
            for (const arg of expr.args) {
                this.collectExpressionVariables(arg, vars);
            }
        }
    }
    /**
     * Build RETURN results from resolved node/edge IDs
     */
    buildReturnResults(returnClause, allResolvedIds) {
        const results = [];
        for (const resolvedIds of allResolvedIds) {
            const resultRow = {};
            for (const item of returnClause.items) {
                const alias = item.alias || this.getExpressionName(item.expression);
                if (item.expression.type === "variable") {
                    const variable = item.expression.variable;
                    const nodeId = resolvedIds[variable];
                    if (nodeId) {
                        // Query the node/edge by ID
                        const nodeResult = this.db.execute("SELECT id, label, properties FROM nodes WHERE id = ?", [nodeId]);
                        if (nodeResult.rows.length > 0) {
                            const row = nodeResult.rows[0];
                            resultRow[alias] = {
                                id: row.id,
                                label: this.normalizeLabelForOutput(row.label),
                                properties: typeof row.properties === "string"
                                    ? JSON.parse(row.properties)
                                    : row.properties,
                            };
                        }
                        else {
                            // Try edges
                            const edgeResult = this.db.execute("SELECT id, type, source_id, target_id, properties FROM edges WHERE id = ?", [nodeId]);
                            if (edgeResult.rows.length > 0) {
                                const row = edgeResult.rows[0];
                                resultRow[alias] = {
                                    id: row.id,
                                    type: row.type,
                                    source_id: row.source_id,
                                    target_id: row.target_id,
                                    properties: typeof row.properties === "string"
                                        ? JSON.parse(row.properties)
                                        : row.properties,
                                };
                            }
                        }
                    }
                }
                else if (item.expression.type === "property") {
                    const variable = item.expression.variable;
                    const property = item.expression.property;
                    const nodeId = resolvedIds[variable];
                    if (nodeId) {
                        // Try nodes first
                        const nodeResult = this.db.execute(`SELECT json_extract(properties, '$.${property}') as value FROM nodes WHERE id = ?`, [nodeId]);
                        if (nodeResult.rows.length > 0) {
                            resultRow[alias] = nodeResult.rows[0].value;
                        }
                        else {
                            // Try edges
                            const edgeResult = this.db.execute(`SELECT json_extract(properties, '$.${property}') as value FROM edges WHERE id = ?`, [nodeId]);
                            if (edgeResult.rows.length > 0) {
                                resultRow[alias] = edgeResult.rows[0].value;
                            }
                        }
                    }
                }
                else if (item.expression.type === "function" && item.expression.functionName === "ID") {
                    // Handle id(n) function
                    const args = item.expression.args;
                    if (args && args.length > 0 && args[0].type === "variable") {
                        const variable = args[0].variable;
                        const nodeId = resolvedIds[variable];
                        if (nodeId) {
                            resultRow[alias] = nodeId;
                        }
                    }
                }
            }
            if (Object.keys(resultRow).length > 0) {
                results.push(resultRow);
            }
        }
        return results;
    }
    /**
     * Execute a MATCH...CREATE pattern in multiple phases (legacy, for backwards compatibility)
     */
    executeMultiPhase(matchClauses, createClauses, referencedVars, params) {
        return this.executeMultiPhaseGeneral(matchClauses, createClauses, [], [], null, referencedVars, referencedVars, params);
    }
    /**
     * Execute SET clause with pre-resolved node IDs
     */
    executeSetWithResolvedIds(setClause, resolvedIds, params) {
        for (const assignment of setClause.assignments) {
            const nodeId = resolvedIds[assignment.variable];
            if (!nodeId) {
                throw new Error(`Cannot resolve variable for SET: ${assignment.variable}`);
            }
            // Handle label assignments
            if (assignment.labels && assignment.labels.length > 0) {
                const newLabelsJson = JSON.stringify(assignment.labels);
                this.db.execute(`UPDATE nodes SET label = (SELECT json_group_array(value) FROM (
            SELECT DISTINCT value FROM (
              SELECT value FROM json_each(nodes.label)
              UNION ALL
              SELECT value FROM json_each(?)
            ) ORDER BY value
          )) WHERE id = ?`, [newLabelsJson, nodeId]);
                continue;
            }
            // Handle SET n = {props} - replace all properties
            if (assignment.replaceProps && assignment.value) {
                const newProps = this.evaluateObjectExpression(assignment.value, params);
                // Filter out null values (they should be removed)
                const filteredProps = {};
                for (const [key, val] of Object.entries(newProps)) {
                    if (val !== null) {
                        filteredProps[key] = val;
                    }
                }
                // Try nodes first, then edges
                const nodeResult = this.db.execute(`UPDATE nodes SET properties = ? WHERE id = ?`, [JSON.stringify(filteredProps), nodeId]);
                if (nodeResult.changes === 0) {
                    this.db.execute(`UPDATE edges SET properties = ? WHERE id = ?`, [JSON.stringify(filteredProps), nodeId]);
                }
                continue;
            }
            // Handle SET n += {props} - merge properties
            if (assignment.mergeProps && assignment.value) {
                const newProps = this.evaluateObjectExpression(assignment.value, params);
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
                    const nodeResult = this.db.execute(`UPDATE nodes SET properties = json_remove(json_patch(properties, ?), ${removePaths}) WHERE id = ?`, [JSON.stringify(nonNullProps), nodeId]);
                    if (nodeResult.changes === 0) {
                        this.db.execute(`UPDATE edges SET properties = json_remove(json_patch(properties, ?), ${removePaths}) WHERE id = ?`, [JSON.stringify(nonNullProps), nodeId]);
                    }
                }
                else {
                    // Just merge
                    const nodeResult = this.db.execute(`UPDATE nodes SET properties = json_patch(properties, ?) WHERE id = ?`, [JSON.stringify(nonNullProps), nodeId]);
                    if (nodeResult.changes === 0) {
                        this.db.execute(`UPDATE edges SET properties = json_patch(properties, ?) WHERE id = ?`, [JSON.stringify(nonNullProps), nodeId]);
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
            // Update the property using json_set
            // We need to determine if it's a node or edge - for now assume node
            // Try nodes first, then edges
            const nodeResult = this.db.execute(`UPDATE nodes SET properties = json_set(properties, '$.${assignment.property}', json(?)) WHERE id = ?`, [JSON.stringify(value), nodeId]);
            if (nodeResult.changes === 0) {
                // Try edges
                this.db.execute(`UPDATE edges SET properties = json_set(properties, '$.${assignment.property}', json(?)) WHERE id = ?`, [JSON.stringify(value), nodeId]);
            }
        }
    }
    /**
     * Evaluate an object expression to get its key-value pairs
     */
    evaluateObjectExpression(expr, params) {
        if (expr.type === "object" && expr.properties) {
            const result = {};
            for (const prop of expr.properties) {
                result[prop.key] = this.evaluateExpression(prop.value, params);
            }
            return result;
        }
        if (expr.type === "parameter") {
            const paramValue = params[expr.name];
            if (typeof paramValue === "object" && paramValue !== null) {
                return paramValue;
            }
            throw new Error(`Parameter ${expr.name} is not an object`);
        }
        throw new Error(`Expected object expression, got ${expr.type}`);
    }
    /**
     * Execute DELETE clause with pre-resolved node/edge IDs
     */
    executeDeleteWithResolvedIds(deleteClause, resolvedIds) {
        for (const variable of deleteClause.variables) {
            const id = resolvedIds[variable];
            if (!id) {
                throw new Error(`Cannot resolve variable for DELETE: ${variable}`);
            }
            if (deleteClause.detach) {
                // DETACH DELETE: First delete all edges connected to this node
                this.db.execute("DELETE FROM edges WHERE source_id = ? OR target_id = ?", [id, id]);
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
    evaluateExpression(expr, params) {
        switch (expr.type) {
            case "literal":
                return expr.value;
            case "parameter":
                return params[expr.name];
            default:
                throw new Error(`Cannot evaluate expression of type ${expr.type}`);
        }
    }
    /**
     * Evaluate an expression with access to node/edge context for property lookups
     */
    evaluateExpressionWithContext(expr, params, resolvedIds) {
        switch (expr.type) {
            case "literal":
                return expr.value;
            case "parameter":
                return params[expr.name];
            case "property": {
                // Look up property from node/edge
                const varName = expr.variable;
                const propName = expr.property;
                const entityId = resolvedIds[varName];
                if (!entityId) {
                    throw new Error(`Unknown variable: ${varName}`);
                }
                // Try nodes first
                const nodeResult = this.db.execute(`SELECT json_extract(properties, '$.${propName}') AS value FROM nodes WHERE id = ?`, [entityId]);
                if (nodeResult.rows.length > 0) {
                    return nodeResult.rows[0].value;
                }
                // Try edges
                const edgeResult = this.db.execute(`SELECT json_extract(properties, '$.${propName}') AS value FROM edges WHERE id = ?`, [entityId]);
                if (edgeResult.rows.length > 0) {
                    return edgeResult.rows[0].value;
                }
                return null;
            }
            case "binary": {
                // Evaluate arithmetic expressions
                const left = this.evaluateExpressionWithContext(expr.left, params, resolvedIds);
                const right = this.evaluateExpressionWithContext(expr.right, params, resolvedIds);
                // Handle null values
                if (left === null || right === null) {
                    return null;
                }
                switch (expr.operator) {
                    case "+":
                        return left + right;
                    case "-":
                        return left - right;
                    case "*":
                        return left * right;
                    case "/":
                        return left / right;
                    case "%":
                        return left % right;
                    case "^":
                        return Math.pow(left, right);
                    default:
                        throw new Error(`Unknown binary operator: ${expr.operator}`);
                }
            }
            default:
                throw new Error(`Cannot evaluate expression of type ${expr.type}`);
        }
    }
    /**
     * Execute a CREATE clause with pre-resolved node IDs for referenced variables
     * The resolvedIds map is mutated to include newly created node IDs
     */
    executeCreateWithResolvedIds(createClause, resolvedIds, params) {
        for (const pattern of createClause.patterns) {
            if (this.isRelationshipPattern(pattern)) {
                this.createRelationshipWithResolvedIds(pattern, resolvedIds, params);
            }
            else {
                // Simple node creation - use standard translation
                const nodeQuery = { clauses: [{ type: "CREATE", patterns: [pattern] }] };
                const translator = new Translator(params);
                const translation = translator.translate(nodeQuery);
                for (const stmt of translation.statements) {
                    this.db.execute(stmt.sql, stmt.params);
                }
            }
        }
    }
    /**
     * Create a relationship where some endpoints reference pre-existing nodes.
     * The resolvedIds map is mutated to include newly created node IDs.
     */
    createRelationshipWithResolvedIds(rel, resolvedIds, params) {
        let sourceId;
        let targetId;
        // Determine source node ID
        if (rel.source.variable && resolvedIds[rel.source.variable]) {
            sourceId = resolvedIds[rel.source.variable];
        }
        else if (rel.source.variable && !resolvedIds[rel.source.variable] && !rel.source.label) {
            // Variable referenced but not found and no label - error
            throw new Error(`Cannot resolve source node: ${rel.source.variable}`);
        }
        else {
            // Create new source node (with or without label - anonymous nodes are valid)
            sourceId = crypto.randomUUID();
            const props = this.resolveProperties(rel.source.properties || {}, params);
            const labelJson = this.normalizeLabelToJson(rel.source.label);
            this.db.execute("INSERT INTO nodes (id, label, properties) VALUES (?, ?, ?)", [sourceId, labelJson, JSON.stringify(props)]);
            // Add to resolvedIds so subsequent patterns can reference it
            if (rel.source.variable) {
                resolvedIds[rel.source.variable] = sourceId;
            }
        }
        // Determine target node ID
        if (rel.target.variable && resolvedIds[rel.target.variable]) {
            targetId = resolvedIds[rel.target.variable];
        }
        else if (rel.target.variable && !resolvedIds[rel.target.variable] && !rel.target.label) {
            // Variable referenced but not found and no label - error
            throw new Error(`Cannot resolve target node: ${rel.target.variable}`);
        }
        else {
            // Create new target node (with or without label - anonymous nodes are valid)
            targetId = crypto.randomUUID();
            const props = this.resolveProperties(rel.target.properties || {}, params);
            const labelJson = this.normalizeLabelToJson(rel.target.label);
            this.db.execute("INSERT INTO nodes (id, label, properties) VALUES (?, ?, ?)", [targetId, labelJson, JSON.stringify(props)]);
            // Add to resolvedIds so subsequent patterns can reference it
            if (rel.target.variable) {
                resolvedIds[rel.target.variable] = targetId;
            }
        }
        // Swap source/target for left-directed relationships
        const [actualSource, actualTarget] = rel.edge.direction === "left" ? [targetId, sourceId] : [sourceId, targetId];
        // Create edge
        const edgeId = crypto.randomUUID();
        const edgeType = rel.edge.type || "";
        const edgeProps = this.resolveProperties(rel.edge.properties || {}, params);
        this.db.execute("INSERT INTO edges (id, type, source_id, target_id, properties) VALUES (?, ?, ?, ?, ?)", [edgeId, edgeType, actualSource, actualTarget, JSON.stringify(edgeProps)]);
        // Add edge to resolvedIds if it has a variable
        if (rel.edge.variable) {
            resolvedIds[rel.edge.variable] = edgeId;
        }
    }
    /**
     * Resolve parameter references in properties
     */
    resolveProperties(props, params) {
        const resolved = {};
        for (const [key, value] of Object.entries(props)) {
            if (typeof value === "object" &&
                value !== null &&
                "type" in value &&
                value.type === "parameter" &&
                "name" in value) {
                resolved[key] = params[value.name];
            }
            else {
                resolved[key] = value;
            }
        }
        return resolved;
    }
    /**
     * Type guard for relationship patterns
     */
    isRelationshipPattern(pattern) {
        return "source" in pattern && "edge" in pattern && "target" in pattern;
    }
    /**
     * Format raw database results into a more usable structure
     */
    formatResults(rows, returnColumns) {
        return rows.map((row) => {
            const formatted = {};
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
    deepParseJson(value, key) {
        if (typeof value === "string") {
            try {
                const parsed = JSON.parse(value);
                // Recursively process if it's an object or array
                if (typeof parsed === "object" && parsed !== null) {
                    return this.deepParseJson(parsed, key);
                }
                return parsed;
            }
            catch {
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
            const result = {};
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
    normalizeLabelToJson(label) {
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
    normalizeLabelForOutput(label) {
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
            }
            catch {
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
    generateLabelCondition(label) {
        const labels = Array.isArray(label) ? label : [label];
        if (labels.length === 1) {
            return {
                sql: `EXISTS (SELECT 1 FROM json_each(label) WHERE value = ?)`,
                params: [labels[0]]
            };
        }
        else {
            // Multiple labels: all must exist
            const conditions = labels.map(() => `EXISTS (SELECT 1 FROM json_each(label) WHERE value = ?)`);
            return {
                sql: conditions.join(" AND "),
                params: labels
            };
        }
    }
}
// ============================================================================
// Convenience function
// ============================================================================
export function executeQuery(db, cypher, params = {}) {
    return new Executor(db).execute(cypher, params);
}
//# sourceMappingURL=executor.js.map