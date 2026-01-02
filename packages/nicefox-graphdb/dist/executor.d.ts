import { GraphDatabase } from "./db.js";
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
export declare class Executor {
    private db;
    constructor(db: GraphDatabase);
    /**
     * Execute a Cypher query and return formatted results
     */
    execute(cypher: string, params?: Record<string, unknown>): QueryResponse;
    /**
     * Handle UNWIND with CREATE pattern
     * UNWIND expands an array and executes CREATE for each element
     */
    private tryUnwindCreateExecution;
    /**
     * Evaluate a WITH clause WHERE condition against created nodes
     */
    private evaluateWithWhereCondition;
    /**
     * Evaluate a WITH clause WHERE condition using captured property values
     * This is used for patterns like: WITH n.num AS num ... DELETE n ... WITH num WHERE num % 2 = 0
     */
    private evaluateWithWhereConditionWithPropertyAliases;
    /**
     * Evaluate an expression using captured property values (for property alias references)
     */
    private evaluateExpressionWithPropertyAliases;
    /**
     * Evaluate an expression for filtering in UNWIND+CREATE+WITH context
     */
    private evaluateExpressionForFilter;
    /**
     * Handle UNWIND + MERGE pattern
     * This requires special handling to resolve UNWIND variables in MERGE patterns
     */
    private tryUnwindMergeExecution;
    /**
     * Handle MATCH+WITH(COLLECT)+UNWIND+RETURN pattern
     * This requires a subquery for the aggregate function because SQLite doesn't
     * allow aggregate functions directly inside json_each()
     */
    private tryCollectUnwindExecution;
    /**
     * Handle MATCH+WITH(COLLECT)+DELETE[expr] pattern
     * This handles queries like:
     *   MATCH (:User)-[:FRIEND]->(n)
     *   WITH collect(n) AS friends
     *   DETACH DELETE friends[$friendIndex]
     */
    private tryCollectDeleteExecution;
    /**
     * Evaluate a DELETE expression (like friends[$index]) with collected context
     */
    private evaluateDeleteExpression;
    /**
     * Evaluate UNWIND expressions to get the arrays to iterate over
     */
    private evaluateUnwindExpressions;
    /**
     * Evaluate an expression that should return a list
     */
    private evaluateListExpression;
    /**
     * Evaluate a simple expression (literals, parameters, basic arithmetic)
     */
    private evaluateSimpleExpression;
    /**
     * Generate cartesian product of arrays
     */
    private generateCartesianProduct;
    /**
     * Resolve properties, including unwind variable references and binary expressions
     */
    private resolvePropertiesWithUnwind;
    /**
     * Resolve a single property value, handling binary expressions recursively
     */
    private resolvePropertyValueWithUnwind;
    /**
     * Evaluate a function call within a property value context
     */
    private evaluateFunctionInProperty;
    /**
     * Execute CREATE relationship pattern with unwind context
     */
    private executeCreateRelationshipPatternWithUnwind;
    /**
     * Handle CREATE...RETURN pattern by creating nodes/edges and then querying them back
     */
    private tryCreateReturnExecution;
    /**
     * Handle MERGE clauses that need special execution (relationship patterns or ON CREATE/MATCH SET)
     * Returns null if this is not a MERGE pattern that needs special handling
     */
    private tryMergeExecution;
    /**
     * Execute a MERGE clause with ON CREATE SET and/or ON MATCH SET
     */
    private executeMergeWithSetClauses;
    /**
     * Execute a simple node MERGE
     */
    private executeMergeNode;
    /**
     * Execute a relationship MERGE: MERGE (a)-[:TYPE]->(b)
     * Handles multiple scenarios:
     * 1. MATCH (a), (b) MERGE (a)-[:REL]->(b) - both nodes already matched
     * 2. MATCH (a) MERGE (a)-[:REL]->(b:Label {props}) - source matched, target to find/create
     * 3. MERGE (a:Label)-[:REL]->(b:Label) - entire pattern to find/create
     */
    private executeMergeRelationship;
    /**
     * Find an existing node matching the pattern, or create a new one
     */
    private findOrCreateNode;
    /**
     * Process a RETURN clause using matched nodes and edges
     */
    private processReturnClauseWithEdges;
    /**
     * Process a RETURN clause using matched nodes
     */
    private processReturnClause;
    /**
     * Evaluate an expression for RETURN clause
     */
    private evaluateReturnExpression;
    /**
     * Execute a CREATE relationship pattern, tracking created IDs
     */
    private executeCreateRelationshipPattern;
    /**
     * Get a name for an expression (for default aliases)
     */
    private getExpressionName;
    /**
     * Detect and handle patterns that need multi-phase execution:
     * - MATCH...CREATE that references matched variables
     * - MATCH...SET that updates matched nodes/edges via relationships
     * - MATCH...DELETE that deletes matched nodes/edges via relationships
     * Returns null if this is not a multi-phase pattern, otherwise returns the result data.
     */
    private tryMultiPhaseExecution;
    /**
     * Collect variable names from a pattern
     */
    private collectVariablesFromPattern;
    /**
     * Find variables in CREATE that reference MATCH variables
     */
    private findReferencedVariables;
    /**
     * Find variables in CREATE that reference MATCH variables (with alias support)
     */
    private findReferencedVariablesWithAliases;
    /**
     * Validate that all variable references in CREATE clause properties are defined.
     * Throws an error if an undefined variable is referenced.
     */
    private validateCreatePropertyVariables;
    /**
     * Check a properties object for undefined variable references.
     * Throws an error if found.
     */
    private validatePropertiesForUndefinedVariables;
    /**
     * Recursively check a value for undefined variable references.
     */
    private validateValueForUndefinedVariables;
    /**
     * Execute a complex pattern with MATCH...CREATE/SET/DELETE in multiple phases
     */
    private executeMultiPhaseGeneral;
    /**
     * Collect variable names referenced in a RETURN clause
     */
    private collectReturnVariables;
    /**
     * Collect variable names from an expression
     */
    private collectExpressionVariables;
    /**
     * Build RETURN results from resolved node/edge IDs
     */
    private buildReturnResults;
    /**
     * Execute a MATCH...CREATE pattern in multiple phases (legacy, for backwards compatibility)
     */
    private executeMultiPhase;
    /**
     * Execute SET clause with pre-resolved node IDs
     */
    private executeSetWithResolvedIds;
    /**
     * Evaluate an object expression to get its key-value pairs
     */
    private evaluateObjectExpression;
    /**
     * Execute DELETE clause with pre-resolved node/edge IDs
     */
    private executeDeleteWithResolvedIds;
    /**
     * Evaluate an expression to get its value
     * Note: For property and binary expressions that reference nodes, use evaluateExpressionWithContext
     */
    private evaluateExpression;
    /**
     * Evaluate an expression with access to node/edge context for property lookups
     */
    private evaluateExpressionWithContext;
    /**
     * Execute a CREATE clause with pre-resolved node IDs for referenced variables
     * The resolvedIds map is mutated to include newly created node IDs
     */
    private executeCreateWithResolvedIds;
    /**
     * Create a relationship where some endpoints reference pre-existing nodes.
     * The resolvedIds map is mutated to include newly created node IDs.
     */
    private createRelationshipWithResolvedIds;
    /**
     * Resolve parameter references and binary expressions in properties
     */
    private resolveProperties;
    /**
     * Type guard for relationship patterns
     */
    private isRelationshipPattern;
    /**
     * Format raw database results into a more usable structure
     */
    private formatResults;
    /**
     * Recursively parse JSON strings in a value
     * Also normalizes labels (single-element arrays become strings)
     */
    private deepParseJson;
    /**
     * Normalize label to JSON string for storage
     * Handles both single labels and multiple labels
     */
    private normalizeLabelToJson;
    /**
     * Normalize label for output (from database JSON to user-friendly format)
     * Single label: return string, multiple labels: return array
     */
    private normalizeLabelForOutput;
    /**
     * Generate SQL condition for label matching
     * Supports both single and multiple labels
     */
    private generateLabelCondition;
}
export declare function executeQuery(db: GraphDatabase, cypher: string, params?: Record<string, unknown>): QueryResponse;
//# sourceMappingURL=executor.d.ts.map