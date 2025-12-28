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
     * Handle MATCH+WITH(COLLECT)+UNWIND+RETURN pattern
     * This requires a subquery for the aggregate function because SQLite doesn't
     * allow aggregate functions directly inside json_each()
     */
    private tryCollectUnwindExecution;
    /**
     * Evaluate UNWIND expressions to get the arrays to iterate over
     */
    private evaluateUnwindExpressions;
    /**
     * Generate cartesian product of arrays
     */
    private generateCartesianProduct;
    /**
     * Resolve properties, including unwind variable references
     */
    private resolvePropertiesWithUnwind;
    /**
     * Execute CREATE relationship pattern with unwind context
     */
    private executeCreateRelationshipPatternWithUnwind;
    /**
     * Handle CREATE...RETURN pattern by creating nodes/edges and then querying them back
     */
    private tryCreateReturnExecution;
    /**
     * Handle MERGE with ON CREATE SET / ON MATCH SET
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
     */
    private executeMergeRelationship;
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
     * Execute DELETE clause with pre-resolved node/edge IDs
     */
    private executeDeleteWithResolvedIds;
    /**
     * Evaluate an expression to get its value
     */
    private evaluateExpression;
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
     * Resolve parameter references in properties
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
     */
    private deepParseJson;
    /**
     * Normalize label to JSON string for storage
     * Handles both single labels and multiple labels
     */
    private normalizeLabelToJson;
    /**
     * Generate SQL condition for label matching
     * Supports both single and multiple labels
     */
    private generateLabelCondition;
}
export declare function executeQuery(db: GraphDatabase, cypher: string, params?: Record<string, unknown>): QueryResponse;
//# sourceMappingURL=executor.d.ts.map