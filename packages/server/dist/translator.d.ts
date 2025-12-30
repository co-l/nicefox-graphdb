import { Query, WithClause } from "./parser.js";
export interface SqlStatement {
    sql: string;
    params: unknown[];
}
export interface TranslationResult {
    statements: SqlStatement[];
    returnColumns?: string[];
}
export interface TranslationContext {
    variables: Map<string, {
        type: "node" | "edge" | "path";
        alias: string;
    }>;
    paramValues: Record<string, unknown>;
    aliasCounter: number;
    withClauses?: WithClause[];
}
export declare class Translator {
    private ctx;
    constructor(paramValues?: Record<string, unknown>);
    translate(query: Query): TranslationResult;
    private translateClause;
    private translateCreate;
    private translateCreateNode;
    private translateCreateRelationship;
    private translateMatch;
    private registerPathExpression;
    private registerNodePattern;
    private registerRelationshipPattern;
    private translateMerge;
    private translateSet;
    private translateDelete;
    private translateReturn;
    private translateWith;
    private translateReturnFromCall;
    private translateVariableLengthPath;
    private translateUnion;
    private translateUnwind;
    private translateCall;
    private translateCallWhere;
    private translateExpressionForCall;
    private translateExpression;
    private translateCaseExpression;
    private translateBinaryExpression;
    private isListExpression;
    private wrapForArray;
    private wrapForArithmetic;
    private translateComparisonExpression;
    private wrapForComparison;
    private translateObjectLiteral;
    private translateArrayLiteral;
    private translateWhere;
    private translateExistsCondition;
    private translateInCondition;
    private translateOrderByExpression;
    private translateWhereExpression;
    /**
     * Translate a function argument expression to SQL.
     * Handles property access, literals, parameters, and variables.
     */
    private translateFunctionArg;
    private isRelationshipPattern;
    /**
     * Generate SQL condition to match labels stored as JSON array.
     * For a single label "Person", checks if label array contains "Person"
     * For multiple labels ["A", "B"], checks if label array contains all of them
     */
    private generateLabelMatchCondition;
    /**
     * Normalize label to JSON array string for storage
     */
    private normalizeLabelToJson;
    /**
     * Quote an identifier for use as SQL alias (handles reserved words like FROM, TO)
     */
    private quoteAlias;
    private findVariablesInCondition;
    private isParameterRef;
    /**
     * Check if an expression is an aggregate function (COUNT, SUM, AVG, MIN, MAX, COLLECT)
     */
    private isAggregateExpression;
    private serializeProperties;
    private evaluateExpression;
    private getExpressionName;
    private generateId;
}
export declare function translate(query: Query, params?: Record<string, unknown>): TranslationResult;
//# sourceMappingURL=translator.d.ts.map