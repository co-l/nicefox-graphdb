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
        type: "node" | "edge" | "path" | "varLengthEdge";
        alias: string;
        pathCteName?: string;
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
    /**
     * Evaluate an object expression to get its key-value pairs.
     */
    private evaluateObjectExpression;
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
    /**
     * Translate an expression for a SET on a just-created node.
     * Property references need to use subqueries since the node ID isn't a table alias.
     */
    private translateExpressionForCreatedNode;
    private translateComparisonExpression;
    private wrapForComparison;
    private translateObjectLiteral;
    private translateArrayLiteral;
    /**
     * Translate a list comprehension expression.
     * Syntax: [variable IN listExpr WHERE filterCondition | mapExpr]
     *
     * Translates to SQLite using json_each and json_group_array:
     * (SELECT json_group_array(value_or_mapped) FROM json_each(listExpr) WHERE filter)
     */
    private translateListComprehension;
    /**
     * Translate an expression within a list comprehension, replacing
     * references to the comprehension variable with the json_each value column.
     */
    private translateListComprehensionExpr;
    /**
     * Translate a WHERE condition within a list comprehension.
     */
    private translateListComprehensionCondition;
    /**
     * Translate a list predicate expression: ALL/ANY/NONE/SINGLE(var IN list WHERE cond)
     *
     * Implementation uses a CTE to evaluate the list once and avoid parameter duplication issues:
     * - ALL: true when count of elements NOT satisfying condition = 0 (empty list = true)
     * - ANY: true when count of elements satisfying condition > 0 (empty list = false)
     * - NONE: true when count of elements satisfying condition = 0 (empty list = true)
     * - SINGLE: true when count of elements satisfying condition = 1 (empty list = false)
     */
    private translateListPredicate;
    /**
     * Wrap an expression for use with json_each in list predicates
     */
    private wrapForListPredicate;
    /**
     * Translate a unary expression: NOT expr
     */
    private translateUnaryExpression;
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
     * Check if an expression is or contains an aggregate function (COUNT, SUM, AVG, MIN, MAX, COLLECT, PERCENTILEDISC, PERCENTILECONT)
     */
    private isAggregateExpression;
    private serializeProperties;
    private isVariableRef;
    private evaluateExpression;
    private getExpressionName;
    private generateId;
}
export declare function translate(query: Query, params?: Record<string, unknown>): TranslationResult;
//# sourceMappingURL=translator.d.ts.map