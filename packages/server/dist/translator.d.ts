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
        type: "node" | "edge";
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
    private wrapForArithmetic;
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
    private findVariablesInCondition;
    private isParameterRef;
    private serializeProperties;
    private evaluateExpression;
    private getExpressionName;
    private generateId;
}
export declare function translate(query: Query, params?: Record<string, unknown>): TranslationResult;
//# sourceMappingURL=translator.d.ts.map