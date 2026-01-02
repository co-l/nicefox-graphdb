export interface NodePattern {
    variable?: string;
    label?: string | string[];
    properties?: Record<string, PropertyValue>;
}
export interface EdgePattern {
    variable?: string;
    type?: string;
    types?: string[];
    properties?: Record<string, PropertyValue>;
    direction: "left" | "right" | "none";
    minHops?: number;
    maxHops?: number;
}
export interface RelationshipPattern {
    source: NodePattern;
    edge: EdgePattern;
    target: NodePattern;
}
export interface PathExpression {
    type: "path";
    variable: string;
    patterns: (NodePattern | RelationshipPattern)[];
}
export interface ParameterRef {
    type: "parameter";
    name: string;
}
export interface VariableRef {
    type: "variable";
    name: string;
}
export interface PropertyRef {
    type: "property";
    variable: string;
    property: string;
}
export interface BinaryPropertyValue {
    type: "binary";
    operator: "+" | "-" | "*" | "/" | "%" | "^";
    left: PropertyValue;
    right: PropertyValue;
}
export interface FunctionPropertyValue {
    type: "function";
    name: string;
    args: PropertyValue[];
}
export type PropertyValue = string | number | boolean | null | ParameterRef | VariableRef | PropertyRef | BinaryPropertyValue | FunctionPropertyValue | PropertyValue[];
export interface WhereCondition {
    type: "comparison" | "and" | "or" | "not" | "contains" | "startsWith" | "endsWith" | "isNull" | "isNotNull" | "exists" | "in" | "listPredicate";
    left?: Expression;
    right?: Expression;
    operator?: "=" | "<>" | "<" | ">" | "<=" | ">=";
    conditions?: WhereCondition[];
    condition?: WhereCondition;
    pattern?: NodePattern | RelationshipPattern;
    list?: Expression;
    predicateType?: "ALL" | "ANY" | "NONE" | "SINGLE";
    variable?: string;
    listExpr?: Expression;
    filterCondition?: WhereCondition;
}
export interface CaseWhen {
    condition: WhereCondition;
    result: Expression;
}
export interface CaseExpression {
    type: "case";
    expression?: Expression;
    whens: CaseWhen[];
    elseExpr?: Expression;
}
export interface ObjectProperty {
    key: string;
    value: Expression;
}
export interface Expression {
    type: "property" | "literal" | "parameter" | "variable" | "function" | "case" | "binary" | "object" | "comparison" | "listComprehension" | "listPredicate" | "unary" | "labelPredicate" | "propertyAccess";
    variable?: string;
    property?: string;
    value?: PropertyValue;
    name?: string;
    functionName?: string;
    args?: Expression[];
    distinct?: boolean;
    expression?: Expression;
    whens?: CaseWhen[];
    elseExpr?: Expression;
    operator?: "+" | "-" | "*" | "/" | "%" | "^" | "AND" | "OR" | "NOT";
    left?: Expression;
    right?: Expression;
    operand?: Expression;
    comparisonOperator?: "=" | "<>" | "<" | ">" | "<=" | ">=" | "IS NULL" | "IS NOT NULL";
    properties?: ObjectProperty[];
    listExpr?: Expression;
    filterCondition?: WhereCondition;
    mapExpr?: Expression;
    predicateType?: "ALL" | "ANY" | "NONE" | "SINGLE";
    label?: string;
    labels?: string[];
    object?: Expression;
}
export interface ReturnItem {
    expression: Expression;
    alias?: string;
}
export interface SetAssignment {
    variable: string;
    property?: string;
    value?: Expression;
    labels?: string[];
    replaceProps?: boolean;
    mergeProps?: boolean;
}
export interface CreateClause {
    type: "CREATE";
    patterns: (NodePattern | RelationshipPattern)[];
}
export interface MatchClause {
    type: "MATCH" | "OPTIONAL_MATCH";
    patterns: (NodePattern | RelationshipPattern)[];
    pathExpressions?: PathExpression[];
    where?: WhereCondition;
}
export interface MergeClause {
    type: "MERGE";
    patterns: (NodePattern | RelationshipPattern)[];
    onCreateSet?: SetAssignment[];
    onMatchSet?: SetAssignment[];
}
export interface SetClause {
    type: "SET";
    assignments: SetAssignment[];
}
export interface DeleteClause {
    type: "DELETE";
    variables: string[];
    expressions?: Expression[];
    detach?: boolean;
}
export interface ReturnClause {
    type: "RETURN";
    distinct?: boolean;
    items: ReturnItem[];
    orderBy?: {
        expression: Expression;
        direction: "ASC" | "DESC";
    }[];
    skip?: number;
    limit?: number;
}
export interface WithClause {
    type: "WITH";
    distinct?: boolean;
    items: ReturnItem[];
    orderBy?: {
        expression: Expression;
        direction: "ASC" | "DESC";
    }[];
    skip?: number;
    limit?: number;
    where?: WhereCondition;
}
export interface UnwindClause {
    type: "UNWIND";
    expression: Expression;
    alias: string;
}
export interface UnionClause {
    type: "UNION";
    all: boolean;
    left: Query;
    right: Query;
}
export interface CallClause {
    type: "CALL";
    procedure: string;
    args: Expression[];
    yields?: string[];
    where?: WhereCondition;
}
export type Clause = CreateClause | MatchClause | MergeClause | SetClause | DeleteClause | ReturnClause | WithClause | UnwindClause | UnionClause | CallClause;
export interface Query {
    clauses: Clause[];
}
export interface ParseError {
    message: string;
    position: number;
    line: number;
    column: number;
}
export type ParseResult = {
    success: true;
    query: Query;
} | {
    success: false;
    error: ParseError;
};
export declare class Parser {
    private tokens;
    private pos;
    private anonVarCounter;
    parse(input: string): ParseResult;
    private parseQuery;
    private error;
    private parseClause;
    private parseCreate;
    private parseMatch;
    /**
     * Parse either a regular pattern chain or a named path expression.
     * Syntax: p = (a)-[r]->(b) or just (a)-[r]->(b)
     */
    private parsePatternOrPath;
    private parseOptionalMatch;
    private parseMerge;
    private parseSet;
    private parseSetAssignments;
    private parseDelete;
    private parseDeleteTarget;
    private parseReturn;
    private parseWith;
    private parseUnwind;
    private parseUnwindExpression;
    private parseCall;
    /**
     * Parse a pattern, which can be a single node or a chain of relationships.
     * For chained patterns like (a)-[:R1]->(b)-[:R2]->(c), this returns multiple
     * RelationshipPattern objects via parsePatternChain.
     */
    private parsePattern;
    /**
     * Parse a pattern chain, returning an array of patterns.
     * Handles multi-hop patterns like (a)-[:R1]->(b)-[:R2]->(c).
     */
    private parsePatternChain;
    private parseNodePattern;
    private parseEdgePattern;
    private parseVariableLengthSpec;
    private parseProperties;
    private parsePropertyValue;
    private parsePrimaryPropertyValue;
    private parseArray;
    private parseWhereCondition;
    private parseOrCondition;
    private parseAndCondition;
    private parseNotCondition;
    private parsePrimaryCondition;
    private parseListPredicateCondition;
    private parseExistsCondition;
    private parseComparisonCondition;
    private parseInListExpression;
    private parseExpression;
    private parseReturnExpression;
    private parseOrExpression;
    private parseAndExpression;
    private parseNotExpression;
    private parseComparisonExpression;
    private parseAdditiveExpression;
    private parseMultiplicativeExpression;
    private parseExponentialExpression;
    private parsePostfixExpression;
    private parsePrimaryExpression;
    private parseCaseExpression;
    private parseObjectLiteral;
    private parseListLiteralExpression;
    /**
     * Parse a list comprehension after [variable IN has been consumed.
     * Full syntax: [variable IN listExpr WHERE filterCondition | mapExpr]
     * - WHERE and | are both optional
     */
    private parseListComprehension;
    /**
     * Parse a list predicate after PRED(variable IN has been consumed.
     * Syntax: ALL/ANY/NONE/SINGLE(variable IN listExpr WHERE filterCondition)
     * WHERE is required for list predicates.
     */
    private parseListPredicate;
    /**
     * Parse a condition in a list comprehension, where the variable can be used.
     * Similar to parseWhereCondition but resolves variable references.
     */
    private parseListComprehensionCondition;
    /**
     * Parse an expression in a list comprehension map projection.
     * Similar to parseExpression but the variable is in scope.
     */
    private parseListComprehensionExpression;
    private peek;
    private advance;
    private isAtEnd;
    private check;
    private checkKeyword;
    private expect;
    private expectIdentifier;
    private expectIdentifierOrKeyword;
    private expectLabelOrType;
}
export declare function parse(input: string): ParseResult;
//# sourceMappingURL=parser.d.ts.map