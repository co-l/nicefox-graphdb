export interface NodePattern {
    variable?: string;
    label?: string;
    properties?: Record<string, PropertyValue>;
}
export interface EdgePattern {
    variable?: string;
    type?: string;
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
export interface ParameterRef {
    type: "parameter";
    name: string;
}
export interface VariableRef {
    type: "variable";
    name: string;
}
export type PropertyValue = string | number | boolean | null | ParameterRef | VariableRef | PropertyValue[];
export interface WhereCondition {
    type: "comparison" | "and" | "or" | "not" | "contains" | "startsWith" | "endsWith" | "isNull" | "isNotNull" | "exists" | "in";
    left?: Expression;
    right?: Expression;
    operator?: "=" | "<>" | "<" | ">" | "<=" | ">=";
    conditions?: WhereCondition[];
    condition?: WhereCondition;
    pattern?: NodePattern | RelationshipPattern;
    list?: Expression;
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
    type: "property" | "literal" | "parameter" | "variable" | "function" | "case" | "binary" | "object" | "comparison";
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
    operator?: "+" | "-" | "*" | "/" | "%";
    left?: Expression;
    right?: Expression;
    comparisonOperator?: "=" | "<>" | "<" | ">" | "<=" | ">=";
    properties?: ObjectProperty[];
}
export interface ReturnItem {
    expression: Expression;
    alias?: string;
}
export interface SetAssignment {
    variable: string;
    property: string;
    value: Expression;
}
export interface CreateClause {
    type: "CREATE";
    patterns: (NodePattern | RelationshipPattern)[];
}
export interface MatchClause {
    type: "MATCH" | "OPTIONAL_MATCH";
    patterns: (NodePattern | RelationshipPattern)[];
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
    parse(input: string): ParseResult;
    private parseQuery;
    private error;
    private parseClause;
    private parseCreate;
    private parseMatch;
    private parseOptionalMatch;
    private parseMerge;
    private parseSet;
    private parseSetAssignments;
    private parseDelete;
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
    private parseArray;
    private parseWhereCondition;
    private parseOrCondition;
    private parseAndCondition;
    private parseNotCondition;
    private parsePrimaryCondition;
    private parseExistsCondition;
    private parseComparisonCondition;
    private parseInListExpression;
    private parseExpression;
    private parseReturnExpression;
    private parseAdditiveExpression;
    private parseMultiplicativeExpression;
    private parsePrimaryExpression;
    private parseCaseExpression;
    private parseObjectLiteral;
    private parseListLiteralExpression;
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