// Cypher Parser - Types and Implementation

// ============================================================================
// AST Types
// ============================================================================

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
}

export interface RelationshipPattern {
  source: NodePattern;
  edge: EdgePattern;
  target: NodePattern;
}

export type PropertyValue =
  | string
  | number
  | boolean
  | null
  | ParameterRef
  | PropertyValue[];

export interface ParameterRef {
  type: "parameter";
  name: string;
}

export interface WhereCondition {
  type: "comparison" | "and" | "or" | "not" | "contains" | "startsWith" | "endsWith";
  left?: Expression;
  right?: Expression;
  operator?: "=" | "<>" | "<" | ">" | "<=" | ">=";
  conditions?: WhereCondition[];
  condition?: WhereCondition;
}

export interface Expression {
  type: "property" | "literal" | "parameter" | "variable" | "function";
  variable?: string;
  property?: string;
  value?: PropertyValue;
  name?: string;
  functionName?: string;
  args?: Expression[];
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

// Clause types
export interface CreateClause {
  type: "CREATE";
  patterns: (NodePattern | RelationshipPattern)[];
}

export interface MatchClause {
  type: "MATCH";
  patterns: (NodePattern | RelationshipPattern)[];
  where?: WhereCondition;
}

export interface MergeClause {
  type: "MERGE";
  pattern: NodePattern;
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
  items: ReturnItem[];
  limit?: number;
  orderBy?: { expression: Expression; direction: "ASC" | "DESC" }[];
}

export type Clause =
  | CreateClause
  | MatchClause
  | MergeClause
  | SetClause
  | DeleteClause
  | ReturnClause;

export interface Query {
  clauses: Clause[];
}

export interface ParseError {
  message: string;
  position: number;
  line: number;
  column: number;
}

export type ParseResult =
  | { success: true; query: Query }
  | { success: false; error: ParseError };

// ============================================================================
// Tokenizer
// ============================================================================

type TokenType =
  | "KEYWORD"
  | "IDENTIFIER"
  | "STRING"
  | "NUMBER"
  | "PARAMETER"
  | "LPAREN"
  | "RPAREN"
  | "LBRACKET"
  | "RBRACKET"
  | "LBRACE"
  | "RBRACE"
  | "COLON"
  | "COMMA"
  | "DOT"
  | "ARROW_LEFT"
  | "ARROW_RIGHT"
  | "DASH"
  | "EQUALS"
  | "NOT_EQUALS"
  | "LT"
  | "GT"
  | "LTE"
  | "GTE"
  | "STAR"
  | "EOF";

interface Token {
  type: TokenType;
  value: string;
  position: number;
  line: number;
  column: number;
}

const KEYWORDS = new Set([
  "CREATE",
  "MATCH",
  "MERGE",
  "SET",
  "DELETE",
  "DETACH",
  "RETURN",
  "WHERE",
  "AND",
  "OR",
  "NOT",
  "LIMIT",
  "ORDER",
  "BY",
  "ASC",
  "DESC",
  "COUNT",
  "ON",
  "TRUE",
  "FALSE",
  "NULL",
  "CONTAINS",
  "STARTS",
  "ENDS",
  "WITH",
  "AS",
]);

class Tokenizer {
  private input: string;
  private pos: number = 0;
  private line: number = 1;
  private column: number = 1;
  private tokens: Token[] = [];

  constructor(input: string) {
    this.input = input;
  }

  tokenize(): Token[] {
    while (this.pos < this.input.length) {
      this.skipWhitespace();
      if (this.pos >= this.input.length) break;

      const token = this.nextToken();
      if (token) {
        this.tokens.push(token);
      }
    }

    this.tokens.push({
      type: "EOF",
      value: "",
      position: this.pos,
      line: this.line,
      column: this.column,
    });

    return this.tokens;
  }

  private skipWhitespace(): void {
    while (this.pos < this.input.length) {
      const char = this.input[this.pos];
      if (char === " " || char === "\t") {
        this.pos++;
        this.column++;
      } else if (char === "\n") {
        this.pos++;
        this.line++;
        this.column = 1;
      } else if (char === "\r") {
        this.pos++;
        if (this.input[this.pos] === "\n") {
          this.pos++;
        }
        this.line++;
        this.column = 1;
      } else {
        break;
      }
    }
  }

  private nextToken(): Token | null {
    const startPos = this.pos;
    const startLine = this.line;
    const startColumn = this.column;
    const char = this.input[this.pos];

    // Two-character operators
    if (this.pos + 1 < this.input.length) {
      const twoChars = this.input.slice(this.pos, this.pos + 2);
      if (twoChars === "<-") {
        this.pos += 2;
        this.column += 2;
        return { type: "ARROW_LEFT", value: "<-", position: startPos, line: startLine, column: startColumn };
      }
      if (twoChars === "->") {
        this.pos += 2;
        this.column += 2;
        return { type: "ARROW_RIGHT", value: "->", position: startPos, line: startLine, column: startColumn };
      }
      if (twoChars === "<>") {
        this.pos += 2;
        this.column += 2;
        return { type: "NOT_EQUALS", value: "<>", position: startPos, line: startLine, column: startColumn };
      }
      if (twoChars === "<=") {
        this.pos += 2;
        this.column += 2;
        return { type: "LTE", value: "<=", position: startPos, line: startLine, column: startColumn };
      }
      if (twoChars === ">=") {
        this.pos += 2;
        this.column += 2;
        return { type: "GTE", value: ">=", position: startPos, line: startLine, column: startColumn };
      }
    }

    // Single character tokens
    const singleCharTokens: Record<string, TokenType> = {
      "(": "LPAREN",
      ")": "RPAREN",
      "[": "LBRACKET",
      "]": "RBRACKET",
      "{": "LBRACE",
      "}": "RBRACE",
      ":": "COLON",
      ",": "COMMA",
      ".": "DOT",
      "-": "DASH",
      "=": "EQUALS",
      "<": "LT",
      ">": "GT",
      "*": "STAR",
    };

    if (singleCharTokens[char]) {
      this.pos++;
      this.column++;
      return { type: singleCharTokens[char], value: char, position: startPos, line: startLine, column: startColumn };
    }

    // Parameter
    if (char === "$") {
      this.pos++;
      this.column++;
      const name = this.readIdentifier();
      return { type: "PARAMETER", value: name, position: startPos, line: startLine, column: startColumn };
    }

    // String
    if (char === "'" || char === '"') {
      return this.readString(char, startPos, startLine, startColumn);
    }

    // Number
    if (this.isDigit(char) || (char === "-" && this.isDigit(this.input[this.pos + 1]))) {
      return this.readNumber(startPos, startLine, startColumn);
    }

    // Identifier or keyword
    if (this.isIdentifierStart(char)) {
      const value = this.readIdentifier();
      const upperValue = value.toUpperCase();
      const type: TokenType = KEYWORDS.has(upperValue) ? "KEYWORD" : "IDENTIFIER";
      return { type, value: type === "KEYWORD" ? upperValue : value, position: startPos, line: startLine, column: startColumn };
    }

    throw new Error(`Unexpected character '${char}' at position ${this.pos}`);
  }

  private readString(quote: string, startPos: number, startLine: number, startColumn: number): Token {
    this.pos++;
    this.column++;
    let value = "";

    while (this.pos < this.input.length) {
      const char = this.input[this.pos];
      if (char === quote) {
        this.pos++;
        this.column++;
        return { type: "STRING", value, position: startPos, line: startLine, column: startColumn };
      }
      if (char === "\\") {
        this.pos++;
        this.column++;
        const escaped = this.input[this.pos];
        if (escaped === "n") value += "\n";
        else if (escaped === "t") value += "\t";
        else if (escaped === "\\") value += "\\";
        else if (escaped === quote) value += quote;
        else value += escaped;
        this.pos++;
        this.column++;
      } else {
        value += char;
        this.pos++;
        this.column++;
      }
    }

    throw new Error(`Unterminated string at position ${startPos}`);
  }

  private readNumber(startPos: number, startLine: number, startColumn: number): Token {
    let value = "";

    if (this.input[this.pos] === "-") {
      value += "-";
      this.pos++;
      this.column++;
    }

    while (this.pos < this.input.length && this.isDigit(this.input[this.pos])) {
      value += this.input[this.pos];
      this.pos++;
      this.column++;
    }

    if (this.input[this.pos] === ".") {
      value += ".";
      this.pos++;
      this.column++;
      while (this.pos < this.input.length && this.isDigit(this.input[this.pos])) {
        value += this.input[this.pos];
        this.pos++;
        this.column++;
      }
    }

    return { type: "NUMBER", value, position: startPos, line: startLine, column: startColumn };
  }

  private readIdentifier(): string {
    let value = "";
    while (this.pos < this.input.length && this.isIdentifierChar(this.input[this.pos])) {
      value += this.input[this.pos];
      this.pos++;
      this.column++;
    }
    return value;
  }

  private isDigit(char: string): boolean {
    return char >= "0" && char <= "9";
  }

  private isIdentifierStart(char: string): boolean {
    return (char >= "a" && char <= "z") || (char >= "A" && char <= "Z") || char === "_";
  }

  private isIdentifierChar(char: string): boolean {
    return this.isIdentifierStart(char) || this.isDigit(char);
  }
}

// ============================================================================
// Parser
// ============================================================================

export class Parser {
  private tokens: Token[] = [];
  private pos: number = 0;

  parse(input: string): ParseResult {
    try {
      const tokenizer = new Tokenizer(input);
      this.tokens = tokenizer.tokenize();
      this.pos = 0;

      const clauses: Clause[] = [];

      while (!this.isAtEnd()) {
        const clause = this.parseClause();
        if (clause) {
          clauses.push(clause);
        }
      }

      if (clauses.length === 0) {
        return this.error("Empty query");
      }

      return { success: true, query: { clauses } };
    } catch (e) {
      const currentToken = this.tokens[this.pos] || this.tokens[this.tokens.length - 1];
      return {
        success: false,
        error: {
          message: e instanceof Error ? e.message : String(e),
          position: currentToken?.position ?? 0,
          line: currentToken?.line ?? 1,
          column: currentToken?.column ?? 1,
        },
      };
    }
  }

  private error(message: string): ParseResult {
    const currentToken = this.tokens[this.pos] || this.tokens[this.tokens.length - 1];
    return {
      success: false,
      error: {
        message,
        position: currentToken?.position ?? 0,
        line: currentToken?.line ?? 1,
        column: currentToken?.column ?? 1,
      },
    };
  }

  private parseClause(): Clause | null {
    const token = this.peek();

    if (token.type === "EOF") return null;

    if (token.type !== "KEYWORD") {
      throw new Error(`Unexpected token '${token.value}', expected a clause keyword like CREATE, MATCH, MERGE, SET, DELETE, or RETURN`);
    }

    switch (token.value) {
      case "CREATE":
        return this.parseCreate();
      case "MATCH":
        return this.parseMatch();
      case "MERGE":
        return this.parseMerge();
      case "SET":
        return this.parseSet();
      case "DELETE":
      case "DETACH":
        return this.parseDelete();
      case "RETURN":
        return this.parseReturn();
      default:
        throw new Error(`Unexpected keyword '${token.value}'`);
    }
  }

  private parseCreate(): CreateClause {
    this.expect("KEYWORD", "CREATE");
    const patterns: (NodePattern | RelationshipPattern)[] = [];

    patterns.push(this.parsePattern());

    while (this.check("COMMA")) {
      this.advance();
      patterns.push(this.parsePattern());
    }

    return { type: "CREATE", patterns };
  }

  private parseMatch(): MatchClause {
    this.expect("KEYWORD", "MATCH");
    const patterns: (NodePattern | RelationshipPattern)[] = [];

    patterns.push(this.parsePattern());

    while (this.check("COMMA")) {
      this.advance();
      patterns.push(this.parsePattern());
    }

    let where: WhereCondition | undefined;
    if (this.checkKeyword("WHERE")) {
      this.advance();
      where = this.parseWhereCondition();
    }

    return { type: "MATCH", patterns, where };
  }

  private parseMerge(): MergeClause {
    this.expect("KEYWORD", "MERGE");
    const pattern = this.parseNodePattern();

    let onCreateSet: SetAssignment[] | undefined;
    let onMatchSet: SetAssignment[] | undefined;

    while (this.checkKeyword("ON")) {
      this.advance();
      if (this.checkKeyword("CREATE")) {
        this.advance();
        this.expect("KEYWORD", "SET");
        onCreateSet = this.parseSetAssignments();
      } else if (this.checkKeyword("MATCH")) {
        this.advance();
        this.expect("KEYWORD", "SET");
        onMatchSet = this.parseSetAssignments();
      } else {
        throw new Error("Expected CREATE or MATCH after ON");
      }
    }

    return { type: "MERGE", pattern, onCreateSet, onMatchSet };
  }

  private parseSet(): SetClause {
    this.expect("KEYWORD", "SET");
    const assignments = this.parseSetAssignments();
    return { type: "SET", assignments };
  }

  private parseSetAssignments(): SetAssignment[] {
    const assignments: SetAssignment[] = [];

    do {
      if (assignments.length > 0) {
        this.expect("COMMA");
      }

      const variable = this.expectIdentifier();
      this.expect("DOT");
      const property = this.expectIdentifier();
      this.expect("EQUALS");
      const value = this.parseExpression();

      assignments.push({ variable, property, value });
    } while (this.check("COMMA"));

    return assignments;
  }

  private parseDelete(): DeleteClause {
    let detach = false;

    if (this.checkKeyword("DETACH")) {
      this.advance();
      detach = true;
    }

    this.expect("KEYWORD", "DELETE");
    const variables: string[] = [];

    variables.push(this.expectIdentifier());

    while (this.check("COMMA")) {
      this.advance();
      variables.push(this.expectIdentifier());
    }

    return { type: "DELETE", variables, detach };
  }

  private parseReturn(): ReturnClause {
    this.expect("KEYWORD", "RETURN");
    const items: ReturnItem[] = [];

    do {
      if (items.length > 0) {
        this.expect("COMMA");
      }

      const expression = this.parseExpression();
      let alias: string | undefined;

      if (this.checkKeyword("AS")) {
        this.advance();
        alias = this.expectIdentifierOrKeyword();
      }

      items.push({ expression, alias });
    } while (this.check("COMMA"));

    let limit: number | undefined;
    if (this.checkKeyword("LIMIT")) {
      this.advance();
      const limitToken = this.expect("NUMBER");
      limit = parseInt(limitToken.value, 10);
    }

    return { type: "RETURN", items, limit };
  }

  private parsePattern(): NodePattern | RelationshipPattern {
    const firstNode = this.parseNodePattern();

    // Check for relationship
    if (this.check("DASH") || this.check("ARROW_LEFT")) {
      const edge = this.parseEdgePattern();
      const targetNode = this.parseNodePattern();

      return {
        source: firstNode,
        edge,
        target: targetNode,
      };
    }

    return firstNode;
  }

  private parseNodePattern(): NodePattern {
    this.expect("LPAREN");

    const pattern: NodePattern = {};

    // Variable name
    if (this.check("IDENTIFIER")) {
      pattern.variable = this.advance().value;
    }

    // Label (can be identifier or keyword like "Order", "Set", etc.)
    if (this.check("COLON")) {
      this.advance();
      pattern.label = this.expectLabelOrType();
    }

    // Properties
    if (this.check("LBRACE")) {
      pattern.properties = this.parseProperties();
    }

    this.expect("RPAREN");

    return pattern;
  }

  private parseEdgePattern(): EdgePattern {
    let direction: "left" | "right" | "none" = "none";

    // Left arrow or dash
    if (this.check("ARROW_LEFT")) {
      this.advance();
      direction = "left";
    } else {
      this.expect("DASH");
    }

    const edge: EdgePattern = { direction };

    // Edge details in brackets
    if (this.check("LBRACKET")) {
      this.advance();

      // Variable name
      if (this.check("IDENTIFIER")) {
        edge.variable = this.advance().value;
      }

      // Type (can be identifier or keyword)
      if (this.check("COLON")) {
        this.advance();
        edge.type = this.expectLabelOrType();
      }

      // Properties
      if (this.check("LBRACE")) {
        edge.properties = this.parseProperties();
      }

      this.expect("RBRACKET");
    }

    // Right arrow or dash
    if (this.check("ARROW_RIGHT")) {
      this.advance();
      if (direction === "left") {
        throw new Error("Invalid relationship pattern: cannot have arrows on both sides");
      }
      direction = "right";
      edge.direction = direction;
    } else {
      this.expect("DASH");
    }

    return edge;
  }

  private parseProperties(): Record<string, PropertyValue> {
    this.expect("LBRACE");
    const properties: Record<string, PropertyValue> = {};

    if (!this.check("RBRACE")) {
      do {
        if (Object.keys(properties).length > 0) {
          this.expect("COMMA");
        }

        // Property keys can be identifiers OR keywords (like 'id', 'name', 'set', etc.)
        const key = this.expectIdentifierOrKeyword();
        this.expect("COLON");
        const value = this.parsePropertyValue();
        properties[key] = value;
      } while (this.check("COMMA"));
    }

    this.expect("RBRACE");
    return properties;
  }

  private parsePropertyValue(): PropertyValue {
    const token = this.peek();

    if (token.type === "STRING") {
      this.advance();
      return token.value;
    }

    if (token.type === "NUMBER") {
      this.advance();
      return parseFloat(token.value);
    }

    // Handle negative numbers: DASH followed by NUMBER
    if (token.type === "DASH") {
      const nextToken = this.tokens[this.pos + 1];
      if (nextToken && nextToken.type === "NUMBER") {
        this.advance(); // consume DASH
        this.advance(); // consume NUMBER
        return -parseFloat(nextToken.value);
      }
    }

    if (token.type === "PARAMETER") {
      this.advance();
      return { type: "parameter", name: token.value };
    }

    if (token.type === "KEYWORD") {
      if (token.value === "TRUE") {
        this.advance();
        return true;
      }
      if (token.value === "FALSE") {
        this.advance();
        return false;
      }
      if (token.value === "NULL") {
        this.advance();
        return null;
      }
    }

    if (token.type === "LBRACKET") {
      return this.parseArray();
    }

    throw new Error(`Expected property value, got ${token.type} '${token.value}'`);
  }

  private parseArray(): PropertyValue[] {
    this.expect("LBRACKET");
    const values: PropertyValue[] = [];

    if (!this.check("RBRACKET")) {
      do {
        if (values.length > 0) {
          this.expect("COMMA");
        }
        values.push(this.parsePropertyValue());
      } while (this.check("COMMA"));
    }

    this.expect("RBRACKET");
    return values;
  }

  private parseWhereCondition(): WhereCondition {
    return this.parseOrCondition();
  }

  private parseOrCondition(): WhereCondition {
    let left = this.parseAndCondition();

    while (this.checkKeyword("OR")) {
      this.advance();
      const right = this.parseAndCondition();
      left = { type: "or", conditions: [left, right] };
    }

    return left;
  }

  private parseAndCondition(): WhereCondition {
    let left = this.parseNotCondition();

    while (this.checkKeyword("AND")) {
      this.advance();
      const right = this.parseNotCondition();
      left = { type: "and", conditions: [left, right] };
    }

    return left;
  }

  private parseNotCondition(): WhereCondition {
    if (this.checkKeyword("NOT")) {
      this.advance();
      const condition = this.parseNotCondition();
      return { type: "not", condition };
    }

    return this.parseComparisonCondition();
  }

  private parseComparisonCondition(): WhereCondition {
    const left = this.parseExpression();

    // Check for string operations
    if (this.checkKeyword("CONTAINS")) {
      this.advance();
      const right = this.parseExpression();
      return { type: "contains", left, right };
    }

    if (this.checkKeyword("STARTS")) {
      this.advance();
      this.expect("KEYWORD", "WITH");
      const right = this.parseExpression();
      return { type: "startsWith", left, right };
    }

    if (this.checkKeyword("ENDS")) {
      this.advance();
      this.expect("KEYWORD", "WITH");
      const right = this.parseExpression();
      return { type: "endsWith", left, right };
    }

    // Comparison operators
    const opToken = this.peek();
    let operator: "=" | "<>" | "<" | ">" | "<=" | ">=" | undefined;

    if (opToken.type === "EQUALS") operator = "=";
    else if (opToken.type === "NOT_EQUALS") operator = "<>";
    else if (opToken.type === "LT") operator = "<";
    else if (opToken.type === "GT") operator = ">";
    else if (opToken.type === "LTE") operator = "<=";
    else if (opToken.type === "GTE") operator = ">=";

    if (operator) {
      this.advance();
      const right = this.parseExpression();
      return { type: "comparison", left, right, operator };
    }

    throw new Error(`Expected comparison operator, got ${opToken.type}`);
  }

  private parseExpression(): Expression {
    const token = this.peek();

    // Function call: COUNT(x), id(x)
    if (token.type === "KEYWORD" || token.type === "IDENTIFIER") {
      const nextToken = this.tokens[this.pos + 1];
      if (nextToken && nextToken.type === "LPAREN") {
        const functionName = this.advance().value.toUpperCase();
        this.advance(); // LPAREN
        const args: Expression[] = [];

        if (!this.check("RPAREN")) {
          do {
            if (args.length > 0) {
              this.expect("COMMA");
            }
            args.push(this.parseExpression());
          } while (this.check("COMMA"));
        }

        this.expect("RPAREN");
        return { type: "function", functionName, args };
      }
    }

    // Parameter
    if (token.type === "PARAMETER") {
      this.advance();
      return { type: "parameter", name: token.value };
    }

    // Literal values
    if (token.type === "STRING") {
      this.advance();
      return { type: "literal", value: token.value };
    }

    if (token.type === "NUMBER") {
      this.advance();
      return { type: "literal", value: parseFloat(token.value) };
    }

    if (token.type === "KEYWORD") {
      if (token.value === "TRUE") {
        this.advance();
        return { type: "literal", value: true };
      }
      if (token.value === "FALSE") {
        this.advance();
        return { type: "literal", value: false };
      }
      if (token.value === "NULL") {
        this.advance();
        return { type: "literal", value: null };
      }
    }

    // Variable or property access
    if (token.type === "IDENTIFIER") {
      const variable = this.advance().value;

      if (this.check("DOT")) {
        this.advance();
        const property = this.expectIdentifier();
        return { type: "property", variable, property };
      }

      return { type: "variable", variable };
    }

    throw new Error(`Expected expression, got ${token.type} '${token.value}'`);
  }

  // Token helpers

  private peek(): Token {
    return this.tokens[this.pos];
  }

  private advance(): Token {
    if (!this.isAtEnd()) {
      this.pos++;
    }
    return this.tokens[this.pos - 1];
  }

  private isAtEnd(): boolean {
    return this.peek().type === "EOF";
  }

  private check(type: TokenType): boolean {
    return this.peek().type === type;
  }

  private checkKeyword(keyword: string): boolean {
    const token = this.peek();
    return token.type === "KEYWORD" && token.value === keyword;
  }

  private expect(type: TokenType, value?: string): Token {
    const token = this.peek();
    if (token.type !== type || (value !== undefined && token.value !== value)) {
      throw new Error(`Expected ${type}${value ? ` '${value}'` : ""}, got ${token.type} '${token.value}'`);
    }
    return this.advance();
  }

  private expectIdentifier(): string {
    const token = this.peek();
    if (token.type !== "IDENTIFIER") {
      throw new Error(`Expected identifier, got ${token.type} '${token.value}'`);
    }
    return this.advance().value;
  }

  private expectIdentifierOrKeyword(): string {
    const token = this.peek();
    if (token.type !== "IDENTIFIER" && token.type !== "KEYWORD") {
      throw new Error(`Expected identifier or keyword, got ${token.type} '${token.value}'`);
    }
    // Keywords are stored uppercase, but property keys should be lowercase
    const value = this.advance().value;
    return token.type === "KEYWORD" ? value.toLowerCase() : value;
  }

  private expectLabelOrType(): string {
    const token = this.peek();
    if (token.type !== "IDENTIFIER" && token.type !== "KEYWORD") {
      throw new Error(`Expected label or type, got ${token.type} '${token.value}'`);
    }
    // Labels and types preserve their original case (but keywords are uppercase in token)
    const value = this.advance().value;
    // For labels/types, we want PascalCase typically - return as-is for identifiers
    // For keywords used as labels (like Order), return the capitalized version
    return token.type === "KEYWORD" 
      ? value.charAt(0) + value.slice(1).toLowerCase() 
      : value;
  }
}

// Convenience function
export function parse(input: string): ParseResult {
  return new Parser().parse(input);
}
