// Cypher Parser - Types and Implementation
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
    "IN",
    "LIMIT",
    "SKIP",
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
    "IS",
    "DISTINCT",
    "OPTIONAL",
    "UNWIND",
    "CASE",
    "WHEN",
    "THEN",
    "ELSE",
    "END",
    "EXISTS",
    "UNION",
    "ALL",
    "ANY",
    "NONE",
    "SINGLE",
    "CALL",
    "YIELD",
]);
class Tokenizer {
    input;
    pos = 0;
    line = 1;
    column = 1;
    tokens = [];
    constructor(input) {
        this.input = input;
    }
    tokenize() {
        while (this.pos < this.input.length) {
            this.skipWhitespace();
            if (this.pos >= this.input.length)
                break;
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
    skipWhitespace() {
        while (this.pos < this.input.length) {
            const char = this.input[this.pos];
            if (char === " " || char === "\t") {
                this.pos++;
                this.column++;
            }
            else if (char === "\n") {
                this.pos++;
                this.line++;
                this.column = 1;
            }
            else if (char === "\r") {
                this.pos++;
                if (this.input[this.pos] === "\n") {
                    this.pos++;
                }
                this.line++;
                this.column = 1;
            }
            else {
                break;
            }
        }
    }
    nextToken() {
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
        const singleCharTokens = {
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
            "+": "PLUS",
            "/": "SLASH",
            "%": "PERCENT",
            "^": "CARET",
            "=": "EQUALS",
            "<": "LT",
            ">": "GT",
            "*": "STAR",
            "|": "PIPE",
        };
        // Number - includes floats starting with . like .5
        // Check this before single char tokens so ".5" is parsed as number not DOT
        // But don't match "..3" as ".3" - only match if there's no preceding dot
        if (this.isDigit(char) || (char === "-" && this.isDigit(this.input[this.pos + 1])) || (char === "." && this.isDigit(this.input[this.pos + 1]) && (this.pos === 0 || this.input[this.pos - 1] !== "."))) {
            return this.readNumber(startPos, startLine, startColumn);
        }
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
        // Identifier or keyword
        if (this.isIdentifierStart(char)) {
            const value = this.readIdentifier();
            const upperValue = value.toUpperCase();
            const type = KEYWORDS.has(upperValue) ? "KEYWORD" : "IDENTIFIER";
            // Keywords store uppercase for matching, but we also preserve original casing for when keywords are used as identifiers
            return { type, value: type === "KEYWORD" ? upperValue : value, originalValue: value, position: startPos, line: startLine, column: startColumn };
        }
        throw new Error(`Unexpected character '${char}' at position ${this.pos}`);
    }
    readString(quote, startPos, startLine, startColumn) {
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
                if (escaped === "n")
                    value += "\n";
                else if (escaped === "t")
                    value += "\t";
                else if (escaped === "\\")
                    value += "\\";
                else if (escaped === quote)
                    value += quote;
                else
                    value += escaped;
                this.pos++;
                this.column++;
            }
            else {
                value += char;
                this.pos++;
                this.column++;
            }
        }
        throw new Error(`Unterminated string at position ${startPos}`);
    }
    readNumber(startPos, startLine, startColumn) {
        let value = "";
        if (this.input[this.pos] === "-") {
            value += "-";
            this.pos++;
            this.column++;
        }
        // Handle numbers starting with . like .5
        if (this.input[this.pos] === ".") {
            value += "0.";
            this.pos++;
            this.column++;
            while (this.pos < this.input.length && this.isDigit(this.input[this.pos])) {
                value += this.input[this.pos];
                this.pos++;
                this.column++;
            }
            return { type: "NUMBER", value, position: startPos, line: startLine, column: startColumn };
        }
        while (this.pos < this.input.length && this.isDigit(this.input[this.pos])) {
            value += this.input[this.pos];
            this.pos++;
            this.column++;
        }
        // Only read decimal part if . is followed by a digit (not another .)
        // This prevents "1..2" from being tokenized as "1." + "." + "2"
        if (this.input[this.pos] === "." && this.isDigit(this.input[this.pos + 1])) {
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
    readIdentifier() {
        let value = "";
        while (this.pos < this.input.length && this.isIdentifierChar(this.input[this.pos])) {
            value += this.input[this.pos];
            this.pos++;
            this.column++;
        }
        return value;
    }
    isDigit(char) {
        return char >= "0" && char <= "9";
    }
    isIdentifierStart(char) {
        return (char >= "a" && char <= "z") || (char >= "A" && char <= "Z") || char === "_";
    }
    isIdentifierChar(char) {
        return this.isIdentifierStart(char) || this.isDigit(char);
    }
}
// ============================================================================
// Parser
// ============================================================================
export class Parser {
    tokens = [];
    pos = 0;
    anonVarCounter = 0;
    parse(input) {
        try {
            const tokenizer = new Tokenizer(input);
            this.tokens = tokenizer.tokenize();
            this.pos = 0;
            const query = this.parseQuery();
            if (query.clauses.length === 0) {
                return this.error("Empty query");
            }
            return { success: true, query };
        }
        catch (e) {
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
    parseQuery() {
        // Parse clauses until we hit UNION or end
        const clauses = [];
        while (!this.isAtEnd() && !this.checkKeyword("UNION")) {
            const clause = this.parseClause();
            if (clause) {
                clauses.push(clause);
            }
        }
        // Check for UNION
        if (this.checkKeyword("UNION")) {
            this.advance(); // consume UNION
            // Check for ALL
            const all = this.checkKeyword("ALL");
            if (all) {
                this.advance();
            }
            // Parse the right side of the UNION
            const rightQuery = this.parseQuery();
            // Create a UNION clause that wraps both queries
            const unionClause = {
                type: "UNION",
                all,
                left: { clauses },
                right: rightQuery,
            };
            return { clauses: [unionClause] };
        }
        return { clauses };
    }
    error(message) {
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
    parseClause() {
        const token = this.peek();
        if (token.type === "EOF")
            return null;
        if (token.type !== "KEYWORD") {
            throw new Error(`Unexpected token '${token.value}', expected a clause keyword like CREATE, MATCH, MERGE, SET, DELETE, or RETURN`);
        }
        switch (token.value) {
            case "CREATE":
                return this.parseCreate();
            case "MATCH":
                return this.parseMatch(false);
            case "OPTIONAL":
                return this.parseOptionalMatch();
            case "MERGE":
                return this.parseMerge();
            case "SET":
                return this.parseSet();
            case "DELETE":
            case "DETACH":
                return this.parseDelete();
            case "RETURN":
                return this.parseReturn();
            case "WITH":
                return this.parseWith();
            case "UNWIND":
                return this.parseUnwind();
            case "CALL":
                return this.parseCall();
            default:
                throw new Error(`Unexpected keyword '${token.value}'`);
        }
    }
    parseCreate() {
        this.expect("KEYWORD", "CREATE");
        const patterns = [];
        patterns.push(...this.parsePatternChain());
        while (this.check("COMMA")) {
            this.advance();
            patterns.push(...this.parsePatternChain());
        }
        // Validate: CREATE requires relationship type and direction
        for (const pattern of patterns) {
            if ("edge" in pattern) {
                // This is a RelationshipPattern
                if (!pattern.edge.type && !pattern.edge.types) {
                    throw new Error("A relationship type is required to create a relationship");
                }
                if (pattern.edge.direction === "none") {
                    throw new Error("Only directed relationships are supported in CREATE");
                }
                // Multiple relationship types are not allowed in CREATE
                if (pattern.edge.types && pattern.edge.types.length > 1) {
                    throw new Error("A single relationship type must be specified for CREATE");
                }
                // Variable-length patterns are not allowed in CREATE
                if (pattern.edge.minHops !== undefined || pattern.edge.maxHops !== undefined) {
                    throw new Error("Variable length relationship patterns are not supported in CREATE");
                }
            }
        }
        return { type: "CREATE", patterns };
    }
    parseMatch(optional = false) {
        this.expect("KEYWORD", "MATCH");
        const patterns = [];
        const pathExpressions = [];
        // Parse first pattern or path expression
        const firstPattern = this.parsePatternOrPath();
        if ("type" in firstPattern && firstPattern.type === "path") {
            pathExpressions.push(firstPattern);
        }
        else {
            patterns.push(...(Array.isArray(firstPattern) ? firstPattern : [firstPattern]));
        }
        while (this.check("COMMA")) {
            this.advance();
            const nextPattern = this.parsePatternOrPath();
            if ("type" in nextPattern && nextPattern.type === "path") {
                pathExpressions.push(nextPattern);
            }
            else {
                patterns.push(...(Array.isArray(nextPattern) ? nextPattern : [nextPattern]));
            }
        }
        let where;
        if (this.checkKeyword("WHERE")) {
            this.advance();
            where = this.parseWhereCondition();
        }
        return {
            type: optional ? "OPTIONAL_MATCH" : "MATCH",
            patterns,
            pathExpressions: pathExpressions.length > 0 ? pathExpressions : undefined,
            where
        };
    }
    /**
     * Parse either a regular pattern chain or a named path expression.
     * Syntax: p = (a)-[r]->(b) or just (a)-[r]->(b)
     */
    parsePatternOrPath() {
        // Check for path expression syntax: identifier = pattern
        if (this.check("IDENTIFIER")) {
            const savedPos = this.pos;
            const identifier = this.advance().value;
            if (this.check("EQUALS")) {
                // This is a path expression: p = (a)-[r]->(b)
                this.advance(); // consume "="
                const patterns = this.parsePatternChain();
                return {
                    type: "path",
                    variable: identifier,
                    patterns
                };
            }
            else {
                // Not a path expression, backtrack
                this.pos = savedPos;
            }
        }
        // Regular pattern chain
        return this.parsePatternChain();
    }
    parseOptionalMatch() {
        this.expect("KEYWORD", "OPTIONAL");
        return this.parseMatch(true);
    }
    parseMerge() {
        this.expect("KEYWORD", "MERGE");
        const patterns = this.parsePatternChain();
        let onCreateSet;
        let onMatchSet;
        while (this.checkKeyword("ON")) {
            this.advance();
            if (this.checkKeyword("CREATE")) {
                this.advance();
                this.expect("KEYWORD", "SET");
                onCreateSet = this.parseSetAssignments();
            }
            else if (this.checkKeyword("MATCH")) {
                this.advance();
                this.expect("KEYWORD", "SET");
                onMatchSet = this.parseSetAssignments();
            }
            else {
                throw new Error("Expected CREATE or MATCH after ON");
            }
        }
        return { type: "MERGE", patterns, onCreateSet, onMatchSet };
    }
    parseSet() {
        this.expect("KEYWORD", "SET");
        const assignments = this.parseSetAssignments();
        return { type: "SET", assignments };
    }
    parseSetAssignments() {
        const assignments = [];
        do {
            if (assignments.length > 0) {
                this.expect("COMMA");
            }
            // Handle parenthesized expression: SET (n).property = value
            let variable;
            if (this.check("LPAREN")) {
                this.advance();
                variable = this.expectIdentifier();
                this.expect("RPAREN");
            }
            else {
                variable = this.expectIdentifier();
            }
            // Check for label assignment: SET n:Label or SET n :Label (with whitespace)
            if (this.check("COLON")) {
                // Label assignment: SET n:Label1:Label2
                const labels = [];
                while (this.check("COLON")) {
                    this.advance(); // consume ":"
                    labels.push(this.expectLabelOrType());
                }
                assignments.push({ variable, labels });
            }
            else if (this.check("PLUS")) {
                // Property merge: SET n += {props}
                this.advance(); // consume "+"
                this.expect("EQUALS");
                const value = this.parseExpression();
                assignments.push({ variable, value, mergeProps: true });
            }
            else if (this.check("EQUALS")) {
                // Property replace: SET n = {props}
                this.advance(); // consume "="
                const value = this.parseExpression();
                assignments.push({ variable, value, replaceProps: true });
            }
            else {
                // Property assignment: SET n.property = value
                this.expect("DOT");
                const property = this.expectIdentifier();
                this.expect("EQUALS");
                const value = this.parseExpression();
                assignments.push({ variable, property, value });
            }
        } while (this.check("COMMA"));
        return assignments;
    }
    parseDelete() {
        let detach = false;
        if (this.checkKeyword("DETACH")) {
            this.advance();
            detach = true;
        }
        this.expect("KEYWORD", "DELETE");
        const variables = [];
        const expressions = [];
        // Parse first delete target (can be simple variable or complex expression)
        this.parseDeleteTarget(variables, expressions);
        while (this.check("COMMA")) {
            this.advance();
            this.parseDeleteTarget(variables, expressions);
        }
        const result = { type: "DELETE", variables, detach };
        if (expressions.length > 0) {
            result.expressions = expressions;
        }
        return result;
    }
    parseDeleteTarget(variables, expressions) {
        // Check if this is a simple variable or a complex expression
        // Look ahead to see if it's identifier followed by [ (list access) or . (property access)
        const token = this.peek();
        if (token.type === "IDENTIFIER") {
            const nextToken = this.tokens[this.pos + 1];
            if (nextToken && (nextToken.type === "LBRACKET" || nextToken.type === "DOT")) {
                // This is a list access expression like friends[$index]
                // or a property access expression like nodes.key
                const expr = this.parseExpression();
                expressions.push(expr);
            }
            else {
                // Simple variable name
                variables.push(this.advance().value);
            }
        }
        else {
            // DELETE requires a variable or variable-based expression (like list[index])
            // Other expression types (literals, arithmetic, etc.) are not valid DELETE targets
            throw new Error(`Type mismatch: expected Node or Relationship but was ${token.type === "NUMBER" ? "Integer" : token.type === "STRING" ? "String" : token.value}`);
        }
    }
    parseReturn() {
        this.expect("KEYWORD", "RETURN");
        // Check for DISTINCT after RETURN
        let distinct;
        if (this.checkKeyword("DISTINCT")) {
            this.advance();
            distinct = true;
        }
        const items = [];
        // Check for RETURN * syntax (return all matched variables)
        if (this.check("STAR")) {
            this.advance();
            // Mark with special "*" variable to indicate return all
            items.push({ expression: { type: "variable", variable: "*" } });
            // After *, we might have additional items with comma (unlikely but possible)
            // e.g., RETURN *, count(*) AS cnt - but this is rare
        }
        if (items.length === 0 || this.check("COMMA")) {
            if (items.length > 0) {
                this.advance(); // consume comma after *
            }
            do {
                if (items.length > 0) {
                    this.expect("COMMA");
                }
                // Use parseReturnExpression to allow comparisons in RETURN items
                const expression = this.parseReturnExpression();
                let alias;
                if (this.checkKeyword("AS")) {
                    this.advance();
                    alias = this.expectIdentifierOrKeyword();
                }
                items.push({ expression, alias });
            } while (this.check("COMMA"));
        }
        // Parse ORDER BY
        let orderBy;
        if (this.checkKeyword("ORDER")) {
            this.advance();
            this.expect("KEYWORD", "BY");
            orderBy = [];
            do {
                if (orderBy.length > 0) {
                    this.expect("COMMA");
                }
                const expression = this.parseExpression();
                let direction = "ASC"; // Default to ASC
                if (this.checkKeyword("ASC")) {
                    this.advance();
                }
                else if (this.checkKeyword("DESC")) {
                    this.advance();
                    direction = "DESC";
                }
                orderBy.push({ expression, direction });
            } while (this.check("COMMA"));
        }
        // Parse SKIP
        let skip;
        if (this.checkKeyword("SKIP")) {
            this.advance();
            const skipToken = this.expect("NUMBER");
            skip = parseInt(skipToken.value, 10);
        }
        // Parse LIMIT
        let limit;
        if (this.checkKeyword("LIMIT")) {
            this.advance();
            const limitToken = this.expect("NUMBER");
            limit = parseInt(limitToken.value, 10);
        }
        return { type: "RETURN", distinct, items, orderBy, skip, limit };
    }
    parseWith() {
        this.expect("KEYWORD", "WITH");
        // Check for DISTINCT after WITH
        let distinct;
        if (this.checkKeyword("DISTINCT")) {
            this.advance();
            distinct = true;
        }
        const items = [];
        let star = false;
        // Check for WITH * syntax (pass through all variables)
        if (this.check("STAR")) {
            this.advance();
            star = true;
            // After *, we might have additional items with comma
            // e.g., WITH *, count(n) AS cnt
            // For now, we'll mark this with a special expression
            items.push({ expression: { type: "variable", variable: "*" } });
        }
        if (!star || this.check("COMMA")) {
            if (star) {
                this.advance(); // consume comma after *
            }
            do {
                if (items.length > (star ? 1 : 0)) {
                    this.expect("COMMA");
                }
                const expression = this.parseExpression();
                let alias;
                if (this.checkKeyword("AS")) {
                    this.advance();
                    alias = this.expectIdentifierOrKeyword();
                }
                items.push({ expression, alias });
            } while (this.check("COMMA"));
        }
        // Parse ORDER BY
        let orderBy;
        if (this.checkKeyword("ORDER")) {
            this.advance();
            this.expect("KEYWORD", "BY");
            orderBy = [];
            do {
                if (orderBy.length > 0) {
                    this.expect("COMMA");
                }
                const expression = this.parseExpression();
                let direction = "ASC"; // Default to ASC
                if (this.checkKeyword("ASC")) {
                    this.advance();
                }
                else if (this.checkKeyword("DESC")) {
                    this.advance();
                    direction = "DESC";
                }
                orderBy.push({ expression, direction });
            } while (this.check("COMMA"));
        }
        // Parse SKIP
        let skip;
        if (this.checkKeyword("SKIP")) {
            this.advance();
            const skipToken = this.expect("NUMBER");
            skip = parseInt(skipToken.value, 10);
        }
        // Parse LIMIT
        let limit;
        if (this.checkKeyword("LIMIT")) {
            this.advance();
            const limitToken = this.expect("NUMBER");
            limit = parseInt(limitToken.value, 10);
        }
        // Parse optional WHERE clause after WITH items
        let where;
        if (this.checkKeyword("WHERE")) {
            this.advance();
            where = this.parseWhereCondition();
        }
        return { type: "WITH", distinct, items, orderBy, skip, limit, where };
    }
    parseUnwind() {
        this.expect("KEYWORD", "UNWIND");
        const expression = this.parseUnwindExpression();
        this.expect("KEYWORD", "AS");
        const alias = this.expectIdentifier();
        return { type: "UNWIND", expression, alias };
    }
    parseUnwindExpression() {
        const token = this.peek();
        // NULL literal - UNWIND null produces empty result
        if (token.type === "KEYWORD" && token.value.toUpperCase() === "NULL") {
            this.advance();
            return { type: "literal", value: null };
        }
        // Parenthesized expression like (first + second)
        if (token.type === "LPAREN") {
            this.advance();
            const expr = this.parseExpression();
            this.expect("RPAREN");
            return expr;
        }
        // Array literal
        if (token.type === "LBRACKET") {
            const values = this.parseArray();
            return { type: "literal", value: values };
        }
        // Parameter
        if (token.type === "PARAMETER") {
            this.advance();
            return { type: "parameter", name: token.value };
        }
        // Function call like range(1, 10)
        if ((token.type === "IDENTIFIER" || token.type === "KEYWORD") && this.tokens[this.pos + 1]?.type === "LPAREN") {
            return this.parseExpression();
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
        throw new Error(`Expected array, parameter, or variable in UNWIND, got ${token.type} '${token.value}'`);
    }
    parseCall() {
        this.expect("KEYWORD", "CALL");
        // Parse procedure name (e.g., "db.labels" or "db.relationshipTypes")
        // Procedure names can have dots, so we parse identifier.identifier...
        let procedureName = this.expectIdentifier();
        while (this.check("DOT")) {
            this.advance();
            procedureName += "." + this.expectIdentifier();
        }
        // Parse arguments in parentheses
        this.expect("LPAREN");
        const args = [];
        if (!this.check("RPAREN")) {
            do {
                if (args.length > 0) {
                    this.expect("COMMA");
                }
                args.push(this.parseExpression());
            } while (this.check("COMMA"));
        }
        this.expect("RPAREN");
        // Parse optional YIELD clause
        let yields;
        let where;
        if (this.checkKeyword("YIELD")) {
            this.advance();
            yields = [];
            // Parse yielded field names (can be identifiers or keywords like 'count')
            do {
                if (yields.length > 0) {
                    this.expect("COMMA");
                }
                yields.push(this.expectIdentifierOrKeyword());
            } while (this.check("COMMA"));
            // Parse optional WHERE after YIELD
            if (this.checkKeyword("WHERE")) {
                this.advance();
                where = this.parseWhereCondition();
            }
        }
        return { type: "CALL", procedure: procedureName, args, yields, where };
    }
    /**
     * Parse a pattern, which can be a single node or a chain of relationships.
     * For chained patterns like (a)-[:R1]->(b)-[:R2]->(c), this returns multiple
     * RelationshipPattern objects via parsePatternChain.
     */
    parsePattern() {
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
    /**
     * Parse a pattern chain, returning an array of patterns.
     * Handles multi-hop patterns like (a)-[:R1]->(b)-[:R2]->(c).
     */
    parsePatternChain() {
        const patterns = [];
        const firstNode = this.parseNodePattern();
        // Check for relationship chain
        if (!this.check("DASH") && !this.check("ARROW_LEFT")) {
            // Just a single node
            return [firstNode];
        }
        // Parse first relationship
        let currentSource = firstNode;
        while (this.check("DASH") || this.check("ARROW_LEFT")) {
            const edge = this.parseEdgePattern();
            const targetNode = this.parseNodePattern();
            // Check if there's another relationship pattern coming after this one
            const hasMoreRelationships = this.check("DASH") || this.check("ARROW_LEFT");
            // If target is anonymous (no variable) AND there's more patterns coming,
            // assign a synthetic variable for chaining.
            // This ensures patterns like (:A)<-[:R]-(:B)-[:S]->(:C) share the (:B) node
            // But don't do this for standalone patterns like CREATE ()-[:R]->()
            if (!targetNode.variable && hasMoreRelationships) {
                targetNode.variable = `_anon${this.anonVarCounter++}`;
            }
            patterns.push({
                source: currentSource,
                edge,
                target: targetNode,
            });
            // For the next hop, the source is a reference to the previous target (variable only, no label)
            currentSource = { variable: targetNode.variable };
        }
        return patterns;
    }
    parseNodePattern() {
        this.expect("LPAREN");
        const pattern = {};
        // Variable name
        if (this.check("IDENTIFIER")) {
            pattern.variable = this.advance().value;
        }
        // Labels (can be multiple: :A:B:C)
        if (this.check("COLON")) {
            const labels = [];
            while (this.check("COLON")) {
                this.advance(); // consume ":"
                labels.push(this.expectLabelOrType());
            }
            // Store as array if multiple labels, string if single (for backward compatibility)
            pattern.label = labels.length === 1 ? labels[0] : labels;
        }
        // Properties
        if (this.check("LBRACE")) {
            pattern.properties = this.parseProperties();
        }
        this.expect("RPAREN");
        return pattern;
    }
    parseEdgePattern() {
        let direction = "none";
        // Left arrow or dash
        if (this.check("ARROW_LEFT")) {
            this.advance();
            direction = "left";
        }
        else {
            this.expect("DASH");
        }
        const edge = { direction };
        // Edge details in brackets
        if (this.check("LBRACKET")) {
            this.advance();
            // Variable name
            if (this.check("IDENTIFIER")) {
                edge.variable = this.advance().value;
            }
            // Type (can be identifier or keyword, or multiple types separated by |)
            if (this.check("COLON")) {
                this.advance();
                const firstType = this.expectLabelOrType();
                // Check for multiple types: [:TYPE1|TYPE2|TYPE3] or [:TYPE1|:TYPE2]
                if (this.check("PIPE")) {
                    const types = [firstType];
                    while (this.check("PIPE")) {
                        this.advance();
                        // Some Cypher dialects allow :TYPE after the pipe, consume the optional colon
                        if (this.check("COLON")) {
                            this.advance();
                        }
                        types.push(this.expectLabelOrType());
                    }
                    edge.types = types;
                }
                else {
                    edge.type = firstType;
                }
            }
            // Variable-length pattern: *[min]..[max] or *n or *
            if (this.check("STAR")) {
                this.advance();
                this.parseVariableLengthSpec(edge);
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
                // <--> pattern means "either direction" (bidirectional), same as --
                direction = "none";
            }
            else {
                direction = "right";
            }
            edge.direction = direction;
        }
        else {
            this.expect("DASH");
        }
        return edge;
    }
    parseVariableLengthSpec(edge) {
        // Patterns:
        // *       -> min=1, max=undefined (any length >= 1)
        // *2      -> min=2, max=2 (fixed length)
        // *1..3   -> min=1, max=3 (range)
        // *2..    -> min=2, max=undefined (min only)
        // *..3    -> min=1, max=3 (max only)
        // *0..3   -> min=0, max=3 (can include zero-length)
        // Check for just * with no numbers or dots
        if (!this.check("NUMBER") && !this.check("DOT")) {
            edge.minHops = 1;
            edge.maxHops = undefined;
            return;
        }
        // Check for ..N pattern (*..3) or just *.. (unbounded from 1)
        if (this.check("DOT")) {
            this.advance(); // first dot
            this.expect("DOT"); // second dot
            edge.minHops = 1;
            if (this.check("NUMBER")) {
                edge.maxHops = parseInt(this.advance().value, 10);
            }
            else {
                edge.maxHops = undefined; // unbounded
            }
            return;
        }
        // Parse first number
        const firstNum = parseInt(this.expect("NUMBER").value, 10);
        // Check if this is a range or fixed
        if (this.check("DOT")) {
            this.advance(); // first dot
            // Need to check if next is DOT or if dots were consecutive
            if (this.check("DOT")) {
                this.advance(); // second dot
            }
            // If we just advanced past a DOT and the tokenizer gave us separate dots,
            // we need to handle this. Let's check the current token
            edge.minHops = firstNum;
            // Check for second number
            if (this.check("NUMBER")) {
                edge.maxHops = parseInt(this.advance().value, 10);
            }
            else {
                edge.maxHops = undefined; // unbounded
            }
        }
        else {
            // Fixed length
            edge.minHops = firstNum;
            edge.maxHops = firstNum;
        }
    }
    parseProperties() {
        this.expect("LBRACE");
        const properties = {};
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
    parsePropertyValue() {
        // Parse the primary property value first
        let left = this.parsePrimaryPropertyValue();
        // Check for binary operators: +, -, *, /, %
        while (this.check("PLUS") || this.check("DASH") || this.check("STAR") || this.check("SLASH") || this.check("PERCENT")) {
            const opToken = this.advance();
            let operator;
            if (opToken.type === "PLUS")
                operator = "+";
            else if (opToken.type === "DASH")
                operator = "-";
            else if (opToken.type === "STAR")
                operator = "*";
            else if (opToken.type === "SLASH")
                operator = "/";
            else
                operator = "%";
            const right = this.parsePrimaryPropertyValue();
            left = { type: "binary", operator, left, right };
        }
        return left;
    }
    parsePrimaryPropertyValue() {
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
        // Handle variable references (e.g., from UNWIND), property access (e.g., person.bornIn), or function calls (e.g., datetime())
        if (token.type === "IDENTIFIER") {
            this.advance();
            const varName = token.value;
            // Check for function call: identifier followed by LPAREN
            if (this.check("LPAREN")) {
                this.advance(); // consume LPAREN
                const args = [];
                // Parse function arguments
                if (!this.check("RPAREN")) {
                    do {
                        if (args.length > 0) {
                            this.expect("COMMA");
                        }
                        args.push(this.parsePropertyValue());
                    } while (this.check("COMMA"));
                }
                this.expect("RPAREN");
                return { type: "function", name: varName.toUpperCase(), args };
            }
            // Check for property access: variable.property
            if (this.check("DOT")) {
                this.advance(); // consume DOT
                const propToken = this.expect("IDENTIFIER");
                return { type: "property", variable: varName, property: propToken.value };
            }
            return { type: "variable", name: varName };
        }
        throw new Error(`Expected property value, got ${token.type} '${token.value}'`);
    }
    parseArray() {
        this.expect("LBRACKET");
        const values = [];
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
    parseWhereCondition() {
        return this.parseOrCondition();
    }
    parseOrCondition() {
        let left = this.parseAndCondition();
        while (this.checkKeyword("OR")) {
            this.advance();
            const right = this.parseAndCondition();
            left = { type: "or", conditions: [left, right] };
        }
        return left;
    }
    parseAndCondition() {
        let left = this.parseNotCondition();
        while (this.checkKeyword("AND")) {
            this.advance();
            const right = this.parseNotCondition();
            left = { type: "and", conditions: [left, right] };
        }
        return left;
    }
    parseNotCondition() {
        if (this.checkKeyword("NOT")) {
            this.advance();
            const condition = this.parseNotCondition();
            return { type: "not", condition };
        }
        return this.parsePrimaryCondition();
    }
    parsePrimaryCondition() {
        // Handle EXISTS pattern
        if (this.checkKeyword("EXISTS")) {
            return this.parseExistsCondition();
        }
        // Handle list predicates: ALL, ANY, NONE, SINGLE
        const listPredicates = ["ALL", "ANY", "NONE", "SINGLE"];
        if (this.peek().type === "KEYWORD" && listPredicates.includes(this.peek().value)) {
            const nextToken = this.tokens[this.pos + 1];
            if (nextToken && nextToken.type === "LPAREN") {
                return this.parseListPredicateCondition();
            }
        }
        // Handle parenthesized conditions
        if (this.check("LPAREN")) {
            this.advance(); // consume (
            const condition = this.parseOrCondition(); // parse the inner condition
            this.expect("RPAREN"); // consume )
            return condition;
        }
        return this.parseComparisonCondition();
    }
    parseListPredicateCondition() {
        // Parse list predicate as a condition (for use in WHERE clause)
        const predicateType = this.advance().value.toUpperCase();
        this.expect("LPAREN");
        // Expect variable followed by IN
        const variable = this.expectIdentifier();
        this.expect("KEYWORD", "IN");
        // Parse the source list expression
        const listExpr = this.parseExpression();
        // WHERE clause is required for list predicates
        if (!this.checkKeyword("WHERE")) {
            throw new Error(`Expected WHERE after list expression in ${predicateType}()`);
        }
        this.advance(); // consume WHERE
        // Parse the filter condition
        const filterCondition = this.parseListComprehensionCondition(variable);
        this.expect("RPAREN");
        return {
            type: "listPredicate",
            predicateType,
            variable,
            listExpr,
            filterCondition,
        };
    }
    parseExistsCondition() {
        this.expect("KEYWORD", "EXISTS");
        this.expect("LPAREN"); // outer (
        // Parse the pattern inside EXISTS((pattern))
        const patterns = this.parsePatternChain();
        const pattern = patterns.length === 1 ? patterns[0] : patterns[0]; // Use first pattern for now
        this.expect("RPAREN"); // outer )
        return { type: "exists", pattern };
    }
    parseComparisonCondition() {
        const left = this.parseExpression();
        // Check for IS NULL / IS NOT NULL
        if (this.checkKeyword("IS")) {
            this.advance();
            if (this.checkKeyword("NOT")) {
                this.advance();
                this.expect("KEYWORD", "NULL");
                return { type: "isNotNull", left };
            }
            else {
                this.expect("KEYWORD", "NULL");
                return { type: "isNull", left };
            }
        }
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
        // Check for IN operator
        if (this.checkKeyword("IN")) {
            this.advance();
            // IN can be followed by a list literal [...] or a parameter $param
            const listExpr = this.parseInListExpression();
            return { type: "in", left, list: listExpr };
        }
        // Comparison operators
        const opToken = this.peek();
        let operator;
        if (opToken.type === "EQUALS")
            operator = "=";
        else if (opToken.type === "NOT_EQUALS")
            operator = "<>";
        else if (opToken.type === "LT")
            operator = "<";
        else if (opToken.type === "GT")
            operator = ">";
        else if (opToken.type === "LTE")
            operator = "<=";
        else if (opToken.type === "GTE")
            operator = ">=";
        if (operator) {
            this.advance();
            const right = this.parseExpression();
            return { type: "comparison", left, right, operator };
        }
        throw new Error(`Expected comparison operator, got ${opToken.type}`);
    }
    parseInListExpression() {
        const token = this.peek();
        // Array literal [...]
        if (token.type === "LBRACKET") {
            const values = this.parseArray();
            return { type: "literal", value: values };
        }
        // Parameter $param
        if (token.type === "PARAMETER") {
            this.advance();
            return { type: "parameter", name: token.value };
        }
        // Variable reference
        if (token.type === "IDENTIFIER") {
            const variable = this.advance().value;
            if (this.check("DOT")) {
                this.advance();
                const property = this.expectIdentifier();
                return { type: "property", variable, property };
            }
            return { type: "variable", variable };
        }
        throw new Error(`Expected array, parameter, or variable in IN clause, got ${token.type} '${token.value}'`);
    }
    parseExpression() {
        return this.parseAdditiveExpression();
    }
    // Parse expression that may include comparison and logical operators (for RETURN items)
    parseReturnExpression() {
        return this.parseOrExpression();
    }
    // Handle OR (lowest precedence for logical operators)
    parseOrExpression() {
        let left = this.parseAndExpression();
        while (this.checkKeyword("OR")) {
            this.advance();
            const right = this.parseAndExpression();
            left = { type: "binary", operator: "OR", left, right };
        }
        return left;
    }
    // Handle AND (higher precedence than OR)
    parseAndExpression() {
        let left = this.parseNotExpression();
        while (this.checkKeyword("AND")) {
            this.advance();
            const right = this.parseNotExpression();
            left = { type: "binary", operator: "AND", left, right };
        }
        return left;
    }
    // Handle NOT (highest precedence for logical operators)
    parseNotExpression() {
        if (this.checkKeyword("NOT")) {
            this.advance();
            const operand = this.parseNotExpression();
            return { type: "unary", operator: "NOT", operand };
        }
        return this.parseComparisonExpression();
    }
    // Handle comparison operators
    parseComparisonExpression() {
        let left = this.parseAdditiveExpression();
        // Check for IS NULL / IS NOT NULL
        if (this.checkKeyword("IS")) {
            this.advance();
            if (this.checkKeyword("NOT")) {
                this.advance();
                this.expect("KEYWORD", "NULL");
                return { type: "comparison", comparisonOperator: "IS NOT NULL", left };
            }
            else {
                this.expect("KEYWORD", "NULL");
                return { type: "comparison", comparisonOperator: "IS NULL", left };
            }
        }
        // Check for comparison operators
        const opToken = this.peek();
        let comparisonOperator;
        if (opToken.type === "EQUALS")
            comparisonOperator = "=";
        else if (opToken.type === "NOT_EQUALS")
            comparisonOperator = "<>";
        else if (opToken.type === "LT")
            comparisonOperator = "<";
        else if (opToken.type === "GT")
            comparisonOperator = ">";
        else if (opToken.type === "LTE")
            comparisonOperator = "<=";
        else if (opToken.type === "GTE")
            comparisonOperator = ">=";
        if (comparisonOperator) {
            this.advance();
            const right = this.parseAdditiveExpression();
            return { type: "comparison", comparisonOperator, left, right };
        }
        return left;
    }
    // Handle + and - (lower precedence)
    parseAdditiveExpression() {
        let left = this.parseMultiplicativeExpression();
        while (this.check("PLUS") || this.check("DASH")) {
            const operatorToken = this.advance();
            const operator = operatorToken.type === "PLUS" ? "+" : "-";
            const right = this.parseMultiplicativeExpression();
            left = { type: "binary", operator: operator, left, right };
        }
        return left;
    }
    // Handle *, /, % (higher precedence than +, -)
    parseMultiplicativeExpression() {
        let left = this.parseExponentialExpression();
        while (this.check("STAR") || this.check("SLASH") || this.check("PERCENT")) {
            const operatorToken = this.advance();
            let operator;
            if (operatorToken.type === "STAR")
                operator = "*";
            else if (operatorToken.type === "SLASH")
                operator = "/";
            else
                operator = "%";
            const right = this.parseExponentialExpression();
            left = { type: "binary", operator, left, right };
        }
        return left;
    }
    // Handle ^ (exponentiation - highest precedence among arithmetic operators)
    parseExponentialExpression() {
        let left = this.parsePostfixExpression();
        while (this.check("CARET")) {
            this.advance(); // consume ^
            const right = this.parsePostfixExpression();
            left = { type: "binary", operator: "^", left, right };
        }
        return left;
    }
    // Handle postfix operations: list indexing like expr[0] or expr[1..3], and chained property access like a.b.c
    parsePostfixExpression() {
        let expr = this.parsePrimaryExpression();
        // Handle list/map indexing: expr[index] or expr[start..end], and chained property access: expr.prop
        while (this.check("LBRACKET") || this.check("DOT")) {
            if (this.check("DOT")) {
                this.advance(); // consume .
                // Property access - property names can be keywords too
                const property = this.expectIdentifierOrKeyword();
                // Convert to propertyAccess expression for chained access
                expr = { type: "propertyAccess", object: expr, property };
            }
            else {
                // LBRACKET
                this.advance(); // consume [
                // Check for slice syntax [start..end]
                if (this.check("DOT")) {
                    // [..end] - from start
                    this.advance(); // consume first .
                    this.expect("DOT"); // consume second .
                    const endExpr = this.parseExpression();
                    this.expect("RBRACKET");
                    expr = { type: "function", functionName: "SLICE", args: [expr, { type: "literal", value: null }, endExpr] };
                }
                else {
                    const indexExpr = this.parseExpression();
                    if (this.check("DOT")) {
                        // Check for slice: [start..end]
                        this.advance(); // consume first .
                        if (this.check("DOT")) {
                            this.advance(); // consume second .
                            if (this.check("RBRACKET")) {
                                // [start..] - to end
                                this.expect("RBRACKET");
                                expr = { type: "function", functionName: "SLICE", args: [expr, indexExpr, { type: "literal", value: null }] };
                            }
                            else {
                                const endExpr = this.parseExpression();
                                this.expect("RBRACKET");
                                expr = { type: "function", functionName: "SLICE", args: [expr, indexExpr, endExpr] };
                            }
                        }
                        else {
                            throw new Error("Expected '..' for slice syntax");
                        }
                    }
                    else {
                        // Simple index: [index]
                        this.expect("RBRACKET");
                        expr = { type: "function", functionName: "INDEX", args: [expr, indexExpr] };
                    }
                }
            }
        }
        return expr;
    }
    // Parse primary expressions (atoms)
    parsePrimaryExpression() {
        const token = this.peek();
        // List literal [1, 2, 3]
        if (token.type === "LBRACKET") {
            return this.parseListLiteralExpression();
        }
        // Object literal { key: value, ... }
        if (token.type === "LBRACE") {
            return this.parseObjectLiteral();
        }
        // Parenthesized expression for grouping or label predicate (n:Label)
        if (token.type === "LPAREN") {
            // Check for label predicate: (n:Label) or (n:Label1:Label2)
            // Look ahead: ( IDENTIFIER COLON ...
            const nextToken = this.tokens[this.pos + 1];
            const afterNext = this.tokens[this.pos + 2];
            if (nextToken?.type === "IDENTIFIER" && afterNext?.type === "COLON") {
                this.advance(); // consume (
                const variable = this.advance().value; // consume identifier
                // Parse one or more labels
                const labelsList = [];
                while (this.check("COLON")) {
                    this.advance(); // consume :
                    labelsList.push(this.expectLabelOrType());
                }
                this.expect("RPAREN");
                if (labelsList.length === 1) {
                    return { type: "labelPredicate", variable, label: labelsList[0] };
                }
                else {
                    return { type: "labelPredicate", variable, labels: labelsList };
                }
            }
            // Regular parenthesized expression - use full expression parsing including AND/OR
            this.advance(); // consume (
            const expr = this.parseOrExpression();
            this.expect("RPAREN");
            return expr;
        }
        // CASE expression
        if (this.checkKeyword("CASE")) {
            return this.parseCaseExpression();
        }
        // Function call: COUNT(x), id(x), count(DISTINCT x), COUNT(*)
        // Also handles list predicates: ALL(x IN list WHERE cond), ANY(...), NONE(...), SINGLE(...)
        if (token.type === "KEYWORD" || token.type === "IDENTIFIER") {
            const nextToken = this.tokens[this.pos + 1];
            if (nextToken && nextToken.type === "LPAREN") {
                const functionName = this.advance().value.toUpperCase();
                this.advance(); // LPAREN
                // Check if this is a list predicate: ALL, ANY, NONE, SINGLE
                const listPredicates = ["ALL", "ANY", "NONE", "SINGLE"];
                if (listPredicates.includes(functionName)) {
                    // Check for list predicate syntax: PRED(var IN list WHERE cond)
                    // Lookahead to see if next is identifier followed by IN
                    if (this.check("IDENTIFIER")) {
                        const savedPos = this.pos;
                        const varToken = this.advance();
                        if (this.checkKeyword("IN")) {
                            // This is a list predicate
                            this.advance(); // consume IN
                            return this.parseListPredicate(functionName, varToken.value);
                        }
                        else {
                            // Not a list predicate syntax, backtrack
                            this.pos = savedPos;
                        }
                    }
                }
                const args = [];
                // Check for DISTINCT keyword after opening paren (for aggregation functions)
                let distinct;
                if (this.checkKeyword("DISTINCT")) {
                    this.advance();
                    distinct = true;
                }
                // Special case: COUNT(*) - handle STAR token as "count all"
                if (this.check("STAR")) {
                    this.advance(); // consume STAR
                    // COUNT(*) has no arguments - the * means "count all rows"
                    this.expect("RPAREN");
                    return { type: "function", functionName, args: [], distinct };
                }
                if (!this.check("RPAREN")) {
                    do {
                        if (args.length > 0) {
                            this.expect("COMMA");
                        }
                        args.push(this.parseExpression());
                    } while (this.check("COMMA"));
                }
                this.expect("RPAREN");
                return { type: "function", functionName, args, distinct };
            }
        }
        // Parameter
        if (token.type === "PARAMETER") {
            this.advance();
            return { type: "parameter", name: token.value };
        }
        // Unary minus for negative numbers
        if (token.type === "DASH") {
            this.advance(); // consume the dash
            const nextToken = this.peek();
            if (nextToken.type === "NUMBER") {
                this.advance();
                return { type: "literal", value: -parseFloat(nextToken.value) };
            }
            // For more complex expressions, create a unary minus operation
            const operand = this.parsePrimaryExpression();
            return { type: "binary", operator: "-", left: { type: "literal", value: 0 }, right: operand };
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
        // Allow keywords to be used as variable names when not in keyword position
        if (token.type === "IDENTIFIER" || (token.type === "KEYWORD" && !["TRUE", "FALSE", "NULL", "CASE"].includes(token.value))) {
            const tok = this.advance();
            // Use original casing for keywords used as identifiers
            const variable = tok.originalValue || tok.value;
            if (this.check("DOT")) {
                this.advance();
                // Property names can also be keywords (like 'count', 'order', etc.)
                const property = this.expectIdentifierOrKeyword();
                return { type: "property", variable, property };
            }
            return { type: "variable", variable };
        }
        throw new Error(`Expected expression, got ${token.type} '${token.value}'`);
    }
    parseCaseExpression() {
        this.expect("KEYWORD", "CASE");
        // Check for simple form: CASE expr WHEN val THEN ...
        // vs searched form: CASE WHEN condition THEN ...
        let caseExpr;
        // If the next token is not WHEN, it's a simple form with an expression
        if (!this.checkKeyword("WHEN")) {
            caseExpr = this.parseExpression();
        }
        const whens = [];
        // Parse WHEN ... THEN ... clauses
        while (this.checkKeyword("WHEN")) {
            this.advance(); // consume WHEN
            let condition;
            if (caseExpr) {
                // Simple form: CASE expr WHEN value THEN ...
                // The value is compared for equality with caseExpr
                const whenValue = this.parseExpression();
                // Create an equality comparison condition
                condition = {
                    type: "comparison",
                    left: caseExpr,
                    right: whenValue,
                    operator: "="
                };
            }
            else {
                // Searched form: CASE WHEN condition THEN ...
                condition = this.parseWhereCondition();
            }
            this.expect("KEYWORD", "THEN");
            const result = this.parseExpression();
            whens.push({ condition, result });
        }
        // Parse optional ELSE
        let elseExpr;
        if (this.checkKeyword("ELSE")) {
            this.advance();
            elseExpr = this.parseExpression();
        }
        this.expect("KEYWORD", "END");
        return {
            type: "case",
            expression: caseExpr,
            whens,
            elseExpr,
        };
    }
    parseObjectLiteral() {
        this.expect("LBRACE");
        const properties = [];
        if (!this.check("RBRACE")) {
            do {
                if (properties.length > 0) {
                    this.expect("COMMA");
                }
                // Property keys can be identifiers or keywords
                const key = this.expectIdentifierOrKeyword();
                this.expect("COLON");
                // Use parseReturnExpression to support comparisons like {foo: a.name='Andres'}
                const value = this.parseReturnExpression();
                properties.push({ key, value });
            } while (this.check("COMMA"));
        }
        this.expect("RBRACE");
        return { type: "object", properties };
    }
    parseListLiteralExpression() {
        this.expect("LBRACKET");
        // Check for list comprehension: [x IN list WHERE cond | expr]
        // We need to look ahead to see if this is a list comprehension
        if (this.check("IDENTIFIER")) {
            const savedPos = this.pos;
            const identifier = this.advance().value;
            if (this.checkKeyword("IN")) {
                // This is a list comprehension
                this.advance(); // consume "IN"
                return this.parseListComprehension(identifier);
            }
            else {
                // Not a list comprehension, backtrack
                this.pos = savedPos;
            }
        }
        // Regular list literal - elements can be full expressions (including objects)
        const elements = [];
        if (!this.check("RBRACKET")) {
            do {
                if (elements.length > 0) {
                    this.expect("COMMA");
                }
                elements.push(this.parseExpression());
            } while (this.check("COMMA"));
        }
        this.expect("RBRACKET");
        // If all elements are literals, return as literal list
        // Otherwise wrap in a function-like expression for arrays of expressions
        const allLiterals = elements.every(e => e.type === "literal");
        if (allLiterals) {
            return { type: "literal", value: elements.map(e => e.value) };
        }
        // For lists containing expressions, use a special function type
        return { type: "function", functionName: "LIST", args: elements };
    }
    /**
     * Parse a list comprehension after [variable IN has been consumed.
     * Full syntax: [variable IN listExpr WHERE filterCondition | mapExpr]
     * - WHERE and | are both optional
     */
    parseListComprehension(variable) {
        // Parse the source list expression
        const listExpr = this.parseExpression();
        // Check for optional WHERE filter
        let filterCondition;
        if (this.checkKeyword("WHERE")) {
            this.advance();
            filterCondition = this.parseListComprehensionCondition(variable);
        }
        // Check for optional map projection (| expr)
        let mapExpr;
        if (this.check("PIPE")) {
            this.advance();
            mapExpr = this.parseListComprehensionExpression(variable);
        }
        this.expect("RBRACKET");
        return {
            type: "listComprehension",
            variable,
            listExpr,
            filterCondition,
            mapExpr,
        };
    }
    /**
     * Parse a list predicate after PRED(variable IN has been consumed.
     * Syntax: ALL/ANY/NONE/SINGLE(variable IN listExpr WHERE filterCondition)
     * WHERE is required for list predicates.
     */
    parseListPredicate(predicateType, variable) {
        // Parse the source list expression
        const listExpr = this.parseExpression();
        // WHERE clause is required for list predicates
        if (!this.checkKeyword("WHERE")) {
            throw new Error(`Expected WHERE after list expression in ${predicateType}()`);
        }
        this.advance(); // consume WHERE
        // Parse the filter condition
        const filterCondition = this.parseListComprehensionCondition(variable);
        this.expect("RPAREN");
        return {
            type: "listPredicate",
            predicateType,
            variable,
            listExpr,
            filterCondition,
        };
    }
    /**
     * Parse a condition in a list comprehension, where the variable can be used.
     * Similar to parseWhereCondition but resolves variable references.
     */
    parseListComprehensionCondition(variable) {
        return this.parseOrCondition();
    }
    /**
     * Parse an expression in a list comprehension map projection.
     * Similar to parseExpression but the variable is in scope.
     */
    parseListComprehensionExpression(variable) {
        return this.parseExpression();
    }
    // Token helpers
    peek() {
        return this.tokens[this.pos];
    }
    advance() {
        if (!this.isAtEnd()) {
            this.pos++;
        }
        return this.tokens[this.pos - 1];
    }
    isAtEnd() {
        return this.peek().type === "EOF";
    }
    check(type) {
        return this.peek().type === type;
    }
    checkKeyword(keyword) {
        const token = this.peek();
        return token.type === "KEYWORD" && token.value === keyword;
    }
    expect(type, value) {
        const token = this.peek();
        if (token.type !== type || (value !== undefined && token.value !== value)) {
            throw new Error(`Expected ${type}${value ? ` '${value}'` : ""}, got ${token.type} '${token.value}'`);
        }
        return this.advance();
    }
    expectIdentifier() {
        const token = this.peek();
        if (token.type !== "IDENTIFIER") {
            throw new Error(`Expected identifier, got ${token.type} '${token.value}'`);
        }
        return this.advance().value;
    }
    expectIdentifierOrKeyword() {
        const token = this.peek();
        if (token.type !== "IDENTIFIER" && token.type !== "KEYWORD") {
            throw new Error(`Expected identifier or keyword, got ${token.type} '${token.value}'`);
        }
        // Keywords are stored uppercase, but property keys should be lowercase
        const value = this.advance().value;
        return token.type === "KEYWORD" ? value.toLowerCase() : value;
    }
    expectLabelOrType() {
        const token = this.peek();
        if (token.type !== "IDENTIFIER" && token.type !== "KEYWORD") {
            throw new Error(`Expected label or type, got ${token.type} '${token.value}'`);
        }
        this.advance();
        // Labels and types preserve their original case from the query
        // Use originalValue for keywords (which stores the original casing before uppercasing)
        return token.originalValue || token.value;
    }
}
// Convenience function
export function parse(input) {
    return new Parser().parse(input);
}
//# sourceMappingURL=parser.js.map