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
            const type = KEYWORDS.has(upperValue) ? "KEYWORD" : "IDENTIFIER";
            return { type, value: type === "KEYWORD" ? upperValue : value, position: startPos, line: startLine, column: startColumn };
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
        return { type: "CREATE", patterns };
    }
    parseMatch(optional = false) {
        this.expect("KEYWORD", "MATCH");
        const patterns = [];
        patterns.push(...this.parsePatternChain());
        while (this.check("COMMA")) {
            this.advance();
            patterns.push(...this.parsePatternChain());
        }
        let where;
        if (this.checkKeyword("WHERE")) {
            this.advance();
            where = this.parseWhereCondition();
        }
        return { type: optional ? "OPTIONAL_MATCH" : "MATCH", patterns, where };
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
            const variable = this.expectIdentifier();
            this.expect("DOT");
            const property = this.expectIdentifier();
            this.expect("EQUALS");
            const value = this.parseExpression();
            assignments.push({ variable, property, value });
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
        variables.push(this.expectIdentifier());
        while (this.check("COMMA")) {
            this.advance();
            variables.push(this.expectIdentifier());
        }
        return { type: "DELETE", variables, detach };
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
        do {
            if (items.length > 0) {
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
            // Type (can be identifier or keyword)
            if (this.check("COLON")) {
                this.advance();
                edge.type = this.expectLabelOrType();
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
                throw new Error("Invalid relationship pattern: cannot have arrows on both sides");
            }
            direction = "right";
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
        // Check for ..N pattern (*..3)
        if (this.check("DOT")) {
            this.advance(); // first dot
            this.expect("DOT"); // second dot
            if (this.check("NUMBER")) {
                edge.minHops = 1;
                edge.maxHops = parseInt(this.advance().value, 10);
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
        // Handle variable references (e.g., from UNWIND)
        if (token.type === "IDENTIFIER") {
            this.advance();
            return { type: "variable", name: token.value };
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
        // Handle parenthesized conditions
        if (this.check("LPAREN")) {
            this.advance(); // consume (
            const condition = this.parseOrCondition(); // parse the inner condition
            this.expect("RPAREN"); // consume )
            return condition;
        }
        return this.parseComparisonCondition();
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
    // Parse expression that may include comparison (for RETURN items)
    parseReturnExpression() {
        let left = this.parseAdditiveExpression();
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
    // Handle *, /, % (higher precedence)
    parseMultiplicativeExpression() {
        let left = this.parsePrimaryExpression();
        while (this.check("STAR") || this.check("SLASH") || this.check("PERCENT")) {
            const operatorToken = this.advance();
            let operator;
            if (operatorToken.type === "STAR")
                operator = "*";
            else if (operatorToken.type === "SLASH")
                operator = "/";
            else
                operator = "%";
            const right = this.parsePrimaryExpression();
            left = { type: "binary", operator, left, right };
        }
        return left;
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
        // Parenthesized expression for grouping
        if (token.type === "LPAREN") {
            this.advance(); // consume (
            const expr = this.parseExpression();
            this.expect("RPAREN");
            return expr;
        }
        // CASE expression
        if (this.checkKeyword("CASE")) {
            return this.parseCaseExpression();
        }
        // Function call: COUNT(x), id(x), count(DISTINCT x)
        if (token.type === "KEYWORD" || token.type === "IDENTIFIER") {
            const nextToken = this.tokens[this.pos + 1];
            if (nextToken && nextToken.type === "LPAREN") {
                const functionName = this.advance().value.toUpperCase();
                this.advance(); // LPAREN
                const args = [];
                // Check for DISTINCT keyword after opening paren (for aggregation functions)
                let distinct;
                if (this.checkKeyword("DISTINCT")) {
                    this.advance();
                    distinct = true;
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
                const value = this.parseExpression();
                properties.push({ key, value });
            } while (this.check("COMMA"));
        }
        this.expect("RBRACE");
        return { type: "object", properties };
    }
    parseListLiteralExpression() {
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
        return { type: "literal", value: values };
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
export function parse(input) {
    return new Parser().parse(input);
}
//# sourceMappingURL=parser.js.map