// NiceFox GraphDB - Shared Types
// ============================================================================
// Error Class
// ============================================================================
/**
 * Error thrown by GraphDB operations.
 * Contains optional position information for Cypher parse errors.
 */
export class GraphDBError extends Error {
    position;
    line;
    column;
    constructor(message, options) {
        super(message);
        this.name = "GraphDBError";
        this.position = options?.position;
        this.line = options?.line;
        this.column = options?.column;
    }
}
//# sourceMappingURL=types.js.map