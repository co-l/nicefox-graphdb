// NiceFox GraphDB TypeScript Client
// ============================================================================
// Error Class
// ============================================================================
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
// ============================================================================
// Client Class
// ============================================================================
export class NiceFoxGraphDB {
    url;
    project;
    env;
    apiKey;
    constructor(options) {
        // Normalize URL (remove trailing slash)
        this.url = options.url.replace(/\/$/, "");
        this.project = options.project;
        this.env = options.env || "production";
        this.apiKey = options.apiKey;
    }
    // ==========================================================================
    // Core Query Methods
    // ==========================================================================
    /**
     * Execute a Cypher query and return the data array.
     * Throws GraphDBError if the query fails.
     */
    async query(cypher, params = {}) {
        const response = await this.queryRaw(cypher, params);
        return response.data;
    }
    /**
     * Execute a Cypher query and return the full response including metadata.
     * Throws GraphDBError if the query fails.
     */
    async queryRaw(cypher, params = {}) {
        const endpoint = `${this.url}/query/${this.env}/${this.project}`;
        const headers = {
            "Content-Type": "application/json",
        };
        if (this.apiKey) {
            headers["Authorization"] = `Bearer ${this.apiKey}`;
        }
        const response = await fetch(endpoint, {
            method: "POST",
            headers,
            body: JSON.stringify({ cypher, params }),
        });
        const data = (await response.json());
        if (!data.success || data.error) {
            throw new GraphDBError(data.error?.message || "Query failed", {
                position: data.error?.position,
                line: data.error?.line,
                column: data.error?.column,
            });
        }
        return data;
    }
    /**
     * Execute a mutating query (CREATE, SET, DELETE, MERGE) without expecting return data.
     * Throws GraphDBError if the query fails.
     */
    async execute(cypher, params = {}) {
        await this.queryRaw(cypher, params);
    }
    // ==========================================================================
    // Convenience Methods
    // ==========================================================================
    /**
     * Create a node with the given label and properties.
     * Returns the generated node ID.
     */
    async createNode(label, properties = {}) {
        // Build property assignments for the query
        const propKeys = Object.keys(properties);
        const propAssignments = propKeys.map((k) => `${k}: $${k}`).join(", ");
        const cypher = `CREATE (n:${label} {${propAssignments}}) RETURN id(n) as id`;
        const result = await this.query(cypher, properties);
        return result[0]?.id;
    }
    /**
     * Create an edge between two nodes.
     */
    async createEdge(sourceId, type, targetId, properties = {}) {
        const propKeys = Object.keys(properties);
        const propAssignments = propKeys.length > 0
            ? ` {${propKeys.map((k) => `${k}: $${k}`).join(", ")}}`
            : "";
        const cypher = `
      MATCH (source {id: $sourceId}), (target {id: $targetId})
      MERGE (source)-[:${type}${propAssignments}]->(target)
    `;
        await this.execute(cypher, { sourceId, targetId, ...properties });
    }
    /**
     * Get a node by label and property filter.
     * Returns the node's properties directly, or null if not found.
     *
     * @example
     * ```ts
     * const user = await graph.getNode('User', { id: 'abc123' });
     * // user = { id: 'abc123', name: 'Alice', email: 'alice@example.com' }
     * console.log(user?.name);  // 'Alice'
     * ```
     */
    async getNode(label, filter) {
        const filterKeys = Object.keys(filter);
        const filterProps = filterKeys.map((k) => `${k}: $${k}`).join(", ");
        const cypher = `MATCH (n:${label} {${filterProps}}) RETURN n LIMIT 1`;
        const result = await this.query(cypher, filter);
        return result.length > 0 ? result[0].n : null;
    }
    /**
     * Delete a node by ID (with DETACH to remove connected edges).
     */
    async deleteNode(id) {
        await this.execute("MATCH (n {id: $id}) DETACH DELETE n", { id });
    }
    /**
     * Update properties on a node.
     */
    async updateNode(id, properties) {
        const propKeys = Object.keys(properties);
        const setClause = propKeys.map((k) => `n.${k} = $${k}`).join(", ");
        const cypher = `MATCH (n {id: $id}) SET ${setClause}`;
        await this.execute(cypher, { id, ...properties });
    }
    // ==========================================================================
    // Utility Methods
    // ==========================================================================
    /**
     * Check server health.
     */
    async health() {
        const response = await fetch(`${this.url}/health`);
        return response.json();
    }
}
// ============================================================================
// Test Client Factory
// ============================================================================
/**
 * Create an in-memory test client for unit testing.
 * This spins up an embedded server with an in-memory SQLite database.
 *
 * Requires the server package to be installed: `npm install github:co-l/nicefox-graphdb#main`
 *
 * @example
 * ```ts
 * import { createTestClient } from 'nicefox-graphdb/packages/client/src/index.ts';
 *
 * const client = await createTestClient();
 *
 * // Use like a normal client
 * await client.createNode('User', { name: 'Alice' });
 * const users = await client.query('MATCH (u:User) RETURN u');
 *
 * // Clean up when done
 * client.close();
 * ```
 */
export async function createTestClient(options = {}) {
    const { project = 'test' } = options;
    // Dynamically import the server package
    // Users install via: "nicefox-graphdb": "github:user/nicefox-graphdb#main"
    let createServer;
    let lastError;
    // Try different import paths to support various installation methods
    const importPaths = [
        'nicefox-graphdb/packages/server/src/index.js', // GitHub dependency
        '../../server/src/index.js', // Local workspace (relative)
    ];
    for (const importPath of importPaths) {
        try {
            const serverModule = await import(/* @vite-ignore */ importPath);
            createServer = serverModule.createServer;
            break;
        }
        catch (e) {
            lastError = e;
        }
    }
    if (!createServer) {
        throw new Error('Could not import nicefox-graphdb server. Make sure it is installed:\n' +
            '  Add to package.json dependencies:\n' +
            '  "nicefox-graphdb": "github:your-org/nicefox-graphdb#main"\n' +
            (lastError ? `\nOriginal error: ${lastError.message}` : ''));
    }
    // Create an in-memory server
    const server = createServer({ dataPath: ':memory:' });
    // Create a wrapper that uses app.request() instead of fetch
    const testClient = {
        server,
        project,
        async query(cypher, params = {}) {
            const response = await this.queryRaw(cypher, params);
            return response.data;
        },
        async queryRaw(cypher, params = {}) {
            const response = await this.server.app.request(`/query/test/${this.project}`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ cypher, params }),
            });
            const data = (await response.json());
            if (!data.success || data.error) {
                throw new GraphDBError(data.error?.message || 'Query failed', {
                    position: data.error?.position,
                    line: data.error?.line,
                    column: data.error?.column,
                });
            }
            return data;
        },
        async execute(cypher, params = {}) {
            await this.queryRaw(cypher, params);
        },
        async createNode(label, properties = {}) {
            const propKeys = Object.keys(properties);
            const propAssignments = propKeys.map((k) => `${k}: $${k}`).join(", ");
            const cypher = `CREATE (n:${label} {${propAssignments}}) RETURN id(n) as id`;
            const result = await this.query(cypher, properties);
            return result[0]?.id;
        },
        async createEdge(sourceId, type, targetId, properties = {}) {
            const propKeys = Object.keys(properties);
            const propAssignments = propKeys.length > 0
                ? ` {${propKeys.map((k) => `${k}: $${k}`).join(", ")}}`
                : "";
            const cypher = `
        MATCH (source {id: $sourceId}), (target {id: $targetId})
        MERGE (source)-[:${type}${propAssignments}]->(target)
      `;
            await this.execute(cypher, { sourceId, targetId, ...properties });
        },
        async getNode(label, filter) {
            const filterKeys = Object.keys(filter);
            const filterProps = filterKeys.map((k) => `${k}: $${k}`).join(", ");
            const cypher = `MATCH (n:${label} {${filterProps}}) RETURN n LIMIT 1`;
            const result = await this.query(cypher, filter);
            return result.length > 0 ? result[0].n : null;
        },
        async deleteNode(id) {
            await this.execute("MATCH (n {id: $id}) DETACH DELETE n", { id });
        },
        async updateNode(id, properties) {
            const propKeys = Object.keys(properties);
            const setClause = propKeys.map((k) => `n.${k} = $${k}`).join(", ");
            const cypher = `MATCH (n {id: $id}) SET ${setClause}`;
            await this.execute(cypher, { id, ...properties });
        },
        async health() {
            const response = await this.server.app.request('/health');
            return response.json();
        },
        close() {
            this.server.dbManager.closeAll();
        },
    };
    return testClient;
}
// Default export for convenience
export default NiceFoxGraphDB;
//# sourceMappingURL=index.js.map