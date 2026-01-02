// NiceFox GraphDB - Remote HTTP Client
// Connects to a remote GraphDB server
import { GraphDBError } from "./types.js";
/**
 * Create a remote GraphDB client that connects via HTTP.
 */
export function createRemoteClient(options) {
    // Normalize URL (remove trailing slash)
    const url = options.url.replace(/\/$/, "");
    const project = options.project;
    const env = options.env || "production";
    const apiKey = options.apiKey;
    return {
        async query(cypher, params = {}) {
            const response = await this.queryRaw(cypher, params);
            return response.data;
        },
        async queryRaw(cypher, params = {}) {
            const endpoint = `${url}/query/${env}/${project}`;
            const headers = {
                "Content-Type": "application/json",
            };
            if (apiKey) {
                headers["Authorization"] = `Bearer ${apiKey}`;
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
            const response = await fetch(`${url}/health`);
            return response.json();
        },
        close() {
            // No-op for remote client (no resources to release)
        },
    };
}
//# sourceMappingURL=remote.js.map