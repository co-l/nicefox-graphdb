// NiceFox GraphDB - Local Embedded Client
// Uses SQLite directly without HTTP
import * as fs from "fs";
import * as path from "path";
import { GraphDatabase } from "./db.js";
import { Executor } from "./executor.js";
import { GraphDBError } from "./types.js";
/**
 * Create a local embedded GraphDB client.
 * This client uses SQLite directly without any HTTP layer.
 */
export function createLocalClient(options) {
    const dataPath = options.dataPath || "./data";
    const project = options.project;
    const env = options.env || "production";
    // Determine database path
    let dbPath;
    if (dataPath === ":memory:") {
        dbPath = ":memory:";
    }
    else {
        // Ensure directory exists
        const dir = path.join(dataPath, env);
        if (!fs.existsSync(dir)) {
            fs.mkdirSync(dir, { recursive: true });
        }
        dbPath = path.join(dir, `${project}.db`);
    }
    // Create and initialize database
    const db = new GraphDatabase(dbPath);
    db.initialize();
    const executor = new Executor(db);
    return {
        async query(cypher, params = {}) {
            const result = executor.execute(cypher, params);
            if (!result.success) {
                throw new GraphDBError(result.error.message, {
                    position: result.error.position,
                    line: result.error.line,
                    column: result.error.column,
                });
            }
            return result.data;
        },
        async queryRaw(cypher, params = {}) {
            const result = executor.execute(cypher, params);
            if (!result.success) {
                throw new GraphDBError(result.error.message, {
                    position: result.error.position,
                    line: result.error.line,
                    column: result.error.column,
                });
            }
            return {
                success: true,
                data: result.data,
                meta: result.meta,
            };
        },
        async execute(cypher, params = {}) {
            const result = executor.execute(cypher, params);
            if (!result.success) {
                throw new GraphDBError(result.error.message, {
                    position: result.error.position,
                    line: result.error.line,
                    column: result.error.column,
                });
            }
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
            return {
                status: "ok",
                timestamp: new Date().toISOString(),
            };
        },
        close() {
            db.close();
        },
    };
}
//# sourceMappingURL=local.js.map