// CLI Helper Functions
import * as fs from "fs";
import * as path from "path";
// ============================================================================
// Formatting Helpers
// ============================================================================
export function formatBytes(bytes) {
    if (bytes === 0)
        return "0 B";
    const k = 1024;
    const sizes = ["B", "KB", "MB", "GB"];
    const i = Math.floor(Math.log(bytes) / Math.log(k));
    return `${parseFloat((bytes / Math.pow(k, i)).toFixed(2))} ${sizes[i]}`;
}
export function formatValue(val) {
    if (val === null)
        return "null";
    if (val === undefined)
        return "";
    if (typeof val === "object")
        return JSON.stringify(val);
    return String(val);
}
// ============================================================================
// API Key Helpers
// ============================================================================
export function getApiKeysPath(dataPath) {
    return path.join(dataPath, "api-keys.json");
}
export function loadApiKeys(dataPath) {
    const keysFile = getApiKeysPath(dataPath);
    if (fs.existsSync(keysFile)) {
        try {
            return JSON.parse(fs.readFileSync(keysFile, "utf-8"));
        }
        catch {
            return {};
        }
    }
    return {};
}
export function saveApiKeys(dataPath, keys) {
    const keysFile = getApiKeysPath(dataPath);
    fs.writeFileSync(keysFile, JSON.stringify(keys, null, 2) + "\n");
}
// ============================================================================
// Directory Helpers
// ============================================================================
export function ensureDataDir(dataPath) {
    if (!fs.existsSync(dataPath)) {
        fs.mkdirSync(dataPath, { recursive: true });
    }
    // Ensure env subdirs exist
    for (const env of ["production", "test"]) {
        const envPath = path.join(dataPath, env);
        if (!fs.existsSync(envPath)) {
            fs.mkdirSync(envPath, { recursive: true });
        }
    }
}
// ============================================================================
// Table Formatting
// ============================================================================
export function formatTableRow(columns, row, widths) {
    return columns
        .map((col) => {
        const val = formatValue(row[col]);
        return val.slice(0, widths[col]).padEnd(widths[col]);
    })
        .join(" | ");
}
export function calculateColumnWidths(columns, rows, maxWidth = 40) {
    const widths = {};
    for (const col of columns) {
        widths[col] = col.length;
    }
    for (const row of rows) {
        for (const col of columns) {
            const val = formatValue(row[col]);
            widths[col] = Math.max(widths[col], val.length);
        }
    }
    // Cap max width
    for (const col of columns) {
        widths[col] = Math.min(widths[col], maxWidth);
    }
    return widths;
}
// ============================================================================
// Project Helpers
// ============================================================================
export function listProjects(dataPath) {
    const projects = new Map();
    for (const env of ["production", "test"]) {
        const envPath = path.join(dataPath, env);
        if (fs.existsSync(envPath)) {
            const files = fs.readdirSync(envPath).filter((f) => f.endsWith(".db"));
            for (const file of files) {
                const project = file.replace(".db", "");
                if (!projects.has(project)) {
                    projects.set(project, []);
                }
                projects.get(project).push(env);
            }
        }
    }
    return projects;
}
export function projectExists(dataPath, project, env) {
    if (env) {
        const dbPath = path.join(dataPath, env, `${project}.db`);
        return fs.existsSync(dbPath);
    }
    // Check both environments
    return (fs.existsSync(path.join(dataPath, "production", `${project}.db`)) ||
        fs.existsSync(path.join(dataPath, "test", `${project}.db`)));
}
export function getProjectKeyCount(keys, project) {
    return Object.values(keys).filter((config) => config.project === project).length;
}
//# sourceMappingURL=cli-helpers.js.map