// Backup System for NiceFox GraphDB
// Uses SQLite's backup API for hot (online) backups
import Database from "better-sqlite3";
import * as fs from "fs";
import * as path from "path";
// ============================================================================
// BackupManager Class
// ============================================================================
export class BackupManager {
    backupDir;
    constructor(backupDir) {
        this.backupDir = backupDir;
    }
    /**
     * Create a backup of a single database file using SQLite's backup API.
     * This is a "hot" backup - it works even if the database is open and in use.
     */
    async backupDatabase(sourcePath, project) {
        const startTime = Date.now();
        // Check source exists
        if (!fs.existsSync(sourcePath)) {
            return {
                success: false,
                project,
                sourcePath,
                error: `Source database not found: ${sourcePath}`,
            };
        }
        // Ensure backup directory exists
        if (!fs.existsSync(this.backupDir)) {
            fs.mkdirSync(this.backupDir, { recursive: true });
        }
        // Generate backup filename with timestamp (including milliseconds for uniqueness)
        const now = new Date();
        const timestamp = now.toISOString().replace(/[:.]/g, "-").slice(0, 19);
        const ms = now.getMilliseconds().toString().padStart(3, "0");
        const backupFilename = `${project}_${timestamp}-${ms}.db`;
        const backupPath = path.join(this.backupDir, backupFilename);
        try {
            // Use SQLite's backup API via better-sqlite3
            const sourceDb = new Database(sourcePath, { readonly: true });
            // backup() returns a Promise - wait for it to complete
            await sourceDb.backup(backupPath);
            sourceDb.close();
            const stats = fs.statSync(backupPath);
            const durationMs = Date.now() - startTime;
            return {
                success: true,
                project,
                sourcePath,
                backupPath,
                durationMs,
                sizeBytes: stats.size,
            };
        }
        catch (err) {
            return {
                success: false,
                project,
                sourcePath,
                error: err instanceof Error ? err.message : String(err),
            };
        }
    }
    /**
     * Backup all databases in a data directory.
     * By default, only backs up production databases.
     */
    async backupAll(dataDir, options = {}) {
        const results = [];
        const envs = options.includeTest ? ["production", "test"] : ["production"];
        for (const env of envs) {
            const envPath = path.join(dataDir, env);
            if (!fs.existsSync(envPath)) {
                continue;
            }
            const files = fs.readdirSync(envPath).filter(f => f.endsWith(".db"));
            for (const file of files) {
                const project = file.replace(".db", "");
                const sourcePath = path.join(envPath, file);
                const result = await this.backupDatabase(sourcePath, project);
                results.push(result);
            }
        }
        return results;
    }
    /**
     * List all backups for a specific project, sorted by date (newest first).
     */
    listBackups(project) {
        if (!fs.existsSync(this.backupDir)) {
            return [];
        }
        const files = fs.readdirSync(this.backupDir)
            .filter(f => f.startsWith(`${project}_`) && f.endsWith(".db"))
            .sort((a, b) => b.localeCompare(a)); // Descending order (newest first)
        return files;
    }
    /**
     * Delete old backups, keeping only the specified number of recent ones.
     * Returns the number of deleted backups.
     */
    cleanOldBackups(project, keepCount) {
        const backups = this.listBackups(project);
        if (backups.length <= keepCount) {
            return 0;
        }
        const toDelete = backups.slice(keepCount);
        let deleted = 0;
        for (const file of toDelete) {
            const filePath = path.join(this.backupDir, file);
            try {
                fs.unlinkSync(filePath);
                deleted++;
            }
            catch {
                // Ignore errors during cleanup
            }
        }
        return deleted;
    }
    /**
     * Get overall backup status and statistics.
     */
    getBackupStatus() {
        if (!fs.existsSync(this.backupDir)) {
            return {
                totalBackups: 0,
                totalSizeBytes: 0,
                projects: [],
            };
        }
        const files = fs.readdirSync(this.backupDir).filter(f => f.endsWith(".db"));
        const projects = new Set();
        let totalSizeBytes = 0;
        let oldestBackup;
        let newestBackup;
        for (const file of files) {
            // Extract project name from filename (format: project_timestamp.db)
            const match = file.match(/^(.+)_\d{4}-\d{2}-\d{2}T/);
            if (match) {
                projects.add(match[1]);
            }
            const filePath = path.join(this.backupDir, file);
            const stats = fs.statSync(filePath);
            totalSizeBytes += stats.size;
            // Track oldest and newest
            if (!oldestBackup || file < oldestBackup) {
                oldestBackup = file;
            }
            if (!newestBackup || file > newestBackup) {
                newestBackup = file;
            }
        }
        return {
            totalBackups: files.length,
            totalSizeBytes,
            projects: Array.from(projects).sort(),
            oldestBackup,
            newestBackup,
        };
    }
    /**
     * Restore a backup to a target path.
     */
    restoreBackup(backupFilename, targetPath) {
        const backupPath = path.join(this.backupDir, backupFilename);
        if (!fs.existsSync(backupPath)) {
            return {
                success: false,
                project: "",
                sourcePath: backupPath,
                error: `Backup not found: ${backupFilename}`,
            };
        }
        try {
            // Ensure target directory exists
            const targetDir = path.dirname(targetPath);
            if (!fs.existsSync(targetDir)) {
                fs.mkdirSync(targetDir, { recursive: true });
            }
            // Copy backup to target
            fs.copyFileSync(backupPath, targetPath);
            return {
                success: true,
                project: "",
                sourcePath: backupPath,
                backupPath: targetPath,
            };
        }
        catch (err) {
            return {
                success: false,
                project: "",
                sourcePath: backupPath,
                error: err instanceof Error ? err.message : String(err),
            };
        }
    }
}
//# sourceMappingURL=backup.js.map