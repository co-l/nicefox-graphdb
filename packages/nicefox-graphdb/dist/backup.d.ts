export interface BackupResult {
    success: boolean;
    project: string;
    sourcePath: string;
    backupPath?: string;
    error?: string;
    durationMs?: number;
    sizeBytes?: number;
}
export interface BackupStatus {
    totalBackups: number;
    totalSizeBytes: number;
    projects: string[];
    oldestBackup?: string;
    newestBackup?: string;
}
export interface BackupAllOptions {
    includeTest?: boolean;
}
export declare class BackupManager {
    private backupDir;
    constructor(backupDir: string);
    /**
     * Create a backup of a single database file using SQLite's backup API.
     * This is a "hot" backup - it works even if the database is open and in use.
     */
    backupDatabase(sourcePath: string, project: string): Promise<BackupResult>;
    /**
     * Backup all databases in a data directory.
     * By default, only backs up production databases.
     */
    backupAll(dataDir: string, options?: BackupAllOptions): Promise<BackupResult[]>;
    /**
     * List all backups for a specific project, sorted by date (newest first).
     */
    listBackups(project: string): string[];
    /**
     * Delete old backups, keeping only the specified number of recent ones.
     * Returns the number of deleted backups.
     */
    cleanOldBackups(project: string, keepCount: number): number;
    /**
     * Get overall backup status and statistics.
     */
    getBackupStatus(): BackupStatus;
    /**
     * Restore a backup to a target path.
     */
    restoreBackup(backupFilename: string, targetPath: string): BackupResult;
}
//# sourceMappingURL=backup.d.ts.map