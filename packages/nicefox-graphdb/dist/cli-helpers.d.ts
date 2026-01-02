export interface ApiKeyConfig {
    project?: string;
    env?: string;
    admin?: boolean;
}
export declare function formatBytes(bytes: number): string;
export declare function formatValue(val: unknown): string;
export declare function getApiKeysPath(dataPath: string): string;
export declare function loadApiKeys(dataPath: string): Record<string, ApiKeyConfig>;
export declare function saveApiKeys(dataPath: string, keys: Record<string, ApiKeyConfig>): void;
export declare function ensureDataDir(dataPath: string): void;
export declare function formatTableRow(columns: string[], row: Record<string, unknown>, widths: Record<string, number>): string;
export declare function calculateColumnWidths(columns: string[], rows: Record<string, unknown>[], maxWidth?: number): Record<string, number>;
export declare function listProjects(dataPath: string): Map<string, string[]>;
export declare function projectExists(dataPath: string, project: string, env?: string): boolean;
export declare function getProjectKeyCount(keys: Record<string, ApiKeyConfig>, project: string): number;
//# sourceMappingURL=cli-helpers.d.ts.map