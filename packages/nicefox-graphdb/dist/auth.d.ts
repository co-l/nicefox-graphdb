import { Context, Next } from "hono";
export interface ApiKeyConfig {
    project?: string;
    env?: string;
    admin?: boolean;
}
export interface ValidationResult {
    valid: boolean;
    project?: string;
    env?: string;
    admin?: boolean;
}
export interface KeyInfo {
    prefix: string;
    project?: string;
    env?: string;
    admin?: boolean;
}
export declare class ApiKeyStore {
    private keys;
    /**
     * Add an API key with its configuration.
     */
    addKey(key: string, config: ApiKeyConfig): void;
    /**
     * Remove an API key.
     */
    removeKey(key: string): void;
    /**
     * Validate an API key and return its permissions.
     */
    validate(key: string): ValidationResult;
    /**
     * List all keys (with prefixes only for security).
     */
    listKeys(): KeyInfo[];
    /**
     * Check if any keys are configured.
     */
    hasKeys(): boolean;
    /**
     * Load keys from an object (for initialization from config).
     */
    loadKeys(keys: Record<string, ApiKeyConfig>): void;
}
/**
 * Hono middleware for API key authentication.
 *
 * - Skips authentication for /health endpoint
 * - Requires Bearer token in Authorization header
 * - Checks project/env restrictions for /query endpoints
 * - Requires admin flag for /admin endpoints
 */
export declare function authMiddleware(store: ApiKeyStore): (c: Context, next: Next) => Promise<void | (Response & import("hono").TypedResponse<{
    success: false;
    error: {
        message: string;
    };
}, 401, "json">) | (Response & import("hono").TypedResponse<{
    success: false;
    error: {
        message: string;
    };
}, 403, "json">)>;
export declare function generateApiKey(): string;
//# sourceMappingURL=auth.d.ts.map