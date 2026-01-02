// API Key Authentication for NiceFox GraphDB

import { Context, Next } from "hono";

// ============================================================================
// Types
// ============================================================================

export interface ApiKeyConfig {
  project?: string;  // Restrict to specific project (undefined = all projects)
  env?: string;      // Restrict to specific environment (undefined = all envs)
  admin?: boolean;   // Full admin access
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

// ============================================================================
// ApiKeyStore
// ============================================================================

export class ApiKeyStore {
  private keys: Map<string, ApiKeyConfig> = new Map();

  /**
   * Add an API key with its configuration.
   */
  addKey(key: string, config: ApiKeyConfig): void {
    this.keys.set(key, config);
  }

  /**
   * Remove an API key.
   */
  removeKey(key: string): void {
    this.keys.delete(key);
  }

  /**
   * Validate an API key and return its permissions.
   */
  validate(key: string): ValidationResult {
    const config = this.keys.get(key);
    
    if (!config) {
      return { valid: false };
    }

    return {
      valid: true,
      project: config.project,
      env: config.env,
      admin: config.admin,
    };
  }

  /**
   * List all keys (with prefixes only for security).
   */
  listKeys(): KeyInfo[] {
    const result: KeyInfo[] = [];
    
    for (const [key, config] of this.keys) {
      result.push({
        prefix: key.slice(0, 4) + "...",
        project: config.project,
        env: config.env,
        admin: config.admin,
      });
    }

    return result;
  }

  /**
   * Check if any keys are configured.
   */
  hasKeys(): boolean {
    return this.keys.size > 0;
  }

  /**
   * Load keys from an object (for initialization from config).
   */
  loadKeys(keys: Record<string, ApiKeyConfig>): void {
    for (const [key, config] of Object.entries(keys)) {
      this.addKey(key, config);
    }
  }
}

// ============================================================================
// Auth Middleware
// ============================================================================

/**
 * Hono middleware for API key authentication.
 * 
 * - Skips authentication for /health endpoint
 * - Requires Bearer token in Authorization header
 * - Checks project/env restrictions for /query endpoints
 * - Requires admin flag for /admin endpoints
 */
export function authMiddleware(store: ApiKeyStore) {
  return async (c: Context, next: Next) => {
    const path = c.req.path;

    // Skip auth for health endpoint
    if (path === "/health") {
      return next();
    }

    // Get authorization header
    const authHeader = c.req.header("Authorization");
    
    if (!authHeader) {
      return c.json(
        {
          success: false,
          error: { message: "Missing Authorization header" },
        },
        401
      );
    }

    // Check Bearer format
    if (!authHeader.startsWith("Bearer ")) {
      return c.json(
        {
          success: false,
          error: { message: "Authorization header must use Bearer scheme" },
        },
        401
      );
    }

    const apiKey = authHeader.slice(7); // Remove "Bearer "
    const validation = store.validate(apiKey);

    if (!validation.valid) {
      return c.json(
        {
          success: false,
          error: { message: "Invalid API key" },
        },
        401
      );
    }

    // Check admin access for /admin endpoints
    if (path.startsWith("/admin") && !validation.admin) {
      return c.json(
        {
          success: false,
          error: { message: "Admin access required for this endpoint" },
        },
        403
      );
    }

    // Check project/env restrictions for query endpoints
    if (path.startsWith("/query/")) {
      const parts = path.split("/");
      const env = parts[2];
      const project = parts[3];

      // Check project restriction
      if (validation.project && validation.project !== project) {
        return c.json(
          {
            success: false,
            error: { message: `Access denied for project: ${project}` },
          },
          403
        );
      }

      // Check environment restriction
      if (validation.env && validation.env !== env) {
        return c.json(
          {
            success: false,
            error: { message: `Access denied for environment: ${env}` },
          },
          403
        );
      }
    }

    // Store validation result in context for later use
    c.set("auth", validation);

    return next();
  };
}

// ============================================================================
// Helper: Generate a random API key
// ============================================================================

export function generateApiKey(): string {
  const chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
  let key = "";
  
  for (let i = 0; i < 32; i++) {
    key += chars.charAt(Math.floor(Math.random() * chars.length));
  }
  
  return key;
}
