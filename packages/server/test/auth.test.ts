// API Key Authentication Tests (TDD)

import { describe, it, expect, beforeEach } from "vitest";
import { Hono } from "hono";
import { createApp, createServer } from "../src/routes";
import { DatabaseManager } from "../src/db";
import { authMiddleware, ApiKeyStore } from "../src/auth";

describe("API Key Authentication", () => {
  describe("ApiKeyStore", () => {
    it("should validate a correct API key", () => {
      const store = new ApiKeyStore();
      store.addKey("test-key-123", { project: "myproject" });

      const result = store.validate("test-key-123");
      expect(result.valid).toBe(true);
      expect(result.project).toBe("myproject");
    });

    it("should reject an invalid API key", () => {
      const store = new ApiKeyStore();
      store.addKey("test-key-123", { project: "myproject" });

      const result = store.validate("wrong-key");
      expect(result.valid).toBe(false);
    });

    it("should support keys with environment restrictions", () => {
      const store = new ApiKeyStore();
      store.addKey("prod-key", { project: "myproject", env: "production" });
      store.addKey("test-key", { project: "myproject", env: "test" });

      const prodResult = store.validate("prod-key");
      expect(prodResult.valid).toBe(true);
      expect(prodResult.env).toBe("production");

      const testResult = store.validate("test-key");
      expect(testResult.valid).toBe(true);
      expect(testResult.env).toBe("test");
    });

    it("should support admin keys with full access", () => {
      const store = new ApiKeyStore();
      store.addKey("admin-key", { admin: true });

      const result = store.validate("admin-key");
      expect(result.valid).toBe(true);
      expect(result.admin).toBe(true);
    });

    it("should remove a key", () => {
      const store = new ApiKeyStore();
      store.addKey("temp-key", { project: "temp" });
      
      expect(store.validate("temp-key").valid).toBe(true);
      
      store.removeKey("temp-key");
      expect(store.validate("temp-key").valid).toBe(false);
    });

    it("should list all keys (without revealing full key)", () => {
      const store = new ApiKeyStore();
      store.addKey("abc123def456", { project: "proj1" });
      store.addKey("xyz789uvw012", { project: "proj2" });

      const keys = store.listKeys();
      expect(keys.length).toBe(2);
      expect(keys[0].prefix).toBe("abc1..."); // First 4 chars + ...
      expect(keys[1].prefix).toBe("xyz7...");
    });
  });

  describe("authMiddleware", () => {
    let app: Hono;
    let store: ApiKeyStore;

    beforeEach(() => {
      store = new ApiKeyStore();
      store.addKey("valid-api-key", { project: "myproject" });
      store.addKey("admin-key", { admin: true });

      app = new Hono();
      app.use("*", authMiddleware(store));
      app.get("/test", (c) => c.json({ message: "success" }));
      app.get("/admin/test", (c) => c.json({ message: "admin success" }));
    });

    it("should allow requests with valid Bearer token", async () => {
      const res = await app.request("/test", {
        headers: { Authorization: "Bearer valid-api-key" },
      });

      expect(res.status).toBe(200);
      const body = await res.json();
      expect(body.message).toBe("success");
    });

    it("should reject requests without authorization header", async () => {
      const res = await app.request("/test");

      expect(res.status).toBe(401);
      const body = await res.json();
      expect(body.success).toBe(false);
      expect(body.error.message).toMatch(/missing.*authorization/i);
    });

    it("should reject requests with invalid API key", async () => {
      const res = await app.request("/test", {
        headers: { Authorization: "Bearer invalid-key" },
      });

      expect(res.status).toBe(401);
      const body = await res.json();
      expect(body.success).toBe(false);
      expect(body.error.message).toMatch(/invalid.*api.*key/i);
    });

    it("should reject malformed authorization header", async () => {
      const res = await app.request("/test", {
        headers: { Authorization: "Basic dXNlcjpwYXNz" },
      });

      expect(res.status).toBe(401);
      const body = await res.json();
      expect(body.error.message).toMatch(/bearer/i);
    });

    it("should allow health endpoint without auth", async () => {
      app.get("/health", (c) => c.json({ status: "ok" }));

      const res = await app.request("/health");
      expect(res.status).toBe(200);
    });

    it("should require admin key for admin endpoints", async () => {
      // Regular key should fail on admin endpoints
      const res = await app.request("/admin/test", {
        headers: { Authorization: "Bearer valid-api-key" },
      });

      expect(res.status).toBe(403);
      const body = await res.json();
      expect(body.error.message).toMatch(/admin.*required/i);
    });

    it("should allow admin key on admin endpoints", async () => {
      const res = await app.request("/admin/test", {
        headers: { Authorization: "Bearer admin-key" },
      });

      expect(res.status).toBe(200);
    });
  });

  describe("Project-scoped authentication", () => {
    let dbManager: DatabaseManager;

    beforeEach(() => {
      dbManager = new DatabaseManager(":memory:");
    });

    it("should restrict access to specified project only", async () => {
      const store = new ApiKeyStore();
      store.addKey("project-key", { project: "allowed-project" });

      const app = new Hono();
      app.use("*", authMiddleware(store));
      app.post("/query/:env/:project", (c) => {
        return c.json({ success: true, project: c.req.param("project") });
      });

      // Should allow access to allowed project
      const allowedRes = await app.request("/query/production/allowed-project", {
        method: "POST",
        headers: { Authorization: "Bearer project-key" },
        body: JSON.stringify({ cypher: "MATCH (n) RETURN n" }),
      });
      expect(allowedRes.status).toBe(200);

      // Should deny access to other projects
      const deniedRes = await app.request("/query/production/other-project", {
        method: "POST",
        headers: { Authorization: "Bearer project-key" },
        body: JSON.stringify({ cypher: "MATCH (n) RETURN n" }),
      });
      expect(deniedRes.status).toBe(403);
    });

    it("should restrict access to specified environment only", async () => {
      const store = new ApiKeyStore();
      store.addKey("test-only-key", { project: "myproject", env: "test" });

      const app = new Hono();
      app.use("*", authMiddleware(store));
      app.post("/query/:env/:project", (c) => {
        return c.json({ success: true });
      });

      // Should allow test environment
      const testRes = await app.request("/query/test/myproject", {
        method: "POST",
        headers: { Authorization: "Bearer test-only-key" },
        body: JSON.stringify({ cypher: "MATCH (n) RETURN n" }),
      });
      expect(testRes.status).toBe(200);

      // Should deny production environment
      const prodRes = await app.request("/query/production/myproject", {
        method: "POST",
        headers: { Authorization: "Bearer test-only-key" },
        body: JSON.stringify({ cypher: "MATCH (n) RETURN n" }),
      });
      expect(prodRes.status).toBe(403);
    });
  });
});
