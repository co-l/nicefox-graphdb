// Backup System Tests (TDD)

import { describe, it, expect, beforeEach, afterEach } from "vitest";
import * as fs from "fs";
import * as path from "path";
import { GraphDatabase } from "../src/db";
import { BackupManager } from "../src/backup";

describe("BackupManager", () => {
  const testDir = path.join(process.cwd(), "test-backup-data");
  const sourceDir = path.join(testDir, "source");
  const backupDir = path.join(testDir, "backups");

  beforeEach(() => {
    // Create test directories
    fs.mkdirSync(sourceDir, { recursive: true });
    fs.mkdirSync(backupDir, { recursive: true });
  });

  afterEach(() => {
    // Clean up test directories
    fs.rmSync(testDir, { recursive: true, force: true });
  });

  describe("backupDatabase", () => {
    it("should create a backup of a database file", async () => {
      // Create a source database with some data
      const sourcePath = path.join(sourceDir, "test.db");
      const db = new GraphDatabase(sourcePath);
      db.initialize();
      db.insertNode("n1", "Person", { name: "Alice" });
      db.insertNode("n2", "Person", { name: "Bob" });
      db.close();

      // Perform backup
      const manager = new BackupManager(backupDir);
      const result = await manager.backupDatabase(sourcePath, "test-project");

      expect(result.success).toBe(true);
      expect(result.backupPath).toBeDefined();
      expect(fs.existsSync(result.backupPath!)).toBe(true);

      // Verify backup contains the data
      const backupDb = new GraphDatabase(result.backupPath!);
      backupDb.initialize();
      expect(backupDb.countNodes()).toBe(2);
      const alice = backupDb.getNode("n1");
      expect(alice?.properties.name).toBe("Alice");
      backupDb.close();
    });

    it("should perform hot backup while database is open", async () => {
      const sourcePath = path.join(sourceDir, "hot.db");
      const db = new GraphDatabase(sourcePath);
      db.initialize();
      db.insertNode("n1", "Person", { name: "Alice" });

      // Backup while database is still open (hot backup)
      const manager = new BackupManager(backupDir);
      const result = await manager.backupDatabase(sourcePath, "hot-project");

      expect(result.success).toBe(true);

      // Add more data after backup started
      db.insertNode("n2", "Person", { name: "Bob" });
      db.close();

      // Verify backup has at least the original data
      const backupDb = new GraphDatabase(result.backupPath!);
      backupDb.initialize();
      expect(backupDb.countNodes()).toBeGreaterThanOrEqual(1);
      backupDb.close();
    });

    it("should include timestamp in backup filename", async () => {
      const sourcePath = path.join(sourceDir, "timestamp.db");
      const db = new GraphDatabase(sourcePath);
      db.initialize();
      db.close();

      const manager = new BackupManager(backupDir);
      const result = await manager.backupDatabase(sourcePath, "myproject");

      expect(result.success).toBe(true);
      // Filename should be like: myproject_2024-12-27T14-30-00-123.db
      expect(result.backupPath).toMatch(/myproject_\d{4}-\d{2}-\d{2}T\d{2}-\d{2}-\d{2}-\d{3}\.db$/);
    });

    it("should return error for non-existent source", async () => {
      const manager = new BackupManager(backupDir);
      const result = await manager.backupDatabase("/nonexistent/path.db", "test");

      expect(result.success).toBe(false);
      expect(result.error).toMatch(/not found|does not exist/i);
    });

    it("should create backup directory if it doesn't exist", async () => {
      const nestedBackupDir = path.join(backupDir, "nested", "deep");
      const sourcePath = path.join(sourceDir, "nested.db");
      
      const db = new GraphDatabase(sourcePath);
      db.initialize();
      db.close();

      const manager = new BackupManager(nestedBackupDir);
      const result = await manager.backupDatabase(sourcePath, "test");

      expect(result.success).toBe(true);
      expect(fs.existsSync(nestedBackupDir)).toBe(true);
    });
  });

  describe("backupAll", () => {
    it("should backup all databases in a data directory", async () => {
      // Create multiple databases
      const prodDir = path.join(sourceDir, "production");
      fs.mkdirSync(prodDir, { recursive: true });

      const db1 = new GraphDatabase(path.join(prodDir, "project1.db"));
      db1.initialize();
      db1.insertNode("n1", "Test", {});
      db1.close();

      const db2 = new GraphDatabase(path.join(prodDir, "project2.db"));
      db2.initialize();
      db2.insertNode("n2", "Test", {});
      db2.close();

      // Backup all
      const manager = new BackupManager(backupDir);
      const results = await manager.backupAll(sourceDir);

      expect(results.length).toBe(2);
      expect(results.every(r => r.success)).toBe(true);
    });

    it("should only backup production databases by default", async () => {
      // Create prod and test databases
      const prodDir = path.join(sourceDir, "production");
      const testDirPath = path.join(sourceDir, "test");
      fs.mkdirSync(prodDir, { recursive: true });
      fs.mkdirSync(testDirPath, { recursive: true });

      const prodDb = new GraphDatabase(path.join(prodDir, "project.db"));
      prodDb.initialize();
      prodDb.close();

      const testDb = new GraphDatabase(path.join(testDirPath, "project.db"));
      testDb.initialize();
      testDb.close();

      // Backup all (should only backup production)
      const manager = new BackupManager(backupDir);
      const results = await manager.backupAll(sourceDir);

      expect(results.length).toBe(1);
      expect(results[0].project).toBe("project");
    });

    it("should optionally backup test databases too", async () => {
      const prodDir = path.join(sourceDir, "production");
      const testDirPath = path.join(sourceDir, "test");
      fs.mkdirSync(prodDir, { recursive: true });
      fs.mkdirSync(testDirPath, { recursive: true });

      const prodDb = new GraphDatabase(path.join(prodDir, "project.db"));
      prodDb.initialize();
      prodDb.close();

      const testDb = new GraphDatabase(path.join(testDirPath, "project.db"));
      testDb.initialize();
      testDb.close();

      const manager = new BackupManager(backupDir);
      const results = await manager.backupAll(sourceDir, { includeTest: true });

      expect(results.length).toBe(2);
    });
  });

  describe("listBackups", () => {
    it("should list all backups for a project", async () => {
      const sourcePath = path.join(sourceDir, "list.db");
      const db = new GraphDatabase(sourcePath);
      db.initialize();
      db.close();

      const manager = new BackupManager(backupDir);
      
      // Create multiple backups
      await manager.backupDatabase(sourcePath, "myproject");
      await manager.backupDatabase(sourcePath, "myproject");
      await manager.backupDatabase(sourcePath, "other");

      const backups = manager.listBackups("myproject");
      expect(backups.length).toBe(2);
      expect(backups.every(b => b.includes("myproject"))).toBe(true);
    });

    it("should return empty array for project with no backups", () => {
      const manager = new BackupManager(backupDir);
      const backups = manager.listBackups("nonexistent");
      expect(backups).toEqual([]);
    });

    it("should list backups sorted by date (newest first)", async () => {
      const sourcePath = path.join(sourceDir, "sorted.db");
      const db = new GraphDatabase(sourcePath);
      db.initialize();
      db.close();

      const manager = new BackupManager(backupDir);
      
      // Create backups with slight delay to ensure different timestamp
      await manager.backupDatabase(sourcePath, "sorted");
      await new Promise(resolve => setTimeout(resolve, 10));
      await manager.backupDatabase(sourcePath, "sorted");

      const backups = manager.listBackups("sorted");
      expect(backups.length).toBe(2);
      // First should be newer than second
      expect(backups[0] > backups[1]).toBe(true);
    });
  });

  describe("cleanOldBackups", () => {
    it("should keep only the specified number of recent backups", async () => {
      const sourcePath = path.join(sourceDir, "clean.db");
      const db = new GraphDatabase(sourcePath);
      db.initialize();
      db.close();

      const manager = new BackupManager(backupDir);
      
      // Create 5 backups with different timestamps
      for (let i = 0; i < 5; i++) {
        await manager.backupDatabase(sourcePath, "cleanup");
        await new Promise(resolve => setTimeout(resolve, 10)); // Ensure unique timestamps
      }

      expect(manager.listBackups("cleanup").length).toBe(5);

      // Keep only 2
      const deleted = manager.cleanOldBackups("cleanup", 2);

      expect(deleted).toBe(3);
      expect(manager.listBackups("cleanup").length).toBe(2);
    });

    it("should not delete anything if fewer backups than limit", async () => {
      const sourcePath = path.join(sourceDir, "few.db");
      const db = new GraphDatabase(sourcePath);
      db.initialize();
      db.close();

      const manager = new BackupManager(backupDir);
      await manager.backupDatabase(sourcePath, "few");

      const deleted = manager.cleanOldBackups("few", 5);

      expect(deleted).toBe(0);
      expect(manager.listBackups("few").length).toBe(1);
    });
  });

  describe("getBackupStatus", () => {
    it("should return backup statistics", async () => {
      const sourcePath = path.join(sourceDir, "status.db");
      const db = new GraphDatabase(sourcePath);
      db.initialize();
      db.insertNode("n1", "Person", { name: "Alice" });
      db.close();

      const manager = new BackupManager(backupDir);
      await manager.backupDatabase(sourcePath, "status");

      const status = manager.getBackupStatus();

      expect(status.totalBackups).toBe(1);
      expect(status.totalSizeBytes).toBeGreaterThan(0);
      expect(status.projects).toContain("status");
    });
  });
});
