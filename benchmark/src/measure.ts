import { execSync } from "child_process";
import * as fs from "fs";
import type { TimingStats } from "./types.js";

/**
 * Get disk usage of a file or directory in bytes
 */
export function getDiskUsage(path: string): number {
  if (!fs.existsSync(path)) {
    return 0;
  }

  try {
    // Try du -sb first (Linux), fall back to du -sk (macOS)
    let output: string;
    let isKilobytes = false;

    try {
      output = execSync(`du -sb "${path}"`, { encoding: "utf-8", stdio: ["pipe", "pipe", "pipe"] });
    } catch {
      // du -sb not available (macOS), use -sk
      output = execSync(`du -sk "${path}"`, { encoding: "utf-8" });
      isKilobytes = true;
    }

    const bytes = parseInt(output.split("\t")[0], 10);
    return isKilobytes ? bytes * 1024 : bytes;
  } catch {
    // Fallback: use fs.statSync for single files
    try {
      const stats = fs.statSync(path);
      return stats.size;
    } catch {
      return 0;
    }
  }
}

/**
 * Get RAM usage of current process in bytes
 */
export function getProcessRam(): number {
  return process.memoryUsage().rss;
}

/**
 * Get RAM usage of a Docker container in bytes
 */
export async function getDockerRam(containerName: string): Promise<number> {
  try {
    const output = execSync(
      `docker stats ${containerName} --no-stream --format '{{.MemUsage}}'`,
      { encoding: "utf-8" }
    );
    return parseDockerMemory(output.trim());
  } catch {
    return 0;
  }
}

/**
 * Parse Docker memory string like "1.2GiB / 6GiB" or "500MiB / 2GiB"
 */
function parseDockerMemory(memStr: string): number {
  // Extract the used memory (first part before "/")
  const used = memStr.split("/")[0].trim();

  // Parse value and unit
  const match = used.match(/^([\d.]+)\s*(B|KiB|MiB|GiB|KB|MB|GB)?$/i);
  if (!match) return 0;

  const value = parseFloat(match[1]);
  const unit = (match[2] || "B").toUpperCase();

  const multipliers: Record<string, number> = {
    B: 1,
    KB: 1000,
    KIB: 1024,
    MB: 1000 * 1000,
    MIB: 1024 * 1024,
    GB: 1000 * 1000 * 1000,
    GIB: 1024 * 1024 * 1024,
  };

  return Math.floor(value * (multipliers[unit] || 1));
}

/**
 * Get Docker volume disk usage in bytes
 */
export async function getDockerVolumeSize(volumeName: string): Promise<number> {
  try {
    // First get the mount point
    const inspectOutput = execSync(
      `docker volume inspect ${volumeName} --format '{{.Mountpoint}}'`,
      { encoding: "utf-8" }
    );
    const mountPoint = inspectOutput.trim();

    // Then get the size (requires sudo or proper permissions)
    try {
      const sizeOutput = execSync(`sudo du -sb "${mountPoint}" 2>/dev/null`, {
        encoding: "utf-8",
      });
      return parseInt(sizeOutput.split("\t")[0], 10);
    } catch {
      // Try without sudo
      const sizeOutput = execSync(`du -sb "${mountPoint}" 2>/dev/null || du -sk "${mountPoint}"`, {
        encoding: "utf-8",
      });
      const bytes = parseInt(sizeOutput.split("\t")[0], 10);
      return sizeOutput.includes("k") ? bytes * 1024 : bytes;
    }
  } catch {
    return 0;
  }
}

/**
 * Calculate timing statistics from an array of measurements
 */
export function calculateStats(times: number[]): TimingStats {
  if (times.length === 0) {
    return { min: 0, max: 0, mean: 0, p50: 0, p95: 0, p99: 0, samples: 0 };
  }

  const sorted = [...times].sort((a, b) => a - b);
  const sum = sorted.reduce((a, b) => a + b, 0);

  return {
    min: sorted[0],
    max: sorted[sorted.length - 1],
    mean: sum / sorted.length,
    p50: percentile(sorted, 50),
    p95: percentile(sorted, 95),
    p99: percentile(sorted, 99),
    samples: sorted.length,
  };
}

function percentile(sortedArr: number[], p: number): number {
  const index = (p / 100) * (sortedArr.length - 1);
  const lower = Math.floor(index);
  const upper = Math.ceil(index);
  if (lower === upper) return sortedArr[lower];
  return sortedArr[lower] + (sortedArr[upper] - sortedArr[lower]) * (index - lower);
}

/**
 * Format bytes as human-readable string
 */
export function formatBytes(bytes: number): string {
  if (bytes === 0) return "0 B";
  const units = ["B", "KB", "MB", "GB", "TB"];
  const i = Math.floor(Math.log(bytes) / Math.log(1024));
  return `${(bytes / Math.pow(1024, i)).toFixed(1)} ${units[i]}`;
}

/**
 * Format milliseconds as human-readable string
 */
export function formatMs(ms: number): string {
  if (ms < 1) return `${(ms * 1000).toFixed(0)}µs`;
  if (ms < 1000) return `${ms.toFixed(1)}ms`;
  return `${(ms / 1000).toFixed(2)}s`;
}

/**
 * Format seconds as human-readable string
 */
export function formatSeconds(seconds: number): string {
  if (seconds < 60) return `${seconds.toFixed(1)}s`;
  const mins = Math.floor(seconds / 60);
  const secs = seconds % 60;
  return `${mins}m ${secs.toFixed(0)}s`;
}
