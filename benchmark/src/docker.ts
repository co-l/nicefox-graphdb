import { execSync, spawnSync } from "child_process";
import * as path from "path";

const BENCHMARK_DIR = path.dirname(new URL(import.meta.url).pathname);
const COMPOSE_FILE = path.join(BENCHMARK_DIR, "..", "docker-compose.yml");

const CONTAINERS = {
  neo4j: "benchmark-neo4j",
  memgraph: "benchmark-memgraph",
} as const;

type ContainerName = keyof typeof CONTAINERS;

/**
 * Check if Docker is installed and daemon is running.
 * Throws an error if not available.
 */
export function checkDockerAvailable(): void {
  // Check if docker CLI exists
  const result = spawnSync("docker", ["--version"], { encoding: "utf-8" });
  if (result.error || result.status !== 0) {
    throw new Error(
      "Docker is required to benchmark Neo4j/Memgraph.\n" +
      "Install Docker or run with: -d leangraph"
    );
  }

  // Check if daemon is running
  const pingResult = spawnSync("docker", ["info"], {
    encoding: "utf-8",
    stdio: ["pipe", "pipe", "pipe"],
  });
  if (pingResult.status !== 0) {
    throw new Error(
      "Docker daemon is not running.\n" +
      "Start Docker and try again, or run with: -d leangraph"
    );
  }
}

/**
 * Check if a specific container is running.
 */
export function isContainerRunning(name: ContainerName): boolean {
  const containerName = CONTAINERS[name];
  try {
    const result = execSync(
      `docker ps --filter "name=${containerName}" --filter "status=running" --format "{{.Names}}"`,
      { encoding: "utf-8" }
    );
    return result.trim() === containerName;
  } catch {
    return false;
  }
}

/**
 * Start containers using docker compose.
 */
export async function startContainers(): Promise<void> {
  try {
    execSync(`docker compose -f "${COMPOSE_FILE}" up -d`, {
      encoding: "utf-8",
      stdio: ["pipe", "pipe", "pipe"],
    });
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    throw new Error(`Failed to start Docker containers: ${message}`);
  }
}

/**
 * Stop containers and remove volumes for clean state.
 */
export async function stopContainers(): Promise<void> {
  try {
    execSync(`docker compose -f "${COMPOSE_FILE}" down -v`, {
      encoding: "utf-8",
      stdio: ["pipe", "pipe", "pipe"],
    });
  } catch {
    // Ignore errors during cleanup
  }
}

/**
 * Wait for a container to be healthy.
 */
export async function waitForHealth(
  name: ContainerName,
  timeoutMs: number = 120_000
): Promise<void> {
  const containerName = CONTAINERS[name];
  const startTime = Date.now();
  const checkInterval = 1000; // 1 second

  while (Date.now() - startTime < timeoutMs) {
    try {
      const result = execSync(
        `docker inspect --format='{{.State.Health.Status}}' ${containerName} 2>/dev/null`,
        { encoding: "utf-8" }
      );
      const status = result.trim();

      if (status === "healthy") {
        return;
      }

      // If no health check defined, check if container is running
      if (status === "" || status === "none") {
        const running = isContainerRunning(name);
        if (running) {
          // Give it a moment to initialize even without health check
          await sleep(2000);
          return;
        }
      }
    } catch {
      // Container might not exist yet, continue waiting
    }

    await sleep(checkInterval);
  }

  throw new Error(
    `Container ${containerName} did not become healthy within ${timeoutMs / 1000}s`
  );
}

/**
 * Ensure a container is running and healthy.
 * Starts containers if needed, waits for health.
 */
export async function ensureContainerReady(
  name: ContainerName,
  onStatus?: (msg: string) => void
): Promise<void> {
  const log = onStatus ?? console.log;

  checkDockerAvailable();

  if (!isContainerRunning(name)) {
    log(`  Starting Docker containers...`);
    await startContainers();
  }

  log(`  Waiting for ${name} to be ready...`);
  const startTime = Date.now();
  await waitForHealth(name);
  const elapsed = ((Date.now() - startTime) / 1000).toFixed(1);
  log(`  ${name} ready (${elapsed}s)`);
}

/**
 * Stop a specific service and remove its volume.
 */
export async function stopAndCleanup(
  name: ContainerName,
  onStatus?: (msg: string) => void
): Promise<void> {
  const log = onStatus ?? console.log;
  log(`  Stopping ${name} container...`);

  try {
    // Stop and remove the specific service with its volumes
    execSync(`docker compose -f "${COMPOSE_FILE}" rm -fsv ${name}`, {
      encoding: "utf-8",
      stdio: ["pipe", "pipe", "pipe"],
    });

    // Remove the associated volume
    const volumeName = name === "neo4j" 
      ? "benchmark_neo4j-data" 
      : "benchmark_memgraph-data";
    
    execSync(`docker volume rm ${volumeName} 2>/dev/null || true`, {
      encoding: "utf-8",
      stdio: ["pipe", "pipe", "pipe"],
    });
  } catch {
    // Ignore cleanup errors
  }
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}
