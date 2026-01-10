import { BENCHMARK_CONFIG } from "./config.js";
import type { ScaleConfig } from "./types.js";

// Deterministic pseudo-random number generator (mulberry32)
function createRng(seed: number) {
  return function () {
    let t = (seed += 0x6d2b79f5);
    t = Math.imul(t ^ (t >>> 15), t | 1);
    t ^= t + Math.imul(t ^ (t >>> 7), t | 61);
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}

const rng = createRng(42); // Fixed seed for reproducibility

function randomInt(min: number, max: number): number {
  return Math.floor(rng() * (max - min + 1)) + min;
}

function randomChoice<T>(arr: T[]): T {
  return arr[Math.floor(rng() * arr.length)];
}

// Name generation
const firstNames = [
  "James", "Mary", "John", "Patricia", "Robert", "Jennifer", "Michael", "Linda",
  "William", "Elizabeth", "David", "Barbara", "Richard", "Susan", "Joseph", "Jessica",
  "Thomas", "Sarah", "Charles", "Karen", "Emma", "Olivia", "Ava", "Isabella",
  "Sophia", "Mia", "Charlotte", "Amelia", "Harper", "Evelyn", "Liam", "Noah",
];

const lastNames = [
  "Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis",
  "Rodriguez", "Martinez", "Hernandez", "Lopez", "Gonzalez", "Wilson", "Anderson",
  "Thomas", "Taylor", "Moore", "Jackson", "Martin", "Lee", "Perez", "Thompson",
];

function generateName(): string {
  return `${randomChoice(firstNames)} ${randomChoice(lastNames)}`;
}

function generateEmail(id: number): string {
  return `user${id}@example.com`;
}

function generateItemTitle(id: number): string {
  const adjectives = ["Premium", "Basic", "Pro", "Ultra", "Mini", "Max", "Eco", "Smart"];
  const nouns = ["Widget", "Gadget", "Device", "Tool", "Kit", "Set", "Pack", "Bundle"];
  return `${randomChoice(adjectives)} ${randomChoice(nouns)} ${id}`;
}

// Node generators
export interface UserNode {
  id: number;
  name: string;
  email: string;
  created_at: number;
}

export interface ItemNode {
  id: number;
  title: string;
  category: string;
  price: number;
}

export interface EventNode {
  id: number;
  type: string;
  timestamp: number;
}

export interface Edge {
  fromId: number;
  toId: number;
}

export function* generateUsers(count: number): Generator<UserNode> {
  const baseTime = Date.now() - 365 * 24 * 60 * 60 * 1000; // 1 year ago
  for (let i = 0; i < count; i++) {
    yield {
      id: i,
      name: generateName(),
      email: generateEmail(i),
      created_at: baseTime + randomInt(0, 365 * 24 * 60 * 60 * 1000),
    };
    if (i > 0 && i % 100_000 === 0) {
      console.log(`  Users: ${i.toLocaleString()} / ${count.toLocaleString()}`);
    }
  }
}

export function* generateItems(count: number): Generator<ItemNode> {
  for (let i = 0; i < count; i++) {
    yield {
      id: i,
      title: generateItemTitle(i),
      category: randomChoice(BENCHMARK_CONFIG.categories),
      price: randomInt(1, 1000) + randomInt(0, 99) / 100,
    };
    if (i > 0 && i % 100_000 === 0) {
      console.log(`  Items: ${i.toLocaleString()} / ${count.toLocaleString()}`);
    }
  }
}

export function* generateEvents(count: number): Generator<EventNode> {
  const baseTime = Date.now() - 30 * 24 * 60 * 60 * 1000; // 30 days ago
  for (let i = 0; i < count; i++) {
    yield {
      id: i,
      type: randomChoice(BENCHMARK_CONFIG.eventTypes),
      timestamp: baseTime + randomInt(0, 30 * 24 * 60 * 60 * 1000),
    };
    if (i > 0 && i % 100_000 === 0) {
      console.log(`  Events: ${i.toLocaleString()} / ${count.toLocaleString()}`);
    }
  }
}

export function* generateOwnsEdges(userCount: number, itemCount: number, edgeCount: number): Generator<Edge> {
  // Each user owns some items
  for (let i = 0; i < edgeCount; i++) {
    yield {
      fromId: randomInt(0, userCount - 1),
      toId: randomInt(0, itemCount - 1),
    };
    if (i > 0 && i % 100_000 === 0) {
      console.log(`  OWNS edges: ${i.toLocaleString()} / ${edgeCount.toLocaleString()}`);
    }
  }
}

export function* generateTriggeredEdges(userCount: number, eventCount: number, edgeCount: number): Generator<Edge> {
  // Each user triggers some events
  for (let i = 0; i < edgeCount; i++) {
    yield {
      fromId: randomInt(0, userCount - 1),
      toId: randomInt(0, eventCount - 1),
    };
    if (i > 0 && i % 100_000 === 0) {
      console.log(`  TRIGGERED edges: ${i.toLocaleString()} / ${edgeCount.toLocaleString()}`);
    }
  }
}

export function* generateRelatedToEdges(itemCount: number, edgeCount: number): Generator<Edge> {
  // Items related to other items
  for (let i = 0; i < edgeCount; i++) {
    const fromId = randomInt(0, itemCount - 1);
    let toId = randomInt(0, itemCount - 1);
    // Avoid self-loops
    while (toId === fromId) {
      toId = randomInt(0, itemCount - 1);
    }
    yield { fromId, toId };
    if (i > 0 && i % 100_000 === 0) {
      console.log(`  RELATED_TO edges: ${i.toLocaleString()} / ${edgeCount.toLocaleString()}`);
    }
  }
}

// Batch helper
export function* batch<T>(generator: Generator<T>, size: number): Generator<T[]> {
  let batch: T[] = [];
  for (const item of generator) {
    batch.push(item);
    if (batch.length >= size) {
      yield batch;
      batch = [];
    }
  }
  if (batch.length > 0) {
    yield batch;
  }
}

// Random ID generators for query params (reset seed for reproducibility)
let queryRng = createRng(12345);

export function resetQueryRng() {
  queryRng = createRng(12345);
}

export function randomUserId(config: ScaleConfig): number {
  return Math.floor(queryRng() * config.users);
}

export function randomItemId(config: ScaleConfig): number {
  return Math.floor(queryRng() * config.items);
}

export function randomEventId(config: ScaleConfig): number {
  return Math.floor(queryRng() * config.events);
}

export function randomCategory(): string {
  return BENCHMARK_CONFIG.categories[Math.floor(queryRng() * BENCHMARK_CONFIG.categories.length)];
}

export function randomEventType(): string {
  return BENCHMARK_CONFIG.eventTypes[Math.floor(queryRng() * BENCHMARK_CONFIG.eventTypes.length)];
}

export function uuid(): string {
  return `${Date.now()}-${Math.floor(queryRng() * 1000000)}`;
}

export function fakeName(): string {
  return `${firstNames[Math.floor(queryRng() * firstNames.length)]} ${lastNames[Math.floor(queryRng() * lastNames.length)]}`;
}

export function fakeEmail(): string {
  return `user-${uuid()}@example.com`;
}
