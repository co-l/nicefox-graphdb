/**
 * openCypher TCK Gherkin Parser
 * 
 * Parses .feature files from the openCypher Technology Compatibility Kit
 * and extracts test scenarios that can be run against NiceFox GraphDB.
 */

import * as fs from "fs";
import * as path from "path";

export interface TCKScenario {
  feature: string;
  featureFile: string;
  name: string;
  index: number;
  given: "empty" | "any";
  setupQueries: string[];
  query: string;
  expectResult?: {
    ordered: boolean;
    columns: string[];
    rows: unknown[][];
  };
  expectEmpty?: boolean;
  expectError?: {
    type: string;
    phase: string;
    detail?: string;
  };
  sideEffects?: Record<string, number>;
  tags?: string[];
}

export interface ParsedFeature {
  name: string;
  file: string;
  scenarios: TCKScenario[];
}

/**
 * Parse a single .feature file
 */
export function parseFeatureFile(filePath: string): ParsedFeature {
  const content = fs.readFileSync(filePath, "utf-8");
  const lines = content.split("\n");
  
  let featureName = "";
  const scenarios: TCKScenario[] = [];
  let currentScenario: Partial<TCKScenario> | null = null;
  let inDocString = false;
  let docStringContent: string[] = [];
  let docStringContext: "setup" | "query" | null = null;
  let currentStep: string | null = null;
  let expectingTable = false;
  let tableColumns: string[] = [];
  let tableRows: unknown[][] = [];
  let scenarioIndex = 0;
  
  for (let i = 0; i < lines.length; i++) {
    const line = lines[i];
    const trimmed = line.trim();
    
    // Skip comments and empty lines (unless in docstring)
    if (!inDocString && (trimmed.startsWith("#") || trimmed === "")) {
      continue;
    }
    
    // Handle docstrings
    if (trimmed === '"""') {
      if (inDocString) {
        // End of docstring
        const query = docStringContent.join("\n").trim();
        if (docStringContext === "setup" && currentScenario) {
          currentScenario.setupQueries = currentScenario.setupQueries || [];
          currentScenario.setupQueries.push(query);
        } else if (docStringContext === "query" && currentScenario) {
          currentScenario.query = query;
        }
        inDocString = false;
        docStringContent = [];
        docStringContext = null;
      } else {
        // Start of docstring
        inDocString = true;
        if (currentStep === "setup") {
          docStringContext = "setup";
        } else if (currentStep === "query") {
          docStringContext = "query";
        }
      }
      continue;
    }
    
    if (inDocString) {
      docStringContent.push(line);
      continue;
    }
    
    // Feature name
    if (trimmed.startsWith("Feature:")) {
      featureName = trimmed.substring(8).trim();
      continue;
    }
    
    // Scenario or Scenario Outline
    if (trimmed.startsWith("Scenario:") || trimmed.startsWith("Scenario Outline:")) {
      // Save previous scenario
      if (currentScenario && currentScenario.query) {
        if (expectingTable && tableColumns.length > 0) {
          currentScenario.expectResult = {
            ordered: false,
            columns: tableColumns,
            rows: tableRows,
          };
        }
        scenarios.push(currentScenario as TCKScenario);
      }
      
      // Start new scenario
      scenarioIndex++;
      const scenarioType = trimmed.startsWith("Scenario Outline:") ? "Scenario Outline:" : "Scenario:";
      const name = trimmed.substring(scenarioType.length).trim();
      currentScenario = {
        feature: featureName,
        featureFile: path.basename(filePath),
        name,
        index: scenarioIndex,
        given: "any",
        setupQueries: [],
        query: "",
      };
      expectingTable = false;
      tableColumns = [];
      tableRows = [];
      currentStep = null;
      continue;
    }
    
    // Skip Scenario Outline Examples for now (too complex)
    if (trimmed.startsWith("Examples:")) {
      // Mark scenario as outline to skip
      if (currentScenario) {
        currentScenario.tags = currentScenario.tags || [];
        currentScenario.tags.push("outline");
      }
      continue;
    }
    
    // Given
    if (trimmed.startsWith("Given")) {
      if (trimmed.includes("an empty graph")) {
        if (currentScenario) currentScenario.given = "empty";
      } else if (trimmed.includes("any graph")) {
        if (currentScenario) currentScenario.given = "any";
      }
      continue;
    }
    
    // And having executed (setup query)
    if (trimmed.startsWith("And having executed:")) {
      currentStep = "setup";
      continue;
    }
    
    // When executing query
    if (trimmed.startsWith("When executing query:")) {
      currentStep = "query";
      continue;
    }
    
    // Then the result should be empty (check this BEFORE "Then the result should be")
    if (trimmed.startsWith("Then the result should be empty")) {
      if (currentScenario) {
        currentScenario.expectEmpty = true;
      }
      expectingTable = false;
      continue;
    }
    
    // Then the result should be
    if (trimmed.startsWith("Then the result should be")) {
      expectingTable = true;
      tableColumns = [];
      tableRows = [];
      if (trimmed.includes("in order")) {
        if (currentScenario) {
          currentScenario.expectResult = currentScenario.expectResult || { ordered: true, columns: [], rows: [] };
          currentScenario.expectResult.ordered = true;
        }
      }
      continue;
    }
    
    // Error expectations
    if (trimmed.match(/Then a (\w+) should be raised/)) {
      const match = trimmed.match(/Then a (\w+) should be raised at (\w+) time: (\w+)/);
      if (match && currentScenario) {
        currentScenario.expectError = {
          type: match[1],
          phase: match[2],
          detail: match[3],
        };
      }
      continue;
    }
    
    // Side effects - stop expecting result table and start expecting side effects table
    if (trimmed.startsWith("And the side effects should be:") || trimmed.startsWith("And no side effects")) {
      // First, save any pending result table
      if (expectingTable && tableColumns.length > 0 && currentScenario) {
        currentScenario.expectResult = {
          ordered: currentScenario.expectResult?.ordered ?? false,
          columns: tableColumns,
          rows: tableRows,
        };
      }
      expectingTable = false;
      tableColumns = [];
      tableRows = [];
      
      if (trimmed.includes("no side effects") && currentScenario) {
        currentScenario.sideEffects = {};
      }
      // Next lines will be the side effects table
      continue;
    }
    
    // Parse tables (| col1 | col2 |)
    if (trimmed.startsWith("|") && trimmed.endsWith("|")) {
      const cells = trimmed
        .slice(1, -1)
        .split("|")
        .map(c => c.trim());
      
      if (expectingTable) {
        if (tableColumns.length === 0) {
          // Header row
          tableColumns = cells;
        } else {
          // Data row
          const row = cells.map(cell => parseCellValue(cell));
          tableRows.push(row);
        }
      } else if (currentScenario && cells.length === 2) {
        // Side effects table
        currentScenario.sideEffects = currentScenario.sideEffects || {};
        const key = cells[0];
        const value = parseInt(cells[1], 10);
        if (!isNaN(value)) {
          currentScenario.sideEffects[key] = value;
        }
      }
      continue;
    }
  }
  
  // Save last scenario
  if (currentScenario && currentScenario.query) {
    if (expectingTable && tableColumns.length > 0) {
      currentScenario.expectResult = {
        ordered: currentScenario.expectResult?.ordered ?? false,
        columns: tableColumns,
        rows: tableRows,
      };
    }
    scenarios.push(currentScenario as TCKScenario);
  }
  
  return {
    name: featureName,
    file: filePath,
    scenarios,
  };
}

/**
 * Parse a cell value from the result table
 */
function parseCellValue(cell: string): unknown {
  // Null
  if (cell === "null") return null;
  
  // Boolean
  if (cell === "true") return true;
  if (cell === "false") return false;
  
  // String (quoted)
  if (cell.startsWith("'") && cell.endsWith("'")) {
    return cell.slice(1, -1);
  }
  
  // Number
  const num = Number(cell);
  if (!isNaN(num) && cell !== "") {
    return num;
  }
  
  // Node pattern like (:Label {prop: 'val'})
  if (cell.startsWith("(") && cell.endsWith(")")) {
    return { _nodePattern: cell };
  }
  
  // Path pattern like <(:Start)-[:T]->()>
  if (cell.startsWith("<") && cell.endsWith(">")) {
    return { _pathPattern: cell };
  }
  
  // Relationship pattern
  if (cell.startsWith("[") && cell.endsWith("]")) {
    return { _relPattern: cell };
  }
  
  // List
  if (cell.startsWith("[") && cell.endsWith("]")) {
    // Try to parse as JSON-like list
    try {
      return JSON.parse(cell.replace(/'/g, '"'));
    } catch {
      return cell;
    }
  }
  
  // Map/object
  if (cell.startsWith("{") && cell.endsWith("}")) {
    try {
      return JSON.parse(cell.replace(/'/g, '"'));
    } catch {
      return cell;
    }
  }
  
  return cell;
}

/**
 * Recursively find all .feature files in a directory
 */
export function findFeatureFiles(dir: string): string[] {
  const files: string[] = [];
  
  const entries = fs.readdirSync(dir, { withFileTypes: true });
  for (const entry of entries) {
    const fullPath = path.join(dir, entry.name);
    if (entry.isDirectory()) {
      files.push(...findFeatureFiles(fullPath));
    } else if (entry.name.endsWith(".feature")) {
      files.push(fullPath);
    }
  }
  
  return files;
}

/**
 * Parse all feature files in a directory
 */
export function parseAllFeatures(dir: string): ParsedFeature[] {
  const files = findFeatureFiles(dir);
  return files.map(f => parseFeatureFile(f));
}

/**
 * Get statistics about parsed features
 */
export function getStats(features: ParsedFeature[]): {
  totalFeatures: number;
  totalScenarios: number;
  withExpectedResults: number;
  withExpectedErrors: number;
  withExpectedEmpty: number;
  outlineScenarios: number;
} {
  let totalScenarios = 0;
  let withExpectedResults = 0;
  let withExpectedErrors = 0;
  let withExpectedEmpty = 0;
  let outlineScenarios = 0;
  
  for (const feature of features) {
    for (const scenario of feature.scenarios) {
      totalScenarios++;
      if (scenario.expectResult) withExpectedResults++;
      if (scenario.expectError) withExpectedErrors++;
      if (scenario.expectEmpty) withExpectedEmpty++;
      if (scenario.tags?.includes("outline")) outlineScenarios++;
    }
  }
  
  return {
    totalFeatures: features.length,
    totalScenarios,
    withExpectedResults,
    withExpectedErrors,
    withExpectedEmpty,
    outlineScenarios,
  };
}
