/**
 * openCypher TCK Gherkin Parser
 * 
 * Parses .feature files from the openCypher Technology Compatibility Kit
 * and extracts test scenarios that can be run against LeanGraph.
 */

import * as fs from "fs";
import * as path from "path";

// Named graph definitions for TCK tests
// These are loaded from tck/graphs/*.cypher files
const NAMED_GRAPHS: Record<string, string[]> = {
  "binary-tree-1": [
    `CREATE (a:A {name: 'a'}),
       (b1:X {name: 'b1'}),
       (b2:X {name: 'b2'}),
       (b3:X {name: 'b3'}),
       (b4:X {name: 'b4'}),
       (c11:X {name: 'c11'}),
       (c12:X {name: 'c12'}),
       (c21:X {name: 'c21'}),
       (c22:X {name: 'c22'}),
       (c31:X {name: 'c31'}),
       (c32:X {name: 'c32'}),
       (c41:X {name: 'c41'}),
       (c42:X {name: 'c42'})
CREATE (a)-[:KNOWS]->(b1),
       (a)-[:KNOWS]->(b2),
       (a)-[:FOLLOWS]->(b3),
       (a)-[:FOLLOWS]->(b4)
CREATE (b1)-[:FRIEND]->(c11),
       (b1)-[:FRIEND]->(c12),
       (b2)-[:FRIEND]->(c21),
       (b2)-[:FRIEND]->(c22),
       (b3)-[:FRIEND]->(c31),
       (b3)-[:FRIEND]->(c32),
       (b4)-[:FRIEND]->(c41),
       (b4)-[:FRIEND]->(c42)
CREATE (b1)-[:FRIEND]->(b2),
       (b2)-[:FRIEND]->(b3),
       (b3)-[:FRIEND]->(b4),
       (b4)-[:FRIEND]->(b1)`
  ],
  "binary-tree-2": [
    `CREATE (a:A {name: 'a'}),
       (b1:X {name: 'b1'}),
       (b2:X {name: 'b2'}),
       (b3:X {name: 'b3'}),
       (b4:X {name: 'b4'}),
       (c11:X {name: 'c11'}),
       (c12:Y {name: 'c12'}),
       (c21:X {name: 'c21'}),
       (c22:Y {name: 'c22'}),
       (c31:X {name: 'c31'}),
       (c32:Y {name: 'c32'}),
       (c41:X {name: 'c41'}),
       (c42:Y {name: 'c42'})
CREATE (a)-[:KNOWS]->(b1),
       (a)-[:KNOWS]->(b2),
       (a)-[:FOLLOWS]->(b3),
       (a)-[:FOLLOWS]->(b4)
CREATE (b1)-[:FRIEND]->(c11),
       (b1)-[:FRIEND]->(c12),
       (b2)-[:FRIEND]->(c21),
       (b2)-[:FRIEND]->(c22),
       (b3)-[:FRIEND]->(c31),
       (b3)-[:FRIEND]->(c32),
       (b4)-[:FRIEND]->(c41),
       (b4)-[:FRIEND]->(c42)
CREATE (b1)-[:FRIEND]->(b2),
       (b2)-[:FRIEND]->(b3),
       (b3)-[:FRIEND]->(b4),
       (b4)-[:FRIEND]->(b1)`
  ]
};

function getNamedGraphSetup(graphName: string): string[] | undefined {
  return NAMED_GRAPHS[graphName];
}

export interface TCKScenario {
  feature: string;
  featureFile: string;
  name: string;
  index: number;
  /** For expanded outline scenarios, tracks the example row number */
  exampleIndex?: number;
  given: "empty" | "any";
  setupQueries: string[];
  query: string;
  params?: Record<string, unknown>;
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

interface ScenarioOutlineTemplate {
  feature: string;
  featureFile: string;
  name: string;
  index: number;
  given: "empty" | "any";
  setupQueries: string[];
  queryTemplate: string;
  paramsTemplate?: Record<string, string>;
  expectResultTemplate?: {
    ordered: boolean;
    columns: string[];
    rowTemplates: string[][];
  };
  expectEmpty?: boolean;
  expectErrorTemplate?: {
    type: string;
    phase: string;
    detail?: string;
  };
  sideEffects?: Record<string, number>;
  exampleColumns: string[];
  exampleRows: string[][];
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
  let currentOutline: Partial<ScenarioOutlineTemplate> | null = null;
  let isOutline = false;
  let inDocString = false;
  let docStringContent: string[] = [];
  let docStringContext: "setup" | "query" | "background" | null = null;
  let currentStep: string | null = null;
  let expectingTable = false;
  let expectingExamples = false;
  let expectingParams = false;
  let tableColumns: string[] = [];
  let tableRows: unknown[][] = [];
  let tableRowsRaw: string[][] = [];
  let scenarioIndex = 0;
  // Background setup queries that apply to all scenarios
  let backgroundSetupQueries: string[] = [];
  let inBackground = false;
  
  for (let i = 0; i < lines.length; i++) {
    const line = lines[i];
    const trimmed = line.trim();
    const isTableLine = trimmed.startsWith("|") && trimmed.endsWith("|");
    
    // Skip comments and empty lines (unless in docstring)
    if (!inDocString && (trimmed.startsWith("#") || trimmed === "")) {
      continue;
    }

    // Parameter tables apply only to the immediately following table rows.
    // Most step handlers `continue;` early, so we must reset this flag before
    // processing any non-table line.
    if (!inDocString && expectingParams && !isTableLine) {
      expectingParams = false;
    }
    
    // Handle docstrings
    if (trimmed === '"""') {
      if (inDocString) {
        // End of docstring
        const query = docStringContent.join("\n").trim();
        if (docStringContext === "background") {
          backgroundSetupQueries.push(query);
        } else if (docStringContext === "setup") {
          if (isOutline && currentOutline) {
            currentOutline.setupQueries = currentOutline.setupQueries || [];
            currentOutline.setupQueries.push(query);
          } else if (currentScenario) {
            currentScenario.setupQueries = currentScenario.setupQueries || [];
            currentScenario.setupQueries.push(query);
          }
        } else if (docStringContext === "query") {
          if (isOutline && currentOutline) {
            currentOutline.queryTemplate = query;
          } else if (currentScenario) {
            currentScenario.query = query;
          }
        }
        inDocString = false;
        docStringContent = [];
        docStringContext = null;
      } else {
        // Start of docstring
        inDocString = true;
        if (currentStep === "setup") {
          docStringContext = inBackground ? "background" : "setup";
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
    
    // Background section - setup that applies to all scenarios
    if (trimmed.startsWith("Background:")) {
      inBackground = true;
      continue;
    }
    
    // Scenario or Scenario Outline
    if (trimmed.startsWith("Scenario:") || trimmed.startsWith("Scenario Outline:")) {
      // Exiting Background if we were in it
      inBackground = false;
      
      // Save previous scenario (non-outline)
      if (!isOutline && currentScenario && currentScenario.query) {
        if (expectingTable && tableColumns.length > 0) {
          currentScenario.expectResult = {
            ordered: currentScenario.expectResult?.ordered ?? false,
            columns: tableColumns,
            rows: tableRows,
          };
        }
        scenarios.push(currentScenario as TCKScenario);
      }
      
      // Expand and save previous outline
      if (isOutline && currentOutline && currentOutline.queryTemplate) {
        if (expectingTable && tableColumns.length > 0 && !expectingExamples) {
          currentOutline.expectResultTemplate = {
            ordered: currentOutline.expectResultTemplate?.ordered ?? false,
            columns: tableColumns,
            rowTemplates: tableRowsRaw,
          };
        }
        const expanded = expandOutline(currentOutline as ScenarioOutlineTemplate);
        scenarios.push(...expanded);
      }
      
      // Start new scenario
      scenarioIndex++;
      const isNewOutline = trimmed.startsWith("Scenario Outline:");
      const scenarioType = isNewOutline ? "Scenario Outline:" : "Scenario:";
      const name = trimmed.substring(scenarioType.length).trim();
      
      if (isNewOutline) {
        isOutline = true;
        currentOutline = {
          feature: featureName,
          featureFile: path.basename(filePath),
          name,
          index: scenarioIndex,
          given: "any",
          setupQueries: [...backgroundSetupQueries],
          queryTemplate: "",
          exampleColumns: [],
          exampleRows: [],
        };
        currentScenario = null;
      } else {
        isOutline = false;
        currentScenario = {
          feature: featureName,
          featureFile: path.basename(filePath),
          name,
          index: scenarioIndex,
          given: "any",
          setupQueries: [...backgroundSetupQueries],
          query: "",
        };
        currentOutline = null;
      }
      
      expectingTable = false;
      expectingExamples = false;
      tableColumns = [];
      tableRows = [];
      tableRowsRaw = [];
      currentStep = null;
      continue;
    }
    
    // Examples section for Scenario Outline
    if (trimmed.startsWith("Examples:")) {
      // Save result table before switching to examples
      if (expectingTable && tableColumns.length > 0 && currentOutline) {
        currentOutline.expectResultTemplate = {
          ordered: currentOutline.expectResultTemplate?.ordered ?? false,
          columns: tableColumns,
          rowTemplates: tableRowsRaw,
        };
      }
      expectingTable = false;
      expectingExamples = true;
      expectingParams = false; // Reset params flag when hitting Examples
      tableColumns = [];
      tableRows = [];
      tableRowsRaw = [];
      continue;
    }
    
    // Given
    if (trimmed.startsWith("Given")) {
      if (trimmed.includes("an empty graph")) {
        if (isOutline && currentOutline) currentOutline.given = "empty";
        else if (currentScenario) currentScenario.given = "empty";
      } else if (trimmed.includes("any graph")) {
        if (isOutline && currentOutline) currentOutline.given = "any";
        else if (currentScenario) currentScenario.given = "any";
      } else {
        // Handle named graphs like "Given the binary-tree-1 graph"
        const namedGraphMatch = trimmed.match(/Given the (\S+) graph/);
        if (namedGraphMatch) {
          const graphName = namedGraphMatch[1];
          const graphSetup = getNamedGraphSetup(graphName);
          if (graphSetup) {
            if (isOutline && currentOutline) {
              currentOutline.setupQueries = currentOutline.setupQueries || [];
              currentOutline.setupQueries.push(...graphSetup);
            } else if (currentScenario) {
              currentScenario.setupQueries = currentScenario.setupQueries || [];
              currentScenario.setupQueries.push(...graphSetup);
            }
          }
        }
      }
      continue;
    }
    
    // And having executed (setup query)
    if (trimmed.startsWith("And having executed:")) {
      currentStep = "setup";
      continue;
    }
    
    // And parameters are:
    if (trimmed.startsWith("And parameters are:")) {
      expectingParams = true;
      continue;
    }
    
    // When executing query
    if (trimmed.startsWith("When executing query:")) {
      currentStep = "query";
      continue;
    }
    
    // Then the result should be empty (check this BEFORE "Then the result should be")
    if (trimmed.startsWith("Then the result should be empty")) {
      if (isOutline && currentOutline) {
        currentOutline.expectEmpty = true;
      } else if (currentScenario) {
        currentScenario.expectEmpty = true;
      }
      expectingTable = false;
      continue;
    }
    
    // Then the result should be
    if (trimmed.startsWith("Then the result should be")) {
      expectingTable = true;
      expectingExamples = false;
      tableColumns = [];
      tableRows = [];
      tableRowsRaw = [];
      const ordered = trimmed.includes("in order");
      if (isOutline && currentOutline) {
        currentOutline.expectResultTemplate = { ordered, columns: [], rowTemplates: [] };
      } else if (currentScenario) {
        currentScenario.expectResult = { ordered, columns: [], rows: [] };
      }
      continue;
    }
    
    // Error expectations
    if (trimmed.match(/Then a (\w+) should be raised/)) {
      const match = trimmed.match(/Then a (\w+) should be raised(?: at (\w+)(?: time)?)?(?: ?: ?(\w+))?/);
      if (match) {
        const errorInfo = {
          type: match[1],
          phase: match[2] || "runtime",
          detail: match[3],
        };
        if (isOutline && currentOutline) {
          currentOutline.expectErrorTemplate = errorInfo;
        } else if (currentScenario) {
          currentScenario.expectError = errorInfo;
        }
      }
      continue;
    }
    
    // Side effects - stop expecting result table and start expecting side effects table
    if (trimmed.startsWith("And the side effects should be:") || trimmed.startsWith("And no side effects")) {
      // First, save any pending result table
      if (expectingTable && tableColumns.length > 0) {
        if (isOutline && currentOutline) {
          currentOutline.expectResultTemplate = {
            ordered: currentOutline.expectResultTemplate?.ordered ?? false,
            columns: tableColumns,
            rowTemplates: tableRowsRaw,
          };
        } else if (currentScenario) {
          currentScenario.expectResult = {
            ordered: currentScenario.expectResult?.ordered ?? false,
            columns: tableColumns,
            rows: tableRows,
          };
        }
      }
      expectingTable = false;
      expectingExamples = false;
      tableColumns = [];
      tableRows = [];
      tableRowsRaw = [];
      
      if (trimmed.includes("no side effects")) {
        if (isOutline && currentOutline) {
          currentOutline.sideEffects = {};
        } else if (currentScenario) {
          currentScenario.sideEffects = {};
        }
      }
      continue;
    }
    
    // Parse tables (| col1 | col2 |)
    if (isTableLine) {
      const cells = trimmed
        .slice(1, -1)
        .split("|")
        .map(c => c.trim());
      
      // Parameter table
      if (expectingParams && cells.length === 2) {
        const key = cells[0];
        const value = cells[1];
        if (isOutline && currentOutline) {
          currentOutline.paramsTemplate = currentOutline.paramsTemplate || {};
          currentOutline.paramsTemplate[key] = value;
        } else if (currentScenario) {
          currentScenario.params = currentScenario.params || {};
          currentScenario.params[key] = parseCellValue(value);
        }
        continue;
      }
      
      if (expectingExamples && currentOutline) {
        if (currentOutline.exampleColumns!.length === 0) {
          currentOutline.exampleColumns = cells;
        } else {
          currentOutline.exampleRows!.push(cells);
        }
      } else if (expectingTable) {
        if (tableColumns.length === 0) {
          // Header row
          tableColumns = cells;
        } else {
          // Data row - keep both raw and parsed
          tableRowsRaw.push(cells);
          const row = cells.map(cell => parseCellValue(cell));
          tableRows.push(row);
        }
      } else {
        // Side effects table
        if (cells.length === 2) {
          const key = cells[0];
          const value = parseInt(cells[1], 10);
          if (!isNaN(value)) {
            if (isOutline && currentOutline) {
              currentOutline.sideEffects = currentOutline.sideEffects || {};
              currentOutline.sideEffects[key] = value;
            } else if (currentScenario) {
              currentScenario.sideEffects = currentScenario.sideEffects || {};
              currentScenario.sideEffects[key] = value;
            }
          }
        }
      }
       continue;
     }
     
     // When we hit another step (not a table), stop expecting params
     if (expectingParams) {
       expectingParams = false;
     }
   }
   
   // Save last scenario
  if (!isOutline && currentScenario && currentScenario.query) {
    if (expectingTable && tableColumns.length > 0) {
      currentScenario.expectResult = {
        ordered: currentScenario.expectResult?.ordered ?? false,
        columns: tableColumns,
        rows: tableRows,
      };
    }
    scenarios.push(currentScenario as TCKScenario);
  }
  
  // Expand and save last outline
  if (isOutline && currentOutline && currentOutline.queryTemplate) {
    if (expectingTable && tableColumns.length > 0 && !expectingExamples) {
      currentOutline.expectResultTemplate = {
        ordered: currentOutline.expectResultTemplate?.ordered ?? false,
        columns: tableColumns,
        rowTemplates: tableRowsRaw,
      };
    }
    const expanded = expandOutline(currentOutline as ScenarioOutlineTemplate);
    scenarios.push(...expanded);
  }
  
  return {
    name: featureName,
    file: filePath,
    scenarios,
  };
}

/**
 * Expand a Scenario Outline template into individual test scenarios
 */
function expandOutline(outline: ScenarioOutlineTemplate): TCKScenario[] {
  const scenarios: TCKScenario[] = [];
  
  for (let rowIdx = 0; rowIdx < outline.exampleRows.length; rowIdx++) {
    const exampleRow = outline.exampleRows[rowIdx];
    const substitutions = new Map<string, string>();
    
    // Build substitution map
    for (let colIdx = 0; colIdx < outline.exampleColumns.length; colIdx++) {
      const colName = outline.exampleColumns[colIdx];
      const value = exampleRow[colIdx];
      substitutions.set(colName, value);
    }
    
    // Substitute in query
    const query = substituteTemplate(outline.queryTemplate, substitutions);
    
    // Substitute in setup queries
    const setupQueries = outline.setupQueries.map(q => substituteTemplate(q, substitutions));
    
    // Substitute in params (if any)
    let params: Record<string, unknown> | undefined;
    if (outline.paramsTemplate) {
      params = {};
      for (const [key, valueTemplate] of Object.entries(outline.paramsTemplate)) {
        const substituted = substituteTemplate(valueTemplate, substitutions);
        params[key] = parseCellValue(substituted);
      }
    }
    
    // Substitute in expected result
    let expectResult: TCKScenario["expectResult"];
    if (outline.expectResultTemplate) {
      const rows = outline.expectResultTemplate.rowTemplates.map(rowTemplate => 
        rowTemplate.map(cell => {
          const substituted = substituteTemplate(cell, substitutions);
          return parseCellValue(substituted);
        })
      );
      expectResult = {
        ordered: outline.expectResultTemplate.ordered,
        columns: outline.expectResultTemplate.columns,
        rows,
      };
    }
    
    // Substitute in expected error detail if needed
    let expectError: TCKScenario["expectError"];
    if (outline.expectErrorTemplate) {
      expectError = {
        type: outline.expectErrorTemplate.type,
        phase: outline.expectErrorTemplate.phase,
        detail: outline.expectErrorTemplate.detail 
          ? substituteTemplate(outline.expectErrorTemplate.detail, substitutions)
          : undefined,
      };
    }
    
    scenarios.push({
      feature: outline.feature,
      featureFile: outline.featureFile,
      name: outline.name,
      index: outline.index,
      exampleIndex: rowIdx + 1,
      given: outline.given,
      setupQueries,
      query,
      params,
      expectResult,
      expectEmpty: outline.expectEmpty,
      expectError,
      sideEffects: outline.sideEffects,
    });
  }
  
  return scenarios;
}

/**
 * Substitute <placeholder> values in a template string
 */
function substituteTemplate(template: string, substitutions: Map<string, string>): string {
  let result = template;
  for (const [key, value] of substitutions) {
    const placeholder = new RegExp(`<${key}>`, 'g');
    result = result.replace(placeholder, value);
  }
  return result;
}

/**
 * Process Cypher escape sequences in a string
 */
function unescapeCypherString(str: string): string {
  return str
    .replace(/\\n/g, "\n")
    .replace(/\\t/g, "\t")
    .replace(/\\r/g, "\r")
    .replace(/\\b/g, "\b")
    .replace(/\\f/g, "\f")
    .replace(/\\\\/g, "\\")
    .replace(/\\'/g, "'")
    .replace(/\\"/g, '"');
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
    return unescapeCypherString(cell.slice(1, -1));
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
  
  // Relationship pattern like [:TYPE]
  if (cell.startsWith("[:") && cell.endsWith("]")) {
    return { _relPattern: cell };
  }
  
  // List
  if (cell.startsWith("[") && cell.endsWith("]")) {
    // Try to parse as JSON-like list
    try {
      return JSON.parse(cell.replace(/'/g, '"'));
    } catch {
      // If it looks like a list pattern for relationships, mark it as such
      return { _relPattern: cell };
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
  expandedFromOutlines: number;
} {
  let totalScenarios = 0;
  let withExpectedResults = 0;
  let withExpectedErrors = 0;
  let withExpectedEmpty = 0;
  let expandedFromOutlines = 0;
  
  for (const feature of features) {
    for (const scenario of feature.scenarios) {
      totalScenarios++;
      if (scenario.expectResult) withExpectedResults++;
      if (scenario.expectError) withExpectedErrors++;
      if (scenario.expectEmpty) withExpectedEmpty++;
      if (scenario.exampleIndex !== undefined) expandedFromOutlines++;
    }
  }
  
  return {
    totalFeatures: features.length,
    totalScenarios,
    withExpectedResults,
    withExpectedErrors,
    withExpectedEmpty,
    expandedFromOutlines,
  };
}
