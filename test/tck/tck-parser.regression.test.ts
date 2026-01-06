import { describe, it, expect } from "vitest";
import * as path from "path";
import { parseAllFeatures } from "./tck-parser";

describe("tck-parser regressions", () => {
  it("parses result tables after setup queries (Unwind1|12)", () => {
    const tckPath = path.join(__dirname, "openCypher/tck/features");
    const features = parseAllFeatures(tckPath);
    const unwind1 = features.find(f => f.file.endsWith(`${path.sep}clauses${path.sep}unwind${path.sep}Unwind1.feature`));
    expect(unwind1, "Missing Unwind1.feature").toBeTruthy();

    const scenario12 = unwind1!.scenarios.find(s => s.name.startsWith("[12] "));
    expect(scenario12, "Missing scenario [12]").toBeTruthy();

    expect(scenario12!.expectResult?.columns).toEqual(["a", "b2"]);
    expect(scenario12!.expectResult?.rows).toHaveLength(1);
  });
});

