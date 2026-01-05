/**
 * Tests for TCK utility functions
 * 
 * These tests ensure that the value matching logic correctly validates
 * node patterns, relationship patterns, and other TCK expected values.
 */

import { describe, it, expect } from "vitest";
import { valuesMatch, extractColumns, rowsMatch, isNullEntity } from "./tck-utils";

describe("valuesMatch", () => {
  describe("node patterns", () => {
    it("should pass when property value matches", () => {
      const expected = { _nodePattern: "({name: 'bar'})" };
      const actual = { name: "bar", _nf_id: "123" };
      expect(valuesMatch(expected, actual)).toBe(true);
    });

    it("should fail when property value is wrong", () => {
      const expected = { _nodePattern: "({name: 'bar'})" };
      const actual = { name: "WRONG", _nf_id: "123" };
      expect(valuesMatch(expected, actual)).toBe(false);
    });

    it("should fail when property is missing", () => {
      const expected = { _nodePattern: "({name: 'bar'})" };
      const actual = { other: "bar", _nf_id: "123" };
      expect(valuesMatch(expected, actual)).toBe(false);
    });

    it("should pass when multiple properties all match", () => {
      const expected = { _nodePattern: "({name: 'A', num: 0, id: 0})" };
      const actual = { name: "A", num: 0, id: 0, _nf_id: "123" };
      expect(valuesMatch(expected, actual)).toBe(true);
    });

    it("should fail when one of multiple properties is wrong", () => {
      const expected = { _nodePattern: "({name: 'A', num: 0, id: 0})" };
      const actual = { name: "A", num: 999, id: 0, _nf_id: "123" };
      expect(valuesMatch(expected, actual)).toBe(false);
    });

    it("should pass when array property matches", () => {
      const expected = { _nodePattern: "({numbers: [1, 2, 3]})" };
      const actual = { numbers: [1, 2, 3], _nf_id: "123" };
      expect(valuesMatch(expected, actual)).toBe(true);
    });

    it("should fail when array property has wrong values", () => {
      const expected = { _nodePattern: "({numbers: [1, 2, 3]})" };
      const actual = { numbers: [1, 2, 999], _nf_id: "123" };
      expect(valuesMatch(expected, actual)).toBe(false);
    });

    it("should pass for node pattern with label and properties", () => {
      const expected = { _nodePattern: "(:Person {name: 'Alice'})" };
      const actual = { name: "Alice", _nf_id: "123" };
      expect(valuesMatch(expected, actual)).toBe(true);
    });

    it("should fail for node pattern with label when property is wrong", () => {
      const expected = { _nodePattern: "(:Person {name: 'Alice'})" };
      const actual = { name: "Bob", _nf_id: "123" };
      expect(valuesMatch(expected, actual)).toBe(false);
    });

    it("should pass for node pattern without properties", () => {
      const expected = { _nodePattern: "(:Label)" };
      const actual = { _nf_id: "123" };
      expect(valuesMatch(expected, actual)).toBe(true);
    });

    it("should pass for empty node pattern", () => {
      const expected = { _nodePattern: "()" };
      const actual = { _nf_id: "123" };
      expect(valuesMatch(expected, actual)).toBe(true);
    });
  });

  describe("relationship patterns", () => {
    it("should pass when relationship type matches", () => {
      const expected = { _relPattern: "[:KNOWS]" };
      const actual = { type: "KNOWS", _nf_id: "123" };
      expect(valuesMatch(expected, actual)).toBe(true);
    });

    it("should fail when relationship type is wrong", () => {
      const expected = { _relPattern: "[:KNOWS]" };
      const actual = { type: "LIKES", _nf_id: "123" };
      expect(valuesMatch(expected, actual)).toBe(false);
    });

    it("should pass when relationship has matching properties", () => {
      const expected = { _relPattern: "[:REL {num: 1}]" };
      const actual = { type: "REL", num: 1, _nf_id: "123" };
      expect(valuesMatch(expected, actual)).toBe(true);
    });

    it("should fail when relationship property is wrong", () => {
      const expected = { _relPattern: "[:REL {num: 1}]" };
      const actual = { type: "REL", num: 999, _nf_id: "123" };
      expect(valuesMatch(expected, actual)).toBe(false);
    });

    it("should handle heterogeneous list patterns", () => {
      const expected = { _relPattern: "[(:A), [:T], (:B)]" };
      const actual = [{ _nf_id: "1" }, { _nf_id: "2" }, { _nf_id: "3" }];
      expect(valuesMatch(expected, actual)).toBe(true);
    });
  });

  describe("boolean handling", () => {
    it("should match boolean true with SQLite 1", () => {
      expect(valuesMatch(true, 1)).toBe(true);
    });

    it("should match boolean false with SQLite 0", () => {
      expect(valuesMatch(false, 0)).toBe(true);
    });

    it("should not match boolean true with SQLite 0", () => {
      expect(valuesMatch(true, 0)).toBe(false);
    });

    it("should not match boolean false with SQLite 1", () => {
      expect(valuesMatch(false, 1)).toBe(false);
    });

    it("should match boolean with boolean directly", () => {
      expect(valuesMatch(true, true)).toBe(true);
      expect(valuesMatch(false, false)).toBe(true);
    });
  });

  describe("null handling", () => {
    it("should match null with null", () => {
      expect(valuesMatch(null, null)).toBe(true);
    });

    it("should match null with undefined", () => {
      expect(valuesMatch(null, undefined)).toBe(true);
    });

    it("should not match null with a value", () => {
      expect(valuesMatch(null, "something")).toBe(false);
    });
  });

  describe("number handling", () => {
    it("should match integers exactly", () => {
      expect(valuesMatch(42, 42)).toBe(true);
    });

    it("should not match different integers", () => {
      expect(valuesMatch(42, 43)).toBe(false);
    });

    it("should match floats within tolerance", () => {
      expect(valuesMatch(0, -1.1e-15)).toBe(true);
    });

    it("should match floats with small differences", () => {
      expect(valuesMatch(3.14159, 3.14158)).toBe(true);
    });
  });

  describe("array handling", () => {
    it("should match identical arrays", () => {
      expect(valuesMatch([1, 2, 3], [1, 2, 3])).toBe(true);
    });

    it("should not match arrays with different lengths", () => {
      expect(valuesMatch([1, 2, 3], [1, 2])).toBe(false);
    });

    it("should not match arrays with different values", () => {
      expect(valuesMatch([1, 2, 3], [1, 2, 4])).toBe(false);
    });

    it("should match nested arrays", () => {
      expect(valuesMatch([[1, 2], [3, 4]], [[1, 2], [3, 4]])).toBe(true);
    });
  });

  describe("object handling", () => {
    it("should match identical objects", () => {
      expect(valuesMatch({ a: 1, b: 2 }, { a: 1, b: 2 })).toBe(true);
    });

    it("should not match objects with different values", () => {
      expect(valuesMatch({ a: 1, b: 2 }, { a: 1, b: 3 })).toBe(false);
    });

    it("should not match objects with different keys", () => {
      expect(valuesMatch({ a: 1, b: 2 }, { a: 1, c: 2 })).toBe(false);
    });
  });
});

describe("isNullEntity", () => {
  it("should return true for entity with null id", () => {
    expect(isNullEntity({ id: null, label: "Test" })).toBe(true);
  });

  it("should return false for entity with non-null id", () => {
    expect(isNullEntity({ id: "123", label: "Test" })).toBe(false);
  });

  it("should return false for null", () => {
    expect(isNullEntity(null)).toBe(false);
  });

  it("should return false for non-objects", () => {
    expect(isNullEntity("string")).toBe(false);
    expect(isNullEntity(123)).toBe(false);
  });
});

describe("rowsMatch", () => {
  it("should match identical rows in order", () => {
    const expected = [[1, "a"], [2, "b"]];
    const actual = [[1, "a"], [2, "b"]];
    expect(rowsMatch(expected, actual, true)).toBe(true);
  });

  it("should not match rows in wrong order when ordered", () => {
    const expected = [[1, "a"], [2, "b"]];
    const actual = [[2, "b"], [1, "a"]];
    expect(rowsMatch(expected, actual, true)).toBe(false);
  });

  it("should match rows in any order when unordered", () => {
    const expected = [[1, "a"], [2, "b"]];
    const actual = [[2, "b"], [1, "a"]];
    expect(rowsMatch(expected, actual, false)).toBe(true);
  });

  it("should not match different row counts", () => {
    const expected = [[1, "a"], [2, "b"]];
    const actual = [[1, "a"]];
    expect(rowsMatch(expected, actual, false)).toBe(false);
  });
});

describe("extractColumns", () => {
  it("should extract columns by name", () => {
    const row = { name: "Alice", age: 30 };
    expect(extractColumns(row, ["name", "age"])).toEqual(["Alice", 30]);
  });

  it("should handle dot notation via underscore", () => {
    const row = { n_name: "Alice" };
    expect(extractColumns(row, ["n.name"])).toEqual(["Alice"]);
  });

  it("should return undefined for missing columns", () => {
    const row = { name: "Alice" };
    expect(extractColumns(row, ["missing"])).toEqual([undefined]);
  });

  it("should convert null entities to null", () => {
    const row = { n: { id: null } };
    expect(extractColumns(row, ["n"])).toEqual([null]);
  });
});
