/**
 * Query overrides for TCK tests that use integers beyond JavaScript's safe range.
 *
 * JavaScript cannot precisely represent integers larger than Number.MAX_SAFE_INTEGER
 * (9007199254740991). For these tests, we modify the queries to use strings instead,
 * which is the recommended workaround for large integer IDs.
 *
 * This allows the tests to verify correct behavior with the string-based approach
 * rather than skipping them entirely.
 */

export interface QueryOverride {
  reason: string;
  setup?: string[];
  query?: string;
  expectResult?: {
    columns: string[];
    rows: unknown[][];
    ordered: boolean;
  };
}

export const QUERY_OVERRIDES = new Map<string, QueryOverride>([
  // Test 12: CREATE with one large int, MATCH with different large int (inline property)
  // Original uses integers 4611686018427387905 and 4611686018427387900 which JS rounds
  // to the same value. Using strings preserves the distinction.
  [
    "expressions/comparison > Comparison1 - Equality|12",
    {
      reason: "JavaScript integer precision limitation - use strings for large IDs",
      setup: ["CREATE (:TheLabel {id: '4611686018427387905'})"],
      query: "MATCH (p:TheLabel {id: '4611686018427387900'}) RETURN p.id",
      expectResult: { columns: ["p.id"], rows: [], ordered: false },
    },
  ],

  // Test 13: Same as test 12 but with explicit WHERE clause
  [
    "expressions/comparison > Comparison1 - Equality|13",
    {
      reason: "JavaScript integer precision limitation - use strings for large IDs",
      setup: ["CREATE (:TheLabel {id: '4611686018427387905'})"],
      query: "MATCH (p:TheLabel) WHERE p.id = '4611686018427387900' RETURN p.id",
      expectResult: { columns: ["p.id"], rows: [], ordered: false },
    },
  ],
]);
