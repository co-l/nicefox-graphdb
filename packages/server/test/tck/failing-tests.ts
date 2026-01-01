export const FAILING_TESTS = new Set([
  // Create1|12: Multiple labels (:A:B:C) - now works
  // "clauses/create > Create1 - Creating nodes|12",
  // Delete4|1: Now works with aggregate detection in buildReturnResults
  // "clauses/delete > Delete4 - Delete clause interoperation with other clauses|1",
  // Delete4|2: Now works with undirected variable-length CTE fix
  // "clauses/delete > Delete4 - Delete clause interoperation with other clauses|2",
  // "clauses/delete > Delete5 - Delete clause interoperation with built-in data types|1",
  // "clauses/delete > Delete5 - Delete clause interoperation with built-in data types|2",
  // "clauses/delete > Delete5 - Delete clause interoperation with built-in data types|3",
  // "clauses/delete > Delete5 - Delete clause interoperation with built-in data types|4",
  // "clauses/delete > Delete5 - Delete clause interoperation with built-in data types|5",
  // "clauses/delete > Delete5 - Delete clause interoperation with built-in data types|6",
  // "clauses/delete > Delete5 - Delete clause interoperation with built-in data types|7",
  // Delete6|1,2,3,4,8,9,10,11: Now work with SKIP/LIMIT/literal fix
  // "clauses/delete > Delete6 - Persistence of delete clause side effects|1",
  // "clauses/delete > Delete6 - Persistence of delete clause side effects|2",
  // "clauses/delete > Delete6 - Persistence of delete clause side effects|3",
  // "clauses/delete > Delete6 - Persistence of delete clause side effects|4",
  // Delete6|5: Complex WITH + DELETE + WITH + WHERE pattern (needs chained property alias handling)
  // "clauses/delete > Delete6 - Persistence of delete clause side effects|5",
  // Delete6|6,7: Complex WITH + WHERE patterns
  // "clauses/delete > Delete6 - Persistence of delete clause side effects|6",
  // "clauses/delete > Delete6 - Persistence of delete clause side effects|7",
  // "clauses/delete > Delete6 - Persistence of delete clause side effects|8",
  // "clauses/delete > Delete6 - Persistence of delete clause side effects|9",
  // "clauses/delete > Delete6 - Persistence of delete clause side effects|10",
  // "clauses/delete > Delete6 - Persistence of delete clause side effects|11",
  // Delete6|12: Now works with chained property alias WHERE filtering
  // "clauses/delete > Delete6 - Persistence of delete clause side effects|12",
  // Delete6|13,14: Complex ORDER BY patterns
  // "clauses/delete > Delete6 - Persistence of delete clause side effects|13",
  // "clauses/delete > Delete6 - Persistence of delete clause side effects|14",
  // Delete4|2: Now works with undirected variable-length CTE fix
  // "clauses/delete > Delete4 - Delete clause interoperation with other clauses|2",
  // Create3|5,6: Now work with WITH alias handling for CREATE
  // "clauses/create > Create3 - Interoperation with other clauses|5",
  // "clauses/create > Create3 - Interoperation with other clauses|6",
  // Create3|7,8: Now work with chained alias support
  // "clauses/create > Create3 - Interoperation with other clauses|7",
  // "clauses/create > Create3 - Interoperation with other clauses|8",
  // "clauses/create > Create3 - Interoperation with other clauses|12",
  // Create6|1,2: Now work with SKIP/LIMIT fix in tryCreateReturnExecution
  // "clauses/create > Create6 - Persistence of create clause side effects|1",
  // "clauses/create > Create6 - Persistence of create clause side effects|2",
  // Create6|3,4,5: Now work with SKIP/LIMIT and WITH filtering in UNWIND+CREATE
  // "clauses/create > Create6 - Persistence of create clause side effects|3",
  // "clauses/create > Create6 - Persistence of create clause side effects|4",
  // "clauses/create > Create6 - Persistence of create clause side effects|5",
  // Create6|6: Now works with aggregate function support in UNWIND+CREATE
  // "clauses/create > Create6 - Persistence of create clause side effects|6",
  // Create6|7: Complex UNWIND+CREATE+WITH patterns (WITH aggregation) - Now works
  // "clauses/create > Create6 - Persistence of create clause side effects|7",
  // Create6|8,9: Now work with SKIP/LIMIT fix in tryCreateReturnExecution
  // "clauses/create > Create6 - Persistence of create clause side effects|8",
  // "clauses/create > Create6 - Persistence of create clause side effects|9",
  // Create6|10-13: Now work with edge property access fix
  // "clauses/create > Create6 - Persistence of create clause side effects|10",
  // "clauses/create > Create6 - Persistence of create clause side effects|11",
  // "clauses/create > Create6 - Persistence of create clause side effects|12",
  // "clauses/create > Create6 - Persistence of create clause side effects|13",
  // Create6|14: WITH aggregation pattern - needs special handling
  // "clauses/create > Create6 - Persistence of create clause side effects|14",




  // Match3|15,16: Now work with relationship uniqueness constraint
  // "clauses/match > Match3 - Match fixed length patterns|15",
  // "clauses/match > Match3 - Match fixed length patterns|16",
  // Match3|17,18,22: Now work
  // "clauses/match > Match3 - Match fixed length patterns|17",
  // "clauses/match > Match3 - Match fixed length patterns|18",
  // "clauses/match > Match3 - Match fixed length patterns|22",
  // Match3|25: Now works with additional label constraints fix
  // "clauses/match > Match3 - Match fixed length patterns|25",
  // Match3|28: NULL variable from OPTIONAL MATCH propagation - Now works
  // "clauses/match > Match3 - Match fixed length patterns|28",
  // Match4|1,2,6,9: Now work with variable-length edge tracking in CTE
  // "clauses/match > Match4 - Match variable length patterns scenarios|1",
  // "clauses/match > Match4 - Match variable length patterns scenarios|2", // was never in list, already passed
  // Match4|3: Now works - fixed edge type case sensitivity in expectLabelOrType()
  // "clauses/match > Match4 - Match variable length patterns scenarios|3",
  "clauses/match > Match4 - Match variable length patterns scenarios|4",
  // "clauses/match > Match4 - Match variable length patterns scenarios|5",
  // "clauses/match > Match4 - Match variable length patterns scenarios|6",
  // Match4|7: Pre-existing failure - path3.id reference bug
  "clauses/match > Match4 - Match variable length patterns scenarios|7",
  "clauses/match > Match4 - Match variable length patterns scenarios|8",
  // Match5|1,4,5,7,9,10,14,15,17,18,22,24: Now work with Background parsing support
  // "clauses/match > Match5 - Match variable length patterns over given graphs scenarios|1",
  // Match5|2: Now works with *.. parsing fix
  // "clauses/match > Match5 - Match variable length patterns over given graphs scenarios|2",
  // Match5|3: Now works with *0 zero-length path handling
  // "clauses/match > Match5 - Match variable length patterns over given graphs scenarios|3",
  // "clauses/match > Match5 - Match variable length patterns over given graphs scenarios|4",
  // "clauses/match > Match5 - Match variable length patterns over given graphs scenarios|5",
  // Match5|6: Now works with minHops=0 handling
  // "clauses/match > Match5 - Match variable length patterns over given graphs scenarios|6",
  // "clauses/match > Match5 - Match variable length patterns over given graphs scenarios|7",
  // Match5|8: Now works with *0..0 handling
  // "clauses/match > Match5 - Match variable length patterns over given graphs scenarios|8",
  // "clauses/match > Match5 - Match variable length patterns over given graphs scenarios|9",
  // "clauses/match > Match5 - Match variable length patterns over given graphs scenarios|10",
  // Match5|12,13: Now work with empty interval handling
  // "clauses/match > Match5 - Match variable length patterns over given graphs scenarios|12",
  // "clauses/match > Match5 - Match variable length patterns over given graphs scenarios|13",
  // "clauses/match > Match5 - Match variable length patterns over given graphs scenarios|14",
  // "clauses/match > Match5 - Match variable length patterns over given graphs scenarios|15",
  // Match5|16: Now works with *0.. handling
  // "clauses/match > Match5 - Match variable length patterns over given graphs scenarios|16",
  // "clauses/match > Match5 - Match variable length patterns over given graphs scenarios|17",
  // "clauses/match > Match5 - Match variable length patterns over given graphs scenarios|18",
  // Match5|19,20,21,22,23,24: Now work with fixed patterns after variable-length
  // "clauses/match > Match5 - Match variable length patterns over given graphs scenarios|19",
  // "clauses/match > Match5 - Match variable length patterns over given graphs scenarios|20",
  // "clauses/match > Match5 - Match variable length patterns over given graphs scenarios|21",
  // "clauses/match > Match5 - Match variable length patterns over given graphs scenarios|22",
  // "clauses/match > Match5 - Match variable length patterns over given graphs scenarios|23",
  // "clauses/match > Match5 - Match variable length patterns over given graphs scenarios|24",
  "clauses/match > Match5 - Match variable length patterns over given graphs scenarios|25",
  "clauses/match > Match5 - Match variable length patterns over given graphs scenarios|26",
  "clauses/match > Match5 - Match variable length patterns over given graphs scenarios|27",
  "clauses/match > Match5 - Match variable length patterns over given graphs scenarios|28",
  "clauses/match > Match5 - Match variable length patterns over given graphs scenarios|29",
  // Match6|1,2,3,5,6,7,10,11,12,16,18,19: Now work with named path support
  // "clauses/match > Match6 - Match named paths scenarios|1",
  // "clauses/match > Match6 - Match named paths scenarios|2",
  // "clauses/match > Match6 - Match named paths scenarios|3",
  // "clauses/match > Match6 - Match named paths scenarios|5",
  // "clauses/match > Match6 - Match named paths scenarios|6",
  // "clauses/match > Match6 - Match named paths scenarios|7",
  // Match6|9: Now works with path chaining fix and synthetic anonymous variable
  // "clauses/match > Match6 - Match named paths scenarios|9",
  // "clauses/match > Match6 - Match named paths scenarios|10",
  // "clauses/match > Match6 - Match named paths scenarios|11",
  // "clauses/match > Match6 - Match named paths scenarios|12",
  "clauses/match > Match6 - Match named paths scenarios|13",
  "clauses/match > Match6 - Match named paths scenarios|14",
  // Match6|15: Now works
  // "clauses/match > Match6 - Match named paths scenarios|15",
  // "clauses/match > Match6 - Match named paths scenarios|16",
  "clauses/match > Match6 - Match named paths scenarios|17",
  // "clauses/match > Match6 - Match named paths scenarios|18",
  // "clauses/match > Match6 - Match named paths scenarios|19",
  // Match6|20: Now works
  // "clauses/match > Match6 - Match named paths scenarios|20",
  "clauses/match > Match7 - Optional match|3",
  // Match7|4,8: value mismatches
  "clauses/match > Match7 - Optional match|4",
  "clauses/match > Match7 - Optional match|8",
  "clauses/match > Match7 - Optional match|9",
  "clauses/match > Match7 - Optional match|10",
  "clauses/match > Match7 - Optional match|11",
  "clauses/match > Match7 - Optional match|12",

  // Match7|14,15: variable length optional patterns
  "clauses/match > Match7 - Optional match|14",
  "clauses/match > Match7 - Optional match|15",
  // Match7|16,18: value mismatches
  "clauses/match > Match7 - Optional match|16",
  "clauses/match > Match7 - Optional match|17",
  "clauses/match > Match7 - Optional match|18",
  "clauses/match > Match7 - Optional match|19",
  "clauses/match > Match7 - Optional match|20",
  "clauses/match > Match7 - Optional match|21",
  "clauses/match > Match7 - Optional match|22",
  "clauses/match > Match7 - Optional match|24",
  "clauses/match > Match7 - Optional match|25",
  "clauses/match > Match7 - Optional match|28",
  // Match7|29: Now works
  // "clauses/match > Match7 - Optional match|29",
  // Match7|30: Now works with IS NULL returning true/false
  // "clauses/match > Match7 - Optional match|30",
  "clauses/match > Match7 - Optional match|31",



  "clauses/match > Match8 - Match clause interoperation with other clauses|2",
  // Match8|3: value mismatch
  "clauses/match > Match8 - Match clause interoperation with other clauses|3",
  "clauses/match > Match9 - Match deprecated scenarios|1",
  // Match9|2: Now works with variable-length edge tracking
  // "clauses/match > Match9 - Match deprecated scenarios|2",
  "clauses/match > Match9 - Match deprecated scenarios|3",
  // Match9|4: Now works
  // "clauses/match > Match9 - Match deprecated scenarios|4",
  "clauses/match > Match9 - Match deprecated scenarios|5",
  "clauses/match > Match9 - Match deprecated scenarios|8",
  "clauses/match > Match9 - Match deprecated scenarios|9",
  // Merge1|8: Now works with WITH variable resolution in serializeProperties
  // "clauses/merge > Merge1 - Merge node|8",
  // Merge1|9, 11-14: Complex MERGE scenarios (multiple MERGE, paths, DELETE+MERGE)
  "clauses/merge > Merge1 - Merge node|9",
  // Merge1|10: Multiple labels - now works
  // "clauses/merge > Merge1 - Merge node|10",
  "clauses/merge > Merge1 - Merge node|11",
  "clauses/merge > Merge1 - Merge node|12",
  "clauses/merge > Merge1 - Merge node|13",
  "clauses/merge > Merge1 - Merge node|14",
  // Merge2|5: Uses property of bound node in ON CREATE (needs MATCH context)
  "clauses/merge > Merge2 - Merge node - on create|5",
  // Merge3|4: Uses property of bound node in ON MATCH (needs MATCH context)
  "clauses/merge > Merge3 - Merge node - on match|4",
  "clauses/merge > Merge4 - Merge node - on match and on create|1",
  "clauses/merge > Merge4 - Merge node - on match and on create|2",
  // Merge5|1,2: Now work with count() fix
  // "clauses/merge > Merge5 - Merge relationships|1",
  // "clauses/merge > Merge5 - Merge relationships|2",
  // Merge5|3: value mismatch
  "clauses/merge > Merge5 - Merge relationships|3",
  // Merge5|4: Now works with CREATE+MERGE handling fix
  // "clauses/merge > Merge5 - Merge relationships|4",
  // "clauses/merge > Merge5 - Merge relationships|5",
  // "clauses/merge > Merge5 - Merge relationships|6",
  // "clauses/merge > Merge5 - Merge relationships|7",
  // "clauses/merge > Merge5 - Merge relationships|8",
  "clauses/merge > Merge5 - Merge relationships|10",
  "clauses/merge > Merge5 - Merge relationships|11",
  // "clauses/merge > Merge5 - Merge relationships|12",
  "clauses/merge > Merge5 - Merge relationships|13",
  // Merge5|14: Now works
  // "clauses/merge > Merge5 - Merge relationships|14",
  "clauses/merge > Merge5 - Merge relationships|15",
  "clauses/merge > Merge5 - Merge relationships|16",
  "clauses/merge > Merge5 - Merge relationships|17",
  "clauses/merge > Merge5 - Merge relationships|18",
  "clauses/merge > Merge5 - Merge relationships|19",
  "clauses/merge > Merge5 - Merge relationships|20",
  "clauses/merge > Merge5 - Merge relationships|21",
  "clauses/merge > Merge5 - Merge relationships|29",
  // Merge6|1: Now works (empty result)
  // "clauses/merge > Merge6 - Merge relationships - on create|1",
  // Merge6|2: Now works with count() fix
  // "clauses/merge > Merge6 - Merge relationships - on create|2",
  // Merge6|3-6: Previously masked by multi-phase; has SQL bug with list comprehension r[key] on empty
  "clauses/merge > Merge6 - Merge relationships - on create|3",
  "clauses/merge > Merge6 - Merge relationships - on create|4",
  "clauses/merge > Merge6 - Merge relationships - on create|5",
  "clauses/merge > Merge6 - Merge relationships - on create|6",
  // Merge7|3: Now works
  // "clauses/merge > Merge7 - Merge relationships - on match|3",
  "clauses/merge > Merge7 - Merge relationships - on match|4",
  "clauses/merge > Merge7 - Merge relationships - on match|5",
  // Merge8|1: value mismatch
  "clauses/merge > Merge8 - Merge relationships - on match and on create|1",
  // Merge9|1,2: Now work with UNWIND+MERGE execution
  // "clauses/merge > Merge9 - Merge clause interoperation with other clauses|1",
  // "clauses/merge > Merge9 - Merge clause interoperation with other clauses|2",
  "clauses/merge > Merge9 - Merge clause interoperation with other clauses|3",
  "clauses/merge > Merge9 - Merge clause interoperation with other clauses|4",

  // Return2|9,10: Now work with 'expr' fallback column extraction
  // "clauses/return > Return2 - Return single expression (correctly projecting an expression)|9",
  // "clauses/return > Return2 - Return single expression (correctly projecting an expression)|10",
  // Return2|11: Now works (large integers)
  // "clauses/return > Return2 - Return single expression (correctly projecting an expression)|11",
  // Return2|12: Now works (list of nodes/relationships)
  // "clauses/return > Return2 - Return single expression (correctly projecting an expression)|12",
  // Return2|13: Now works with map pattern matching
  // "clauses/return > Return2 - Return single expression (correctly projecting an expression)|13",
  // Return2|14: type of deleted relationship - needs DETACH DELETE return
  "clauses/return > Return2 - Return single expression (correctly projecting an expression)|14",

  // Return4|4: Now works
  // "clauses/return > Return4 - Column renaming|4",
  // Return4|6: aggregation in expression (needs path variable)
  "clauses/return > Return4 - Column renaming|6",
  // Return4|8: Now works (column renaming for aggregations - uses MATCH () count)
  // "clauses/return > Return4 - Column renaming|8",
  // Return4|9: Now works with map pattern matching
  // "clauses/return > Return4 - Column renaming|9",
  // Return4|11: list comprehension
  "clauses/return > Return4 - Column renaming|11",

  // With5|2: value mismatch
  "clauses/with > With5 - Implicit grouping with DISTINCT|2",

  // Return5|4: Now works with 'expr' fallback
  // "clauses/return > Return5 - Implicit grouping with distinct|4",
  // Return5|5: Now works (distinct on list values)
  // "clauses/return > Return5 - Implicit grouping with distinct|5",
  // Return6|1,3,5,7,8,9,10,11,12,14,15,17,18,19,20,21: Now work
  // Return6|2: Now works with 'expr' fallback
  // "clauses/return > Return6 - Implicit grouping with aggregates|2",
  // Return6|4: Now works with integer literal inlining in SQL
  // "clauses/return > Return6 - Implicit grouping with aggregates|4",
  // Return6|6: Previously masked by multi-phase execution; has SQL bug with aggregates in map on empty results
  "clauses/return > Return6 - Implicit grouping with aggregates|6",
  // Return6|13: aggregate in GROUP BY - needs WITH/aggregate variable scoping
  "clauses/return > Return6 - Implicit grouping with aggregates|13",
  // Return6|16: aggregate function misuse - WITH variables as aggregate args
  "clauses/return > Return6 - Implicit grouping with aggregates|16",


  // Set1|5: needs list comprehension [i IN list | expr]
  "clauses/set > Set1 - Set a Property|5",
  // Set1|6,7: Now work (list concatenation)
  // "clauses/set > Set1 - Set a Property|6",
  // "clauses/set > Set1 - Set a Property|7",
  // Set1|10: expects TypeError for nested map in list, we don't validate types the same way
  "clauses/set > Set1 - Set a Property|10",
  // Set2|1-3: Now work with property removal on null
  // "clauses/set > Set2 - Set a Property to Null|1",
  // "clauses/set > Set2 - Set a Property to Null|2",
  // "clauses/set > Set2 - Set a Property to Null|3",
  // Set3|1-7: Testing now
  // "clauses/set > Set3 - Set a Label|1",
  // "clauses/set > Set3 - Set a Label|2",
  // "clauses/set > Set3 - Set a Label|3",
  // "clauses/set > Set3 - Set a Label|4",
  // "clauses/set > Set3 - Set a Label|5",
  // "clauses/set > Set3 - Set a Label|6",
  // "clauses/set > Set3 - Set a Label|7",
  // Set4|1-4: Now work
  // "clauses/set > Set4 - Set all properties with a map|1",
  // "clauses/set > Set4 - Set all properties with a map|2",
  // "clauses/set > Set4 - Set all properties with a map|3",
  // "clauses/set > Set4 - Set all properties with a map|4",
  // Set5|2-4: Now work
  // "clauses/set > Set5 - Set multiple properties with a map|2",
  // "clauses/set > Set5 - Set multiple properties with a map|3",
  // "clauses/set > Set5 - Set multiple properties with a map|4",
  // Set6|1,2,3: Now work with SKIP/LIMIT fix in buildReturnResults
  // "clauses/set > Set6 - Persistence of set clause side effects|1",
  // "clauses/set > Set6 - Persistence of set clause side effects|2",
  // "clauses/set > Set6 - Persistence of set clause side effects|3",
  // Set6|4: Now works
  // "clauses/set > Set6 - Persistence of set clause side effects|4",
  // Set6|5,6,7: Complex WITH + WHERE patterns
  "clauses/set > Set6 - Persistence of set clause side effects|5",
  "clauses/set > Set6 - Persistence of set clause side effects|6",
  "clauses/set > Set6 - Persistence of set clause side effects|7",
  // Set6|8,9,10: Now work with SKIP/LIMIT fix
  // "clauses/set > Set6 - Persistence of set clause side effects|8",
  // "clauses/set > Set6 - Persistence of set clause side effects|9",
  // "clauses/set > Set6 - Persistence of set clause side effects|10",
  // Set6|11: Now works
  // "clauses/set > Set6 - Persistence of set clause side effects|11",
  // Set6|12: Now works with SKIP/LIMIT fix
  // "clauses/set > Set6 - Persistence of set clause side effects|12",
  // Set6|13: Complex WITH + ORDER BY pattern
  "clauses/set > Set6 - Persistence of set clause side effects|13",
  // Set6|14,15,16,17: Now work with SKIP/LIMIT fix
  // "clauses/set > Set6 - Persistence of set clause side effects|14",
  // "clauses/set > Set6 - Persistence of set clause side effects|15",
  // "clauses/set > Set6 - Persistence of set clause side effects|16",
  // "clauses/set > Set6 - Persistence of set clause side effects|17",
  // Set6|18: Now works
  // "clauses/set > Set6 - Persistence of set clause side effects|18",
  // Set6|19,20,21: Complex WHERE/ORDER BY patterns
  "clauses/set > Set6 - Persistence of set clause side effects|19",
  "clauses/set > Set6 - Persistence of set clause side effects|20",
  "clauses/set > Set6 - Persistence of set clause side effects|21",
  // With1|3: Relationship variable aliasing in WITH then re-matching
  "clauses/with > With1 - Forward single variable|3",
  // With2|1,2: Complex expression forwarding with joins and nested maps
  "clauses/with > With2 - Forward single expression|1",
  "clauses/with > With2 - Forward single expression|2",
  // Unwind1|4,5,6 need WITH + collect + UNWIND chains
  "clauses/unwind > Unwind1|4",
  "clauses/unwind > Unwind1|5",
  "clauses/unwind > Unwind1|6",
  "clauses/unwind > Unwind1|12",
  "clauses/unwind > Unwind1|14",




  // With3|1: Forwarding multiple node/relationship variables and re-matching
  "clauses/with > With3 - Forward multiple expressions|1",
  "clauses/with > With4 - Variable aliasing|6",
  // With4|7: value mismatch
  "clauses/with > With4 - Variable aliasing|7",


  // With6|2-4: Now work
  // "clauses/with > With6 - Implicit grouping with aggregates|2",
  // "clauses/with > With6 - Implicit grouping with aggregates|3",
  // "clauses/with > With6 - Implicit grouping with aggregates|4",
  // With6|5-7: Now work
  // "clauses/with > With6 - Implicit grouping with aggregates|5",
  // "clauses/with > With6 - Implicit grouping with aggregates|6",
  // "clauses/with > With6 - Implicit grouping with aggregates|7",
  "clauses/with > With7 - WITH on WITH|1",
  // With7|2: value mismatch
  "clauses/with > With7 - WITH on WITH|2",
  // Aggregation1|1: Now works with GROUP BY fix
  // Aggregation2|9,10: Now work
  // "expressions/aggregation > Aggregation2 - Min and Max|9",
  // "expressions/aggregation > Aggregation2 - Min and Max|10",
  // Aggregation2|11,12: value mismatches
  "expressions/aggregation > Aggregation2 - Min and Max|11",
  "expressions/aggregation > Aggregation2 - Min and Max|12",
  // Aggregation3|2: value mismatch
  "expressions/aggregation > Aggregation3 - Sum|2",
  // Aggregation5|1-2: Now work with null filtering
  // "expressions/aggregation > Aggregation5 - Collect|1",
  // "expressions/aggregation > Aggregation5 - Collect|2",
  "expressions/aggregation > Aggregation6 - Percentiles|5",
  // Aggregation8|1 and |2 now work with DISTINCT count
  // Aggregation8|3 and |4 now work with null filtering in collect(DISTINCT)
  // "expressions/aggregation > Aggregation8 - DISTINCT|3",
  // "expressions/aggregation > Aggregation8 - DISTINCT|4",
]);
