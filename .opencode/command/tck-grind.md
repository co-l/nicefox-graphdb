---
description: Grind TCK compliance by a delta percentage
---

Improve openCypher TCK compliance for NiceFox GraphDB by {{delta}}% (default: 5%).

## State Files
- TDD_CYPHER_COMPLIANCE.md - Current session status, priorities, and skipped categories
- TCK_COMPLIANCE_PLAN.md - Detailed progress tracking

## Loop Workflow

### 1. Measure Baseline
- Enable TCK: `mv packages/server/test/tck/tck.test.ts.skip packages/server/test/tck/tck.test.ts`
- Run `pnpm test -- --run` and calculate baseline pass rate
- Set target = min(baseline + {{delta}}%, 100%)
- If already at 100%, stop and report success

### 2. Fix Cycle (repeat until target reached)

#### 2a. Analyze & Pick Target
- Run: `pnpm test -- --run 2>&1 | grep "Query failed:" | sed 's/Query:.*//' | sort | uniq -c | sort -rn | head -10`
- Read "Skipped Categories" in TDD_CYPHER_COMPLIANCE.md, avoid those
- Pick highest-impact category from "Current Priority Fixes"

#### 2b. Fix Using TDD
- Write minimal unit test in appropriate test file
- Implement the fix
- Verify unit test passes with `pnpm test -- --run packages/server/test/<file>.test.ts`

#### 2c. Verify Improvement
- Re-run full TCK, calculate new pass rate
- If unit tests fail: `git checkout .` to revert, add category to "Skipped Categories" in TDD_CYPHER_COMPLIANCE.md with reason, pick different category
- If compliance decreased: `git checkout .` to revert, add category to "Skipped Categories" with reason, pick different category
- If same error persists after 2 attempts: `git checkout .` to revert, add to "Skipped Categories" with reason, pick next

#### 2d. Document & Commit
- Update TDD_CYPHER_COMPLIANCE.md with new stats and priorities
- Rename TCK back: `mv packages/server/test/tck/tck.test.ts packages/server/test/tck/tck.test.ts.skip`
- Commit: `git add -A && git commit -m "fix(tck): <category> - now at X% compliance"`
- Re-enable TCK for next cycle

#### 2e. Loop Check
- If target reached: stop
- If 5 cycles completed: stop
- If no more categories to try (all skipped): stop
- Otherwise: go to 2a

## Stopping Conditions
- Target compliance reached (baseline + delta, capped at 100%)
- 5 fix cycles completed
- A fix breaks existing unit tests (revert, document skip, continue)
- Compliance decreases (revert, document skip, continue)
- Same error category fails twice (document skip, continue)
- No remaining categories to try

## Skipped Categories Format
When adding to "Skipped Categories" section in TDD_CYPHER_COMPLIANCE.md, use:
```
## Skipped Categories
| Category | Reason | Date |
|----------|--------|------|
| <name> | <why it failed: broke tests / decreased compliance / stuck> | YYYY-MM-DD |
```

## Rules
- Always TDD: failing test first
- One commit per fix category
- Keep unit tests passing at all times
- Update markdown files every cycle
- Never skip documenting a failed attempt
