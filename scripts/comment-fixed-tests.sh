#!/bin/bash
# Automatically comment out tests in FAILING_TESTS that are now passing
# Usage: ./scripts/comment-fixed-tests.sh

FAILING_TESTS_FILE="test/tck/failing-tests.ts"

# Run the check-fixed command and capture output
echo "Running tck:check-fixed to find passing tests..."
OUTPUT=$(npm run tck:check-fixed 2>&1)

# Check if any tests were found to be passing
if ! echo "$OUTPUT" | grep -q "Tests from FAILING_TESTS that now PASS"; then
    echo "No newly passing tests found."
    exit 0
fi

# Extract test names from lines like:
#    // "expressions/literals > Literals6 - String|10",
# We need to get the part between quotes
TESTS=$(echo "$OUTPUT" | grep '^\s*// "' | sed 's/.*"\(.*\)".*/\1/')

if [ -z "$TESTS" ]; then
    echo "No test names could be extracted."
    exit 0
fi

COUNT=0
while IFS= read -r test; do
    [ -z "$test" ] && continue
    
    # Escape special regex characters in test name for sed
    ESCAPED_TEST=$(echo "$test" | sed 's/[[\.*^$()+?{|]/\\&/g')
    
    # Check if this test exists as an uncommented entry in the file
    # Pattern: starts with whitespace, then a quote (not preceded by //)
    if grep -q "^[[:space:]]*\"${ESCAPED_TEST}\"" "$FAILING_TESTS_FILE"; then
        # Comment out the line by adding // before the quote
        sed -i "s|^\([[:space:]]*\)\(\"${ESCAPED_TEST}\"\)|\\1// \\2|" "$FAILING_TESTS_FILE"
        echo "Commented out: $test"
        ((COUNT++))
    else
        echo "Skipped (already commented or not found): $test"
    fi
done <<< "$TESTS"

echo ""
echo "Done! Commented out $COUNT test(s)."
