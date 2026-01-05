#!/bin/bash
# Comment out the first uncommented test in FAILING_TESTS
# Usage: ./scripts/comment-first-failing.sh

FAILING_TESTS_FILE="test/tck/failing-tests.ts"

# Find the first uncommented test line (starts with whitespace, then a quote)
LINE=$(grep -n '^[[:space:]]*"' "$FAILING_TESTS_FILE" | head -1)

if [ -z "$LINE" ]; then
    echo "No uncommented tests found in $FAILING_TESTS_FILE"
    exit 0
fi

# Extract line number and test name
LINE_NUM=$(echo "$LINE" | cut -d: -f1)
TEST_NAME=$(echo "$LINE" | sed 's/.*"\(.*\)".*/\1/')

# Comment out the line
sed -i "${LINE_NUM}s|^\([[:space:]]*\)\"|\\1// \"|" "$FAILING_TESTS_FILE"

echo "Commented out line $LINE_NUM: $TEST_NAME"
