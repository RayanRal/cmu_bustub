#!/bin/bash

# Exit on error
set -e

# Number of CPU cores for parallel build
if [[ "$OSTYPE" == "darwin"* ]]; then
    NPROC=$(sysctl -n hw.ncpu)
else
    NPROC=$(nproc)
fi

echo "Building project..."
cd build
make -j$NPROC sqllogictest

echo "Running SQL Logic Tests..."

TESTS=(
    "test/sql/p3.00-primer.slt"
    "test/sql/p3.01-seqscan.slt"
    "test/sql/p3.02-insert.slt"
    "test/sql/p3.03-update.slt"
    "test/sql/p3.04-delete.slt"
    "test/sql/p3.05-index-scan-btree.slt"
    "test/sql/p3.06-empty-table.slt"
)

# Move back to root to use relative paths correctly if needed, 
# but sqllogictest usually expects paths relative to CWD or absolute.
# We'll run from build dir.

FAILED_TESTS=()

for TEST in "${TESTS[@]}"; do
    echo "----------------------------------------------------------------------"
    echo "Running $TEST..."
    # Using set +e to continue even if a test fails
    set +e
    ./bin/bustub-sqllogictest ../$TEST
    if [ $? -ne 0 ]; then
        FAILED_TESTS+=("$TEST")
    fi
    set -e
done

echo "----------------------------------------------------------------------"
if [ ${#FAILED_TESTS[@]} -eq 0 ]; then
    echo "All tests passed!"
else
    echo "The following tests failed:"
    for FAILED in "${FAILED_TESTS[@]}"; do
        echo "  - $FAILED"
    done
    exit 1
fi
