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

echo "Running SQL Logic Tests (Project 0)..."

TESTS=(
    "test/sql/p0.01-lower-upper.slt"
    "test/sql/p0.02-function-error.slt"
    "test/sql/p0.03-string-scan.slt"
)

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
    echo "All Project 0 tests passed!"
else
    echo "The following tests failed:"
    for FAILED in "${FAILED_TESTS[@]}"; do
        echo "  - $FAILED"
    done
    exit 1
fi
