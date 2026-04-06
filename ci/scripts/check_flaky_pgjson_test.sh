#!/usr/bin/env bash
# check_flaky_pgjson_test.sh
#
# Runs the pgjson explain.slt test multiple times to determine whether
# the failure at line 642 is flaky (non-deterministic) or consistent.
#
# Usage:
#   ./ci/scripts/check_flaky_pgjson_test.sh [ITERATIONS]
#
# Default: 100 iterations

set -euo pipefail

ITERATIONS="${1:-200}"
PASS=0
FAIL=0
RESULTS=()

# Locate workspace root (directory containing Cargo.toml with [workspace])
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

echo "=== Flakiness check for: explain format pgjson select * from values (1) ==="
echo "    File : datafusion/sqllogictest/test_files/explain.slt (line 642)"
echo "    Iterations: $ITERATIONS"
echo ""

for i in $(seq 1 "$ITERATIONS"); do
    # Run only the explain sqllogictest file.
    # The -- explain argument matches files whose path contains "explain".
    # Capture both stdout and stderr; use the exit code of cargo test directly.
    output=$(cargo test \
            --manifest-path "$REPO_ROOT/Cargo.toml" \
            -p datafusion-sqllogictest \
            --test sqllogictests \
            -- explain \
            2>&1) && cargo_exit=0 || cargo_exit=$?

    if [[ $cargo_exit -eq 0 ]]; then
        STATUS="PASS"
        PASS=$((PASS + 1))
        RESULTS+=("  Run $i: $STATUS")
        echo "  Run $i: $STATUS"
    else
        STATUS="FAIL"
        FAIL=$((FAIL + 1))
        RESULTS+=("  Run $i: $STATUS")
        echo "  Run $i: $STATUS"

        echo ""
        echo "--- First failure output ---"
        echo "$output" | tail -40
        echo "----------------------------"
        echo ""

        echo "Stopping on first failure."
        echo ""
        echo "=== Results ==="
        echo "  Passed : $PASS / $i"
        echo "  Failed : $FAIL / $i"
        exit 1
    fi
done

echo ""
echo "=== Results ==="
echo "  Passed : $PASS / $ITERATIONS"
echo "  Failed : $FAIL / $ITERATIONS"
echo ""

if [[ $PASS -gt 0 && $FAIL -gt 0 ]]; then
    echo "VERDICT: FLAKY — test result was non-deterministic ($PASS pass, $FAIL fail)"
    exit 0
elif [[ $FAIL -eq "$ITERATIONS" ]]; then
    echo "VERDICT: CONSISTENTLY FAILING — not flaky, this is a deterministic bug"
    echo ""
    echo "Likely cause: serde_json is compiled without the 'preserve_order' feature,"
    echo "so JSON object keys are sorted alphabetically (BTreeMap) instead of"
    echo "preserving insertion order (IndexMap). The test expects insertion order:"
    echo "  Node Type → Values → Plans → Output"
    echo "but always gets alphabetical order:"
    echo "  Node Type → Output → Plans → Values"
    exit 1
else
    echo "VERDICT: CONSISTENTLY PASSING — test is healthy"
    exit 0
fi
