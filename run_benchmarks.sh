#!/usr/bin/env bash
# Benchmark driver for the batch-normalizer branch.
#
# tpch / tpch_mem use the criterion "sql" bench harness: run config-off as a
# saved baseline, then config-on compared against it (same binary, env flip).
# clickbench_partitioned / tpcds use dfbench JSON results: bn-off vs bn-on
# from the branch binary, plus a main-binary baseline for clickbench.
set -e

export DATA_DIR="$HOME/GitHub/datafusion/benchmarks/data"
MAIN_DIR="$HOME/GitHub/datafusion"
BRANCH_DIR="/tmp/datafusion-batch-normalizer"
TARGET_BYTES=16777216  # 16MiB soft target
ON_ENV="DATAFUSION_EXECUTION_TARGET_BATCH_SIZE_BYTES=$TARGET_BYTES"

echo "=== prebuild binaries ==="
(cd "$BRANCH_DIR" && cargo build --release -p datafusion-benchmarks --bin dfbench 2>&1 | tail -1)
(cd "$MAIN_DIR" && cargo build --release -p datafusion-benchmarks --bin dfbench 2>&1 | tail -1)

# --- criterion suites: tpch, tpch_mem (branch binary, off-baseline vs on) ---
criterion_suite() {
    local suite=$1
    echo "=== $suite: branch, normalizer OFF (save baseline) ==="
    (cd "$BRANCH_DIR" && SQL_CARGO_COMMAND="cargo bench --bench sql -- --save-baseline bnoff" \
        ./benchmarks/bench.sh run "$suite")
    echo "=== $suite: branch, normalizer ON vs baseline ==="
    (cd "$BRANCH_DIR" && env $ON_ENV \
        SQL_CARGO_COMMAND="cargo bench --bench sql -- --baseline bnoff" \
        ./benchmarks/bench.sh run "$suite")
}

# --- dfbench suites (JSON results) ---
dfbench_branch() {
    local suite=$1
    echo "=== $suite: branch, normalizer OFF ==="
    (cd "$BRANCH_DIR" && RESULTS_NAME=bn-off ./benchmarks/bench.sh run "$suite")
    echo "=== $suite: branch, normalizer ON ==="
    (cd "$BRANCH_DIR" && env $ON_ENV RESULTS_NAME=bn-on ./benchmarks/bench.sh run "$suite")
}

criterion_suite tpch
criterion_suite tpch_mem

echo "=== clickbench_partitioned: main baseline ==="
(cd "$MAIN_DIR" && RESULTS_NAME=bn-main ./benchmarks/bench.sh run clickbench_partitioned)
dfbench_branch clickbench_partitioned
dfbench_branch tpcds

echo "=== ALL BENCHMARKS DONE ==="
