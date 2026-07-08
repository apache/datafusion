#!/usr/bin/env bash
# Interleaved A/B benchmark: normalizer off vs on, same branch binary.
# Two rounds of off->on per suite; compare per-query minimums across rounds
# to be robust against environment drift (the failure mode of run 1).
set -e

BRANCH_DIR="/tmp/datafusion-batch-normalizer"
DATA="$HOME/GitHub/datafusion/benchmarks/data"
QUERIES="$HOME/GitHub/datafusion/benchmarks/queries"
TPCDS_QUERIES="$HOME/GitHub/datafusion/datafusion/core/tests/tpc-ds"
BIN="$BRANCH_DIR/target/release/dfbench"
OUT="$BRANCH_DIR/ab_results"
TARGET_BYTES=16777216

mkdir -p "$OUT"
cd "$BRANCH_DIR"

run_one() {
    local suite=$1 cfg=$2 round=$3
    local envargs=()
    if [ "$cfg" = on ]; then
        envargs=(DATAFUSION_EXECUTION_TARGET_BATCH_SIZE_BYTES=$TARGET_BYTES)
    fi
    local out="$OUT/${suite}_${cfg}_r${round}.json"
    echo ">>> $suite cfg=$cfg round=$round"
    case $suite in
        tpch)
            env "${envargs[@]}" "$BIN" tpch --iterations 3 \
                --path "$DATA/tpch_sf1" --format parquet -o "$out" > /dev/null
            ;;
        clickbench)
            env "${envargs[@]}" "$BIN" clickbench --iterations 3 \
                --path "$DATA/hits_partitioned" \
                --queries-path "$QUERIES/clickbench/queries" -o "$out" > /dev/null
            ;;
        tpcds)
            env "${envargs[@]}" "$BIN" tpcds --iterations 3 \
                --path "$DATA/tpcds_sf1" --query_path "$TPCDS_QUERIES" \
                --prefer_hash_join true -o "$out" > /dev/null
            ;;
    esac
}

for suite in tpch clickbench tpcds; do
    for round in 1 2; do
        for cfg in off on; do
            run_one "$suite" "$cfg" "$round"
        done
    done
done

echo "AB BENCHMARKS DONE"
