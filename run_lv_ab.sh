#!/usr/bin/env bash
# Interleaved A/B for the large_values benchmark: normalizer off vs on,
# unlimited memory and 2GB memory limit (fair pool + disk spill).
set -e

BRANCH_DIR="/tmp/datafusion-batch-normalizer"
BIN="$BRANCH_DIR/target/release/dfbench"
DATA="$HOME/GitHub/datafusion/benchmarks/data/large_values"
OUT="$BRANCH_DIR/ab_results"
TARGET_BYTES=16777216

mkdir -p "$OUT"

run_one() {
    local mode=$1 cfg=$2 round=$3
    local envargs=() limitargs=() iters=3
    [ "$cfg" = on ] && envargs=(DATAFUSION_EXECUTION_TARGET_BATCH_SIZE_BYTES=$TARGET_BYTES)
    if [ "$mode" = mem2g ]; then
        limitargs=(--memory-limit 2G)
        iters=2
    fi
    echo ">>> large_values mode=$mode cfg=$cfg round=$round"
    env "${envargs[@]}" "$BIN" large-values --iterations $iters \
        --path "$DATA" "${limitargs[@]}" \
        -o "$OUT/lv-${mode}_${cfg}_r${round}.json" 2>&1 | grep -E 'failed|avg'
}

for mode in unlimited mem2g; do
    for round in 1 2; do
        for cfg in off on; do
            run_one "$mode" "$cfg" "$round"
        done
    done
done

echo "LV AB DONE"
