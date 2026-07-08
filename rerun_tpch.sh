#!/usr/bin/env bash
# tpch-only rerun (rounds 3,4) after the polluted rounds 1,2; overwrites nothing.
set -e
BRANCH_DIR="/tmp/datafusion-batch-normalizer"
DATA="$HOME/GitHub/datafusion/benchmarks/data"
BIN="$BRANCH_DIR/target/release/dfbench"
OUT="$BRANCH_DIR/ab_results"
rm -f "$OUT"/tpch_*_r1.json "$OUT"/tpch_*_r2.json
for round in 3 4; do
  for cfg in off on; do
    envargs=()
    [ "$cfg" = on ] && envargs=(DATAFUSION_EXECUTION_TARGET_BATCH_SIZE_BYTES=16777216)
    echo ">>> tpch cfg=$cfg round=$round"
    env "${envargs[@]}" "$BIN" tpch --iterations 3 \
      --path "$DATA/tpch_sf1" --format parquet -o "$OUT/tpch_${cfg}_r${round}.json" > /dev/null
  done
done
echo "TPCH RERUN DONE"
