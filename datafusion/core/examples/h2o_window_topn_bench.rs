// Standalone H2O groupby Q8 benchmark: PartitionedTopKExec enabled vs disabled
//
// Usage:
//   cargo run --release --example h2o_window_topn_bench
//
// Generates 10M rows in-memory (matching H2O SMALL), then runs Q8
// (ROW_NUMBER top-2 per partition) with the optimization on and off.

use datafusion::prelude::*;
use datafusion_common::instant::Instant;
use std::sync::Arc;

use arrow::array::{Int64Array, Float64Array, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema};
use datafusion::datasource::MemTable;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};

const NUM_ROWS: usize = 10_000_000; // 10M rows (H2O SMALL)
const BATCH_SIZE: usize = 100_000;
const ITERATIONS: usize = 3;

fn generate_data(num_partitions: i64) -> Arc<MemTable> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id6", DataType::Int64, false),
        Field::new("v3", DataType::Float64, true),
    ]));

    let mut rng = StdRng::seed_from_u64(42);
    let mut batches = Vec::new();
    let mut remaining = NUM_ROWS;

    while remaining > 0 {
        let batch_len = remaining.min(BATCH_SIZE);
        remaining -= batch_len;

        let id6: Int64Array = (0..batch_len)
            .map(|_| rng.random_range(0..num_partitions))
            .collect();

        let v3: Float64Array = (0..batch_len)
            .map(|_| {
                if rng.random_range(0..100) < 5 {
                    None // 5% nulls
                } else {
                    Some(rng.random_range(0.0..1000.0))
                }
            })
            .collect();

        batches.push(
            RecordBatch::try_new(Arc::clone(&schema), vec![
                Arc::new(id6),
                Arc::new(v3),
            ])
                .unwrap(),
        );
    }

    // Split into 8 partitions
    let partition_size = batches.len() / 8;
    let mut partitions: Vec<Vec<RecordBatch>> = Vec::new();
    for chunk in batches.chunks(partition_size.max(1)) {
        partitions.push(chunk.to_vec());
    }

    Arc::new(MemTable::try_new(schema, partitions).unwrap())
}

const Q8: &str = "\
SELECT id6, largest2_v3 FROM (\
    SELECT id6, v3 AS largest2_v3, \
           ROW_NUMBER() OVER (PARTITION BY id6 ORDER BY v3 DESC) AS order_v3 \
    FROM x WHERE v3 IS NOT NULL\
) sub_query WHERE order_v3 <= 2";

#[tokio::main]
async fn main() {
    // Test across different partition cardinalities
    let scenarios = [
        (100, "100 partitions (100K rows/partition)"),
        (1_000, "1K partitions (10K rows/partition)"),
        (10_000, "10K partitions (1K rows/partition)"),
        (100_000, "100K partitions (100 rows/partition, H2O-like)"),
    ];

    for (num_partitions, label) in scenarios {
        println!("=== Scenario: {label} ===");
        println!("Generating {NUM_ROWS} rows with {num_partitions} partitions...");
        let table = generate_data(num_partitions);

        for (tag, enabled) in [("ENABLED ", true), ("DISABLED", false)] {
            let mut config = SessionConfig::new();
            config.options_mut().optimizer.enable_window_topn = enabled;
            let ctx = SessionContext::new_with_config(config);
            ctx.register_table("x", Arc::clone(&table) as _).unwrap();

            // Warmup
            let df = ctx.sql(Q8).await.unwrap();
            let _ = df.collect().await.unwrap();

            // Benchmark
            let mut times = Vec::new();
            for _ in 0..ITERATIONS {
                let start = Instant::now();
                let df = ctx.sql(Q8).await.unwrap();
                let batches = df.collect().await.unwrap();
                let elapsed = start.elapsed();
                let _row_count: usize = batches.iter().map(|b| b.num_rows()).sum();
                times.push(elapsed.as_millis());
            }

            let avg = times.iter().sum::<u128>() / times.len() as u128;
            let min = *times.iter().min().unwrap();
            println!("  [{tag}] avg={avg} ms, min={min} ms");
        }
        println!();
    }
}