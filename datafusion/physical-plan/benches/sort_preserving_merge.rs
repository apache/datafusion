// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use arrow::{
    array::{ArrayRef, AsArray, StringArray, UInt64Array},
    record_batch::RecordBatch,
};
use arrow_schema::{SchemaRef, SortOptions};
use criterion::{BatchSize, Criterion, criterion_group, criterion_main};
use datafusion_execution::TaskContext;
use datafusion_physical_expr::{LexOrdering, PhysicalSortExpr, expressions::col};
use datafusion_physical_plan::test::TestMemoryExec;
use datafusion_physical_plan::{
    collect, sorts::sort_preserving_merge::SortPreservingMergeExec,
};

use std::sync::Arc;

const BENCH_ROWS: usize = 1_000_000; // 1 million rows

fn get_large_string(idx: usize) -> String {
    let base_content = [
        concat!(
            "# Advanced Topics in Computer Science\n\n",
            "## Summary\nThis article explores complex system design patterns and...\n\n",
            "```rust\nfn process_data(data: &mut [i32]) {\n    // Parallel processing example\n    data.par_iter_mut().for_each(|x| *x *= 2);\n}\n```\n\n",
            "## Performance Considerations\nWhen implementing concurrent systems...\n"
        ),
        concat!(
            "## API Documentation\n\n",
            "```json\n{\n  \"endpoint\": \"/api/v2/users\",\n  \"methods\": [\"GET\", \"POST\"],\n  \"parameters\": {\n    \"page\": \"number\"\n  }\n}\n```\n\n",
            "# Authentication Guide\nSecure your API access using OAuth 2.0...\n"
        ),
        concat!(
            "# Data Processing Pipeline\n\n",
            "```python\nfrom multiprocessing import Pool\n\ndef main():\n    with Pool(8) as p:\n        results = p.map(process_item, data)\n```\n\n",
            "## Summary of Optimizations\n1. Batch processing\n2. Memory pooling\n3. Concurrent I/O operations\n"
        ),
        concat!(
            "# System Architecture Overview\n\n",
            "## Components\n- Load Balancer\n- Database Cluster\n- Cache Service\n\n",
            "```go\nfunc main() {\n    router := gin.Default()\n    router.GET(\"/api/health\", healthCheck)\n    router.Run(\":8080\")\n}\n```\n"
        ),
        concat!(
            "## Configuration Reference\n\n",
            "```yaml\nserver:\n  port: 8080\n  max_threads: 32\n\ndatabase:\n  url: postgres://user@prod-db:5432/main\n```\n\n",
            "# Deployment Strategies\nBlue-green deployment patterns with...\n"
        ),
    ];
    base_content[idx % base_content.len()].to_string()
}

fn generate_sorted_string_column(rows: usize) -> ArrayRef {
    let mut values = Vec::with_capacity(rows);
    for i in 0..rows {
        values.push(get_large_string(i));
    }
    values.sort();
    Arc::new(StringArray::from(values))
}

fn generate_sorted_u64_column(rows: usize) -> ArrayRef {
    Arc::new(UInt64Array::from((0_u64..rows as u64).collect::<Vec<_>>()))
}

fn create_partitions<const IS_LARGE_COLUMN_TYPE: bool>(
    num_partitions: usize,
    num_columns: usize,
    num_rows: usize,
) -> Vec<Vec<RecordBatch>> {
    (0..num_partitions)
        .map(|_| {
            let rows = (0..num_columns)
                .map(|i| {
                    (
                        format!("col-{i}"),
                        if IS_LARGE_COLUMN_TYPE {
                            generate_sorted_string_column(num_rows)
                        } else {
                            generate_sorted_u64_column(num_rows)
                        },
                    )
                })
                .collect::<Vec<_>>();

            let batch = RecordBatch::try_from_iter(rows).unwrap();
            vec![batch]
        })
        .collect()
}

struct BenchData {
    bench_name: String,
    partitions: Vec<Vec<RecordBatch>>,
    schema: SchemaRef,
    sort_order: LexOrdering,
}

fn get_bench_data() -> Vec<BenchData> {
    let mut ret = Vec::new();
    let mut push_bench_data = |bench_name: &str, partitions: Vec<Vec<RecordBatch>>| {
        let schema = partitions[0][0].schema();
        // Define sort order (col1 ASC, col2 ASC, col3 ASC)
        let sort_order = LexOrdering::new(schema.fields().iter().map(|field| {
            PhysicalSortExpr::new(
                col(field.name(), &schema).unwrap(),
                SortOptions::default(),
            )
        }))
        .unwrap();
        ret.push(BenchData {
            bench_name: bench_name.to_string(),
            partitions,
            schema,
            sort_order,
        });
    };
    // 1. single large string column
    {
        let partitions = create_partitions::<true>(3, 1, BENCH_ROWS);
        push_bench_data("single_large_string_column_with_1m_rows", partitions);
    }
    // 2. single u64 column
    {
        let partitions = create_partitions::<false>(3, 1, BENCH_ROWS);
        push_bench_data("single_u64_column_with_1m_rows", partitions);
    }
    // 3. multiple large string columns
    {
        let partitions = create_partitions::<true>(3, 3, BENCH_ROWS);
        push_bench_data("multiple_large_string_columns_with_1m_rows", partitions);
    }
    // 4. multiple u64 columns
    {
        let partitions = create_partitions::<false>(3, 3, BENCH_ROWS);
        push_bench_data("multiple_u64_columns_with_1m_rows", partitions);
    }
    ret
}

/// Add a benchmark to test the optimization effect of reusing Rows.
/// Run this benchmark with:
/// ```sh
/// cargo bench --features="bench"  --bench sort_preserving_merge -- --sample-size=10
/// ```
fn bench_merge_sorted_preserving(c: &mut Criterion) {
    let task_ctx = Arc::new(TaskContext::default());
    let bench_data = get_bench_data();
    for data in bench_data.into_iter() {
        let BenchData {
            bench_name,
            partitions,
            schema,
            sort_order,
        } = data;
        c.bench_function(
            &format!("bench_merge_sorted_preserving/{bench_name}"),
            |b| {
                b.iter_batched(
                    || {
                        let exec = TestMemoryExec::try_new_exec(
                            &partitions,
                            schema.clone(),
                            None,
                        )
                        .unwrap();
                        Arc::new(SortPreservingMergeExec::new(sort_order.clone(), exec))
                    },
                    |merge_exec| {
                        let rt = tokio::runtime::Runtime::new().unwrap();
                        rt.block_on(async {
                            collect(merge_exec, task_ctx.clone()).await.unwrap();
                        });
                    },
                    BatchSize::LargeInput,
                )
            },
        );
    }
}

/// Merge inputs whose keys are mostly *tied* and whose producers do real work
/// per batch.
///
/// `SortPreservingMergeExec` runs each input in its own task, buffered one
/// batch ahead (`spawn_buffered(_, 1)`). If the merge keeps draining a single
/// partition during a run of equal keys, that partition's producer becomes the
/// bottleneck while the others idle on their one buffered batch. The
/// round-robin tie breaker is meant to spread consumption across the tied
/// partitions so all producers stay busy.
fn bench_merge_tied_keys_slow_producers(c: &mut Criterion) {
    use datafusion_execution::memory_pool::{
        MemoryConsumer, MemoryPool, UnboundedMemoryPool,
    };
    use datafusion_physical_plan::common::spawn_buffered;
    use datafusion_physical_plan::metrics::{BaselineMetrics, ExecutionPlanMetricsSet};
    use datafusion_physical_plan::sorts::streaming_merge::StreamingMergeBuilder;
    use datafusion_physical_plan::stream::RecordBatchStreamAdapter;
    use futures::StreamExt;

    const ROWS: usize = 400_000;
    const BATCH: usize = 8192;
    const ROWS_PER_KEY: usize = 100_000;

    let schema: SchemaRef = Arc::new(arrow_schema::Schema::new(vec![
        arrow_schema::Field::new("key", arrow_schema::DataType::UInt64, false),
        arrow_schema::Field::new("val", arrow_schema::DataType::UInt64, false),
    ]));
    let sort_order = LexOrdering::new(vec![PhysicalSortExpr::new(
        col("key", &schema).unwrap(),
        SortOptions::default(),
    )])
    .unwrap();

    // Every partition holds the same long runs of equal keys.
    let batches: Vec<RecordBatch> = (0..ROWS.div_ceil(BATCH))
        .map(|b| {
            let start = b * BATCH;
            let n = BATCH.min(ROWS - start);
            let keys = UInt64Array::from_iter_values(
                (start..start + n).map(|i| (i / ROWS_PER_KEY) as u64),
            );
            let vals =
                UInt64Array::from_iter_values((start..start + n).map(|i| i as u64));
            RecordBatch::try_new(
                Arc::clone(&schema),
                vec![Arc::new(keys), Arc::new(vals)],
            )
            .unwrap()
        })
        .collect();

    /// Stand-in for an upstream operator: ~fixed CPU cost per batch.
    fn produce(batch: RecordBatch) -> RecordBatch {
        let vals = batch
            .column(1)
            .as_primitive::<arrow::datatypes::UInt64Type>();
        let mut acc = 0u64;
        for _ in 0..200 {
            for v in vals.values() {
                acc = acc.wrapping_mul(6364136223846793005).wrapping_add(*v);
            }
        }
        std::hint::black_box(acc);
        batch
    }

    let rt = tokio::runtime::Runtime::new().unwrap();
    // With 2 inputs the root comparison is the whole tree, so the tie breaker
    // balances all producers; with 4 it only balances the two sub-tree winners.
    //
    // Each case is run with the tie breaker both enabled and disabled so the
    // pair measures what the tie breaker actually buys on this workload: if
    // the two ever converge, the balancing has regressed into the
    // lowest-index-wins baseline.
    for partitions in [2, 4] {
        for (label, round_robin) in [("on", true), ("off", false)] {
            c.bench_function(
                &format!(
                    "bench_merge_tied_keys_slow_producers/{partitions}_partitions/tie_breaker_{label}"
                ),
                |b| {
                    b.iter(|| {
                        rt.block_on(async {
                            let streams = (0..partitions)
                                .map(|_| {
                                    let s = RecordBatchStreamAdapter::new(
                                        Arc::clone(&schema),
                                        futures::stream::iter(batches.clone())
                                            .map(|b| Ok(produce(b))),
                                    );
                                    spawn_buffered(Box::pin(s), 1)
                                })
                                .collect();
                            let pool: Arc<dyn MemoryPool> =
                                Arc::new(UnboundedMemoryPool::default());
                            let merged = StreamingMergeBuilder::new()
                                .with_streams(streams)
                                .with_schema(Arc::clone(&schema))
                                .with_expressions(&sort_order)
                                .with_metrics(BaselineMetrics::new(
                                    &ExecutionPlanMetricsSet::new(),
                                    0,
                                ))
                                .with_batch_size(BATCH)
                                .with_reservation(
                                    MemoryConsumer::new("bench").register(&pool),
                                )
                                .with_round_robin_tie_breaker(round_robin)
                                .build()
                                .unwrap();
                            datafusion_physical_plan::common::collect(merged)
                                .await
                                .unwrap();
                        })
                    })
                },
            );
        }
    }
}

criterion_group!(
    benches,
    bench_merge_sorted_preserving,
    bench_merge_tied_keys_slow_producers
);
criterion_main!(benches);
