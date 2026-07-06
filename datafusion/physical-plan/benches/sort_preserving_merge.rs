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
    array::{ArrayRef, StringArray, UInt64Array},
    record_batch::RecordBatch,
};
use arrow_schema::{SchemaRef, SortOptions};
use criterion::{BatchSize, Criterion, criterion_group, criterion_main};
use datafusion_execution::SendableRecordBatchStream;
use datafusion_execution::TaskContext;
use datafusion_physical_expr::{LexOrdering, PhysicalSortExpr, expressions::col};
use datafusion_physical_plan::test::TestMemoryExec;
use datafusion_physical_plan::{
    collect, execute_stream, sorts::sort_preserving_merge::SortPreservingMergeExec,
};
use futures::StreamExt;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};

use std::hint::black_box;
use std::sync::Arc;

/// Consume the stream batch by batch, dropping each batch as it arrives
/// instead of holding the whole result in memory like `collect` would
async fn drain(mut stream: SendableRecordBatchStream) {
    while let Some(batch) = stream.next().await {
        black_box(batch.unwrap());
    }
}

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

// ---------------------------------------------------------------------------
// Benchmarks across data orderings (sorted / nearly sorted / reverse /
// unsorted), sort key types (u64 / string / complex) and payload widths
// (5 / 20 / 100 columns).
// ---------------------------------------------------------------------------

const NUM_PARTITIONS: usize = 4;
const ROWS_PER_PARTITION: usize = 100_000;
const BATCH_SIZE: usize = 8192;

const ORDERINGS: [&str; 4] = ["sorted", "nearly_sorted", "reverse", "unsorted"];
const KEY_TYPES: [&str; 3] = ["u64", "string", "complex"];
const PAYLOAD_WIDTHS: [usize; 3] = [5, 20, 100];

/// Generate the keys in their "arrival" order, before partitioning
fn generate_keys(ordering: &str) -> Vec<u64> {
    let n = (NUM_PARTITIONS * ROWS_PER_PARTITION) as u64;
    let mut rng = StdRng::seed_from_u64(42);
    match ordering {
        "sorted" => (0..n).collect(),
        "reverse" => (0..n).rev().collect(),
        // Sorted except for ~1% of items misplaced to random positions
        "nearly_sorted" => {
            let mut keys: Vec<u64> = (0..n).collect();
            for _ in 0..(n / 100) {
                let a = rng.random_range(0..n as usize);
                let b = rng.random_range(0..n as usize);
                keys.swap(a, b);
            }
            keys
        }
        "unsorted" => (0..n).map(|_| rng.random_range(0..n)).collect(),
        _ => unreachable!(),
    }
}

/// Distribute the arrival sequence round-robin (a batch at a time) over the
/// partitions, then sort each partition's keys, as SortExec would before a
/// sort preserving merge
fn partition_keys(keys: &[u64]) -> Vec<Vec<u64>> {
    let mut partitions = (0..NUM_PARTITIONS)
        .map(|_| Vec::with_capacity(ROWS_PER_PARTITION))
        .collect::<Vec<_>>();
    for (i, chunk) in keys.chunks(BATCH_SIZE).enumerate() {
        partitions[i % NUM_PARTITIONS].extend_from_slice(chunk);
    }
    for partition in &mut partitions {
        partition.sort_unstable();
    }
    partitions
}

fn key_columns(key_type: &str, keys: &[u64]) -> Vec<(String, ArrayRef)> {
    let as_string = || {
        Arc::new(StringArray::from_iter_values(
            keys.iter().map(|k| format!("{k:012}")),
        )) as ArrayRef
    };
    match key_type {
        "u64" => vec![(
            "key0".to_string(),
            Arc::new(UInt64Array::from(keys.to_vec())) as _,
        )],
        "string" => vec![("key0".to_string(), as_string())],
        // Two sort columns force the row-based (normalized key) cursor
        "complex" => vec![
            (
                "key0".to_string(),
                Arc::new(UInt64Array::from_iter_values(keys.iter().map(|k| k / 8))) as _,
            ),
            ("key1".to_string(), as_string()),
        ],
        _ => unreachable!(),
    }
}

fn create_case(
    ordering: &str,
    key_type: &str,
    payload_width: usize,
) -> (Vec<Vec<RecordBatch>>, SchemaRef, LexOrdering) {
    let partitions = partition_keys(generate_keys(ordering).as_slice())
        .into_iter()
        .map(|keys| {
            keys.chunks(BATCH_SIZE)
                .map(|chunk| {
                    let mut columns = key_columns(key_type, chunk);
                    // All payload columns share the same buffer, so wide
                    // payloads don't blow up memory
                    let payload = Arc::new(UInt64Array::from(chunk.to_vec())) as ArrayRef;
                    for i in 0..payload_width {
                        columns.push((format!("col{i}"), Arc::clone(&payload)));
                    }
                    RecordBatch::try_from_iter(columns).unwrap()
                })
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();

    let schema = partitions[0][0].schema();
    let sort_order = LexOrdering::new(
        schema
            .fields()
            .iter()
            .filter(|field| field.name().starts_with("key"))
            .map(|field| {
                PhysicalSortExpr::new(
                    col(field.name(), &schema).unwrap(),
                    SortOptions::default(),
                )
            }),
    )
    .unwrap();

    (partitions, schema, sort_order)
}

fn bench_spm_data_patterns(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let task_ctx = Arc::new(TaskContext::default());

    for ordering in ORDERINGS {
        for key_type in KEY_TYPES {
            for payload_width in PAYLOAD_WIDTHS {
                let (partitions, schema, sort_order) =
                    create_case(ordering, key_type, payload_width);

                c.bench_function(
                    &format!("spm/{ordering}/{key_type}/payload_{payload_width}"),
                    |b| {
                        b.iter(|| {
                            let exec = TestMemoryExec::try_new_exec(
                                &partitions,
                                Arc::clone(&schema),
                                None,
                            )
                            .unwrap();
                            let merge = Arc::new(SortPreservingMergeExec::new(
                                sort_order.clone(),
                                exec,
                            ));
                            rt.block_on(drain(
                                execute_stream(merge, Arc::clone(&task_ctx)).unwrap(),
                            ))
                        })
                    },
                );
            }
        }
    }
}

criterion_group!(
    benches,
    bench_merge_sorted_preserving,
    bench_spm_data_patterns
);
criterion_main!(benches);
