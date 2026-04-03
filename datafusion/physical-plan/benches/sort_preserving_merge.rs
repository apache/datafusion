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

criterion_group!(benches, bench_merge_sorted_preserving);
criterion_main!(benches);
