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
    array::{ArrayRef, StringArray},
    record_batch::RecordBatch,
};
use arrow_schema::SortOptions;
use criterion::{criterion_group, criterion_main, BatchSize, Criterion};
use datafusion_execution::TaskContext;
use datafusion_physical_expr::{expressions::col, LexOrdering, PhysicalSortExpr};
use datafusion_physical_plan::test::TestMemoryExec;
use datafusion_physical_plan::{
    collect, sorts::sort_preserving_merge::SortPreservingMergeExec,
};

use std::sync::Arc;

const BENCH_ROWS: usize = 1_000_000; // 1 million rows

fn get_random_large_string(idx: usize) -> String {
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
        values.push(get_random_large_string(i));
    }
    values.sort();
    Arc::new(StringArray::from(values))
}

fn create_partitions(
    num_partitions: usize,
    column_names: &[&str],
) -> Vec<Vec<RecordBatch>> {
    (0..num_partitions)
        .map(|_| {
            let rows = column_names
                .iter()
                .map(|&name| (name.to_owned(), generate_sorted_string_column(BENCH_ROWS)))
                .collect::<Vec<_>>();

            let batch = RecordBatch::try_from_iter(rows).unwrap();
            vec![batch]
        })
        .collect()
}

/// Run this benchmark with:
/// ```sh
/// cargo bench --features="bench"  --bench sort_preserving -- --sample-size=10
/// ```
fn bench_merge_sorted_preserving(c: &mut Criterion) {
    let num_partitions = 3;

    let column_names = vec!["col1", "col2", "col3"];

    // Create sorted partitions
    let partitions = create_partitions(num_partitions, &column_names);
    let schema = partitions[0][0].schema();

    // Define sort order (col1 ASC, col2 ASC, col3 ASC)
    let sort_order = LexOrdering::new(
        column_names
            .iter()
            .map(|&name| {
                PhysicalSortExpr::new(col(name, &schema).unwrap(), SortOptions::default())
            })
            .collect(),
    );

    let task_ctx = Arc::new(TaskContext::default());

    c.bench_function("sort_preserving_merge_1m_rows_and_3_columns", |b| {
        b.iter_batched(
            || {
                let exec =
                    TestMemoryExec::try_new_exec(&partitions, schema.clone(), None)
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
    });
}

criterion_group!(benches, bench_merge_sorted_preserving);
criterion_main!(benches);
