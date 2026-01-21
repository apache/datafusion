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

//! Benchmark for DataFusion SortExec with Arrow IPC serialization.
//!
//! This benchmark measures the end-to-end latency of:
//! 1. Deserializing Arrow IPC data into RecordBatches
//! 2. Executing a SortExec operator (ORDER BY colInt ASC)
//! 3. Serializing the output back to Arrow IPC format
//!
//! Two sort variants are tested:
//! - **Full sort**: Sort all 1M rows (no limit)
//! - **TopK sort**: Sort with LIMIT 10,000 (uses heap-based TopK algorithm)
//!
//! The benchmark helps understand sort performance characteristics,
//! how the TopK optimization affects latency, and the overhead of
//! serializing sorted results.
//!
//! ## Running the benchmark
//!
//! ```bash
//! # Run all configurations
//! cargo bench --bench sort_bench -p datafusion-physical-plan
//!
//! # Run with fewer samples for quick testing
//! cargo bench --bench sort_bench -p datafusion-physical-plan -- --sample-size 10
//!
//! # Run specific configuration
//! cargo bench --bench sort_bench -p datafusion-physical-plan -- "sort_no_limit"
//! ```

// Include shared benchmark utilities
#[path = "bench_utils.rs"]
mod bench_utils;

use std::hint::black_box;
use std::sync::Arc;

use arrow::compute::SortOptions;
use arrow::datatypes::SchemaRef;
use criterion::{
    BatchSize, BenchmarkId, Criterion, SamplingMode, Throughput, criterion_group, criterion_main,
};
use datafusion_execution::TaskContext;
use datafusion_physical_expr::expressions::Column;
use datafusion_physical_expr_common::sort_expr::{LexOrdering, PhysicalSortExpr};
use datafusion_physical_plan::sorts::sort::SortExec;
use datafusion_physical_plan::{ExecutionPlan, collect};
use tokio::runtime::Runtime;

use bench_utils::{
    BatchSourceExec, FunctionalBatchGenerator, create_schema, deserialize_from_ipc,
    serialize_results_to_ipc, serialize_to_ipc,
};

// ============================================================================
// Sort Plan Creation
// ============================================================================

/// Creates a SortExec that sorts by `colInt ASC`.
///
/// With the data generation pattern `colInt = i % 5000`, the sort will
/// group all rows with the same colInt value together, with values
/// ranging from 0 to 4999.
///
/// # Arguments
/// * `input` - The input execution plan (typically BatchSourceExec)
/// * `schema` - Schema of the input data
/// * `fetch` - Optional limit for TopK optimization:
///   - `None`: Full sort of all rows
///   - `Some(n)`: TopK sort returning only top n rows
///
/// # Returns
/// A SortExec wrapped in Arc<dyn ExecutionPlan>
fn create_sort_plan(
    input: Arc<dyn ExecutionPlan>,
    schema: &SchemaRef,
    fetch: Option<usize>,
) -> Arc<dyn ExecutionPlan> {
    // Build sort expression: ORDER BY colInt ASC
    let col_int = Arc::new(Column::new_with_schema("colInt", schema).unwrap());
    let sort_expr = PhysicalSortExpr::new(col_int, SortOptions::default());
    let sort_exprs = LexOrdering::new(vec![sort_expr]).unwrap();

    // Create SortExec with optional fetch limit
    let sort = SortExec::new(sort_exprs, input);
    let sort = if let Some(limit) = fetch {
        // TopK optimization: uses a heap to track only the top `limit` rows
        sort.with_fetch(Some(limit))
    } else {
        sort
    };

    Arc::new(sort)
}

// ============================================================================
// Benchmark Implementation
// ============================================================================

/// Main benchmark function for sort execution.
///
/// This benchmark measures six scenarios for each binary column size:
///
/// 1. **deser_only**: Just IPC deserialization, no execution
///    - Establishes baseline deserialization cost
///
/// 2. **ser_only**: Just IPC serialization, no execution
///    - Establishes baseline serialization cost
///    - Uses pre-generated batches directly
///
/// 3. **sort_no_limit**: Full sort execution only
///    - Sorts all 1M rows by colInt
///    - Uses pre-generated batches directly (no deserialization)
///    - Isolates SortExec performance
///
/// 4. **sort_limit_10k**: TopK sort execution only
///    - Uses TopK algorithm to find top 10,000 rows
///    - Uses pre-generated batches directly (no deserialization)
///    - Should be faster than full sort for large datasets
///
/// 5. **full_pipeline_no_limit**: Complete deser + full sort + output serialization
///    - Real-world latency for ORDER BY queries including result serialization
///
/// 6. **full_pipeline_limit_10k**: Complete deser + TopK sort + output serialization
///    - Real-world latency for LIMIT queries including result serialization
fn bench_sort(c: &mut Criterion) {
    // Create a Tokio runtime for async execution
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("sort_bench");

    // Use flat sampling to collect exactly the requested samples without time constraints
    group.sampling_mode(SamplingMode::Flat);

    // Configuration: 1M rows total (10K rows × 100 batches)
    let rows_per_batch = 10_000;
    let num_batches = 100;
    let total_rows = rows_per_batch * num_batches;

    // Test different binary column sizes to understand serialization overhead
    let binary_sizes = vec![10, 1024, 2048];

    for binary_size in binary_sizes {
        let label = format!("1M_rows_binary_{binary_size}B");

        // Generate test data and serialize to IPC format
        let schema = create_schema();
        let mut generator = FunctionalBatchGenerator::new(
            Arc::clone(&schema),
            rows_per_batch,
            num_batches,
            binary_size,
        );
        let batches = generator.generate_batches();
        let ipc_data = serialize_to_ipc(&batches, &schema);
        let ipc_size = ipc_data.len();

        // Log configuration for visibility in benchmark output
        println!(
            "Config: {} rows, binary_size={} bytes, IPC size={:.2} MB",
            total_rows,
            binary_size,
            ipc_size as f64 / (1024.0 * 1024.0)
        );

        // Set throughput metric for bytes/second calculations
        group.throughput(Throughput::Bytes(ipc_size as u64));

        // Benchmark 1: IPC Deserialization only
        // Measures the cost of parsing Arrow IPC format into RecordBatches
        group.bench_with_input(
            BenchmarkId::new("deser_only", &label),
            &ipc_data,
            |b, ipc_data| {
                b.iter(|| {
                    let (schema, batches) = deserialize_from_ipc(ipc_data);
                    black_box((schema, batches))
                })
            },
        );

        // Benchmark 2: IPC Serialization only
        // Measures the cost of serializing RecordBatches to IPC format
        group.bench_with_input(
            BenchmarkId::new("ser_only", &label),
            &batches,
            |b, batches| {
                b.iter(|| {
                    let output_ipc = serialize_to_ipc(batches, &schema);
                    black_box(output_ipc)
                })
            },
        );

        // Benchmark 3: Full sort (no limit) - execution only
        // Uses pre-generated batches directly, isolating SortExec performance
        group.bench_with_input(
            BenchmarkId::new("sort_no_limit", &label),
            &batches,
            |b, batches| {
                b.iter_batched(
                    // Setup: clone batches (NOT timed) - needed because execution consumes them
                    || batches.clone(),
                    // Benchmark: execute sort (TIMED)
                    |batches| {
                        rt.block_on(async {
                            let source = Arc::new(BatchSourceExec::new(
                                Arc::clone(&schema),
                                batches,
                            )) as Arc<dyn ExecutionPlan>;
                            let plan = create_sort_plan(source, &schema, None);
                            let task_ctx = Arc::new(TaskContext::default());
                            let results = collect(plan, task_ctx).await.unwrap();
                            black_box(results)
                        })
                    },
                    BatchSize::SmallInput,
                )
            },
        );

        // Benchmark 4: TopK sort (LIMIT 10,000) - execution only
        // Uses pre-generated batches directly; should be faster than full sort
        group.bench_with_input(
            BenchmarkId::new("sort_limit_10k", &label),
            &batches,
            |b, batches| {
                b.iter_batched(
                    // Setup: clone batches (NOT timed) - needed because execution consumes them
                    || batches.clone(),
                    // Benchmark: execute TopK sort (TIMED)
                    |batches| {
                        rt.block_on(async {
                            let source = Arc::new(BatchSourceExec::new(
                                Arc::clone(&schema),
                                batches,
                            )) as Arc<dyn ExecutionPlan>;
                            let plan = create_sort_plan(source, &schema, Some(10_000));
                            let task_ctx = Arc::new(TaskContext::default());
                            let results = collect(plan, task_ctx).await.unwrap();
                            black_box(results)
                        })
                    },
                    BatchSize::SmallInput,
                )
            },
        );

        // Benchmark 5: Full pipeline with full sort + output serialization
        // Measures complete round-trip: IPC in -> sort all rows -> IPC out
        group.bench_with_input(
            BenchmarkId::new("full_pipeline_no_limit", &label),
            &ipc_data,
            |b, ipc_data| {
                b.iter(|| {
                    rt.block_on(async {
                        let (schema, batches) = deserialize_from_ipc(ipc_data);
                        let source = Arc::new(BatchSourceExec::new(
                            Arc::clone(&schema),
                            batches,
                        )) as Arc<dyn ExecutionPlan>;
                        let plan = create_sort_plan(source, &schema, None);
                        let task_ctx = Arc::new(TaskContext::default());
                        let results = collect(plan, task_ctx).await.unwrap();
                        // Serialize sorted results back to IPC format
                        let output_ipc = serialize_results_to_ipc(&results);
                        black_box(output_ipc)
                    })
                })
            },
        );

        // Benchmark 6: Full pipeline with TopK sort + output serialization
        // Measures complete round-trip: IPC in -> TopK sort -> IPC out
        // Output size is limited to 10K rows, so serialization should be faster
        group.bench_with_input(
            BenchmarkId::new("full_pipeline_limit_10k", &label),
            &ipc_data,
            |b, ipc_data| {
                b.iter(|| {
                    rt.block_on(async {
                        let (schema, batches) = deserialize_from_ipc(ipc_data);
                        let source = Arc::new(BatchSourceExec::new(
                            Arc::clone(&schema),
                            batches,
                        )) as Arc<dyn ExecutionPlan>;
                        let plan = create_sort_plan(source, &schema, Some(10_000));
                        let task_ctx = Arc::new(TaskContext::default());
                        let results = collect(plan, task_ctx).await.unwrap();
                        // Serialize TopK results back to IPC format
                        let output_ipc = serialize_results_to_ipc(&results);
                        black_box(output_ipc)
                    })
                })
            },
        );
    }

    group.finish();
}

criterion_group!(benches, bench_sort);
criterion_main!(benches);
