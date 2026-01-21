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

//! Benchmark for DataFusion FilterExec with Arrow IPC serialization.
//!
//! This benchmark measures the end-to-end latency of:
//! 1. Deserializing Arrow IPC data into RecordBatches
//! 2. Executing a FilterExec operator (predicate: colInt > 2500)
//! 3. Serializing the output back to Arrow IPC format
//!
//! The benchmark helps understand the overhead of IPC deserialization
//! and serialization relative to actual query execution, and how filter
//! performance scales with data size.
//!
//! ## Running the benchmark
//!
//! ```bash
//! # Run all configurations
//! cargo bench --bench filter_bench -p datafusion-physical-plan
//!
//! # Run with fewer samples for quick testing
//! cargo bench --bench filter_bench -p datafusion-physical-plan -- --sample-size 10
//!
//! # Run specific configuration
//! cargo bench --bench filter_bench -p datafusion-physical-plan -- "1M_rows_binary_10B"
//! ```

// Include shared benchmark utilities
#[path = "bench_utils.rs"]
mod bench_utils;

use std::hint::black_box;
use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use criterion::{
    BatchSize, BenchmarkId, Criterion, SamplingMode, Throughput, criterion_group, criterion_main,
};
use datafusion_common::ScalarValue;
use datafusion_execution::TaskContext;
use datafusion_expr::Operator;
use datafusion_physical_expr::expressions::{BinaryExpr, Column, Literal};
use datafusion_physical_expr::PhysicalExpr;
use datafusion_physical_plan::filter::FilterExecBuilder;
use datafusion_physical_plan::{ExecutionPlan, collect};
use tokio::runtime::Runtime;

use bench_utils::{
    BatchSourceExec, FunctionalBatchGenerator, create_schema, deserialize_from_ipc,
    serialize_results_to_ipc, serialize_to_ipc,
};

// ============================================================================
// Filter Plan Creation
// ============================================================================

/// Creates a FilterExec that evaluates `colInt > 2500`.
///
/// With the data generation pattern `colInt = i % 5000`, this predicate
/// has approximately 50% selectivity (values 2501-4999 pass, 0-2500 don't).
///
/// # Arguments
/// * `input` - The input execution plan (typically BatchSourceExec)
/// * `schema` - Schema of the input data
///
/// # Returns
/// A FilterExec wrapped in Arc<dyn ExecutionPlan>
fn create_filter_plan(
    input: Arc<dyn ExecutionPlan>,
    schema: &SchemaRef,
) -> Arc<dyn ExecutionPlan> {
    // Build the predicate: colInt > 2500
    let col_int = Arc::new(Column::new_with_schema("colInt", schema).unwrap())
        as Arc<dyn PhysicalExpr>;
    let threshold =
        Arc::new(Literal::new(ScalarValue::Int32(Some(2500)))) as Arc<dyn PhysicalExpr>;
    let predicate =
        Arc::new(BinaryExpr::new(col_int, Operator::Gt, threshold)) as Arc<dyn PhysicalExpr>;

    Arc::new(FilterExecBuilder::new(predicate, input).build().unwrap())
}

// ============================================================================
// Benchmark Implementation
// ============================================================================

/// Main benchmark function for filter execution.
///
/// This benchmark measures four scenarios for each binary column size:
///
/// 1. **deser_only**: Just IPC deserialization, no execution
///    - Establishes baseline deserialization cost
///    - Useful for understanding I/O vs compute ratio
///
/// 2. **ser_only**: Just IPC serialization, no execution
///    - Establishes baseline serialization cost
///    - Uses pre-generated batches directly
///
/// 3. **filter_only**: Filter execution only
///    - Isolates the FilterExec performance
///    - Uses pre-generated batches directly (no deserialization)
///    - Timed phase runs only the filter operator
///
/// 4. **full_pipeline**: Complete deser + filter + output serialization
///    - Real-world end-to-end latency including result serialization
///    - Relevant for scenarios where results are sent over network
fn bench_filter(c: &mut Criterion) {
    // Create a Tokio runtime for async execution
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("filter_bench");

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
                    // black_box prevents compiler from optimizing away unused results
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

        // Benchmark 3: Filter execution only
        // Uses pre-generated batches directly, isolating FilterExec performance
        group.bench_with_input(
            BenchmarkId::new("filter_only", &label),
            &batches,
            |b, batches| {
                b.iter_batched(
                    // Setup: clone batches (NOT timed) - needed because execution consumes them
                    || batches.clone(),
                    // Benchmark: execute filter (TIMED)
                    |batches| {
                        rt.block_on(async {
                            let source = Arc::new(BatchSourceExec::new(
                                Arc::clone(&schema),
                                batches,
                            )) as Arc<dyn ExecutionPlan>;
                            let plan = create_filter_plan(source, &schema);
                            let task_ctx = Arc::new(TaskContext::default());
                            let results = collect(plan, task_ctx).await.unwrap();
                            black_box(results)
                        })
                    },
                    BatchSize::SmallInput,
                )
            },
        );

        // Benchmark 4: Full pipeline (deser + filter + output serialization)
        // Measures complete round-trip: IPC in -> filter -> IPC out
        // Relevant for scenarios where results are sent over network or stored
        group.bench_with_input(
            BenchmarkId::new("full_pipeline", &label),
            &ipc_data,
            |b, ipc_data| {
                b.iter(|| {
                    rt.block_on(async {
                        let (schema, batches) = deserialize_from_ipc(ipc_data);
                        let source = Arc::new(BatchSourceExec::new(
                            Arc::clone(&schema),
                            batches,
                        )) as Arc<dyn ExecutionPlan>;
                        let plan = create_filter_plan(source, &schema);
                        let task_ctx = Arc::new(TaskContext::default());
                        let results = collect(plan, task_ctx).await.unwrap();
                        // Serialize results back to IPC format
                        let output_ipc = serialize_results_to_ipc(&results);
                        black_box(output_ipc)
                    })
                })
            },
        );
    }

    group.finish();
}

criterion_group!(benches, bench_filter);
criterion_main!(benches);
