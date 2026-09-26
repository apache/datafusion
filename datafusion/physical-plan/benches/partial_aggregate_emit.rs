// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file except in
// compliance with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Benchmarks partial hash-aggregate output after memory-pressure emission.
//!
//! The bounded memory pool forces the partial aggregate to materialize state
//! while processing high-cardinality input. This exercises the output path
//! that must not repeatedly compact the retained hash table.

use std::hint::black_box;
use std::sync::Arc;

use arrow::array::{Int32Array, Int64Array};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use datafusion_common::ScalarValue;
use datafusion_execution::TaskContext;
use datafusion_execution::memory_pool::FairSpillPool;
use datafusion_execution::runtime_env::RuntimeEnvBuilder;
use datafusion_functions_aggregate::count::count_udaf;
use datafusion_physical_expr::aggregate::AggregateExprBuilder;
use datafusion_physical_expr::expressions::col;
use datafusion_physical_plan::aggregates::{
    AggregateExec, AggregateMode, PhysicalGroupBy,
};
use datafusion_physical_plan::test::TestMemoryExec;
use datafusion_physical_plan::{ExecutionPlan, collect};
use tokio::runtime::Runtime;

const BATCH_SIZE: usize = 8_192;
const CASES: &[(usize, usize)] = &[
    // (distinct groups, fair-pool limit). The limit is below the retained
    // aggregate state and therefore forces partial output before EOF.
    (65_536, 2 * 1024 * 1024),
    (262_144, 8 * 1024 * 1024),
];

fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("group_key", DataType::Int32, false),
        Field::new("value", DataType::Int64, false),
    ]))
}

fn input_batches(schema: &SchemaRef, num_groups: usize) -> Vec<RecordBatch> {
    (0..num_groups)
        .step_by(BATCH_SIZE)
        .map(|start| {
            let end = (start + BATCH_SIZE).min(num_groups);
            RecordBatch::try_new(
                Arc::clone(schema),
                vec![
                    Arc::new(Int32Array::from_iter_values(
                        (start..end).map(|group| group as i32),
                    )),
                    Arc::new(Int64Array::from(vec![1; end - start])),
                ],
            )
            .unwrap()
        })
        .collect()
}

fn make_plan(num_groups: usize) -> Arc<AggregateExec> {
    let schema = schema();
    let input = TestMemoryExec::try_new_exec(
        &[input_batches(&schema, num_groups)],
        Arc::clone(&schema),
        None,
    )
    .unwrap();
    let group_by = PhysicalGroupBy::new_single(vec![(
        col("group_key", &schema).unwrap(),
        "group_key".to_string(),
    )]);
    let aggregates = vec![Arc::new(
        AggregateExprBuilder::new(count_udaf(), vec![col("value", &schema).unwrap()])
            .schema(Arc::clone(&schema))
            .alias("count_value")
            .build()
            .unwrap(),
    )];

    Arc::new(
        AggregateExec::try_new(
            AggregateMode::Partial,
            group_by,
            aggregates,
            vec![None],
            input,
            schema,
        )
        .unwrap(),
    )
}

fn task_context(memory_limit: usize) -> Arc<TaskContext> {
    let runtime = RuntimeEnvBuilder::new()
        .with_memory_pool(Arc::new(FairSpillPool::new(memory_limit)))
        .build_arc()
        .unwrap();
    let session_config = TaskContext::default().session_config().clone().set(
        "datafusion.execution.skip_partial_aggregation_probe_ratio_threshold",
        &ScalarValue::Float64(Some(1.0)),
    );
    Arc::new(
        TaskContext::default()
            .with_runtime(runtime)
            .with_session_config(session_config),
    )
}

fn early_emit_count(plan: &AggregateExec) -> usize {
    plan.metrics()
        .and_then(|metrics| metrics.sum_by_name("early_emit_count"))
        .map(|count| count.as_usize())
        .unwrap_or_default()
}

fn partial_aggregate_emit_benchmark(c: &mut Criterion) {
    let runtime = Runtime::new().unwrap();
    let mut group = c.benchmark_group("partial_aggregate_memory_pressure_emit");
    group.sample_size(10);

    for &(num_groups, memory_limit) in CASES {
        let plan = make_plan(num_groups);
        let task_ctx = task_context(memory_limit);
        group.bench_function(
            BenchmarkId::new(
                "int32_count",
                format!("{num_groups}_groups_{memory_limit}_bytes"),
            ),
            |b| {
                b.iter(|| {
                    let before = early_emit_count(&plan);
                    let execution_plan: Arc<dyn ExecutionPlan> = plan.clone();
                    let output = runtime
                        .block_on(collect(execution_plan, Arc::clone(&task_ctx)))
                        .unwrap();
                    assert!(
                        early_emit_count(&plan) > before,
                        "benchmark input must trigger memory-pressure partial emission"
                    );
                    black_box(output);
                });
            },
        );
    }

    group.finish();
}

criterion_group!(benches, partial_aggregate_emit_benchmark);
criterion_main!(benches);
