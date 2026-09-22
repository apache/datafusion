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

//! Fully ordered grouping: compare the selected implementation with the
//! existing streaming hash table on identical batches. Include emission and
//! cross-batch continuation, not just lookup. Input preparation is untimed.
//! The physical-plan benchmark includes aggregation but excludes SQL planning,
//! sorting and I/O; the data already has a proven ordering.

use std::hint::black_box;
use std::sync::Arc;
use std::time::Duration;

use arrow::array::{ArrayRef, Int32Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use datafusion_execution::TaskContext;
use datafusion_expr::EmitTo;
use datafusion_functions_aggregate::sum::sum_udaf;
use datafusion_physical_expr::aggregate::AggregateExprBuilder;
use datafusion_physical_expr::expressions::col;
use datafusion_physical_expr::{LexOrdering, PhysicalSortExpr};
use datafusion_physical_plan::aggregates::group_values::multi_group_by::GroupValuesColumn;
use datafusion_physical_plan::aggregates::group_values::{GroupValues, new_group_values};
use datafusion_physical_plan::aggregates::order::GroupOrdering;
use datafusion_physical_plan::aggregates::{
    AggregateExec, AggregateMode, PhysicalGroupBy,
};
use datafusion_physical_plan::test::TestMemoryExec;
use datafusion_physical_plan::{ExecutionPlan, InputOrderMode, collect};
use tokio::runtime::Runtime;

const ROWS: usize = 131_072;

fn inputs(
    run_length: usize,
    batch_size: usize,
    strings: bool,
) -> (SchemaRef, Vec<Vec<ArrayRef>>) {
    let schema = Arc::new(Schema::new(vec![
        Field::new("a", DataType::Int32, false),
        Field::new(
            "b",
            if strings {
                DataType::Utf8
            } else {
                DataType::Int32
            },
            false,
        ),
    ]));
    let batches = (0..ROWS)
        .step_by(batch_size)
        .map(|start| {
            let end = (start + batch_size).min(ROWS);
            let first: ArrayRef = Arc::new(Int32Array::from_iter_values(
                (start..end).map(|row| ((row / run_length) / 4) as i32),
            ));
            let second: ArrayRef = if strings {
                Arc::new(StringArray::from_iter_values((start..end).map(|row| {
                    format!("key-{}-a-longer-than-inline-string", (row / run_length) % 4)
                })))
            } else {
                Arc::new(Int32Array::from_iter_values(
                    (start..end).map(|row| ((row / run_length) % 4) as i32),
                ))
            };
            vec![first, second]
        })
        .collect();
    (schema, batches)
}

fn grouping(c: &mut Criterion) {
    let mut group = c.benchmark_group("fully_ordered_grouping");
    group.sample_size(10);
    group.warm_up_time(Duration::from_millis(250));
    group.measurement_time(Duration::from_secs(1));
    for strings in [false, true] {
        for batch_size in [127, 8192] {
            for run_length in [1, 8, 128, 8192] {
                let (schema, batches) = inputs(run_length, batch_size, strings);
                let case = format!(
                    "{}_batch{batch_size}_run{run_length}",
                    if strings { "string" } else { "int" }
                );
                let (hashed_bytes, selected_bytes) = check_case(&schema, &batches);
                eprintln!(
                    "group_state_bytes {case} hashed={hashed_bytes} selected={selected_bytes}"
                );
                for selected in [false, true] {
                    let name = if selected { "selected" } else { "hashed" };
                    group.bench_function(BenchmarkId::new(name, &case), |b| {
                        b.iter_batched_ref(
                            || {
                                let values: Box<dyn GroupValues> = if selected {
                                    new_group_values(
                                        Arc::clone(&schema),
                                        &GroupOrdering::try_new(&InputOrderMode::Sorted)
                                            .unwrap(),
                                    )
                                    .unwrap()
                                } else {
                                    Box::new(
                                        GroupValuesColumn::<true>::try_new(Arc::clone(
                                            &schema,
                                        ))
                                        .unwrap(),
                                    )
                                };
                                (values, Vec::new())
                            },
                            |(values, ids)| {
                                for batch in &batches {
                                    values.intern(batch, ids).unwrap();
                                    black_box(&*ids);
                                    let completed = values.len().saturating_sub(1);
                                    if completed > 0 {
                                        black_box(
                                            values
                                                .emit(EmitTo::First(completed))
                                                .unwrap(),
                                        );
                                    }
                                }
                                black_box(values.emit(EmitTo::All).unwrap());
                            },
                            criterion::BatchSize::LargeInput,
                        );
                    });
                }
            }
        }
    }
    group.finish();
}

// Validate the benchmark input and report retained grouping-state memory.
// These counters exclude the shared input and transient kernel allocations;
// they are not process RSS measurements. Neither validation nor reporting is timed.
fn check_case(schema: &SchemaRef, batches: &[Vec<ArrayRef>]) -> (usize, usize) {
    let mut hashed = GroupValuesColumn::<true>::try_new(Arc::clone(schema)).unwrap();
    let mut selected = new_group_values(
        Arc::clone(schema),
        &GroupOrdering::try_new(&InputOrderMode::Sorted).unwrap(),
    )
    .unwrap();
    let mut expected = Vec::new();
    let mut actual = Vec::new();
    let mut hashed_peak = 0;
    let mut selected_peak = 0;
    for batch in batches {
        hashed.intern(batch, &mut expected).unwrap();
        selected.intern(batch, &mut actual).unwrap();
        assert_eq!(actual, expected);
        hashed_peak = hashed_peak.max(hashed.size());
        selected_peak = selected_peak.max(selected.size());
        let completed = hashed.len().saturating_sub(1);
        assert_eq!(
            selected.emit(EmitTo::First(completed)).unwrap(),
            hashed.emit(EmitTo::First(completed)).unwrap()
        );
    }
    assert_eq!(
        selected.emit(EmitTo::All).unwrap(),
        hashed.emit(EmitTo::All).unwrap()
    );
    (hashed_peak, selected_peak)
}

fn aggregate_plan(
    schema: &SchemaRef,
    keys: Vec<Vec<ArrayRef>>,
) -> Arc<dyn ExecutionPlan> {
    let mut fields = schema.fields().to_vec();
    fields.push(Arc::new(Field::new("v", DataType::Int64, false)));
    let schema = Arc::new(Schema::new(fields));
    let batches = keys
        .into_iter()
        .map(|mut cols| {
            cols.push(Arc::new(Int64Array::from(vec![1; cols[0].len()])));
            RecordBatch::try_new(Arc::clone(&schema), cols).unwrap()
        })
        .collect::<Vec<_>>();
    let ordering = LexOrdering::new(vec![
        PhysicalSortExpr::new_default(col("a", &schema).unwrap()),
        PhysicalSortExpr::new_default(col("b", &schema).unwrap()),
    ])
    .unwrap();
    let input =
        TestMemoryExec::try_new_exec(&[batches], Arc::clone(&schema), None).unwrap();
    let input = Arc::new(
        input
            .as_ref()
            .clone()
            .try_with_sort_information(vec![ordering])
            .unwrap(),
    );
    let expr = AggregateExprBuilder::new(sum_udaf(), vec![col("v", &schema).unwrap()])
        .schema(Arc::clone(&schema))
        .alias("sum_v")
        .build()
        .unwrap();
    let plan = AggregateExec::try_new(
        AggregateMode::Single,
        PhysicalGroupBy::new_single(vec![
            (col("a", &schema).unwrap(), "a".into()),
            (col("b", &schema).unwrap(), "b".into()),
        ]),
        vec![Arc::new(expr)],
        vec![None],
        input,
        schema,
    )
    .unwrap();
    assert_eq!(plan.input_order_mode(), &InputOrderMode::Sorted);
    Arc::new(plan)
}

fn aggregation(c: &mut Criterion) {
    let runtime = Runtime::new().unwrap();
    let mut group = c.benchmark_group("fully_ordered_aggregate_exec");
    group.sample_size(10);
    group.warm_up_time(Duration::from_millis(250));
    group.measurement_time(Duration::from_secs(1));
    for strings in [false, true] {
        for run_length in [1, 8, 128, 8192] {
            let (schema, keys) = inputs(run_length, 8192, strings);
            let plan = aggregate_plan(&schema, keys);
            let name =
                format!("{}_run{run_length}", if strings { "string" } else { "int" });
            group.bench_function(name, |b| {
                b.iter(|| {
                    black_box(
                        runtime
                            .block_on(collect(
                                Arc::clone(&plan),
                                Arc::new(TaskContext::default()),
                            ))
                            .unwrap(),
                    )
                });
            });
        }
    }
    group.finish();
}

criterion_group!(benches, grouping, aggregation);
criterion_main!(benches);
