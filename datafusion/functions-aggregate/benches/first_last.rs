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

use arrow::array::{ArrayRef, BooleanArray, Int64Array};
use arrow::compute::SortOptions;
use arrow::datatypes::{DataType, Field, Int64Type, Schema};
use arrow::util::bench_util::{create_boolean_array, create_primitive_array};
use datafusion_common::instant::Instant;
use std::hint::black_box;
use std::sync::Arc;

use datafusion_expr::{
    Accumulator, AggregateUDFImpl, EmitTo, GroupsAccumulator, function::AccumulatorArgs,
};
use datafusion_functions_aggregate::first_last::{
    FirstValue, LastValue, TrivialFirstValueAccumulator, TrivialLastValueAccumulator,
};
use datafusion_physical_expr::PhysicalSortExpr;
use datafusion_physical_expr::expressions::col;

use criterion::{BatchSize, Criterion, criterion_group, criterion_main};

fn prepare_groups_accumulator(is_first: bool) -> Box<dyn GroupsAccumulator> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("value", DataType::Int64, true),
        Field::new("ord", DataType::Int64, true),
    ]));

    let order_expr = col("ord", &schema).unwrap();
    let sort_expr = PhysicalSortExpr {
        expr: order_expr,
        options: SortOptions::default(),
    };

    let value_field: Arc<Field> = Field::new("value", DataType::Int64, true).into();
    let accumulator_args = AccumulatorArgs {
        return_field: Arc::clone(&value_field),
        schema: &schema,
        expr_fields: &[value_field],
        ignore_nulls: false,
        order_bys: std::slice::from_ref(&sort_expr),
        is_reversed: false,
        name: if is_first {
            "FIRST_VALUE(value ORDER BY ord)"
        } else {
            "LAST_VALUE(value ORDER BY ord)"
        },
        is_distinct: false,
        exprs: &[col("value", &schema).unwrap()],
    };

    if is_first {
        FirstValue::new()
            .create_groups_accumulator(accumulator_args)
            .unwrap()
    } else {
        LastValue::new()
            .create_groups_accumulator(accumulator_args)
            .unwrap()
    }
}

fn create_trivial_accumulator(
    is_first: bool,
    ignore_nulls: bool,
) -> Box<dyn Accumulator> {
    if is_first {
        Box::new(
            TrivialFirstValueAccumulator::try_new(&DataType::Int64, ignore_nulls)
                .unwrap(),
        )
    } else {
        Box::new(
            TrivialLastValueAccumulator::try_new(&DataType::Int64, ignore_nulls).unwrap(),
        )
    }
}

#[expect(clippy::needless_pass_by_value)]
#[expect(clippy::too_many_arguments)]
fn evaluate_bench(
    c: &mut Criterion,
    is_first: bool,
    emit_to: EmitTo,
    name: &str,
    values: ArrayRef,
    ord: ArrayRef,
    opt_filter: Option<&BooleanArray>,
    num_groups: usize,
) {
    let n = values.len();
    let group_indices: Vec<usize> = (0..n).map(|i| i % num_groups).collect();

    c.bench_function(name, |b| {
        b.iter_batched(
            || {
                let mut accumulator = prepare_groups_accumulator(is_first);
                accumulator
                    .update_batch(
                        &[Arc::clone(&values), Arc::clone(&ord)],
                        &group_indices,
                        opt_filter,
                        num_groups,
                    )
                    .unwrap();
                accumulator
            },
            |mut accumulator| {
                black_box(accumulator.evaluate(emit_to).unwrap());
            },
            BatchSize::SmallInput,
        )
    });
}

#[expect(clippy::needless_pass_by_value)]
fn update_bench(
    c: &mut Criterion,
    is_first: bool,
    name: &str,
    values: ArrayRef,
    ord: ArrayRef,
    opt_filter: Option<&BooleanArray>,
    num_groups: usize,
) {
    let n = values.len();
    let group_indices: Vec<usize> = (0..n).map(|i| i % num_groups).collect();

    // Initialize with worst-case ordering so update_batch forces rows comparison for all groups.
    let worst_ord: ArrayRef = Arc::new(Int64Array::from(vec![
        if is_first {
            i64::MAX
        } else {
            i64::MIN
        };
        n
    ]));

    c.bench_function(name, |b| {
        b.iter_batched(
            || {
                let mut accumulator = prepare_groups_accumulator(is_first);
                accumulator
                    .update_batch(
                        &[Arc::clone(&values), Arc::clone(&worst_ord)],
                        &group_indices,
                        None, // no filter: ensure all groups are initialised
                        num_groups,
                    )
                    .unwrap();
                accumulator
            },
            |mut accumulator| {
                for _ in 0..100 {
                    #[expect(clippy::unit_arg)]
                    black_box(
                        accumulator
                            .update_batch(
                                &[Arc::clone(&values), Arc::clone(&ord)],
                                &group_indices,
                                opt_filter,
                                num_groups,
                            )
                            .unwrap(),
                    );
                }
            },
            BatchSize::SmallInput,
        )
    });
}

#[expect(clippy::needless_pass_by_value)]
fn merge_bench(
    c: &mut Criterion,
    is_first: bool,
    name: &str,
    values: ArrayRef,
    ord: ArrayRef,
    opt_filter: Option<&BooleanArray>,
    num_groups: usize,
) {
    let n = values.len();
    let group_indices: Vec<usize> = (0..n).map(|i| i % num_groups).collect();
    let is_set: ArrayRef = Arc::new(BooleanArray::from(vec![true; n]));

    // Initialize with worst-case ordering so update_batch forces rows comparison for all groups.
    let worst_ord: ArrayRef = Arc::new(Int64Array::from(vec![
        if is_first {
            i64::MAX
        } else {
            i64::MIN
        };
        n
    ]));

    c.bench_function(name, |b| {
        b.iter_batched(
            || {
                // Prebuild accumulator
                let mut accumulator = prepare_groups_accumulator(is_first);
                accumulator
                    .update_batch(
                        &[Arc::clone(&values), Arc::clone(&worst_ord)],
                        &group_indices,
                        opt_filter,
                        num_groups,
                    )
                    .unwrap();
                accumulator
            },
            |mut accumulator| {
                for _ in 0..100 {
                    #[expect(clippy::unit_arg)]
                    black_box(
                        accumulator
                            .merge_batch(
                                &[
                                    Arc::clone(&values),
                                    Arc::clone(&ord),
                                    Arc::clone(&is_set),
                                ],
                                &group_indices,
                                opt_filter,
                                num_groups,
                            )
                            .unwrap(),
                    );
                }
            },
            BatchSize::SmallInput,
        )
    });
}

#[expect(clippy::needless_pass_by_value)]
fn trivial_update_bench(
    c: &mut Criterion,
    is_first: bool,
    ignore_nulls: bool,
    name: &str,
    values: ArrayRef,
) {
    c.bench_function(name, |b| {
        b.iter_custom(|iters| {
            // The bench is way too fast, so apply scaling factor
            let mut accumulators: Vec<Box<dyn Accumulator>> = (0..iters * 100)
                .map(|_| create_trivial_accumulator(is_first, ignore_nulls))
                .collect();
            let start = Instant::now();
            for acc in &mut accumulators {
                #[expect(clippy::unit_arg)]
                black_box(acc.update_batch(&[Arc::clone(&values)]).unwrap());
            }
            start.elapsed()
        })
    });
}

fn first_last_benchmark(c: &mut Criterion) {
    const N: usize = 65536;
    const NUM_GROUPS: usize = 1024;

    assert_eq!(N % NUM_GROUPS, 0);

    for is_first in [true, false] {
        for pct in [0, 90] {
            let fn_name = if is_first {
                "first_value"
            } else {
                "last_value"
            };

            let null_density = (pct as f32) / 100.0;
            let values = Arc::new(create_primitive_array::<Int64Type>(N, null_density))
                as ArrayRef;
            let ord = Arc::new(create_primitive_array::<Int64Type>(N, null_density))
                as ArrayRef;

            for with_filter in [false, true] {
                let filter = create_boolean_array(N, 0.0, 0.5);
                let opt_filter = if with_filter { Some(&filter) } else { None };

                evaluate_bench(
                    c,
                    is_first,
                    EmitTo::First(2),
                    &format!(
                        "{fn_name} evaluate_bench nulls={pct}%, filter={with_filter}, first(2)"
                    ),
                    values.clone(),
                    ord.clone(),
                    opt_filter,
                    NUM_GROUPS,
                );
                evaluate_bench(
                    c,
                    is_first,
                    EmitTo::All,
                    &format!(
                        "{fn_name} evaluate_bench nulls={pct}%, filter={with_filter}, all"
                    ),
                    values.clone(),
                    ord.clone(),
                    opt_filter,
                    NUM_GROUPS,
                );

                update_bench(
                    c,
                    is_first,
                    &format!("{fn_name} update_bench nulls={pct}%, filter={with_filter}"),
                    values.clone(),
                    ord.clone(),
                    opt_filter,
                    NUM_GROUPS,
                );
                merge_bench(
                    c,
                    is_first,
                    &format!("{fn_name} merge_bench nulls={pct}%, filter={with_filter}"),
                    values.clone(),
                    ord.clone(),
                    opt_filter,
                    NUM_GROUPS,
                );
            }

            for ignore_nulls in [false, true] {
                trivial_update_bench(
                    c,
                    is_first,
                    ignore_nulls,
                    &format!(
                        "{fn_name} trivial_update_bench nulls={pct}%, ignore_nulls={ignore_nulls}"
                    ),
                    values.clone(),
                );
            }
        }
    }
}

criterion_group!(benches, first_last_benchmark);
criterion_main!(benches);
