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

use std::hint::black_box;
use std::sync::Arc;

use arrow::array::{ArrayRef, BooleanArray};
use arrow::compute::SortOptions;
use arrow::datatypes::{DataType, Field, Int64Type, Schema};
use arrow::util::bench_util::{create_boolean_array, create_primitive_array};

use datafusion_expr::{
    Accumulator, AggregateUDFImpl, EmitTo, GroupsAccumulator, function::AccumulatorArgs,
};
use datafusion_functions_aggregate::first_last::{FirstValue, LastValue};
use datafusion_physical_expr::PhysicalSortExpr;
use datafusion_physical_expr::expressions::col;

use criterion::{Criterion, criterion_group, criterion_main};

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

fn prepare_accumulator(is_first: bool) -> Box<dyn Accumulator> {
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
        FirstValue::new().accumulator(accumulator_args).unwrap()
    } else {
        LastValue::new().accumulator(accumulator_args).unwrap()
    }
}

#[expect(clippy::needless_pass_by_value)]
fn convert_to_state_bench(
    c: &mut Criterion,
    is_first: bool,
    name: &str,
    values: ArrayRef,
    opt_filter: Option<&BooleanArray>,
) {
    c.bench_function(name, |b| {
        b.iter(|| {
            let accumulator = prepare_groups_accumulator(is_first);
            black_box(
                accumulator
                    .convert_to_state(std::slice::from_ref(&values), opt_filter)
                    .unwrap(),
            )
        })
    });
}

#[expect(clippy::needless_pass_by_value)]
fn evaluate_accumulator_bench(
    c: &mut Criterion,
    is_first: bool,
    name: &str,
    values: ArrayRef,
    ord: ArrayRef,
) {
    c.bench_function(name, |b| {
        b.iter_batched(
            || {
                // setup, not timed
                let mut accumulator = prepare_accumulator(is_first);
                accumulator
                    .update_batch(&[Arc::clone(&values), Arc::clone(&ord)])
                    .unwrap();
                accumulator
            },
            |mut accumulator| black_box(accumulator.evaluate().unwrap()),
            criterion::BatchSize::SmallInput,
        )
    });
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
                // setup, not timed
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
            |mut accumulator| black_box(accumulator.evaluate(emit_to).unwrap()),
            criterion::BatchSize::SmallInput,
        )
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

            evaluate_accumulator_bench(
                c,
                is_first,
                &format!("{fn_name} evaluate_accumulator_bench nulls={pct}%"),
                values.clone(),
                ord.clone(),
            );

            for with_filter in [false, true] {
                let filter = create_boolean_array(N, 0.0, 0.5);
                let opt_filter = if with_filter { Some(&filter) } else { None };

                convert_to_state_bench(
                    c,
                    is_first,
                    &format!(
                        "{fn_name} convert_to_state nulls={pct}%, filter={with_filter}"
                    ),
                    values.clone(),
                    opt_filter,
                );
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
            }
        }
    }
}

criterion_group!(benches, first_last_benchmark);
criterion_main!(benches);
