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

use arrow::array::{ArrayRef, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use criterion::{BatchSize, Criterion, criterion_group, criterion_main};
use datafusion_expr::function::AccumulatorArgs;
use datafusion_expr::{AggregateUDFImpl, EmitTo, GroupsAccumulator};
use datafusion_functions_aggregate::any_value::AnyValue;
use datafusion_physical_expr::GroupsAccumulatorAdapter;
use datafusion_physical_expr::expressions::col;

const BATCH_SIZE: usize = 8192;
const NUM_GROUPS: usize = 4096;

fn with_accumulator_args<T>(
    data_type: DataType,
    f: impl FnOnce(AccumulatorArgs<'_>) -> T,
) -> T {
    let schema = Schema::new(vec![Field::new("value", data_type.clone(), true)]);
    let expr = col("value", &schema).unwrap();
    let expr_fields = vec![expr.return_field(&schema).unwrap()];
    let exprs = vec![expr];
    let return_field = Field::new("any_value", data_type, true).into();

    f(AccumulatorArgs {
        return_field,
        schema: &schema,
        expr_fields: &expr_fields,
        ignore_nulls: false,
        order_bys: &[],
        is_reversed: false,
        name: "any_value(value)",
        is_distinct: false,
        exprs: &exprs,
    })
}

fn native_accumulator(data_type: DataType) -> Box<dyn GroupsAccumulator> {
    with_accumulator_args(data_type, |args| {
        AnyValue::new().create_groups_accumulator(args).unwrap()
    })
}

fn adapter_accumulator(data_type: DataType) -> Box<dyn GroupsAccumulator> {
    Box::new(GroupsAccumulatorAdapter::new(move || {
        with_accumulator_args(data_type.clone(), |args| AnyValue::new().accumulator(args))
    }))
}

fn run_grouped(
    accumulator: &mut dyn GroupsAccumulator,
    values: &ArrayRef,
    group_indices: &[usize],
) {
    accumulator
        .update_batch(
            std::slice::from_ref(values),
            group_indices,
            None,
            NUM_GROUPS,
        )
        .unwrap();
    black_box(accumulator.evaluate(EmitTo::All).unwrap());
}

fn benchmark_type(c: &mut Criterion, name: &str, values: &ArrayRef) {
    let group_indices = (0..BATCH_SIZE)
        .map(|row| row % NUM_GROUPS)
        .collect::<Vec<_>>();
    let data_type = values.data_type().clone();

    let mut group = c.benchmark_group(format!("any_value grouped {name}"));
    group.bench_function("native", |b| {
        b.iter_batched(
            || native_accumulator(data_type.clone()),
            |mut accumulator| {
                run_grouped(accumulator.as_mut(), values, &group_indices);
            },
            BatchSize::SmallInput,
        )
    });
    group.bench_function("adapter", |b| {
        b.iter_batched(
            || adapter_accumulator(data_type.clone()),
            |mut accumulator| {
                run_grouped(accumulator.as_mut(), values, &group_indices);
            },
            BatchSize::SmallInput,
        )
    });
    group.finish();
}

fn criterion_benchmark(c: &mut Criterion) {
    let int_values = Arc::new(
        (0..BATCH_SIZE)
            .map(|row| (row % 17 != 0).then_some(row as i64))
            .collect::<Int64Array>(),
    ) as ArrayRef;
    benchmark_type(c, "int64", &int_values);

    let strings = (0..BATCH_SIZE)
        .map(|row| (row % 17 != 0).then(|| format!("value-{row}")))
        .collect::<Vec<_>>();
    let string_values =
        Arc::new(StringArray::from_iter(strings.iter().map(Option::as_deref)))
            as ArrayRef;
    benchmark_type(c, "utf8", &string_values);
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
