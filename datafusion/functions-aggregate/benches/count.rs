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
use arrow::datatypes::{DataType, Field, Int32Type, Schema};
use arrow::util::bench_util::{
    create_boolean_array, create_dict_from_values, create_primitive_array,
    create_string_array_with_len,
};

use datafusion_expr::function::AccumulatorArgs;
use datafusion_expr::{Accumulator, AggregateUDFImpl, GroupsAccumulator};
use datafusion_functions_aggregate::count::Count;
use datafusion_physical_expr::expressions::col;

use criterion::{Criterion, criterion_group, criterion_main};

fn prepare_group_accumulator() -> Box<dyn GroupsAccumulator> {
    let schema = Arc::new(Schema::new(vec![Field::new("f", DataType::Int32, true)]));
    let expr = col("f", &schema).unwrap();
    let accumulator_args = AccumulatorArgs {
        return_field: Field::new("f", DataType::Int64, true).into(),
        schema: &schema,
        expr_fields: &[expr.return_field(&schema).unwrap()],
        ignore_nulls: false,
        order_bys: &[],
        is_reversed: false,
        name: "COUNT(f)",
        is_distinct: false,
        exprs: &[expr],
    };
    let count_fn = Count::new();

    count_fn
        .create_groups_accumulator(accumulator_args)
        .unwrap()
}

fn prepare_accumulator() -> Box<dyn Accumulator> {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "f",
        DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
        true,
    )]));
    let expr = col("f", &schema).unwrap();
    let accumulator_args = AccumulatorArgs {
        return_field: Arc::new(Field::new_list_field(DataType::Int64, true)),
        schema: &schema,
        expr_fields: &[expr.return_field(&schema).unwrap()],
        ignore_nulls: false,
        order_bys: &[],
        is_reversed: false,
        name: "COUNT(f)",
        is_distinct: true,
        exprs: &[expr],
    };
    let count_fn = Count::new();

    count_fn.accumulator(accumulator_args).unwrap()
}

#[expect(clippy::needless_pass_by_value)]
fn convert_to_state_bench(
    c: &mut Criterion,
    name: &str,
    values: ArrayRef,
    opt_filter: Option<&BooleanArray>,
) {
    let accumulator = prepare_group_accumulator();
    c.bench_function(name, |b| {
        b.iter(|| {
            black_box(
                accumulator
                    .convert_to_state(std::slice::from_ref(&values), opt_filter)
                    .unwrap(),
            )
        })
    });
}

fn count_benchmark(c: &mut Criterion) {
    let values = Arc::new(create_primitive_array::<Int32Type>(8192, 0.0)) as ArrayRef;
    convert_to_state_bench(c, "count convert state no nulls, no filter", values, None);

    let values = Arc::new(create_primitive_array::<Int32Type>(8192, 0.3)) as ArrayRef;
    convert_to_state_bench(c, "count convert state 30% nulls, no filter", values, None);

    let values = Arc::new(create_primitive_array::<Int32Type>(8192, 0.3)) as ArrayRef;
    convert_to_state_bench(c, "count convert state 70% nulls, no filter", values, None);

    let values = Arc::new(create_primitive_array::<Int32Type>(8192, 0.0)) as ArrayRef;
    let filter = create_boolean_array(8192, 0.0, 0.5);
    convert_to_state_bench(
        c,
        "count convert state no nulls, filter",
        values,
        Some(&filter),
    );

    let values = Arc::new(create_primitive_array::<Int32Type>(8192, 0.3)) as ArrayRef;
    let filter = create_boolean_array(8192, 0.0, 0.5);
    convert_to_state_bench(
        c,
        "count convert state nulls, filter",
        values,
        Some(&filter),
    );

    let arr = create_string_array_with_len::<i32>(20, 0.0, 50);
    let values =
        Arc::new(create_dict_from_values::<Int32Type>(200_000, 0.8, &arr)) as ArrayRef;

    let mut accumulator = prepare_accumulator();
    c.bench_function("count low cardinality dict 20% nulls, no filter", |b| {
        b.iter(|| {
            #[expect(clippy::unit_arg)]
            black_box(
                accumulator
                    .update_batch(std::slice::from_ref(&values))
                    .unwrap(),
            )
        })
    });
}

criterion_group!(benches, count_benchmark);
criterion_main!(benches);
