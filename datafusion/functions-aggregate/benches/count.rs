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

use arrow::array::{ArrayRef, BooleanArray};
use arrow::datatypes::Int32Type;
use arrow::util::bench_util::{create_boolean_array, create_primitive_array};
use arrow_schema::{DataType, Field, Schema};
use criterion::{black_box, criterion_group, criterion_main, Criterion};
use datafusion_expr::{function::AccumulatorArgs, AggregateUDFImpl, GroupsAccumulator};
use datafusion_functions_aggregate::count::Count;
use datafusion_physical_expr::expressions::col;
use datafusion_physical_expr_common::sort_expr::LexOrderingRef;
use std::sync::Arc;

fn prepare_accumulator() -> Box<dyn GroupsAccumulator> {
    let schema = Arc::new(Schema::new(vec![Field::new("f", DataType::Int32, true)]));
    let accumulator_args = AccumulatorArgs {
        return_type: &DataType::Int64,
        schema: &schema,
        ignore_nulls: false,
        ordering_req: LexOrderingRef::default(),
        is_reversed: false,
        name: "COUNT(f)",
        is_distinct: false,
        exprs: &[col("f", &schema).unwrap()],
    };
    let count_fn = Count::new();

    count_fn
        .create_groups_accumulator(accumulator_args)
        .unwrap()
}

fn convert_to_state_bench(
    c: &mut Criterion,
    name: &str,
    values: ArrayRef,
    opt_filter: Option<&BooleanArray>,
) {
    let accumulator = prepare_accumulator();
    c.bench_function(name, |b| {
        b.iter(|| {
            black_box(
                accumulator
                    .convert_to_state(&[values.clone()], opt_filter)
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
}

criterion_group!(benches, count_benchmark);
criterion_main!(benches);
