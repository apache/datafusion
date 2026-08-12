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

use arrow::array::{
    ArrayRef, BinaryArray, BinaryViewArray, Int32Array, ListArray, StringArray,
    StringViewArray,
};
use arrow::buffer::{OffsetBuffer, ScalarBuffer};
use arrow::datatypes::Field;
use criterion::{Criterion, criterion_group, criterion_main};
use datafusion_common::ScalarValue;
use datafusion_common::config::ConfigOptions;
use datafusion_expr::planner::ExprPlanner;
use datafusion_expr::{ColumnarValue, Expr, ScalarFunctionArgs};
use datafusion_functions_nested::map::map_udf;
use datafusion_functions_nested::planner::NestedFunctionPlanner;
use rand::Rng;
use rand::prelude::ThreadRng;
use std::collections::HashSet;
use std::hash::Hash;
use std::hint::black_box;
use std::sync::Arc;

const MAP_ROWS: usize = 1000;
const MAP_KEYS_PER_ROW: usize = 1000;

fn gen_unique_values<T>(
    rng: &mut ThreadRng,
    mut make_value: impl FnMut(i32) -> T,
) -> Vec<T>
where
    T: Eq + Hash,
{
    let mut values = HashSet::with_capacity(MAP_KEYS_PER_ROW);

    while values.len() < MAP_KEYS_PER_ROW {
        values.insert(make_value(rng.random_range(0..10000)));
    }

    values.into_iter().collect()
}

fn gen_repeat_values<T: Clone>(values: &[T], repeats: usize) -> Vec<T> {
    let mut repeated = Vec::with_capacity(values.len() * repeats);

    for _ in 0..repeats {
        repeated.extend_from_slice(values);
    }

    repeated
}

fn gen_utf8_values(rng: &mut ThreadRng) -> Vec<String> {
    gen_unique_values(rng, |value| value.to_string())
}

fn gen_binary_values(rng: &mut ThreadRng) -> Vec<Vec<u8>> {
    gen_unique_values(rng, |value| value.to_le_bytes().to_vec())
}

fn gen_primitive_values(rng: &mut ThreadRng) -> Vec<i32> {
    gen_unique_values(rng, |value| value)
}

fn list_array(values: ArrayRef, row_count: usize, values_per_row: usize) -> ArrayRef {
    let offsets = (0..=row_count)
        .map(|index| (index * values_per_row) as i32)
        .collect::<Vec<_>>();
    Arc::new(ListArray::new(
        Arc::new(Field::new_list_field(values.data_type().clone(), true)),
        OffsetBuffer::new(ScalarBuffer::from(offsets)),
        values,
        None,
    ))
}

fn bench_map_case(c: &mut Criterion, name: &str, keys: ArrayRef, values: ArrayRef) {
    let number_rows = keys.len();
    let keys = ColumnarValue::Array(keys);
    let values = ColumnarValue::Array(values);

    let return_type = map_udf()
        .return_type(&[keys.data_type(), values.data_type()])
        .expect("should get return type");
    let arg_fields = vec![
        Field::new("a", keys.data_type(), true).into(),
        Field::new("a", values.data_type(), true).into(),
    ];
    let return_field = Field::new("f", return_type, true).into();
    let config_options = Arc::new(ConfigOptions::default());

    c.bench_function(name, |b| {
        b.iter(|| {
            black_box(
                map_udf()
                    .invoke_with_args(ScalarFunctionArgs {
                        args: vec![keys.clone(), values.clone()],
                        arg_fields: arg_fields.clone(),
                        number_rows,
                        return_field: Arc::clone(&return_field),
                        config_options: Arc::clone(&config_options),
                    })
                    .expect("map should work on valid values"),
            );
        });
    });
}

fn criterion_benchmark(c: &mut Criterion) {
    c.bench_function("make_map_1000", |b| {
        let mut rng = rand::rng();
        let keys = gen_utf8_values(&mut rng);
        let values = gen_primitive_values(&mut rng);
        let mut buffer = Vec::new();
        for i in 0..1000 {
            buffer.push(Expr::Literal(
                ScalarValue::Utf8(Some(keys[i].clone())),
                None,
            ));
            buffer.push(Expr::Literal(ScalarValue::Int32(Some(values[i])), None));
        }
        let planner = NestedFunctionPlanner {};
        b.iter(|| {
            black_box(
                planner
                    .plan_make_map(buffer.clone())
                    .expect("map should work on valid values"),
            );
        });
    });

    let mut rng = rand::rng();
    let values = Arc::new(Int32Array::from(gen_repeat_values(
        &gen_primitive_values(&mut rng),
        MAP_ROWS,
    ))) as ArrayRef;
    let values = list_array(values, MAP_ROWS, MAP_KEYS_PER_ROW);
    let map_cases = [
        (
            "map_1000_utf8",
            list_array(
                Arc::new(StringArray::from(gen_repeat_values(
                    &gen_utf8_values(&mut rng),
                    MAP_ROWS,
                ))) as ArrayRef,
                MAP_ROWS,
                MAP_KEYS_PER_ROW,
            ),
        ),
        (
            "map_1000_binary",
            list_array(
                Arc::new(BinaryArray::from_iter_values(gen_repeat_values(
                    &gen_binary_values(&mut rng),
                    MAP_ROWS,
                ))) as ArrayRef,
                MAP_ROWS,
                MAP_KEYS_PER_ROW,
            ),
        ),
        (
            "map_1000_utf8_view",
            list_array(
                Arc::new(StringViewArray::from(gen_repeat_values(
                    &gen_utf8_values(&mut rng),
                    MAP_ROWS,
                ))) as ArrayRef,
                MAP_ROWS,
                MAP_KEYS_PER_ROW,
            ),
        ),
        (
            "map_1000_binary_view",
            list_array(
                Arc::new(BinaryViewArray::from_iter_values(gen_repeat_values(
                    &gen_binary_values(&mut rng),
                    MAP_ROWS,
                ))) as ArrayRef,
                MAP_ROWS,
                MAP_KEYS_PER_ROW,
            ),
        ),
        (
            "map_1000_int32",
            list_array(
                Arc::new(Int32Array::from(gen_repeat_values(
                    &gen_primitive_values(&mut rng),
                    MAP_ROWS,
                ))) as ArrayRef,
                MAP_ROWS,
                MAP_KEYS_PER_ROW,
            ),
        ),
    ];

    for (name, keys) in map_cases {
        bench_map_case(c, name, keys, Arc::clone(&values));
    }
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
