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
    Array, ArrayRef, BinaryArray, BinaryViewArray, Int32Array, ListArray, MapArray,
    StringArray, StringViewArray, StructArray,
};
use arrow::buffer::{OffsetBuffer, ScalarBuffer};
use arrow::datatypes::{DataType, Field};
use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use datafusion_common::ScalarValue;
use datafusion_common::config::ConfigOptions;
use datafusion_expr::planner::ExprPlanner;
use datafusion_expr::{ColumnarValue, Expr, ScalarFunctionArgs};
use datafusion_functions_nested::map::map_udf;
use datafusion_functions_nested::map_extract::map_extract_udf;
use datafusion_functions_nested::planner::NestedFunctionPlanner;
use rand::prelude::*;
use std::collections::HashSet;
use std::hash::Hash;
use std::hint::black_box;
use std::sync::Arc;

const MAP_ROWS: usize = 1000;
const MAP_KEYS_PER_ROW: usize = 1000;

fn gen_unique_values<T>(rng: &mut StdRng, mut make_value: impl FnMut(i32) -> T) -> Vec<T>
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

fn gen_utf8_values(rng: &mut StdRng) -> Vec<String> {
    gen_unique_values(rng, |value| value.to_string())
}

fn gen_binary_values(rng: &mut StdRng) -> Vec<Vec<u8>> {
    gen_unique_values(rng, |value| value.to_le_bytes().to_vec())
}

fn gen_primitive_values(rng: &mut StdRng) -> Vec<i32> {
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
        let mut rng = StdRng::seed_from_u64(0);
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

    let mut rng = StdRng::seed_from_u64(0);
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

fn bench_map_extract(c: &mut Criterion) {
    let udf = map_extract_udf();
    let config_options = Arc::new(ConfigOptions::default());
    let mut group = c.benchmark_group("map_extract");

    // Cases are named `{key type}/{lookup}/{rows}x{entries}`. The single-row
    // shapes measure per-batch fixed cost. `shuffled` looks up a key that
    // every row holds at a different position, and `varying` looks up a
    // different key per row, mixing matches and misses.
    let shapes: &[(usize, usize, &[&str])] = &[
        (1, 0, &["last"]),
        (1, 1, &["last"]),
        (1024, 4, &["last", "shuffled", "missing", "varying"]),
        (
            1024,
            32,
            &["first", "last", "shuffled", "missing", "varying"],
        ),
    ];
    for &(rows, width, lookups) in shapes {
        let key_types: &[&str] = if rows == 1 {
            &["int32"]
        } else {
            &["int32", "utf8_view", "struct"]
        };
        for &key_type in key_types {
            let make_keys = |keys: Vec<i32>| -> ArrayRef {
                match key_type {
                    "int32" => Arc::new(Int32Array::from(keys)),
                    "utf8_view" => Arc::new(StringViewArray::from_iter_values(
                        keys.iter().map(|key| format!("key_{key:016}")),
                    )),
                    "struct" => Arc::new(StructArray::from(vec![(
                        Arc::new(Field::new("key", DataType::Int32, false)),
                        Arc::new(Int32Array::from(keys)) as ArrayRef,
                    )])),
                    _ => unreachable!(),
                }
            };
            // Every row holds the keys `0..width`. With `shuffled`, each
            // row's entries are rotated by the row number.
            let make_map = |shuffled: bool| -> ArrayRef {
                let keys = (0..rows)
                    .flat_map(|row| {
                        (0..width).map(move |position| {
                            if shuffled {
                                ((position + row) % width) as i32
                            } else {
                                position as i32
                            }
                        })
                    })
                    .collect();
                let keys = make_keys(keys);
                let entries = StructArray::from(vec![
                    (
                        Arc::new(Field::new("key", keys.data_type().clone(), false)),
                        keys,
                    ),
                    (
                        Arc::new(Field::new("value", DataType::Int32, false)),
                        Arc::new(Int32Array::from_iter_values(0..(rows * width) as i32))
                            as ArrayRef,
                    ),
                ]);
                Arc::new(MapArray::new(
                    Arc::new(Field::new("entries", entries.data_type().clone(), false)),
                    OffsetBuffer::from_lengths(std::iter::repeat_n(width, rows)),
                    entries,
                    None,
                    false,
                ))
            };
            let map = make_map(false);
            let shuffled_map = make_map(true);
            for &lookup in lookups {
                let (map, query_keys) = match lookup {
                    "first" => (&map, vec![0]),
                    "last" => (&map, vec![width.saturating_sub(1) as i32]),
                    "shuffled" => (&shuffled_map, vec![0]),
                    "missing" => (&map, vec![width as i32]),
                    "varying" => (
                        &map,
                        (0..rows).map(|row| (row % (width + 1)) as i32).collect(),
                    ),
                    _ => unreachable!(),
                };
                let query_keys = make_keys(query_keys);
                let query_keys = if lookup == "varying" {
                    ColumnarValue::Array(query_keys)
                } else {
                    ColumnarValue::Scalar(
                        ScalarValue::try_from_array(&query_keys, 0).unwrap(),
                    )
                };
                let args = vec![ColumnarValue::Array(Arc::clone(map)), query_keys];
                let arg_fields = args
                    .iter()
                    .map(|arg| Field::new("arg", arg.data_type(), true).into())
                    .collect::<Vec<_>>();
                let return_type = udf
                    .return_type(
                        &args
                            .iter()
                            .map(ColumnarValue::data_type)
                            .collect::<Vec<_>>(),
                    )
                    .unwrap();
                let return_field = Arc::new(Field::new("result", return_type, true));
                group.bench_function(
                    BenchmarkId::new(
                        format!("{key_type}/{lookup}"),
                        format!("{rows}x{width}"),
                    ),
                    |b| {
                        b.iter(|| {
                            black_box(
                                udf.invoke_with_args(ScalarFunctionArgs {
                                    args: args.clone(),
                                    arg_fields: arg_fields.clone(),
                                    number_rows: rows,
                                    return_field: Arc::clone(&return_field),
                                    config_options: Arc::clone(&config_options),
                                })
                                .unwrap(),
                            )
                        });
                    },
                );
            }
        }
    }
    group.finish();
}

criterion_group!(benches, criterion_benchmark, bench_map_extract);
criterion_main!(benches);
