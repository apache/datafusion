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

use arrow::array::{ArrayRef, Int64Builder, ListArray};
use arrow::datatypes::{DataType, Field};
use criterion::{
    BenchmarkGroup, Criterion, criterion_group, criterion_main, measurement::WallTime,
};
use datafusion_common::ScalarValue;
use datafusion_common::config::ConfigOptions;
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDF};
use datafusion_functions_nested::replace::{
    array_replace_all_udf, array_replace_n_udf, array_replace_udf,
};
use rand::seq::IndexedRandom;
use rand::{Rng, SeedableRng, rngs::StdRng};
use std::hint::black_box;
use std::sync::Arc;

// (num_rows, list_size) — tuned so benches finish in a few seconds. At
// NEEDLE_DENSITY = 0.1, list_size = 100/500 reliably exercise the
// match-and-replace path (and the counter cutoff for `array_replace_n`);
// list_size = 10 exercises the null-row and no-match early-return paths.
const SIZES: &[(usize, usize)] = &[(4_000, 10), (10_000, 100), (10_000, 500)];
const SEED: u64 = 42;
const HAYSTACK_NULL_DENSITY: f64 = 0.1;
const NEEDLE_DENSITY: f64 = 0.1;
const NEEDLE: i64 = 0;
const REPLACEMENT: i64 = -1;

fn criterion_benchmark(c: &mut Criterion) {
    let list_data_type =
        DataType::List(Arc::new(Field::new_list_field(DataType::Int64, true)));
    let return_field: Arc<Field> =
        Field::new("result", list_data_type.clone(), true).into();
    let config_options = Arc::new(ConfigOptions::default());

    let array_field: Arc<Field> = Field::new("array", list_data_type, true).into();
    let from_field: Arc<Field> = Field::new("from", DataType::Int64, true).into();
    let to_field: Arc<Field> = Field::new("to", DataType::Int64, true).into();
    let max_field: Arc<Field> = Field::new("max", DataType::Int64, true).into();

    let three_arg_fields =
        vec![array_field.clone(), from_field.clone(), to_field.clone()];
    let four_arg_fields = vec![array_field, from_field, to_field, max_field];

    let mut group = c.benchmark_group("array_replace_int64");
    let udf = array_replace_udf();
    for &(num_rows, list_size) in SIZES {
        let fn_args = ScalarFunctionArgs {
            args: vec![
                ColumnarValue::Array(create_list_array(num_rows, list_size)),
                ColumnarValue::Scalar(ScalarValue::from(NEEDLE)),
                ColumnarValue::Scalar(ScalarValue::from(REPLACEMENT)),
            ],
            arg_fields: three_arg_fields.clone(),
            number_rows: num_rows,
            return_field: return_field.clone(),
            config_options: config_options.clone(),
        };
        bench_case(&mut group, &udf, list_size, num_rows, &fn_args);
    }
    group.finish();

    let mut group = c.benchmark_group("array_replace_n_int64");
    let udf = array_replace_n_udf();
    // n = 2 makes the counter cutoff fire on any row with ≥ 2 needles.
    let n = 2_i64;
    for &(num_rows, list_size) in SIZES {
        let fn_args = ScalarFunctionArgs {
            args: vec![
                ColumnarValue::Array(create_list_array(num_rows, list_size)),
                ColumnarValue::Scalar(ScalarValue::from(NEEDLE)),
                ColumnarValue::Scalar(ScalarValue::from(REPLACEMENT)),
                ColumnarValue::Scalar(ScalarValue::from(n)),
            ],
            arg_fields: four_arg_fields.clone(),
            number_rows: num_rows,
            return_field: return_field.clone(),
            config_options: config_options.clone(),
        };
        bench_case(&mut group, &udf, list_size, num_rows, &fn_args);
    }
    group.finish();

    let mut group = c.benchmark_group("array_replace_all_int64");
    let udf = array_replace_all_udf();
    for &(num_rows, list_size) in SIZES {
        let fn_args = ScalarFunctionArgs {
            args: vec![
                ColumnarValue::Array(create_list_array(num_rows, list_size)),
                ColumnarValue::Scalar(ScalarValue::from(NEEDLE)),
                ColumnarValue::Scalar(ScalarValue::from(REPLACEMENT)),
            ],
            arg_fields: three_arg_fields.clone(),
            number_rows: num_rows,
            return_field: return_field.clone(),
            config_options: config_options.clone(),
        };
        bench_case(&mut group, &udf, list_size, num_rows, &fn_args);
    }
    group.finish();
}

fn bench_case(
    group: &mut BenchmarkGroup<'_, WallTime>,
    udf: &Arc<ScalarUDF>,
    list_size: usize,
    num_rows: usize,
    fn_args: &ScalarFunctionArgs,
) {
    let name = format!("list_size: {list_size}, num_rows: {num_rows}");
    group.bench_function(name, |b| {
        b.iter(|| black_box(udf.invoke_with_args(fn_args.clone()).unwrap()))
    });
}

fn create_list_array(num_rows: usize, list_size: usize) -> ArrayRef {
    let filler_values = [None, Some(1_i64), Some(2), Some(3), Some(4), Some(5)];
    let mut rng = StdRng::seed_from_u64(SEED);
    let values = (0..num_rows)
        .map(|_| {
            if rng.random_bool(HAYSTACK_NULL_DENSITY) {
                None
            } else {
                let list = (0..list_size)
                    .map(|_| {
                        if rng.random_bool(NEEDLE_DENSITY) {
                            Some(NEEDLE)
                        } else {
                            *filler_values.choose(&mut rng).unwrap()
                        }
                    })
                    .collect::<Vec<_>>();
                Some(list)
            }
        })
        .collect::<Vec<_>>();
    Arc::new(ListArray::from_nested_iter::<Int64Builder, _, _, _>(values))
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
