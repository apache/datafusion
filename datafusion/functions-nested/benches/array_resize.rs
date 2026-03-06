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

use arrow::array::{ArrayRef, Int64Array, ListArray};
use arrow::buffer::OffsetBuffer;
use arrow::datatypes::{DataType, Field};
use criterion::{
    BenchmarkGroup, Criterion, criterion_group, criterion_main, measurement::WallTime,
};
use datafusion_common::config::ConfigOptions;
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl};
use datafusion_functions_nested::resize::ArrayResize;
use std::hint::black_box;
use std::sync::Arc;

const NUM_ROWS: usize = 1_000;

fn criterion_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("array_resize_i64");
    let list_field: Arc<Field> = Field::new_list_field(DataType::Int64, true).into();
    let list_data_type = DataType::List(Arc::clone(&list_field));
    let arg_fields = vec![
        Field::new("array", list_data_type.clone(), true).into(),
        Field::new("size", DataType::Int64, false).into(),
        Field::new("value", DataType::Int64, true).into(),
    ];
    let return_field: Arc<Field> = Field::new("result", list_data_type, true).into();
    let config_options = Arc::new(ConfigOptions::default());
    let two_arg_fields = arg_fields[..2].to_vec();

    bench_case(
        &mut group,
        "grow_uniform_fill_10_to_500",
        &[
            ColumnarValue::Array(create_int64_list_array(NUM_ROWS, 10)),
            ColumnarValue::Array(repeated_int64_array(500)),
            ColumnarValue::Array(repeated_int64_array(7)),
        ],
        &arg_fields,
        &return_field,
        &config_options,
    );

    bench_case(
        &mut group,
        "shrink_uniform_fill_500_to_10",
        &[
            ColumnarValue::Array(create_int64_list_array(NUM_ROWS, 500)),
            ColumnarValue::Array(repeated_int64_array(10)),
            ColumnarValue::Array(repeated_int64_array(7)),
        ],
        &arg_fields,
        &return_field,
        &config_options,
    );

    bench_case(
        &mut group,
        "grow_default_null_fill_10_to_500",
        &[
            ColumnarValue::Array(create_int64_list_array(NUM_ROWS, 10)),
            ColumnarValue::Array(repeated_int64_array(500)),
        ],
        &two_arg_fields,
        &return_field,
        &config_options,
    );

    bench_case(
        &mut group,
        "grow_variable_fill_10_to_500",
        &[
            ColumnarValue::Array(create_int64_list_array(NUM_ROWS, 10)),
            ColumnarValue::Array(repeated_int64_array(500)),
            ColumnarValue::Array(distinct_fill_array()),
        ],
        &arg_fields,
        &return_field,
        &config_options,
    );

    bench_case(
        &mut group,
        "mixed_grow_shrink_1000x_100",
        &[
            ColumnarValue::Array(create_int64_list_array(NUM_ROWS, 100)),
            ColumnarValue::Array(mixed_size_array()),
        ],
        &arg_fields[..2],
        &return_field,
        &config_options,
    );

    group.finish();
}

fn bench_case(
    group: &mut BenchmarkGroup<'_, WallTime>,
    name: &str,
    args: &[ColumnarValue],
    arg_fields: &[Arc<Field>],
    return_field: &Arc<Field>,
    config_options: &Arc<ConfigOptions>,
) {
    let udf = ArrayResize::new();
    group.bench_function(name, |b| {
        b.iter(|| {
            black_box(
                udf.invoke_with_args(ScalarFunctionArgs {
                    args: args.to_vec(),
                    arg_fields: arg_fields.to_vec(),
                    number_rows: NUM_ROWS,
                    return_field: return_field.clone(),
                    config_options: config_options.clone(),
                })
                .unwrap(),
            )
        })
    });
}

fn create_int64_list_array(num_rows: usize, list_len: usize) -> ArrayRef {
    let values = (0..(num_rows * list_len))
        .map(|v| Some(v as i64))
        .collect::<Int64Array>();
    let offsets = (0..=num_rows)
        .map(|i| (i * list_len) as i32)
        .collect::<Vec<i32>>();

    Arc::new(
        ListArray::try_new(
            Arc::new(Field::new_list_field(DataType::Int64, true)),
            OffsetBuffer::new(offsets.into()),
            Arc::new(values),
            None,
        )
        .unwrap(),
    )
}

fn repeated_int64_array(value: i64) -> ArrayRef {
    Arc::new(Int64Array::from_value(value, NUM_ROWS))
}

fn distinct_fill_array() -> ArrayRef {
    Arc::new(Int64Array::from_iter((0..NUM_ROWS).map(|i| Some(i as i64))))
}

fn mixed_size_array() -> ArrayRef {
    Arc::new(Int64Array::from_iter(
        (0..NUM_ROWS).map(|i| Some(if i % 2 == 0 { 200_i64 } else { 10_i64 })),
    ))
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
