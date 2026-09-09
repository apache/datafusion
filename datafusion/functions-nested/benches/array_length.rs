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

use arrow::array::{ArrayRef, FixedSizeListArray, Int32Array, ListArray};
use arrow::buffer::OffsetBuffer;
use arrow::datatypes::{DataType, Field};
use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use datafusion_common::config::ConfigOptions;
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs};
use datafusion_functions_nested::length::array_length_udf;
use std::hint::black_box;
use std::sync::Arc;

fn bench_array_length(c: &mut Criterion) {
    let mut group = c.benchmark_group("array_length");
    let udf = array_length_udf();
    let return_field = Arc::new(Field::new("length", DataType::UInt64, true));
    let config_options = Arc::new(ConfigOptions::default());

    let rows = 8192;
    let width = 32;
    let values = Arc::new(Int32Array::new_null(rows * width)) as ArrayRef;
    let field = Arc::new(Field::new_list_field(DataType::Int32, true));
    let flat = Arc::new(ListArray::new(
        Arc::clone(&field),
        OffsetBuffer::from_repeated_length(width, rows),
        Arc::clone(&values),
        None,
    )) as ArrayRef;
    let fixed =
        Arc::new(FixedSizeListArray::new(field, width as i32, values, None)) as ArrayRef;

    for (name, array) in [
        ("list", Arc::clone(&flat)),
        ("fixed_size_list", fixed),
        ("list", flat.slice(0, 1)),
    ] {
        let number_rows = array.len();
        let id = BenchmarkId::new(name, number_rows);
        let args = vec![ColumnarValue::Array(array)];
        let arg_fields: Vec<_> = args
            .iter()
            .map(|arg| Arc::new(Field::new("arg", arg.data_type(), true)))
            .collect();
        group.bench_function(id, |b| {
            b.iter(|| {
                black_box(
                    udf.invoke_with_args(ScalarFunctionArgs {
                        args: args.clone(),
                        arg_fields: arg_fields.clone(),
                        number_rows,
                        return_field: Arc::clone(&return_field),
                        config_options: Arc::clone(&config_options),
                    })
                    .unwrap(),
                )
            });
        });
    }
    group.finish();
}

criterion_group!(benches, bench_array_length);
criterion_main!(benches);
