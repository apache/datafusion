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

use arrow::array::ArrayRef;
use arrow::datatypes::{Field, Float32Type, Float64Type};
use arrow::util::bench_util::create_primitive_array;
use criterion::{Criterion, criterion_group};
use datafusion_common::config::ConfigOptions;
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs};
use datafusion_functions::math::{degrees, sqrt};

const BATCH_SIZE: usize = 8192;

fn criterion_benchmark(c: &mut Criterion) {
    let config_options = Arc::new(ConfigOptions::default());

    // Both functions are cheap enough that overhead in the code they share
    // shows up: `sqrt` returns an error for invalid inputs, `degrees` doesn't.
    for function in [sqrt(), degrees()] {
        let mut group = c.benchmark_group(function.name().to_string());

        for (nulls, null_density) in [("", 0.0), (" with nulls", 0.1)] {
            let arrays: [(&str, ArrayRef); 2] = [
                (
                    "f64",
                    Arc::new(create_primitive_array::<Float64Type>(
                        BATCH_SIZE,
                        null_density,
                    )),
                ),
                (
                    "f32",
                    Arc::new(create_primitive_array::<Float32Type>(
                        BATCH_SIZE,
                        null_density,
                    )),
                ),
            ];

            for (type_name, array) in arrays {
                let field = Arc::new(Field::new("a", array.data_type().clone(), true));
                let args = vec![ColumnarValue::Array(array)];

                group.bench_function(format!("{type_name}{nulls}"), |b| {
                    b.iter(|| {
                        black_box(
                            function
                                .invoke_with_args(ScalarFunctionArgs {
                                    args: args.clone(),
                                    arg_fields: vec![Arc::clone(&field)],
                                    number_rows: BATCH_SIZE,
                                    return_field: Arc::clone(&field),
                                    config_options: Arc::clone(&config_options),
                                })
                                .unwrap(),
                        )
                    })
                });
            }
        }

        group.finish();
    }
}

criterion_group!(benches, criterion_benchmark);
