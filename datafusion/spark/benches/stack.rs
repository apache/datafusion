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

use arrow::datatypes::{Field, Int64Type};
use arrow::util::bench_util::{
    create_boolean_array_with_seed, create_primitive_array_with_seed,
    create_string_array_with_len_range_and_prefix_and_seed,
};
use criterion::{Criterion, criterion_group, criterion_main};
use datafusion_common::ScalarValue;
use datafusion_common::config::ConfigOptions;
use datafusion_expr::{ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs};
use datafusion_spark::function::generator::stack;
use std::hint::black_box;
use std::sync::Arc;

const SIZE: usize = 8192;

fn bench_stack(c: &mut Criterion, name: &str, n: i32, values: Vec<ColumnarValue>) {
    let stack = stack();
    let n = ScalarValue::Int32(Some(n));
    let args = std::iter::once(ColumnarValue::Scalar(n.clone()))
        .chain(values)
        .collect::<Vec<_>>();
    let arg_fields = args
        .iter()
        .enumerate()
        .map(|(idx, arg)| Field::new(format!("arg_{idx}"), arg.data_type(), true).into())
        .collect::<Vec<_>>();
    let scalar_arguments = std::iter::once(Some(&n))
        .chain(std::iter::repeat_n(None, args.len() - 1))
        .collect::<Vec<_>>();
    let return_field = stack
        .return_field_from_args(ReturnFieldArgs {
            arg_fields: &arg_fields,
            scalar_arguments: &scalar_arguments,
        })
        .unwrap();
    let config_options = Arc::new(ConfigOptions::default());
    c.bench_function(name, |b| {
        b.iter(|| {
            black_box(
                stack
                    .invoke_with_args(ScalarFunctionArgs {
                        args: args.clone(),
                        arg_fields: arg_fields.clone(),
                        number_rows: SIZE,
                        return_field: Arc::clone(&return_field),
                        config_options: Arc::clone(&config_options),
                    })
                    .unwrap(),
            )
        })
    });
}

fn criterion_benchmark(c: &mut Criterion) {
    // One seed per column, so the columns differ
    let ints = (0..4)
        .map(|seed| create_primitive_array_with_seed::<Int64Type>(SIZE, 0.1, seed))
        .map(|array| ColumnarValue::Array(Arc::new(array)))
        .collect::<Vec<_>>();
    let strings = (0..2)
        .map(|seed| {
            create_string_array_with_len_range_and_prefix_and_seed::<i32>(
                SIZE, 0.1, 12, 12, "", seed,
            )
        })
        .map(|array| ColumnarValue::Array(Arc::new(array)))
        .collect::<Vec<_>>();
    let booleans = (0..2)
        .map(|seed| create_boolean_array_with_seed(SIZE, 0.1, 0.5, seed))
        .map(|array| ColumnarValue::Array(Arc::new(array)))
        .collect::<Vec<_>>();
    let key = |k: &str| ColumnarValue::Scalar(ScalarValue::from(k));

    // stack(4, 'k1', c1, 'k2', c2, 'k3', c3, 'k4', c4): unpivot 4 columns
    let unpivot = ["k1", "k2", "k3", "k4"]
        .into_iter()
        .zip(&ints)
        .flat_map(|(k, column)| [key(k), column.clone()])
        .collect();
    bench_stack(c, "stack: unpivot 4 int64 columns", 4, unpivot);
    bench_stack(c, "stack: 4 int64 columns into 2 rows", 2, ints.clone());
    bench_stack(c, "stack: 4 int64 columns into 4 rows", 4, ints.clone());
    bench_stack(c, "stack: 4 int64 columns into 1 row", 1, ints);
    bench_stack(c, "stack: 2 utf8 columns into 2 rows", 2, strings);
    bench_stack(c, "stack: 2 boolean columns into 2 rows", 2, booleans);
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
