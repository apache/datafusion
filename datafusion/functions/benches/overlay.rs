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

mod helper;

use arrow::datatypes::{DataType, Field};
use criterion::{Criterion, criterion_group, criterion_main};
use datafusion_common::ScalarValue;
use datafusion_common::config::ConfigOptions;
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs};
use helper::gen_string_array;
use std::hint::black_box;
use std::sync::Arc;

fn criterion_benchmark(c: &mut Criterion) {
    const N_ROWS: usize = 8192;
    const STR_LEN: usize = 128;

    let overlay = datafusion_functions::core::overlay();
    let config_options = Arc::new(ConfigOptions::default());

    let mut args = gen_string_array(N_ROWS, STR_LEN, 0.1, 0.5, false);
    args.push(ColumnarValue::Scalar(ScalarValue::Utf8(Some(
        "DataFusion".to_string(),
    ))));
    args.push(ColumnarValue::Scalar(ScalarValue::Int64(Some(32))));
    args.push(ColumnarValue::Scalar(ScalarValue::Int64(Some(8))));

    let arg_fields = args
        .iter()
        .enumerate()
        .map(|(idx, arg)| Field::new(format!("arg_{idx}"), arg.data_type(), true).into())
        .collect::<Vec<_>>();
    let return_field = Arc::new(Field::new("f", DataType::Utf8, true));

    c.bench_function("overlay_StringArray_utf8_scalar_args", |b| {
        b.iter(|| {
            black_box(
                overlay
                    .invoke_with_args(ScalarFunctionArgs {
                        args: args.clone(),
                        arg_fields: arg_fields.clone(),
                        number_rows: N_ROWS,
                        return_field: Arc::clone(&return_field),
                        config_options: Arc::clone(&config_options),
                    })
                    .unwrap(),
            )
        })
    });
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
