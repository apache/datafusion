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

use arrow::array::Int64Array;
use arrow::array::OffsetSizeTrait;
use arrow::datatypes::{DataType, Field};
use arrow::util::bench_util::create_string_array_with_len;
use criterion::{Criterion, SamplingMode, criterion_group, criterion_main};
use datafusion_common::config::ConfigOptions;
use datafusion_common::{DataFusionError, ScalarValue};
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs};
use datafusion_functions::regex;
use std::hint::black_box;
use std::sync::Arc;
use std::time::Duration;

fn create_args<O: OffsetSizeTrait>(
    size: usize,
    str_len: usize,
    with_start: bool,
) -> Vec<ColumnarValue> {
    let string_array = Arc::new(create_string_array_with_len::<O>(size, 0.1, str_len));

    // Use a simple pattern that matches common characters
    let pattern = ColumnarValue::Scalar(ScalarValue::Utf8(Some("a".to_string())));

    if with_start {
        // Test with start position (this is where the optimization matters)
        let start_array = Arc::new(Int64Array::from(
            (0..size).map(|i| (i % 10 + 1) as i64).collect::<Vec<_>>(),
        ));
        vec![
            ColumnarValue::Array(string_array),
            pattern,
            ColumnarValue::Array(start_array),
        ]
    } else {
        vec![ColumnarValue::Array(string_array), pattern]
    }
}

fn invoke_regexp_count_with_args(
    args: Vec<ColumnarValue>,
    number_rows: usize,
) -> Result<ColumnarValue, DataFusionError> {
    let arg_fields = args
        .iter()
        .enumerate()
        .map(|(idx, arg)| Field::new(format!("arg_{idx}"), arg.data_type(), true).into())
        .collect::<Vec<_>>();
    let config_options = Arc::new(ConfigOptions::default());

    regex::regexp_count().invoke_with_args(ScalarFunctionArgs {
        args,
        arg_fields,
        number_rows,
        return_field: Field::new("f", DataType::Int64, true).into(),
        config_options: Arc::clone(&config_options),
    })
}

fn criterion_benchmark(c: &mut Criterion) {
    for size in [1024, 4096] {
        let mut group = c.benchmark_group(format!("regexp_count size={size}"));
        group.sampling_mode(SamplingMode::Flat);
        group.sample_size(10);
        group.measurement_time(Duration::from_secs(10));

        // Test without start position (no optimization impact)
        for str_len in [32, 128] {
            let args = create_args::<i32>(size, str_len, false);
            group.bench_function(
                format!("regexp_count_no_start [size={size}, str_len={str_len}]"),
                |b| {
                    b.iter(|| {
                        let args_cloned = args.clone();
                        black_box(invoke_regexp_count_with_args(args_cloned, size))
                    })
                },
            );
        }

        // Test with start position (optimization should help here)
        for str_len in [32, 128] {
            let args = create_args::<i32>(size, str_len, true);
            group.bench_function(
                format!("regexp_count_with_start [size={size}, str_len={str_len}]"),
                |b| {
                    b.iter(|| {
                        let args_cloned = args.clone();
                        black_box(invoke_regexp_count_with_args(args_cloned, size))
                    })
                },
            );
        }

        group.finish();
    }
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
