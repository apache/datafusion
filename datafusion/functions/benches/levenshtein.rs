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

use arrow::array::OffsetSizeTrait;
use arrow::datatypes::{DataType, Field};
use arrow::util::bench_util::create_string_array_with_len;
use criterion::{Criterion, SamplingMode, criterion_group, criterion_main};
use datafusion_common::DataFusionError;
use datafusion_common::config::ConfigOptions;
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs};
use datafusion_functions::string;
use std::hint::black_box;
use std::sync::Arc;
use std::time::Duration;

fn create_args<O: OffsetSizeTrait>(size: usize, str_len: usize) -> Vec<ColumnarValue> {
    let string1_array = Arc::new(create_string_array_with_len::<O>(size, 0.1, str_len));
    let string2_array = Arc::new(create_string_array_with_len::<O>(size, 0.1, str_len));

    vec![
        ColumnarValue::Array(string1_array),
        ColumnarValue::Array(string2_array),
    ]
}

fn invoke_levenshtein_with_args(
    args: Vec<ColumnarValue>,
    number_rows: usize,
) -> Result<ColumnarValue, DataFusionError> {
    let arg_fields = args
        .iter()
        .enumerate()
        .map(|(idx, arg)| Field::new(format!("arg_{idx}"), arg.data_type(), true).into())
        .collect::<Vec<_>>();
    let config_options = Arc::new(ConfigOptions::default());

    string::levenshtein().invoke_with_args(ScalarFunctionArgs {
        args,
        arg_fields,
        number_rows,
        return_field: Field::new("f", DataType::Int32, true).into(),
        config_options: Arc::clone(&config_options),
    })
}

fn criterion_benchmark(c: &mut Criterion) {
    for size in [1024, 4096] {
        let mut group = c.benchmark_group(format!("levenshtein size={size}"));
        group.sampling_mode(SamplingMode::Flat);
        group.sample_size(10);
        group.measurement_time(Duration::from_secs(10));

        for str_len in [8, 32] {
            let args = create_args::<i32>(size, str_len);
            group.bench_function(
                format!("levenshtein_string [size={size}, str_len={str_len}]"),
                |b| {
                    b.iter(|| {
                        let args_cloned = args.clone();
                        black_box(invoke_levenshtein_with_args(args_cloned, size))
                    })
                },
            );
        }

        group.finish();
    }
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
