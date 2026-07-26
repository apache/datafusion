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

use arrow::array::{ArrayRef, StringArray};
use criterion::{Criterion, criterion_group, criterion_main};
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl};
use datafusion_spark::function::string::soundex::SparkSoundex;
use std::hint::black_box;
use std::sync::Arc;

fn create_string_array(rows: usize) -> ArrayRef {
    let strings: Vec<String> = (0..rows)
        .map(|i| match i % 5 {
            0 => format!("Robert{i}"),
            1 => format!("Rupert{i}"),
            2 => format!("Washington{i}"),
            3 => format!("123Invalid{i}"),
            _ => format!("Short{i}"),
        })
        .collect();

    Arc::new(StringArray::from(
        strings.iter().map(|s| s.as_str()).collect::<Vec<&str>>(),
    )) as ArrayRef
}

fn criterion_benchmark(c: &mut Criterion) {
    let rows = 8192;
    let array = create_string_array(rows);
    let soundex_udf = SparkSoundex::new();

    c.bench_function("spark_soundex: standard utf8 array", |b| {
        let args = vec![ColumnarValue::Array(Arc::clone(&array))];
        b.iter(|| {
            black_box(
                soundex_udf
                    .invoke_with_args(ScalarFunctionArgs {
                        args: args.clone(),
                        arg_fields: vec![],
                        number_rows: rows,
                        return_field: Arc::new(arrow::datatypes::Field::new(
                            "soundex",
                            arrow::datatypes::DataType::Utf8,
                            true,
                        )),
                        config_options: Arc::new(
                            datafusion::config::ConfigOptions::default(),
                        ),
                    })
                    .unwrap(),
            )
        })
    });
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
