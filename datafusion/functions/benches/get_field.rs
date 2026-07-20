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

extern crate criterion;

use arrow::array::{ArrayRef, Int32Builder, MapBuilder, StringBuilder};
use arrow::datatypes::{DataType, Field};
use criterion::{Criterion, criterion_group, criterion_main};
use datafusion_common::ScalarValue;
use datafusion_common::config::ConfigOptions;
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs};
use datafusion_functions::core::get_field;
use std::hint::black_box;
use std::sync::Arc;

/// A map array with `size` rows, each holding `entries` key/value pairs.
/// Every tenth row is null.
fn map_array(size: usize, entries: usize) -> ArrayRef {
    let mut builder = MapBuilder::new(None, StringBuilder::new(), Int32Builder::new());
    for row in 0..size {
        if row % 10 == 0 {
            builder.append(false).unwrap();
            continue;
        }
        for entry in 0..entries {
            builder.keys().append_value(format!("key_{entry}"));
            builder.values().append_value((row * entry) as i32);
        }
        builder.append(true).unwrap();
    }
    Arc::new(builder.finish())
}

fn bench_get_field(
    c: &mut Criterion,
    name: &str,
    size: usize,
    entries: usize,
    key: &str,
) {
    let udf = get_field();
    let args = vec![
        ColumnarValue::Array(map_array(size, entries)),
        ColumnarValue::Scalar(ScalarValue::Utf8(Some(key.to_string()))),
    ];
    let arg_fields = vec![
        Field::new("map", args[0].data_type(), true).into(),
        Field::new("key", DataType::Utf8, false).into(),
    ];
    let config_options = Arc::new(ConfigOptions::default());

    c.bench_function(name, |b| {
        b.iter(|| {
            black_box(
                udf.invoke_with_args(ScalarFunctionArgs {
                    args: args.clone(),
                    arg_fields: arg_fields.clone(),
                    number_rows: size,
                    return_field: Field::new("f", DataType::Int32, true).into(),
                    config_options: Arc::clone(&config_options),
                })
                .unwrap(),
            )
        })
    });
}

fn criterion_benchmark(c: &mut Criterion) {
    // First key: the match is found immediately, so the per-row overhead
    // dominates.
    bench_get_field(c, "get_field_map_1024_entries_4_first", 1024, 4, "key_0");
    // Last key: every entry of the row is compared before the match.
    bench_get_field(c, "get_field_map_1024_entries_4_last", 1024, 4, "key_3");
    bench_get_field(c, "get_field_map_1024_entries_16_last", 1024, 16, "key_15");
    // Key that is not present in any row.
    bench_get_field(c, "get_field_map_1024_entries_4_missing", 1024, 4, "key_9");
    bench_get_field(c, "get_field_map_8192_entries_4_last", 8192, 4, "key_3");
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
