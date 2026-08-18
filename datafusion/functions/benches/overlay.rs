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
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDF};
use helper::gen_string_array;
use std::hint::black_box;
use std::sync::Arc;

#[expect(clippy::too_many_arguments)]
fn bench_overlay(
    c: &mut Criterion,
    name: &str,
    overlay: &ScalarUDF,
    n_rows: usize,
    null_density: f32,
    utf8_density: f32,
    is_string_view: bool,
    with_for: bool,
) {
    const STR_LEN: usize = 128;

    let mut args =
        gen_string_array(n_rows, STR_LEN, null_density, utf8_density, is_string_view);
    // The substring scalar's type must match the string column's type (the
    // function dispatches per-type without coercion).
    let substr = "DataFusion".to_string();
    let substr_scalar = if is_string_view {
        ScalarValue::Utf8View(Some(substr))
    } else {
        ScalarValue::Utf8(Some(substr))
    };
    args.push(ColumnarValue::Scalar(substr_scalar));
    args.push(ColumnarValue::Scalar(ScalarValue::Int64(Some(32))));
    if with_for {
        args.push(ColumnarValue::Scalar(ScalarValue::Int64(Some(8))));
    }

    let arg_fields = args
        .iter()
        .enumerate()
        .map(|(idx, arg)| Field::new(format!("arg_{idx}"), arg.data_type(), true).into())
        .collect::<Vec<_>>();
    let return_field = Arc::new(Field::new("f", DataType::Utf8, true));
    let config_options = Arc::new(ConfigOptions::default());

    c.bench_function(name, |b| {
        b.iter(|| {
            black_box(
                overlay
                    .invoke_with_args(ScalarFunctionArgs {
                        args: args.clone(),
                        arg_fields: arg_fields.clone(),
                        number_rows: n_rows,
                        return_field: Arc::clone(&return_field),
                        config_options: Arc::clone(&config_options),
                    })
                    .unwrap(),
            )
        })
    });
}

fn criterion_benchmark(c: &mut Criterion) {
    const N_ROWS: usize = 8192;
    const MIXED_UTF8: f32 = 0.5;
    let overlay = datafusion_functions::core::overlay();

    // Null-density variants on StringArray (mixed ASCII/UTF-8, 4-arg form).
    bench_overlay(
        c,
        "overlay_StringArray_low_nulls",
        &overlay,
        N_ROWS,
        0.1,
        MIXED_UTF8,
        false,
        true,
    );
    bench_overlay(
        c,
        "overlay_StringArray_high_nulls",
        &overlay,
        N_ROWS,
        0.9,
        MIXED_UTF8,
        false,
        true,
    );
    bench_overlay(
        c,
        "overlay_StringArray_no_nulls",
        &overlay,
        N_ROWS,
        0.0,
        MIXED_UTF8,
        false,
        true,
    );

    // Content variants on StringArray (no nulls, 4-arg form). Pair against
    // `overlay_StringArray_no_nulls` to isolate the impact of UTF-8 density.
    bench_overlay(
        c,
        "overlay_StringArray_ascii",
        &overlay,
        N_ROWS,
        0.0,
        0.0,
        false,
        true,
    );
    bench_overlay(
        c,
        "overlay_StringArray_all_utf8",
        &overlay,
        N_ROWS,
        0.0,
        1.0,
        false,
        true,
    );

    // 3-arg form (no FOR clause), where the replace length is derived from
    // the substring per row.
    bench_overlay(
        c,
        "overlay_StringArray_no_for",
        &overlay,
        N_ROWS,
        0.0,
        MIXED_UTF8,
        false,
        false,
    );

    // StringViewArray counterparts.
    bench_overlay(
        c,
        "overlay_StringViewArray_low_nulls",
        &overlay,
        N_ROWS,
        0.1,
        MIXED_UTF8,
        true,
        true,
    );
    bench_overlay(
        c,
        "overlay_StringViewArray_ascii",
        &overlay,
        N_ROWS,
        0.0,
        0.0,
        true,
        true,
    );
    bench_overlay(
        c,
        "overlay_StringViewArray_all_utf8",
        &overlay,
        N_ROWS,
        0.0,
        1.0,
        true,
        true,
    );
    bench_overlay(
        c,
        "overlay_StringViewArray_no_for",
        &overlay,
        N_ROWS,
        0.0,
        MIXED_UTF8,
        true,
        false,
    );
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
