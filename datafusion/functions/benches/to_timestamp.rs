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

use std::sync::Arc;

use arrow::array::builder::StringBuilder;
use arrow::array::ArrayRef;
use criterion::{black_box, criterion_group, criterion_main, Criterion};

use datafusion_expr::ColumnarValue;
use datafusion_functions::datetime::to_timestamp;

fn criterion_benchmark(c: &mut Criterion) {
    c.bench_function("to_timestamp_no_formats", |b| {
        let mut inputs = StringBuilder::new();
        inputs.append_value("1997-01-31T09:26:56.123Z");
        inputs.append_value("1997-01-31T09:26:56.123-05:00");
        inputs.append_value("1997-01-31 09:26:56.123-05:00");
        inputs.append_value("2023-01-01 04:05:06.789 -08");
        inputs.append_value("1997-01-31T09:26:56.123");
        inputs.append_value("1997-01-31 09:26:56.123");
        inputs.append_value("1997-01-31 09:26:56");
        inputs.append_value("1997-01-31 13:26:56");
        inputs.append_value("1997-01-31 13:26:56+04:00");
        inputs.append_value("1997-01-31");

        let string_array = ColumnarValue::Array(Arc::new(inputs.finish()) as ArrayRef);

        b.iter(|| {
            black_box(
                to_timestamp()
                    .invoke(&[string_array.clone()])
                    .expect("to_timestamp should work on valid values"),
            )
        })
    });

    c.bench_function("to_timestamp_with_formats", |b| {
        let mut inputs = StringBuilder::new();
        let mut format1_builder = StringBuilder::with_capacity(2, 10);
        let mut format2_builder = StringBuilder::with_capacity(2, 10);
        let mut format3_builder = StringBuilder::with_capacity(2, 10);

        inputs.append_value("1997-01-31T09:26:56.123Z");
        format1_builder.append_value("%+");
        format2_builder.append_value("%c");
        format3_builder.append_value("%Y-%m-%dT%H:%M:%S%.f%Z");

        inputs.append_value("1997-01-31T09:26:56.123-05:00");
        format1_builder.append_value("%+");
        format2_builder.append_value("%c");
        format3_builder.append_value("%Y-%m-%dT%H:%M:%S%.f%z");

        inputs.append_value("1997-01-31 09:26:56.123-05:00");
        format1_builder.append_value("%+");
        format2_builder.append_value("%c");
        format3_builder.append_value("%Y-%m-%d %H:%M:%S%.f%Z");

        inputs.append_value("2023-01-01 04:05:06.789 -08");
        format1_builder.append_value("%+");
        format2_builder.append_value("%c");
        format3_builder.append_value("%Y-%m-%d %H:%M:%S%.f %#z");

        inputs.append_value("1997-01-31T09:26:56.123");
        format1_builder.append_value("%+");
        format2_builder.append_value("%c");
        format3_builder.append_value("%Y-%m-%dT%H:%M:%S%.f");

        inputs.append_value("1997-01-31 09:26:56.123");
        format1_builder.append_value("%+");
        format2_builder.append_value("%c");
        format3_builder.append_value("%Y-%m-%d %H:%M:%S%.f");

        inputs.append_value("1997-01-31 09:26:56");
        format1_builder.append_value("%+");
        format2_builder.append_value("%c");
        format3_builder.append_value("%Y-%m-%d %H:%M:%S");

        inputs.append_value("1997-01-31 092656");
        format1_builder.append_value("%+");
        format2_builder.append_value("%c");
        format3_builder.append_value("%Y-%m-%d %H%M%S");

        inputs.append_value("1997-01-31 092656+04:00");
        format1_builder.append_value("%+");
        format2_builder.append_value("%c");
        format3_builder.append_value("%Y-%m-%d %H%M%S%:z");

        inputs.append_value("Sun Jul  8 00:34:60 2001");
        format1_builder.append_value("%+");
        format2_builder.append_value("%c");
        format3_builder.append_value("%Y-%m-%d 00:00:00");

        let args = [
            ColumnarValue::Array(Arc::new(inputs.finish()) as ArrayRef),
            ColumnarValue::Array(Arc::new(format1_builder.finish()) as ArrayRef),
            ColumnarValue::Array(Arc::new(format2_builder.finish()) as ArrayRef),
            ColumnarValue::Array(Arc::new(format3_builder.finish()) as ArrayRef),
        ];
        b.iter(|| {
            black_box(
                to_timestamp()
                    .invoke(&args.clone())
                    .expect("to_timestamp should work on valid values"),
            )
        })
    });
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
