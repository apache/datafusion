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

use arrow::compute::cast;
use arrow_array::builder::Decimal128Builder;
use arrow_schema::DataType;
use criterion::{black_box, criterion_group, criterion_main, Criterion};
use datafusion_comet_spark_expr::spark_decimal_div;
use datafusion_expr::ColumnarValue;
use std::sync::Arc;

fn criterion_benchmark(c: &mut Criterion) {
    // create input data
    let mut c1 = Decimal128Builder::new();
    let mut c2 = Decimal128Builder::new();
    for i in 0..1000 {
        c1.append_value(99999999 + i);
        c2.append_value(88888888 - i);
    }
    let c1 = Arc::new(c1.finish());
    let c2 = Arc::new(c2.finish());

    let c1_type = DataType::Decimal128(10, 4);
    let c1 = cast(c1.as_ref(), &c1_type).unwrap();
    let c2_type = DataType::Decimal128(10, 3);
    let c2 = cast(c2.as_ref(), &c2_type).unwrap();

    let args = [ColumnarValue::Array(c1), ColumnarValue::Array(c2)];
    c.bench_function("decimal_div", |b| {
        b.iter(|| {
            black_box(spark_decimal_div(
                black_box(&args),
                black_box(&DataType::Decimal128(10, 4)),
            ))
        })
    });
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
