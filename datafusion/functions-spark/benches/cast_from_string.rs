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

use arrow_array::{builder::StringBuilder, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use criterion::{criterion_group, criterion_main, Criterion};
use datafusion_comet_spark_expr::{Cast, EvalMode, SparkCastOptions};
use datafusion_physical_expr::{expressions::Column, PhysicalExpr};
use std::sync::Arc;

fn criterion_benchmark(c: &mut Criterion) {
    let batch = create_utf8_batch();
    let expr = Arc::new(Column::new("a", 0));
    let spark_cast_options = SparkCastOptions::new(EvalMode::Legacy, "", false);
    let cast_string_to_i8 = Cast::new(expr.clone(), DataType::Int8, spark_cast_options.clone());
    let cast_string_to_i16 = Cast::new(expr.clone(), DataType::Int16, spark_cast_options.clone());
    let cast_string_to_i32 = Cast::new(expr.clone(), DataType::Int32, spark_cast_options.clone());
    let cast_string_to_i64 = Cast::new(expr, DataType::Int64, spark_cast_options);

    let mut group = c.benchmark_group("cast_string_to_int");
    group.bench_function("cast_string_to_i8", |b| {
        b.iter(|| cast_string_to_i8.evaluate(&batch).unwrap());
    });
    group.bench_function("cast_string_to_i16", |b| {
        b.iter(|| cast_string_to_i16.evaluate(&batch).unwrap());
    });
    group.bench_function("cast_string_to_i32", |b| {
        b.iter(|| cast_string_to_i32.evaluate(&batch).unwrap());
    });
    group.bench_function("cast_string_to_i64", |b| {
        b.iter(|| cast_string_to_i64.evaluate(&batch).unwrap());
    });
}

// Create UTF8 batch with strings representing ints, floats, nulls
fn create_utf8_batch() -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Utf8, true)]));
    let mut b = StringBuilder::new();
    for i in 0..1000 {
        if i % 10 == 0 {
            b.append_null();
        } else if i % 2 == 0 {
            b.append_value(format!("{}", rand::random::<f64>()));
        } else {
            b.append_value(format!("{}", rand::random::<i64>()));
        }
    }
    let array = b.finish();

    RecordBatch::try_new(schema.clone(), vec![Arc::new(array)]).unwrap()
}

fn config() -> Criterion {
    Criterion::default()
}

criterion_group! {
    name = benches;
    config = config();
    targets = criterion_benchmark
}
criterion_main!(benches);
