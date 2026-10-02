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

//! Measure the physical evaluator with a timezone column. The narrow and wide batches use
//! identical timestamp and timezone columns; only the number of unused Int64 columns differs.
//! Mixed-null timestamps expose any cost from filtering those unrelated columns.

use std::hint::black_box;
use std::sync::Arc;

use arrow::array::{ArrayRef, Int64Array, StringArray, TimestampMicrosecondArray};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use datafusion::physical_expr::expressions::Column;
use datafusion_physical_expr_common::physical_expr::PhysicalExpr;
use datafusion_spark::function::datetime::from_utc_timestamp::SparkFromUtcTimestampExpr;

fn batch(rows: usize, columns: usize, half_null: bool) -> RecordBatch {
    let mut fields = vec![
        Field::new(
            "timestamp",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            true,
        ),
        Field::new("timezone", DataType::Utf8, false),
    ];
    let mut arrays: Vec<ArrayRef> = vec![
        Arc::new(TimestampMicrosecondArray::from_iter((0..rows).map(|row| {
            (!half_null || row % 2 != 0).then_some(row as i64 * 86_400_000_000)
        }))),
        Arc::new(StringArray::from_iter_values(
            (0..rows).map(|row| if (row / 2) % 2 == 0 { "PST" } else { "EST" }),
        )),
    ];

    for column in 2..columns {
        fields.push(Field::new(
            format!("unused_{column}"),
            DataType::Int64,
            false,
        ));
        arrays.push(Arc::new(Int64Array::from_iter_values(
            (0..rows).map(|row| (column * rows + row) as i64),
        )));
    }

    RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays).unwrap()
}

fn criterion_benchmark(c: &mut Criterion) {
    let rows = 8192;
    let expr = SparkFromUtcTimestampExpr::new(
        Arc::new(Column::new("timestamp", 0)),
        Arc::new(Column::new("timezone", 1)),
    );
    let mut group = c.benchmark_group("from_utc_timestamp_column");
    group.throughput(Throughput::Elements(rows as u64));

    for columns in [2, 64, 256] {
        for (half_null, name) in [(false, "no_nulls"), (true, "half_nulls")] {
            let batch = batch(rows, columns, half_null);
            let result = expr.evaluate(&batch).unwrap().into_array(rows).unwrap();
            assert_eq!(result.len(), rows);
            assert_eq!(result.null_count(), if half_null { rows / 2 } else { 0 });

            group.bench_with_input(
                BenchmarkId::new(format!("{columns}_columns"), name),
                &batch,
                |b, batch| {
                    b.iter(|| black_box(expr.evaluate(black_box(batch)).unwrap()));
                },
            );
        }
    }
    group.finish();
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
