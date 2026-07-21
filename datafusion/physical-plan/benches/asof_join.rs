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

//! Criterion benchmarks for the pre-sorted ASOF join kernel.

use std::sync::Arc;

use arrow::array::{
    ArrayRef, Int64Array, RecordBatch, StringArray, StringDictionaryBuilder,
};
use arrow::datatypes::{DataType, Field, Int32Type, Schema, SchemaRef};
use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use datafusion_execution::{TaskContext, config::SessionConfig};
use datafusion_expr::Operator;
use datafusion_physical_expr::expressions::col;
use datafusion_physical_plan::joins::{AsOfJoinExec, AsOfMatchExpr, utils::JoinOn};
use datafusion_physical_plan::test::TestMemoryExec;
use datafusion_physical_plan::{ExecutionPlan, collect};
use tokio::runtime::Runtime;

#[derive(Clone, Copy)]
enum Payload {
    Int64,
    WideUtf8,
    Dictionary,
}

impl Payload {
    fn name(self) -> &'static str {
        match self {
            Self::Int64 => "int64",
            Self::WideUtf8 => "wide_utf8",
            Self::Dictionary => "dictionary",
        }
    }

    fn data_type(self) -> DataType {
        match self {
            Self::Int64 => DataType::Int64,
            Self::WideUtf8 => DataType::Utf8,
            Self::Dictionary => {
                DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8))
            }
        }
    }

    fn array(self, rows: &[(i64, i64, usize)]) -> ArrayRef {
        match self {
            Self::Int64 => Arc::new(Int64Array::from_iter_values(
                rows.iter().map(|(_, _, row)| *row as i64),
            )),
            Self::WideUtf8 => Arc::new(StringArray::from_iter_values(
                rows.iter()
                    .map(|(_, _, row)| format!("row_{row:08}_{}", "x".repeat(244))),
            )),
            Self::Dictionary => {
                let mut builder = StringDictionaryBuilder::<Int32Type>::new();
                for (_, _, row) in rows {
                    builder.append_value(format!("category_{}", row % 64));
                }
                Arc::new(builder.finish())
            }
        }
    }
}

fn schema(payload: Payload) -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("key", DataType::Int64, false),
        Field::new("ts", DataType::Int64, false),
        Field::new("payload", payload.data_type(), false),
    ]))
}

fn build_sorted_batches(
    num_rows: usize,
    num_groups: usize,
    time_offset: i64,
    payload: Payload,
    schema: &SchemaRef,
) -> Vec<RecordBatch> {
    let mut rows = (0..num_rows)
        .map(|row| {
            (
                (row % num_groups) as i64,
                (row / num_groups) as i64 + time_offset,
                row,
            )
        })
        .collect::<Vec<_>>();
    rows.sort_unstable_by_key(|(key, ts, _)| (*key, *ts));

    let batch = RecordBatch::try_new(
        Arc::clone(schema),
        vec![
            Arc::new(Int64Array::from_iter_values(
                rows.iter().map(|(key, _, _)| *key),
            )),
            Arc::new(Int64Array::from_iter_values(
                rows.iter().map(|(_, ts, _)| *ts),
            )),
            payload.array(&rows),
        ],
    )
    .unwrap();

    let mut batches = Vec::new();
    let mut offset = 0;
    while offset < batch.num_rows() {
        let len = (batch.num_rows() - offset).min(8192);
        batches.push(batch.slice(offset, len));
        offset += len;
    }
    batches
}

fn make_exec(batches: &[RecordBatch], schema: &SchemaRef) -> Arc<dyn ExecutionPlan> {
    TestMemoryExec::try_new_exec(&[batches.to_vec()], Arc::clone(schema), None).unwrap()
}

fn do_join(
    left: Arc<dyn ExecutionPlan>,
    right: Arc<dyn ExecutionPlan>,
    rt: &Runtime,
) -> usize {
    let on: JoinOn = vec![(
        col("key", &left.schema()).unwrap(),
        col("key", &right.schema()).unwrap(),
    )];
    let left_match = col("ts", &left.schema()).unwrap();
    let right_match = col("ts", &right.schema()).unwrap();
    let join = AsOfJoinExec::try_new(
        left,
        right,
        on,
        AsOfMatchExpr::new(left_match, Operator::GtEq, right_match),
        vec![2],
    )
    .unwrap();
    let task_ctx = Arc::new(
        TaskContext::default()
            .with_session_config(SessionConfig::new().with_batch_size(8192)),
    );
    rt.block_on(async {
        collect(Arc::new(join), task_ctx)
            .await
            .unwrap()
            .iter()
            .map(RecordBatch::num_rows)
            .sum()
    })
}

fn bench_asof_join(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let num_rows = 100_000;
    let num_groups = 10_000;
    let mut group = c.benchmark_group("asof_join");

    for payload in [Payload::Int64, Payload::WideUtf8, Payload::Dictionary] {
        let schema = schema(payload);
        let left_batches =
            build_sorted_batches(num_rows, num_groups, 1, payload, &schema);
        let right_batches =
            build_sorted_batches(num_rows, num_groups, 0, payload, &schema);
        group.bench_function(
            BenchmarkId::new(payload.name(), format!("{num_rows}_rows_10_per_key")),
            |b| {
                b.iter(|| {
                    let left = make_exec(&left_batches, &schema);
                    let right = make_exec(&right_batches, &schema);
                    do_join(left, right, &rt)
                })
            },
        );
    }

    group.finish();
}

criterion_group!(benches, bench_asof_join);
criterion_main!(benches);
