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

//! Group hourly range partitions without moving groups across partitions.
//! The same benchmark can run on main: only the optimized plan differs.

use std::hint::black_box;
use std::sync::Arc;

use criterion::{Criterion, criterion_group, criterion_main};
use datafusion::arrow::array::{ArrayRef, Int64Array, TimestampNanosecondArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::catalog::streaming::StreamingTable;
use datafusion::common::{Result, ScalarValue, SplitPoint};
use datafusion::logical_expr::col;
use datafusion::physical_expr::{
    Partitioning, PhysicalSortExpr, RangePartitioning, expressions::col as physical_col,
};
use datafusion::physical_plan::{
    collect, execution_plan::reset_plan_states, streaming::PartitionStream,
    test::TestPartitionStream,
};
use datafusion::prelude::{SessionConfig, SessionContext};
use tokio::runtime::Runtime;

const PARTITIONS: usize = 8;
const REPETITIONS: i64 = 8;

fn context(keys_per_partition: i64) -> Result<SessionContext> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("key", DataType::Int64, false),
        Field::new("ts", DataType::Timestamp(TimeUnit::Nanosecond, None), false),
        Field::new("value", DataType::Int64, false),
    ]));
    let start = 1_704_067_200_000_000_000_i64;
    let hour = 3_600_000_000_000_i64;
    let mut streams: Vec<Arc<dyn PartitionStream>> = vec![];
    for partition in 0..PARTITIONS {
        let mut batches = vec![];
        let rows = keys_per_partition * REPETITIONS;
        for offset in (0..rows).step_by(8192) {
            let end = (offset + 8192).min(rows);
            let columns: Vec<ArrayRef> = vec![
                Arc::new(Int64Array::from_iter_values(
                    (offset..end).map(|i| i / REPETITIONS),
                )),
                Arc::new(TimestampNanosecondArray::from_iter_values(
                    (offset..end).map(|i| {
                        start
                            + partition as i64 * hour
                            + (i % REPETITIONS) * 1_000_000_000
                    }),
                )),
                Arc::new(Int64Array::from_iter_values((offset..end).map(|_| 1))),
            ];
            batches.push(RecordBatch::try_new(Arc::clone(&schema), columns)?);
        }
        streams.push(Arc::new(TestPartitionStream::new_with_batches(batches)));
    }
    let partitioning = Partitioning::Range(RangePartitioning::try_new(
        [PhysicalSortExpr::new_default(physical_col("ts", &schema)?)].into(),
        (1..PARTITIONS)
            .map(|partition| {
                SplitPoint::new(vec![ScalarValue::TimestampNanosecond(
                    Some(start + partition as i64 * hour),
                    None,
                )])
            })
            .collect(),
    )?);
    let mut config = SessionConfig::new().with_target_partitions(PARTITIONS);
    config
        .options_mut()
        .optimizer
        .enable_round_robin_repartition = false;
    config.options_mut().optimizer.subset_repartition_threshold = PARTITIONS;
    let context = SessionContext::new_with_config(config);
    context.register_table(
        "t",
        Arc::new(
            StreamingTable::try_new(schema, streams)?
                .with_sort_order(vec![
                    col("key").sort(true, true),
                    col("ts").sort(true, true),
                ])
                .with_output_partitioning(partitioning),
        ),
    )?;
    Ok(context)
}

fn criterion_benchmark(criterion: &mut Criterion) {
    let runtime = Runtime::new().unwrap();
    for keys_per_partition in [1024, 65_536] {
        let context = context(keys_per_partition).unwrap();
        let plan =
            runtime.block_on(async {
                context
                .sql("SELECT key, date_trunc('hour', ts) AS bucket, sum(value) AS total \
                      FROM t GROUP BY key, bucket")
                .await
                .unwrap()
                .create_physical_plan()
                .await
                .unwrap()
            });
        let results = runtime
            .block_on(collect(Arc::clone(&plan), context.task_ctx()))
            .unwrap();
        assert_eq!(
            results.iter().map(RecordBatch::num_rows).sum::<usize>(),
            PARTITIONS * keys_per_partition as usize
        );
        for batch in results {
            let sums = batch
                .column(2)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            assert!(sums.values().iter().all(|sum| *sum == REPETITIONS));
        }
        criterion.bench_function(
            &format!("range_date_trunc/{keys_per_partition}_keys_per_partition"),
            |bencher| {
                bencher.iter(|| {
                    let plan = reset_plan_states(Arc::clone(&plan)).unwrap();
                    black_box(
                        runtime.block_on(collect(plan, context.task_ctx())).unwrap(),
                    )
                });
            },
        );
    }
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
