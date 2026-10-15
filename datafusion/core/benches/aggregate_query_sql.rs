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

mod data_utils;

use arrow::array::{Int64Array, TimestampNanosecondArray, UInt64Array};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use arrow::record_batch::RecordBatch;
use criterion::{BatchSize, Criterion, criterion_group, criterion_main};
use data_utils::create_table_provider;
use datafusion::catalog::streaming::StreamingTable;
use datafusion::error::Result;
use datafusion::execution::context::SessionContext;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_plan::execution_plan::reset_plan_states;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::streaming::PartitionStream;
use datafusion::physical_plan::{collect, displayable};
use datafusion::prelude::{SessionConfig, col};
use datafusion_common::ScalarValue;
use datafusion_physical_expr::expressions::Column;
use datafusion_physical_expr::{
    Partitioning, PhysicalSortExpr, RangePartitioning, SplitPoint,
};
use parking_lot::Mutex;
use std::hint::black_box;
use std::sync::Arc;
use tokio::runtime::Runtime;

#[expect(clippy::needless_pass_by_value)]
fn query(ctx: Arc<Mutex<SessionContext>>, rt: &Runtime, sql: &str) {
    let df = rt.block_on(ctx.lock().sql(sql)).unwrap();
    black_box(rt.block_on(df.collect()).unwrap());
}

fn create_context(
    partitions_len: usize,
    array_len: usize,
    batch_size: usize,
) -> Result<Arc<Mutex<SessionContext>>> {
    let ctx = SessionContext::new();
    let provider = create_table_provider(partitions_len, array_len, batch_size)?;
    ctx.register_table("t", provider)?;
    Ok(Arc::new(Mutex::new(ctx)))
}

fn criterion_benchmark(c: &mut Criterion) {
    let partitions_len = 8;
    let array_len = 32768 * 2; // 2^16
    let batch_size = 2048; // 2^11
    let ctx = create_context(partitions_len, array_len, batch_size).unwrap();
    let rt = Runtime::new().unwrap();

    c.bench_function("aggregate_query_no_group_by 15 12", |b| {
        b.iter(|| {
            query(
                ctx.clone(),
                &rt,
                "SELECT MIN(f64), AVG(f64), COUNT(f64) \
                 FROM t",
            )
        })
    });

    c.bench_function("aggregate_query_no_group_by_min_max_f64", |b| {
        b.iter(|| {
            query(
                ctx.clone(),
                &rt,
                "SELECT MIN(f64), MAX(f64) \
                 FROM t",
            )
        })
    });

    c.bench_function("aggregate_query_no_group_by_count_distinct_wide", |b| {
        b.iter(|| {
            query(
                ctx.clone(),
                &rt,
                "SELECT COUNT(DISTINCT u64_wide) \
                 FROM t",
            )
        })
    });

    c.bench_function("aggregate_query_no_group_by_count_distinct_narrow", |b| {
        b.iter(|| {
            query(
                ctx.clone(),
                &rt,
                "SELECT COUNT(DISTINCT u64_narrow) \
                 FROM t",
            )
        })
    });

    c.bench_function("aggregate_query_group_by", |b| {
        b.iter(|| {
            query(
                ctx.clone(),
                &rt,
                "SELECT utf8, MIN(f64), AVG(f64), COUNT(f64) \
                 FROM t GROUP BY utf8",
            )
        })
    });

    c.bench_function("aggregate_query_group_by_with_filter", |b| {
        b.iter(|| {
            query(
                ctx.clone(),
                &rt,
                "SELECT utf8, MIN(f64), AVG(f64), COUNT(f64) \
                 FROM t \
                 WHERE f32 > 10 AND f32 < 20 GROUP BY utf8",
            )
        })
    });

    c.bench_function("aggregate_query_group_by_u64 15 12", |b| {
        b.iter(|| {
            query(
                ctx.clone(),
                &rt,
                "SELECT u64_narrow, MIN(f64), AVG(f64), COUNT(f64) \
                 FROM t GROUP BY u64_narrow",
            )
        })
    });

    c.bench_function("aggregate_query_group_by_with_filter_u64 15 12", |b| {
        b.iter(|| {
            query(
                ctx.clone(),
                &rt,
                "SELECT u64_narrow, MIN(f64), AVG(f64), COUNT(f64) \
                 FROM t \
                 WHERE f32 > 10 AND f32 < 20 GROUP BY u64_narrow",
            )
        })
    });

    c.bench_function("aggregate_query_group_by_u64_multiple_keys", |b| {
        b.iter(|| {
            query(
                ctx.clone(),
                &rt,
                "SELECT u64_wide, utf8, MIN(f64), AVG(f64), COUNT(f64) \
                 FROM t GROUP BY u64_wide, utf8",
            )
        })
    });

    c.bench_function(
        "aggregate_query_group_by_wide_u64_and_string_without_aggregate_expressions",
        |b| {
            b.iter(|| {
                query(
                    ctx.clone(),
                    &rt,
                    // Due to the large number of distinct values in u64_wide,
                    // this query test the actual grouping performance for more than 1 column
                    "SELECT u64_wide, utf8 \
                 FROM t GROUP BY u64_wide, utf8",
                )
            })
        },
    );

    c.bench_function(
        "aggregate_query_group_by_wide_u64_and_f32_without_aggregate_expressions",
        |b| {
            b.iter(|| {
                query(
                    ctx.clone(),
                    &rt,
                    // Due to the large number of distinct values in u64_wide,
                    // this query test the actual grouping performance for more than 1 column
                    "SELECT u64_wide, f32 \
                 FROM t GROUP BY u64_wide, f32",
                )
            })
        },
    );

    c.bench_function("aggregate_query_approx_percentile_cont_on_u64", |b| {
        b.iter(|| {
            query(
                ctx.clone(),
                &rt,
                "SELECT utf8, approx_percentile_cont(0.5, 2500) WITHIN GROUP (ORDER BY u64_wide)  \
                 FROM t GROUP BY utf8",
            )
        })
    });

    c.bench_function("aggregate_query_approx_percentile_cont_on_f32", |b| {
        b.iter(|| {
            query(
                ctx.clone(),
                &rt,
                "SELECT utf8, approx_percentile_cont(0.5, 2500) WITHIN GROUP (ORDER BY f32)  \
                 FROM t GROUP BY utf8",
            )
        })
    });

    c.bench_function("aggregate_query_distinct_median", |b| {
        b.iter(|| {
            query(
                ctx.clone(),
                &rt,
                "SELECT MEDIAN(DISTINCT u64_wide), MEDIAN(DISTINCT u64_narrow) \
                 FROM t",
            )
        })
    });

    c.bench_function("first_last_many_columns", |b| {
        b.iter(|| {
            query(
                ctx.clone(),
                &rt,
                "SELECT first_value(u64_wide order by f64, u64_narrow, utf8),\
                            last_value(u64_wide order by f64, u64_narrow, utf8)  \
                 FROM t GROUP BY u64_narrow",
            )
        })
    });

    c.bench_function("first_last_ignore_nulls", |b| {
        b.iter(|| {
            query(
                ctx.clone(),
                &rt,
                "SELECT first_value(u64_wide ignore nulls order by f64, u64_narrow, utf8),  \
                            last_value(u64_wide ignore nulls order by f64, u64_narrow, utf8)    \
                 FROM t GROUP BY u64_narrow",
            )
        })
    });

    c.bench_function("first_last_one_column", |b| {
        b.iter(|| {
            query(
                ctx.clone(),
                &rt,
                "SELECT first_value(u64_wide order by f64), \
                            last_value(u64_wide order by f64)   \
                FROM t GROUP BY u64_narrow",
            )
        })
    });

    c.bench_function("array_agg_query_group_by_few_groups", |b| {
        b.iter(|| {
            query(
                ctx.clone(),
                &rt,
                "SELECT u64_narrow, array_agg(f64) \
                 FROM t GROUP BY u64_narrow",
            )
        })
    });

    c.bench_function("array_agg_query_group_by_mid_groups", |b| {
        b.iter(|| {
            query(
                ctx.clone(),
                &rt,
                "SELECT u64_mid, array_agg(f64) \
                 FROM t GROUP BY u64_mid",
            )
        })
    });

    c.bench_function("array_agg_query_group_by_many_groups", |b| {
        b.iter(|| {
            query(
                ctx.clone(),
                &rt,
                "SELECT u64_wide, array_agg(f64) \
                 FROM t GROUP BY u64_wide",
            )
        })
    });

    c.bench_function("array_agg_struct_query_group_by_mid_groups", |b| {
        b.iter(|| {
            query(
                ctx.clone(),
                &rt,
                "SELECT u64_mid, array_agg(named_struct('market', dict10, 'price', f64)) \
                 FROM t GROUP BY u64_mid",
            )
        })
    });

    c.bench_function("string_agg_query_group_by_few_groups", |b| {
        b.iter(|| {
            query(
                ctx.clone(),
                &rt,
                "SELECT u64_narrow, string_agg(utf8, ',') \
                 FROM t GROUP BY u64_narrow",
            )
        })
    });

    c.bench_function("string_agg_query_group_by_mid_groups", |b| {
        b.iter(|| {
            query(
                ctx.clone(),
                &rt,
                "SELECT u64_mid, string_agg(utf8, ',') \
                 FROM t GROUP BY u64_mid",
            )
        })
    });

    c.bench_function("string_agg_query_group_by_many_groups", |b| {
        b.iter(|| {
            query(
                ctx.clone(),
                &rt,
                "SELECT u64_wide, string_agg(utf8, ',') \
                 FROM t GROUP BY u64_wide",
            )
        })
    });
}

#[derive(Debug)]
struct TimestampRangePartition {
    schema: SchemaRef,
    batches: Vec<RecordBatch>,
}

impl PartitionStream for TimestampRangePartition {
    fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    fn execute(&self, _: Arc<TaskContext>) -> SendableRecordBatchStream {
        Box::pin(RecordBatchStreamAdapter::new(
            Arc::clone(&self.schema),
            futures::stream::iter(self.batches.clone().into_iter().map(Ok)),
        ))
    }
}

const RANGE_AGGREGATE_ROWS: usize = 1 << 20;
const ROWS_PER_SERIES: usize = 8;
const HOUR_NANOS: i64 = 3_600_000_000_000;

fn range_aggregate_context(partition_count: usize) -> Result<SessionContext> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("series_id", DataType::UInt64, false),
        Field::new(
            "timestamp",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            false,
        ),
        Field::new("value", DataType::Int64, false),
    ]));
    let rows_per_partition = RANGE_AGGREGATE_ROWS / partition_count;
    let partitions = (0..partition_count)
        .map(|partition| -> Result<Arc<dyn PartitionStream>> {
            let batch = RecordBatch::try_new(
                Arc::clone(&schema),
                vec![
                    Arc::new(UInt64Array::from_iter_values(
                        (0..rows_per_partition).map(|row| (row / ROWS_PER_SERIES) as u64),
                    )),
                    Arc::new(TimestampNanosecondArray::from_iter_values(
                        (0..rows_per_partition).map(|row| {
                            partition as i64 * HOUR_NANOS
                                + (row % ROWS_PER_SERIES) as i64
                                    * (HOUR_NANOS / ROWS_PER_SERIES as i64)
                        }),
                    )),
                    Arc::new(Int64Array::from_value(1, rows_per_partition)),
                ],
            )?;
            let batches = (0..rows_per_partition)
                .step_by(8192)
                .map(|offset| batch.slice(offset, 8192.min(rows_per_partition - offset)))
                .collect();
            Ok(Arc::new(TimestampRangePartition {
                schema: Arc::clone(&schema),
                batches,
            }))
        })
        .collect::<Result<Vec<_>>>()?;
    let range = RangePartitioning::try_new(
        [PhysicalSortExpr::new_default(Arc::new(Column::new(
            "timestamp",
            1,
        )))]
        .into(),
        (1..partition_count)
            .map(|partition| {
                SplitPoint::new(vec![ScalarValue::TimestampNanosecond(
                    Some(partition as i64 * HOUR_NANOS),
                    None,
                )])
            })
            .collect(),
    )?;
    let table = StreamingTable::try_new(schema, partitions)?
        .with_sort_order(vec![
            col("series_id").sort(true, true),
            col("timestamp").sort(true, true),
        ])
        .with_output_partitioning(Partitioning::Range(range));
    let mut config = SessionConfig::new().with_target_partitions(partition_count);
    config.options_mut().optimizer.subset_repartition_threshold = 1;
    config.options_mut().optimizer.preserve_file_partitions = 1;
    config
        .options_mut()
        .optimizer
        .enable_round_robin_repartition = false;
    let context = SessionContext::new_with_config(config);
    context.register_table("range_points", Arc::new(table))?;
    Ok(context)
}

fn check_range_aggregate_results(
    batches: &[RecordBatch],
    partition_count: usize,
    hours_per_bucket: usize,
) {
    let mut actual = batches
        .iter()
        .flat_map(|batch| {
            let series = batch
                .column(0)
                .as_any()
                .downcast_ref::<UInt64Array>()
                .unwrap();
            let buckets = batch
                .column(1)
                .as_any()
                .downcast_ref::<TimestampNanosecondArray>()
                .unwrap();
            let sums = batch
                .column(2)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            (0..batch.num_rows())
                .map(move |row| (series.value(row), buckets.value(row), sums.value(row)))
        })
        .collect::<Vec<_>>();
    let series_count = RANGE_AGGREGATE_ROWS / partition_count / ROWS_PER_SERIES;
    let mut expected = (0..partition_count)
        .step_by(hours_per_bucket)
        .flat_map(|first_hour| {
            let contributing_hours = hours_per_bucket.min(partition_count - first_hour);
            (0..series_count).map(move |series| {
                (
                    series as u64,
                    first_hour as i64 * HOUR_NANOS,
                    (contributing_hours * ROWS_PER_SERIES) as i64,
                )
            })
        })
        .collect::<Vec<_>>();
    actual.sort_unstable();
    expected.sort_unstable();
    assert_eq!(actual, expected);
}

/// Measures SQL planning and execution over one million rows in hourly ranges.
/// Hourly buckets are local; daily buckets cross ranges and must still shuffle.
/// Run the identical benchmark on main and the feature branch for an A/B comparison.
fn range_partitioned_aggregate(c: &mut Criterion) {
    let runtime = Runtime::new().unwrap();
    for partition_count in [8, 128] {
        let context = range_aggregate_context(partition_count).unwrap();
        for (precision, hours_per_bucket) in [("hour", 1), ("day", 24)] {
            let sql = format!(
                "SELECT series_id, date_trunc('{precision}', timestamp) AS bucket, \
                 sum(value) FROM range_points GROUP BY series_id, bucket"
            );
            let plan = runtime.block_on(async {
                context
                    .sql(&sql)
                    .await
                    .unwrap()
                    .create_physical_plan()
                    .await
                    .unwrap()
            });
            println!(
                "\nRange aggregate: partitions={partition_count}, precision={precision}\n{}",
                displayable(plan.as_ref()).indent(true)
            );
            let batches = runtime
                .block_on(collect(
                    reset_plan_states(Arc::clone(&plan)).unwrap(),
                    context.task_ctx(),
                ))
                .unwrap();
            check_range_aggregate_results(&batches, partition_count, hours_per_bucket);

            let mut group = c.benchmark_group(format!(
                "range_partitioned_aggregate/{partition_count}/{precision}"
            ));
            group.bench_function("planning", |b| {
                b.iter(|| {
                    black_box(runtime.block_on(async {
                        context
                            .sql(&sql)
                            .await
                            .unwrap()
                            .create_physical_plan()
                            .await
                            .unwrap()
                    }))
                })
            });
            group.bench_function("execution", |b| {
                b.iter_batched(
                    || reset_plan_states(Arc::clone(&plan)).unwrap(),
                    |plan| {
                        black_box(
                            runtime.block_on(collect(plan, context.task_ctx())).unwrap(),
                        )
                    },
                    BatchSize::SmallInput,
                )
            });
            group.finish();
        }
    }
}

criterion_group!(benches, criterion_benchmark, range_partitioned_aggregate);
criterion_main!(benches);
