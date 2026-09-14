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

use std::sync::Arc;
use std::time::Duration;

use arrow::array::{ArrayRef, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use datafusion_common::test_util::batches_to_sort_string;
use datafusion_common::{JoinSide, JoinType, NullEquality, Result};
use datafusion_execution::TaskContext;
use datafusion_execution::config::SessionConfig;
use datafusion_execution::memory_pool::GreedyMemoryPool;
use datafusion_execution::runtime_env::RuntimeEnvBuilder;
use datafusion_expr::Operator;
use datafusion_physical_expr::PhysicalExprRef;
use datafusion_physical_expr::expressions::{BinaryExpr, Column};
use futures::StreamExt;

use super::HashJoinExecBuilder;
use super::selection::SelectionExchange;
use crate::joins::PartitionMode;
use crate::joins::utils::{ColumnIndex, JoinFilter};
use crate::repartition::RepartitionExec;
use crate::test::TestMemoryExec;
use crate::{ExecutionPlan, Partitioning, collect};

fn source(seed: usize, width: usize) -> Result<Arc<dyn ExecutionPlan>> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("key", DataType::Int64, true),
        Field::new("id", DataType::Int64, false),
        Field::new("payload", DataType::Utf8, false),
    ]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from_iter(
                (0..137).map(|i| (i % 11 != 0).then_some(((i * 17 + seed) % 23) as i64)),
            )) as ArrayRef,
            Arc::new(Int64Array::from_iter_values((0..137).map(|i| i as i64))),
            Arc::new(StringArray::from_iter_values(
                (0..137).map(|i| format!("row-{i}-{}", "x".repeat(width))),
            )),
        ],
    )?;
    let mut partitions = vec![vec![], vec![], vec![]];
    let step = 7 + seed;
    for start in (0..137).step_by(step) {
        partitions[start % 3].push(batch.slice(start, step.min(137 - start)));
    }
    Ok(TestMemoryExec::try_new_exec(&partitions, schema, None)?)
}

fn context(batch_size: usize, array_map: bool, bounded: bool) -> Arc<TaskContext> {
    let mut config = SessionConfig::new().with_batch_size(batch_size);
    if !array_map {
        config
            .options_mut()
            .execution
            .perfect_hash_join_small_build_threshold = 0;
        config
            .options_mut()
            .execution
            .perfect_hash_join_min_key_density = f64::INFINITY;
    }
    let mut context = TaskContext::default().with_session_config(config);
    if bounded {
        context = context.with_runtime(
            RuntimeEnvBuilder::new()
                .with_memory_pool(Arc::new(GreedyMemoryPool::new(16 * 1024 * 1024)))
                .build_arc()
                .unwrap(),
        );
    }
    Arc::new(context)
}

async fn run_join(
    enabled: bool,
    join_type: JoinType,
    seed: usize,
    partitions: usize,
    filtered: bool,
    null_equality: NullEquality,
    context: Arc<TaskContext>,
) -> Result<(Vec<RecordBatch>, usize)> {
    let mut config = context.session_config().clone();
    config
        .options_mut()
        .execution
        .enable_hash_join_probe_selection = enabled;
    let context = Arc::new(
        TaskContext::default()
            .with_runtime(context.runtime_env())
            .with_session_config(config),
    );
    let key: PhysicalExprRef = Arc::new(Column::new("key", 0));
    let left = Arc::new(RepartitionExec::try_new(
        source(seed, 64)?,
        Partitioning::Hash(vec![Arc::clone(&key)], partitions),
    )?);
    let right = Arc::new(RepartitionExec::try_new(
        source(seed + 1, 128)?,
        Partitioning::Hash(vec![Arc::clone(&key)], partitions),
    )?);
    let filter = filtered.then(|| {
        JoinFilter::new(
            Arc::new(BinaryExpr::new(
                Arc::new(Column::new("l", 0)),
                Operator::Lt,
                Arc::new(Column::new("r", 1)),
            )),
            vec![
                ColumnIndex {
                    index: 1,
                    side: JoinSide::Left,
                },
                ColumnIndex {
                    index: 1,
                    side: JoinSide::Right,
                },
            ],
            Arc::new(Schema::new(vec![
                Field::new("l", DataType::Int64, false),
                Field::new("r", DataType::Int64, false),
            ])),
        )
    });
    let join = Arc::new(
        HashJoinExecBuilder::new(left, right, vec![(Arc::clone(&key), key)], join_type)
            .with_partition_mode(PartitionMode::Partitioned)
            .with_filter(filter)
            .with_null_equality(null_equality)
            .build()?,
    );
    let output = tokio::time::timeout(
        Duration::from_secs(10),
        collect(
            Arc::clone(&join) as Arc<dyn ExecutionPlan>,
            Arc::clone(&context),
        ),
    )
    .await
    .expect("join must not deadlock")?;
    let used = join
        .metrics()
        .unwrap()
        .sum_by_name("probe_selection_partitions")
        .map_or(0, |m| m.as_usize());
    drop(join);
    // Producers abort asynchronously when the last output is dropped.
    tokio::time::timeout(Duration::from_secs(5), async {
        while context.memory_pool().reserved() != 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("all reservations released");
    Ok((output, used))
}

#[tokio::test]
async fn selected_probe_matches_materialized_join() -> Result<()> {
    for seed in 1..=3 {
        for partitions in [2, 8, 32] {
            for array_map in [false, true] {
                for filtered in [false, true] {
                    for nulls in [
                        NullEquality::NullEqualsNothing,
                        NullEquality::NullEqualsNull,
                    ] {
                        let ctx = context(5, array_map, false);
                        let (expected, ordinary) = run_join(
                            false,
                            JoinType::Inner,
                            seed,
                            partitions,
                            filtered,
                            nulls,
                            Arc::clone(&ctx),
                        )
                        .await?;
                        let (actual, selected) = run_join(
                            true,
                            JoinType::Inner,
                            seed,
                            partitions,
                            filtered,
                            nulls,
                            ctx,
                        )
                        .await?;
                        assert_eq!(ordinary, 0);
                        assert_eq!(selected, partitions);
                        assert_eq!(
                            batches_to_sort_string(&actual),
                            batches_to_sort_string(&expected)
                        );
                        assert!(actual.iter().all(|b| b.num_rows() <= 5));
                    }
                }
            }
        }
    }
    Ok(())
}

#[tokio::test]
async fn unsupported_selection_uses_ordinary_exchange() -> Result<()> {
    for (join_type, bounded, partitions) in [
        (JoinType::Left, false, 4),
        (JoinType::Inner, true, 4),
        (JoinType::Inner, false, 1),
    ] {
        let ctx = context(7, false, bounded);
        let (expected, _) = run_join(
            false,
            join_type,
            2,
            partitions,
            true,
            NullEquality::NullEqualsNothing,
            Arc::clone(&ctx),
        )
        .await?;
        let (actual, used) = run_join(
            true,
            join_type,
            2,
            partitions,
            true,
            NullEquality::NullEqualsNothing,
            ctx,
        )
        .await?;
        assert_eq!(used, 0);
        assert_eq!(
            batches_to_sort_string(&actual),
            batches_to_sort_string(&expected)
        );
    }
    Ok(())
}

#[tokio::test]
async fn selection_cancel_releases_unclaimed_partitions() -> Result<()> {
    let ctx = context(5, false, false);
    let exchange = Arc::new(SelectionExchange::default());
    let key: PhysicalExprRef = Arc::new(Column::new("key", 0));
    let stream = exchange.execute(source(1, 4096)?.as_ref(), &[key], 8, 0, &ctx)?;
    tokio::time::timeout(Duration::from_secs(5), async {
        while ctx.memory_pool().reserved() == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("producer started");
    drop(stream);
    // Keep the plan/exchange alive deliberately. It must not retain the other
    // seven output queues or keep the producer tasks alive.
    tokio::time::timeout(Duration::from_secs(5), async {
        while ctx.memory_pool().reserved() != 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("canceled exchange leaked reservations");
    drop(exchange);
    Ok(())
}

#[tokio::test]
async fn selection_propagates_input_failure_without_polling_other_outputs() -> Result<()>
{
    use crate::test::exec::{MockExec, PanicExec};
    let schema = source(1, 0)?.schema();
    let sources: Vec<Arc<dyn ExecutionPlan>> = vec![
        Arc::new(MockExec::new(
            vec![datafusion_common::exec_err!("selection input failure")],
            Arc::clone(&schema),
        )),
        Arc::new(PanicExec::new(schema, 1)),
    ];
    for source in sources {
        let ctx = context(5, false, false);
        let exchange = Arc::new(SelectionExchange::default());
        let key: PhysicalExprRef = Arc::new(Column::new("key", 0));
        let mut stream = exchange.execute(source.as_ref(), &[key], 8, 7, &ctx)?;
        let result = tokio::time::timeout(Duration::from_secs(5), stream.next())
            .await
            .expect("an idle output must not block error delivery")
            .unwrap();
        let error = result
            .expect_err("input failure must propagate")
            .to_string();
        assert!(
            error.contains("selection input failure") || error.contains("panicked"),
            "{error}"
        );
        drop(stream);
        assert_eq!(ctx.memory_pool().reserved(), 0);
    }
    Ok(())
}

#[tokio::test]
async fn selection_reservation_failure_is_reported_and_released() -> Result<()> {
    // Exercise the private exchange's error boundary directly. Public execution
    // uses the spill-capable ordinary exchange for a finite pool.
    let pool = Arc::new(GreedyMemoryPool::new(1));
    let runtime = RuntimeEnvBuilder::new()
        .with_memory_pool(pool)
        .build_arc()?;
    let ctx = Arc::new(TaskContext::default().with_runtime(runtime));
    let exchange = Arc::new(SelectionExchange::default());
    let key: PhysicalExprRef = Arc::new(Column::new("key", 0));
    let mut stream = exchange.execute(source(1, 128)?.as_ref(), &[key], 8, 7, &ctx)?;
    let result = tokio::time::timeout(Duration::from_secs(5), stream.next())
        .await
        .expect("reservation failure must propagate")
        .unwrap();
    assert!(
        result
            .err()
            .unwrap()
            .to_string()
            .contains("Resources exhausted")
    );
    drop(stream);
    assert_eq!(ctx.memory_pool().reserved(), 0);
    Ok(())
}
