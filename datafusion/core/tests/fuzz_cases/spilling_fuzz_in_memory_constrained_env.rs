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

//! Fuzz Test for different operators in memory constrained environment

use std::pin::Pin;
use std::sync::Arc;

use crate::fuzz_cases::aggregate_fuzz::assert_spill_count_metric;
use crate::fuzz_cases::once_exec::OnceExec;
use arrow::array::UInt64Array;
use arrow::{array::StringArray, compute::SortOptions, record_batch::RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use datafusion::common::Result;
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::physical_plan::expressions::PhysicalSortExpr;
use datafusion::physical_plan::sorts::sort::SortExec;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::SessionConfig;
use datafusion_execution::memory_pool::units::{KB, MB};
use datafusion_execution::memory_pool::{
    FairSpillPool, MemoryConsumer, MemoryReservation,
};
use datafusion_execution::{SendableRecordBatchStream, TaskContext};
use datafusion_functions_aggregate::array_agg::array_agg_udaf;
use datafusion_physical_expr::aggregate::AggregateExprBuilder;
use datafusion_physical_expr::expressions::{col, Column};
use datafusion_physical_expr_common::sort_expr::LexOrdering;
use datafusion_physical_plan::aggregates::{
    AggregateExec, AggregateMode, PhysicalGroupBy,
};
use datafusion_physical_plan::stream::RecordBatchStreamAdapter;
use futures::StreamExt;

#[tokio::test]
async fn test_sort_with_limited_memory() -> Result<()> {
    let record_batch_size = 8192;
    let pool_size = 2 * MB as usize;
    let task_ctx = {
        let memory_pool = Arc::new(FairSpillPool::new(pool_size));
        TaskContext::default()
            .with_session_config(
                SessionConfig::new()
                    .with_batch_size(record_batch_size)
                    .with_sort_spill_reservation_bytes(1),
            )
            .with_runtime(Arc::new(
                RuntimeEnvBuilder::new()
                    .with_memory_pool(memory_pool)
                    .build()?,
            ))
    };

    let record_batch_size = pool_size / 16;

    // Basic test with a lot of groups that cannot all fit in memory and 1 record batch
    // from each spill file is too much memory
    let spill_count = run_sort_test_with_limited_memory(RunTestWithLimitedMemoryArgs {
        pool_size,
        task_ctx: Arc::new(task_ctx),
        number_of_record_batches: 100,
        get_size_of_record_batch_to_generate: Box::pin(move |_| record_batch_size),
        memory_behavior: Default::default(),
    })
    .await?;

    let total_spill_files_size = spill_count * record_batch_size;
    assert!(
    total_spill_files_size > pool_size,
    "Total spill files size {total_spill_files_size} should be greater than pool size {pool_size}",
  );

    Ok(())
}

#[tokio::test]
async fn test_sort_with_limited_memory_and_different_sizes_of_record_batch() -> Result<()>
{
    let record_batch_size = 8192;
    let pool_size = 2 * MB as usize;
    let task_ctx = {
        let memory_pool = Arc::new(FairSpillPool::new(pool_size));
        TaskContext::default()
            .with_session_config(
                SessionConfig::new()
                    .with_batch_size(record_batch_size)
                    .with_sort_spill_reservation_bytes(1),
            )
            .with_runtime(Arc::new(
                RuntimeEnvBuilder::new()
                    .with_memory_pool(memory_pool)
                    .build()?,
            ))
    };

    run_sort_test_with_limited_memory(RunTestWithLimitedMemoryArgs {
        pool_size,
        task_ctx: Arc::new(task_ctx),
        number_of_record_batches: 100,
        get_size_of_record_batch_to_generate: Box::pin(move |i| {
            if i % 25 == 1 {
                pool_size / 6
            } else {
                16 * KB as usize
            }
        }),
        memory_behavior: Default::default(),
    })
    .await?;

    Ok(())
}

#[tokio::test]
async fn test_sort_with_limited_memory_and_different_sizes_of_record_batch_and_changing_memory_reservation(
) -> Result<()> {
    let record_batch_size = 8192;
    let pool_size = 2 * MB as usize;
    let task_ctx = {
        let memory_pool = Arc::new(FairSpillPool::new(pool_size));
        TaskContext::default()
            .with_session_config(
                SessionConfig::new()
                    .with_batch_size(record_batch_size)
                    .with_sort_spill_reservation_bytes(1),
            )
            .with_runtime(Arc::new(
                RuntimeEnvBuilder::new()
                    .with_memory_pool(memory_pool)
                    .build()?,
            ))
    };

    run_sort_test_with_limited_memory(RunTestWithLimitedMemoryArgs {
        pool_size,
        task_ctx: Arc::new(task_ctx),
        number_of_record_batches: 100,
        get_size_of_record_batch_to_generate: Box::pin(move |i| {
            if i % 25 == 1 {
                pool_size / 6
            } else {
                16 * KB as usize
            }
        }),
        memory_behavior: MemoryBehavior::TakeAllMemoryAndReleaseEveryNthBatch(10),
    })
    .await?;

    Ok(())
}

#[tokio::test]
async fn test_sort_with_limited_memory_and_different_sizes_of_record_batch_and_take_all_memory(
) -> Result<()> {
    let record_batch_size = 8192;
    let pool_size = 2 * MB as usize;
    let task_ctx = {
        let memory_pool = Arc::new(FairSpillPool::new(pool_size));
        TaskContext::default()
            .with_session_config(
                SessionConfig::new()
                    .with_batch_size(record_batch_size)
                    .with_sort_spill_reservation_bytes(1),
            )
            .with_runtime(Arc::new(
                RuntimeEnvBuilder::new()
                    .with_memory_pool(memory_pool)
                    .build()?,
            ))
    };

    run_sort_test_with_limited_memory(RunTestWithLimitedMemoryArgs {
        pool_size,
        task_ctx: Arc::new(task_ctx),
        number_of_record_batches: 100,
        get_size_of_record_batch_to_generate: Box::pin(move |i| {
            if i % 25 == 1 {
                pool_size / 6
            } else {
                16 * KB as usize
            }
        }),
        memory_behavior: MemoryBehavior::TakeAllMemoryAtTheBeginning,
    })
    .await?;

    Ok(())
}

#[tokio::test]
async fn test_sort_with_limited_memory_and_large_record_batch() -> Result<()> {
    let record_batch_size = 8192;
    let pool_size = 2 * MB as usize;
    let task_ctx = {
        let memory_pool = Arc::new(FairSpillPool::new(pool_size));
        TaskContext::default()
            .with_session_config(
                SessionConfig::new()
                    .with_batch_size(record_batch_size)
                    .with_sort_spill_reservation_bytes(1),
            )
            .with_runtime(Arc::new(
                RuntimeEnvBuilder::new()
                    .with_memory_pool(memory_pool)
                    .build()?,
            ))
    };

    // Test that the merge degree of multi level merge sort cannot be fixed size when there is not enough memory
    run_sort_test_with_limited_memory(RunTestWithLimitedMemoryArgs {
        pool_size,
        task_ctx: Arc::new(task_ctx),
        number_of_record_batches: 100,
        get_size_of_record_batch_to_generate: Box::pin(move |_| pool_size / 6),
        memory_behavior: Default::default(),
    })
    .await?;

    Ok(())
}

struct RunTestWithLimitedMemoryArgs {
    pool_size: usize,
    task_ctx: Arc<TaskContext>,
    number_of_record_batches: usize,
    get_size_of_record_batch_to_generate:
        Pin<Box<dyn Fn(usize) -> usize + Send + 'static>>,
    memory_behavior: MemoryBehavior,
}

#[derive(Default)]
enum MemoryBehavior {
    #[default]
    AsIs,
    TakeAllMemoryAtTheBeginning,
    TakeAllMemoryAndReleaseEveryNthBatch(usize),
}

async fn run_sort_test_with_limited_memory(
    mut args: RunTestWithLimitedMemoryArgs,
) -> Result<usize> {
    let get_size_of_record_batch_to_generate = std::mem::replace(
        &mut args.get_size_of_record_batch_to_generate,
        Box::pin(move |_| unreachable!("should not be called after take")),
    );

    let scan_schema = Arc::new(Schema::new(vec![
        Field::new("col_0", DataType::UInt64, true),
        Field::new("col_1", DataType::Utf8, true),
    ]));

    let record_batch_size = args.task_ctx.session_config().batch_size() as u64;

    let schema = Arc::clone(&scan_schema);
    let plan: Arc<dyn ExecutionPlan> =
        Arc::new(OnceExec::new(Box::pin(RecordBatchStreamAdapter::new(
            Arc::clone(&schema),
            futures::stream::iter((0..args.number_of_record_batches as u64).map(
                move |index| {
                    let mut record_batch_memory_size =
                        get_size_of_record_batch_to_generate(index as usize);
                    record_batch_memory_size = record_batch_memory_size
                        .saturating_sub(size_of::<u64>() * record_batch_size as usize);

                    let string_item_size =
                        record_batch_memory_size / record_batch_size as usize;
                    let string_array = Arc::new(StringArray::from_iter_values(
                        (0..record_batch_size).map(|_| "a".repeat(string_item_size)),
                    ));

                    RecordBatch::try_new(
                        Arc::clone(&schema),
                        vec![
                            Arc::new(UInt64Array::from_iter_values(
                                (index * record_batch_size)
                                    ..(index * record_batch_size) + record_batch_size,
                            )),
                            string_array,
                        ],
                    )
                    .map_err(|err| err.into())
                },
            )),
        ))));
    let sort_exec = Arc::new(SortExec::new(
        LexOrdering::new(vec![PhysicalSortExpr {
            expr: col("col_0", &scan_schema).unwrap(),
            options: SortOptions {
                descending: false,
                nulls_first: true,
            },
        }])
        .unwrap(),
        plan,
    ));

    let result = sort_exec.execute(0, Arc::clone(&args.task_ctx))?;

    run_test(args, sort_exec, result).await
}

fn grow_memory_as_much_as_possible(
    memory_step: usize,
    memory_reservation: &mut MemoryReservation,
) -> Result<bool> {
    let mut was_able_to_grow = false;
    while memory_reservation.try_grow(memory_step).is_ok() {
        was_able_to_grow = true;
    }

    Ok(was_able_to_grow)
}

#[tokio::test]
async fn test_aggregate_with_high_cardinality_with_limited_memory() -> Result<()> {
    let record_batch_size = 8192;
    let pool_size = 2 * MB as usize;
    let task_ctx = {
        let memory_pool = Arc::new(FairSpillPool::new(pool_size));
        TaskContext::default()
            .with_session_config(SessionConfig::new().with_batch_size(record_batch_size))
            .with_runtime(Arc::new(
                RuntimeEnvBuilder::new()
                    .with_memory_pool(memory_pool)
                    .build()?,
            ))
    };

    let record_batch_size = pool_size / 16;

    // Basic test with a lot of groups that cannot all fit in memory and 1 record batch
    // from each spill file is too much memory
    let spill_count =
        run_test_aggregate_with_high_cardinality(RunTestWithLimitedMemoryArgs {
            pool_size,
            task_ctx: Arc::new(task_ctx),
            number_of_record_batches: 100,
            get_size_of_record_batch_to_generate: Box::pin(move |_| record_batch_size),
            memory_behavior: Default::default(),
        })
        .await?;

    let total_spill_files_size = spill_count * record_batch_size;
    assert!(
    total_spill_files_size > pool_size,
    "Total spill files size {total_spill_files_size} should be greater than pool size {pool_size}",
  );

    Ok(())
}

#[tokio::test]
async fn test_aggregate_with_high_cardinality_with_limited_memory_and_different_sizes_of_record_batch(
) -> Result<()> {
    let record_batch_size = 8192;
    let pool_size = 2 * MB as usize;
    let task_ctx = {
        let memory_pool = Arc::new(FairSpillPool::new(pool_size));
        TaskContext::default()
            .with_session_config(SessionConfig::new().with_batch_size(record_batch_size))
            .with_runtime(Arc::new(
                RuntimeEnvBuilder::new()
                    .with_memory_pool(memory_pool)
                    .build()?,
            ))
    };

    run_test_aggregate_with_high_cardinality(RunTestWithLimitedMemoryArgs {
        pool_size,
        task_ctx: Arc::new(task_ctx),
        number_of_record_batches: 100,
        get_size_of_record_batch_to_generate: Box::pin(move |i| {
            if i % 25 == 1 {
                pool_size / 6
            } else {
                (16 * KB) as usize
            }
        }),
        memory_behavior: Default::default(),
    })
    .await?;

    Ok(())
}

#[tokio::test]
async fn test_aggregate_with_high_cardinality_with_limited_memory_and_different_sizes_of_record_batch_and_changing_memory_reservation(
) -> Result<()> {
    let record_batch_size = 8192;
    let pool_size = 2 * MB as usize;
    let task_ctx = {
        let memory_pool = Arc::new(FairSpillPool::new(pool_size));
        TaskContext::default()
            .with_session_config(SessionConfig::new().with_batch_size(record_batch_size))
            .with_runtime(Arc::new(
                RuntimeEnvBuilder::new()
                    .with_memory_pool(memory_pool)
                    .build()?,
            ))
    };

    run_test_aggregate_with_high_cardinality(RunTestWithLimitedMemoryArgs {
        pool_size,
        task_ctx: Arc::new(task_ctx),
        number_of_record_batches: 100,
        get_size_of_record_batch_to_generate: Box::pin(move |i| {
            if i % 25 == 1 {
                pool_size / 6
            } else {
                (16 * KB) as usize
            }
        }),
        memory_behavior: MemoryBehavior::TakeAllMemoryAndReleaseEveryNthBatch(10),
    })
    .await?;

    Ok(())
}

#[tokio::test]
async fn test_aggregate_with_high_cardinality_with_limited_memory_and_different_sizes_of_record_batch_and_take_all_memory(
) -> Result<()> {
    let record_batch_size = 8192;
    let pool_size = 2 * MB as usize;
    let task_ctx = {
        let memory_pool = Arc::new(FairSpillPool::new(pool_size));
        TaskContext::default()
            .with_session_config(SessionConfig::new().with_batch_size(record_batch_size))
            .with_runtime(Arc::new(
                RuntimeEnvBuilder::new()
                    .with_memory_pool(memory_pool)
                    .build()?,
            ))
    };

    run_test_aggregate_with_high_cardinality(RunTestWithLimitedMemoryArgs {
        pool_size,
        task_ctx: Arc::new(task_ctx),
        number_of_record_batches: 100,
        get_size_of_record_batch_to_generate: Box::pin(move |i| {
            if i % 25 == 1 {
                pool_size / 6
            } else {
                (16 * KB) as usize
            }
        }),
        memory_behavior: MemoryBehavior::TakeAllMemoryAtTheBeginning,
    })
    .await?;

    Ok(())
}

#[tokio::test]
async fn test_aggregate_with_high_cardinality_with_limited_memory_and_large_record_batch(
) -> Result<()> {
    let record_batch_size = 8192;
    let pool_size = 2 * MB as usize;
    let task_ctx = {
        let memory_pool = Arc::new(FairSpillPool::new(pool_size));
        TaskContext::default()
            .with_session_config(SessionConfig::new().with_batch_size(record_batch_size))
            .with_runtime(Arc::new(
                RuntimeEnvBuilder::new()
                    .with_memory_pool(memory_pool)
                    .build()?,
            ))
    };

    // Test that the merge degree of multi level merge sort cannot be fixed size when there is not enough memory
    run_test_aggregate_with_high_cardinality(RunTestWithLimitedMemoryArgs {
        pool_size,
        task_ctx: Arc::new(task_ctx),
        number_of_record_batches: 100,
        get_size_of_record_batch_to_generate: Box::pin(move |_| pool_size / 6),
        memory_behavior: Default::default(),
    })
    .await?;

    Ok(())
}

async fn run_test_aggregate_with_high_cardinality(
    mut args: RunTestWithLimitedMemoryArgs,
) -> Result<usize> {
    let get_size_of_record_batch_to_generate = std::mem::replace(
        &mut args.get_size_of_record_batch_to_generate,
        Box::pin(move |_| unreachable!("should not be called after take")),
    );
    let scan_schema = Arc::new(Schema::new(vec![
        Field::new("col_0", DataType::UInt64, true),
        Field::new("col_1", DataType::Utf8, true),
    ]));

    let group_by = PhysicalGroupBy::new_single(vec![(
        Arc::new(Column::new("col_0", 0)),
        "col_0".to_string(),
    )]);

    let aggregate_expressions = vec![Arc::new(
        AggregateExprBuilder::new(
            array_agg_udaf(),
            vec![col("col_1", &scan_schema).unwrap()],
        )
        .schema(Arc::clone(&scan_schema))
        .alias("array_agg(col_1)")
        .build()?,
    )];

    let record_batch_size = args.task_ctx.session_config().batch_size() as u64;

    let schema = Arc::clone(&scan_schema);
    let plan: Arc<dyn ExecutionPlan> =
        Arc::new(OnceExec::new(Box::pin(RecordBatchStreamAdapter::new(
            Arc::clone(&schema),
            futures::stream::iter((0..args.number_of_record_batches as u64).map(
                move |index| {
                    let mut record_batch_memory_size =
                        get_size_of_record_batch_to_generate(index as usize);
                    record_batch_memory_size = record_batch_memory_size
                        .saturating_sub(size_of::<u64>() * record_batch_size as usize);

                    let string_item_size =
                        record_batch_memory_size / record_batch_size as usize;
                    let string_array = Arc::new(StringArray::from_iter_values(
                        (0..record_batch_size).map(|_| "a".repeat(string_item_size)),
                    ));

                    RecordBatch::try_new(
                        Arc::clone(&schema),
                        vec![
                            // Grouping key
                            Arc::new(UInt64Array::from_iter_values(
                                (index * record_batch_size)
                                    ..(index * record_batch_size) + record_batch_size,
                            )),
                            // Grouping value
                            string_array,
                        ],
                    )
                    .map_err(|err| err.into())
                },
            )),
        ))));

    let aggregate_exec = Arc::new(AggregateExec::try_new(
        AggregateMode::Partial,
        group_by.clone(),
        aggregate_expressions.clone(),
        vec![None; aggregate_expressions.len()],
        plan,
        Arc::clone(&scan_schema),
    )?);
    let aggregate_final = Arc::new(AggregateExec::try_new(
        AggregateMode::Final,
        group_by,
        aggregate_expressions.clone(),
        vec![None; aggregate_expressions.len()],
        aggregate_exec,
        Arc::clone(&scan_schema),
    )?);

    let result = aggregate_final.execute(0, Arc::clone(&args.task_ctx))?;

    run_test(args, aggregate_final, result).await
}

async fn run_test(
    args: RunTestWithLimitedMemoryArgs,
    plan: Arc<dyn ExecutionPlan>,
    result_stream: SendableRecordBatchStream,
) -> Result<usize> {
    let number_of_record_batches = args.number_of_record_batches;

    consume_stream_and_simulate_other_running_memory_consumers(args, result_stream)
        .await?;

    let spill_count = assert_spill_count_metric(true, plan);

    assert!(
        spill_count > 0,
        "Expected spill, but did not, number of record batches: {number_of_record_batches}",
    );

    Ok(spill_count)
}

/// Consume the stream and change the amount of memory used while consuming it based on the [`MemoryBehavior`] provided
async fn consume_stream_and_simulate_other_running_memory_consumers(
    args: RunTestWithLimitedMemoryArgs,
    mut result_stream: SendableRecordBatchStream,
) -> Result<()> {
    let mut number_of_rows = 0;
    let record_batch_size = args.task_ctx.session_config().batch_size() as u64;

    let memory_pool = args.task_ctx.memory_pool();
    let memory_consumer = MemoryConsumer::new("mock_memory_consumer");
    let mut memory_reservation = memory_consumer.register(memory_pool);

    let mut index = 0;
    let mut memory_took = false;

    while let Some(batch) = result_stream.next().await {
        match args.memory_behavior {
            MemoryBehavior::AsIs => {
                // Do nothing
            }
            MemoryBehavior::TakeAllMemoryAtTheBeginning => {
                if !memory_took {
                    memory_took = true;
                    grow_memory_as_much_as_possible(10, &mut memory_reservation)?;
                }
            }
            MemoryBehavior::TakeAllMemoryAndReleaseEveryNthBatch(n) => {
                if !memory_took {
                    memory_took = true;
                    grow_memory_as_much_as_possible(
                        args.pool_size,
                        &mut memory_reservation,
                    )?;
                } else if index % n == 0 {
                    // release memory
                    memory_reservation.free();
                }
            }
        }

        let batch = batch?;
        number_of_rows += batch.num_rows();

        index += 1;
    }

    assert_eq!(
        number_of_rows,
        args.number_of_record_batches * record_batch_size as usize
    );

    Ok(())
}
