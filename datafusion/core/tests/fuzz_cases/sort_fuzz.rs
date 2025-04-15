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

//! Fuzz Test for various corner cases sorting RecordBatches exceeds available memory and should spill

use std::pin::Pin;
use std::sync::Arc;

use arrow::array::UInt64Array;
use arrow::{
    array::{as_string_array, ArrayRef, Int32Array, StringArray},
    compute::SortOptions,
    record_batch::RecordBatch,
};
use arrow_schema::{DataType, Field, Schema};
use datafusion::common::Result;
use datafusion::datasource::memory::MemorySourceConfig;
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::physical_plan::expressions::PhysicalSortExpr;
use datafusion::physical_plan::sorts::sort::SortExec;
use datafusion::physical_plan::{collect, ExecutionPlan};
use datafusion::prelude::{SessionConfig, SessionContext};
use datafusion_common::cast::as_int32_array;
use datafusion_execution::memory_pool::{
    FairSpillPool, GreedyMemoryPool, MemoryConsumer, MemoryReservation,
};
use datafusion_physical_expr::expressions::col;
use datafusion_physical_expr_common::sort_expr::LexOrdering;
use futures::StreamExt;

use crate::fuzz_cases::stream_exec::StreamExec;
use datafusion_execution::memory_pool::units::MB;
use datafusion_execution::TaskContext;
use datafusion_physical_plan::stream::RecordBatchStreamAdapter;
use rand::Rng;
use test_utils::{batches_to_vec, partitions_to_sorted_vec};

const KB: usize = 1 << 10;
#[tokio::test]
#[cfg_attr(tarpaulin, ignore)]
async fn test_sort_10k_mem() {
    for (batch_size, should_spill) in [(5, false), (20000, true), (500000, true)] {
        let (input, collected) = SortTest::new()
            .with_int32_batches(batch_size)
            .with_sort_columns(vec!["x"])
            .with_pool_size(10 * KB)
            .with_should_spill(should_spill)
            .run()
            .await;

        let expected = partitions_to_sorted_vec(&input);
        let actual = batches_to_vec(&collected);
        assert_eq!(expected, actual, "failure in @ batch_size {batch_size:?}");
    }
}

#[tokio::test]
#[cfg_attr(tarpaulin, ignore)]
async fn test_sort_100k_mem() {
    for (batch_size, should_spill) in
        [(5, false), (10000, false), (20000, true), (1000000, true)]
    {
        let (input, collected) = SortTest::new()
            .with_int32_batches(batch_size)
            .with_sort_columns(vec!["x"])
            .with_pool_size(100 * KB)
            .with_should_spill(should_spill)
            .run()
            .await;

        let expected = partitions_to_sorted_vec(&input);
        let actual = batches_to_vec(&collected);
        assert_eq!(expected, actual, "failure in @ batch_size {batch_size:?}");
    }
}

#[tokio::test]
#[cfg_attr(tarpaulin, ignore)]
async fn test_sort_strings_100k_mem() {
    for (batch_size, should_spill) in
        [(5, false), (1000, false), (10000, true), (20000, true)]
    {
        let (input, collected) = SortTest::new()
            .with_utf8_batches(batch_size)
            .with_sort_columns(vec!["x"])
            .with_pool_size(100 * KB)
            .with_should_spill(should_spill)
            .run()
            .await;

        let mut input = input
            .iter()
            .flat_map(|p| p.iter())
            .flat_map(|b| {
                let array = b.column(0);
                as_string_array(array)
                    .iter()
                    .map(|s| s.unwrap().to_string())
            })
            .collect::<Vec<String>>();
        input.sort_unstable();
        let actual = collected
            .iter()
            .flat_map(|b| {
                let array = b.column(0);
                as_string_array(array)
                    .iter()
                    .map(|s| s.unwrap().to_string())
            })
            .collect::<Vec<String>>();
        assert_eq!(input, actual);
    }
}

#[tokio::test]
#[cfg_attr(tarpaulin, ignore)]
async fn test_sort_multi_columns_100k_mem() {
    for (batch_size, should_spill) in
        [(5, false), (1000, false), (10000, true), (20000, true)]
    {
        let (input, collected) = SortTest::new()
            .with_int32_utf8_batches(batch_size)
            .with_sort_columns(vec!["x", "y"])
            .with_pool_size(100 * KB)
            .with_should_spill(should_spill)
            .run()
            .await;

        fn record_batch_to_vec(b: &RecordBatch) -> Vec<(i32, String)> {
            let mut rows: Vec<_> = Vec::new();
            let i32_array = as_int32_array(b.column(0)).unwrap();
            let string_array = as_string_array(b.column(1));
            for i in 0..b.num_rows() {
                let str = string_array.value(i).to_string();
                let i32 = i32_array.value(i);
                rows.push((i32, str));
            }
            rows
        }
        let mut input = input
            .iter()
            .flat_map(|p| p.iter())
            .flat_map(record_batch_to_vec)
            .collect::<Vec<(i32, String)>>();
        input.sort_unstable();
        let actual = collected
            .iter()
            .flat_map(record_batch_to_vec)
            .collect::<Vec<(i32, String)>>();
        assert_eq!(input, actual);
    }
}

#[tokio::test]
async fn test_sort_unlimited_mem() {
    for (batch_size, should_spill) in [(5, false), (20000, false), (1000000, false)] {
        let (input, collected) = SortTest::new()
            .with_int32_batches(batch_size)
            .with_sort_columns(vec!["x"])
            .with_pool_size(usize::MAX)
            .with_should_spill(should_spill)
            .run()
            .await;

        let expected = partitions_to_sorted_vec(&input);
        let actual = batches_to_vec(&collected);
        assert_eq!(expected, actual, "failure in @ batch_size {batch_size:?}");
    }
}

#[derive(Debug, Default)]
struct SortTest {
    input: Vec<Vec<RecordBatch>>,
    /// The names of the columns to sort by
    sort_columns: Vec<String>,
    /// GreedyMemoryPool size, if specified
    pool_size: Option<usize>,
    /// If true, expect the sort to spill
    should_spill: bool,
}

impl SortTest {
    fn new() -> Self {
        Default::default()
    }

    fn with_sort_columns(mut self, sort_columns: Vec<&str>) -> Self {
        self.sort_columns = sort_columns.iter().map(|s| s.to_string()).collect();
        self
    }

    /// Create batches of int32 values of rows
    fn with_int32_batches(mut self, rows: usize) -> Self {
        self.input = vec![make_staggered_i32_batches(rows)];
        self
    }

    /// Create batches of utf8 values of rows
    fn with_utf8_batches(mut self, rows: usize) -> Self {
        self.input = vec![make_staggered_utf8_batches(rows)];
        self
    }

    /// Create batches of int32 and utf8 values of rows
    fn with_int32_utf8_batches(mut self, rows: usize) -> Self {
        self.input = vec![make_staggered_i32_utf8_batches(rows)];
        self
    }

    /// specify that this test should use a memory pool of the specified size
    fn with_pool_size(mut self, pool_size: usize) -> Self {
        self.pool_size = Some(pool_size);
        self
    }

    fn with_should_spill(mut self, should_spill: bool) -> Self {
        self.should_spill = should_spill;
        self
    }

    /// Sort the input using SortExec and ensure the results are
    /// correct according to `Vec::sort` both with and without spilling
    async fn run(&self) -> (Vec<Vec<RecordBatch>>, Vec<RecordBatch>) {
        let input = self.input.clone();
        let first_batch = input
            .iter()
            .flat_map(|p| p.iter())
            .next()
            .expect("at least one batch");
        let schema = first_batch.schema();

        let sort_ordering = LexOrdering::new(
            self.sort_columns
                .iter()
                .map(|c| PhysicalSortExpr {
                    expr: col(c, &schema).unwrap(),
                    options: SortOptions {
                        descending: false,
                        nulls_first: true,
                    },
                })
                .collect(),
        );

        let exec = MemorySourceConfig::try_new_exec(&input, schema, None).unwrap();
        let sort = Arc::new(SortExec::new(sort_ordering, exec));

        let session_config = SessionConfig::new();
        let session_ctx = if let Some(pool_size) = self.pool_size {
            // Make sure there is enough space for the initial spill
            // reservation
            let pool_size = pool_size.saturating_add(
                session_config
                    .options()
                    .execution
                    .sort_spill_reservation_bytes,
            );

            let runtime = RuntimeEnvBuilder::new()
                .with_memory_pool(Arc::new(GreedyMemoryPool::new(pool_size)))
                .build_arc()
                .unwrap();
            SessionContext::new_with_config_rt(session_config, runtime)
        } else {
            SessionContext::new_with_config(session_config)
        };

        let task_ctx = session_ctx.task_ctx();
        let collected = collect(sort.clone(), task_ctx).await.unwrap();

        if self.should_spill {
            assert_ne!(
                sort.metrics().unwrap().spill_count().unwrap(),
                0,
                "Expected spill, but did not: {self:?}"
            );
        } else {
            assert_eq!(
                sort.metrics().unwrap().spill_count().unwrap(),
                0,
                "Expected no spill, but did: {self:?}"
            );
        }

        assert_eq!(
            session_ctx.runtime_env().memory_pool.reserved(),
            0,
            "The sort should have returned all memory used back to the memory pool"
        );

        (input, collected)
    }
}

/// Return randomly sized record batches in a field named 'x' of type `Int32`
/// with randomized i32 content
fn make_staggered_i32_batches(len: usize) -> Vec<RecordBatch> {
    let mut rng = rand::thread_rng();
    let max_batch = 1024;

    let mut batches = vec![];
    let mut remaining = len;
    while remaining != 0 {
        let to_read = rng.gen_range(0..=remaining.min(max_batch));
        remaining -= to_read;

        batches.push(
            RecordBatch::try_from_iter(vec![(
                "x",
                Arc::new(Int32Array::from_iter_values(
                    (0..to_read).map(|_| rng.gen()),
                )) as ArrayRef,
            )])
            .unwrap(),
        )
    }
    batches
}

/// Return randomly sized record batches in a field named 'x' of type `Utf8`
/// with randomized content
fn make_staggered_utf8_batches(len: usize) -> Vec<RecordBatch> {
    let mut rng = rand::thread_rng();
    let max_batch = 1024;

    let mut batches = vec![];
    let mut remaining = len;
    while remaining != 0 {
        let to_read = rng.gen_range(0..=remaining.min(max_batch));
        remaining -= to_read;

        batches.push(
            RecordBatch::try_from_iter(vec![(
                "x",
                Arc::new(StringArray::from_iter_values(
                    (0..to_read).map(|_| format!("test_string_{}", rng.gen::<u32>())),
                )) as ArrayRef,
            )])
            .unwrap(),
        )
    }
    batches
}

/// Return randomly sized record batches in a field named 'x' of type `Int32`
/// with randomized i32 content and a field named 'y' of type `Utf8`
/// with randomized content
fn make_staggered_i32_utf8_batches(len: usize) -> Vec<RecordBatch> {
    let mut rng = rand::thread_rng();
    let max_batch = 1024;

    let mut batches = vec![];
    let mut remaining = len;
    while remaining != 0 {
        let to_read = rng.gen_range(0..=remaining.min(max_batch));
        remaining -= to_read;

        batches.push(
            RecordBatch::try_from_iter(vec![
                (
                    "x",
                    Arc::new(Int32Array::from_iter_values(
                        (0..to_read).map(|_| rng.gen()),
                    )) as ArrayRef,
                ),
                (
                    "y",
                    Arc::new(StringArray::from_iter_values(
                        (0..to_read).map(|_| format!("test_string_{}", rng.gen::<u32>())),
                    )) as ArrayRef,
                ),
            ])
            .unwrap(),
        )
    }

    batches
}

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
    let spill_count =
        run_sort_test_with_limited_memory(RunSortTestWithLimitedMemoryArgs {
            pool_size,
            task_ctx,
            number_of_record_batches: 100,
            get_size_of_record_batch_to_generate: Box::pin(move |_| record_batch_size),
            memory_behavior: Default::default(),
        })
        .await?;

    let total_spill_files_size = spill_count * record_batch_size;
    assert!(
        total_spill_files_size > pool_size,
        "Total spill files size {} should be greater than pool size {}",
        total_spill_files_size,
        pool_size
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

    run_sort_test_with_limited_memory(RunSortTestWithLimitedMemoryArgs {
        pool_size,
        task_ctx,
        number_of_record_batches: 100,
        get_size_of_record_batch_to_generate: Box::pin(move |i| {
            if i % 25 == 1 {
                pool_size / 4
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

    run_sort_test_with_limited_memory(RunSortTestWithLimitedMemoryArgs {
        pool_size,
        task_ctx,
        number_of_record_batches: 100,
        get_size_of_record_batch_to_generate: Box::pin(move |i| {
            if i % 25 == 1 {
                pool_size / 4
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

    run_sort_test_with_limited_memory(RunSortTestWithLimitedMemoryArgs {
        pool_size,
        task_ctx,
        number_of_record_batches: 100,
        get_size_of_record_batch_to_generate: Box::pin(move |i| {
            if i % 25 == 1 {
                pool_size / 4
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
    run_sort_test_with_limited_memory(RunSortTestWithLimitedMemoryArgs {
        pool_size,
        task_ctx,
        number_of_record_batches: 100,
        get_size_of_record_batch_to_generate: Box::pin(move |_| pool_size / 4),
        memory_behavior: Default::default(),
    })
    .await?;

    Ok(())
}

struct RunSortTestWithLimitedMemoryArgs {
    pool_size: usize,
    task_ctx: TaskContext,
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
    args: RunSortTestWithLimitedMemoryArgs,
) -> Result<usize> {
    let RunSortTestWithLimitedMemoryArgs {
        pool_size,
        task_ctx,
        number_of_record_batches,
        get_size_of_record_batch_to_generate,
        memory_behavior,
    } = args;
    let scan_schema = Arc::new(Schema::new(vec![
        Field::new("col_0", DataType::UInt64, true),
        Field::new("col_1", DataType::Utf8, true),
    ]));

    let record_batch_size = task_ctx.session_config().batch_size() as u64;

    let schema = Arc::clone(&scan_schema);
    let plan: Arc<dyn ExecutionPlan> =
        Arc::new(StreamExec::new(Box::pin(RecordBatchStreamAdapter::new(
            Arc::clone(&schema),
            futures::stream::iter((0..number_of_record_batches as u64).map(
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
        }]),
        plan,
    ));

    let task_ctx = Arc::new(task_ctx);

    let mut result = sort_exec.execute(0, Arc::clone(&task_ctx))?;

    let mut number_of_rows = 0;

    let memory_pool = task_ctx.memory_pool();
    let memory_consumer = MemoryConsumer::new("mock_memory_consumer");
    let mut memory_reservation = memory_consumer.register(memory_pool);

    let mut index = 0;
    let mut memory_took = false;

    while let Some(batch) = result.next().await {
        match memory_behavior {
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
                    grow_memory_as_much_as_possible(pool_size, &mut memory_reservation)?;
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
        number_of_record_batches * record_batch_size as usize
    );

    let spill_count = sort_exec.metrics().unwrap().spill_count().unwrap();
    assert!(
        spill_count > 0,
        "Expected spill, but did not: {number_of_record_batches:?}"
    );

    Ok(spill_count)
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
