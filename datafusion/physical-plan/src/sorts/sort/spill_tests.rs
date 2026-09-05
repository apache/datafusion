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

use super::{
    ExternalSorter, get_reserved_bytes_for_record_batch, sort_batch, sort_batch_chunked,
};
use crate::metrics::ExecutionPlanMetricsSet;
use crate::spill::get_record_batch_memory_size;
use crate::spill::spill_manager::GetSlicedSize;
use arrow::array::{
    ArrayRef, Decimal128Array, DictionaryArray, Int8Array, Int64Array, StringArray,
    StringViewArray,
};
use arrow::compute::{SortOptions, concat_batches};
use arrow::datatypes::{DataType, Field, Int8Type, Schema};
use arrow::record_batch::RecordBatch;
use datafusion_common::config::{ExecutionOptions, SpillCompression};
use datafusion_common::{DataFusionError, Result};
use datafusion_execution::disk_manager::{DiskManagerBuilder, DiskManagerMode};
use datafusion_execution::memory_pool::{
    GreedyMemoryPool, MemoryConsumer, MemoryLimit, MemoryPool, MemoryReservation,
};
use datafusion_execution::runtime_env::{RuntimeEnv, RuntimeEnvBuilder};
use datafusion_physical_expr::expressions::Column;
use datafusion_physical_expr::{LexOrdering, PhysicalSortExpr};
use futures::TryStreamExt;
use std::fmt::{Display, Formatter};
use std::sync::{Arc, Mutex};
use std::time::Duration;

const MERGE_BYTES: usize = 64 * 1024;
const BUFFERED_BATCHES: usize = 9;

#[derive(Debug, Default)]
struct AllocationState {
    used: usize,
    limit: usize,
    peak: usize,
    denied: usize,
    unchecked_over_limit: usize,
}

/// Existing allocations survive a lower limit, but fresh allocations must fit.
/// The test changes the limit before insertion, independently of spill internals.
#[derive(Debug)]
struct AdjustablePool {
    capacity: usize,
    state: Mutex<AllocationState>,
}

impl AdjustablePool {
    fn new(capacity: usize) -> Arc<Self> {
        Arc::new(Self {
            capacity,
            state: Mutex::new(AllocationState {
                limit: capacity,
                ..Default::default()
            }),
        })
    }

    fn set_limit(&self, limit: usize) {
        assert!(limit <= self.capacity);
        self.state.lock().unwrap().limit = limit;
    }
}

impl Display for AdjustablePool {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "AdjustablePool")
    }
}

impl MemoryPool for AdjustablePool {
    fn name(&self) -> &str {
        "AdjustablePool"
    }

    fn grow(&self, _: &MemoryReservation, additional: usize) {
        // Honor the infallible API, but detect attempts to evade try_grow.
        let mut state = self.state.lock().unwrap();
        state.used = state.used.checked_add(additional).unwrap();
        state.peak = state.peak.max(state.used);
        if additional > 0 && state.used > state.limit {
            state.unchecked_over_limit += 1;
        }
    }

    fn shrink(&self, _: &MemoryReservation, shrink: usize) {
        let mut state = self.state.lock().unwrap();
        state.used = state.used.checked_sub(shrink).unwrap();
    }

    fn try_grow(&self, _: &MemoryReservation, additional: usize) -> Result<()> {
        let mut state = self.state.lock().unwrap();
        if additional == 0 {
            return Ok(());
        }
        let requested = state.used.checked_add(additional).unwrap();
        if requested > state.limit {
            state.denied += 1;
            return Err(DataFusionError::ResourcesExhausted(
                "allocation limit reached".into(),
            ));
        }
        state.used = requested;
        state.peak = state.peak.max(requested);
        Ok(())
    }

    fn reserved(&self) -> usize {
        self.state.lock().unwrap().used
    }
    fn memory_limit(&self) -> MemoryLimit {
        MemoryLimit::Finite(self.capacity)
    }
}

struct Fixture {
    parent: RecordBatch,
    batches: Vec<RecordBatch>,
    ordering: LexOrdering,
    capacity: usize,
}

fn fixture() -> Result<Fixture> {
    let rows = 1024;
    let count = 30;
    let total = rows * count;
    let schema = Arc::new(Schema::new(vec![
        Field::new("category", DataType::Utf8, true),
        Field::new("sales", DataType::Decimal128(28, 2), true),
        Field::new("payload", DataType::Utf8, false),
    ]));
    let categories = StringArray::from_iter(
        (0..total).map(|i| (i % 11 != 0).then(|| format!("category-{:04}", i % 100))),
    );
    // Every non-null sales value is distinct, so sorting has no ambiguous ties.
    let sales = Decimal128Array::from_iter(
        (0..total).map(|i| (i != 1).then_some(((total - i) * 100) as i128)),
    )
    .with_precision_and_scale(28, 2)?;
    let payload = StringArray::from_iter_values(
        (0..total).map(|i| format!("row-{i:08}-{}", "x".repeat(64))),
    );
    let columns: Vec<ArrayRef> =
        vec![Arc::new(categories), Arc::new(sales), Arc::new(payload)];
    let parent = RecordBatch::try_new(schema, columns)?;
    let batches: Vec<_> = (0..count).map(|i| parent.slice(i * rows, rows)).collect();
    let ordering = [
        PhysicalSortExpr::new(
            Arc::new(Column::new("category", 0)),
            SortOptions {
                descending: false,
                nulls_first: false,
            },
        ),
        PhysicalSortExpr::new(
            Arc::new(Column::new("sales", 1)),
            SortOptions {
                descending: true,
                nulls_first: true,
            },
        ),
    ]
    .into();
    let buffered = batches[..BUFFERED_BATCHES]
        .iter()
        .map(get_reserved_bytes_for_record_batch)
        .collect::<Result<Vec<_>>>()?
        .into_iter()
        .sum::<usize>();
    let capacity = MERGE_BYTES
        + buffered
        + get_reserved_bytes_for_record_batch(&batches[BUFFERED_BATCHES])? / 2;
    Ok(Fixture {
        parent,
        batches,
        ordering,
        capacity,
    })
}

fn sorter(
    fixture: &Fixture,
    pool: Arc<AdjustablePool>,
) -> Result<(ExternalSorter, Arc<RuntimeEnv>)> {
    let runtime = RuntimeEnvBuilder::new()
        .with_memory_pool(pool)
        .build_arc()?;
    let sorter = ExternalSorter::new(
        0,
        fixture.parent.schema(),
        fixture.ordering.clone(),
        128,
        MERGE_BYTES,
        0, // Force per-batch sorting and merging, rather than concatenation.
        SpillCompression::Uncompressed,
        &ExecutionPlanMetricsSet::new(),
        Arc::clone(&runtime),
    )?;
    Ok((sorter, runtime))
}

async fn assert_released(pool: &AdjustablePool, runtime: &RuntimeEnv) {
    // Buffered readers may finish dropping their state after the owning stream.
    tokio::time::timeout(Duration::from_secs(5), async {
        while pool.reserved() != 0
            || runtime.disk_manager.spilling_progress().active_files_count != 0
            || runtime.disk_manager.used_disk_space() != 0
        {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("sort reservations and spill files must be released");
    assert_eq!(runtime.disk_manager.used_disk_space(), 0);
    let state = pool.state.lock().unwrap();
    assert!(state.peak <= pool.capacity);
    assert_eq!(state.unchecked_over_limit, 0);
}

#[tokio::test]
async fn test_spill_preserves_merge_workspace_after_limit_decreases() -> Result<()> {
    let fixture = fixture()?;
    let pool = AdjustablePool::new(fixture.capacity);
    let (mut sorter, runtime) = sorter(&fixture, Arc::clone(&pool))?;
    for batch in &fixture.batches[..BUFFERED_BATCHES] {
        sorter.insert_batch(batch.clone()).await?;
    }
    assert!(!sorter.spilled_before());
    let reduced_limit = fixture.capacity * 3 / 4;
    assert!(pool.reserved() > reduced_limit);
    pool.set_limit(reduced_limit);
    // Unlike a hook on free(), this pressure transition still occurs when the
    // implementation preserves its merge reservation instead of releasing it.
    for batch in &fixture.batches[BUFFERED_BATCHES..] {
        sorter.insert_batch(batch.clone()).await?;
        assert!(
            runtime.disk_manager.spilling_progress().active_files_count
                <= fixture.batches.len()
        );
    }
    assert!(pool.state.lock().unwrap().denied > 0);
    assert!(sorter.spilled_before());
    let stream = sorter.sort().await?;
    drop(sorter); // The output stream must retain any transferred workspace.
    let batches: Vec<RecordBatch> = stream.try_collect().await?;
    assert!(batches.len() > 1);
    let actual = concat_batches(&fixture.parent.schema(), &batches)?;
    let expected = sort_batch(&fixture.parent, &fixture.ordering, None)?;
    assert_eq!(actual, expected);
    assert_released(&pool, &runtime).await;
    Ok(())
}

#[tokio::test]
async fn test_spill_workspace_does_not_hide_insufficient_memory() -> Result<()> {
    let fixture = fixture()?;
    let capacity =
        MERGE_BYTES + get_reserved_bytes_for_record_batch(&fixture.batches[0])? - 1;
    let pool = AdjustablePool::new(capacity);
    let (mut sorter, runtime) = sorter(&fixture, Arc::clone(&pool))?;
    let error = sorter
        .insert_batch(fixture.batches[0].clone())
        .await
        .unwrap_err();
    assert!(matches!(
        error.find_root(),
        DataFusionError::ResourcesExhausted(_)
    ));
    drop(sorter);
    assert_released(&pool, &runtime).await;
    Ok(())
}

#[tokio::test]
async fn test_spill_workspace_cleanup_after_drop_or_error() -> Result<()> {
    for inject_error in [false, true] {
        let fixture = fixture()?;
        let pool = AdjustablePool::new(fixture.capacity);
        let (mut sorter, runtime) = sorter(&fixture, Arc::clone(&pool))?;
        for batch in &fixture.batches[..BUFFERED_BATCHES + 2] {
            sorter.insert_batch(batch.clone()).await?;
        }
        assert!(runtime.disk_manager.spilling_progress().active_files_count > 0);
        if inject_error {
            pool.set_limit(0);
            let error = sorter
                .insert_batch(fixture.batches[BUFFERED_BATCHES + 2].clone())
                .await
                .unwrap_err();
            assert!(matches!(
                error.find_root(),
                DataFusionError::ResourcesExhausted(_)
            ));
        }
        drop(sorter);
        assert_released(&pool, &runtime).await;
    }
    Ok(())
}

#[tokio::test]
async fn test_spill_workspace_cleanup_after_output_is_dropped() -> Result<()> {
    let fixture = fixture()?;
    let pool = AdjustablePool::new(fixture.capacity);
    let (mut sorter, runtime) = sorter(&fixture, Arc::clone(&pool))?;
    for batch in &fixture.batches[..BUFFERED_BATCHES + 2] {
        sorter.insert_batch(batch.clone()).await?;
    }
    assert!(runtime.disk_manager.spilling_progress().active_files_count > 0);
    let mut stream = sorter.sort().await?;
    drop(sorter);
    assert!(stream.try_next().await?.is_some());
    drop(stream);
    assert_released(&pool, &runtime).await;
    Ok(())
}

fn aliased_batches() -> Result<(Vec<RecordBatch>, LexOrdering)> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("a", DataType::Int64, false),
        Field::new("alias_of_a", DataType::Int64, false),
    ]));
    let batches = [0, 128]
        .into_iter()
        .map(|start| {
            let values: ArrayRef =
                Arc::new(Int64Array::from_iter_values((start..start + 128).rev()));
            RecordBatch::try_new(Arc::clone(&schema), vec![Arc::clone(&values), values])
                .map_err(Into::into)
        })
        .collect::<Result<Vec<_>>>()?;
    let ordering = [PhysicalSortExpr::new_default(Arc::new(Column::new("a", 0)))].into();
    Ok((batches, ordering))
}

#[tokio::test]
async fn test_concat_sort_releases_unused_merge_workspace() -> Result<()> {
    let (batches, ordering) = aliased_batches()?;
    let schema = batches[0].schema();
    let input_bytes = batches
        .iter()
        .map(get_reserved_bytes_for_record_batch)
        .collect::<Result<Vec<_>>>()?
        .into_iter()
        .sum::<usize>();
    let concatenated = concat_batches(&schema, &batches)?;
    let concat_bytes = get_reserved_bytes_for_record_batch(&concatenated)?;
    let expected = sort_batch(&concatenated, &ordering, None)?;
    let headroom = 4096;
    let pool_capacity = input_bytes + headroom;
    // Concatenation breaks the aliases and needs more space than its inputs.
    assert!(concat_bytes > input_bytes);
    assert!(concat_bytes <= pool_capacity);

    let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(pool_capacity));
    let runtime = RuntimeEnvBuilder::new()
        .with_memory_pool(Arc::clone(&pool))
        .build_arc()?;
    let mut sorter = ExternalSorter::new(
        0,
        Arc::clone(&schema),
        ordering,
        64,
        headroom,
        usize::MAX, // Force concatenation instead of a streaming merge.
        SpillCompression::Uncompressed,
        &ExecutionPlanMetricsSet::new(),
        runtime,
    )?;
    for batch in batches {
        sorter.insert_batch(batch).await?;
    }
    assert_eq!(pool.reserved(), pool_capacity);
    assert!(!sorter.spilled_before());
    let stream = sorter.sort().await?;
    assert_eq!(pool.reserved(), concat_bytes);
    drop(sorter);
    let output: Vec<RecordBatch> = stream.try_collect().await?;
    assert_eq!(concat_batches(&schema, &output)?, expected);
    assert_eq!(pool.reserved(), 0);
    Ok(())
}

#[tokio::test]
async fn test_disabled_spilling_does_not_reserve_merge_workspace() -> Result<()> {
    let (batches, ordering) = aliased_batches()?;
    let schema = batches[0].schema();
    let expected = sort_batch(&concat_batches(&schema, &batches)?, &ordering, None)?;
    let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(16 * 1024));
    let runtime = RuntimeEnvBuilder::new()
        .with_memory_pool(Arc::clone(&pool))
        .with_disk_manager_builder(
            DiskManagerBuilder::default().with_mode(DiskManagerMode::Disabled),
        )
        .build_arc()?;
    let mut sorter = ExternalSorter::new(
        0,
        Arc::clone(&schema),
        ordering,
        64,
        10 * 1024 * 1024, // Must not be acquired when spilling is disabled.
        0,
        SpillCompression::Uncompressed,
        &ExecutionPlanMetricsSet::new(),
        runtime,
    )?;
    assert_eq!(pool.reserved(), 0);
    for batch in batches {
        sorter.insert_batch(batch).await?;
    }
    assert_eq!(sorter.merge_reservation_size(), 0);
    let stream = sorter.sort().await?;
    drop(sorter);
    let output: Vec<RecordBatch> = stream.try_collect().await?;
    assert_eq!(concat_batches(&schema, &output)?, expected);
    assert_eq!(pool.reserved(), 0);
    Ok(())
}

async fn check_chunked_string_view_workspace(during_spill: bool) -> Result<()> {
    let options = ExecutionOptions::default();
    let rows = 4096;
    let batch_size = 1024;
    let batch_count = if during_spill { 3 } else { 2 };
    let schema = Arc::new(Schema::new(vec![Field::new(
        "key",
        DataType::Utf8View,
        false,
    )]));
    let batches = (0..batch_count)
        .map(|batch_id| {
            let values: ArrayRef =
                Arc::new(StringViewArray::from_iter_values((0..rows).rev().map(
                    |i| format!("row-{:08}-{}", batch_id * rows + i, "x".repeat(87)),
                )));
            RecordBatch::try_new(Arc::clone(&schema), vec![values])
        })
        .collect::<std::result::Result<Vec<_>, _>>()?;
    let ordering: LexOrdering = [PhysicalSortExpr::new_default(Arc::new(Column::new(
        "key", 0,
    )))]
    .into();
    let input_bytes = batches[..2]
        .iter()
        .map(get_reserved_bytes_for_record_batch)
        .collect::<Result<Vec<_>>>()?
        .into_iter()
        .sum::<usize>();
    assert!(input_bytes > options.sort_in_place_threshold_bytes);

    // Every chunk retains the long-string buffers. The ordinary sort
    // reservation must grow even though all input was already reserved.
    let sorted_bytes = sort_batch_chunked(&batches[0], &ordering, batch_size)?
        .iter()
        .map(get_record_batch_memory_size)
        .sum::<usize>();
    assert!(sorted_bytes > get_reserved_bytes_for_record_batch(&batches[0])?);

    // Leave no unreserved capacity: the sort must be able to use its workspace.
    let capacity = options.sort_spill_reservation_bytes + input_bytes;
    let pool = AdjustablePool::new(capacity);
    let runtime = RuntimeEnvBuilder::new()
        .with_memory_pool(Arc::clone(&pool) as Arc<dyn MemoryPool>)
        .build_arc()?;
    let mut sorter = ExternalSorter::new(
        0,
        Arc::clone(&schema),
        ordering.clone(),
        batch_size,
        options.sort_spill_reservation_bytes,
        options.sort_in_place_threshold_bytes,
        options.spill_compression,
        &ExecutionPlanMetricsSet::new(),
        Arc::clone(&runtime),
    )?;
    for batch in &batches[..2] {
        sorter.insert_batch(batch.clone()).await?;
    }
    assert_eq!(pool.reserved(), capacity);
    assert!(!sorter.spilled_before());
    if during_spill {
        sorter.insert_batch(batches[2].clone()).await?;
        assert!(pool.state.lock().unwrap().denied > 0);
        assert!(sorter.spilled_before());
    }

    let stream = sorter.sort().await?;
    drop(sorter);
    let output: Vec<RecordBatch> = stream.try_collect().await?;
    let actual = concat_batches(&schema, &output)?;
    let expected = sort_batch(&concat_batches(&schema, &batches)?, &ordering, None)?;
    assert_eq!(actual, expected);
    assert_eq!(actual.num_rows(), batch_count * rows);
    assert_released(&pool, &runtime).await;
    Ok(())
}

#[tokio::test]
async fn test_chunked_string_view_final_sort_can_use_workspace() -> Result<()> {
    check_chunked_string_view_workspace(false).await
}

#[tokio::test]
async fn test_chunked_string_view_spill_can_use_workspace() -> Result<()> {
    check_chunked_string_view_workspace(true).await
}

#[tokio::test]
async fn test_single_batch_spill_preserves_workspace_after_limit_decreases() -> Result<()>
{
    let rows = 4096;
    let batch_size = 1024;
    let schema = Arc::new(Schema::new(vec![Field::new(
        "key",
        DataType::Utf8View,
        false,
    )]));
    let batches = [rows, rows - batch_size]
        .into_iter()
        .enumerate()
        .map(|(batch_id, batch_rows)| {
            let values: ArrayRef = Arc::new(StringViewArray::from_iter_values(
                (0..batch_rows).rev().map(|i| {
                    format!("row-{:08}-{}", batch_id * rows + i, "x".repeat(87))
                }),
            ));
            RecordBatch::try_new(Arc::clone(&schema), vec![values])
        })
        .collect::<std::result::Result<Vec<_>, _>>()?;
    let ordering: LexOrdering = [PhysicalSortExpr::new_default(Arc::new(Column::new(
        "key", 0,
    )))]
    .into();
    let input_bytes = batches
        .iter()
        .map(get_reserved_bytes_for_record_batch)
        .collect::<Result<Vec<_>>>()?;
    let sorted_bytes = batches
        .iter()
        .map(|batch| {
            Ok(sort_batch_chunked(batch, &ordering, batch_size)?
                .iter()
                .map(get_record_batch_memory_size)
                .sum::<usize>())
        })
        .collect::<Result<Vec<_>>>()?;
    assert!(sorted_bytes[0] > input_bytes[0]);
    assert!(sorted_bytes[1] > input_bytes[1]);
    let workspace = sorted_bytes[0] - input_bytes[0];
    assert!(sorted_bytes[1] - input_bytes[1] <= workspace);
    let capacity = workspace + input_bytes[0];
    let reduced_limit = workspace + input_bytes[1];
    // Both spills need a workspace loan. The first sorted batch cannot be
    // charged to the execution pool if its workspace is released and regranted.
    assert!(reduced_limit < sorted_bytes[0]);

    let pool = AdjustablePool::new(capacity);
    let runtime = RuntimeEnvBuilder::new()
        .with_memory_pool(Arc::clone(&pool) as Arc<dyn MemoryPool>)
        .build_arc()?;
    let mut sorter = ExternalSorter::new(
        0,
        Arc::clone(&schema),
        ordering.clone(),
        batch_size,
        workspace,
        0,
        SpillCompression::Uncompressed,
        &ExecutionPlanMetricsSet::new(),
        Arc::clone(&runtime),
    )?;
    sorter.insert_batch(batches[0].clone()).await?;
    assert_eq!(pool.reserved(), capacity);
    assert_eq!(sorter.in_mem_batches.len(), 1);
    assert!(!sorter.spilled_before());

    pool.set_limit(reduced_limit);
    sorter.insert_batch(batches[1].clone()).await?;
    assert_eq!(sorter.finished_spill_files.len(), 1);
    assert_eq!(sorter.in_mem_batches.len(), 1);
    assert_eq!(pool.reserved(), reduced_limit);

    // Finalizing the sort must spill its one remaining input using the same
    // workspace, then merge both files within the reduced memory limit.
    let spill_count = sorter.metrics.spill_metrics.spill_file_count.clone();
    let stream = sorter.sort().await?;
    drop(sorter);
    let output: Vec<RecordBatch> = stream.try_collect().await?;
    assert_eq!(spill_count.value(), 2);
    assert!(output.iter().all(|batch| batch.num_rows() <= batch_size));
    let actual = concat_batches(&schema, &output)?;
    let expected = sort_batch(&concat_batches(&schema, &batches)?, &ordering, None)?;
    assert_eq!(actual, expected);
    assert_eq!(actual.num_rows(), 2 * rows - batch_size);
    assert_released(&pool, &runtime).await;
    Ok(())
}

#[tokio::test]
async fn test_chunked_dictionary_sort_can_use_workspace_with_defaults() -> Result<()> {
    let options = ExecutionOptions::default();
    let rows = 32_768;
    let schema = Arc::new(Schema::new(vec![
        Field::new("key", DataType::Int64, false),
        Field::new(
            "payload",
            DataType::Dictionary(Box::new(DataType::Int8), Box::new(DataType::Utf8)),
            false,
        ),
    ]));
    let values: ArrayRef = Arc::new(StringArray::from_iter_values(
        (0..64).map(|i| format!("{i:02}{}", "x".repeat(8190))),
    ));
    let mut batches = Vec::new();
    for start in [0, rows] {
        let key: ArrayRef = Arc::new(Int64Array::from_iter_values(
            (start..start + rows).rev().map(|i| i as i64),
        ));
        let payload: ArrayRef = Arc::new(DictionaryArray::<Int8Type>::try_new(
            Int8Array::from_iter_values((0..rows).map(|i| (i % 64) as i8)),
            Arc::clone(&values),
        )?);
        batches.push(RecordBatch::try_new(
            Arc::clone(&schema),
            vec![key, payload],
        )?);
    }
    let ordering: LexOrdering = [PhysicalSortExpr::new_default(Arc::new(Column::new(
        "key", 0,
    )))]
    .into();
    let input_bytes = batches
        .iter()
        .map(get_reserved_bytes_for_record_batch)
        .collect::<Result<Vec<_>>>()?;
    let buffered_bytes = input_bytes.iter().sum::<usize>();
    assert!(buffered_bytes > options.sort_in_place_threshold_bytes);

    // Dictionary values remain shared, but each output batch must account for
    // its backing buffers. Splitting the input therefore increases its charge.
    let sorted_bytes =
        sort_batch_chunked(&batches[0], &ordering, options.batch_size.get())?
            .iter()
            .map(get_record_batch_memory_size)
            .sum::<usize>();
    assert!(sorted_bytes > input_bytes[0]);

    let capacity = options.sort_spill_reservation_bytes + buffered_bytes;
    let pool = AdjustablePool::new(capacity);
    let runtime = RuntimeEnvBuilder::new()
        .with_memory_pool(Arc::clone(&pool) as Arc<dyn MemoryPool>)
        .build_arc()?;
    let mut sorter = ExternalSorter::new(
        0,
        Arc::clone(&schema),
        ordering.clone(),
        options.batch_size.get(),
        options.sort_spill_reservation_bytes,
        options.sort_in_place_threshold_bytes,
        options.spill_compression,
        &ExecutionPlanMetricsSet::new(),
        Arc::clone(&runtime),
    )?;
    for batch in &batches {
        sorter.insert_batch(batch.clone()).await?;
    }
    assert_eq!(pool.reserved(), capacity);
    assert!(!sorter.spilled_before());

    let stream = sorter.sort().await?;
    drop(sorter);
    let output: Vec<RecordBatch> = stream.try_collect().await?;
    let actual = concat_batches(&schema, &output)?;
    let expected = sort_batch(&concat_batches(&schema, &batches)?, &ordering, None)?;
    // RecordBatch equality compares dictionary values, so this checks payloads
    // as well as keys without depending on the dictionary's physical encoding.
    assert_eq!(actual, expected);
    assert_eq!(actual.num_rows(), rows * 2);
    assert_released(&pool, &runtime).await;
    Ok(())
}

#[tokio::test]
async fn test_overlapping_final_outputs_release_unused_workspace() -> Result<()> {
    let options = ExecutionOptions::default();
    let rows = 80_000;
    let capacity = 16 * 1024 * 1024;
    let schema = Arc::new(Schema::new(vec![Field::new("key", DataType::Int64, false)]));
    let batches = (0..2)
        .map(|batch_id| {
            let values: ArrayRef = Arc::new(Int64Array::from_iter_values(
                (batch_id * rows..(batch_id + 1) * rows)
                    .rev()
                    .map(|value| value as i64),
            ));
            RecordBatch::try_new(Arc::clone(&schema), vec![values])
        })
        .collect::<std::result::Result<Vec<_>, _>>()?;
    let input_bytes = batches
        .iter()
        .map(get_reserved_bytes_for_record_batch)
        .collect::<Result<Vec<_>>>()?
        .into_iter()
        .sum::<usize>();
    assert!(input_bytes > options.sort_in_place_threshold_bytes);
    assert!(options.sort_spill_reservation_bytes + input_bytes < capacity);
    assert!(capacity < 2 * options.sort_spill_reservation_bytes);
    let ordering: LexOrdering = [PhysicalSortExpr::new_default(Arc::new(Column::new(
        "key", 0,
    )))]
    .into();
    let expected = sort_batch(&concat_batches(&schema, &batches)?, &ordering, None)?;
    let pool = AdjustablePool::new(capacity);
    let runtime = RuntimeEnvBuilder::new()
        .with_memory_pool(Arc::clone(&pool) as Arc<dyn MemoryPool>)
        .build_arc()?;
    let make_sorter = |partition| {
        ExternalSorter::new(
            partition,
            Arc::clone(&schema),
            ordering.clone(),
            options.batch_size.get(),
            options.sort_spill_reservation_bytes,
            options.sort_in_place_threshold_bytes,
            options.spill_compression,
            &ExecutionPlanMetricsSet::new(),
            Arc::clone(&runtime),
        )
    };

    let mut first_sorter = make_sorter(0)?;
    for batch in &batches {
        first_sorter.insert_batch(batch.clone()).await?;
    }
    assert!(!first_sorter.spilled_before());
    let mut first_stream = first_sorter.sort().await?;
    drop(first_sorter);
    let first_batch = first_stream
        .try_next()
        .await?
        .expect("first sort must produce output");
    assert_eq!(first_batch.num_rows(), options.batch_size.get());

    // Keep the first merge alive and partially consumed. Its remaining data
    // fits comfortably, but an idle spill-workspace floor would block another
    // sorter from acquiring its own workspace and input reservations.
    assert!(
        pool.reserved() + options.sort_spill_reservation_bytes + input_bytes <= capacity,
        "final output must release unused spill workspace for the next sorter"
    );
    let mut second_sorter = make_sorter(1)?;
    for batch in &batches {
        second_sorter.insert_batch(batch.clone()).await?;
    }
    assert!(!second_sorter.spilled_before());
    let second_stream = second_sorter.sort().await?;
    drop(second_sorter);
    let second_output: Vec<RecordBatch> = second_stream.try_collect().await?;

    let mut first_output = vec![first_batch];
    first_output.extend(first_stream.try_collect::<Vec<RecordBatch>>().await?);
    for output in [first_output, second_output] {
        let actual = concat_batches(&schema, &output)?;
        assert_eq!(actual, expected);
        assert_eq!(actual.num_rows(), rows * 2);
    }
    assert_released(&pool, &runtime).await;
    Ok(())
}

async fn check_final_spilled_merge_releases_unused_workspace(
    intermediate: bool,
) -> Result<()> {
    let options = ExecutionOptions::default();
    let workspace = options.sort_spill_reservation_bytes;
    let rows = options.batch_size.get();
    let schema = Arc::new(Schema::new(vec![
        Field::new("key", DataType::Int64, false),
        Field::new("payload", DataType::Utf8, false),
    ]));
    let ordering: LexOrdering = [PhysicalSortExpr::new_default(Arc::new(Column::new(
        "key", 0,
    )))]
    .into();
    let make_batch = |rows: usize, start: usize, payload_bytes: usize| {
        let key: ArrayRef = Arc::new(Int64Array::from_iter_values(
            (start..start + rows).rev().map(|value| value as i64),
        ));
        let value = "x".repeat(payload_bytes);
        let payload: ArrayRef = Arc::new(StringArray::from_iter_values(
            (0..rows).map(|_| value.as_str()),
        ));
        RecordBatch::try_new(Arc::clone(&schema), vec![key, payload])
    };
    let batch_count = if intermediate { 16 } else { 8 };
    let big = make_batch(rows, 0, 200)?;
    let tail = make_batch(1, rows * batch_count, 200)?;
    let big_bytes = get_reserved_bytes_for_record_batch(&big)?;
    let tail_bytes = get_reserved_bytes_for_record_batch(&tail)?;
    let capacity = workspace + 4 * big_bytes + tail_bytes / 2;
    assert!(4 * big_bytes > options.sort_in_place_threshold_bytes);
    assert!(tail_bytes > 1);

    // Generate the initial spill files with a fixed capacity and normal pressure.
    let pool = AdjustablePool::new(capacity);
    let runtime = RuntimeEnvBuilder::new()
        .with_memory_pool(Arc::clone(&pool) as Arc<dyn MemoryPool>)
        .build_arc()?;
    let make_sorter = |partition| {
        ExternalSorter::new(
            partition,
            Arc::clone(&schema),
            ordering.clone(),
            options.batch_size.get(),
            options.sort_spill_reservation_bytes,
            options.sort_in_place_threshold_bytes,
            options.spill_compression,
            &ExecutionPlanMetricsSet::new(),
            Arc::clone(&runtime),
        )
    };

    let mut first_sorter = make_sorter(0)?;
    let mut original = Vec::new();
    for index in 0..batch_count {
        let batch = make_batch(rows, index * rows, 200)?;
        assert_eq!(get_reserved_bytes_for_record_batch(&batch)?, big_bytes);
        first_sorter.insert_batch(batch.clone()).await?;
        original.push(batch);
    }
    assert_eq!(first_sorter.finished_spill_files.len(), batch_count / 4 - 1);
    // Four batches fit, but this tiny insertion exceeds the remaining room
    // by half its size and triggers another spill through normal pressure.
    first_sorter.insert_batch(tail.clone()).await?;
    original.push(tail.clone());
    assert_eq!(first_sorter.finished_spill_files.len(), batch_count / 4);
    assert_eq!(first_sorter.in_mem_batches.len(), 1);

    let large_spill_bytes = first_sorter.finished_spill_files[0].max_record_batch_memory;
    assert!(
        first_sorter
            .finished_spill_files
            .iter()
            .all(|spill| spill.max_record_batch_memory == large_spill_bytes)
    );
    let tail_spill_bytes = sort_batch(&tail, &ordering, None)?.get_sliced_size()?;
    let single_buffer_bytes = if intermediate {
        4 * large_spill_bytes // Two intermediate files remain for the final merge.
    } else {
        4 * large_spill_bytes + 2 * tail_spill_bytes
    };
    let available_during_merge = workspace + 128 * 1024;
    assert!(tail_bytes < 128 * 1024);
    assert!(single_buffer_bytes < workspace);
    assert!(8 * large_spill_bytes > available_during_merge);
    assert!(capacity > available_during_merge);

    // Leave room for the final input spill, but not two buffers for the first
    // two files. With five files, two intermediate spill passes are also needed.
    let transient =
        MemoryConsumer::new("final merge contender").register(&runtime.memory_pool);
    transient.try_grow(capacity - available_during_merge)?;
    let spill_file_count = first_sorter.metrics.spill_metrics.spill_file_count.clone();
    let mut first_stream = first_sorter.sort().await?;
    let spills_before = spill_file_count.value();
    drop(first_sorter);
    if intermediate {
        // Deny all fresh parent grants: intermediate passes must reuse the
        // workspace acquired before the sort's share of memory decreased.
        pool.set_limit(transient.size());
    }
    let denied_before = pool.state.lock().unwrap().denied;
    let first_batch = first_stream
        .try_next()
        .await?
        .expect("the final spilled merge must produce output");
    assert_eq!(first_batch.num_rows(), rows);
    assert!(
        pool.state.lock().unwrap().denied > denied_before,
        "the initial two-buffer reservation must fail before retrying"
    );
    if intermediate {
        assert!(spill_file_count.value() >= spills_before + 2);
    } else {
        assert_eq!(spill_file_count.value(), spills_before);
    }
    assert_eq!(
        pool.reserved() - transient.size(),
        single_buffer_bytes,
        "the final disk merge must release its unused workspace"
    );
    pool.set_limit(capacity);
    drop(transient);

    // Leave the first final merge alive with most of its output still unread.
    // The next sort's input fits alongside the single-buffer reservation, but
    // not alongside the old workspace floor. Use a default-sized input again.
    let next_batch = make_batch(rows, rows * 16, 400)?;
    let next_bytes = get_reserved_bytes_for_record_batch(&next_batch)?;
    assert!(single_buffer_bytes + workspace + next_bytes <= capacity);
    assert!(2 * workspace + next_bytes > capacity);
    let mut second_sorter = make_sorter(1)?;
    second_sorter.insert_batch(next_batch.clone()).await?;
    let second_stream = second_sorter.sort().await?;
    drop(second_sorter);
    let second_output: Vec<RecordBatch> = second_stream.try_collect().await?;
    assert_eq!(
        concat_batches(&schema, &second_output)?,
        sort_batch(&next_batch, &ordering, None)?
    );

    let mut first_output = vec![first_batch];
    while let Some(batch) = first_stream.try_next().await? {
        first_output.push(batch);
    }
    assert_eq!(
        concat_batches(&schema, &first_output)?,
        sort_batch(&concat_batches(&schema, &original)?, &ordering, None)?
    );
    // EOF must release resources even while the exhausted outer stream lives.
    assert_released(&pool, &runtime).await;
    drop(first_stream);
    Ok(())
}

#[tokio::test]
async fn test_final_spilled_merge_releases_unused_workspace() -> Result<()> {
    check_final_spilled_merge_releases_unused_workspace(false).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_final_spilled_merge_releases_unused_workspace_multithreaded() -> Result<()>
{
    check_final_spilled_merge_releases_unused_workspace(false).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_final_spilled_merge_retains_workspace_between_passes() -> Result<()> {
    check_final_spilled_merge_releases_unused_workspace(true).await
}

#[tokio::test]
async fn test_single_batch_spill_returns_live_workspace_loan_on_drop() -> Result<()> {
    let options = ExecutionOptions::default();
    let rows = 4096;
    let batch_size = 1024;
    let schema = Arc::new(Schema::new(vec![Field::new(
        "key",
        DataType::Utf8View,
        false,
    )]));
    let values: ArrayRef = Arc::new(StringViewArray::from_iter_values(
        (0..rows)
            .rev()
            .map(|i| format!("row-{i:08}-{}", "x".repeat(87))),
    ));
    let batch = RecordBatch::try_new(Arc::clone(&schema), vec![values])?;
    let ordering: LexOrdering = [PhysicalSortExpr::new_default(Arc::new(Column::new(
        "key", 0,
    )))]
    .into();
    let input_bytes = get_reserved_bytes_for_record_batch(&batch)?;
    let expected = sort_batch_chunked(&batch, &ordering, batch_size)?;
    let sorted_bytes = expected
        .iter()
        .map(get_record_batch_memory_size)
        .sum::<usize>();
    let first_bytes = get_record_batch_memory_size(&expected[0]);
    assert!(sorted_bytes > input_bytes);
    let initial_loan = sorted_bytes - input_bytes;
    assert!(initial_loan <= options.sort_spill_reservation_bytes);
    assert!(
        initial_loan > first_bytes,
        "cancellation must leave a positive workspace loan"
    );
    let remaining_loan = initial_loan - first_bytes;

    let capacity = options.sort_spill_reservation_bytes + input_bytes;
    let pool = AdjustablePool::new(capacity);
    let runtime = RuntimeEnvBuilder::new()
        .with_memory_pool(Arc::clone(&pool) as Arc<dyn MemoryPool>)
        .build_arc()?;
    let mut sorter = ExternalSorter::new(
        0,
        Arc::clone(&schema),
        ordering,
        batch_size,
        options.sort_spill_reservation_bytes,
        options.sort_in_place_threshold_bytes,
        options.spill_compression,
        &ExecutionPlanMetricsSet::new(),
        Arc::clone(&runtime),
    )?;
    sorter.insert_batch(batch).await?;
    assert_eq!(pool.reserved(), capacity);

    // Follow spill preparation through the one-input branch, retaining the
    // workspace but assigning it no merge cursors.
    sorter.merge_reservation.free();
    let merge_pool = Arc::clone(&sorter.merge_pool);
    assert_eq!(sorter.in_mem_batches.len(), 1);
    let mut stream = sorter.in_mem_sort_stream(false, false)?;
    assert!(sorter.in_mem_batches.is_empty());
    drop(sorter);

    let first_batch = stream
        .try_next()
        .await?
        .expect("the sorted stream must produce a batch");
    assert_eq!(first_batch, expected[0]);
    assert_eq!(first_batch.num_rows(), batch_size);
    assert_eq!(pool.reserved(), capacity);

    // Check actual loan accounting rather than inferring a loan from the input
    // type. Only the workspace not held by the remaining output is available.
    let unused_workspace = merge_pool.borrow(usize::MAX);
    assert_eq!(
        unused_workspace.size(),
        options.sort_spill_reservation_bytes - remaining_loan
    );
    drop(unused_workspace);
    drop(merge_pool);
    assert_eq!(pool.reserved(), capacity);

    // The stream is the remaining owner of the positive loan. Drop it without
    // consuming the remaining batches and require both reservations to retire.
    drop(stream);
    assert_released(&pool, &runtime).await;
    Ok(())
}
