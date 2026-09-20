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

use super::*;

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use arrow::array::{ArrayRef, DictionaryArray, Int32Array, StringArray, StructArray};
use arrow::compute::concat_batches;
use arrow::datatypes::{Field, Int32Type, Schema};
use datafusion_common::cast::as_int32_array;
use datafusion_common::utils::memory::RecordBatchMemoryCounter;
use datafusion_common::{DataFusionError, JoinType};
use datafusion_execution::TaskContext;
use datafusion_execution::memory_pool::{
    GreedyMemoryPool, MemoryConsumer, MemoryPool, PeakRecordingPool,
};
use datafusion_execution::runtime_env::RuntimeEnvBuilder;
use datafusion_expr::{Volatility, create_udf};
use datafusion_physical_expr::ScalarFunctionExpr;

use super::super::exec::HASH_JOIN_SEED;
use crate::ExecutionPlan;
use crate::common;
use crate::joins::PartitionMode;
use crate::joins::hash_join::HashJoinExec;
use crate::joins::join_hash_map::{JoinHashMapType, JoinHashMapU32};
use crate::joins::utils::update_hash;
use crate::test::TestMemoryExec;

#[tokio::test]
async fn compact_hash_build_with_duplicates_and_nulls() -> Result<()> {
    let rows = 65_536;
    let schema = Arc::new(Schema::new(vec![
        Field::new("key", DataType::Utf8, true),
        Field::new("value", DataType::Int32, false),
    ]));
    let build = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(StringArray::from_iter(
                (0..rows).map(|i| [Some("GA"), None, Some("CA")][i % 3]),
            )),
            Arc::new(Int32Array::from_iter_values(0..rows as i32)),
        ],
    )?;
    let probe = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(StringArray::from(vec![Some("GA"), None, Some("CA")])),
            Arc::new(Int32Array::from(vec![-1, -2, -3])),
        ],
    )?;
    let small_limit = 2 * 1024 * 1024;
    assert!(
        estimate_memory_size::<(u64, u32)>(rows, size_of::<JoinHashMapU32>())?
            > small_limit
    );
    for null_equality in [
        NullEquality::NullEqualsNothing,
        NullEquality::NullEqualsNull,
    ] {
        let mut outputs = vec![];
        for limit in [16 * 1024 * 1024, small_limit] {
            let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(limit));
            let runtime = RuntimeEnvBuilder::new()
                .with_memory_pool(Arc::clone(&pool))
                .build_arc()?;
            let context = Arc::new(TaskContext::default().with_runtime(runtime));
            let left = TestMemoryExec::try_new_exec(
                &[vec![
                    build.slice(0, 17_003),
                    build.slice(17_003, rows - 17_003 - 1),
                    build.slice(rows - 1, 1),
                ]],
                Arc::clone(&schema),
                None,
            )?;
            let right = TestMemoryExec::try_new_exec(
                &[vec![probe.clone()]],
                Arc::clone(&schema),
                None,
            )?;
            let join = HashJoinExec::try_new(
                left,
                right,
                vec![(
                    Arc::new(Column::new("key", 0)),
                    Arc::new(Column::new("key", 0)),
                )],
                None,
                &JoinType::Inner,
                None,
                PartitionMode::Partitioned,
                null_equality,
                false,
            )?;
            let batches = common::collect(join.execute(0, context)?).await?;
            outputs.push(concat_batches(&join.schema(), &batches)?);
            // Even with ample memory, duplicate keys must not allocate a
            // row-count-sized bucket array. Uneven batches exercise offsets.
            let peak = join
                .metrics()
                .unwrap()
                .sum_by_name("build_mem_used")
                .unwrap()
                .as_usize();
            assert!(peak < small_limit);
            drop(join);
            assert_eq!(pool.reserved(), 0);
        }
        assert_eq!(outputs[0], outputs[1]);
        let mut values = as_int32_array(outputs[1].column(1).as_ref())?
            .values()
            .to_vec();
        values.sort_unstable();
        assert_eq!(
            values,
            (0..rows as i32)
                .filter(|i| null_equality == NullEquality::NullEqualsNull || i % 3 != 1)
                .collect::<Vec<_>>()
        );
    }
    Ok(())
}

#[tokio::test]
async fn compact_hash_build_leaves_room_for_visited_bitmap() -> Result<()> {
    // Each budget admits a different initial bucket allocation, exercising
    // compaction when a sampled capacity also needs to release probe headroom.
    let mut errors = Vec::new();
    for (distinct_keys, table_rows) in [
        (64, HASH_BUILD_CHUNK_ROWS),
        (6144, 2 * HASH_BUILD_CHUNK_ROWS),
        (20_000, 4 * HASH_BUILD_CHUNK_ROWS),
    ] {
        let rows = 1_000_000;
        let schema =
            Arc::new(Schema::new(vec![Field::new("key", DataType::Utf8, false)]));
        let build = (0..rows)
            .step_by(HASH_BUILD_CHUNK_ROWS)
            .map(|start| {
                RecordBatch::try_new(
                    Arc::clone(&schema),
                    vec![Arc::new(StringArray::from_iter_values(
                        (start..(start + HASH_BUILD_CHUNK_ROWS).min(rows))
                            .map(|row| format!("key_{}", row % distinct_keys)),
                    ))],
                )
            })
            .collect::<std::result::Result<Vec<_>, _>>()?;
        let mut counter = RecordBatchMemoryCounter::new();
        let input_bytes = build
            .iter()
            .map(|batch| counter.count_batch(batch))
            .sum::<usize>();
        // The table allowance and hash scratch fit, but retaining the entire
        // allowance leaves too little room for the one-bit-per-row visited bitmap.
        let fixed_bytes = size_of::<JoinHashMapU32>();
        let limit = input_bytes
            + fixed_bytes
            + rows * size_of::<u32>()
            + HASH_BUILD_CHUNK_ROWS * size_of::<u64>()
            + estimate_memory_size::<(u64, u32)>(table_rows, fixed_bytes)?;
        let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(limit));
        let runtime = RuntimeEnvBuilder::new()
            .with_memory_pool(Arc::clone(&pool))
            .build_arc()?;
        let context = Arc::new(TaskContext::default().with_runtime(runtime));
        let left = TestMemoryExec::try_new_exec(&[build], Arc::clone(&schema), None)?;
        let probe = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(StringArray::from(vec!["absent"]))],
        )?;
        let right =
            TestMemoryExec::try_new_exec(&[vec![probe]], Arc::clone(&schema), None)?;
        let join = HashJoinExec::try_new(
            left,
            right,
            vec![(
                Arc::new(Column::new("key", 0)),
                Arc::new(Column::new("key", 0)),
            )],
            None,
            &JoinType::LeftAnti,
            None,
            PartitionMode::CollectLeft,
            NullEquality::NullEqualsNothing,
            false,
        )?;
        let result = common::collect(join.execute(0, context)?).await;
        drop(join);
        assert_eq!(pool.reserved(), 0);
        let output = match result {
            Ok(output) => output,
            Err(error) => {
                errors.push(format!(
                    "distinct_keys={distinct_keys}, limit={limit}: {error}"
                ));
                continue;
            }
        };
        assert_eq!(
            output.iter().map(RecordBatch::num_rows).sum::<usize>(),
            rows
        );
    }
    assert!(errors.is_empty(), "{}", errors.join("\n"));
    Ok(())
}

#[test]
fn compact_hash_build_evaluates_complex_keys_once() -> Result<()> {
    let rows = 2 * HASH_BUILD_CHUNK_ROWS + 17;
    let dictionary: ArrayRef = Arc::new(DictionaryArray::<Int32Type>::try_new(
        Int32Array::from_iter(
            (0..rows + 3).map(|i| (i % 7 != 0).then_some((i % 64) as i32)),
        ),
        Arc::new(StringArray::from_iter_values(
            (0..rows).map(|i| format!("key-{i}")),
        )),
    )?);
    let dictionary = dictionary.slice(3, rows);
    let nested: ArrayRef = Arc::new(StructArray::from(vec![(
        Arc::new(Field::new("value", dictionary.data_type().clone(), true)),
        Arc::clone(&dictionary),
    )]));
    for key in [dictionary, nested] {
        let batch = RecordBatch::try_from_iter([("key", Arc::clone(&key))])?;
        for computed in [false, true] {
            let evaluations = Arc::new(AtomicUsize::new(0));
            let counter = Arc::clone(&evaluations);
            let udf = create_udf(
                "identity",
                vec![key.data_type().clone()],
                key.data_type().clone(),
                Volatility::Immutable,
                Arc::new(move |args| {
                    counter.fetch_add(1, Ordering::Relaxed);
                    Ok(args[0].clone())
                }),
            );
            let expression: PhysicalExprRef = if computed {
                Arc::new(ScalarFunctionExpr::new(
                    "identity",
                    Arc::new(udf),
                    vec![Arc::new(Column::new("key", 0))],
                    Arc::new(Field::new("identity", key.data_type().clone(), true)),
                    Arc::default(),
                ))
            } else {
                Arc::new(Column::new("key", 0))
            };
            let on = vec![expression];
            let pool: Arc<dyn MemoryPool> =
                Arc::new(GreedyMemoryPool::new(16 * 1024 * 1024));
            let reservation = MemoryConsumer::new("complex key test").register(&pool);
            let (table, next) = build_compact_hash_map::<u32>(
                std::slice::from_ref(&batch),
                &on,
                rows,
                HASH_JOIN_SEED.random_state(),
                NullEquality::NullEqualsNothing,
                &reservation,
                &mut 0,
            )?;
            assert_eq!(evaluations.load(Ordering::Relaxed), usize::from(computed));

            // Compare with the existing whole-batch insertion path, including
            // rows excluded by dictionary NULLs and nested key hashing.
            let compact = JoinHashMapU32::new(table, next);
            let mut expected = JoinHashMapU32::with_capacity(rows);
            let mut hashes = vec![0; rows];
            update_hash(
                &on,
                &batch,
                &mut expected,
                0,
                HASH_JOIN_SEED.random_state(),
                &mut hashes,
                0,
                true,
                NullEquality::NullEqualsNothing,
            )?;
            // Sampling the probe hashes avoids quadratic output with forced
            // hash collisions while still traversing each sampled full chain.
            let probe_hashes = hashes
                .iter()
                .step_by(257)
                .take(8)
                .copied()
                .collect::<Vec<_>>();
            assert_eq!(
                compact
                    .get_matched_indices(Box::new(probe_hashes.iter().enumerate()), None),
                expected
                    .get_matched_indices(Box::new(probe_hashes.iter().enumerate()), None),
            );
            drop((compact, reservation));
            assert_eq!(pool.reserved(), 0);
        }
    }
    Ok(())
}

#[test]
fn compact_hash_build_preserves_fifo_across_batches() -> Result<()> {
    let rows = 3 * HASH_BUILD_CHUNK_ROWS + 17;
    let batch = RecordBatch::try_from_iter([(
        "key",
        Arc::new(Int32Array::from_iter(
            (0..rows).map(|i| (i % 7 != 0).then_some((i % 37) as i32)),
        )) as ArrayRef,
    )])?;
    let batches = vec![
        batch.slice(0, HASH_BUILD_CHUNK_ROWS + 5),
        batch.slice(HASH_BUILD_CHUNK_ROWS + 5, 2 * HASH_BUILD_CHUNK_ROWS + 11),
        batch.slice(rows - 1, 1),
    ];
    let on = vec![Arc::new(Column::new("key", 0)) as PhysicalExprRef];
    for null_equality in [
        NullEquality::NullEqualsNothing,
        NullEquality::NullEqualsNull,
    ] {
        let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(16 * 1024 * 1024));
        let reservation = MemoryConsumer::new("FIFO compact build").register(&pool);
        let (table, next) = build_compact_hash_map::<u32>(
            &batches,
            &on,
            rows,
            HASH_JOIN_SEED.random_state(),
            null_equality,
            &reservation,
            &mut 0,
        )?;
        let compact = JoinHashMapU32::new(table, next);
        let mut expected = JoinHashMapU32::with_capacity(rows);
        let mut offset = 0;
        for batch in batches.iter().rev() {
            let mut hashes = vec![0; batch.num_rows()];
            update_hash(
                &on,
                batch,
                &mut expected,
                offset,
                HASH_JOIN_SEED.random_state(),
                &mut hashes,
                0,
                true,
                null_equality,
            )?;
            offset += batch.num_rows();
        }
        let probe = batch.slice(0, 38);
        let mut probe_hashes = vec![0; probe.num_rows()];
        create_hashes(
            probe.columns(),
            HASH_JOIN_SEED.random_state(),
            &mut probe_hashes,
        )?;
        assert_eq!(
            compact.get_matched_indices(Box::new(probe_hashes.iter().enumerate()), None),
            expected.get_matched_indices(Box::new(probe_hashes.iter().enumerate()), None),
        );
        drop((compact, reservation));
        assert_eq!(pool.reserved(), 0);
    }
    Ok(())
}

#[test]
fn compact_hash_build_growth_and_chain_accounting() -> Result<()> {
    let rows = 8 * HASH_BUILD_CHUNK_ROWS;
    for (rows, distinct_keys) in [
        (rows, rows),
        (rows, HASH_BUILD_CHUNK_ROWS),
        (rows, 2 * HASH_BUILD_CHUNK_ROWS),
        (1_000_000, 100_000),
    ] {
        let batch = RecordBatch::try_from_iter([(
            "key",
            Arc::new(Int32Array::from_iter_values(
                (0..rows).map(|i| (i % distinct_keys) as i32),
            )) as ArrayRef,
        )])?;
        let on = vec![Arc::new(Column::new("key", 0)) as PhysicalExprRef];
        let fixed_and_chain = size_of::<JoinHashMapU32>() + rows * size_of::<u32>();
        // The replacement fits, but old and new tables cannot coexist.
        // Both budgets must produce identical index heads and duplicate chains.
        let mut compact_limit = fixed_and_chain
            + HASH_BUILD_CHUNK_ROWS * size_of::<u64>()
            + estimate_memory_size::<(u64, u32)>(
                (distinct_keys + HASH_BUILD_CHUNK_ROWS).min(rows),
                size_of::<JoinHashMapU32>(),
            )?;
        let deny_final_compaction = rows == 1_000_000;
        if deny_final_compaction {
            // Admit the row-count preallocation alongside the first small table,
            // but leave too little headroom for the final compact replacement.
            compact_limit = fixed_and_chain
                + HASH_BUILD_CHUNK_ROWS * size_of::<u64>()
                + estimate_memory_size::<(u64, u32)>(
                    HASH_BUILD_CHUNK_ROWS,
                    size_of::<JoinHashMapU32>(),
                )?
                + estimate_memory_size::<(u64, u32)>(rows, size_of::<JoinHashMapU32>())?;
        }
        let mut expected = None;
        for limit in [128 * 1024 * 1024, compact_limit] {
            let recording = Arc::new(PeakRecordingPool::new(Arc::new(
                GreedyMemoryPool::new(limit),
            )));
            let pool: Arc<dyn MemoryPool> = Arc::clone(&recording) as _;
            let reservation = MemoryConsumer::new("compact growth test").register(&pool);
            let mut peak = 0;
            let (table, next) = build_compact_hash_map::<u32>(
                std::slice::from_ref(&batch),
                &on,
                rows,
                HASH_JOIN_SEED.random_state(),
                NullEquality::NullEqualsNothing,
                &reservation,
                &mut peak,
            )?;
            assert_eq!(
                reservation.size(),
                table.allocation_size() + fixed_and_chain
            );
            assert!(peak > reservation.size() && peak <= limit);
            assert_eq!(peak, recording.peak_reserved());
            // With forced hash collisions, no row-count preallocation occurs.
            if deny_final_compaction && table.len() > HASH_BUILD_CHUNK_ROWS {
                assert_eq!(table.capacity() / 4 > table.len(), limit == compact_limit);
            }
            let mut entries = table.iter().copied().collect::<Vec<_>>();
            entries.sort_unstable();
            if let Some(expected) = &expected {
                assert_eq!(&(entries, next), expected);
            } else {
                expected = Some((entries, next));
            }
            drop((table, reservation));
            assert_eq!(pool.reserved(), 0);
        }
    }
    Ok(())
}

#[test]
fn compact_hash_build_compacts_speculative_capacity() -> Result<()> {
    fn check<T>() -> Result<()>
    where
        T: Copy + Default + TryFrom<usize> + PartialOrd + Into<u64>,
        <T as TryFrom<usize>>::Error: fmt::Debug,
    {
        let rows = 1_000_000;
        let distinct_keys = 10_000;
        let batch = RecordBatch::try_from_iter([(
            "key",
            Arc::new(Int32Array::from_iter_values(
                (0..rows).map(|i| (i % distinct_keys) as i32),
            )) as ArrayRef,
        )])?;
        let on = vec![Arc::new(Column::new("key", 0)) as PhysicalExprRef];
        let pool: Arc<dyn MemoryPool> =
            Arc::new(GreedyMemoryPool::new(128 * 1024 * 1024));
        let reservation =
            MemoryConsumer::new("compact speculative capacity").register(&pool);
        let mut peak = 0;
        let (table, next) = build_compact_hash_map::<T>(
            std::slice::from_ref(&batch),
            &on,
            rows,
            HASH_JOIN_SEED.random_state(),
            NullEquality::NullEqualsNothing,
            &reservation,
            &mut peak,
        )?;
        let fixed_bytes = size_of::<HashTable<(u64, T)>>() + size_of::<Vec<T>>();
        // Crossing the first chunk's capacity can speculate on all build rows.
        // The retained table should instead reflect the distinct build hashes.
        assert!(
            table.allocation_size()
                <= estimate_memory_size::<(u64, T)>(4 * distinct_keys, fixed_bytes)?
        );
        assert_eq!(
            reservation.size(),
            fixed_bytes + rows * size_of::<T>() + table.allocation_size()
        );
        assert_eq!(pool.reserved(), reservation.size());
        assert!(peak > reservation.size());

        let mut hashes = vec![0; distinct_keys];
        create_hashes(
            batch.slice(0, distinct_keys).columns(),
            HASH_JOIN_SEED.random_state(),
            &mut hashes,
        )?;
        // Traverse each chain once, including when hashes deliberately collide.
        // This checks that compaction preserves every row and its FIFO order.
        let mut seen = vec![false; rows];
        for &(hash, head) in table.iter() {
            let mut row = head.into() as usize;
            while row != 0 {
                let index = row - 1;
                assert!(!seen[index]);
                seen[index] = true;
                assert_eq!(hash, hashes[index % distinct_keys]);
                let next_row = next[index].into() as usize;
                assert!(next_row == 0 || next_row > row);
                row = next_row;
            }
        }
        assert!(seen.into_iter().all(|visited| visited));
        drop((table, reservation));
        assert_eq!(pool.reserved(), 0);
        Ok(())
    }

    check::<u32>()?;
    check::<u64>()
}

#[test]
fn compact_hash_build_releases_failed_admissions() -> Result<()> {
    let rows = HASH_BUILD_CHUNK_ROWS;
    let batch = RecordBatch::try_from_iter([(
        "key",
        Arc::new(Int32Array::from_iter_values(0..rows as i32)) as ArrayRef,
    )])?;
    let on = vec![Arc::new(Column::new("key", 0)) as PhysicalExprRef];
    let fixed_and_chain = size_of::<JoinHashMapU32>() + rows * size_of::<u32>();
    // Reject the chain, then scratch, then the first bucket allocation.
    for limit in [
        fixed_and_chain - 1,
        fixed_and_chain,
        fixed_and_chain + rows * size_of::<u64>(),
    ] {
        let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(limit));
        let reservation = MemoryConsumer::new("failed compact build").register(&pool);
        let result = build_compact_hash_map::<u32>(
            std::slice::from_ref(&batch),
            &on,
            rows,
            HASH_JOIN_SEED.random_state(),
            NullEquality::NullEqualsNothing,
            &reservation,
            &mut 0,
        );
        assert!(matches!(
            result,
            Err(DataFusionError::ResourcesExhausted(_))
        ));
        drop(reservation);
        assert_eq!(pool.reserved(), 0);
    }
    Ok(())
}

#[test]
fn compact_hash_build_index_widths_and_empty_input() -> Result<()> {
    let rows = 2 * HASH_BUILD_CHUNK_ROWS + 17;
    for count in [0, rows] {
        for all_null in [false, true] {
            let batch = RecordBatch::try_from_iter([(
                "key",
                Arc::new(Int32Array::from_iter(
                    (0..count)
                        .map(|i| (!all_null && i % 7 != 0).then_some((i % 17) as i32)),
                )) as ArrayRef,
            )])?;
            let on = vec![Arc::new(Column::new("key", 0)) as PhysicalExprRef];
            for null_equality in [
                NullEquality::NullEqualsNothing,
                NullEquality::NullEqualsNull,
            ] {
                let pool: Arc<dyn MemoryPool> =
                    Arc::new(GreedyMemoryPool::new(16 * 1024 * 1024));
                let narrow = MemoryConsumer::new("u32 compact build").register(&pool);
                let wide = MemoryConsumer::new("u64 compact build").register(&pool);
                let (table32, next32) = build_compact_hash_map::<u32>(
                    std::slice::from_ref(&batch),
                    &on,
                    count,
                    HASH_JOIN_SEED.random_state(),
                    null_equality,
                    &narrow,
                    &mut 0,
                )?;
                let (table64, next64) = build_compact_hash_map::<u64>(
                    std::slice::from_ref(&batch),
                    &on,
                    count,
                    HASH_JOIN_SEED.random_state(),
                    null_equality,
                    &wide,
                    &mut 0,
                )?;
                let mut entries32 = table32
                    .iter()
                    .map(|&(hash, row)| (hash, u64::from(row)))
                    .collect::<Vec<_>>();
                let mut entries64 = table64.iter().copied().collect::<Vec<_>>();
                entries32.sort_unstable();
                entries64.sort_unstable();
                assert_eq!(entries32, entries64);
                assert_eq!(
                    next32.into_iter().map(u64::from).collect::<Vec<_>>(),
                    next64
                );
                if count == 0
                    || (all_null && null_equality == NullEquality::NullEqualsNothing)
                {
                    assert!(table32.is_empty());
                }
                drop((table32, table64, narrow, wide));
                assert_eq!(pool.reserved(), 0);
            }
        }
    }
    Ok(())
}

fn sampled_string_batch(
    rows: usize,
    key: impl Fn(usize) -> Option<String>,
) -> Result<RecordBatch> {
    Ok(RecordBatch::try_from_iter([(
        "key",
        Arc::new(StringArray::from_iter((0..rows).map(key))) as ArrayRef,
    )])?)
}

// Validate every row once, including when all hashes deliberately collide.
// Returns distinct hashes, retained table allocation, and tracked build peak.
fn assert_sampled_build_preserves_rows(
    batches: &[RecordBatch],
    on: &[PhysicalExprRef],
    null_equality: NullEquality,
) -> Result<(usize, usize, usize)> {
    assert_sampled_build_preserves_rows_with_limit(
        batches,
        on,
        null_equality,
        128 * 1024 * 1024,
    )
}

fn assert_sampled_build_preserves_rows_with_limit(
    batches: &[RecordBatch],
    on: &[PhysicalExprRef],
    null_equality: NullEquality,
    limit: usize,
) -> Result<(usize, usize, usize)> {
    let rows = batches.iter().map(RecordBatch::num_rows).sum();
    let recording = Arc::new(PeakRecordingPool::new(Arc::new(GreedyMemoryPool::new(
        limit,
    ))));
    let pool: Arc<dyn MemoryPool> = Arc::clone(&recording) as _;
    let reservation = MemoryConsumer::new("sampled string build").register(&pool);
    let mut peak = 0;
    let (table, next) = build_compact_hash_map::<u32>(
        batches,
        on,
        rows,
        HASH_JOIN_SEED.random_state(),
        null_equality,
        &reservation,
        &mut peak,
    )?;
    assert_eq!(peak, recording.peak_reserved());
    assert_eq!(
        reservation.size(),
        size_of::<JoinHashMapU32>() + rows * size_of::<u32>() + table.allocation_size()
    );
    assert_eq!(pool.reserved(), reservation.size());
    let summary = (table.len(), table.allocation_size(), peak);

    // The builder indexes batches in reverse order. Identity expressions in the
    // computed-key test produce the same hashes as the underlying string column.
    let mut expected_hashes = vec![0; rows];
    let mut expected_valid = vec![false; rows];
    let mut input_order = vec![0; rows];
    let mut offset = 0;
    for batch in batches.iter().rev() {
        let end = offset + batch.num_rows();
        create_hashes(
            batch.columns(),
            HASH_JOIN_SEED.random_state(),
            &mut expected_hashes[offset..end],
        )?;
        for row in 0..batch.num_rows() {
            expected_valid[offset + row] = null_equality == NullEquality::NullEqualsNull
                || !batch.column(0).is_null(row);
            input_order[offset + row] = rows - end + row;
        }
        offset = end;
    }
    let mut seen = vec![false; rows];
    for &(hash, head) in table.iter() {
        let mut row = head as usize;
        while row != 0 {
            let index = row - 1;
            assert!(!seen[index], "duplicate row index {index}");
            seen[index] = true;
            assert_eq!(hash, expected_hashes[index]);
            let next_row = next[index] as usize;
            assert!(next_row == 0 || input_order[next_row - 1] > input_order[index]);
            row = next_row;
        }
    }
    for (row, (seen, valid)) in seen.into_iter().zip(expected_valid).enumerate() {
        assert_eq!(seen, valid, "row index {row}");
    }
    drop((table, next, reservation));
    assert_eq!(pool.reserved(), 0);
    Ok(summary)
}

#[test]
fn sampled_hash_build_limits_uniform_key_memory() -> Result<()> {
    let rows = 1_000_000;
    let on = vec![Arc::new(Column::new("key", 0)) as PhysicalExprRef];
    for distinct in [20_000, 100_000] {
        for grouped in [false, true] {
            let batch = sampled_string_batch(rows, |row| {
                let key = if grouped {
                    row / (rows / distinct)
                } else {
                    row % distinct
                };
                Some(format!("key_{key}"))
            })?;
            let (_, retained, peak) = assert_sampled_build_preserves_rows(
                &[batch],
                &on,
                NullEquality::NullEqualsNothing,
            )?;
            let row_sized =
                estimate_memory_size::<(u64, u32)>(rows, size_of::<JoinHashMapU32>())?;
            // The entire construction should fit below the old bucket allocation
            // alone; final shrinking must not merely hide an oversized build peak.
            assert!(peak < row_sized, "distinct={distinct}, grouped={grouped}");
            assert!(retained < row_sized / 4);
            assert!(
                retained
                    <= estimate_memory_size::<(u64, u32)>(
                        4 * distinct,
                        size_of::<JoinHashMapU32>(),
                    )?
            );
        }
    }
    Ok(())
}

#[test]
fn sampled_hash_build_preserves_unique_prefix() -> Result<()> {
    let rows = 300_000;
    let on = vec![Arc::new(Column::new("key", 0)) as PhysicalExprRef];
    for reverse in [false, true] {
        let batch = sampled_string_batch(rows, |row| {
            let row = if reverse { rows - 1 - row } else { row };
            let key = if row < rows / 2 { row + 64 } else { row % 64 };
            Some(format!("key_{key}"))
        })?;
        let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(1024 * 1024));
        let reservation = MemoryConsumer::new("skewed sample").register(&pool);
        let (estimate, _) = sampled_capacity(
            std::slice::from_ref(&batch),
            &on,
            rows,
            HASH_JOIN_SEED.random_state(),
            &reservation,
        )
        .expect("large flat string keys can be sampled");
        // Strong skew keeps the initial allocation small, then actual distinct
        // hashes trigger row-count preallocation for the long unique tail.
        assert!(estimate <= HASH_BUILD_CHUNK_ROWS);
        assert_eq!(pool.reserved(), 0);
        let (distinct, _, peak) = assert_sampled_build_preserves_rows(
            &[batch],
            &on,
            NullEquality::NullEqualsNothing,
        )?;
        if distinct > 1 {
            assert!(
                peak >= estimate_memory_size::<(u64, u32)>(
                    rows,
                    size_of::<JoinHashMapU32>()
                )?
            );
        }
    }
    Ok(())
}

#[test]
fn sampled_hash_build_preserves_skewed_unique_tail() -> Result<()> {
    let rows = 1_000_000;
    let unique_rows = 25_000;
    let final_duplicate_rows = HASH_BUILD_CHUNK_ROWS;
    let on = vec![Arc::new(Column::new("key", 0)) as PhysicalExprRef];
    let batch = sampled_string_batch(rows, |row| {
        // The builder visits the unique region near the end. One final chunk
        // of repeated keys then exceeds the sampled table's spare capacity.
        let key = if (final_duplicate_rows..final_duplicate_rows + unique_rows)
            .contains(&row)
        {
            row - final_duplicate_rows + 1
        } else {
            0
        };
        Some(format!("key_{key}"))
    })?;
    let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(1024 * 1024));
    let reservation = MemoryConsumer::new("underestimated unique tail").register(&pool);
    let (estimate, _) = sampled_capacity(
        std::slice::from_ref(&batch),
        &on,
        rows,
        HASH_JOIN_SEED.random_state(),
        &reservation,
    )
    .expect("large flat string keys can be sampled");
    assert!(estimate > 0 && estimate < unique_rows, "hint={estimate}");
    assert_eq!(pool.reserved(), 0);
    let (distinct, _, peak) = assert_sampled_build_preserves_rows(
        std::slice::from_ref(&batch),
        &on,
        NullEquality::NullEqualsNothing,
    )?;
    // With forced hash collisions the actual index never outgrows its hint.
    // Otherwise the builder falls back to row-count preallocation before final
    // compaction, preserving all the rows checked by the helper above.
    if distinct > 1 {
        assert_eq!(distinct, unique_rows + 1);
        assert!(
            peak >= estimate_memory_size::<(u64, u32)>(
                rows,
                size_of::<JoinHashMapU32>(),
            )?
        );
    }
    // The final table fits, but cannot coexist with the initial table during
    // growth. Recovering from an underestimated hint must also work on replay.
    let fixed_bytes = size_of::<JoinHashMapU32>();
    let limit = fixed_bytes
        + rows * size_of::<u32>()
        + HASH_BUILD_CHUNK_ROWS * size_of::<u64>()
        + estimate_memory_size::<(u64, u32)>(
            unique_rows + HASH_BUILD_CHUNK_ROWS,
            fixed_bytes,
        )?;
    assert_sampled_build_preserves_rows_with_limit(
        &[batch],
        &on,
        NullEquality::NullEqualsNothing,
        limit,
    )?;
    Ok(())
}

#[test]
fn sampled_hash_build_preserves_broad_skew() -> Result<()> {
    let rows = 300_000;
    let on = vec![Arc::new(Column::new("key", 0)) as PhysicalExprRef];
    for reverse in [false, true] {
        let batch = sampled_string_batch(rows, |row| {
            let row = if reverse { rows - 1 - row } else { row };
            // Repeated values spread across a broad domain need not look like
            // heavy hitters. A mistaken estimate must preserve the unique half.
            let key = if row < rows / 2 {
                row + 8000
            } else {
                row % 8000
            };
            Some(format!("key_{key}"))
        })?;
        assert_sampled_build_preserves_rows(
            &[batch],
            &on,
            NullEquality::NullEqualsNothing,
        )?;
    }
    Ok(())
}

#[test]
fn sampled_hash_build_handles_nulls_and_empty_batches() -> Result<()> {
    let rows = 1_000_000;
    let on = vec![Arc::new(Column::new("key", 0)) as PhysicalExprRef];
    let batch = sampled_string_batch(rows, |row| {
        (row % 1000 == 0).then(|| format!("key_{}", row / 1000))
    })?;
    let split = rows / 3 + 1;
    let batches = [
        batch.slice(0, 0),
        batch.slice(0, split),
        batch.slice(split, 0),
        batch.slice(split, rows - split),
        batch.slice(rows, 0),
    ];
    let recording = Arc::new(PeakRecordingPool::new(Arc::new(GreedyMemoryPool::new(
        1024 * 1024,
    ))));
    let pool: Arc<dyn MemoryPool> = Arc::clone(&recording) as _;
    let reservation = MemoryConsumer::new("nullable sample").register(&pool);
    assert!(
        sampled_capacity(
            &batches,
            &on,
            rows,
            HASH_JOIN_SEED.random_state(),
            &reservation,
        )
        .is_none()
    );
    // Abstain before allocating sample scratch: sampling mostly NULL rows must
    // not turn a small valid-key domain into row-count preallocation.
    assert_eq!(recording.peak_reserved(), 0);
    assert_eq!(pool.reserved(), 0);
    let fixed_bytes = size_of::<JoinHashMapU32>();
    let initial_peak = fixed_bytes
        + rows * size_of::<u32>()
        + HASH_BUILD_CHUNK_ROWS * size_of::<u64>()
        + estimate_memory_size::<(u64, u32)>(HASH_BUILD_CHUNK_ROWS, fixed_bytes)?;
    for equality in [
        NullEquality::NullEqualsNothing,
        NullEquality::NullEqualsNull,
    ] {
        let (_, _, peak) = assert_sampled_build_preserves_rows_with_limit(
            &batches,
            &on,
            equality,
            initial_peak,
        )?;
        assert_eq!(peak, initial_peak);
    }
    Ok(())
}

#[test]
fn sampled_hash_build_handles_flat_byte_representations() -> Result<()> {
    use arrow::array::{
        BinaryArray, BinaryViewArray, LargeBinaryArray, LargeStringArray, StringViewArray,
    };

    let rows = 300_000;
    let batch = sampled_string_batch(rows, |row| Some(format!("key_{}", row % 20_000)))?;
    let source = batch
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let arrays: [ArrayRef; 6] = [
        Arc::clone(batch.column(0)),
        Arc::new(LargeStringArray::from_iter(source.iter())),
        Arc::new(StringViewArray::from_iter(source.iter())),
        Arc::new(BinaryArray::from_iter(
            source.iter().map(|value| value.map(str::as_bytes)),
        )),
        Arc::new(LargeBinaryArray::from_iter(
            source.iter().map(|value| value.map(str::as_bytes)),
        )),
        Arc::new(BinaryViewArray::from_iter(
            source.iter().map(|value| value.map(str::as_bytes)),
        )),
    ];
    let on = vec![Arc::new(Column::new("key", 0)) as PhysicalExprRef];
    for array in arrays {
        // A nullable schema with no actual NULLs remains eligible. Sampling must
        // honor array offsets and empty batches for each flat representation.
        let schema = Arc::new(Schema::new(vec![Field::new(
            "key",
            array.data_type().clone(),
            true,
        )]));
        let batch = RecordBatch::try_new(schema, vec![array.slice(3, rows - 7)])?;
        let rows = batch.num_rows();
        let split = rows / 3 + 1;
        let batches = [
            batch.slice(0, 0),
            batch.slice(0, split),
            batch.slice(split, 0),
            batch.slice(split, rows - split),
            batch.slice(rows, 0),
        ];
        let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(1024 * 1024));
        let reservation = MemoryConsumer::new("flat byte sample").register(&pool);
        assert!(
            sampled_capacity(
                &batches,
                &on,
                rows,
                HASH_JOIN_SEED.random_state(),
                &reservation,
            )
            .is_some()
        );
        assert_eq!(pool.reserved(), 0);
        assert_sampled_build_preserves_rows(
            &batches,
            &on,
            NullEquality::NullEqualsNothing,
        )?;
    }
    Ok(())
}

#[test]
fn sampled_capacity_excludes_dictionary_and_multiple_keys() -> Result<()> {
    let rows = 300_000;
    let values: ArrayRef = Arc::new(StringArray::from_iter_values(
        (0..64).map(|key| format!("key_{key}")),
    ));
    let dictionary: ArrayRef = Arc::new(DictionaryArray::<Int32Type>::try_new(
        Int32Array::from_iter_values((0..rows).map(|row| (row % 64) as i32)),
        values,
    )?);
    let dictionary_batch = RecordBatch::try_from_iter([("key", dictionary)])?;
    let strings = sampled_string_batch(rows, |row| Some(format!("key_{}", row % 64)))?;
    let multi_key_batch = RecordBatch::try_from_iter([
        ("key", Arc::clone(strings.column(0))),
        ("other", Arc::clone(strings.column(0))),
    ])?;
    let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(1024 * 1024));
    let reservation = MemoryConsumer::new("unsupported sample keys").register(&pool);
    let column = Arc::new(Column::new("key", 0)) as PhysicalExprRef;
    for (batch, on) in [
        (dictionary_batch, vec![Arc::clone(&column)]),
        (
            multi_key_batch,
            vec![column, Arc::new(Column::new("other", 1)) as PhysicalExprRef],
        ),
    ] {
        assert!(
            sampled_capacity(
                &[batch],
                &on,
                rows,
                HASH_JOIN_SEED.random_state(),
                &reservation,
            )
            .is_none()
        );
        assert_eq!(pool.reserved(), 0);
    }
    Ok(())
}

#[test]
fn sampled_hash_build_does_not_evaluate_computed_keys_again() -> Result<()> {
    let rows = 300_000;
    let batch = sampled_string_batch(rows, |row| Some(format!("key_{}", row % 20_000)))?;
    let evaluations = Arc::new(AtomicUsize::new(0));
    let counter = Arc::clone(&evaluations);
    let udf = create_udf(
        "identity",
        vec![DataType::Utf8],
        DataType::Utf8,
        Volatility::Immutable,
        Arc::new(move |args| {
            counter.fetch_add(1, Ordering::Relaxed);
            Ok(args[0].clone())
        }),
    );
    let expression: PhysicalExprRef = Arc::new(ScalarFunctionExpr::new(
        "identity",
        Arc::new(udf),
        vec![Arc::new(Column::new("key", 0))],
        Arc::new(Field::new("identity", DataType::Utf8, true)),
        Arc::default(),
    ));
    let on = vec![expression];
    let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(1024 * 1024));
    let reservation = MemoryConsumer::new("computed key sample").register(&pool);
    assert!(
        sampled_capacity(
            std::slice::from_ref(&batch),
            &on,
            rows,
            HASH_JOIN_SEED.random_state(),
            &reservation,
        )
        .is_none()
    );
    assert_eq!(evaluations.load(Ordering::Relaxed), 0);
    assert_eq!(pool.reserved(), 0);
    assert_sampled_build_preserves_rows(&[batch], &on, NullEquality::NullEqualsNothing)?;
    assert_eq!(evaluations.load(Ordering::Relaxed), 1);
    Ok(())
}

#[test]
fn sampled_capacity_releases_failed_admission() -> Result<()> {
    let rows = 300_000;
    let batch = sampled_string_batch(rows, |row| Some(format!("key_{}", row % 20_000)))?;
    let on = vec![Arc::new(Column::new("key", 0)) as PhysicalExprRef];
    let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(1024));
    let reservation = MemoryConsumer::new("exhausted sample pool").register(&pool);
    reservation.try_grow(1024)?;
    assert!(
        sampled_capacity(
            std::slice::from_ref(&batch),
            &on,
            rows,
            HASH_JOIN_SEED.random_state(),
            &reservation,
        )
        .is_none()
    );
    assert_eq!(reservation.size(), 1024);
    assert_eq!(pool.reserved(), 1024);
    drop(reservation);
    assert_eq!(pool.reserved(), 0);

    // Exercise cleanup when sampling is denied, and when sampling succeeds but
    // neither the hinted table nor its bounded fallback can be admitted.
    let scratch_bytes = HASH_BUILD_CHUNK_ROWS * size_of::<u64>();
    let fixed_bytes = size_of::<JoinHashMapU32>();
    let initial_peak = fixed_bytes
        + rows * size_of::<u32>()
        + scratch_bytes
        + estimate_memory_size::<(u64, u32)>(HASH_BUILD_CHUNK_ROWS, fixed_bytes)?;
    for limit in [initial_peak, initial_peak + scratch_bytes] {
        let recording = Arc::new(PeakRecordingPool::new(Arc::new(
            GreedyMemoryPool::new(limit),
        )));
        let pool: Arc<dyn MemoryPool> = Arc::clone(&recording) as _;
        let reservation = MemoryConsumer::new("failed sampled build").register(&pool);
        let mut peak = 0;
        let result = build_compact_hash_map::<u32>(
            std::slice::from_ref(&batch),
            &on,
            rows,
            HASH_JOIN_SEED.random_state(),
            NullEquality::NullEqualsNothing,
            &reservation,
            &mut peak,
        );
        match result {
            Ok((table, next)) => {
                // Forced collisions keep every row in one chain, so the
                // initial table never needs growth or sampling.
                assert_eq!(table.len(), 1);
                drop((table, next));
            }
            Err(error) => {
                assert!(matches!(error, DataFusionError::ResourcesExhausted(_)));
                assert_eq!(peak > initial_peak, limit > initial_peak);
            }
        }
        assert_eq!(peak, recording.peak_reserved());
        drop(reservation);
        assert_eq!(pool.reserved(), 0);
    }
    Ok(())
}

#[test]
fn sampled_hash_build_preserves_index_widths() -> Result<()> {
    type NormalizedHashIndex = (Vec<(u64, u64)>, Vec<u64>);

    fn check<T>(
        batch: &RecordBatch,
        on: &[PhysicalExprRef],
        hashes: &[u64],
        distinct_keys: usize,
    ) -> Result<NormalizedHashIndex>
    where
        T: Copy + Default + TryFrom<usize> + PartialOrd + Into<u64>,
        <T as TryFrom<usize>>::Error: fmt::Debug,
    {
        let rows = batch.num_rows();
        let recording = Arc::new(PeakRecordingPool::new(Arc::new(
            GreedyMemoryPool::new(128 * 1024 * 1024),
        )));
        let pool: Arc<dyn MemoryPool> = Arc::clone(&recording) as _;
        let reservation = MemoryConsumer::new("sampled index widths").register(&pool);
        let mut peak = 0;
        let (table, next) = build_compact_hash_map::<T>(
            std::slice::from_ref(batch),
            on,
            rows,
            HASH_JOIN_SEED.random_state(),
            NullEquality::NullEqualsNothing,
            &reservation,
            &mut peak,
        )?;
        let fixed_bytes = size_of::<HashTable<(u64, T)>>() + size_of::<Vec<T>>();
        assert_eq!(peak, recording.peak_reserved());
        assert_eq!(
            reservation.size(),
            fixed_bytes + rows * size_of::<T>() + table.allocation_size()
        );
        assert_eq!(pool.reserved(), reservation.size());
        if distinct_keys < rows {
            assert!(peak < estimate_memory_size::<(u64, T)>(rows, fixed_bytes)?);
        }

        let mut seen = vec![false; rows];
        for &(hash, head) in table.iter() {
            let mut row = head.into() as usize;
            while row != 0 {
                let index = row - 1;
                assert!(!seen[index]);
                seen[index] = true;
                assert_eq!(hash, hashes[index]);
                let next_row = next[index].into() as usize;
                assert!(next_row == 0 || next_row > row);
                row = next_row;
            }
        }
        assert!(seen.into_iter().all(|visited| visited));
        let mut entries = table
            .iter()
            .map(|&(hash, row)| (hash, row.into()))
            .collect::<Vec<_>>();
        entries.sort_unstable();
        let normalized_next = next.iter().map(|&row| row.into()).collect();
        drop((table, next, reservation));
        assert_eq!(pool.reserved(), 0);
        Ok((entries, normalized_next))
    }

    // Both fixtures are large enough for sampling. Repeated keys exercise a
    // partial capacity hint; unique keys exercise full preallocation.
    let rows = 300_000;
    let on = vec![Arc::new(Column::new("key", 0)) as PhysicalExprRef];
    for distinct_keys in [20_000, rows] {
        let batch = sampled_string_batch(rows, |row| {
            Some(format!("key_{}", row % distinct_keys))
        })?;
        let mut hashes = vec![0; rows];
        create_hashes(batch.columns(), HASH_JOIN_SEED.random_state(), &mut hashes)?;
        assert_eq!(
            check::<u32>(&batch, &on, &hashes, distinct_keys)?,
            check::<u64>(&batch, &on, &hashes, distinct_keys)?,
        );
    }
    Ok(())
}

#[test]
fn sampled_hash_build_outgrown_hint_preserves_retained_memory() -> Result<()> {
    let rows = 1_000_000;
    let hot_keys = 30_000;
    let unique_rows = 77_000;
    let final_duplicate_rows = HASH_BUILD_CHUNK_ROWS;
    let distinct_keys = hot_keys + unique_rows;
    let batch = sampled_string_batch(rows, |row| {
        // The builder visits this batch backwards: repeated keys arrive first,
        // then 77K unique keys, then one duplicate chunk. That last chunk trips
        // the conservative growth guard even though the distinct count is fixed.
        let key = if (final_duplicate_rows..final_duplicate_rows + unique_rows)
            .contains(&row)
        {
            hot_keys + row - final_duplicate_rows
        } else {
            row % hot_keys
        };
        Some(format!("key_{key}"))
    })?;
    let column = Arc::new(Column::new("key", 0)) as PhysicalExprRef;
    let sampled_on = vec![Arc::clone(&column)];
    // Repeating the same join key preserves its equivalence classes and chunk
    // size but disables sampling, exercising the original growth policy.
    let original_on = vec![Arc::clone(&column), column];
    let fixed_bytes = size_of::<JoinHashMapU32>();
    let full_table_estimate = estimate_memory_size::<(u64, u32)>(rows, fixed_bytes)?;
    let compact_table_estimate =
        estimate_memory_size::<(u64, u32)>(distinct_keys, fixed_bytes)?;
    // The original full allocation and its final compact replacement fit.
    // During sampled growth, keeping the larger partial table and hash scratch
    // live makes the full allocation fail by almost one scratch buffer.
    let limit = fixed_bytes
        + rows * size_of::<u32>()
        + full_table_estimate
        + compact_table_estimate;
    let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(limit));
    let reservation = MemoryConsumer::new("retained-memory sample hint").register(&pool);
    let (hint, _) = sampled_capacity(
        std::slice::from_ref(&batch),
        &sampled_on,
        rows,
        HASH_JOIN_SEED.random_state(),
        &reservation,
    )
    .expect("large flat string keys can be sampled");
    let hinted_entries = hint.saturating_add(HASH_BUILD_CHUNK_ROWS).min(rows);
    assert_eq!(
        estimate_memory_size::<(u64, u32)>(hinted_entries, fixed_bytes)?,
        compact_table_estimate,
        "fixture must initially select the table later outgrown by 107K keys: hint={hint}, entries={hinted_entries}",
    );
    assert!(
        sampled_capacity(
            std::slice::from_ref(&batch),
            &original_on,
            rows,
            HASH_JOIN_SEED.random_state(),
            &reservation,
        )
        .is_none()
    );
    drop(reservation);
    assert_eq!(pool.reserved(), 0);

    let build = |on: &[PhysicalExprRef]| -> Result<(usize, usize, usize, usize)> {
        let recording = Arc::new(PeakRecordingPool::new(Arc::new(
            GreedyMemoryPool::new(limit),
        )));
        let pool: Arc<dyn MemoryPool> = Arc::clone(&recording) as _;
        let reservation = MemoryConsumer::new("outgrown sample hint").register(&pool);
        let mut peak = 0;
        let (table, next) = build_compact_hash_map::<u32>(
            std::slice::from_ref(&batch),
            on,
            rows,
            HASH_JOIN_SEED.random_state(),
            NullEquality::NullEqualsNothing,
            &reservation,
            &mut peak,
        )?;
        assert_eq!(peak, recording.peak_reserved());
        assert_eq!(
            reservation.size(),
            fixed_bytes + rows * size_of::<u32>() + table.allocation_size()
        );
        let summary = (table.len(), table.capacity(), table.allocation_size(), peak);
        drop((table, next, reservation));
        assert_eq!(pool.reserved(), 0);
        Ok(summary)
    };
    let original = build(&original_on)?;
    let sampled = build(&sampled_on)?;
    assert_eq!(original.0, sampled.0);
    if sampled.0 > 1 {
        assert_eq!(sampled.0, distinct_keys);
        assert!(
            sampled.2 <= original.2,
            "a sampled hint must not leave more retained memory after denied full growth: original={original:?}, sampled={sampled:?}",
        );
    }
    Ok(())
}
