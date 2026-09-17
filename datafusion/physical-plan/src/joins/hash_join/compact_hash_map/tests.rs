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
use datafusion_common::{DataFusionError, JoinType};
use datafusion_execution::TaskContext;
use datafusion_execution::memory_pool::{GreedyMemoryPool, MemoryConsumer, MemoryPool};
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
    for distinct_keys in [rows, HASH_BUILD_CHUNK_ROWS, 2 * HASH_BUILD_CHUNK_ROWS] {
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
        let compact_limit = fixed_and_chain
            + HASH_BUILD_CHUNK_ROWS * size_of::<u64>()
            + estimate_memory_size::<(u64, u32)>(
                (distinct_keys + HASH_BUILD_CHUNK_ROWS).min(rows),
                size_of::<JoinHashMapU32>(),
            )?;
        let mut expected = None;
        for limit in [4 * 1024 * 1024, compact_limit] {
            let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(limit));
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
