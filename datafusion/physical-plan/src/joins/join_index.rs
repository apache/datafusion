// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

//! Immutable join index, independent of payload storage.
//!
//! Lookup resolves exact keys; the join operator handles output batching and
//! residual predicates. For build keys `[A, B, A]`, the directory maps `A` to
//! group `[0, 2]` and `B` to row `1`. Lookup records only successful probe
//! positions and their row/group IDs; see [`MatchHandles`] for an example.
//!
//! `directory` owns layouts and construction, `keys` owns hashing and exact
//! comparison, and `matches` owns grouped row IDs and lookup-result memory.

mod directory;
mod keys;
mod matches;

use std::mem::size_of;
use std::ops::Deref;

use arrow::array::{ArrayRef, BooleanArray};
use datafusion_common::hash_utils::RandomState;
use datafusion_common::{NullEquality, Result};
use datafusion_execution::memory_pool::MemoryReservation;

use directory::IndexWidth;
pub(crate) use keys::LookupScratch;
pub(crate) use matches::{GroupRows, LookupResult, MatchHandle, MatchHandles, RowId};

/// Maps exact join keys (one or more columns) to matching build-row IDs.
/// Lookup returns compact [`MatchHandles`] for the join processor to consume.
/// See [`directory::JoinHashIndex`] for the storage design and a small example.
#[derive(Debug)]
pub(crate) struct JoinIndexMap {
    /// Index implementation specialized on row-id width.
    index: IndexWidth,
    random_state: RandomState,
    /// See [`NullEquality`] for semantics.
    ///
    /// For implementation:
    /// - `NullEqualsNothing`: skip NULL-key rows during build and probe.
    /// - `NullEqualsNull`: hash and compare NULL keys
    null_equality: NullEquality,
}

/// Keeps the index charged while processors or dynamic filters retain it.
pub(crate) struct ReservedJoinIndex {
    index: JoinIndexMap,
    // Drop the index before releasing its reservation.
    reservation: MemoryReservation,
}

impl ReservedJoinIndex {
    pub(crate) fn new(index: JoinIndexMap, reservation: MemoryReservation) -> Self {
        Self { index, reservation }
    }

    pub(crate) fn reserved_bytes(&self) -> usize {
        self.reservation.size()
    }
}

impl Deref for ReservedJoinIndex {
    type Target = JoinIndexMap;

    fn deref(&self) -> &Self::Target {
        &self.index
    }
}

impl JoinIndexMap {
    /// Build a join index.
    ///
    /// # Arguments
    ///
    /// - `keys`: evaluated build-side join keys.
    /// - `random_state`: hashing state shared by build and probe.
    /// - `null_equality`: NULL matching semantics for the join.
    /// - `dense_range`: `Some((min, max))` enables direct indexing; `None` keeps
    ///   hashing. See the Specializations section below.
    /// - `hash_chunk_size`: maximum rows hashed at once during construction.
    ///   Reusing the hash buffer across chunks reduces peak memory usage by
    ///   avoiding a buffer of hashes for the entire build input.
    /// - `reservation`: memory budget for index storage and build scratch.
    ///
    /// # Specializations
    ///
    /// The index uses the following specializations:
    /// - If `dense_range` is provided for a single integer key column, its
    ///   inclusive `(min, max)` bounds must cover all non-NULL build keys.
    ///   NULL build keys must not be matchable.
    ///   Use one array slot per value in that range, including gaps:
    ///   `Some((10, 12))` allocates three slots; key `12` maps to slot `2`.
    /// - Otherwise, if `keys` contains a single integer column, keep a hash table
    ///   but store each join key inline instead of its cached hash.
    pub(crate) fn try_new(
        keys: &[ArrayRef],
        random_state: &RandomState,
        null_equality: NullEquality,
        dense_range: Option<(u64, u64)>,
        hash_chunk_size: usize,
        reservation: &MemoryReservation,
    ) -> Result<Self> {
        Ok(Self {
            index: IndexWidth::build(
                keys,
                random_state,
                null_equality,
                dense_range,
                hash_chunk_size,
                reservation,
                false,
            )?,
            random_state: random_state.clone(),
            null_equality,
        })
    }

    /// Resolve a probe batch to compact, verified row/group handles.
    ///
    /// See comments on [`JoinIndexMap`] for the explanations for 'group handles' and
    /// more details.
    pub(crate) fn lookup_batch(
        &self,
        build_keys: &[ArrayRef],
        probe_keys: &[ArrayRef],
        scratch: &mut LookupScratch,
    ) -> Result<LookupResult> {
        self.index.lookup_batch(
            build_keys,
            probe_keys,
            self.null_equality,
            &self.random_state,
            scratch,
        )
    }

    /// Borrow a duplicate group's row IDs in their original build order.
    #[inline]
    pub(crate) fn group_rows(&self, group: u64) -> GroupRows<'_> {
        self.index.group_rows(group)
    }

    pub(crate) fn is_dense(&self) -> bool {
        self.index.is_dense()
    }

    pub(crate) fn num_of_distinct_key(&self) -> usize {
        self.index.num_of_distinct_key()
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.num_of_distinct_key() == 0
    }

    /// Hash-based counterpart to [`Self::contain_keys`]; collisions may produce
    /// false positives.
    pub(crate) fn contain_hashes(&self, hashes: &[u64]) -> BooleanArray {
        self.index.contain_hashes(hashes, &self.random_state)
    }

    /// Return a mask indicating which keys exist in the dense integer directory.
    ///
    /// Dynamic filters call this through [`HashTableLookupExpr`] to discard
    /// unmatched probe rows before the join.
    ///
    /// [`HashTableLookupExpr`]: crate::joins::hash_join::HashTableLookupExpr
    pub(crate) fn contain_keys(&self, keys: &[ArrayRef]) -> Result<BooleanArray> {
        self.index.contain_keys(keys)
    }
}

/// Grow reusable probe buffers geometrically, charging memory before allocating.
fn reserve<T>(
    buffer: &mut Vec<T>,
    needed: usize,
    reservation: &MemoryReservation,
) -> Result<()> {
    if needed > buffer.capacity() {
        let capacity = needed.max(buffer.capacity().saturating_mul(2)).max(8);
        reservation.try_grow((capacity - buffer.capacity()) * size_of::<T>())?;
        buffer.reserve_exact(capacity - buffer.len());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    use arrow::array::Int32Array;
    use arrow::compute::cast;
    use arrow::datatypes::DataType;
    use datafusion_execution::memory_pool::{
        MemoryConsumer, MemoryPool, UnboundedMemoryPool,
    };
    use rstest::rstest;

    /// Targets [`IntegerDirectory`](super::directory::integer::IntegerDirectory).
    #[rstest]
    #[case::nulls_unequal(NullEquality::NullEqualsNothing)]
    #[case::nulls_equal(NullEquality::NullEqualsNull)]
    fn lookup_integer_unique_keys(
        #[case] null_equality: NullEquality,
        // force u64 row width in JoinIndexMap for testing
        #[values(false, true)] force_u64: bool,
    ) -> Result<()> {
        // Setup
        let case = LookupTestCase {
            build_keys: &[Some(10), Some(12), None],
            probe_keys: &[Some(12), Some(11), Some(10), None, Some(12)],
            expected_nulls_unequal: vec![vec![1], vec![], vec![0], vec![], vec![1]],
            expected_nulls_equal: vec![vec![1], vec![], vec![0], vec![2], vec![1]],
        };
        let mut index = case.setup(DataType::Int32, None, null_equality, force_u64)?;

        // Execution
        let result = index.lookup(case.probe_keys)?;

        // Validation
        case.check(result, null_equality);

        Ok(())
    }

    /// Targets [`IntegerDirectory`](super::directory::integer::IntegerDirectory).
    #[rstest]
    #[case::nulls_unequal(NullEquality::NullEqualsNothing)]
    #[case::nulls_equal(NullEquality::NullEqualsNull)]
    fn lookup_integer_duplicate_keys(
        #[case] null_equality: NullEquality,
        #[values(false, true)] force_u64: bool,
    ) -> Result<()> {
        let case = LookupTestCase {
            build_keys: &[Some(10), Some(12), Some(10), None, None],
            probe_keys: &[Some(10), Some(12), Some(11), None, Some(10)],
            expected_nulls_unequal: vec![vec![0, 2], vec![1], vec![], vec![], vec![0, 2]],
            expected_nulls_equal: vec![
                vec![0, 2],
                vec![1],
                vec![],
                vec![3, 4],
                vec![0, 2],
            ],
        };
        let mut index = case.setup(DataType::Int32, None, null_equality, force_u64)?;

        let result = index.lookup(case.probe_keys)?;
        case.check(result, null_equality);
        Ok(())
    }

    /// Targets [`DenseIntegerDirectory`](super::directory::dense::DenseIntegerDirectory).
    #[rstest]
    #[case::nulls_unequal(NullEquality::NullEqualsNothing)]
    #[case::nulls_equal(NullEquality::NullEqualsNull)]
    fn lookup_dense_unique_keys(
        #[case] null_equality: NullEquality,
        #[values(false, true)] force_u64: bool,
    ) -> Result<()> {
        let case = LookupTestCase {
            build_keys: &[Some(10), Some(12)],
            probe_keys: &[Some(12), Some(11), Some(10), None, Some(9), Some(13)],
            expected_nulls_unequal: vec![
                vec![1],
                vec![],
                vec![0],
                vec![],
                vec![],
                vec![],
            ],
            expected_nulls_equal: vec![vec![1], vec![], vec![0], vec![], vec![], vec![]],
        };
        let mut index =
            case.setup(DataType::Int32, Some((10, 12)), null_equality, force_u64)?;

        let result = index.lookup(case.probe_keys)?;
        case.check(result, null_equality);
        Ok(())
    }

    /// Targets [`DenseIntegerDirectory`](super::directory::dense::DenseIntegerDirectory).
    #[rstest]
    #[case::nulls_unequal(NullEquality::NullEqualsNothing)]
    #[case::nulls_equal(NullEquality::NullEqualsNull)]
    fn lookup_dense_duplicate_keys(
        #[case] null_equality: NullEquality,
        #[values(false, true)] force_u64: bool,
    ) -> Result<()> {
        let case = LookupTestCase {
            build_keys: &[Some(10), Some(12), Some(10)],
            probe_keys: &[
                Some(10),
                Some(12),
                Some(11),
                None,
                Some(9),
                Some(13),
                Some(10),
            ],
            expected_nulls_unequal: vec![
                vec![0, 2],
                vec![1],
                vec![],
                vec![],
                vec![],
                vec![],
                vec![0, 2],
            ],
            expected_nulls_equal: vec![
                vec![0, 2],
                vec![1],
                vec![],
                vec![],
                vec![],
                vec![],
                vec![0, 2],
            ],
        };
        let mut index =
            case.setup(DataType::Int32, Some((10, 12)), null_equality, force_u64)?;

        let result = index.lookup(case.probe_keys)?;
        case.check(result, null_equality);
        Ok(())
    }

    /// Targets [`GeneralDirectory`](super::directory::general::GeneralDirectory).
    #[rstest]
    #[case::nulls_unequal(NullEquality::NullEqualsNothing)]
    #[case::nulls_equal(NullEquality::NullEqualsNull)]
    fn lookup_general_unique_keys(
        #[case] null_equality: NullEquality,
        #[values(false, true)] force_u64: bool,
    ) -> Result<()> {
        let case = LookupTestCase {
            build_keys: &[Some(10), Some(12), None],
            probe_keys: &[Some(12), Some(11), Some(10), None, Some(12)],
            expected_nulls_unequal: vec![vec![1], vec![], vec![0], vec![], vec![1]],
            expected_nulls_equal: vec![vec![1], vec![], vec![0], vec![2], vec![1]],
        };
        let mut index = case.setup(DataType::Utf8, None, null_equality, force_u64)?;

        let result = index.lookup(case.probe_keys)?;
        case.check(result, null_equality);
        let reserved = index.scratch.reservation.size();
        for _ in 0..2 {
            let result = index.lookup(case.probe_keys)?;
            case.check(result, null_equality);
            assert_eq!(index.scratch.reservation.size(), reserved);
        }
        Ok(())
    }

    /// Targets [`GeneralDirectory`](super::directory::general::GeneralDirectory).
    #[rstest]
    #[case::nulls_unequal(NullEquality::NullEqualsNothing)]
    #[case::nulls_equal(NullEquality::NullEqualsNull)]
    fn lookup_general_duplicate_keys(
        #[case] null_equality: NullEquality,
        #[values(false, true)] force_u64: bool,
    ) -> Result<()> {
        let case = LookupTestCase {
            build_keys: &[Some(10), Some(12), Some(10), None, None],
            probe_keys: &[Some(10), Some(12), Some(11), None, Some(10)],
            expected_nulls_unequal: vec![vec![0, 2], vec![1], vec![], vec![], vec![0, 2]],
            expected_nulls_equal: vec![
                vec![0, 2],
                vec![1],
                vec![],
                vec![3, 4],
                vec![0, 2],
            ],
        };
        let mut index = case.setup(DataType::Utf8, None, null_equality, force_u64)?;

        let result = index.lookup(case.probe_keys)?;
        case.check(result, null_equality);
        Ok(())
    }

    #[rstest]
    fn lookup_duplicate_build_without_group_hits(
        #[values(DataType::Int32, DataType::Utf8)] data_type: DataType,
        #[values(false, true)] force_u64: bool,
    ) -> Result<()> {
        let case = LookupTestCase {
            build_keys: &[Some(10), Some(12), Some(10)],
            probe_keys: &[Some(12), Some(11), Some(12)],
            expected_nulls_unequal: vec![vec![1], vec![], vec![1]],
            expected_nulls_equal: vec![vec![1], vec![], vec![1]],
        };
        let mut index =
            case.setup(data_type, None, NullEquality::NullEqualsNothing, force_u64)?;
        for (values, expected_indices) in [
            (case.probe_keys, &[0, 2][..]),
            (&[Some(11), None][..], &[][..]),
        ] {
            let probe = [cast(
                &Int32Array::from(values.to_vec()),
                index.build_keys[0].data_type(),
            )?];
            let result = index.index.lookup_batch(
                &index.build_keys,
                &probe,
                &mut index.scratch,
            )?;
            let (positions, has_groups) = match &result {
                LookupResult::U32(handles) => {
                    (handles.probe_indices().as_ref(), handles.has_groups())
                }
                LookupResult::U64(handles) => {
                    (handles.probe_indices().as_ref(), handles.has_groups())
                }
            };
            assert!(!has_groups);
            assert_eq!(positions, expected_indices);
        }
        let result = index.lookup(case.probe_keys)?;
        case.check(result, NullEquality::NullEqualsNothing);
        Ok(())
    }

    #[rstest]
    fn lookup_compact_flags_across_words(
        #[values(None, Some((10, 12)))] dense_range: Option<(u64, u64)>,
        #[values(false, true)] force_u64: bool,
    ) -> Result<()> {
        // The first group follows 63 singleton hits and a miss. Its flag crosses
        // a word boundary together with the next singleton and group.
        let mut probe_keys = vec![Some(12); 63];
        probe_keys.extend([None, Some(10), None, Some(12), Some(10)]);
        probe_keys.extend([None; 64]);
        let mut expected = vec![vec![1]; 63];
        expected.extend([vec![], vec![0, 2], vec![], vec![1], vec![0, 2]]);
        expected.extend(vec![vec![]; 64]);
        let case = LookupTestCase {
            build_keys: &[Some(10), Some(12), Some(10)],
            probe_keys: &probe_keys,
            expected_nulls_unequal: expected.clone(),
            expected_nulls_equal: expected,
        };
        let mut index = case.setup(
            DataType::Int32,
            dense_range,
            NullEquality::NullEqualsNothing,
            force_u64,
        )?;
        let result = index.lookup(case.probe_keys)?;
        case.check(result, NullEquality::NullEqualsNothing);
        Ok(())
    }

    /// Expectations contain matching build-row IDs for each probe row.
    ///
    /// # Example
    ///
    /// ```text
    /// build_keys: [10, 12, 10, NULL, NULL]
    /// probe_keys: [10, NULL, 11]
    /// expected_nulls_unequal: [[0, 2], [], []]
    /// expected_nulls_equal: [[0, 2], [3, 4], []]
    /// ```
    struct LookupTestCase<'a> {
        build_keys: &'a [Option<i32>],
        probe_keys: &'a [Option<i32>],
        expected_nulls_unequal: Vec<Vec<u64>>,
        expected_nulls_equal: Vec<Vec<u64>>,
    }

    impl LookupTestCase<'_> {
        fn setup(
            &self,
            data_type: DataType,
            dense_range: Option<(u64, u64)>,
            null_equality: NullEquality,
            force_u64: bool,
        ) -> Result<TestJoinIndex> {
            let build_keys = [cast(
                &Int32Array::from(self.build_keys.to_vec()),
                &data_type,
            )?];
            let pool: Arc<dyn MemoryPool> = Arc::new(UnboundedMemoryPool::default());
            let reservation = MemoryConsumer::new("join index test").register(&pool);
            let random_state = RandomState::default();
            let index = JoinIndexMap {
                index: IndexWidth::build(
                    &build_keys,
                    &random_state,
                    null_equality,
                    dense_range,
                    build_keys[0].len(),
                    &reservation,
                    force_u64,
                )?,
                random_state,
                null_equality,
            };
            assert_eq!(matches!(index.index, IndexWidth::U64(_)), force_u64);
            assert_eq!(index.is_dense(), dense_range.is_some());
            let scratch = LookupScratch::new(&reservation);
            Ok(TestJoinIndex {
                build_keys,
                index: ReservedJoinIndex::new(index, reservation),
                scratch,
            })
        }

        fn check(&self, result: Vec<Vec<u64>>, null_equality: NullEquality) {
            let expected = match null_equality {
                NullEquality::NullEqualsNothing => &self.expected_nulls_unequal,
                NullEquality::NullEqualsNull => &self.expected_nulls_equal,
            };
            assert_eq!(&result, expected);
        }
    }

    struct TestJoinIndex {
        build_keys: [ArrayRef; 1],
        index: ReservedJoinIndex,
        scratch: LookupScratch,
    }

    impl TestJoinIndex {
        fn lookup(&mut self, probe_keys: &[Option<i32>]) -> Result<Vec<Vec<u64>>> {
            let probe = [cast(
                &Int32Array::from(probe_keys.to_vec()),
                self.build_keys[0].data_type(),
            )?];
            let result =
                self.index
                    .lookup_batch(&self.build_keys, &probe, &mut self.scratch)?;
            Ok(match result {
                LookupResult::U32(handles) => {
                    lookup_rows(&self.index, &handles, probe_keys.len())
                }
                LookupResult::U64(handles) => {
                    lookup_rows(&self.index, &handles, probe_keys.len())
                }
            })
        }
    }

    fn lookup_rows<T: RowId>(
        index: &JoinIndexMap,
        handles: &MatchHandles<T>,
        num_probe_rows: usize,
    ) -> Vec<Vec<u64>> {
        assert_eq!(handles.probe_indices().len(), handles.len());
        assert!(
            handles
                .probe_indices()
                .windows(2)
                .all(|pair| pair[0] < pair[1])
        );
        let mut rows = vec![vec![]; num_probe_rows];
        for (position, &probe_row) in handles.probe_indices().iter().enumerate() {
            rows[probe_row as usize] = match handles.get(position) {
                MatchHandle::One(row) => vec![row.as_u64()],
                MatchHandle::Group(group) => match index.group_rows(group.as_u64()) {
                    GroupRows::U32(rows) => {
                        rows.iter().map(|&row| u64::from(row)).collect()
                    }
                    GroupRows::U64(rows) => rows.to_vec(),
                },
            };
        }
        rows
    }
}
