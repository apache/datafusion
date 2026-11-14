// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

//! Expand duplicate groups at output-batch granularity.
//!
//! Unique results fit in one bounded probe batch and need no cursor. Groups
//! remember two positions: the probe row and the next member of its group.
//! For `A -> [0, 2, 3, 4]` and batch size 2, a probe of `A` yields build rows
//! `[0, 2]`, then `[3, 4]`. The probe row is complete only after the second call.
//!
//! Completion means all key matches were enumerated. It does not say whether
//! any pair survived the residual ON predicate; that is the join's decision.

use arrow::array::{UInt32Array, UInt64Array};
use arrow::buffer::ScalarBuffer;
use arrow::util::bit_iterator::BitIndexIterator;
use datafusion_common::{Result, internal_err};
use datafusion_execution::memory_pool::MemoryReservation;

use crate::joins::join_index::{
    GroupRows, JoinIndexMap, MatchHandle, MatchHandleArray, MatchHandles, RowId,
};

/// At most one batch of key-equal pairs, before residual filtering.
pub(super) struct MatchChunk {
    pub(super) build_rows: UInt64Array,
    pub(super) probe_rows: UInt32Array,
    /// Probe rows whose first equality pair was enumerated in this chunk.
    /// Continuing a large group must not count that probe row again.
    pub(super) newly_matched_probe_rows: usize,
}

/// Continuation for one probe batch's grouped lookup results. Group IDs come
/// from `matches`; the offset advances within that group's build-row list.
/// The processor owns the reusable pair buffers across input batches.
pub(super) struct GroupCursor {
    matches: MatchHandles,
    position: GroupPosition,
}

#[derive(Default)]
struct GroupPosition {
    row_in_probe_batch: usize,
    offset_in_group: usize,
}

impl GroupCursor {
    pub(super) fn new(matches: MatchHandles) -> Self {
        Self {
            matches,
            position: GroupPosition::default(),
        }
    }

    pub(super) fn is_finished(&self) -> bool {
        self.position.row_in_probe_batch == self.matches.len()
    }

    /// Expand at most `batch_size` pairs. Misses advance without producing pairs;
    /// an empty chunk can therefore finish the input. Filtering never changes
    /// either enumeration coordinate.
    pub(super) fn next_batch(
        &mut self,
        index: &JoinIndexMap,
        batch_size: usize,
        buffers: &mut PairBuffer,
    ) -> Result<MatchChunk> {
        if batch_size == 0 {
            return internal_err!("Join pair batch size must be positive");
        }
        buffers.clear_for(batch_size)?;
        // Dispatch ID width once per chunk, outside the expansion loop.
        let newly_matched_probe_rows = match &self.matches {
            MatchHandles::U32(matches) => {
                self.position.expand(index, matches, batch_size, buffers)
            }
            MatchHandles::U64(matches) => {
                self.position.expand(index, matches, batch_size, buffers)
            }
        };
        let (build_rows, probe_rows) = buffers.finish();
        Ok(MatchChunk {
            build_rows,
            probe_rows,
            newly_matched_probe_rows,
        })
    }
}

impl GroupPosition {
    fn expand<T: RowId>(
        &mut self,
        index: &JoinIndexMap,
        matches: &MatchHandleArray<T>,
        batch_size: usize,
        output: &mut PairBuffer,
    ) -> usize {
        let build_rows = &mut output.build_rows;
        let probe_rows = &mut output.probe_rows;
        // A duplicate build can still produce a batch of singleton-only hits.
        if !matches.has_groups() && matches.validity().is_none() {
            let end = matches.len().min(self.row_in_probe_batch + batch_size);
            build_rows.extend(
                matches.ids()[self.row_in_probe_batch..end]
                    .iter()
                    .map(|row| row.as_u64()),
            );
            probe_rows.extend(self.row_in_probe_batch as u32..end as u32);
            let newly_matched = end - self.row_in_probe_batch;
            self.row_in_probe_batch = end;
            return newly_matched;
        }

        // Visit set validity bits to skip runs of misses, without decoding IDs.
        let start = self.row_in_probe_batch;
        let mut valid_rows = matches.validity().map(|valid| {
            BitIndexIterator::new(
                valid.inner().values(),
                valid.offset() + start,
                matches.len() - start,
            )
        });
        let mut all_rows = start..matches.len();
        let mut newly_matched = 0;
        loop {
            let next = match &mut valid_rows {
                Some(rows) => rows.next().map(|row| start + row),
                None => all_rows.next(),
            };
            let Some(row) = next else {
                self.row_in_probe_batch = matches.len();
                return newly_matched;
            };
            self.row_in_probe_batch = row;
            if build_rows.len() == batch_size {
                return newly_matched;
            }
            let handle = if matches.has_groups() {
                matches
                    .get(row)
                    .expect("selected probe row has a valid match")
            } else {
                MatchHandle::One(matches.ids()[row])
            };
            match handle {
                MatchHandle::One(build) => {
                    newly_matched += 1;
                    build_rows.push(build.as_u64());
                    probe_rows.push(row as u32);
                    self.row_in_probe_batch += 1;
                }
                MatchHandle::Group(group) => {
                    if self.offset_in_group == 0 {
                        newly_matched += 1;
                    }
                    let members = index.group_rows(group.as_u64());
                    let count = (members.len() - self.offset_in_group)
                        .min(batch_size - build_rows.len());
                    let end = self.offset_in_group + count;
                    match members {
                        GroupRows::U32(rows) => build_rows.extend(
                            rows[self.offset_in_group..end]
                                .iter()
                                .map(|&row| u64::from(row)),
                        ),
                        GroupRows::U64(rows) => {
                            build_rows.extend_from_slice(&rows[self.offset_in_group..end])
                        }
                    }
                    probe_rows.resize(probe_rows.len() + count, row as u32);
                    if end == members.len() {
                        self.row_in_probe_batch += 1;
                        self.offset_in_group = 0;
                    } else {
                        self.offset_in_group = end;
                        return newly_matched;
                    }
                }
            }
        }
    }
}

/// Own both reusable vectors and their Arrow views. Retaining the views lets
/// the next chunk reclaim their allocation after the caller materializes output.
/// The memory charge stays with this owner throughout that handoff.
pub(super) struct PairBuffer {
    build_rows: Vec<u64>,
    probe_rows: Vec<u32>,
    retained: Option<(ScalarBuffer<u64>, ScalarBuffer<u32>)>,
    reservation: MemoryReservation,
}

impl PairBuffer {
    pub(super) fn new(reservation: &MemoryReservation) -> Self {
        Self {
            build_rows: vec![],
            probe_rows: vec![],
            retained: None,
            reservation: reservation.new_empty(),
        }
    }

    fn clear_for(&mut self, batch_size: usize) -> Result<()> {
        if let Some((build, probe)) = self.retained.take() {
            self.build_rows = build.into();
            self.probe_rows = probe.into();
        }
        self.build_rows.clear();
        self.probe_rows.clear();
        let bytes = self.build_rows.capacity().max(batch_size) * size_of::<u64>()
            + self.probe_rows.capacity().max(batch_size) * size_of::<u32>();
        if bytes > self.reservation.size() {
            self.reservation.try_resize(bytes)?;
        }
        if self.build_rows.capacity() < batch_size {
            self.build_rows.reserve_exact(batch_size);
        }
        if self.probe_rows.capacity() < batch_size {
            self.probe_rows.reserve_exact(batch_size);
        }
        Ok(())
    }

    fn finish(&mut self) -> (UInt64Array, UInt32Array) {
        let build: UInt64Array = std::mem::take(&mut self.build_rows).into();
        let probe: UInt32Array = std::mem::take(&mut self.probe_rows).into();
        self.retained = Some((build.values().clone(), probe.values().clone()));
        (build, probe)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{ArrayRef, Int64Array};
    use datafusion_common::NullEquality;
    use datafusion_common::hash_utils::RandomState;
    use datafusion_execution::memory_pool::{
        MemoryConsumer, MemoryPool, UnboundedMemoryPool,
    };

    use super::*;
    use crate::joins::join_index::{LookupResult, LookupScratch};

    #[test]
    fn groups_resume_without_loss_or_early_probe_completion() -> Result<()> {
        // Interleaved groups of 7 and 4 members, plus a singleton.
        let build_values = [1, 2, 1, 3, 1, 2, 1, 1, 2, 1, 2, 1];
        let probe_batches = [
            vec![0, 1, 4, 2, 3, 1, 9],
            vec![3; 129],  // All-valid singletons and a partial bitmap word.
            vec![1, 2, 1], // All-valid groups.
            (0..131)
                .map(|row| {
                    if matches!(row, 63 | 64 | 127 | 130) {
                        1
                    } else {
                        9
                    }
                })
                .collect(),
            vec![],
        ];
        let build = [Arc::new(Int64Array::from(build_values.to_vec())) as ArrayRef];
        for probe_values in probe_batches {
            let probe = [Arc::new(Int64Array::from(probe_values.clone())) as ArrayRef];
            let expected: Vec<_> = probe_values
                .iter()
                .enumerate()
                .flat_map(|(p, key)| {
                    build_values
                        .iter()
                        .enumerate()
                        .filter_map(move |(b, value)| {
                            (key == value).then_some((b as u64, p as u32))
                        })
                })
                .collect();
            let counts: Vec<_> = probe_values
                .iter()
                .map(|key| build_values.iter().filter(|value| *value == key).count())
                .collect();
            let random = RandomState::with_seed(0);
            for dense_range in [None, Some((1, 3))] {
                for wide in [false, true] {
                    for batch_size in [1, 2, 4, 7, 32] {
                        let pool: Arc<dyn MemoryPool> =
                            Arc::new(UnboundedMemoryPool::default());
                        let reservation =
                            MemoryConsumer::new("probe cursor test").register(&pool);
                        let index = JoinIndexMap::try_new(
                            &build,
                            &random,
                            NullEquality::NullEqualsNothing,
                            dense_range,
                            &reservation,
                        )?;
                        let mut scratch = LookupScratch::new(&reservation);
                        let matches = index.lookup_batch(&build, &probe, &mut scratch)?;
                        let matches = if wide { matches.widen()? } else { matches };
                        let LookupResult::Groups(matches) = matches else {
                            panic!("duplicate build must return group handles");
                        };
                        let mut buffers = PairBuffer::new(&reservation);
                        let mut cursor = GroupCursor::new(matches);
                        let mut actual = vec![];
                        let mut matched = 0;
                        loop {
                            let chunk =
                                cursor.next_batch(&index, batch_size, &mut buffers)?;
                            matched += chunk.newly_matched_probe_rows;
                            assert!(chunk.build_rows.len() <= batch_size);
                            assert_eq!(chunk.build_rows.len(), chunk.probe_rows.len());
                            actual.extend(
                                chunk
                                    .build_rows
                                    .values()
                                    .iter()
                                    .copied()
                                    .zip(chunk.probe_rows.values().iter().copied()),
                            );
                            let mut consumed = 0;
                            let expected_completed = counts
                                .iter()
                                .take_while(|&&count| {
                                    consumed += count;
                                    consumed <= actual.len()
                                })
                                .count();
                            assert_eq!(
                                cursor.position.row_in_probe_batch,
                                expected_completed
                            );
                            let expected_matched = actual
                                .iter()
                                .map(|&(_, row)| row)
                                .collect::<std::collections::HashSet<_>>()
                                .len();
                            assert_eq!(matched, expected_matched);
                            if cursor.is_finished() {
                                assert_eq!(expected_completed, probe_values.len());
                                break;
                            }
                            assert_eq!(chunk.build_rows.len(), batch_size);
                        }
                        assert_eq!(actual, expected);
                        let chunk =
                            cursor.next_batch(&index, batch_size, &mut buffers)?;
                        assert!(cursor.is_finished());
                        assert_eq!(chunk.newly_matched_probe_rows, 0);
                        assert!(
                            chunk.build_rows.is_empty() && chunk.probe_rows.is_empty()
                        );
                        drop(chunk);
                        assert!(cursor.next_batch(&index, 0, &mut buffers).is_err());
                        let reserved_bytes = buffers.reservation.size();

                        // A new input owns a new cursor, but reuses the pair buffers.
                        let misses = [Arc::new(Int64Array::from(vec![8, 9])) as ArrayRef];
                        let LookupResult::Groups(matches) =
                            index.lookup_batch(&build, &misses, &mut scratch)?
                        else {
                            unreachable!()
                        };
                        let mut cursor = GroupCursor::new(matches);
                        let chunk =
                            cursor.next_batch(&index, batch_size, &mut buffers)?;
                        assert!(cursor.is_finished());
                        assert_eq!(chunk.newly_matched_probe_rows, 0);
                        assert_eq!(cursor.position.row_in_probe_batch, 2);
                        assert_eq!(buffers.reservation.size(), reserved_bytes);
                        assert!(
                            chunk.build_rows.is_empty() && chunk.probe_rows.is_empty()
                        );
                    }
                }
            }
        }
        Ok(())
    }
}
