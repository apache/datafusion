// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

//! Expand duplicate groups at output-batch granularity.
//!
//! Lookup results contain only successful matches, with their original probe
//! indices. The cursor remembers the next result and the next member of its group.
//! For build keys `[A, A, A]`, probe keys `[X, A]`, and batch size 2:
//!
//! ```text
//! first chunk build rows: [0, 1]
//! first chunk probe rows: [1, 1]
//! second chunk build rows: [2]
//! second chunk probe rows: [1]
//! ```
//!
//! Completion means all key matches were enumerated. It does not say whether
//! any pair survived the residual ON predicate; that is the join's decision.

use arrow::array::{UInt32Array, UInt64Array};
use arrow::buffer::ScalarBuffer;
use datafusion_common::{Result, internal_err};
use datafusion_execution::memory_pool::MemoryReservation;

use crate::joins::join_index::{
    GroupRows, JoinIndexMap, LookupResult, MatchHandle, MatchHandles, RowId,
};

/// At most one batch of key-equal pairs, before residual filtering.
pub(super) struct MatchChunk {
    pub(super) build_rows: UInt64Array,
    pub(super) probe_rows: UInt32Array,
    /// Probe rows whose first equality pair was enumerated in this chunk.
    /// Continuing a large group must not count that probe row again.
    pub(super) newly_matched_probe_rows: usize,
}

/// See this module's example for lookup results and chunk continuation.
pub(super) struct GroupCursor {
    matches: LookupResult,
    position: GroupPosition,
}

/// Position of the next build/probe pair to emit.
/// For example, if handle 2 refers to build rows `[10, 20]`, then
/// `(match_index, offset_in_group) = (2, 1)` resumes at build row 20.
#[derive(Default)]
struct GroupPosition {
    /// Index into the parent cursor's `matches` handle array.
    match_index: usize,
    /// Index of the next build row within this handle's group.
    /// For a singleton, this stays zero; emitting its pair advances `match_index`.
    offset_in_group: usize,
}

impl GroupCursor {
    pub(super) fn new(matches: LookupResult) -> Self {
        Self {
            matches,
            position: GroupPosition::default(),
        }
    }

    pub(super) fn is_finished(&self) -> bool {
        self.position.match_index == self.matches.len()
    }

    /// Return the next chunk of at most `batch_size` build/probe row-index pairs,
    /// resuming from the saved position, which may be partway through a group.
    ///
    /// For probe row 7 matching build rows `[10, 20, 30]`, with `batch_size = 2`:
    ///
    /// ```text
    /// first chunk build rows: [10, 20]
    /// first chunk probe rows: [7, 7]
    /// next chunk build rows: [30]
    /// next chunk probe rows: [7]
    /// ```
    ///
    /// An empty result still produces a final chunk so the processor can emit
    /// unmatched outer-join rows. Filtering never changes the saved position.
    pub(super) fn next_batch(
        &mut self,
        index: &JoinIndexMap,
        batch_size: usize,
        buffers: &mut PairBuffer,
    ) -> Result<MatchChunk> {
        if batch_size == 0 {
            return internal_err!("Join pair batch size must be positive");
        }
        if let LookupResult::U64(matches) = &self.matches
            && !matches.has_groups()
        {
            let start = self.position.match_index;
            let len = (matches.len() - start).min(batch_size);
            self.position.match_index += len;
            return Ok(MatchChunk {
                build_rows: UInt64Array::new(
                    matches.ids_buffer().slice(start, len),
                    None,
                ),
                probe_rows: UInt32Array::new(
                    matches.probe_indices().slice(start, len),
                    None,
                ),
                newly_matched_probe_rows: len,
            });
        }
        buffers.clear_for(batch_size)?;
        // Dispatch ID width once per chunk, outside the expansion loop.
        let newly_matched_probe_rows = match &self.matches {
            LookupResult::U32(matches) => {
                self.position.expand(index, matches, batch_size, buffers)
            }
            LookupResult::U64(matches) => {
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
    /// Fill empty `output` with up to `batch_size` build/probe pairs, advancing
    /// `self` to the next pair.
    ///
    /// For handle 2 matching probe row 7 to build rows `[10, 20, 30]`,
    /// with `batch_size = 2`:
    ///
    /// ```text
    /// before self: GroupPosition { match_index: 2, offset_in_group: 1 }
    /// output.build_rows: [20, 30]
    /// output.probe_rows: [7, 7]
    /// after self: GroupPosition { match_index: 3, offset_in_group: 0 }
    /// ```
    fn expand<T: RowId>(
        &mut self,
        index: &JoinIndexMap,
        matches: &MatchHandles<T>,
        batch_size: usize,
        output: &mut PairBuffer,
    ) -> usize {
        let build_rows = &mut output.build_rows;
        let probe_rows = &mut output.probe_rows;
        if !matches.has_groups() {
            let end = matches.len().min(self.match_index + batch_size);
            build_rows.extend(
                matches.ids()[self.match_index..end]
                    .iter()
                    .map(|row| row.as_u64()),
            );
            probe_rows.extend_from_slice(&matches.probe_indices()[self.match_index..end]);
            let newly_matched = end - self.match_index;
            self.match_index = end;
            return newly_matched;
        }

        let mut newly_matched = 0;
        while self.match_index < matches.len() && build_rows.len() < batch_size {
            let row = matches.probe_indices()[self.match_index];
            match matches.get(self.match_index) {
                MatchHandle::One(build) => {
                    newly_matched += 1;
                    build_rows.push(build.as_u64());
                    probe_rows.push(row);
                    self.match_index += 1;
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
                    probe_rows.resize(probe_rows.len() + count, row);
                    if end == members.len() {
                        self.match_index += 1;
                        self.offset_in_group = 0;
                    } else {
                        self.offset_in_group = end;
                        return newly_matched;
                    }
                }
            }
        }
        newly_matched
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
