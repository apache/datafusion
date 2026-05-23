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

use std::collections::VecDeque;
use std::sync::Arc;

use arrow::array::RecordBatch;
use arrow::compute::{SortColumn, concat_batches};
use datafusion_common::utils::{evaluate_partition_ranges, get_row_at_idx};
use datafusion_common::{Result, ScalarValue};
use datafusion_physical_expr::PhysicalSortExpr;

/// Coalesces sorted input batches into partition-local window tiles so the window
/// operator can process fixed-size row frames more memory-efficiently.
///
/// The input MUST already be clustered by the window partition keys, otherwise the
/// behavior is undefined.
///
/// Example: for
///
/// ```sql
/// avg(x) OVER (
///   PARTITION BY k
///   ORDER BY ts
///   ROWS BETWEEN 1 PRECEDING AND 2 FOLLOWING
/// )
/// ```
///
/// use `preceding_halo_len = 1` and `following_halo_len = 2` given query window frame.
///
/// Suppose input has 240 rows, all in the same window partition, and arrives in
/// small batches of 40 rows. If `target_tile_len = 100`, the coalesced batches
/// are:
///
/// - `output_batch_1`: size 102
///   - first 100 rows
///   - no preceding halo because a new partition just start
///   - 2 following halo rows
///
/// - `output_batch_2`: size 103
///   - second 100 rows
///   - 1 preceding halo row
///   - 2 following halo rows
///
/// - `output_batch_3`: size 41
///   - last 40 rows
///   - 1 preceding halo row
///   - no following halo row because a partition has ended
///
/// Coalesced output batches are padded with 'halos', to evaluate window boundaries
/// Input:    aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa
/// output1:  {               }
/// output2:                {                }
/// output3:                 ^             {     }
///                          |
///                          |
///                     overlapped range
///
#[derive(Debug)]
pub(crate) struct WindowTileCoalescer {
    // ====================
    // States and buffer
    // ====================
    /// Buffered slices for the current partition only.
    buffered_batches: Vec<RecordBatch>,
    /// Number of rows in `buffered_batches`.
    buffered_rows: usize,
    /// Partition-relative row index of the first buffered row.
    ///
    /// With `target_tile_len = 100` and `preceding_halo_len = 3`, after emitting
    /// output rows `0..100` this coalescer can prune rows `0..97`. The remaining
    /// buffer starts at partition row `97`, so `buffered_start` becomes `97`.
    buffered_start: usize,
    /// Partition-relative start row of the next tile's owned output range.
    ///
    /// Continuing the `buffered_start = 97` example, the next output range starts
    /// at partition row `100`; the local offset inside `buffered_batches` is
    /// `next_output_start - buffered_start = 3`.
    next_output_start: usize,

    current_partition_val: Option<Vec<ScalarValue>>,
    partition_keys: Vec<PhysicalSortExpr>,

    completed_tiles: VecDeque<CompletedWindowTile>,

    mode: WindowTileMode,
}

/// Controls when buffered rows become a completed window tile.
#[cfg_attr(
    not(test),
    expect(
        dead_code,
        reason = "new window tile coalescer is staged before executor integration"
    )
)]
#[derive(Debug, Clone, Copy)]
pub(crate) enum WindowTileMode {
    /// Emit fixed-size output ranges, with optional halo rows around each range.
    ///
    /// For window expr `...ROWS BETWEEN 2 PRECEDING AND 3 FOLLOWING`, use
    /// `preceding_halo_len = 2` and `following_halo_len = 3`.
    FixedSize {
        /// Configured max tile row count.
        target_tile_len: usize,
        preceding_halo_len: usize,
        following_halo_len: usize,
    },
    /// Emit one tile per window partition, with no halo rows and no target tile
    /// size limit.
    PartitionBoundary,
}

/// A window tile contains enough input rows to evaluate a contiguous output
/// range.
///
/// For `ROWS BETWEEN 3 PRECEDING AND 3 FOLLOWING`, each tile owns a
/// non-overlapping output range, while the materialized batch may include
/// preceding and following halo rows. With `target_tile_len = 100`, a partition
/// that starts at global row `0` produces:
///
/// - First tile: batch rows `0..103`, output rows `0..100`, local output range
///   `0..100`.
/// - Second tile: batch rows `97..203`, output rows `100..200`, local output
///   range `3..103`.
#[derive(Debug, Clone)]
pub(crate) struct CompletedWindowTile {
    /// Input rows needed to evaluate this tile, including halo rows.
    pub(crate) batch: RecordBatch,
    /// Start of the non-halo output range inside `batch`.
    pub(crate) output_start: usize,
    /// End of the non-halo output range inside `batch`.
    pub(crate) output_end: usize,
}

#[cfg_attr(
    not(test),
    expect(
        dead_code,
        reason = "new window tile coalescer is staged before executor integration"
    )
)]
impl WindowTileCoalescer {
    pub(crate) fn new_with_mode(
        partition_keys: Vec<PhysicalSortExpr>,
        mode: WindowTileMode,
    ) -> Self {
        if let WindowTileMode::FixedSize {
            target_tile_len, ..
        } = mode
        {
            assert!(target_tile_len > 0, "target tile length must be positive");
        }

        Self {
            buffered_batches: vec![],
            buffered_rows: 0,
            buffered_start: 0,
            next_output_start: 0,
            current_partition_val: None,
            partition_keys,
            completed_tiles: VecDeque::new(),
            mode,
        }
    }

    /// Push one batch into the coalescer.
    ///
    /// This updates the internal state and may complete one or more tiles. Call
    /// [`Self::next_completed_tile`] to drain completed tiles in output order.
    ///
    /// Caller is responsible for making sure the input batches are globally
    /// clustered by the partition key.
    #[expect(
        clippy::needless_pass_by_value,
        reason = "matches BatchCoalescer-style ownership API"
    )]
    pub(crate) fn push_batch(&mut self, batch: RecordBatch) -> Result<()> {
        if batch.num_rows() == 0 {
            return Ok(());
        }

        match self.mode {
            WindowTileMode::FixedSize {
                target_tile_len,
                preceding_halo_len,
                following_halo_len,
            } => self.push_fixed_size_batch(
                &batch,
                target_tile_len,
                preceding_halo_len,
                following_halo_len,
            ),
            WindowTileMode::PartitionBoundary => {
                self.push_partition_boundary_batch(&batch)
            }
        }
    }

    fn push_fixed_size_batch(
        &mut self,
        batch: &RecordBatch,
        target_tile_len: usize,
        preceding_halo_len: usize,
        following_halo_len: usize,
    ) -> Result<()> {
        for PartitionSlice {
            partition_val,
            batch,
        } in self.partition_slices(batch)?
        {
            let starts_new_partition = self.current_partition_val.as_ref().is_some_and(
                |current_partition_val| current_partition_val != &partition_val,
            );
            if starts_new_partition {
                self.complete_fixed_size_partition(preceding_halo_len)?;
            }

            if self.current_partition_val.is_none() {
                self.current_partition_val = Some(partition_val);
            }
            self.push_fixed_size_partition_slice(
                batch,
                target_tile_len,
                preceding_halo_len,
                following_halo_len,
            )?;
        }

        Ok(())
    }

    fn push_partition_boundary_batch(&mut self, batch: &RecordBatch) -> Result<()> {
        for PartitionSlice {
            partition_val,
            batch,
        } in self.partition_slices(batch)?
        {
            let starts_new_partition = self.current_partition_val.as_ref().is_some_and(
                |current_partition_val| current_partition_val != &partition_val,
            );
            if starts_new_partition {
                self.complete_partition_boundary_partition()?;
            }

            if self.current_partition_val.is_none() {
                self.current_partition_val = Some(partition_val);
            }
            self.buffered_rows += batch.num_rows();
            self.buffered_batches.push(batch);
        }

        Ok(())
    }

    fn partition_slices(&self, batch: &RecordBatch) -> Result<Vec<PartitionSlice>> {
        let partition_columns = self.partition_columns(batch)?;
        let partition_ranges =
            evaluate_partition_ranges(batch.num_rows(), &partition_columns)?;
        let partition_column_values = partition_columns
            .iter()
            .map(|column| Arc::clone(&column.values))
            .collect::<Vec<_>>();

        partition_ranges
            .iter()
            .map(|range| {
                let partition_val = if partition_column_values.is_empty() {
                    vec![]
                } else {
                    get_row_at_idx(&partition_column_values, range.start)?
                };

                Ok(PartitionSlice {
                    partition_val,
                    batch: batch.slice(range.start, range.end - range.start),
                })
            })
            .collect()
    }

    /// Complete the current partition and queue any remaining partial tile.
    pub(crate) fn finish(&mut self) -> Result<()> {
        match self.mode {
            WindowTileMode::FixedSize {
                preceding_halo_len, ..
            } => self.complete_fixed_size_partition(preceding_halo_len),
            WindowTileMode::PartitionBoundary => {
                self.complete_partition_boundary_partition()
            }
        }
    }

    /// Return true when at least one completed tile is ready to be drained.
    pub(crate) fn has_completed_tile(&self) -> bool {
        !self.completed_tiles.is_empty()
    }

    /// Return the next completed tile in output order.
    pub(crate) fn next_completed_tile(&mut self) -> Option<CompletedWindowTile> {
        self.completed_tiles.pop_front()
    }

    fn partition_columns(&self, batch: &RecordBatch) -> Result<Vec<SortColumn>> {
        self.partition_keys
            .iter()
            .map(|partition_key| partition_key.evaluate_to_sort_column(batch))
            .collect()
    }

    /// Flush the current fixed-size partition buffer as one completed tile.
    ///
    /// This is used when the current partition has ended. The completed tile is
    /// therefore allowed to have partial or no following halo, and the buffer is
    /// reset afterward for the next partition.
    fn complete_fixed_size_partition(&mut self, preceding_halo_len: usize) -> Result<()> {
        if self.buffered_rows == 0 {
            self.reset_buffer();
            return Ok(());
        }

        // All indices are relative to current partition start
        let output_start = self.next_output_start;
        let output_end = self.buffered_end();
        debug_assert!(output_start <= output_end);

        if output_start == output_end {
            self.reset_buffer();
            return Ok(());
        }

        let batch_start = output_start
            .saturating_sub(preceding_halo_len)
            .max(self.buffered_start);

        let batch = concat_batches(
            self.buffered_batches[0].schema_ref(),
            &self.buffered_batches,
        )?;
        let batch =
            batch.slice(batch_start - self.buffered_start, output_end - batch_start);
        self.completed_tiles.push_back(CompletedWindowTile {
            output_start: output_start - batch_start,
            output_end: output_end - batch_start,
            batch,
        });
        self.reset_buffer();

        Ok(())
    }

    /// The caller ensures the input batch all rows has the same partition as the
    /// in-progress buffer. Then push the batch into it. If the row limit is reached,
    /// finish all possible tiles.
    fn push_fixed_size_partition_slice(
        &mut self,
        batch: RecordBatch,
        target_tile_len: usize,
        preceding_halo_len: usize,
        following_halo_len: usize,
    ) -> Result<()> {
        self.buffered_rows += batch.num_rows();
        self.buffered_batches.push(batch);

        // Exceeded row count threshold
        if self.next_output_start + target_tile_len + following_halo_len
            <= self.buffered_end()
        {
            // For simplicity, put all currently buffered rows into a single
            // completed tile, even if the buffer contains enough rows for more
            // than one tile. This can make the tile larger than `target_tile_len`,
            // but it preserves correctness and leaves further splitting as an
            // optimization.
            let output_start = self.next_output_start;
            let output_end = self.buffered_end() - following_halo_len;
            let batch_start = output_start
                .saturating_sub(preceding_halo_len)
                .max(self.buffered_start);
            let batch_end = self.buffered_end();

            let batch = concat_batches(
                self.buffered_batches[0].schema_ref(),
                &self.buffered_batches,
            )?;
            let completed_batch =
                batch.slice(batch_start - self.buffered_start, batch_end - batch_start);
            self.completed_tiles.push_back(CompletedWindowTile {
                output_start: output_start - batch_start,
                output_end: output_end - batch_start,
                batch: completed_batch,
            });

            self.next_output_start = output_end;
            self.retain_buffer_from(
                output_end
                    .saturating_sub(preceding_halo_len)
                    .max(self.buffered_start),
                &batch,
            );
        }

        Ok(())
    }

    fn complete_partition_boundary_partition(&mut self) -> Result<()> {
        if self.buffered_rows == 0 {
            self.reset_buffer();
            return Ok(());
        }

        let batch = concat_batches(
            self.buffered_batches[0].schema_ref(),
            &self.buffered_batches,
        )?;
        self.completed_tiles.push_back(CompletedWindowTile {
            output_start: 0,
            output_end: batch.num_rows(),
            batch,
        });
        self.reset_buffer();

        Ok(())
    }

    fn retain_buffer_from(&mut self, keep_from: usize, batch: &RecordBatch) {
        let keep_from = keep_from.clamp(self.buffered_start, self.buffered_end());
        let retained_rows = self.buffered_end() - keep_from;
        self.buffered_batches = if retained_rows == 0 {
            vec![]
        } else {
            vec![batch.slice(keep_from - self.buffered_start, retained_rows)]
        };
        self.buffered_rows = retained_rows;
        self.buffered_start = keep_from;
    }

    fn reset_buffer(&mut self) {
        self.buffered_batches.clear();
        self.buffered_rows = 0;
        self.buffered_start = 0;
        self.next_output_start = 0;
        self.current_partition_val = None;
    }

    fn buffered_end(&self) -> usize {
        self.buffered_start + self.buffered_rows
    }
}

struct PartitionSlice {
    partition_val: Vec<ScalarValue>,
    batch: RecordBatch,
}

#[cfg(test)]
mod tests {
    use super::*;

    use arrow::array::{ArrayRef, Int32Array};
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion_physical_expr::expressions::Column;
    use std::ops::Range;

    // Testing strategy:
    // - Drive WindowTileCoalescer through push_batch/finish, the same contract
    //   its caller uses, instead of mutating private buffer state.
    // - Assert each completed tile's materialized rows and owned output range.
    //   This catches halo clipping, partition resets, delayed following halos,
    //   and empty retained-halo tails without making tests depend on internal
    //   buffering layout.
    #[derive(Debug, PartialEq, Eq)]
    struct TileSnapshot {
        batch_values: Vec<i32>,
        output_range: Range<usize>,
        output_values: Vec<i32>,
    }

    fn unpartitioned_batch(values: &[i32]) -> RecordBatch {
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("v", DataType::Int32, false)])),
            vec![Arc::new(Int32Array::from(values.to_vec())) as ArrayRef],
        )
        .unwrap()
    }

    fn partitioned_batch(partitions: &[Option<i32>], values: &[i32]) -> RecordBatch {
        assert_eq!(partitions.len(), values.len());

        RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("k", DataType::Int32, true),
                Field::new("v", DataType::Int32, false),
            ])),
            vec![
                Arc::new(Int32Array::from(partitions.to_vec())) as ArrayRef,
                Arc::new(Int32Array::from(values.to_vec())) as ArrayRef,
            ],
        )
        .unwrap()
    }

    fn partition_key() -> PhysicalSortExpr {
        PhysicalSortExpr::new_default(Arc::new(Column::new("k", 0)))
    }

    fn collect_tiles(
        mut coalescer: WindowTileCoalescer,
        batches: Vec<RecordBatch>,
    ) -> Result<Vec<TileSnapshot>> {
        let mut tiles = vec![];
        for batch in batches {
            coalescer.push_batch(batch)?;
            drain_tiles(&mut coalescer, &mut tiles);
        }
        coalescer.finish()?;
        drain_tiles(&mut coalescer, &mut tiles);
        Ok(tiles)
    }

    fn drain_tiles(coalescer: &mut WindowTileCoalescer, tiles: &mut Vec<TileSnapshot>) {
        while coalescer.has_completed_tile() {
            let tile = coalescer.next_completed_tile().unwrap();
            tiles.push(snapshot(tile));
        }
        assert!(coalescer.next_completed_tile().is_none());
    }

    fn snapshot(tile: CompletedWindowTile) -> TileSnapshot {
        let batch_values = tile
            .batch
            .column(tile.batch.num_columns() - 1)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap()
            .values()
            .to_vec();
        let output_range = tile.output_start..tile.output_end;
        let output_values = batch_values[output_range.clone()].to_vec();

        TileSnapshot {
            batch_values,
            output_range,
            output_values,
        }
    }

    fn tile(batch_values: Vec<i32>, output_range: Range<usize>) -> TileSnapshot {
        let output_values = batch_values[output_range.clone()].to_vec();
        TileSnapshot {
            batch_values,
            output_range,
            output_values,
        }
    }

    #[test]
    fn coalesces_unpartitioned_input_across_batch_boundaries() -> Result<()> {
        let tiles = collect_tiles(
            WindowTileCoalescer::new_with_mode(
                vec![],
                WindowTileMode::FixedSize {
                    target_tile_len: 4,
                    preceding_halo_len: 1,
                    following_halo_len: 2,
                },
            ),
            vec![
                unpartitioned_batch(&[0, 1]),
                unpartitioned_batch(&[]),
                unpartitioned_batch(&[2, 3, 4]),
                unpartitioned_batch(&[5]),
                unpartitioned_batch(&[6, 7, 8, 9]),
            ],
        )?;

        assert_eq!(
            tiles,
            vec![
                tile((0..6).collect(), 0..4),
                tile((3..10).collect(), 1..5),
                tile((7..10).collect(), 1..3),
            ]
        );

        Ok(())
    }

    #[test]
    fn partition_boundaries_clip_halos_and_reset_tile_state() -> Result<()> {
        let tiles = collect_tiles(
            WindowTileCoalescer::new_with_mode(
                vec![partition_key()],
                WindowTileMode::FixedSize {
                    target_tile_len: 3,
                    preceding_halo_len: 1,
                    following_halo_len: 1,
                },
            ),
            vec![
                partitioned_batch(&[None, None], &[10, 11]),
                partitioned_batch(
                    &[None, None, None, Some(2), Some(2)],
                    &[12, 13, 14, 20, 21],
                ),
                partitioned_batch(&[Some(2), Some(2), Some(3)], &[22, 23, 30]),
            ],
        )?;

        assert_eq!(
            tiles,
            vec![
                tile(vec![10, 11, 12, 13, 14], 0..4),
                tile(vec![13, 14], 1..2),
                tile(vec![20, 21, 22, 23], 0..3),
                tile(vec![22, 23], 1..2),
                tile(vec![30], 0..1),
            ]
        );

        Ok(())
    }

    #[test]
    fn empty_batches_do_not_start_or_finish_partitions() -> Result<()> {
        let tiles = collect_tiles(
            WindowTileCoalescer::new_with_mode(
                vec![],
                WindowTileMode::FixedSize {
                    target_tile_len: 4,
                    preceding_halo_len: 1,
                    following_halo_len: 1,
                },
            ),
            vec![unpartitioned_batch(&[]), unpartitioned_batch(&[])],
        )?;

        assert_eq!(tiles, vec![]);

        Ok(())
    }

    #[test]
    fn no_halo_constructor_emits_exact_completed_tile() -> Result<()> {
        let tiles = collect_tiles(
            WindowTileCoalescer::new_with_mode(
                vec![],
                WindowTileMode::FixedSize {
                    target_tile_len: 4,
                    preceding_halo_len: 0,
                    following_halo_len: 0,
                },
            ),
            vec![unpartitioned_batch(&[0, 1, 2, 3])],
        )?;

        assert_eq!(tiles, vec![tile(vec![0, 1, 2, 3], 0..4)]);

        Ok(())
    }

    #[test]
    fn partition_boundary_mode_emits_only_when_partition_ends() -> Result<()> {
        let mut coalescer =
            WindowTileCoalescer::new_with_mode(vec![], WindowTileMode::PartitionBoundary);
        coalescer.push_batch(unpartitioned_batch(&[0, 1, 2, 3]))?;
        assert!(!coalescer.has_completed_tile());

        coalescer.push_batch(unpartitioned_batch(&[4, 5, 6, 7]))?;
        assert!(!coalescer.has_completed_tile());

        coalescer.finish()?;
        let completed_tile = coalescer.next_completed_tile().unwrap();
        assert_eq!(snapshot(completed_tile), tile((0..8).collect(), 0..8));
        assert!(coalescer.next_completed_tile().is_none());

        Ok(())
    }

    #[test]
    fn partition_boundary_mode_has_no_halo_tiles() -> Result<()> {
        let tiles = collect_tiles(
            WindowTileCoalescer::new_with_mode(
                vec![partition_key()],
                WindowTileMode::PartitionBoundary,
            ),
            vec![
                partitioned_batch(&[None, None], &[10, 11]),
                partitioned_batch(
                    &[None, None, None, Some(2), Some(2)],
                    &[12, 13, 14, 20, 21],
                ),
                partitioned_batch(&[Some(2), Some(2), Some(3)], &[22, 23, 30]),
            ],
        )?;

        assert_eq!(
            tiles,
            vec![
                tile(vec![10, 11, 12, 13, 14], 0..5),
                tile(vec![20, 21, 22, 23], 0..4),
                tile(vec![30], 0..1),
            ]
        );

        Ok(())
    }

    #[test]
    fn retained_preceding_halo_does_not_create_empty_tail_tile() -> Result<()> {
        let tiles = collect_tiles(
            WindowTileCoalescer::new_with_mode(
                vec![],
                WindowTileMode::FixedSize {
                    target_tile_len: 4,
                    preceding_halo_len: 1,
                    following_halo_len: 0,
                },
            ),
            vec![unpartitioned_batch(&[0, 1, 2, 3])],
        )?;

        assert_eq!(tiles, vec![tile(vec![0, 1, 2, 3], 0..4)]);

        Ok(())
    }
}
