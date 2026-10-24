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

//! Memory accounting shared by the grouped aggregate streams.
//!
//! - [`StreamMemory`] holds a stream's reservation and the rules for sizing
//!   it.
//! - [`SlicedOutput`] keeps a materialized batch reserved while a stream hands
//!   it out in `batch_size` slices.

use arrow::record_batch::RecordBatch;
use datafusion_common::{DataFusionError, Result};
use datafusion_execution::memory_pool::MemoryReservation;

/// The reservation of one aggregate stream.
///
/// A stream reserves its table, plus sort indices when it spills, and every
/// batch it holds while handing it out. It calls, in order:
///
/// 1. [`Self::try_new`] with its initial table, then
///    [`Self::with_sort_index_headroom`] if it spills;
/// 2. [`Self::try_fit_table`] after each input batch. On `ResourcesExhausted`
///    it emits its states early, spills them, or fails, and after a spill
///    calls [`Self::shrink_after_spill`];
/// 3. [`Self::hold`] for a materialized batch it hands out in slices, and
///    [`Self::try_resize`] while its hash table hands out the final output;
/// 4. [`Self::release_for_replay`] before replaying spills, or
///    [`Self::release`] when it ends with memory the steps above did not
///    release: an empty table, or a clustered single stream that is done or
///    failed.
pub(super) struct StreamMemory {
    reservation: MemoryReservation,
    /// Whether the table is reserved with one `u32` per group on top, for the
    /// indices that sort the groups before a spill. This is not the pool's
    /// `can_spill`: a stream that frees memory by emitting its states early
    /// registers `can_spill` but never sorts for a spill.
    sort_index_headroom: bool,
}

impl StreamMemory {
    /// Reserves the initial table on the stream's `reservation`, which it
    /// holds for its lifetime.
    pub(super) fn try_new(
        reservation: MemoryReservation,
        table_size: usize,
    ) -> Result<Self> {
        reservation.try_grow(table_size)?;
        Ok(Self {
            reservation,
            sort_index_headroom: false,
        })
    }

    /// Sets whether the table is reserved with sort-index headroom: true when
    /// the stream has a spill context.
    pub(super) fn with_sort_index_headroom(mut self, sort_index_headroom: bool) -> Self {
        self.sort_index_headroom = sort_index_headroom;
        self
    }

    /// Bytes to reserve for a table of `table_size` bytes holding `groups`.
    fn table_reservation(&self, table_size: usize, groups: usize) -> usize {
        if self.sort_index_headroom {
            table_size.saturating_add(groups.saturating_mul(size_of::<u32>()))
        } else {
            table_size
        }
    }

    /// Reserves the table after an input batch was aggregated into it.
    #[inline]
    pub(super) fn try_fit_table(&self, table_size: usize, groups: usize) -> Result<()> {
        self.reservation
            .try_resize(self.table_reservation(table_size, groups))
    }

    /// Resizes to the table, with no sort-index headroom, after a spill took
    /// all of its states.
    pub(super) fn shrink_after_spill(&self, table_size: usize) -> Result<()> {
        self.try_resize(table_size)
            .map_err(|e| e.context("Decreasing allocation after spilling should succeed"))
    }

    /// Resizes to exactly `size`, with no headroom and no error context. The
    /// caller decides what a refusal means: the hash streams while their table
    /// hands out its materialized output (the size of the batch until its last
    /// slice, then 0), and the clustered single stream after a spill.
    pub(super) fn try_resize(&self, size: usize) -> Result<()> {
        self.reservation.try_resize(size)
    }

    /// Holds `batch` on top of the table, including its sort-index headroom,
    /// while it is handed out in `batch_size` slices; see [`SlicedOutput`].
    pub(super) fn hold(
        &self,
        table_size: usize,
        groups: usize,
        batch: RecordBatch,
        batch_size: usize,
    ) -> Result<SlicedOutput> {
        let table_memory = self.table_reservation(table_size, groups);
        let batch_memory = batch.get_array_memory_size();
        let held = match self.reservation.try_resize(table_memory + batch_memory) {
            Ok(()) => Some(batch_memory),
            Err(DataFusionError::ResourcesExhausted(_)) => {
                self.reservation.try_resize(table_memory)?;
                None
            }
            Err(e) => return Err(e),
        };
        Ok(SlicedOutput {
            batch: Some(batch),
            offset: 0,
            held,
            reserved: self.reservation.size(),
            batch_size,
        })
    }

    /// Releases everything and returns a sibling reservation for the spill
    /// merge and replay. Afterwards only [`Self::release`] may be called,
    /// which then has nothing to free.
    pub(super) fn release_for_replay(&self) -> MemoryReservation {
        self.reservation.free();
        self.reservation.new_empty()
    }

    /// Releases everything still reserved.
    pub(super) fn release(&self) {
        self.reservation.free();
    }
}

/// A materialized batch of groups that a stream hands out in `batch_size`
/// slices.
///
/// The slices share the batch's buffers, so the whole batch stays reserved
/// until the last slice is handed out; from then on the consumer owns the
/// buffers. If the pool could not hold the batch, it is handed out whole and
/// only the table stays reserved.
///
/// Drain it with the [`StreamMemory`] that returned it from
/// [`StreamMemory::hold`], and resize nothing in between.
pub(super) struct SlicedOutput {
    /// The batch, until its last slice is out.
    batch: Option<RecordBatch>,
    /// Rows of `batch` already handed out.
    offset: usize,
    /// Bytes of the batch held in the reservation; `None` when the pool could
    /// not hold it.
    held: Option<usize>,
    /// The size of the reservation when [`StreamMemory::hold`] returned, which
    /// must not change until the last slice.
    reserved: usize,
    batch_size: usize,
}

impl SlicedOutput {
    /// Returns the next slice, or `None` when all rows are out. Releases the
    /// held bytes, and drops this handle on the buffers, before returning the
    /// last slice.
    pub(super) fn next_slice(
        &mut self,
        memory: &StreamMemory,
    ) -> Result<Option<RecordBatch>> {
        let Some(batch) = self.batch.take() else {
            return Ok(None);
        };
        let Some(held) = self.held else {
            return Ok(Some(batch));
        };
        let rest = batch.num_rows() - self.offset;
        if rest > self.batch_size {
            let slice = batch.slice(self.offset, self.batch_size);
            self.offset += self.batch_size;
            self.batch = Some(batch);
            return Ok(Some(slice));
        }
        let last = if self.offset == 0 {
            batch
        } else {
            batch.slice(self.offset, rest)
        };
        debug_assert!(last.num_rows() <= self.batch_size);
        debug_assert_eq!(
            memory.reservation.size(),
            self.reserved,
            "the reservation changed while a SlicedOutput held its batch"
        );
        memory.reservation.try_shrink(held)?;
        Ok(Some(last))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::Int64Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion_execution::memory_pool::{
        GreedyMemoryPool, MemoryConsumer, MemoryPool,
    };

    use super::*;

    const TABLE: usize = 1000;

    fn batch(rows: usize) -> Result<RecordBatch> {
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, false)]));
        let a = Int64Array::from_iter_values(0..rows as i64);
        Ok(RecordBatch::try_new(schema, vec![Arc::new(a)])?)
    }

    /// A stream with a table of `TABLE` bytes in a pool of `limit` bytes.
    fn stream(limit: usize) -> Result<(Arc<dyn MemoryPool>, StreamMemory)> {
        let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(limit));
        let reservation = MemoryConsumer::new("test").register(&pool);
        let memory = StreamMemory::try_new(reservation, TABLE)?;
        Ok((pool, memory))
    }

    /// Drains `output` and returns, for each slice, its rows and the bytes
    /// reserved when it was returned.
    fn drain(
        mut output: SlicedOutput,
        memory: &StreamMemory,
        pool: &Arc<dyn MemoryPool>,
    ) -> Result<Vec<(usize, usize)>> {
        let mut slices = vec![];
        while let Some(slice) = output.next_slice(memory)? {
            slices.push((slice.num_rows(), pool.reserved()));
        }
        Ok(slices)
    }

    #[test]
    fn holds_the_batch_until_the_last_slice() -> Result<()> {
        let (pool, memory) = stream(usize::MAX)?;
        let batch = batch(10)?;
        let held = TABLE + batch.get_array_memory_size();
        let output = memory.hold(TABLE, 0, batch, 4)?;
        assert_eq!(pool.reserved(), held);
        assert_eq!(
            drain(output, &memory, &pool)?,
            [(4, held), (4, held), (2, TABLE)]
        );
        Ok(())
    }

    #[test]
    fn rows_an_exact_multiple_of_batch_size() -> Result<()> {
        let (pool, memory) = stream(usize::MAX)?;
        let batch = batch(8)?;
        let held = TABLE + batch.get_array_memory_size();
        let output = memory.hold(TABLE, 0, batch, 4)?;
        assert_eq!(drain(output, &memory, &pool)?, [(4, held), (4, TABLE)]);
        Ok(())
    }

    #[test]
    fn empty_batch() -> Result<()> {
        let (pool, memory) = stream(usize::MAX)?;
        let output = memory.hold(TABLE, 0, batch(0)?, 4)?;
        assert_eq!(drain(output, &memory, &pool)?, [(0, TABLE)]);
        Ok(())
    }

    #[test]
    fn hands_out_the_batch_whole_when_the_pool_refuses_it() -> Result<()> {
        let batch = batch(10)?;
        let (pool, memory) = stream(TABLE + batch.get_array_memory_size() - 1)?;
        let output = memory.hold(TABLE, 0, batch, 4)?;
        assert_eq!(pool.reserved(), TABLE);
        assert_eq!(drain(output, &memory, &pool)?, [(10, TABLE)]);
        Ok(())
    }

    #[test]
    fn sort_index_headroom_covers_the_table_and_the_held_batch() -> Result<()> {
        let (pool, memory) = stream(usize::MAX)?;
        let memory = memory.with_sort_index_headroom(true);
        let table = TABLE + 10 * size_of::<u32>();
        memory.try_fit_table(TABLE, 10)?;
        assert_eq!(pool.reserved(), table);

        let batch = batch(10)?;
        let held = table + batch.get_array_memory_size();
        let output = memory.hold(TABLE, 10, batch, 4)?;
        assert_eq!(
            drain(output, &memory, &pool)?,
            [(4, held), (4, held), (2, table)]
        );

        memory.shrink_after_spill(TABLE)?;
        assert_eq!(pool.reserved(), TABLE);
        Ok(())
    }
}
