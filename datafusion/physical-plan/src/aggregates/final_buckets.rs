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

//! Hash buckets of partial aggregate state for the final hash aggregation.
//!
//! See [`FinalBuckets`].

use std::collections::VecDeque;
use std::sync::Arc;

use arrow::array::{ArrayRef, PrimitiveArray};
use arrow::compute::{BatchCoalescer, take_arrays};
use arrow::datatypes::{SchemaRef, UInt32Type};
use arrow::record_batch::{RecordBatch, RecordBatchOptions};
use datafusion_common::Result;
use datafusion_common::hash_utils::{RandomState, create_hashes};
use datafusion_execution::async_try_stream;
use futures::StreamExt;

use crate::SendableRecordBatchStream;
use crate::spill::in_progress_spill_file::InProgressSpillFile;
use crate::spill::spill_manager::SpillManager;
use crate::stream::RecordBatchStreamAdapter;

/// Number of bits of the routing hash consumed by one bucketing level.
const BUCKET_BITS: u32 = 6;

/// Number of buckets rows are split into at each level.
const NUM_BUCKETS: usize = 1 << BUCKET_BITS;

/// A bucket that is still too large is split again with the next hash bits,
/// up to this many levels.
pub(super) const MAX_BUCKET_LEVELS: u32 = 4;

/// Seed of the routing hash. It differs from both the `RepartitionExec` seed,
/// whose hash is the same for every row of one final partition modulo the
/// partition count, and the aggregation seed, so that the rows of one bucket
/// still spread evenly over the bucket's own hash table.
const BUCKET_HASH_SEED: RandomState = RandomState::with_seed(5364907223173859721);

/// A bucket is compacted for the first time once it holds this many batches
/// worth of rows. With 64 buckets per partition this floor is what the
/// buckets hold when every bucket has few groups, so it is kept small.
const MIN_COMPACTION_BATCHES: usize = 1;

/// A compaction that kept more than this share of its rows found (almost) no
/// repeated groups.
const POOR_COMPACTION: f64 = 0.8;

/// After a compaction that paid off, the bucket is compacted again once it
/// has received at most this many times its own rows. Measured on unreduced
/// input with 5 to 8 rows per group: 2 holds peak memory at about 1.35x of a
/// single table, 16 lets it reach 2.3x.
const MAX_COMPACTION_FACTOR: f64 = 2.0;

/// After a poor compaction the bucket waits for this many times its own rows.
/// Aggregating rows that are already unique again and again is pure rework
/// (1.1x -> 1.5x run time on a wide string key with a factor of 2). But a
/// small sample of a bucket with many groups looks unique even when every
/// group repeats, so the wait must stay short enough to find that out before
/// the whole input is buffered (16 let peak memory reach 1.8x of a single
/// table on input with 5 rows per group).
const POOR_COMPACTION_FACTOR: f64 = 4.0;

/// One hash bucket: its completed batches, held in memory or appended to a
/// spill file, plus the rows not yet forming a full batch.
struct Bucket {
    coalescer: BatchCoalescer,
    batches: VecDeque<RecordBatch>,
    /// Memory held by `batches`
    batches_bytes: usize,
    /// Set once the bucket has been spilled: completed batches go here
    spill_file: Option<InProgressSpillFile>,
    /// Rows routed to the bucket that are still held in memory
    rows: usize,
    /// The bucket is due for compaction once it holds this many rows
    compact_at: usize,
}

/// Rows of partial aggregate state, split by a hash of their group keys.
///
/// Once the table of a final hash aggregation has outgrown the CPU caches,
/// every probe and every accumulator update of a further row is a cache
/// miss. The stream then stops growing that table: it moves the table's
/// state in here, routes the rest of its input here without aggregating it,
/// and finally aggregates the buckets one after another, each with a table
/// that is `NUM_BUCKETS` times smaller. Buckets are independent, so they can
/// be spilled, read back and released one at a time.
///
/// # Compaction
///
/// The input may hold the same group many times (for example when the partial
/// aggregation was skipped), so buffering it as is could take far more memory
/// than the single table it replaces. A bucket that has grown enough is
/// therefore *compacted*: the stream aggregates its rows with a small table
/// and puts the resulting state, one row per group, back in their place. The
/// next compaction waits for as many new rows as made the last one pay off,
/// so the buffered rows stay within a small multiple of the groups they hold,
/// and waits much longer after a compaction that found nothing to merge, so
/// input that does not reduce is not aggregated over and over.
pub(super) struct FinalBuckets {
    /// Bucketing level, selects which bits of the routing hash are used
    level: u32,
    /// Number of leading columns of a batch that are the group keys
    num_group_columns: usize,
    buckets: Vec<Bucket>,
    /// `None` if spilling is not supported by the configured `DiskManager`
    spill_manager: Option<SpillManager>,
    batch_size: usize,
    min_compaction_rows: usize,
    /// Reused buffers
    hashes: Vec<u64>,
    bucket_sizes: Vec<u32>,
    reordered_indices: Vec<u32>,
}

impl FinalBuckets {
    pub(super) fn new(
        schema: &SchemaRef,
        num_group_columns: usize,
        batch_size: usize,
        level: u32,
        spill_manager: Option<SpillManager>,
    ) -> Self {
        debug_assert!(level < MAX_BUCKET_LEVELS);
        let min_compaction_rows = MIN_COMPACTION_BATCHES * batch_size;
        let buckets = (0..NUM_BUCKETS)
            .map(|_| Bucket {
                coalescer: BatchCoalescer::new(Arc::clone(schema), batch_size),
                batches: VecDeque::new(),
                batches_bytes: 0,
                spill_file: None,
                rows: 0,
                compact_at: min_compaction_rows,
            })
            .collect();
        Self {
            level,
            num_group_columns,
            buckets,
            spill_manager,
            batch_size,
            min_compaction_rows,
            hashes: vec![],
            bucket_sizes: vec![0; NUM_BUCKETS],
            reordered_indices: vec![],
        }
    }

    pub(super) fn level(&self) -> u32 {
        self.level
    }

    /// Appends every row of `batch` to the bucket of its group key.
    pub(super) fn route(&mut self, batch: &RecordBatch) -> Result<()> {
        let num_rows = batch.num_rows();
        if num_rows == 0 {
            return Ok(());
        }

        let group_columns: &[ArrayRef] = &batch.columns()[..self.num_group_columns];
        self.hashes.clear();
        self.hashes.resize(num_rows, 0);
        create_hashes(group_columns, &BUCKET_HASH_SEED, &mut self.hashes)?;

        // Counting sort of the row indices by bucket
        let shift = self.level * BUCKET_BITS;
        let bucket_of = |hash: u64| ((hash >> shift) as usize) & (NUM_BUCKETS - 1);
        self.bucket_sizes.fill(0);
        for &hash in &self.hashes {
            self.bucket_sizes[bucket_of(hash)] += 1;
        }
        let mut starts = [0u32; NUM_BUCKETS];
        let mut next = 0;
        for (start, size) in starts.iter_mut().zip(&self.bucket_sizes) {
            *start = next;
            next += size;
        }
        self.reordered_indices.clear();
        self.reordered_indices.resize(num_rows, 0);
        let mut cursors = starts;
        for (row, &hash) in self.hashes.iter().enumerate() {
            let cursor = &mut cursors[bucket_of(hash)];
            self.reordered_indices[*cursor as usize] = row as u32;
            *cursor += 1;
        }

        // One gather for the whole batch, then a slice per bucket. The
        // coalescer copies the slice, so a bucket owns its batches.
        let indices: PrimitiveArray<UInt32Type> =
            std::mem::take(&mut self.reordered_indices).into();
        let columns = take_arrays(batch.columns(), &indices, None)?;
        let options = RecordBatchOptions::new().with_row_count(Some(num_rows));
        let reordered =
            RecordBatch::try_new_with_options(batch.schema(), columns, &options)?;

        for (bucket, (&start, &size)) in self
            .buckets
            .iter_mut()
            .zip(starts.iter().zip(&self.bucket_sizes))
        {
            if size == 0 {
                continue;
            }
            let slice = reordered.slice(start as usize, size as usize);
            match &mut bucket.spill_file {
                // A spilled bucket holds no memory: its rows go straight to
                // the file and are combined into batches when read back.
                Some(spill_file) => {
                    spill_file.append_batch(&slice)?;
                }
                None => {
                    bucket.coalescer.push_batch(slice)?;
                    bucket.rows += size as usize;
                    bucket.collect_completed()?;
                }
            }
        }
        Ok(())
    }

    /// Returns a bucket that is due for compaction, if any.
    pub(super) fn bucket_to_compact(&self) -> Option<usize> {
        self.buckets.iter().position(|bucket| {
            bucket.spill_file.is_none() && bucket.rows >= bucket.compact_at
        })
    }

    /// Removes and returns the rows of an in-memory bucket, to be followed by
    /// [`Self::put_compacted`].
    pub(super) fn take_bucket(&mut self, index: usize) -> Result<Vec<RecordBatch>> {
        let bucket = &mut self.buckets[index];
        debug_assert!(bucket.spill_file.is_none());
        bucket.coalescer.finish_buffered_batch()?;
        bucket.collect_completed()?;
        bucket.batches_bytes = 0;
        Ok(bucket.batches.drain(..).collect())
    }

    /// Puts back the aggregated state of the `input_rows` rows that
    /// [`Self::take_bucket`] returned.
    pub(super) fn put_compacted(
        &mut self,
        index: usize,
        state: Option<RecordBatch>,
        input_rows: usize,
    ) {
        let bucket = &mut self.buckets[index];
        let state_rows = state.as_ref().map_or(0, |state| state.num_rows());
        if let Some(state) = state {
            bucket.batches_bytes += state.get_array_memory_size();
            bucket.batches.push_back(state);
        }
        bucket.rows = state_rows;

        // Compacting again costs about `state_rows + new_rows` and removes
        // about `(1 - kept) * new_rows` rows, where `kept` is the share of rows
        // that survived this time. Wait for enough new rows to pay for it.
        let kept = state_rows as f64 / input_rows.max(1) as f64;
        let factor = if kept > POOR_COMPACTION {
            POOR_COMPACTION_FACTOR
        } else {
            (kept / (1.0 - kept)).clamp(1.0, MAX_COMPACTION_FACTOR)
        };
        let new_rows =
            ((state_rows as f64 * factor) as usize).max(self.min_compaction_rows);
        bucket.compact_at = state_rows + new_rows;
    }

    /// Memory held by the buckets
    pub(super) fn memory_size(&self) -> usize {
        self.buckets
            .iter()
            .map(|bucket| bucket.batches_bytes + bucket.coalescer.size())
            .sum::<usize>()
            + self.hashes.capacity() * size_of::<u64>()
    }

    /// Spills the in-memory batches of the largest bucket, which from then on
    /// appends its batches to the spill file. Returns false if nothing can be
    /// spilled.
    pub(super) fn spill_largest(&mut self) -> Result<bool> {
        let Some(spill_manager) = &self.spill_manager else {
            return Ok(false);
        };
        let Some(bucket) = self
            .buckets
            .iter_mut()
            .filter(|bucket| bucket.spill_file.is_none() && bucket.rows > 0)
            .max_by_key(|bucket| bucket.batches_bytes + bucket.coalescer.size())
        else {
            return Ok(false);
        };
        bucket.spill_file =
            Some(spill_manager.create_in_progress_file("FinalHashAggregateBucket")?);
        // Rows that do not form a full batch yet are spilled as well, and the
        // coalescer is replaced to give up the buffers it allocated.
        bucket.coalescer.finish_buffered_batch()?;
        bucket.collect_completed()?;
        bucket.coalescer = BatchCoalescer::new(bucket.coalescer.schema(), 1);
        Ok(true)
    }

    /// True if no bucket holds rows in memory.
    pub(super) fn is_fully_spilled(&self) -> bool {
        self.buckets.iter().all(|bucket| bucket.rows == 0)
    }

    /// Finishes the buckets and returns the non-empty ones.
    pub(super) fn into_sources(self) -> Result<Vec<BucketSource>> {
        let mut sources = Vec::with_capacity(NUM_BUCKETS);
        for mut bucket in self.buckets {
            bucket.coalescer.finish_buffered_batch()?;
            bucket.collect_completed()?;
            match bucket.spill_file {
                Some(mut spill_file) => {
                    let spill_manager = self
                        .spill_manager
                        .as_ref()
                        .expect("a spilled bucket has a spill manager");
                    if let Some(file) = spill_file.finish()? {
                        sources.push(BucketSource::Spilled {
                            stream: spill_manager.read_spill_as_stream(file, None)?,
                            batch_size: self.batch_size,
                        });
                    }
                }
                None if bucket.batches.is_empty() => {}
                None => sources.push(BucketSource::Memory {
                    batches: bucket.batches,
                    bytes: bucket.batches_bytes,
                }),
            }
        }
        Ok(sources)
    }
}

impl Bucket {
    /// Moves the batches the coalescer has completed to where the bucket
    /// lives: the spill file if it has been spilled, memory otherwise.
    fn collect_completed(&mut self) -> Result<()> {
        while let Some(batch) = self.coalescer.next_completed_batch() {
            self.batches_bytes += batch.get_array_memory_size();
            self.batches.push_back(batch);
        }
        if let Some(spill_file) = &mut self.spill_file {
            for batch in self.batches.drain(..) {
                spill_file.append_batch(&batch)?;
            }
            self.batches_bytes = 0;
            self.rows = 0;
        }
        Ok(())
    }
}

/// The rows of one finished bucket
pub(super) enum BucketSource {
    Memory {
        batches: VecDeque<RecordBatch>,
        bytes: usize,
    },
    /// Rows were appended to the file in pieces much smaller than a batch
    Spilled {
        stream: SendableRecordBatchStream,
        batch_size: usize,
    },
}

impl BucketSource {
    /// Memory held by the bucket's rows
    pub(super) fn memory_size(&self) -> usize {
        match self {
            Self::Memory { bytes, .. } => *bytes,
            Self::Spilled { .. } => 0,
        }
    }

    pub(super) fn into_stream(self, schema: &SchemaRef) -> SendableRecordBatchStream {
        match self {
            Self::Memory { batches, .. } => Box::pin(RecordBatchStreamAdapter::new(
                Arc::clone(schema),
                futures::stream::iter(batches.into_iter().map(Ok)),
            )),
            Self::Spilled { stream, batch_size } => {
                coalesce_stream(stream, Arc::clone(schema), batch_size)
            }
        }
    }
}

/// Combines the batches of `input` into batches of `batch_size` rows.
fn coalesce_stream(
    mut input: SendableRecordBatchStream,
    schema: SchemaRef,
    batch_size: usize,
) -> SendableRecordBatchStream {
    let mut coalescer = BatchCoalescer::new(Arc::clone(&schema), batch_size);
    let stream = async_try_stream(|mut emitter| async move {
        while let Some(batch) = input.next().await.transpose()? {
            coalescer.push_batch(batch)?;
            while let Some(batch) = coalescer.next_completed_batch() {
                emitter.emit(batch).await;
            }
        }
        coalescer.finish_buffered_batch()?;
        while let Some(batch) = coalescer.next_completed_batch() {
            emitter.emit(batch).await;
        }
        Ok(())
    });
    Box::pin(RecordBatchStreamAdapter::new(schema, stream))
}

#[cfg(test)]
mod tests {
    use super::*;

    use arrow::array::{AsArray, Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Int64Type, Schema};
    use std::collections::HashMap;

    fn test_batch(schema: &SchemaRef, keys: std::ops::Range<i64>) -> RecordBatch {
        let names: StringArray = keys
            .clone()
            .map(|k| Some(format!("key-{}", k % 97)))
            .collect();
        let values: Int64Array = keys.clone().map(|k| k * 10).collect::<Vec<_>>().into();
        let keys: Int64Array = keys.collect::<Vec<_>>().into();
        RecordBatch::try_new(
            Arc::clone(schema),
            vec![Arc::new(keys), Arc::new(names), Arc::new(values)],
        )
        .unwrap()
    }

    #[tokio::test]
    async fn routes_every_row_once_and_keeps_keys_together() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("k", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
            Field::new("v", DataType::Int64, false),
        ]));
        let mut buckets = FinalBuckets::new(&schema, 2, 1024, 0, None);
        // the same keys arrive in two different batches
        buckets.route(&test_batch(&schema, 0..20_000))?;
        buckets.route(&test_batch(&schema, 10_000..30_000))?;
        assert!(buckets.memory_size() > 0);
        assert!(
            !buckets.spill_largest()?,
            "no spill manager, nothing spilled"
        );

        let sources = buckets.into_sources()?;
        assert!(
            sources.len() > NUM_BUCKETS / 2,
            "rows spread over the buckets"
        );

        let mut bucket_of_key: HashMap<i64, usize> = HashMap::new();
        let mut rows = 0;
        for (bucket, source) in sources.into_iter().enumerate() {
            let mut stream = source.into_stream(&schema);
            while let Some(batch) = stream.next().await.transpose()? {
                rows += batch.num_rows();
                let keys = batch.column(0).as_primitive::<Int64Type>();
                let values = batch.column(2).as_primitive::<Int64Type>();
                for (key, value) in keys.values().iter().zip(values.values()) {
                    assert_eq!(*value, key * 10, "columns stay aligned");
                    let seen = *bucket_of_key.entry(*key).or_insert(bucket);
                    assert_eq!(seen, bucket, "key {key} in two buckets");
                }
            }
        }
        assert_eq!(rows, 40_000);
        assert_eq!(bucket_of_key.len(), 30_000);
        Ok(())
    }

    #[test]
    fn levels_use_different_hash_bits() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("k", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
            Field::new("v", DataType::Int64, false),
        ]));
        // Rows of one level-0 bucket must spread again at level 1
        let mut level0 = FinalBuckets::new(&schema, 1, 1024, 0, None);
        level0.route(&test_batch(&schema, 0..50_000))?;
        let source = level0.into_sources()?.swap_remove(0);
        let BucketSource::Memory { batches, .. } = source else {
            unreachable!()
        };
        let mut level1 = FinalBuckets::new(&schema, 1, 1024, 1, None);
        for batch in &batches {
            level1.route(batch)?;
        }
        assert!(level1.into_sources()?.len() > NUM_BUCKETS / 2);
        Ok(())
    }
}
