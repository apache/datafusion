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

//! Bucket (distribution) sort used by `ExternalSorter` when
//! `datafusion.execution.sort_bucket_sort_enabled` is set.
//!
//! Instead of sorting every input batch and k-way merging the sorted batches
//! (where the interleave of the merge reads rows from every batch at random), the
//! rows are scattered into key-range buckets as they arrive, and every bucket is
//! sorted on its own, in cache, and emitted in key order.
//!
//! ```text
//!  input batch
//!      │
//!      ▼
//!  sort keys ──▶ order preserving u128 key per row, packing several sort keys:
//!                 a key with few values takes the bits of its dictionary code,
//!                 an exact 64 bit key takes 64, and the first other key the rest
//!      │
//!      ├── until the boundaries are chosen (sort_bucket_sample_rows rows):
//!      │     reservoir sample of the keys     ─▶ quantile boundaries
//!      │     Misra-Gries heavy hitters        ─▶ a bucket of their own
//!      │     HyperLogLog distinct count       ─▶ a bucket per value (counting sort)
//!      │     distinct values of every key     ─▶ dictionaries of keys with few values
//!      │     presortedness                    ─▶ keep appending, no sort at all
//!      │     the batch is buffered as is
//!      │
//!      └── after: counting sort of the rows by bucket (one in-cache take, skipped
//!          when the rows are already in bucket order); every bucket keeps the
//!          (batch, offset, len, min key, max key) pieces it got
//!
//!  output, bucket by bucket in key order, level by level of the sort keys
//!      constant on the key      ─▶ on to the next key, no copy; past the last key
//!                                  copied out, not sorted
//!      sorted, disjoint pieces  ─▶ copied out in zone map order, not sorted
//!      keys over the target     ─▶ split by quantiles of the key (the next bytes of
//!                                  a string key when they all share the first ones)
//!      otherwise                ─▶ concatenated and sorted in cache, by the keys
//!                                  from this one on
//!      no memory for it         ─▶ the rows of later groups are spilled to make
//!                                  room; when that is not enough, spilled and
//!                                  sorted by a nested merge sort
//!      a bucket's sorted rows are copied out once, into output batches shared
//!      with the next buckets, of batch_size rows and at most the memory of the
//!      largest input batch
//!
//!  memory full
//!      presorted so far         ─▶ the buffered batches are one sorted run on disk
//!      otherwise                ─▶ every bucket's rows are appended, unsorted, to the
//!                                  spill file of its bucket group, the pieces of a
//!                                  group written together in batches of batch_size
//!                                  rows; at the end every
//!                                  group is loaded and bucket sorted on its own, or,
//!                                  past the memory it gets, spilled again and merge
//!                                  sorted by a nested sorter
//! ```
//!
//! # Memory
//!
//! A scattered batch is reserved at its size: unlike the merge sort, which sorts every
//! batch into a copy at once and so reserves twice what it holds, the bucket sort only
//! works on one bucket at a time. Batches buffered before the boundaries are chosen
//! keep the merge sort's 2x estimate ([`get_reserved_bytes_for_record_batch`]), as a
//! small input is handed to the merge sort. The reservation is reconciled after every
//! change of what is held, and when it cannot grow everything held is spilled. While
//! emitting, room for sorting the largest bucket is reserved once next to what is
//! held: its sort key values (their share of the rows' memory is measured on the
//! first rows), its row numbers, and one output batch, as the rows are copied out a
//! batch at a time. A bucket that does not fit is spilled and sorted by the nested
//! sorter.

use std::cmp::Ordering;
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, AsArray, DynComparator, RecordBatch, UInt32Array, make_comparator,
};
use arrow::buffer::NullBuffer;
use arrow::compute::kernels::sort::{LexicographicalComparator, SortColumn};
use arrow::compute::{
    BatchCoalescer, SortOptions, concat, concat_batches, interleave,
    interleave_record_batch, lexsort_to_indices, take_record_batch,
};
use arrow::datatypes::{DataType, Schema, SchemaRef, i256};
use datafusion_common::instant::Instant;
use datafusion_common::{DataFusionError, Result, internal_datafusion_err};
use datafusion_execution::memory_pool::MemoryReservation;
use datafusion_execution::runtime_env::RuntimeEnv;
use datafusion_execution::{TryEmitter, async_try_stream};
use datafusion_physical_expr::{LexOrdering, PhysicalExpr};
use futures::StreamExt;
use futures::future::BoxFuture;
use log::debug;

use crate::SendableRecordBatchStream;
use crate::metrics::{
    BaselineMetrics, Count, ExecutionPlanMetricsSet, Gauge, MetricBuilder, Time,
};
use crate::sorts::deep_copy::deep_copy_batch;
use crate::sorts::sort::{
    get_reserved_bytes_for_record_batch, sorted_copy_keeps_buffers,
};
use crate::sorts::streaming_merge::{SortedSpillFile, StreamingMergeBuilder};
use crate::spill::get_record_batch_memory_size;
use crate::spill::in_progress_spill_file::InProgressSpillFile;
use crate::spill::spill_manager::GetSlicedSize;
use crate::spill::spill_manager::SpillManager;
use crate::stream::{ObservedStream, RecordBatchStreamAdapter};
use datafusion_execution::SpillFile;

/// Most buckets rows are scattered into on arrival, so a bucket id fits a `u16`
const MAX_BUCKETS: usize = 4096;
/// Number of spill files the buckets are grouped into when spilling
const MAX_SPILL_GROUPS: usize = 64;
/// Most parts an overfull bucket is split into
const MAX_SPLIT: usize = 256;
/// Most times rows are split by the same sort key
const MAX_SPLIT_DEPTH: usize = 2;
/// Bytes of a row's packed key, the least its sort keys are counted as when sizing the
/// parts a bucket is split into, see [`BucketContext::key_row_bytes`]
const KEY_ROW_BYTES: usize = size_of::<u128>();
/// Memory a bucket's sort holds per row, besides its sort key values: the row numbers in
/// key order, the sort indices, and when it is split, its packed keys and parts
const SORT_ROW_BYTES: usize = 64;
/// Memory of the held rows the auto mode times both modes on
const PROBE_BYTES: usize = 16 * 1024 * 1024;
/// Deepest string or binary key bytes encoded, for values sharing a long prefix
const MAX_KEY_BYTE_OFFSET: usize = 64;
/// Keys kept by the reservoir sample the boundaries are chosen from
const RESERVOIR_CAPACITY: usize = 16 * 1024;
/// Most distinct values of a key replaced by dictionary codes
const MAX_DICTIONARY_VALUES: usize = 4096;
/// Rows the sample must have per distinct value of a key to give it a dictionary
const MIN_DICTIONARY_REPEATS: usize = 16;

/// How the bucket sort holds arriving rows until it sorts them
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum BucketSortMode {
    /// Each batch is reordered by bucket on arrival, so a bucket's rows are copied out
    /// of a small area when it is sorted: two copies, both local
    Scatter,
    /// Batches are kept as they came and each bucket keeps the numbers of its rows,
    /// copied once when it is sorted, read from all over memory
    Gather,
    /// Both are timed on the rows held when the boundaries are chosen, and the faster
    /// is kept
    Auto,
}

impl std::str::FromStr for BucketSortMode {
    type Err = DataFusionError;

    fn from_str(s: &str) -> Result<Self> {
        match s.to_ascii_lowercase().as_str() {
            "scatter" => Ok(Self::Scatter),
            "gather" => Ok(Self::Gather),
            "auto" => Ok(Self::Auto),
            other => Err(DataFusionError::Configuration(format!(
                "Unknown bucket sort mode {other}, expected scatter, gather or auto"
            ))),
        }
    }
}

/// Settings of the bucket sort, from the session's execution options
#[derive(Debug, Clone, Copy)]
pub(crate) struct BucketSortOptions {
    pub bucket_count: usize,
    pub target_bucket_bytes: usize,
    pub sample_rows: usize,
    /// How arriving rows are held until their bucket is sorted
    pub mode: BucketSortMode,
}

/// Counters of the paths the bucket sort takes, so a run shows which ones fire.
///
/// They are registered with the plan's metrics only once the bucket sort produces
/// the output ([`Self::publish`]): an input small enough for the merge sort, or a key
/// type the bucket sort does not support, keeps the merge sort's metrics alone.
#[derive(Debug, Clone)]
pub(crate) struct BucketSortMetrics {
    /// Where the metrics are built, and the plan's metrics they are published to
    local: ExecutionPlanMetricsSet,
    target: ExecutionPlanMetricsSet,
    published: Arc<std::sync::atomic::AtomicBool>,
    /// Batches scattered into buckets
    pub scattered_batches: Count,
    /// Batches whose rows were already in bucket order, scattered without a take
    pub scattered_in_place_batches: Count,
    /// Buckets sorted (concatenated and lexsorted)
    pub sorted_buckets: Count,
    /// Buckets holding a single key value, emitted without sorting
    pub constant_buckets: Count,
    /// Buckets (or parts of them) constant on a leading sort key, sorted by the
    /// later keys only
    pub leading_key_constant: Count,
    /// Buckets (or parts of them) larger than the target, split by a sample of the
    /// keys of the first sort key they are not constant on, before sorting
    pub split_buckets: Count,
    /// Buckets sorted by a nested merge sort, for lack of memory to sort them in place
    pub external_buckets: Count,
    /// Heavy hitter keys given a bucket of their own
    pub heavy_hitters: Count,
    /// Partitions whose few distinct keys each got a bucket (a counting sort)
    pub dictionary_path: Count,
    /// Buckets sorted by their packed keys alone, which order every sort key exactly
    pub packed_key_sorts: Count,
    /// Sort keys the packed key is made of, and of them those it orders exactly
    pub key_segments: Gauge,
    pub exact_key_levels: Gauge,
    /// Partitions whose string or binary first key had few enough distinct values
    /// to be replaced by order preserving codes, making it exact
    pub key_dictionary: Count,
    /// Partitions whose input was already sorted, emitted without sorting
    pub presorted_input: Count,
    /// Partitions whose batches were kept as they came, each row copied once when its
    /// bucket is sorted (gather mode)
    pub gather_mode: Count,
    /// In auto mode, the best time of reordering the probed rows by bucket and
    /// gathering every bucket from the reordered pieces
    pub probe_scatter_time: Time,
    /// In auto mode, the best time of gathering every bucket straight from the probed
    /// rows as they came
    pub probe_gather_time: Time,
    /// Spilled bucket groups loaded back and bucket sorted in memory
    pub spill_groups_in_memory: Count,
    /// Spilled bucket groups too large for memory, merge sorted by a nested sorter
    pub spill_groups_external: Count,
    /// Buckets given the room to sort them by spilling the rows of later groups
    pub room_spills: Count,
    /// Batches written to the spill files of the bucket groups
    pub spill_batches: Count,
    /// Independent runs of overlapping sorted pieces found by the zone maps
    pub zone_map_components: Count,
    /// Sorted pieces emitted without sorting thanks to the zone maps
    pub zone_map_presorted_pieces: Count,
    /// Batches emitted as they were held, without a copy
    pub zero_copy_batches: Count,
    /// HyperLogLog estimate of the distinct first keys seen before choosing the boundaries
    pub estimated_distinct_keys: Count,
    /// Estimated memory of the largest bucket sorted, to see how evenly the
    /// boundaries spread the rows
    pub largest_bucket_bytes: Gauge,
    /// Computing the keys, the sketches and the presortedness
    pub stats_time: Time,
    /// Scattering rows into buckets
    pub scatter_time: Time,
    /// Sorting and copying out buckets
    pub bucket_time: Time,
    /// Evaluating the sort keys of the rows sorted, ordering them, and gathering the
    /// rows in sorted order
    pub sort_columns_time: Time,
    pub sort_indices_time: Time,
    pub gather_time: Time,
    /// Writing and reading the spill files of the bucket groups
    pub spill_write_time: Time,
    pub spill_read_time: Time,
    /// Largest memory reserved by the bucket sort
    pub peak_reserved_bytes: Gauge,
}

impl BucketSortMetrics {
    pub(crate) fn new(target: &ExecutionPlanMetricsSet, partition: usize) -> Self {
        let local = ExecutionPlanMetricsSet::new();
        let metrics = &local;
        let counter =
            |name: &'static str| MetricBuilder::new(metrics).counter(name, partition);
        let time =
            |name: &'static str| MetricBuilder::new(metrics).subset_time(name, partition);
        Self {
            target: target.clone(),
            published: Default::default(),
            scattered_batches: counter("bucket_sort_scattered_batches"),
            scattered_in_place_batches: counter("bucket_sort_scattered_in_place_batches"),
            sorted_buckets: counter("bucket_sort_sorted_buckets"),
            constant_buckets: counter("bucket_sort_constant_buckets"),
            leading_key_constant: counter("bucket_sort_leading_key_constant"),
            split_buckets: counter("bucket_sort_split_buckets"),
            external_buckets: counter("bucket_sort_external_buckets"),
            heavy_hitters: counter("bucket_sort_heavy_hitters"),
            dictionary_path: counter("bucket_sort_dictionary_path"),
            packed_key_sorts: counter("bucket_sort_packed_key_sorts"),
            key_segments: MetricBuilder::new(metrics)
                .gauge("bucket_sort_key_segments", partition),
            exact_key_levels: MetricBuilder::new(metrics)
                .gauge("bucket_sort_exact_key_levels", partition),
            key_dictionary: counter("bucket_sort_key_dictionary"),
            presorted_input: counter("bucket_sort_presorted_input"),
            gather_mode: counter("bucket_sort_gather_mode"),
            probe_scatter_time: time("time_bucket_sort_probe_scatter"),
            probe_gather_time: time("time_bucket_sort_probe_gather"),
            spill_groups_in_memory: counter("bucket_sort_spill_groups_in_memory"),
            spill_groups_external: counter("bucket_sort_spill_groups_external"),
            room_spills: counter("bucket_sort_room_spills"),
            spill_batches: counter("bucket_sort_spill_batches"),
            zone_map_components: counter("bucket_sort_zone_map_components"),
            zone_map_presorted_pieces: counter("bucket_sort_zone_map_presorted_pieces"),
            zero_copy_batches: counter("bucket_sort_zero_copy_batches"),
            estimated_distinct_keys: counter("bucket_sort_estimated_distinct_keys"),
            largest_bucket_bytes: MetricBuilder::new(metrics)
                .gauge("bucket_sort_largest_bucket_bytes", partition),
            stats_time: time("time_bucket_sort_stats"),
            scatter_time: time("time_bucket_sort_scatter"),
            bucket_time: time("time_bucket_sort_buckets"),
            sort_columns_time: time("time_bucket_sort_evaluating_keys"),
            sort_indices_time: time("time_bucket_sort_indices"),
            gather_time: time("time_bucket_sort_gather"),
            spill_write_time: time("time_bucket_sort_spill_write"),
            spill_read_time: time("time_bucket_sort_spill_read"),
            peak_reserved_bytes: MetricBuilder::new(metrics)
                .gauge("bucket_sort_peak_reserved_bytes", partition),
            local,
        }
    }

    /// Registers the metrics with the plan's metrics, once
    pub(crate) fn publish(&self) {
        if self
            .published
            .swap(true, std::sync::atomic::Ordering::Relaxed)
        {
            return;
        }
        for metric in self.local.clone_inner().iter() {
            self.target.register(Arc::clone(metric));
        }
    }
}

/// Sorts a stream with a nested merge sort (an `ExternalSorter` without the bucket
/// sort), for data too large to bucket sort in the memory available
pub(crate) type NestedSort = Arc<
    dyn Fn(
            SchemaRef,
            SendableRecordBatchStream,
        ) -> BoxFuture<'static, Result<SendableRecordBatchStream>>
        + Send
        + Sync,
>;

// ============================================================================
// Order preserving keys
// ============================================================================

/// Maps sort keys to `u128` keys such that `a <= b` in the sort order implies
/// `key(a) <= key(b)`. Rows with different keys are therefore ordered by their keys
/// alone, which is all bucketing needs.
///
/// The key packs segments, from the high bits down, starting with the sort key at
/// `level`:
/// - a key with few distinct values takes its [`KeyDictionary`] code, only the bits
///   the code needs, and the next key follows it,
/// - an exact key of 64 bits (integers, dates, ...) as the first segment takes 64
///   bits, and the next key follows it,
/// - any other key takes the bits left, a prefix of its encoding, and ends the key.
///
/// A row whose segment does not hold a single value (a value between dictionary
/// values, a truncated prefix) is not ordered by the later keys, so the later
/// segments of such rows are zero.
#[derive(Debug, Clone)]
pub(crate) struct KeyEncoder {
    segments: Vec<Segment>,
}

#[derive(Debug, Clone)]
struct Segment {
    expr: Arc<dyn PhysicalExpr>,
    options: SortOptions,
    kind: SegmentKind,
    /// Bits of the key, from `shift` up
    bits: u32,
    shift: u32,
}

#[derive(Debug, Clone)]
enum SegmentKind {
    Dictionary(Arc<KeyDictionary>),
    /// The order preserving encoding of the values. A string or binary key skips
    /// `byte_offset` bytes, for values sharing their first bytes.
    Raw {
        exact_type: bool,
        bytes: bool,
        byte_offset: usize,
    },
}

/// The keys of a batch, nulls of the key at `level` are not ordered by `values`
pub(crate) struct Keys {
    values: Vec<u128>,
    nulls: Option<NullBuffer>,
}

impl Keys {
    #[inline]
    fn is_null(&self, i: usize) -> bool {
        self.nulls.as_ref().is_some_and(|n| n.is_null(i))
    }

    fn len(&self) -> usize {
        self.values.len()
    }
}

fn is_bytes_type(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Utf8
            | DataType::LargeUtf8
            | DataType::Utf8View
            | DataType::Binary
            | DataType::LargeBinary
            | DataType::BinaryView
            | DataType::FixedSizeBinary(_)
    )
}

/// Bits that hold every value up to `max`
fn bits_for(max: u64) -> u32 {
    (u64::BITS - max.leading_zeros()).max(1)
}

impl KeyEncoder {
    /// Encoder of the sort keys from `level` on, `None` when the type of the key at
    /// `level` is not supported. `dictionaries` has the dictionary of every level
    /// that has one.
    pub(crate) fn try_new(
        ordering: &LexOrdering,
        level: usize,
        schema: &Schema,
        dictionaries: &[Option<Arc<KeyDictionary>>],
    ) -> Result<Option<Self>> {
        let mut segments: Vec<Segment> = vec![];
        let mut used = 0u32;
        for (d, key) in ordering.iter().enumerate().skip(level) {
            let remaining = 128 - used;
            let first = segments.is_empty();
            if !first && remaining < 8 {
                break;
            }
            let data_type = key.expr.data_type(schema)?;
            let Some(exact_type) = key_exactness(&data_type) else {
                break;
            };
            let segment = |kind, bits: u32| Segment {
                expr: Arc::clone(&key.expr),
                options: key.options,
                kind,
                bits,
                shift: 128 - used - bits,
            };
            if let Some(Some(dictionary)) = dictionaries.get(d) {
                // Values, and nulls one past them
                let bits = bits_for(dictionary.max_code() + 1);
                if bits <= remaining {
                    segments.push(segment(
                        SegmentKind::Dictionary(Arc::clone(dictionary)),
                        bits,
                    ));
                    used += bits;
                    continue;
                }
            }
            let bytes = is_bytes_type(&data_type);
            let bits = if bytes { remaining } else { remaining.min(64) };
            segments.push(segment(
                SegmentKind::Raw {
                    exact_type,
                    bytes,
                    byte_offset: 0,
                },
                bits,
            ));
            used += bits;
            // Only an exact first segment of 64 bits orders the rows by the next key
            if !(first && exact_type && !bytes && bits == 64) {
                break;
            }
        }
        Ok((!segments.is_empty()).then_some(Self { segments }))
    }

    /// Whether a key packing every sort key from `level` on exactly fits 128 bits, see
    /// [`Self::exact_keys`]
    fn packs_exactly(&self, keys: usize, level: usize) -> bool {
        self.exact_levels() == keys - level && self.segments.len() == keys - level
    }

    /// Leading segments that [`Self::exact_keys`] packs exactly within 128 bits: a
    /// dictionary code takes its bits and an exact integer key its 64
    fn exact_levels(&self) -> usize {
        let mut bits = 0;
        let mut levels = 0;
        for segment in &self.segments {
            bits += match &segment.kind {
                SegmentKind::Dictionary(_) => segment.bits,
                SegmentKind::Raw {
                    exact_type: true,
                    bytes: false,
                    ..
                } => 64,
                SegmentKind::Raw { .. } => break,
            };
            if bits > 128 {
                break;
            }
            levels += 1;
        }
        levels
    }

    /// Keys ordering the rows by every sort key, from `columns`, the values of the sort
    /// keys from the encoder's level, when [`Self::packs_exactly`]: dictionary codes,
    /// and the whole 64 bits of an integer key. `None` when a row's key is not exact:
    /// a value missing from a dictionary, or a null of an integer key.
    fn exact_keys(&self, columns: &[SortColumn]) -> Result<Option<Vec<u128>>> {
        let rows = columns.first().map_or(0, |c| c.values.len());
        let mut keys = vec![0u128; rows];
        let mut used = 0u32;
        for (i, (segment, column)) in self.segments.iter().zip(columns).enumerate() {
            let array = column.values.as_ref();
            if let SegmentKind::Dictionary(_) = &segment.kind {
                used += segment.bits;
                let shift = 128 - used;
                for (key, code) in keys.iter_mut().zip(segment.codes(array, i == 0)?) {
                    if !segment.code_exact(code, i == 0) {
                        return Ok(None);
                    }
                    *key |= code << shift;
                }
            } else {
                if array.logical_null_count() > 0 {
                    return Ok(None);
                }
                used += 64;
                let shift = 128 - used;
                let values = encode_column(array, segment.options, 0)?;
                for (key, value) in keys.iter_mut().zip(values) {
                    *key |= u128::from(value) << shift;
                }
            }
        }
        Ok(Some(keys))
    }

    fn first(&self) -> &Segment {
        &self.segments[0]
    }

    pub(crate) fn options(&self) -> SortOptions {
        self.first().options
    }

    /// Whether a row with these bits of the first segment has a single value of its key
    #[inline]
    fn first_exact(&self, code: u128) -> bool {
        self.first().code_exact(code, true)
    }

    #[inline]
    fn first_code(&self, key: u128) -> u128 {
        let first = self.first();
        (key >> first.shift) & mask(first.bits)
    }

    /// Encoder of the 16 bytes after the ones the first segment encodes, for a
    /// string or binary key. Only rows sharing those bytes are ordered by it.
    fn next_bytes(&self) -> Option<Self> {
        let first = self.first();
        match first.kind {
            SegmentKind::Raw {
                bytes: true,
                byte_offset,
                exact_type,
            } if byte_offset < MAX_KEY_BYTE_OFFSET => Some(Self {
                segments: vec![Segment {
                    kind: SegmentKind::Raw {
                        bytes: true,
                        byte_offset: byte_offset + 16,
                        exact_type,
                    },
                    bits: 128,
                    shift: 0,
                    ..first.clone()
                }],
            }),
            _ => None,
        }
    }

    /// Equal keys mean equal rows in the sort order, whatever the values
    fn exact_key(&self, keys: usize, level: usize) -> bool {
        self.segments.len() == 1
            && level + 1 == keys
            && matches!(
                self.first().kind,
                SegmentKind::Raw {
                    exact_type: true,
                    bytes: false,
                    ..
                }
            )
            && self.first().bits == 64
    }

    /// Whether every row from `min` to `max` has the same value of the key at `level`
    #[inline]
    fn level_constant(&self, min: u128, max: u128) -> bool {
        let code = self.first_code(min);
        code == self.first_code(max) && self.first_exact(code)
    }

    pub(crate) fn encode(&self, batch: &RecordBatch) -> Result<Keys> {
        let rows = batch.num_rows();
        let mut values = vec![0u128; rows];
        // Rows whose segments so far each hold a single value
        let mut ordered = vec![true; rows];
        let mut nulls = None;
        for (i, segment) in self.segments.iter().enumerate() {
            let first = i == 0;
            let array = segment.expr.evaluate(batch)?.into_array(rows)?;
            let codes = segment.codes(array.as_ref(), first)?;
            let last = i + 1 == self.segments.len();
            for (row, code) in codes.into_iter().enumerate() {
                if ordered[row] {
                    values[row] |= code << segment.shift;
                    if !last {
                        ordered[row] = segment.code_exact(code, first);
                    }
                }
            }
            if first {
                nulls = array.logical_nulls().filter(|n| n.null_count() > 0);
                // Nulls of the first key are bucketed apart, and are one value
                if let Some(n) = &nulls {
                    for (row, ordered) in ordered.iter_mut().enumerate() {
                        *ordered |= n.is_null(row);
                    }
                }
            }
        }
        Ok(Keys { values, nulls })
    }
}

#[inline]
fn mask(bits: u32) -> u128 {
    if bits >= 128 {
        u128::MAX
    } else {
        (1u128 << bits) - 1
    }
}

impl Segment {
    /// Codes of `bits` bits. Nulls of the first segment are bucketed apart, so they
    /// take any code; nulls of a later one go before or after every value.
    fn codes(&self, array: &dyn Array, first: bool) -> Result<Vec<u128>> {
        let nulls = array.logical_nulls().filter(|n| n.null_count() > 0);
        let is_null = |row: usize| nulls.as_ref().is_some_and(|n| n.is_null(row));
        match &self.kind {
            SegmentKind::Dictionary(dictionary) => {
                // Values from 1 and nulls 0 when nulls go first, else nulls past them
                let null = if self.options.nulls_first {
                    0
                } else {
                    dictionary.max_code() as u128 + 1
                };
                let shift = u128::from(self.options.nulls_first);
                Ok(dictionary
                    .encode(array, self.options.descending)?
                    .into_iter()
                    .enumerate()
                    .map(|(row, code)| {
                        if is_null(row) {
                            null
                        } else {
                            code as u128 + shift
                        }
                    })
                    .collect())
            }
            SegmentKind::Raw {
                bytes, byte_offset, ..
            } => {
                let wide: Vec<u128> = if *bytes {
                    let high = encode_column(array, self.options, *byte_offset)?;
                    let low = encode_column(array, self.options, byte_offset + 8)?;
                    high.into_iter()
                        .zip(low)
                        .map(|(h, l)| ((h as u128) << 64) | l as u128)
                        .collect()
                } else {
                    encode_column(array, self.options, 0)?
                        .into_iter()
                        .map(|v| (v as u128) << 64)
                        .collect()
                };
                let bits = self.bits;
                if first {
                    // The top bits; nulls are bucketed apart
                    return Ok(wide.into_iter().map(|v| v >> (128 - bits)).collect());
                }
                // One bit less, so nulls go before (0) or after (all ones) every value
                let null = if self.options.nulls_first {
                    0
                } else {
                    mask(bits)
                };
                Ok(wide
                    .into_iter()
                    .enumerate()
                    .map(|(row, v)| {
                        if is_null(row) {
                            null
                        } else {
                            1 + (v >> (129 - bits))
                        }
                    })
                    .collect())
            }
        }
    }

    /// Whether rows with this code hold a single value of the key
    #[inline]
    fn code_exact(&self, code: u128, first: bool) -> bool {
        match &self.kind {
            SegmentKind::Dictionary(dictionary) => {
                let max = dictionary.max_code() as u128;
                if self.options.nulls_first {
                    code == 0 || (code - 1) & 1 == 1
                } else {
                    code > max || code & 1 == 1
                }
            }
            SegmentKind::Raw {
                exact_type, bytes, ..
            } => first && *exact_type && !*bytes && self.bits == 64,
        }
    }
}

/// Order preserving codes of the distinct values of a key: value `i` of the sorted
/// values is `2i + 1`, and a value between values `i - 1` and `i` is `2i`. Odd codes
/// therefore hold a single value.
#[derive(Debug)]
pub(crate) enum KeyDictionary {
    /// String and binary values, sorted by their bytes
    Bytes {
        sorted: Vec<Vec<u8>>,
        index: HashMap<Vec<u8>, u64>,
    },
    /// Ascending order preserving encodings of exact keys
    Encoded(Vec<u64>),
}

/// Distinct values of a key seen while sampling, until there are too many
#[derive(Debug)]
enum DistinctValues {
    Bytes(std::collections::HashSet<Vec<u8>>),
    Encoded(std::collections::HashSet<u64>),
}

impl DistinctValues {
    fn new(data_type: &DataType) -> Option<Self> {
        if is_bytes_type(data_type) {
            Some(Self::Bytes(Default::default()))
        } else if key_exactness(data_type) == Some(true) {
            Some(Self::Encoded(Default::default()))
        } else {
            None
        }
    }

    fn len(&self) -> usize {
        match self {
            Self::Bytes(values) => values.len(),
            Self::Encoded(values) => values.len(),
        }
    }

    fn add(&mut self, array: &dyn Array) -> Result<()> {
        let nulls = array.logical_nulls();
        let valid = |row: usize| nulls.as_ref().is_none_or(|n| n.is_valid(row));
        match self {
            Self::Bytes(values) => {
                let mut row = 0;
                for_each_bytes(array, |value| {
                    if valid(row) && !values.contains(value) {
                        values.insert(value.to_vec());
                    }
                    row += 1;
                })?;
            }
            Self::Encoded(values) => {
                for (row, value) in encode_array(array, 0)?.into_iter().enumerate() {
                    if valid(row) {
                        values.insert(value);
                    }
                }
            }
        }
        Ok(())
    }

    fn into_dictionary(self) -> KeyDictionary {
        match self {
            Self::Bytes(values) => {
                let mut sorted: Vec<Vec<u8>> = values.into_iter().collect();
                sorted.sort_unstable();
                let index = sorted
                    .iter()
                    .enumerate()
                    .map(|(i, v)| (v.clone(), i as u64))
                    .collect();
                KeyDictionary::Bytes { sorted, index }
            }
            Self::Encoded(values) => {
                let mut sorted: Vec<u64> = values.into_iter().collect();
                sorted.sort_unstable();
                KeyDictionary::Encoded(sorted)
            }
        }
    }
}

impl KeyDictionary {
    fn len(&self) -> usize {
        match self {
            Self::Bytes { sorted, .. } => sorted.len(),
            Self::Encoded(sorted) => sorted.len(),
        }
    }

    /// Largest code of a value
    fn max_code(&self) -> u64 {
        2 * self.len() as u64
    }

    /// Codes in the sort order: descending flips them
    fn encode(&self, array: &dyn Array, descending: bool) -> Result<Vec<u64>> {
        let mut codes = Vec::with_capacity(array.len());
        match self {
            Self::Bytes { sorted, index } => for_each_bytes(array, |value| {
                codes.push(match index.get(value) {
                    Some(i) => 2 * i + 1,
                    None => 2 * sorted.partition_point(|v| v.as_slice() < value) as u64,
                })
            })?,
            Self::Encoded(sorted) => {
                for value in encode_array(array, 0)? {
                    codes.push(match sorted.binary_search(&value) {
                        Ok(i) => 2 * i as u64 + 1,
                        Err(i) => 2 * i as u64,
                    });
                }
            }
        }
        if descending {
            let max = self.max_code();
            for c in codes.iter_mut() {
                *c = max - *c;
            }
        }
        Ok(codes)
    }
}

/// Calls `f` with the bytes of every value of a string or binary array, empty for nulls
fn for_each_bytes(array: &dyn Array, mut f: impl FnMut(&[u8])) -> Result<()> {
    use DataType::*;
    match array.data_type() {
        Utf8 => array
            .as_string::<i32>()
            .iter()
            .for_each(|v| f(v.unwrap_or("").as_bytes())),
        LargeUtf8 => array
            .as_string::<i64>()
            .iter()
            .for_each(|v| f(v.unwrap_or("").as_bytes())),
        Utf8View => array
            .as_string_view()
            .iter()
            .for_each(|v| f(v.unwrap_or("").as_bytes())),
        Binary => array
            .as_binary::<i32>()
            .iter()
            .for_each(|v| f(v.unwrap_or(&[]))),
        LargeBinary => array
            .as_binary::<i64>()
            .iter()
            .for_each(|v| f(v.unwrap_or(&[]))),
        BinaryView => array
            .as_binary_view()
            .iter()
            .for_each(|v| f(v.unwrap_or(&[]))),
        FixedSizeBinary(_) => array
            .as_fixed_size_binary()
            .iter()
            .for_each(|v| f(v.unwrap_or(&[]))),
        other => {
            return Err(internal_datafusion_err!(
                "Bucket sort has no dictionary for sort key type {other}"
            ));
        }
    }
    Ok(())
}

/// Order preserving `u64` of every value of a column, nulls aside
fn encode_column(
    array: &dyn Array,
    options: SortOptions,
    byte_offset: usize,
) -> Result<Vec<u64>> {
    let mut values = encode_array(array, byte_offset)?;
    if options.descending {
        for k in values.iter_mut() {
            *k = !*k;
        }
    }
    Ok(values)
}

/// `Some(exact)` for supported key types: whether equal codes mean equal values, see [`KeyEncoder::exact_key`]
fn key_exactness(data_type: &DataType) -> Option<bool> {
    use DataType::*;
    match data_type {
        Boolean
        | Int8
        | Int16
        | Int32
        | Int64
        | UInt8
        | UInt16
        | UInt32
        | UInt64
        | Float16
        | Float32
        | Float64
        | Date32
        | Date64
        | Time32(_)
        | Time64(_)
        | Timestamp(_, _)
        | Duration(_)
        | Decimal32(_, _)
        | Decimal64(_, _) => Some(true),
        // Clamped into the i64 range, which holds every value of precision 18
        Decimal128(p, _) | Decimal256(p, _) => Some(*p <= 18),
        // The first 16 bytes
        Utf8 | LargeUtf8 | Utf8View | Binary | LargeBinary | BinaryView
        | FixedSizeBinary(_) => Some(false),
        _ => None,
    }
}

#[inline]
fn signed_key(v: i64) -> u64 {
    (v as u64) ^ (1 << 63)
}

/// Matches `total_cmp`, which arrow sorts floats by
#[inline]
fn float_key(bits: u64, sign: u64) -> u64 {
    if bits & sign != 0 {
        !bits & (sign | (sign - 1))
    } else {
        bits | sign
    }
}

#[inline]
fn prefix_key(bytes: &[u8], offset: usize) -> u64 {
    let bytes = &bytes[offset.min(bytes.len())..];
    let mut buf = [0u8; 8];
    let n = bytes.len().min(8);
    buf[..n].copy_from_slice(&bytes[..n]);
    u64::from_be_bytes(buf)
}

fn encode_array(array: &dyn Array, byte_offset: usize) -> Result<Vec<u64>> {
    use DataType::*;
    let data = array.to_data();
    let keys = match array.data_type() {
        Boolean => array.as_boolean().values().iter().map(u64::from).collect(),
        Int8 => data
            .buffer::<i8>(0)
            .iter()
            .map(|v| signed_key(*v as i64))
            .collect(),
        Int16 => data
            .buffer::<i16>(0)
            .iter()
            .map(|v| signed_key(*v as i64))
            .collect(),
        Int32 | Date32 | Time32(_) | Decimal32(_, _) => data
            .buffer::<i32>(0)
            .iter()
            .map(|v| signed_key(*v as i64))
            .collect(),
        Int64 | Date64 | Time64(_) | Timestamp(_, _) | Duration(_) | Decimal64(_, _) => {
            data.buffer::<i64>(0)
                .iter()
                .map(|v| signed_key(*v))
                .collect()
        }
        UInt8 => data.buffer::<u8>(0).iter().map(|v| *v as u64).collect(),
        UInt16 => data.buffer::<u16>(0).iter().map(|v| *v as u64).collect(),
        UInt32 => data.buffer::<u32>(0).iter().map(|v| *v as u64).collect(),
        UInt64 => data.buffer::<u64>(0).to_vec(),
        Float16 => data
            .buffer::<u16>(0)
            .iter()
            .map(|v| float_key(*v as u64, 1 << 15))
            .collect(),
        Float32 => data
            .buffer::<u32>(0)
            .iter()
            .map(|v| float_key(*v as u64, 1 << 31))
            .collect(),
        Float64 => data
            .buffer::<u64>(0)
            .iter()
            .map(|v| float_key(*v, 1 << 63))
            .collect(),
        Decimal128(_, _) => data
            .buffer::<i128>(0)
            .iter()
            .map(|v| signed_key((*v).clamp(i64::MIN as i128, i64::MAX as i128) as i64))
            .collect(),
        Decimal256(_, _) => data
            .buffer::<i256>(0)
            .iter()
            .map(|v| {
                let v = match v.to_i128() {
                    Some(v) => v.clamp(i64::MIN as i128, i64::MAX as i128) as i64,
                    None if v.is_negative() => i64::MIN,
                    None => i64::MAX,
                };
                signed_key(v)
            })
            .collect(),
        Utf8 => {
            let a = array.as_string::<i32>();
            (0..a.len())
                .map(|i| prefix_key(a.value(i).as_bytes(), byte_offset))
                .collect()
        }
        LargeUtf8 => {
            let a = array.as_string::<i64>();
            (0..a.len())
                .map(|i| prefix_key(a.value(i).as_bytes(), byte_offset))
                .collect()
        }
        Utf8View => {
            let a = array.as_string_view();
            (0..a.len())
                .map(|i| prefix_key(a.value(i).as_bytes(), byte_offset))
                .collect()
        }
        Binary => {
            let a = array.as_binary::<i32>();
            (0..a.len())
                .map(|i| prefix_key(a.value(i), byte_offset))
                .collect()
        }
        LargeBinary => {
            let a = array.as_binary::<i64>();
            (0..a.len())
                .map(|i| prefix_key(a.value(i), byte_offset))
                .collect()
        }
        BinaryView => {
            let a = array.as_binary_view();
            (0..a.len())
                .map(|i| prefix_key(a.value(i), byte_offset))
                .collect()
        }
        FixedSizeBinary(_) => {
            let a = array.as_fixed_size_binary();
            (0..a.len())
                .map(|i| prefix_key(a.value(i), byte_offset))
                .collect()
        }
        other => {
            return Err(internal_datafusion_err!(
                "Bucket sort does not support sort key type {other}"
            ));
        }
    };
    Ok(keys)
}

// ============================================================================
// Sketches
// ============================================================================

/// xorshift64*, enough for sampling
struct Rng(u64);

impl Rng {
    fn next_u64(&mut self) -> u64 {
        self.0 ^= self.0 >> 12;
        self.0 ^= self.0 << 25;
        self.0 ^= self.0 >> 27;
        self.0.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }

    /// Uniform in the open interval (0, 1)
    fn next_open01(&mut self) -> f64 {
        ((self.next_u64() >> 11) as f64 + 0.5) / (1u64 << 53) as f64
    }

    fn below(&mut self, n: usize) -> usize {
        (self.next_u64() % n as u64) as usize
    }
}

/// Uniform sample of the non null keys, Li's algorithm L: it skips between the
/// replaced positions, so the cost after the reservoir is full is per sample, not per row.
struct Reservoir {
    sample: Vec<u128>,
    capacity: usize,
    /// Rows offered so far
    seen: u64,
    /// 1-based position of the next row to take once the reservoir is full
    next: u64,
    w: f64,
    rng: Rng,
}

impl Reservoir {
    fn new(capacity: usize) -> Self {
        Self {
            sample: Vec::with_capacity(capacity),
            capacity,
            seen: 0,
            next: 0,
            w: 1.0,
            rng: Rng(0x9E37_79B9_7F4A_7C15),
        }
    }

    fn skip(&mut self) -> u64 {
        let skip = (self.rng.next_open01().ln() / (1.0 - self.w).ln()).floor();
        (skip as u64).saturating_add(1)
    }

    fn advance_w(&mut self) {
        self.w *= (self.rng.next_open01().ln() / self.capacity as f64).exp();
    }

    fn add(&mut self, keys: &Keys) {
        let n = keys.len();
        let mut i = 0;
        while i < n && self.sample.len() < self.capacity {
            if !keys.is_null(i) {
                self.sample.push(keys.values[i]);
            }
            i += 1;
            self.seen += 1;
            if self.sample.len() == self.capacity {
                self.advance_w();
                self.next = self.seen.saturating_add(self.skip());
            }
        }
        if self.sample.len() < self.capacity {
            return;
        }
        let end = self.seen + (n - i) as u64;
        while self.next <= end {
            let pos = i + (self.next - self.seen - 1) as usize;
            if !keys.is_null(pos) {
                let slot = self.rng.below(self.capacity);
                self.sample[slot] = keys.values[pos];
            }
            self.advance_w();
            self.next = self.next.saturating_add(self.skip());
        }
        self.seen = end;
    }
}

/// HyperLogLog with 2^12 registers, about 1.6% standard error
struct HyperLogLog {
    registers: Vec<u8>,
}

impl HyperLogLog {
    const P: u32 = 12;

    fn new() -> Self {
        Self {
            registers: vec![0; 1 << Self::P],
        }
    }

    #[inline]
    fn add(&mut self, key: u128) {
        let hash = mix64((key as u64) ^ mix64((key >> 64) as u64));
        let index = (hash >> (64 - Self::P)) as usize;
        let rest = (hash << Self::P) | (1 << (Self::P - 1));
        let rank = rest.leading_zeros() as u8 + 1;
        if rank > self.registers[index] {
            self.registers[index] = rank;
        }
    }

    fn estimate(&self) -> f64 {
        let m = self.registers.len() as f64;
        let alpha = 0.7213 / (1.0 + 1.079 / m);
        let sum: f64 = self.registers.iter().map(|r| 2f64.powi(-(*r as i32))).sum();
        let raw = alpha * m * m / sum;
        let zeros: usize = self.registers.iter().map(|r| usize::from(*r == 0)).sum();
        if raw <= 2.5 * m && zeros > 0 {
            m * (m / zeros as f64).ln()
        } else {
            raw
        }
    }
}

/// splitmix64 finalizer
#[inline]
fn mix64(mut x: u64) -> u64 {
    x ^= x >> 30;
    x = x.wrapping_mul(0xBF58_476D_1CE4_E5B9);
    x ^= x >> 27;
    x = x.wrapping_mul(0x94D0_49BB_1331_11EB);
    x ^ (x >> 31)
}

/// Misra-Gries frequent items: every key with frequency above `total / (k + 1)` is
/// kept, with a count that undercounts by at most that much
struct MisraGries {
    counters: HashMap<u128, u64>,
    k: usize,
    total: u64,
}

impl MisraGries {
    fn new(k: usize) -> Self {
        Self {
            counters: HashMap::with_capacity(k + 1),
            k,
            total: 0,
        }
    }

    #[inline]
    fn add(&mut self, key: u128) {
        self.total += 1;
        if let Some(count) = self.counters.get_mut(&key) {
            *count += 1;
        } else if self.counters.len() < self.k {
            self.counters.insert(key, 1);
        } else {
            self.counters.retain(|_, count| {
                *count -= 1;
                *count > 0
            });
        }
    }

    /// Keys seen at least `min_count` times
    fn heavy(&self, min_count: u64) -> Vec<u128> {
        let mut heavy: Vec<u128> = self
            .counters
            .iter()
            .filter(|(_, count)| **count >= min_count)
            .map(|(key, _)| *key)
            .collect();
        heavy.sort_unstable();
        heavy
    }
}

// ============================================================================
// Boundaries
// ============================================================================

/// Bucket boundaries in key space. Value bucket `j` holds the keys in
/// `[splitters[j - 1], splitters[j])`, and nulls have a bucket of their own, first
/// or last as the sort puts them. Bucket ids are in output order.
#[derive(Debug, Clone)]
pub(crate) struct Boundaries {
    splitters: Vec<u128>,
    nulls_first: bool,
}

impl Boundaries {
    fn num_buckets(&self) -> usize {
        self.splitters.len() + 2
    }

    fn null_bucket(&self) -> usize {
        if self.nulls_first {
            0
        } else {
            self.splitters.len() + 1
        }
    }

    #[inline]
    fn bucket_of(&self, keys: &Keys, row: usize) -> usize {
        if keys.is_null(row) {
            self.null_bucket()
        } else {
            self.splitters.partition_point(|s| *s <= keys.values[row])
                + self.nulls_first as usize
        }
    }
}

/// What the sketches made of the keys
struct BoundaryChoice {
    boundaries: Boundaries,
    dictionary: bool,
    heavy_hitters: usize,
}

/// Splitters that put every value of `points` in a bucket of its own
fn point_splitters(points: &[u128]) -> impl Iterator<Item = u128> + '_ {
    points
        .iter()
        .flat_map(|p| std::iter::once(*p).chain(p.checked_add(1)))
}

/// Quantile splitters of a sorted sample, at most `count` of them
fn quantile_splitters(sorted: &[u128], count: usize) -> Vec<u128> {
    if sorted.is_empty() || count == 0 {
        return vec![];
    }
    let mut splitters: Vec<u128> = (1..=count)
        .map(|j| sorted[(j * sorted.len() / (count + 1)).min(sorted.len() - 1)])
        .collect();
    splitters.dedup();
    splitters
}

/// The cut points splitting `keys` into `count + 1` parts of equal size, exactly: each
/// is selected (`select_nth_unstable`) between its neighbours, so the cost is
/// `n * log(count)` rather than a full sort
fn exact_quantile_splitters(keys: &mut [u128], count: usize) -> Vec<u128> {
    let n = keys.len();
    if n == 0 || count == 0 {
        return vec![];
    }
    let mut positions: Vec<usize> = (1..=count)
        .map(|j| (j * n / (count + 1)).min(n - 1))
        .collect();
    positions.dedup();
    let mut cuts = vec![0u128; positions.len()];
    select_positions(keys, 0, &positions, &mut cuts);
    // Positions are ascending, so the cuts are too
    cuts.dedup();
    cuts
}

/// Fills `out[i]` with the key that sorts at `positions[i]` of `keys`, where `keys` is
/// the part of the whole starting at `offset`
fn select_positions(
    keys: &mut [u128],
    offset: usize,
    positions: &[usize],
    out: &mut [u128],
) {
    if positions.is_empty() {
        return;
    }
    let mid = positions.len() / 2;
    let at = positions[mid] - offset;
    let (left, nth, right) = keys.select_nth_unstable(at);
    out[mid] = *nth;
    select_positions(left, offset, &positions[..mid], &mut out[..mid]);
    select_positions(
        right,
        offset + at + 1,
        &positions[mid + 1..],
        &mut out[mid + 1..],
    );
}

fn choose_boundaries(
    mut sample: Vec<u128>,
    heavy: &MisraGries,
    distinct_estimate: f64,
    value_buckets: usize,
    nulls_first: bool,
) -> BoundaryChoice {
    sample.sort_unstable();
    let mut distinct = sample.clone();
    distinct.dedup();

    // Few distinct keys, all of them in the sample: a bucket per key, so with an
    // exact key no bucket has to be sorted
    if !distinct.is_empty()
        && distinct.len() * 2 < value_buckets
        && distinct_estimate <= distinct.len() as f64 * 1.1
    {
        let mut splitters: Vec<u128> = point_splitters(&distinct).collect();
        splitters.dedup();
        return BoundaryChoice {
            boundaries: Boundaries {
                splitters,
                nulls_first,
            },
            dictionary: true,
            heavy_hitters: 0,
        };
    }

    // Keys frequent enough to fill a bucket get a bucket of their own, so the
    // quantiles spread over the rest
    let min_count = (heavy.total / value_buckets as u64).max(1);
    let mut hitters = heavy.heavy(min_count);
    hitters.truncate(value_buckets / 4);
    let rest: Vec<u128> = sample
        .iter()
        .copied()
        .filter(|k| hitters.binary_search(k).is_err())
        .collect();
    let range_splitters = value_buckets.saturating_sub(1 + 2 * hitters.len());
    let mut splitters = quantile_splitters(&rest, range_splitters);
    splitters.extend(point_splitters(&hitters));
    splitters.sort_unstable();
    splitters.dedup();

    BoundaryChoice {
        boundaries: Boundaries {
            splitters,
            nulls_first,
        },
        dictionary: false,
        heavy_hitters: hitters.len(),
    }
}

// ============================================================================
// Buckets
// ============================================================================

/// A batch scattered into buckets, shared by the pieces of its rows in every bucket
struct HeldBatch {
    batch: RecordBatch,
    /// Reserved for it: its memory size
    reserved: usize,
    /// Pieces not yet emitted
    live_pieces: usize,
    /// (bucket, offset, len) of its pieces, to spill those not yet emitted
    ranges: Vec<(usize, usize, usize)>,
}

/// Rows of one bucket in a held batch
#[derive(Debug, Clone)]
struct Piece {
    held: usize,
    offset: usize,
    len: usize,
    /// Zone map of the piece's keys, meaningless in the null bucket
    min: u128,
    max: u128,
    /// The rows are in sort order
    sorted: bool,
    /// In gather mode, the numbers of the bucket's rows in the held batch, which is not
    /// reordered; `offset` and `len` then do not apply
    rows: Option<Arc<[u32]>>,
}

#[derive(Debug, Clone)]
struct Bucket {
    pieces: Vec<Piece>,
    rows: usize,
    /// Estimated memory of the rows
    bytes: usize,
    min: u128,
    max: u128,
}

impl Bucket {
    fn new() -> Self {
        Self {
            pieces: vec![],
            rows: 0,
            bytes: 0,
            min: u128::MAX,
            max: 0,
        }
    }
}

/// The rows of a batch grouped by bucket
struct Scattered {
    batch: RecordBatch,
    /// (bucket, offset, len, min, max)
    ranges: Vec<(usize, usize, usize, u128, u128)>,
    sorted: bool,
    /// In gather mode, the numbers of the rows of every range, in `ranges` order
    rows: Option<Vec<Arc<[u32]>>>,
}

/// Counting sort of the rows of `batch` by bucket. When the rows are already in
/// bucket order the batch is kept as is.
/// In gather mode the batch is not reordered: every bucket gets the numbers of its rows.
fn scatter(
    boundaries: &Boundaries,
    batch: RecordBatch,
    keys: &Keys,
    sorted: bool,
    gather: bool,
    metrics: &BucketSortMetrics,
) -> Result<Scattered> {
    let _timer = metrics.scatter_time.timer();
    let n = batch.num_rows();
    let num_buckets = boundaries.num_buckets();
    let mut ids: Vec<u16> = Vec::with_capacity(n);
    let mut counts = vec![0u32; num_buckets];
    let mut mins = vec![u128::MAX; num_buckets];
    let mut maxs = vec![0u128; num_buckets];
    let mut in_order = true;
    let mut previous = 0usize;
    for row in 0..n {
        let bucket = boundaries.bucket_of(keys, row);
        ids.push(bucket as u16);
        counts[bucket] += 1;
        if !keys.is_null(row) {
            let key = keys.values[row];
            mins[bucket] = mins[bucket].min(key);
            maxs[bucket] = maxs[bucket].max(key);
        }
        in_order &= bucket >= previous;
        previous = bucket;
    }

    let mut ranges = Vec::new();
    let mut start = 0usize;
    for bucket in 0..num_buckets {
        let count = counts[bucket] as usize;
        if count > 0 {
            ranges.push((bucket, start, count, mins[bucket], maxs[bucket]));
        }
        start += count;
    }

    metrics.scattered_batches.add(1);
    if gather && !in_order {
        let mut rows: Vec<Vec<u32>> = ranges
            .iter()
            .map(|(_, _, len, _, _)| Vec::with_capacity(*len))
            .collect();
        let mut range_of = vec![usize::MAX; num_buckets];
        for (i, (bucket, _, _, _, _)) in ranges.iter().enumerate() {
            range_of[*bucket] = i;
        }
        for (row, bucket) in ids.iter().enumerate() {
            rows[range_of[*bucket as usize]].push(row as u32);
        }
        return Ok(Scattered {
            batch,
            ranges,
            sorted,
            rows: Some(rows.into_iter().map(Arc::from).collect()),
        });
    }
    let batch = if in_order {
        metrics.scattered_in_place_batches.add(1);
        batch
    } else {
        let mut cursors: Vec<u32> = Vec::with_capacity(num_buckets);
        let mut acc = 0u32;
        for count in &counts {
            cursors.push(acc);
            acc += count;
        }
        let mut permutation = vec![0u32; n];
        for (row, bucket) in ids.iter().enumerate() {
            let cursor = &mut cursors[*bucket as usize];
            permutation[*cursor as usize] = row as u32;
            *cursor += 1;
        }
        take_record_batch(&batch, &UInt32Array::from(permutation))?
    };

    Ok(Scattered {
        batch,
        ranges,
        // A stable counting sort keeps every bucket's rows in their order
        sorted,
        rows: None,
    })
}

#[derive(Default)]
struct BucketStore {
    held: Vec<Option<HeldBatch>>,
    buckets: Vec<Bucket>,
    /// Sum of `reserved` of the held batches
    reserved: usize,
    /// Memory of the largest batch added
    max_batch_bytes: usize,
    /// Largest memory of a row of a batch added, on average over the batch
    max_row_bytes: usize,
}

impl BucketStore {
    fn new(num_buckets: usize) -> Self {
        Self {
            held: vec![],
            buckets: (0..num_buckets).map(|_| Bucket::new()).collect(),
            reserved: 0,
            max_batch_bytes: 0,
            max_row_bytes: 0,
        }
    }

    fn is_empty(&self) -> bool {
        self.reserved == 0 && self.held.iter().all(Option::is_none)
    }

    /// Most memory an output batch of `batch_size` rows takes: `batch_size` rows of
    /// the widest rows held, and no less than the largest input batch
    fn output_batch_bytes(&self, batch_size: usize) -> usize {
        self.max_row_bytes
            .saturating_mul(batch_size)
            .max(self.max_batch_bytes)
            .max(1)
    }

    fn add(&mut self, scattered: Scattered) -> Result<()> {
        let Scattered {
            batch,
            ranges,
            sorted,
            rows,
        } = scattered;
        if batch.num_rows() == 0 {
            return Ok(());
        }
        let memory = get_record_batch_memory_size(&batch);
        let reserved = memory;
        self.max_batch_bytes = self.max_batch_bytes.max(memory);
        self.max_row_bytes = self.max_row_bytes.max(memory.div_ceil(batch.num_rows()));
        let held = self.held.len();
        for (i, (bucket, offset, len, min, max)) in ranges.iter().enumerate() {
            let b = &mut self.buckets[*bucket];
            b.pieces.push(Piece {
                held,
                offset: *offset,
                len: *len,
                min: *min,
                max: *max,
                sorted,
                rows: rows.as_ref().map(|rows| Arc::clone(&rows[i])),
            });
            b.rows += len;
            b.bytes += memory * len / batch.num_rows();
            b.min = b.min.min(*min);
            b.max = b.max.max(*max);
        }
        self.held.push(Some(HeldBatch {
            batch,
            reserved,
            live_pieces: ranges.len(),
            ranges: ranges
                .iter()
                .map(|(bucket, offset, len, _, _)| (*bucket, *offset, *len))
                .collect(),
        }));
        self.reserved += reserved;
        Ok(())
    }

    /// The pieces of a bucket as batches, with whether each covers its whole held batch
    fn slices(&self, pieces: &[Piece]) -> Vec<(RecordBatch, bool)> {
        pieces
            .iter()
            .map(|p| {
                let held = self.held[p.held]
                    .as_ref()
                    .expect("piece of a released batch");
                let whole = p.offset == 0 && p.len == held.batch.num_rows();
                (held.batch.slice(p.offset, p.len), whole)
            })
            .collect()
    }

    /// The rows of `pieces`, with a selection when some are rows of a batch kept as it
    /// came (gather mode)
    fn row_set(&self, pieces: &[Piece], bytes: usize) -> RowSet {
        if pieces.iter().all(|p| p.rows.is_none()) {
            return RowSet::all(self.slices(pieces), bytes);
        }
        let mut slices = Vec::with_capacity(pieces.len());
        let mut selection = vec![];
        for p in pieces {
            let held = self.held[p.held]
                .as_ref()
                .expect("piece of a released batch");
            let slice = slices.len();
            slices.push((held.batch.clone(), false));
            match &p.rows {
                Some(rows) => selection.extend(rows.iter().map(|r| (slice, *r as usize))),
                None => {
                    selection.extend((p.offset..p.offset + p.len).map(|r| (slice, r)))
                }
            }
        }
        RowSet {
            rows: selection.len(),
            slices: Arc::new(slices),
            selection: Some(selection),
            bytes,
        }
    }

    /// Releases emitted pieces, dropping the held batches with no pieces left
    fn release(&mut self, pieces: &[Piece]) {
        for p in pieces {
            let slot = &mut self.held[p.held];
            let held = slot.as_mut().expect("piece released twice");
            held.live_pieces -= 1;
            if held.live_pieces == 0 {
                self.reserved -= held.reserved;
                *slot = None;
            }
        }
    }

    fn take_bucket(&mut self, bucket: usize) -> Bucket {
        std::mem::replace(&mut self.buckets[bucket], Bucket::new())
    }

    /// Writes the pieces of held batch `index` not yet emitted to their groups' spill
    /// files and drops the batch, returning the memory freed. The pieces of the `keep`
    /// buckets stay in memory, copied into a batch of their own.
    fn spill_held(
        &mut self,
        index: usize,
        spill: &mut SpillGroups,
        keep: &std::ops::Range<usize>,
    ) -> Result<usize> {
        let Some(held) = self.held[index].take() else {
            return Ok(0);
        };
        let rows = held.batch.num_rows().max(1);
        let mut kept = vec![];
        for (bucket, offset, len) in &held.ranges {
            let b = &mut self.buckets[*bucket];
            // Emitted already when its bucket no longer has it
            let Some(at) = b.pieces.iter().position(|p| p.held == index) else {
                continue;
            };
            let piece = b.pieces.remove(at);
            b.rows -= len;
            b.bytes = b.bytes.saturating_sub(held.reserved * len / rows);
            let bytes = held.reserved * len / rows;
            let set = match &piece.rows {
                Some(selected) => RowSet {
                    slices: Arc::new(vec![(held.batch.clone(), false)]),
                    selection: Some(selected.iter().map(|r| (0, *r as usize)).collect()),
                    rows: *len,
                    bytes,
                },
                None => {
                    RowSet::all(vec![(held.batch.slice(*offset, *len), false)], bytes)
                }
            };
            if keep.contains(bucket) {
                let rows =
                    copy_row_sets(&held.batch.schema(), std::slice::from_ref(&set))?;
                kept.push((*bucket, rows, piece));
            } else {
                // Gathered with the group's other rows when written: copied once
                spill.append(*bucket, set, self.max_batch_bytes)?;
            }
        }
        self.reserved -= held.reserved;
        let mut freed = held.reserved;
        if !kept.is_empty() {
            let batches: Vec<&RecordBatch> =
                kept.iter().map(|(_, rows, _)| rows).collect();
            let batch = concat_batches(&held.batch.schema(), batches)?;
            let mut ranges = Vec::with_capacity(kept.len());
            let mut offset = 0;
            for (bucket, rows, piece) in &kept {
                ranges.push((*bucket, offset, rows.num_rows(), piece.min, piece.max));
                offset += rows.num_rows();
            }
            let sorted = kept.iter().all(|(_, _, piece)| piece.sorted);
            let before = self.reserved;
            self.add(Scattered {
                batch,
                ranges,
                sorted,
                rows: None,
            })?;
            freed = freed.saturating_sub(self.reserved - before);
        }
        Ok(freed)
    }
}

/// Spills the held batches of `store` before `end`, oldest first, until `want` bytes are
/// freed or none is left, keeping the rows of the `keep` buckets in memory. Returns the
/// memory freed.
fn spill_held_batches(
    store: &mut BucketStore,
    spill: &mut SpillGroups,
    want: usize,
    keep: &std::ops::Range<usize>,
    end: usize,
) -> Result<usize> {
    let mut freed = 0;
    for index in 0..end.min(store.held.len()) {
        if freed >= want {
            break;
        }
        freed += store.spill_held(index, spill, keep)?;
    }
    // The rows left buffered hold the batches spilled until they are written
    spill.write_buffered()?;
    Ok(freed)
}

/// Spills every held batch of `store`
fn spill_all_held(store: &mut BucketStore, spill: &mut SpillGroups) -> Result<()> {
    let end = store.held.len();
    spill_held_batches(store, spill, usize::MAX, &(0..0), end)?;
    Ok(())
}

// ============================================================================
// Sorting a bucket
// ============================================================================

/// Rows of a bucket, or of a part of one: the bucket's pieces, and which of their rows
/// when not all of them
#[derive(Clone)]
struct RowSet {
    /// The pieces, and whether each covers its whole held batch
    slices: Arc<Vec<(RecordBatch, bool)>>,
    /// (slice, row) of the rows, in their order; `None` is every row of every slice
    selection: Option<Vec<(usize, usize)>>,
    rows: usize,
    /// Estimated memory of the rows
    bytes: usize,
}

impl RowSet {
    fn all(slices: Vec<(RecordBatch, bool)>, bytes: usize) -> Self {
        let rows = slices.iter().map(|(b, _)| b.num_rows()).sum();
        Self {
            slices: Arc::new(slices),
            selection: None,
            rows,
            bytes,
        }
    }

    /// Removes the first `n` rows and returns them
    fn split_front(&mut self, n: usize) -> RowSet {
        let bytes = self.bytes * n / self.rows.max(1);
        self.bytes -= bytes;
        self.rows -= n;
        match &mut self.selection {
            Some(selection) => {
                let tail = selection.split_off(n);
                RowSet {
                    slices: Arc::clone(&self.slices),
                    selection: Some(std::mem::replace(selection, tail)),
                    rows: n,
                    bytes,
                }
            }
            None => {
                let mut head = vec![];
                let mut tail = vec![];
                let mut left = n;
                for (batch, whole) in self.slices.iter() {
                    if left == 0 {
                        tail.push((batch.clone(), *whole));
                    } else if batch.num_rows() <= left {
                        left -= batch.num_rows();
                        head.push((batch.clone(), *whole));
                    } else {
                        head.push((batch.slice(0, left), false));
                        tail.push((batch.slice(left, batch.num_rows() - left), false));
                        left = 0;
                    }
                }
                self.slices = Arc::new(tail);
                RowSet::all(head, bytes)
            }
        }
    }

    /// Calls `f` with the (slice, row) of every row, in order
    fn for_each(&self, mut f: impl FnMut(usize, usize)) {
        match &self.selection {
            Some(selection) => selection.iter().for_each(|(slice, row)| f(*slice, *row)),
            None => {
                for (slice, (batch, _)) in self.slices.iter().enumerate() {
                    for row in 0..batch.num_rows() {
                        f(slice, row);
                    }
                }
            }
        }
    }
}

/// Sorted rows not copied out yet. The rows of consecutive buckets share output batches
/// of `batch_size` rows, so a small bucket does not emit a small batch, and every
/// output row is copied once, a batch at a time, whatever the size of its bucket.
#[derive(Default)]
struct Output {
    pending: VecDeque<RowSet>,
    /// Rows in `pending`, and their estimated memory
    rows: usize,
    bytes: usize,
    /// Rows pushed and copied out so far
    pushed: usize,
    emitted: usize,
    /// The pieces of the buckets pushed, with the rows pushed once each was: they stay
    /// held, and reserved, until their last row is copied out
    unreleased: VecDeque<(usize, Vec<Piece>)>,
}

impl Output {
    /// Adds the sorted rows of a bucket made of `pieces`
    fn push(&mut self, sets: Vec<RowSet>, pieces: Vec<Piece>) {
        for set in sets {
            if set.rows > 0 {
                self.rows += set.rows;
                self.bytes += set.bytes;
                self.pushed += set.rows;
                self.pending.push_back(set);
            }
        }
        self.unreleased.push_back((self.pushed, pieces));
    }

    /// Copies the pending rows, fewer than a batch, into a batch of their own and
    /// releases every piece they pointed into, so the held batches can be spilled
    /// while the next buckets fill the rest of the output batch
    fn detach(&mut self, ctx: &BucketContext, store: &mut BucketStore) -> Result<()> {
        if self.rows > 0 {
            let sets: Vec<RowSet> = self.pending.drain(..).collect();
            let batch = copy_row_sets(&ctx.schema, &sets)?;
            let batch = if sets.len() == 1 && sets[0].selection.is_none() {
                // A whole held batch passed on as it is would keep it alive unaccounted
                deep_copy_batch(&batch, 0, batch.num_rows())
            } else {
                batch
            };
            self.pending
                .push_back(RowSet::all(vec![(batch, false)], self.bytes));
        }
        for (_, pieces) in self.unreleased.drain(..) {
            store.release(&pieces);
        }
        Ok(())
    }

    /// Releases the pieces of the buckets whose rows are all copied out
    fn release(&mut self, store: &mut BucketStore) {
        while let Some((end, _)) = self.unreleased.front()
            && *end <= self.emitted
        {
            let (_, pieces) = self.unreleased.pop_front().unwrap();
            store.release(&pieces);
        }
    }

    /// The next `batch_size` rows copied out, or the rows left when `last`. A batch
    /// takes no more than `max_bytes` (one row at least), the largest input batch, so
    /// wide rows do not make output batches larger than the input ones.
    fn next_batch(
        &mut self,
        ctx: &BucketContext,
        last: bool,
        max_bytes: usize,
    ) -> Result<Option<RecordBatch>> {
        let full = self.rows >= ctx.batch_size || self.bytes >= max_bytes;
        if self.rows == 0 || !(last || full) {
            return Ok(None);
        }
        let mut need = 0;
        let mut bytes = 0;
        for set in &self.pending {
            let row_bytes = set.bytes.div_ceil(set.rows).max(1);
            let fit = (max_bytes.saturating_sub(bytes) / row_bytes)
                .min(ctx.batch_size - need)
                .min(set.rows);
            need += fit;
            bytes += fit * row_bytes;
            if fit < set.rows {
                break;
            }
        }
        let mut need = need.max(1);
        self.rows -= need;
        self.emitted += need;
        let mut sets = vec![];
        while need > 0 {
            let front = self.pending.front_mut().expect("pending rows");
            if front.rows <= need {
                need -= front.rows;
                sets.push(self.pending.pop_front().unwrap());
            } else {
                sets.push(front.split_front(need));
                need = 0;
            }
        }
        self.bytes = self
            .bytes
            .saturating_sub(sets.iter().map(|set| set.bytes).sum());
        ctx.copy_batch(&sets).map(Some)
    }
}

/// The rows of `sets` copied, in their order, into one batch: one concatenation of
/// slices, or one gather when some rows are a selection. A whole held batch alone is
/// passed on without a copy.
fn copy_row_sets(schema: &SchemaRef, sets: &[RowSet]) -> Result<RecordBatch> {
    if sets.iter().all(|set| set.selection.is_none()) {
        let mut slices: Vec<(RecordBatch, bool)> = sets
            .iter()
            .flat_map(|set| set.slices.iter().cloned())
            .collect();
        if slices.len() == 1 {
            let (batch, whole) = slices.pop().unwrap();
            if whole {
                return Ok(batch);
            }
            return Ok(deep_copy_batch(&batch, 0, batch.num_rows()));
        }
        let batches: Vec<RecordBatch> = slices.into_iter().map(|(b, _)| b).collect();
        return Ok(concat_batches(schema, &batches)?);
    }
    let mut sources: Vec<&RecordBatch> = vec![];
    let mut rows = Vec::with_capacity(sets.iter().map(|set| set.rows).sum());
    for set in sets {
        let base = sources.len();
        sources.extend(set.slices.iter().map(|(b, _)| b));
        set.for_each(|slice, row| rows.push((base + slice, row)));
    }
    Ok(interleave_record_batch(&sources, &rows)?)
}

/// Everything needed to sort buckets, shared by the insert and the output phases
#[derive(Clone)]
struct BucketContext {
    expr: LexOrdering,
    /// `suffixes[d]` sorts by the sort keys from `d` on, for rows constant on the
    /// keys before `d`
    suffixes: Vec<LexOrdering>,
    /// Encoder of the keys rows are scattered by, `encoders[0]`
    encoder: KeyEncoder,
    /// Encoder of every sort key level, `None` when its type is not supported
    encoders: Vec<Option<KeyEncoder>>,
    batch_size: usize,
    target_bucket_bytes: usize,
    /// The schema of the held batches
    schema: SchemaRef,
    /// Arriving batches are not reordered: rows are large, so each is copied once, when
    /// its bucket is sorted, and buckets are not split (only their keys are sorted)
    gather: bool,
    /// Share of the memory of the rows taken by their sort key values, measured on
    /// the first rows: sorting a bucket copies its keys, not its rows
    key_fraction: f64,
    metrics: BucketSortMetrics,
}

impl BucketContext {
    /// Memory sorting `bucket` takes next to its rows: its sort key values, copied
    /// together, and the row numbers and indices of [`SORT_ROW_BYTES`]
    fn sort_room(&self, bucket: &Bucket) -> usize {
        let keys = (bucket.bytes as f64 * self.key_fraction) as usize;
        keys.saturating_add(bucket.rows.saturating_mul(SORT_ROW_BYTES))
    }

    /// Bytes of the sort keys of a row of `set` that sorting it from `level` works on:
    /// the packed key when it orders every key exactly, else the measured size of the
    /// key values (strings take more than the packed key). The rows themselves are not
    /// moved by the sort, only gathered once in sorted order, so their size does not
    /// count.
    fn key_row_bytes(&self, level: usize, set: &RowSet) -> usize {
        let packed = self.encoders[level]
            .as_ref()
            .is_some_and(|e| e.packs_exactly(self.expr.len(), level));
        if packed {
            return KEY_ROW_BYTES;
        }
        let keys = set.bytes as f64 * self.key_fraction / set.rows.max(1) as f64;
        (keys as usize).max(KEY_ROW_BYTES)
    }

    /// Equal scatter keys mean equal rows in the sort order
    fn exact_key(&self) -> bool {
        self.encoder.exact_key(self.expr.len(), 0)
    }

    /// Copies the rows of `sets` out, in their order, into one batch. A whole held batch
    /// alone in it is passed on without a copy.
    fn copy_batch(&self, sets: &[RowSet]) -> Result<RecordBatch> {
        if sets.iter().any(|set| set.selection.is_some()) {
            let _timer = self.metrics.gather_time.timer();
            return copy_row_sets(&self.schema, sets);
        }
        let whole = sets.len() == 1 && sets[0].slices.len() == 1 && sets[0].slices[0].1;
        if whole {
            self.metrics.zero_copy_batches.add(1);
        }
        copy_row_sets(&self.schema, sets)
    }

    /// Sorts the rows by `by`. Only the sort key columns are concatenated and sorted;
    /// the rows are returned in sorted order, to be copied out once by [`Output`].
    fn sort(&self, set: RowSet, level: usize) -> Result<RowSet> {
        self.metrics.sorted_buckets.add(1);
        let by = &self.suffixes[level];
        let columns: Vec<SortColumn> = {
            let _timer = self.metrics.sort_columns_time.timer();
            by.iter()
                .map(|key| {
                    let arrays = set
                        .slices
                        .iter()
                        .map(|(batch, _)| {
                            key.expr.evaluate(batch)?.into_array(batch.num_rows())
                        })
                        .collect::<Result<Vec<ArrayRef>>>()?;
                    let refs: Vec<&dyn Array> =
                        arrays.iter().map(|a| a.as_ref()).collect();
                    let values = match &set.selection {
                        None if arrays.len() == 1 => Arc::clone(&arrays[0]),
                        None => concat(&refs)?,
                        Some(selection) => interleave(&refs, selection)?,
                    };
                    Ok(SortColumn {
                        values,
                        options: Some(key.options),
                    })
                })
                .collect::<Result<_>>()?
        };
        let order = {
            let _timer = self.metrics.sort_indices_time.timer();
            match self.packed_order(level, &columns)? {
                Some(order) => order,
                None => lexsort_to_indices(&columns, None)?,
            }
        };
        drop(columns);

        if order
            .values()
            .iter()
            .enumerate()
            .all(|(i, o)| *o as usize == i)
        {
            return Ok(set);
        }
        let rows: Vec<(usize, usize)> = match &set.selection {
            Some(selection) => order
                .values()
                .iter()
                .map(|o| selection[*o as usize])
                .collect(),
            None => {
                let mut all = Vec::with_capacity(set.rows);
                set.for_each(|slice, row| all.push((slice, row)));
                order.values().iter().map(|o| all[*o as usize]).collect()
            }
        };
        Ok(RowSet {
            slices: set.slices,
            selection: Some(rows),
            rows: set.rows,
            bytes: set.bytes,
        })
    }

    /// The order of rows by the sort keys from `level`, from their packed keys alone
    /// when those order every key exactly, see [`KeyEncoder::exact_keys`]: no
    /// comparison of the key columns
    fn packed_order(
        &self,
        level: usize,
        columns: &[SortColumn],
    ) -> Result<Option<UInt32Array>> {
        let Some(encoder) = &self.encoders[level] else {
            return Ok(None);
        };
        if !encoder.packs_exactly(self.expr.len(), level) {
            return Ok(None);
        }
        let Some(keys) = encoder.exact_keys(columns)? else {
            return Ok(None);
        };
        // Equal keys are equal rows, so their order does not matter
        let mut order: Vec<(u128, u32)> = keys.into_iter().zip(0u32..).collect();
        order.sort_unstable_by_key(|(key, _)| *key);
        self.metrics.packed_key_sorts.add(1);
        Ok(Some(order.into_iter().map(|(_, row)| row).collect()))
    }

    /// Sorts the rows of one bucket of `store`, returning them in sorted order
    fn sort_bucket(
        &self,
        bucket: &Bucket,
        store: &BucketStore,
        is_null_bucket: bool,
    ) -> Result<Vec<RowSet>> {
        let _timer = self.metrics.bucket_time.timer();
        if is_null_bucket || self.encoder.level_constant(bucket.min, bucket.max) {
            self.metrics.leading_key_constant.add(1);
            return self.sort_from(1, store.row_set(&bucket.pieces, bucket.bytes), 0);
        }

        if bucket.pieces.iter().all(|p| p.sorted && p.rows.is_none()) {
            return self.emit_sorted_pieces(bucket, &store.slices(&bucket.pieces));
        }

        self.sort_from(0, store.row_set(&bucket.pieces, bucket.bytes), 0)
    }

    /// Sorts rows that are equal on every sort key before `level`. Rows whose keys are
    /// too large for the target are split by the key at `level` first, `splits` times at
    /// most before moving on to the next key.
    fn sort_from(&self, level: usize, set: RowSet, splits: usize) -> Result<Vec<RowSet>> {
        if level == self.expr.len() {
            self.metrics.constant_buckets.add(1);
            return Ok(vec![set]);
        }
        if !self.gather
            && set.rows.saturating_mul(self.key_row_bytes(level, &set))
                > 2 * self.target_bucket_bytes
            && splits < MAX_SPLIT_DEPTH
            && let Some(encoder) = &self.encoders[level]
            && let Some(sorted) = self.split_level(level, encoder, &set, splits)?
        {
            return Ok(sorted);
        }
        Ok(vec![self.sort(set, level)?])
    }

    /// Every piece is sorted: the zone maps split them into runs of overlapping
    /// pieces, a run of one piece is copied out as is and only the others are sorted
    fn emit_sorted_pieces(
        &self,
        bucket: &Bucket,
        slices: &[(RecordBatch, bool)],
    ) -> Result<Vec<RowSet>> {
        let mut order: Vec<usize> = (0..bucket.pieces.len()).collect();
        order.sort_by_key(|i| (bucket.pieces[*i].min, bucket.pieces[*i].max));

        // Equal keys of different pieces are ordered only by an exact key
        let exact_single = self.exact_key();
        let overlaps = |max: u128, next_min: u128| {
            if exact_single {
                next_min < max
            } else {
                next_min <= max
            }
        };

        let bytes_of =
            |i: &usize| bucket.bytes * bucket.pieces[*i].len / bucket.rows.max(1);
        let mut out = vec![];
        let mut run: Vec<usize> = vec![];
        let mut run_max = 0u128;
        let flush = |run: &mut Vec<usize>, out: &mut Vec<RowSet>| -> Result<()> {
            self.metrics.zone_map_components.add(1);
            if run.len() == 1 {
                self.metrics.zone_map_presorted_pieces.add(1);
                out.push(RowSet::all(vec![slices[run[0]].clone()], bytes_of(&run[0])));
            } else {
                let parts: Vec<(RecordBatch, bool)> =
                    run.iter().map(|i| slices[*i].clone()).collect();
                let bytes = run.iter().map(bytes_of).sum();
                out.push(self.sort(RowSet::all(parts, bytes), 0)?);
            }
            run.clear();
            Ok(())
        };
        for i in order {
            let piece = &bucket.pieces[i];
            if !run.is_empty() && !overlaps(run_max, piece.min) {
                flush(&mut run, &mut out)?;
            }
            if run.is_empty() {
                run_max = piece.max;
            } else {
                run_max = run_max.max(piece.max);
            }
            run.push(i);
        }
        if !run.is_empty() {
            flush(&mut run, &mut out)?;
        }
        Ok(out)
    }

    /// Whether every non null value of the key at `encoder`'s level is the same in `set`
    fn values_equal(&self, encoder: &KeyEncoder, set: &RowSet) -> Result<bool> {
        let key = encoder.first();
        let arrays = set
            .slices
            .iter()
            .map(|(batch, _)| key.expr.evaluate(batch)?.into_array(batch.num_rows()))
            .collect::<Result<Vec<ArrayRef>>>()?;
        let mut first: Option<ArrayRef> = None;
        let mut comparators: Vec<Option<DynComparator>> =
            arrays.iter().map(|_| None).collect();
        let mut equal = true;
        let mut error = None;
        set.for_each(|slice, row| {
            let array = &arrays[slice];
            if !equal || error.is_some() || array.is_null(row) {
                return;
            }
            let first = first.get_or_insert_with(|| array.slice(row, 1));
            if comparators[slice].is_none() {
                match make_comparator(first.as_ref(), array.as_ref(), key.options) {
                    Ok(c) => comparators[slice] = Some(c),
                    Err(e) => {
                        error = Some(e);
                        return;
                    }
                }
            }
            equal = comparators[slice].as_ref().unwrap()(0, row) == Ordering::Equal;
        });
        match error {
            Some(e) => Err(e.into()),
            None => Ok(equal),
        }
    }

    /// Splits rows equal on the keys before `level` by quantiles of their keys at
    /// `level`, and sorts every part on its own. A part keeps the rows it holds as a
    /// selection, so splitting copies no rows. Rows constant on the key at `level` go on
    /// to the next key. Returns `None` when the keys cannot split the rows.
    fn split_level(
        &self,
        level: usize,
        encoder: &KeyEncoder,
        set: &RowSet,
        splits: usize,
    ) -> Result<Option<Vec<RowSet>>> {
        let keys: Vec<Keys> = set
            .slices
            .iter()
            .map(|(batch, _)| encoder.encode(batch))
            .collect::<Result<_>>()?;

        let mut min = u128::MAX;
        let mut max = 0u128;
        let mut nulls = 0;
        set.for_each(|slice, row| {
            let k = &keys[slice];
            if k.is_null(row) {
                nulls += 1;
            } else {
                min = min.min(k.values[row]);
                max = max.max(k.values[row]);
            }
        });
        let constant = nulls == set.rows
            || (nulls == 0
                && (encoder.level_constant(min, max)
                    || (min == max && self.values_equal(encoder, set)?)));
        if constant {
            self.metrics.leading_key_constant.add(1);
            return self.sort_from(level + 1, set.clone(), 0).map(Some);
        }
        // Every value shares the bytes encoded: split by the bytes after them
        if nulls == 0 && min == max {
            return match encoder.next_bytes() {
                Some(deeper) => self.split_level(level, &deeper, set, splits),
                None => Ok(None),
            };
        }

        let parts = set
            .rows
            .saturating_mul(self.key_row_bytes(level, set))
            .div_ceil(self.target_bucket_bytes.max(1))
            .clamp(2, MAX_SPLIT);
        let mut values: Vec<u128> = Vec::with_capacity(set.rows - nulls);
        set.for_each(|slice, row| {
            let k = &keys[slice];
            if !k.is_null(row) {
                values.push(k.values[row]);
            }
        });
        let splitters = exact_quantile_splitters(&mut values, parts - 1);
        drop(values);
        let sub = Boundaries {
            splitters,
            nulls_first: encoder.options().nulls_first,
        };
        let num_parts = sub.num_buckets();
        let null_part = sub.null_bucket();

        let mut part_rows: Vec<Vec<(usize, usize)>> = vec![vec![]; num_parts];
        let mut mins = vec![u128::MAX; num_parts];
        let mut maxs = vec![0u128; num_parts];
        set.for_each(|slice, row| {
            let k = &keys[slice];
            let part = sub.bucket_of(k, row);
            part_rows[part].push((slice, row));
            if part != null_part {
                mins[part] = mins[part].min(k.values[row]);
                maxs[part] = maxs[part].max(k.values[row]);
            }
        });
        drop(keys);
        // All the rows in one part that is not constant: the keys cannot split them
        if let Some(part) = part_rows.iter().position(|r| r.len() == set.rows)
            && part != null_part
            && !encoder.level_constant(mins[part], maxs[part])
        {
            return Ok(None);
        }
        self.metrics.split_buckets.add(1);

        let mut out = vec![];
        for (part, rows) in part_rows.into_iter().enumerate() {
            if rows.is_empty() {
                continue;
            }
            let len = rows.len();
            let part_set = RowSet {
                slices: Arc::clone(&set.slices),
                selection: Some(rows),
                rows: len,
                bytes: set.bytes * len / set.rows,
            };
            if part == null_part || encoder.level_constant(mins[part], maxs[part]) {
                self.metrics.leading_key_constant.add(1);
                out.extend(self.sort_from(level + 1, part_set, 0)?);
            } else {
                out.extend(self.sort_from(level, part_set, splits + 1)?);
            }
        }
        Ok(Some(out))
    }
}

// ============================================================================
// Spilling
// ============================================================================

/// The spill files of a range of buckets. Rows are appended unsorted, so spilling
/// does no sorting and reading back needs no merge.
struct SpillGroup {
    buckets: std::ops::Range<usize>,
    /// Pieces not written yet, with their rows and estimated memory: the pieces of a
    /// group are written together, as batches of `batch_size` rows, rather than each
    /// as a small batch, which compresses poorly and is slow to write and read
    buffered: Vec<RowSet>,
    buffered_rows: usize,
    buffered_bytes: usize,
    /// The file being appended to, and the largest batch memory in it
    file: Option<(InProgressSpillFile, usize)>,
    /// Finished files not read yet, with their largest batch memory
    files: Vec<(Arc<dyn SpillFile>, usize)>,
    /// Memory of the rows on disk, to reserve when loading them back
    memory: usize,
}

/// The spill files of every bucket, by groups of neighbouring buckets
struct SpillGroups {
    groups: Vec<SpillGroup>,
    /// The group of every bucket
    group_of: Vec<usize>,
    spill_manager: SpillManager,
    batch_size: usize,
    metrics: BucketSortMetrics,
    /// The schema of the held batches
    schema: SchemaRef,
}

impl SpillGroups {
    fn new(
        num_buckets: usize,
        spill_manager: SpillManager,
        batch_size: usize,
        metrics: BucketSortMetrics,
    ) -> Self {
        let count = num_buckets.clamp(1, MAX_SPILL_GROUPS);
        let groups: Vec<SpillGroup> = (0..count)
            .map(|g| SpillGroup {
                buckets: (g * num_buckets / count)..((g + 1) * num_buckets / count),
                buffered: vec![],
                buffered_rows: 0,
                buffered_bytes: 0,
                file: None,
                files: vec![],
                memory: 0,
            })
            .collect();
        let mut group_of = vec![0; num_buckets];
        for (g, group) in groups.iter().enumerate() {
            group_of[group.buckets.clone()].fill(g);
        }
        Self {
            groups,
            group_of,
            schema: Arc::clone(spill_manager.schema()),
            spill_manager,
            batch_size,
            metrics,
        }
    }

    /// Adds rows of `bucket`, written once the group has `batch_size` rows or
    /// `max_bytes` buffered
    fn append(&mut self, bucket: usize, rows: RowSet, max_bytes: usize) -> Result<()> {
        let g = self.group_of[bucket];
        let group = &mut self.groups[g];
        group.buffered_rows += rows.rows;
        group.buffered_bytes += rows.bytes;
        group.buffered.push(rows);
        if group.buffered_rows >= self.batch_size || group.buffered_bytes >= max_bytes {
            self.write(g)?;
        }
        Ok(())
    }

    /// Writes the rows buffered for group `g` to its file, as one batch
    fn write(&mut self, g: usize) -> Result<()> {
        let group = &mut self.groups[g];
        if group.buffered.is_empty() {
            return Ok(());
        }
        let _timer = self.metrics.spill_write_time.timer();
        // A single slice is written as it is: writing it copies its rows anyway
        let batch = match group.buffered.as_slice() {
            [set] if set.selection.is_none() && set.slices.len() == 1 => {
                set.slices[0].0.clone()
            }
            sets => copy_row_sets(&self.schema, sets)?,
        };
        group.buffered.clear();
        group.buffered_rows = 0;
        group.buffered_bytes = 0;
        if group.file.is_none() {
            group.file =
                Some((self.spill_manager.create_in_progress_file("Sorting")?, 0));
        }
        let (file, max_batch_memory) = group.file.as_mut().unwrap();
        let memory = file.append_batch(&batch)?;
        self.metrics.spill_batches.add(1);
        group.memory += memory;
        *max_batch_memory = (*max_batch_memory).max(memory);
        Ok(())
    }

    /// Writes the rows buffered for every group
    fn write_buffered(&mut self) -> Result<()> {
        for g in 0..self.groups.len() {
            self.write(g)?;
        }
        Ok(())
    }

    /// Whether group `g` has rows on disk
    fn has_rows(&self, g: usize) -> bool {
        let group = &self.groups[g];
        group.file.is_some() || !group.files.is_empty() || !group.buffered.is_empty()
    }

    /// The rows of group `g` on disk, read one file after the other in compact batches
    /// of `batch_size` rows and `max_bytes` at most; they are no longer on disk for the
    /// group after this
    fn read(
        &mut self,
        g: usize,
        schema: &SchemaRef,
        max_bytes: usize,
    ) -> Result<SendableRecordBatchStream> {
        self.write(g)?;
        let batch_size = self.batch_size;
        let group = &mut self.groups[g];
        if let Some((mut file, max_batch_memory)) = group.file.take()
            && let Some(file) = file.finish()?
        {
            group.files.push((file, max_batch_memory));
        }
        group.memory = 0;
        let streams = std::mem::take(&mut group.files)
            .into_iter()
            .map(|(file, max_batch_memory)| {
                self.spill_manager
                    .read_spill_as_stream_unbuffered(file, Some(max_batch_memory))
            })
            .collect::<Result<Vec<_>>>()?;
        let input = futures::stream::iter(streams).flatten();
        let read_time = self.metrics.spill_read_time.clone();
        Ok(compacted(
            Arc::clone(schema),
            input,
            batch_size,
            max_bytes,
            read_time,
        ))
    }
}

/// `input` in compact batches of `batch_size` rows, or of `max_bytes` when that comes
/// first. The batches read from a spill file can share the buffer they were decoded
/// from, each holding all of it for its rows: the copy keeps only their rows. A batch
/// that fills an output batch alone and holds little more than its rows is passed on
/// as it is.
fn compacted(
    schema: SchemaRef,
    input: impl futures::Stream<Item = Result<RecordBatch>> + Send + 'static,
    batch_size: usize,
    max_bytes: usize,
    read_time: Time,
) -> SendableRecordBatchStream {
    let output_schema = Arc::clone(&schema);
    Box::pin(RecordBatchStreamAdapter::new(
        output_schema,
        async_try_stream(move |mut emitter| async move {
            let mut input = Box::pin(input);
            let mut buffered: Vec<RecordBatch> = vec![];
            let mut rows = 0;
            let mut bytes = 0;
            let copy = |buffered: &mut Vec<RecordBatch>| -> Result<RecordBatch> {
                let batch = if buffered.len() == 1 {
                    let batch = buffered.pop().unwrap();
                    let memory = get_record_batch_memory_size(&batch);
                    if memory <= batch.get_sliced_size()? / 4 * 5 + 4096 {
                        batch
                    } else {
                        deep_copy_batch(&batch, 0, batch.num_rows())
                    }
                } else {
                    concat_batches(&schema, buffered.iter())?
                };
                buffered.clear();
                Ok(batch)
            };
            loop {
                let next = {
                    let _timer = read_time.timer();
                    input.next().await
                };
                let Some(batch) = next else {
                    break;
                };
                let batch = batch?;
                rows += batch.num_rows();
                bytes += get_record_batch_memory_size(&batch);
                buffered.push(batch);
                if rows >= batch_size || bytes >= max_bytes {
                    rows = 0;
                    bytes = 0;
                    emitter.emit(copy(&mut buffered)?).await;
                }
            }
            if rows > 0 {
                emitter.emit(copy(&mut buffered)?).await;
            }
            Ok(())
        }),
    ))
}

// ============================================================================
// Bucket sorter
// ============================================================================

/// A buffered batch, before the boundaries are chosen
struct Pending {
    batch: RecordBatch,
    reserved: usize,
    sorted: bool,
}

/// The bucket sort state of an `ExternalSorter`
pub(crate) struct BucketSorter {
    /// Input and output schema
    schema: SchemaRef,
    options: BucketSortOptions,
    ctx: BucketContext,
    runtime: Arc<RuntimeEnv>,
    spill_manager: SpillManager,

    // Before the boundaries are chosen
    reservoir: Reservoir,
    hll: HyperLogLog,
    heavy: MisraGries,
    /// Rows added to `hll` and `heavy`
    sketched_rows: usize,
    /// Distinct values of every sort key level, until there are too many
    distinct_values: Vec<Option<DistinctValues>>,
    /// Every row so far is in sort order, batches included
    presorted: bool,
    /// Every key so far is no greater than the one before it, and at least one is
    /// smaller, by the rank of `last_key`: the input arrives in reverse order, so
    /// boundaries chosen from its first rows would hold none of the next ones
    reversed: bool,
    decreased: bool,
    last_key: Option<(bool, u128)>,
    /// Batches are held in buffers of their own, see [`Self::insert`]
    compact_shared: bool,
    /// Sort columns of the last row so far, to check the next batch continues it
    last_row: Option<Vec<ArrayRef>>,
    pending: Vec<Pending>,
    pending_rows: usize,
    pending_reserved: usize,
    /// Spilled presorted batches, in order
    sorted_runs: Vec<SortedSpillFile>,

    // After
    boundaries: Option<Boundaries>,
    store: BucketStore,
    /// The spill files of the buckets, once anything was spilled
    spill: Option<SpillGroups>,
    /// The mode was chosen, see [`BucketContext::gather`]
    mode_decided: bool,
    /// Rows, memory and sort key memory of the batches measured for
    /// [`BucketContext::key_fraction`]
    measured_rows: usize,
    measured_bytes: usize,
    measured_key_bytes: usize,
}

impl BucketSorter {
    /// Returns `None` when the first sort key type is not supported
    pub(crate) fn try_new(
        schema: SchemaRef,
        expr: LexOrdering,
        batch_size: usize,
        options: BucketSortOptions,
        metrics: BucketSortMetrics,
        runtime: Arc<RuntimeEnv>,
        spill_manager: SpillManager,
    ) -> Result<Option<Self>> {
        let Some(encoder) = KeyEncoder::try_new(&expr, 0, &schema, &[])? else {
            return Ok(None);
        };
        let distinct_values = expr
            .iter()
            .map(|key| Ok(DistinctValues::new(&key.expr.data_type(&schema)?)))
            .collect::<Result<Vec<_>>>()?;

        let encoders = (0..expr.len())
            .map(|level| KeyEncoder::try_new(&expr, level, &schema, &[]))
            .collect::<Result<Vec<_>>>()?;
        let suffixes = (0..expr.len())
            .map(|level| {
                LexOrdering::new(expr.iter().skip(level).cloned())
                    .expect("a suffix of the sort keys is not empty")
            })
            .collect();
        let bucket_count = options.bucket_count.clamp(3, MAX_BUCKETS);
        let compact_shared = !schema
            .fields()
            .iter()
            .any(|field| sorted_copy_keeps_buffers(field.data_type()));
        let sorter = Self {
            ctx: BucketContext {
                expr,
                suffixes,
                encoder,
                encoders,
                batch_size,
                target_bucket_bytes: options.target_bucket_bytes.max(1),
                schema: Arc::clone(&schema),
                gather: false,
                key_fraction: 1.0,
                metrics,
            },
            schema,
            options: BucketSortOptions {
                bucket_count,
                ..options
            },
            runtime,
            spill_manager,
            reservoir: Reservoir::new(RESERVOIR_CAPACITY),
            hll: HyperLogLog::new(),
            heavy: MisraGries::new(2 * bucket_count),
            sketched_rows: 0,
            distinct_values,
            presorted: true,
            reversed: true,
            decreased: false,
            last_key: None,
            compact_shared,
            last_row: None,
            pending: vec![],
            pending_rows: 0,
            pending_reserved: 0,
            sorted_runs: vec![],
            boundaries: None,
            store: BucketStore::default(),
            spill: None,
            mode_decided: false,
            measured_rows: 0,
            measured_bytes: 0,
            measured_key_bytes: 0,
        };
        sorter.record_key_layout();
        Ok(Some(sorter))
    }

    /// Bytes the held data needs reserved
    pub(crate) fn reserved(&self) -> usize {
        self.pending_reserved + self.store.reserved
    }

    pub(crate) fn has_in_memory_data(&self) -> bool {
        !self.pending.is_empty() || !self.store.is_empty()
    }

    fn spilled(&self) -> bool {
        !self.sorted_runs.is_empty() || self.spill.is_some()
    }

    /// Whether the input so far was in sort order and is buffered as is
    pub(crate) fn presorted_without_boundaries(&self) -> bool {
        self.presorted && self.boundaries.is_none() && self.spill.is_none()
    }

    /// No boundaries were chosen and nothing was spilled: the merge sort can take
    /// the buffered batches
    /// Memory to reserve before inserting `batch`: twice the size of the rows it holds,
    /// for the copy that scatters or compacts them
    pub(crate) fn reservation_for(&self, batch: &RecordBatch) -> Result<usize> {
        let sliced = batch.get_sliced_size()?;
        if self.compact_shared && get_record_batch_memory_size(batch) > sliced * 2 {
            return Ok(sliced * 2);
        }
        get_reserved_bytes_for_record_batch(batch)
    }

    /// Tracks whether every key so far is in reverse sort order
    fn check_reversed(&mut self, keys: &Keys) {
        if !self.reversed {
            return;
        }
        let nulls_first = self.ctx.encoder.options().nulls_first;
        for row in 0..keys.len() {
            let rank = if keys.is_null(row) {
                (!nulls_first, 0)
            } else {
                (nulls_first, keys.values[row])
            };
            if let Some(last) = self.last_key {
                if rank > last {
                    self.reversed = false;
                    self.last_key = None;
                    return;
                }
                self.decreased |= rank < last;
            }
            self.last_key = Some(rank);
        }
    }

    /// The input so far arrives in reverse sort order and the boundaries are not
    /// chosen yet
    fn reversed_input(&self) -> bool {
        self.boundaries.is_none() && self.reversed && self.decreased
    }

    /// The buffered batches, when the input so far arrives in reverse sort order: the
    /// boundaries would come from its first rows only, and every row after them would
    /// land in one bucket, so the merge sort takes over when memory runs out. Their
    /// reservation is kept by the caller.
    pub(crate) fn take_reversed_input(&mut self) -> Option<Vec<RecordBatch>> {
        if !self.reversed_input() || self.spilled() {
            return None;
        }
        self.pending_rows = 0;
        self.pending_reserved = 0;
        Some(self.pending.drain(..).map(|p| p.batch).collect())
    }

    pub(crate) fn take_small_input(
        &mut self,
        threshold: usize,
    ) -> Option<Vec<RecordBatch>> {
        if self.boundaries.is_some()
            || self.spilled()
            || self.pending_reserved >= threshold
        {
            return None;
        }
        self.pending_rows = 0;
        self.pending_reserved = 0;
        Some(self.pending.drain(..).map(|p| p.batch).collect())
    }

    /// Whether the rows of `batch` with `keys` are in sort order, and continue the
    /// rows seen before
    fn check_sorted(&mut self, batch: &RecordBatch, keys: &Keys) -> Result<bool> {
        let n = batch.num_rows();
        let nulls_first = self.ctx.encoder.options().nulls_first;
        // Nulls rank before or after every key
        let rank = |row: usize| -> (bool, u128) {
            if keys.is_null(row) {
                (!nulls_first, 0)
            } else {
                (nulls_first, keys.values[row])
            }
        };
        let exact_single = self.ctx.exact_key();

        let mut comparator: Option<LexicographicalComparator> = None;
        let mut sort_columns: Option<Vec<SortColumn>> = None;
        let mut sorted = true;
        let mut previous = rank(0);
        for row in 1..n {
            let current = rank(row);
            match previous.cmp(&current) {
                Ordering::Less => {}
                Ordering::Greater => {
                    sorted = false;
                    break;
                }
                Ordering::Equal => {
                    if exact_single {
                        previous = current;
                        continue;
                    }
                    if comparator.is_none() {
                        let columns = self.sort_columns(batch)?;
                        comparator = Some(LexicographicalComparator::try_new(&columns)?);
                        sort_columns = Some(columns);
                    }
                    if comparator.as_ref().unwrap().compare(row - 1, row)
                        == Ordering::Greater
                    {
                        sorted = false;
                        break;
                    }
                }
            }
            previous = current;
        }

        if self.presorted {
            let first = self.sort_columns(&batch.slice(0, 1))?;
            if sorted
                && let Some(last) = &self.last_row
                && compare_rows(last, &first, &self.ctx.expr)? == Ordering::Greater
            {
                self.presorted = false;
            }
            self.presorted &= sorted;
            if self.presorted {
                self.last_row = Some(match sort_columns {
                    Some(columns) => columns
                        .into_iter()
                        .map(|c| c.values.slice(n - 1, 1))
                        .collect(),
                    None => self
                        .sort_columns(&batch.slice(n - 1, 1))?
                        .into_iter()
                        .map(|c| c.values)
                        .collect(),
                });
            } else {
                self.last_row = None;
            }
        }
        Ok(sorted)
    }

    fn sort_columns(&self, batch: &RecordBatch) -> Result<Vec<SortColumn>> {
        self.ctx
            .expr
            .iter()
            .map(|e| e.evaluate_to_sort_column(batch))
            .collect()
    }

    /// Records how the packed key covers the sort keys
    fn record_key_layout(&self) {
        let metrics = &self.ctx.metrics;
        metrics.key_segments.set(self.ctx.encoder.segments.len());
        metrics
            .exact_key_levels
            .set(self.ctx.encoder.exact_levels());
    }

    /// Measures the share of the batch's memory its sort key values take
    fn measure_keys(&mut self, batch: &RecordBatch) -> Result<()> {
        for key in self.ctx.expr.iter() {
            let values = key.expr.evaluate(batch)?.into_array(batch.num_rows())?;
            self.measured_key_bytes += values.get_array_memory_size();
        }
        self.measured_rows += batch.num_rows();
        self.measured_bytes += get_record_batch_memory_size(batch);
        if self.measured_bytes > 0 {
            self.ctx.key_fraction =
                (self.measured_key_bytes as f64 / self.measured_bytes as f64).min(1.0);
        }
        Ok(())
    }

    /// Adds a batch whose memory the caller reserved. The caller then reconciles the
    /// reservation with [`Self::reserved`].
    pub(crate) fn insert(&mut self, batch: RecordBatch) -> Result<()> {
        // A batch sharing a larger buffer, such as a slice of a bigger batch, would be
        // reserved at the size of that buffer: its rows are copied out instead, so
        // what is held is reserved exactly. Types whose copies keep the buffers they
        // reference (views, dictionaries) are held as they are.
        let batch = if self.compact_shared
            && get_record_batch_memory_size(&batch) > batch.get_sliced_size()? * 2
        {
            deep_copy_batch(&batch, 0, batch.num_rows())
        } else {
            batch
        };
        let stats_time = self.ctx.metrics.stats_time.clone();
        let (keys, sorted) = {
            let _timer = stats_time.timer();
            if self.measured_rows < self.options.sample_rows {
                self.measure_keys(&batch)?;
            }
            let keys = self.ctx.encoder.encode(&batch)?;
            let sorted = self.check_sorted(&batch, &keys)?;
            if self.boundaries.is_none() {
                self.check_reversed(&keys);
                self.reservoir.add(&keys);
                if self.sketched_rows < self.options.sample_rows {
                    for row in 0..keys.len() {
                        if !keys.is_null(row) {
                            self.hll.add(keys.values[row]);
                            self.heavy.add(keys.values[row]);
                        }
                    }
                    for (level, slot) in self.distinct_values.iter_mut().enumerate() {
                        if let Some(values) = slot {
                            let array = self.ctx.expr[level]
                                .expr
                                .evaluate(&batch)?
                                .into_array(batch.num_rows())?;
                            values.add(array.as_ref())?;
                            if values.len() > MAX_DICTIONARY_VALUES {
                                *slot = None;
                            }
                        }
                    }
                    self.sketched_rows += keys.len();
                }
            }
            (keys, sorted)
        };

        if self.boundaries.is_some() {
            self.decide_mode(std::slice::from_ref(&batch))?;
        }
        match &self.boundaries {
            Some(boundaries) => {
                let scattered = scatter(
                    boundaries,
                    batch,
                    &keys,
                    sorted,
                    self.ctx.gather,
                    &self.ctx.metrics,
                )?;
                self.store.add(scattered)?;
            }
            None => {
                let reserved = get_reserved_bytes_for_record_batch(&batch)?;
                self.pending_rows += batch.num_rows();
                self.pending_reserved += reserved;
                self.pending.push(Pending {
                    batch,
                    reserved,
                    sorted,
                });
                // Presorted input keeps being appended, it needs no boundaries, and
                // reversed input keeps being buffered, for the merge sort when memory
                // runs out (see `take_reversed_input`)
                if self.pending_rows >= self.options.sample_rows
                    && !self.presorted
                    && !self.reversed_input()
                {
                    self.choose_boundaries()?;
                }
            }
        }
        Ok(())
    }

    /// Chooses the boundaries from the sketches, and scatters
    /// the buffered batches
    fn choose_boundaries(&mut self) -> Result<()> {
        if self.boundaries.is_some() {
            return Ok(());
        }
        // Keys with few distinct values take dictionary codes, packing more keys. The
        // values must repeat in the sample, or the sample did not see most of them.
        let sketched_rows = self.sketched_rows;
        let dictionaries: Vec<Option<Arc<KeyDictionary>>> =
            std::mem::take(&mut self.distinct_values)
                .into_iter()
                .map(|values| {
                    values
                        .filter(|v| {
                            v.len() > 0
                                && v.len() * MIN_DICTIONARY_REPEATS <= sketched_rows
                        })
                        .map(|v| Arc::new(v.into_dictionary()))
                })
                .collect();
        let with_dictionaries = dictionaries.iter().filter(|d| d.is_some()).count();
        if with_dictionaries > 0 {
            self.ctx.metrics.key_dictionary.add(with_dictionaries);
            let schema = Arc::clone(&self.schema);
            for level in 0..self.ctx.expr.len() {
                self.ctx.encoders[level] =
                    KeyEncoder::try_new(&self.ctx.expr, level, &schema, &dictionaries)?;
            }
            self.ctx.encoder = self.ctx.encoders[0]
                .clone()
                .expect("the first key was supported without a dictionary");
            self.record_key_layout();
            debug!(
                "Bucket sort packs {} key segments with {with_dictionaries} dictionaries",
                self.ctx.encoder.segments.len()
            );
            // The sketches again, in the new key space, from the buffered rows
            self.reservoir = Reservoir::new(RESERVOIR_CAPACITY);
            self.hll = HyperLogLog::new();
            self.heavy = MisraGries::new(2 * self.options.bucket_count);
            for pending in &self.pending {
                let keys = self.ctx.encoder.encode(&pending.batch)?;
                self.reservoir.add(&keys);
                for row in 0..keys.len() {
                    if !keys.is_null(row) {
                        self.hll.add(keys.values[row]);
                        self.heavy.add(keys.values[row]);
                    }
                }
            }
        }
        let metrics = &self.ctx.metrics;
        let distinct = self.hll.estimate();
        metrics.estimated_distinct_keys.add(distinct as usize);
        let choice = choose_boundaries(
            std::mem::take(&mut self.reservoir.sample),
            &self.heavy,
            distinct,
            self.options.bucket_count - 1,
            self.ctx.encoder.options().nulls_first,
        );
        if choice.dictionary {
            metrics.dictionary_path.add(1);
        }
        metrics.heavy_hitters.add(choice.heavy_hitters);
        debug!(
            "Bucket sort chose {} buckets (dictionary: {}, heavy hitters: {}, distinct keys ~{distinct:.0})",
            choice.boundaries.num_buckets(),
            choice.dictionary,
            choice.heavy_hitters
        );

        self.store = BucketStore::new(choice.boundaries.num_buckets());
        self.boundaries = Some(choice.boundaries);
        self.reservoir = Reservoir::new(0);
        self.heavy = MisraGries::new(0);
        self.last_row = None;

        let pending: Vec<RecordBatch> =
            self.pending.iter().map(|p| p.batch.clone()).collect();
        self.decide_mode(&pending)?;
        drop(pending);
        for Pending { batch, sorted, .. } in std::mem::take(&mut self.pending) {
            let keys = {
                let _timer = self.ctx.metrics.stats_time.timer();
                self.ctx.encoder.encode(&batch)?
            };
            let scattered = scatter(
                self.boundaries.as_ref().unwrap(),
                batch,
                &keys,
                sorted,
                self.ctx.gather,
                &self.ctx.metrics,
            )?;
            self.store.add(scattered)?;
        }
        self.pending_rows = 0;
        self.pending_reserved = 0;
        Ok(())
    }

    /// Chooses, once, whether arriving batches are reordered by bucket or kept as they
    /// came (gather mode), from the first batches scattered
    fn decide_mode(&mut self, batches: &[RecordBatch]) -> Result<()> {
        if self.mode_decided || batches.iter().all(|b| b.num_rows() == 0) {
            return Ok(());
        }
        self.mode_decided = true;
        self.ctx.gather = match self.options.mode {
            BucketSortMode::Scatter => false,
            BucketSortMode::Gather => true,
            BucketSortMode::Auto => self.probe_gather(batches)?,
        };
        if self.ctx.gather {
            self.ctx.metrics.gather_mode.add(1);
        }
        Ok(())
    }

    /// Times reordering rows by bucket and then gathering every bucket from the reordered
    /// pieces, against gathering every bucket straight from the rows as they came, on
    /// the first `PROBE_BYTES` of `batches`, and returns whether the direct gather was
    /// clearly faster: by a third at least, as the probe sees a few batches only, where
    /// gathering from the rows as they came stays in cache, while the whole input
    /// spreads every bucket over all the batches held. Each runs twice, alternating,
    /// and keeps its best time, as other tasks share the machine.
    fn probe_gather(&self, batches: &[RecordBatch]) -> Result<bool> {
        let boundaries = self
            .boundaries
            .as_ref()
            .expect("the boundaries are chosen before the mode");
        let mut probed = vec![];
        let mut bytes = 0;
        for batch in batches {
            if bytes >= PROBE_BYTES {
                break;
            }
            bytes += get_record_batch_memory_size(batch);
            probed.push(batch.clone());
        }
        let keys = probed
            .iter()
            .map(|b| self.ctx.encoder.encode(b))
            .collect::<Result<Vec<_>>>()?;
        let num_buckets = boundaries.num_buckets();

        // Every bucket's rows, in an order as random as a sort leaves them
        let mut rng = Rng(0x9E37_79B9_7F4A_7C15);
        let mut shuffle = |rows: &mut Vec<(usize, usize)>| {
            for i in (1..rows.len()).rev() {
                rows.swap(i, rng.below(i + 1));
            }
        };
        let mut as_came: Vec<Vec<(usize, usize)>> = vec![vec![]; num_buckets];
        for (batch, k) in keys.iter().enumerate() {
            for row in 0..k.len() {
                as_came[boundaries.bucket_of(k, row)].push((batch, row));
            }
        }
        as_came.iter_mut().for_each(&mut shuffle);
        let batch_size = self.ctx.batch_size;
        let probe_metrics = BucketSortMetrics::new(&ExecutionPlanMetricsSet::new(), 0);

        let gather = || -> Result<std::time::Duration> {
            let start = Instant::now();
            let sources: Vec<&RecordBatch> = probed.iter().collect();
            for rows in &as_came {
                for chunk in rows.chunks(batch_size) {
                    std::hint::black_box(interleave_record_batch(&sources, chunk)?);
                }
            }
            Ok(start.elapsed())
        };
        let mut scatter_rows: Option<Vec<Vec<(usize, usize)>>> = None;
        let mut scatter = || -> Result<std::time::Duration> {
            let start = Instant::now();
            let mut pieces: Vec<RecordBatch> = vec![];
            let mut piece_of: Vec<Vec<(usize, usize)>> = vec![vec![]; num_buckets];
            for (batch, k) in probed.iter().zip(&keys) {
                let scattered =
                    scatter(boundaries, batch.clone(), k, false, false, &probe_metrics)?;
                for (bucket, offset, len, _, _) in &scattered.ranges {
                    piece_of[*bucket].push((pieces.len(), *len));
                    pieces.push(scattered.batch.slice(*offset, *len));
                }
            }
            // The same random order within a bucket, over its pieces
            let rows = scatter_rows.get_or_insert_with(|| {
                piece_of
                    .iter()
                    .map(|bucket_pieces| {
                        let mut rows: Vec<(usize, usize)> = bucket_pieces
                            .iter()
                            .flat_map(|(piece, len)| {
                                (0..*len).map(move |row| (*piece, row))
                            })
                            .collect();
                        let mut rng = Rng(0x2545_F491_4F6C_DD1D);
                        for i in (1..rows.len()).rev() {
                            rows.swap(i, rng.below(i + 1));
                        }
                        rows
                    })
                    .collect()
            });
            let sources: Vec<&RecordBatch> = pieces.iter().collect();
            for bucket_rows in rows.iter() {
                for chunk in bucket_rows.chunks(batch_size) {
                    std::hint::black_box(interleave_record_batch(&sources, chunk)?);
                }
            }
            Ok(start.elapsed())
        };

        let mut best_scatter = std::time::Duration::MAX;
        let mut best_gather = std::time::Duration::MAX;
        for _ in 0..2 {
            best_scatter = best_scatter.min(scatter()?);
            best_gather = best_gather.min(gather()?);
        }
        self.ctx
            .metrics
            .probe_scatter_time
            .add_duration(best_scatter);
        self.ctx.metrics.probe_gather_time.add_duration(best_gather);
        debug!("Bucket sort probe: scatter {best_scatter:?}, gather {best_gather:?}");
        Ok(best_gather.as_nanos() * 3 < best_scatter.as_nanos() * 2)
    }

    /// Makes `reservation` cover [`Self::reserved`], spilling part of what is held when
    /// it cannot grow, and everything when that is not enough
    pub(crate) fn reconcile(
        &mut self,
        reservation: &mut MemoryReservation,
    ) -> Result<()> {
        let target = self.reserved();
        let size = reservation.size();
        if target <= size {
            reservation.shrink(size - target);
            return Ok(());
        }
        if reservation.try_grow(target - size).is_ok() {
            self.ctx
                .metrics
                .peak_reserved_bytes
                .set_max(reservation.size());
            return Ok(());
        }
        self.spill_part(reservation, target - size)?;
        let (target, size) = (self.reserved(), reservation.size());
        if target > size && reservation.try_grow(target - size).is_err() {
            self.spill(reservation)?;
        }
        self.ctx
            .metrics
            .peak_reserved_bytes
            .set_max(reservation.size());
        Ok(())
    }

    /// Writes everything held to disk and frees `reservation`
    pub(crate) fn spill(&mut self, reservation: &mut MemoryReservation) -> Result<()> {
        if self.presorted_without_boundaries() {
            self.spill_sorted_run()?;
        } else {
            self.choose_boundaries()?;
            self.spill_held(usize::MAX)?;
        }
        debug_assert_eq!(self.reserved(), 0);
        reservation.free();
        Ok(())
    }

    /// Writes part of what is held to disk, the oldest batches first: at least
    /// `at_least` bytes, and at least a quarter of what is held so that the next batches
    /// do not spill again right away. `reservation` shrinks to what is left.
    pub(crate) fn spill_part(
        &mut self,
        reservation: &mut MemoryReservation,
        at_least: usize,
    ) -> Result<()> {
        if self.presorted_without_boundaries() {
            return self.spill(reservation);
        }
        self.choose_boundaries()?;
        self.spill_held(at_least.max(self.store.reserved / 4))?;
        let target = self.reserved();
        if reservation.size() > target {
            reservation.shrink(reservation.size() - target);
        }
        Ok(())
    }

    /// Spills held batches, oldest first, until `want` bytes are freed or none is left
    fn spill_held(&mut self, want: usize) -> Result<usize> {
        if self.spill.is_none() {
            self.spill = Some(SpillGroups::new(
                self.store.buckets.len(),
                self.held_spill_manager(),
                self.ctx.batch_size,
                self.ctx.metrics.clone(),
            ));
        }
        let end = self.store.held.len();
        spill_held_batches(
            &mut self.store,
            self.spill.as_mut().unwrap(),
            want,
            &(0..0),
            end,
        )
    }

    /// The buffered batches are in sort order and follow the runs before them
    fn spill_sorted_run(&mut self) -> Result<()> {
        if self.pending.is_empty() {
            return Ok(());
        }
        let mut file = self.spill_manager.create_in_progress_file("Sorting")?;
        let mut max_record_batch_memory = 0;
        for pending in self.pending.drain(..) {
            let memory = file.append_batch(&pending.batch)?;
            max_record_batch_memory = max_record_batch_memory.max(memory);
        }
        self.pending_rows = 0;
        self.pending_reserved = 0;
        if let Some(file) = file.finish()? {
            self.sorted_runs.push(SortedSpillFile {
                file,
                max_record_batch_memory,
            });
        }
        Ok(())
    }

    /// Spill manager of the held batches' schema
    fn held_spill_manager(&self) -> SpillManager {
        SpillManager::new(
            Arc::clone(&self.runtime),
            self.spill_manager.metrics.clone(),
            Arc::clone(&self.ctx.schema),
        )
        .with_compression_type(self.spill_manager.compression)
    }

    /// The sorted output, after the input ended. `reservation` holds [`Self::reserved`].
    pub(crate) fn into_stream(
        mut self,
        mut reservation: MemoryReservation,
        merge_reservation: MemoryReservation,
        baseline: BaselineMetrics,
        nested: NestedSort,
    ) -> Result<SendableRecordBatchStream> {
        self.ctx.metrics.publish();
        let schema = Arc::clone(&self.schema);

        // Everything was in order: the spilled runs then the buffered batches, as they are
        if self.presorted_without_boundaries() {
            self.ctx.metrics.presorted_input.add(1);
            let runs = std::mem::take(&mut self.sorted_runs);
            let pending = std::mem::take(&mut self.pending);
            let spill_manager = self.spill_manager.clone();
            let runs_reservation = reservation.new_empty();
            // The input batches as they came, in batches of the batch size
            let mut coalescer =
                BatchCoalescer::new(Arc::clone(&schema), self.ctx.batch_size);
            let stream = async_try_stream(move |mut emitter| async move {
                emit_sorted_runs(
                    &spill_manager,
                    runs,
                    runs_reservation,
                    Some(&mut coalescer),
                    &mut emitter,
                )
                .await?;
                for p in pending {
                    emit_coalesced(Some(&mut coalescer), p.batch, &mut emitter).await?;
                    reservation.shrink(p.reserved);
                }
                coalescer.finish_buffered_batch()?;
                while let Some(batch) = coalescer.next_completed_batch() {
                    emitter.emit(batch).await;
                }
                reservation.free();
                Ok(())
            });
            return Ok(observe(schema, stream, &baseline));
        }

        self.choose_boundaries()?;
        self.reconcile(&mut reservation)?;
        // Merging the spilled runs with the buckets needs room for the merge
        if !self.sorted_runs.is_empty() {
            self.spill(&mut reservation)?;
        }
        let runs = std::mem::take(&mut self.sorted_runs);
        let expr = self.ctx.expr.clone();
        let batch_size = self.ctx.batch_size;
        let spill_manager = self.spill_manager.clone();
        let buckets = self.bucket_stream(reservation, baseline.intermediate(), nested)?;
        if runs.is_empty() {
            return Ok(observe(schema, buckets, &baseline));
        }

        // The input was in order until some point after spilling: merge the spilled
        // runs, which are one sorted stream, with the buckets
        let runs_reservation = merge_reservation.new_empty();
        let sorted = Box::pin(RecordBatchStreamAdapter::new(
            Arc::clone(&schema),
            async_try_stream(move |mut emitter| async move {
                emit_sorted_runs(
                    &spill_manager,
                    runs,
                    runs_reservation,
                    None,
                    &mut emitter,
                )
                .await
            }),
        ));
        let buckets =
            Box::pin(RecordBatchStreamAdapter::new(Arc::clone(&schema), buckets));
        StreamingMergeBuilder::new()
            .with_streams(vec![sorted, buckets])
            .with_schema(schema)
            .with_expressions(&expr)
            .with_metrics(baseline)
            .with_batch_size(batch_size)
            .with_fetch(None)
            .with_reservation(merge_reservation)
            .with_round_robin_tie_breaker(false)
            .build()
    }

    /// Emits every bucket in order. When anything was spilled, group by group: each
    /// group's rows on disk are loaded next to its rows still in memory.
    fn bucket_stream(
        mut self,
        mut reservation: MemoryReservation,
        baseline: BaselineMetrics,
        nested: NestedSort,
    ) -> Result<impl futures::Stream<Item = Result<RecordBatch>> + Send + 'static> {
        let ctx = self.ctx.clone();
        let boundaries = self.boundaries.clone().unwrap();
        let mut spill = match self.spill.take() {
            Some(spill) => spill,
            None => SpillGroups::new(
                boundaries.num_buckets(),
                self.held_spill_manager(),
                self.ctx.batch_size,
                self.ctx.metrics.clone(),
            ),
        };
        let mut store = std::mem::take(&mut self.store);
        let ranges: Vec<std::ops::Range<usize>> =
            spill.groups.iter().map(|g| g.buckets.clone()).collect();

        Ok(async_try_stream(move |mut emitter| async move {
            let mut output = Output::default();
            for (g, buckets) in ranges.into_iter().enumerate() {
                // A bucket sorted by the nested sorter spills everything else held, so
                // the group is loaded again for its remaining buckets
                loop {
                    if spill.has_rows(g) {
                        // Loading makes room by spilling held batches: the rows still
                        // pending must not point into them
                        let _timer = baseline.elapsed_compute().timer();
                        output.detach(&ctx, &mut store)?;
                    }
                    if spill.has_rows(g)
                        && !load_group(
                            &ctx,
                            &boundaries,
                            &mut store,
                            &mut spill,
                            g,
                            &mut reservation,
                            &nested,
                            &mut output,
                            &baseline,
                            &mut emitter,
                        )
                        .await?
                    {
                        break;
                    }
                    let emitted = emit_store(
                        &ctx,
                        &boundaries,
                        &mut store,
                        buckets.clone(),
                        &mut reservation,
                        &mut spill,
                        &nested,
                        &mut output,
                        &baseline,
                        &mut emitter,
                    )
                    .await?;
                    if emitted {
                        break;
                    }
                }
            }
            flush_output(&ctx, &mut store, &mut output, true, &baseline, &mut emitter)
                .await?;
            reservation.free();
            Ok(())
        }))
    }
}

/// Grows `reservation` to `target` when it is smaller, returning whether it could
fn grow_to(reservation: &mut MemoryReservation, target: usize) -> bool {
    let size = reservation.size();
    target <= size || reservation.try_grow(target - size).is_ok()
}

/// Shrinks `reservation` to `target` when it is larger
fn shrink_to(reservation: &mut MemoryReservation, target: usize) {
    let size = reservation.size();
    if size > target {
        reservation.shrink(size - target);
    }
}

/// Loads the rows group `g` has on disk into `store`, next to its rows still in memory.
/// Room is made by spilling other held batches, oldest first, keeping their rows of the
/// group in memory. Returns false when the group does not fit even then: it was sorted
/// from disk by the nested sorter and emitted.
#[expect(clippy::too_many_arguments)]
async fn load_group(
    ctx: &BucketContext,
    boundaries: &Boundaries,
    store: &mut BucketStore,
    spill: &mut SpillGroups,
    g: usize,
    reservation: &mut MemoryReservation,
    nested: &NestedSort,
    output: &mut Output,
    baseline: &BaselineMetrics,
    emitter: &mut TryEmitter<RecordBatch, DataFusionError>,
) -> Result<bool> {
    let keep = spill.groups[g].buckets.clone();
    let need = |store: &BucketStore, spill: &SpillGroups| {
        store.reserved + spill.groups[g].memory
    };
    let mut fits = grow_to(reservation, need(store, spill));
    if !fits {
        let end = store.held.len();
        let want = need(store, spill) - reservation.size();
        spill_held_batches(store, spill, want, &keep, end)?;
        shrink_to(reservation, store.reserved);
        fits = grow_to(reservation, need(store, spill));
    }
    // Rows of the group still to read when it turns out not to fit while loading
    let mut unread: Option<SendableRecordBatchStream> = None;
    if fits {
        // Batches loaded here hold only the group's rows: making room spills the others
        let loaded_from = store.held.len();
        let mut input = spill.read(g, &ctx.schema, store.max_batch_bytes.max(1))?;
        while let Some(batch) = input.next().await {
            let batch = batch?;
            let keys = ctx.encoder.encode(&batch)?;
            store.add(scatter(
                boundaries,
                batch,
                &keys,
                false,
                ctx.gather,
                &ctx.metrics,
            )?)?;
            // The written size only estimates what the rows take once read
            if !grow_to(reservation, store.reserved) {
                let want = store.reserved - reservation.size();
                spill_held_batches(store, spill, want, &keep, loaded_from)?;
                shrink_to(reservation, store.reserved);
                if !grow_to(reservation, store.reserved) {
                    fits = false;
                    break;
                }
            }
        }
        if !fits {
            unread = Some(input);
        }
    }
    if fits {
        ctx.metrics.spill_groups_in_memory.add(1);
        ctx.metrics.peak_reserved_bytes.set_max(reservation.size());
        return Ok(true);
    }

    // The group does not fit: everything goes to disk, and the group is sorted from there
    ctx.metrics.spill_groups_external.add(1);
    spill_all_held(store, spill)?;
    shrink_to(reservation, store.reserved);
    let mut input = spill.read(g, &ctx.schema, store.max_batch_bytes.max(1))?;
    if let Some(unread) = unread {
        input = Box::pin(RecordBatchStreamAdapter::new(
            Arc::clone(&ctx.schema),
            input.chain(unread),
        ));
    }
    // The rows before the group's go out first
    flush_output(ctx, store, output, true, baseline, emitter).await?;
    let mut sorted = nested(Arc::clone(&ctx.schema), input).await?;
    while let Some(batch) = sorted.next().await {
        emitter.emit(batch?).await;
    }
    Ok(false)
}

fn observe(
    schema: SchemaRef,
    stream: impl futures::Stream<Item = Result<RecordBatch>> + Send + 'static,
    baseline: &BaselineMetrics,
) -> SendableRecordBatchStream {
    Box::pin(ObservedStream::new(
        Box::pin(RecordBatchStreamAdapter::new(schema, stream)),
        baseline.clone(),
        None,
    ))
}

/// Compares two single row sets of sort columns
fn compare_rows(
    left: &[ArrayRef],
    right: &[SortColumn],
    expr: &LexOrdering,
) -> Result<Ordering> {
    for ((l, r), e) in left.iter().zip(right).zip(expr.iter()) {
        let comparator = make_comparator(l.as_ref(), r.values.as_ref(), e.options)?;
        match comparator(0, 0) {
            Ordering::Equal => {}
            other => return Ok(other),
        }
    }
    Ok(Ordering::Equal)
}

/// Emits the spilled presorted runs in order, reserving room for the batches the
/// reader buffers ahead
async fn emit_sorted_runs(
    spill_manager: &SpillManager,
    runs: Vec<SortedSpillFile>,
    reservation: MemoryReservation,
    mut coalescer: Option<&mut BatchCoalescer>,
    emitter: &mut TryEmitter<RecordBatch, DataFusionError>,
) -> Result<()> {
    for run in runs {
        // The reader buffers up to two batches ahead of the one emitted
        reservation.try_resize(run.max_record_batch_memory.saturating_mul(3))?;
        let mut stream = spill_manager
            .read_spill_as_stream(run.file, Some(run.max_record_batch_memory))?;
        while let Some(batch) = stream.next().await {
            emit_coalesced(coalescer.as_deref_mut(), batch?, emitter).await?;
        }
    }
    reservation.free();
    Ok(())
}

/// Emits `batch`, through `coalescer` when there is one, which gathers small batches
/// into batches of its target size
async fn emit_coalesced(
    coalescer: Option<&mut BatchCoalescer>,
    batch: RecordBatch,
    emitter: &mut TryEmitter<RecordBatch, DataFusionError>,
) -> Result<()> {
    let Some(coalescer) = coalescer else {
        emitter.emit(batch).await;
        return Ok(());
    };
    coalescer.push_batch(batch)?;
    while let Some(batch) = coalescer.next_completed_batch() {
        emitter.emit(batch).await;
    }
    Ok(())
}

/// Emits the `buckets` of `store` in order. `reservation` covers `store.reserved` at
/// least. A bucket without memory to sort it is sorted by the nested sorter, after
/// everything else held is spilled to give it the memory; it then returns false, with
/// the buckets after it on disk.
#[expect(clippy::too_many_arguments)]
async fn emit_store(
    ctx: &BucketContext,
    boundaries: &Boundaries,
    store: &mut BucketStore,
    buckets: std::ops::Range<usize>,
    reservation: &mut MemoryReservation,
    spill: &mut SpillGroups,
    nested: &NestedSort,
    output: &mut Output,
    baseline: &BaselineMetrics,
    emitter: &mut TryEmitter<RecordBatch, DataFusionError>,
) -> Result<bool> {
    let null_bucket = boundaries.null_bucket();
    // Sorting a bucket needs its keys and its order next to what is held, and one
    // output batch is copied at a time: room for the largest bucket's sort, reserved
    // once for all of them
    let batch_room = store.output_batch_bytes(ctx.batch_size);
    let working = store.buckets[buckets.clone()]
        .iter()
        .map(|b| ctx.sort_room(b))
        .max()
        .unwrap_or(0)
        .saturating_add(batch_room);
    let floor = |store: &BucketStore| store.reserved + working;
    // When it cannot grow, every bucket tries for its own room below
    grow_to(reservation, floor(store));
    let group = buckets.clone();
    for index in buckets {
        if store.buckets[index].rows == 0 {
            continue;
        }
        let room = ctx.sort_room(&store.buckets[index]) + batch_room;
        let mut fits = grow_to(reservation, store.reserved + room);
        if !fits && store.held.iter().flatten().count() > 0 {
            // Room from the rows of later groups, read back when their turn comes,
            // rather than from sorting this bucket on disk. The pending output must not
            // point into the held batches spilled.
            {
                let _timer = baseline.elapsed_compute().timer();
                output.detach(ctx, store)?;
            }
            let want = (store.reserved + room).saturating_sub(reservation.size());
            let end = store.held.len();
            spill_held_batches(store, spill, want, &group, end)?;
            shrink_to(reservation, store.reserved);
            fits = grow_to(reservation, store.reserved + room);
            if fits {
                ctx.metrics.room_spills.add(1);
            }
        }
        let bucket = store.take_bucket(index);
        ctx.metrics.largest_bucket_bytes.set_max(bucket.bytes);

        if !fits {
            ctx.metrics.external_buckets.add(1);
            flush_output(ctx, store, output, true, baseline, emitter).await?;
            // The nested sorter reserves the rows itself, so they are spilled and
            // released here first, rather than held twice, and so is everything else
            let spill_manager = &spill.spill_manager;
            let mut file = spill_manager.create_in_progress_file("Sorting")?;
            let mut max_batch_memory = 0;
            // In batches no larger than the input ones, as the nested sorter reserves
            // each batch it gets at once
            let mut chunks = Output::default();
            chunks.push(vec![store.row_set(&bucket.pieces, bucket.bytes)], vec![]);
            while let Some(chunk) = chunks.next_batch(ctx, true, batch_room.max(1))? {
                let _timer = ctx.metrics.spill_write_time.timer();
                let memory = file.append_batch(&chunk)?;
                max_batch_memory = max_batch_memory.max(memory);
            }
            store.release(&bucket.pieces);
            let file = file.finish()?;
            spill_all_held(store, spill)?;
            shrink_to(reservation, store.reserved);
            if let Some(file) = file {
                let input = spill
                    .spill_manager
                    .read_spill_as_stream(file, Some(max_batch_memory))?;
                let mut sorted = nested(Arc::clone(&ctx.schema), input).await?;
                while let Some(batch) = sorted.next().await {
                    emitter.emit(batch?).await;
                }
            }
            return Ok(false);
        }

        ctx.metrics.peak_reserved_bytes.set_max(reservation.size());
        let sorted = {
            let _timer = baseline.elapsed_compute().timer();
            ctx.sort_bucket(&bucket, store, index == null_bucket)?
        };
        output.push(sorted, bucket.pieces);
        flush_output(ctx, store, output, false, baseline, emitter).await?;
        // The emitted batches are the consumer's now
        shrink_to(reservation, floor(store));
    }
    shrink_to(reservation, store.reserved);
    Ok(true)
}

/// Emits the full batches of `output`, and its last rows too when `last`, releasing the
/// pieces of the buckets whose rows are all emitted
async fn flush_output(
    ctx: &BucketContext,
    store: &mut BucketStore,
    output: &mut Output,
    last: bool,
    baseline: &BaselineMetrics,
    emitter: &mut TryEmitter<RecordBatch, DataFusionError>,
) -> Result<()> {
    loop {
        let batch = {
            let _timer = baseline.elapsed_compute().timer();
            output.next_batch(ctx, last, store.output_batch_bytes(ctx.batch_size))?
        };
        let Some(batch) = batch else {
            break;
        };
        emitter.emit(batch).await;
    }
    output.release(store);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::collect;
    use crate::execution_plan::ExecutionPlan;
    use crate::expressions::{PhysicalSortExpr, col};
    use crate::sorts::sort::SortExec;
    use crate::test::TestMemoryExec;
    use arrow::array::{
        Decimal128Array, Float64Array, Int32Array, Int64Array, StringArray,
        TimestampMicrosecondArray,
    };
    use arrow::compute::lexsort_to_indices;
    use arrow::datatypes::Field;
    use datafusion_execution::TaskContext;
    use datafusion_execution::config::SessionConfig;
    use datafusion_execution::runtime_env::RuntimeEnvBuilder;
    use datafusion_physical_expr_common::metrics::MetricsSet;

    /// Deterministic random numbers for the inputs
    struct Gen(Rng);

    impl Gen {
        fn new(seed: u64) -> Self {
            Self(Rng(seed | 1))
        }
        fn below(&mut self, n: u64) -> u64 {
            self.0.next_u64() % n
        }
        fn chance(&mut self, percent: u64) -> bool {
            self.below(100) < percent
        }
        fn string(&mut self, prefix: &str, len: usize) -> String {
            let mut s = prefix.to_string();
            while s.len() < len {
                s.push((b'a' + self.below(26) as u8) as char);
            }
            s
        }
    }

    struct Run {
        output: Vec<RecordBatch>,
        metrics: MetricsSet,
    }

    #[derive(Clone)]
    struct Setup {
        batch_size: usize,
        bucket_count: usize,
        target_bucket_bytes: usize,
        sample_rows: usize,
        in_place_threshold: usize,
        memory_limit: Option<usize>,
        mode: &'static str,
    }

    impl Default for Setup {
        fn default() -> Self {
            Self {
                batch_size: 1024,
                bucket_count: 64,
                target_bucket_bytes: 64 * 1024,
                sample_rows: 8 * 1024,
                in_place_threshold: 1024,
                memory_limit: None,
                mode: "scatter",
            }
        }
    }

    async fn run(
        setup: &Setup,
        bucket: bool,
        batches: &[RecordBatch],
        ordering: &LexOrdering,
    ) -> Result<Run> {
        let config = SessionConfig::new()
            .with_batch_size(setup.batch_size)
            .set_bool("datafusion.execution.sort_bucket_sort_enabled", bucket)
            .set_usize("datafusion.execution.sort_bucket_count", setup.bucket_count)
            .set_usize(
                "datafusion.execution.sort_bucket_target_bytes",
                setup.target_bucket_bytes,
            )
            .set_usize(
                "datafusion.execution.sort_bucket_sample_rows",
                setup.sample_rows,
            )
            .set_usize(
                "datafusion.execution.sort_in_place_threshold_bytes",
                setup.in_place_threshold,
            )
            .set_usize(
                "datafusion.execution.sort_spill_reservation_bytes",
                64 * 1024,
            )
            .set_str("datafusion.execution.sort_bucket_mode", setup.mode);
        let mut runtime = RuntimeEnvBuilder::new();
        if let Some(limit) = setup.memory_limit {
            runtime = runtime.with_memory_limit(limit, 1.0);
        }
        let task_ctx = Arc::new(
            TaskContext::default()
                .with_session_config(config)
                .with_runtime(runtime.build_arc()?),
        );
        let schema = batches[0].schema();
        let input = TestMemoryExec::try_new_exec(&[batches.to_vec()], schema, None)?;
        let sort = Arc::new(SortExec::new(ordering.clone(), input));
        let output = collect(
            Arc::clone(&sort) as Arc<dyn ExecutionPlan>,
            Arc::clone(&task_ctx),
        )
        .await
        .map_err(|e| e.context(if bucket { "bucket sort" } else { "merge sort" }))?;
        assert_eq!(
            task_ctx.runtime_env().memory_pool.reserved(),
            0,
            "the sort returns all its memory"
        );
        Ok(Run {
            output,
            metrics: sort.metrics().unwrap(),
        })
    }

    fn count(metrics: &MetricsSet, name: &str) -> usize {
        metrics.sum_by_name(name).map(|m| m.as_usize()).unwrap_or(0)
    }

    fn concat(batches: &[RecordBatch]) -> RecordBatch {
        concat_batches(&batches[0].schema(), batches).unwrap()
    }

    fn assert_sorted(batch: &RecordBatch, ordering: &LexOrdering) {
        let columns: Vec<SortColumn> = ordering
            .iter()
            .map(|e| e.evaluate_to_sort_column(batch).unwrap())
            .collect();
        let comparator = LexicographicalComparator::try_new(&columns).unwrap();
        for row in 1..batch.num_rows() {
            assert_ne!(
                comparator.compare(row - 1, row),
                Ordering::Greater,
                "rows {} and {row} are out of order",
                row - 1
            );
        }
    }

    /// The rows sorted by every column, to compare the rows regardless of the
    /// order of rows with equal sort keys
    fn canonical(batch: &RecordBatch) -> RecordBatch {
        let columns: Vec<SortColumn> = batch
            .columns()
            .iter()
            .map(|c| SortColumn {
                values: Arc::clone(c),
                options: None,
            })
            .collect();
        let indices = lexsort_to_indices(&columns, None).unwrap();
        take_record_batch(batch, &indices).unwrap()
    }

    fn assert_same_rows(actual: &RecordBatch, expected: &RecordBatch) {
        if actual == expected {
            return;
        }
        let formatter = |batch: &RecordBatch, row: usize| -> String {
            batch
                .columns()
                .iter()
                .map(|c| {
                    arrow::util::display::ArrayFormatter::try_new(
                        c.as_ref(),
                        &Default::default(),
                    )
                    .unwrap()
                    .value(row)
                    .to_string()
                })
                .collect::<Vec<_>>()
                .join(", ")
        };
        for row in 0..actual.num_rows() {
            let (a, e) = (formatter(actual, row), formatter(expected, row));
            assert_eq!(a, e, "row {row} differs");
        }
        panic!("batches differ in their layout only");
    }

    /// Sorts with the merge sort and the bucket sort, checks both agree and returns
    /// the bucket sort's metrics
    async fn check(
        setup: &Setup,
        batches: &[RecordBatch],
        ordering: &LexOrdering,
    ) -> Result<MetricsSet> {
        let expected = run(setup, false, batches, ordering).await?;
        let actual = run(setup, true, batches, ordering).await?;
        let expected = concat(&expected.output);
        let output = concat(&actual.output);
        assert_eq!(output.schema(), batches[0].schema());
        assert_eq!(output.num_rows(), expected.num_rows());
        assert_sorted(&output, ordering);
        let input = canonical(&concat(batches));
        assert_same_rows(&canonical(&expected), &input);
        assert_same_rows(&canonical(&output), &input);
        for batch in &actual.output {
            assert!(batch.num_rows() <= setup.batch_size);
        }
        let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(actual.metrics.output_rows(), Some(rows));
        Ok(actual.metrics)
    }

    fn ordering(schema: &SchemaRef, keys: &[(&str, bool, bool)]) -> LexOrdering {
        LexOrdering::new(keys.iter().map(|(name, descending, nulls_first)| {
            PhysicalSortExpr::new(
                col(name, schema).unwrap(),
                SortOptions {
                    descending: *descending,
                    nulls_first: *nulls_first,
                },
            )
        }))
        .unwrap()
    }

    /// `k` keys from `key`, a short and a wide string payload
    fn int_batches(
        batches: usize,
        rows: usize,
        seed: u64,
        mut key: impl FnMut(&mut Gen, usize) -> Option<i64>,
    ) -> Vec<RecordBatch> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("k", DataType::Int64, true),
            Field::new("id", DataType::Int64, false),
            Field::new("short", DataType::Utf8, false),
            Field::new("wide", DataType::Utf8, true),
        ]));
        let mut g = Gen::new(seed);
        let mut id = 0i64;
        (0..batches)
            .map(|b| {
                let keys: Int64Array =
                    (0..rows).map(|r| key(&mut g, b * rows + r)).collect();
                let ids: Int64Array = (0..rows)
                    .map(|_| {
                        id += 1;
                        id
                    })
                    .collect();
                let short: StringArray =
                    (0..rows).map(|_| Some(g.string("", 3))).collect();
                let wide: StringArray = (0..rows)
                    .map(|_| (!g.chance(5)).then(|| g.string("payload-", 100)))
                    .collect();
                RecordBatch::try_new(
                    Arc::clone(&schema),
                    vec![
                        Arc::new(keys),
                        Arc::new(ids),
                        Arc::new(short),
                        Arc::new(wide),
                    ],
                )
                .unwrap()
            })
            .collect()
    }

    #[tokio::test]
    async fn random_keys_with_nulls() -> Result<()> {
        for (descending, nulls_first) in
            [(false, false), (false, true), (true, false), (true, true)]
        {
            let batches = int_batches(40, 1000, 7, |g, _| {
                (!g.chance(3)).then(|| g.below(1 << 40) as i64 - (1 << 39))
            });
            let ordering =
                ordering(&batches[0].schema(), &[("k", descending, nulls_first)]);
            let metrics = check(&Setup::default(), &batches, &ordering).await?;
            assert!(count(&metrics, "bucket_sort_scattered_batches") >= 40);
            assert!(count(&metrics, "bucket_sort_sorted_buckets") > 1);
            assert_eq!(count(&metrics, "bucket_sort_presorted_input"), 0);
        }
        Ok(())
    }

    #[tokio::test]
    async fn buckets_share_full_output_batches() -> Result<()> {
        let batches = int_batches(40, 1000, 11, |g, _| Some(g.below(1 << 40) as i64));
        let ordering = ordering(&batches[0].schema(), &[("k", false, false)]);
        for mode in ["scatter", "gather"] {
            let setup = Setup {
                mode,
                ..Default::default()
            };
            check(&setup, &batches, &ordering).await?;
            let sorted = run(&setup, true, &batches, &ordering).await?;
            assert!(count(&sorted.metrics, "bucket_sort_sorted_buckets") > 1);
            // Every batch but the last is about as full as an input batch (1000 rows),
            // though a bucket holds about 600 rows
            let rows: Vec<usize> = sorted.output.iter().map(|b| b.num_rows()).collect();
            assert!(
                rows[..rows.len() - 1].iter().all(|r| *r >= 900),
                "{mode}: {rows:?}"
            );
        }
        Ok(())
    }

    #[tokio::test]
    async fn small_input_batches_fill_output_batches() -> Result<()> {
        // Input batches of 100 rows, far below the batch size in rows: the output
        // batches still take the batch size, and the memory of that many rows
        let batches = int_batches(40, 100, 13, |g, _| Some(g.below(1 << 40) as i64));
        let ordering = ordering(&batches[0].schema(), &[("k", false, false)]);
        let row_bytes = batches
            .iter()
            .map(|b| get_record_batch_memory_size(b).div_ceil(b.num_rows()))
            .max()
            .unwrap();
        for mode in ["scatter", "gather"] {
            let setup = Setup {
                mode,
                sample_rows: 1000,
                ..Default::default()
            };
            check(&setup, &batches, &ordering).await?;
            let sorted = run(&setup, true, &batches, &ordering).await?;
            let rows: Vec<usize> = sorted.output.iter().map(|b| b.num_rows()).collect();
            assert!(
                rows[..rows.len() - 1]
                    .iter()
                    .all(|r| *r == setup.batch_size),
                "{mode}: {rows:?}"
            );
            for batch in &sorted.output {
                let memory = get_record_batch_memory_size(batch);
                let bound = 2 * row_bytes * setup.batch_size;
                assert!(memory <= bound, "{mode}: {memory} > {bound}");
            }
        }
        Ok(())
    }

    #[tokio::test]
    async fn few_distinct_keys_take_the_dictionary_path() -> Result<()> {
        let batches = int_batches(30, 1000, 11, |g, _| {
            (!g.chance(10)).then(|| g.below(12) as i64 * 1000)
        });
        let ordering = ordering(&batches[0].schema(), &[("k", false, true)]);
        let metrics = check(&Setup::default(), &batches, &ordering).await?;
        assert_eq!(count(&metrics, "bucket_sort_dictionary_path"), 1);
        assert!(count(&metrics, "bucket_sort_constant_buckets") >= 12);
        assert_eq!(count(&metrics, "bucket_sort_sorted_buckets"), 0);

        // With a second key, the first is constant in every bucket
        let ordering = ordering_with_id(&batches);
        let metrics = check(&Setup::default(), &batches, &ordering).await?;
        assert!(count(&metrics, "bucket_sort_leading_key_constant") >= 12);
        Ok(())
    }

    fn ordering_with_id(batches: &[RecordBatch]) -> LexOrdering {
        ordering(
            &batches[0].schema(),
            &[("k", false, true), ("id", true, false)],
        )
    }

    #[tokio::test]
    async fn heavy_hitters_get_their_own_bucket() -> Result<()> {
        let batches = int_batches(30, 1000, 13, |g, _| {
            Some(if g.chance(40) {
                42
            } else if g.chance(30) {
                -7
            } else {
                g.below(1 << 30) as i64
            })
        });
        let ordering = ordering(&batches[0].schema(), &[("k", false, false)]);
        let metrics = check(&Setup::default(), &batches, &ordering).await?;
        assert!(count(&metrics, "bucket_sort_heavy_hitters") >= 2);
        assert!(count(&metrics, "bucket_sort_constant_buckets") >= 2);
        Ok(())
    }

    #[tokio::test]
    async fn presorted_input_is_not_sorted() -> Result<()> {
        let batches = int_batches(30, 1000, 17, |_, i| Some(i as i64 / 3));
        let ordering = ordering(&batches[0].schema(), &[("k", false, false)]);
        let metrics = check(&Setup::default(), &batches, &ordering).await?;
        assert_eq!(count(&metrics, "bucket_sort_presorted_input"), 1);
        assert_eq!(count(&metrics, "bucket_sort_sorted_buckets"), 0);

        // Sorted batches arriving out of order: scattered without a take, and the
        // zone maps emit the pieces without sorting them
        let mut shuffled = batches.clone();
        shuffled.reverse();
        shuffled.swap(3, 20);
        let metrics = check(&Setup::default(), &shuffled, &ordering).await?;
        assert_eq!(count(&metrics, "bucket_sort_presorted_input"), 0);
        assert!(count(&metrics, "bucket_sort_scattered_in_place_batches") >= 30);
        assert!(count(&metrics, "bucket_sort_zone_map_presorted_pieces") > 0);
        assert_eq!(count(&metrics, "bucket_sort_sorted_buckets"), 0);
        Ok(())
    }

    #[tokio::test]
    async fn overfull_buckets_are_split() -> Result<()> {
        let batches = int_batches(30, 1000, 19, |g, _| Some(g.below(1 << 30) as i64));
        let ordering = ordering(&batches[0].schema(), &[("k", false, false)]);
        let setup = Setup {
            bucket_count: 4,
            target_bucket_bytes: 16 * 1024,
            ..Default::default()
        };
        let metrics = check(&setup, &batches, &ordering).await?;
        assert!(count(&metrics, "bucket_sort_split_buckets") > 0);
        Ok(())
    }

    #[tokio::test]
    async fn string_keys_sharing_a_prefix() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("s", DataType::Utf8, true),
            Field::new("i", DataType::Int32, true),
        ]));
        let mut g = Gen::new(23);
        let batches: Vec<RecordBatch> = (0..25)
            .map(|_| {
                let s: StringArray = (0..800)
                    .map(|_| {
                        (!g.chance(4)).then(|| {
                            let prefix = ["https://www.", "https://", "abc", ""]
                                [g.below(4) as usize];
                            let len = prefix.len() + g.below(12) as usize;
                            g.string(prefix, len)
                        })
                    })
                    .collect();
                let i: Int32Array = (0..800)
                    .map(|_| (!g.chance(4)).then(|| g.below(5) as i32))
                    .collect();
                RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(s), Arc::new(i)])
                    .unwrap()
            })
            .collect();
        for keys in [
            vec![("s", false, false)],
            vec![("s", true, true), ("i", false, true)],
            vec![("s", false, true), ("i", true, false)],
        ] {
            let ordering = ordering(&schema, &keys);
            check(&Setup::default(), &batches, &ordering).await?;
            let setup = Setup {
                bucket_count: 3,
                target_bucket_bytes: 4 * 1024,
                ..Default::default()
            };
            check(&setup, &batches, &ordering).await?;
        }
        Ok(())
    }

    #[tokio::test]
    async fn float_decimal_and_timestamp_keys() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("f", DataType::Float64, true),
            Field::new("d", DataType::Decimal128(30, 2), true),
            Field::new(
                "t",
                DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, None),
                true,
            ),
        ]));
        let mut g = Gen::new(29);
        let specials = [
            f64::NAN,
            -f64::NAN,
            f64::INFINITY,
            f64::NEG_INFINITY,
            0.0,
            -0.0,
        ];
        let batches: Vec<RecordBatch> = (0..20)
            .map(|_| {
                let f: Float64Array = (0..1000)
                    .map(|_| {
                        (!g.chance(5)).then(|| {
                            if g.chance(10) {
                                specials[g.below(6) as usize]
                            } else {
                                (g.below(1 << 20) as f64 - (1 << 19) as f64) / 7.0
                            }
                        })
                    })
                    .collect();
                let d: Decimal128Array = (0..1000)
                    .map(|_| {
                        (!g.chance(5)).then(|| {
                            let v = g.below(1 << 40) as i128 - (1 << 39);
                            if g.chance(5) {
                                v * 1_000_000_000_000_000_000
                            } else {
                                v
                            }
                        })
                    })
                    .collect::<Decimal128Array>()
                    .with_precision_and_scale(30, 2)
                    .unwrap();
                let t: TimestampMicrosecondArray = (0..1000)
                    .map(|_| (!g.chance(5)).then(|| g.below(1 << 50) as i64 - (1 << 49)))
                    .collect();
                RecordBatch::try_new(
                    Arc::clone(&schema),
                    vec![Arc::new(f), Arc::new(d), Arc::new(t)],
                )
                .unwrap()
            })
            .collect();
        for keys in [
            vec![("f", false, false)],
            vec![("f", true, true)],
            vec![("d", false, true), ("t", false, false)],
            vec![("d", true, false)],
            vec![("t", true, true), ("f", false, false)],
        ] {
            check(&Setup::default(), &batches, &ordering(&schema, &keys)).await?;
        }
        Ok(())
    }

    #[tokio::test]
    async fn small_input_uses_the_merge_sort() -> Result<()> {
        let batches = int_batches(2, 100, 31, |g, _| Some(g.below(1000) as i64));
        let ordering = ordering(&batches[0].schema(), &[("k", false, false)]);
        let setup = Setup {
            in_place_threshold: 1024 * 1024,
            ..Default::default()
        };
        let metrics = check(&setup, &batches, &ordering).await?;
        assert_eq!(count(&metrics, "bucket_sort_scattered_batches"), 0);
        Ok(())
    }

    #[tokio::test]
    async fn unsupported_key_falls_back() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "l",
            DataType::List(Arc::new(Field::new_list_field(DataType::Int32, true))),
            true,
        )]));
        let mut g = Gen::new(37);
        let batches: Vec<RecordBatch> = (0..5)
            .map(|_| {
                let values: Vec<Option<Vec<Option<i32>>>> = (0..100)
                    .map(|_| Some(vec![Some(g.below(100) as i32)]))
                    .collect();
                let list = arrow::array::ListArray::from_iter_primitive::<
                    arrow::datatypes::Int32Type,
                    _,
                    _,
                >(values);
                RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(list)]).unwrap()
            })
            .collect();
        let ordering = ordering(&schema, &[("l", false, false)]);
        let metrics = check(&Setup::default(), &batches, &ordering).await?;
        // Sorted by the merge sort alone, which shows no bucket sort metric
        assert!(
            metrics
                .iter()
                .all(|m| !m.value().name().starts_with("bucket_sort"))
        );
        Ok(())
    }

    /// Memory that holds roughly `fraction` of the input as the sort reserves it
    fn memory_for(batches: &[RecordBatch], fraction: f64) -> usize {
        let reserved: usize = batches
            .iter()
            .map(|b| get_reserved_bytes_for_record_batch(b).unwrap())
            .sum();
        (reserved as f64 * fraction) as usize + 64 * 1024
    }

    #[tokio::test]
    async fn spilled_groups_are_bucket_sorted() -> Result<()> {
        let batches = int_batches(60, 1000, 41, |g, _| {
            (!g.chance(3)).then(|| g.below(1 << 30) as i64)
        });
        let ordering = ordering_with_id(&batches);
        let setup = Setup {
            memory_limit: Some(memory_for(&batches, 0.3)),
            ..Default::default()
        };
        let metrics = check(&setup, &batches, &ordering).await?;
        assert!(metrics.spill_count().unwrap() > 0);
        assert!(count(&metrics, "bucket_sort_spill_groups_in_memory") > 0);
        assert_eq!(count(&metrics, "bucket_sort_spill_groups_external"), 0);
        // A group's pieces (about 15 rows each) are written together
        let rows = metrics.spilled_rows().unwrap();
        let written = count(&metrics, "bucket_sort_spill_batches");
        assert!(rows >= 50 * written, "{rows} rows in {written} batches");
        Ok(())
    }

    #[tokio::test]
    async fn spilled_groups_too_large_are_merge_sorted() -> Result<()> {
        // Two value buckets for data that hardly fits: every group is a third or
        // more of the data
        let batches = int_batches(60, 1000, 43, |g, _| Some(g.below(1 << 30) as i64));
        let ordering = ordering(&batches[0].schema(), &[("k", true, false)]);
        let setup = Setup {
            bucket_count: 3,
            memory_limit: Some(memory_for(&batches, 0.2)),
            ..Default::default()
        };
        let metrics = check(&setup, &batches, &ordering).await?;
        assert!(count(&metrics, "bucket_sort_spill_groups_external") > 0);
        Ok(())
    }

    #[tokio::test]
    async fn bucket_without_memory_is_merge_sorted() -> Result<()> {
        // One bucket with all the rows: the keys share their first 8 bytes, so they
        // cannot split it, and sorting it needs more than the memory left
        let schema = Arc::new(Schema::new(vec![
            Field::new("s", DataType::Utf8, false),
            Field::new("p", DataType::Int64, false),
        ]));
        let mut g = Gen::new(47);
        let batches: Vec<RecordBatch> = (0..30)
            .map(|_| {
                let s: StringArray = (0..1000)
                    .map(|_| Some(g.string("samekey-samekey-samekey-", 30)))
                    .collect();
                let p: Int64Array =
                    (0..1000).map(|_| Some(g.below(1000) as i64)).collect();
                RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(s), Arc::new(p)])
                    .unwrap()
            })
            .collect();
        let ordering = ordering(&schema, &[("s", false, false)]);
        let setup = |fraction| Setup {
            memory_limit: Some(memory_for(&batches, fraction)),
            target_bucket_bytes: 64 * 1024 * 1024,
            ..Default::default()
        };
        let metrics = check(&setup(1.2), &batches, &ordering).await?;
        assert_eq!(count(&metrics, "bucket_sort_external_buckets"), 1);

        // Correct and returning all memory at any limit
        for fraction in [0.5, 0.9, 1.0, 1.1, 1.3, 1.5, 1.8, 3.0] {
            check(&setup(fraction), &batches, &ordering).await?;
        }
        Ok(())
    }

    #[tokio::test]
    async fn bucket_of_wide_rows_sorts_in_the_room_of_its_keys() -> Result<()> {
        // One bucket with all the rows, as above, but the rows are mostly a wide
        // payload: sorting them takes room for their keys, not a copy of the rows
        let schema = Arc::new(Schema::new(vec![
            Field::new("s", DataType::Utf8, false),
            Field::new("payload", DataType::Utf8, false),
        ]));
        let mut g = Gen::new(53);
        let batches: Vec<RecordBatch> = (0..30)
            .map(|_| {
                let s: StringArray = (0..200)
                    .map(|_| Some(g.string("samekey-samekey-samekey-", 30)))
                    .collect();
                let payload: StringArray =
                    (0..200).map(|_| Some(g.string("payload-", 2000))).collect();
                RecordBatch::try_new(
                    Arc::clone(&schema),
                    vec![Arc::new(s), Arc::new(payload)],
                )
                .unwrap()
            })
            .collect();
        let ordering = ordering(&schema, &[("s", false, false)]);
        let setup = Setup {
            memory_limit: Some(memory_for(&batches, 1.2)),
            target_bucket_bytes: 64 * 1024 * 1024,
            ..Default::default()
        };
        let metrics = check(&setup, &batches, &ordering).await?;
        assert_eq!(count(&metrics, "bucket_sort_external_buckets"), 0);
        assert_eq!(metrics.spill_count(), Some(0));
        Ok(())
    }

    /// Constant and few-valued string keys, then an integer key close to unique: the
    /// keys of a write partitioned by some columns, then an id and an offset
    fn offset_batches(
        batches: usize,
        rows: usize,
        values: u64,
        seed: u64,
    ) -> Vec<RecordBatch> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("dt", DataType::Utf8, true),
            Field::new("pid", DataType::Utf8, true),
            Field::new("app", DataType::Utf8, true),
            Field::new("offset", DataType::Int64, true),
            Field::new("payload", DataType::Utf8, true),
        ]));
        let mut g = Gen::new(seed);
        (0..batches)
            .map(|_| {
                let dt: StringArray = (0..rows).map(|_| Some("2025-11-07")).collect();
                let pid: StringArray = (0..rows)
                    .map(|_| Some(format!("pid_{}", g.below(3))))
                    .collect();
                let app: StringArray = (0..rows)
                    .map(|_| Some(format!("com.app.{}", g.below(values))))
                    .collect();
                let offset: Int64Array =
                    (0..rows).map(|_| Some(g.below(1 << 40) as i64)).collect();
                let payload: StringArray =
                    (0..rows).map(|_| Some(g.string("p-", 50))).collect();
                RecordBatch::try_new(
                    Arc::clone(&schema),
                    vec![
                        Arc::new(dt),
                        Arc::new(pid),
                        Arc::new(app),
                        Arc::new(offset),
                        Arc::new(payload),
                    ],
                )
                .unwrap()
            })
            .collect()
    }

    #[tokio::test]
    async fn exact_packed_keys_sort_without_comparing_columns() -> Result<()> {
        let keys = [
            ("dt", false, true),
            ("pid", false, true),
            ("app", true, false),
            ("offset", false, true),
        ];
        // Few app values: every key gets a dictionary, and the offset its 64 bits
        let batches = offset_batches(20, 1000, 20, 61);
        let by = ordering(&batches[0].schema(), &keys);
        for mode in ["scatter", "gather"] {
            let setup = Setup {
                mode,
                ..Default::default()
            };
            let metrics = check(&setup, &batches, &by).await?;
            assert_eq!(count(&metrics, "bucket_sort_exact_key_levels"), 4, "{mode}");
            assert!(
                count(&metrics, "bucket_sort_packed_key_sorts") > 0,
                "{mode}"
            );
        }
        // Too many app values for a dictionary: the columns are compared
        let batches = offset_batches(20, 1000, 100_000, 67);
        let by = ordering(&batches[0].schema(), &keys);
        let metrics = check(&Setup::default(), &batches, &by).await?;
        assert_eq!(count(&metrics, "bucket_sort_packed_key_sorts"), 0);
        Ok(())
    }

    #[tokio::test]
    async fn bucket_gets_room_from_later_groups() -> Result<()> {
        // A third of the rows share a long prefix: one bucket, unsplittable, its keys
        // most of its rows. Everything fits held, but not sorting that bucket next to it
        let schema = Arc::new(Schema::new(vec![
            Field::new("s", DataType::Utf8, false),
            Field::new("p", DataType::Int64, false),
        ]));
        let mut g = Gen::new(83);
        let batches: Vec<RecordBatch> = (0..30)
            .map(|_| {
                let s: StringArray = (0..1000)
                    .map(|_| {
                        Some(if g.chance(30) {
                            g.string("samekey-samekey-samekey-", 60)
                        } else {
                            g.string("", 60)
                        })
                    })
                    .collect();
                let p: Int64Array =
                    (0..1000).map(|_| Some(g.below(1000) as i64)).collect();
                RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(s), Arc::new(p)])
                    .unwrap()
            })
            .collect();
        let ordering = ordering(&schema, &[("s", false, false)]);
        let setup = Setup {
            memory_limit: Some(memory_for(&batches, 0.6)),
            target_bucket_bytes: 64 * 1024 * 1024,
            ..Default::default()
        };
        let metrics = check(&setup, &batches, &ordering).await?;
        // The rows of later groups are spilled to make room, not the bucket merge sorted
        assert_eq!(count(&metrics, "bucket_sort_room_spills"), 1);
        assert_eq!(count(&metrics, "bucket_sort_external_buckets"), 0);
        assert!(metrics.spill_count().unwrap() > 0);
        Ok(())
    }

    #[tokio::test]
    async fn reversed_input_short_of_memory_is_merge_sorted() -> Result<()> {
        // Keys arriving in reverse order: boundaries from the first rows would hold
        // none of the next ones, so the merge sort takes over when memory runs out
        let batches = int_batches(30, 1000, 89, |_, i| Some(-(i as i64)));
        let ordering = ordering(&batches[0].schema(), &[("k", false, false)]);
        let setup = Setup {
            memory_limit: Some(memory_for(&batches, 0.3)),
            ..Default::default()
        };
        let metrics = check(&setup, &batches, &ordering).await?;
        assert!(metrics.spill_count().unwrap() > 0);
        assert!(
            metrics
                .iter()
                .all(|m| !m.value().name().starts_with("bucket_sort"))
        );
        // With the memory to hold it, the bucket sort sorts it
        let metrics = check(&Setup::default(), &batches, &ordering).await?;
        assert!(count(&metrics, "bucket_sort_sorted_buckets") > 0);
        Ok(())
    }

    #[tokio::test]
    async fn slices_of_a_larger_batch_are_held_at_their_size() -> Result<()> {
        // Every input batch is a slice of one batch ten times its size: held as they
        // came, each would be reserved at the size of the whole batch
        let whole = concat(&int_batches(10, 1000, 97, |g, _| {
            Some(g.below(1 << 40) as i64)
        }));
        let batches: Vec<RecordBatch> =
            (0..10).map(|i| whole.slice(i * 1000, 1000)).collect();
        let ordering = ordering(&batches[0].schema(), &[("k", false, false)]);
        let setup = Setup {
            // Twice the data, far below ten times every batch
            memory_limit: Some(2 * get_record_batch_memory_size(&whole) + 256 * 1024),
            sample_rows: 2000,
            ..Default::default()
        };
        let metrics = check(&setup, &batches, &ordering).await?;
        assert_eq!(metrics.spill_count(), Some(0));
        assert!(count(&metrics, "bucket_sort_sorted_buckets") > 0);
        Ok(())
    }

    /// Partition-like leading keys with few values, then keys close to unique
    fn partitioned_batches(batches: usize, rows: usize, seed: u64) -> Vec<RecordBatch> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("day", DataType::Utf8, true),
            Field::new("region", DataType::Int32, true),
            Field::new("x", DataType::Int64, true),
            Field::new("url", DataType::Utf8, true),
            Field::new("payload", DataType::Utf8, true),
        ]));
        let mut g = Gen::new(seed);
        (0..batches)
            .map(|_| {
                let day: StringArray = (0..rows)
                    .map(|_| {
                        (!g.chance(2)).then(|| format!("2024-01-{:02}", 10 + g.below(4)))
                    })
                    .collect();
                let region: Int32Array = (0..rows)
                    .map(|_| (!g.chance(2)).then(|| g.below(3) as i32))
                    .collect();
                let x: Int64Array = (0..rows)
                    .map(|_| (!g.chance(2)).then(|| g.below(1 << 20) as i64))
                    .collect();
                let url: StringArray = (0..rows)
                    .map(|_| {
                        (!g.chance(2)).then(|| {
                            let len = 40 + g.below(10) as usize;
                            g.string("https://www.example.com/items/", len)
                        })
                    })
                    .collect();
                let payload: StringArray =
                    (0..rows).map(|_| Some(g.string("p-", 80))).collect();
                RecordBatch::try_new(
                    Arc::clone(&schema),
                    vec![
                        Arc::new(day),
                        Arc::new(region),
                        Arc::new(x),
                        Arc::new(url),
                        Arc::new(payload),
                    ],
                )
                .unwrap()
            })
            .collect()
    }

    #[tokio::test]
    async fn multi_column_keys_with_partition_like_leading_keys() -> Result<()> {
        let batches = partitioned_batches(40, 1000, 59);
        let schema = batches[0].schema();
        let setup = Setup {
            target_bucket_bytes: 2 * 1024,
            ..Default::default()
        };
        for keys in [
            vec![
                ("region", false, false),
                ("x", false, false),
                ("url", false, false),
            ],
            vec![
                ("day", false, false),
                ("region", true, true),
                ("x", false, true),
            ],
            vec![
                ("day", true, true),
                ("region", false, false),
                ("url", true, false),
                ("x", false, false),
            ],
            vec![
                ("region", false, true),
                ("day", false, false),
                ("url", false, false),
            ],
        ] {
            let metrics = check(&setup, &batches, &ordering(&schema, &keys)).await?;
            assert!(
                count(&metrics, "bucket_sort_leading_key_constant") > 0,
                "{keys:?}"
            );
            assert!(count(&metrics, "bucket_sort_split_buckets") > 0, "{keys:?}");
            assert_eq!(
                count(&metrics, "bucket_sort_external_buckets"),
                0,
                "{keys:?}"
            );
            assert!(
                count(&metrics, "bucket_sort_key_dictionary") >= 1,
                "{keys:?}"
            );
            check(&Setup::default(), &batches, &ordering(&schema, &keys)).await?;
        }
        Ok(())
    }

    /// Sort order implies key order, and a code claiming a single value holds one,
    /// for packed keys of every kind
    #[test]
    fn keys_preserve_the_sort_order() -> Result<()> {
        let batch = concat(&partitioned_batches(4, 500, 71));
        let schema = batch.schema();
        // Also a float key and long shared prefixes
        let mut g = Gen::new(73);
        let f: Float64Array = (0..batch.num_rows())
            .map(|_| (!g.chance(5)).then(|| g.below(7) as f64 - 3.5))
            .collect();
        let mut fields = schema.fields().to_vec();
        fields.push(Arc::new(Field::new("f", DataType::Float64, true)));
        let mut columns = batch.columns().to_vec();
        columns.push(Arc::new(f));
        let batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)?;
        let schema = batch.schema();
        let names = ["day", "region", "x", "url", "f"];

        for case in 0..400 {
            let mut g = Gen::new(1000 + case);
            let levels = 1 + g.below(4) as usize;
            let keys: Vec<(&str, bool, bool)> = (0..levels)
                .map(|_| (names[g.below(5) as usize], g.chance(50), g.chance(50)))
                .collect();
            let ordering = ordering(&schema, &keys);
            // Dictionaries of some levels, from all the rows or from some only, so
            // values between dictionary values occur
            let dictionaries: Vec<Option<Arc<KeyDictionary>>> = ordering
                .iter()
                .map(|key| {
                    let mut values =
                        DistinctValues::new(&key.expr.data_type(&schema).unwrap())?;
                    if g.chance(30) {
                        return None;
                    }
                    let rows = if g.chance(50) { batch.num_rows() } else { 40 };
                    let array = key.expr.evaluate(&batch.slice(0, rows)).unwrap();
                    values
                        .add(array.into_array(rows).unwrap().as_ref())
                        .unwrap();
                    Some(Arc::new(values.into_dictionary()))
                })
                .collect();
            let Some(encoder) =
                KeyEncoder::try_new(&ordering, 0, &schema, &dictionaries)?
            else {
                continue;
            };
            let encoded = encoder.encode(&batch)?;
            let columns: Vec<SortColumn> = ordering
                .iter()
                .map(|e| e.evaluate_to_sort_column(&batch))
                .collect::<Result<_>>()?;
            let order = lexsort_to_indices(&columns, None)?;
            let first = make_comparator(
                columns[0].values.as_ref(),
                columns[0].values.as_ref(),
                ordering[0].options,
            )?;
            for pair in order.values().windows(2) {
                let (a, b) = (pair[0] as usize, pair[1] as usize);
                // Nulls of the first key are bucketed apart and keep no order
                if encoded.is_null(a) || encoded.is_null(b) {
                    continue;
                }
                let (ka, kb) = (encoded.values[a], encoded.values[b]);
                assert!(
                    ka <= kb,
                    "case {case} {keys:?}: rows {a} then {b} have keys {ka:x} > {kb:x}"
                );
                if encoder.level_constant(ka, kb) {
                    assert_eq!(
                        first(a, b),
                        Ordering::Equal,
                        "case {case} {keys:?}: rows {a}, {b}"
                    );
                }
            }
        }
        Ok(())
    }

    fn ordering_of(batches: &[RecordBatch], keys: &[(&str, bool, bool)]) -> LexOrdering {
        ordering(&batches[0].schema(), keys)
    }

    #[tokio::test]
    async fn holds_twice_the_rows_of_the_merge_sort_before_spilling() -> Result<()> {
        // Memory for about 1.2x the input: the merge sort reserves 2x what it holds,
        // so it spills; the bucket sort reserves what it holds plus one bucket's
        // working space, so it fits
        let batches = int_batches(40, 1000, 107, |g, _| Some(g.below(1 << 30) as i64));
        let ordering = ordering(&batches[0].schema(), &[("k", false, false)]);
        let setup = Setup {
            memory_limit: Some(memory_for(&batches, 0.6)),
            ..Default::default()
        };
        let merge = run(&setup, false, &batches, &ordering).await?;
        assert!(merge.metrics.spill_count().unwrap_or(0) > 0);
        let metrics = check(&setup, &batches, &ordering).await?;
        assert_eq!(metrics.spill_count().unwrap_or(0), 0);
        Ok(())
    }

    #[test]
    fn exact_quantiles_match_a_full_sort() {
        let mut g = Gen::new(113);
        for n in [1usize, 2, 7, 100, 1000, 12345] {
            for count in [1usize, 2, 3, 15, 63, 255] {
                let keys: Vec<u128> = (0..n)
                    .map(|_| {
                        if g.chance(30) {
                            7
                        } else {
                            g.below(1 << 20) as u128
                        }
                    })
                    .collect();
                let mut sorted = keys.clone();
                sorted.sort_unstable();
                let mut expected: Vec<u128> = (1..=count)
                    .map(|j| sorted[(j * n / (count + 1)).min(n - 1)])
                    .collect();
                expected.dedup();
                let mut work = keys.clone();
                assert_eq!(
                    exact_quantile_splitters(&mut work, count),
                    expected,
                    "n {n} count {count}"
                );
            }
        }
    }

    #[tokio::test]
    async fn spills_only_what_does_not_fit() -> Result<()> {
        // Memory for about 80% of the input
        let batches = int_batches(60, 1000, 127, |g, _| Some(g.below(1 << 30) as i64));
        let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        let ordering = ordering(&batches[0].schema(), &[("k", false, false)]);
        let setup = Setup {
            memory_limit: Some(memory_for(&batches, 0.4)),
            ..Default::default()
        };
        let merge = run(&setup, false, &batches, &ordering).await?;
        let metrics = check(&setup, &batches, &ordering).await?;
        let spilled = metrics.spilled_rows().unwrap_or(0);
        println!(
            "spilled rows: merge sort {:?}, bucket sort {spilled} of {rows}",
            merge.metrics.spilled_rows()
        );
        assert!(spilled > 0);
        assert!(spilled < rows / 2, "spilled {spilled} of {rows} rows");
        assert!(merge.metrics.spilled_rows().unwrap_or(0) >= rows / 2);
        assert_eq!(count(&metrics, "bucket_sort_spill_groups_external"), 0);
        Ok(())
    }

    #[tokio::test]
    async fn gather_mode_matches_the_merge_sort() -> Result<()> {
        // Forced on smaller rows, in memory and spilling
        let always = |memory: Option<usize>| Setup {
            mode: "gather",
            target_bucket_bytes: 16 * 1024,
            memory_limit: memory,
            ..Default::default()
        };
        let batches = int_batches(30, 1000, 131, |g, _| {
            (!g.chance(3)).then(|| g.below(1 << 30) as i64)
        });
        let ordering_k = ordering(&batches[0].schema(), &[("k", true, true)]);
        let metrics = check(&always(None), &batches, &ordering_k).await?;
        assert_eq!(count(&metrics, "bucket_sort_gather_mode"), 1);
        assert_eq!(count(&metrics, "bucket_sort_split_buckets"), 0);
        let metrics = check(
            &always(Some(memory_for(&batches, 0.3))),
            &batches,
            &ordering_k,
        )
        .await?;
        assert!(metrics.spill_count().unwrap() > 0);

        let batches = partitioned_batches(30, 1000, 137);
        let ordering_p = ordering_of(
            &batches,
            &[
                ("day", false, false),
                ("region", true, true),
                ("url", false, false),
            ],
        );
        check(&always(None), &batches, &ordering_p).await?;
        check(
            &always(Some(memory_for(&batches, 0.3))),
            &batches,
            &ordering_p,
        )
        .await?;

        // Auto mode, timing both: either way the output matches
        let schema = Arc::new(Schema::new(vec![
            Field::new("k", DataType::Int64, true),
            Field::new("blob", DataType::Utf8, false),
        ]));
        let mut g = Gen::new(139);
        let batches: Vec<RecordBatch> = (0..20)
            .map(|_| {
                let k: Int64Array =
                    (0..100).map(|_| Some(g.below(1 << 20) as i64)).collect();
                let blob: StringArray = (0..100)
                    .map(|_| Some(g.string("blob-", 24 * 1024)))
                    .collect();
                RecordBatch::try_new(
                    Arc::clone(&schema),
                    vec![Arc::new(k), Arc::new(blob)],
                )
                .unwrap()
            })
            .collect();
        let ordering_w = ordering(&schema, &[("k", false, false)]);
        let auto = Setup {
            mode: "auto",
            ..Default::default()
        };
        let metrics = check(&auto, &batches, &ordering_w).await?;
        assert!(
            metrics
                .sum_by_name("time_bucket_sort_probe_gather")
                .unwrap()
                .as_usize()
                > 0
        );
        let setup = Setup {
            memory_limit: Some(memory_for(&batches, 0.3)),
            ..auto
        };
        let metrics = check(&setup, &batches, &ordering_w).await?;
        assert!(metrics.spill_count().unwrap() > 0);
        Ok(())
    }

    #[tokio::test]
    async fn multi_column_keys_spilling() -> Result<()> {
        let batches = partitioned_batches(60, 1000, 61);
        let schema = batches[0].schema();
        let keys = [
            ("day", false, false),
            ("region", false, false),
            ("x", true, false),
        ];
        for fraction in [0.2, 0.35] {
            let setup = Setup {
                target_bucket_bytes: 16 * 1024,
                memory_limit: Some(memory_for(&batches, fraction)),
                ..Default::default()
            };
            let metrics = check(&setup, &batches, &ordering(&schema, &keys)).await?;
            assert!(metrics.spill_count().unwrap() > 0);
            // The partition-like keys pack into the key, so the rows spread over
            // every group and each fits in memory
            assert_eq!(count(&metrics, "bucket_sort_key_dictionary"), 2);
            assert!(count(&metrics, "bucket_sort_spill_groups_in_memory") > 32);
            assert_eq!(count(&metrics, "bucket_sort_spill_groups_external"), 0);
            assert_eq!(count(&metrics, "bucket_sort_external_buckets"), 0);
        }
        Ok(())
    }

    #[tokio::test]
    async fn string_keys_sharing_a_long_prefix_are_split() -> Result<()> {
        let batches = partitioned_batches(30, 1000, 67);
        let schema = batches[0].schema();
        let setup = Setup {
            bucket_count: 4,
            target_bucket_bytes: 16 * 1024,
            ..Default::default()
        };
        for keys in [
            vec![("url", false, false)],
            vec![("url", true, true), ("x", false, false)],
        ] {
            let metrics = check(&setup, &batches, &ordering(&schema, &keys)).await?;
            assert!(count(&metrics, "bucket_sort_split_buckets") > 0, "{keys:?}");
        }
        Ok(())
    }

    #[tokio::test]
    async fn presorted_spills_are_runs() -> Result<()> {
        let batches = int_batches(60, 1000, 53, |_, i| Some(i as i64));
        let ordering = ordering(&batches[0].schema(), &[("k", false, false)]);
        let setup = Setup {
            memory_limit: Some(memory_for(&batches, 0.3)),
            ..Default::default()
        };
        let metrics = check(&setup, &batches, &ordering).await?;
        assert!(metrics.spill_count().unwrap() > 0);
        assert_eq!(count(&metrics, "bucket_sort_presorted_input"), 1);
        assert_eq!(count(&metrics, "bucket_sort_sorted_buckets"), 0);

        // The order breaks after spilling sorted runs: they are merged with the buckets
        let mut broken = batches.clone();
        let tail = broken.split_off(45);
        broken.splice(5..5, tail);
        let metrics = check(&setup, &broken, &ordering).await?;
        assert!(metrics.spill_count().unwrap() > 0);
        assert_eq!(count(&metrics, "bucket_sort_presorted_input"), 0);
        Ok(())
    }
}
