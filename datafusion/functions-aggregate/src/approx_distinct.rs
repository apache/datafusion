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

//! Defines physical expressions that can evaluated at runtime during query execution

use crate::hyperloglog::{
    DEFAULT_HLL_P, HLL_HASH_STATE, HLL_P_MAX, HLL_P_MIN, HyperLogLog, count_from_hashes,
};
use arrow::array::{
    Array, ArrayRef, BinaryArray, BinaryBuilder, BooleanArray, PrimitiveArray,
    UInt64Array,
};
use arrow::buffer::NullBuffer;
use arrow::datatypes::{
    ArrowPrimitiveType, DataType, Date32Type, Date64Type, Decimal32Type, Decimal64Type,
    Decimal128Type, Decimal256Type, DurationMicrosecondType, DurationMillisecondType,
    DurationNanosecondType, DurationSecondType, Field, FieldRef, Int32Type, Int64Type,
    IntervalDayTimeType, IntervalMonthDayNanoType, IntervalUnit, IntervalYearMonthType,
    Time32MillisecondType, Time32SecondType, Time64MicrosecondType, Time64NanosecondType,
    TimeUnit, TimestampMicrosecondType, TimestampMillisecondType,
    TimestampNanosecondType, TimestampSecondType, UInt32Type, UInt64Type,
};
use datafusion_common::ScalarValue;
use datafusion_common::hash_utils::create_hashes;
use datafusion_common::{
    DataFusionError, Result, downcast_value, internal_datafusion_err, internal_err,
    not_impl_err,
};
use datafusion_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion_expr::utils::format_state_name;
use datafusion_expr::{
    Accumulator, AggregateUDFImpl, Documentation, EmitTo, GroupsAccumulator, Signature,
    Volatility,
};
use datafusion_functions_aggregate_common::aggregate::count_distinct::{
    Bitmap65536DistinctCountAccumulator, Bitmap65536DistinctCountAccumulatorI16,
    BoolArray256DistinctCountAccumulator, BoolArray256DistinctCountAccumulatorI8,
    BooleanDistinctCountAccumulator,
};
use datafusion_functions_aggregate_common::aggregate::groups_accumulator::nulls::filter_to_nulls;
use datafusion_functions_aggregate_common::noop_accumulator::NoopAccumulator;
use datafusion_macros::user_doc;
use std::fmt::{Debug, Formatter};
use std::hash::Hash;
use std::mem::{size_of, size_of_val};
use std::sync::Arc;

make_udaf_expr_and_func!(
    ApproxDistinct,
    approx_distinct,
    expression,
    "approximate number of distinct input values",
    approx_distinct_udaf
);

impl<T: Hash + ?Sized> From<&HyperLogLog<T>> for ScalarValue {
    fn from(v: &HyperLogLog<T>) -> ScalarValue {
        let values = v.as_ref().to_vec();
        ScalarValue::Binary(Some(values))
    }
}

impl<T: Hash + ?Sized> TryFrom<&[u8]> for HyperLogLog<T> {
    type Error = DataFusionError;
    fn try_from(v: &[u8]) -> Result<HyperLogLog<T>> {
        if v.is_empty() || !v.len().is_power_of_two() {
            return internal_err!(
                "approx_distinct: invalid HLL state length {} (must be a power of two)",
                v.len()
            );
        }
        let p = v.len().ilog2() as usize;
        if !(HLL_P_MIN..=HLL_P_MAX).contains(&p) {
            return internal_err!(
                "approx_distinct: HLL state length {} implies precision {} outside {}..={}",
                v.len(),
                p,
                HLL_P_MIN,
                HLL_P_MAX
            );
        }
        Ok(HyperLogLog::<T>::from_registers(v.to_vec()))
    }
}

impl<T: Hash + ?Sized> TryFrom<&ScalarValue> for HyperLogLog<T> {
    type Error = DataFusionError;
    fn try_from(v: &ScalarValue) -> Result<HyperLogLog<T>> {
        if let ScalarValue::Binary(Some(slice)) = v {
            slice.as_slice().try_into()
        } else {
            internal_err!(
                "Impossibly got invalid scalar value while converting to HyperLogLog"
            )
        }
    }
}

#[derive(Debug)]
struct ApproxDistinctBitmapWrapper<A: Accumulator> {
    inner: A,
}

impl<A: Accumulator> Accumulator for ApproxDistinctBitmapWrapper<A> {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        self.inner.update_batch(values)
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        match self.inner.evaluate()? {
            ScalarValue::Int64(Some(v)) => Ok(ScalarValue::UInt64(Some(v as u64))),
            other => internal_err!("unexpected: {other}"),
        }
    }

    fn size(&self) -> usize {
        self.inner.size()
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        self.inner.state()
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        self.inner.merge_batch(states)
    }
}

#[derive(Debug)]
struct HLLAccumulator {
    hll: HyperLogLog<u8>,
    hashes: Vec<u64>,
}

impl Default for HLLAccumulator {
    fn default() -> Self {
        Self::with_precision(DEFAULT_HLL_P)
    }
}

impl HLLAccumulator {
    pub fn with_precision(p: usize) -> Self {
        Self {
            hll: HyperLogLog::with_precision(p),
            hashes: Vec::new(),
        }
    }
}

impl Accumulator for HLLAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let array = values[0].as_ref();
        self.hashes.clear();
        self.hashes.resize(array.len(), 0);
        create_hashes([array], &HLL_HASH_STATE, &mut self.hashes)?;

        match array.logical_nulls() {
            None => {
                for &hash in &self.hashes {
                    self.hll.add_hashed(hash);
                }
            }
            Some(nulls) => {
                for row in 0..array.len() {
                    if nulls.is_valid(row) {
                        self.hll.add_hashed(self.hashes[row]);
                    }
                }
            }
        }
        Ok(())
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        assert_eq!(1, states.len(), "expect only 1 element in the states");
        let binary_array = downcast_value!(states[0], BinaryArray);
        for v in binary_array.iter() {
            let v = v.ok_or_else(|| {
                internal_datafusion_err!("Impossibly got empty binary array from states")
            })?;
            let other = v.try_into()?;
            self.hll.merge(&other);
        }
        Ok(())
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        let value = ScalarValue::from(&self.hll);
        Ok(vec![value])
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        Ok(ScalarValue::UInt64(Some(self.hll.count() as u64)))
    }

    fn size(&self) -> usize {
        size_of_val(self) + self.hashes.capacity() * size_of::<u64>()
    }
}

/// Specialize the numeric case for extra performance.
#[derive(Debug)]
struct NumericHLLAccumulator<T>
where
    T: ArrowPrimitiveType,
    T::Native: Hash,
{
    hll: HyperLogLog<T::Native>,
}

impl<T> Default for NumericHLLAccumulator<T>
where
    T: ArrowPrimitiveType,
    T::Native: Hash,
{
    fn default() -> Self {
        Self::with_precision(DEFAULT_HLL_P)
    }
}

impl<T> NumericHLLAccumulator<T>
where
    T: ArrowPrimitiveType,
    T::Native: Hash,
{
    pub fn with_precision(p: usize) -> Self {
        Self {
            hll: HyperLogLog::with_precision(p),
        }
    }
}

impl<T> Accumulator for NumericHLLAccumulator<T>
where
    T: ArrowPrimitiveType + Debug,
    T::Native: Hash,
{
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let array: &PrimitiveArray<T> = downcast_value!(values[0], PrimitiveArray, T);
        self.hll.extend(array.into_iter().flatten());
        Ok(())
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        assert_eq!(1, states.len(), "expect only 1 element in the states");
        let binary_array = downcast_value!(states[0], BinaryArray);
        for v in binary_array.iter() {
            let v = v.ok_or_else(|| {
                internal_datafusion_err!("Impossibly got empty binary array from states")
            })?;
            let other = v.try_into()?;
            self.hll.merge(&other);
        }
        Ok(())
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        let value = ScalarValue::from(&self.hll);
        Ok(vec![value])
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        Ok(ScalarValue::UInt64(Some(self.hll.count() as u64)))
    }

    fn size(&self) -> usize {
        size_of_val(self)
    }
}

/// Maximum number of distinct hashes kept in the sparse representation of a
/// per-group sketch before it is promoted to a dense [`HyperLogLog`].
///
/// A dense sketch occupies `2^p` bytes regardless of how
/// many values it has seen. The vast majority of groups in a high-cardinality
/// `GROUP BY` only observe a handful of distinct values, so keeping their state
/// as a small list of hashes saves a huge amount of memory (both while
/// aggregating and when serializing the partial state for the final phase).
const SPARSE_LIMIT: usize = 256;

/// Per-group HyperLogLog state used by [`HllGroupsAccumulator`].
///
/// Starts out as a compact list of the (deduplicated) hashes observed for the
/// group and only switches to a full dense [`HyperLogLog`] once it has seen more
/// than [`SPARSE_LIMIT`] distinct values. Folding the stored hashes into a dense
/// sketch produces exactly the same registers as adding the original values one
/// by one, so the cardinality estimate is identical to the per-group
/// [`Accumulator`] path.
#[derive(Clone, Debug)]
enum GroupHll {
    /// Distinct hashes seen so far. May contain duplicates between compactions.
    Sparse(Vec<u64>),
    Dense(Box<HyperLogLog<u8>>),
}

/// Fold a slice of pre-computed hashes into a fresh [`HyperLogLog`] sketch with
/// the given precision.
fn fold_sparse_to_hll(hashes: &[u64], p: usize) -> HyperLogLog<u8> {
    let mut hll = HyperLogLog::<u8>::with_precision(p);
    for &h in hashes {
        hll.add_hashed(h);
    }
    hll
}

impl GroupHll {
    fn new_sparse() -> Self {
        GroupHll::Sparse(Vec::new())
    }

    /// Add a pre-computed hash, returning the change in heap-allocated bytes so
    /// the accumulator can track its memory usage incrementally.
    #[inline]
    fn add_hash(&mut self, hash: u64, p: usize) -> isize {
        match self {
            GroupHll::Dense(hll) => {
                hll.add_hashed(hash);
                0
            }
            GroupHll::Sparse(v) => {
                let cap_before = v.capacity();
                v.push(hash);
                if v.len() >= 2 * SPARSE_LIMIT {
                    return self.compact_or_promote(cap_before, p);
                }
                ((v.capacity() - cap_before) * size_of::<u64>()) as isize
            }
        }
    }

    /// Deduplicate the sparse hash list and, if it still exceeds
    /// [`SPARSE_LIMIT`] distinct values, promote it to a dense sketch.
    #[cold]
    fn compact_or_promote(&mut self, cap_before: usize, p: usize) -> isize {
        let GroupHll::Sparse(v) = self else {
            return 0;
        };
        v.sort_unstable();
        v.dedup();
        if v.len() > SPARSE_LIMIT {
            let num_registers = 1_usize << p;
            // cap_before is the capacity already reflected in allocated_bytes.
            // Any reallocation caused by the triggering push was never counted and
            // is also freed here, so the two cancel out.
            *self = GroupHll::Dense(Box::new(fold_sparse_to_hll(v, p)));
            (num_registers as isize) - ((cap_before * size_of::<u64>()) as isize)
        } else {
            // Account for any Vec growth caused by the triggering push.
            // sort/dedup do not reallocate, so v.capacity() is the post-push capacity.
            ((v.capacity() - cap_before) * size_of::<u64>()) as isize
        }
    }

    /// Merge a serialized state (produced by [`Self::serialize`] or by the
    /// per-group [`Accumulator`]) into this sketch.
    fn merge_serialized(&mut self, bytes: &[u8], p: usize) -> Result<isize> {
        if bytes.is_empty() {
            return Ok(0);
        }
        let num_registers = 1_usize << p;
        if bytes.len() == num_registers {
            let other: HyperLogLog<u8> = bytes.try_into()?;
            Ok(self.merge_dense(&other, p))
        } else {
            if !bytes.len().is_multiple_of(size_of::<u64>()) {
                return internal_err!(
                    "approx_distinct: malformed sparse state: length {} is not a multiple of {}",
                    bytes.len(),
                    size_of::<u64>()
                );
            }
            if bytes.len() > SPARSE_LIMIT * size_of::<u64>() {
                return internal_err!(
                    "approx_distinct: malformed sparse state: length {} exceeds sparse limit {}",
                    bytes.len(),
                    SPARSE_LIMIT * size_of::<u64>()
                );
            }
            let mut delta = 0;
            for chunk in bytes.chunks_exact(size_of::<u64>()) {
                let h = u64::from_le_bytes(chunk.try_into().unwrap());
                delta += self.add_hash(h, p);
            }
            Ok(delta)
        }
    }

    /// Merge a dense sketch into this one, promoting to dense if necessary.
    fn merge_dense(&mut self, other: &HyperLogLog<u8>, p: usize) -> isize {
        match self {
            GroupHll::Dense(hll) => {
                hll.merge(other);
                0
            }
            GroupHll::Sparse(v) => {
                let cap_before = v.capacity();
                let mut hll = other.clone();
                for &h in v.iter() {
                    hll.add_hashed(h);
                }
                let num_registers = 1_usize << p;
                *self = GroupHll::Dense(Box::new(hll));
                (num_registers as isize) - ((cap_before * size_of::<u64>()) as isize)
            }
        }
    }

    /// The approximate number of distinct values seen by this group.
    fn count(&self, p: usize) -> u64 {
        match self {
            GroupHll::Dense(hll) => hll.count() as u64,
            // Estimate directly from the stored hashes; this produces exactly the
            // same value as folding them into a dense sketch but avoids
            // allocating and scanning a register array for every group.
            GroupHll::Sparse(v) => count_from_hashes(v, p) as u64,
        }
    }

    /// Heap bytes held by this sketch. Mirrors the deltas accrued in
    /// [`Self::add_hash`] / [`Self::merge_dense`] so emitting a group can
    /// precisely reverse them.
    fn heap_bytes(&self) -> usize {
        match self {
            GroupHll::Sparse(v) => v.capacity() * size_of::<u64>(),
            GroupHll::Dense(hll) => 1 << hll.precision(),
        }
    }

    /// Serialize the sketch into `scratch` (which is cleared first). A dense
    /// sketch is written as its raw register bytes (wire-compatible with the
    /// per-group [`Accumulator`]); a sparse sketch is written as its distinct
    /// hashes in little-endian order unless it has crossed [`SPARSE_LIMIT`],
    /// in which case it is emitted as dense state so the final merge path
    /// accepts it.
    fn serialize(&mut self, scratch: &mut Vec<u8>, p: usize) {
        scratch.clear();
        match self {
            GroupHll::Dense(hll) => {
                let registers: &[u8] = (**hll).as_ref();
                scratch.extend_from_slice(registers);
            }
            GroupHll::Sparse(v) => {
                v.sort_unstable();
                v.dedup();
                if v.len() > SPARSE_LIMIT {
                    scratch.extend_from_slice(fold_sparse_to_hll(v, p).as_ref());
                } else {
                    for &h in v.iter() {
                        scratch.extend_from_slice(&h.to_le_bytes());
                    }
                }
            }
        }
    }
}

/// A [`GroupsAccumulator`] for `approx_distinct` that keeps one adaptive
/// (sparse → dense) HyperLogLog sketch per group.
///
/// This is dramatically faster than the generic `GroupsAccumulatorAdapter`
/// fallback for high-cardinality `GROUP BY`s: it processes the whole input in a
/// single vectorized pass (no per-group `take`/slice and no dynamic dispatch),
/// and the sparse representation avoids allocating a 16 KiB sketch for every
/// group when most groups only see a few distinct values.
///
///
/// # Example
///
/// For `SELECT k, approx_distinct(v) FROM t GROUP BY k`, each group owns one
/// independent sketch:
///
/// ```text
/// group   state
/// a       Sparse([h1, h2, h3, h2])
/// b       Dense(HLL registers)
/// ...
/// ```
///
/// Group `a` has fewer than [`SPARSE_LIMIT`] distinct hashes, so it stays in
/// the sparse representation. Before emitting state or estimating the count, the
/// hash list is sorted and deduplicated to `[h1, h2, h3]`, then those hashes are
/// interpreted exactly as if they had been added to a dense [`HyperLogLog`].
///
/// Group `b` has crossed the sparse limit, so its hashes have already been
/// replayed into a dense sketch. New values for `b` update the dense registers
/// directly, and serialized state is the raw register bytes.
struct HllGroupsAccumulator {
    /// Per-group sketches, indexed by `group_index`.
    groups: Vec<GroupHll>,
    /// Incrementally maintained estimate of heap bytes used by `groups`.
    allocated_bytes: usize,
    /// Reused workspace for vectorized value hashing.
    hashes: Vec<u64>,
    /// HLL precision used by every group in this accumulator.
    p: usize,
}

impl Default for HllGroupsAccumulator {
    fn default() -> Self {
        Self::with_precision(DEFAULT_HLL_P)
    }
}

impl HllGroupsAccumulator {
    fn with_precision(p: usize) -> Self {
        Self {
            groups: Vec::new(),
            allocated_bytes: 0,
            hashes: Vec::new(),
            p,
        }
    }

    #[inline]
    fn ensure_groups(&mut self, total_num_groups: usize) {
        if total_num_groups > self.groups.len() {
            self.groups
                .resize_with(total_num_groups, GroupHll::new_sparse);
        }
    }

    #[inline]
    fn apply_delta(&mut self, delta: isize) {
        self.allocated_bytes =
            (self.allocated_bytes as isize).saturating_add(delta).max(0) as usize;
    }
}

impl GroupsAccumulator for HllGroupsAccumulator {
    fn update_batch(
        &mut self,
        values: &[ArrayRef],
        group_indices: &[usize],
        opt_filter: Option<&BooleanArray>,
        total_num_groups: usize,
    ) -> Result<()> {
        self.ensure_groups(total_num_groups);
        let array = values[0].as_ref();
        self.hashes.clear();
        self.hashes.resize(array.len(), 0);
        create_hashes([array], &HLL_HASH_STATE, &mut self.hashes)?;

        let p = self.p;
        let mut delta: isize = 0;
        // Pre-combine value-nulls and filter into one mask so the update loop
        // only visits rows that should affect the sketch.
        let filter_nulls = opt_filter.map(filter_to_nulls);
        let value_nulls = array.logical_nulls();
        let combined_nulls =
            NullBuffer::union(filter_nulls.as_ref(), value_nulls.as_ref());
        match combined_nulls {
            None => {
                for (row, &hash) in self.hashes.iter().enumerate() {
                    delta += self.groups[group_indices[row]].add_hash(hash, p);
                }
            }
            Some(nulls) => {
                for row in nulls.valid_indices() {
                    delta += self.groups[group_indices[row]].add_hash(self.hashes[row], p);
                }
            }
        }
        self.apply_delta(delta);
        Ok(())
    }

    fn merge_batch(
        &mut self,
        values: &[ArrayRef],
        group_indices: &[usize],
        total_num_groups: usize,
    ) -> Result<()> {
        self.ensure_groups(total_num_groups);
        let states = downcast_value!(values[0], BinaryArray);
        let p = self.p;
        let mut delta: isize = 0;
        for (row, &group_index) in group_indices.iter().enumerate() {
            if states.is_valid(row) {
                delta +=
                    self.groups[group_index].merge_serialized(states.value(row), p)?;
            }
        }
        self.apply_delta(delta);
        Ok(())
    }

    fn evaluate(&mut self, emit_to: EmitTo) -> Result<ArrayRef> {
        let groups = emit_to.take_needed(&mut self.groups);
        let p = self.p;
        let mut freed = 0;
        let counts: UInt64Array = groups
            .iter()
            .map(|g| {
                freed += g.heap_bytes();
                Some(g.count(p))
            })
            .collect();
        // The emitted groups have been removed; reclaim their tracked bytes.
        self.allocated_bytes = self.allocated_bytes.saturating_sub(freed);
        Ok(Arc::new(counts))
    }

    fn state(&mut self, emit_to: EmitTo) -> Result<Vec<ArrayRef>> {
        let mut groups = emit_to.take_needed(&mut self.groups);
        let p = self.p;
        let mut builder = BinaryBuilder::new();
        let mut scratch: Vec<u8> = Vec::new();
        let mut freed = 0;
        for g in groups.iter_mut() {
            freed += g.heap_bytes();
            g.serialize(&mut scratch, p);
            builder.append_value(&scratch);
        }
        // The emitted groups have been removed; reclaim their tracked bytes.
        self.allocated_bytes = self.allocated_bytes.saturating_sub(freed);
        Ok(vec![Arc::new(builder.finish())])
    }

    fn convert_to_state(
        &self,
        values: &[ArrayRef],
        opt_filter: Option<&BooleanArray>,
    ) -> Result<Vec<ArrayRef>> {
        assert_eq!(values.len(), 1, "single argument to convert_to_state");
        let array = values[0].as_ref();
        let mut hashes = vec![0; array.len()];
        create_hashes([array], &HLL_HASH_STATE, &mut hashes)?;

        let filter_nulls = opt_filter.map(filter_to_nulls);
        let value_nulls = array.logical_nulls();
        let combined_nulls =
            NullBuffer::union(filter_nulls.as_ref(), value_nulls.as_ref());

        let mut builder = BinaryBuilder::new();
        let mut scratch = Vec::new();
        for (row, hash) in hashes.into_iter().enumerate() {
            if combined_nulls
                .as_ref()
                .is_none_or(|nulls| nulls.is_valid(row))
            {
                scratch.clear();
                scratch.extend_from_slice(&hash.to_le_bytes());
                builder.append_value(&scratch);
            } else {
                builder.append_value([]);
            }
        }

        Ok(vec![Arc::new(builder.finish())])
    }

    fn supports_convert_to_state(&self) -> bool {
        true
    }

    fn size(&self) -> usize {
        self.groups.capacity() * size_of::<GroupHll>()
            + self.allocated_bytes
            + self.hashes.capacity() * size_of::<u64>()
    }
}

impl Debug for ApproxDistinct {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ApproxDistinct")
            .field("name", &self.name())
            .field("signature", &self.signature)
            .finish()
    }
}

impl Default for ApproxDistinct {
    fn default() -> Self {
        Self::new()
    }
}

#[user_doc(
    doc_section(label = "Approximate Functions"),
    description = "Returns the approximate number of distinct input values calculated using the HyperLogLog algorithm.",
    syntax_example = "approx_distinct(expression)",
    sql_example = r#"```sql
> SELECT approx_distinct(column_name) FROM table_name;
+-----------------------------------+
| approx_distinct(column_name)      |
+-----------------------------------+
| 42                                |
+-----------------------------------+
```"#,
    standard_argument(name = "expression",)
)]
#[derive(PartialEq, Eq, Hash)]
pub struct ApproxDistinct {
    signature: Signature,
    /// HLL register precision. Only used for types that take the HLL code path
    /// (i.e. not Boolean / small-int types, which use exact bitmap counting).
    hll_precision: usize,
}

impl ApproxDistinct {
    pub fn new() -> Self {
        Self::with_hll_precision(DEFAULT_HLL_P)
    }

    /// Creates an `ApproxDistinct` that uses HLL sketches with `2^p` registers.
    ///
    /// This only has effect for types that use the HLL accumulator path. Small
    /// integer and boolean types use exact bitmap counting regardless of this
    /// value. Valid range: `HLL_P_MIN..=HLL_P_MAX` (4..=18).
    pub fn with_hll_precision(p: usize) -> Self {
        assert!(
            (HLL_P_MIN..=HLL_P_MAX).contains(&p),
            "HLL precision must be in {HLL_P_MIN}..={HLL_P_MAX}, got {p}",
        );
        Self {
            signature: Signature::any(1, Volatility::Immutable),
            hll_precision: p,
        }
    }
}

#[cold]
fn get_fixed_domain_approx_accumulator(
    data_type: &DataType,
) -> Result<Box<dyn Accumulator>> {
    match data_type {
        DataType::Boolean => Ok(Box::new(ApproxDistinctBitmapWrapper {
            inner: BooleanDistinctCountAccumulator::new(),
        })),
        DataType::UInt8 => Ok(Box::new(ApproxDistinctBitmapWrapper {
            inner: BoolArray256DistinctCountAccumulator::new(),
        })),
        DataType::Int8 => Ok(Box::new(ApproxDistinctBitmapWrapper {
            inner: BoolArray256DistinctCountAccumulatorI8::new(),
        })),
        DataType::UInt16 => Ok(Box::new(ApproxDistinctBitmapWrapper {
            inner: Bitmap65536DistinctCountAccumulator::new(),
        })),
        DataType::Int16 => Ok(Box::new(ApproxDistinctBitmapWrapper {
            inner: Bitmap65536DistinctCountAccumulatorI16::new(),
        })),
        _ => internal_err!("unsupported small int type: {}", data_type),
    }
}

#[cold]
fn get_fixed_domain_state_field(
    name: &str,
    data_type: &DataType,
) -> Result<Vec<FieldRef>> {
    Ok(vec![
        Field::new_list(
            format_state_name(name, "approx_distinct"),
            Field::new_list_field(data_type.clone(), true),
            false,
        )
        .into(),
    ])
}

impl AggregateUDFImpl for ApproxDistinct {
    fn name(&self) -> &str {
        "approx_distinct"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _: &[DataType]) -> Result<DataType> {
        Ok(DataType::UInt64)
    }

    fn default_value(&self, _data_type: &DataType) -> Result<ScalarValue> {
        Ok(ScalarValue::UInt64(Some(0)))
    }

    fn is_nullable(&self) -> bool {
        false
    }

    fn state_fields(&self, args: StateFieldsArgs) -> Result<Vec<FieldRef>> {
        let data_type = args.input_fields[0].data_type();
        match data_type {
            DataType::Null => Ok(vec![
                Field::new(
                    format_state_name(args.name, self.name()),
                    DataType::Null,
                    true,
                )
                .into(),
            ]),
            DataType::Boolean
            | DataType::UInt8
            | DataType::Int8
            | DataType::UInt16
            | DataType::Int16 => get_fixed_domain_state_field(args.name, data_type),
            _ => Ok(vec![
                Field::new(
                    format_state_name(args.name, "hll_registers"),
                    DataType::Binary,
                    false,
                )
                .into(),
            ]),
        }
    }

    fn accumulator(&self, acc_args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        let data_type = acc_args.expr_fields[0].data_type();
        let p = self.hll_precision;

        // For primitive types, use specialized accumulators for better performance.
        let accumulator: Box<dyn Accumulator> = match data_type {
            DataType::Boolean
            | DataType::UInt8
            | DataType::Int8
            | DataType::UInt16
            | DataType::Int16 => {
                return get_fixed_domain_approx_accumulator(data_type);
            }
            DataType::UInt32 => Box::new(NumericHLLAccumulator::<UInt32Type>::with_precision(p)),
            DataType::UInt64 => Box::new(NumericHLLAccumulator::<UInt64Type>::with_precision(p)),
            DataType::Int32 => Box::new(NumericHLLAccumulator::<Int32Type>::with_precision(p)),
            DataType::Int64 => Box::new(NumericHLLAccumulator::<Int64Type>::with_precision(p)),
            DataType::Date32 => Box::new(NumericHLLAccumulator::<Date32Type>::with_precision(p)),
            DataType::Date64 => Box::new(NumericHLLAccumulator::<Date64Type>::with_precision(p)),
            DataType::Time32(TimeUnit::Second) => {
                Box::new(NumericHLLAccumulator::<Time32SecondType>::with_precision(p))
            }
            DataType::Time32(TimeUnit::Millisecond) => {
                Box::new(NumericHLLAccumulator::<Time32MillisecondType>::with_precision(p))
            }
            DataType::Time64(TimeUnit::Microsecond) => {
                Box::new(NumericHLLAccumulator::<Time64MicrosecondType>::with_precision(p))
            }
            DataType::Time64(TimeUnit::Nanosecond) => {
                Box::new(NumericHLLAccumulator::<Time64NanosecondType>::with_precision(p))
            }
            DataType::Timestamp(TimeUnit::Second, _) => {
                Box::new(NumericHLLAccumulator::<TimestampSecondType>::with_precision(p))
            }
            DataType::Timestamp(TimeUnit::Millisecond, _) => {
                Box::new(NumericHLLAccumulator::<TimestampMillisecondType>::with_precision(p))
            }
            DataType::Timestamp(TimeUnit::Microsecond, _) => {
                Box::new(NumericHLLAccumulator::<TimestampMicrosecondType>::with_precision(p))
            }
            DataType::Timestamp(TimeUnit::Nanosecond, _) => {
                Box::new(NumericHLLAccumulator::<TimestampNanosecondType>::with_precision(p))
            }
            DataType::Interval(IntervalUnit::YearMonth) => {
                Box::new(NumericHLLAccumulator::<IntervalYearMonthType>::with_precision(p))
            }
            DataType::Interval(IntervalUnit::DayTime) => {
                Box::new(NumericHLLAccumulator::<IntervalDayTimeType>::with_precision(p))
            }
            DataType::Interval(IntervalUnit::MonthDayNano) => {
                Box::new(NumericHLLAccumulator::<IntervalMonthDayNanoType>::with_precision(p))
            }
            DataType::Decimal32(_, _) => {
                Box::new(NumericHLLAccumulator::<Decimal32Type>::with_precision(p))
            }
            DataType::Decimal64(_, _) => {
                Box::new(NumericHLLAccumulator::<Decimal64Type>::with_precision(p))
            }
            DataType::Decimal128(_, _) => {
                Box::new(NumericHLLAccumulator::<Decimal128Type>::with_precision(p))
            }
            DataType::Decimal256(_, _) => {
                Box::new(NumericHLLAccumulator::<Decimal256Type>::with_precision(p))
            }
            DataType::Duration(TimeUnit::Second) => {
                Box::new(NumericHLLAccumulator::<DurationSecondType>::with_precision(p))
            }
            DataType::Duration(TimeUnit::Millisecond) => {
                Box::new(NumericHLLAccumulator::<DurationMillisecondType>::with_precision(p))
            }
            DataType::Duration(TimeUnit::Microsecond) => {
                Box::new(NumericHLLAccumulator::<DurationMicrosecondType>::with_precision(p))
            }
            DataType::Duration(TimeUnit::Nanosecond) => {
                Box::new(NumericHLLAccumulator::<DurationNanosecondType>::with_precision(p))
            }
            DataType::Utf8
            | DataType::LargeUtf8
            | DataType::Utf8View
            | DataType::Binary
            | DataType::BinaryView
            | DataType::FixedSizeBinary(_)
            | DataType::List(_)
            | DataType::LargeList(_)
            | DataType::FixedSizeList(_, _)
            | DataType::ListView(_)
            | DataType::LargeListView(_)
            | DataType::Map(_, _)
            | DataType::Struct(_)
            | DataType::Union(_, _)
            | DataType::LargeBinary => Box::new(HLLAccumulator::with_precision(p)),
            DataType::Null => {
                Box::new(NoopAccumulator::new(ScalarValue::UInt64(Some(0))))
            }
            other => {
                return not_impl_err!(
                    "Support for 'approx_distinct' for data type {other} is not implemented"
                );
            }
        };
        Ok(accumulator)
    }

    fn groups_accumulator_supported(&self, args: AccumulatorArgs) -> bool {
        is_hll_groups_type(args.expr_fields[0].data_type())
    }

    fn create_groups_accumulator(
        &self,
        args: AccumulatorArgs,
    ) -> Result<Box<dyn GroupsAccumulator>> {
        let data_type = args.expr_fields[0].data_type();
        if is_hll_groups_type(data_type) {
            Ok(Box::new(HllGroupsAccumulator::with_precision(self.hll_precision)))
        } else {
            not_impl_err!(
                "GroupsAccumulator for 'approx_distinct' is not implemented for data type {data_type}"
            )
        }
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.doc()
    }
}

/// Returns true for the data types backed by the HyperLogLog
/// [`HllGroupsAccumulator`]. The fixed-domain types (booleans / small ints) and
/// `Null` fall back to the per-group [`Accumulator`] path.
fn is_hll_groups_type(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::UInt32
            | DataType::UInt64
            | DataType::Int32
            | DataType::Int64
            | DataType::Date32
            | DataType::Date64
            | DataType::Time32(TimeUnit::Second)
            | DataType::Time32(TimeUnit::Millisecond)
            | DataType::Time64(TimeUnit::Microsecond)
            | DataType::Time64(TimeUnit::Nanosecond)
            | DataType::Timestamp(TimeUnit::Second, _)
            | DataType::Timestamp(TimeUnit::Millisecond, _)
            | DataType::Timestamp(TimeUnit::Microsecond, _)
            | DataType::Timestamp(TimeUnit::Nanosecond, _)
            | DataType::Interval(IntervalUnit::YearMonth)
            | DataType::Interval(IntervalUnit::DayTime)
            | DataType::Interval(IntervalUnit::MonthDayNano)
            | DataType::Decimal32(_, _)
            | DataType::Decimal64(_, _)
            | DataType::Decimal128(_, _)
            | DataType::Decimal256(_, _)
            | DataType::Duration(_)
            | DataType::Utf8
            | DataType::LargeUtf8
            | DataType::Utf8View
            | DataType::Binary
            | DataType::BinaryView
            | DataType::FixedSizeBinary(_)
            | DataType::LargeBinary
            | DataType::List(_)
            | DataType::LargeList(_)
            | DataType::FixedSizeList(_, _)
            | DataType::ListView(_)
            | DataType::LargeListView(_)
            | DataType::Map(_, _)
            | DataType::Struct(_)
            | DataType::Union(_, _)
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::hyperloglog::NUM_REGISTERS;
    use std::hash::BuildHasher;

    #[cfg(not(feature = "force_hash_collisions"))]
    mod real_hash_test {
        use super::*;
        use arrow::array::{
            AsArray, Decimal32Array, Decimal64Array, Decimal128Array, Decimal256Array,
            Int64Array, IntervalDayTimeArray, IntervalMonthDayNanoArray,
            IntervalYearMonthArray, StringViewArray,
        };
        use arrow::datatypes::{IntervalDayTime, IntervalMonthDayNano, i256};
        use std::sync::Arc;
        // A string longer than the 12-byte inline limit
        const LONG: &str = "this string is definitely longer than twelve bytes";

        fn distinct_count(acc: &mut HLLAccumulator) -> u64 {
            match acc.evaluate().unwrap() {
                ScalarValue::UInt64(Some(v)) => v,
                other => panic!("unexpected evaluate result: {other:?}"),
            }
        }

        fn assert_count_numerical_acc_and_group_acc<T>(array: ArrayRef, expected: u64)
        where
            T: ArrowPrimitiveType + Debug,
            T::Native: Hash,
        {
            assert!(
                is_hll_groups_type(array.data_type()),
                "{} should be groups-capable",
                array.data_type()
            );

            let mut acc = NumericHLLAccumulator::<T>::default();
            acc.update_batch(&[Arc::clone(&array)]).unwrap();
            let per_group_count = match acc.evaluate().unwrap() {
                ScalarValue::UInt64(Some(v)) => v,
                other => panic!("unexpected evaluate result: {other:?}"),
            };

            let group_indices = vec![0usize; array.len()];
            let mut acc = HllGroupsAccumulator::default();
            acc.update_batch(std::slice::from_ref(&array), &group_indices, None, 1)
                .unwrap();
            let groups_count = acc
                .evaluate(EmitTo::All)
                .unwrap()
                .as_any()
                .downcast_ref::<UInt64Array>()
                .unwrap()
                .value(0);

            assert_eq!(
                per_group_count,
                groups_count,
                "paths disagree for {}",
                array.data_type()
            );
            assert_eq!(
                per_group_count,
                expected,
                "wrong count for {}",
                array.data_type()
            );
        }

        #[test]
        fn decimal_support_numerical_acc_and_group_acc() {
            let decimal_32: ArrayRef = Arc::new(
                Decimal32Array::from(vec![
                    1i32,
                    2,
                    2,
                    3,
                    3,
                    3,
                    0,
                    0,
                    123_456_789,
                    999_999_999,
                    999_999_999,
                ])
                .with_precision_and_scale(9, 2)
                .unwrap(),
            );
            assert_count_numerical_acc_and_group_acc::<Decimal32Type>(decimal_32, 6);

            let decimal_64: ArrayRef = Arc::new(
                Decimal64Array::from(vec![
                    1i64,
                    2,
                    2,
                    3,
                    3,
                    3,
                    0,
                    0,
                    1_234_567_890_123,
                    9_999_999_999_999,
                    9_999_999_999_999,
                ])
                .with_precision_and_scale(18, 2)
                .unwrap(),
            );
            assert_count_numerical_acc_and_group_acc::<Decimal64Type>(decimal_64, 6);

            let decimal_128: ArrayRef = Arc::new(
                Decimal128Array::from(vec![
                    1i128,
                    2,
                    2,
                    3,
                    3,
                    3,
                    0,
                    0,
                    1_234_567_890,
                    9_999_999_999,
                    9_999_999_999,
                ])
                .with_precision_and_scale(38, 2)
                .unwrap(),
            );
            assert_count_numerical_acc_and_group_acc::<Decimal128Type>(decimal_128, 6);

            let big_256_a =
                i256::from_string("123456789012345678901234567890123456").unwrap();
            let big_256_b =
                i256::from_string("987654321098765432109876543210987654").unwrap();

            let decimal_256: ArrayRef = Arc::new(
                Decimal256Array::from(vec![
                    i256::from_i128(1),
                    i256::from_i128(2),
                    i256::from_i128(2),
                    i256::from_i128(3),
                    i256::from_i128(3),
                    i256::from_i128(3),
                    i256::from_i128(0),
                    i256::from_i128(0),
                    big_256_a,
                    big_256_b,
                    big_256_b,
                ])
                .with_precision_and_scale(40, 2)
                .unwrap(),
            );
            assert_count_numerical_acc_and_group_acc::<Decimal256Type>(decimal_256, 6);
        }

        #[test]
        fn interval_support_numerical_acc_and_group_acc() {
            let year_month: ArrayRef =
                Arc::new(IntervalYearMonthArray::from(vec![1, 2, 2, 3, 3, 3, 0, 0]));
            assert_count_numerical_acc_and_group_acc::<IntervalYearMonthType>(
                year_month, 4,
            );

            let day_time: ArrayRef = Arc::new(IntervalDayTimeArray::from(vec![
                IntervalDayTime::new(1, 0),
                IntervalDayTime::new(1, 0),
                IntervalDayTime::new(1, 5),
                IntervalDayTime::new(2, 0),
            ]));
            assert_count_numerical_acc_and_group_acc::<IntervalDayTimeType>(day_time, 3);

            let month_day_nano: ArrayRef =
                Arc::new(IntervalMonthDayNanoArray::from(vec![
                    IntervalMonthDayNano::new(1, 0, 0),
                    IntervalMonthDayNano::new(1, 0, 0),
                    IntervalMonthDayNano::new(1, 0, 5),
                    IntervalMonthDayNano::new(0, 2, 0),
                    IntervalMonthDayNano::new(0, 0, 0),
                ]));
            assert_count_numerical_acc_and_group_acc::<IntervalMonthDayNanoType>(
                month_day_nano,
                4,
            );
        }

        /// `approx_distinct(v) FILTER (WHERE nullable_bool)` — a NULL filter row
        /// must not be counted (null filter is treated the same as false).
        #[test]
        fn update_batch_nullable_filter_excludes_null_filter_rows() {
            let values: ArrayRef = Arc::new(Int64Array::from(vec![1i64, 2, 3, 4, 5]));
            // row 0: filter=true, row 1: filter=NULL, row 2: filter=false,
            // row 3: filter=NULL, row 4: filter=true
            let filter =
                BooleanArray::from(vec![Some(true), None, Some(false), None, Some(true)]);

            let mut acc = HllGroupsAccumulator::default();
            // put all rows in group 0
            let group_indices = vec![0usize; 5];
            acc.update_batch(&[values], &group_indices, Some(&filter), 1)
                .unwrap();

            // Only rows 0 and 4 (values 1 and 5) should be counted.
            let result = acc.evaluate(EmitTo::All).unwrap();
            let counts = result.as_any().downcast_ref::<UInt64Array>().unwrap();
            // reference: hash 1 and 5 into a dense sketch
            let expected = reference_count(&[h(1), h(5)]);
            assert_eq!(counts.value(0), expected);
        }

        #[test]
        fn groups_convert_to_state_roundtrips_through_merge() {
            let values: ArrayRef = Arc::new(Int64Array::from(vec![
                Some(1),
                Some(2),
                Some(2),
                None,
                Some(3),
            ]));
            let filter = BooleanArray::from(vec![
                Some(true),
                Some(true),
                Some(true),
                Some(true),
                None,
            ]);
            let group_indices = vec![0usize, 1, 0, 1, 0];

            let mut direct = HllGroupsAccumulator::default();
            direct
                .update_batch(
                    std::slice::from_ref(&values),
                    &group_indices,
                    Some(&filter),
                    2,
                )
                .unwrap();
            let direct = direct
                .evaluate(EmitTo::All)
                .unwrap()
                .as_any()
                .downcast_ref::<UInt64Array>()
                .unwrap()
                .clone();

            let converter = HllGroupsAccumulator::default();
            let state = converter
                .convert_to_state(std::slice::from_ref(&values), Some(&filter))
                .unwrap();
            assert_eq!(state[0].null_count(), 0);
            let mut merged = HllGroupsAccumulator::default();
            merged.merge_batch(&state, &group_indices, 2).unwrap();
            let merged = merged
                .evaluate(EmitTo::All)
                .unwrap()
                .as_any()
                .downcast_ref::<UInt64Array>()
                .unwrap()
                .clone();

            assert_eq!(direct, merged);
        }

        #[test]
        fn groups_convert_to_state_preserves_empty_and_filtered_rows() {
            let converter = HllGroupsAccumulator::default();
            let empty_values: ArrayRef =
                Arc::new(Int64Array::from(Vec::<Option<i64>>::new()));
            let state = converter
                .convert_to_state(std::slice::from_ref(&empty_values), None)
                .unwrap();
            assert_eq!(state[0].len(), 0);
            assert_eq!(state[0].null_count(), 0);

            let values: ArrayRef =
                Arc::new(Int64Array::from(vec![Some(1), Some(2), None]));
            let filter = BooleanArray::from(vec![Some(false), None, Some(false)]);
            let group_indices = vec![0usize, 1, 0];
            let state = converter
                .convert_to_state(std::slice::from_ref(&values), Some(&filter))
                .unwrap();
            assert_eq!(state[0].len(), values.len());
            assert_eq!(state[0].null_count(), 0);
            let state = state[0].as_any().downcast_ref::<BinaryArray>().unwrap();
            for row in 0..state.len() {
                assert_eq!(state.value(row), b"");
            }

            let mut merged = HllGroupsAccumulator::default();
            merged
                .merge_batch(&[Arc::new(state.clone())], &group_indices, 2)
                .unwrap();
            let result = merged
                .evaluate(EmitTo::All)
                .unwrap()
                .as_any()
                .downcast_ref::<UInt64Array>()
                .unwrap()
                .clone();
            assert_eq!(result, UInt64Array::from(vec![0, 0]));
        }

        /// Regression: a short (≤ 12-byte) Utf8View string must hash identically
        /// in an all-inline batch and in a mixed batch that also contains a long
        /// string (which forces a data buffer).
        #[test]
        fn utf8view_groups_short_string_hashed_consistently_across_batches() {
            // Batch 1: all-inline (no data buffers) — "aaa" is hashed as u128 view.
            let batch1: ArrayRef = Arc::new(StringViewArray::from(vec!["aaa", "bbb"]));
            assert!(batch1.as_string_view().data_buffers().is_empty());

            // Batch 2: mixed — LONG forces a data buffer; "aaa" must still be
            // hashed as u128 view so it matches its appearance in batch 1.
            let batch2: ArrayRef = Arc::new(StringViewArray::from(vec!["aaa", LONG]));
            assert!(!batch2.as_string_view().data_buffers().is_empty());

            let group_indices = vec![0usize, 0];
            let mut acc = HllGroupsAccumulator::default();
            acc.update_batch(&[batch1], &group_indices, None, 1)
                .unwrap();
            acc.update_batch(&[batch2], &group_indices, None, 1)
                .unwrap();

            // True distinct values: {"aaa", "bbb", LONG} == 3.
            let result = acc.evaluate(EmitTo::All).unwrap();
            let counts = result.as_any().downcast_ref::<UInt64Array>().unwrap();
            assert_eq!(counts.value(0), 3);
        }

        /// Regression: a short (≤ 12-byte) Utf8View string must hash identically
        /// regardless of which batch it appears in — all-inline or mixed.
        #[test]
        fn utf8view_acc_split_batches_match_single_mixed_batch() {
            // Multiset: {"aaa" x2, "bbb", LONG}, so 3 distinct values.
            let mixed: ArrayRef =
                Arc::new(StringViewArray::from(vec!["aaa", "bbb", LONG, "aaa"]));
            let mut acc_single = HLLAccumulator::default();
            acc_single.update_batch(&[mixed]).unwrap();

            // Same multiset, but split so "aaa" lands in both an all-inline batch
            // and a batch with a data buffer (forced by LONG).
            let inline_only: ArrayRef =
                Arc::new(StringViewArray::from(vec!["aaa", "bbb"]));
            let with_buffer: ArrayRef =
                Arc::new(StringViewArray::from(vec!["aaa", LONG]));
            assert!(inline_only.as_string_view().data_buffers().is_empty());
            assert!(!with_buffer.as_string_view().data_buffers().is_empty());

            let mut acc_split = HLLAccumulator::default();
            acc_split.update_batch(&[inline_only]).unwrap();
            acc_split.update_batch(&[with_buffer]).unwrap();

            assert_eq!(
                distinct_count(&mut acc_single),
                distinct_count(&mut acc_split)
            );
            assert_eq!(distinct_count(&mut acc_single), 3);
        }
    }

    fn h(v: u64) -> u64 {
        HLL_HASH_STATE.hash_one(v)
    }

    /// Reference count: fold the given distinct hashes straight into a dense
    /// HyperLogLog. The grouped sketch must agree with this exactly.
    fn reference_count(hashes: &[u64]) -> u64 {
        let mut hll = HyperLogLog::<u8>::new();
        for &hash in hashes {
            hll.add_hashed(hash);
        }
        hll.count() as u64
    }

    fn serialize(g: &mut GroupHll, p: usize) -> Vec<u8> {
        let mut buf = Vec::new();
        g.serialize(&mut buf, p);
        buf
    }

    #[test]
    fn sparse_stays_sparse_for_small_groups() {
        let p = DEFAULT_HLL_P;
        let mut g = GroupHll::new_sparse();
        let hashes: Vec<u64> = (0..50).map(h).collect();
        for &hash in &hashes {
            g.add_hash(hash, p);
        }
        // duplicates must not change the estimate or trigger promotion
        for &hash in &hashes {
            g.add_hash(hash, p);
        }
        assert!(
            matches!(g, GroupHll::Sparse(_)),
            "small group must be sparse"
        );
        assert_eq!(g.count(p), reference_count(&hashes));
        // sparse serialized state is far smaller than a dense sketch
        // and must not exceed the sparse limit contract enforced by merge_serialized
        let serialized = serialize(&mut g, p);
        assert!(serialized.len() < NUM_REGISTERS);
        assert!(serialized.len() <= SPARSE_LIMIT * size_of::<u64>());
    }

    #[test]
    fn promotes_to_dense_for_large_groups() {
        let p = DEFAULT_HLL_P;
        let mut g = GroupHll::new_sparse();
        let hashes: Vec<u64> = (0..(SPARSE_LIMIT as u64 * 4)).map(h).collect();
        for &hash in &hashes {
            g.add_hash(hash, p);
        }
        assert!(matches!(g, GroupHll::Dense(_)), "large group must be dense");
        assert_eq!(g.count(p), reference_count(&hashes));
    }

    #[test]
    fn serialize_then_merge_roundtrips() {
        let p = DEFAULT_HLL_P;
        for n in [0u64, 10, SPARSE_LIMIT as u64 * 4] {
            let hashes: Vec<u64> = (0..n).map(h).collect();
            let mut src = GroupHll::new_sparse();
            for &hash in &hashes {
                src.add_hash(hash, p);
            }
            let bytes = serialize(&mut src, p);
            let mut dst = GroupHll::new_sparse();
            dst.merge_serialized(&bytes, p).unwrap();
            assert_eq!(dst.count(p), reference_count(&hashes), "n = {n}");
        }
    }

    #[test]
    fn sparse_limit_group_serializes_as_mergeable_sparse_state() {
        let p = DEFAULT_HLL_P;
        let hashes: Vec<u64> = (0..SPARSE_LIMIT as u64).map(h).collect();
        let mut src = GroupHll::new_sparse();
        for &hash in &hashes {
            src.add_hash(hash, p);
        }
        assert!(matches!(src, GroupHll::Sparse(_)));

        let bytes = serialize(&mut src, p);
        assert_eq!(bytes.len(), SPARSE_LIMIT * size_of::<u64>());

        let mut dst = GroupHll::new_sparse();
        dst.merge_serialized(&bytes, p).unwrap();
        assert_eq!(dst.count(p), reference_count(&hashes));
    }

    #[test]
    fn medium_sparse_group_serializes_as_mergeable_dense_state() {
        let p = DEFAULT_HLL_P;
        let n = SPARSE_LIMIT as u64 + 44;
        let hashes: Vec<u64> = (0..n).map(h).collect();
        let mut src = GroupHll::new_sparse();
        for &hash in &hashes {
            src.add_hash(hash, p);
        }
        assert!(
            matches!(src, GroupHll::Sparse(_)),
            "group should not promote during update before the compaction threshold"
        );

        let bytes = serialize(&mut src, p);
        assert_eq!(bytes.len(), NUM_REGISTERS);

        let mut dst = GroupHll::new_sparse();
        dst.merge_serialized(&bytes, p).unwrap();
        assert_eq!(dst.count(p), reference_count(&hashes));
    }

    #[test]
    fn merge_combines_disjoint_groups() {
        let p = DEFAULT_HLL_P;
        // sparse + sparse, sparse + dense, dense + dense
        let left: Vec<u64> = (0..100).map(h).collect();
        let right: Vec<u64> = (100..(SPARSE_LIMIT as u64 * 4)).map(h).collect();
        let all: Vec<u64> = left.iter().chain(right.iter()).copied().collect();

        let mut a = GroupHll::new_sparse();
        for &hash in &left {
            a.add_hash(hash, p);
        }
        let mut b = GroupHll::new_sparse();
        for &hash in &right {
            b.add_hash(hash, p);
        }
        let b_bytes = serialize(&mut b, p);
        a.merge_serialized(&b_bytes, p).unwrap();
        assert_eq!(a.count(p), reference_count(&all));
    }

    #[test]
    fn empty_group_counts_zero() {
        let p = DEFAULT_HLL_P;
        let mut g = GroupHll::new_sparse();
        assert_eq!(g.count(p), 0);
        let bytes = serialize(&mut g, p);
        assert!(bytes.is_empty());
        let mut dst = GroupHll::new_sparse();
        dst.merge_serialized(&bytes, p).unwrap();
        assert_eq!(dst.count(p), 0);
    }

    // --- non-default precision tests ---

    #[test]
    fn group_hll_at_precision_12_serialize_roundtrip() {
        let p = 12;
        let hashes: Vec<u64> = (0..100).map(h).collect();
        let mut src = GroupHll::new_sparse();
        for &hash in &hashes {
            src.add_hash(hash, p);
        }
        let src_count = src.count(p);
        let bytes = serialize(&mut src, p);
        let mut dst = GroupHll::new_sparse();
        dst.merge_serialized(&bytes, p).unwrap();
        // After serialization and merge, count must agree exactly with pre-serialize count.
        assert_eq!(dst.count(p), src_count);
    }

    #[test]
    fn group_hll_dense_at_precision_12_serialize_roundtrip() {
        let p = 12;
        let hashes: Vec<u64> = (0..(SPARSE_LIMIT as u64 * 4)).map(h).collect();
        let mut src = GroupHll::new_sparse();
        for &hash in &hashes {
            src.add_hash(hash, p);
        }
        assert!(matches!(src, GroupHll::Dense(_)));
        let bytes = serialize(&mut src, p);
        assert_eq!(bytes.len(), 1 << p);
        let mut dst = GroupHll::new_sparse();
        dst.merge_serialized(&bytes, p).unwrap();
        assert_eq!(dst.count(p), src.count(p));
    }

    // --- TryFrom<&[u8]> error path tests ---

    #[test]
    fn try_from_empty_bytes_is_err() {
        let result = HyperLogLog::<u8>::try_from(&[][..]);
        assert!(result.is_err(), "empty slice must be rejected");
        let msg = result.unwrap_err().to_string();
        assert!(msg.contains("invalid HLL state length"), "got: {msg}");
    }

    #[test]
    fn try_from_non_power_of_two_is_err() {
        // 3 bytes is not a power of two
        let result = HyperLogLog::<u8>::try_from(&[0u8; 3][..]);
        assert!(result.is_err(), "non-power-of-two slice must be rejected");
        let msg = result.unwrap_err().to_string();
        assert!(msg.contains("invalid HLL state length"), "got: {msg}");
    }

    #[test]
    fn try_from_out_of_range_precision_is_err() {
        // 2 bytes => p = 1, which is below HLL_P_MIN (4)
        let result = HyperLogLog::<u8>::try_from(&[0u8; 2][..]);
        assert!(result.is_err(), "precision below min must be rejected");
        let msg = result.unwrap_err().to_string();
        assert!(msg.contains("implies precision"), "got: {msg}");
    }

    // --- ApproxDistinct with non-default precision ---

    use arrow::datatypes::{Schema, UnionFields, UnionMode};
    use std::sync::Arc;

    fn make_acc_args<'a>(
        schema: &'a Schema,
        return_field: &'a FieldRef,
        expr_field: &'a FieldRef,
    ) -> AccumulatorArgs<'a> {
        AccumulatorArgs {
            return_field: FieldRef::clone(return_field),
            schema,
            ignore_nulls: false,
            order_bys: &[],
            is_reversed: false,
            name: "approx_distinct",
            is_distinct: false,
            exprs: &[],
            expr_fields: std::slice::from_ref(expr_field),
        }
    }

    #[cfg(not(feature = "force_hash_collisions"))]
    #[test]
    fn approx_distinct_with_hll_precision_12_produces_correct_estimate() {
        use arrow::array::Int64Array;

        let func = ApproxDistinct::with_hll_precision(12);
        let values: ArrayRef = Arc::new(Int64Array::from_iter_values(0..1000i64));
        let expr_field: FieldRef =
            Arc::new(Field::new("v", DataType::Int64, false));
        let schema = Schema::new(vec![(*expr_field).clone()]);
        let return_field: FieldRef =
            Arc::new(Field::new("r", DataType::UInt64, false));
        let acc_args = make_acc_args(&schema, &return_field, &expr_field);
        let mut acc = func.accumulator(acc_args).unwrap();
        acc.update_batch(&[values]).unwrap();
        let result = acc.evaluate().unwrap();
        match result {
            ScalarValue::UInt64(Some(count)) => {
                // p=12 → ~1.6% error; allow 10% margin
                assert!(
                    count > 900 && count < 1100,
                    "count {count} out of expected range for p=12"
                );
            }
            other => panic!("unexpected result: {other:?}"),
        }
    }

    #[cfg(not(feature = "force_hash_collisions"))]
    #[test]
    fn approx_distinct_union_type_uses_hll_accumulator() {
        let union_type = DataType::Union(
            UnionFields::try_new(
                vec![0i8],
                vec![Field::new("a", DataType::Int32, false)],
            )
            .unwrap(),
            UnionMode::Dense,
        );
        let func = ApproxDistinct::new();
        let expr_field: FieldRef =
            Arc::new(Field::new("v", union_type.clone(), false));
        let schema = Schema::new(vec![(*expr_field).clone()]);
        let return_field: FieldRef =
            Arc::new(Field::new("r", DataType::UInt64, false));
        let acc_args = make_acc_args(&schema, &return_field, &expr_field);
        // Confirm the accumulator can be created and evaluated for Union type.
        let mut acc = func.accumulator(acc_args).unwrap();
        let result = acc.evaluate().unwrap();
        assert_eq!(result, ScalarValue::UInt64(Some(0)));
    }

    #[test]
    #[should_panic(expected = "HLL precision must be in")]
    fn with_hll_precision_out_of_range_panics() {
        let _ = ApproxDistinct::with_hll_precision(HLL_P_MAX + 1);
    }

    // merge_dense Sparse arm: a sparse group absorbs an incoming dense state.
    #[test]
    fn merge_dense_into_sparse_group_produces_dense() {
        let p = DEFAULT_HLL_P;
        // Build a dense sketch with some hashes.
        let hashes_a: Vec<u64> = (0..100).map(h).collect();
        let mut dense_src = GroupHll::new_sparse();
        for &hash in &hashes_a {
            dense_src.add_hash(hash, p);
        }
        // Force promotion to dense.
        let extra: Vec<u64> = (100..(SPARSE_LIMIT as u64 * 3)).map(h).collect();
        for &hash in &extra {
            dense_src.add_hash(hash, p);
        }
        assert!(matches!(dense_src, GroupHll::Dense(_)));
        let dense_bytes = serialize(&mut dense_src, p);

        // Target starts sparse with a few hashes.
        let hashes_b: Vec<u64> = (1000..1010).map(h).collect();
        let mut sparse_dst = GroupHll::new_sparse();
        for &hash in &hashes_b {
            sparse_dst.add_hash(hash, p);
        }
        assert!(matches!(sparse_dst, GroupHll::Sparse(_)));

        // Merging the dense state into the sparse group should promote it.
        sparse_dst.merge_serialized(&dense_bytes, p).unwrap();
        assert!(
            matches!(sparse_dst, GroupHll::Dense(_)),
            "sparse group must be dense after absorbing a dense state"
        );
    }

    // HllGroupsAccumulator at non-default precision: full update → state → merge → evaluate.
    #[cfg(not(feature = "force_hash_collisions"))]
    #[test]
    fn groups_accumulator_with_precision_12_roundtrips() {
        use arrow::array::Int64Array;

        let p = 12;
        let values: ArrayRef = Arc::new(Int64Array::from_iter_values(0..200i64));
        let group_indices = vec![0usize; 200];

        // Partial accumulator at p=12.
        let mut partial = HllGroupsAccumulator::with_precision(p);
        partial
            .update_batch(&[Arc::clone(&values)], &group_indices, None, 1)
            .unwrap();

        // Emit partial state and merge into a final accumulator at the same precision.
        let state = partial.state(EmitTo::All).unwrap();
        let mut final_acc = HllGroupsAccumulator::with_precision(p);
        final_acc
            .merge_batch(&state, &group_indices[..1], 1)
            .unwrap();

        let result = final_acc.evaluate(EmitTo::All).unwrap();
        let counts = result
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();
        let count = counts.value(0);
        // 200 distinct values, p=12 → ~1.6% error; allow 10% margin.
        assert!(count > 180 && count < 220, "count {count} out of range");
    }

    // NumericHLLAccumulator::with_precision propagates precision through evaluate.
    #[cfg(not(feature = "force_hash_collisions"))]
    #[test]
    fn numeric_hll_accumulator_with_precision_12() {
        use arrow::array::Int64Array;

        let values: ArrayRef = Arc::new(Int64Array::from_iter_values(0..500i64));
        let mut acc = NumericHLLAccumulator::<Int64Type>::with_precision(12);
        acc.update_batch(&[values]).unwrap();
        match acc.evaluate().unwrap() {
            ScalarValue::UInt64(Some(count)) => {
                assert!(count > 450 && count < 550, "count {count} out of range");
            }
            other => panic!("unexpected: {other:?}"),
        }
    }
}
