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

use crate::hyperloglog::{HLL_HASH_STATE, HyperLogLog, NUM_REGISTERS, count_from_hashes};
use arrow::array::{
    Array, ArrayRef, BinaryArray, BinaryBuilder, BooleanArray, PrimitiveArray,
    UInt64Array,
};
use arrow::buffer::NullBuffer;
use arrow::datatypes::{
    ArrowPrimitiveType, DataType, Date32Type, Date64Type, Decimal32Type, Decimal64Type,
    Decimal128Type, Decimal256Type, Field, FieldRef, Int32Type, Int64Type,
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
        let arr: [u8; 16384] = v.try_into().map_err(|_| {
            internal_datafusion_err!("Impossibly got invalid binary array from states")
        })?;
        Ok(HyperLogLog::<T>::new_with_registers(arr))
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

impl HLLAccumulator {
    pub fn new() -> Self {
        Self {
            hll: HyperLogLog::new(),
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

impl<T> NumericHLLAccumulator<T>
where
    T: ArrowPrimitiveType,
    T::Native: Hash,
{
    pub fn new() -> Self {
        Self {
            hll: HyperLogLog::new(),
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
/// A dense sketch always occupies [`NUM_REGISTERS`] (16 KiB) regardless of how
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

impl Default for GroupHll {
    fn default() -> Self {
        GroupHll::Sparse(Vec::new())
    }
}

/// Fold a slice of pre-computed hashes into a fresh [`HyperLogLog`] sketch.
fn fold_sparse_to_hll(hashes: &[u64]) -> HyperLogLog<u8> {
    let mut hll = HyperLogLog::<u8>::new();
    for &h in hashes {
        hll.add_hashed(h);
    }
    hll
}

impl GroupHll {
    /// Add a pre-computed hash, returning the change in heap-allocated bytes so
    /// the accumulator can track its memory usage incrementally.
    #[inline]
    fn add_hash(&mut self, hash: u64) -> isize {
        match self {
            GroupHll::Dense(hll) => {
                hll.add_hashed(hash);
                0
            }
            GroupHll::Sparse(v) => {
                let cap_before = v.capacity();
                v.push(hash);
                if v.len() >= 2 * SPARSE_LIMIT {
                    return self.compact_or_promote(cap_before);
                }
                ((v.capacity() - cap_before) * size_of::<u64>()) as isize
            }
        }
    }

    /// Deduplicate the sparse hash list and, if it still exceeds
    /// [`SPARSE_LIMIT`] distinct values, promote it to a dense sketch.
    #[cold]
    fn compact_or_promote(&mut self, cap_before: usize) -> isize {
        let GroupHll::Sparse(v) = self else {
            return 0;
        };
        v.sort_unstable();
        v.dedup();
        if v.len() > SPARSE_LIMIT {
            // cap_before is the capacity already reflected in allocated_bytes.
            // Any reallocation caused by the triggering push was never counted and
            // is also freed here, so the two cancel out.
            *self = GroupHll::Dense(Box::new(fold_sparse_to_hll(v)));
            (NUM_REGISTERS as isize) - ((cap_before * size_of::<u64>()) as isize)
        } else {
            // Account for any Vec growth caused by the triggering push.
            // sort/dedup do not reallocate, so v.capacity() is the post-push capacity.
            ((v.capacity() - cap_before) * size_of::<u64>()) as isize
        }
    }

    /// Merge a serialized state (produced by [`Self::serialize`] or by the
    /// per-group [`Accumulator`]) into this sketch.
    fn merge_serialized(&mut self, bytes: &[u8]) -> Result<isize> {
        if bytes.is_empty() {
            return Ok(0);
        }
        if bytes.len() == NUM_REGISTERS {
            let other: HyperLogLog<u8> = bytes.try_into()?;
            Ok(self.merge_dense(&other))
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
                delta += self.add_hash(h);
            }
            Ok(delta)
        }
    }

    /// Merge a dense sketch into this one, promoting to dense if necessary.
    fn merge_dense(&mut self, other: &HyperLogLog<u8>) -> isize {
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
                *self = GroupHll::Dense(Box::new(hll));
                (NUM_REGISTERS as isize) - ((cap_before * size_of::<u64>()) as isize)
            }
        }
    }

    /// The approximate number of distinct values seen by this group.
    fn count(&self) -> u64 {
        match self {
            GroupHll::Dense(hll) => hll.count() as u64,
            // Estimate directly from the stored hashes; this produces exactly the
            // same value as folding them into a dense sketch but avoids
            // allocating and scanning a 16 KiB register array for every group.
            GroupHll::Sparse(v) => count_from_hashes(v) as u64,
        }
    }

    /// Heap bytes held by this sketch. Mirrors the deltas accrued in
    /// [`Self::add_hash`] / [`Self::merge_dense`] so emitting a group can
    /// precisely reverse them.
    fn heap_bytes(&self) -> usize {
        match self {
            GroupHll::Sparse(v) => v.capacity() * size_of::<u64>(),
            GroupHll::Dense(_) => NUM_REGISTERS,
        }
    }

    /// Serialize the sketch into `scratch` (which is cleared first). A dense
    /// sketch is written as its raw [`NUM_REGISTERS`] registers (wire-compatible
    /// with the per-group [`Accumulator`]); a sparse sketch is written as its
    /// distinct hashes in little-endian order unless it has crossed
    /// [`SPARSE_LIMIT`], in which case it is emitted as dense state so the final
    /// merge path accepts it.
    fn serialize(&mut self, scratch: &mut Vec<u8>) {
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
                    scratch.extend_from_slice(fold_sparse_to_hll(v).as_ref());
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
/// directly, and serialized state is the raw [`NUM_REGISTERS`]-byte register
/// array.
struct HllGroupsAccumulator {
    /// Per-group sketches, indexed by `group_index`.
    groups: Vec<GroupHll>,
    /// Incrementally maintained estimate of heap bytes used by `groups`.
    allocated_bytes: usize,
    /// Reused workspace for vectorized value hashing.
    hashes: Vec<u64>,
}

impl HllGroupsAccumulator {
    fn new() -> Self {
        Self {
            groups: Vec::new(),
            allocated_bytes: 0,
            hashes: Vec::new(),
        }
    }

    #[inline]
    fn ensure_groups(&mut self, total_num_groups: usize) {
        if total_num_groups > self.groups.len() {
            self.groups.resize_with(total_num_groups, GroupHll::default);
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
                    delta += self.groups[group_indices[row]].add_hash(hash);
                }
            }
            Some(nulls) => {
                for row in nulls.valid_indices() {
                    delta += self.groups[group_indices[row]].add_hash(self.hashes[row]);
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
        let mut delta: isize = 0;
        for (row, &group_index) in group_indices.iter().enumerate() {
            if states.is_valid(row) {
                delta += self.groups[group_index].merge_serialized(states.value(row))?;
            }
        }
        self.apply_delta(delta);
        Ok(())
    }

    fn evaluate(&mut self, emit_to: EmitTo) -> Result<ArrayRef> {
        let groups = emit_to.take_needed(&mut self.groups);
        let mut freed = 0;
        let counts: UInt64Array = groups
            .iter()
            .map(|g| {
                freed += g.heap_bytes();
                Some(g.count())
            })
            .collect();
        // The emitted groups have been removed; reclaim their tracked bytes.
        self.allocated_bytes = self.allocated_bytes.saturating_sub(freed);
        Ok(Arc::new(counts))
    }

    fn state(&mut self, emit_to: EmitTo) -> Result<Vec<ArrayRef>> {
        let mut groups = emit_to.take_needed(&mut self.groups);
        let mut builder = BinaryBuilder::new();
        let mut scratch: Vec<u8> = Vec::new();
        let mut freed = 0;
        for g in groups.iter_mut() {
            freed += g.heap_bytes();
            g.serialize(&mut scratch);
            builder.append_value(&scratch);
        }
        // The emitted groups have been removed; reclaim their tracked bytes.
        self.allocated_bytes = self.allocated_bytes.saturating_sub(freed);
        Ok(vec![Arc::new(builder.finish())])
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
}

impl ApproxDistinct {
    pub fn new() -> Self {
        Self {
            signature: Signature::any(1, Volatility::Immutable),
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

        // For primitive types, use specialized accumulators for better performance.
        let accumulator: Box<dyn Accumulator> = match data_type {
            DataType::Boolean
            | DataType::UInt8
            | DataType::Int8
            | DataType::UInt16
            | DataType::Int16 => {
                return get_fixed_domain_approx_accumulator(data_type);
            }
            DataType::UInt32 => Box::new(NumericHLLAccumulator::<UInt32Type>::new()),
            DataType::UInt64 => Box::new(NumericHLLAccumulator::<UInt64Type>::new()),
            DataType::Int32 => Box::new(NumericHLLAccumulator::<Int32Type>::new()),
            DataType::Int64 => Box::new(NumericHLLAccumulator::<Int64Type>::new()),
            DataType::Date32 => Box::new(NumericHLLAccumulator::<Date32Type>::new()),
            DataType::Date64 => Box::new(NumericHLLAccumulator::<Date64Type>::new()),
            DataType::Time32(TimeUnit::Second) => {
                Box::new(NumericHLLAccumulator::<Time32SecondType>::new())
            }
            DataType::Time32(TimeUnit::Millisecond) => {
                Box::new(NumericHLLAccumulator::<Time32MillisecondType>::new())
            }
            DataType::Time64(TimeUnit::Microsecond) => {
                Box::new(NumericHLLAccumulator::<Time64MicrosecondType>::new())
            }
            DataType::Time64(TimeUnit::Nanosecond) => {
                Box::new(NumericHLLAccumulator::<Time64NanosecondType>::new())
            }
            DataType::Timestamp(TimeUnit::Second, _) => {
                Box::new(NumericHLLAccumulator::<TimestampSecondType>::new())
            }
            DataType::Timestamp(TimeUnit::Millisecond, _) => {
                Box::new(NumericHLLAccumulator::<TimestampMillisecondType>::new())
            }
            DataType::Timestamp(TimeUnit::Microsecond, _) => {
                Box::new(NumericHLLAccumulator::<TimestampMicrosecondType>::new())
            }
            DataType::Timestamp(TimeUnit::Nanosecond, _) => {
                Box::new(NumericHLLAccumulator::<TimestampNanosecondType>::new())
            }
            DataType::Interval(IntervalUnit::YearMonth) => {
                Box::new(NumericHLLAccumulator::<IntervalYearMonthType>::new())
            }
            DataType::Interval(IntervalUnit::DayTime) => {
                Box::new(NumericHLLAccumulator::<IntervalDayTimeType>::new())
            }
            DataType::Interval(IntervalUnit::MonthDayNano) => {
                Box::new(NumericHLLAccumulator::<IntervalMonthDayNanoType>::new())
            }
            DataType::Decimal32(_, _) => {
                Box::new(NumericHLLAccumulator::<Decimal32Type>::new())
            }
            DataType::Decimal64(_, _) => {
                Box::new(NumericHLLAccumulator::<Decimal64Type>::new())
            }
            DataType::Decimal128(_, _) => {
                Box::new(NumericHLLAccumulator::<Decimal128Type>::new())
            }
            DataType::Decimal256(_, _) => {
                Box::new(NumericHLLAccumulator::<Decimal256Type>::new())
            }
            DataType::Utf8
            | DataType::LargeUtf8
            | DataType::Utf8View
            | DataType::Binary
            | DataType::LargeBinary => Box::new(HLLAccumulator::new()),
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
            Ok(Box::new(HllGroupsAccumulator::new()))
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
            | DataType::Utf8
            | DataType::LargeUtf8
            | DataType::Utf8View
            | DataType::Binary
            | DataType::LargeBinary
    )
}

#[cfg(test)]
mod tests {
    use super::*;
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

            let mut acc = NumericHLLAccumulator::<T>::new();
            acc.update_batch(&[Arc::clone(&array)]).unwrap();
            let per_group_count = match acc.evaluate().unwrap() {
                ScalarValue::UInt64(Some(v)) => v,
                other => panic!("unexpected evaluate result: {other:?}"),
            };

            let group_indices = vec![0usize; array.len()];
            let mut acc = HllGroupsAccumulator::new();
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

            let mut acc = HllGroupsAccumulator::new();
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
            let mut acc = HllGroupsAccumulator::new();
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
            let mut acc_single = HLLAccumulator::new();
            acc_single.update_batch(&[mixed]).unwrap();

            // Same multiset, but split so "aaa" lands in both an all-inline batch
            // and a batch with a data buffer (forced by LONG).
            let inline_only: ArrayRef =
                Arc::new(StringViewArray::from(vec!["aaa", "bbb"]));
            let with_buffer: ArrayRef =
                Arc::new(StringViewArray::from(vec!["aaa", LONG]));
            assert!(inline_only.as_string_view().data_buffers().is_empty());
            assert!(!with_buffer.as_string_view().data_buffers().is_empty());

            let mut acc_split = HLLAccumulator::new();
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

    fn serialize(g: &mut GroupHll) -> Vec<u8> {
        let mut buf = Vec::new();
        g.serialize(&mut buf);
        buf
    }

    #[test]
    fn sparse_stays_sparse_for_small_groups() {
        let mut g = GroupHll::default();
        let hashes: Vec<u64> = (0..50).map(h).collect();
        for &hash in &hashes {
            g.add_hash(hash);
        }
        // duplicates must not change the estimate or trigger promotion
        for &hash in &hashes {
            g.add_hash(hash);
        }
        assert!(
            matches!(g, GroupHll::Sparse(_)),
            "small group must be sparse"
        );
        assert_eq!(g.count(), reference_count(&hashes));
        // sparse serialized state is far smaller than a dense 16 KiB sketch
        // and must not exceed the sparse limit contract enforced by merge_serialized
        let serialized = serialize(&mut g);
        assert!(serialized.len() < NUM_REGISTERS);
        assert!(serialized.len() <= SPARSE_LIMIT * size_of::<u64>());
    }

    #[test]
    fn promotes_to_dense_for_large_groups() {
        let mut g = GroupHll::default();
        let hashes: Vec<u64> = (0..(SPARSE_LIMIT as u64 * 4)).map(h).collect();
        for &hash in &hashes {
            g.add_hash(hash);
        }
        assert!(matches!(g, GroupHll::Dense(_)), "large group must be dense");
        assert_eq!(g.count(), reference_count(&hashes));
    }

    #[test]
    fn serialize_then_merge_roundtrips() {
        for n in [0u64, 10, SPARSE_LIMIT as u64 * 4] {
            let hashes: Vec<u64> = (0..n).map(h).collect();
            let mut src = GroupHll::default();
            for &hash in &hashes {
                src.add_hash(hash);
            }
            let bytes = serialize(&mut src);
            let mut dst = GroupHll::default();
            dst.merge_serialized(&bytes).unwrap();
            assert_eq!(dst.count(), reference_count(&hashes), "n = {n}");
        }
    }

    #[test]
    fn sparse_limit_group_serializes_as_mergeable_sparse_state() {
        let hashes: Vec<u64> = (0..SPARSE_LIMIT as u64).map(h).collect();
        let mut src = GroupHll::default();
        for &hash in &hashes {
            src.add_hash(hash);
        }
        assert!(matches!(src, GroupHll::Sparse(_)));

        let bytes = serialize(&mut src);
        assert_eq!(bytes.len(), SPARSE_LIMIT * size_of::<u64>());

        let mut dst = GroupHll::default();
        dst.merge_serialized(&bytes).unwrap();
        assert_eq!(dst.count(), reference_count(&hashes));
    }

    #[test]
    fn medium_sparse_group_serializes_as_mergeable_dense_state() {
        let n = SPARSE_LIMIT as u64 + 44;
        let hashes: Vec<u64> = (0..n).map(h).collect();
        let mut src = GroupHll::default();
        for &hash in &hashes {
            src.add_hash(hash);
        }
        assert!(
            matches!(src, GroupHll::Sparse(_)),
            "group should not promote during update before the compaction threshold"
        );

        let bytes = serialize(&mut src);
        assert_eq!(bytes.len(), NUM_REGISTERS);

        let mut dst = GroupHll::default();
        dst.merge_serialized(&bytes).unwrap();
        assert_eq!(dst.count(), reference_count(&hashes));
    }

    #[test]
    fn merge_combines_disjoint_groups() {
        // sparse + sparse, sparse + dense, dense + dense
        let left: Vec<u64> = (0..100).map(h).collect();
        let right: Vec<u64> = (100..(SPARSE_LIMIT as u64 * 4)).map(h).collect();
        let all: Vec<u64> = left.iter().chain(right.iter()).copied().collect();

        let mut a = GroupHll::default();
        for &hash in &left {
            a.add_hash(hash);
        }
        let mut b = GroupHll::default();
        for &hash in &right {
            b.add_hash(hash);
        }
        let b_bytes = serialize(&mut b);
        a.merge_serialized(&b_bytes).unwrap();
        assert_eq!(a.count(), reference_count(&all));
    }

    #[test]
    fn empty_group_counts_zero() {
        let mut g = GroupHll::default();
        assert_eq!(g.count(), 0);
        let bytes = serialize(&mut g);
        assert!(bytes.is_empty());
        let mut dst = GroupHll::default();
        dst.merge_serialized(&bytes).unwrap();
        assert_eq!(dst.count(), 0);
    }
}
