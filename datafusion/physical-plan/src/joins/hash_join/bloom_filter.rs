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

//! Bloom filter membership test over the build side keys of a hash join, for
//! the join's dynamic filter.
//!
//! Each build partition inserts the hash join hashes of its keys into its own
//! bloom filter ([`partition_filter`]), in its own build task. Once every
//! partition is done, one task ORs the partition filters into one filter
//! ([`Sbbf::union`]) and folds it to the target false positive probability.
//! Folding and union commute, so this is the filter of all build keys.

use std::fmt::{self, Display};
use std::hash::Hash;
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, BooleanArray};
use arrow::buffer::NullBuffer;
use arrow::datatypes::{DataType, Schema};
use arrow::record_batch::RecordBatch;
use datafusion_common::hash_utils::create_hashes;
use datafusion_common::{DataFusionError, Result, ScalarValue};
use datafusion_execution::memory_pool::{MemoryConsumer, MemoryPool, MemoryReservation};
use datafusion_expr::ColumnarValue;
use datafusion_expr_common::dyn_eq::DynHash;
use datafusion_physical_expr_common::physical_expr::{PhysicalExpr, PhysicalExprRef};
use parquet::bloom_filter::Sbbf;

use crate::joins::hash_join::exec::HASH_JOIN_SEED;
use crate::joins::key_range_bitmap::KeyRangeBitmap;

/// Sizing options of a [`BloomFilterExpr`].
#[derive(Debug, Clone, Copy)]
pub(crate) struct BloomFilterOptions {
    /// Target false positive probability.
    pub(crate) fpp: f64,
    /// Largest filter, in bytes. A build side that needs a larger filter at
    /// `fpp` gets no bloom filter.
    pub(crate) max_bytes: usize,
    /// Do not use the filter when its estimated false positive probability is
    /// above this value.
    pub(crate) max_fpp: f64,
}

impl BloomFilterOptions {
    /// Bytes of a filter for `num_distinct` keys at `fpp`, or `None` when
    /// that is more than `max_bytes`.
    fn bytes(&self, num_distinct: usize) -> Option<usize> {
        let bits = -8.0 * num_distinct as f64 / (1.0 - self.fpp.powf(1.0 / 8.0)).ln();
        let bytes = (bits / 8.0) as usize;
        (bytes <= self.max_bytes).then_some(bytes)
    }
}

fn parquet_err(e: parquet::errors::ParquetError) -> DataFusionError {
    DataFusionError::External(Box::new(e))
}

/// Rows hashed at a time when filling a filter.
const HASH_BATCH_SIZE: usize = 8192;

/// Hashes `keys` like the hash join and calls `f` with the hashes of each
/// batch of rows. A row with a NULL key gets no hash.
fn for_each_hash_batch(
    keys: &[ArrayRef],
    mut f: impl FnMut(&[u64]) -> Result<()>,
) -> Result<()> {
    let num_rows = keys.first().map_or(0, |k| k.len());
    let mut hashes = Vec::with_capacity(HASH_BATCH_SIZE.min(num_rows));
    for offset in (0..num_rows).step_by(HASH_BATCH_SIZE) {
        let len = HASH_BATCH_SIZE.min(num_rows - offset);
        let batch: Vec<ArrayRef> = keys.iter().map(|k| k.slice(offset, len)).collect();
        hashes.clear();
        hashes.resize(len, 0);
        create_hashes(&batch, HASH_JOIN_SEED.random_state(), &mut hashes)?;
        if let Some(valid) = valid_rows(&batch) {
            let mut num_valid = 0;
            for i in valid.valid_indices() {
                hashes[num_valid] = hashes[i];
                num_valid += 1;
            }
            hashes.truncate(num_valid);
        }
        f(&hashes)?;
    }
    Ok(())
}

/// The rows where no key is NULL, or `None` when no key has NULLs.
fn valid_rows(keys: &[ArrayRef]) -> Option<NullBuffer> {
    keys.iter()
        .filter_map(|k| k.logical_nulls())
        .reduce(|a, b| NullBuffer::union(Some(&a), Some(&b)).unwrap())
}

/// The bloom filter of the keys of one build partition, sized for
/// `num_distinct` keys (the estimated number of distinct keys of all
/// partitions, so that the partition filters can be ORed). `None` when that
/// filter would be larger than `options.max_bytes`.
///
/// The filter memory is charged to `reservation`. `None` also when it cannot
/// grow.
pub(crate) fn partition_filter(
    keys: &[ArrayRef],
    num_distinct: usize,
    options: BloomFilterOptions,
    reservation: &MemoryReservation,
) -> Result<Option<Sbbf>> {
    let Some(bytes) = options.bytes(num_distinct) else {
        return Ok(None);
    };
    // The size that `Sbbf::new_with_num_of_bytes` allocates
    if reservation
        .try_grow(bytes.max(32).next_power_of_two())
        .is_err()
    {
        return Ok(None);
    }
    let mut filter = Sbbf::new_with_num_of_bytes(bytes);
    for_each_hash_batch(keys, |hashes| {
        filter.insert_hashes(hashes);
        Ok(())
    })?;
    Ok(Some(filter))
}

/// The bloom filter of a [`BloomFilterExpr`] and the reservation for its
/// memory, which is released when the filter is dropped.
struct Filter {
    filter: Sbbf,
    _reservation: MemoryReservation,
}

/// ORs the filters of all build partitions and folds the result. Returns
/// `None` (no bloom filter) when `pool` cannot hold the filter or it is
/// saturated.
fn merge(
    partitions: &[&Sbbf],
    options: BloomFilterOptions,
    pool: &Arc<dyn MemoryPool>,
) -> Result<Option<Filter>> {
    // Partitions estimate the number of keys of all partitions from their own,
    // so their filters can differ in size: fold the larger ones into the
    // smallest.
    let Some((smallest, first)) = partitions
        .iter()
        .enumerate()
        .min_by_key(|(_, f)| f.num_blocks())
        .map(|(i, f)| (i, *f))
    else {
        return Ok(None);
    };
    let reservation = MemoryConsumer::new("HashJoinBloomFilter").register(pool);
    if reservation.try_grow(first.num_blocks() * 32).is_err() {
        return Ok(None);
    }
    let mut filter = first.clone();
    for (i, partition) in partitions.iter().enumerate() {
        if i != smallest {
            filter.union(partition).map_err(parquet_err)?;
        }
    }
    filter.fold_to_target_fpp(options.fpp);
    // A saturated filter costs CPU time and removes almost no rows
    if filter.estimated_fpp() > options.max_fpp {
        return Ok(None);
    }
    reservation.resize(filter.num_blocks() * 32);
    Ok(Some(Filter {
        filter,
        _reservation: reservation,
    }))
}

/// Physical expression that tests join keys against a bloom filter of the
/// build side keys of a hash join.
///
/// The result is `true` for a row whose keys may be on the build side, and
/// `false` for a row whose keys are definitely not on the build side (or are
/// NULL). The filter holds the hash join hashes of the keys (all key columns
/// together), so it supports every key type that the hash join supports.
pub struct BloomFilterExpr {
    /// Probe side join key expressions
    on_columns: Vec<PhysicalExprRef>,
    /// `None` when no bloom filter was built
    filter: Option<Arc<Filter>>,
    /// Which parts of the build side's key range hold keys, for container pruning
    pruning_bitmap: Option<Arc<KeyRangeBitmap>>,
}

impl BloomFilterExpr {
    /// ORs the bloom filters of every build partition ([`partition_filter`])
    /// into one filter, charged to `pool`.
    ///
    /// When no useful filter is possible (`partitions` is `None` because a
    /// partition has no filter, `pool` cannot hold the filter, or the filter
    /// is saturated), the expression has no filter: it is `true` for every
    /// row and only carries `pruning_bitmap` (see [`Self::has_filter`]).
    pub(crate) fn try_new(
        on_columns: Vec<PhysicalExprRef>,
        partitions: Option<&[&Sbbf]>,
        options: BloomFilterOptions,
        pool: &Arc<dyn MemoryPool>,
        pruning_bitmap: Option<Arc<KeyRangeBitmap>>,
    ) -> Result<Self> {
        let filter = match partitions {
            Some(partitions) => merge(partitions, options, pool)?.map(Arc::new),
            None => None,
        };
        Ok(Self {
            on_columns,
            filter,
            pruning_bitmap,
        })
    }

    /// Returns `true` if the expression tests rows against a bloom filter.
    /// Without a filter it only carries the pruning bitmap.
    pub fn has_filter(&self) -> bool {
        self.filter.is_some()
    }

    /// Which parts of the build side's key range hold keys, for container pruning.
    pub fn pruning_bitmap(&self) -> Option<&Arc<KeyRangeBitmap>> {
        self.pruning_bitmap.as_ref()
    }

    /// The probe side key expressions.
    pub fn on_columns(&self) -> &[PhysicalExprRef] {
        &self.on_columns
    }
}

impl fmt::Debug for BloomFilterExpr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let cols = self
            .on_columns
            .iter()
            .map(|e| e.to_string())
            .collect::<Vec<_>>()
            .join(", ");
        write!(f, "bloom_filter({cols})")
    }
}

impl Hash for BloomFilterExpr {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.on_columns.dyn_hash(state);
        self.filter.as_ref().map(Arc::as_ptr).hash(state);
    }
}

impl PartialEq for BloomFilterExpr {
    fn eq(&self, other: &Self) -> bool {
        self.on_columns == other.on_columns
            && match (&self.filter, &other.filter) {
                (Some(a), Some(b)) => Arc::ptr_eq(a, b),
                (None, None) => true,
                _ => false,
            }
    }
}

impl Eq for BloomFilterExpr {}

impl Display for BloomFilterExpr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.has_filter() {
            write!(f, "bloom_filter")
        } else {
            write!(f, "key_range_pruning")
        }
    }
}

impl PhysicalExpr for BloomFilterExpr {
    fn children(&self) -> Vec<&Arc<dyn PhysicalExpr>> {
        self.on_columns.iter().collect()
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn PhysicalExpr>>,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        Ok(Arc::new(Self {
            on_columns: children,
            filter: self.filter.clone(),
            pruning_bitmap: self.pruning_bitmap.clone(),
        }))
    }

    fn data_type(&self, _input_schema: &Schema) -> Result<DataType> {
        Ok(DataType::Boolean)
    }

    fn nullable(&self, _input_schema: &Schema) -> Result<bool> {
        Ok(false)
    }

    fn evaluate(&self, batch: &RecordBatch) -> Result<ColumnarValue> {
        let Some(filter) = &self.filter else {
            // No filter: only the pruning bitmap, which is not tested per row
            return Ok(ColumnarValue::Scalar(ScalarValue::Boolean(Some(true))));
        };
        let num_rows = batch.num_rows();
        let keys = self
            .on_columns
            .iter()
            .map(|c| c.evaluate(batch)?.into_array(num_rows))
            .collect::<Result<Vec<_>>>()?;
        let mut hashes = vec![0; num_rows];
        create_hashes(&keys, HASH_JOIN_SEED.random_state(), &mut hashes)?;
        let matches = filter.filter.check_hashes(&hashes);
        // A NULL key matches no build key
        let matches = match valid_rows(&keys) {
            Some(valid) => &matches & valid.inner(),
            None => matches,
        };
        Ok(ColumnarValue::Array(Arc::new(BooleanArray::new(
            matches, None,
        ))))
    }

    #[cfg(feature = "proto")]
    fn try_to_proto(
        &self,
        _ctx: &datafusion_physical_expr_common::physical_expr::proto_encode::PhysicalExprEncodeCtx<'_>,
    ) -> Result<Option<datafusion_proto_models::protobuf::PhysicalExprNode>> {
        use datafusion_proto_models::protobuf;
        use datafusion_proto_models::protobuf::physical_expr_node::ExprType;

        // Like `HashTableLookupExpr`, the expression holds runtime build side
        // state that is not serialized. It is replaced with lit(true), which
        // keeps every row: the dynamic filter is only an optimization. A plan
        // that is executed again calls reset_state(), and the join builds a new
        // dynamic filter at runtime.
        let Self {
            on_columns: _,
            filter: _,
            pruning_bitmap: _,
        } = self;
        let value = datafusion_proto_common::ScalarValue {
            value: Some(datafusion_proto_common::scalar_value::Value::BoolValue(
                true,
            )),
        };
        Ok(Some(protobuf::PhysicalExprNode {
            expr_id: None,
            expr_type: Some(ExprType::Literal(value)),
        }))
    }

    fn fmt_sql(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self}")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Float64Array, Int64Array, StringArray};
    use arrow::datatypes::Field;
    use datafusion_execution::memory_pool::{GreedyMemoryPool, UnboundedMemoryPool};
    use datafusion_physical_expr::expressions::Column;

    const OPTIONS: BloomFilterOptions = BloomFilterOptions {
        fpp: 0.01,
        max_bytes: 8 * 1024 * 1024,
        max_fpp: 0.15,
    };

    fn unbounded() -> Arc<dyn MemoryPool> {
        Arc::new(UnboundedMemoryPool::default())
    }

    /// The filters of `partitions`, each sized for the keys of all partitions.
    fn partition_filters(
        partitions: &[Vec<ArrayRef>],
        options: BloomFilterOptions,
    ) -> Option<Vec<Sbbf>> {
        let num_distinct = partitions.iter().map(|p| p[0].len()).sum();
        partitions
            .iter()
            .map(|keys| filter_of(keys, num_distinct, options))
            .collect()
    }

    fn filter_of(
        keys: &[ArrayRef],
        num_distinct: usize,
        options: BloomFilterOptions,
    ) -> Option<Sbbf> {
        let reservation = MemoryConsumer::new("test").register(&unbounded());
        partition_filter(keys, num_distinct, options, &reservation).unwrap()
    }

    fn expr_with(
        columns: Vec<PhysicalExprRef>,
        partitions: &[Vec<ArrayRef>],
        options: BloomFilterOptions,
        pool: &Arc<dyn MemoryPool>,
    ) -> Option<BloomFilterExpr> {
        let filters = partition_filters(partitions, options);
        let refs = filters.as_ref().map(|f| f.iter().collect::<Vec<_>>());
        let expr =
            BloomFilterExpr::try_new(columns, refs.as_deref(), options, pool, None)
                .unwrap();
        expr.has_filter().then_some(expr)
    }

    fn int_keys(keys: Vec<i64>) -> Vec<ArrayRef> {
        vec![Arc::new(Int64Array::from(keys))]
    }

    fn int_expr_with(
        partitions: Vec<Vec<i64>>,
        options: BloomFilterOptions,
        pool: &Arc<dyn MemoryPool>,
    ) -> Option<BloomFilterExpr> {
        let partitions: Vec<_> = partitions.into_iter().map(int_keys).collect();
        expr_with(
            vec![Arc::new(Column::new("k", 0))],
            &partitions,
            options,
            pool,
        )
    }

    fn int_expr(partitions: Vec<Vec<i64>>) -> BloomFilterExpr {
        int_expr_with(partitions, OPTIONS, &unbounded()).unwrap()
    }

    fn int_batch(keys: Vec<Option<i64>>) -> RecordBatch {
        let schema = Schema::new(vec![Field::new("k", DataType::Int64, true)]);
        RecordBatch::try_new(Arc::new(schema), vec![Arc::new(Int64Array::from(keys))])
            .unwrap()
    }

    fn eval(expr: &BloomFilterExpr, batch: &RecordBatch) -> Vec<bool> {
        let result = expr
            .evaluate(batch)
            .unwrap()
            .into_array(batch.num_rows())
            .unwrap();
        let result = result.as_any().downcast_ref::<BooleanArray>().unwrap();
        result.iter().map(|v| v.unwrap()).collect()
    }

    #[test]
    fn finds_build_keys_and_rejects_others() {
        let expr = int_expr(vec![(0..1000).collect(), (5000..6000).collect()]);
        let batch = int_batch((0..10_000).map(Some).collect());
        let result = eval(&expr, &batch);
        // No false negatives
        for k in (0..1000).chain(5000..6000) {
            assert!(result[k], "key {k} dropped");
        }
        // Few false positives
        let false_positives = result.iter().filter(|v| **v).count() - 2000;
        assert!(false_positives < 200, "{false_positives} false positives");
    }

    #[test]
    fn null_keys_do_not_match() {
        // A NULL build key is not inserted, a NULL probe key does not match
        let build: ArrayRef = Arc::new(Int64Array::from(vec![Some(1), None, Some(3)]));
        let expr = expr_with(
            vec![Arc::new(Column::new("k", 0))],
            &[vec![build]],
            OPTIONS,
            &unbounded(),
        )
        .unwrap();
        let batch = int_batch(vec![Some(1), None, Some(3)]);
        assert_eq!(eval(&expr, &batch), vec![true, false, true]);
    }

    /// ORing the partition filters gives the filter of all keys.
    #[test]
    fn merged_filter_matches_one_filter_of_all_keys() {
        let partitions: Vec<Vec<i64>> = (0..8)
            .map(|p| (0..10_000).map(|i| (i * 7919 + p) % 1_000_000).collect())
            .collect();
        let expr = int_expr(partitions.clone());
        let all: Vec<i64> = partitions.into_iter().flatten().collect();
        let mut expected = filter_of(&int_keys(all.clone()), all.len(), OPTIONS).unwrap();
        expected.fold_to_target_fpp(OPTIONS.fpp);
        let (mut a, mut b) = (vec![], vec![]);
        expected.write_bitset(&mut a).unwrap();
        let filter = &expr.filter.as_ref().unwrap().filter;
        filter.write_bitset(&mut b).unwrap();
        assert_eq!(a, b);
    }

    /// Partition filters of different sizes (from different estimates of the
    /// number of keys) are folded into the smallest.
    #[test]
    fn partition_filters_of_different_sizes() {
        let small = filter_of(&int_keys((0..100).collect()), 1000, OPTIONS).unwrap();
        let large =
            filter_of(&int_keys((1000..1100).collect()), 100_000, OPTIONS).unwrap();
        assert!(large.num_blocks() > small.num_blocks());
        let expr = BloomFilterExpr::try_new(
            vec![Arc::new(Column::new("k", 0))],
            Some(&[&large, &small]),
            OPTIONS,
            &unbounded(),
            None,
        )
        .unwrap();
        let result = eval(&expr, &int_batch((0..1100).map(Some).collect()));
        assert!(result[..100].iter().all(|v| *v));
        assert!(result[1000..].iter().all(|v| *v));
    }

    /// A build side that needs a filter larger than `max_bytes` at the target
    /// fpp gets no bloom filter: it is too big to be useful.
    #[test]
    fn too_big_build_side_has_no_filter() {
        let options = BloomFilterOptions {
            max_bytes: 1024,
            ..OPTIONS
        };
        assert!(
            int_expr_with(vec![(0..10_000).collect()], options, &unbounded()).is_none()
        );
        assert!(int_expr_with(vec![(0..100).collect()], options, &unbounded()).is_some());
    }

    /// A saturated filter is not used.
    #[test]
    fn saturated_filter_is_not_used() {
        // Sized for 10 keys, filled with 10k keys
        let keys = int_keys((0..10_000).collect());
        let filter = filter_of(&keys, 10, OPTIONS).unwrap();
        let expr = BloomFilterExpr::try_new(
            vec![Arc::new(Column::new("k", 0))],
            Some(&[&filter]),
            OPTIONS,
            &unbounded(),
            None,
        )
        .unwrap();
        assert!(!expr.has_filter());
        assert_eq!(expr.to_string(), "key_range_pruning");
        let batch = int_batch(vec![Some(-1)]);
        assert_eq!(eval(&expr, &batch), vec![true]);
    }

    /// The filter memory is charged to the pool and released on drop.
    #[test]
    fn memory_is_reserved_and_released() {
        let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(64 << 20));
        let expr = int_expr_with(vec![(0..100_000).collect()], OPTIONS, &pool).unwrap();
        let built = &expr.filter.as_ref().unwrap().filter;
        assert_eq!(pool.reserved(), built.num_blocks() * 32);
        drop(expr);
        assert_eq!(pool.reserved(), 0);
    }

    /// When the pool cannot hold the filter, no filter is built. The query
    /// does not fail.
    #[test]
    fn small_memory_pool_skips_the_filter() {
        let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(1024));
        assert!(int_expr_with(vec![(0..100_000).collect()], OPTIONS, &pool).is_none());
        assert_eq!(pool.reserved(), 0);
    }

    #[test]
    fn multi_column_keys() {
        let keys = vec![
            Arc::new(Int64Array::from(vec![1, 2])) as ArrayRef,
            Arc::new(StringArray::from(vec!["a", "b"])) as ArrayRef,
        ];
        let expr = expr_with(
            vec![Arc::new(Column::new("k", 0)), Arc::new(Column::new("s", 1))],
            &[keys],
            OPTIONS,
            &unbounded(),
        )
        .unwrap();
        let schema = Schema::new(vec![
            Field::new("k", DataType::Int64, true),
            Field::new("s", DataType::Utf8, true),
        ]);
        let batch = RecordBatch::try_new(
            Arc::new(schema),
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 1, 99])),
                Arc::new(StringArray::from(vec!["a", "b", "zzz", "a"])),
            ],
        )
        .unwrap();
        assert_eq!(eval(&expr, &batch), vec![true, true, false, false]);
    }

    /// Any key type the hash join hashes is supported, floats included.
    #[test]
    fn float_keys() {
        let keys: ArrayRef = Arc::new(Float64Array::from(vec![1.5, 2.5]));
        let expr = expr_with(
            vec![Arc::new(Column::new("f", 0))],
            &[vec![keys]],
            OPTIONS,
            &unbounded(),
        )
        .unwrap();
        let schema = Schema::new(vec![Field::new("f", DataType::Float64, true)]);
        let batch = RecordBatch::try_new(
            Arc::new(schema),
            vec![Arc::new(Float64Array::from(vec![1.5, 2.5, 3.5]))],
        )
        .unwrap();
        assert_eq!(eval(&expr, &batch), vec![true, true, false]);
    }

    /// The filter holds runtime build side state, so it serializes as lit(true).
    #[cfg(feature = "proto")]
    #[test]
    fn serializes_as_literal_true() {
        use datafusion_common::internal_datafusion_err;
        use datafusion_physical_expr_common::physical_expr::proto_encode::{
            PhysicalExprEncode, PhysicalExprEncodeCtx,
        };
        use datafusion_proto_models::protobuf;

        struct TestEncoder;
        impl PhysicalExprEncode for TestEncoder {
            fn encode(
                &self,
                expr: &Arc<dyn PhysicalExpr>,
            ) -> Result<protobuf::PhysicalExprNode> {
                let ctx = PhysicalExprEncodeCtx::new(self);
                expr.try_to_proto(&ctx)?
                    .ok_or_else(|| internal_datafusion_err!("cannot encode {expr:?}"))
            }
        }

        let expr: Arc<dyn PhysicalExpr> = Arc::new(int_expr(vec![(0..1000).collect()]));
        let proto = TestEncoder.encode(&expr).unwrap();
        let Some(protobuf::physical_expr_node::ExprType::Literal(value)) =
            proto.expr_type
        else {
            panic!("expected a literal, got {proto:?}");
        };
        assert_eq!(
            value.value,
            Some(datafusion_proto_common::scalar_value::Value::BoolValue(
                true
            ))
        );
    }
}
