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
//! Build: each build partition hashes its keys and sorts the hashes (see
//! [`SortedKeyHashes`]). At the barrier one task allocates the filter, inserts
//! the sorted hashes of all partitions (so the writes go through the filter in
//! order), and folds the filter.

use std::fmt::{self, Display};
use std::hash::Hash;
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, BooleanArray};
use arrow::buffer::BooleanBuffer;
use arrow::datatypes::{DataType, Schema};
use arrow::record_batch::RecordBatch;
use datafusion_common::{DataFusionError, Result, ScalarValue};
use datafusion_execution::memory_pool::{MemoryConsumer, MemoryPool, MemoryReservation};
use datafusion_expr::ColumnarValue;
use datafusion_expr_common::dyn_eq::DynHash;
use datafusion_physical_expr_common::physical_expr::{PhysicalExpr, PhysicalExprRef};
use parquet::bloom_filter::Sbbf;

use crate::joins::key_range_bitmap::KeyRangeBitmap;

/// Sizing options of a [`BloomFilterExpr`].
#[derive(Debug, Clone, Copy)]
pub(crate) struct BloomFilterOptions {
    /// Target false positive probability.
    pub(crate) fpp: f64,
    /// Largest filter, in bytes, per key column. A build side that needs a
    /// larger filter at `fpp` gets no bloom filter.
    pub(crate) max_bytes: usize,
    /// Do not use a column's filter when its estimated false positive
    /// probability is above this value.
    pub(crate) max_fpp: f64,
}

impl BloomFilterOptions {
    /// Returns `true` if a filter for `num_distinct` keys at `fpp` fits in
    /// `max_bytes`.
    pub(crate) fn fits(&self, num_distinct: usize) -> bool {
        bytes_for_ndv_fpp(num_distinct, self.fpp) <= self.max_bytes
    }
}

/// Returns `true` if a [`BloomFilterExpr`] can test keys of `data_type`.
pub(crate) fn supports_data_type(data_type: &DataType) -> bool {
    Sbbf::supports_data_type(data_type)
}

/// Number of bytes for a filter with `ndv` distinct values at `fpp`
/// (the size that `Sbbf::new_with_ndv_fpp` allocates before rounding).
fn bytes_for_ndv_fpp(ndv: usize, fpp: f64) -> usize {
    let bits = -8.0 * ndv as f64 / (1.0 - fpp.powf(1.0 / 8.0)).ln();
    (bits / 8.0) as usize
}

/// Bytes that `Sbbf::new_with_ndv_fpp` allocates (a power of two, at least 32).
fn filter_bytes(ndv: usize, fpp: f64) -> usize {
    bytes_for_ndv_fpp(ndv, fpp).max(32).next_power_of_two()
}

fn parquet_err(e: parquet::errors::ParquetError) -> DataFusionError {
    DataFusionError::External(Box::new(e))
}

/// The bloom filter hashes of the keys of one build partition: one sorted,
/// deduplicated vector per key column. NULL keys are not included.
///
/// The block of a hash in the filter is a monotonic function of its upper 32
/// bits, whatever the size of the filter. So hashes sorted by value are also
/// sorted by block, and inserting them writes the filter in order.
#[derive(Debug, Default)]
pub(crate) struct SortedKeyHashes {
    columns: Vec<Vec<u64>>,
}

impl SortedKeyHashes {
    /// Hashes `keys`. Returns `None` when a key type is not supported.
    pub(crate) fn try_new(keys: &[ArrayRef]) -> Result<Option<Self>> {
        if !keys.iter().all(|k| supports_data_type(k.data_type())) {
            return Ok(None);
        }
        let mut hashes = vec![];
        let columns = keys
            .iter()
            .map(|array| {
                Sbbf::hash_array(array.as_ref(), &mut hashes).map_err(parquet_err)?;
                let mut column: Vec<u64> = match array.logical_nulls() {
                    Some(nulls) => nulls.valid_indices().map(|i| hashes[i]).collect(),
                    None => hashes.clone(),
                };
                column.sort_unstable();
                column.dedup();
                Ok(column)
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(Some(Self { columns }))
    }

    /// Bytes that `try_new` allocates for `keys`, for the memory reservation
    /// (before the hashes are computed).
    pub(crate) fn estimated_size(keys: &[ArrayRef]) -> usize {
        keys.iter().map(|k| k.len() * size_of::<u64>()).sum::<usize>()
            // `try_new` holds the unsorted hashes of one column at the same time
            + keys.first().map_or(0, |k| k.len() * size_of::<u64>())
    }

    /// Bytes held.
    pub(crate) fn size(&self) -> usize {
        self.columns
            .iter()
            .map(|c| c.capacity() * size_of::<u64>())
            .sum()
    }
}

/// The filters of a [`BloomFilterExpr`] and the reservation for their memory,
/// which is released when the filters are dropped.
struct Filters {
    /// One filter per key column. `None` for a saturated column. Empty when no
    /// bloom filter was built.
    filters: Vec<Option<Sbbf>>,
    _reservation: Option<MemoryReservation>,
}

/// Builds the filters from the sorted hashes of all build partitions.
///
/// Returns `None` (no bloom filter) when the filter for `num_distinct` keys at
/// the target fpp is larger than `max_bytes` (the build side is too big for a
/// useful filter), when `pool` cannot hold the filters, or when every column's
/// filter is saturated.
fn build(
    partitions: &[&SortedKeyHashes],
    num_distinct: usize,
    options: BloomFilterOptions,
    pool: &Arc<dyn MemoryPool>,
) -> Result<Option<Filters>> {
    if !options.fits(num_distinct) {
        return Ok(None);
    }
    let num_columns = partitions.first().map_or(0, |p| p.columns.len());
    let reservation = MemoryConsumer::new("HashJoinBloomFilter").register(pool);
    if reservation
        .try_grow(filter_bytes(num_distinct, options.fpp) * num_columns)
        .is_err()
    {
        return Ok(None);
    }
    let filters: Vec<Option<Sbbf>> = (0..num_columns)
        .map(|column| {
            let mut filter = Sbbf::new_with_ndv_fpp(num_distinct as u64, options.fpp)
                .map_err(parquet_err)?;
            for partition in partitions {
                for hash in &partition.columns[column] {
                    filter.insert_hash(*hash);
                }
            }
            filter.fold_to_target_fpp(options.fpp);
            // A saturated filter costs CPU time and removes almost no rows
            Ok((filter.estimated_fpp() <= options.max_fpp).then_some(filter))
        })
        .collect::<Result<_>>()?;
    if filters.iter().all(Option::is_none) {
        return Ok(None);
    }
    reservation.resize(filters.iter().flatten().map(|f| f.num_blocks() * 32).sum());
    Ok(Some(Filters {
        filters,
        _reservation: Some(reservation),
    }))
}

/// Physical expression that tests join keys against bloom filters of the build
/// side keys of a hash join.
///
/// The result is `true` for a row whose keys may be on the build side, and
/// `false` for a row whose keys are definitely not on the build side (or are
/// NULL). There is one filter per key column. A row passes if every column
/// passes its filter.
///
/// The filters use the Parquet bloom filter hash (`Sbbf`), so they give the
/// same answers as a Parquet bloom filter of the same values.
pub struct BloomFilterExpr {
    /// Probe side join key expressions, one per filter
    on_columns: Vec<PhysicalExprRef>,
    filters: Arc<Filters>,
    /// Which parts of the build side's key range hold keys, for container pruning
    pruning_bitmap: Option<Arc<KeyRangeBitmap>>,
}

impl BloomFilterExpr {
    /// Builds the filters from the sorted key hashes of every build partition
    /// (`partitions`, in the order of `on_columns`). `num_distinct` is the sum
    /// of the distinct keys of the partitions, used to size the filters. The
    /// filter memory is charged to `pool`.
    ///
    /// When no useful filter is possible (the build side is too big for
    /// `max_bytes` at the target fpp, `pool` cannot hold the filters, or the
    /// filters are saturated), or `partitions` is `None` (a partition has no
    /// hashes), the expression has no filter: it is `true` for every row and
    /// only carries `pruning_bitmap` (see [`Self::has_filter`]).
    pub(crate) fn try_new(
        on_columns: Vec<PhysicalExprRef>,
        partitions: Option<&[&SortedKeyHashes]>,
        num_distinct: usize,
        options: BloomFilterOptions,
        pool: &Arc<dyn MemoryPool>,
        pruning_bitmap: Option<Arc<KeyRangeBitmap>>,
    ) -> Result<Self> {
        let filters = match partitions {
            Some(partitions) => build(partitions, num_distinct, options, pool)?,
            None => None,
        };
        Ok(Self {
            on_columns,
            filters: Arc::new(filters.unwrap_or(Filters {
                filters: vec![],
                _reservation: None,
            })),
            pruning_bitmap,
        })
    }

    /// Returns `true` if the expression tests rows against a bloom filter.
    /// Without a filter it only carries the pruning bitmap.
    pub fn has_filter(&self) -> bool {
        !self.filters.filters.is_empty()
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
        Arc::as_ptr(&self.filters).hash(state);
    }
}

impl PartialEq for BloomFilterExpr {
    fn eq(&self, other: &Self) -> bool {
        self.on_columns == other.on_columns && Arc::ptr_eq(&self.filters, &other.filters)
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
            filters: Arc::clone(&self.filters),
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
        let num_rows = batch.num_rows();
        let mut result: Option<BooleanBuffer> = None;
        for (column, filter) in self.on_columns.iter().zip(&self.filters.filters) {
            let Some(filter) = filter else {
                continue;
            };
            let keys = column.evaluate(batch)?.into_array(num_rows)?;
            let matches = filter.check_array(keys.as_ref()).map_err(parquet_err)?;
            result = Some(match result {
                Some(acc) => &acc & &matches,
                None => matches,
            });
        }
        Ok(match result {
            Some(result) => {
                ColumnarValue::Array(Arc::new(BooleanArray::new(result, None)))
            }
            // No filter: only the pruning bitmap, which is not tested per row
            None => ColumnarValue::Scalar(ScalarValue::Boolean(Some(true))),
        })
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
            filters: _,
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
    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::Field;
    use datafusion_execution::memory_pool::{GreedyMemoryPool, UnboundedMemoryPool};
    use datafusion_physical_expr::expressions::Column;

    const OPTIONS: BloomFilterOptions = BloomFilterOptions {
        fpp: 0.01,
        max_bytes: 128 * 1024 * 1024,
        max_fpp: 0.15,
    };

    fn unbounded() -> Arc<dyn MemoryPool> {
        Arc::new(UnboundedMemoryPool::default())
    }

    fn hashes(keys: &[ArrayRef]) -> SortedKeyHashes {
        SortedKeyHashes::try_new(keys).unwrap().unwrap()
    }

    fn int_expr_with(
        partitions: Vec<Vec<i64>>,
        options: BloomFilterOptions,
        pool: &Arc<dyn MemoryPool>,
    ) -> Option<BloomFilterExpr> {
        let num_distinct = partitions.iter().map(Vec::len).sum();
        let hashes: Vec<SortedKeyHashes> = partitions
            .into_iter()
            .map(|keys| hashes(&[Arc::new(Int64Array::from(keys)) as ArrayRef]))
            .collect();
        let refs: Vec<&SortedKeyHashes> = hashes.iter().collect();
        let expr = BloomFilterExpr::try_new(
            vec![Arc::new(Column::new("k", 0))],
            Some(&refs),
            num_distinct,
            options,
            pool,
            None,
        )
        .unwrap();
        expr.has_filter().then_some(expr)
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
    fn null_probe_keys_do_not_match() {
        let expr = int_expr(vec![vec![1, 2, 3]]);
        let batch = int_batch(vec![Some(1), None, Some(3)]);
        assert_eq!(eval(&expr, &batch), vec![true, false, true]);
    }

    /// Hashes sorted per partition and inserted in order give the same filter
    /// as `insert_array` of the keys in their original order.
    #[test]
    fn sorted_insert_matches_insert_array() {
        let partitions: Vec<Vec<i64>> = (0..8)
            .map(|p| (0..10_000).map(|i| (i * 7919 + p) % 1_000_000).collect())
            .collect();
        let num_distinct: usize = partitions.iter().map(Vec::len).sum();
        let expr = int_expr(partitions.clone());
        let mut expected = Sbbf::new_with_ndv_fpp(num_distinct as u64, 0.01).unwrap();
        for keys in partitions {
            expected.insert_array(&Int64Array::from(keys)).unwrap();
        }
        expected.fold_to_target_fpp(0.01);
        let (mut a, mut b) = (vec![], vec![]);
        expected.write_bitset(&mut a).unwrap();
        expr.filters.filters[0]
            .as_ref()
            .unwrap()
            .write_bitset(&mut b)
            .unwrap();
        assert_eq!(a, b);
    }

    #[test]
    fn sorted_key_hashes_skip_nulls_and_duplicates() {
        let keys: ArrayRef =
            Arc::new(Int64Array::from(vec![Some(3), None, Some(1), Some(3)]));
        let hashes = hashes(&[keys]);
        let mut expected = vec![Sbbf::hash_value(&1i64), Sbbf::hash_value(&3i64)];
        expected.sort_unstable();
        assert_eq!(hashes.columns, vec![expected]);
        let floats: ArrayRef = Arc::new(arrow::array::Float64Array::from(vec![1.0]));
        assert!(SortedKeyHashes::try_new(&[floats]).unwrap().is_none());
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
        let keys: ArrayRef = Arc::new(Int64Array::from((0..10_000).collect::<Vec<_>>()));
        let hashes = hashes(&[keys]);
        let expr = BloomFilterExpr::try_new(
            vec![Arc::new(Column::new("k", 0))],
            Some(&[&hashes]),
            10,
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
        let built = expr.filters.filters[0].as_ref().unwrap();
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
        let hashes = hashes(&keys);
        let expr = BloomFilterExpr::try_new(
            vec![Arc::new(Column::new("k", 0)), Arc::new(Column::new("s", 1))],
            Some(&[&hashes]),
            2,
            OPTIONS,
            &unbounded(),
            None,
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
