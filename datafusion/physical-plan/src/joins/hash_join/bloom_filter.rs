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

use std::fmt::{self, Display};
use std::hash::Hash;
use std::sync::{Arc, OnceLock};

use arrow::array::{Array, ArrayRef, BooleanArray};
use arrow::buffer::BooleanBuffer;
use arrow::datatypes::{DataType, Schema};
use arrow::record_batch::RecordBatch;
use datafusion_common::{DataFusionError, Result, ScalarValue};
use datafusion_expr::ColumnarValue;
use datafusion_expr_common::dyn_eq::DynHash;
use datafusion_physical_expr_common::physical_expr::{PhysicalExpr, PhysicalExprRef};
use parking_lot::Mutex;
use parquet::bloom_filter::Sbbf;

use crate::joins::key_range_bitmap::KeyRangeBitmap;

/// Fill a filter from many threads only above this many build rows.
const PARALLEL_FILL_MIN_ROWS: usize = 1 << 16;

/// Sizing options of a [`BloomFilterExpr`].
#[derive(Debug, Clone, Copy)]
pub(crate) struct BloomFilterOptions {
    /// Target false positive probability.
    pub(crate) fpp: f64,
    /// Largest filter, in bytes, per key column.
    pub(crate) max_bytes: usize,
    /// Do not use a column's filter when its estimated false positive
    /// probability is above this value.
    pub(crate) max_fpp: f64,
}

/// Returns `true` if a [`BloomFilterExpr`] can test keys of `data_type`.
pub(crate) fn supports_data_type(data_type: &DataType) -> bool {
    Sbbf::supports_data_type(data_type)
}

/// Build side keys, and the filters that are built from them on first use.
///
/// The filters are not built until a probe batch is evaluated. Thus a scan that
/// uses the dynamic filter only for statistics pruning (for example parquet with
/// `pushdown_filters = false`) does not pay for the filter.
struct LazyBloomFilter {
    /// Key columns of each build partition. Taken when the filters are built.
    input: Mutex<Option<Vec<Vec<ArrayRef>>>>,
    /// Sum of the distinct keys of all build partitions.
    num_distinct: usize,
    options: BloomFilterOptions,
    /// One filter per key column. `None` for a column whose filter is saturated.
    filters: OnceLock<Vec<Option<Sbbf>>>,
}

impl LazyBloomFilter {
    fn filters(&self) -> Result<&[Option<Sbbf>]> {
        if let Some(filters) = self.filters.get() {
            return Ok(filters);
        }
        let mut input = self.input.lock();
        if let Some(filters) = self.filters.get() {
            return Ok(filters);
        }
        let partitions = input.take().unwrap_or_default();
        let filters = build_filters(&partitions, self.num_distinct, self.options)?;
        Ok(self.filters.get_or_init(|| filters))
    }
}

/// Number of bytes for a filter with `ndv` distinct values at `fpp`
/// (the size that `Sbbf::new_with_ndv_fpp` allocates before rounding).
fn bytes_for_ndv_fpp(ndv: usize, fpp: f64) -> usize {
    let bits = -8.0 * ndv as f64 / (1.0 - fpp.powf(1.0 / 8.0)).ln();
    (bits / 8.0) as usize
}

fn build_filters(
    partitions: &[Vec<ArrayRef>],
    num_distinct: usize,
    options: BloomFilterOptions,
) -> Result<Vec<Option<Sbbf>>> {
    let num_columns = partitions.first().map(Vec::len).unwrap_or(0);
    let num_bytes = bytes_for_ndv_fpp(num_distinct, options.fpp).min(options.max_bytes);
    (0..num_columns)
        .map(|column| {
            let mut filter = Sbbf::new_with_num_of_bytes(num_bytes);
            let arrays: Vec<&ArrayRef> = partitions.iter().map(|p| &p[column]).collect();
            fill(&mut filter, &arrays)?;
            filter.fold_to_target_fpp(options.fpp);
            // A saturated filter costs CPU time and removes almost no rows
            Ok((filter.estimated_fpp() <= options.max_fpp).then_some(filter))
        })
        .collect()
}

fn parquet_err(e: parquet::errors::ParquetError) -> DataFusionError {
    DataFusionError::External(Box::new(e))
}

/// Inserts the non-null values of `arrays` into `filter`.
///
/// Large inputs are inserted from many threads. Each thread hashes some of the
/// arrays and groups the hashes by shard of the filter. Then each thread inserts
/// the hashes of one shard. The shards do not share memory, so no locks or
/// atomic operations are necessary.
fn fill(filter: &mut Sbbf, arrays: &[&ArrayRef]) -> Result<()> {
    let num_rows: usize = arrays.iter().map(|a| a.len()).sum();
    let num_threads = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(1)
        .min(arrays.len())
        .min(filter.num_blocks());
    if num_threads <= 1 || num_rows < PARALLEL_FILL_MIN_ROWS {
        for array in arrays {
            filter.insert_array(array.as_ref()).map_err(parquet_err)?;
        }
        return Ok(());
    }

    let per_thread = arrays.len().div_ceil(num_threads);
    let shared: &Sbbf = filter;
    let buckets: Vec<Vec<Vec<u64>>> = std::thread::scope(|s| {
        let handles: Vec<_> = arrays
            .chunks(per_thread)
            .map(|chunk| {
                s.spawn(move || -> Result<Vec<Vec<u64>>> {
                    let mut buckets = vec![vec![]; num_threads];
                    let mut hashes = vec![];
                    for array in chunk {
                        Sbbf::hash_array(array.as_ref(), &mut hashes)
                            .map_err(parquet_err)?;
                        let mut push = |i: usize| {
                            let hash = hashes[i];
                            buckets[shared.shard_index(hash, num_threads)].push(hash);
                        };
                        match array.logical_nulls() {
                            Some(nulls) => nulls.valid_indices().for_each(&mut push),
                            None => (0..hashes.len()).for_each(&mut push),
                        }
                    }
                    Ok(buckets)
                })
            })
            .collect();
        handles
            .into_iter()
            .map(|h| h.join().unwrap_or_else(|e| std::panic::resume_unwind(e)))
            .collect::<Result<Vec<_>>>()
    })?;

    std::thread::scope(|s| {
        for (shard_index, mut shard) in
            filter.shards_mut(num_threads).into_iter().enumerate()
        {
            let buckets = &buckets;
            s.spawn(move || {
                for thread_buckets in buckets {
                    for hash in &thread_buckets[shard_index] {
                        shard.insert_hash(*hash);
                    }
                }
            });
        }
    });
    Ok(())
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
///
/// The filters are built on the first call to `evaluate`, from the build side
/// keys of all partitions of the join. A column whose filter is saturated (the
/// estimated false positive probability is above
/// `datafusion.optimizer.hash_join_bloom_filter_max_fpp`) is not tested.
pub struct BloomFilterExpr {
    /// Probe side join key expressions, one per filter
    on_columns: Vec<PhysicalExprRef>,
    filter: Arc<LazyBloomFilter>,
    /// Which parts of the build side's key range hold keys, for container pruning
    pruning_bitmap: Option<Arc<KeyRangeBitmap>>,
}

impl BloomFilterExpr {
    /// `partitions` holds the build side key columns of each partition, in the
    /// order of `on_columns`. `num_distinct` is the sum of the distinct keys of
    /// the partitions, used to size the filters.
    pub(crate) fn new(
        on_columns: Vec<PhysicalExprRef>,
        partitions: Vec<Vec<ArrayRef>>,
        num_distinct: usize,
        options: BloomFilterOptions,
        pruning_bitmap: Option<Arc<KeyRangeBitmap>>,
    ) -> Self {
        Self {
            on_columns,
            filter: Arc::new(LazyBloomFilter {
                input: Mutex::new(Some(partitions)),
                num_distinct,
                options,
                filters: OnceLock::new(),
            }),
            pruning_bitmap,
        }
    }

    /// Which parts of the build side's key range hold keys, for container pruning.
    pub fn pruning_bitmap(&self) -> Option<&Arc<KeyRangeBitmap>> {
        self.pruning_bitmap.as_ref()
    }

    /// The probe side key expressions.
    pub fn on_columns(&self) -> &[PhysicalExprRef] {
        &self.on_columns
    }

    /// Returns `true` if the filters are built (see [`Self`]).
    #[cfg(test)]
    pub(crate) fn is_built(&self) -> bool {
        self.filter.filters.get().is_some()
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
        Arc::as_ptr(&self.filter).hash(state);
    }
}

impl PartialEq for BloomFilterExpr {
    fn eq(&self, other: &Self) -> bool {
        self.on_columns == other.on_columns && Arc::ptr_eq(&self.filter, &other.filter)
    }
}

impl Eq for BloomFilterExpr {}

impl Display for BloomFilterExpr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "bloom_filter")
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
            filter: Arc::clone(&self.filter),
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
        for (column, filter) in self.on_columns.iter().zip(self.filter.filters()?) {
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
            // Every filter is saturated: every row may match
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
        write!(f, "bloom_filter")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::Field;
    use datafusion_physical_expr::expressions::Column;

    const OPTIONS: BloomFilterOptions = BloomFilterOptions {
        fpp: 0.01,
        max_bytes: 128 * 1024 * 1024,
        max_fpp: 0.15,
    };

    fn int_expr(
        partitions: Vec<Vec<i64>>,
        options: BloomFilterOptions,
    ) -> BloomFilterExpr {
        let num_distinct = partitions.iter().map(Vec::len).sum();
        let partitions = partitions
            .into_iter()
            .map(|keys| vec![Arc::new(Int64Array::from(keys)) as ArrayRef])
            .collect();
        BloomFilterExpr::new(
            vec![Arc::new(Column::new("k", 0))],
            partitions,
            num_distinct,
            options,
            None,
        )
    }

    fn int_batch(keys: Vec<Option<i64>>) -> RecordBatch {
        let schema = Schema::new(vec![Field::new("k", DataType::Int64, true)]);
        RecordBatch::try_new(Arc::new(schema), vec![Arc::new(Int64Array::from(keys))])
            .unwrap()
    }

    fn eval(expr: &BloomFilterExpr, batch: &RecordBatch) -> Vec<bool> {
        match expr.evaluate(batch).unwrap() {
            ColumnarValue::Array(a) => {
                let a = a.as_any().downcast_ref::<BooleanArray>().unwrap();
                a.iter().map(|v| v.unwrap()).collect()
            }
            ColumnarValue::Scalar(ScalarValue::Boolean(Some(v))) => {
                vec![v; batch.num_rows()]
            }
            other => panic!("unexpected {other:?}"),
        }
    }

    #[test]
    fn finds_build_keys_and_rejects_others() {
        let expr = int_expr(vec![(0..1000).collect(), (5000..6000).collect()], OPTIONS);
        assert!(!expr.is_built());
        let batch = int_batch((0..10_000).map(Some).collect());
        let result = eval(&expr, &batch);
        assert!(expr.is_built());
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
        let expr = int_expr(vec![vec![1, 2, 3]], OPTIONS);
        let batch = int_batch(vec![Some(1), None, Some(3)]);
        assert_eq!(eval(&expr, &batch), vec![true, false, true]);
    }

    #[test]
    fn parallel_fill_matches_serial_fill() {
        let partitions: Vec<Vec<i64>> = (0..8)
            .map(|p| (0..100_000).map(|i| i * 8 + p).collect())
            .collect();
        let expr = int_expr(partitions.clone(), OPTIONS);
        let probe: Vec<Option<i64>> = (0..2_000_000).map(Some).collect();
        let batch = int_batch(probe);
        let result = eval(&expr, &batch);
        assert!(result[..800_000].iter().all(|v| *v));

        let arrays: Vec<ArrayRef> = partitions
            .into_iter()
            .map(|keys| Arc::new(Int64Array::from(keys)) as ArrayRef)
            .collect();
        let mut serial = Sbbf::new_with_num_of_bytes(bytes_for_ndv_fpp(800_000, 0.01));
        for array in &arrays {
            serial.insert_array(array.as_ref()).unwrap();
        }
        serial.fold_to_target_fpp(0.01);
        let parallel = &expr.filter.filters().unwrap()[0];
        let (mut a, mut b) = (vec![], vec![]);
        serial.write_bitset(&mut a).unwrap();
        parallel.as_ref().unwrap().write_bitset(&mut b).unwrap();
        assert_eq!(a, b);
    }

    #[test]
    fn saturated_filter_passes_every_row() {
        // A 32 byte filter for 10k keys is saturated
        let options = BloomFilterOptions {
            max_bytes: 32,
            ..OPTIONS
        };
        let expr = int_expr(vec![(0..10_000).collect()], options);
        let batch = int_batch(vec![Some(-1), Some(1_000_000)]);
        assert_eq!(eval(&expr, &batch), vec![true, true]);
        assert!(expr.filter.filters().unwrap()[0].is_none());
    }

    #[test]
    fn multi_column_keys() {
        let partitions = vec![vec![
            Arc::new(Int64Array::from(vec![1, 2])) as ArrayRef,
            Arc::new(StringArray::from(vec!["a", "b"])) as ArrayRef,
        ]];
        let expr = BloomFilterExpr::new(
            vec![Arc::new(Column::new("k", 0)), Arc::new(Column::new("s", 1))],
            partitions,
            2,
            OPTIONS,
            None,
        );
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

        let expr: Arc<dyn PhysicalExpr> =
            Arc::new(int_expr(vec![(0..1000).collect()], OPTIONS));
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
