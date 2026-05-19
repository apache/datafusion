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

//! Loaded Parquet Split Block Bloom Filter (SBBF) data, with a
//! [`PruningStatistics`] adapter so the predicate-pruning machinery in
//! [`datafusion_pruning`] can consume it.

use std::collections::{HashMap, HashSet};

use arrow::array::{ArrayRef, BooleanArray};
use datafusion_common::pruning::PruningStatistics;
use datafusion_common::{Column, ScalarValue};
use parquet::basic::Type;
use parquet::bloom_filter::Sbbf;
use parquet::data_type::Decimal;

/// In memory Parquet Split Block Bloom Filters (SBBF).
///
/// This structure implements [`PruningStatistics`] and is used to prune
/// Parquet row groups and data pages based on the query predicate.
#[derive(Debug, Clone, Default)]
pub(crate) struct BloomFilterStatistics {
    /// Per-column Bloom filters
    /// Key: predicate column name
    /// Value:
    /// * [`Sbbf`] (Bloom filter),
    /// * Parquet physical [`Type`] needed to evaluate  literals against the filter
    column_sbbf: HashMap<String, (Sbbf, Type)>,
}

impl BloomFilterStatistics {
    /// Create an empty [`BloomFilterStatistics`]
    pub(crate) fn new() -> Self {
        Default::default()
    }

    /// Create an empty [`BloomFilterStatistics`] with the specified capacity
    pub(crate) fn with_capacity(capacity: usize) -> Self {
        Self {
            column_sbbf: HashMap::with_capacity(capacity),
        }
    }

    /// Add a Bloom filter and type for the specified column
    pub(crate) fn insert(&mut self, column: impl Into<String>, sbbf: Sbbf, ty: Type) {
        self.column_sbbf.insert(column.into(), (sbbf, ty));
    }

    /// Helper function for checking if [`Sbbf`] filter contains [`ScalarValue`].
    ///
    /// In case the type of scalar is not supported, returns `true`, assuming that the
    /// value may be present.
    fn check_scalar(sbbf: &Sbbf, value: &ScalarValue, parquet_type: &Type) -> bool {
        match value {
            ScalarValue::Utf8(Some(v))
            | ScalarValue::Utf8View(Some(v))
            | ScalarValue::LargeUtf8(Some(v)) => sbbf.check(&v.as_str()),
            ScalarValue::Binary(Some(v))
            | ScalarValue::BinaryView(Some(v))
            | ScalarValue::LargeBinary(Some(v)) => sbbf.check(v),
            ScalarValue::FixedSizeBinary(_size, Some(v)) => sbbf.check(v),
            ScalarValue::Boolean(Some(v)) => sbbf.check(v),
            ScalarValue::Float64(Some(v)) => sbbf.check(v),
            ScalarValue::Float32(Some(v)) => sbbf.check(v),
            ScalarValue::Int64(Some(v)) => sbbf.check(v),
            ScalarValue::Int32(Some(v)) => sbbf.check(v),
            ScalarValue::UInt64(Some(v)) => sbbf.check(v),
            ScalarValue::UInt32(Some(v)) => sbbf.check(v),
            ScalarValue::Decimal128(Some(v), p, s) => match parquet_type {
                Type::INT32 => {
                    //https://github.com/apache/parquet-format/blob/eb4b31c1d64a01088d02a2f9aefc6c17c54cc6fc/Encodings.md?plain=1#L35-L42
                    // All physical type  are little-endian
                    if *p > 9 {
                        //DECIMAL can be used to annotate the following types:
                        //
                        // int32: for 1 <= precision <= 9
                        // int64: for 1 <= precision <= 18
                        return true;
                    }
                    let b = (*v as i32).to_le_bytes();
                    // Use Decimal constructor after https://github.com/apache/arrow-rs/issues/5325
                    let decimal = Decimal::Int32 {
                        value: b,
                        precision: *p as i32,
                        scale: *s as i32,
                    };
                    sbbf.check(&decimal)
                }
                Type::INT64 => {
                    if *p > 18 {
                        return true;
                    }
                    let b = (*v as i64).to_le_bytes();
                    let decimal = Decimal::Int64 {
                        value: b,
                        precision: *p as i32,
                        scale: *s as i32,
                    };
                    sbbf.check(&decimal)
                }
                Type::FIXED_LEN_BYTE_ARRAY => {
                    // keep with from_bytes_to_i128
                    let b = v.to_be_bytes().to_vec();
                    // Use Decimal constructor after https://github.com/apache/arrow-rs/issues/5325
                    let decimal = Decimal::Bytes {
                        value: b.into(),
                        precision: *p as i32,
                        scale: *s as i32,
                    };
                    sbbf.check(&decimal)
                }
                _ => true,
            },
            ScalarValue::Dictionary(_, inner) => {
                BloomFilterStatistics::check_scalar(sbbf, inner, parquet_type)
            }
            _ => true,
        }
    }
}

impl PruningStatistics for BloomFilterStatistics {
    fn min_values(&self, _column: &Column) -> Option<ArrayRef> {
        None
    }

    fn max_values(&self, _column: &Column) -> Option<ArrayRef> {
        None
    }

    fn num_containers(&self) -> usize {
        1
    }

    fn null_counts(&self, _column: &Column) -> Option<ArrayRef> {
        None
    }

    fn row_counts(&self) -> Option<ArrayRef> {
        None
    }

    /// Use bloom filters to determine if we are sure this column can not
    /// possibly contain `values`
    ///
    /// The `contained` API returns false if the bloom filters knows that *ALL*
    /// of the values in a column are not present.
    fn contained(
        &self,
        column: &Column,
        values: &HashSet<ScalarValue>,
    ) -> Option<BooleanArray> {
        let (sbbf, parquet_type) = self.column_sbbf.get(column.name.as_str())?;

        // Bloom filters are probabilistic data structures that can return false
        // positives (i.e. it might return true even if the value is not
        // present) however, the bloom filter will return `false` if the value is
        // definitely not present.

        let known_not_present = values
            .iter()
            .map(|value| BloomFilterStatistics::check_scalar(sbbf, value, parquet_type))
            // The row group doesn't contain any of the values if
            // all the checks are false
            .all(|v| !v);

        let contains = if known_not_present {
            Some(false)
        } else {
            // Given the bloom filter is probabilistic, we can't be sure that
            // the row group actually contains the values. Return `None` to
            // indicate this uncertainty
            None
        };

        Some(BooleanArray::from(vec![contains]))
    }
}
