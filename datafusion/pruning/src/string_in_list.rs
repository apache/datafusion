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

use std::fmt::{self, Display, Formatter};
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use arrow::array::{Array, AsArray, BooleanArray};
use arrow::compute::cast;
use arrow::datatypes::{DataType, Schema};
use arrow::record_batch::RecordBatch;
use datafusion_common::{Result, assert_eq_or_internal_err};
use datafusion_physical_expr::{PhysicalExpr, PhysicalExprRef};
use datafusion_physical_plan::ColumnarValue;

use crate::in_list::{SetMembership, not_in_may_match};

fn has_oversized_string_buffer(array: &dyn Array, limit: usize) -> bool {
    match array.data_type() {
        DataType::Utf8 => array.as_string::<i32>().values().len() >= limit,
        DataType::LargeUtf8 => array.as_string::<i64>().values().len() >= limit,
        DataType::Dictionary(_, _) => has_oversized_string_buffer(
            array.as_any_dictionary().values().as_ref(),
            limit,
        ),
        _ => false,
    }
}

fn has_oversized_binary_buffer(array: &dyn Array, limit: usize) -> bool {
    match array.data_type() {
        DataType::Binary => array.as_binary::<i32>().values().len() >= limit,
        DataType::LargeBinary => array.as_binary::<i64>().values().len() >= limit,
        DataType::Dictionary(_, _) => has_oversized_binary_buffer(
            array.as_any_dictionary().values().as_ref(),
            limit,
        ),
        _ => false,
    }
}

macro_rules! string_bytes {
    ($value:expr) => {
        ($value).as_bytes()
    };
}

macro_rules! binary_bytes {
    ($value:expr) => {
        &($value)[..]
    };
}

/// Defines a byte-domain pruning expression for one variable-length value type.
macro_rules! define_byte_in_list_expr {
    (
        $name:ident,
        $value_type:ty,
        $view_type:expr,
        $as_view:ident,
        $bytes:ident,
        $has_oversized_buffer:path
    ) => {
        /// Tests an inclusive statistics interval against a sorted byte domain.
        ///
        /// [`PhysicalExpr::evaluate`] returns one nullable Boolean per min/max
        /// interval: `true` means matching rows may exist, `false` means the
        /// available bounds prove no row can match, and `NULL` means the bounds
        /// cannot support a safe decision.
        ///
        /// [`SetMembership::In`] can use one known bound to prove disjointness.
        /// For [`SetMembership::NotIn`], one known bound outside the domain
        /// proves the container may match, while equal bounds in the domain
        /// prove it cannot. Otherwise, the result keeps the container eligible.
        /// The original `IN` expression remains the row filter.
        #[derive(Debug, Eq)]
        pub(crate) struct $name {
            membership: SetMembership,
            min: PhysicalExprRef,
            max: PhysicalExprRef,
            values: Arc<[$value_type]>,
        }

        impl $name {
            pub(crate) fn new(
                membership: SetMembership,
                min: PhysicalExprRef,
                max: PhysicalExprRef,
                mut values: Vec<$value_type>,
            ) -> Self {
                values.sort_unstable_by(|left, right| {
                    membership.compare_bytes($bytes!(left), $bytes!(right))
                });
                values.dedup();
                Self {
                    membership,
                    min,
                    max,
                    values: values.into(),
                }
            }

            /// Does the sorted, deduplicated domain hold `value`?
            fn contains(&self, value: &[u8]) -> bool {
                self.values
                    .binary_search_by(|candidate| {
                        self.membership.compare_bytes($bytes!(candidate), value)
                    })
                    .is_ok()
            }
        }

        impl PartialEq for $name {
            fn eq(&self, other: &Self) -> bool {
                self.membership == other.membership
                    && self.min.eq(&other.min)
                    && self.max.eq(&other.max)
                    && self.values == other.values
            }
        }

        impl Hash for $name {
            fn hash<H: Hasher>(&self, state: &mut H) {
                self.membership.hash(state);
                self.min.hash(state);
                self.max.hash(state);
                self.values.hash(state);
            }
        }

        impl Display for $name {
            fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
                let name = self.membership.display_name();
                write!(
                    f,
                    "{name}({}, {}, {} values)",
                    self.min,
                    self.max,
                    self.values.len()
                )
            }
        }

        impl PhysicalExpr for $name {
            fn data_type(&self, _input_schema: &Schema) -> Result<DataType> {
                Ok(DataType::Boolean)
            }

            fn nullable(&self, _input_schema: &Schema) -> Result<bool> {
                Ok(true)
            }

            fn evaluate(&self, batch: &RecordBatch) -> Result<ColumnarValue> {
                // Normalize byte arrays and dictionary-encoded statistics to a view.
                let min = self.min.evaluate(batch)?.into_array(batch.num_rows())?;
                let max = self.max.evaluate(batch)?.into_array(batch.num_rows())?;
                // A short slice can retain a buffer too large for view arrays' u32
                // offsets. Avoid a panic in the cast and keep pruning conservative.
                if $has_oversized_buffer(min.as_ref(), u32::MAX as usize)
                    || $has_oversized_buffer(max.as_ref(), u32::MAX as usize)
                {
                    return Ok(ColumnarValue::Array(Arc::new(BooleanArray::new_null(
                        batch.num_rows(),
                    ))));
                }
                // Unlike primitive casts, view casts can drop NULLs stored behind
                // valid dictionary keys. Preserve that logical validity explicitly.
                // TODO: Revisit this workaround once the Arrow dependency includes
                // https://github.com/apache/arrow-rs/pull/10510.
                let min_nulls = min.logical_nulls();
                let max_nulls = max.logical_nulls();
                let min = cast(&min, &$view_type)?;
                let max = cast(&max, &$view_type)?;
                let min = min.$as_view();
                let max = max.$as_view();
                let matches: BooleanArray = (0..batch.num_rows())
                    .map(|i| {
                        let min: Option<&[u8]> = (min.is_valid(i)
                            && min_nulls.as_ref().is_none_or(|nulls| nulls.is_valid(i)))
                        .then(|| $bytes!(min.value(i)));
                        let max: Option<&[u8]> = (max.is_valid(i)
                            && max_nulls.as_ref().is_none_or(|nulls| nulls.is_valid(i)))
                        .then(|| $bytes!(max.value(i)));
                        if self.membership == SetMembership::NotIn {
                            // Membership is checked before bound equality. NOT IN ordering
                            // rejects different byte lengths first, avoiding scans of long
                            // common prefixes.
                            return not_in_may_match(min, max, |value| {
                                self.contains(value)
                            });
                        }
                        match (min, max) {
                            (Some(min), Some(max)) => {
                                if min > max {
                                    return None;
                                }
                                // String and binary values use unsigned lexicographic byte
                                // order, as required by the PruningStatistics min/max
                                // contract. Parquet adapters mask bounds with unusable
                                // ordering; PartitionPruningStatistics uses actual Arrow
                                // partition values. PrunableStatistics trusts file
                                // providers' bounds: there is no ordering gate for arbitrary
                                // statistics providers here.
                                let index =
                                    self.values.partition_point(|v| $bytes!(v) < min);
                                Some(
                                    self.values
                                        .get(index)
                                        .is_some_and(|v| $bytes!(v) <= max),
                                )
                            }
                            // A missing bound makes that end of the interval unbounded.
                            // Exclude only when the whole domain lies beyond the known bound;
                            // gaps within the domain and equality cannot prove disjointness.
                            (Some(min), None)
                                if self
                                    .values
                                    .last()
                                    .is_some_and(|v| $bytes!(v) < min) =>
                            {
                                Some(false)
                            }
                            (None, Some(max))
                                if self
                                    .values
                                    .first()
                                    .is_some_and(|v| $bytes!(v) > max) =>
                            {
                                Some(false)
                            }
                            _ => None,
                        }
                    })
                    .collect();
                Ok(ColumnarValue::Array(Arc::new(matches)))
            }

            fn children(&self) -> Vec<&PhysicalExprRef> {
                vec![&self.min, &self.max]
            }

            fn with_new_children(
                self: Arc<Self>,
                children: Vec<PhysicalExprRef>,
            ) -> Result<PhysicalExprRef> {
                assert_eq_or_internal_err!(children.len(), 2);
                Ok(Arc::new(Self {
                    membership: self.membership,
                    min: Arc::clone(&children[0]),
                    max: Arc::clone(&children[1]),
                    values: Arc::clone(&self.values),
                }))
            }

            fn fmt_sql(&self, f: &mut Formatter<'_>) -> fmt::Result {
                write!(f, "{self}")
            }
        }
    };
}

define_byte_in_list_expr!(
    StringInListPruningExpr,
    String,
    DataType::Utf8View,
    as_string_view,
    string_bytes,
    has_oversized_string_buffer
);

define_byte_in_list_expr!(
    BinaryInListPruningExpr,
    Box<[u8]>,
    DataType::BinaryView,
    as_binary_view,
    binary_bytes,
    has_oversized_binary_buffer
);

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{ArrayRef, BinaryArray, StringArray};

    fn assert_oversized_buffers(
        array: &ArrayRef,
        value_types: impl IntoIterator<Item = DataType>,
        view_type: DataType,
        limit: usize,
        has_oversized_buffer: fn(&dyn Array, usize) -> bool,
    ) -> Result<()> {
        for value_type in value_types {
            for data_type in [
                value_type.clone(),
                DataType::Dictionary(
                    Box::new(DataType::Int32),
                    Box::new(value_type.clone()),
                ),
                DataType::Dictionary(Box::new(DataType::UInt64), Box::new(value_type)),
            ] {
                let slice = cast(array, &data_type)?.slice(0, 1);
                assert!(has_oversized_buffer(slice.as_ref(), limit - 1));
                assert!(has_oversized_buffer(slice.as_ref(), limit));
                assert!(!has_oversized_buffer(slice.as_ref(), limit + 1));
            }
        }

        for data_type in [
            view_type.clone(),
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(view_type)),
        ] {
            let slice = cast(array, &data_type)?.slice(0, 1);
            assert!(!has_oversized_buffer(slice.as_ref(), limit));
        }
        Ok(())
    }

    #[test]
    fn oversized_buffers_check_retained_data_not_visible_offsets() -> Result<()> {
        // Exercise the size boundary without allocating a 4 GiB buffer.
        let limit = 32;
        let padding = "p".repeat(limit - 1);
        let array: ArrayRef = Arc::new(StringArray::from(vec!["a", padding.as_str()]));
        assert_oversized_buffers(
            &array,
            [DataType::Utf8, DataType::LargeUtf8],
            DataType::Utf8View,
            limit,
            has_oversized_string_buffer,
        )?;

        let padding = vec![0; limit - 1];
        let array: ArrayRef = Arc::new(BinaryArray::from(vec![&[1][..], &padding]));
        assert_oversized_buffers(
            &array,
            [DataType::Binary, DataType::LargeBinary],
            DataType::BinaryView,
            limit,
            has_oversized_binary_buffer,
        )?;
        Ok(())
    }
}
