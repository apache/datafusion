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

/// Tests whether a sorted string domain intersects an inclusive statistics interval.
/// This expression is used only for pruning; the original IN remains the row filter.
#[derive(Debug, Eq)]
pub(crate) struct StringInListPruningExpr {
    min: PhysicalExprRef,
    max: PhysicalExprRef,
    values: Arc<[String]>,
}

impl StringInListPruningExpr {
    pub(crate) fn new(
        min: PhysicalExprRef,
        max: PhysicalExprRef,
        mut values: Vec<String>,
    ) -> Self {
        values.sort_unstable();
        values.dedup();
        Self {
            min,
            max,
            values: values.into(),
        }
    }
}

impl PartialEq for StringInListPruningExpr {
    fn eq(&self, other: &Self) -> bool {
        self.min.eq(&other.min) && self.max.eq(&other.max) && self.values == other.values
    }
}

impl Hash for StringInListPruningExpr {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.min.hash(state);
        self.max.hash(state);
        self.values.hash(state);
    }
}

impl Display for StringInListPruningExpr {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "IN_SET_INTERSECTS({}, {}, {} values)",
            self.min,
            self.max,
            self.values.len()
        )
    }
}

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

impl PhysicalExpr for StringInListPruningExpr {
    fn data_type(&self, _input_schema: &Schema) -> Result<DataType> {
        Ok(DataType::Boolean)
    }

    fn nullable(&self, _input_schema: &Schema) -> Result<bool> {
        Ok(true)
    }

    fn evaluate(&self, batch: &RecordBatch) -> Result<ColumnarValue> {
        // Normalize Utf8, LargeUtf8, Utf8View, and dictionary-encoded statistics.
        let min = self.min.evaluate(batch)?.into_array(batch.num_rows())?;
        let max = self.max.evaluate(batch)?.into_array(batch.num_rows())?;
        // A short string slice can retain a buffer too large for Utf8View's
        // u32 offsets. Avoid a panic in the cast and keep pruning conservative.
        if has_oversized_string_buffer(min.as_ref(), u32::MAX as usize)
            || has_oversized_string_buffer(max.as_ref(), u32::MAX as usize)
        {
            return Ok(ColumnarValue::Array(Arc::new(BooleanArray::new_null(
                batch.num_rows(),
            ))));
        }
        // Dictionary values can be NULL behind valid keys. Preserve their
        // validity even if the view cast only carries the key nulls.
        let min_nulls = min.logical_nulls();
        let max_nulls = max.logical_nulls();
        let min = cast(&min, &DataType::Utf8View)?;
        let max = cast(&max, &DataType::Utf8View)?;
        let min = min.as_string_view();
        let max = max.as_string_view();
        let matches: BooleanArray = (0..batch.num_rows())
            .map(|i| {
                let min = (min.is_valid(i)
                    && min_nulls.as_ref().is_none_or(|nulls| nulls.is_valid(i)))
                .then(|| min.value(i).as_bytes());
                let max = (max.is_valid(i)
                    && max_nulls.as_ref().is_none_or(|nulls| nulls.is_valid(i)))
                .then(|| max.value(i).as_bytes());
                match (min, max) {
                    (Some(min), Some(max)) => {
                        if min > max {
                            return None;
                        }
                        let index = self.values.partition_point(|v| v.as_bytes() < min);
                        Some(self.values.get(index).is_some_and(|v| v.as_bytes() <= max))
                    }
                    // A single known bound can still exclude the whole domain.
                    (Some(min), None)
                        if self.values.last().is_some_and(|v| v.as_bytes() < min) =>
                    {
                        Some(false)
                    }
                    (None, Some(max))
                        if self.values.first().is_some_and(|v| v.as_bytes() > max) =>
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
            min: Arc::clone(&children[0]),
            max: Arc::clone(&children[1]),
            values: Arc::clone(&self.values),
        }))
    }

    fn fmt_sql(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{self}")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{ArrayRef, StringArray};

    #[test]
    fn oversized_buffers_check_retained_data_not_visible_offsets() -> Result<()> {
        // Exercise the size boundary without allocating a 4 GiB buffer.
        let limit = 32;
        let padding = "p".repeat(limit - 1);
        let array: ArrayRef = Arc::new(StringArray::from(vec!["a", padding.as_str()]));

        for value_type in [DataType::Utf8, DataType::LargeUtf8] {
            for data_type in [
                value_type.clone(),
                DataType::Dictionary(
                    Box::new(DataType::Int32),
                    Box::new(value_type.clone()),
                ),
                DataType::Dictionary(
                    Box::new(DataType::UInt64),
                    Box::new(value_type.clone()),
                ),
            ] {
                let slice = cast(&array, &data_type)?.slice(0, 1);
                assert!(has_oversized_string_buffer(slice.as_ref(), limit - 1));
                assert!(has_oversized_string_buffer(slice.as_ref(), limit));
                assert!(!has_oversized_string_buffer(slice.as_ref(), limit + 1));
            }
        }

        // Already-normalized views do not have the byte-array cast limitation.
        for data_type in [
            DataType::Utf8View,
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8View)),
        ] {
            let slice = cast(&array, &data_type)?.slice(0, 1);
            assert!(!has_oversized_string_buffer(slice.as_ref(), limit));
        }
        Ok(())
    }
}
