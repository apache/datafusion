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
        let min = cast(&min, &DataType::Utf8View)?;
        let max = cast(&max, &DataType::Utf8View)?;
        let min = min.as_string_view();
        let max = max.as_string_view();
        let matches: BooleanArray = (0..batch.num_rows())
            .map(|i| {
                if min.is_null(i) || max.is_null(i) {
                    return None;
                }
                let min = min.value(i).as_bytes();
                let max = max.value(i).as_bytes();
                if min > max {
                    return None;
                }
                let index = self.values.partition_point(|v| v.as_bytes() < min);
                Some(self.values.get(index).is_some_and(|v| v.as_bytes() <= max))
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
