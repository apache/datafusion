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

use arrow::datatypes::{DataType, Schema};
use arrow::record_batch::RecordBatch;
use datafusion_common::{Result, assert_eq_or_internal_err};
use datafusion_physical_expr::{PhysicalExpr, PhysicalExprRef};
use datafusion_physical_plan::ColumnarValue;
use datafusion_physical_plan::joins::key_range_bitmap::KeyRangeBitmap;

/// Tests containers' `[min, max]` statistics against a build side's
/// [`KeyRangeBitmap`]: one nullable Boolean per container, where `false` proves
/// no build key lies in it and `NULL` means the bounds could not decide.
#[derive(Debug)]
pub(crate) struct KeyRangeBitmapPruningExpr {
    pub(crate) min: PhysicalExprRef,
    pub(crate) max: PhysicalExprRef,
    pub(crate) bitmap: Arc<KeyRangeBitmap>,
}

impl PartialEq for KeyRangeBitmapPruningExpr {
    fn eq(&self, other: &Self) -> bool {
        self.min.eq(&other.min)
            && self.max.eq(&other.max)
            && Arc::ptr_eq(&self.bitmap, &other.bitmap)
    }
}
impl Eq for KeyRangeBitmapPruningExpr {}

impl Hash for KeyRangeBitmapPruningExpr {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.min.hash(state);
        self.max.hash(state);
        Arc::as_ptr(&self.bitmap).hash(state);
    }
}

impl Display for KeyRangeBitmapPruningExpr {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        let (set, total) = self.bitmap.fill_stats();
        write!(
            f,
            "KEY_RANGE_BITMAP({}, {}, {set}/{total} buckets)",
            self.min, self.max
        )
    }
}

impl PhysicalExpr for KeyRangeBitmapPruningExpr {
    fn data_type(&self, _input_schema: &Schema) -> Result<DataType> {
        Ok(DataType::Boolean)
    }

    fn nullable(&self, _input_schema: &Schema) -> Result<bool> {
        Ok(true)
    }

    fn evaluate(&self, batch: &RecordBatch) -> Result<ColumnarValue> {
        let rows = batch.num_rows();
        let min = self.min.evaluate(batch)?.into_array(rows)?;
        let max = self.max.evaluate(batch)?.into_array(rows)?;
        let matches = self.bitmap.may_contain_ranges(&min, &max);
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
            bitmap: Arc::clone(&self.bitmap),
        }))
    }

    fn fmt_sql(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{self}")
    }
}
