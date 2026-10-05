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

//! Exact existence checks for one cross-side column comparison.
//!
//! A buffered key group witnesses `outer <[=] inner` iff it does so against
//! the inner maximum, and `outer >[=] inner` iff it does so against the minimum.
//! For `!=`, two distinct non-null values match every non-null outer value.
//! Complex expressions and floating-point types retain ordinary evaluation.

use std::sync::Arc;

use arrow::array::{Array, AsArray, BooleanArray, RecordBatch};
use arrow::compute::or;
use arrow::datatypes::{DataType, Schema};
use datafusion_common::{JoinSide, Result, ScalarValue};
use datafusion_expr::{Accumulator, ColumnarValue, Operator};
use datafusion_functions_aggregate_common::min_max::{MaxAccumulator, MinAccumulator};
use datafusion_physical_expr::expressions::{BinaryExpr, Column, NotExpr};
use datafusion_physical_expr_common::datum::apply_cmp;

use crate::joins::utils::{JoinFilter, boolean_mask_from_filter};

/// A recognized comparison, normalized to `outer OP inner`.
#[derive(Debug)]
pub(super) struct SemiAntiComparison {
    pub(super) outer_column: usize,
    inner_column: usize,
    op: Operator,
}

/// Cached once per buffered key group and reused across outer batches.
#[derive(Debug)]
pub(super) struct GroupSummary {
    extreme: ScalarValue,
    // A not-equal comparison needs both min and max.
    other: Option<ScalarValue>,
}

impl GroupSummary {
    /// The retained extrema plus scalar clones and Arrow scalar buffers used
    /// by `apply_cmp`. Admission used the source buffer as an upper bound;
    /// once reduction finishes, only these actual scalar sizes are needed.
    pub(super) fn memory_size(&self) -> usize {
        self.extreme
            .size()
            .max(self.other.as_ref().map_or(0, ScalarValue::size))
            .saturating_mul(6)
    }
}

impl SemiAntiComparison {
    pub(super) fn try_new(
        filter: &JoinFilter,
        outer_is_left: bool,
        outer_schema: &Schema,
        inner_schema: &Schema,
    ) -> Result<Option<Self>> {
        let (binary, mut op) = if let Some(not) =
            filter.expression().downcast_ref::<NotExpr>()
        {
            let Some(binary) = not.arg().downcast_ref::<BinaryExpr>() else {
                return Ok(None);
            };
            let Some(op) = binary.op().negate() else {
                return Ok(None);
            };
            (binary, op)
        } else if let Some(binary) = filter.expression().downcast_ref::<BinaryExpr>() {
            (binary, *binary.op())
        } else {
            return Ok(None);
        };
        if !matches!(
            op,
            Operator::NotEq
                | Operator::Lt
                | Operator::LtEq
                | Operator::Gt
                | Operator::GtEq
        ) {
            return Ok(None);
        }
        let (Some(left), Some(right)) = (
            binary.left().downcast_ref::<Column>(),
            binary.right().downcast_ref::<Column>(),
        ) else {
            return Ok(None);
        };
        let (Some(left), Some(right)) = (
            filter.column_indices().get(left.index()),
            filter.column_indices().get(right.index()),
        ) else {
            return Ok(None);
        };
        let outer_side = if outer_is_left {
            JoinSide::Left
        } else {
            JoinSide::Right
        };
        let inner_side = outer_side.negate();
        let (outer_column, inner_column) =
            if left.side == outer_side && right.side == inner_side {
                (left.index, right.index)
            } else if right.side == outer_side && left.side == inner_side {
                op = op.swap().unwrap();
                (right.index, left.index)
            } else {
                return Ok(None);
            };
        let (Some(outer), Some(inner)) = (
            outer_schema.fields().get(outer_column),
            inner_schema.fields().get(inner_column),
        ) else {
            return Ok(None);
        };
        if outer.data_type() != inner.data_type() || !supported_type(inner.data_type()) {
            return Ok(None);
        }
        Ok(Some(Self {
            outer_column,
            inner_column,
            op,
        }))
    }

    /// Bound the scalar copies made during reduction and comparison before
    /// allocating them. Strings conservatively use the largest source buffer;
    /// refusal simply leaves the ordinary filter path available. This avoids a
    /// second per-row pass just to measure the longest string.
    pub(super) fn memory_size(&self, batches: &[RecordBatch]) -> usize {
        let payload = batches
            .iter()
            .map(|batch| {
                let values = batch.column(self.inner_column);
                match values.data_type() {
                    DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => {
                        values.get_buffer_memory_size()
                    }
                    DataType::Timestamp(_, zone) => {
                        zone.as_ref().map_or(0, |zone| zone.len())
                    }
                    _ => 0,
                }
            })
            .max()
            .unwrap_or(0);
        size_of::<ScalarValue>()
            .saturating_add(payload)
            .saturating_mul(6)
    }

    /// Reduce only an in-memory group after its scalar storage is admitted.
    /// The shared accumulators ignore NULLs and use vectorized batch kernels.
    pub(super) fn summarize(&self, batches: &[RecordBatch]) -> Result<GroupSummary> {
        let data_type = batches[0].column(self.inner_column).data_type();
        let mut min = MinAccumulator::try_new(data_type)?;
        let mut max = MaxAccumulator::try_new(data_type)?;
        for batch in batches {
            let values = std::slice::from_ref(batch.column(self.inner_column));
            if matches!(self.op, Operator::NotEq | Operator::Gt | Operator::GtEq) {
                min.update_batch(values)?;
            }
            if matches!(self.op, Operator::NotEq | Operator::Lt | Operator::LtEq) {
                max.update_batch(values)?;
            }
        }
        let extreme = if matches!(self.op, Operator::Lt | Operator::LtEq) {
            max.evaluate()?
        } else {
            min.evaluate()?
        };
        let other = (self.op == Operator::NotEq)
            .then(|| max.evaluate())
            .transpose()?;
        Ok(GroupSummary { extreme, other })
    }

    pub(super) fn evaluate(
        &self,
        summary: &GroupSummary,
        outer: &RecordBatch,
    ) -> Result<BooleanArray> {
        let matched = self.compare(summary.extreme.clone(), outer)?;
        // For `!=`, either extreme can witness the comparison. All-null inner
        // groups produce null extrema, whose comparison masks are all false.
        match &summary.other {
            Some(other) => Ok(or(&matched, &self.compare(other.clone(), outer)?)?),
            None => Ok(matched),
        }
    }

    /// Probe an initial inner row using the same normalized comparison as
    /// the extrema, without building a filter batch or broadcasting the scalar.
    pub(super) fn evaluate_inner_row(
        &self,
        inner: &RecordBatch,
        row: usize,
        outer: &RecordBatch,
    ) -> Result<BooleanArray> {
        let value =
            ScalarValue::try_from_array(inner.column(self.inner_column).as_ref(), row)?;
        self.compare(value, outer)
    }

    fn compare(&self, value: ScalarValue, outer: &RecordBatch) -> Result<BooleanArray> {
        let values = ColumnarValue::Array(Arc::clone(outer.column(self.outer_column)));
        let compared = apply_cmp(self.op, &values, &ColumnarValue::Scalar(value))?
            .into_array(outer.num_rows())?;
        Ok(boolean_mask_from_filter(compared.as_boolean()))
    }
}

fn supported_type(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Boolean
            | DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64
            | DataType::Date32
            | DataType::Date64
            | DataType::Time32(_)
            | DataType::Time64(_)
            | DataType::Timestamp(_, _)
            | DataType::Duration(_)
            | DataType::Decimal32(_, _)
            | DataType::Decimal64(_, _)
            | DataType::Decimal128(_, _)
            | DataType::Decimal256(_, _)
            | DataType::Utf8
            | DataType::LargeUtf8
            | DataType::Utf8View
    )
}
