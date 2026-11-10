// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

//! Exact existence checks for one cross-side column comparison.
//!
//! A buffered key group witnesses `outer <[=] inner` iff it does so against
//! the inner maximum, and `outer >[=] inner` iff it does so against the minimum.
//! For `!=`, two distinct non-null values match every non-null outer value.
//! Complex expressions and floating-point types retain ordinary evaluation.

use std::cmp::Ordering;

use arrow::array::{Array, ArrayRef, BooleanArray, Datum, RecordBatch, Scalar};
use arrow::compute::kernels::cmp;
use arrow::compute::or;
use arrow::datatypes::{DataType, Schema};
use datafusion_common::{JoinSide, Result, ScalarValue};
use datafusion_expr::Operator;
use datafusion_functions_aggregate_common::min_max::{max_batch, min_batch};
use datafusion_physical_expr::expressions::{BinaryExpr, Column, NotExpr};

use crate::joins::utils::{JoinFilter, boolean_mask_from_filter};

/// Direct probes avoid setting up min/max when either of the first two rows
/// witnesses the comparison, as in the common early-match benchmark cases.
const DIRECT_PROBES: usize = 2;
/// Check the first small vector of rows before doubling the prefix length.
const FIRST_PREFIX_ROWS: usize = 8;

/// A recognized comparison, normalized to `outer OP inner`.
#[derive(Debug, Clone, Copy)]
pub(super) struct SemiAntiComparison {
    pub(super) outer_column: usize,
    inner_column: usize,
    op: Operator,
}

/// Cached once per buffered key group and reused across outer batches.
#[derive(Debug)]
pub(super) struct GroupSummary {
    extreme: Scalar<ArrayRef>,
    // A not-equal comparison needs both min and max.
    other: Option<Scalar<ArrayRef>>,
}

impl GroupSummary {
    fn from_extrema(extreme: &ScalarValue, other: Option<&ScalarValue>) -> Result<Self> {
        Ok(Self {
            extreme: extreme.to_scalar()?,
            other: other.map(ScalarValue::to_scalar).transpose()?,
        })
    }

    /// Inline state and the compact Arrow arrays retained for cached comparisons.
    pub(super) fn memory_size(&self) -> usize {
        let array_size = |value: &Scalar<ArrayRef>| value.get().0.get_array_memory_size();
        size_of::<Self>()
            + array_size(&self.extreme)
            + self.other.as_ref().map_or(0, array_size)
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

    /// Apply two direct row probes, then check the extrema after 8, 16, 32, ...
    /// rows. Only the newly reached rows enter each reduction, including when a
    /// checkpoint crosses an input batch boundary. A witness at row k costs
    /// O(k) inner rows even when the full group is much larger.
    ///
    /// The callback records the mask for this outer slice and returns true once
    /// every matchable row has a witness. A complete scan returns the exact
    /// summary for later outer slices. An early stop leaves the input buffered.
    pub(super) fn scan(
        &self,
        batches: &[RecordBatch],
        outer: &RecordBatch,
        mut matches_all: impl FnMut(&BooleanArray) -> bool,
    ) -> Result<Option<GroupSummary>> {
        let mut probes_left = DIRECT_PROBES;
        for batch in batches {
            let len = probes_left.min(batch.num_rows());
            for row in 0..len {
                if matches_all(&self.evaluate_inner_row(batch, row, outer)?) {
                    return Ok(None);
                }
            }
            probes_left -= len;
            if probes_left == 0 {
                break;
            }
        }

        // The probes neither copy string payloads nor build reduction state.
        // Include their values in the first reduction for later outer slices.
        let data_type = batches[0].column(self.inner_column).data_type();
        let mut extreme = ScalarValue::try_from(data_type)?;
        let mut other = (self.op == Operator::NotEq).then(|| extreme.clone());
        type Reduce = fn(&ArrayRef) -> Result<ScalarValue>;
        let (reduce, order): (Reduce, Ordering) = match self.op {
            Operator::Lt | Operator::LtEq => (max_batch, Ordering::Greater),
            _ => (min_batch, Ordering::Less),
        };

        let mut scanned = 0;
        let mut checkpoint = FIRST_PREFIX_ROWS;
        let mut changed = false;
        for (index, batch) in batches.iter().enumerate() {
            let column = batch.column(self.inner_column);
            let mut start = 0;
            while start < column.len() {
                let len = (checkpoint - scanned).min(column.len() - start);
                let finished = index + 1 == batches.len() && start + len == column.len();
                let values = column.slice(start, len);
                changed |= update_extreme(&mut extreme, reduce(&values)?, order);
                if let Some(max) = &mut other {
                    changed |=
                        update_extreme(max, max_batch(&values)?, Ordering::Greater);
                }
                start += len;
                scanned += len;
                if scanned == checkpoint && !finished {
                    // The prior mask already included any unchanged extrema.
                    if changed {
                        let partial =
                            GroupSummary::from_extrema(&extreme, other.as_ref())?;
                        if matches_all(&self.evaluate(&partial, outer)?) {
                            return Ok(None);
                        }
                    }
                    changed = false;
                    checkpoint = checkpoint.saturating_mul(2);
                }
            }
        }
        // Reuse compact Arrow scalars across outer batches. A cached comparison
        // with a long string must not copy that string into a new Arrow buffer.
        let summary = GroupSummary::from_extrema(&extreme, other.as_ref())?;
        if changed {
            matches_all(&self.evaluate(&summary, outer)?);
        }
        Ok(Some(summary))
    }

    pub(super) fn evaluate(
        &self,
        summary: &GroupSummary,
        outer: &RecordBatch,
    ) -> Result<BooleanArray> {
        let matched = self.compare(&summary.extreme, outer)?;
        match &summary.other {
            Some(other) => Ok(or(&matched, &self.compare(other, outer)?)?),
            None => Ok(matched),
        }
    }

    /// Borrow the input for a single-row Arrow scalar, including string payloads.
    pub(super) fn evaluate_inner_row(
        &self,
        inner: &RecordBatch,
        row: usize,
        outer: &RecordBatch,
    ) -> Result<BooleanArray> {
        let value = Scalar::new(inner.column(self.inner_column).slice(row, 1));
        self.compare(&value, outer)
    }

    fn compare(&self, value: &dyn Datum, outer: &RecordBatch) -> Result<BooleanArray> {
        let values = outer.column(self.outer_column).as_ref();
        // Only ordinary comparisons on non-floating primitive or string columns
        // reach here, so these are the same Arrow kernels used by apply_cmp.
        let compared = match self.op {
            Operator::Lt => cmp::lt(&values, value)?,
            Operator::LtEq => cmp::lt_eq(&values, value)?,
            Operator::Gt => cmp::gt(&values, value)?,
            Operator::GtEq => cmp::gt_eq(&values, value)?,
            Operator::NotEq => cmp::neq(&values, value)?,
            _ => unreachable!("invalid semi/anti summary comparison"),
        };
        Ok(boolean_mask_from_filter(&compared))
    }
}

/// Move a winning batch extremum into place. A losing batch never clones the
/// running winner, which may be a long string accumulated many batches ago.
fn update_extreme(current: &mut ScalarValue, next: ScalarValue, order: Ordering) -> bool {
    if !next.is_null() && (current.is_null() || next.partial_cmp(current) == Some(order))
    {
        *current = next;
        true
    } else {
        false
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
