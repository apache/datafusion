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

use arrow::array::{Array, ArrayRef, AsArray, BooleanArray, Datum, RecordBatch, Scalar};
use arrow::buffer::BooleanBuffer;
use arrow::compute::kernels::cmp;
use arrow::compute::{is_not_null, or};
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
/// Also check a pending string winner before the next value crosses this budget.
/// A single string can exceed the budget on its own.
const STRING_PREFIX_BYTES: usize = 64 * 1024;

/// A recognized comparison, normalized to `outer OP inner`.
#[derive(Debug, Clone, Copy)]
pub(super) struct SemiAntiComparison {
    pub(super) outer_column: usize,
    inner_column: usize,
    op: Operator,
}

/// Cached once per buffered key group and reused across outer batches.
#[derive(Debug)]
pub(super) enum GroupSummary<S = Box<str>> {
    Scalars {
        extreme: Scalar<ArrayRef>,
        other: Option<Scalar<ArrayRef>>,
    },
    /// The range extremum or the single distinct non-null string for `!=`.
    String(Option<S>),
    /// Two distinct non-null strings witness `!=` for every non-null outer row.
    AllNonNull,
}

impl GroupSummary<&str> {
    fn from_scalars(extreme: &ScalarValue, other: Option<&ScalarValue>) -> Result<Self> {
        Ok(Self::Scalars {
            extreme: extreme.to_scalar()?,
            other: other.map(ScalarValue::to_scalar).transpose()?,
        })
    }

    pub(super) fn memory_size(&self) -> usize {
        let array_size = |value: &Scalar<ArrayRef>| value.get().0.get_array_memory_size();
        size_of::<GroupSummary>()
            + match self {
                Self::Scalars { extreme, other } => {
                    array_size(extreme) + other.as_ref().map_or(0, array_size)
                }
                Self::String(value) => value.map_or(0, str::len),
                Self::AllNonNull => 0,
            }
    }

    /// String winners borrow the buffered input until the reservation admits
    /// their exact byte lengths. Fixed-width Arrow scalars are already owned.
    pub(super) fn into_owned(self) -> GroupSummary {
        match self {
            Self::Scalars { extreme, other } => GroupSummary::Scalars { extreme, other },
            Self::String(value) => GroupSummary::String(value.map(Box::from)),
            Self::AllNonNull => GroupSummary::AllNonNull,
        }
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

    /// Probe the first two rows directly, then check extrema after 8, 16, 32, ...
    /// rows, or earlier for wide strings. Each reduction visits only new rows
    /// even when a checkpoint crosses an input batch. String `!=` probes the
    /// first non-null value; a second distinct value determines every outer slice.
    ///
    /// The callback records the mask for this outer slice and returns true once
    /// every matchable row has a witness. An exact summary can serve later
    /// outer slices; an early stop without one leaves the input buffered.
    pub(super) fn scan<'a>(
        &self,
        batches: &'a [RecordBatch],
        outer: &RecordBatch,
        mut matches_all: impl FnMut(&BooleanArray) -> bool,
    ) -> Result<Option<GroupSummary<&'a str>>> {
        let data_type = batches[0].column(self.inner_column).data_type();
        if data_type.is_string() && self.op == Operator::NotEq {
            return self.scan_string_not_equal(batches, outer, matches_all);
        }

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
        let wants_max = matches!(self.op, Operator::Lt | Operator::LtEq);
        let order = if wants_max {
            Ordering::Greater
        } else {
            Ordering::Less
        };
        if data_type.is_string() {
            return self.scan_string_range(batches, outer, order, matches_all);
        }
        let mut extreme = ScalarValue::try_from(data_type)?;
        let mut other = (self.op == Operator::NotEq).then(|| extreme.clone());
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
                let next = if wants_max {
                    max_batch(&values)?
                } else {
                    min_batch(&values)?
                };
                changed |= update_extreme(&mut extreme, next, order);
                if let Some(max) = &mut other {
                    changed |=
                        update_extreme(max, max_batch(&values)?, Ordering::Greater);
                }
                start += len;
                scanned += len;
                if scanned == checkpoint && !finished {
                    if changed {
                        let summary =
                            GroupSummary::from_scalars(&extreme, other.as_ref())?;
                        if matches_all(&self.evaluate(&summary, outer)?) {
                            return Ok(None);
                        }
                    }
                    changed = false;
                    checkpoint = checkpoint.saturating_mul(2);
                }
            }
        }
        let summary = GroupSummary::from_scalars(&extreme, other.as_ref())?;
        if changed {
            matches_all(&self.evaluate(&summary, outer)?);
        }
        Ok(Some(summary))
    }

    fn scan_string_range<'a>(
        &self,
        batches: &'a [RecordBatch],
        outer: &RecordBatch,
        order: Ordering,
        mut matches_all: impl FnMut(&BooleanArray) -> bool,
    ) -> Result<Option<GroupSummary<&'a str>>> {
        let mut extreme = None;
        let mut changed = false;
        let mut checkpoint = FIRST_PREFIX_ROWS;
        let mut bytes = 0usize;
        let values = batches
            .iter()
            .flat_map(|b| strings(b.column(self.inner_column)));
        for (row, value) in values.enumerate() {
            let len = value.map_or(0, str::len);
            if row == checkpoint || len > STRING_PREFIX_BYTES.saturating_sub(bytes) {
                if changed
                    && matches_all(&self.evaluate(&GroupSummary::String(extreme), outer)?)
                {
                    return Ok(None);
                }
                changed = false;
                bytes = 0;
                if row == checkpoint {
                    checkpoint = checkpoint.saturating_mul(2);
                }
            }
            bytes = bytes.saturating_add(len);
            if let Some(value) = value
                && extreme.is_none_or(|old| value.cmp(old) == order)
            {
                extreme = Some(value);
                // The direct probes already recorded matches for these rows.
                changed |= row >= DIRECT_PROBES;
            }
        }
        let summary = GroupSummary::String(extreme);
        if changed {
            matches_all(&self.evaluate(&summary, outer)?);
        }
        Ok(Some(summary))
    }

    fn scan_string_not_equal<'a>(
        &self,
        batches: &'a [RecordBatch],
        outer: &RecordBatch,
        mut matches_all: impl FnMut(&BooleanArray) -> bool,
    ) -> Result<Option<GroupSummary<&'a str>>> {
        let mut representative = None;
        for batch in batches {
            for (row, value) in strings(batch.column(self.inner_column)).enumerate() {
                let Some(value) = value else { continue };
                match representative {
                    None => {
                        representative = Some(value);
                        if matches_all(&self.evaluate_inner_row(batch, row, outer)?) {
                            return Ok(None);
                        }
                    }
                    Some(first) if value != first => {
                        let summary = GroupSummary::AllNonNull;
                        matches_all(&self.evaluate(&summary, outer)?);
                        return Ok(Some(summary));
                    }
                    _ => {}
                }
            }
        }
        Ok(Some(GroupSummary::String(representative)))
    }

    pub(super) fn evaluate<S: AsRef<str>>(
        &self,
        summary: &GroupSummary<S>,
        outer: &RecordBatch,
    ) -> Result<BooleanArray> {
        match summary {
            GroupSummary::Scalars { extreme, other } => {
                let matched = self.compare(extreme, outer)?;
                match other {
                    Some(other) => Ok(or(&matched, &self.compare(other, outer)?)?),
                    None => Ok(matched),
                }
            }
            GroupSummary::AllNonNull => Ok(is_not_null(outer.column(self.outer_column))?),
            GroupSummary::String(value) => {
                let extreme = value.as_ref().map(AsRef::as_ref);
                let hits = strings(outer.column(self.outer_column)).map(|value| {
                    value.zip(extreme).is_some_and(|(x, y)| match self.op {
                        Operator::Lt => x < y,
                        Operator::LtEq => x <= y,
                        Operator::Gt => x > y,
                        Operator::GtEq => x >= y,
                        Operator::NotEq => x != y,
                        _ => unreachable!("invalid semi/anti summary comparison"),
                    })
                });
                Ok(BooleanArray::new(BooleanBuffer::from_iter(hits), None))
            }
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

fn strings(column: &ArrayRef) -> Box<dyn Iterator<Item = Option<&str>> + '_> {
    match column.data_type() {
        DataType::Utf8 => Box::new(column.as_string::<i32>().iter()),
        DataType::LargeUtf8 => Box::new(column.as_string::<i64>().iter()),
        DataType::Utf8View => Box::new(column.as_string_view().iter()),
        _ => unreachable!("expected a string column"),
    }
}

/// Update a fixed-width extremum, ignoring NULLs.
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
