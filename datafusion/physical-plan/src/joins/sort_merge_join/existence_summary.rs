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

//! Exact bounded summaries of an ordinary semi/anti join's residual predicate.
//!
//! For one equi-key group, existence of `outer != inner` needs at most one
//! non-null representative and a second-distinct-value bit. Existence of
//! `outer <[=] inner` needs the inner maximum, and `outer >[=] inner` needs its
//! minimum. Existence distributes over OR, but not AND: two comparisons may
//! have different witnesses. Each compiled disjunct therefore has at most one
//! cross-side comparison, with any total side-local guards attached to it.
//!
//! Results are non-null *existence* bits, not the residual's three-valued SQL
//! result. A null comparison is not a witness. The caller supplies only rows
//! from one matching equi-key group and implements semi/anti polarity itself.
//! Mark and null-aware joins must not use this interface.
//!
//! Input batches are borrowed only during update/evaluation. A representative
//! is copied after reserving its storage, never retained as a slice of a source
//! array. The same dedicated reservation must accompany updates and resets.
//! Memory admission failure is an execution error; after input has been
//! consumed it is not safe to fall back without an explicit replay path.

use std::cmp::Ordering;
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, BooleanArray, RecordBatch};
use arrow::compute::SortOptions;
use arrow::compute::kernels::cmp::{gt, gt_eq, lt, lt_eq, neq};
use arrow::datatypes::{DataType, Schema};
use arrow_ord::ord::make_comparator;
use datafusion_common::{JoinSide, Result, ScalarValue, internal_err};
use datafusion_execution::memory_pool::MemoryReservation;
use datafusion_expr::Operator;
use datafusion_physical_expr::PhysicalExpr;
use datafusion_physical_expr::expressions::{
    BinaryExpr, CaseExpr, Column, IsNotNullExpr, IsNullExpr, Literal, NotExpr,
};

use crate::joins::utils::JoinFilter;

type Expr = Arc<dyn PhysicalExpr>;

// Bound compilation work, expression recursion and the number of retained
// representatives even if distributive normalization would grow exponentially.
const MAX_CLAUSES: usize = 32;
const MAX_DEPTH: usize = 64;

#[derive(Debug, Clone)]
struct Comparison {
    outer: Expr,
    inner: Expr,
    op: Operator,
}

#[derive(Debug, Default)]
struct State {
    /// Used for clauses with only side-local predicates. Even an outer TRUE
    /// needs at least one qualifying inner row to witness existence.
    present: bool,
    representative: Option<ScalarValue>,
    multiple: bool,
    reserved: usize,
}

impl State {
    fn clear(&mut self, reservation: &MemoryReservation) {
        self.representative = None;
        reservation.shrink(self.reserved);
        *self = Self::default();
    }

    fn set_multiple(&mut self, reservation: &MemoryReservation) {
        self.clear(reservation);
        self.multiple = true;
    }

    /// Copy only the candidate representative. Reserve the simultaneous old
    /// and new copies, then release the old copy; do not count only net growth.
    fn replace_from_array(
        &mut self,
        array: &ArrayRef,
        index: usize,
        reservation: &MemoryReservation,
        peak: &mut usize,
    ) -> Result<()> {
        let bytes = scalar_storage_size(array, index)?;
        reservation.try_grow(bytes)?;
        *peak = (*peak).max(reservation.size());
        let candidate = match ScalarValue::try_from_array(array, index) {
            Ok(value) => value,
            Err(error) => {
                reservation.shrink(bytes);
                return Err(error);
            }
        };
        self.representative = Some(candidate);
        reservation.shrink(self.reserved);
        self.reserved = bytes;
        Ok(())
    }
}

#[derive(Debug, Default)]
struct Clause {
    outer_guard: Option<Expr>,
    inner_guard: Option<Expr>,
    comparison: Option<Comparison>,
    state: State,
}

/// A compiled residual and the bounded state for its current inner key group.
#[derive(Debug)]
pub(super) struct ExistenceSummary {
    clauses: Vec<Clause>,
    peak: usize,
}

impl ExistenceSummary {
    /// Compile only exact, total expressions over supported identically typed
    /// values. `None` means the ordinary residual path must be retained.
    pub(super) fn try_new(
        filter: &JoinFilter,
        outer_is_left: bool,
        outer_schema: &Schema,
        inner_schema: &Schema,
    ) -> Result<Option<Self>> {
        let compiler = Compiler {
            filter,
            outer_is_left,
            outer_schema,
            inner_schema,
        };
        Ok(compiler
            .compile(filter.expression(), 0)?
            .map(|clauses| Self { clauses, peak: 0 }))
    }

    /// Incorporate one slice of the current inner group. The caller bounds
    /// slice size for cancellation latency and continues draining the group
    /// even when every not-equal summary has saturated.
    pub(super) fn update(
        &mut self,
        inner: &RecordBatch,
        reservation: &MemoryReservation,
    ) -> Result<()> {
        for clause in &mut self.clauses {
            let guard = evaluate_guard(clause.inner_guard.as_ref(), inner)?;
            let selected = |row| {
                guard
                    .as_ref()
                    .is_none_or(|g| g.is_valid(row) && g.value(row))
            };
            let Some(comparison) = &clause.comparison else {
                clause.state.present |= (0..inner.num_rows()).any(selected);
                continue;
            };
            if clause.state.multiple {
                continue;
            }
            let values = comparison
                .inner
                .evaluate(inner)?
                .into_array(inner.num_rows())?;
            let Some(first) =
                (0..values.len()).find(|&row| selected(row) && values.is_valid(row))
            else {
                continue;
            };
            if comparison.op == Operator::NotEq {
                if clause.state.representative.is_none() {
                    clause.state.replace_from_array(
                        &values,
                        first,
                        reservation,
                        &mut self.peak,
                    )?;
                }
                let value = clause.state.representative.as_ref().unwrap();
                for row in first..values.len() {
                    if selected(row)
                        && values.is_valid(row)
                        && !value.eq_array(&values, row)?
                    {
                        clause.state.set_multiple(reservation);
                        break;
                    }
                }
            } else {
                let maximize = matches!(comparison.op, Operator::Lt | Operator::LtEq);
                let comparator = make_comparator(
                    values.as_ref(),
                    values.as_ref(),
                    SortOptions::default(),
                )?;
                let mut candidate = first;
                for row in first + 1..values.len() {
                    if selected(row) && values.is_valid(row) {
                        let ordering = comparator(row, candidate);
                        if (maximize && ordering == Ordering::Greater)
                            || (!maximize && ordering == Ordering::Less)
                        {
                            candidate = row;
                        }
                    }
                }
                // Compare one candidate with the previous batch's summary.
                // The temporary scalar copy is admitted before allocation too.
                let replace = if let Some(previous) = &clause.state.representative {
                    let bytes = scalar_storage_size(&values, candidate)?;
                    reservation.try_grow(bytes)?;
                    self.peak = self.peak.max(reservation.size());
                    let ordering = match ScalarValue::try_from_array(&values, candidate) {
                        Ok(value) => value.partial_cmp(previous),
                        Err(error) => {
                            reservation.shrink(bytes);
                            return Err(error);
                        }
                    };
                    reservation.shrink(bytes);
                    let Some(ordering) = ordering else {
                        return internal_err!("Invalid existence-summary comparison");
                    };
                    (maximize && ordering == Ordering::Greater)
                        || (!maximize && ordering == Ordering::Less)
                } else {
                    true
                };
                if replace {
                    clause.state.replace_from_array(
                        &values,
                        candidate,
                        reservation,
                        &mut self.peak,
                    )?;
                }
            }
        }
        Ok(())
    }

    /// Return non-null witness bits for an outer slice. Null outer values do
    /// not match even when a not-equal summary contains two distinct values.
    pub(super) fn evaluate(&self, outer: &RecordBatch) -> Result<BooleanArray> {
        let mut matches = vec![false; outer.num_rows()];
        for clause in &self.clauses {
            let guard = evaluate_guard(clause.outer_guard.as_ref(), outer)?;
            let values = match &clause.comparison {
                Some(comparison) => Some(
                    comparison
                        .outer
                        .evaluate(outer)?
                        .into_array(outer.num_rows())?,
                ),
                None => None,
            };
            let compared = match (&clause.comparison, &clause.state.representative) {
                (Some(comparison), Some(value)) => {
                    let values = values.as_ref().unwrap();
                    let scalar = value.to_scalar()?;
                    let compare = match comparison.op {
                        Operator::NotEq => neq,
                        Operator::Lt => lt,
                        Operator::LtEq => lt_eq,
                        Operator::Gt => gt,
                        Operator::GtEq => gt_eq,
                        _ => unreachable!("compiler admits only summary comparisons"),
                    };
                    Some(compare(&values.as_ref(), &scalar)?)
                }
                _ => None,
            };
            for (row, matched) in matches.iter_mut().enumerate() {
                if guard
                    .as_ref()
                    .is_some_and(|g| g.is_null(row) || !g.value(row))
                {
                    continue;
                }
                *matched |= match &values {
                    None => clause.state.present,
                    Some(values) if clause.state.multiple => values.is_valid(row),
                    Some(_) => compared
                        .as_ref()
                        .is_some_and(|c| c.is_valid(row) && c.value(row)),
                };
            }
        }
        Ok(BooleanArray::from(matches))
    }

    /// Release all representative storage before beginning another key group.
    pub(super) fn reset(&mut self, reservation: &MemoryReservation) {
        for clause in &mut self.clauses {
            clause.state.clear(reservation);
        }
    }

    /// Admitted owned representative storage. Fixed compiler metadata is
    /// bounded by MAX_CLAUSES and shared expression trees are not input buffers.
    #[cfg(test)]
    pub(super) fn size(&self) -> usize {
        self.clauses.iter().map(|c| c.state.reserved).sum()
    }

    /// Peak admitted representative storage over this stream's lifetime,
    /// including old/new copy overlap and states released during an update.
    /// Batch expression evaluation and Arrow comparison scratch are transient
    /// batch-sized allocations, not retained group state measured here.
    pub(super) fn peak_size(&self) -> usize {
        self.peak
    }
}

fn evaluate_guard(
    guard: Option<&Expr>,
    batch: &RecordBatch,
) -> Result<Option<BooleanArray>> {
    guard
        .map(|guard| {
            let array = guard.evaluate(batch)?.into_array(batch.num_rows())?;
            array
                .as_any()
                .downcast_ref::<BooleanArray>()
                .cloned()
                .ok_or_else(|| {
                    datafusion_common::internal_datafusion_err!(
                        "Non-Boolean summary guard"
                    )
                })
        })
        .transpose()
}

/// ScalarValue copies strings with `to_string`, whose requested capacity is
/// exactly their byte length. Fixed-width values do not retain array buffers.
fn scalar_storage_size(array: &ArrayRef, row: usize) -> Result<usize> {
    use arrow::array::{LargeStringArray, StringArray, StringViewArray};
    let payload = match array.data_type() {
        DataType::Utf8 => array
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(row)
            .len(),
        DataType::LargeUtf8 => array
            .as_any()
            .downcast_ref::<LargeStringArray>()
            .unwrap()
            .value(row)
            .len(),
        DataType::Utf8View => array
            .as_any()
            .downcast_ref::<StringViewArray>()
            .unwrap()
            .value(row)
            .len(),
        DataType::Timestamp(_, zone) => zone.as_ref().map_or(0, |z| z.len()),
        _ => 0,
    };
    size_of::<ScalarValue>()
        .checked_add(payload)
        .ok_or_else(|| {
            datafusion_common::internal_datafusion_err!("Summary storage size overflow")
        })
}

struct Compiler<'a> {
    filter: &'a JoinFilter,
    outer_is_left: bool,
    outer_schema: &'a Schema,
    inner_schema: &'a Schema,
}

impl Compiler<'_> {
    /// 0 = literal only; 1 = outer; 2 = inner; 3 = both.
    fn sides(&self, expression: &Expr, depth: usize) -> Option<u8> {
        if depth > MAX_DEPTH {
            return None;
        }
        if let Some(column) = expression.downcast_ref::<Column>() {
            let mapping = self.filter.column_indices().get(column.index())?;
            let is_outer = (mapping.side == JoinSide::Left) == self.outer_is_left;
            if !matches!(mapping.side, JoinSide::Left | JoinSide::Right) {
                return None;
            }
            let source = if is_outer {
                self.outer_schema
            } else {
                self.inner_schema
            };
            let field = source.fields().get(mapping.index)?;
            if field.data_type()
                != self
                    .filter
                    .schema()
                    .fields()
                    .get(column.index())?
                    .data_type()
            {
                return None;
            }
            return Some(if is_outer { 1 } else { 2 });
        }
        let mut sides = 0;
        for child in expression.children() {
            sides |= self.sides(child, depth + 1)?;
        }
        Some(sides)
    }

    fn localize(&self, expression: &Expr) -> Result<Expr> {
        if let Some(column) = expression.downcast_ref::<Column>() {
            let mapping = &self.filter.column_indices()[column.index()];
            return Ok(Arc::new(Column::new(column.name(), mapping.index)));
        }
        let children = expression
            .children()
            .iter()
            .map(|child| self.localize(child))
            .collect::<Result<Vec<_>>>()?;
        Arc::clone(expression).with_new_children(children)
    }

    /// Whitelist concrete total expression implementations, not names or just
    /// volatility. Arithmetic, casts, UDFs and dictionary/nested/float values
    /// retain the generic path. CASE admits normalized COALESCE expressions
    /// only when every branch and condition is independently total.
    fn safe(&self, expression: &Expr, depth: usize) -> Result<bool> {
        if depth > MAX_DEPTH {
            return Ok(false);
        }
        let data_type = expression.data_type(self.filter.schema())?;
        if !supported_type(&data_type) && data_type != DataType::Null {
            return Ok(false);
        }
        if expression.is::<Column>() || expression.is::<Literal>() {
            return Ok(true);
        }
        let allowed = if let Some(binary) = expression.downcast_ref::<BinaryExpr>() {
            let left = binary.left().data_type(self.filter.schema())?;
            let right = binary.right().data_type(self.filter.schema())?;
            match binary.op() {
                Operator::And | Operator::Or => {
                    left == DataType::Boolean && right == left
                }
                Operator::Eq
                | Operator::NotEq
                | Operator::Lt
                | Operator::LtEq
                | Operator::Gt
                | Operator::GtEq => supported_type(&left) && left == right,
                _ => false,
            }
        } else if expression.is::<NotExpr>() {
            expression.children()[0].data_type(self.filter.schema())? == DataType::Boolean
        } else if expression.is::<IsNullExpr>() || expression.is::<IsNotNullExpr>() {
            true
        } else if let Some(case) = expression.downcast_ref::<CaseExpr>() {
            // Searched CASE only. It includes the normalized COALESCE form;
            // excluding simple CASE avoids additional comparison semantics.
            case.expr().is_none()
                && case.when_then_expr().iter().all(|(when, then)| {
                    when.data_type(self.filter.schema()).ok() == Some(DataType::Boolean)
                        && then
                            .data_type(self.filter.schema())
                            .is_ok_and(|t| t == data_type || t == DataType::Null)
                })
                && case.else_expr().is_none_or(|e| {
                    e.data_type(self.filter.schema())
                        .is_ok_and(|t| t == data_type || t == DataType::Null)
                })
        } else {
            false
        };
        if !allowed {
            return Ok(false);
        }
        for child in expression.children() {
            if !self.safe(child, depth + 1)? {
                return Ok(false);
            }
        }
        Ok(true)
    }

    fn compile(&self, expression: &Expr, depth: usize) -> Result<Option<Vec<Clause>>> {
        if depth > MAX_DEPTH
            || expression.data_type(self.filter.schema())? != DataType::Boolean
        {
            return Ok(None);
        }
        let Some(sides) = self.sides(expression, 0) else {
            return Ok(None);
        };
        // Keep whole local predicates together, preserving their SQL null
        // semantics and avoiding unnecessary distributive expansion.
        if sides != 3 {
            if !self.safe(expression, 0)? {
                return Ok(None);
            }
            let mut clause = Clause::default();
            if sides == 1 {
                clause.outer_guard = Some(self.localize(expression)?);
            } else {
                clause.inner_guard = Some(self.localize(expression)?);
            }
            return Ok(Some(vec![clause]));
        }
        if let Some(binary) = expression.downcast_ref::<BinaryExpr>()
            && matches!(binary.op(), Operator::And | Operator::Or)
        {
            let Some(left) = self.compile(binary.left(), depth + 1)? else {
                return Ok(None);
            };
            let Some(right) = self.compile(binary.right(), depth + 1)? else {
                return Ok(None);
            };
            if binary.op() == &Operator::Or {
                if left.len() + right.len() > MAX_CLAUSES {
                    return Ok(None);
                }
                return Ok(Some(left.into_iter().chain(right).collect()));
            }
            if left.len() * right.len() > MAX_CLAUSES {
                return Ok(None);
            }
            let mut clauses = Vec::with_capacity(left.len() * right.len());
            for a in &left {
                for b in &right {
                    // Independent summaries cannot preserve a shared witness.
                    if a.comparison.is_some() && b.comparison.is_some() {
                        return Ok(None);
                    }
                    clauses.push(Clause {
                        outer_guard: and_guards(
                            a.outer_guard.as_ref(),
                            b.outer_guard.as_ref(),
                        ),
                        inner_guard: and_guards(
                            a.inner_guard.as_ref(),
                            b.inner_guard.as_ref(),
                        ),
                        comparison: a.comparison.clone().or_else(|| b.comparison.clone()),
                        state: State::default(),
                    });
                }
            }
            return Ok(Some(clauses));
        }
        let (binary, mut op) = if let Some(not) = expression.downcast_ref::<NotExpr>() {
            let Some(binary) = not.arg().downcast_ref::<BinaryExpr>() else {
                return Ok(None);
            };
            let op = match binary.op() {
                Operator::Eq => Operator::NotEq,
                Operator::Lt => Operator::GtEq,
                Operator::LtEq => Operator::Gt,
                Operator::Gt => Operator::LtEq,
                Operator::GtEq => Operator::Lt,
                _ => return Ok(None),
            };
            (binary, op)
        } else if let Some(binary) = expression.downcast_ref::<BinaryExpr>() {
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
        ) || !self.safe(binary.left(), 0)?
            || !self.safe(binary.right(), 0)?
        {
            return Ok(None);
        }
        let (outer, inner) =
            match (self.sides(binary.left(), 0), self.sides(binary.right(), 0)) {
                (Some(1), Some(2)) => (binary.left(), binary.right()),
                (Some(2), Some(1)) => {
                    op = op.swap().unwrap();
                    (binary.right(), binary.left())
                }
                _ => return Ok(None),
            };
        let data_type = outer.data_type(self.filter.schema())?;
        if !supported_type(&data_type)
            || data_type != inner.data_type(self.filter.schema())?
        {
            return Ok(None);
        }
        Ok(Some(vec![Clause {
            comparison: Some(Comparison {
                outer: self.localize(outer)?,
                inner: self.localize(inner)?,
                op,
            }),
            ..Default::default()
        }]))
    }
}

fn and_guards(left: Option<&Expr>, right: Option<&Expr>) -> Option<Expr> {
    match (left, right) {
        (Some(left), Some(right)) => Some(Arc::new(BinaryExpr::new(
            Arc::clone(left),
            Operator::And,
            Arc::clone(right),
        ))),
        (Some(expression), None) | (None, Some(expression)) => {
            Some(Arc::clone(expression))
        }
        (None, None) => None,
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

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{Field, SchemaRef};
    use datafusion_execution::memory_pool::{
        GreedyMemoryPool, MemoryConsumer, MemoryPool,
    };

    fn column(index: usize) -> Expr {
        Arc::new(Column::new("value", index))
    }

    fn binary(left: Expr, op: Operator, right: Expr) -> Expr {
        Arc::new(BinaryExpr::new(left, op, right))
    }

    fn filter(expression: Expr, data_type: DataType) -> (JoinFilter, SchemaRef) {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            data_type.clone(),
            true,
        )]));
        let filter_schema = Arc::new(Schema::new(vec![
            Field::new("left", data_type.clone(), true),
            Field::new("right", data_type, true),
        ]));
        (
            JoinFilter::new(
                expression,
                JoinFilter::build_column_indices(vec![0], vec![0]),
                filter_schema,
            ),
            schema,
        )
    }

    fn reservation(limit: usize) -> MemoryReservation {
        let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(limit));
        MemoryConsumer::new("existence_summary_test").register(&pool)
    }

    fn ints(schema: &SchemaRef, values: Vec<Option<i64>>) -> RecordBatch {
        RecordBatch::try_new(Arc::clone(schema), vec![Arc::new(Int64Array::from(values))])
            .unwrap()
    }

    #[test]
    fn not_equal_saturates_across_batches_and_reset_releases_memory() -> Result<()> {
        let expression =
            Arc::new(NotExpr::new(binary(column(0), Operator::Eq, column(1)))) as Expr;
        let (filter, schema) = filter(expression, DataType::Int64);
        let mut summary =
            ExistenceSummary::try_new(&filter, true, &schema, &schema)?.unwrap();
        let reservation = reservation(4096);
        let outer = ints(&schema, vec![Some(7), Some(12), None]);
        summary.update(&ints(&schema, vec![None, Some(7), Some(7)]), &reservation)?;
        assert_eq!(
            summary.evaluate(&outer)?,
            BooleanArray::from(vec![false, true, false])
        );
        assert!(summary.size() > 0);
        assert_eq!(summary.size(), reservation.size());
        summary.update(&ints(&schema, vec![Some(12)]), &reservation)?;
        assert_eq!(
            summary.evaluate(&outer)?,
            BooleanArray::from(vec![true, true, false])
        );
        assert_eq!(reservation.size(), 0);
        assert!(summary.peak_size() > 0);
        summary.reset(&reservation);
        assert_eq!(
            summary.evaluate(&outer)?,
            BooleanArray::from(vec![false; 3])
        );
        summary.update(&ints(&schema, vec![Some(12)]), &reservation)?;
        summary.reset(&reservation);
        assert_eq!(summary.size(), 0);
        assert_eq!(reservation.size(), 0);
        assert!(summary.peak_size() > 0);
        Ok(())
    }

    #[test]
    fn ranges_retain_extrema_across_batches() -> Result<()> {
        for (op, expected) in [
            (Operator::Lt, vec![true, true, true, false, false, false]),
            (Operator::LtEq, vec![true, true, true, true, false, false]),
            (Operator::Gt, vec![false, false, true, true, true, false]),
            (Operator::GtEq, vec![false, true, true, true, true, false]),
        ] {
            let (filter, schema) =
                filter(binary(column(0), op, column(1)), DataType::Int64);
            let mut summary =
                ExistenceSummary::try_new(&filter, true, &schema, &schema)?.unwrap();
            let reservation = reservation(4096);
            summary.update(&ints(&schema, vec![Some(5), None]), &reservation)?;
            summary.update(&ints(&schema, vec![Some(9), Some(7)]), &reservation)?;
            let outer = ints(
                &schema,
                vec![Some(4), Some(5), Some(7), Some(9), Some(10), None],
            );
            assert_eq!(summary.evaluate(&outer)?, BooleanArray::from(expected));
        }
        Ok(())
    }

    #[test]
    fn pure_outer_guard_still_requires_inner_presence() -> Result<()> {
        let (filter, schema) =
            filter(Arc::new(IsNotNullExpr::new(column(0))), DataType::Int64);
        let mut summary =
            ExistenceSummary::try_new(&filter, true, &schema, &schema)?.unwrap();
        let reservation = reservation(4096);
        let outer = ints(&schema, vec![Some(7), None]);
        assert_eq!(
            summary.evaluate(&outer)?,
            BooleanArray::from(vec![false, false])
        );
        summary.update(&ints(&schema, vec![None]), &reservation)?;
        assert_eq!(
            summary.evaluate(&outer)?,
            BooleanArray::from(vec![true, false])
        );
        Ok(())
    }

    #[test]
    fn conjunction_cannot_combine_independent_cross_side_witnesses() -> Result<()> {
        let expression = binary(
            binary(column(0), Operator::Lt, column(1)),
            Operator::And,
            binary(column(0), Operator::Gt, column(1)),
        );
        let (filter, schema) = filter(expression, DataType::Int64);
        assert!(ExistenceSummary::try_new(&filter, true, &schema, &schema)?.is_none());
        Ok(())
    }

    #[test]
    fn normalized_string_case_uses_exact_bytes_and_does_not_pin_sources() -> Result<()> {
        let normalized = |index| -> Result<Expr> {
            Ok(Arc::new(CaseExpr::try_new(
                None,
                vec![(
                    Arc::new(IsNullExpr::new(column(index))) as Expr,
                    Arc::new(Literal::new(ScalarValue::Utf8(Some(String::new()))))
                        as Expr,
                )],
                Some(column(index)),
            )?))
        };
        let expression = binary(normalized(0)?, Operator::NotEq, normalized(1)?);
        let (filter, schema) = filter(expression, DataType::Utf8);
        let mut summary =
            ExistenceSummary::try_new(&filter, true, &schema, &schema)?.unwrap();
        let reservation = reservation(4096);
        let source: ArrayRef = Arc::new(StringArray::from(vec![None::<&str>, None]));
        let inner = RecordBatch::try_new(Arc::clone(&schema), vec![Arc::clone(&source)])?;
        let references = Arc::strong_count(&source);
        summary.update(&inner, &reservation)?;
        assert_eq!(Arc::strong_count(&source), references);
        let outer = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(StringArray::from(vec![
                None,
                Some(""),
                Some("é"),
                Some("e"),
            ]))],
        )?;
        assert_eq!(
            summary.evaluate(&outer)?,
            BooleanArray::from(vec![false, false, true, true])
        );
        summary.reset(&reservation);
        assert_eq!(reservation.size(), 0);
        Ok(())
    }

    #[test]
    fn representative_allocation_is_admitted_before_copy() -> Result<()> {
        let (filter, schema) = filter(
            binary(column(0), Operator::NotEq, column(1)),
            DataType::Utf8,
        );
        let mut summary =
            ExistenceSummary::try_new(&filter, true, &schema, &schema)?.unwrap();
        let reservation = reservation(128);
        let value = "x".repeat(4096);
        let inner = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(StringArray::from(vec![value.as_str()]))],
        )?;
        assert!(summary.update(&inner, &reservation).is_err());
        assert_eq!(summary.size(), 0);
        assert_eq!(reservation.size(), 0);
        summary.reset(&reservation);
        Ok(())
    }

    #[test]
    fn unsupported_arithmetic_float_and_cross_equality_keep_generic_path() -> Result<()> {
        let arithmetic = binary(
            binary(
                column(0),
                Operator::Plus,
                Arc::new(Literal::new(ScalarValue::Int64(Some(1)))),
            ),
            Operator::NotEq,
            column(1),
        );
        for (expression, data_type) in [
            (arithmetic, DataType::Int64),
            (
                binary(column(0), Operator::NotEq, column(1)),
                DataType::Float64,
            ),
            (binary(column(0), Operator::Eq, column(1)), DataType::Int64),
        ] {
            let (filter, schema) = filter(expression, data_type);
            assert!(
                ExistenceSummary::try_new(&filter, true, &schema, &schema)?.is_none()
            );
        }
        Ok(())
    }
}
