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

//! Evaluation of aggregate function arguments against input batches.

use std::sync::Arc;

use crate::PhysicalExpr;
use arrow::array::{ArrayRef, BooleanArray};
use arrow::record_batch::RecordBatch;
use datafusion_common::{Result, ScalarValue};
use datafusion_physical_expr::expressions::Literal;

/// Largest literal array, in bytes, that an [`AggregateArgument`] keeps for
/// later batches.
///
/// The kept array lives as long as the aggregate stream and is not tracked by
/// a memory pool, so larger arrays are rebuilt for every batch instead.
const MAX_KEPT_LITERAL_ARRAY_BYTES: usize = 1024 * 1024;

/// One argument of an aggregate function, such as `x` in `SUM(x)`.
///
/// Accumulators take one value per input row, so an argument that evaluates to
/// a single value must be expanded to an array for every batch. A literal
/// argument, such as the `1` in `COUNT(1)` (which is how `COUNT(*)` is planned)
/// or the `','` in `STRING_AGG(x, ',')`, has the same value for every batch of
/// the query. Its array is built on demand and reused for later batches, which
/// receive a zero-copy slice of it.
///
/// Only literals are treated this way. Other expressions are evaluated against
/// every batch: a [`ColumnarValue::Scalar`] result holds one value for the rows
/// of a single batch, and the next batch may produce a different value.
///
/// [`ColumnarValue::Scalar`]: datafusion_expr::ColumnarValue::Scalar
pub(in crate::aggregates) struct AggregateArgument {
    expr: Arc<dyn PhysicalExpr>,
    /// Set when `expr` is a [`Literal`].
    literal: Option<LiteralArray>,
}

impl AggregateArgument {
    pub(in crate::aggregates) fn new(expr: Arc<dyn PhysicalExpr>) -> Self {
        let literal = expr
            .downcast_ref::<Literal>()
            .map(|literal| LiteralArray::new(literal.value().clone()));
        Self { expr, literal }
    }

    pub(in crate::aggregates) fn expr(&self) -> &Arc<dyn PhysicalExpr> {
        &self.expr
    }

    /// Evaluates the argument against `batch`, returning one value per row.
    pub(in crate::aggregates) fn evaluate(
        &mut self,
        batch: &RecordBatch,
    ) -> Result<ArrayRef> {
        match &mut self.literal {
            Some(literal) => literal.array_of_size(batch.num_rows()),
            None => self
                .expr
                .evaluate(batch)?
                .into_array_of_size(batch.num_rows()),
        }
    }

    /// Evaluates the argument for the rows of `batch` where `selection` is
    /// true, returning one value per row of `batch`.
    ///
    /// Rows where `selection` is not true may hold any value, because the
    /// selection is also passed to [`GroupsAccumulator::convert_to_state`],
    /// which ignores them.
    ///
    /// [`GroupsAccumulator::convert_to_state`]: datafusion_expr::GroupsAccumulator::convert_to_state
    pub(in crate::aggregates) fn evaluate_selection(
        &mut self,
        batch: &RecordBatch,
        selection: &BooleanArray,
    ) -> Result<ArrayRef> {
        match &mut self.literal {
            Some(literal) if selection.has_true() => {
                literal.array_of_size(batch.num_rows())
            }
            _ => self
                .expr
                .evaluate_selection(batch, selection)?
                .into_array_of_size(batch.num_rows()),
        }
    }
}

/// A literal value and the largest reusable array retained so far.
struct LiteralArray {
    value: ScalarValue,
    /// `None` until the first array is built, or while every array built has
    /// been larger than [`MAX_KEPT_LITERAL_ARRAY_BYTES`].
    array: Option<ArrayRef>,
}

impl LiteralArray {
    fn new(value: ScalarValue) -> Self {
        Self { value, array: None }
    }

    fn array_of_size(&mut self, num_rows: usize) -> Result<ArrayRef> {
        if let Some(array) = &self.array
            && array.len() >= num_rows
        {
            return Ok(array.slice(0, num_rows));
        }

        let array = self.value.to_array_of_size(num_rows)?;
        if array.get_array_memory_size() <= MAX_KEPT_LITERAL_ARRAY_BYTES {
            self.array = Some(Arc::clone(&array));
        }
        Ok(array)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use arrow::array::{AsArray, Int32Array};
    use arrow::datatypes::{DataType, Field, Int32Type, Schema};
    use datafusion_physical_expr::expressions::{col, lit};

    fn batch(num_rows: usize) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, true)]));
        let values = Int32Array::from_iter_values(0..num_rows as i32);
        RecordBatch::try_new(schema, vec![Arc::new(values)]).unwrap()
    }

    fn values_ptr(array: &ArrayRef) -> *const i32 {
        array.as_primitive::<Int32Type>().values().as_ptr()
    }

    #[test]
    fn literal_array_is_reused_across_batches() -> Result<()> {
        let mut argument = AggregateArgument::new(lit(1i32));

        let first = argument.evaluate(&batch(4))?;
        let smaller = argument.evaluate(&batch(3))?;
        assert_eq!(
            smaller.as_primitive::<Int32Type>(),
            &Int32Array::from(vec![1; 3])
        );
        assert_eq!(values_ptr(&smaller), values_ptr(&first));

        // A larger batch builds a longer array, which serves later batches
        let larger = argument.evaluate(&batch(6))?;
        assert_eq!(
            larger.as_primitive::<Int32Type>(),
            &Int32Array::from(vec![1; 6])
        );
        let after_larger = argument.evaluate(&batch(5))?;
        assert_eq!(values_ptr(&after_larger), values_ptr(&larger));

        assert!(argument.evaluate(&batch(0))?.is_empty());
        Ok(())
    }

    #[test]
    fn large_literal_array_is_not_kept() -> Result<()> {
        let mut argument = AggregateArgument::new(lit("x".repeat(1024)));

        let first = argument.evaluate(&batch(2048))?;
        let second = argument.evaluate(&batch(2048))?;
        assert_eq!(first.as_ref(), second.as_ref());
        assert_ne!(
            first.as_string::<i32>().values().as_ptr(),
            second.as_string::<i32>().values().as_ptr()
        );
        Ok(())
    }

    #[test]
    fn non_literal_is_evaluated_for_every_batch() -> Result<()> {
        let schema = batch(0).schema();
        let mut argument = AggregateArgument::new(col("a", &schema)?);

        let first = argument.evaluate(&batch(3))?;
        assert_eq!(
            first.as_primitive::<Int32Type>(),
            &Int32Array::from(vec![0, 1, 2])
        );
        let second = argument.evaluate(&batch(2))?;
        assert_eq!(
            second.as_primitive::<Int32Type>(),
            &Int32Array::from(vec![0, 1])
        );
        Ok(())
    }

    #[test]
    fn literal_selection_matches_evaluate_selection() -> Result<()> {
        let batch = batch(3);
        let expr = lit(7i32);
        let mut argument = AggregateArgument::new(Arc::clone(&expr));

        for selection in [
            BooleanArray::from(vec![true, true, true]),
            BooleanArray::from(vec![false, true, false]),
            BooleanArray::from(vec![Some(false), None, Some(true)]),
            BooleanArray::from(vec![false, false, false]),
            BooleanArray::from(vec![None, Some(false), None]),
        ] {
            let expected = expr
                .evaluate_selection(&batch, &selection)?
                .into_array(batch.num_rows())?;
            let actual = argument.evaluate_selection(&batch, &selection)?;
            assert_eq!(
                actual.as_ref(),
                expected.as_ref(),
                "selection {selection:?}"
            );
        }
        Ok(())
    }
}
