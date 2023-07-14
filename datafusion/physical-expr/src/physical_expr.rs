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

use arrow::datatypes::{DataType, Schema};

use arrow::record_batch::RecordBatch;

use datafusion_common::utils::DataPtr;
use datafusion_common::{ColumnStatistics, DataFusionError, Result, ScalarValue};
use datafusion_expr::ColumnarValue;

use std::fmt::{Debug, Display};

use arrow::array::{make_array, Array, ArrayRef, BooleanArray, MutableArrayData};
use arrow::compute::{and_kleene, filter_record_batch, is_not_null, SlicesIterator};

use crate::expressions::Column;
use crate::intervals::cp_solver::PropagationResult;
use crate::intervals::{cardinality_ratio, ExprIntervalGraph, Interval, IntervalBound};
use crate::utils::collect_columns;
use std::any::Any;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

/// Expression that can be evaluated against a RecordBatch
/// A Physical expression knows its type, nullability and how to evaluate itself.
pub trait PhysicalExpr: Send + Sync + Display + Debug + PartialEq<dyn Any> {
    /// Returns the physical expression as [`Any`](std::any::Any) so that it can be
    /// downcast to a specific implementation.
    fn as_any(&self) -> &dyn Any;
    /// Get the data type of this expression, given the schema of the input
    fn data_type(&self, input_schema: &Schema) -> Result<DataType>;
    /// Determine whether this expression is nullable, given the schema of the input
    fn nullable(&self, input_schema: &Schema) -> Result<bool>;
    /// Evaluate an expression against a RecordBatch
    fn evaluate(&self, batch: &RecordBatch) -> Result<ColumnarValue>;
    /// Evaluate an expression against a RecordBatch after first applying a
    /// validity array
    fn evaluate_selection(
        &self,
        batch: &RecordBatch,
        selection: &BooleanArray,
    ) -> Result<ColumnarValue> {
        let tmp_batch = filter_record_batch(batch, selection)?;

        let tmp_result = self.evaluate(&tmp_batch)?;
        // All values from the `selection` filter are true.
        if batch.num_rows() == tmp_batch.num_rows() {
            return Ok(tmp_result);
        }
        if let ColumnarValue::Array(a) = tmp_result {
            let result = scatter(selection, a.as_ref())?;
            Ok(ColumnarValue::Array(result))
        } else {
            Ok(tmp_result)
        }
    }

    /// Get a list of child PhysicalExpr that provide the input for this expr.
    fn children(&self) -> Vec<Arc<dyn PhysicalExpr>>;

    /// Returns a new PhysicalExpr where all children were replaced by new exprs.
    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn PhysicalExpr>>,
    ) -> Result<Arc<dyn PhysicalExpr>>;

    /// Computes bounds for the expression using interval arithmetic.
    fn evaluate_bounds(&self, _children: &[&Interval]) -> Result<Interval> {
        Err(DataFusionError::NotImplemented(format!(
            "Not implemented for {self}"
        )))
    }

    /// Updates/shrinks bounds for the expression using interval arithmetic.
    /// If constraint propagation reveals an infeasibility, returns [None] for
    /// the child causing infeasibility. If none of the children intervals
    /// change, may return an empty vector instead of cloning `children`.
    fn propagate_constraints(
        &self,
        _interval: &Interval,
        _children: &[&Interval],
    ) -> Result<Vec<Option<Interval>>> {
        Err(DataFusionError::NotImplemented(format!(
            "Not implemented for {self}"
        )))
    }

    /// Update the hash `state` with this expression requirements from
    /// [`Hash`].
    ///
    /// This method is required to support hashing [`PhysicalExpr`]s.  To
    /// implement it, typically the type implementing
    /// [`PhysicalExpr`] implements [`Hash`] and
    /// then the following boiler plate is used:
    ///
    /// # Example:
    /// ```
    /// // User defined expression that derives Hash
    /// #[derive(Hash, Debug, PartialEq, Eq)]
    /// struct MyExpr {
    ///   val: u64
    /// }
    ///
    /// // impl PhysicalExpr {
    /// // ...
    /// # impl MyExpr {
    ///   // Boiler plate to call the derived Hash impl
    ///   fn dyn_hash(&self, state: &mut dyn std::hash::Hasher) {
    ///     use std::hash::Hash;
    ///     let mut s = state;
    ///     self.hash(&mut s);
    ///   }
    /// // }
    /// # }
    /// ```
    /// Note: [`PhysicalExpr`] is not constrained by [`Hash`]
    /// directly because it must remain object safe.
    fn dyn_hash(&self, _state: &mut dyn Hasher);
}

/// Attempts to refine column boundaries and compute selectivity value.
///
/// The function accepts the boundaries of the input columns in the `context` parameter.
/// It then tries to tighten these boundaries based on the provided `expr`.
/// The resulting selectivity value is calculated by comparing the initial and final boundaries.
/// The computation assumes that the data within the column is uniformly distributed and is not sorted.
///
/// # Arguments
///
/// * `context` - The context which holds the boundaries of the input columns.
/// * `expr` - The expression used to shrink the column boundaries.
///
/// # Returns
///
/// * `AnalysisContext` constructed by pruned boundaries and a selectivity value.
pub fn analyze(
    expr: &Arc<dyn PhysicalExpr>,
    context: AnalysisContext,
) -> Result<AnalysisContext> {
    let target_boundaries = context.boundaries.ok_or_else(|| {
        DataFusionError::Internal("No column exists at the input to filter".to_string())
    })?;

    let mut graph = ExprIntervalGraph::try_new(expr.clone())?;

    let columns: Vec<Arc<dyn PhysicalExpr>> = collect_columns(expr)
        .into_iter()
        .map(|c| Arc::new(c) as Arc<dyn PhysicalExpr>)
        .collect();

    let target_expr_and_indices: Vec<(Arc<dyn PhysicalExpr>, usize)> =
        graph.gather_node_indices(columns.as_slice());

    let mut target_indices_and_boundaries: Vec<(usize, Interval)> =
        target_expr_and_indices
            .iter()
            .filter_map(|(expr, i)| {
                target_boundaries.iter().find_map(|bound| {
                    expr.as_any()
                        .downcast_ref::<Column>()
                        .filter(|expr_column| bound.column.eq(*expr_column))
                        .map(|_| (*i, bound.interval.clone()))
                })
            })
            .collect();

    match graph.update_ranges(&mut target_indices_and_boundaries)? {
        PropagationResult::Success => {
            shrink_boundaries(expr, graph, target_boundaries, target_expr_and_indices)
        }
        PropagationResult::Infeasible => Ok(AnalysisContext::new_with_selectivity(
            target_boundaries,
            0.0,
        )),
        PropagationResult::CannotPropagate => Ok(AnalysisContext::new_with_selectivity(
            target_boundaries,
            1.0,
        )),
    }
}

// If the `PropagationResult` returns a success, this function calculates the selectivity by comparing the initial
// and final column boundaries. Following this, the function constructs and returns a new `AnalysisContext`, with
// the updated parameters.
fn shrink_boundaries(
    expr: &Arc<dyn PhysicalExpr>,
    mut graph: ExprIntervalGraph,
    mut target_boundaries: Vec<ExprBoundaries>,
    target_expr_and_indices: Vec<(Arc<dyn PhysicalExpr>, usize)>,
) -> Result<AnalysisContext> {
    let initial_boundaries = target_boundaries.clone();
    target_expr_and_indices.iter().for_each(|(expr, i)| {
        if let Some(column) = expr.as_any().downcast_ref::<Column>() {
            if let Some(bound) = target_boundaries
                .iter_mut()
                .find(|bound| bound.column.eq(column))
            {
                bound.update_interval(graph.get_interval(*i))
            };
        }
    });
    let graph_nodes = graph.gather_node_indices(&[expr.clone()]);
    let (_, root_index) = graph_nodes.first().ok_or_else(|| {
        DataFusionError::Internal("Error in constructing predicate graph".to_string())
    })?;
    let final_result = graph.get_interval(*root_index);

    let selectivity = calculate_selectivity(
        &final_result.lower.value,
        &final_result.upper.value,
        &target_boundaries,
        &initial_boundaries,
    )?;

    if !(0.0..=1.0).contains(&selectivity) {
        return Err(DataFusionError::Internal(format!(
            "Selectivity is out of limit: {}",
            selectivity
        )));
    }

    Ok(AnalysisContext::new_with_selectivity(
        target_boundaries,
        selectivity,
    ))
}

// This function calculates the filter predicate's selectivity by comparing the initial and pruned column boundaries.
// Selectivity is defined as the ratio of the rows in a table that satisfy the filter's predicate. This function estimates
// the selectivity by comparing the initial and pruned column boundaries. Exact results of propagation at the root, those are
// `[true, true]` or `[false, false]`, lead to early exit, returning a selectivity value of either 1.0 or 0.0.
// `[true, true]` indicates that all data values satisfy the predicate (hence, selectivity is 1.0), and `[false, false]`
// suggests that no data value meets the predicate (therefore, selectivity is 0.0).
fn calculate_selectivity(
    lower_value: &ScalarValue,
    upper_value: &ScalarValue,
    target_boundaries: &[ExprBoundaries],
    initial_boundaries: &[ExprBoundaries],
) -> Result<f64> {
    match (lower_value, upper_value) {
        (ScalarValue::Boolean(Some(true)), ScalarValue::Boolean(Some(true))) => Ok(1.0),
        (ScalarValue::Boolean(Some(false)), ScalarValue::Boolean(Some(false))) => Ok(0.0),
        _ => {
            // Since the intervals are assumed as uniform and we do not
            // have the sort information, we need to multiply the selectivities
            // of multiple columns to get overall selectivity.
            target_boundaries.iter().enumerate().try_fold(
                1.0,
                |acc, (i, ExprBoundaries { interval, .. })| {
                    let temp =
                        cardinality_ratio(&initial_boundaries[i].interval, interval)?;
                    Ok(acc * temp)
                },
            )
        }
    }
}

impl Hash for dyn PhysicalExpr {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.dyn_hash(state);
    }
}

/// Shared [`PhysicalExpr`].
pub type PhysicalExprRef = Arc<dyn PhysicalExpr>;

/// The shared context used during the analysis of an expression. Includes
/// the boundaries for all known columns.
#[derive(Clone, Debug, PartialEq)]
pub struct AnalysisContext {
    // A list of known column boundaries, ordered by the index
    // of the column in the current schema.
    pub boundaries: Option<Vec<ExprBoundaries>>,
    /// The estimated percentage of rows that this expression would select, if
    /// it were to be used as a boolean predicate on a filter. The value will be
    /// between 0.0 (selects nothing) and 1.0 (selects everything).
    pub selectivity: Option<f64>,
}

impl AnalysisContext {
    pub fn new(boundaries: Vec<ExprBoundaries>) -> Self {
        Self {
            boundaries: Some(boundaries),
            selectivity: None,
        }
    }

    pub fn new_with_selectivity(
        boundaries: Vec<ExprBoundaries>,
        selectivity: f64,
    ) -> Self {
        Self {
            boundaries: Some(boundaries),
            selectivity: Some(selectivity),
        }
    }

    /// Create a new analysis context from column statistics.
    pub fn from_statistics(
        input_schema: &Schema,
        statistics: &[ColumnStatistics],
    ) -> Self {
        let mut column_boundaries = vec![];
        for (i, stats) in statistics.iter().enumerate() {
            column_boundaries.push(ExprBoundaries::from_column(
                stats,
                input_schema.fields()[i].name().clone(),
                i,
            ));
        }
        Self::new(column_boundaries)
    }
}

/// Represents the boundaries of the resulting value from a physical expression,
/// if it were to be an expression, if it were to be evaluated.
#[derive(Clone, Debug, PartialEq)]
pub struct ExprBoundaries {
    pub column: Column,
    /// Minimum and Maximum values this expression's result can have.
    pub interval: Interval,
    /// Maximum number of distinct values this expression can produce, if known.
    pub distinct_count: Option<usize>,
}

impl ExprBoundaries {
    /// Create a new `ExprBoundaries` from a column level statistics.
    pub fn from_column(stats: &ColumnStatistics, col: String, index: usize) -> Self {
        Self {
            column: Column::new(&col, index),
            interval: Interval::new(
                IntervalBound::new(
                    stats.min_value.clone().unwrap_or(ScalarValue::Null),
                    false,
                ),
                IntervalBound::new(
                    stats.max_value.clone().unwrap_or(ScalarValue::Null),
                    false,
                ),
            ),
            distinct_count: stats.distinct_count,
        }
    }

    pub fn update_interval(&mut self, interval: Interval) {
        self.interval = interval;
    }
}

/// Returns a copy of this expr if we change any child according to the pointer comparison.
/// The size of `children` must be equal to the size of `PhysicalExpr::children()`.
pub fn with_new_children_if_necessary(
    expr: Arc<dyn PhysicalExpr>,
    children: Vec<Arc<dyn PhysicalExpr>>,
) -> Result<Arc<dyn PhysicalExpr>> {
    let old_children = expr.children();
    if children.len() != old_children.len() {
        Err(DataFusionError::Internal(
            "PhysicalExpr: Wrong number of children".to_string(),
        ))
    } else if children.is_empty()
        || children
            .iter()
            .zip(old_children.iter())
            .any(|(c1, c2)| !Arc::data_ptr_eq(c1, c2))
    {
        expr.with_new_children(children)
    } else {
        Ok(expr)
    }
}

pub fn down_cast_any_ref(any: &dyn Any) -> &dyn Any {
    if any.is::<Arc<dyn PhysicalExpr>>() {
        any.downcast_ref::<Arc<dyn PhysicalExpr>>()
            .unwrap()
            .as_any()
    } else if any.is::<Box<dyn PhysicalExpr>>() {
        any.downcast_ref::<Box<dyn PhysicalExpr>>()
            .unwrap()
            .as_any()
    } else {
        any
    }
}

/// Scatter `truthy` array by boolean mask. When the mask evaluates `true`, next values of `truthy`
/// are taken, when the mask evaluates `false` values null values are filled.
///
/// # Arguments
/// * `mask` - Boolean values used to determine where to put the `truthy` values
/// * `truthy` - All values of this array are to scatter according to `mask` into final result.
fn scatter(mask: &BooleanArray, truthy: &dyn Array) -> Result<ArrayRef> {
    let truthy = truthy.to_data();

    // update the mask so that any null values become false
    // (SlicesIterator doesn't respect nulls)
    let mask = and_kleene(mask, &is_not_null(mask)?)?;

    let mut mutable = MutableArrayData::new(vec![&truthy], true, mask.len());

    // the SlicesIterator slices only the true values. So the gaps left by this iterator we need to
    // fill with falsy values

    // keep track of how much is filled
    let mut filled = 0;
    // keep track of current position we have in truthy array
    let mut true_pos = 0;

    SlicesIterator::new(&mask).for_each(|(start, end)| {
        // the gap needs to be filled with nulls
        if start > filled {
            mutable.extend_nulls(start - filled);
        }
        // fill with truthy values
        let len = end - start;
        mutable.extend(0, true_pos, true_pos + len);
        true_pos += len;
        filled = end;
    });
    // the remaining part is falsy
    if filled < mask.len() {
        mutable.extend_nulls(mask.len() - filled);
    }

    let data = mutable.freeze();
    Ok(make_array(data))
}

#[macro_export]
// If the given expression is None, return the given context
// without setting the boundaries.
macro_rules! analysis_expect {
    ($context: ident, $expr: expr) => {
        match $expr {
            Some(expr) => expr,
            None => return Ok($context.with_boundaries(None)),
        }
    };
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use arrow::array::Int32Array;
    use datafusion_common::{
        cast::{as_boolean_array, as_int32_array},
        Result,
    };

    #[test]
    fn scatter_int() -> Result<()> {
        let truthy = Arc::new(Int32Array::from(vec![1, 10, 11, 100]));
        let mask = BooleanArray::from(vec![true, true, false, false, true]);

        // the output array is expected to be the same length as the mask array
        let expected =
            Int32Array::from_iter(vec![Some(1), Some(10), None, None, Some(11)]);
        let result = scatter(&mask, truthy.as_ref())?;
        let result = as_int32_array(&result)?;

        assert_eq!(&expected, result);
        Ok(())
    }

    #[test]
    fn scatter_int_end_with_false() -> Result<()> {
        let truthy = Arc::new(Int32Array::from(vec![1, 10, 11, 100]));
        let mask = BooleanArray::from(vec![true, false, true, false, false, false]);

        // output should be same length as mask
        let expected =
            Int32Array::from_iter(vec![Some(1), None, Some(10), None, None, None]);
        let result = scatter(&mask, truthy.as_ref())?;
        let result = as_int32_array(&result)?;

        assert_eq!(&expected, result);
        Ok(())
    }

    #[test]
    fn scatter_with_null_mask() -> Result<()> {
        let truthy = Arc::new(Int32Array::from(vec![1, 10, 11]));
        let mask: BooleanArray = vec![Some(false), None, Some(true), Some(true), None]
            .into_iter()
            .collect();

        // output should treat nulls as though they are false
        let expected = Int32Array::from_iter(vec![None, None, Some(1), Some(10), None]);
        let result = scatter(&mask, truthy.as_ref())?;
        let result = as_int32_array(&result)?;

        assert_eq!(&expected, result);
        Ok(())
    }

    #[test]
    fn scatter_boolean() -> Result<()> {
        let truthy = Arc::new(BooleanArray::from(vec![false, false, false, true]));
        let mask = BooleanArray::from(vec![true, true, false, false, true]);

        // the output array is expected to be the same length as the mask array
        let expected = BooleanArray::from_iter(vec![
            Some(false),
            Some(false),
            None,
            None,
            Some(false),
        ]);
        let result = scatter(&mask, truthy.as_ref())?;
        let result = as_boolean_array(&result)?;

        assert_eq!(&expected, result);
        Ok(())
    }
}
