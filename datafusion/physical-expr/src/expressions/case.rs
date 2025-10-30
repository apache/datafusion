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

use super::{Column, Literal};
use crate::expressions::case::ResultState::{Complete, Empty, Partial};
use crate::expressions::try_cast;
use crate::PhysicalExpr;
use arrow::array::*;
use arrow::compute::kernels::zip::zip;
use arrow::compute::{
    is_not_null, not, nullif, prep_null_mask_filter, FilterBuilder, FilterPredicate,
};
use arrow::datatypes::{DataType, Schema, UInt32Type};
use arrow::error::ArrowError;
use datafusion_common::cast::as_boolean_array;
use datafusion_common::{
    exec_err, internal_datafusion_err, internal_err, DataFusionError, Result, ScalarValue,
};
use datafusion_expr::ColumnarValue;
use datafusion_physical_expr_common::datum::compare_with_eq;
use itertools::Itertools;
use std::borrow::Cow;
use std::fmt::{Debug, Formatter};
use std::hash::Hash;
use std::{any::Any, sync::Arc};

type WhenThen = (Arc<dyn PhysicalExpr>, Arc<dyn PhysicalExpr>);

#[derive(Debug, Hash, PartialEq, Eq)]
enum EvalMethod {
    /// CASE WHEN condition THEN result
    ///      [WHEN ...]
    ///      [ELSE result]
    /// END
    NoExpression,
    /// CASE expression
    ///     WHEN value THEN result
    ///     [WHEN ...]
    ///     [ELSE result]
    /// END
    WithExpression,
    /// This is a specialization for a specific use case where we can take a fast path
    /// for expressions that are infallible and can be cheaply computed for the entire
    /// record batch rather than just for the rows where the predicate is true.
    ///
    /// CASE WHEN condition THEN column [ELSE NULL] END
    InfallibleExprOrNull,
    /// This is a specialization for a specific use case where we can take a fast path
    /// if there is just one when/then pair and both the `then` and `else` expressions
    /// are literal values
    /// CASE WHEN condition THEN literal ELSE literal END
    ScalarOrScalar,
    /// This is a specialization for a specific use case where we can take a fast path
    /// if there is just one when/then pair and both the `then` and `else` are expressions
    ///
    /// CASE WHEN condition THEN expression ELSE expression END
    ExpressionOrExpression,
}

/// The CASE expression is similar to a series of nested if/else and there are two forms that
/// can be used. The first form consists of a series of boolean "when" expressions with
/// corresponding "then" expressions, and an optional "else" expression.
///
/// CASE WHEN condition THEN result
///      [WHEN ...]
///      [ELSE result]
/// END
///
/// The second form uses a base expression and then a series of "when" clauses that match on a
/// literal value.
///
/// CASE expression
///     WHEN value THEN result
///     [WHEN ...]
///     [ELSE result]
/// END
#[derive(Debug, Hash, PartialEq, Eq)]
pub struct CaseExpr {
    /// Optional base expression that can be compared to literal values in the "when" expressions
    expr: Option<Arc<dyn PhysicalExpr>>,
    /// One or more when/then expressions
    when_then_expr: Vec<WhenThen>,
    /// Optional "else" expression
    else_expr: Option<Arc<dyn PhysicalExpr>>,
    /// Evaluation method to use
    eval_method: EvalMethod,
}

impl std::fmt::Display for CaseExpr {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "CASE ")?;
        if let Some(e) = &self.expr {
            write!(f, "{e} ")?;
        }
        for (w, t) in &self.when_then_expr {
            write!(f, "WHEN {w} THEN {t} ")?;
        }
        if let Some(e) = &self.else_expr {
            write!(f, "ELSE {e} ")?;
        }
        write!(f, "END")
    }
}

/// This is a specialization for a specific use case where we can take a fast path
/// for expressions that are infallible and can be cheaply computed for the entire
/// record batch rather than just for the rows where the predicate is true. For now,
/// this is limited to use with Column expressions but could potentially be used for other
/// expressions in the future
fn is_cheap_and_infallible(expr: &Arc<dyn PhysicalExpr>) -> bool {
    expr.as_any().is::<Column>()
}

/// Creates a [FilterPredicate] from a boolean array.
fn create_filter(predicate: &BooleanArray) -> FilterPredicate {
    let mut filter_builder = FilterBuilder::new(predicate);
    // Always optimize the filter since we use them multiple times.
    filter_builder = filter_builder.optimize();
    filter_builder.build()
}

// This should be removed when https://github.com/apache/arrow-rs/pull/8693
// is merged and becomes available.
fn filter_record_batch(
    record_batch: &RecordBatch,
    filter: &FilterPredicate,
) -> std::result::Result<RecordBatch, ArrowError> {
    let filtered_columns = record_batch
        .columns()
        .iter()
        .map(|a| filter_array(a, filter))
        .collect::<std::result::Result<Vec<_>, _>>()?;
    // SAFETY: since we start from a valid RecordBatch, there's no need to revalidate the schema
    // since the set of columns has not changed.
    // The input column arrays all had the same length (since they're coming from a valid RecordBatch)
    // and the filtering them with the same filter will produces a new set of arrays with identical
    // lengths.
    unsafe {
        Ok(RecordBatch::new_unchecked(
            record_batch.schema(),
            filtered_columns,
            filter.count(),
        ))
    }
}

// This function exists purely to be able to use the same call style
// for `filter_record_batch` and `filter_array` at the point of use.
// When https://github.com/apache/arrow-rs/pull/8693 is available, replace
// both with method calls on `FilterPredicate`.
#[inline(always)]
fn filter_array(
    array: &dyn Array,
    filter: &FilterPredicate,
) -> std::result::Result<ArrayRef, ArrowError> {
    filter.filter(array)
}

/// Merges elements by index from a list of [`ArrayData`], creating a new [`ColumnarValue`] from
/// those values.
///
/// Each element in `indices` is the index of an array in `values`. The `indices` array is processed
/// sequentially. The first occurrence of index value `n` will be mapped to the first
/// value of the array at index `n`. The second occurrence to the second value, and so on.
/// An index value where `PartialResultIndex::is_none` is `true` is used to indicate null values.
///
/// # Implementation notes
///
/// This algorithm is similar in nature to both `zip` and `interleave`, but there are some important
/// differences.
///
/// In contrast to `zip`, this function supports multiple input arrays. Instead of a boolean
/// selection vector, an index array is to take values from the input arrays, and a special marker
/// value is used to indicate null values.
///
/// In contrast to `interleave`, this function does not use pairs of indices. The values in
/// `indices` serve the same purpose as the first value in the pairs passed to `interleave`.
/// The index in the array is implicit and is derived from the number of times a particular array
/// index occurs.
/// The more constrained indexing mechanism used by this algorithm makes it easier to copy values
/// in contiguous slices. In the example below, the two subsequent elements from array `2` can be
/// copied in a single operation from the source array instead of copying them one by one.
/// Long spans of null values are also especially cheap because they do not need to be represented
/// in an input array.
///
/// # Safety
///
/// This function does not check that the number of occurrences of any particular array index matches
/// the length of the corresponding input array. If an array contains more values than required, the
/// spurious values will be ignored. If an array contains fewer values than necessary, this function
/// will panic.
///
/// # Example
///
/// ```text
/// ┌───────────┐  ┌─────────┐                             ┌─────────┐
/// │┌─────────┐│  │   None  │                             │   NULL  │
/// ││    A    ││  ├─────────┤                             ├─────────┤
/// │└─────────┘│  │    1    │                             │    B    │
/// │┌─────────┐│  ├─────────┤                             ├─────────┤
/// ││    B    ││  │    0    │    merge(values, indices)   │    A    │
/// │└─────────┘│  ├─────────┤  ─────────────────────────▶ ├─────────┤
/// │┌─────────┐│  │   None  │                             │   NULL  │
/// ││    C    ││  ├─────────┤                             ├─────────┤
/// │├─────────┤│  │    2    │                             │    C    │
/// ││    D    ││  ├─────────┤                             ├─────────┤
/// │└─────────┘│  │    2    │                             │    D    │
/// └───────────┘  └─────────┘                             └─────────┘
///    values        indices                                  result
///
/// ```
fn merge(values: &[ArrayData], indices: &[PartialResultIndex]) -> Result<ArrayRef> {
    #[cfg(debug_assertions)]
    for ix in indices {
        if let Some(index) = ix.index() {
            assert!(
                index < values.len(),
                "Index out of bounds: {} >= {}",
                index,
                values.len()
            );
        }
    }

    let data_refs = values.iter().collect();
    let mut mutable = MutableArrayData::new(data_refs, true, indices.len());

    // This loop extends the mutable array by taking slices from the partial results.
    //
    // take_offsets keeps track of how many values have been taken from each array.
    let mut take_offsets = vec![0; values.len() + 1];
    let mut start_row_ix = 0;
    loop {
        let array_ix = indices[start_row_ix];

        // Determine the length of the slice to take.
        let mut end_row_ix = start_row_ix + 1;
        while end_row_ix < indices.len() && indices[end_row_ix] == array_ix {
            end_row_ix += 1;
        }
        let slice_length = end_row_ix - start_row_ix;

        // Extend mutable with either nulls or with values from the array.
        match array_ix.index() {
            None => mutable.extend_nulls(slice_length),
            Some(index) => {
                let start_offset = take_offsets[index];
                let end_offset = start_offset + slice_length;
                mutable.extend(index, start_offset, end_offset);
                take_offsets[index] = end_offset;
            }
        }

        if end_row_ix == indices.len() {
            break;
        } else {
            // Set the start_row_ix for the next slice.
            start_row_ix = end_row_ix;
        }
    }

    Ok(make_array(mutable.freeze()))
}

/// An index into the partial results array that's more compact than `usize`.
///
/// `u32::MAX` is reserved as a special 'none' value. This is used instead of
/// `Option` to keep the array of indices as compact as possible.
#[derive(Copy, Clone, PartialEq, Eq)]
struct PartialResultIndex {
    index: u32,
}

const NONE_VALUE: u32 = u32::MAX;

impl PartialResultIndex {
    /// Returns the 'none' placeholder value.
    fn none() -> Self {
        Self { index: NONE_VALUE }
    }

    fn zero() -> Self {
        Self { index: 0 }
    }

    /// Creates a new partial result index.
    ///
    /// If the provided value is greater than or equal to `u32::MAX`
    /// an error will be returned.
    fn try_new(index: usize) -> Result<Self> {
        let Ok(index) = u32::try_from(index) else {
            return internal_err!("Partial result index exceeds limit");
        };

        if index == NONE_VALUE {
            return internal_err!("Partial result index exceeds limit");
        }

        Ok(Self { index })
    }

    /// Determines if this index is the 'none' placeholder value or not.
    fn is_none(&self) -> bool {
        self.index == NONE_VALUE
    }

    /// Returns `Some(index)` if this value is not the 'none' placeholder, `None` otherwise.
    fn index(&self) -> Option<usize> {
        if self.is_none() {
            None
        } else {
            Some(self.index as usize)
        }
    }
}

impl Debug for PartialResultIndex {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        if self.is_none() {
            write!(f, "null")
        } else {
            write!(f, "{}", self.index)
        }
    }
}

enum ResultState {
    /// The final result is an array containing only null values.
    Empty,
    /// The final result needs to be computed by merging the data in `arrays`.
    Partial {
        // A `Vec` of partial results that should be merged.
        // `partial_result_indices` contains indexes into this vec.
        arrays: Vec<ArrayData>,
        // Indicates per result row from which array in `partial_results` a value should be taken.
        indices: Vec<PartialResultIndex>,
    },
    /// A single branch matched all input rows. When creating the final result, no further merging
    /// of partial results is necessary.
    Complete(ColumnarValue),
}

/// A builder for constructing result arrays for CASE expressions.
///
/// Rather than building a monolithic array containing all results, it maintains a set of
/// partial result arrays and a mapping that indicates for each row which partial array
/// contains the result value for that row.
///
/// On finish(), the builder will merge all partial results into a single array if necessary.
/// If all rows evaluated to the same array, that array can be returned directly without
/// any merging overhead.
struct ResultBuilder {
    data_type: DataType,
    /// The number of rows in the final result.
    row_count: usize,
    state: ResultState,
}

impl ResultBuilder {
    /// Creates a new ResultBuilder that will produce arrays of the given data type.
    ///
    /// The `row_count` parameter indicates the number of rows in the final result.
    fn new(data_type: &DataType, row_count: usize) -> Self {
        Self {
            data_type: data_type.clone(),
            row_count,
            state: Empty,
        }
    }

    /// Adds a result for one branch of the case expression.
    ///
    /// `row_indices` should be a [UInt32Array] containing [RecordBatch] relative row indices
    /// for which `value` contains result values.
    ///
    /// If `value` is a scalar, the scalar value will be used as the value for each row in `row_indices`.
    ///
    /// If `value` is an array, the values from the array and the indices from `row_indices` will be
    /// processed pairwise. The lengths of `value` and `row_indices` must match.
    ///
    /// The diagram below shows a situation where a when expression matched rows 1 and 4 of the
    /// record batch. The then expression produced the value array `[A, D]`.
    /// After adding this result, the result array will have been added to `partial arrays` and
    /// `partial indices` will have been updated at indexes `1` and `4`.
    ///
    /// ```text
    ///  ┌─────────┐     ┌─────────┐┌───────────┐                            ┌─────────┐┌───────────┐
    ///  │    C    │     │ 0: None ││┌ 0 ──────┐│                            │ 0: None ││┌ 0 ──────┐│
    ///  ├─────────┤     ├─────────┤││    A    ││                            ├─────────┤││    A    ││
    ///  │    D    │     │ 1: None ││└─────────┘│                            │ 1:  2   ││└─────────┘│
    ///  └─────────┘     ├─────────┤│┌ 1 ──────┐│   add_branch_result(       ├─────────┤│┌ 1 ──────┐│
    ///   matching       │ 2:  0   │││    B    ││     row indices,           │ 2:  0   │││    B    ││
    /// 'then' values    ├─────────┤│└─────────┘│     value                  ├─────────┤│└─────────┘│
    ///                  │ 3: None ││           │   )                        │ 3: None ││┌ 2 ──────┐│
    ///  ┌─────────┐     ├─────────┤│           │ ─────────────────────────▶ ├─────────┤││    C    ││
    ///  │    1    │     │ 4: None ││           │                            │ 4:  2   ││├─────────┤│
    ///  ├─────────┤     ├─────────┤│           │                            ├─────────┤││    D    ││
    ///  │    4    │     │ 5:  1   ││           │                            │ 5:  1   ││└─────────┘│
    ///  └─────────┘     └─────────┘└───────────┘                            └─────────┘└───────────┘
    /// row indices        partial     partial                                 partial     partial
    ///                    indices     arrays                                  indices     arrays
    /// ```
    fn add_branch_result(
        &mut self,
        row_indices: &ArrayRef,
        value: ColumnarValue,
    ) -> Result<()> {
        match value {
            ColumnarValue::Array(a) => {
                if a.len() != row_indices.len() {
                    internal_err!("Array length must match row indices length")
                } else if row_indices.len() == self.row_count {
                    self.set_complete_result(ColumnarValue::Array(a))
                } else {
                    self.add_partial_result(row_indices, a.to_data())
                }
            }
            ColumnarValue::Scalar(s) => {
                if row_indices.len() == self.row_count {
                    self.set_complete_result(ColumnarValue::Scalar(s))
                } else {
                    self.add_partial_result(
                        row_indices,
                        s.to_array_of_size(row_indices.len())?.to_data(),
                    )
                }
            }
        }
    }

    /// Adds a partial result array.
    ///
    /// This method adds the given array data as a partial result and updates the index mapping
    /// to indicate that the specified rows should take their values from this array.
    /// The partial results will be merged into a single array when finish() is called.
    fn add_partial_result(
        &mut self,
        row_indices: &ArrayRef,
        row_values: ArrayData,
    ) -> Result<()> {
        if row_indices.null_count() != 0 {
            return internal_err!("Row indices must not contain nulls");
        }

        match &mut self.state {
            Empty => {
                let array_index = PartialResultIndex::zero();
                let mut indices = vec![PartialResultIndex::none(); self.row_count];
                for row_ix in row_indices.as_primitive::<UInt32Type>().values().iter() {
                    indices[*row_ix as usize] = array_index;
                }

                self.state = Partial {
                    arrays: vec![row_values],
                    indices,
                };

                Ok(())
            }
            Partial { arrays, indices } => {
                let array_index = PartialResultIndex::try_new(arrays.len())?;

                arrays.push(row_values);

                for row_ix in row_indices.as_primitive::<UInt32Type>().values().iter() {
                    // This is check is only active for debug config because the callers of this method,
                    // `case_when_with_expr` and `case_when_no_expr`, already ensure that
                    // they only calculate a value for each row at most once.
                    #[cfg(debug_assertions)]
                    if !indices[*row_ix as usize].is_none() {
                        return internal_err!("Duplicate value for row {}", *row_ix);
                    }

                    indices[*row_ix as usize] = array_index;
                }
                Ok(())
            }
            Complete(_) => internal_err!(
                "Cannot add a partial result when complete result is already set"
            ),
        }
    }

    /// Sets a result that applies to all rows.
    ///
    /// This is an optimization for cases where all rows evaluate to the same result.
    /// When a complete result is set, the builder will return it directly from finish()
    /// without any merging overhead.
    fn set_complete_result(&mut self, value: ColumnarValue) -> Result<()> {
        match &self.state {
            Empty => {
                self.state = Complete(value);
                Ok(())
            }
            Partial { .. } => {
                internal_err!(
                    "Cannot set a complete result when there are already partial results"
                )
            }
            Complete(_) => internal_err!("Complete result already set"),
        }
    }

    /// Finishes building the result and returns the final array.
    fn finish(self) -> Result<ColumnarValue> {
        match self.state {
            Empty => {
                // No complete result and no partial results.
                // This can happen for case expressions with no else branch where no rows
                // matched.
                Ok(ColumnarValue::Scalar(ScalarValue::try_new_null(
                    &self.data_type,
                )?))
            }
            Partial { arrays, indices } => {
                // Merge partial results into a single array.
                Ok(ColumnarValue::Array(merge(&arrays, &indices)?))
            }
            Complete(v) => {
                // If we have a complete result, we can just return it.
                Ok(v)
            }
        }
    }
}

impl CaseExpr {
    /// Create a new CASE WHEN expression
    pub fn try_new(
        expr: Option<Arc<dyn PhysicalExpr>>,
        when_then_expr: Vec<WhenThen>,
        else_expr: Option<Arc<dyn PhysicalExpr>>,
    ) -> Result<Self> {
        // normalize null literals to None in the else_expr (this already happens
        // during SQL planning, but not necessarily for other use cases)
        let else_expr = match &else_expr {
            Some(e) => match e.as_any().downcast_ref::<Literal>() {
                Some(lit) if lit.value().is_null() => None,
                _ => else_expr,
            },
            _ => else_expr,
        };

        if when_then_expr.is_empty() {
            exec_err!("There must be at least one WHEN clause")
        } else {
            let eval_method = if expr.is_some() {
                EvalMethod::WithExpression
            } else if when_then_expr.len() == 1
                && is_cheap_and_infallible(&(when_then_expr[0].1))
                && else_expr.is_none()
            {
                EvalMethod::InfallibleExprOrNull
            } else if when_then_expr.len() == 1
                && when_then_expr[0].1.as_any().is::<Literal>()
                && else_expr.is_some()
                && else_expr.as_ref().unwrap().as_any().is::<Literal>()
            {
                EvalMethod::ScalarOrScalar
            } else if when_then_expr.len() == 1 && else_expr.is_some() {
                EvalMethod::ExpressionOrExpression
            } else {
                EvalMethod::NoExpression
            };

            Ok(Self {
                expr,
                when_then_expr,
                else_expr,
                eval_method,
            })
        }
    }

    /// Optional base expression that can be compared to literal values in the "when" expressions
    pub fn expr(&self) -> Option<&Arc<dyn PhysicalExpr>> {
        self.expr.as_ref()
    }

    /// One or more when/then expressions
    pub fn when_then_expr(&self) -> &[WhenThen] {
        &self.when_then_expr
    }

    /// Optional "else" expression
    pub fn else_expr(&self) -> Option<&Arc<dyn PhysicalExpr>> {
        self.else_expr.as_ref()
    }
}

impl CaseExpr {
    /// This function evaluates the form of CASE that matches an expression to fixed values.
    ///
    /// CASE expression
    ///     WHEN value THEN result
    ///     [WHEN ...]
    ///     [ELSE result]
    /// END
    fn case_when_with_expr(&self, batch: &RecordBatch) -> Result<ColumnarValue> {
        let return_type = self.data_type(&batch.schema())?;
        let mut result_builder = ResultBuilder::new(&return_type, batch.num_rows());

        // `remainder_rows` contains the indices of the rows that need to be evaluated
        let mut remainder_rows: ArrayRef =
            Arc::new(UInt32Array::from_iter_values(0..batch.num_rows() as u32));
        // `remainder_batch` contains the rows themselves that need to be evaluated
        let mut remainder_batch = Cow::Borrowed(batch);

        // evaluate the base expression
        let mut base_values = self
            .expr
            .as_ref()
            .unwrap()
            .evaluate(batch)?
            .into_array(batch.num_rows())?;

        // Fill in a result value already for rows where the base expression value is null
        // Since each when expression is tested against the base expression using the equality
        // operator, null base values can never match any when expression. `x = NULL` is falsy,
        // for all possible values of `x`.
        if base_values.null_count() > 0 {
            // Use `is_not_null` since this is a cheap clone of the null buffer from 'base_value'.
            // We already checked there are nulls, so we can be sure a new buffer will not be
            // created.
            let base_not_nulls = is_not_null(base_values.as_ref())?;
            let base_all_null = base_values.null_count() == remainder_batch.num_rows();

            // If there is an else expression, use that as the default value for the null rows
            // Otherwise the default `null` value from the result builder will be used.
            if let Some(e) = self.else_expr() {
                let expr = try_cast(Arc::clone(e), &batch.schema(), return_type.clone())?;

                if base_all_null {
                    // All base values were null, so no need to filter
                    let nulls_value = expr.evaluate(&remainder_batch)?;
                    result_builder.add_branch_result(&remainder_rows, nulls_value)?;
                } else {
                    // Filter out the null rows and evaluate the else expression for those
                    let nulls_filter = create_filter(&not(&base_not_nulls)?);
                    let nulls_batch =
                        filter_record_batch(&remainder_batch, &nulls_filter)?;
                    let nulls_rows = filter_array(&remainder_rows, &nulls_filter)?;
                    let nulls_value = expr.evaluate(&nulls_batch)?;
                    result_builder.add_branch_result(&nulls_rows, nulls_value)?;
                }
            }

            // All base values are null, so we can return early
            if base_all_null {
                return result_builder.finish();
            }

            // Remove the null rows from the remainder batch
            let not_null_filter = create_filter(&base_not_nulls);
            remainder_batch =
                Cow::Owned(filter_record_batch(&remainder_batch, &not_null_filter)?);
            remainder_rows = filter_array(&remainder_rows, &not_null_filter)?;
            base_values = filter_array(&base_values, &not_null_filter)?;
        }

        // The types of case and when expressions will be coerced to match.
        // We only need to check if the base_value is nested.
        let base_value_is_nested = base_values.data_type().is_nested();

        for i in 0..self.when_then_expr.len() {
            // Evaluate the 'when' predicate for the remainder batch
            // This results in a boolean array with the same length as the remaining number of rows
            let when_expr = &self.when_then_expr[i].0;
            let when_value = match when_expr.evaluate(&remainder_batch)? {
                ColumnarValue::Array(a) => {
                    compare_with_eq(&a, &base_values, base_value_is_nested)
                }
                ColumnarValue::Scalar(s) => {
                    let scalar = Scalar::new(s.to_array()?);
                    compare_with_eq(&scalar, &base_values, base_value_is_nested)
                }
            }?;

            // `true_count` ignores `true` values where the validity bit is not set, so there's
            // no need to call `prep_null_mask_filter`.
            let when_true_count = when_value.true_count();

            // If the 'when' predicate did not match any rows, continue to the next branch immediately
            if when_true_count == 0 {
                continue;
            }

            // If the 'when' predicate matched all remaining rows, there is no need to filter
            if when_true_count == remainder_batch.num_rows() {
                let then_expression = &self.when_then_expr[i].1;
                let then_value = then_expression.evaluate(&remainder_batch)?;
                result_builder.add_branch_result(&remainder_rows, then_value)?;
                return result_builder.finish();
            }

            // Filter the remainder batch based on the 'when' value
            // This results in a batch containing only the rows that need to be evaluated
            // for the current branch
            // Still no need to call `prep_null_mask_filter` since `create_filter` will already do
            // this unconditionally.
            let then_filter = create_filter(&when_value);
            let then_batch = filter_record_batch(&remainder_batch, &then_filter)?;
            let then_rows = filter_array(&remainder_rows, &then_filter)?;

            let then_expression = &self.when_then_expr[i].1;
            let then_value = then_expression.evaluate(&then_batch)?;
            result_builder.add_branch_result(&then_rows, then_value)?;

            // If this is the last 'when' branch and there is no 'else' expression, there's no
            // point in calculating the remaining rows.
            if self.else_expr.is_none() && i == self.when_then_expr.len() - 1 {
                return result_builder.finish();
            }

            // Prepare the next when branch (or the else branch)
            let next_selection = match when_value.null_count() {
                0 => not(&when_value),
                _ => {
                    // `prep_null_mask_filter` is required to ensure the not operation treats nulls
                    // as false
                    not(&prep_null_mask_filter(&when_value))
                }
            }?;
            let next_filter = create_filter(&next_selection);
            remainder_batch =
                Cow::Owned(filter_record_batch(&remainder_batch, &next_filter)?);
            remainder_rows = filter_array(&remainder_rows, &next_filter)?;
            base_values = filter_array(&base_values, &next_filter)?;
        }

        // If we reached this point, some rows were left unmatched.
        // Check if those need to be evaluated using the 'else' expression.
        if let Some(e) = self.else_expr() {
            // keep `else_expr`'s data type and return type consistent
            let expr = try_cast(Arc::clone(e), &batch.schema(), return_type.clone())?;
            let else_value = expr.evaluate(&remainder_batch)?;
            result_builder.add_branch_result(&remainder_rows, else_value)?;
        }

        result_builder.finish()
    }

    /// This function evaluates the form of CASE where each WHEN expression is a boolean
    /// expression.
    ///
    /// CASE WHEN condition THEN result
    ///      [WHEN ...]
    ///      [ELSE result]
    /// END
    fn case_when_no_expr(&self, batch: &RecordBatch) -> Result<ColumnarValue> {
        let return_type = self.data_type(&batch.schema())?;
        let mut result_builder = ResultBuilder::new(&return_type, batch.num_rows());

        // `remainder_rows` contains the indices of the rows that need to be evaluated
        let mut remainder_rows: ArrayRef =
            Arc::new(UInt32Array::from_iter(0..batch.num_rows() as u32));
        // `remainder_batch` contains the rows themselves that need to be evaluated
        let mut remainder_batch = Cow::Borrowed(batch);

        for i in 0..self.when_then_expr.len() {
            // Evaluate the 'when' predicate for the remainder batch
            // This results in a boolean array with the same length as the remaining number of rows
            let when_predicate = &self.when_then_expr[i].0;
            let when_value = when_predicate
                .evaluate(&remainder_batch)?
                .into_array(remainder_batch.num_rows())?;
            let when_value = as_boolean_array(&when_value).map_err(|_| {
                internal_datafusion_err!("WHEN expression did not return a BooleanArray")
            })?;

            // `true_count` ignores `true` values where the validity bit is not set, so there's
            // no need to call `prep_null_mask_filter`.
            let when_true_count = when_value.true_count();

            // If the 'when' predicate did not match any rows, continue to the next branch immediately
            if when_true_count == 0 {
                continue;
            }

            // If the 'when' predicate matched all remaining rows, there is no need to filter
            if when_true_count == remainder_batch.num_rows() {
                let then_expression = &self.when_then_expr[i].1;
                let then_value = then_expression.evaluate(&remainder_batch)?;
                result_builder.add_branch_result(&remainder_rows, then_value)?;
                return result_builder.finish();
            }

            // Filter the remainder batch based on the 'when' value
            // This results in a batch containing only the rows that need to be evaluated
            // for the current branch
            // Still no need to call `prep_null_mask_filter` since `create_filter` will already do
            // this unconditionally.
            let then_filter = create_filter(when_value);
            let then_batch = filter_record_batch(&remainder_batch, &then_filter)?;
            let then_rows = filter_array(&remainder_rows, &then_filter)?;

            let then_expression = &self.when_then_expr[i].1;
            let then_value = then_expression.evaluate(&then_batch)?;
            result_builder.add_branch_result(&then_rows, then_value)?;

            // If this is the last 'when' branch and there is no 'else' expression, there's no
            // point in calculating the remaining rows.
            if self.else_expr.is_none() && i == self.when_then_expr.len() - 1 {
                return result_builder.finish();
            }

            // Prepare the next when branch (or the else branch)
            let next_selection = match when_value.null_count() {
                0 => not(when_value),
                _ => {
                    // `prep_null_mask_filter` is required to ensure the not operation treats nulls
                    // as false
                    not(&prep_null_mask_filter(when_value))
                }
            }?;
            let next_filter = create_filter(&next_selection);
            remainder_batch =
                Cow::Owned(filter_record_batch(&remainder_batch, &next_filter)?);
            remainder_rows = filter_array(&remainder_rows, &next_filter)?;
        }

        // If we reached this point, some rows were left unmatched.
        // Check if those need to be evaluated using the 'else' expression.
        if let Some(e) = self.else_expr() {
            // keep `else_expr`'s data type and return type consistent
            let expr = try_cast(Arc::clone(e), &batch.schema(), return_type.clone())?;
            let else_value = expr.evaluate(&remainder_batch)?;
            result_builder.add_branch_result(&remainder_rows, else_value)?;
        }

        result_builder.finish()
    }

    /// This function evaluates the specialized case of:
    ///
    /// CASE WHEN condition THEN column
    ///      [ELSE NULL]
    /// END
    ///
    /// Note that this function is only safe to use for "then" expressions
    /// that are infallible because the expression will be evaluated for all
    /// rows in the input batch.
    fn case_column_or_null(&self, batch: &RecordBatch) -> Result<ColumnarValue> {
        let when_expr = &self.when_then_expr[0].0;
        let then_expr = &self.when_then_expr[0].1;

        match when_expr.evaluate(batch)? {
            // WHEN true --> column
            ColumnarValue::Scalar(ScalarValue::Boolean(Some(true))) => {
                then_expr.evaluate(batch)
            }
            // WHEN [false | null] --> NULL
            ColumnarValue::Scalar(_) => {
                // return scalar NULL value
                ScalarValue::try_from(self.data_type(&batch.schema())?)
                    .map(ColumnarValue::Scalar)
            }
            // WHEN column --> column
            ColumnarValue::Array(bit_mask) => {
                let bit_mask = bit_mask
                    .as_any()
                    .downcast_ref::<BooleanArray>()
                    .expect("predicate should evaluate to a boolean array");
                // invert the bitmask
                let bit_mask = match bit_mask.null_count() {
                    0 => not(bit_mask)?,
                    _ => not(&prep_null_mask_filter(bit_mask))?,
                };
                match then_expr.evaluate(batch)? {
                    ColumnarValue::Array(array) => {
                        Ok(ColumnarValue::Array(nullif(&array, &bit_mask)?))
                    }
                    ColumnarValue::Scalar(_) => {
                        internal_err!("expression did not evaluate to an array")
                    }
                }
            }
        }
    }

    fn scalar_or_scalar(&self, batch: &RecordBatch) -> Result<ColumnarValue> {
        let return_type = self.data_type(&batch.schema())?;

        // evaluate when expression
        let when_value = self.when_then_expr[0].0.evaluate(batch)?;
        let when_value = when_value.into_array(batch.num_rows())?;
        let when_value = as_boolean_array(&when_value).map_err(|_| {
            internal_datafusion_err!("WHEN expression did not return a BooleanArray")
        })?;

        // Treat 'NULL' as false value
        let when_value = match when_value.null_count() {
            0 => Cow::Borrowed(when_value),
            _ => Cow::Owned(prep_null_mask_filter(when_value)),
        };

        // evaluate then_value
        let then_value = self.when_then_expr[0].1.evaluate(batch)?;
        let then_value = Scalar::new(then_value.into_array(1)?);

        let Some(e) = self.else_expr() else {
            return internal_err!("expression did not evaluate to an array");
        };
        // keep `else_expr`'s data type and return type consistent
        let expr = try_cast(Arc::clone(e), &batch.schema(), return_type)?;
        let else_ = Scalar::new(expr.evaluate(batch)?.into_array(1)?);
        Ok(ColumnarValue::Array(zip(&when_value, &then_value, &else_)?))
    }

    fn expr_or_expr(&self, batch: &RecordBatch) -> Result<ColumnarValue> {
        let return_type = self.data_type(&batch.schema())?;

        // evaluate when condition on batch
        let when_value = self.when_then_expr[0].0.evaluate(batch)?;
        let when_value = when_value.into_array(batch.num_rows())?;
        let when_value = as_boolean_array(&when_value).map_err(|e| {
            DataFusionError::Context(
                "WHEN expression did not return a BooleanArray".to_string(),
                Box::new(e),
            )
        })?;

        // For the true and false/null selection vectors, bypass `evaluate_selection` and merging
        // results. This avoids materializing the array for the other branch which we will discard
        // entirely anyway.
        let true_count = when_value.true_count();
        if true_count == batch.num_rows() {
            return self.when_then_expr[0].1.evaluate(batch);
        } else if true_count == 0 {
            return self.else_expr.as_ref().unwrap().evaluate(batch);
        }

        // Treat 'NULL' as false value
        let when_value = match when_value.null_count() {
            0 => Cow::Borrowed(when_value),
            _ => Cow::Owned(prep_null_mask_filter(when_value)),
        };

        let then_value = self.when_then_expr[0]
            .1
            .evaluate_selection(batch, &when_value)?
            .into_array(batch.num_rows())?;

        // evaluate else expression on the values not covered by when_value
        let remainder = not(&when_value)?;
        let e = self.else_expr.as_ref().unwrap();
        // keep `else_expr`'s data type and return type consistent
        let expr = try_cast(Arc::clone(e), &batch.schema(), return_type.clone())
            .unwrap_or_else(|_| Arc::clone(e));
        let else_ = expr
            .evaluate_selection(batch, &remainder)?
            .into_array(batch.num_rows())?;

        Ok(ColumnarValue::Array(zip(&remainder, &else_, &then_value)?))
    }
}

impl PhysicalExpr for CaseExpr {
    /// Return a reference to Any that can be used for down-casting
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn data_type(&self, input_schema: &Schema) -> Result<DataType> {
        // since all then results have the same data type, we can choose any one as the
        // return data type except for the null.
        let mut data_type = DataType::Null;
        for i in 0..self.when_then_expr.len() {
            data_type = self.when_then_expr[i].1.data_type(input_schema)?;
            if !data_type.equals_datatype(&DataType::Null) {
                break;
            }
        }
        // if all then results are null, we use data type of else expr instead if possible.
        if data_type.equals_datatype(&DataType::Null) {
            if let Some(e) = &self.else_expr {
                data_type = e.data_type(input_schema)?;
            }
        }

        Ok(data_type)
    }

    fn nullable(&self, input_schema: &Schema) -> Result<bool> {
        // this expression is nullable if any of the input expressions are nullable
        let then_nullable = self
            .when_then_expr
            .iter()
            .map(|(_, t)| t.nullable(input_schema))
            .collect::<Result<Vec<_>>>()?;
        if then_nullable.contains(&true) {
            Ok(true)
        } else if let Some(e) = &self.else_expr {
            e.nullable(input_schema)
        } else {
            // CASE produces NULL if there is no `else` expr
            // (aka when none of the `when_then_exprs` match)
            Ok(true)
        }
    }

    fn evaluate(&self, batch: &RecordBatch) -> Result<ColumnarValue> {
        match self.eval_method {
            EvalMethod::WithExpression => {
                // this use case evaluates "expr" and then compares the values with the "when"
                // values
                self.case_when_with_expr(batch)
            }
            EvalMethod::NoExpression => {
                // The "when" conditions all evaluate to boolean in this use case and can be
                // arbitrary expressions
                self.case_when_no_expr(batch)
            }
            EvalMethod::InfallibleExprOrNull => {
                // Specialization for CASE WHEN expr THEN column [ELSE NULL] END
                self.case_column_or_null(batch)
            }
            EvalMethod::ScalarOrScalar => self.scalar_or_scalar(batch),
            EvalMethod::ExpressionOrExpression => self.expr_or_expr(batch),
        }
    }

    fn children(&self) -> Vec<&Arc<dyn PhysicalExpr>> {
        let mut children = vec![];
        if let Some(expr) = &self.expr {
            children.push(expr)
        }
        self.when_then_expr.iter().for_each(|(cond, value)| {
            children.push(cond);
            children.push(value);
        });

        if let Some(else_expr) = &self.else_expr {
            children.push(else_expr)
        }
        children
    }

    // For physical CaseExpr, we do not allow modifying children size
    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn PhysicalExpr>>,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        if children.len() != self.children().len() {
            internal_err!("CaseExpr: Wrong number of children")
        } else {
            let (expr, when_then_expr, else_expr) =
                match (self.expr().is_some(), self.else_expr().is_some()) {
                    (true, true) => (
                        Some(&children[0]),
                        &children[1..children.len() - 1],
                        Some(&children[children.len() - 1]),
                    ),
                    (true, false) => {
                        (Some(&children[0]), &children[1..children.len()], None)
                    }
                    (false, true) => (
                        None,
                        &children[0..children.len() - 1],
                        Some(&children[children.len() - 1]),
                    ),
                    (false, false) => (None, &children[0..children.len()], None),
                };
            Ok(Arc::new(CaseExpr::try_new(
                expr.cloned(),
                when_then_expr.iter().cloned().tuples().collect(),
                else_expr.cloned(),
            )?))
        }
    }

    fn fmt_sql(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "CASE ")?;
        if let Some(e) = &self.expr {
            e.fmt_sql(f)?;
            write!(f, " ")?;
        }

        for (w, t) in &self.when_then_expr {
            write!(f, "WHEN ")?;
            w.fmt_sql(f)?;
            write!(f, " THEN ")?;
            t.fmt_sql(f)?;
            write!(f, " ")?;
        }

        if let Some(e) = &self.else_expr {
            write!(f, "ELSE ")?;
            e.fmt_sql(f)?;
            write!(f, " ")?;
        }
        write!(f, "END")
    }
}

/// Create a CASE expression
pub fn case(
    expr: Option<Arc<dyn PhysicalExpr>>,
    when_thens: Vec<WhenThen>,
    else_expr: Option<Arc<dyn PhysicalExpr>>,
) -> Result<Arc<dyn PhysicalExpr>> {
    Ok(Arc::new(CaseExpr::try_new(expr, when_thens, else_expr)?))
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::expressions::{binary, cast, col, lit, BinaryExpr};
    use arrow::buffer::Buffer;
    use arrow::datatypes::DataType::Float64;
    use arrow::datatypes::Field;
    use datafusion_common::cast::{as_float64_array, as_int32_array};
    use datafusion_common::plan_err;
    use datafusion_common::tree_node::{Transformed, TransformedResult, TreeNode};
    use datafusion_expr::type_coercion::binary::comparison_coercion;
    use datafusion_expr::Operator;
    use datafusion_physical_expr_common::physical_expr::fmt_sql;

    #[test]
    fn case_with_expr() -> Result<()> {
        let batch = case_test_batch()?;
        let schema = batch.schema();

        // CASE a WHEN 'foo' THEN 123 WHEN 'bar' THEN 456 END
        let when1 = lit("foo");
        let then1 = lit(123i32);
        let when2 = lit("bar");
        let then2 = lit(456i32);

        let expr = generate_case_when_with_type_coercion(
            Some(col("a", &schema)?),
            vec![(when1, then1), (when2, then2)],
            None,
            schema.as_ref(),
        )?;
        let result = expr
            .evaluate(&batch)?
            .into_array(batch.num_rows())
            .expect("Failed to convert to array");
        let result = as_int32_array(&result)?;

        let expected = &Int32Array::from(vec![Some(123), None, None, Some(456)]);

        assert_eq!(expected, result);

        Ok(())
    }

    #[test]
    fn case_with_expr_else() -> Result<()> {
        let batch = case_test_batch()?;
        let schema = batch.schema();

        // CASE a WHEN 'foo' THEN 123 WHEN 'bar' THEN 456 ELSE 999 END
        let when1 = lit("foo");
        let then1 = lit(123i32);
        let when2 = lit("bar");
        let then2 = lit(456i32);
        let else_value = lit(999i32);

        let expr = generate_case_when_with_type_coercion(
            Some(col("a", &schema)?),
            vec![(when1, then1), (when2, then2)],
            Some(else_value),
            schema.as_ref(),
        )?;
        let result = expr
            .evaluate(&batch)?
            .into_array(batch.num_rows())
            .expect("Failed to convert to array");
        let result = as_int32_array(&result)?;

        let expected =
            &Int32Array::from(vec![Some(123), Some(999), Some(999), Some(456)]);

        assert_eq!(expected, result);

        Ok(())
    }

    #[test]
    fn case_with_expr_divide_by_zero() -> Result<()> {
        let batch = case_test_batch1()?;
        let schema = batch.schema();

        // CASE a when 0 THEN float64(null) ELSE 25.0 / cast(a, float64)  END
        let when1 = lit(0i32);
        let then1 = lit(ScalarValue::Float64(None));
        let else_value = binary(
            lit(25.0f64),
            Operator::Divide,
            cast(col("a", &schema)?, &batch.schema(), Float64)?,
            &batch.schema(),
        )?;

        let expr = generate_case_when_with_type_coercion(
            Some(col("a", &schema)?),
            vec![(when1, then1)],
            Some(else_value),
            schema.as_ref(),
        )?;
        let result = expr
            .evaluate(&batch)?
            .into_array(batch.num_rows())
            .expect("Failed to convert to array");
        let result =
            as_float64_array(&result).expect("failed to downcast to Float64Array");

        let expected = &Float64Array::from(vec![Some(25.0), None, None, Some(5.0)]);

        assert_eq!(expected, result);

        Ok(())
    }

    #[test]
    fn case_without_expr() -> Result<()> {
        let batch = case_test_batch()?;
        let schema = batch.schema();

        // CASE WHEN a = 'foo' THEN 123 WHEN a = 'bar' THEN 456 END
        let when1 = binary(
            col("a", &schema)?,
            Operator::Eq,
            lit("foo"),
            &batch.schema(),
        )?;
        let then1 = lit(123i32);
        let when2 = binary(
            col("a", &schema)?,
            Operator::Eq,
            lit("bar"),
            &batch.schema(),
        )?;
        let then2 = lit(456i32);

        let expr = generate_case_when_with_type_coercion(
            None,
            vec![(when1, then1), (when2, then2)],
            None,
            schema.as_ref(),
        )?;
        let result = expr
            .evaluate(&batch)?
            .into_array(batch.num_rows())
            .expect("Failed to convert to array");
        let result = as_int32_array(&result)?;

        let expected = &Int32Array::from(vec![Some(123), None, None, Some(456)]);

        assert_eq!(expected, result);

        Ok(())
    }

    #[test]
    fn case_with_expr_when_null() -> Result<()> {
        let batch = case_test_batch()?;
        let schema = batch.schema();

        // CASE a WHEN NULL THEN 0 WHEN a THEN 123 ELSE 999 END
        let when1 = lit(ScalarValue::Utf8(None));
        let then1 = lit(0i32);
        let when2 = col("a", &schema)?;
        let then2 = lit(123i32);
        let else_value = lit(999i32);

        let expr = generate_case_when_with_type_coercion(
            Some(col("a", &schema)?),
            vec![(when1, then1), (when2, then2)],
            Some(else_value),
            schema.as_ref(),
        )?;
        let result = expr
            .evaluate(&batch)?
            .into_array(batch.num_rows())
            .expect("Failed to convert to array");
        let result = as_int32_array(&result)?;

        let expected =
            &Int32Array::from(vec![Some(123), Some(123), Some(999), Some(123)]);

        assert_eq!(expected, result);

        Ok(())
    }

    #[test]
    fn case_without_expr_divide_by_zero() -> Result<()> {
        let batch = case_test_batch1()?;
        let schema = batch.schema();

        // CASE WHEN a > 0 THEN 25.0 / cast(a, float64) ELSE float64(null) END
        let when1 = binary(col("a", &schema)?, Operator::Gt, lit(0i32), &batch.schema())?;
        let then1 = binary(
            lit(25.0f64),
            Operator::Divide,
            cast(col("a", &schema)?, &batch.schema(), Float64)?,
            &batch.schema(),
        )?;
        let x = lit(ScalarValue::Float64(None));

        let expr = generate_case_when_with_type_coercion(
            None,
            vec![(when1, then1)],
            Some(x),
            schema.as_ref(),
        )?;
        let result = expr
            .evaluate(&batch)?
            .into_array(batch.num_rows())
            .expect("Failed to convert to array");
        let result =
            as_float64_array(&result).expect("failed to downcast to Float64Array");

        let expected = &Float64Array::from(vec![Some(25.0), None, None, Some(5.0)]);

        assert_eq!(expected, result);

        Ok(())
    }

    fn case_test_batch1() -> Result<RecordBatch> {
        let schema = Schema::new(vec![
            Field::new("a", DataType::Int32, true),
            Field::new("b", DataType::Int32, true),
            Field::new("c", DataType::Int32, true),
        ]);
        let a = Int32Array::from(vec![Some(1), Some(0), None, Some(5)]);
        let b = Int32Array::from(vec![Some(3), None, Some(14), Some(7)]);
        let c = Int32Array::from(vec![Some(0), Some(-3), Some(777), None]);
        let batch = RecordBatch::try_new(
            Arc::new(schema),
            vec![Arc::new(a), Arc::new(b), Arc::new(c)],
        )?;
        Ok(batch)
    }

    #[test]
    fn case_without_expr_else() -> Result<()> {
        let batch = case_test_batch()?;
        let schema = batch.schema();

        // CASE WHEN a = 'foo' THEN 123 WHEN a = 'bar' THEN 456 ELSE 999 END
        let when1 = binary(
            col("a", &schema)?,
            Operator::Eq,
            lit("foo"),
            &batch.schema(),
        )?;
        let then1 = lit(123i32);
        let when2 = binary(
            col("a", &schema)?,
            Operator::Eq,
            lit("bar"),
            &batch.schema(),
        )?;
        let then2 = lit(456i32);
        let else_value = lit(999i32);

        let expr = generate_case_when_with_type_coercion(
            None,
            vec![(when1, then1), (when2, then2)],
            Some(else_value),
            schema.as_ref(),
        )?;
        let result = expr
            .evaluate(&batch)?
            .into_array(batch.num_rows())
            .expect("Failed to convert to array");
        let result = as_int32_array(&result)?;

        let expected =
            &Int32Array::from(vec![Some(123), Some(999), Some(999), Some(456)]);

        assert_eq!(expected, result);

        Ok(())
    }

    #[test]
    fn case_with_type_cast() -> Result<()> {
        let batch = case_test_batch()?;
        let schema = batch.schema();

        // CASE WHEN a = 'foo' THEN 123.3 ELSE 999 END
        let when = binary(
            col("a", &schema)?,
            Operator::Eq,
            lit("foo"),
            &batch.schema(),
        )?;
        let then = lit(123.3f64);
        let else_value = lit(999i32);

        let expr = generate_case_when_with_type_coercion(
            None,
            vec![(when, then)],
            Some(else_value),
            schema.as_ref(),
        )?;
        let result = expr
            .evaluate(&batch)?
            .into_array(batch.num_rows())
            .expect("Failed to convert to array");
        let result =
            as_float64_array(&result).expect("failed to downcast to Float64Array");

        let expected =
            &Float64Array::from(vec![Some(123.3), Some(999.0), Some(999.0), Some(999.0)]);

        assert_eq!(expected, result);

        Ok(())
    }

    #[test]
    fn case_with_matches_and_nulls() -> Result<()> {
        let batch = case_test_batch_nulls()?;
        let schema = batch.schema();

        // SELECT CASE WHEN load4 = 1.77 THEN load4 END
        let when = binary(
            col("load4", &schema)?,
            Operator::Eq,
            lit(1.77f64),
            &batch.schema(),
        )?;
        let then = col("load4", &schema)?;

        let expr = generate_case_when_with_type_coercion(
            None,
            vec![(when, then)],
            None,
            schema.as_ref(),
        )?;
        let result = expr
            .evaluate(&batch)?
            .into_array(batch.num_rows())
            .expect("Failed to convert to array");
        let result =
            as_float64_array(&result).expect("failed to downcast to Float64Array");

        let expected =
            &Float64Array::from(vec![Some(1.77), None, None, None, None, Some(1.77)]);

        assert_eq!(expected, result);

        Ok(())
    }

    #[test]
    fn case_with_scalar_predicate() -> Result<()> {
        let batch = case_test_batch_nulls()?;
        let schema = batch.schema();

        // SELECT CASE WHEN TRUE THEN load4 END
        let when = lit(true);
        let then = col("load4", &schema)?;
        let expr = generate_case_when_with_type_coercion(
            None,
            vec![(when, then)],
            None,
            schema.as_ref(),
        )?;

        // many rows
        let result = expr
            .evaluate(&batch)?
            .into_array(batch.num_rows())
            .expect("Failed to convert to array");
        let result =
            as_float64_array(&result).expect("failed to downcast to Float64Array");
        let expected = &Float64Array::from(vec![
            Some(1.77),
            None,
            None,
            Some(1.78),
            None,
            Some(1.77),
        ]);
        assert_eq!(expected, result);

        // one row
        let expected = Float64Array::from(vec![Some(1.1)]);
        let batch =
            RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(expected.clone())])?;
        let result = expr
            .evaluate(&batch)?
            .into_array(batch.num_rows())
            .expect("Failed to convert to array");
        let result =
            as_float64_array(&result).expect("failed to downcast to Float64Array");
        assert_eq!(&expected, result);

        Ok(())
    }

    #[test]
    fn case_expr_matches_and_nulls() -> Result<()> {
        let batch = case_test_batch_nulls()?;
        let schema = batch.schema();

        // SELECT CASE load4 WHEN 1.77 THEN load4 END
        let expr = col("load4", &schema)?;
        let when = lit(1.77f64);
        let then = col("load4", &schema)?;

        let expr = generate_case_when_with_type_coercion(
            Some(expr),
            vec![(when, then)],
            None,
            schema.as_ref(),
        )?;
        let result = expr
            .evaluate(&batch)?
            .into_array(batch.num_rows())
            .expect("Failed to convert to array");
        let result =
            as_float64_array(&result).expect("failed to downcast to Float64Array");

        let expected =
            &Float64Array::from(vec![Some(1.77), None, None, None, None, Some(1.77)]);

        assert_eq!(expected, result);

        Ok(())
    }

    #[test]
    fn test_when_null_and_some_cond_else_null() -> Result<()> {
        let batch = case_test_batch()?;
        let schema = batch.schema();

        let when = binary(
            Arc::new(Literal::new(ScalarValue::Boolean(None))),
            Operator::And,
            binary(col("a", &schema)?, Operator::Eq, lit("foo"), &schema)?,
            &schema,
        )?;
        let then = col("a", &schema)?;

        // SELECT CASE WHEN (NULL AND a = 'foo') THEN a ELSE NULL END
        let expr = Arc::new(CaseExpr::try_new(None, vec![(when, then)], None)?);
        let result = expr
            .evaluate(&batch)?
            .into_array(batch.num_rows())
            .expect("Failed to convert to array");
        let result = as_string_array(&result);

        // all result values should be null
        assert_eq!(result.logical_null_count(), batch.num_rows());
        Ok(())
    }

    fn case_test_batch() -> Result<RecordBatch> {
        let schema = Schema::new(vec![Field::new("a", DataType::Utf8, true)]);
        let a = StringArray::from(vec![Some("foo"), Some("baz"), None, Some("bar")]);
        let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(a)])?;
        Ok(batch)
    }

    // Construct an array that has several NULL values whose
    // underlying buffer actually matches the where expr predicate
    fn case_test_batch_nulls() -> Result<RecordBatch> {
        let load4: Float64Array = vec![
            Some(1.77), // 1.77
            Some(1.77), // null <-- same value, but will be set to null
            Some(1.77), // null <-- same value, but will be set to null
            Some(1.78), // 1.78
            None,       // null
            Some(1.77), // 1.77
        ]
        .into_iter()
        .collect();

        let null_buffer = Buffer::from([0b00101001u8]);
        let load4 = load4
            .into_data()
            .into_builder()
            .null_bit_buffer(Some(null_buffer))
            .build()
            .unwrap();
        let load4: Float64Array = load4.into();

        let batch =
            RecordBatch::try_from_iter(vec![("load4", Arc::new(load4) as ArrayRef)])?;
        Ok(batch)
    }

    #[test]
    fn case_test_incompatible() -> Result<()> {
        // 1 then is int64
        // 2 then is boolean
        let batch = case_test_batch()?;
        let schema = batch.schema();

        // CASE WHEN a = 'foo' THEN 123 WHEN a = 'bar' THEN true END
        let when1 = binary(
            col("a", &schema)?,
            Operator::Eq,
            lit("foo"),
            &batch.schema(),
        )?;
        let then1 = lit(123i32);
        let when2 = binary(
            col("a", &schema)?,
            Operator::Eq,
            lit("bar"),
            &batch.schema(),
        )?;
        let then2 = lit(true);

        let expr = generate_case_when_with_type_coercion(
            None,
            vec![(when1, then1), (when2, then2)],
            None,
            schema.as_ref(),
        );
        assert!(expr.is_err());

        // then 1 is int32
        // then 2 is int64
        // else is float
        // CASE WHEN a = 'foo' THEN 123 WHEN a = 'bar' THEN 456 ELSE 1.23 END
        let when1 = binary(
            col("a", &schema)?,
            Operator::Eq,
            lit("foo"),
            &batch.schema(),
        )?;
        let then1 = lit(123i32);
        let when2 = binary(
            col("a", &schema)?,
            Operator::Eq,
            lit("bar"),
            &batch.schema(),
        )?;
        let then2 = lit(456i64);
        let else_expr = lit(1.23f64);

        let expr = generate_case_when_with_type_coercion(
            None,
            vec![(when1, then1), (when2, then2)],
            Some(else_expr),
            schema.as_ref(),
        );
        assert!(expr.is_ok());
        let result_type = expr.unwrap().data_type(schema.as_ref())?;
        assert_eq!(Float64, result_type);
        Ok(())
    }

    #[test]
    fn case_eq() -> Result<()> {
        let schema = Schema::new(vec![Field::new("a", DataType::Int32, true)]);

        let when1 = lit("foo");
        let then1 = lit(123i32);
        let when2 = lit("bar");
        let then2 = lit(456i32);
        let else_value = lit(999i32);

        let expr1 = generate_case_when_with_type_coercion(
            Some(col("a", &schema)?),
            vec![
                (Arc::clone(&when1), Arc::clone(&then1)),
                (Arc::clone(&when2), Arc::clone(&then2)),
            ],
            Some(Arc::clone(&else_value)),
            &schema,
        )?;

        let expr2 = generate_case_when_with_type_coercion(
            Some(col("a", &schema)?),
            vec![
                (Arc::clone(&when1), Arc::clone(&then1)),
                (Arc::clone(&when2), Arc::clone(&then2)),
            ],
            Some(Arc::clone(&else_value)),
            &schema,
        )?;

        let expr3 = generate_case_when_with_type_coercion(
            Some(col("a", &schema)?),
            vec![(Arc::clone(&when1), Arc::clone(&then1)), (when2, then2)],
            None,
            &schema,
        )?;

        let expr4 = generate_case_when_with_type_coercion(
            Some(col("a", &schema)?),
            vec![(when1, then1)],
            Some(else_value),
            &schema,
        )?;

        assert!(expr1.eq(&expr2));
        assert!(expr2.eq(&expr1));

        assert!(expr2.ne(&expr3));
        assert!(expr3.ne(&expr2));

        assert!(expr1.ne(&expr4));
        assert!(expr4.ne(&expr1));

        Ok(())
    }

    #[test]
    fn case_transform() -> Result<()> {
        let schema = Schema::new(vec![Field::new("a", DataType::Int32, true)]);

        let when1 = lit("foo");
        let then1 = lit(123i32);
        let when2 = lit("bar");
        let then2 = lit(456i32);
        let else_value = lit(999i32);

        let expr = generate_case_when_with_type_coercion(
            Some(col("a", &schema)?),
            vec![
                (Arc::clone(&when1), Arc::clone(&then1)),
                (Arc::clone(&when2), Arc::clone(&then2)),
            ],
            Some(Arc::clone(&else_value)),
            &schema,
        )?;

        let expr2 = Arc::clone(&expr)
            .transform(|e| {
                let transformed = match e.as_any().downcast_ref::<Literal>() {
                    Some(lit_value) => match lit_value.value() {
                        ScalarValue::Utf8(Some(str_value)) => {
                            Some(lit(str_value.to_uppercase()))
                        }
                        _ => None,
                    },
                    _ => None,
                };
                Ok(if let Some(transformed) = transformed {
                    Transformed::yes(transformed)
                } else {
                    Transformed::no(e)
                })
            })
            .data()
            .unwrap();

        let expr3 = Arc::clone(&expr)
            .transform_down(|e| {
                let transformed = match e.as_any().downcast_ref::<Literal>() {
                    Some(lit_value) => match lit_value.value() {
                        ScalarValue::Utf8(Some(str_value)) => {
                            Some(lit(str_value.to_uppercase()))
                        }
                        _ => None,
                    },
                    _ => None,
                };
                Ok(if let Some(transformed) = transformed {
                    Transformed::yes(transformed)
                } else {
                    Transformed::no(e)
                })
            })
            .data()
            .unwrap();

        assert!(expr.ne(&expr2));
        assert!(expr2.eq(&expr3));

        Ok(())
    }

    #[test]
    fn test_column_or_null_specialization() -> Result<()> {
        // create input data
        let mut c1 = Int32Builder::new();
        let mut c2 = StringBuilder::new();
        for i in 0..1000 {
            c1.append_value(i);
            if i % 7 == 0 {
                c2.append_null();
            } else {
                c2.append_value(format!("string {i}"));
            }
        }
        let c1 = Arc::new(c1.finish());
        let c2 = Arc::new(c2.finish());
        let schema = Schema::new(vec![
            Field::new("c1", DataType::Int32, true),
            Field::new("c2", DataType::Utf8, true),
        ]);
        let batch = RecordBatch::try_new(Arc::new(schema), vec![c1, c2]).unwrap();

        // CaseWhenExprOrNull should produce same results as CaseExpr
        let predicate = Arc::new(BinaryExpr::new(
            make_col("c1", 0),
            Operator::LtEq,
            make_lit_i32(250),
        ));
        let expr = CaseExpr::try_new(None, vec![(predicate, make_col("c2", 1))], None)?;
        assert!(matches!(expr.eval_method, EvalMethod::InfallibleExprOrNull));
        match expr.evaluate(&batch)? {
            ColumnarValue::Array(array) => {
                assert_eq!(1000, array.len());
                assert_eq!(785, array.null_count());
            }
            _ => unreachable!(),
        }
        Ok(())
    }

    #[test]
    fn test_expr_or_expr_specialization() -> Result<()> {
        let batch = case_test_batch1()?;
        let schema = batch.schema();
        let when = binary(
            col("a", &schema)?,
            Operator::LtEq,
            lit(2i32),
            &batch.schema(),
        )?;
        let then = col("b", &schema)?;
        let else_expr = col("c", &schema)?;
        let expr = CaseExpr::try_new(None, vec![(when, then)], Some(else_expr))?;
        assert!(matches!(
            expr.eval_method,
            EvalMethod::ExpressionOrExpression
        ));
        let result = expr
            .evaluate(&batch)?
            .into_array(batch.num_rows())
            .expect("Failed to convert to array");
        let result = as_int32_array(&result).expect("failed to downcast to Int32Array");

        let expected = &Int32Array::from(vec![Some(3), None, Some(777), None]);

        assert_eq!(expected, result);
        Ok(())
    }

    fn make_col(name: &str, index: usize) -> Arc<dyn PhysicalExpr> {
        Arc::new(Column::new(name, index))
    }

    fn make_lit_i32(n: i32) -> Arc<dyn PhysicalExpr> {
        Arc::new(Literal::new(ScalarValue::Int32(Some(n))))
    }

    fn generate_case_when_with_type_coercion(
        expr: Option<Arc<dyn PhysicalExpr>>,
        when_thens: Vec<WhenThen>,
        else_expr: Option<Arc<dyn PhysicalExpr>>,
        input_schema: &Schema,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        let coerce_type =
            get_case_common_type(&when_thens, else_expr.clone(), input_schema);
        let (when_thens, else_expr) = match coerce_type {
            None => plan_err!(
                "Can't get a common type for then {when_thens:?} and else {else_expr:?} expression"
            ),
            Some(data_type) => {
                // cast then expr
                let left = when_thens
                    .into_iter()
                    .map(|(when, then)| {
                        let then = try_cast(then, input_schema, data_type.clone())?;
                        Ok((when, then))
                    })
                    .collect::<Result<Vec<_>>>()?;
                let right = match else_expr {
                    None => None,
                    Some(expr) => Some(try_cast(expr, input_schema, data_type.clone())?),
                };

                Ok((left, right))
            }
        }?;
        case(expr, when_thens, else_expr)
    }

    fn get_case_common_type(
        when_thens: &[WhenThen],
        else_expr: Option<Arc<dyn PhysicalExpr>>,
        input_schema: &Schema,
    ) -> Option<DataType> {
        let thens_type = when_thens
            .iter()
            .map(|when_then| {
                let data_type = &when_then.1.data_type(input_schema).unwrap();
                data_type.clone()
            })
            .collect::<Vec<_>>();
        let else_type = match else_expr {
            None => {
                // case when then exprs must have one then value
                thens_type[0].clone()
            }
            Some(else_phy_expr) => else_phy_expr.data_type(input_schema).unwrap(),
        };
        thens_type
            .iter()
            .try_fold(else_type, |left_type, right_type| {
                // TODO: now just use the `equal` coercion rule for case when. If find the issue, and
                // refactor again.
                comparison_coercion(&left_type, right_type)
            })
    }

    #[test]
    fn test_fmt_sql() -> Result<()> {
        let schema = Schema::new(vec![Field::new("a", DataType::Utf8, true)]);

        // CASE WHEN a = 'foo' THEN 123.3 ELSE 999 END
        let when = binary(col("a", &schema)?, Operator::Eq, lit("foo"), &schema)?;
        let then = lit(123.3f64);
        let else_value = lit(999i32);

        let expr = generate_case_when_with_type_coercion(
            None,
            vec![(when, then)],
            Some(else_value),
            &schema,
        )?;

        let display_string = expr.to_string();
        assert_eq!(
            display_string,
            "CASE WHEN a@0 = foo THEN 123.3 ELSE TRY_CAST(999 AS Float64) END"
        );

        let sql_string = fmt_sql(expr.as_ref()).to_string();
        assert_eq!(
            sql_string,
            "CASE WHEN a = foo THEN 123.3 ELSE TRY_CAST(999 AS Float64) END"
        );

        Ok(())
    }

    #[test]
    fn test_merge() {
        let a1 = StringArray::from(vec![Some("A")]).to_data();
        let a2 = StringArray::from(vec![Some("B")]).to_data();
        let a3 = StringArray::from(vec![Some("C"), Some("D")]).to_data();

        let indices = vec![
            PartialResultIndex::none(),
            PartialResultIndex::try_new(1).unwrap(),
            PartialResultIndex::try_new(0).unwrap(),
            PartialResultIndex::none(),
            PartialResultIndex::try_new(2).unwrap(),
            PartialResultIndex::try_new(2).unwrap(),
        ];

        let merged = merge(&vec![a1, a2, a3], &indices).unwrap();
        let merged = merged.as_string::<i32>();

        assert_eq!(merged.len(), indices.len());
        assert!(!merged.is_valid(0));
        assert!(merged.is_valid(1));
        assert_eq!(merged.value(1), "B");
        assert!(merged.is_valid(2));
        assert_eq!(merged.value(2), "A");
        assert!(!merged.is_valid(3));
        assert!(merged.is_valid(4));
        assert_eq!(merged.value(4), "C");
        assert!(merged.is_valid(5));
        assert_eq!(merged.value(5), "D");
    }
}
