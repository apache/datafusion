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

mod literal_lookup_table;

use super::{Column, Literal};
use crate::PhysicalExpr;
use crate::expressions::{lit, try_cast};
use arrow::array::*;
use arrow::compute::kernels::zip::zip;
use arrow::compute::{
    FilterBuilder, FilterPredicate, is_not_null, not, nullif, prep_null_mask_filter,
};
use arrow::datatypes::{DataType, Schema, UInt32Type, UnionMode};
use arrow::error::ArrowError;
use datafusion_common::cast::as_boolean_array;
use datafusion_common::{
    DataFusionError, HashMap, HashSet, Result, ScalarValue, assert_or_internal_err,
    exec_err, internal_datafusion_err, internal_err,
};
use datafusion_expr::ColumnarValue;
use std::borrow::Cow;
use std::hash::Hash;
use std::{any::Any, sync::Arc};

use crate::expressions::case::literal_lookup_table::LiteralLookupTable;
use arrow::compute::kernels::merge::{MergeIndex, merge, merge_n};
use datafusion_common::tree_node::{Transformed, TreeNode, TreeNodeRecursion};
use datafusion_physical_expr_common::datum::compare_with_eq;
use itertools::Itertools;
use std::fmt::{Debug, Formatter};

pub(super) type WhenThen = (Arc<dyn PhysicalExpr>, Arc<dyn PhysicalExpr>);

#[derive(Debug, Hash, PartialEq, Eq)]
enum EvalMethod {
    /// CASE WHEN condition THEN result
    ///      [WHEN ...]
    ///      [ELSE result]
    /// END
    NoExpression(ProjectedCaseBody),
    /// CASE expression
    ///     WHEN value THEN result
    ///     [WHEN ...]
    ///     [ELSE result]
    /// END
    WithExpression(ProjectedCaseBody),
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
    ExpressionOrExpression(ProjectedCaseBody),

    /// This is a specialization for [`EvalMethod::WithExpression`] when the value and results are literals
    ///
    /// See [`LiteralLookupTable`] for more details
    WithExprScalarLookupTable(LiteralLookupTable),
}

/// Implementing hash so we can use `derive` on [`EvalMethod`].
///
/// not implementing actual [`Hash`] as it is not dyn compatible so we cannot implement it for
/// `dyn` [`literal_lookup_table::WhenLiteralIndexMap`].
///
/// So implementing empty hash is still valid as the data is derived from `PhysicalExpr` s which are already hashed
impl Hash for LiteralLookupTable {
    fn hash<H: std::hash::Hasher>(&self, _state: &mut H) {}
}

/// Implementing Equal so we can use `derive` on [`EvalMethod`].
///
/// not implementing actual [`PartialEq`] as it is not dyn compatible so we cannot implement it for
/// `dyn` [`literal_lookup_table::WhenLiteralIndexMap`].
///
/// So we always return true as the data is derived from `PhysicalExpr` s which are already compared
impl PartialEq for LiteralLookupTable {
    fn eq(&self, _other: &Self) -> bool {
        true
    }
}

impl Eq for LiteralLookupTable {}

/// The body of a CASE expression which consists of an optional base expression, the "when/then"
/// branches and an optional "else" branch.
#[derive(Debug, Hash, PartialEq, Eq)]
struct CaseBody {
    /// Optional base expression that can be compared to literal values in the "when" expressions
    expr: Option<Arc<dyn PhysicalExpr>>,
    /// One or more when/then expressions
    when_then_expr: Vec<WhenThen>,
    /// Optional "else" expression
    else_expr: Option<Arc<dyn PhysicalExpr>>,
}

impl CaseBody {
    /// Derives a [ProjectedCaseBody] from this [CaseBody].
    fn project(&self) -> Result<ProjectedCaseBody> {
        // Determine the set of columns that are used in all the expressions of the case body.
        let mut used_column_indices = HashSet::<usize>::new();
        let mut collect_column_indices = |expr: &Arc<dyn PhysicalExpr>| {
            expr.apply(|expr| {
                if let Some(column) = expr.as_any().downcast_ref::<Column>() {
                    used_column_indices.insert(column.index());
                }
                Ok(TreeNodeRecursion::Continue)
            })
            .expect("Closure cannot fail");
        };

        if let Some(e) = &self.expr {
            collect_column_indices(e);
        }
        self.when_then_expr.iter().for_each(|(w, t)| {
            collect_column_indices(w);
            collect_column_indices(t);
        });
        if let Some(e) = &self.else_expr {
            collect_column_indices(e);
        }

        // Construct a mapping from the original column index to the projected column index.
        let column_index_map = used_column_indices
            .iter()
            .enumerate()
            .map(|(projected, original)| (*original, projected))
            .collect::<HashMap<usize, usize>>();

        // Construct the projected body by rewriting each expression from the original body
        // using the column index mapping.
        let project = |expr: &Arc<dyn PhysicalExpr>| -> Result<Arc<dyn PhysicalExpr>> {
            Arc::clone(expr)
                .transform_down(|e| {
                    if let Some(column) = e.as_any().downcast_ref::<Column>() {
                        let original = column.index();
                        let projected = *column_index_map.get(&original).unwrap();
                        if projected != original {
                            return Ok(Transformed::yes(Arc::new(Column::new(
                                column.name(),
                                projected,
                            ))));
                        }
                    }
                    Ok(Transformed::no(e))
                })
                .map(|t| t.data)
        };

        let projected_body = CaseBody {
            expr: self.expr.as_ref().map(project).transpose()?,
            when_then_expr: self
                .when_then_expr
                .iter()
                .map(|(e, t)| Ok((project(e)?, project(t)?)))
                .collect::<Result<Vec<_>>>()?,
            else_expr: self.else_expr.as_ref().map(project).transpose()?,
        };

        // Construct the projection vector
        let projection = column_index_map
            .iter()
            .sorted_by_key(|(_, v)| **v)
            .map(|(k, _)| *k)
            .collect::<Vec<_>>();

        Ok(ProjectedCaseBody {
            projection,
            body: projected_body,
        })
    }
}

/// A derived case body that can be used to evaluate a case expression after projecting
/// record batches using a projection vector.
///
/// This is used to avoid filtering columns that are not used in the
/// input `RecordBatch` when progressively evaluating a `CASE` expression's
/// remainder batches. Filtering these columns is wasteful since for a record
/// batch of `n` rows, filtering requires at worst a copy of `n - 1` values
/// per array. If these filtered values will never be accessed, the time spent
/// producing them is better avoided.
///
/// For example, if we are evaluating the following case expression that
/// only references columns B and D:
///
/// ```sql
/// SELECT CASE WHEN B > 10 THEN D ELSE NULL END FROM (VALUES (...)) T(A, B, C, D)
/// ```
///
/// Of the 4 input columns `[A, B, C, D]`, the `CASE` expression only access `B` and `D`.
/// Filtering `A` and `C` would be unnecessary and wasteful.
///
/// If we only retain columns `B` and `D` using `RecordBatch::project` and the projection vector
/// `[1, 3]`, the indices of these two columns will change to `[0, 1]`. To evaluate the
/// case expression, it will need to be rewritten from `CASE WHEN B@1 > 10 THEN D@3 ELSE NULL END`
/// to `CASE WHEN B@0 > 10 THEN D@1 ELSE NULL END`.
///
/// The projection vector and the rewritten expression (which only differs from the original in
/// column reference indices) are held in a `ProjectedCaseBody`.
#[derive(Debug, Hash, PartialEq, Eq)]
struct ProjectedCaseBody {
    projection: Vec<usize>,
    body: CaseBody,
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
    /// The case expression body
    body: CaseBody,
    /// Evaluation method to use
    eval_method: EvalMethod,
}

impl std::fmt::Display for CaseExpr {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "CASE ")?;
        if let Some(e) = &self.body.expr {
            write!(f, "{e} ")?;
        }
        for (w, t) in &self.body.when_then_expr {
            write!(f, "WHEN {w} THEN {t} ")?;
        }
        if let Some(e) = &self.body.else_expr {
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
fn create_filter(predicate: &BooleanArray, optimize: bool) -> FilterPredicate {
    let mut filter_builder = FilterBuilder::new(predicate);
    if optimize {
        // Always optimize the filter since we use them multiple times.
        filter_builder = filter_builder.optimize();
    }
    filter_builder.build()
}

fn multiple_arrays(data_type: &DataType) -> bool {
    match data_type {
        DataType::Struct(fields) => {
            fields.len() > 1
                || fields.len() == 1 && multiple_arrays(fields[0].data_type())
        }
        DataType::Union(fields, UnionMode::Sparse) => !fields.is_empty(),
        _ => false,
    }
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

        assert_or_internal_err!(
            index != NONE_VALUE,
            "Partial result index exceeds limit"
        );

        Ok(Self { index })
    }

    /// Determines if this index is the 'none' placeholder value or not.
    fn is_none(&self) -> bool {
        self.index == NONE_VALUE
    }
}

impl MergeIndex for PartialResultIndex {
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
        arrays: Vec<ArrayRef>,
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
            state: ResultState::Empty,
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
                    self.add_partial_result(row_indices, a)
                }
            }
            ColumnarValue::Scalar(s) => {
                if row_indices.len() == self.row_count {
                    self.set_complete_result(ColumnarValue::Scalar(s))
                } else {
                    self.add_partial_result(
                        row_indices,
                        s.to_array_of_size(row_indices.len())?,
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
        row_values: ArrayRef,
    ) -> Result<()> {
        assert_or_internal_err!(
            row_indices.null_count() == 0,
            "Row indices must not contain nulls"
        );

        match &mut self.state {
            ResultState::Empty => {
                let array_index = PartialResultIndex::zero();
                let mut indices = vec![PartialResultIndex::none(); self.row_count];
                for row_ix in row_indices.as_primitive::<UInt32Type>().values().iter() {
                    indices[*row_ix as usize] = array_index;
                }

                self.state = ResultState::Partial {
                    arrays: vec![row_values],
                    indices,
                };

                Ok(())
            }
            ResultState::Partial { arrays, indices } => {
                let array_index = PartialResultIndex::try_new(arrays.len())?;

                arrays.push(row_values);

                for row_ix in row_indices.as_primitive::<UInt32Type>().values().iter() {
                    // This is check is only active for debug config because the callers of this method,
                    // `case_when_with_expr` and `case_when_no_expr`, already ensure that
                    // they only calculate a value for each row at most once.
                    #[cfg(debug_assertions)]
                    assert_or_internal_err!(
                        indices[*row_ix as usize].is_none(),
                        "Duplicate value for row {}",
                        *row_ix
                    );

                    indices[*row_ix as usize] = array_index;
                }
                Ok(())
            }
            ResultState::Complete(_) => internal_err!(
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
            ResultState::Empty => {
                self.state = ResultState::Complete(value);
                Ok(())
            }
            ResultState::Partial { .. } => {
                internal_err!(
                    "Cannot set a complete result when there are already partial results"
                )
            }
            ResultState::Complete(_) => internal_err!("Complete result already set"),
        }
    }

    /// Finishes building the result and returns the final array.
    fn finish(self) -> Result<ColumnarValue> {
        match self.state {
            ResultState::Empty => {
                // No complete result and no partial results.
                // This can happen for case expressions with no else branch where no rows
                // matched.
                Ok(ColumnarValue::Scalar(ScalarValue::try_new_null(
                    &self.data_type,
                )?))
            }
            ResultState::Partial { arrays, indices } => {
                // Merge partial results into a single array.
                let array_refs = arrays.iter().map(|a| a.as_ref()).collect::<Vec<_>>();
                Ok(ColumnarValue::Array(merge_n(&array_refs, &indices)?))
            }
            ResultState::Complete(v) => {
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
            return exec_err!("There must be at least one WHEN clause");
        }

        let body = CaseBody {
            expr,
            when_then_expr,
            else_expr,
        };

        let eval_method = Self::find_best_eval_method(&body)?;

        Ok(Self { body, eval_method })
    }

    fn find_best_eval_method(body: &CaseBody) -> Result<EvalMethod> {
        if body.expr.is_some() {
            if let Some(mapping) = LiteralLookupTable::maybe_new(body) {
                return Ok(EvalMethod::WithExprScalarLookupTable(mapping));
            }

            return Ok(EvalMethod::WithExpression(body.project()?));
        }

        Ok(
            if body.when_then_expr.len() == 1
                && is_cheap_and_infallible(&(body.when_then_expr[0].1))
                && body.else_expr.is_none()
            {
                EvalMethod::InfallibleExprOrNull
            } else if body.when_then_expr.len() == 1
                && body.when_then_expr[0].1.as_any().is::<Literal>()
                && body.else_expr.is_some()
                && body.else_expr.as_ref().unwrap().as_any().is::<Literal>()
            {
                EvalMethod::ScalarOrScalar
            } else if body.when_then_expr.len() == 1 && body.else_expr.is_some() {
                EvalMethod::ExpressionOrExpression(body.project()?)
            } else {
                EvalMethod::NoExpression(body.project()?)
            },
        )
    }

    /// Optional base expression that can be compared to literal values in the "when" expressions
    pub fn expr(&self) -> Option<&Arc<dyn PhysicalExpr>> {
        self.body.expr.as_ref()
    }

    /// One or more when/then expressions
    pub fn when_then_expr(&self) -> &[WhenThen] {
        &self.body.when_then_expr
    }

    /// Optional "else" expression
    pub fn else_expr(&self) -> Option<&Arc<dyn PhysicalExpr>> {
        self.body.else_expr.as_ref()
    }
}

impl CaseBody {
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
        if data_type.equals_datatype(&DataType::Null)
            && let Some(e) = &self.else_expr
        {
            data_type = e.data_type(input_schema)?;
        }

        Ok(data_type)
    }

    /// See [CaseExpr::case_when_with_expr].
    fn case_when_with_expr(
        &self,
        batch: &RecordBatch,
        return_type: &DataType,
    ) -> Result<ColumnarValue> {
        let mut result_builder = ResultBuilder::new(return_type, batch.num_rows());

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
        let base_null_count = base_values.logical_null_count();
        if base_null_count > 0 {
            // Use `is_not_null` since this is a cheap clone of the null buffer from 'base_value'.
            // We already checked there are nulls, so we can be sure a new buffer will not be
            // created.
            let base_not_nulls = is_not_null(base_values.as_ref())?;
            let base_all_null = base_null_count == remainder_batch.num_rows();

            // If there is an else expression, use that as the default value for the null rows
            // Otherwise the default `null` value from the result builder will be used.
            if let Some(e) = &self.else_expr {
                let expr = try_cast(Arc::clone(e), &batch.schema(), return_type.clone())?;

                if base_all_null {
                    // All base values were null, so no need to filter
                    let nulls_value = expr.evaluate(&remainder_batch)?;
                    result_builder.add_branch_result(&remainder_rows, nulls_value)?;
                } else {
                    // Filter out the null rows and evaluate the else expression for those
                    let nulls_filter = create_filter(&not(&base_not_nulls)?, true);
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
            let not_null_filter = create_filter(&base_not_nulls, true);
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
                    compare_with_eq(&s.to_scalar()?, &base_values, base_value_is_nested)
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
            let then_filter = create_filter(&when_value, true);
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
            let next_filter = create_filter(&next_selection, true);
            remainder_batch =
                Cow::Owned(filter_record_batch(&remainder_batch, &next_filter)?);
            remainder_rows = filter_array(&remainder_rows, &next_filter)?;
            base_values = filter_array(&base_values, &next_filter)?;
        }

        // If we reached this point, some rows were left unmatched.
        // Check if those need to be evaluated using the 'else' expression.
        if let Some(e) = &self.else_expr {
            // keep `else_expr`'s data type and return type consistent
            let expr = try_cast(Arc::clone(e), &batch.schema(), return_type.clone())?;
            let else_value = expr.evaluate(&remainder_batch)?;
            result_builder.add_branch_result(&remainder_rows, else_value)?;
        }

        result_builder.finish()
    }

    /// See [CaseExpr::case_when_no_expr].
    fn case_when_no_expr(
        &self,
        batch: &RecordBatch,
        return_type: &DataType,
    ) -> Result<ColumnarValue> {
        let mut result_builder = ResultBuilder::new(return_type, batch.num_rows());

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
            let then_filter = create_filter(when_value, true);
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
            let next_filter = create_filter(&next_selection, true);
            remainder_batch =
                Cow::Owned(filter_record_batch(&remainder_batch, &next_filter)?);
            remainder_rows = filter_array(&remainder_rows, &next_filter)?;
        }

        // If we reached this point, some rows were left unmatched.
        // Check if those need to be evaluated using the 'else' expression.
        if let Some(e) = &self.else_expr {
            // keep `else_expr`'s data type and return type consistent
            let expr = try_cast(Arc::clone(e), &batch.schema(), return_type.clone())?;
            let else_value = expr.evaluate(&remainder_batch)?;
            result_builder.add_branch_result(&remainder_rows, else_value)?;
        }

        result_builder.finish()
    }

    /// See [CaseExpr::expr_or_expr].
    fn expr_or_expr(
        &self,
        batch: &RecordBatch,
        when_value: &BooleanArray,
    ) -> Result<ColumnarValue> {
        let when_value = match when_value.null_count() {
            0 => Cow::Borrowed(when_value),
            _ => {
                // `prep_null_mask_filter` is required to ensure null is treated as false
                Cow::Owned(prep_null_mask_filter(when_value))
            }
        };

        let optimize_filter = batch.num_columns() > 1
            || (batch.num_columns() == 1 && multiple_arrays(batch.column(0).data_type()));

        let when_filter = create_filter(&when_value, optimize_filter);
        let then_batch = filter_record_batch(batch, &when_filter)?;
        let then_value = self.when_then_expr[0].1.evaluate(&then_batch)?;

        let else_selection = not(&when_value)?;
        let else_filter = create_filter(&else_selection, optimize_filter);
        let else_batch = filter_record_batch(batch, &else_filter)?;

        // keep `else_expr`'s data type and return type consistent
        let e = self.else_expr.as_ref().unwrap();
        let return_type = self.data_type(&batch.schema())?;
        let else_expr = try_cast(Arc::clone(e), &batch.schema(), return_type.clone())
            .unwrap_or_else(|_| Arc::clone(e));

        let else_value = else_expr.evaluate(&else_batch)?;

        Ok(ColumnarValue::Array(match (then_value, else_value) {
            (ColumnarValue::Array(t), ColumnarValue::Array(e)) => {
                merge(&when_value, &t, &e)
            }
            (ColumnarValue::Scalar(t), ColumnarValue::Array(e)) => {
                merge(&when_value, &t.to_scalar()?, &e)
            }
            (ColumnarValue::Array(t), ColumnarValue::Scalar(e)) => {
                merge(&when_value, &t, &e.to_scalar()?)
            }
            (ColumnarValue::Scalar(t), ColumnarValue::Scalar(e)) => {
                merge(&when_value, &t.to_scalar()?, &e.to_scalar()?)
            }
        }?))
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
    fn case_when_with_expr(
        &self,
        batch: &RecordBatch,
        projected: &ProjectedCaseBody,
    ) -> Result<ColumnarValue> {
        let return_type = self.data_type(&batch.schema())?;
        if projected.projection.len() < batch.num_columns() {
            let projected_batch = batch.project(&projected.projection)?;
            projected
                .body
                .case_when_with_expr(&projected_batch, &return_type)
        } else {
            self.body.case_when_with_expr(batch, &return_type)
        }
    }

    /// This function evaluates the form of CASE where each WHEN expression is a boolean
    /// expression.
    ///
    /// CASE WHEN condition THEN result
    ///      [WHEN ...]
    ///      [ELSE result]
    /// END
    fn case_when_no_expr(
        &self,
        batch: &RecordBatch,
        projected: &ProjectedCaseBody,
    ) -> Result<ColumnarValue> {
        let return_type = self.data_type(&batch.schema())?;
        if projected.projection.len() < batch.num_columns() {
            let projected_batch = batch.project(&projected.projection)?;
            projected
                .body
                .case_when_no_expr(&projected_batch, &return_type)
        } else {
            self.body.case_when_no_expr(batch, &return_type)
        }
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
        let when_expr = &self.body.when_then_expr[0].0;
        let then_expr = &self.body.when_then_expr[0].1;

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
        let when_value = self.body.when_then_expr[0].0.evaluate(batch)?;
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
        let then_value = self.body.when_then_expr[0].1.evaluate(batch)?;
        let then_value = Scalar::new(then_value.into_array(1)?);

        let Some(e) = &self.body.else_expr else {
            return internal_err!("expression did not evaluate to an array");
        };
        // keep `else_expr`'s data type and return type consistent
        let expr = try_cast(Arc::clone(e), &batch.schema(), return_type)?;
        let else_ = Scalar::new(expr.evaluate(batch)?.into_array(1)?);
        Ok(ColumnarValue::Array(zip(&when_value, &then_value, &else_)?))
    }

    fn expr_or_expr(
        &self,
        batch: &RecordBatch,
        projected: &ProjectedCaseBody,
    ) -> Result<ColumnarValue> {
        // evaluate when condition on batch
        let when_value = self.body.when_then_expr[0].0.evaluate(batch)?;
        // `num_rows == 1` is intentional to avoid expanding scalars.
        // If the `when_value` is effectively a scalar, the 'all true' and 'all false' checks
        // below will avoid incorrectly using the scalar as a merge/zip mask.
        let when_value = when_value.into_array(1)?;
        let when_value = as_boolean_array(&when_value).map_err(|e| {
            DataFusionError::Context(
                "WHEN expression did not return a BooleanArray".to_string(),
                Box::new(e),
            )
        })?;

        let true_count = when_value.true_count();
        if true_count == when_value.len() {
            // All input rows are true, just call the 'then' expression
            self.body.when_then_expr[0].1.evaluate(batch)
        } else if true_count == 0 {
            // All input rows are false/null, just call the 'else' expression
            self.body.else_expr.as_ref().unwrap().evaluate(batch)
        } else if projected.projection.len() < batch.num_columns() {
            // The case expressions do not use all the columns of the input batch.
            // Project first to reduce time spent filtering.
            let projected_batch = batch.project(&projected.projection)?;
            projected.body.expr_or_expr(&projected_batch, when_value)
        } else {
            // All columns are used in the case expressions, so there is no need to project.
            self.body.expr_or_expr(batch, when_value)
        }
    }

    fn with_lookup_table(
        &self,
        batch: &RecordBatch,
        lookup_table: &LiteralLookupTable,
    ) -> Result<ColumnarValue> {
        let expr = self.body.expr.as_ref().unwrap();
        let evaluated_expression = expr.evaluate(batch)?;

        let is_scalar = matches!(evaluated_expression, ColumnarValue::Scalar(_));
        let evaluated_expression = evaluated_expression.to_array(1)?;

        let values = lookup_table.map_keys_to_values(&evaluated_expression)?;

        let result = if is_scalar {
            ColumnarValue::Scalar(ScalarValue::try_from_array(values.as_ref(), 0)?)
        } else {
            ColumnarValue::Array(values)
        };

        Ok(result)
    }
}

impl PhysicalExpr for CaseExpr {
    /// Return a reference to Any that can be used for down-casting
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn data_type(&self, input_schema: &Schema) -> Result<DataType> {
        self.body.data_type(input_schema)
    }

    fn nullable(&self, input_schema: &Schema) -> Result<bool> {
        let nullable_then = self
            .body
            .when_then_expr
            .iter()
            .filter_map(|(w, t)| {
                let is_nullable = match t.nullable(input_schema) {
                    // Pass on error determining nullability verbatim
                    Err(e) => return Some(Err(e)),
                    Ok(n) => n,
                };

                // Branches with a then expression that is not nullable do not impact the
                // nullability of the case expression.
                if !is_nullable {
                    return None;
                }

                // For case-with-expression assume all 'then' expressions are reachable
                if self.body.expr.is_some() {
                    return Some(Ok(()));
                }

                // For branches with a nullable 'then' expression, try to determine
                // if the 'then' expression is ever reachable in the situation where
                // it would evaluate to null.

                // Replace the `then` expression with `NULL` in the `when` expression
                let with_null = match replace_with_null(w, t.as_ref(), input_schema) {
                    Err(e) => return Some(Err(e)),
                    Ok(e) => e,
                };

                // Try to const evaluate the modified `when` expression.
                let predicate_result = match evaluate_predicate(&with_null) {
                    Err(e) => return Some(Err(e)),
                    Ok(b) => b,
                };

                match predicate_result {
                    // Evaluation was inconclusive or true, so the 'then' expression is reachable
                    None | Some(true) => Some(Ok(())),
                    // Evaluation proves the branch will never be taken.
                    // The most common pattern for this is `WHEN x IS NOT NULL THEN x`.
                    Some(false) => None,
                }
            })
            .next();

        if let Some(nullable_then) = nullable_then {
            // There is at least one reachable nullable 'then' expression, so the case
            // expression itself is nullable.
            // Use `Result::map` to propagate the error from `nullable_then` if there is one.
            nullable_then.map(|_| true)
        } else if let Some(e) = &self.body.else_expr {
            // There are no reachable nullable 'then' expressions, so all we still need to
            // check is the 'else' expression's nullability.
            e.nullable(input_schema)
        } else {
            // CASE produces NULL if there is no `else` expr
            // (aka when none of the `when_then_exprs` match)
            Ok(true)
        }
    }

    fn evaluate(&self, batch: &RecordBatch) -> Result<ColumnarValue> {
        match &self.eval_method {
            EvalMethod::WithExpression(p) => {
                // this use case evaluates "expr" and then compares the values with the "when"
                // values
                self.case_when_with_expr(batch, p)
            }
            EvalMethod::NoExpression(p) => {
                // The "when" conditions all evaluate to boolean in this use case and can be
                // arbitrary expressions
                self.case_when_no_expr(batch, p)
            }
            EvalMethod::InfallibleExprOrNull => {
                // Specialization for CASE WHEN expr THEN column [ELSE NULL] END
                self.case_column_or_null(batch)
            }
            EvalMethod::ScalarOrScalar => self.scalar_or_scalar(batch),
            EvalMethod::ExpressionOrExpression(p) => self.expr_or_expr(batch, p),
            EvalMethod::WithExprScalarLookupTable(lookup_table) => {
                self.with_lookup_table(batch, lookup_table)
            }
        }
    }

    fn children(&self) -> Vec<&Arc<dyn PhysicalExpr>> {
        let mut children = vec![];
        if let Some(expr) = &self.body.expr {
            children.push(expr)
        }
        self.body.when_then_expr.iter().for_each(|(cond, value)| {
            children.push(cond);
            children.push(value);
        });

        if let Some(else_expr) = &self.body.else_expr {
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
                match (self.expr().is_some(), self.body.else_expr.is_some()) {
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
        if let Some(e) = &self.body.expr {
            e.fmt_sql(f)?;
            write!(f, " ")?;
        }

        for (w, t) in &self.body.when_then_expr {
            write!(f, "WHEN ")?;
            w.fmt_sql(f)?;
            write!(f, " THEN ")?;
            t.fmt_sql(f)?;
            write!(f, " ")?;
        }

        if let Some(e) = &self.body.else_expr {
            write!(f, "ELSE ")?;
            e.fmt_sql(f)?;
            write!(f, " ")?;
        }
        write!(f, "END")
    }
}

/// Attempts to const evaluate the given `predicate`.
/// Returns:
/// - `Some(true)` if the predicate evaluates to a truthy value.
/// - `Some(false)` if the predicate evaluates to a falsy value.
/// - `None` if the predicate could not be evaluated.
fn evaluate_predicate(predicate: &Arc<dyn PhysicalExpr>) -> Result<Option<bool>> {
    // Create a dummy record with no columns and one row
    let batch = RecordBatch::try_new_with_options(
        Arc::new(Schema::empty()),
        vec![],
        &RecordBatchOptions::new().with_row_count(Some(1)),
    )?;

    // Evaluate the predicate and interpret the result as a boolean
    let result = match predicate.evaluate(&batch) {
        // An error during evaluation means we couldn't const evaluate the predicate, so return `None`
        Err(_) => None,
        Ok(ColumnarValue::Array(array)) => Some(
            ScalarValue::try_from_array(array.as_ref(), 0)?
                .cast_to(&DataType::Boolean)?,
        ),
        Ok(ColumnarValue::Scalar(scalar)) => Some(scalar.cast_to(&DataType::Boolean)?),
    };
    Ok(result.map(|v| matches!(v, ScalarValue::Boolean(Some(true)))))
}

fn replace_with_null(
    expr: &Arc<dyn PhysicalExpr>,
    expr_to_replace: &dyn PhysicalExpr,
    input_schema: &Schema,
) -> Result<Arc<dyn PhysicalExpr>, DataFusionError> {
    let with_null = Arc::clone(expr)
        .transform_down(|e| {
            if e.as_ref().dyn_eq(expr_to_replace) {
                let data_type = e.data_type(input_schema)?;
                let null_literal = lit(ScalarValue::try_new_null(&data_type)?);
                Ok(Transformed::yes(null_literal))
            } else {
                Ok(Transformed::no(e))
            }
        })?
        .data;
    Ok(with_null)
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

    use crate::expressions;
    use crate::expressions::{BinaryExpr, binary, cast, col, is_not_null, lit};
    use arrow::buffer::Buffer;
    use arrow::datatypes::DataType::Float64;
    use arrow::datatypes::Field;
    use datafusion_common::cast::{as_float64_array, as_int32_array};
    use datafusion_common::plan_err;
    use datafusion_common::tree_node::{Transformed, TransformedResult, TreeNode};
    use datafusion_expr::type_coercion::binary::comparison_coercion;
    use datafusion_expr_common::operator::Operator;
    use datafusion_physical_expr_common::physical_expr::fmt_sql;
    use half::f16;

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
    fn case_with_expr_dictionary() -> Result<()> {
        let schema = Schema::new(vec![Field::new(
            "a",
            DataType::Dictionary(Box::new(DataType::UInt8), Box::new(DataType::Utf8)),
            true,
        )]);
        let keys = UInt8Array::from(vec![0u8, 1u8, 2u8, 3u8]);
        let values = StringArray::from(vec![Some("foo"), Some("baz"), None, Some("bar")]);
        let dictionary = DictionaryArray::new(keys, Arc::new(values));
        let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(dictionary)])?;

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

    // Make sure we are not failing when got literal in case when but input is dictionary encoded
    #[test]
    fn case_with_expr_primitive_dictionary() -> Result<()> {
        let schema = Schema::new(vec![Field::new(
            "a",
            DataType::Dictionary(Box::new(DataType::UInt8), Box::new(DataType::UInt64)),
            true,
        )]);
        let keys = UInt8Array::from(vec![0u8, 1u8, 2u8, 3u8]);
        let values = UInt64Array::from(vec![Some(10), Some(20), None, Some(30)]);
        let dictionary = DictionaryArray::new(keys, Arc::new(values));
        let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(dictionary)])?;

        let schema = batch.schema();

        // CASE a WHEN 10 THEN 123 WHEN 30 THEN 456 END
        let when1 = lit(10_u64);
        let then1 = lit(123_i32);
        let when2 = lit(30_u64);
        let then2 = lit(456_i32);

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

    // Make sure we are not failing when got literal in case when but input is dictionary encoded
    #[test]
    fn case_with_expr_boolean_dictionary() -> Result<()> {
        let schema = Schema::new(vec![Field::new(
            "a",
            DataType::Dictionary(Box::new(DataType::UInt8), Box::new(DataType::Boolean)),
            true,
        )]);
        let keys = UInt8Array::from(vec![0u8, 1u8, 2u8, 3u8]);
        let values = BooleanArray::from(vec![Some(true), Some(false), None, Some(true)]);
        let dictionary = DictionaryArray::new(keys, Arc::new(values));
        let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(dictionary)])?;

        let schema = batch.schema();

        // CASE a WHEN true THEN 123 WHEN false THEN 456 END
        let when1 = lit(true);
        let then1 = lit(123i32);
        let when2 = lit(false);
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

        let expected = &Int32Array::from(vec![Some(123), Some(456), None, Some(123)]);

        assert_eq!(expected, result);

        Ok(())
    }

    #[test]
    fn case_with_expr_all_null_dictionary() -> Result<()> {
        let schema = Schema::new(vec![Field::new(
            "a",
            DataType::Dictionary(Box::new(DataType::UInt8), Box::new(DataType::Utf8)),
            true,
        )]);
        let keys = UInt8Array::from(vec![2u8, 2u8, 2u8, 2u8]);
        let values = StringArray::from(vec![Some("foo"), Some("baz"), None, Some("bar")]);
        let dictionary = DictionaryArray::new(keys, Arc::new(values));
        let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(dictionary)])?;

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

        let expected = &Int32Array::from(vec![None, None, None, None]);

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
            EvalMethod::ExpressionOrExpression(_)
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

    fn when_then_else(
        when: &Arc<dyn PhysicalExpr>,
        then: &Arc<dyn PhysicalExpr>,
        els: &Arc<dyn PhysicalExpr>,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        let case = CaseExpr::try_new(
            None,
            vec![(Arc::clone(when), Arc::clone(then))],
            Some(Arc::clone(els)),
        )?;
        Ok(Arc::new(case))
    }

    #[test]
    fn test_case_expression_nullability_with_nullable_column() -> Result<()> {
        case_expression_nullability(true)
    }

    #[test]
    fn test_case_expression_nullability_with_not_nullable_column() -> Result<()> {
        case_expression_nullability(false)
    }

    fn case_expression_nullability(col_is_nullable: bool) -> Result<()> {
        let schema =
            Schema::new(vec![Field::new("foo", DataType::Int32, col_is_nullable)]);

        let foo = col("foo", &schema)?;
        let foo_is_not_null = is_not_null(Arc::clone(&foo))?;
        let foo_is_null = expressions::is_null(Arc::clone(&foo))?;
        let not_foo_is_null = expressions::not(Arc::clone(&foo_is_null))?;
        let zero = lit(0);
        let foo_eq_zero =
            binary(Arc::clone(&foo), Operator::Eq, Arc::clone(&zero), &schema)?;

        assert_not_nullable(when_then_else(&foo_is_not_null, &foo, &zero)?, &schema);
        assert_not_nullable(when_then_else(&not_foo_is_null, &foo, &zero)?, &schema);
        assert_not_nullable(when_then_else(&foo_eq_zero, &foo, &zero)?, &schema);

        assert_not_nullable(
            when_then_else(
                &binary(
                    Arc::clone(&foo_is_not_null),
                    Operator::And,
                    Arc::clone(&foo_eq_zero),
                    &schema,
                )?,
                &foo,
                &zero,
            )?,
            &schema,
        );

        assert_not_nullable(
            when_then_else(
                &binary(
                    Arc::clone(&foo_eq_zero),
                    Operator::And,
                    Arc::clone(&foo_is_not_null),
                    &schema,
                )?,
                &foo,
                &zero,
            )?,
            &schema,
        );

        assert_not_nullable(
            when_then_else(
                &binary(
                    Arc::clone(&foo_is_not_null),
                    Operator::Or,
                    Arc::clone(&foo_eq_zero),
                    &schema,
                )?,
                &foo,
                &zero,
            )?,
            &schema,
        );

        assert_not_nullable(
            when_then_else(
                &binary(
                    Arc::clone(&foo_eq_zero),
                    Operator::Or,
                    Arc::clone(&foo_is_not_null),
                    &schema,
                )?,
                &foo,
                &zero,
            )?,
            &schema,
        );

        assert_nullability(
            when_then_else(
                &binary(
                    Arc::clone(&foo_is_null),
                    Operator::Or,
                    Arc::clone(&foo_eq_zero),
                    &schema,
                )?,
                &foo,
                &zero,
            )?,
            &schema,
            col_is_nullable,
        );

        assert_nullability(
            when_then_else(
                &binary(
                    binary(Arc::clone(&foo), Operator::Eq, Arc::clone(&zero), &schema)?,
                    Operator::Or,
                    Arc::clone(&foo_is_null),
                    &schema,
                )?,
                &foo,
                &zero,
            )?,
            &schema,
            col_is_nullable,
        );

        assert_not_nullable(
            when_then_else(
                &binary(
                    binary(
                        binary(
                            Arc::clone(&foo),
                            Operator::Eq,
                            Arc::clone(&zero),
                            &schema,
                        )?,
                        Operator::And,
                        Arc::clone(&foo_is_not_null),
                        &schema,
                    )?,
                    Operator::Or,
                    binary(
                        binary(
                            Arc::clone(&foo),
                            Operator::Eq,
                            Arc::clone(&foo),
                            &schema,
                        )?,
                        Operator::And,
                        Arc::clone(&foo_is_not_null),
                        &schema,
                    )?,
                    &schema,
                )?,
                &foo,
                &zero,
            )?,
            &schema,
        );

        Ok(())
    }

    fn assert_not_nullable(expr: Arc<dyn PhysicalExpr>, schema: &Schema) {
        assert!(!expr.nullable(schema).unwrap());
    }

    fn assert_nullable(expr: Arc<dyn PhysicalExpr>, schema: &Schema) {
        assert!(expr.nullable(schema).unwrap());
    }

    fn assert_nullability(expr: Arc<dyn PhysicalExpr>, schema: &Schema, nullable: bool) {
        if nullable {
            assert_nullable(expr, schema);
        } else {
            assert_not_nullable(expr, schema);
        }
    }

    // Test Lookup evaluation

    fn test_case_when_literal_lookup(
        values: ArrayRef,
        lookup_map: &[(ScalarValue, ScalarValue)],
        else_value: Option<ScalarValue>,
        expected: ArrayRef,
    ) {
        // Create lookup
        // CASE <expr>
        // WHEN <when_constant_1> THEN <then_constant_1>
        // WHEN <when_constant_2> THEN <then_constant_2>
        // [ ELSE <else_constant> ]

        let schema = Schema::new(vec![Field::new(
            "a",
            values.data_type().clone(),
            values.is_nullable(),
        )]);
        let schema = Arc::new(schema);

        let batch = RecordBatch::try_new(schema, vec![values])
            .expect("failed to create RecordBatch");

        let schema = batch.schema_ref();
        let case = col("a", schema).expect("failed to create col");

        let when_then = lookup_map
            .iter()
            .map(|(when, then)| {
                (
                    Arc::new(Literal::new(when.clone())) as _,
                    Arc::new(Literal::new(then.clone())) as _,
                )
            })
            .collect::<Vec<WhenThen>>();

        let else_expr = else_value.map(|else_value| {
            Arc::new(Literal::new(else_value)) as Arc<dyn PhysicalExpr>
        });
        let expr = CaseExpr::try_new(Some(case), when_then, else_expr)
            .expect("failed to create case");

        // Assert that we are testing what we intend to assert
        assert!(
            matches!(
                expr.eval_method,
                EvalMethod::WithExprScalarLookupTable { .. }
            ),
            "we should use the expected eval method"
        );

        let actual = expr
            .evaluate(&batch)
            .expect("failed to evaluate case")
            .into_array(batch.num_rows())
            .expect("Failed to convert to array");

        assert_eq!(
            actual.data_type(),
            expected.data_type(),
            "Data type mismatch"
        );

        assert_eq!(
            actual.as_ref(),
            expected.as_ref(),
            "actual (left) does not match expected (right)"
        );
    }

    fn create_lookup<When, Then>(
        when_then_pairs: impl IntoIterator<Item = (When, Then)>,
    ) -> Vec<(ScalarValue, ScalarValue)>
    where
        ScalarValue: From<When>,
        ScalarValue: From<Then>,
    {
        when_then_pairs
            .into_iter()
            .map(|(when, then)| (ScalarValue::from(when), ScalarValue::from(then)))
            .collect()
    }

    fn create_input_and_expected<Input, Expected, InputFromItem, ExpectedFromItem>(
        input_and_expected_pairs: impl IntoIterator<Item = (InputFromItem, ExpectedFromItem)>,
    ) -> (Input, Expected)
    where
        Input: Array + From<Vec<InputFromItem>>,
        Expected: Array + From<Vec<ExpectedFromItem>>,
    {
        let (input_items, expected_items): (Vec<InputFromItem>, Vec<ExpectedFromItem>) =
            input_and_expected_pairs.into_iter().unzip();

        (Input::from(input_items), Expected::from(expected_items))
    }

    fn test_lookup_eval_with_and_without_else(
        lookup_map: &[(ScalarValue, ScalarValue)],
        input_values: ArrayRef,
        expected: StringArray,
    ) {
        // Testing without ELSE should fallback to None
        test_case_when_literal_lookup(
            Arc::clone(&input_values),
            lookup_map,
            None,
            Arc::new(expected.clone()),
        );

        // Testing with Else
        let else_value = "___fallback___";

        // Changing each expected None to be fallback
        let expected_with_else = expected
            .iter()
            .map(|item| item.unwrap_or(else_value))
            .map(Some)
            .collect::<StringArray>();

        // Test case
        test_case_when_literal_lookup(
            input_values,
            lookup_map,
            Some(ScalarValue::Utf8(Some(else_value.to_string()))),
            Arc::new(expected_with_else),
        );
    }

    #[test]
    fn test_case_when_literal_lookup_int32_to_string() {
        let lookup_map = create_lookup([
            (Some(4), Some("four")),
            (Some(2), Some("two")),
            (Some(3), Some("three")),
            (Some(1), Some("one")),
        ]);

        let (input_values, expected) =
            create_input_and_expected::<Int32Array, StringArray, _, _>([
                (1, Some("one")),
                (2, Some("two")),
                (3, Some("three")),
                (3, Some("three")),
                (2, Some("two")),
                (3, Some("three")),
                (5, None), // No match in WHEN
                (5, None), // No match in WHEN
                (3, Some("three")),
                (5, None), // No match in WHEN
            ]);

        test_lookup_eval_with_and_without_else(
            &lookup_map,
            Arc::new(input_values),
            expected,
        );
    }

    #[test]
    fn test_case_when_literal_lookup_none_case_should_never_match() {
        let lookup_map = create_lookup([
            (Some(4), Some("four")),
            (None, Some("none")),
            (Some(2), Some("two")),
            (Some(1), Some("one")),
        ]);

        let (input_values, expected) =
            create_input_and_expected::<Int32Array, StringArray, _, _>([
                (Some(1), Some("one")),
                (Some(5), None), // No match in WHEN
                (None, None), // None cases are never match in CASE <expr> WHEN <value> syntax
                (Some(2), Some("two")),
                (None, None), // None cases are never match in CASE <expr> WHEN <value> syntax
                (None, None), // None cases are never match in CASE <expr> WHEN <value> syntax
                (Some(2), Some("two")),
                (Some(5), None), // No match in WHEN
            ]);

        test_lookup_eval_with_and_without_else(
            &lookup_map,
            Arc::new(input_values),
            expected,
        );
    }

    #[test]
    fn test_case_when_literal_lookup_int32_to_string_with_duplicate_cases() {
        let lookup_map = create_lookup([
            (Some(4), Some("four")),
            (Some(4), Some("no 4")),
            (Some(2), Some("two")),
            (Some(2), Some("no 2")),
            (Some(3), Some("three")),
            (Some(3), Some("no 3")),
            (Some(2), Some("no 2")),
            (Some(4), Some("no 4")),
            (Some(2), Some("no 2")),
            (Some(3), Some("no 3")),
            (Some(4), Some("no 4")),
            (Some(2), Some("no 2")),
            (Some(3), Some("no 3")),
            (Some(3), Some("no 3")),
        ]);

        let (input_values, expected) =
            create_input_and_expected::<Int32Array, StringArray, _, _>([
                (1, None), // No match in WHEN
                (2, Some("two")),
                (3, Some("three")),
                (3, Some("three")),
                (2, Some("two")),
                (3, Some("three")),
                (5, None), // No match in WHEN
                (5, None), // No match in WHEN
                (3, Some("three")),
                (5, None), // No match in WHEN
            ]);

        test_lookup_eval_with_and_without_else(
            &lookup_map,
            Arc::new(input_values),
            expected,
        );
    }

    #[test]
    fn test_case_when_literal_lookup_f32_to_string_with_special_values_and_duplicate_cases()
     {
        let lookup_map = create_lookup([
            (Some(4.0), Some("four point zero")),
            (Some(f32::NAN), Some("NaN")),
            (Some(3.2), Some("three point two")),
            // Duplicate case to make sure it is not used
            (Some(f32::NAN), Some("should not use this NaN branch")),
            (Some(f32::INFINITY), Some("Infinity")),
            (Some(0.0), Some("zero")),
            // Duplicate case to make sure it is not used
            (
                Some(f32::INFINITY),
                Some("should not use this Infinity branch"),
            ),
            (Some(1.1), Some("one point one")),
        ]);

        let (input_values, expected) =
            create_input_and_expected::<Float32Array, StringArray, _, _>([
                (1.1, Some("one point one")),
                (f32::NAN, Some("NaN")),
                (3.2, Some("three point two")),
                (3.2, Some("three point two")),
                (0.0, Some("zero")),
                (f32::INFINITY, Some("Infinity")),
                (3.2, Some("three point two")),
                (f32::NEG_INFINITY, None), // No match in WHEN
                (f32::NEG_INFINITY, None), // No match in WHEN
                (3.2, Some("three point two")),
                (-0.0, None), // No match in WHEN
            ]);

        test_lookup_eval_with_and_without_else(
            &lookup_map,
            Arc::new(input_values),
            expected,
        );
    }

    #[test]
    fn test_case_when_literal_lookup_f16_to_string_with_special_values() {
        let lookup_map = create_lookup([
            (
                ScalarValue::Float16(Some(f16::from_f32(3.2))),
                Some("3 dot 2"),
            ),
            (ScalarValue::Float16(Some(f16::NAN)), Some("NaN")),
            (
                ScalarValue::Float16(Some(f16::from_f32(17.4))),
                Some("17 dot 4"),
            ),
            (ScalarValue::Float16(Some(f16::INFINITY)), Some("Infinity")),
            (ScalarValue::Float16(Some(f16::ZERO)), Some("zero")),
        ]);

        let (input_values, expected) =
            create_input_and_expected::<Float16Array, StringArray, _, _>([
                (f16::from_f32(3.2), Some("3 dot 2")),
                (f16::NAN, Some("NaN")),
                (f16::from_f32(17.4), Some("17 dot 4")),
                (f16::from_f32(17.4), Some("17 dot 4")),
                (f16::INFINITY, Some("Infinity")),
                (f16::from_f32(17.4), Some("17 dot 4")),
                (f16::NEG_INFINITY, None), // No match in WHEN
                (f16::NEG_INFINITY, None), // No match in WHEN
                (f16::from_f32(17.4), Some("17 dot 4")),
                (f16::NEG_ZERO, None), // No match in WHEN
            ]);

        test_lookup_eval_with_and_without_else(
            &lookup_map,
            Arc::new(input_values),
            expected,
        );
    }

    #[test]
    fn test_case_when_literal_lookup_f32_to_string_with_special_values() {
        let lookup_map = create_lookup([
            (3.2, Some("3 dot 2")),
            (f32::NAN, Some("NaN")),
            (17.4, Some("17 dot 4")),
            (f32::INFINITY, Some("Infinity")),
            (f32::ZERO, Some("zero")),
        ]);

        let (input_values, expected) =
            create_input_and_expected::<Float32Array, StringArray, _, _>([
                (3.2, Some("3 dot 2")),
                (f32::NAN, Some("NaN")),
                (17.4, Some("17 dot 4")),
                (17.4, Some("17 dot 4")),
                (f32::INFINITY, Some("Infinity")),
                (17.4, Some("17 dot 4")),
                (f32::NEG_INFINITY, None), // No match in WHEN
                (f32::NEG_INFINITY, None), // No match in WHEN
                (17.4, Some("17 dot 4")),
                (-0.0, None), // No match in WHEN
            ]);

        test_lookup_eval_with_and_without_else(
            &lookup_map,
            Arc::new(input_values),
            expected,
        );
    }

    #[test]
    fn test_case_when_literal_lookup_f64_to_string_with_special_values() {
        let lookup_map = create_lookup([
            (3.2, Some("3 dot 2")),
            (f64::NAN, Some("NaN")),
            (17.4, Some("17 dot 4")),
            (f64::INFINITY, Some("Infinity")),
            (f64::ZERO, Some("zero")),
        ]);

        let (input_values, expected) =
            create_input_and_expected::<Float64Array, StringArray, _, _>([
                (3.2, Some("3 dot 2")),
                (f64::NAN, Some("NaN")),
                (17.4, Some("17 dot 4")),
                (17.4, Some("17 dot 4")),
                (f64::INFINITY, Some("Infinity")),
                (17.4, Some("17 dot 4")),
                (f64::NEG_INFINITY, None), // No match in WHEN
                (f64::NEG_INFINITY, None), // No match in WHEN
                (17.4, Some("17 dot 4")),
                (-0.0, None), // No match in WHEN
            ]);

        test_lookup_eval_with_and_without_else(
            &lookup_map,
            Arc::new(input_values),
            expected,
        );
    }

    // Test that we don't lose the decimal precision and scale info
    #[test]
    fn test_decimal_with_non_default_precision_and_scale() {
        let lookup_map = create_lookup([
            (ScalarValue::Decimal32(Some(4), 3, 2), Some("four")),
            (ScalarValue::Decimal32(Some(2), 3, 2), Some("two")),
            (ScalarValue::Decimal32(Some(3), 3, 2), Some("three")),
            (ScalarValue::Decimal32(Some(1), 3, 2), Some("one")),
        ]);

        let (input_values, expected) =
            create_input_and_expected::<Decimal32Array, StringArray, _, _>([
                (1, Some("one")),
                (2, Some("two")),
                (3, Some("three")),
                (3, Some("three")),
                (2, Some("two")),
                (3, Some("three")),
                (5, None), // No match in WHEN
                (5, None), // No match in WHEN
                (3, Some("three")),
                (5, None), // No match in WHEN
            ]);

        let input_values = input_values
            .with_precision_and_scale(3, 2)
            .expect("must be able to set precision and scale");

        test_lookup_eval_with_and_without_else(
            &lookup_map,
            Arc::new(input_values),
            expected,
        );
    }

    // Test that we don't lose the timezone info
    #[test]
    fn test_timestamp_with_non_default_timezone() {
        let timezone: Option<Arc<str>> = Some("-10:00".into());
        let lookup_map = create_lookup([
            (
                ScalarValue::TimestampMillisecond(Some(4), timezone.clone()),
                Some("four"),
            ),
            (
                ScalarValue::TimestampMillisecond(Some(2), timezone.clone()),
                Some("two"),
            ),
            (
                ScalarValue::TimestampMillisecond(Some(3), timezone.clone()),
                Some("three"),
            ),
            (
                ScalarValue::TimestampMillisecond(Some(1), timezone.clone()),
                Some("one"),
            ),
        ]);

        let (input_values, expected) =
            create_input_and_expected::<TimestampMillisecondArray, StringArray, _, _>([
                (1, Some("one")),
                (2, Some("two")),
                (3, Some("three")),
                (3, Some("three")),
                (2, Some("two")),
                (3, Some("three")),
                (5, None), // No match in WHEN
                (5, None), // No match in WHEN
                (3, Some("three")),
                (5, None), // No match in WHEN
            ]);

        let input_values = input_values.with_timezone_opt(timezone);

        test_lookup_eval_with_and_without_else(
            &lookup_map,
            Arc::new(input_values),
            expected,
        );
    }

    #[test]
    fn test_with_strings_to_int32() {
        let lookup_map = create_lookup([
            (Some("why"), Some(42)),
            (Some("what"), Some(22)),
            (Some("when"), Some(17)),
        ]);

        let (input_values, expected) =
            create_input_and_expected::<StringArray, Int32Array, _, _>([
                (Some("why"), Some(42)),
                (Some("5"), None), // No match in WHEN
                (None, None), // None cases are never match in CASE <expr> WHEN <value> syntax
                (Some("what"), Some(22)),
                (None, None), // None cases are never match in CASE <expr> WHEN <value> syntax
                (None, None), // None cases are never match in CASE <expr> WHEN <value> syntax
                (Some("what"), Some(22)),
                (Some("5"), None), // No match in WHEN
            ]);

        let input_values = Arc::new(input_values) as ArrayRef;

        // Testing without ELSE should fallback to None
        test_case_when_literal_lookup(
            Arc::clone(&input_values),
            &lookup_map,
            None,
            Arc::new(expected.clone()),
        );

        // Testing with Else
        let else_value = 101;

        // Changing each expected None to be fallback
        let expected_with_else = expected
            .iter()
            .map(|item| item.unwrap_or(else_value))
            .map(Some)
            .collect::<Int32Array>();

        // Test case
        test_case_when_literal_lookup(
            input_values,
            &lookup_map,
            Some(ScalarValue::Int32(Some(else_value))),
            Arc::new(expected_with_else),
        );
    }
}
