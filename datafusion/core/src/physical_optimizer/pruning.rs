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

//! This module contains code to prune "containers" of row groups
//! based on statistics prior to execution. This can lead to
//! significant performance improvements by avoiding the need
//! to evaluate a plan on entire containers (e.g. an entire file)
//!
//! For example, DataFusion uses this code to prune (skip) row groups
//! while reading parquet files if it can be determined from the
//! predicate that nothing in the row group can match.
//!
//! This code can also be used by other systems to prune other
//! entities (e.g. entire files) if the statistics are known via some
//! other source (e.g. a catalog)

use std::collections::HashSet;
use std::convert::TryFrom;
use std::sync::Arc;

use crate::{
    common::{Column, DFSchema},
    error::{DataFusionError, Result},
    logical_expr::Operator,
    physical_plan::{ColumnarValue, PhysicalExpr},
};
use arrow::record_batch::RecordBatchOptions;
use arrow::{
    array::{new_null_array, ArrayRef, BooleanArray},
    datatypes::{DataType, Field, Schema, SchemaRef},
    record_batch::RecordBatch,
};
use datafusion_common::tree_node::{Transformed, TreeNode};
use datafusion_common::{downcast_value, ScalarValue};
use datafusion_physical_expr::utils::collect_columns;
use datafusion_physical_expr::{expressions as phys_expr, PhysicalExprRef};
use log::trace;

/// Interface to pass statistics information to [`PruningPredicate`]
///
/// Returns statistics for containers / files of data in Arrays.
///
/// For example, for the following three files with a single column
/// ```text
/// file1: column a: min=5, max=10
/// file2: column a: No stats
/// file2: column a: min=20, max=30
/// ```
///
/// PruningStatistics should return:
///
/// ```text
/// min_values("a") -> Some([5, Null, 20])
/// max_values("a") -> Some([10, Null, 30])
/// min_values("X") -> None
/// ```
pub trait PruningStatistics {
    /// return the minimum values for the named column, if known.
    /// Note: the returned array must contain `num_containers()` rows
    fn min_values(&self, column: &Column) -> Option<ArrayRef>;

    /// return the maximum values for the named column, if known.
    /// Note: the returned array must contain `num_containers()` rows.
    fn max_values(&self, column: &Column) -> Option<ArrayRef>;

    /// return the number of containers (e.g. row groups) being
    /// pruned with these statistics
    fn num_containers(&self) -> usize;

    /// return the number of null values for the named column as an
    /// `Option<UInt64Array>`.
    ///
    /// Note: the returned array must contain `num_containers()` rows.
    fn null_counts(&self, column: &Column) -> Option<ArrayRef>;
}

/// Evaluates filter expressions on statistics in order to
/// prune data containers (e.g. parquet row group)
///
/// See [`PruningPredicate::try_new`] for more information.
#[derive(Debug, Clone)]
pub struct PruningPredicate {
    /// The input schema against which the predicate will be evaluated
    schema: SchemaRef,
    /// Actual pruning predicate (rewritten in terms of column min/max statistics)
    predicate_expr: Arc<dyn PhysicalExpr>,
    /// The statistics required to evaluate this predicate
    required_columns: RequiredStatColumns,
    /// Original physical predicate from which this predicate expr is derived (required for serialization)
    orig_expr: Arc<dyn PhysicalExpr>,
}

impl PruningPredicate {
    /// Try to create a new instance of [`PruningPredicate`]
    ///
    /// This will translate the provided `expr` filter expression into
    /// a *pruning predicate*.
    ///
    /// A pruning predicate is one that has been rewritten in terms of
    /// the min and max values of column references and that evaluates
    /// to FALSE if the filter predicate would evaluate FALSE *for
    /// every row* whose values fell within the min / max ranges (aka
    /// could be pruned).
    ///
    /// The pruning predicate evaluates to TRUE or NULL
    /// if the filter predicate *might* evaluate to TRUE for at least
    /// one row whose values fell within the min/max ranges (in other
    /// words they might pass the predicate)
    ///
    /// For example, the filter expression `(column / 2) = 4` becomes
    /// the pruning predicate
    /// `(column_min / 2) <= 4 && 4 <= (column_max / 2))`
    pub fn try_new(expr: Arc<dyn PhysicalExpr>, schema: SchemaRef) -> Result<Self> {
        // build predicate expression once
        let mut required_columns = RequiredStatColumns::new();
        let predicate_expr =
            build_predicate_expression(&expr, schema.as_ref(), &mut required_columns);
        Ok(Self {
            schema,
            predicate_expr,
            required_columns,
            orig_expr: expr,
        })
    }

    /// For each set of statistics, evaluates the pruning predicate
    /// and returns a `bool` with the following meaning for a
    /// all rows whose values match the statistics:
    ///
    /// `true`: There MAY be rows that match the predicate
    ///
    /// `false`: There are no rows that could match the predicate
    ///
    /// Note this function takes a slice of statistics as a parameter
    /// to amortize the cost of the evaluation of the predicate
    /// against a single record batch.
    ///
    /// Note: the predicate passed to `prune` should be simplified as
    /// much as possible (e.g. this pass doesn't handle some
    /// expressions like `b = false`, but it does handle the
    /// simplified version `b`. The predicates are simplified via the
    /// ConstantFolding optimizer pass
    pub fn prune<S: PruningStatistics>(&self, statistics: &S) -> Result<Vec<bool>> {
        // build a RecordBatch that contains the min/max values in the
        // appropriate statistics columns
        let statistics_batch =
            build_statistics_record_batch(statistics, &self.required_columns)?;

        // Evaluate the pruning predicate on that record batch.
        //
        // Use true when the result of evaluating a predicate
        // expression on a row group is null (aka `None`). Null can
        // arise when the statistics are unknown or some calculation
        // in the predicate means we don't know for sure if the row
        // group can be filtered out or not. To maintain correctness
        // the row group must be kept and thus `true` is returned.
        match self.predicate_expr.evaluate(&statistics_batch)? {
            ColumnarValue::Array(array) => {
                let predicate_array = downcast_value!(array, BooleanArray);

                Ok(predicate_array
                    .into_iter()
                    .map(|x| x.unwrap_or(true)) // None -> true per comments above
                    .collect::<Vec<_>>())
            }
            // result was a column
            ColumnarValue::Scalar(ScalarValue::Boolean(v)) => {
                let v = v.unwrap_or(true); // None -> true per comments above
                Ok(vec![v; statistics.num_containers()])
            }
            other => {
                Err(DataFusionError::Internal(format!(
                    "Unexpected result of pruning predicate evaluation. Expected Boolean array \
                     or scalar but got {other:?}"
                )))
            }
        }
    }

    /// Return a reference to the input schema
    pub fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    /// Returns a reference to the physical expr used to construct this pruning predicate
    pub fn orig_expr(&self) -> &Arc<dyn PhysicalExpr> {
        &self.orig_expr
    }

    /// Returns a reference to the predicate expr
    pub fn predicate_expr(&self) -> &Arc<dyn PhysicalExpr> {
        &self.predicate_expr
    }

    /// Returns true if this pruning predicate is "always true" (aka will not prune anything)
    pub fn allways_true(&self) -> bool {
        is_always_true(&self.predicate_expr)
    }

    pub(crate) fn required_columns(&self) -> &RequiredStatColumns {
        &self.required_columns
    }
}

fn is_always_true(expr: &Arc<dyn PhysicalExpr>) -> bool {
    expr.as_any()
        .downcast_ref::<phys_expr::Literal>()
        .map(|l| matches!(l.value(), ScalarValue::Boolean(Some(true))))
        .unwrap_or_default()
}

/// Records for which columns statistics are necessary to evaluate a
/// pruning predicate.
///
/// Handles creating references to the min/max statistics
/// for columns as well as recording which statistics are needed
#[derive(Debug, Default, Clone)]
pub(crate) struct RequiredStatColumns {
    /// The statistics required to evaluate this predicate:
    /// * The unqualified column in the input schema
    /// * Statistics type (e.g. Min or Max or Null_Count)
    /// * The field the statistics value should be placed in for
    ///   pruning predicate evaluation
    columns: Vec<(phys_expr::Column, StatisticsType, Field)>,
}

impl RequiredStatColumns {
    fn new() -> Self {
        Self::default()
    }

    /// Returns number of unique columns.
    pub(crate) fn n_columns(&self) -> usize {
        self.iter()
            .map(|(c, _s, _f)| c)
            .collect::<HashSet<_>>()
            .len()
    }

    /// Returns an iterator over items in columns (see doc on
    /// `self.columns` for details)
    pub(crate) fn iter(
        &self,
    ) -> impl Iterator<Item = &(phys_expr::Column, StatisticsType, Field)> {
        self.columns.iter()
    }

    fn find_stat_column(
        &self,
        column: &phys_expr::Column,
        statistics_type: StatisticsType,
    ) -> Option<usize> {
        self.columns
            .iter()
            .enumerate()
            .find(|(_i, (c, t, _f))| c == column && t == &statistics_type)
            .map(|(i, (_c, _t, _f))| i)
    }

    /// Rewrites column_expr so that all appearances of column
    /// are replaced with a reference to either the min or max
    /// statistics column, while keeping track that a reference to the statistics
    /// column is required
    ///
    /// for example, an expression like `col("foo") > 5`, when called
    /// with Max would result in an expression like `col("foo_max") >
    /// 5` with the appropriate entry noted in self.columns
    fn stat_column_expr(
        &mut self,
        column: &phys_expr::Column,
        column_expr: &Arc<dyn PhysicalExpr>,
        field: &Field,
        stat_type: StatisticsType,
        suffix: &str,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        let (idx, need_to_insert) = match self.find_stat_column(column, stat_type) {
            Some(idx) => (idx, false),
            None => (self.columns.len(), true),
        };

        let stat_column =
            phys_expr::Column::new(&format!("{}_{}", column.name(), suffix), idx);

        // only add statistics column if not previously added
        if need_to_insert {
            let stat_field = Field::new(
                stat_column.name(),
                field.data_type().clone(),
                field.is_nullable(),
            );
            self.columns.push((column.clone(), stat_type, stat_field));
        }
        rewrite_column_expr(column_expr.clone(), column, &stat_column)
    }

    /// rewrite col --> col_min
    fn min_column_expr(
        &mut self,
        column: &phys_expr::Column,
        column_expr: &Arc<dyn PhysicalExpr>,
        field: &Field,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        self.stat_column_expr(column, column_expr, field, StatisticsType::Min, "min")
    }

    /// rewrite col --> col_max
    fn max_column_expr(
        &mut self,
        column: &phys_expr::Column,
        column_expr: &Arc<dyn PhysicalExpr>,
        field: &Field,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        self.stat_column_expr(column, column_expr, field, StatisticsType::Max, "max")
    }

    /// rewrite col --> col_null_count
    fn null_count_column_expr(
        &mut self,
        column: &phys_expr::Column,
        column_expr: &Arc<dyn PhysicalExpr>,
        field: &Field,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        self.stat_column_expr(
            column,
            column_expr,
            field,
            StatisticsType::NullCount,
            "null_count",
        )
    }
}

impl From<Vec<(phys_expr::Column, StatisticsType, Field)>> for RequiredStatColumns {
    fn from(columns: Vec<(phys_expr::Column, StatisticsType, Field)>) -> Self {
        Self { columns }
    }
}

/// Build a RecordBatch from a list of statistics, creating arrays,
/// with one row for each PruningStatistics and columns specified in
/// in the required_columns parameter.
///
/// For example, if the requested columns are
/// ```text
/// ("s1", Min, Field:s1_min)
/// ("s2", Max, field:s2_max)
///```
///
/// And the input statistics had
/// ```text
/// S1(Min: 5, Max: 10)
/// S2(Min: 99, Max: 1000)
/// S3(Min: 1, Max: 2)
/// ```
///
/// Then this function would build a record batch with 2 columns and
/// one row s1_min and s2_max as follows (s3 is not requested):
///
/// ```text
/// s1_min | s2_max
/// -------+--------
///   5    | 1000
/// ```
fn build_statistics_record_batch<S: PruningStatistics>(
    statistics: &S,
    required_columns: &RequiredStatColumns,
) -> Result<RecordBatch> {
    let mut fields = Vec::<Field>::new();
    let mut arrays = Vec::<ArrayRef>::new();
    // For each needed statistics column:
    for (column, statistics_type, stat_field) in required_columns.iter() {
        let column = Column::from_name(column.name());
        let data_type = stat_field.data_type();

        let num_containers = statistics.num_containers();

        let array = match statistics_type {
            StatisticsType::Min => statistics.min_values(&column),
            StatisticsType::Max => statistics.max_values(&column),
            StatisticsType::NullCount => statistics.null_counts(&column),
        };
        let array = array.unwrap_or_else(|| new_null_array(data_type, num_containers));

        if num_containers != array.len() {
            return Err(DataFusionError::Internal(format!(
                "mismatched statistics length. Expected {}, got {}",
                num_containers,
                array.len()
            )));
        }

        // cast statistics array to required data type (e.g. parquet
        // provides timestamp statistics as "Int64")
        let array = arrow::compute::cast(&array, data_type)?;

        fields.push(stat_field.clone());
        arrays.push(array);
    }

    let schema = Arc::new(Schema::new(fields));
    // provide the count in case there were no needed statistics
    let mut options = RecordBatchOptions::default();
    options.row_count = Some(statistics.num_containers());

    trace!(
        "Creating statistics batch for {:#?} with {:#?}",
        required_columns,
        arrays
    );

    RecordBatch::try_new_with_options(schema, arrays, &options).map_err(|err| {
        DataFusionError::Plan(format!("Can not create statistics record batch: {err}"))
    })
}

struct PruningExpressionBuilder<'a> {
    column: phys_expr::Column,
    column_expr: Arc<dyn PhysicalExpr>,
    op: Operator,
    scalar_expr: Arc<dyn PhysicalExpr>,
    field: &'a Field,
    required_columns: &'a mut RequiredStatColumns,
}

impl<'a> PruningExpressionBuilder<'a> {
    fn try_new(
        left: &'a Arc<dyn PhysicalExpr>,
        right: &'a Arc<dyn PhysicalExpr>,
        op: Operator,
        schema: &'a Schema,
        required_columns: &'a mut RequiredStatColumns,
    ) -> Result<Self> {
        // find column name; input could be a more complicated expression
        let left_columns = collect_columns(left);
        let right_columns = collect_columns(right);
        let (column_expr, scalar_expr, columns, correct_operator) =
            match (left_columns.len(), right_columns.len()) {
                (1, 0) => (left, right, left_columns, op),
                (0, 1) => (right, left, right_columns, reverse_operator(op)?),
                _ => {
                    // if more than one column used in expression - not supported
                    return Err(DataFusionError::Plan(
                        "Multi-column expressions are not currently supported"
                            .to_string(),
                    ));
                }
            };

        let df_schema = DFSchema::try_from(schema.clone())?;
        let (column_expr, correct_operator, scalar_expr) = rewrite_expr_to_prunable(
            column_expr,
            correct_operator,
            scalar_expr,
            df_schema,
        )?;
        let column = columns.iter().next().unwrap().clone();
        let field = match schema.column_with_name(column.name()) {
            Some((_, f)) => f,
            _ => {
                return Err(DataFusionError::Plan(
                    "Field not found in schema".to_string(),
                ));
            }
        };

        Ok(Self {
            column,
            column_expr,
            op: correct_operator,
            scalar_expr,
            field,
            required_columns,
        })
    }

    fn op(&self) -> Operator {
        self.op
    }

    fn scalar_expr(&self) -> &Arc<dyn PhysicalExpr> {
        &self.scalar_expr
    }

    fn min_column_expr(&mut self) -> Result<Arc<dyn PhysicalExpr>> {
        self.required_columns
            .min_column_expr(&self.column, &self.column_expr, self.field)
    }

    fn max_column_expr(&mut self) -> Result<Arc<dyn PhysicalExpr>> {
        self.required_columns
            .max_column_expr(&self.column, &self.column_expr, self.field)
    }
}

/// This function is designed to rewrite the column_expr to
/// ensure the column_expr is monotonically increasing.
///
/// For example,
/// 1. `col > 10`
/// 2. `-col > 10` should be rewritten to `col < -10`
/// 3. `!col = true` would be rewritten to `col = !true`
/// 4. `abs(a - 10) > 0` not supported
/// 5. `cast(can_prunable_expr) > 10`
/// 6. `try_cast(can_prunable_expr) > 10`
///
/// More rewrite rules are still in progress.
fn rewrite_expr_to_prunable(
    column_expr: &PhysicalExprRef,
    op: Operator,
    scalar_expr: &PhysicalExprRef,
    schema: DFSchema,
) -> Result<(PhysicalExprRef, Operator, PhysicalExprRef)> {
    if !is_compare_op(op) {
        return Err(DataFusionError::Plan(
            "rewrite_expr_to_prunable only support compare expression".to_string(),
        ));
    }

    let column_expr_any = column_expr.as_any();

    if column_expr_any
        .downcast_ref::<phys_expr::Column>()
        .is_some()
    {
        // `col op lit()`
        Ok((column_expr.clone(), op, scalar_expr.clone()))
    } else if let Some(cast) = column_expr_any.downcast_ref::<phys_expr::CastExpr>() {
        // `cast(col) op lit()`
        let arrow_schema: SchemaRef = schema.clone().into();
        let from_type = cast.expr().data_type(&arrow_schema)?;
        verify_support_type_for_prune(&from_type, cast.cast_type())?;
        let (left, op, right) =
            rewrite_expr_to_prunable(cast.expr(), op, scalar_expr, schema)?;
        let left = Arc::new(phys_expr::CastExpr::new(
            left,
            cast.cast_type().clone(),
            None,
        ));
        Ok((left, op, right))
    } else if let Some(try_cast) =
        column_expr_any.downcast_ref::<phys_expr::TryCastExpr>()
    {
        // `try_cast(col) op lit()`
        let arrow_schema: SchemaRef = schema.clone().into();
        let from_type = try_cast.expr().data_type(&arrow_schema)?;
        verify_support_type_for_prune(&from_type, try_cast.cast_type())?;
        let (left, op, right) =
            rewrite_expr_to_prunable(try_cast.expr(), op, scalar_expr, schema)?;
        let left = Arc::new(phys_expr::TryCastExpr::new(
            left,
            try_cast.cast_type().clone(),
        ));
        Ok((left, op, right))
    } else if let Some(neg) = column_expr_any.downcast_ref::<phys_expr::NegativeExpr>() {
        // `-col > lit()`  --> `col < -lit()`
        let (left, op, right) =
            rewrite_expr_to_prunable(neg.arg(), op, scalar_expr, schema)?;
        let right = Arc::new(phys_expr::NegativeExpr::new(right));
        Ok((left, reverse_operator(op)?, right))
    } else if let Some(not) = column_expr_any.downcast_ref::<phys_expr::NotExpr>() {
        // `!col = true` --> `col = !true`
        if op != Operator::Eq && op != Operator::NotEq {
            return Err(DataFusionError::Plan(
                "Not with operator other than Eq / NotEq is not supported".to_string(),
            ));
        }
        if not
            .arg()
            .as_any()
            .downcast_ref::<phys_expr::Column>()
            .is_some()
        {
            let left = not.arg().clone();
            let right = Arc::new(phys_expr::NotExpr::new(scalar_expr.clone()));
            Ok((left, reverse_operator(op)?, right))
        } else {
            Err(DataFusionError::Plan(format!(
                "Not with complex expression {column_expr:?} is not supported"
            )))
        }
    } else {
        Err(DataFusionError::Plan(format!(
            "column expression {column_expr:?} is not supported"
        )))
    }
}

fn is_compare_op(op: Operator) -> bool {
    matches!(
        op,
        Operator::Eq
            | Operator::NotEq
            | Operator::Lt
            | Operator::LtEq
            | Operator::Gt
            | Operator::GtEq
    )
}

// The pruning logic is based on the comparing the min/max bounds.
// Must make sure the two type has order.
// For example, casts from string to numbers is not correct.
// Because the "13" is less than "3" with UTF8 comparison order.
fn verify_support_type_for_prune(from_type: &DataType, to_type: &DataType) -> Result<()> {
    // TODO: support other data type for prunable cast or try cast
    if matches!(
        from_type,
        DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::Decimal128(_, _)
    ) && matches!(
        to_type,
        DataType::Int8 | DataType::Int32 | DataType::Int64 | DataType::Decimal128(_, _)
    ) {
        Ok(())
    } else {
        Err(DataFusionError::Plan(format!(
            "Try Cast/Cast with from type {from_type} to type {to_type} is not supported"
        )))
    }
}

/// replaces a column with an old name with a new name in an expression
fn rewrite_column_expr(
    e: Arc<dyn PhysicalExpr>,
    column_old: &phys_expr::Column,
    column_new: &phys_expr::Column,
) -> Result<Arc<dyn PhysicalExpr>> {
    e.transform(&|expr| {
        if let Some(column) = expr.as_any().downcast_ref::<phys_expr::Column>() {
            if column == column_old {
                return Ok(Transformed::Yes(Arc::new(column_new.clone())));
            }
        }

        Ok(Transformed::No(expr))
    })
}

fn reverse_operator(op: Operator) -> Result<Operator> {
    op.swap().ok_or_else(|| {
        DataFusionError::Internal(format!(
            "Could not reverse operator {op} while building pruning predicate"
        ))
    })
}

/// Given a column reference to `column`, returns a pruning
/// expression in terms of the min and max that will evaluate to true
/// if the column may contain values, and false if definitely does not
/// contain values
fn build_single_column_expr(
    column: &phys_expr::Column,
    schema: &Schema,
    required_columns: &mut RequiredStatColumns,
    is_not: bool, // if true, treat as !col
) -> Option<Arc<dyn PhysicalExpr>> {
    let field = schema.field_with_name(column.name()).ok()?;

    if matches!(field.data_type(), &DataType::Boolean) {
        let col_ref = Arc::new(column.clone()) as _;

        let min = required_columns
            .min_column_expr(column, &col_ref, field)
            .ok()?;
        let max = required_columns
            .max_column_expr(column, &col_ref, field)
            .ok()?;

        // remember -- we want an expression that is:
        // TRUE: if there may be rows that match
        // FALSE: if there are no rows that match
        if is_not {
            // The only way we know a column couldn't match is if both the min and max are true
            // !(min && max)
            Some(Arc::new(phys_expr::NotExpr::new(Arc::new(
                phys_expr::BinaryExpr::new(min, Operator::And, max),
            ))))
        } else {
            // the only way we know a column couldn't match is if both the min and max are false
            // !(!min && !max) --> min || max
            Some(Arc::new(phys_expr::BinaryExpr::new(min, Operator::Or, max)))
        }
    } else {
        None
    }
}

/// Given an expression reference to `expr`, if `expr` is a column expression,
/// returns a pruning expression in terms of IsNull that will evaluate to true
/// if the column may contain null, and false if definitely does not
/// contain null.
fn build_is_null_column_expr(
    expr: &Arc<dyn PhysicalExpr>,
    schema: &Schema,
    required_columns: &mut RequiredStatColumns,
) -> Option<Arc<dyn PhysicalExpr>> {
    if let Some(col) = expr.as_any().downcast_ref::<phys_expr::Column>() {
        let field = schema.field_with_name(col.name()).ok()?;

        let null_count_field = &Field::new(field.name(), DataType::UInt64, true);
        required_columns
            .null_count_column_expr(col, expr, null_count_field)
            .map(|null_count_column_expr| {
                // IsNull(column) => null_count > 0
                Arc::new(phys_expr::BinaryExpr::new(
                    null_count_column_expr,
                    Operator::Gt,
                    Arc::new(phys_expr::Literal::new(ScalarValue::UInt64(Some(0)))),
                )) as _
            })
            .ok()
    } else {
        None
    }
}

/// Translate logical filter expression into pruning predicate
/// expression that will evaluate to FALSE if it can be determined no
/// rows between the min/max values could pass the predicates.
///
/// Returns the pruning predicate as an [`PhysicalExpr`]
fn build_predicate_expression(
    expr: &Arc<dyn PhysicalExpr>,
    schema: &Schema,
    required_columns: &mut RequiredStatColumns,
) -> Arc<dyn PhysicalExpr> {
    // Returned for unsupported expressions. Such expressions are
    // converted to TRUE.
    let unhandled = Arc::new(phys_expr::Literal::new(ScalarValue::Boolean(Some(true))));

    // predicate expression can only be a binary expression
    let expr_any = expr.as_any();
    if let Some(is_null) = expr_any.downcast_ref::<phys_expr::IsNullExpr>() {
        return build_is_null_column_expr(is_null.arg(), schema, required_columns)
            .unwrap_or(unhandled);
    }
    if let Some(col) = expr_any.downcast_ref::<phys_expr::Column>() {
        return build_single_column_expr(col, schema, required_columns, false)
            .unwrap_or(unhandled);
    }
    if let Some(not) = expr_any.downcast_ref::<phys_expr::NotExpr>() {
        // match !col (don't do so recursively)
        if let Some(col) = not.arg().as_any().downcast_ref::<phys_expr::Column>() {
            return build_single_column_expr(col, schema, required_columns, true)
                .unwrap_or(unhandled);
        } else {
            return unhandled;
        }
    }
    if let Some(in_list) = expr_any.downcast_ref::<phys_expr::InListExpr>() {
        if !in_list.list().is_empty() && in_list.list().len() < 20 {
            let eq_op = if in_list.negated() {
                Operator::NotEq
            } else {
                Operator::Eq
            };
            let re_op = if in_list.negated() {
                Operator::And
            } else {
                Operator::Or
            };
            let change_expr = in_list
                .list()
                .iter()
                .cloned()
                .map(|e| {
                    Arc::new(phys_expr::BinaryExpr::new(
                        in_list.expr().clone(),
                        eq_op,
                        e.clone(),
                    )) as _
                })
                .reduce(|a, b| Arc::new(phys_expr::BinaryExpr::new(a, re_op, b)) as _)
                .unwrap();
            return build_predicate_expression(&change_expr, schema, required_columns);
        } else {
            return unhandled;
        }
    }

    let (left, op, right) = {
        if let Some(bin_expr) = expr_any.downcast_ref::<phys_expr::BinaryExpr>() {
            (
                bin_expr.left().clone(),
                *bin_expr.op(),
                bin_expr.right().clone(),
            )
        } else {
            return unhandled;
        }
    };

    if op == Operator::And || op == Operator::Or {
        let left_expr = build_predicate_expression(&left, schema, required_columns);
        let right_expr = build_predicate_expression(&right, schema, required_columns);
        // simplify boolean expression if applicable
        let expr = match (&left_expr, op, &right_expr) {
            (left, Operator::And, _) if is_always_true(left) => right_expr,
            (_, Operator::And, right) if is_always_true(right) => left_expr,
            (left, Operator::Or, right)
                if is_always_true(left) || is_always_true(right) =>
            {
                unhandled
            }
            _ => Arc::new(phys_expr::BinaryExpr::new(left_expr, op, right_expr)),
        };
        return expr;
    }

    let expr_builder =
        PruningExpressionBuilder::try_new(&left, &right, op, schema, required_columns);
    let mut expr_builder = match expr_builder {
        Ok(builder) => builder,
        // allow partial failure in predicate expression generation
        // this can still produce a useful predicate when multiple conditions are joined using AND
        Err(_) => {
            return unhandled;
        }
    };

    build_statistics_expr(&mut expr_builder).unwrap_or(unhandled)
}

fn build_statistics_expr(
    expr_builder: &mut PruningExpressionBuilder,
) -> Result<Arc<dyn PhysicalExpr>> {
    let statistics_expr: Arc<dyn PhysicalExpr> =
        match expr_builder.op() {
            Operator::NotEq => {
                // column != literal => (min, max) = literal =>
                // !(min != literal && max != literal) ==>
                // min != literal || literal != max
                let min_column_expr = expr_builder.min_column_expr()?;
                let max_column_expr = expr_builder.max_column_expr()?;
                Arc::new(phys_expr::BinaryExpr::new(
                    Arc::new(phys_expr::BinaryExpr::new(
                        min_column_expr,
                        Operator::NotEq,
                        expr_builder.scalar_expr().clone(),
                    )),
                    Operator::Or,
                    Arc::new(phys_expr::BinaryExpr::new(
                        expr_builder.scalar_expr().clone(),
                        Operator::NotEq,
                        max_column_expr,
                    )),
                ))
            }
            Operator::Eq => {
                // column = literal => (min, max) = literal => min <= literal && literal <= max
                // (column / 2) = 4 => (column_min / 2) <= 4 && 4 <= (column_max / 2)
                let min_column_expr = expr_builder.min_column_expr()?;
                let max_column_expr = expr_builder.max_column_expr()?;
                Arc::new(phys_expr::BinaryExpr::new(
                    Arc::new(phys_expr::BinaryExpr::new(
                        min_column_expr,
                        Operator::LtEq,
                        expr_builder.scalar_expr().clone(),
                    )),
                    Operator::And,
                    Arc::new(phys_expr::BinaryExpr::new(
                        expr_builder.scalar_expr().clone(),
                        Operator::LtEq,
                        max_column_expr,
                    )),
                ))
            }
            Operator::Gt => {
                // column > literal => (min, max) > literal => max > literal
                Arc::new(phys_expr::BinaryExpr::new(
                    expr_builder.max_column_expr()?,
                    Operator::Gt,
                    expr_builder.scalar_expr().clone(),
                ))
            }
            Operator::GtEq => {
                // column >= literal => (min, max) >= literal => max >= literal
                Arc::new(phys_expr::BinaryExpr::new(
                    expr_builder.max_column_expr()?,
                    Operator::GtEq,
                    expr_builder.scalar_expr().clone(),
                ))
            }
            Operator::Lt => {
                // column < literal => (min, max) < literal => min < literal
                Arc::new(phys_expr::BinaryExpr::new(
                    expr_builder.min_column_expr()?,
                    Operator::Lt,
                    expr_builder.scalar_expr().clone(),
                ))
            }
            Operator::LtEq => {
                // column <= literal => (min, max) <= literal => min <= literal
                Arc::new(phys_expr::BinaryExpr::new(
                    expr_builder.min_column_expr()?,
                    Operator::LtEq,
                    expr_builder.scalar_expr().clone(),
                ))
            }
            // other expressions are not supported
            _ => return Err(DataFusionError::Plan(
                "expressions other than (neq, eq, gt, gteq, lt, lteq) are not supported"
                    .to_string(),
            )),
        };
    Ok(statistics_expr)
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub(crate) enum StatisticsType {
    Min,
    Max,
    NullCount,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::logical_expr::{col, lit};
    use crate::{assert_batches_eq, physical_optimizer::pruning::StatisticsType};
    use arrow::array::Decimal128Array;
    use arrow::{
        array::{BinaryArray, Int32Array, Int64Array, StringArray},
        datatypes::{DataType, TimeUnit},
    };
    use datafusion_common::{ScalarValue, ToDFSchema};
    use datafusion_expr::expr::InList;
    use datafusion_expr::{cast, is_null, try_cast, Expr};
    use datafusion_physical_expr::create_physical_expr;
    use datafusion_physical_expr::execution_props::ExecutionProps;
    use std::collections::HashMap;
    use std::ops::{Not, Rem};

    #[derive(Debug)]
    /// Mock statistic provider for tests
    ///
    /// Each row represents the statistics for a "container" (which
    /// might represent an entire parquet file, or directory of files,
    /// or some other collection of data for which we had statistics)
    ///
    /// Note All `ArrayRefs` must be the same size.
    struct ContainerStats {
        min: ArrayRef,
        max: ArrayRef,
        /// Optional values
        null_counts: Option<ArrayRef>,
    }

    impl ContainerStats {
        fn new_decimal128(
            min: impl IntoIterator<Item = Option<i128>>,
            max: impl IntoIterator<Item = Option<i128>>,
            precision: u8,
            scale: i8,
        ) -> Self {
            Self {
                min: Arc::new(
                    min.into_iter()
                        .collect::<Decimal128Array>()
                        .with_precision_and_scale(precision, scale)
                        .unwrap(),
                ),
                max: Arc::new(
                    max.into_iter()
                        .collect::<Decimal128Array>()
                        .with_precision_and_scale(precision, scale)
                        .unwrap(),
                ),
                null_counts: None,
            }
        }

        fn new_i64(
            min: impl IntoIterator<Item = Option<i64>>,
            max: impl IntoIterator<Item = Option<i64>>,
        ) -> Self {
            Self {
                min: Arc::new(min.into_iter().collect::<Int64Array>()),
                max: Arc::new(max.into_iter().collect::<Int64Array>()),
                null_counts: None,
            }
        }

        fn new_i32(
            min: impl IntoIterator<Item = Option<i32>>,
            max: impl IntoIterator<Item = Option<i32>>,
        ) -> Self {
            Self {
                min: Arc::new(min.into_iter().collect::<Int32Array>()),
                max: Arc::new(max.into_iter().collect::<Int32Array>()),
                null_counts: None,
            }
        }

        fn new_utf8<'a>(
            min: impl IntoIterator<Item = Option<&'a str>>,
            max: impl IntoIterator<Item = Option<&'a str>>,
        ) -> Self {
            Self {
                min: Arc::new(min.into_iter().collect::<StringArray>()),
                max: Arc::new(max.into_iter().collect::<StringArray>()),
                null_counts: None,
            }
        }

        fn new_bool(
            min: impl IntoIterator<Item = Option<bool>>,
            max: impl IntoIterator<Item = Option<bool>>,
        ) -> Self {
            Self {
                min: Arc::new(min.into_iter().collect::<BooleanArray>()),
                max: Arc::new(max.into_iter().collect::<BooleanArray>()),
                null_counts: None,
            }
        }

        fn min(&self) -> Option<ArrayRef> {
            Some(self.min.clone())
        }

        fn max(&self) -> Option<ArrayRef> {
            Some(self.max.clone())
        }

        fn null_counts(&self) -> Option<ArrayRef> {
            self.null_counts.clone()
        }

        fn len(&self) -> usize {
            assert_eq!(self.min.len(), self.max.len());
            self.min.len()
        }

        /// Add null counts. There must be the same number of null counts as
        /// there are containers
        fn with_null_counts(
            mut self,
            counts: impl IntoIterator<Item = Option<i64>>,
        ) -> Self {
            // take stats out and update them
            let null_counts: ArrayRef =
                Arc::new(counts.into_iter().collect::<Int64Array>());

            assert_eq!(null_counts.len(), self.len());
            self.null_counts = Some(null_counts);
            self
        }
    }

    #[derive(Debug, Default)]
    struct TestStatistics {
        // key: column name
        stats: HashMap<Column, ContainerStats>,
    }

    impl TestStatistics {
        fn new() -> Self {
            Self::default()
        }

        fn with(
            mut self,
            name: impl Into<String>,
            container_stats: ContainerStats,
        ) -> Self {
            let col = Column::from_name(name.into());
            self.stats.insert(col, container_stats);
            self
        }

        /// Add null counts for the specified columm.
        /// There must be the same number of null counts as
        /// there are containers
        fn with_null_counts(
            mut self,
            name: impl Into<String>,
            counts: impl IntoIterator<Item = Option<i64>>,
        ) -> Self {
            let col = Column::from_name(name.into());

            // take stats out and update them
            let container_stats = self
                .stats
                .remove(&col)
                .expect("Can not find stats for column")
                .with_null_counts(counts);

            // put stats back in
            self.stats.insert(col, container_stats);
            self
        }
    }

    impl PruningStatistics for TestStatistics {
        fn min_values(&self, column: &Column) -> Option<ArrayRef> {
            self.stats
                .get(column)
                .map(|container_stats| container_stats.min())
                .unwrap_or(None)
        }

        fn max_values(&self, column: &Column) -> Option<ArrayRef> {
            self.stats
                .get(column)
                .map(|container_stats| container_stats.max())
                .unwrap_or(None)
        }

        fn num_containers(&self) -> usize {
            self.stats
                .values()
                .next()
                .map(|container_stats| container_stats.len())
                .unwrap_or(0)
        }

        fn null_counts(&self, column: &Column) -> Option<ArrayRef> {
            self.stats
                .get(column)
                .map(|container_stats| container_stats.null_counts())
                .unwrap_or(None)
        }
    }

    /// Returns the specified min/max container values
    struct OneContainerStats {
        min_values: Option<ArrayRef>,
        max_values: Option<ArrayRef>,
        num_containers: usize,
    }

    impl PruningStatistics for OneContainerStats {
        fn min_values(&self, _column: &Column) -> Option<ArrayRef> {
            self.min_values.clone()
        }

        fn max_values(&self, _column: &Column) -> Option<ArrayRef> {
            self.max_values.clone()
        }

        fn num_containers(&self) -> usize {
            self.num_containers
        }

        fn null_counts(&self, _column: &Column) -> Option<ArrayRef> {
            None
        }
    }

    #[test]
    fn test_build_statistics_record_batch() {
        // Request a record batch with of s1_min, s2_max, s3_max, s3_min
        let required_columns = RequiredStatColumns::from(vec![
            // min of original column s1, named s1_min
            (
                phys_expr::Column::new("s1", 1),
                StatisticsType::Min,
                Field::new("s1_min", DataType::Int32, true),
            ),
            // max of original column s2, named s2_max
            (
                phys_expr::Column::new("s2", 2),
                StatisticsType::Max,
                Field::new("s2_max", DataType::Int32, true),
            ),
            // max of original column s3, named s3_max
            (
                phys_expr::Column::new("s3", 3),
                StatisticsType::Max,
                Field::new("s3_max", DataType::Utf8, true),
            ),
            // min of original column s3, named s3_min
            (
                phys_expr::Column::new("s3", 3),
                StatisticsType::Min,
                Field::new("s3_min", DataType::Utf8, true),
            ),
        ]);

        let statistics = TestStatistics::new()
            .with(
                "s1",
                ContainerStats::new_i32(
                    vec![None, None, Some(9), None],  // min
                    vec![Some(10), None, None, None], // max
                ),
            )
            .with(
                "s2",
                ContainerStats::new_i32(
                    vec![Some(2), None, None, None],  // min
                    vec![Some(20), None, None, None], // max
                ),
            )
            .with(
                "s3",
                ContainerStats::new_utf8(
                    vec![Some("a"), None, None, None],      // min
                    vec![Some("q"), None, Some("r"), None], // max
                ),
            );

        let batch =
            build_statistics_record_batch(&statistics, &required_columns).unwrap();
        let expected = vec![
            "+--------+--------+--------+--------+",
            "| s1_min | s2_max | s3_max | s3_min |",
            "+--------+--------+--------+--------+",
            "|        | 20     | q      | a      |",
            "|        |        |        |        |",
            "| 9      |        | r      |        |",
            "|        |        |        |        |",
            "+--------+--------+--------+--------+",
        ];

        assert_batches_eq!(expected, &[batch]);
    }

    #[test]
    fn test_build_statistics_casting() {
        // Test requesting a Timestamp column, but getting statistics as Int64
        // which is what Parquet does

        // Request a record batch with of s1_min as a timestamp
        let required_columns = RequiredStatColumns::from(vec![(
            phys_expr::Column::new("s3", 3),
            StatisticsType::Min,
            Field::new(
                "s1_min",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                true,
            ),
        )]);

        // Note the statistics pass back i64 (not timestamp)
        let statistics = OneContainerStats {
            min_values: Some(Arc::new(Int64Array::from(vec![Some(10)]))),
            max_values: Some(Arc::new(Int64Array::from(vec![Some(20)]))),
            num_containers: 1,
        };

        let batch =
            build_statistics_record_batch(&statistics, &required_columns).unwrap();
        let expected = vec![
            "+-------------------------------+",
            "| s1_min                        |",
            "+-------------------------------+",
            "| 1970-01-01T00:00:00.000000010 |",
            "+-------------------------------+",
        ];

        assert_batches_eq!(expected, &[batch]);
    }

    #[test]
    fn test_build_statistics_no_required_stats() {
        let required_columns = RequiredStatColumns::new();

        let statistics = OneContainerStats {
            min_values: Some(Arc::new(Int64Array::from(vec![Some(10)]))),
            max_values: Some(Arc::new(Int64Array::from(vec![Some(20)]))),
            num_containers: 1,
        };

        let batch =
            build_statistics_record_batch(&statistics, &required_columns).unwrap();
        assert_eq!(batch.num_rows(), 1); // had 1 container
    }

    #[test]
    fn test_build_statistics_inconsistent_types() {
        // Test requesting a Utf8 column when the stats return some other type

        // Request a record batch with of s1_min as a timestamp
        let required_columns = RequiredStatColumns::from(vec![(
            phys_expr::Column::new("s3", 3),
            StatisticsType::Min,
            Field::new("s1_min", DataType::Utf8, true),
        )]);

        // Note the statistics return an invalid UTF-8 sequence which will be converted to null
        let statistics = OneContainerStats {
            min_values: Some(Arc::new(BinaryArray::from(vec![&[255u8] as &[u8]]))),
            max_values: None,
            num_containers: 1,
        };

        let batch =
            build_statistics_record_batch(&statistics, &required_columns).unwrap();
        let expected = vec![
            "+--------+",
            "| s1_min |",
            "+--------+",
            "|        |",
            "+--------+",
        ];

        assert_batches_eq!(expected, &[batch]);
    }

    #[test]
    fn test_build_statistics_inconsistent_length() {
        // return an inconsistent length to the actual statistics arrays
        let required_columns = RequiredStatColumns::from(vec![(
            phys_expr::Column::new("s1", 3),
            StatisticsType::Min,
            Field::new("s1_min", DataType::Int64, true),
        )]);

        // Note the statistics pass back i64 (not timestamp)
        let statistics = OneContainerStats {
            min_values: Some(Arc::new(Int64Array::from(vec![Some(10)]))),
            max_values: Some(Arc::new(Int64Array::from(vec![Some(20)]))),
            num_containers: 3,
        };

        let result =
            build_statistics_record_batch(&statistics, &required_columns).unwrap_err();
        assert!(
            result
                .to_string()
                .contains("mismatched statistics length. Expected 3, got 1"),
            "{}",
            result
        );
    }

    #[test]
    fn row_group_predicate_eq() -> Result<()> {
        let schema = Schema::new(vec![Field::new("c1", DataType::Int32, false)]);
        let expected_expr = "c1_min@0 <= 1 AND 1 <= c1_max@1";

        // test column on the left
        let expr = col("c1").eq(lit(1));
        let predicate_expr = test_build_predicate_expression(
            &expr,
            &schema,
            &mut RequiredStatColumns::new(),
        );
        assert_eq!(predicate_expr.to_string(), expected_expr);

        // test column on the right
        let expr = lit(1).eq(col("c1"));
        let predicate_expr = test_build_predicate_expression(
            &expr,
            &schema,
            &mut RequiredStatColumns::new(),
        );
        assert_eq!(predicate_expr.to_string(), expected_expr);

        Ok(())
    }

    #[test]
    fn row_group_predicate_not_eq() -> Result<()> {
        let schema = Schema::new(vec![Field::new("c1", DataType::Int32, false)]);
        let expected_expr = "c1_min@0 != 1 OR 1 != c1_max@1";

        // test column on the left
        let expr = col("c1").not_eq(lit(1));
        let predicate_expr = test_build_predicate_expression(
            &expr,
            &schema,
            &mut RequiredStatColumns::new(),
        );
        assert_eq!(predicate_expr.to_string(), expected_expr);

        // test column on the right
        let expr = lit(1).not_eq(col("c1"));
        let predicate_expr = test_build_predicate_expression(
            &expr,
            &schema,
            &mut RequiredStatColumns::new(),
        );
        assert_eq!(predicate_expr.to_string(), expected_expr);

        Ok(())
    }

    #[test]
    fn row_group_predicate_gt() -> Result<()> {
        let schema = Schema::new(vec![Field::new("c1", DataType::Int32, false)]);
        let expected_expr = "c1_max@0 > 1";

        // test column on the left
        let expr = col("c1").gt(lit(1));
        let predicate_expr = test_build_predicate_expression(
            &expr,
            &schema,
            &mut RequiredStatColumns::new(),
        );
        assert_eq!(predicate_expr.to_string(), expected_expr);

        // test column on the right
        let expr = lit(1).lt(col("c1"));
        let predicate_expr = test_build_predicate_expression(
            &expr,
            &schema,
            &mut RequiredStatColumns::new(),
        );
        assert_eq!(predicate_expr.to_string(), expected_expr);

        Ok(())
    }

    #[test]
    fn row_group_predicate_gt_eq() -> Result<()> {
        let schema = Schema::new(vec![Field::new("c1", DataType::Int32, false)]);
        let expected_expr = "c1_max@0 >= 1";

        // test column on the left
        let expr = col("c1").gt_eq(lit(1));
        let predicate_expr = test_build_predicate_expression(
            &expr,
            &schema,
            &mut RequiredStatColumns::new(),
        );
        assert_eq!(predicate_expr.to_string(), expected_expr);
        // test column on the right
        let expr = lit(1).lt_eq(col("c1"));
        let predicate_expr = test_build_predicate_expression(
            &expr,
            &schema,
            &mut RequiredStatColumns::new(),
        );
        assert_eq!(predicate_expr.to_string(), expected_expr);

        Ok(())
    }

    #[test]
    fn row_group_predicate_lt() -> Result<()> {
        let schema = Schema::new(vec![Field::new("c1", DataType::Int32, false)]);
        let expected_expr = "c1_min@0 < 1";

        // test column on the left
        let expr = col("c1").lt(lit(1));
        let predicate_expr = test_build_predicate_expression(
            &expr,
            &schema,
            &mut RequiredStatColumns::new(),
        );
        assert_eq!(predicate_expr.to_string(), expected_expr);

        // test column on the right
        let expr = lit(1).gt(col("c1"));
        let predicate_expr = test_build_predicate_expression(
            &expr,
            &schema,
            &mut RequiredStatColumns::new(),
        );
        assert_eq!(predicate_expr.to_string(), expected_expr);

        Ok(())
    }

    #[test]
    fn row_group_predicate_lt_eq() -> Result<()> {
        let schema = Schema::new(vec![Field::new("c1", DataType::Int32, false)]);
        let expected_expr = "c1_min@0 <= 1";

        // test column on the left
        let expr = col("c1").lt_eq(lit(1));
        let predicate_expr = test_build_predicate_expression(
            &expr,
            &schema,
            &mut RequiredStatColumns::new(),
        );
        assert_eq!(predicate_expr.to_string(), expected_expr);
        // test column on the right
        let expr = lit(1).gt_eq(col("c1"));
        let predicate_expr = test_build_predicate_expression(
            &expr,
            &schema,
            &mut RequiredStatColumns::new(),
        );
        assert_eq!(predicate_expr.to_string(), expected_expr);

        Ok(())
    }

    #[test]
    fn row_group_predicate_and() -> Result<()> {
        let schema = Schema::new(vec![
            Field::new("c1", DataType::Int32, false),
            Field::new("c2", DataType::Int32, false),
            Field::new("c3", DataType::Int32, false),
        ]);
        // test AND operator joining supported c1 < 1 expression and unsupported c2 > c3 expression
        let expr = col("c1").lt(lit(1)).and(col("c2").lt(col("c3")));
        let expected_expr = "c1_min@0 < 1";
        let predicate_expr = test_build_predicate_expression(
            &expr,
            &schema,
            &mut RequiredStatColumns::new(),
        );
        assert_eq!(predicate_expr.to_string(), expected_expr);

        Ok(())
    }

    #[test]
    fn row_group_predicate_or() -> Result<()> {
        let schema = Schema::new(vec![
            Field::new("c1", DataType::Int32, false),
            Field::new("c2", DataType::Int32, false),
        ]);
        // test OR operator joining supported c1 < 1 expression and unsupported c2 % 2 = 0 expression
        let expr = col("c1").lt(lit(1)).or(col("c2").rem(lit(2)).eq(lit(0)));
        let expected_expr = "true";
        let predicate_expr = test_build_predicate_expression(
            &expr,
            &schema,
            &mut RequiredStatColumns::new(),
        );
        assert_eq!(predicate_expr.to_string(), expected_expr);

        Ok(())
    }

    #[test]
    fn row_group_predicate_not() -> Result<()> {
        let schema = Schema::new(vec![Field::new("c1", DataType::Int32, false)]);
        let expected_expr = "true";

        let expr = col("c1").not();
        let predicate_expr = test_build_predicate_expression(
            &expr,
            &schema,
            &mut RequiredStatColumns::new(),
        );
        assert_eq!(predicate_expr.to_string(), expected_expr);

        Ok(())
    }

    #[test]
    fn row_group_predicate_not_bool() -> Result<()> {
        let schema = Schema::new(vec![Field::new("c1", DataType::Boolean, false)]);
        let expected_expr = "NOT c1_min@0 AND c1_max@1";

        let expr = col("c1").not();
        let predicate_expr = test_build_predicate_expression(
            &expr,
            &schema,
            &mut RequiredStatColumns::new(),
        );
        assert_eq!(predicate_expr.to_string(), expected_expr);

        Ok(())
    }

    #[test]
    fn row_group_predicate_bool() -> Result<()> {
        let schema = Schema::new(vec![Field::new("c1", DataType::Boolean, false)]);
        let expected_expr = "c1_min@0 OR c1_max@1";

        let expr = col("c1");
        let predicate_expr = test_build_predicate_expression(
            &expr,
            &schema,
            &mut RequiredStatColumns::new(),
        );
        assert_eq!(predicate_expr.to_string(), expected_expr);

        Ok(())
    }

    #[test]
    fn row_group_predicate_lt_bool() -> Result<()> {
        let schema = Schema::new(vec![Field::new("c1", DataType::Boolean, false)]);
        let expected_expr = "c1_min@0 < true";

        // DF doesn't support arithmetic on boolean columns so
        // this predicate will error when evaluated
        let expr = col("c1").lt(lit(true));
        let predicate_expr = test_build_predicate_expression(
            &expr,
            &schema,
            &mut RequiredStatColumns::new(),
        );
        assert_eq!(predicate_expr.to_string(), expected_expr);

        Ok(())
    }

    #[test]
    fn row_group_predicate_required_columns() -> Result<()> {
        let schema = Schema::new(vec![
            Field::new("c1", DataType::Int32, false),
            Field::new("c2", DataType::Int32, false),
        ]);
        let mut required_columns = RequiredStatColumns::new();
        // c1 < 1 and (c2 = 2 or c2 = 3)
        let expr = col("c1")
            .lt(lit(1))
            .and(col("c2").eq(lit(2)).or(col("c2").eq(lit(3))));
        let expected_expr = "c1_min@0 < 1 AND (c2_min@1 <= 2 AND 2 <= c2_max@2 OR c2_min@1 <= 3 AND 3 <= c2_max@2)";
        let predicate_expr =
            test_build_predicate_expression(&expr, &schema, &mut required_columns);
        assert_eq!(predicate_expr.to_string(), expected_expr);
        // c1 < 1 should add c1_min
        let c1_min_field = Field::new("c1_min", DataType::Int32, false);
        assert_eq!(
            required_columns.columns[0],
            (
                phys_expr::Column::new("c1", 0),
                StatisticsType::Min,
                c1_min_field
            )
        );
        // c2 = 2 should add c2_min and c2_max
        let c2_min_field = Field::new("c2_min", DataType::Int32, false);
        assert_eq!(
            required_columns.columns[1],
            (
                phys_expr::Column::new("c2", 1),
                StatisticsType::Min,
                c2_min_field
            )
        );
        let c2_max_field = Field::new("c2_max", DataType::Int32, false);
        assert_eq!(
            required_columns.columns[2],
            (
                phys_expr::Column::new("c2", 1),
                StatisticsType::Max,
                c2_max_field
            )
        );
        // c2 = 3 shouldn't add any new statistics fields
        assert_eq!(required_columns.columns.len(), 3);

        Ok(())
    }

    #[test]
    fn row_group_predicate_in_list() -> Result<()> {
        let schema = Schema::new(vec![
            Field::new("c1", DataType::Int32, false),
            Field::new("c2", DataType::Int32, false),
        ]);
        // test c1 in(1, 2, 3)
        let expr = Expr::InList(InList::new(
            Box::new(col("c1")),
            vec![lit(1), lit(2), lit(3)],
            false,
        ));
        let expected_expr = "c1_min@0 <= 1 AND 1 <= c1_max@1 OR c1_min@0 <= 2 AND 2 <= c1_max@1 OR c1_min@0 <= 3 AND 3 <= c1_max@1";
        let predicate_expr = test_build_predicate_expression(
            &expr,
            &schema,
            &mut RequiredStatColumns::new(),
        );
        assert_eq!(predicate_expr.to_string(), expected_expr);

        Ok(())
    }

    #[test]
    fn row_group_predicate_in_list_empty() -> Result<()> {
        let schema = Schema::new(vec![
            Field::new("c1", DataType::Int32, false),
            Field::new("c2", DataType::Int32, false),
        ]);
        // test c1 in()
        let expr = Expr::InList(InList::new(Box::new(col("c1")), vec![], false));
        let expected_expr = "true";
        let predicate_expr = test_build_predicate_expression(
            &expr,
            &schema,
            &mut RequiredStatColumns::new(),
        );
        assert_eq!(predicate_expr.to_string(), expected_expr);

        Ok(())
    }

    #[test]
    fn row_group_predicate_in_list_negated() -> Result<()> {
        let schema = Schema::new(vec![
            Field::new("c1", DataType::Int32, false),
            Field::new("c2", DataType::Int32, false),
        ]);
        // test c1 not in(1, 2, 3)
        let expr = Expr::InList(InList::new(
            Box::new(col("c1")),
            vec![lit(1), lit(2), lit(3)],
            true,
        ));
        let expected_expr = "(c1_min@0 != 1 OR 1 != c1_max@1) \
        AND (c1_min@0 != 2 OR 2 != c1_max@1) \
        AND (c1_min@0 != 3 OR 3 != c1_max@1)";
        let predicate_expr = test_build_predicate_expression(
            &expr,
            &schema,
            &mut RequiredStatColumns::new(),
        );
        assert_eq!(predicate_expr.to_string(), expected_expr);

        Ok(())
    }

    #[test]
    fn row_group_predicate_cast() -> Result<()> {
        let schema = Schema::new(vec![Field::new("c1", DataType::Int32, false)]);
        let expected_expr =
            "CAST(c1_min@0 AS Int64) <= 1 AND 1 <= CAST(c1_max@1 AS Int64)";

        // test column on the left
        let expr = cast(col("c1"), DataType::Int64).eq(lit(ScalarValue::Int64(Some(1))));
        let predicate_expr = test_build_predicate_expression(
            &expr,
            &schema,
            &mut RequiredStatColumns::new(),
        );
        assert_eq!(predicate_expr.to_string(), expected_expr);

        // test column on the right
        let expr = lit(ScalarValue::Int64(Some(1))).eq(cast(col("c1"), DataType::Int64));
        let predicate_expr = test_build_predicate_expression(
            &expr,
            &schema,
            &mut RequiredStatColumns::new(),
        );
        assert_eq!(predicate_expr.to_string(), expected_expr);

        let expected_expr = "TRY_CAST(c1_max@0 AS Int64) > 1";

        // test column on the left
        let expr =
            try_cast(col("c1"), DataType::Int64).gt(lit(ScalarValue::Int64(Some(1))));
        let predicate_expr = test_build_predicate_expression(
            &expr,
            &schema,
            &mut RequiredStatColumns::new(),
        );
        assert_eq!(predicate_expr.to_string(), expected_expr);

        // test column on the right
        let expr =
            lit(ScalarValue::Int64(Some(1))).lt(try_cast(col("c1"), DataType::Int64));
        let predicate_expr = test_build_predicate_expression(
            &expr,
            &schema,
            &mut RequiredStatColumns::new(),
        );
        assert_eq!(predicate_expr.to_string(), expected_expr);

        Ok(())
    }

    #[test]
    fn row_group_predicate_cast_list() -> Result<()> {
        let schema = Schema::new(vec![Field::new("c1", DataType::Int32, false)]);
        // test cast(c1 as int64) in int64(1, 2, 3)
        let expr = Expr::InList(InList::new(
            Box::new(cast(col("c1"), DataType::Int64)),
            vec![
                lit(ScalarValue::Int64(Some(1))),
                lit(ScalarValue::Int64(Some(2))),
                lit(ScalarValue::Int64(Some(3))),
            ],
            false,
        ));
        let expected_expr = "CAST(c1_min@0 AS Int64) <= 1 AND 1 <= CAST(c1_max@1 AS Int64) OR CAST(c1_min@0 AS Int64) <= 2 AND 2 <= CAST(c1_max@1 AS Int64) OR CAST(c1_min@0 AS Int64) <= 3 AND 3 <= CAST(c1_max@1 AS Int64)";
        let predicate_expr = test_build_predicate_expression(
            &expr,
            &schema,
            &mut RequiredStatColumns::new(),
        );
        assert_eq!(predicate_expr.to_string(), expected_expr);

        let expr = Expr::InList(InList::new(
            Box::new(cast(col("c1"), DataType::Int64)),
            vec![
                lit(ScalarValue::Int64(Some(1))),
                lit(ScalarValue::Int64(Some(2))),
                lit(ScalarValue::Int64(Some(3))),
            ],
            true,
        ));
        let expected_expr =
            "(CAST(c1_min@0 AS Int64) != 1 OR 1 != CAST(c1_max@1 AS Int64)) \
        AND (CAST(c1_min@0 AS Int64) != 2 OR 2 != CAST(c1_max@1 AS Int64)) \
        AND (CAST(c1_min@0 AS Int64) != 3 OR 3 != CAST(c1_max@1 AS Int64))";
        let predicate_expr = test_build_predicate_expression(
            &expr,
            &schema,
            &mut RequiredStatColumns::new(),
        );
        assert_eq!(predicate_expr.to_string(), expected_expr);

        Ok(())
    }

    #[test]
    fn prune_decimal_data() {
        // decimal(9,2)
        let schema = Arc::new(Schema::new(vec![Field::new(
            "s1",
            DataType::Decimal128(9, 2),
            true,
        )]));
        // s1 > 5
        let expr = col("s1").gt(lit(ScalarValue::Decimal128(Some(500), 9, 2)));
        let expr = logical2physical(&expr, &schema);
        // If the data is written by spark, the physical data type is INT32 in the parquet
        // So we use the INT32 type of statistic.
        let statistics = TestStatistics::new().with(
            "s1",
            ContainerStats::new_i32(
                vec![Some(0), Some(4), None, Some(3)], // min
                vec![Some(5), Some(6), Some(4), None], // max
            ),
        );
        let p = PruningPredicate::try_new(expr, schema.clone()).unwrap();
        let result = p.prune(&statistics).unwrap();
        let expected = vec![false, true, false, true];
        assert_eq!(result, expected);

        // with cast column to other type
        let expr = cast(col("s1"), DataType::Decimal128(14, 3))
            .gt(lit(ScalarValue::Decimal128(Some(5000), 14, 3)));
        let expr = logical2physical(&expr, &schema);
        let statistics = TestStatistics::new().with(
            "s1",
            ContainerStats::new_i32(
                vec![Some(0), Some(4), None, Some(3)], // min
                vec![Some(5), Some(6), Some(4), None], // max
            ),
        );
        let p = PruningPredicate::try_new(expr, schema.clone()).unwrap();
        let result = p.prune(&statistics).unwrap();
        let expected = vec![false, true, false, true];
        assert_eq!(result, expected);

        // with try cast column to other type
        let expr = try_cast(col("s1"), DataType::Decimal128(14, 3))
            .gt(lit(ScalarValue::Decimal128(Some(5000), 14, 3)));
        let expr = logical2physical(&expr, &schema);
        let statistics = TestStatistics::new().with(
            "s1",
            ContainerStats::new_i32(
                vec![Some(0), Some(4), None, Some(3)], // min
                vec![Some(5), Some(6), Some(4), None], // max
            ),
        );
        let p = PruningPredicate::try_new(expr, schema).unwrap();
        let result = p.prune(&statistics).unwrap();
        let expected = vec![false, true, false, true];
        assert_eq!(result, expected);

        // decimal(18,2)
        let schema = Arc::new(Schema::new(vec![Field::new(
            "s1",
            DataType::Decimal128(18, 2),
            true,
        )]));
        // s1 > 5
        let expr = col("s1").gt(lit(ScalarValue::Decimal128(Some(500), 18, 2)));
        let expr = logical2physical(&expr, &schema);
        // If the data is written by spark, the physical data type is INT64 in the parquet
        // So we use the INT32 type of statistic.
        let statistics = TestStatistics::new().with(
            "s1",
            ContainerStats::new_i64(
                vec![Some(0), Some(4), None, Some(3)], // min
                vec![Some(5), Some(6), Some(4), None], // max
            ),
        );
        let p = PruningPredicate::try_new(expr, schema).unwrap();
        let result = p.prune(&statistics).unwrap();
        let expected = vec![false, true, false, true];
        assert_eq!(result, expected);

        // decimal(23,2)
        let schema = Arc::new(Schema::new(vec![Field::new(
            "s1",
            DataType::Decimal128(23, 2),
            true,
        )]));
        // s1 > 5
        let expr = col("s1").gt(lit(ScalarValue::Decimal128(Some(500), 23, 2)));
        let expr = logical2physical(&expr, &schema);
        let statistics = TestStatistics::new().with(
            "s1",
            ContainerStats::new_decimal128(
                vec![Some(0), Some(400), None, Some(300)], // min
                vec![Some(500), Some(600), Some(400), None], // max
                23,
                2,
            ),
        );
        let p = PruningPredicate::try_new(expr, schema).unwrap();
        let result = p.prune(&statistics).unwrap();
        let expected = vec![false, true, false, true];
        assert_eq!(result, expected);
    }

    #[test]
    fn prune_api() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("s1", DataType::Utf8, true),
            Field::new("s2", DataType::Int32, true),
        ]));

        // Prune using s2 > 5
        let expr = col("s2").gt(lit(5));
        let expr = logical2physical(&expr, &schema);

        let statistics = TestStatistics::new().with(
            "s2",
            ContainerStats::new_i32(
                vec![Some(0), Some(4), None, Some(3)], // min
                vec![Some(5), Some(6), None, None],    // max
            ),
        );

        // s2 [0, 5] ==> no rows should pass
        // s2 [4, 6] ==> some rows could pass
        // No stats for s2 ==> some rows could pass
        // s2 [3, None] (null max) ==> some rows could pass

        let p = PruningPredicate::try_new(expr, schema.clone()).unwrap();
        let result = p.prune(&statistics).unwrap();
        let expected = vec![false, true, true, true];
        assert_eq!(result, expected);

        // filter with cast
        let expr = cast(col("s2"), DataType::Int64).gt(lit(ScalarValue::Int64(Some(5))));
        let expr = logical2physical(&expr, &schema);
        let p = PruningPredicate::try_new(expr, schema).unwrap();
        let result = p.prune(&statistics).unwrap();
        let expected = vec![false, true, true, true];
        assert_eq!(result, expected);
    }

    #[test]
    fn prune_not_eq_data() {
        let schema = Arc::new(Schema::new(vec![Field::new("s1", DataType::Utf8, true)]));

        // Prune using s2 != 'M'
        let expr = col("s1").not_eq(lit("M"));
        let expr = logical2physical(&expr, &schema);

        let statistics = TestStatistics::new().with(
            "s1",
            ContainerStats::new_utf8(
                vec![Some("A"), Some("A"), Some("N"), Some("M"), None, Some("A")], // min
                vec![Some("Z"), Some("L"), Some("Z"), Some("M"), None, None],      // max
            ),
        );

        // s1 [A, Z] ==> might have values that pass predicate
        // s1 [A, L] ==> all rows pass the predicate
        // s1 [N, Z] ==> all rows pass the predicate
        // s1 [M, M] ==> all rows do not pass the predicate
        // No stats for s2 ==> some rows could pass
        // s2 [3, None] (null max) ==> some rows could pass

        let p = PruningPredicate::try_new(expr, schema).unwrap();
        let result = p.prune(&statistics).unwrap();
        let expected = vec![true, true, true, false, true, true];
        assert_eq!(result, expected);
    }

    /// Creates setup for boolean chunk pruning
    ///
    /// For predicate "b1" (boolean expr)
    /// b1 [false, false] ==> no rows can pass (not keep)
    /// b1 [false, true] ==> some rows could pass (must keep)
    /// b1 [true, true] ==> all rows must pass (must keep)
    /// b1 [NULL, NULL]  ==> unknown (must keep)
    /// b1 [false, NULL]  ==> unknown (must keep)
    ///
    /// For predicate "!b1" (boolean expr)
    /// b1 [false, false] ==> all rows pass (must keep)
    /// b1 [false, true] ==> some rows could pass (must keep)
    /// b1 [true, true] ==> no rows can pass (not keep)
    /// b1 [NULL, NULL]  ==> unknown (must keep)
    /// b1 [false, NULL]  ==> unknown (must keep)
    fn bool_setup() -> (SchemaRef, TestStatistics, Vec<bool>, Vec<bool>) {
        let schema =
            Arc::new(Schema::new(vec![Field::new("b1", DataType::Boolean, true)]));

        let statistics = TestStatistics::new().with(
            "b1",
            ContainerStats::new_bool(
                vec![Some(false), Some(false), Some(true), None, Some(false)], // min
                vec![Some(false), Some(true), Some(true), None, None],         // max
            ),
        );
        let expected_true = vec![false, true, true, true, true];
        let expected_false = vec![true, true, false, true, true];

        (schema, statistics, expected_true, expected_false)
    }

    #[test]
    fn prune_bool_const_expr() {
        let (schema, statistics, _, _) = bool_setup();

        // true
        let expr = lit(true);
        let expr = logical2physical(&expr, &schema);
        let p = PruningPredicate::try_new(expr, schema.clone()).unwrap();
        let result = p.prune(&statistics).unwrap();
        assert_eq!(result, vec![true, true, true, true, true]);

        // false
        // constant literals that do NOT refer to any columns are currently not evaluated at all, hence the result is
        // "all true"
        let expr = lit(false);
        let expr = logical2physical(&expr, &schema);
        let p = PruningPredicate::try_new(expr, schema).unwrap();
        let result = p.prune(&statistics).unwrap();
        assert_eq!(result, vec![true, true, true, true, true]);
    }

    #[test]
    fn prune_bool_column() {
        let (schema, statistics, expected_true, _) = bool_setup();

        // b1
        let expr = col("b1");
        let expr = logical2physical(&expr, &schema);
        let p = PruningPredicate::try_new(expr, schema).unwrap();
        let result = p.prune(&statistics).unwrap();
        assert_eq!(result, expected_true);
    }

    #[test]
    fn prune_bool_not_column() {
        let (schema, statistics, _, expected_false) = bool_setup();

        // !b1
        let expr = col("b1").not();
        let expr = logical2physical(&expr, &schema);
        let p = PruningPredicate::try_new(expr, schema).unwrap();
        let result = p.prune(&statistics).unwrap();
        assert_eq!(result, expected_false);
    }

    #[test]
    fn prune_bool_column_eq_true() {
        let (schema, statistics, expected_true, _) = bool_setup();

        // b1 = true
        let expr = col("b1").eq(lit(true));
        let expr = logical2physical(&expr, &schema);
        let p = PruningPredicate::try_new(expr, schema).unwrap();
        let result = p.prune(&statistics).unwrap();
        assert_eq!(result, expected_true);
    }

    #[test]
    fn prune_bool_not_column_eq_true() {
        let (schema, statistics, _, expected_false) = bool_setup();

        // !b1 = true
        let expr = col("b1").not().eq(lit(true));
        let expr = logical2physical(&expr, &schema);
        let p = PruningPredicate::try_new(expr, schema).unwrap();
        let result = p.prune(&statistics).unwrap();
        assert_eq!(result, expected_false);
    }

    /// Creates a setup for chunk pruning, modeling a int32 column "i"
    /// with 5 different containers (e.g. RowGroups). They have [min,
    /// max]:
    ///
    /// i [-5, 5]
    /// i [1, 11]
    /// i [-11, -1]
    /// i [NULL, NULL]
    /// i [1, NULL]
    fn int32_setup() -> (SchemaRef, TestStatistics) {
        let schema = Arc::new(Schema::new(vec![Field::new("i", DataType::Int32, true)]));

        let statistics = TestStatistics::new().with(
            "i",
            ContainerStats::new_i32(
                vec![Some(-5), Some(1), Some(-11), None, Some(1)], // min
                vec![Some(5), Some(11), Some(-1), None, None],     // max
            ),
        );
        (schema, statistics)
    }

    #[test]
    fn prune_int32_col_gt_zero() {
        let (schema, statistics) = int32_setup();

        // Expression "i > 0" and "-i < 0"
        // i [-5, 5] ==> some rows could pass (must keep)
        // i [1, 11] ==> all rows must pass (must keep)
        // i [-11, -1] ==>  no rows can pass (not keep)
        // i [NULL, NULL]  ==> unknown (must keep)
        // i [1, NULL]  ==> unknown (must keep)
        let expected_ret = vec![true, true, false, true, true];

        // i > 0
        let expr = col("i").gt(lit(0));
        let expr = logical2physical(&expr, &schema);
        let p = PruningPredicate::try_new(expr, schema.clone()).unwrap();
        let result = p.prune(&statistics).unwrap();
        assert_eq!(result, expected_ret);

        // -i < 0
        let expr = Expr::Negative(Box::new(col("i"))).lt(lit(0));
        let expr = logical2physical(&expr, &schema);
        let p = PruningPredicate::try_new(expr, schema).unwrap();
        let result = p.prune(&statistics).unwrap();
        assert_eq!(result, expected_ret);
    }

    #[test]
    fn prune_int32_col_lte_zero() {
        let (schema, statistics) = int32_setup();

        // Expression "i <= 0" and "-i >= 0"
        // i [-5, 5] ==> some rows could pass (must keep)
        // i [1, 11] ==> no rows can pass (not keep)
        // i [-11, -1] ==>  all rows must pass (must keep)
        // i [NULL, NULL]  ==> unknown (must keep)
        // i [1, NULL]  ==> no rows can pass (not keep)
        let expected_ret = vec![true, false, true, true, false];

        // i <= 0
        let expr = col("i").lt_eq(lit(0));
        let expr = logical2physical(&expr, &schema);
        let p = PruningPredicate::try_new(expr, schema.clone()).unwrap();
        let result = p.prune(&statistics).unwrap();
        assert_eq!(result, expected_ret);

        // -i >= 0
        let expr = Expr::Negative(Box::new(col("i"))).gt_eq(lit(0));
        let expr = logical2physical(&expr, &schema);
        let p = PruningPredicate::try_new(expr, schema).unwrap();
        let result = p.prune(&statistics).unwrap();
        assert_eq!(result, expected_ret);
    }

    #[test]
    fn prune_int32_col_lte_zero_cast() {
        let (schema, statistics) = int32_setup();

        // Expression "cast(i as utf8) <= '0'"
        // i [-5, 5] ==> some rows could pass (must keep)
        // i [1, 11] ==> no rows can pass in theory, -0.22 (conservatively keep)
        // i [-11, -1] ==>  no rows could pass in theory (conservatively keep)
        // i [NULL, NULL]  ==> unknown (must keep)
        // i [1, NULL]  ==> no rows can pass (conservatively keep)
        let expected_ret = vec![true, true, true, true, true];

        // cast(i as utf8) <= 0
        let expr = cast(col("i"), DataType::Utf8).lt_eq(lit("0"));
        let expr = logical2physical(&expr, &schema);
        let p = PruningPredicate::try_new(expr, schema.clone()).unwrap();
        let result = p.prune(&statistics).unwrap();
        assert_eq!(result, expected_ret);

        // try_cast(i as utf8) <= 0
        let expr = try_cast(col("i"), DataType::Utf8).lt_eq(lit("0"));
        let expr = logical2physical(&expr, &schema);
        let p = PruningPredicate::try_new(expr, schema.clone()).unwrap();
        let result = p.prune(&statistics).unwrap();
        assert_eq!(result, expected_ret);

        // cast(-i as utf8) >= 0
        let expr =
            cast(Expr::Negative(Box::new(col("i"))), DataType::Utf8).gt_eq(lit("0"));
        let expr = logical2physical(&expr, &schema);
        let p = PruningPredicate::try_new(expr, schema.clone()).unwrap();
        let result = p.prune(&statistics).unwrap();
        assert_eq!(result, expected_ret);

        // try_cast(-i as utf8) >= 0
        let expr =
            try_cast(Expr::Negative(Box::new(col("i"))), DataType::Utf8).gt_eq(lit("0"));
        let expr = logical2physical(&expr, &schema);
        let p = PruningPredicate::try_new(expr, schema).unwrap();
        let result = p.prune(&statistics).unwrap();
        assert_eq!(result, expected_ret);
    }

    #[test]
    fn prune_int32_col_eq_zero() {
        let (schema, statistics) = int32_setup();

        // Expression "i = 0"
        // i [-5, 5] ==> some rows could pass (must keep)
        // i [1, 11] ==> no rows can pass (not keep)
        // i [-11, -1] ==>  no rows can pass (not keep)
        // i [NULL, NULL]  ==> unknown (must keep)
        // i [1, NULL]  ==> no rows can pass (not keep)
        let expected_ret = vec![true, false, false, true, false];

        // i = 0
        let expr = col("i").eq(lit(0));
        let expr = logical2physical(&expr, &schema);
        let p = PruningPredicate::try_new(expr, schema).unwrap();
        let result = p.prune(&statistics).unwrap();
        assert_eq!(result, expected_ret);
    }

    #[test]
    fn prune_int32_col_eq_zero_cast() {
        let (schema, statistics) = int32_setup();

        // Expression "cast(i as int64) = 0"
        // i [-5, 5] ==> some rows could pass (must keep)
        // i [1, 11] ==> no rows can pass (not keep)
        // i [-11, -1] ==>  no rows can pass (not keep)
        // i [NULL, NULL]  ==> unknown (must keep)
        // i [1, NULL]  ==> no rows can pass (not keep)
        let expected_ret = vec![true, false, false, true, false];

        let expr = cast(col("i"), DataType::Int64).eq(lit(0i64));
        let expr = logical2physical(&expr, &schema);
        let p = PruningPredicate::try_new(expr, schema.clone()).unwrap();
        let result = p.prune(&statistics).unwrap();
        assert_eq!(result, expected_ret);

        let expr = try_cast(col("i"), DataType::Int64).eq(lit(0i64));
        let expr = logical2physical(&expr, &schema);
        let p = PruningPredicate::try_new(expr, schema).unwrap();
        let result = p.prune(&statistics).unwrap();
        assert_eq!(result, expected_ret);
    }

    #[test]
    fn prune_int32_col_eq_zero_cast_as_str() {
        let (schema, statistics) = int32_setup();

        // Note the cast is to a string where sorting properties are
        // not the same as integers
        //
        // Expression "cast(i as utf8) = '0'"
        // i [-5, 5] ==> some rows could pass (keep)
        // i [1, 11] ==> no rows can pass  (could keep)
        // i [-11, -1] ==>  no rows can pass (could keep)
        // i [NULL, NULL]  ==> unknown (keep)
        // i [1, NULL]  ==> no rows can pass (could keep)
        let expected_ret = vec![true, true, true, true, true];

        let expr = cast(col("i"), DataType::Utf8).eq(lit("0"));
        let expr = logical2physical(&expr, &schema);
        let p = PruningPredicate::try_new(expr, schema).unwrap();
        let result = p.prune(&statistics).unwrap();
        assert_eq!(result, expected_ret);
    }

    #[test]
    fn prune_int32_col_lt_neg_one() {
        let (schema, statistics) = int32_setup();

        // Expression "i > -1" and "-i < 1"
        // i [-5, 5] ==> some rows could pass (must keep)
        // i [1, 11] ==> all rows must pass (must keep)
        // i [-11, -1] ==>  no rows can pass (not keep)
        // i [NULL, NULL]  ==> unknown (must keep)
        // i [1, NULL]  ==> all rows must pass (must keep)
        let expected_ret = vec![true, true, false, true, true];

        // i > -1
        let expr = col("i").gt(lit(-1));
        let expr = logical2physical(&expr, &schema);
        let p = PruningPredicate::try_new(expr, schema.clone()).unwrap();
        let result = p.prune(&statistics).unwrap();
        assert_eq!(result, expected_ret);

        // -i < 1
        let expr = Expr::Negative(Box::new(col("i"))).lt(lit(1));
        let expr = logical2physical(&expr, &schema);
        let p = PruningPredicate::try_new(expr, schema).unwrap();
        let result = p.prune(&statistics).unwrap();
        assert_eq!(result, expected_ret);
    }

    #[test]
    fn prune_int32_is_null() {
        let (schema, statistics) = int32_setup();

        // Expression "i IS NULL" when there are no null statistics,
        // should all be kept
        let expected_ret = vec![true, true, true, true, true];

        // i IS NULL, no null statistics
        let expr = col("i").is_null();
        let expr = logical2physical(&expr, &schema);
        let p = PruningPredicate::try_new(expr, schema.clone()).unwrap();
        let result = p.prune(&statistics).unwrap();
        assert_eq!(result, expected_ret);

        // provide null counts for each column
        let statistics = statistics.with_null_counts(
            "i",
            vec![
                Some(0), // no nulls (don't keep)
                Some(1), // 1 null
                None,    // unknown nulls
                None, // unknown nulls (min/max are both null too, like no stats at all)
                Some(0), // 0 nulls (max=null too which means no known max) (don't keep)
            ],
        );

        let expected_ret = vec![false, true, true, true, false];

        // i IS NULL, with actual null statistcs
        let expr = col("i").is_null();
        let expr = logical2physical(&expr, &schema);
        let p = PruningPredicate::try_new(expr, schema).unwrap();
        let result = p.prune(&statistics).unwrap();
        assert_eq!(result, expected_ret);
    }

    #[test]
    fn prune_cast_column_scalar() {
        // The data type of column i is INT32
        let (schema, statistics) = int32_setup();
        let expected_ret = vec![true, true, false, true, true];

        // i > int64(0)
        let expr = col("i").gt(cast(lit(ScalarValue::Int64(Some(0))), DataType::Int32));
        let expr = logical2physical(&expr, &schema);
        let p = PruningPredicate::try_new(expr, schema.clone()).unwrap();
        let result = p.prune(&statistics).unwrap();
        assert_eq!(result, expected_ret);

        // cast(i as int64) > int64(0)
        let expr = cast(col("i"), DataType::Int64).gt(lit(ScalarValue::Int64(Some(0))));
        let expr = logical2physical(&expr, &schema);
        let p = PruningPredicate::try_new(expr, schema.clone()).unwrap();
        let result = p.prune(&statistics).unwrap();
        assert_eq!(result, expected_ret);

        // try_cast(i as int64) > int64(0)
        let expr =
            try_cast(col("i"), DataType::Int64).gt(lit(ScalarValue::Int64(Some(0))));
        let expr = logical2physical(&expr, &schema);
        let p = PruningPredicate::try_new(expr, schema.clone()).unwrap();
        let result = p.prune(&statistics).unwrap();
        assert_eq!(result, expected_ret);

        // `-cast(i as int64) < 0` convert to `cast(i as int64) > -0`
        let expr = Expr::Negative(Box::new(cast(col("i"), DataType::Int64)))
            .lt(lit(ScalarValue::Int64(Some(0))));
        let expr = logical2physical(&expr, &schema);
        let p = PruningPredicate::try_new(expr, schema).unwrap();
        let result = p.prune(&statistics).unwrap();
        assert_eq!(result, expected_ret);
    }

    #[test]
    fn test_rewrite_expr_to_prunable() {
        let schema = Schema::new(vec![Field::new("a", DataType::Int32, true)]);
        let df_schema = DFSchema::try_from(schema.clone()).unwrap();

        // column op lit
        let left_input = col("a");
        let left_input = logical2physical(&left_input, &schema);
        let right_input = lit(ScalarValue::Int32(Some(12)));
        let right_input = logical2physical(&right_input, &schema);
        let (result_left, _, result_right) = rewrite_expr_to_prunable(
            &left_input,
            Operator::Eq,
            &right_input,
            df_schema.clone(),
        )
        .unwrap();
        assert_eq!(result_left.to_string(), left_input.to_string());
        assert_eq!(result_right.to_string(), right_input.to_string());

        // cast op lit
        let left_input = cast(col("a"), DataType::Decimal128(20, 3));
        let left_input = logical2physical(&left_input, &schema);
        let right_input = lit(ScalarValue::Decimal128(Some(12), 20, 3));
        let right_input = logical2physical(&right_input, &schema);
        let (result_left, _, result_right) = rewrite_expr_to_prunable(
            &left_input,
            Operator::Gt,
            &right_input,
            df_schema.clone(),
        )
        .unwrap();
        assert_eq!(result_left.to_string(), left_input.to_string());
        assert_eq!(result_right.to_string(), right_input.to_string());

        // try_cast op lit
        let left_input = try_cast(col("a"), DataType::Int64);
        let left_input = logical2physical(&left_input, &schema);
        let right_input = lit(ScalarValue::Int64(Some(12)));
        let right_input = logical2physical(&right_input, &schema);
        let (result_left, _, result_right) =
            rewrite_expr_to_prunable(&left_input, Operator::Gt, &right_input, df_schema)
                .unwrap();
        assert_eq!(result_left.to_string(), left_input.to_string());
        assert_eq!(result_right.to_string(), right_input.to_string());

        // TODO: add test for other case and op
    }

    #[test]
    fn test_rewrite_expr_to_prunable_error() {
        // cast string value to numeric value
        // this cast is not supported
        let schema = Schema::new(vec![Field::new("a", DataType::Utf8, true)]);
        let df_schema = DFSchema::try_from(schema.clone()).unwrap();
        let left_input = cast(col("a"), DataType::Int64);
        let left_input = logical2physical(&left_input, &schema);
        let right_input = lit(ScalarValue::Int64(Some(12)));
        let right_input = logical2physical(&right_input, &schema);
        let result = rewrite_expr_to_prunable(
            &left_input,
            Operator::Gt,
            &right_input,
            df_schema.clone(),
        );
        assert!(result.is_err());

        // other expr
        let left_input = is_null(col("a"));
        let left_input = logical2physical(&left_input, &schema);
        let right_input = lit(ScalarValue::Int64(Some(12)));
        let right_input = logical2physical(&right_input, &schema);
        let result =
            rewrite_expr_to_prunable(&left_input, Operator::Gt, &right_input, df_schema);
        assert!(result.is_err());
        // TODO: add other negative test for other case and op
    }

    fn test_build_predicate_expression(
        expr: &Expr,
        schema: &Schema,
        required_columns: &mut RequiredStatColumns,
    ) -> Arc<dyn PhysicalExpr> {
        let expr = logical2physical(expr, schema);
        build_predicate_expression(&expr, schema, required_columns)
    }

    fn logical2physical(expr: &Expr, schema: &Schema) -> Arc<dyn PhysicalExpr> {
        let df_schema = schema.clone().to_dfschema().unwrap();
        let execution_props = ExecutionProps::new();
        create_physical_expr(expr, &df_schema, schema, &execution_props).unwrap()
    }
}
