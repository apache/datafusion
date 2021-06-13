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
//! For example, it is used to prune (skip) row groups while reading
//! parquet files if it can be determined from the predicate that
//! nothing in the row group can match.
//!
//! This code is currently specific to Parquet, but soon (TM), via
//! https://github.com/apache/arrow-datafusion/issues/363 it will
//! be genericized.

use std::{collections::HashSet, sync::Arc};

use arrow::{
    array::{new_null_array, ArrayRef, BooleanArray},
    datatypes::{DataType, Field, Schema, SchemaRef},
    record_batch::RecordBatch,
};

use crate::{
    error::{DataFusionError, Result},
    execution::context::ExecutionContextState,
    logical_plan::{Expr, Operator},
    optimizer::utils,
    physical_plan::{planner::DefaultPhysicalPlanner, ColumnarValue, PhysicalExpr},
};

/// Interface to pass statistics information to [`PruningPredicates`]
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
/// max_values("a") -> Some([20, Null, 30])
/// min_values("X") -> None
/// ```
pub trait PruningStatistics {
    /// return the minimum values for the named column, if known.
    /// Note: the returned array must contain `num_containers()` rows
    fn min_values(&self, column: &str) -> Option<ArrayRef>;

    /// return the maximum values for the named column, if known.
    /// Note: the returned array must contain `num_containers()` rows.
    fn max_values(&self, column: &str) -> Option<ArrayRef>;

    /// return the number of containers (e.g. row groups) being
    /// pruned with these statistics
    fn num_containers(&self) -> usize;
}

/// Evaluates filter expressions on statistics in order to
/// prune data containers (e.g. parquet row group)
///
/// See [`try_new`] for more information.
#[derive(Debug, Clone)]
pub struct PruningPredicate {
    /// The input schema against which the predicate will be evaluated
    schema: SchemaRef,
    /// Actual pruning predicate (rewritten in terms of column min/max statistics)
    predicate_expr: Arc<dyn PhysicalExpr>,
    /// The statistics required to evaluate this predicate
    required_columns: RequiredStatColumns,
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
    /// one row whose vaules fell within the min/max ranges (in other
    /// words they might pass the predicate)
    ///
    /// For example, the filter expression `(column / 2) = 4` becomes
    /// the pruning predicate
    /// `(column_min / 2) <= 4 && 4 <= (column_max / 2))`
    pub fn try_new(expr: &Expr, schema: SchemaRef) -> Result<Self> {
        // build predicate expression once
        let mut required_columns = RequiredStatColumns::new();
        let logical_predicate_expr =
            build_predicate_expression(expr, schema.as_ref(), &mut required_columns)?;
        let stat_fields = required_columns
            .iter()
            .map(|(_, _, f)| f.clone())
            .collect::<Vec<_>>();
        let stat_schema = Schema::new(stat_fields);
        let execution_context_state = ExecutionContextState::new();
        let predicate_expr = DefaultPhysicalPlanner::default().create_physical_expr(
            &logical_predicate_expr,
            &stat_schema,
            &execution_context_state,
        )?;
        Ok(Self {
            schema,
            predicate_expr,
            required_columns,
        })
    }

    /// For each set of statistics, evalates the pruning predicate
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
        // build statistics record batch
        let predicate_array =
            build_statistics_record_batch(statistics, &self.required_columns)
                .and_then(|statistics_batch| {
                    // execute predicate expression
                    self.predicate_expr.evaluate(&statistics_batch)
                })
                .and_then(|v| match v {
                    ColumnarValue::Array(array) => Ok(array),
                    ColumnarValue::Scalar(_) => Err(DataFusionError::Internal(
                        "predicate expression didn't return an array".to_string(),
                    )),
                })?;

        let predicate_array = predicate_array
            .as_any()
            .downcast_ref::<BooleanArray>()
            .ok_or_else(|| {
                DataFusionError::Internal(format!(
                    "Expected pruning predicate evaluation to be BooleanArray, \
                     but was {:?}",
                    predicate_array
                ))
            })?;

        // when the result of the predicate expression for a row group is null / undefined,
        // e.g. due to missing statistics, this row group can't be filtered out,
        // so replace with true
        Ok(predicate_array
            .into_iter()
            .map(|x| x.unwrap_or(true))
            .collect::<Vec<_>>())
    }

    /// Return a reference to the input schema
    pub fn schema(&self) -> &SchemaRef {
        &self.schema
    }
}

/// Handles creating references to the min/max statistics
/// for columns as well as recording which statistics are needed
#[derive(Debug, Default, Clone)]
struct RequiredStatColumns {
    /// The statistics required to evaluate this predicate:
    /// * The column name in the input schema
    /// * Statistics type (e.g. Min or Max)
    /// * The field the statistics value should be placed in for
    ///   pruning predicate evaluation
    columns: Vec<(String, StatisticsType, Field)>,
}

impl RequiredStatColumns {
    fn new() -> Self {
        Self::default()
    }

    /// Retur an iterator over items in columns (see doc on
    /// `self.columns` for details)
    fn iter(&self) -> impl Iterator<Item = &(String, StatisticsType, Field)> {
        self.columns.iter()
    }

    fn is_stat_column_missing(
        &self,
        column_name: &str,
        statistics_type: StatisticsType,
    ) -> bool {
        !self
            .columns
            .iter()
            .any(|(c, t, _f)| c == column_name && t == &statistics_type)
    }

    /// Rewrites column_expr so that all appearances of column_name
    /// are replaced with a reference to either the min or max
    /// statistics column, while keeping track that a reference to the statistics
    /// column is required
    ///
    /// for example, an expression like `col("foo") > 5`, when called
    /// with Max would result in an expression like `col("foo_max") >
    /// 5` with the approprate entry noted in self.columns
    fn stat_column_expr(
        &mut self,
        column_name: &str,
        column_expr: &Expr,
        field: &Field,
        stat_type: StatisticsType,
        suffix: &str,
    ) -> Result<Expr> {
        let stat_column_name = format!("{}_{}", column_name, suffix);
        let stat_field = Field::new(
            stat_column_name.as_str(),
            field.data_type().clone(),
            field.is_nullable(),
        );
        if self.is_stat_column_missing(column_name, stat_type) {
            // only add statistics column if not previously added
            self.columns
                .push((column_name.to_string(), stat_type, stat_field));
        }
        rewrite_column_expr(column_expr, column_name, stat_column_name.as_str())
    }

    /// rewrite col --> col_min
    fn min_column_expr(
        &mut self,
        column_name: &str,
        column_expr: &Expr,
        field: &Field,
    ) -> Result<Expr> {
        self.stat_column_expr(column_name, column_expr, field, StatisticsType::Min, "min")
    }

    /// rewrite col --> col_max
    fn max_column_expr(
        &mut self,
        column_name: &str,
        column_expr: &Expr,
        field: &Field,
    ) -> Result<Expr> {
        self.stat_column_expr(column_name, column_expr, field, StatisticsType::Max, "max")
    }
}

impl From<Vec<(String, StatisticsType, Field)>> for RequiredStatColumns {
    fn from(columns: Vec<(String, StatisticsType, Field)>) -> Self {
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
    for (column_name, statistics_type, stat_field) in required_columns.iter() {
        let data_type = stat_field.data_type();

        let num_containers = statistics.num_containers();

        let array = match statistics_type {
            StatisticsType::Min => statistics.min_values(column_name),
            StatisticsType::Max => statistics.max_values(column_name),
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
    RecordBatch::try_new(schema, arrays)
        .map_err(|err| DataFusionError::Plan(err.to_string()))
}

struct PruningExpressionBuilder<'a> {
    column_name: String,
    column_expr: &'a Expr,
    scalar_expr: &'a Expr,
    field: &'a Field,
    required_columns: &'a mut RequiredStatColumns,
    reverse_operator: bool,
}

impl<'a> PruningExpressionBuilder<'a> {
    fn try_new(
        left: &'a Expr,
        right: &'a Expr,
        schema: &'a Schema,
        required_columns: &'a mut RequiredStatColumns,
    ) -> Result<Self> {
        // find column name; input could be a more complicated expression
        let mut left_columns = HashSet::<String>::new();
        utils::expr_to_column_names(left, &mut left_columns)?;
        let mut right_columns = HashSet::<String>::new();
        utils::expr_to_column_names(right, &mut right_columns)?;
        let (column_expr, scalar_expr, column_names, reverse_operator) =
            match (left_columns.len(), right_columns.len()) {
                (1, 0) => (left, right, left_columns, false),
                (0, 1) => (right, left, right_columns, true),
                _ => {
                    // if more than one column used in expression - not supported
                    return Err(DataFusionError::Plan(
                        "Multi-column expressions are not currently supported"
                            .to_string(),
                    ));
                }
            };
        let column_name = column_names.iter().next().unwrap().clone();
        let field = match schema.column_with_name(&column_name) {
            Some((_, f)) => f,
            _ => {
                return Err(DataFusionError::Plan(
                    "Field not found in schema".to_string(),
                ));
            }
        };

        Ok(Self {
            column_name,
            column_expr,
            scalar_expr,
            field,
            required_columns,
            reverse_operator,
        })
    }

    fn correct_operator(&self, op: Operator) -> Operator {
        if !self.reverse_operator {
            return op;
        }

        match op {
            Operator::Lt => Operator::Gt,
            Operator::Gt => Operator::Lt,
            Operator::LtEq => Operator::GtEq,
            Operator::GtEq => Operator::LtEq,
            _ => op,
        }
    }

    fn scalar_expr(&self) -> &Expr {
        self.scalar_expr
    }

    fn min_column_expr(&mut self) -> Result<Expr> {
        self.required_columns.min_column_expr(
            &self.column_name,
            self.column_expr,
            self.field,
        )
    }

    fn max_column_expr(&mut self) -> Result<Expr> {
        self.required_columns.max_column_expr(
            &self.column_name,
            self.column_expr,
            self.field,
        )
    }
}

/// replaces a column with an old name with a new name in an expression
fn rewrite_column_expr(
    expr: &Expr,
    column_old_name: &str,
    column_new_name: &str,
) -> Result<Expr> {
    let expressions = utils::expr_sub_expressions(expr)?;
    let expressions = expressions
        .iter()
        .map(|e| rewrite_column_expr(e, column_old_name, column_new_name))
        .collect::<Result<Vec<_>>>()?;

    if let Expr::Column(name) = expr {
        if name == column_old_name {
            return Ok(Expr::Column(column_new_name.to_string()));
        }
    }
    utils::rewrite_expression(expr, &expressions)
}

/// Given a column reference to `column_name`, returns a pruning
/// expression in terms of the min and max that will evaluate to true
/// if the column may contain values, and false if definitely does not
/// contain values
fn build_single_column_expr(
    column_name: &str,
    schema: &Schema,
    required_columns: &mut RequiredStatColumns,
    is_not: bool, // if true, treat as !col
) -> Option<Expr> {
    use crate::logical_plan;
    let field = schema.field_with_name(column_name).ok()?;

    if matches!(field.data_type(), &DataType::Boolean) {
        let col_ref = logical_plan::col(column_name);

        let min = required_columns
            .min_column_expr(column_name, &col_ref, field)
            .ok()?;
        let max = required_columns
            .max_column_expr(column_name, &col_ref, field)
            .ok()?;

        // remember -- we want an expression that is:
        // TRUE: if there may be rows that match
        // FALSE: if there are no rows that match
        if is_not {
            // The only way we know a column couldn't match is if both the min and max are true
            // !(min && max)
            Some((min.and(max)).not())
        } else {
            // the only way we know a column couldn't match is if both the min and max are false
            // !(!min && !max) --> min || max
            Some(min.or(max))
        }
    } else {
        None
    }
}

/// Translate logical filter expression into pruning predicate
/// expression that will evaluate to FALSE if it can be determined no
/// rows between the min/max values could pass the predicates.
///
/// Returns the pruning predicate as an [`Expr`]
fn build_predicate_expression(
    expr: &Expr,
    schema: &Schema,
    required_columns: &mut RequiredStatColumns,
) -> Result<Expr> {
    use crate::logical_plan;

    // Returned for unsupported expressions. Such expressions are
    // converted to TRUE. This can still be useful when multiple
    // conditions are joined using AND such as: column > 10 AND TRUE
    let unhandled = logical_plan::lit(true);

    // predicate expression can only be a binary expression
    let (left, op, right) = match expr {
        Expr::BinaryExpr { left, op, right } => (left, *op, right),
        Expr::Column(name) => {
            let expr = build_single_column_expr(name, schema, required_columns, false)
                .unwrap_or(unhandled);
            return Ok(expr);
        }
        // match !col (don't do so recursively)
        Expr::Not(input) => {
            if let Expr::Column(name) = input.as_ref() {
                let expr = build_single_column_expr(name, schema, required_columns, true)
                    .unwrap_or(unhandled);
                return Ok(expr);
            } else {
                return Ok(unhandled);
            }
        }
        _ => {
            return Ok(unhandled);
        }
    };

    if op == Operator::And || op == Operator::Or {
        let left_expr = build_predicate_expression(left, schema, required_columns)?;
        let right_expr = build_predicate_expression(right, schema, required_columns)?;
        return Ok(logical_plan::binary_expr(left_expr, op, right_expr));
    }

    let expr_builder =
        PruningExpressionBuilder::try_new(left, right, schema, required_columns);
    let mut expr_builder = match expr_builder {
        Ok(builder) => builder,
        // allow partial failure in predicate expression generation
        // this can still produce a useful predicate when multiple conditions are joined using AND
        Err(_) => {
            return Ok(logical_plan::lit(true));
        }
    };
    let corrected_op = expr_builder.correct_operator(op);
    let statistics_expr = match corrected_op {
        Operator::Eq => {
            // column = literal => (min, max) = literal => min <= literal && literal <= max
            // (column / 2) = 4 => (column_min / 2) <= 4 && 4 <= (column_max / 2)
            let min_column_expr = expr_builder.min_column_expr()?;
            let max_column_expr = expr_builder.max_column_expr()?;
            min_column_expr
                .lt_eq(expr_builder.scalar_expr().clone())
                .and(expr_builder.scalar_expr().clone().lt_eq(max_column_expr))
        }
        Operator::Gt => {
            // column > literal => (min, max) > literal => max > literal
            expr_builder
                .max_column_expr()?
                .gt(expr_builder.scalar_expr().clone())
        }
        Operator::GtEq => {
            // column >= literal => (min, max) >= literal => max >= literal
            expr_builder
                .max_column_expr()?
                .gt_eq(expr_builder.scalar_expr().clone())
        }
        Operator::Lt => {
            // column < literal => (min, max) < literal => min < literal
            expr_builder
                .min_column_expr()?
                .lt(expr_builder.scalar_expr().clone())
        }
        Operator::LtEq => {
            // column <= literal => (min, max) <= literal => min <= literal
            expr_builder
                .min_column_expr()?
                .lt_eq(expr_builder.scalar_expr().clone())
        }
        // other expressions are not supported
        _ => logical_plan::lit(true),
    };
    Ok(statistics_expr)
}

#[derive(Debug, Copy, Clone, PartialEq)]
enum StatisticsType {
    Min,
    Max,
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;
    use crate::logical_plan::{col, lit};
    use crate::{assert_batches_eq, physical_optimizer::pruning::StatisticsType};
    use arrow::{
        array::{BinaryArray, Int32Array, Int64Array, StringArray},
        datatypes::{DataType, TimeUnit},
    };

    #[derive(Debug)]
    /// Test for container stats
    struct ContainerStats {
        min: ArrayRef,
        max: ArrayRef,
    }

    impl ContainerStats {
        fn new_i32(
            min: impl IntoIterator<Item = Option<i32>>,
            max: impl IntoIterator<Item = Option<i32>>,
        ) -> Self {
            Self {
                min: Arc::new(min.into_iter().collect::<Int32Array>()),
                max: Arc::new(max.into_iter().collect::<Int32Array>()),
            }
        }

        fn new_utf8<'a>(
            min: impl IntoIterator<Item = Option<&'a str>>,
            max: impl IntoIterator<Item = Option<&'a str>>,
        ) -> Self {
            Self {
                min: Arc::new(min.into_iter().collect::<StringArray>()),
                max: Arc::new(max.into_iter().collect::<StringArray>()),
            }
        }

        fn new_bool(
            min: impl IntoIterator<Item = Option<bool>>,
            max: impl IntoIterator<Item = Option<bool>>,
        ) -> Self {
            Self {
                min: Arc::new(min.into_iter().collect::<BooleanArray>()),
                max: Arc::new(max.into_iter().collect::<BooleanArray>()),
            }
        }

        fn min(&self) -> Option<ArrayRef> {
            Some(self.min.clone())
        }

        fn max(&self) -> Option<ArrayRef> {
            Some(self.max.clone())
        }

        fn len(&self) -> usize {
            assert_eq!(self.min.len(), self.max.len());
            self.min.len()
        }
    }

    #[derive(Debug, Default)]
    struct TestStatistics {
        // key: column name
        stats: HashMap<String, ContainerStats>,
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
            self.stats.insert(name.into(), container_stats);
            self
        }
    }

    impl PruningStatistics for TestStatistics {
        fn min_values(&self, column: &str) -> Option<ArrayRef> {
            self.stats
                .get(column)
                .map(|container_stats| container_stats.min())
                .unwrap_or(None)
        }

        fn max_values(&self, column: &str) -> Option<ArrayRef> {
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
    }

    /// Returns the specified min/max container values
    struct OneContainerStats {
        min_values: Option<ArrayRef>,
        max_values: Option<ArrayRef>,
        num_containers: usize,
    }

    impl PruningStatistics for OneContainerStats {
        fn min_values(&self, _column: &str) -> Option<ArrayRef> {
            self.min_values.clone()
        }

        fn max_values(&self, _column: &str) -> Option<ArrayRef> {
            self.max_values.clone()
        }

        fn num_containers(&self) -> usize {
            self.num_containers
        }
    }

    #[test]
    fn test_build_statistics_record_batch() {
        // Request a record batch with of s1_min, s2_max, s3_max, s3_min
        let required_columns = RequiredStatColumns::from(vec![
            // min of original column s1, named s1_min
            (
                "s1".to_string(),
                StatisticsType::Min,
                Field::new("s1_min", DataType::Int32, true),
            ),
            // max of original column s2, named s2_max
            (
                "s2".to_string(),
                StatisticsType::Max,
                Field::new("s2_max", DataType::Int32, true),
            ),
            // max of original column s3, named s3_max
            (
                "s3".to_string(),
                StatisticsType::Max,
                Field::new("s3_max", DataType::Utf8, true),
            ),
            // min of original column s3, named s3_min
            (
                "s3".to_string(),
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
            "s1".to_string(),
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
            "| 1970-01-01 00:00:00.000000010 |",
            "+-------------------------------+",
        ];

        assert_batches_eq!(expected, &[batch]);
    }

    #[test]
    fn test_build_statistics_no_stats() {
        let required_columns = RequiredStatColumns::new();

        let statistics = OneContainerStats {
            min_values: Some(Arc::new(Int64Array::from(vec![Some(10)]))),
            max_values: Some(Arc::new(Int64Array::from(vec![Some(20)]))),
            num_containers: 1,
        };

        let result =
            build_statistics_record_batch(&statistics, &required_columns).unwrap_err();
        assert!(
            result.to_string().contains("Invalid argument error"),
            "{}",
            result
        );
    }

    #[test]
    fn test_build_statistics_inconsistent_types() {
        // Test requesting a Utf8 column when the stats return some other type

        // Request a record batch with of s1_min as a timestamp
        let required_columns = RequiredStatColumns::from(vec![(
            "s1".to_string(),
            StatisticsType::Min,
            Field::new("s1_min", DataType::Utf8, true),
        )]);

        // Note the statistics return binary (which can't be cast to string)
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
            "s1".to_string(),
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
        let expected_expr = "#c1_min LtEq Int32(1) And Int32(1) LtEq #c1_max";

        // test column on the left
        let expr = col("c1").eq(lit(1));
        let predicate_expr =
            build_predicate_expression(&expr, &schema, &mut RequiredStatColumns::new())?;
        assert_eq!(format!("{:?}", predicate_expr), expected_expr);

        // test column on the right
        let expr = lit(1).eq(col("c1"));
        let predicate_expr =
            build_predicate_expression(&expr, &schema, &mut RequiredStatColumns::new())?;
        assert_eq!(format!("{:?}", predicate_expr), expected_expr);

        Ok(())
    }

    #[test]
    fn row_group_predicate_gt() -> Result<()> {
        let schema = Schema::new(vec![Field::new("c1", DataType::Int32, false)]);
        let expected_expr = "#c1_max Gt Int32(1)";

        // test column on the left
        let expr = col("c1").gt(lit(1));
        let predicate_expr =
            build_predicate_expression(&expr, &schema, &mut RequiredStatColumns::new())?;
        assert_eq!(format!("{:?}", predicate_expr), expected_expr);

        // test column on the right
        let expr = lit(1).lt(col("c1"));
        let predicate_expr =
            build_predicate_expression(&expr, &schema, &mut RequiredStatColumns::new())?;
        assert_eq!(format!("{:?}", predicate_expr), expected_expr);

        Ok(())
    }

    #[test]
    fn row_group_predicate_gt_eq() -> Result<()> {
        let schema = Schema::new(vec![Field::new("c1", DataType::Int32, false)]);
        let expected_expr = "#c1_max GtEq Int32(1)";

        // test column on the left
        let expr = col("c1").gt_eq(lit(1));
        let predicate_expr =
            build_predicate_expression(&expr, &schema, &mut RequiredStatColumns::new())?;
        assert_eq!(format!("{:?}", predicate_expr), expected_expr);
        // test column on the right
        let expr = lit(1).lt_eq(col("c1"));
        let predicate_expr =
            build_predicate_expression(&expr, &schema, &mut RequiredStatColumns::new())?;
        assert_eq!(format!("{:?}", predicate_expr), expected_expr);

        Ok(())
    }

    #[test]
    fn row_group_predicate_lt() -> Result<()> {
        let schema = Schema::new(vec![Field::new("c1", DataType::Int32, false)]);
        let expected_expr = "#c1_min Lt Int32(1)";

        // test column on the left
        let expr = col("c1").lt(lit(1));
        let predicate_expr =
            build_predicate_expression(&expr, &schema, &mut RequiredStatColumns::new())?;
        assert_eq!(format!("{:?}", predicate_expr), expected_expr);

        // test column on the right
        let expr = lit(1).gt(col("c1"));
        let predicate_expr =
            build_predicate_expression(&expr, &schema, &mut RequiredStatColumns::new())?;
        assert_eq!(format!("{:?}", predicate_expr), expected_expr);

        Ok(())
    }

    #[test]
    fn row_group_predicate_lt_eq() -> Result<()> {
        let schema = Schema::new(vec![Field::new("c1", DataType::Int32, false)]);
        let expected_expr = "#c1_min LtEq Int32(1)";

        // test column on the left
        let expr = col("c1").lt_eq(lit(1));
        let predicate_expr =
            build_predicate_expression(&expr, &schema, &mut RequiredStatColumns::new())?;
        assert_eq!(format!("{:?}", predicate_expr), expected_expr);
        // test column on the right
        let expr = lit(1).gt_eq(col("c1"));
        let predicate_expr =
            build_predicate_expression(&expr, &schema, &mut RequiredStatColumns::new())?;
        assert_eq!(format!("{:?}", predicate_expr), expected_expr);

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
        let expected_expr = "#c1_min Lt Int32(1) And Boolean(true)";
        let predicate_expr =
            build_predicate_expression(&expr, &schema, &mut RequiredStatColumns::new())?;
        assert_eq!(format!("{:?}", predicate_expr), expected_expr);

        Ok(())
    }

    #[test]
    fn row_group_predicate_or() -> Result<()> {
        let schema = Schema::new(vec![
            Field::new("c1", DataType::Int32, false),
            Field::new("c2", DataType::Int32, false),
        ]);
        // test OR operator joining supported c1 < 1 expression and unsupported c2 % 2 expression
        let expr = col("c1").lt(lit(1)).or(col("c2").modulus(lit(2)));
        let expected_expr = "#c1_min Lt Int32(1) Or Boolean(true)";
        let predicate_expr =
            build_predicate_expression(&expr, &schema, &mut RequiredStatColumns::new())?;
        assert_eq!(format!("{:?}", predicate_expr), expected_expr);

        Ok(())
    }

    #[test]
    fn row_group_predicate_not() -> Result<()> {
        let schema = Schema::new(vec![Field::new("c1", DataType::Int32, false)]);
        let expected_expr = "Boolean(true)";

        let expr = col("c1").not();
        let predicate_expr =
            build_predicate_expression(&expr, &schema, &mut RequiredStatColumns::new())?;
        assert_eq!(format!("{:?}", predicate_expr), expected_expr);

        Ok(())
    }

    #[test]
    fn row_group_predicate_not_bool() -> Result<()> {
        let schema = Schema::new(vec![Field::new("c1", DataType::Boolean, false)]);
        let expected_expr = "NOT #c1_min And #c1_max";

        let expr = col("c1").not();
        let predicate_expr =
            build_predicate_expression(&expr, &schema, &mut RequiredStatColumns::new())?;
        assert_eq!(format!("{:?}", predicate_expr), expected_expr);

        Ok(())
    }

    #[test]
    fn row_group_predicate_bool() -> Result<()> {
        let schema = Schema::new(vec![Field::new("c1", DataType::Boolean, false)]);
        let expected_expr = "#c1_min Or #c1_max";

        let expr = col("c1");
        let predicate_expr =
            build_predicate_expression(&expr, &schema, &mut RequiredStatColumns::new())?;
        assert_eq!(format!("{:?}", predicate_expr), expected_expr);

        Ok(())
    }

    #[test]
    fn row_group_predicate_lt_bool() -> Result<()> {
        let schema = Schema::new(vec![Field::new("c1", DataType::Boolean, false)]);
        let expected_expr = "#c1_min Lt Boolean(true)";

        // DF doesn't support arithmetic on boolean columns so
        // this predicate will error when evaluated
        let expr = col("c1").lt(lit(true));
        let predicate_expr =
            build_predicate_expression(&expr, &schema, &mut RequiredStatColumns::new())?;
        assert_eq!(format!("{:?}", predicate_expr), expected_expr);

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
        let expected_expr = "#c1_min Lt Int32(1) And #c2_min LtEq Int32(2) And Int32(2) LtEq #c2_max Or #c2_min LtEq Int32(3) And Int32(3) LtEq #c2_max";
        let predicate_expr =
            build_predicate_expression(&expr, &schema, &mut required_columns)?;
        assert_eq!(format!("{:?}", predicate_expr), expected_expr);
        // c1 < 1 should add c1_min
        let c1_min_field = Field::new("c1_min", DataType::Int32, false);
        assert_eq!(
            required_columns.columns[0],
            ("c1".to_owned(), StatisticsType::Min, c1_min_field)
        );
        // c2 = 2 should add c2_min and c2_max
        let c2_min_field = Field::new("c2_min", DataType::Int32, false);
        assert_eq!(
            required_columns.columns[1],
            ("c2".to_owned(), StatisticsType::Min, c2_min_field)
        );
        let c2_max_field = Field::new("c2_max", DataType::Int32, false);
        assert_eq!(
            required_columns.columns[2],
            ("c2".to_owned(), StatisticsType::Max, c2_max_field)
        );
        // c2 = 3 shouldn't add any new statistics fields
        assert_eq!(required_columns.columns.len(), 3);

        Ok(())
    }

    #[test]
    fn prune_api() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("s1", DataType::Utf8, true),
            Field::new("s2", DataType::Int32, true),
        ]));

        // Prune using s2 > 5
        let expr = col("s2").gt(lit(5));

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

        let p = PruningPredicate::try_new(&expr, schema).unwrap();
        let result = p.prune(&statistics).unwrap();
        let expected = vec![false, true, true, true];

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
    fn prune_bool_column() {
        let (schema, statistics, expected_true, _) = bool_setup();

        // b1
        let expr = col("b1");
        let p = PruningPredicate::try_new(&expr, schema).unwrap();
        let result = p.prune(&statistics).unwrap();
        assert_eq!(result, expected_true);
    }

    #[test]
    fn prune_bool_not_column() {
        let (schema, statistics, _, expected_false) = bool_setup();

        // !b1
        let expr = col("b1").not();
        let p = PruningPredicate::try_new(&expr, schema).unwrap();
        let result = p.prune(&statistics).unwrap();
        assert_eq!(result, expected_false);
    }

    #[test]
    fn prune_bool_column_eq_true() {
        let (schema, statistics, _, _) = bool_setup();

        // b1 = true
        let expr = col("b1").eq(lit(true));
        let p = PruningPredicate::try_new(&expr, schema).unwrap();
        let result = p.prune(&statistics).unwrap_err();
        assert!(
            result.to_string().contains(
                "Data type Boolean not supported for scalar operation on dyn array"
            ),
            "{}",
            result
        )
    }

    #[test]
    fn prune_bool_not_column_eq_true() {
        let (schema, statistics, _, _) = bool_setup();

        // !b1 = true
        let expr = col("b1").not().eq(lit(true));
        let p = PruningPredicate::try_new(&expr, schema).unwrap();
        let result = p.prune(&statistics).unwrap_err();
        assert!(
            result.to_string().contains(
                "Data type Boolean not supported for scalar operation on dyn array"
            ),
            "{}",
            result
        )
    }
}
