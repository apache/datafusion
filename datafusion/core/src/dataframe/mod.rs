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

//! [`DataFrame`] API for building and executing query plans.

#[cfg(feature = "parquet")]
mod parquet;

use crate::arrow::record_batch::RecordBatch;
use crate::arrow::util::pretty;
use crate::datasource::file_format::csv::CsvFormatFactory;
use crate::datasource::file_format::format_as_file_type;
use crate::datasource::file_format::json::JsonFormatFactory;
use crate::datasource::{
    provider_as_source, DefaultTableSource, MemTable, TableProvider,
};
use crate::error::Result;
use crate::execution::context::{SessionState, TaskContext};
use crate::execution::FunctionRegistry;
use crate::logical_expr::utils::find_window_exprs;
use crate::logical_expr::{
    col, ident, Expr, JoinType, LogicalPlan, LogicalPlanBuilder,
    LogicalPlanBuilderOptions, Partitioning, TableType,
};
use crate::physical_plan::{
    collect, collect_partitioned, execute_stream, execute_stream_partitioned,
    ExecutionPlan, SendableRecordBatchStream,
};
use crate::prelude::SessionContext;
use std::any::Any;
use std::borrow::Cow;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, Int64Array, StringArray};
use arrow::compute::{cast, concat};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow_schema::FieldRef;
use datafusion_common::config::{CsvOptions, JsonOptions};
use datafusion_common::{
    exec_err, internal_datafusion_err, not_impl_err, plan_datafusion_err, plan_err,
    unqualified_field_not_found, Column, DFSchema, DataFusionError, ParamValues,
    ScalarValue, SchemaError, TableReference, UnnestOptions,
};
use datafusion_expr::select_expr::SelectExpr;
use datafusion_expr::{
    case,
    dml::InsertOp,
    expr::{Alias, ScalarFunction},
    is_null, lit,
    utils::COUNT_STAR_EXPANSION,
    ExplainOption, SortExpr, TableProviderFilterPushDown, UNNAMED_TABLE,
};
use datafusion_functions::core::coalesce;
use datafusion_functions_aggregate::expr_fn::{
    avg, count, max, median, min, stddev, sum,
};

use async_trait::async_trait;
use datafusion_catalog::Session;

/// Contains options that control how data is
/// written out from a DataFrame
pub struct DataFrameWriteOptions {
    /// Controls how new data should be written to the table, determining whether
    /// to append, overwrite, or replace existing data.
    insert_op: InsertOp,
    /// Controls if all partitions should be coalesced into a single output file
    /// Generally will have slower performance when set to true.
    single_file_output: bool,
    /// Sets which columns should be used for hive-style partitioned writes by name.
    /// Can be set to empty vec![] for non-partitioned writes.
    partition_by: Vec<String>,
    /// Sets which columns should be used for sorting the output by name.
    /// Can be set to empty vec![] for non-sorted writes.
    sort_by: Vec<SortExpr>,
}

impl DataFrameWriteOptions {
    /// Create a new DataFrameWriteOptions with default values
    pub fn new() -> Self {
        DataFrameWriteOptions {
            insert_op: InsertOp::Append,
            single_file_output: false,
            partition_by: vec![],
            sort_by: vec![],
        }
    }

    /// Set the insert operation
    pub fn with_insert_operation(mut self, insert_op: InsertOp) -> Self {
        self.insert_op = insert_op;
        self
    }

    /// Set the single_file_output value to true or false
    pub fn with_single_file_output(mut self, single_file_output: bool) -> Self {
        self.single_file_output = single_file_output;
        self
    }

    /// Sets the partition_by columns for output partitioning
    pub fn with_partition_by(mut self, partition_by: Vec<String>) -> Self {
        self.partition_by = partition_by;
        self
    }

    /// Sets the sort_by columns for output sorting
    pub fn with_sort_by(mut self, sort_by: Vec<SortExpr>) -> Self {
        self.sort_by = sort_by;
        self
    }
}

impl Default for DataFrameWriteOptions {
    fn default() -> Self {
        Self::new()
    }
}

/// Represents a logical set of rows with the same named columns.
///
/// Similar to a [Pandas DataFrame] or [Spark DataFrame], a DataFusion DataFrame
/// represents a 2 dimensional table of rows and columns.
///
/// The typical workflow using DataFrames looks like
///
/// 1. Create a DataFrame via methods on [SessionContext], such as [`read_csv`]
///    and [`read_parquet`].
///
/// 2. Build a desired calculation by calling methods such as [`filter`],
///    [`select`], [`aggregate`], and [`limit`]
///
/// 3. Execute into [`RecordBatch`]es by calling [`collect`]
///
/// A `DataFrame` is a wrapper around a [`LogicalPlan`] and the [`SessionState`]
///    required for execution.
///
/// DataFrames are "lazy" in the sense that most methods do not actually compute
/// anything, they just build up a plan. Calling [`collect`] executes the plan
/// using the same DataFusion planning and execution process used to execute SQL
/// and other queries.
///
/// [Pandas DataFrame]: https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html
/// [Spark DataFrame]: https://spark.apache.org/docs/latest/sql-programming-guide.html
/// [`read_csv`]: SessionContext::read_csv
/// [`read_parquet`]: SessionContext::read_parquet
/// [`filter`]: DataFrame::filter
/// [`select`]: DataFrame::select
/// [`aggregate`]: DataFrame::aggregate
/// [`limit`]: DataFrame::limit
/// [`collect`]: DataFrame::collect
///
/// # Example
/// ```
/// # use std::sync::Arc;
/// # use datafusion::prelude::*;
/// # use datafusion::error::Result;
/// # use datafusion::functions_aggregate::expr_fn::min;
/// # use datafusion::arrow::array::{Int32Array, RecordBatch, StringArray};
/// # use datafusion::arrow::datatypes::{DataType, Field, Schema};
/// # #[tokio::main]
/// # async fn main() -> Result<()> {
/// let ctx = SessionContext::new();
/// // Read the data from a csv file
/// let df = ctx.read_csv("tests/data/example.csv", CsvReadOptions::new()).await?;
/// // create a new dataframe that computes the equivalent of
/// // `SELECT a, MIN(b) FROM df WHERE a <= b GROUP BY a LIMIT 100;`
/// let df = df.filter(col("a").lt_eq(col("b")))?
///            .aggregate(vec![col("a")], vec![min(col("b"))])?
///            .limit(0, Some(100))?;
/// // Perform the actual computation
/// let results = df.collect();
///
/// // Create a new dataframe with in-memory data
/// let schema = Schema::new(vec![
///     Field::new("id", DataType::Int32, true),
///     Field::new("name", DataType::Utf8, true),
/// ]);
/// let batch = RecordBatch::try_new(
///     Arc::new(schema),
///     vec![
///         Arc::new(Int32Array::from(vec![1, 2, 3])),
///         Arc::new(StringArray::from(vec!["foo", "bar", "baz"])),
///     ],
/// )?;
/// let df = ctx.read_batch(batch)?;
/// df.show().await?;
///
/// // Create a new dataframe with in-memory data using macro
/// let df = dataframe!(
///     "id" => [1, 2, 3],
///     "name" => ["foo", "bar", "baz"]
///  )?;
/// df.show().await?;
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Clone)]
pub struct DataFrame {
    // Box the (large) SessionState to reduce the size of DataFrame on the stack
    session_state: Box<SessionState>,
    plan: LogicalPlan,
    // Whether projection ops can skip validation or not. This flag if false
    // allows for an optimization in `with_column` and `with_column_renamed` functions
    // where the recursive work required to columnize and normalize expressions can
    // be skipped if set to false. Since these function calls are often chained or
    // called many times in dataframe operations this can result in a significant
    // performance gain.
    //
    // The conditions where this can be set to false is when the dataframe function
    // call results in the last operation being a
    // `LogicalPlanBuilder::from(plan).project(fields)?.build()` or
    // `LogicalPlanBuilder::from(plan).project_with_validation(fields)?.build()`
    // call. This requirement guarantees that the plan has had all columnization
    // and normalization applied to existing expressions and only new expressions
    // will require that work. Any operation that update the plan in any way
    // via anything other than a `project` call should set this to true.
    projection_requires_validation: bool,
}

impl DataFrame {
    /// Create a new `DataFrame ` based on an existing `LogicalPlan`
    ///
    /// This is a low-level method and is not typically used by end users. See
    /// [`SessionContext::read_csv`] and other methods for creating a
    /// `DataFrame` from an existing datasource.
    pub fn new(session_state: SessionState, plan: LogicalPlan) -> Self {
        Self {
            session_state: Box::new(session_state),
            plan,
            projection_requires_validation: true,
        }
    }

    /// Creates logical expression from a SQL query text.
    /// The expression is created and processed against the current schema.
    ///
    /// # Example: Parsing SQL queries
    /// ```
    /// # use arrow::datatypes::{DataType, Field, Schema};
    /// # use datafusion::prelude::*;
    /// # use datafusion_common::{DFSchema, Result};
    /// # #[tokio::main]
    /// # async fn main() -> Result<()> {
    /// // datafusion will parse number as i64 first.
    /// let sql = "a > 1 and b in (1, 10)";
    /// let expected = col("a")
    ///     .gt(lit(1 as i64))
    ///     .and(col("b").in_list(vec![lit(1 as i64), lit(10 as i64)], false));
    /// let ctx = SessionContext::new();
    /// let df = ctx
    ///     .read_csv("tests/data/example.csv", CsvReadOptions::new())
    ///     .await?;
    /// let expr = df.parse_sql_expr(sql)?;
    /// assert_eq!(expected, expr);
    /// # Ok(())
    /// # }
    /// ```
    #[cfg(feature = "sql")]
    pub fn parse_sql_expr(&self, sql: &str) -> Result<Expr> {
        let df_schema = self.schema();

        self.session_state.create_logical_expr(sql, df_schema)
    }

    /// Consume the DataFrame and produce a physical plan
    pub async fn create_physical_plan(self) -> Result<Arc<dyn ExecutionPlan>> {
        self.session_state.create_physical_plan(&self.plan).await
    }

    /// Filter the DataFrame by column. Returns a new DataFrame only containing the
    /// specified columns.
    ///
    /// ```
    /// # use datafusion::prelude::*;
    /// # use datafusion::error::Result;
    /// # use datafusion_common::assert_batches_sorted_eq;
    /// # #[tokio::main]
    /// # async fn main() -> Result<()> {
    /// let ctx = SessionContext::new();
    /// let df = ctx
    ///     .read_csv("tests/data/example.csv", CsvReadOptions::new())
    ///     .await?;
    /// let df = df.select_columns(&["a", "b"])?;
    /// let expected = vec![
    ///     "+---+---+",
    ///     "| a | b |",
    ///     "+---+---+",
    ///     "| 1 | 2 |",
    ///     "+---+---+",
    /// ];
    /// # assert_batches_sorted_eq!(expected, &df.collect().await?);
    /// # Ok(())
    /// # }
    /// ```
    pub fn select_columns(self, columns: &[&str]) -> Result<DataFrame> {
        let fields = columns
            .iter()
            .map(|name| {
                let fields = self
                    .plan
                    .schema()
                    .qualified_fields_with_unqualified_name(name);
                if fields.is_empty() {
                    Err(unqualified_field_not_found(name, self.plan.schema()))
                } else {
                    Ok(fields)
                }
            })
            .collect::<Result<Vec<_>, _>>()?
            .into_iter()
            .flatten()
            .collect::<Vec<_>>();
        let expr: Vec<Expr> = fields
            .into_iter()
            .map(|(qualifier, field)| Expr::Column(Column::from((qualifier, field))))
            .collect();
        self.select(expr)
    }
    /// Project arbitrary list of expression strings into a new `DataFrame`.
    /// Method will parse string expressions into logical plan expressions.
    ///
    /// The output `DataFrame` has one column for each element in `exprs`.
    ///
    /// # Example
    /// ```
    /// # use datafusion::prelude::*;
    /// # use datafusion::error::Result;
    /// # #[tokio::main]
    /// # async fn main() -> Result<()> {
    /// let ctx = SessionContext::new();
    /// let df = ctx
    ///     .read_csv("tests/data/example.csv", CsvReadOptions::new())
    ///     .await?;
    /// let df: DataFrame = df.select_exprs(&["a * b", "c"])?;
    /// # Ok(())
    /// # }
    /// ```
    #[cfg(feature = "sql")]
    pub fn select_exprs(self, exprs: &[&str]) -> Result<DataFrame> {
        let expr_list = exprs
            .iter()
            .map(|e| self.parse_sql_expr(e))
            .collect::<Result<Vec<_>>>()?;

        self.select(expr_list)
    }

    /// Project arbitrary expressions (like SQL SELECT expressions) into a new
    /// `DataFrame`.
    ///
    /// The output `DataFrame` has one column for each element in `expr_list`.
    ///
    /// # Example
    /// ```
    /// # use datafusion::prelude::*;
    /// # use datafusion::error::Result;
    /// # use datafusion_common::assert_batches_sorted_eq;
    /// # #[tokio::main]
    /// # async fn main() -> Result<()> {
    /// let ctx = SessionContext::new();
    /// let df = ctx
    ///     .read_csv("tests/data/example.csv", CsvReadOptions::new())
    ///     .await?;
    /// let df = df.select(vec![col("a"), col("b") * col("c")])?;
    /// let expected = vec![
    ///     "+---+-----------------------+",
    ///     "| a | ?table?.b * ?table?.c |",
    ///     "+---+-----------------------+",
    ///     "| 1 | 6                     |",
    ///     "+---+-----------------------+",
    /// ];
    /// # assert_batches_sorted_eq!(expected, &df.collect().await?);
    /// # Ok(())
    /// # }
    /// ```
    pub fn select(
        self,
        expr_list: impl IntoIterator<Item = impl Into<SelectExpr>>,
    ) -> Result<DataFrame> {
        let expr_list: Vec<SelectExpr> =
            expr_list.into_iter().map(|e| e.into()).collect::<Vec<_>>();

        let expressions = expr_list.iter().filter_map(|e| match e {
            SelectExpr::Expression(expr) => Some(expr),
            _ => None,
        });

        let window_func_exprs = find_window_exprs(expressions);
        let plan = if window_func_exprs.is_empty() {
            self.plan
        } else {
            LogicalPlanBuilder::window_plan(self.plan, window_func_exprs)?
        };

        let project_plan = LogicalPlanBuilder::from(plan).project(expr_list)?.build()?;

        Ok(DataFrame {
            session_state: self.session_state,
            plan: project_plan,
            projection_requires_validation: false,
        })
    }

    /// Returns a new DataFrame containing all columns except the specified columns.
    ///
    /// ```
    /// # use datafusion::prelude::*;
    /// # use datafusion::error::Result;
    /// # use datafusion_common::assert_batches_sorted_eq;
    /// # #[tokio::main]
    /// # async fn main() -> Result<()> {
    /// let ctx = SessionContext::new();
    /// let df = ctx
    ///     .read_csv("tests/data/example.csv", CsvReadOptions::new())
    ///     .await?;
    /// // +----+----+----+
    /// // | a  | b  | c  |
    /// // +----+----+----+
    /// // | 1  | 2  | 3  |
    /// // +----+----+----+
    /// let df = df.drop_columns(&["a"])?;
    /// let expected = vec![
    ///     "+---+---+",
    ///     "| b | c |",
    ///     "+---+---+",
    ///     "| 2 | 3 |",
    ///     "+---+---+",
    /// ];
    /// # assert_batches_sorted_eq!(expected, &df.collect().await?);
    /// # Ok(())
    /// # }
    /// ```
    pub fn drop_columns(self, columns: &[&str]) -> Result<DataFrame> {
        let fields_to_drop = columns
            .iter()
            .flat_map(|name| {
                self.plan
                    .schema()
                    .qualified_fields_with_unqualified_name(name)
            })
            .collect::<Vec<_>>();
        let expr: Vec<Expr> = self
            .plan
            .schema()
            .fields()
            .into_iter()
            .enumerate()
            .map(|(idx, _)| self.plan.schema().qualified_field(idx))
            .filter(|(qualifier, f)| !fields_to_drop.contains(&(*qualifier, f)))
            .map(|(qualifier, field)| Expr::Column(Column::from((qualifier, field))))
            .collect();
        self.select(expr)
    }

    /// Expand multiple list/struct columns into a set of rows and new columns.
    ///
    /// See also: [`UnnestOptions`] documentation for the behavior of `unnest`
    ///
    /// # Example
    /// ```
    /// # use datafusion::prelude::*;
    /// # use datafusion::error::Result;
    /// # use datafusion_common::assert_batches_sorted_eq;
    /// # #[tokio::main]
    /// # async fn main() -> Result<()> {
    /// let ctx = SessionContext::new();
    /// let df = ctx.read_json("tests/data/unnest.json", NdJsonReadOptions::default()).await?;
    /// // expand into multiple columns if it's json array, flatten field name if it's nested structure
    /// let df = df.unnest_columns(&["b","c","d"])?;
    /// let expected = vec![
    ///     "+---+------+-------+-----+-----+",
    ///     "| a | b    | c     | d.e | d.f |",
    ///     "+---+------+-------+-----+-----+",
    ///     "| 1 | 2.0  | false | 1   | 2   |",
    ///     "| 1 | 1.3  | true  | 1   | 2   |",
    ///     "| 1 | -6.1 |       | 1   | 2   |",
    ///     "| 2 | 3.0  | false |     |     |",
    ///     "| 2 | 2.3  | true  |     |     |",
    ///     "| 2 | -7.1 |       |     |     |",
    ///     "+---+------+-------+-----+-----+"
    /// ];
    /// # assert_batches_sorted_eq!(expected, &df.collect().await?);
    /// # Ok(())
    /// # }
    /// ```
    pub fn unnest_columns(self, columns: &[&str]) -> Result<DataFrame> {
        self.unnest_columns_with_options(columns, UnnestOptions::new())
    }

    /// Expand multiple list columns into a set of rows, with
    /// behavior controlled by [`UnnestOptions`].
    ///
    /// Please see the documentation on [`UnnestOptions`] for more
    /// details about the meaning of unnest.
    pub fn unnest_columns_with_options(
        self,
        columns: &[&str],
        options: UnnestOptions,
    ) -> Result<DataFrame> {
        let columns = columns.iter().map(|c| Column::from(*c)).collect();
        let plan = LogicalPlanBuilder::from(self.plan)
            .unnest_columns_with_options(columns, options)?
            .build()?;
        Ok(DataFrame {
            session_state: self.session_state,
            plan,
            projection_requires_validation: true,
        })
    }

    /// Return a DataFrame with only rows for which `predicate` evaluates to
    /// `true`.
    ///
    /// Rows for which `predicate` evaluates to `false` or `null`
    /// are filtered out.
    ///
    /// # Example
    /// ```
    /// # use datafusion::prelude::*;
    /// # use datafusion::error::Result;
    /// # use datafusion_common::assert_batches_sorted_eq;
    /// # #[tokio::main]
    /// # async fn main() -> Result<()> {
    /// let ctx = SessionContext::new();
    /// let df = ctx
    ///     .read_csv("tests/data/example_long.csv", CsvReadOptions::new())
    ///     .await?;
    /// let df = df.filter(col("a").lt_eq(col("b")))?;
    /// // all rows where a <= b are returned
    /// let expected = vec![
    ///     "+---+---+---+",
    ///     "| a | b | c |",
    ///     "+---+---+---+",
    ///     "| 1 | 2 | 3 |",
    ///     "| 4 | 5 | 6 |",
    ///     "| 7 | 8 | 9 |",
    ///     "+---+---+---+",
    /// ];
    /// # assert_batches_sorted_eq!(expected, &df.collect().await?);
    /// # Ok(())
    /// # }
    /// ```
    pub fn filter(self, predicate: Expr) -> Result<DataFrame> {
        let plan = LogicalPlanBuilder::from(self.plan)
            .filter(predicate)?
            .build()?;
        Ok(DataFrame {
            session_state: self.session_state,
            plan,
            projection_requires_validation: true,
        })
    }

    /// Return a new `DataFrame` that aggregates the rows of the current
    /// `DataFrame`, first optionally grouping by the given expressions.
    ///
    /// # Example
    /// ```
    /// # use datafusion::prelude::*;
    /// # use datafusion::error::Result;
    /// # use datafusion::functions_aggregate::expr_fn::min;
    /// # use datafusion_common::assert_batches_sorted_eq;
    /// # #[tokio::main]
    /// # async fn main() -> Result<()> {
    /// let ctx = SessionContext::new();
    /// let df = ctx
    ///     .read_csv("tests/data/example_long.csv", CsvReadOptions::new())
    ///     .await?;
    ///
    /// // The following use is the equivalent of "SELECT MIN(b) GROUP BY a"
    /// let df1 = df.clone().aggregate(vec![col("a")], vec![min(col("b"))])?;
    /// let expected1 = vec![
    ///     "+---+----------------+",
    ///     "| a | min(?table?.b) |",
    ///     "+---+----------------+",
    ///     "| 1 | 2              |",
    ///     "| 4 | 5              |",
    ///     "| 7 | 8              |",
    ///     "+---+----------------+",
    /// ];
    /// assert_batches_sorted_eq!(expected1, &df1.collect().await?);
    /// // The following use is the equivalent of "SELECT MIN(b)"
    /// let df2 = df.aggregate(vec![], vec![min(col("b"))])?;
    /// let expected2 = vec![
    ///     "+----------------+",
    ///     "| min(?table?.b) |",
    ///     "+----------------+",
    ///     "| 2              |",
    ///     "+----------------+",
    /// ];
    /// # assert_batches_sorted_eq!(expected2, &df2.collect().await?);
    /// # Ok(())
    /// # }
    /// ```
    pub fn aggregate(
        self,
        group_expr: Vec<Expr>,
        aggr_expr: Vec<Expr>,
    ) -> Result<DataFrame> {
        let is_grouping_set = matches!(group_expr.as_slice(), [Expr::GroupingSet(_)]);
        let aggr_expr_len = aggr_expr.len();
        let options =
            LogicalPlanBuilderOptions::new().with_add_implicit_group_by_exprs(true);
        let plan = LogicalPlanBuilder::from(self.plan)
            .with_options(options)
            .aggregate(group_expr, aggr_expr)?
            .build()?;
        let plan = if is_grouping_set {
            let grouping_id_pos = plan.schema().fields().len() - 1 - aggr_expr_len;
            // For grouping sets we do a project to not expose the internal grouping id
            let exprs = plan
                .schema()
                .columns()
                .into_iter()
                .enumerate()
                .filter(|(idx, _)| *idx != grouping_id_pos)
                .map(|(_, column)| Expr::Column(column))
                .collect::<Vec<_>>();
            LogicalPlanBuilder::from(plan).project(exprs)?.build()?
        } else {
            plan
        };
        Ok(DataFrame {
            session_state: self.session_state,
            plan,
            projection_requires_validation: !is_grouping_set,
        })
    }

    /// Return a new DataFrame that adds the result of evaluating one or more
    /// window functions ([`Expr::WindowFunction`]) to the existing columns
    pub fn window(self, window_exprs: Vec<Expr>) -> Result<DataFrame> {
        let plan = LogicalPlanBuilder::from(self.plan)
            .window(window_exprs)?
            .build()?;
        Ok(DataFrame {
            session_state: self.session_state,
            plan,
            projection_requires_validation: true,
        })
    }

    /// Returns a new `DataFrame` with a limited number of rows.
    ///
    /// # Arguments
    /// `skip` - Number of rows to skip before fetch any row
    /// `fetch` - Maximum number of rows to return, after skipping `skip` rows.
    ///
    /// # Example
    /// ```
    /// # use datafusion::prelude::*;
    /// # use datafusion::error::Result;
    /// # use datafusion_common::assert_batches_sorted_eq;
    /// # #[tokio::main]
    /// # async fn main() -> Result<()> {
    /// let ctx = SessionContext::new();
    /// let df = ctx
    ///     .read_csv("tests/data/example_long.csv", CsvReadOptions::new())
    ///     .await?;
    /// let df = df.limit(1, Some(2))?;
    /// let expected = vec![
    ///     "+---+---+---+",
    ///     "| a | b | c |",
    ///     "+---+---+---+",
    ///     "| 4 | 5 | 6 |",
    ///     "| 7 | 8 | 9 |",
    ///     "+---+---+---+",
    /// ];
    /// # assert_batches_sorted_eq!(expected, &df.collect().await?);
    /// # Ok(())
    /// # }
    /// ```
    pub fn limit(self, skip: usize, fetch: Option<usize>) -> Result<DataFrame> {
        let plan = LogicalPlanBuilder::from(self.plan)
            .limit(skip, fetch)?
            .build()?;
        Ok(DataFrame {
            session_state: self.session_state,
            plan,
            projection_requires_validation: self.projection_requires_validation,
        })
    }

    /// Calculate the union of two [`DataFrame`]s, preserving duplicate rows.
    ///
    /// The two [`DataFrame`]s must have exactly the same schema
    ///
    /// # Example
    /// ```
    /// # use datafusion::prelude::*;
    /// # use datafusion::error::Result;
    /// # use datafusion_common::assert_batches_sorted_eq;
    /// # #[tokio::main]
    /// # async fn main() -> Result<()> {
    /// let ctx = SessionContext::new();
    /// let df = ctx
    ///     .read_csv("tests/data/example.csv", CsvReadOptions::new())
    ///     .await?;
    /// let d2 = df.clone();
    /// let df = df.union(d2)?;
    /// let expected = vec![
    ///     "+---+---+---+",
    ///     "| a | b | c |",
    ///     "+---+---+---+",
    ///     "| 1 | 2 | 3 |",
    ///     "| 1 | 2 | 3 |",
    ///     "+---+---+---+",
    /// ];
    /// # assert_batches_sorted_eq!(expected, &df.collect().await?);
    /// # Ok(())
    /// # }
    /// ```
    pub fn union(self, dataframe: DataFrame) -> Result<DataFrame> {
        let plan = LogicalPlanBuilder::from(self.plan)
            .union(dataframe.plan)?
            .build()?;
        Ok(DataFrame {
            session_state: self.session_state,
            plan,
            projection_requires_validation: true,
        })
    }

    /// Calculate the union of two [`DataFrame`]s using column names, preserving duplicate rows.
    ///
    /// The two [`DataFrame`]s are combined using column names rather than position,
    /// filling missing columns with null.
    ///
    ///
    /// # Example
    /// ```
    /// # use datafusion::prelude::*;
    /// # use datafusion::error::Result;
    /// # use datafusion_common::assert_batches_sorted_eq;
    /// # #[tokio::main]
    /// # async fn main() -> Result<()> {
    /// let ctx = SessionContext::new();
    /// let df = ctx
    ///     .read_csv("tests/data/example.csv", CsvReadOptions::new())
    ///     .await?;
    /// let d2 = df
    ///     .clone()
    ///     .select_columns(&["b", "c", "a"])?
    ///     .with_column("d", lit("77"))?;
    /// let df = df.union_by_name(d2)?;
    /// let expected = vec![
    ///     "+---+---+---+----+",
    ///     "| a | b | c | d  |",
    ///     "+---+---+---+----+",
    ///     "| 1 | 2 | 3 |    |",
    ///     "| 1 | 2 | 3 | 77 |",
    ///     "+---+---+---+----+",
    /// ];
    /// # assert_batches_sorted_eq!(expected, &df.collect().await?);
    /// # Ok(())
    /// # }
    /// ```
    pub fn union_by_name(self, dataframe: DataFrame) -> Result<DataFrame> {
        let plan = LogicalPlanBuilder::from(self.plan)
            .union_by_name(dataframe.plan)?
            .build()?;
        Ok(DataFrame {
            session_state: self.session_state,
            plan,
            projection_requires_validation: true,
        })
    }

    /// Calculate the distinct union of two [`DataFrame`]s.
    ///
    /// The two [`DataFrame`]s must have exactly the same schema. Any duplicate
    /// rows are discarded.
    ///
    /// # Example
    /// ```
    /// # use datafusion::prelude::*;
    /// # use datafusion::error::Result;
    /// # use datafusion_common::assert_batches_sorted_eq;
    /// # #[tokio::main]
    /// # async fn main() -> Result<()> {
    /// let ctx = SessionContext::new();
    /// let df = ctx
    ///     .read_csv("tests/data/example.csv", CsvReadOptions::new())
    ///     .await?;
    /// let d2 = df.clone();
    /// let df = df.union_distinct(d2)?;
    /// // df2 are duplicate of df
    /// let expected = vec![
    ///     "+---+---+---+",
    ///     "| a | b | c |",
    ///     "+---+---+---+",
    ///     "| 1 | 2 | 3 |",
    ///     "+---+---+---+",
    /// ];
    /// # assert_batches_sorted_eq!(expected, &df.collect().await?);
    /// # Ok(())
    /// # }
    /// ```
    pub fn union_distinct(self, dataframe: DataFrame) -> Result<DataFrame> {
        let plan = LogicalPlanBuilder::from(self.plan)
            .union_distinct(dataframe.plan)?
            .build()?;
        Ok(DataFrame {
            session_state: self.session_state,
            plan,
            projection_requires_validation: true,
        })
    }

    /// Calculate the union of two [`DataFrame`]s using column names with all duplicated rows removed.
    ///
    /// The two [`DataFrame`]s are combined using column names rather than position,
    /// filling missing columns with null.
    ///
    ///
    /// # Example
    /// ```
    /// # use datafusion::prelude::*;
    /// # use datafusion::error::Result;
    /// # use datafusion_common::assert_batches_sorted_eq;
    /// # #[tokio::main]
    /// # async fn main() -> Result<()> {
    /// let ctx = SessionContext::new();
    /// let df = ctx
    ///     .read_csv("tests/data/example.csv", CsvReadOptions::new())
    ///     .await?;
    /// let d2 = df.clone().select_columns(&["b", "c", "a"])?;
    /// let df = df.union_by_name_distinct(d2)?;
    /// let expected = vec![
    ///     "+---+---+---+",
    ///     "| a | b | c |",
    ///     "+---+---+---+",
    ///     "| 1 | 2 | 3 |",
    ///     "+---+---+---+",
    /// ];
    /// # assert_batches_sorted_eq!(expected, &df.collect().await?);
    /// # Ok(())
    /// # }
    /// ```
    pub fn union_by_name_distinct(self, dataframe: DataFrame) -> Result<DataFrame> {
        let plan = LogicalPlanBuilder::from(self.plan)
            .union_by_name_distinct(dataframe.plan)?
            .build()?;
        Ok(DataFrame {
            session_state: self.session_state,
            plan,
            projection_requires_validation: true,
        })
    }

    /// Return a new `DataFrame` with all duplicated rows removed.
    ///
    /// # Example
    /// ```
    /// # use datafusion::prelude::*;
    /// # use datafusion::error::Result;
    /// # use datafusion_common::assert_batches_sorted_eq;
    /// # #[tokio::main]
    /// # async fn main() -> Result<()> {
    /// let ctx = SessionContext::new();
    /// let df = ctx
    ///     .read_csv("tests/data/example.csv", CsvReadOptions::new())
    ///     .await?;
    /// let df = df.distinct()?;
    /// let expected = vec![
    ///     "+---+---+---+",
    ///     "| a | b | c |",
    ///     "+---+---+---+",
    ///     "| 1 | 2 | 3 |",
    ///     "+---+---+---+",
    /// ];
    /// # assert_batches_sorted_eq!(expected, &df.collect().await?);
    /// # Ok(())
    /// # }
    /// ```
    pub fn distinct(self) -> Result<DataFrame> {
        let plan = LogicalPlanBuilder::from(self.plan).distinct()?.build()?;
        Ok(DataFrame {
            session_state: self.session_state,
            plan,
            projection_requires_validation: true,
        })
    }

    /// Return a new `DataFrame` with duplicated rows removed as per the specified expression list
    /// according to the provided sorting expressions grouped by the `DISTINCT ON` clause
    /// expressions.
    ///
    /// # Example
    /// ```
    /// # use datafusion::prelude::*;
    /// # use datafusion::error::Result;
    /// # use datafusion_common::assert_batches_sorted_eq;
    /// # #[tokio::main]
    /// # async fn main() -> Result<()> {
    /// let ctx = SessionContext::new();
    /// let df = ctx
    ///     .read_csv("tests/data/example.csv", CsvReadOptions::new())
    ///     .await?
    ///     // Return a single row (a, b) for each distinct value of a
    ///     .distinct_on(vec![col("a")], vec![col("a"), col("b")], None)?;
    /// let expected = vec![
    ///     "+---+---+",
    ///     "| a | b |",
    ///     "+---+---+",
    ///     "| 1 | 2 |",
    ///     "+---+---+",
    /// ];
    /// # assert_batches_sorted_eq!(expected, &df.collect().await?);
    /// # Ok(())
    /// # }
    /// ```
    pub fn distinct_on(
        self,
        on_expr: Vec<Expr>,
        select_expr: Vec<Expr>,
        sort_expr: Option<Vec<SortExpr>>,
    ) -> Result<DataFrame> {
        let plan = LogicalPlanBuilder::from(self.plan)
            .distinct_on(on_expr, select_expr, sort_expr)?
            .build()?;
        Ok(DataFrame {
            session_state: self.session_state,
            plan,
            projection_requires_validation: true,
        })
    }

    /// Return a new `DataFrame` that has statistics for a DataFrame.
    ///
    /// Only summarizes numeric datatypes at the moment and returns nulls for
    /// non numeric datatypes. The output format is modeled after pandas
    ///
    /// # Example
    /// ```
    /// # use datafusion::prelude::*;
    /// # use datafusion::error::Result;
    /// # use arrow::util::pretty;
    /// # use datafusion_common::assert_batches_sorted_eq;
    /// # #[tokio::main]
    /// # async fn main() -> Result<()> {
    /// let ctx = SessionContext::new();
    /// let df = ctx.read_csv("tests/tpch-csv/customer.csv", CsvReadOptions::new()).await?;
    /// let stat = df.describe().await?;
    /// # // some output column are ignored
    /// let expected = vec![
    ///     "+------------+--------------------+--------------------+------------------------------------+--------------------+-----------------+--------------------+--------------+----------------------------------------------------------------------------------------------------------+",
    ///     "| describe   | c_custkey          | c_name             | c_address                          | c_nationkey        | c_phone         | c_acctbal          | c_mktsegment | c_comment                                                                                                |",
    ///     "+------------+--------------------+--------------------+------------------------------------+--------------------+-----------------+--------------------+--------------+----------------------------------------------------------------------------------------------------------+",
    ///     "| count      | 9.0                | 9                  | 9                                  | 9.0                | 9               | 9.0                | 9            | 9                                                                                                        |",
    ///     "| max        | 10.0               | Customer#000000010 | xKiAFTjUsCuxfeleNqefumTrjS         | 20.0               | 30-114-968-4951 | 9561.95            | MACHINERY    | tions. even deposits boost according to the slyly bold packages. final accounts cajole requests. furious |",
    ///     "| mean       | 6.0                | null               | null                               | 9.88888888888889   | null            | 5153.2155555555555 | null         | null                                                                                                     |",
    ///     "| median     | 6.0                | null               | null                               | 8.0                | null            | 6819.74            | null         | null                                                                                                     |",
    ///     "| min        | 2.0                | Customer#000000002 | 6LrEaV6KR6PLVcgl2ArL Q3rqzLzcT1 v2 | 1.0                | 11-719-748-3364 | 121.65             | AUTOMOBILE   |  deposits eat slyly ironic, even instructions. express foxes detect slyly. blithely even accounts abov   |",
    ///     "| null_count | 0.0                | 0                  | 0                                  | 0.0                | 0               | 0.0                | 0            | 0                                                                                                        |",
    ///     "| std        | 2.7386127875258306 | null               | null                               | 7.2188026092359046 | null            | 3522.169804254585  | null         | null                                                                                                     |",
    ///     "+------------+--------------------+--------------------+------------------------------------+--------------------+-----------------+--------------------+--------------+----------------------------------------------------------------------------------------------------------+"];
    /// assert_batches_sorted_eq!(expected, &stat.collect().await?);
    /// # Ok(())
    /// # }
    /// ```
    pub async fn describe(self) -> Result<Self> {
        //the functions now supported
        let supported_describe_functions =
            vec!["count", "null_count", "mean", "std", "min", "max", "median"];

        let original_schema_fields = self.schema().fields().iter();

        //define describe column
        let mut describe_schemas = vec![Field::new("describe", DataType::Utf8, false)];
        describe_schemas.extend(original_schema_fields.clone().map(|field| {
            if field.data_type().is_numeric() {
                Field::new(field.name(), DataType::Float64, true)
            } else {
                Field::new(field.name(), DataType::Utf8, true)
            }
        }));

        //collect recordBatch
        let describe_record_batch = [
            // count aggregation
            self.clone().aggregate(
                vec![],
                original_schema_fields
                    .clone()
                    .map(|f| count(ident(f.name())).alias(f.name()))
                    .collect::<Vec<_>>(),
            ),
            // null_count aggregation
            self.clone().aggregate(
                vec![],
                original_schema_fields
                    .clone()
                    .map(|f| {
                        sum(case(is_null(ident(f.name())))
                            .when(lit(true), lit(1))
                            .otherwise(lit(0))
                            .unwrap())
                        .alias(f.name())
                    })
                    .collect::<Vec<_>>(),
            ),
            // mean aggregation
            self.clone().aggregate(
                vec![],
                original_schema_fields
                    .clone()
                    .filter(|f| f.data_type().is_numeric())
                    .map(|f| avg(ident(f.name())).alias(f.name()))
                    .collect::<Vec<_>>(),
            ),
            // std aggregation
            self.clone().aggregate(
                vec![],
                original_schema_fields
                    .clone()
                    .filter(|f| f.data_type().is_numeric())
                    .map(|f| stddev(ident(f.name())).alias(f.name()))
                    .collect::<Vec<_>>(),
            ),
            // min aggregation
            self.clone().aggregate(
                vec![],
                original_schema_fields
                    .clone()
                    .filter(|f| {
                        !matches!(f.data_type(), DataType::Binary | DataType::Boolean)
                    })
                    .map(|f| min(ident(f.name())).alias(f.name()))
                    .collect::<Vec<_>>(),
            ),
            // max aggregation
            self.clone().aggregate(
                vec![],
                original_schema_fields
                    .clone()
                    .filter(|f| {
                        !matches!(f.data_type(), DataType::Binary | DataType::Boolean)
                    })
                    .map(|f| max(ident(f.name())).alias(f.name()))
                    .collect::<Vec<_>>(),
            ),
            // median aggregation
            self.clone().aggregate(
                vec![],
                original_schema_fields
                    .clone()
                    .filter(|f| f.data_type().is_numeric())
                    .map(|f| median(ident(f.name())).alias(f.name()))
                    .collect::<Vec<_>>(),
            ),
        ];

        // first column with function names
        let mut array_ref_vec: Vec<ArrayRef> = vec![Arc::new(StringArray::from(
            supported_describe_functions.clone(),
        ))];
        for field in original_schema_fields {
            let mut array_datas = vec![];
            for result in describe_record_batch.iter() {
                let array_ref = match result {
                    Ok(df) => {
                        let batches = df.clone().collect().await;
                        match batches {
                            Ok(batches)
                                if batches.len() == 1
                                    && batches[0]
                                        .column_by_name(field.name())
                                        .is_some() =>
                            {
                                let column =
                                    batches[0].column_by_name(field.name()).unwrap();

                                if column.data_type().is_null() {
                                    Arc::new(StringArray::from(vec!["null"]))
                                } else if field.data_type().is_numeric() {
                                    cast(column, &DataType::Float64)?
                                } else {
                                    cast(column, &DataType::Utf8)?
                                }
                            }
                            _ => Arc::new(StringArray::from(vec!["null"])),
                        }
                    }
                    //Handling error when only boolean/binary column, and in other cases
                    Err(err)
                        if err.to_string().contains(
                            "Error during planning: \
                                            Aggregate requires at least one grouping \
                                            or aggregate expression",
                        ) =>
                    {
                        Arc::new(StringArray::from(vec!["null"]))
                    }
                    Err(e) => return exec_err!("{}", e),
                };
                array_datas.push(array_ref);
            }
            array_ref_vec.push(concat(
                array_datas
                    .iter()
                    .map(|af| af.as_ref())
                    .collect::<Vec<_>>()
                    .as_slice(),
            )?);
        }

        let describe_record_batch =
            RecordBatch::try_new(Arc::new(Schema::new(describe_schemas)), array_ref_vec)?;

        let provider = MemTable::try_new(
            describe_record_batch.schema(),
            vec![vec![describe_record_batch]],
        )?;

        let plan = LogicalPlanBuilder::scan(
            UNNAMED_TABLE,
            provider_as_source(Arc::new(provider)),
            None,
        )?
        .build()?;

        Ok(DataFrame {
            session_state: self.session_state,
            plan,
            projection_requires_validation: self.projection_requires_validation,
        })
    }

    /// Apply a sort by provided expressions with default direction
    pub fn sort_by(self, expr: Vec<Expr>) -> Result<DataFrame> {
        self.sort(
            expr.into_iter()
                .map(|e| e.sort(true, false))
                .collect::<Vec<SortExpr>>(),
        )
    }

    /// Sort the DataFrame by the specified sorting expressions.
    ///
    /// Note that any expression can be turned into
    /// a sort expression by calling its [sort](Expr::sort) method.
    ///
    /// # Example
    ///
    /// ```
    /// # use datafusion::prelude::*;
    /// # use datafusion::error::Result;
    /// # use datafusion_common::assert_batches_sorted_eq;
    /// # #[tokio::main]
    /// # async fn main() -> Result<()> {
    /// let ctx = SessionContext::new();
    /// let df = ctx
    ///     .read_csv("tests/data/example_long.csv", CsvReadOptions::new())
    ///     .await?;
    /// let df = df.sort(vec![
    ///     col("a").sort(false, true), // a DESC, nulls first
    ///     col("b").sort(true, false), // b ASC, nulls last
    /// ])?;
    /// let expected = vec![
    ///     "+---+---+---+",
    ///     "| a | b | c |",
    ///     "+---+---+---+",
    ///     "| 1 | 2 | 3 |",
    ///     "| 4 | 5 | 6 |",
    ///     "| 7 | 8 | 9 |",
    ///     "+---+---+---+",
    /// ];
    /// # assert_batches_sorted_eq!(expected, &df.collect().await?);
    /// # Ok(())
    /// # }
    /// ```
    pub fn sort(self, expr: Vec<SortExpr>) -> Result<DataFrame> {
        let plan = LogicalPlanBuilder::from(self.plan).sort(expr)?.build()?;
        Ok(DataFrame {
            session_state: self.session_state,
            plan,
            projection_requires_validation: self.projection_requires_validation,
        })
    }

    /// Join this `DataFrame` with another `DataFrame` using explicitly specified
    /// columns and an optional filter expression.
    ///
    /// See [`join_on`](Self::join_on) for a more concise way to specify the
    /// join condition. Since DataFusion will automatically identify and
    /// optimize equality predicates there is no performance difference between
    /// this function and `join_on`
    ///
    /// `left_cols` and `right_cols` are used to form "equijoin" predicates (see
    /// example below), which are then combined with the optional `filter`
    /// expression. If `left_cols` and `right_cols` contain ambiguous column
    /// references, they will be disambiguated by prioritizing the left relation
    /// for `left_cols` and the right relation for `right_cols`.
    ///
    /// Note that in case of outer join, the `filter` is applied to only matched rows.
    ///
    /// # Example
    /// ```
    /// # use datafusion::prelude::*;
    /// # use datafusion::error::Result;
    /// # use datafusion_common::assert_batches_sorted_eq;
    /// # #[tokio::main]
    /// # async fn main() -> Result<()> {
    /// let ctx = SessionContext::new();
    /// let left = ctx
    ///     .read_csv("tests/data/example.csv", CsvReadOptions::new())
    ///     .await?;
    /// let right = ctx
    ///     .read_csv("tests/data/example.csv", CsvReadOptions::new())
    ///     .await?
    ///     .select(vec![
    ///         col("a").alias("a2"),
    ///         col("b").alias("b2"),
    ///         col("c").alias("c2"),
    ///     ])?;
    /// // Perform the equivalent of `left INNER JOIN right ON (a = a2 AND b = b2)`
    /// // finding all pairs of rows from `left` and `right` where `a = a2` and `b = b2`.
    /// let join = left.join(right, JoinType::Inner, &["a", "b"], &["a2", "b2"], None)?;
    /// let expected = vec![
    ///     "+---+---+---+----+----+----+",
    ///     "| a | b | c | a2 | b2 | c2 |",
    ///     "+---+---+---+----+----+----+",
    ///     "| 1 | 2 | 3 | 1  | 2  | 3  |",
    ///     "+---+---+---+----+----+----+",
    /// ];
    /// assert_batches_sorted_eq!(expected, &join.collect().await?);
    /// # Ok(())
    /// # }
    /// ```
    pub fn join(
        self,
        right: DataFrame,
        join_type: JoinType,
        left_cols: &[&str],
        right_cols: &[&str],
        filter: Option<Expr>,
    ) -> Result<DataFrame> {
        let plan = LogicalPlanBuilder::from(self.plan)
            .join(
                right.plan,
                join_type,
                (left_cols.to_vec(), right_cols.to_vec()),
                filter,
            )?
            .build()?;
        Ok(DataFrame {
            session_state: self.session_state,
            plan,
            projection_requires_validation: true,
        })
    }

    /// Join this `DataFrame` with another `DataFrame` using the specified
    /// expressions.
    ///
    /// Note that DataFusion automatically optimizes joins, including
    /// identifying and optimizing equality predicates.
    ///
    /// # Example
    /// ```
    /// # use datafusion::prelude::*;
    /// # use datafusion::error::Result;
    /// # use datafusion_common::assert_batches_sorted_eq;
    /// # #[tokio::main]
    /// # async fn main() -> Result<()> {
    /// let ctx = SessionContext::new();
    /// let left = ctx
    ///     .read_csv("tests/data/example.csv", CsvReadOptions::new())
    ///     .await?;
    /// let right = ctx
    ///     .read_csv("tests/data/example.csv", CsvReadOptions::new())
    ///     .await?
    ///     .select(vec![
    ///         col("a").alias("a2"),
    ///         col("b").alias("b2"),
    ///         col("c").alias("c2"),
    ///     ])?;
    ///
    /// // Perform the equivalent of `left INNER JOIN right ON (a != a2 AND b != b2)`
    /// // finding all pairs of rows from `left` and `right` where
    /// // where `a != a2` and `b != b2`.
    /// let join_on = left.join_on(
    ///     right,
    ///     JoinType::Inner,
    ///     [col("a").not_eq(col("a2")), col("b").not_eq(col("b2"))],
    /// )?;
    /// let expected = vec![
    ///     "+---+---+---+----+----+----+",
    ///     "| a | b | c | a2 | b2 | c2 |",
    ///     "+---+---+---+----+----+----+",
    ///     "+---+---+---+----+----+----+",
    /// ];
    /// # assert_batches_sorted_eq!(expected, &join_on.collect().await?);
    /// # Ok(())
    /// # }
    /// ```
    pub fn join_on(
        self,
        right: DataFrame,
        join_type: JoinType,
        on_exprs: impl IntoIterator<Item = Expr>,
    ) -> Result<DataFrame> {
        let plan = LogicalPlanBuilder::from(self.plan)
            .join_on(right.plan, join_type, on_exprs)?
            .build()?;
        Ok(DataFrame {
            session_state: self.session_state,
            plan,
            projection_requires_validation: true,
        })
    }

    /// Repartition a DataFrame based on a logical partitioning scheme.
    ///
    /// # Example
    /// ```
    /// # use datafusion::prelude::*;
    /// # use datafusion::error::Result;
    /// # use datafusion_common::assert_batches_sorted_eq;
    /// # #[tokio::main]
    /// # async fn main() -> Result<()> {
    /// let ctx = SessionContext::new();
    /// let df = ctx
    ///     .read_csv("tests/data/example_long.csv", CsvReadOptions::new())
    ///     .await?;
    /// let df1 = df.repartition(Partitioning::RoundRobinBatch(4))?;
    /// let expected = vec![
    ///     "+---+---+---+",
    ///     "| a | b | c |",
    ///     "+---+---+---+",
    ///     "| 1 | 2 | 3 |",
    ///     "| 4 | 5 | 6 |",
    ///     "| 7 | 8 | 9 |",
    ///     "+---+---+---+",
    /// ];
    /// # assert_batches_sorted_eq!(expected, &df1.collect().await?);
    /// # Ok(())
    /// # }
    /// ```
    pub fn repartition(self, partitioning_scheme: Partitioning) -> Result<DataFrame> {
        let plan = LogicalPlanBuilder::from(self.plan)
            .repartition(partitioning_scheme)?
            .build()?;
        Ok(DataFrame {
            session_state: self.session_state,
            plan,
            projection_requires_validation: true,
        })
    }

    /// Return the total number of rows in this `DataFrame`.
    ///
    /// Note that this method will actually run a plan to calculate the count,
    /// which may be slow for large or complicated DataFrames.
    ///
    /// # Example
    /// ```
    /// # use datafusion::prelude::*;
    /// # use datafusion::error::Result;
    /// # #[tokio::main]
    /// # async fn main() -> Result<()> {
    /// let ctx = SessionContext::new();
    /// let df = ctx
    ///     .read_csv("tests/data/example.csv", CsvReadOptions::new())
    ///     .await?;
    /// let count = df.count().await?; // 1
    /// # assert_eq!(count, 1);
    /// # Ok(())
    /// # }
    /// ```
    pub async fn count(self) -> Result<usize> {
        let rows = self
            .aggregate(
                vec![],
                vec![count(Expr::Literal(COUNT_STAR_EXPANSION, None))],
            )?
            .collect()
            .await?;
        let len = *rows
            .first()
            .and_then(|r| r.columns().first())
            .and_then(|c| c.as_any().downcast_ref::<Int64Array>())
            .and_then(|a| a.values().first())
            .ok_or_else(|| {
                internal_datafusion_err!("Unexpected output when collecting for count()")
            })? as usize;
        Ok(len)
    }

    /// Execute this `DataFrame` and buffer all resulting `RecordBatch`es  into memory.
    ///
    /// Prior to calling `collect`, modifying a DataFrame simply updates a plan
    /// (no actual computation is performed). `collect` triggers the computation.
    ///
    /// See [`Self::execute_stream`] to execute a DataFrame without buffering.
    ///
    /// # Example
    /// ```
    /// # use datafusion::prelude::*;
    /// # use datafusion::error::Result;
    /// # #[tokio::main]
    /// # async fn main() -> Result<()> {
    /// let ctx = SessionContext::new();
    /// let df = ctx
    ///     .read_csv("tests/data/example.csv", CsvReadOptions::new())
    ///     .await?;
    /// let batches = df.collect().await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn collect(self) -> Result<Vec<RecordBatch>> {
        let task_ctx = Arc::new(self.task_ctx());
        let plan = self.create_physical_plan().await?;
        collect(plan, task_ctx).await
    }

    /// Execute the `DataFrame` and print the results to the console.
    ///
    /// # Example
    /// ```
    /// # use datafusion::prelude::*;
    /// # use datafusion::error::Result;
    /// # #[tokio::main]
    /// # async fn main() -> Result<()> {
    /// let ctx = SessionContext::new();
    /// let df = ctx
    ///     .read_csv("tests/data/example.csv", CsvReadOptions::new())
    ///     .await?;
    /// df.show().await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn show(self) -> Result<()> {
        println!("{}", self.to_string().await?);
        Ok(())
    }

    /// Execute the `DataFrame` and return a string representation of the results.
    ///
    /// # Example
    /// ```
    /// # use datafusion::prelude::*;
    /// # use datafusion::error::Result;
    /// # use datafusion::execution::SessionStateBuilder;
    ///
    /// # #[tokio::main]
    /// # async fn main() -> Result<()> {
    /// let cfg = SessionConfig::new()
    ///     .set_str("datafusion.format.null", "no-value");
    /// let session_state = SessionStateBuilder::new()
    ///     .with_config(cfg)
    ///     .with_default_features()
    ///     .build();
    /// let ctx = SessionContext::new_with_state(session_state);
    /// let df = ctx.sql("select null as 'null-column'").await?;
    /// let result = df.to_string().await?;
    /// assert_eq!(result,
    /// "+-------------+
    /// | null-column |
    /// +-------------+
    /// | no-value    |
    /// +-------------+"
    /// );
    /// # Ok(())
    /// # }
    pub async fn to_string(self) -> Result<String> {
        let options = self.session_state.config().options().format.clone();
        let arrow_options: arrow::util::display::FormatOptions = (&options).try_into()?;

        let results = self.collect().await?;
        Ok(
            pretty::pretty_format_batches_with_options(&results, &arrow_options)?
                .to_string(),
        )
    }

    /// Execute the `DataFrame` and print only the first `num` rows of the
    /// result to the console.
    ///
    /// # Example
    /// ```
    /// # use datafusion::prelude::*;
    /// # use datafusion::error::Result;
    /// # #[tokio::main]
    /// # async fn main() -> Result<()> {
    /// let ctx = SessionContext::new();
    /// let df = ctx
    ///     .read_csv("tests/data/example.csv", CsvReadOptions::new())
    ///     .await?;
    /// df.show_limit(10).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn show_limit(self, num: usize) -> Result<()> {
        let results = self.limit(0, Some(num))?.collect().await?;
        Ok(pretty::print_batches(&results)?)
    }

    /// Return a new [`TaskContext`] which would be used to execute this DataFrame
    pub fn task_ctx(&self) -> TaskContext {
        TaskContext::from(self.session_state.as_ref())
    }

    /// Executes this DataFrame and returns a stream over a single partition
    ///
    /// See [Self::collect] to buffer the `RecordBatch`es in memory.
    ///
    /// # Example
    /// ```
    /// # use datafusion::prelude::*;
    /// # use datafusion::error::Result;
    /// # #[tokio::main]
    /// # async fn main() -> Result<()> {
    /// let ctx = SessionContext::new();
    /// let df = ctx
    ///     .read_csv("tests/data/example.csv", CsvReadOptions::new())
    ///     .await?;
    /// let stream = df.execute_stream().await?;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// # Aborting Execution
    ///
    /// Dropping the stream will abort the execution of the query, and free up
    /// any allocated resources
    pub async fn execute_stream(self) -> Result<SendableRecordBatchStream> {
        let task_ctx = Arc::new(self.task_ctx());
        let plan = self.create_physical_plan().await?;
        execute_stream(plan, task_ctx)
    }

    /// Executes this DataFrame and collects all results into a vector of vector of RecordBatch
    /// maintaining the input partitioning.
    ///
    /// # Example
    /// ```
    /// # use datafusion::prelude::*;
    /// # use datafusion::error::Result;
    /// # #[tokio::main]
    /// # async fn main() -> Result<()> {
    /// let ctx = SessionContext::new();
    /// let df = ctx
    ///     .read_csv("tests/data/example.csv", CsvReadOptions::new())
    ///     .await?;
    /// let batches = df.collect_partitioned().await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn collect_partitioned(self) -> Result<Vec<Vec<RecordBatch>>> {
        let task_ctx = Arc::new(self.task_ctx());
        let plan = self.create_physical_plan().await?;
        collect_partitioned(plan, task_ctx).await
    }

    /// Executes this DataFrame and returns one stream per partition.
    ///
    /// # Example
    /// ```
    /// # use datafusion::prelude::*;
    /// # use datafusion::error::Result;
    /// # #[tokio::main]
    /// # async fn main() -> Result<()> {
    /// let ctx = SessionContext::new();
    /// let df = ctx
    ///     .read_csv("tests/data/example.csv", CsvReadOptions::new())
    ///     .await?;
    /// let batches = df.execute_stream_partitioned().await?;
    /// # Ok(())
    /// # }
    /// ```
    /// # Aborting Execution
    ///
    /// Dropping the stream will abort the execution of the query, and free up
    /// any allocated resources
    pub async fn execute_stream_partitioned(
        self,
    ) -> Result<Vec<SendableRecordBatchStream>> {
        let task_ctx = Arc::new(self.task_ctx());
        let plan = self.create_physical_plan().await?;
        execute_stream_partitioned(plan, task_ctx)
    }

    /// Returns the `DFSchema` describing the output of this DataFrame.
    ///
    /// The output `DFSchema` contains information on the name, data type, and
    /// nullability for each column.
    ///
    /// # Example
    /// ```
    /// # use datafusion::prelude::*;
    /// # use datafusion::error::Result;
    /// # #[tokio::main]
    /// # async fn main() -> Result<()> {
    /// let ctx = SessionContext::new();
    /// let df = ctx
    ///     .read_csv("tests/data/example.csv", CsvReadOptions::new())
    ///     .await?;
    /// let schema = df.schema();
    /// # Ok(())
    /// # }
    /// ```
    pub fn schema(&self) -> &DFSchema {
        self.plan.schema()
    }

    /// Return a reference to the unoptimized [`LogicalPlan`] that comprises
    /// this DataFrame.
    ///
    /// See [`Self::into_unoptimized_plan`] for more details.
    pub fn logical_plan(&self) -> &LogicalPlan {
        &self.plan
    }

    /// Returns both the [`LogicalPlan`] and [`SessionState`] that comprise this [`DataFrame`]
    pub fn into_parts(self) -> (SessionState, LogicalPlan) {
        (*self.session_state, self.plan)
    }

    /// Return the [`LogicalPlan`] represented by this DataFrame without running
    /// any optimizers
    ///
    /// Note: This method should not be used outside testing, as it loses the
    /// snapshot of the [`SessionState`] attached to this [`DataFrame`] and
    /// consequently subsequent operations may take place against a different
    /// state (e.g. a different value of `now()`)
    ///
    /// See [`Self::into_parts`] to retrieve the owned [`LogicalPlan`] and
    /// corresponding [`SessionState`].
    pub fn into_unoptimized_plan(self) -> LogicalPlan {
        self.plan
    }

    /// Return the optimized [`LogicalPlan`] represented by this DataFrame.
    ///
    /// Note: This method should not be used outside testing -- see
    /// [`Self::into_unoptimized_plan`] for more details.
    pub fn into_optimized_plan(self) -> Result<LogicalPlan> {
        // Optimize the plan first for better UX
        self.session_state.optimize(&self.plan)
    }

    /// Converts this [`DataFrame`] into a [`TableProvider`] that can be registered
    /// as a table view using [`SessionContext::register_table`].
    ///
    /// Note: This discards the [`SessionState`] associated with this
    /// [`DataFrame`] in favour of the one passed to [`TableProvider::scan`]
    pub fn into_view(self) -> Arc<dyn TableProvider> {
        Arc::new(DataFrameTableProvider {
            plan: self.plan,
            table_type: TableType::View,
        })
    }

    /// See [`Self::into_view`]. The returned [`TableProvider`] will
    /// create a transient table.
    pub fn into_temporary_view(self) -> Arc<dyn TableProvider> {
        Arc::new(DataFrameTableProvider {
            plan: self.plan,
            table_type: TableType::Temporary,
        })
    }

    /// Return a DataFrame with the explanation of its plan so far.
    ///
    /// if `analyze` is specified, runs the plan and reports metrics
    /// if `verbose` is true, prints out additional details.
    /// The default format is Indent format.
    ///
    /// ```
    /// # use datafusion::prelude::*;
    /// # use datafusion::error::Result;
    /// # #[tokio::main]
    /// # async fn main() -> Result<()> {
    /// let ctx = SessionContext::new();
    /// let df = ctx
    ///     .read_csv("tests/data/example.csv", CsvReadOptions::new())
    ///     .await?;
    /// let batches = df
    ///     .limit(0, Some(100))?
    ///     .explain(false, false)?
    ///     .collect()
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn explain(self, verbose: bool, analyze: bool) -> Result<DataFrame> {
        // Set the default format to Indent to keep the previous behavior
        let opts = ExplainOption::default()
            .with_verbose(verbose)
            .with_analyze(analyze);
        self.explain_with_options(opts)
    }

    /// Return a DataFrame with the explanation of its plan so far.
    ///
    /// `opt` is used to specify the options for the explain operation.
    /// Details of the options can be found in [`ExplainOption`].
    /// ```
    /// # use datafusion::prelude::*;
    /// # use datafusion::error::Result;
    /// # #[tokio::main]
    /// # async fn main() -> Result<()> {
    /// use datafusion_expr::{Explain, ExplainOption};
    /// let ctx = SessionContext::new();
    /// let df = ctx
    ///     .read_csv("tests/data/example.csv", CsvReadOptions::new())
    ///     .await?;
    /// let batches = df
    ///     .limit(0, Some(100))?
    ///     .explain_with_options(
    ///         ExplainOption::default()
    ///             .with_verbose(false)
    ///             .with_analyze(false),
    ///     )?
    ///     .collect()
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn explain_with_options(
        self,
        explain_option: ExplainOption,
    ) -> Result<DataFrame> {
        if matches!(self.plan, LogicalPlan::Explain(_)) {
            return plan_err!("Nested EXPLAINs are not supported");
        }
        let plan = LogicalPlanBuilder::from(self.plan)
            .explain_option_format(explain_option)?
            .build()?;
        Ok(DataFrame {
            session_state: self.session_state,
            plan,
            projection_requires_validation: self.projection_requires_validation,
        })
    }

    /// Return a `FunctionRegistry` used to plan udf's calls
    ///
    /// # Example
    /// ```
    /// # use datafusion::prelude::*;
    /// # use datafusion::error::Result;
    /// # #[tokio::main]
    /// # async fn main() -> Result<()> {
    /// let ctx = SessionContext::new();
    /// let df = ctx
    ///     .read_csv("tests/data/example.csv", CsvReadOptions::new())
    ///     .await?;
    /// let f = df.registry();
    /// // use f.udf("name", vec![...]) to use the udf
    /// # Ok(())
    /// # }
    /// ```
    pub fn registry(&self) -> &dyn FunctionRegistry {
        self.session_state.as_ref()
    }

    /// Calculate the intersection of two [`DataFrame`]s.  The two [`DataFrame`]s must have exactly the same schema
    ///
    /// ```
    /// # use datafusion::prelude::*;
    /// # use datafusion::error::Result;
    /// # use datafusion_common::assert_batches_sorted_eq;
    /// # #[tokio::main]
    /// # async fn main() -> Result<()> {
    /// let ctx = SessionContext::new();
    /// let df = ctx
    ///     .read_csv("tests/data/example.csv", CsvReadOptions::new())
    ///     .await?;
    /// let d2 = ctx
    ///     .read_csv("tests/data/example_long.csv", CsvReadOptions::new())
    ///     .await?;
    /// let df = df.intersect(d2)?;
    /// let expected = vec![
    ///     "+---+---+---+",
    ///     "| a | b | c |",
    ///     "+---+---+---+",
    ///     "| 1 | 2 | 3 |",
    ///     "+---+---+---+",
    /// ];
    /// # assert_batches_sorted_eq!(expected, &df.collect().await?);
    /// # Ok(())
    /// # }
    /// ```
    pub fn intersect(self, dataframe: DataFrame) -> Result<DataFrame> {
        let left_plan = self.plan;
        let right_plan = dataframe.plan;
        let plan = LogicalPlanBuilder::intersect(left_plan, right_plan, true)?;
        Ok(DataFrame {
            session_state: self.session_state,
            plan,
            projection_requires_validation: true,
        })
    }

    /// Calculate the distinct intersection of two [`DataFrame`]s.  The two [`DataFrame`]s must have exactly the same schema
    ///
    /// ```
    /// # use datafusion::prelude::*;
    /// # use datafusion::error::Result;
    /// # use datafusion_common::assert_batches_sorted_eq;
    /// # #[tokio::main]
    /// # async fn main() -> Result<()> {
    /// let ctx = SessionContext::new();
    /// let df = ctx
    ///     .read_csv("tests/data/example.csv", CsvReadOptions::new())
    ///     .await?;
    /// let d2 = ctx
    ///     .read_csv("tests/data/example_long.csv", CsvReadOptions::new())
    ///     .await?;
    /// let df = df.intersect_distinct(d2)?;
    /// let expected = vec![
    ///     "+---+---+---+",
    ///     "| a | b | c |",
    ///     "+---+---+---+",
    ///     "| 1 | 2 | 3 |",
    ///     "+---+---+---+",
    /// ];
    /// # assert_batches_sorted_eq!(expected, &df.collect().await?);
    /// # Ok(())
    /// # }
    /// ```
    pub fn intersect_distinct(self, dataframe: DataFrame) -> Result<DataFrame> {
        let left_plan = self.plan;
        let right_plan = dataframe.plan;
        let plan = LogicalPlanBuilder::intersect(left_plan, right_plan, false)?;
        Ok(DataFrame {
            session_state: self.session_state,
            plan,
            projection_requires_validation: true,
        })
    }

    /// Calculate the exception of two [`DataFrame`]s.  The two [`DataFrame`]s must have exactly the same schema
    ///
    /// ```
    /// # use datafusion::prelude::*;
    /// # use datafusion::error::Result;
    /// # use datafusion_common::assert_batches_sorted_eq;
    /// # #[tokio::main]
    /// # async fn main() -> Result<()> {
    /// let ctx = SessionContext::new();
    /// let df = ctx
    ///     .read_csv("tests/data/example_long.csv", CsvReadOptions::new())
    ///     .await?;
    /// let d2 = ctx
    ///     .read_csv("tests/data/example.csv", CsvReadOptions::new())
    ///     .await?;
    /// let result = df.except(d2)?;
    /// // those columns are not in example.csv, but in example_long.csv
    /// let expected = vec![
    ///     "+---+---+---+",
    ///     "| a | b | c |",
    ///     "+---+---+---+",
    ///     "| 4 | 5 | 6 |",
    ///     "| 7 | 8 | 9 |",
    ///     "+---+---+---+",
    /// ];
    /// # assert_batches_sorted_eq!(expected, &result.collect().await?);
    /// # Ok(())
    /// # }
    /// ```
    pub fn except(self, dataframe: DataFrame) -> Result<DataFrame> {
        let left_plan = self.plan;
        let right_plan = dataframe.plan;
        let plan = LogicalPlanBuilder::except(left_plan, right_plan, true)?;
        Ok(DataFrame {
            session_state: self.session_state,
            plan,
            projection_requires_validation: true,
        })
    }

    /// Calculate the distinct exception of two [`DataFrame`]s.  The two [`DataFrame`]s must have exactly the same schema
    ///
    /// ```
    /// # use datafusion::prelude::*;
    /// # use datafusion::error::Result;
    /// # use datafusion_common::assert_batches_sorted_eq;
    /// # #[tokio::main]
    /// # async fn main() -> Result<()> {
    /// let ctx = SessionContext::new();
    /// let df = ctx
    ///     .read_csv("tests/data/example_long.csv", CsvReadOptions::new())
    ///     .await?;
    /// let d2 = ctx
    ///     .read_csv("tests/data/example.csv", CsvReadOptions::new())
    ///     .await?;
    /// let result = df.except_distinct(d2)?;
    /// // those columns are not in example.csv, but in example_long.csv
    /// let expected = vec![
    ///     "+---+---+---+",
    ///     "| a | b | c |",
    ///     "+---+---+---+",
    ///     "| 4 | 5 | 6 |",
    ///     "| 7 | 8 | 9 |",
    ///     "+---+---+---+",
    /// ];
    /// # assert_batches_sorted_eq!(expected, &result.collect().await?);
    /// # Ok(())
    /// # }
    /// ```
    pub fn except_distinct(self, dataframe: DataFrame) -> Result<DataFrame> {
        let left_plan = self.plan;
        let right_plan = dataframe.plan;
        let plan = LogicalPlanBuilder::except(left_plan, right_plan, false)?;
        Ok(DataFrame {
            session_state: self.session_state,
            plan,
            projection_requires_validation: true,
        })
    }

    /// Execute this `DataFrame` and write the results to `table_name`.
    ///
    /// Returns a single [RecordBatch] containing a single column and
    /// row representing the count of total rows written.
    ///
    /// Unlike most other `DataFrame` methods, this method executes eagerly.
    /// Data is written to the table using the [`TableProvider::insert_into`]
    /// method. This is the same underlying implementation used by SQL `INSERT
    /// INTO` statements.
    pub async fn write_table(
        self,
        table_name: &str,
        write_options: DataFrameWriteOptions,
    ) -> Result<Vec<RecordBatch>, DataFusionError> {
        let plan = if write_options.sort_by.is_empty() {
            self.plan
        } else {
            LogicalPlanBuilder::from(self.plan)
                .sort(write_options.sort_by)?
                .build()?
        };

        let table_ref: TableReference = table_name.into();
        let table_schema = self.session_state.schema_for_ref(table_ref.clone())?;
        let target = match table_schema.table(table_ref.table()).await? {
            Some(ref provider) => Ok(Arc::clone(provider)),
            _ => plan_err!("No table named '{table_name}'"),
        }?;

        let target = Arc::new(DefaultTableSource::new(target));

        let plan = LogicalPlanBuilder::insert_into(
            plan,
            table_ref,
            target,
            write_options.insert_op,
        )?
        .build()?;

        DataFrame {
            session_state: self.session_state,
            plan,
            projection_requires_validation: self.projection_requires_validation,
        }
        .collect()
        .await
    }

    /// Execute the `DataFrame` and write the results to CSV file(s).
    ///
    /// # Example
    /// ```
    /// # use datafusion::prelude::*;
    /// # use datafusion::error::Result;
    /// # use std::fs;
    /// # #[tokio::main]
    /// # async fn main() -> Result<()> {
    /// use datafusion::dataframe::DataFrameWriteOptions;
    /// let ctx = SessionContext::new();
    /// // Sort the data by column "b" and write it to a new location
    /// ctx.read_csv("tests/data/example.csv", CsvReadOptions::new())
    ///     .await?
    ///     .sort(vec![col("b").sort(true, true)])? // sort by b asc, nulls first
    ///     .write_csv(
    ///         "output.csv",
    ///         DataFrameWriteOptions::new(),
    ///         None, // can also specify CSV writing options here
    ///     )
    ///     .await?;
    /// # fs::remove_file("output.csv")?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn write_csv(
        self,
        path: &str,
        options: DataFrameWriteOptions,
        writer_options: Option<CsvOptions>,
    ) -> Result<Vec<RecordBatch>, DataFusionError> {
        if options.insert_op != InsertOp::Append {
            return not_impl_err!(
                "{} is not implemented for DataFrame::write_csv.",
                options.insert_op
            );
        }

        let format = if let Some(csv_opts) = writer_options {
            Arc::new(CsvFormatFactory::new_with_options(csv_opts))
        } else {
            Arc::new(CsvFormatFactory::new())
        };

        let file_type = format_as_file_type(format);

        let plan = if options.sort_by.is_empty() {
            self.plan
        } else {
            LogicalPlanBuilder::from(self.plan)
                .sort(options.sort_by)?
                .build()?
        };

        let plan = LogicalPlanBuilder::copy_to(
            plan,
            path.into(),
            file_type,
            HashMap::new(),
            options.partition_by,
        )?
        .build()?;

        DataFrame {
            session_state: self.session_state,
            plan,
            projection_requires_validation: self.projection_requires_validation,
        }
        .collect()
        .await
    }

    /// Execute the `DataFrame` and write the results to JSON file(s).
    ///
    /// # Example
    /// ```
    /// # use datafusion::prelude::*;
    /// # use datafusion::error::Result;
    /// # use std::fs;
    /// # #[tokio::main]
    /// # async fn main() -> Result<()> {
    /// use datafusion::dataframe::DataFrameWriteOptions;
    /// let ctx = SessionContext::new();
    /// // Sort the data by column "b" and write it to a new location
    /// ctx.read_csv("tests/data/example.csv", CsvReadOptions::new())
    ///     .await?
    ///     .sort(vec![col("b").sort(true, true)])? // sort by b asc, nulls first
    ///     .write_json("output.json", DataFrameWriteOptions::new(), None)
    ///     .await?;
    /// # fs::remove_file("output.json")?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn write_json(
        self,
        path: &str,
        options: DataFrameWriteOptions,
        writer_options: Option<JsonOptions>,
    ) -> Result<Vec<RecordBatch>, DataFusionError> {
        if options.insert_op != InsertOp::Append {
            return not_impl_err!(
                "{} is not implemented for DataFrame::write_json.",
                options.insert_op
            );
        }

        let format = if let Some(json_opts) = writer_options {
            Arc::new(JsonFormatFactory::new_with_options(json_opts))
        } else {
            Arc::new(JsonFormatFactory::new())
        };

        let file_type = format_as_file_type(format);

        let plan = if options.sort_by.is_empty() {
            self.plan
        } else {
            LogicalPlanBuilder::from(self.plan)
                .sort(options.sort_by)?
                .build()?
        };

        let plan = LogicalPlanBuilder::copy_to(
            plan,
            path.into(),
            file_type,
            Default::default(),
            options.partition_by,
        )?
        .build()?;

        DataFrame {
            session_state: self.session_state,
            plan,
            projection_requires_validation: self.projection_requires_validation,
        }
        .collect()
        .await
    }

    /// Add or replace a column in the DataFrame.
    ///
    /// # Example
    /// ```
    /// # use datafusion::prelude::*;
    /// # use datafusion::error::Result;
    /// # #[tokio::main]
    /// # async fn main() -> Result<()> {
    /// let ctx = SessionContext::new();
    /// let df = ctx
    ///     .read_csv("tests/data/example.csv", CsvReadOptions::new())
    ///     .await?;
    /// let df = df.with_column("ab_sum", col("a") + col("b"))?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn with_column(self, name: &str, expr: Expr) -> Result<DataFrame> {
        let window_func_exprs = find_window_exprs([&expr]);

        let original_names: HashSet<String> = self
            .plan
            .schema()
            .iter()
            .map(|(_, f)| f.name().clone())
            .collect();

        // Maybe build window plan
        let plan = if window_func_exprs.is_empty() {
            self.plan
        } else {
            LogicalPlanBuilder::window_plan(self.plan, window_func_exprs)?
        };

        let new_column = expr.alias(name);
        let mut col_exists = false;

        let mut fields: Vec<(Expr, bool)> = plan
            .schema()
            .iter()
            .filter_map(|(qualifier, field)| {
                // Skip new fields introduced by window_plan
                if !original_names.contains(field.name()) {
                    return None;
                }

                if field.name() == name {
                    col_exists = true;
                    Some((new_column.clone(), true))
                } else {
                    let e = col(Column::from((qualifier, field)));
                    Some((e, self.projection_requires_validation))
                }
            })
            .collect();

        if !col_exists {
            fields.push((new_column, true));
        }

        let project_plan = LogicalPlanBuilder::from(plan)
            .project_with_validation(fields)?
            .build()?;

        Ok(DataFrame {
            session_state: self.session_state,
            plan: project_plan,
            projection_requires_validation: false,
        })
    }

    /// Rename one column by applying a new projection. This is a no-op if the column to be
    /// renamed does not exist.
    ///
    /// The method supports case sensitive rename with wrapping column name into one of following symbols (  "  or  '  or  `  )
    ///
    /// Alternatively setting DataFusion param `datafusion.sql_parser.enable_ident_normalization` to `false` will enable
    /// case sensitive rename without need to wrap column name into special symbols
    ///
    /// # Example
    /// ```
    /// # use datafusion::prelude::*;
    /// # use datafusion::error::Result;
    /// # #[tokio::main]
    /// # async fn main() -> Result<()> {
    /// let ctx = SessionContext::new();
    /// let df = ctx
    ///     .read_csv("tests/data/example.csv", CsvReadOptions::new())
    ///     .await?;
    /// let df = df.with_column_renamed("ab_sum", "total")?;
    ///
    /// # Ok(())
    /// # }
    /// ```
    pub fn with_column_renamed(
        self,
        old_name: impl Into<String>,
        new_name: &str,
    ) -> Result<DataFrame> {
        let ident_opts = self
            .session_state
            .config_options()
            .sql_parser
            .enable_ident_normalization;
        let old_column: Column = if ident_opts {
            Column::from_qualified_name(old_name)
        } else {
            Column::from_qualified_name_ignore_case(old_name)
        };

        let (qualifier_rename, field_rename) =
            match self.plan.schema().qualified_field_from_column(&old_column) {
                Ok(qualifier_and_field) => qualifier_and_field,
                // no-op if field not found
                Err(DataFusionError::SchemaError(e, _))
                    if matches!(*e, SchemaError::FieldNotFound { .. }) =>
                {
                    return Ok(self);
                }
                Err(err) => return Err(err),
            };
        let projection = self
            .plan
            .schema()
            .iter()
            .map(|(qualifier, field)| {
                if qualifier.eq(&qualifier_rename) && field == field_rename {
                    (
                        col(Column::from((qualifier, field)))
                            .alias_qualified(qualifier.cloned(), new_name),
                        false,
                    )
                } else {
                    (col(Column::from((qualifier, field))), false)
                }
            })
            .collect::<Vec<_>>();
        let project_plan = LogicalPlanBuilder::from(self.plan)
            .project_with_validation(projection)?
            .build()?;
        Ok(DataFrame {
            session_state: self.session_state,
            plan: project_plan,
            projection_requires_validation: false,
        })
    }

    /// Replace all parameters in logical plan with the specified
    /// values, in preparation for execution.
    ///
    /// # Example
    ///
    /// ```
    /// use datafusion::prelude::*;
    /// # use datafusion::{error::Result, assert_batches_eq};
    /// # #[tokio::main]
    /// # async fn main() -> Result<()> {
    /// # use datafusion_common::ScalarValue;
    /// let ctx = SessionContext::new();
    /// # ctx.register_csv("example", "tests/data/example.csv", CsvReadOptions::new()).await?;
    /// let results = ctx
    ///   .sql("SELECT a FROM example WHERE b = $1")
    ///   .await?
    ///    // replace $1 with value 2
    ///   .with_param_values(vec![
    ///      // value at index 0 --> $1
    ///      ScalarValue::from(2i64)
    ///    ])?
    ///   .collect()
    ///   .await?;
    /// assert_batches_eq!(
    ///  &[
    ///    "+---+",
    ///    "| a |",
    ///    "+---+",
    ///    "| 1 |",
    ///    "+---+",
    ///  ],
    ///  &results
    /// );
    /// // Note you can also provide named parameters
    /// let results = ctx
    ///   .sql("SELECT a FROM example WHERE b = $my_param")
    ///   .await?
    ///    // replace $my_param with value 2
    ///    // Note you can also use a HashMap as well
    ///   .with_param_values(vec![
    ///       ("my_param", ScalarValue::from(2i64))
    ///    ])?
    ///   .collect()
    ///   .await?;
    /// assert_batches_eq!(
    ///  &[
    ///    "+---+",
    ///    "| a |",
    ///    "+---+",
    ///    "| 1 |",
    ///    "+---+",
    ///  ],
    ///  &results
    /// );
    /// # Ok(())
    /// # }
    /// ```
    pub fn with_param_values(self, query_values: impl Into<ParamValues>) -> Result<Self> {
        let plan = self.plan.with_param_values(query_values)?;
        Ok(DataFrame {
            session_state: self.session_state,
            plan,
            projection_requires_validation: self.projection_requires_validation,
        })
    }

    /// Cache DataFrame as a memory table.
    ///
    /// Default behavior could be changed using
    /// a [`crate::execution::session_state::CacheFactory`]
    /// configured via [`SessionState`].
    ///
    /// ```
    /// # use datafusion::prelude::*;
    /// # use datafusion::error::Result;
    /// # #[tokio::main]
    /// # async fn main() -> Result<()> {
    /// let ctx = SessionContext::new();
    /// let df = ctx
    ///     .read_csv("tests/data/example.csv", CsvReadOptions::new())
    ///     .await?;
    /// let df = df.cache().await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn cache(self) -> Result<DataFrame> {
        if let Some(cache_factory) = self.session_state.cache_factory() {
            let new_plan =
                cache_factory.create(self.plan, self.session_state.as_ref())?;
            Ok(Self::new(*self.session_state, new_plan))
        } else {
            let context = SessionContext::new_with_state((*self.session_state).clone());
            // The schema is consistent with the output
            let plan = self.clone().create_physical_plan().await?;
            let schema = plan.schema();
            let task_ctx = Arc::new(self.task_ctx());
            let partitions = collect_partitioned(plan, task_ctx).await?;
            let mem_table = MemTable::try_new(schema, partitions)?;
            context.read_table(Arc::new(mem_table))
        }
    }

    /// Apply an alias to the DataFrame.
    ///
    /// This method replaces the qualifiers of output columns with the given alias.
    pub fn alias(self, alias: &str) -> Result<DataFrame> {
        let plan = LogicalPlanBuilder::from(self.plan).alias(alias)?.build()?;
        Ok(DataFrame {
            session_state: self.session_state,
            plan,
            projection_requires_validation: self.projection_requires_validation,
        })
    }

    /// Fill null values in specified columns with a given value
    /// If no columns are specified (empty vector), applies to all columns
    /// Only fills if the value can be cast to the column's type
    ///
    /// # Arguments
    /// * `value` - Value to fill nulls with
    /// * `columns` - List of column names to fill. If empty, fills all columns.
    ///
    /// # Example
    /// ```
    /// # use datafusion::prelude::*;
    /// # use datafusion::error::Result;
    /// # use datafusion_common::ScalarValue;
    /// # #[tokio::main]
    /// # async fn main() -> Result<()> {
    /// let ctx = SessionContext::new();
    /// let df = ctx
    ///     .read_csv("tests/data/example.csv", CsvReadOptions::new())
    ///     .await?;
    /// // Fill nulls in only columns "a" and "c":
    /// let df = df.fill_null(ScalarValue::from(0), vec!["a".to_owned(), "c".to_owned()])?;
    /// // Fill nulls across all columns:
    /// let df = df.fill_null(ScalarValue::from(0), vec![])?;
    /// # Ok(())
    /// # }
    /// ```
    #[expect(clippy::needless_pass_by_value)]
    pub fn fill_null(
        &self,
        value: ScalarValue,
        columns: Vec<String>,
    ) -> Result<DataFrame> {
        let cols = if columns.is_empty() {
            self.logical_plan()
                .schema()
                .fields()
                .iter()
                .map(Arc::clone)
                .collect()
        } else {
            self.find_columns(&columns)?
        };

        // Create projections for each column
        let projections = self
            .logical_plan()
            .schema()
            .fields()
            .iter()
            .map(|field| {
                if cols.contains(field) {
                    // Try to cast fill value to column type. If the cast fails, fallback to the original column.
                    match value.clone().cast_to(field.data_type()) {
                        Ok(fill_value) => Expr::Alias(Alias {
                            expr: Box::new(Expr::ScalarFunction(ScalarFunction {
                                func: coalesce(),
                                args: vec![col(field.name()), lit(fill_value)],
                            })),
                            relation: None,
                            name: field.name().to_string(),
                            metadata: None,
                        }),
                        Err(_) => col(field.name()),
                    }
                } else {
                    col(field.name())
                }
            })
            .collect::<Vec<_>>();

        self.clone().select(projections)
    }

    // Helper to find columns from names
    fn find_columns(&self, names: &[String]) -> Result<Vec<FieldRef>> {
        let schema = self.logical_plan().schema();
        names
            .iter()
            .map(|name| {
                schema
                    .field_with_name(None, name)
                    .cloned()
                    .map_err(|_| plan_datafusion_err!("Column '{}' not found", name))
            })
            .collect()
    }

    /// Helper for creating DataFrame.
    /// # Example
    /// ```
    /// use arrow::array::{ArrayRef, Int32Array, StringArray};
    /// use datafusion::prelude::DataFrame;
    /// use std::sync::Arc;
    /// let id: ArrayRef = Arc::new(Int32Array::from(vec![1, 2, 3]));
    /// let name: ArrayRef = Arc::new(StringArray::from(vec!["foo", "bar", "baz"]));
    /// let df = DataFrame::from_columns(vec![("id", id), ("name", name)]).unwrap();
    /// // +----+------+,
    /// // | id | name |,
    /// // +----+------+,
    /// // | 1  | foo  |,
    /// // | 2  | bar  |,
    /// // | 3  | baz  |,
    /// // +----+------+,
    /// ```
    pub fn from_columns(columns: Vec<(&str, ArrayRef)>) -> Result<Self> {
        let fields = columns
            .iter()
            .map(|(name, array)| Field::new(*name, array.data_type().clone(), true))
            .collect::<Vec<_>>();

        let arrays = columns
            .into_iter()
            .map(|(_, array)| array)
            .collect::<Vec<_>>();

        let schema = Arc::new(Schema::new(fields));
        let batch = RecordBatch::try_new(schema, arrays)?;
        let ctx = SessionContext::new();
        let df = ctx.read_batch(batch)?;
        Ok(df)
    }
}

/// Macro for creating DataFrame.
/// # Example
/// ```
/// use datafusion::prelude::dataframe;
/// # use datafusion::error::Result;
/// # #[tokio::main]
/// # async fn main() -> Result<()> {
/// let df = dataframe!(
///    "id" => [1, 2, 3],
///    "name" => ["foo", "bar", "baz"]
///  )?;
/// df.show().await?;
/// // +----+------+,
/// // | id | name |,
/// // +----+------+,
/// // | 1  | foo  |,
/// // | 2  | bar  |,
/// // | 3  | baz  |,
/// // +----+------+,
/// let df_empty = dataframe!()?; // empty DataFrame
/// assert_eq!(df_empty.schema().fields().len(), 0);
/// assert_eq!(df_empty.count().await?, 0);
/// # Ok(())
/// # }
/// ```
#[macro_export]
macro_rules! dataframe {
    () => {{
        use std::sync::Arc;

        use datafusion::prelude::SessionContext;
        use datafusion::arrow::array::RecordBatch;
        use datafusion::arrow::datatypes::Schema;

        let ctx = SessionContext::new();
        let batch = RecordBatch::new_empty(Arc::new(Schema::empty()));
        ctx.read_batch(batch)
    }};

    ($($name:expr => $data:expr),+ $(,)?) => {{
        use datafusion::prelude::DataFrame;
        use datafusion::common::test_util::IntoArrayRef;

        let columns = vec![
            $(
                ($name, $data.into_array_ref()),
            )+
        ];

        DataFrame::from_columns(columns)
    }};
}

#[derive(Debug)]
struct DataFrameTableProvider {
    plan: LogicalPlan,
    table_type: TableType,
}

#[async_trait]
impl TableProvider for DataFrameTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn get_logical_plan(&self) -> Option<Cow<'_, LogicalPlan>> {
        Some(Cow::Borrowed(&self.plan))
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> Result<Vec<TableProviderFilterPushDown>> {
        // A filter is added on the DataFrame when given
        Ok(vec![TableProviderFilterPushDown::Exact; filters.len()])
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(self.plan.schema().inner())
    }

    fn table_type(&self) -> TableType {
        self.table_type
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let mut expr = LogicalPlanBuilder::from(self.plan.clone());
        // Add filter when given
        let filter = filters.iter().cloned().reduce(|acc, new| acc.and(new));
        if let Some(filter) = filter {
            expr = expr.filter(filter)?
        }

        if let Some(p) = projection {
            expr = expr.select(p.iter().copied())?
        }

        // add a limit if given
        if let Some(l) = limit {
            expr = expr.limit(0, Some(l))?
        }
        let plan = expr.build()?;
        state.create_physical_plan(&plan).await
    }
}

// see tests in datafusion/core/tests/dataframe/mod.rs:2816
