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

//! DataFrame API for building and executing query plans.

use crate::arrow::datatypes::Schema;
use crate::arrow::datatypes::SchemaRef;
use crate::arrow::record_batch::RecordBatch;
use crate::arrow::util::pretty;
use crate::datasource::{MemTable, TableProvider};
use crate::error::Result;
use crate::execution::{
    context::{SessionState, TaskContext},
    FunctionRegistry,
};
use crate::logical_expr::{utils::find_window_exprs, TableType};
use crate::logical_plan::{
    col, DFSchema, Expr, JoinType, LogicalPlan, LogicalPlanBuilder, Partitioning,
};
use crate::physical_plan::file_format::{plan_to_csv, plan_to_json, plan_to_parquet};
use crate::physical_plan::SendableRecordBatchStream;
use crate::physical_plan::{collect, collect_partitioned};
use crate::physical_plan::{execute_stream, execute_stream_partitioned, ExecutionPlan};
use crate::prelude::SessionContext;
use crate::scalar::ScalarValue;
use async_trait::async_trait;
use parking_lot::RwLock;
use parquet::file::properties::WriterProperties;
use std::any::Any;
use std::sync::Arc;

/// DataFrame represents a logical set of rows with the same named columns.
/// Similar to a [Pandas DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html) or
/// [Spark DataFrame](https://spark.apache.org/docs/latest/sql-programming-guide.html)
///
/// DataFrames are typically created by the `read_csv` and `read_parquet` methods on the
/// [SessionContext](../execution/context/struct.SessionContext.html) and can then be modified
/// by calling the transformation methods, such as `filter`, `select`, `aggregate`, and `limit`
/// to build up a query definition.
///
/// The query can be executed by calling the `collect` method.
///
/// ```
/// # use datafusion::prelude::*;
/// # use datafusion::error::Result;
/// # #[tokio::main]
/// # async fn main() -> Result<()> {
/// let ctx = SessionContext::new();
/// let df = ctx.read_csv("tests/example.csv", CsvReadOptions::new()).await?;
/// let df = df.filter(col("a").lt_eq(col("b")))?
///            .aggregate(vec![col("a")], vec![min(col("b"))])?
///            .limit(0, Some(100))?;
/// let results = df.collect();
/// # Ok(())
/// # }
/// ```
#[derive(Debug)]
pub struct DataFrame {
    session_state: Arc<RwLock<SessionState>>,
    plan: LogicalPlan,
}

impl DataFrame {
    /// Create a new Table based on an existing logical plan
    pub fn new(session_state: Arc<RwLock<SessionState>>, plan: &LogicalPlan) -> Self {
        Self {
            session_state,
            plan: plan.clone(),
        }
    }

    /// Create a physical plan
    pub async fn create_physical_plan(&self) -> Result<Arc<dyn ExecutionPlan>> {
        // this function is copied from SessionContext function of the
        // same name
        let state_cloned = {
            let mut state = self.session_state.write();
            state.execution_props.start_execution();

            // We need to clone `state` to release the lock that is not `Send`. We could
            // make the lock `Send` by using `tokio::sync::Mutex`, but that would require to
            // propagate async even to the `LogicalPlan` building methods.
            // Cloning `state` here is fine as we then pass it as immutable `&state`, which
            // means that we avoid write consistency issues as the cloned version will not
            // be written to. As for eventual modifications that would be applied to the
            // original state after it has been cloned, they will not be picked up by the
            // clone but that is okay, as it is equivalent to postponing the state update
            // by keeping the lock until the end of the function scope.
            state.clone()
        };

        state_cloned.create_physical_plan(&self.plan).await
    }

    /// Filter the DataFrame by column. Returns a new DataFrame only containing the
    /// specified columns.
    ///
    /// ```
    /// # use datafusion::prelude::*;
    /// # use datafusion::error::Result;
    /// # #[tokio::main]
    /// # async fn main() -> Result<()> {
    /// let ctx = SessionContext::new();
    /// let df = ctx.read_csv("tests/example.csv", CsvReadOptions::new()).await?;
    /// let df = df.select_columns(&["a", "b"])?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn select_columns(&self, columns: &[&str]) -> Result<Arc<DataFrame>> {
        let fields = columns
            .iter()
            .map(|name| self.plan.schema().field_with_unqualified_name(name))
            .collect::<Result<Vec<_>>>()?;
        let expr: Vec<Expr> = fields.iter().map(|f| col(f.name())).collect();
        self.select(expr)
    }

    /// Create a projection based on arbitrary expressions.
    ///
    /// ```
    /// # use datafusion::prelude::*;
    /// # use datafusion::error::Result;
    /// # #[tokio::main]
    /// # async fn main() -> Result<()> {
    /// let ctx = SessionContext::new();
    /// let df = ctx.read_csv("tests/example.csv", CsvReadOptions::new()).await?;
    /// let df = df.select(vec![col("a") * col("b"), col("c")])?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn select(&self, expr_list: Vec<Expr>) -> Result<Arc<DataFrame>> {
        let window_func_exprs = find_window_exprs(&expr_list);
        let plan = if window_func_exprs.is_empty() {
            self.plan.clone()
        } else {
            LogicalPlanBuilder::window_plan(self.plan.clone(), window_func_exprs)?
        };
        let project_plan = LogicalPlanBuilder::from(plan).project(expr_list)?.build()?;

        Ok(Arc::new(DataFrame::new(
            self.session_state.clone(),
            &project_plan,
        )))
    }

    /// Filter a DataFrame to only include rows that match the specified filter expression.
    ///
    /// ```
    /// # use datafusion::prelude::*;
    /// # use datafusion::error::Result;
    /// # #[tokio::main]
    /// # async fn main() -> Result<()> {
    /// let ctx = SessionContext::new();
    /// let df = ctx.read_csv("tests/example.csv", CsvReadOptions::new()).await?;
    /// let df = df.filter(col("a").lt_eq(col("b")))?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn filter(&self, predicate: Expr) -> Result<Arc<DataFrame>> {
        let plan = LogicalPlanBuilder::from(self.plan.clone())
            .filter(predicate)?
            .build()?;
        Ok(Arc::new(DataFrame::new(self.session_state.clone(), &plan)))
    }

    /// Perform an aggregate query with optional grouping expressions.
    ///
    /// ```
    /// # use datafusion::prelude::*;
    /// # use datafusion::error::Result;
    /// # #[tokio::main]
    /// # async fn main() -> Result<()> {
    /// let ctx = SessionContext::new();
    /// let df = ctx.read_csv("tests/example.csv", CsvReadOptions::new()).await?;
    ///
    /// // The following use is the equivalent of "SELECT MIN(b) GROUP BY a"
    /// let _ = df.aggregate(vec![col("a")], vec![min(col("b"))])?;
    ///
    /// // The following use is the equivalent of "SELECT MIN(b)"
    /// let _ = df.aggregate(vec![], vec![min(col("b"))])?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn aggregate(
        &self,
        group_expr: Vec<Expr>,
        aggr_expr: Vec<Expr>,
    ) -> Result<Arc<DataFrame>> {
        let plan = LogicalPlanBuilder::from(self.plan.clone())
            .aggregate(group_expr, aggr_expr)?
            .build()?;
        Ok(Arc::new(DataFrame::new(self.session_state.clone(), &plan)))
    }

    /// Limit the number of rows returned from this DataFrame.
    ///
    /// `skip` - Number of rows to skip before fetch any row
    ///
    /// `fetch` - Maximum number of rows to fetch, after skipping `skip` rows.
    /// ```
    /// # use datafusion::prelude::*;
    /// # use datafusion::error::Result;
    /// # #[tokio::main]
    /// # async fn main() -> Result<()> {
    /// let ctx = SessionContext::new();
    /// let df = ctx.read_csv("tests/example.csv", CsvReadOptions::new()).await?;
    /// let df = df.limit(0, Some(100))?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn limit(&self, skip: usize, fetch: Option<usize>) -> Result<Arc<DataFrame>> {
        let plan = LogicalPlanBuilder::from(self.plan.clone())
            .limit(skip, fetch)?
            .build()?;
        Ok(Arc::new(DataFrame::new(self.session_state.clone(), &plan)))
    }

    /// Calculate the union of two [`DataFrame`]s, preserving duplicate rows.The
    /// two [`DataFrame`]s must have exactly the same schema
    ///
    /// ```
    /// # use datafusion::prelude::*;
    /// # use datafusion::error::Result;
    /// # #[tokio::main]
    /// # async fn main() -> Result<()> {
    /// let ctx = SessionContext::new();
    /// let df = ctx.read_csv("tests/example.csv", CsvReadOptions::new()).await?;
    /// let df = df.union(df.clone())?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn union(&self, dataframe: Arc<DataFrame>) -> Result<Arc<DataFrame>> {
        let plan = LogicalPlanBuilder::from(self.plan.clone())
            .union(dataframe.plan.clone())?
            .build()?;
        Ok(Arc::new(DataFrame::new(self.session_state.clone(), &plan)))
    }

    /// Calculate the distinct union of two [`DataFrame`]s.  The
    /// two [`DataFrame`]s must have exactly the same schema
    ///
    /// ```
    /// # use datafusion::prelude::*;
    /// # use datafusion::error::Result;
    /// # #[tokio::main]
    /// # async fn main() -> Result<()> {
    /// let ctx = SessionContext::new();
    /// let df = ctx.read_csv("tests/example.csv", CsvReadOptions::new()).await?;
    /// let df = df.union_distinct(df.clone())?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn union_distinct(&self, dataframe: Arc<DataFrame>) -> Result<Arc<DataFrame>> {
        Ok(Arc::new(DataFrame::new(
            self.session_state.clone(),
            &LogicalPlanBuilder::from(self.plan.clone())
                .union_distinct(dataframe.plan.clone())?
                .build()?,
        )))
    }

    /// Filter out duplicate rows
    ///
    /// ```
    /// # use datafusion::prelude::*;
    /// # use datafusion::error::Result;
    /// # #[tokio::main]
    /// # async fn main() -> Result<()> {
    /// let ctx = SessionContext::new();
    /// let df = ctx.read_csv("tests/example.csv", CsvReadOptions::new()).await?;
    /// let df = df.distinct()?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn distinct(&self) -> Result<Arc<DataFrame>> {
        Ok(Arc::new(DataFrame::new(
            self.session_state.clone(),
            &LogicalPlanBuilder::from(self.plan.clone())
                .distinct()?
                .build()?,
        )))
    }

    /// Sort the DataFrame by the specified sorting expressions. Any expression can be turned into
    /// a sort expression by calling its [sort](../logical_plan/enum.Expr.html#method.sort) method.
    ///
    /// ```
    /// # use datafusion::prelude::*;
    /// # use datafusion::error::Result;
    /// # #[tokio::main]
    /// # async fn main() -> Result<()> {
    /// let ctx = SessionContext::new();
    /// let df = ctx.read_csv("tests/example.csv", CsvReadOptions::new()).await?;
    /// let df = df.sort(vec![col("a").sort(true, true), col("b").sort(false, false)])?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn sort(&self, expr: Vec<Expr>) -> Result<Arc<DataFrame>> {
        let plan = LogicalPlanBuilder::from(self.plan.clone())
            .sort(expr)?
            .build()?;
        Ok(Arc::new(DataFrame::new(self.session_state.clone(), &plan)))
    }

    /// Join this DataFrame with another DataFrame using the specified columns as join keys.
    ///
    /// Filter expression expected to contain non-equality predicates that can not be pushed
    /// down to any of join inputs.
    /// In case of outer join, filter applied to only matched rows.
    ///
    /// ```
    /// # use datafusion::prelude::*;
    /// # use datafusion::error::Result;
    /// # #[tokio::main]
    /// # async fn main() -> Result<()> {
    /// let ctx = SessionContext::new();
    /// let left = ctx.read_csv("tests/example.csv", CsvReadOptions::new()).await?;
    /// let right = ctx.read_csv("tests/example.csv", CsvReadOptions::new()).await?
    ///   .select(vec![
    ///     col("a").alias("a2"),
    ///     col("b").alias("b2"),
    ///     col("c").alias("c2")])?;
    /// let join = left.join(right, JoinType::Inner, &["a", "b"], &["a2", "b2"], None)?;
    /// let batches = join.collect().await?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn join(
        &self,
        right: Arc<DataFrame>,
        join_type: JoinType,
        left_cols: &[&str],
        right_cols: &[&str],
        filter: Option<Expr>,
    ) -> Result<Arc<DataFrame>> {
        let plan = LogicalPlanBuilder::from(self.plan.clone())
            .join(
                &right.plan.clone(),
                join_type,
                (left_cols.to_vec(), right_cols.to_vec()),
                filter,
            )?
            .build()?;
        Ok(Arc::new(DataFrame::new(self.session_state.clone(), &plan)))
    }

    // TODO: add join_using

    /// Repartition a DataFrame based on a logical partitioning scheme.
    ///
    /// ```
    /// # use datafusion::prelude::*;
    /// # use datafusion::error::Result;
    /// # #[tokio::main]
    /// # async fn main() -> Result<()> {
    /// let ctx = SessionContext::new();
    /// let df = ctx.read_csv("tests/example.csv", CsvReadOptions::new()).await?;
    /// let df1 = df.repartition(Partitioning::RoundRobinBatch(4))?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn repartition(
        &self,
        partitioning_scheme: Partitioning,
    ) -> Result<Arc<DataFrame>> {
        let plan = LogicalPlanBuilder::from(self.plan.clone())
            .repartition(partitioning_scheme)?
            .build()?;
        Ok(Arc::new(DataFrame::new(self.session_state.clone(), &plan)))
    }

    /// Convert the logical plan represented by this DataFrame into a physical plan and
    /// execute it, collecting all resulting batches into memory
    /// Executes this DataFrame and collects all results into a vector of RecordBatch.
    /// ```
    /// # use datafusion::prelude::*;
    /// # use datafusion::error::Result;
    /// # #[tokio::main]
    /// # async fn main() -> Result<()> {
    /// let ctx = SessionContext::new();
    /// let df = ctx.read_csv("tests/example.csv", CsvReadOptions::new()).await?;
    /// let batches = df.collect().await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn collect(&self) -> Result<Vec<RecordBatch>> {
        let plan = self.create_physical_plan().await?;
        let task_ctx = Arc::new(TaskContext::from(&self.session_state.read().clone()));
        collect(plan, task_ctx).await
    }

    /// Print results.
    ///
    /// ```
    /// # use datafusion::prelude::*;
    /// # use datafusion::error::Result;
    /// # #[tokio::main]
    /// # async fn main() -> Result<()> {
    /// let ctx = SessionContext::new();
    /// let df = ctx.read_csv("tests/example.csv", CsvReadOptions::new()).await?;
    /// df.show().await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn show(&self) -> Result<()> {
        let results = self.collect().await?;
        Ok(pretty::print_batches(&results)?)
    }

    /// Print results and limit rows.
    ///
    /// ```
    /// # use datafusion::prelude::*;
    /// # use datafusion::error::Result;
    /// # #[tokio::main]
    /// # async fn main() -> Result<()> {
    /// let ctx = SessionContext::new();
    /// let df = ctx.read_csv("tests/example.csv", CsvReadOptions::new()).await?;
    /// df.show_limit(10).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn show_limit(&self, num: usize) -> Result<()> {
        let results = self.limit(0, Some(num))?.collect().await?;
        Ok(pretty::print_batches(&results)?)
    }

    /// Executes this DataFrame and returns a stream over a single partition
    ///
    /// ```
    /// # use datafusion::prelude::*;
    /// # use datafusion::error::Result;
    /// # #[tokio::main]
    /// # async fn main() -> Result<()> {
    /// let ctx = SessionContext::new();
    /// let df = ctx.read_csv("tests/example.csv", CsvReadOptions::new()).await?;
    /// let stream = df.execute_stream().await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn execute_stream(&self) -> Result<SendableRecordBatchStream> {
        let plan = self.create_physical_plan().await?;
        let task_ctx = Arc::new(TaskContext::from(&self.session_state.read().clone()));
        execute_stream(plan, task_ctx).await
    }

    /// Executes this DataFrame and collects all results into a vector of vector of RecordBatch
    /// maintaining the input partitioning.
    ///
    /// ```
    /// # use datafusion::prelude::*;
    /// # use datafusion::error::Result;
    /// # #[tokio::main]
    /// # async fn main() -> Result<()> {
    /// let ctx = SessionContext::new();
    /// let df = ctx.read_csv("tests/example.csv", CsvReadOptions::new()).await?;
    /// let batches = df.collect_partitioned().await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn collect_partitioned(&self) -> Result<Vec<Vec<RecordBatch>>> {
        let plan = self.create_physical_plan().await?;
        let task_ctx = Arc::new(TaskContext::from(&self.session_state.read().clone()));
        collect_partitioned(plan, task_ctx).await
    }

    /// Executes this DataFrame and returns one stream per partition.
    ///
    /// ```
    /// # use datafusion::prelude::*;
    /// # use datafusion::error::Result;
    /// # #[tokio::main]
    /// # async fn main() -> Result<()> {
    /// let ctx = SessionContext::new();
    /// let df = ctx.read_csv("tests/example.csv", CsvReadOptions::new()).await?;
    /// let batches = df.execute_stream_partitioned().await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn execute_stream_partitioned(
        &self,
    ) -> Result<Vec<SendableRecordBatchStream>> {
        let plan = self.create_physical_plan().await?;
        let task_ctx = Arc::new(TaskContext::from(&self.session_state.read().clone()));
        execute_stream_partitioned(plan, task_ctx).await
    }

    /// Returns the schema describing the output of this DataFrame in terms of columns returned,
    /// where each column has a name, data type, and nullability attribute.

    /// ```
    /// # use datafusion::prelude::*;
    /// # use datafusion::error::Result;
    /// # #[tokio::main]
    /// # async fn main() -> Result<()> {
    /// let ctx = SessionContext::new();
    /// let df = ctx.read_csv("tests/example.csv", CsvReadOptions::new()).await?;
    /// let schema = df.schema();
    /// # Ok(())
    /// # }
    /// ```
    pub fn schema(&self) -> &DFSchema {
        self.plan.schema()
    }

    /// Return the unoptimized logical plan represented by this DataFrame.
    pub fn to_unoptimized_plan(&self) -> LogicalPlan {
        self.plan.clone()
    }

    /// Return the optimized logical plan represented by this DataFrame.
    pub fn to_logical_plan(&self) -> Result<LogicalPlan> {
        // Optimize the plan first for better UX
        let state = self.session_state.read().clone();
        state.optimize(&self.plan)
    }

    /// Return a DataFrame with the explanation of its plan so far.
    ///
    /// if `analyze` is specified, runs the plan and reports metrics
    ///
    /// ```
    /// # use datafusion::prelude::*;
    /// # use datafusion::error::Result;
    /// # #[tokio::main]
    /// # async fn main() -> Result<()> {
    /// let ctx = SessionContext::new();
    /// let df = ctx.read_csv("tests/example.csv", CsvReadOptions::new()).await?;
    /// let batches = df.limit(0, Some(100))?.explain(false, false)?.collect().await?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn explain(&self, verbose: bool, analyze: bool) -> Result<Arc<DataFrame>> {
        let plan = LogicalPlanBuilder::from(self.plan.clone())
            .explain(verbose, analyze)?
            .build()?;
        Ok(Arc::new(DataFrame::new(self.session_state.clone(), &plan)))
    }

    /// Return a `FunctionRegistry` used to plan udf's calls
    ///
    /// ```
    /// # use datafusion::prelude::*;
    /// # use datafusion::error::Result;
    /// # #[tokio::main]
    /// # async fn main() -> Result<()> {
    /// let ctx = SessionContext::new();
    /// let df = ctx.read_csv("tests/example.csv", CsvReadOptions::new()).await?;
    /// let f = df.registry();
    /// // use f.udf("name", vec![...]) to use the udf
    /// # Ok(())
    /// # }
    /// ```
    pub fn registry(&self) -> Arc<dyn FunctionRegistry> {
        let registry = self.session_state.read().clone();
        Arc::new(registry)
    }

    /// Calculate the intersection of two [`DataFrame`]s.  The two [`DataFrame`]s must have exactly the same schema
    ///
    /// ```
    /// # use datafusion::prelude::*;
    /// # use datafusion::error::Result;
    /// # #[tokio::main]
    /// # async fn main() -> Result<()> {
    /// let ctx = SessionContext::new();
    /// let df = ctx.read_csv("tests/example.csv", CsvReadOptions::new()).await?;
    /// let df = df.intersect(df.clone())?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn intersect(&self, dataframe: Arc<DataFrame>) -> Result<Arc<DataFrame>> {
        let left_plan = self.plan.clone();
        let right_plan = dataframe.plan.clone();
        Ok(Arc::new(DataFrame::new(
            self.session_state.clone(),
            &LogicalPlanBuilder::intersect(left_plan, right_plan, true)?,
        )))
    }

    /// Calculate the exception of two [`DataFrame`]s.  The two [`DataFrame`]s must have exactly the same schema
    ///
    /// ```
    /// # use datafusion::prelude::*;
    /// # use datafusion::error::Result;
    /// # #[tokio::main]
    /// # async fn main() -> Result<()> {
    /// let ctx = SessionContext::new();
    /// let df = ctx.read_csv("tests/example.csv", CsvReadOptions::new()).await?;
    /// let df = df.except(df.clone())?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn except(&self, dataframe: Arc<DataFrame>) -> Result<Arc<DataFrame>> {
        let left_plan = self.plan.clone();
        let right_plan = dataframe.plan.clone();

        Ok(Arc::new(DataFrame::new(
            self.session_state.clone(),
            &LogicalPlanBuilder::except(left_plan, right_plan, true)?,
        )))
    }

    /// Write a `DataFrame` to a CSV file.
    pub async fn write_csv(&self, path: &str) -> Result<()> {
        let plan = self.create_physical_plan().await?;
        let state = self.session_state.read().clone();
        plan_to_csv(&state, plan, path).await
    }

    /// Write a `DataFrame` to a Parquet file.
    pub async fn write_parquet(
        &self,
        path: &str,
        writer_properties: Option<WriterProperties>,
    ) -> Result<()> {
        let plan = self.create_physical_plan().await?;
        let state = self.session_state.read().clone();
        plan_to_parquet(&state, plan, path, writer_properties).await
    }

    /// Executes a query and writes the results to a partitioned JSON file.
    pub async fn write_json(&self, path: impl AsRef<str>) -> Result<()> {
        let plan = self.create_physical_plan().await?;
        let state = self.session_state.read().clone();
        plan_to_json(&state, plan, path).await
    }

    /// Add an additional column to the DataFrame.
    ///
    /// ```
    /// # use datafusion::prelude::*;
    /// # use datafusion::error::Result;
    /// # #[tokio::main]
    /// # async fn main() -> Result<()> {
    /// let ctx = SessionContext::new();
    /// let df = ctx.read_csv("tests/example.csv", CsvReadOptions::new()).await?;
    /// let df = df.with_column("ab_sum", col("a") + col("b"))?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn with_column(&self, name: &str, expr: Expr) -> Result<Arc<DataFrame>> {
        let window_func_exprs = find_window_exprs(&[expr.clone()]);
        let plan = if window_func_exprs.is_empty() {
            self.plan.clone()
        } else {
            LogicalPlanBuilder::window_plan(self.plan.clone(), window_func_exprs)?
        };

        let new_column = Expr::Alias(Box::new(expr), name.to_string());
        let mut col_exists = false;
        let mut fields: Vec<Expr> = plan
            .schema()
            .fields()
            .iter()
            .map(|f| {
                if f.name() == name {
                    col_exists = true;
                    new_column.clone()
                } else {
                    col(f.name())
                }
            })
            .collect();

        if !col_exists {
            fields.push(new_column);
        }

        let project_plan = LogicalPlanBuilder::from(plan).project(fields)?.build()?;

        Ok(Arc::new(DataFrame::new(
            self.session_state.clone(),
            &project_plan,
        )))
    }

    /// Rename one column by applying a new projection. This is a no-op if the column to be
    /// renamed does not exist.
    ///
    /// ```
    /// # use datafusion::prelude::*;
    /// # use datafusion::error::Result;
    /// # #[tokio::main]
    /// # async fn main() -> Result<()> {
    /// let ctx = SessionContext::new();
    /// let df = ctx.read_csv("tests/example.csv", CsvReadOptions::new()).await?;
    /// let df = df.with_column_renamed("ab_sum", "total")?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn with_column_renamed(
        &self,
        old_name: &str,
        new_name: &str,
    ) -> Result<Arc<DataFrame>> {
        let mut projection = vec![];
        let mut rename_applied = false;
        for field in self.plan.schema().fields() {
            let field_name = field.qualified_name();
            if old_name == field_name {
                projection.push(col(&field_name).alias(new_name));
                rename_applied = true;
            } else {
                projection.push(col(&field_name));
            }
        }
        if rename_applied {
            let project_plan = LogicalPlanBuilder::from(self.plan.clone())
                .project(projection)?
                .build()?;
            Ok(Arc::new(DataFrame::new(
                self.session_state.clone(),
                &project_plan,
            )))
        } else {
            Ok(Arc::new(DataFrame::new(
                self.session_state.clone(),
                &self.plan,
            )))
        }
    }

    /// Cache DataFrame as a memory table.
    ///
    /// ```
    /// # use datafusion::prelude::*;
    /// # use datafusion::error::Result;
    /// # #[tokio::main]
    /// # async fn main() -> Result<()> {
    /// let ctx = SessionContext::new();
    /// let df = ctx.read_csv("tests/example.csv", CsvReadOptions::new()).await?;
    /// let df = df.cache().await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn cache(&self) -> Result<Arc<DataFrame>> {
        let mem_table = MemTable::try_new(
            SchemaRef::from(self.schema().clone()),
            self.collect_partitioned().await?,
        )?;

        SessionContext::with_state(self.session_state.read().clone())
            .read_table(Arc::new(mem_table))
    }
}

// TODO: This will introduce a ref cycle (#2659)
#[async_trait]
impl TableProvider for DataFrame {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        let schema: Schema = self.plan.schema().as_ref().into();
        Arc::new(schema)
    }

    fn table_type(&self) -> TableType {
        TableType::View
    }

    async fn scan(
        &self,
        _ctx: &SessionState,
        projection: &Option<Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let expr = projection
            .as_ref()
            // construct projections
            .map_or_else(
                || {
                    Ok(Arc::new(Self::new(self.session_state.clone(), &self.plan))
                        as Arc<_>)
                },
                |projection| {
                    let schema = TableProvider::schema(self).project(projection)?;
                    let names = schema
                        .fields()
                        .iter()
                        .map(|field| field.name().as_str())
                        .collect::<Vec<_>>();
                    self.select_columns(names.as_slice())
                },
            )?
            // add predicates, otherwise use `true` as the predicate
            .filter(filters.iter().cloned().fold(
                Expr::Literal(ScalarValue::Boolean(Some(true))),
                |acc, new| acc.and(new),
            ))?;
        // add a limit if given
        Self::new(
            self.session_state.clone(),
            &limit
                .map_or_else(|| Ok(expr.clone()), |n| expr.limit(0, Some(n)))?
                .plan
                .clone(),
        )
        .create_physical_plan()
        .await
    }
}

#[cfg(test)]
mod tests {
    use std::vec;

    use super::*;
    use crate::execution::options::CsvReadOptions;
    use crate::physical_plan::ColumnarValue;
    use crate::test_util;
    use crate::{assert_batches_sorted_eq, execution::context::SessionContext};
    use arrow::datatypes::DataType;
    use datafusion_expr::{
        avg, cast, count, count_distinct, create_udf, lit, max, min, sum,
        BuiltInWindowFunction, ScalarFunctionImplementation, Volatility, WindowFunction,
    };

    #[tokio::test]
    async fn select_columns() -> Result<()> {
        // build plan using Table API

        let t = test_table().await?;
        let t2 = t.select_columns(&["c1", "c2", "c11"])?;
        let plan = t2.plan.clone();

        // build query using SQL
        let sql_plan = create_plan("SELECT c1, c2, c11 FROM aggregate_test_100").await?;

        // the two plans should be identical
        assert_same_plan(&plan, &sql_plan);

        Ok(())
    }

    #[tokio::test]
    async fn select_expr() -> Result<()> {
        // build plan using Table API
        let t = test_table().await?;
        let t2 = t.select(vec![col("c1"), col("c2"), col("c11")])?;
        let plan = t2.plan.clone();

        // build query using SQL
        let sql_plan = create_plan("SELECT c1, c2, c11 FROM aggregate_test_100").await?;

        // the two plans should be identical
        assert_same_plan(&plan, &sql_plan);

        Ok(())
    }

    #[tokio::test]
    async fn select_with_window_exprs() -> Result<()> {
        // build plan using Table API
        let t = test_table().await?;
        let first_row = Expr::WindowFunction {
            fun: WindowFunction::BuiltInWindowFunction(BuiltInWindowFunction::FirstValue),
            args: vec![col("aggregate_test_100.c1")],
            partition_by: vec![col("aggregate_test_100.c2")],
            order_by: vec![],
            window_frame: None,
        };
        let t2 = t.select(vec![col("c1"), first_row])?;
        let plan = t2.plan.clone();

        let sql_plan = create_plan(
            "select c1, first_value(c1) over (partition by c2) from aggregate_test_100",
        )
        .await?;

        assert_same_plan(&plan, &sql_plan);
        Ok(())
    }

    #[tokio::test]
    async fn aggregate() -> Result<()> {
        // build plan using DataFrame API
        let df = test_table().await?;
        let group_expr = vec![col("c1")];
        let aggr_expr = vec![
            min(col("c12")),
            max(col("c12")),
            avg(col("c12")),
            sum(col("c12")),
            count(col("c12")),
            count_distinct(col("c12")),
        ];

        let df: Vec<RecordBatch> = df.aggregate(group_expr, aggr_expr)?.collect().await?;

        assert_batches_sorted_eq!(
            vec![
                "+----+-----------------------------+-----------------------------+-----------------------------+-----------------------------+-------------------------------+----------------------------------------+",
                "| c1 | MIN(aggregate_test_100.c12) | MAX(aggregate_test_100.c12) | AVG(aggregate_test_100.c12) | SUM(aggregate_test_100.c12) | COUNT(aggregate_test_100.c12) | COUNT(DISTINCT aggregate_test_100.c12) |",
                "+----+-----------------------------+-----------------------------+-----------------------------+-----------------------------+-------------------------------+----------------------------------------+",
                "| a  | 0.02182578039211991         | 0.9800193410444061          | 0.48754517466109415         | 10.238448667882977          | 21                            | 21                                     |",
                "| b  | 0.04893135681998029         | 0.9185813970744787          | 0.41040709263815384         | 7.797734760124923           | 19                            | 19                                     |",
                "| c  | 0.0494924465469434          | 0.991517828651004           | 0.6600456536439784          | 13.860958726523545          | 21                            | 21                                     |",
                "| d  | 0.061029375346466685        | 0.9748360509016578          | 0.48855379387549824         | 8.793968289758968           | 18                            | 18                                     |",
                "| e  | 0.01479305307777301         | 0.9965400387585364          | 0.48600669271341534         | 10.206140546981722          | 21                            | 21                                     |",
                "+----+-----------------------------+-----------------------------+-----------------------------+-----------------------------+-------------------------------+----------------------------------------+",
            ],
            &df
        );

        Ok(())
    }

    #[tokio::test]
    async fn join() -> Result<()> {
        let left = test_table().await?.select_columns(&["c1", "c2"])?;
        let right = test_table_with_name("c2")
            .await?
            .select_columns(&["c1", "c3"])?;
        let left_rows = left.collect().await?;
        let right_rows = right.collect().await?;
        let join = left.join(right, JoinType::Inner, &["c1"], &["c1"], None)?;
        let join_rows = join.collect().await?;
        assert_eq!(100, left_rows.iter().map(|x| x.num_rows()).sum::<usize>());
        assert_eq!(100, right_rows.iter().map(|x| x.num_rows()).sum::<usize>());
        assert_eq!(2008, join_rows.iter().map(|x| x.num_rows()).sum::<usize>());
        Ok(())
    }

    #[tokio::test]
    async fn limit() -> Result<()> {
        // build query using Table API
        let t = test_table().await?;
        let t2 = t.select_columns(&["c1", "c2", "c11"])?.limit(0, Some(10))?;
        let plan = t2.plan.clone();

        // build query using SQL
        let sql_plan =
            create_plan("SELECT c1, c2, c11 FROM aggregate_test_100 LIMIT 10").await?;

        // the two plans should be identical
        assert_same_plan(&plan, &sql_plan);

        Ok(())
    }

    #[tokio::test]
    async fn explain() -> Result<()> {
        // build query using Table API
        let df = test_table().await?;
        let df = df
            .select_columns(&["c1", "c2", "c11"])?
            .limit(0, Some(10))?
            .explain(false, false)?;
        let plan = df.plan.clone();

        // build query using SQL
        let sql_plan =
            create_plan("EXPLAIN SELECT c1, c2, c11 FROM aggregate_test_100 LIMIT 10")
                .await?;

        // the two plans should be identical
        assert_same_plan(&plan, &sql_plan);

        Ok(())
    }

    #[tokio::test]
    async fn registry() -> Result<()> {
        let mut ctx = SessionContext::new();
        register_aggregate_csv(&mut ctx, "aggregate_test_100").await?;

        // declare the udf
        let my_fn: ScalarFunctionImplementation =
            Arc::new(|_: &[ColumnarValue]| unimplemented!("my_fn is not implemented"));

        // create and register the udf
        ctx.register_udf(create_udf(
            "my_fn",
            vec![DataType::Float64],
            Arc::new(DataType::Float64),
            Volatility::Immutable,
            my_fn,
        ));

        // build query with a UDF using DataFrame API
        let df = ctx.table("aggregate_test_100")?;

        let f = df.registry();

        let df = df.select(vec![f.udf("my_fn")?.call(vec![col("c12")])])?;
        let plan = df.plan.clone();

        // build query using SQL
        let sql_plan =
            ctx.create_logical_plan("SELECT my_fn(c12) FROM aggregate_test_100")?;

        // the two plans should be identical
        assert_same_plan(&plan, &sql_plan);

        Ok(())
    }

    #[tokio::test]
    async fn sendable() {
        let df = test_table().await.unwrap();
        // dataframes should be sendable between threads/tasks
        let task = tokio::task::spawn(async move {
            df.select_columns(&["c1"])
                .expect("should be usable in a task")
        });
        task.await.expect("task completed successfully");
    }

    #[tokio::test]
    async fn intersect() -> Result<()> {
        let df = test_table().await?.select_columns(&["c1", "c3"])?;
        let plan = df.intersect(df.clone())?;
        let result = plan.plan.clone();
        let expected = create_plan(
            "SELECT c1, c3 FROM aggregate_test_100
            INTERSECT ALL SELECT c1, c3 FROM aggregate_test_100",
        )
        .await?;
        assert_same_plan(&result, &expected);
        Ok(())
    }

    #[tokio::test]
    async fn except() -> Result<()> {
        let df = test_table().await?.select_columns(&["c1", "c3"])?;
        let plan = df.except(df.clone())?;
        let result = plan.plan.clone();
        let expected = create_plan(
            "SELECT c1, c3 FROM aggregate_test_100
            EXCEPT ALL SELECT c1, c3 FROM aggregate_test_100",
        )
        .await?;
        assert_same_plan(&result, &expected);
        Ok(())
    }

    #[tokio::test]
    async fn register_table() -> Result<()> {
        let df = test_table().await?.select_columns(&["c1", "c12"])?;
        let ctx = SessionContext::new();
        let df_impl = Arc::new(DataFrame::new(ctx.state.clone(), &df.plan.clone()));

        // register a dataframe as a table
        ctx.register_table("test_table", df_impl.clone())?;

        // pull the table out
        let table = ctx.table("test_table")?;

        let group_expr = vec![col("c1")];
        let aggr_expr = vec![sum(col("c12"))];

        // check that we correctly read from the table
        let df_results = &df_impl
            .aggregate(group_expr.clone(), aggr_expr.clone())?
            .collect()
            .await?;
        let table_results = &table.aggregate(group_expr, aggr_expr)?.collect().await?;

        assert_batches_sorted_eq!(
            vec![
                "+----+-----------------------------+",
                "| c1 | SUM(aggregate_test_100.c12) |",
                "+----+-----------------------------+",
                "| a  | 10.238448667882977          |",
                "| b  | 7.797734760124923           |",
                "| c  | 13.860958726523545          |",
                "| d  | 8.793968289758968           |",
                "| e  | 10.206140546981722          |",
                "+----+-----------------------------+",
            ],
            df_results
        );

        // the results are the same as the results from the view, modulo the leaf table name
        assert_batches_sorted_eq!(
            vec![
                "+----+---------------------+",
                "| c1 | SUM(test_table.c12) |",
                "+----+---------------------+",
                "| a  | 10.238448667882977  |",
                "| b  | 7.797734760124923   |",
                "| c  | 13.860958726523545  |",
                "| d  | 8.793968289758968   |",
                "| e  | 10.206140546981722  |",
                "+----+---------------------+",
            ],
            table_results
        );
        Ok(())
    }

    /// Compare the formatted string representation of two plans for equality
    fn assert_same_plan(plan1: &LogicalPlan, plan2: &LogicalPlan) {
        assert_eq!(format!("{:?}", plan1), format!("{:?}", plan2));
    }

    /// Create a logical plan from a SQL query
    async fn create_plan(sql: &str) -> Result<LogicalPlan> {
        let mut ctx = SessionContext::new();
        register_aggregate_csv(&mut ctx, "aggregate_test_100").await?;
        ctx.create_logical_plan(sql)
    }

    async fn test_table_with_name(name: &str) -> Result<Arc<DataFrame>> {
        let mut ctx = SessionContext::new();
        register_aggregate_csv(&mut ctx, name).await?;
        ctx.table(name)
    }

    async fn test_table() -> Result<Arc<DataFrame>> {
        test_table_with_name("aggregate_test_100").await
    }

    async fn register_aggregate_csv(
        ctx: &mut SessionContext,
        table_name: &str,
    ) -> Result<()> {
        let schema = test_util::aggr_test_schema();
        let testdata = test_util::arrow_test_data();
        ctx.register_csv(
            table_name,
            &format!("{}/csv/aggregate_test_100.csv", testdata),
            CsvReadOptions::new().schema(schema.as_ref()),
        )
        .await?;
        Ok(())
    }

    #[tokio::test]
    async fn with_column() -> Result<()> {
        let df = test_table().await?.select_columns(&["c1", "c2", "c3"])?;
        let ctx = SessionContext::new();
        let df_impl = Arc::new(DataFrame::new(ctx.state.clone(), &df.plan.clone()));

        let df = &df_impl
            .filter(col("c2").eq(lit(3)).and(col("c1").eq(lit("a"))))?
            .with_column("sum", col("c2") + col("c3"))?;

        // check that new column added
        let df_results = df.collect().await?;

        assert_batches_sorted_eq!(
            vec![
                "+----+----+-----+-----+",
                "| c1 | c2 | c3  | sum |",
                "+----+----+-----+-----+",
                "| a  | 3  | -12 | -9  |",
                "| a  | 3  | -72 | -69 |",
                "| a  | 3  | 13  | 16  |",
                "| a  | 3  | 13  | 16  |",
                "| a  | 3  | 14  | 17  |",
                "| a  | 3  | 17  | 20  |",
                "+----+----+-----+-----+",
            ],
            &df_results
        );

        // check that col with the same name ovwewritten
        let df_results_overwrite = df
            .with_column("c1", col("c2") + col("c3"))?
            .collect()
            .await?;

        assert_batches_sorted_eq!(
            vec![
                "+-----+----+-----+-----+",
                "| c1  | c2 | c3  | sum |",
                "+-----+----+-----+-----+",
                "| -69 | 3  | -72 | -69 |",
                "| -9  | 3  | -12 | -9  |",
                "| 16  | 3  | 13  | 16  |",
                "| 16  | 3  | 13  | 16  |",
                "| 17  | 3  | 14  | 17  |",
                "| 20  | 3  | 17  | 20  |",
                "+-----+----+-----+-----+",
            ],
            &df_results_overwrite
        );

        // check that col with the same name ovwewritten using same name as reference
        let df_results_overwrite_self =
            df.with_column("c2", col("c2") + lit(1))?.collect().await?;

        assert_batches_sorted_eq!(
            vec![
                "+----+----+-----+-----+",
                "| c1 | c2 | c3  | sum |",
                "+----+----+-----+-----+",
                "| a  | 4  | -12 | -9  |",
                "| a  | 4  | -72 | -69 |",
                "| a  | 4  | 13  | 16  |",
                "| a  | 4  | 13  | 16  |",
                "| a  | 4  | 14  | 17  |",
                "| a  | 4  | 17  | 20  |",
                "+----+----+-----+-----+",
            ],
            &df_results_overwrite_self
        );

        Ok(())
    }

    #[tokio::test]
    async fn with_column_renamed() -> Result<()> {
        let df = test_table()
            .await?
            .select_columns(&["c1", "c2", "c3"])?
            .filter(col("c2").eq(lit(3)).and(col("c1").eq(lit("a"))))?
            .limit(0, Some(1))?
            .sort(vec![
                // make the test deterministic
                col("c1").sort(true, true),
                col("c2").sort(true, true),
                col("c3").sort(true, true),
            ])?
            .with_column("sum", col("c2") + col("c3"))?;

        let df_sum_renamed = df.with_column_renamed("sum", "total")?.collect().await?;

        assert_batches_sorted_eq!(
            vec![
                "+----+----+----+-------+",
                "| c1 | c2 | c3 | total |",
                "+----+----+----+-------+",
                "| a  | 3  | 13 | 16    |",
                "+----+----+----+-------+",
            ],
            &df_sum_renamed
        );

        Ok(())
    }

    #[tokio::test]
    async fn with_column_renamed_join() -> Result<()> {
        let df = test_table().await?.select_columns(&["c1", "c2", "c3"])?;
        let ctx = SessionContext::new();
        ctx.register_table("t1", df.clone())?;
        ctx.register_table("t2", df)?;
        let df = ctx
            .table("t1")?
            .join(ctx.table("t2")?, JoinType::Inner, &["c1"], &["c1"], None)?
            .sort(vec![
                // make the test deterministic
                col("t1.c1").sort(true, true),
                col("t1.c2").sort(true, true),
                col("t1.c3").sort(true, true),
                col("t2.c1").sort(true, true),
                col("t2.c2").sort(true, true),
                col("t2.c3").sort(true, true),
            ])?
            .limit(0, Some(1))?;

        let df_results = df.collect().await?;
        assert_batches_sorted_eq!(
            vec![
                "+----+----+-----+----+----+-----+",
                "| c1 | c2 | c3  | c1 | c2 | c3  |",
                "+----+----+-----+----+----+-----+",
                "| a  | 1  | -85 | a  | 1  | -85 |",
                "+----+----+-----+----+----+-----+",
            ],
            &df_results
        );

        let df_renamed = df.with_column_renamed("t1.c1", "AAA")?;

        assert_eq!("\
        Projection: #t1.c1 AS AAA, #t1.c2, #t1.c3, #t2.c1, #t2.c2, #t2.c3\
        \n  Limit: skip=0, fetch=1\
        \n    Sort: #t1.c1 ASC NULLS FIRST, #t1.c2 ASC NULLS FIRST, #t1.c3 ASC NULLS FIRST, #t2.c1 ASC NULLS FIRST, #t2.c2 ASC NULLS FIRST, #t2.c3 ASC NULLS FIRST\
        \n      Inner Join: #t1.c1 = #t2.c1\
        \n        TableScan: t1\
        \n        TableScan: t2",
                   format!("{:?}", df_renamed.to_unoptimized_plan())
        );

        assert_eq!("\
        Projection: #t1.c1 AS AAA, #t1.c2, #t1.c3, #t2.c1, #t2.c2, #t2.c3\
        \n  Limit: skip=0, fetch=1\
        \n    Sort: #t1.c1 ASC NULLS FIRST, #t1.c2 ASC NULLS FIRST, #t1.c3 ASC NULLS FIRST, #t2.c1 ASC NULLS FIRST, #t2.c2 ASC NULLS FIRST, #t2.c3 ASC NULLS FIRST, fetch=1\
        \n      Inner Join: #t1.c1 = #t2.c1\
        \n        TableScan: t1 projection=[c1, c2, c3]\
        \n        TableScan: t2 projection=[c1, c2, c3]",
                   format!("{:?}", df_renamed.to_logical_plan()?)
        );

        let df_results = df_renamed.collect().await?;

        assert_batches_sorted_eq!(
            vec![
                "+-----+----+-----+----+----+-----+",
                "| AAA | c2 | c3  | c1 | c2 | c3  |",
                "+-----+----+-----+----+----+-----+",
                "| a   | 1  | -85 | a  | 1  | -85 |",
                "+-----+----+-----+----+----+-----+",
            ],
            &df_results
        );

        Ok(())
    }

    #[tokio::test]
    async fn cast_expr_test() -> Result<()> {
        let df = test_table()
            .await?
            .select_columns(&["c2", "c3"])?
            .limit(0, Some(1))?
            .with_column("sum", cast(col("c2") + col("c3"), DataType::Int64))?;

        let df_results = df.collect().await?;
        df.show().await?;
        assert_batches_sorted_eq!(
            vec![
                "+----+----+-----+",
                "| c2 | c3 | sum |",
                "+----+----+-----+",
                "| 2  | 1  | 3   |",
                "+----+----+-----+",
            ],
            &df_results
        );

        Ok(())
    }

    #[tokio::test]
    async fn row_writer_resize_test() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![arrow::datatypes::Field::new(
            "column_1",
            DataType::Utf8,
            false,
        )]));

        let data = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(arrow::array::StringArray::from(vec![
                    Some("2a0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000"),
                    Some("3a0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000800"),
                ]))
            ],
        )?;

        let table = crate::datasource::MemTable::try_new(schema, vec![vec![data]])?;

        let ctx = SessionContext::new();
        ctx.register_table("test", Arc::new(table))?;

        let sql = r#"
        SELECT 
            COUNT(1)
        FROM 
            test
        GROUP BY
            column_1"#;

        let df = ctx.sql(sql).await.unwrap();
        df.show_limit(10).await.unwrap();

        Ok(())
    }

    #[tokio::test]
    async fn cache_test() -> Result<()> {
        let df = test_table()
            .await?
            .select_columns(&["c2", "c3"])?
            .limit(0, Some(1))?
            .with_column("sum", cast(col("c2") + col("c3"), DataType::Int64))?;

        let cached_df = df.cache().await?;

        assert_eq!(
            "TableScan: ?table? projection=[c2, c3, sum]",
            format!("{:?}", cached_df.to_logical_plan()?)
        );

        let df_results = df.collect().await?;
        let cached_df_results = cached_df.collect().await?;
        assert_batches_sorted_eq!(
            vec![
                "+----+----+-----+",
                "| c2 | c3 | sum |",
                "+----+----+-----+",
                "| 2  | 1  | 3   |",
                "+----+----+-----+",
            ],
            &cached_df_results
        );

        assert_eq!(&df_results, &cached_df_results);

        Ok(())
    }
}
